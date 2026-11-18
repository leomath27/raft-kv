CC      ?= gcc
CFLAGS  ?= -std=gnu11 -O2 -g -Wall -Wextra -Wno-unused-parameter
LDFLAGS ?=
B       := build

LIB_SRC := src/util.c src/resp.c src/kv.c src/raft.c src/wal.c
LIB_OBJ := $(LIB_SRC:src/%.c=$(B)/%.o)

all: $(B)/raft-kv $(B)/sim

$(B)/%.o: src/%.c src/*.h | $(B)
	$(CC) $(CFLAGS) -c $< -o $@

$(B)/raft-kv: $(LIB_OBJ) $(B)/server.o $(B)/main.o
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

$(B)/sim: tests/sim.c $(B)/raft.o $(B)/util.o src/*.h | $(B)
	$(CC) $(CFLAGS) tests/sim.c $(B)/raft.o $(B)/util.o -o $@

$(B)/unit: tests/unit.c $(LIB_OBJ) | $(B)
	$(CC) $(CFLAGS) tests/unit.c $(LIB_OBJ) -o $@

$(B):
	mkdir -p $(B)

# ---- tests ----
sim: $(B)/sim
	$(B)/sim 300

unit: $(B)/unit
	$(B)/unit

# sanitizer build of everything (slower; catches memory bugs in the core)
asan:
	$(MAKE) clean
	$(MAKE) CFLAGS="-std=gnu11 -O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer -Wall -Wextra -Wno-unused-parameter" LDFLAGS="-fsanitize=address,undefined"

check: all unit sim
	python3 tests/harness/integration.py

jepsen: all
	python3 tests/harness/jepsen.py

clean:
	rm -rf $(B)

.PHONY: all sim unit asan check jepsen clean

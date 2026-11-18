/* unit.c - RESP parser, KV state machine (incl. exactly-once sessions), WAL
 * crash recovery and snapshot files. */
#include <fcntl.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../src/kv.h"
#include "../src/resp.h"
#include "../src/wal.h"

static int fails, checks;
#define CHECK(c) do { checks++; if (!(c)) { fails++; fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #c); } } while (0)

static void parse_one(const char *in, int expect, int argc, const char *a0) {
    resp_cmd_t c; cmd_init(&c);
    size_t used = 0;
    int r = resp_parse((const uint8_t *)in, strlen(in), &c, &used);
    CHECK(r == expect);
    if (expect == 1) { CHECK(c.argc == argc); if (argc) CHECK(mem_ieq(c.argv[0], c.lens[0], a0)); }
    cmd_free(&c);
}

static void test_resp(void) {
    parse_one("*2\r\n$3\r\nGET\r\n$1\r\nk\r\n", 1, 2, "GET");
    parse_one("*2\r\n$3\r\nGET\r\n$1\r\n", 0, 0, NULL);          /* partial */
    parse_one("*2\r\n$3\r\nGET\r\n$1\r\nk\r", 0, 0, NULL);
    parse_one("PING\r\n", 1, 1, "PING");                          /* inline */
    parse_one("SET a  b\r\n", 1, 3, "SET");
    parse_one("*1\r\n$x\r\n", -1, 0, NULL);                       /* malformed */
    parse_one("*-5\r\n", -1, 0, NULL);
    parse_one("*1\r\n$3\r\nabcXY", -1, 0, NULL);                  /* bad terminator */
    parse_one("*0\r\n", 1, 0, NULL);
    /* binary-safe bulk containing CRLF */
    const char bin[] = "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$4\r\na\r\nb\r\n";
    resp_cmd_t c; cmd_init(&c); size_t used;
    CHECK(resp_parse((const uint8_t *)bin, sizeof bin - 1, &c, &used) == 1 && c.lens[2] == 4 && memcmp(c.argv[2], "a\r\nb", 4) == 0);
    cmd_free(&c);
}

static char *run(kv_t *kv, int argc, ...) { /* returns reply text (malloc) */
    va_list ap; va_start(ap, argc);
    const uint8_t *av[16]; size_t al[16];
    for (int i = 0; i < argc; i++) { const char *s = va_arg(ap, const char *); av[i] = (const uint8_t *)s; al[i] = strlen(s); }
    va_end(ap);
    buf_t e, out; buf_init(&e); buf_init(&out);
    cmd_encode(&e, argc, av, al);
    kv_apply(kv, buf_data(&e), buf_len(&e), &out);
    char *r = strndup((char *)buf_data(&out), buf_len(&out));
    buf_free(&e); buf_free(&out);
    return r;
}
#define EQ(kv, expect, ...) do { char *_r = run(kv, __VA_ARGS__); CHECK(strcmp(_r, expect) == 0); if (strcmp(_r, expect)) fprintf(stderr, "   got %s", _r); free(_r); } while (0)

static void test_kv(void) {
    kv_t *kv = kv_new();
    EQ(kv, "$-1\r\n", 2, "GET", "a");
    EQ(kv, "+OK\r\n", 3, "SET", "a", "1");
    EQ(kv, "$1\r\n1\r\n", 2, "GET", "a");
    EQ(kv, ":2\r\n", 2, "INCR", "a");
    EQ(kv, ":5\r\n", 3, "INCRBY", "a", "3");
    EQ(kv, ":4\r\n", 3, "DECRBY", "a", "1");
    EQ(kv, ":1\r\n", 4, "CAS", "a", "4", "10");
    EQ(kv, ":0\r\n", 4, "CAS", "a", "4", "11");
    EQ(kv, ":0\r\n", 4, "CAS", "missing", "x", "y");
    EQ(kv, ":1\r\n", 3, "DEL", "a", "zz");
    EQ(kv, ":0\r\n", 2, "EXISTS", "a");
    EQ(kv, "+OK\r\n", 3, "SET", "s", "notnum");
    EQ(kv, "-ERR value is not an integer or out of range\r\n", 2, "INCR", "s");
    EQ(kv, "-ERR wrong number of arguments\r\n", 2, "SET", "x");
    EQ(kv, ":1\r\n", 1, "DBSIZE");
    /* exactly-once: a retried command with the same (client,seq) is not re-executed */
    EQ(kv, ":1\r\n", 5, "INCR", "ctr", "IDEM", "7", "1");
    EQ(kv, ":1\r\n", 5, "INCR", "ctr", "IDEM", "7", "1");   /* duplicate: same reply, no second increment */
    EQ(kv, ":2\r\n", 5, "INCR", "ctr", "IDEM", "7", "2");
    EQ(kv, ":2\r\n", 5, "INCR", "ctr", "IDEM", "7", "2");
    EQ(kv, "-ERR stale idempotency sequence\r\n", 5, "INCR", "ctr", "IDEM", "7", "1");
    EQ(kv, ":1\r\n", 5, "INCR", "other", "IDEM", "8", "1"); /* independent client */
    EQ(kv, "$1\r\n2\r\n", 2, "GET", "ctr");
    /* snapshot round trip keeps data AND sessions */
    uint64_t d = kv_digest(kv);
    buf_t sb; buf_init(&sb); kv_serialize(kv, &sb);
    kv_t *kv2 = kv_new();
    CHECK(kv_load(kv2, buf_data(&sb), buf_len(&sb)) == 0);
    CHECK(kv_digest(kv2) == d && kv_size(kv2) == kv_size(kv));
    EQ(kv2, ":2\r\n", 5, "INCR", "ctr", "IDEM", "7", "2");   /* dedup survives the snapshot */
    CHECK(kv_load(kv2, buf_data(&sb), buf_len(&sb) / 2) != 0); /* truncated snapshot rejected */
    buf_free(&sb); kv_free(kv); kv_free(kv2);
}

static void mk_entries(raft_entry_t *e, int n, uint64_t term) {
    for (int i = 0; i < n; i++) {
        e[i].term = term; e[i].type = ENT_CMD; e[i].len = 5;
        e[i].data = malloc(5); snprintf((char *)e[i].data, 5, "e%03d", i % 1000);
    }
}

static void test_wal(void) {
    char dir[] = "/tmp/rkv-unit-XXXXXX";
    CHECK(mkdtemp(dir) != NULL);
    wal_state_t st;
    wal_t *w = wal_open(dir, 0, 1, &st);
    CHECK(w && st.n == 0 && st.term == 0);
    raft_entry_t e[10]; mk_entries(e, 10, 3);
    wal_state(w, 3, 2);
    for (int i = 0; i < 10; i++) wal_append(w, (uint64_t)i + 1, &e[i]);
    wal_truncate(w, 8);                                   /* drop 8..10 */
    raft_entry_t e2[2]; mk_entries(e2, 2, 4);
    wal_append(w, 8, &e2[0]); wal_append(w, 9, &e2[1]);   /* new-term suffix */
    wal_state(w, 4, 0);
    wal_sync(w);
    wal_close(w);

    w = wal_open(dir, 0, 1, &st);
    CHECK(w && st.n == 9 && st.term == 4 && st.vote == 0);
    CHECK(st.ents[6].term == 3 && st.ents[7].term == 4 && st.ents[8].term == 4);
    CHECK(memcmp(st.ents[2].data, "e002", 4) == 0);
    for (size_t i = 0; i < st.n; i++) free(st.ents[i].data);
    free(st.ents);

    /* a second process must not be able to open the same WAL */
    wal_state_t st2; CHECK(wal_open(dir, 0, 1, &st2) == NULL);

    /* append more, then simulate a crash mid-record: torn tail must be discarded */
    raft_entry_t e3[1]; mk_entries(e3, 1, 4);
    wal_append(w, 10, &e3[0]);
    wal_sync(w);
    uint64_t good = wal_bytes_written(w);
    wal_close(w);
    char path[300]; snprintf(path, sizeof path, "%s/raft.wal", dir);
    int fd = open(path, O_WRONLY | O_APPEND);
    CHECK(write(fd, "\x40\x00\x00\x00\xde\xad\xbe\xef" "partial-garbage", 23) == 23);
    close(fd);
    w = wal_open(dir, 0, 1, &st);
    CHECK(w && st.n == 10);                               /* garbage dropped, 10 good entries kept */
    for (size_t i = 0; i < st.n; i++) free(st.ents[i].data);
    free(st.ents);
    struct stat sb; stat(path, &sb); CHECK((uint64_t)sb.st_size == good);

    /* bit-flip inside a record => everything from there on is discarded */
    wal_close(w);
    fd = open(path, O_RDWR);
    off_t mid = (off_t)(good / 2);
    uint8_t b; CHECK(pread(fd, &b, 1, mid) == 1); b ^= 0xff; CHECK(pwrite(fd, &b, 1, mid) == 1);
    close(fd);
    w = wal_open(dir, 0, 1, &st);
    CHECK(w && st.n < 10);
    for (size_t i = 0; i < st.n; i++) free(st.ents[i].data);
    free(st.ents);

    /* rewrite (compaction): keep entries after index 5 */
    raft_entry_t keep[3]; mk_entries(keep, 3, 4);
    wal_rewrite(w, 4, 1, 6, keep, 3);
    wal_close(w);
    w = wal_open(dir, 5, 1, &st);
    CHECK(w && st.n == 3 && st.term == 4 && st.vote == 1);
    for (size_t i = 0; i < st.n; i++) free(st.ents[i].data);
    free(st.ents);
    wal_close(w);

    /* snapshot files */
    uint8_t blob[100]; memset(blob, 0xab, sizeof blob);
    CHECK(snap_save(dir, 42, 7, blob, sizeof blob, 1) == 0);
    uint64_t idx, term; uint8_t *data; size_t len;
    CHECK(snap_load(dir, &idx, &term, &data, &len) == 0 && idx == 42 && term == 7 && len == 100 && !memcmp(data, blob, 100));
    free(data);
    snprintf(path, sizeof path, "%s/snapshot.bin", dir);
    fd = open(path, O_RDWR); CHECK(pwrite(fd, "X", 1, 50) == 1); close(fd);
    CHECK(snap_load(dir, &idx, &term, &data, &len) == -1);  /* CRC catches corruption */
    unlink(path);
    CHECK(snap_load(dir, &idx, &term, &data, &len) == 1);   /* none */

    char cmd[300]; snprintf(cmd, sizeof cmd, "rm -rf %s", dir); (void)!system(cmd);
}

int main(void) {
    g_log_level = LOG_QUIET;
    test_resp(); test_kv(); test_wal();
    printf("unit tests: %d checks, %d failures\n", checks, fails);
    return fails ? 1 : 0;
}

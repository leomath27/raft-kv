# raft-kv

A replicated, fault-tolerant key-value store in C. Nodes elect a leader, replicate a write-ahead log, commit by majority, and keep serving correct, linearizable results while nodes crash, restart, pause or get partitioned. It speaks the Redis protocol (RESP2), so `redis-cli`, `redis-benchmark` and client libraries work against it.

~2,900 lines of C11 (plus ~2,100 lines of tests), no dependencies. Single-threaded epoll event loop.

```
        clients (RESP)                       Raft peer links (TCP, one dial per peer)
   redis-cli / any Redis client  ───►  ┌─────────────────────────────────────────────┐
                                        │ node 1 (leader)  ◄────────────►  node 2     │
   -MOVED 0 host:port  ◄── followers    │   epoll loop → RESP parse → propose          │
                                        │   Raft core ─► WAL (group-commit fsync)      │
                                        │   apply ─► KV state machine + session table │
                                        │              ▲ snapshot / InstallSnapshot    │
                                        └──────────────┴──────────────────────────────┘
```

## Quick start

```sh
make                         # builds build/raft-kv and build/sim
scripts/cluster.sh start 3   # 3 nodes on 127.0.0.1:6381-6383 (raft 7381-7383), data in ./data
redis-cli -c -p 6381 SET hello world      # -c follows the -MOVED redirect to the leader
redis-cli -c -p 6382 GET hello
redis-cli -p 6381 INFO                    # role, term, commit/applied index, snapshot index ...
kill -9 $(cat data/n1.pid); sleep 1; scripts/cluster.sh status 3    # a new leader appears; nothing is lost
scripts/cluster.sh stop
```

Run a node by hand: `build/raft-kv --id 1 --nodes h1:6381:7381,h2:6382:7382,h3:6383:7383 --dir /var/lib/raftkv` (see `--help`).

## What is implemented

**Raft core** (`src/raft.c`, transport- and storage-agnostic; every side effect goes through a callback table, so the same code runs in the real server and in the simulator):

- Leader election with randomized timeouts, **PreVote** and **leader stickiness** (a partitioned or flapping node cannot depose a healthy leader or inflate terms), and **CheckQuorum** (a leader that loses its majority steps down instead of serving stale data).
- Log replication with consistency check, fast conflict backtracking, and batched replication with a one-batch-per-peer flow-control window.
- Majority commit, advancing only for entries of the leader's own term (Raft paper §5.4.2 / Fig. 8), with a no-op appended at election. The leader counts **its own durable index**, not its in-memory one.
- **Log compaction + InstallSnapshot** for followers that fall behind the leader's snapshot.
- **ReadIndex linearizable reads**: reads never touch the log; the leader confirms it is still leader with a heartbeat round started after the read arrived (batched across all reads in one loop iteration), then serves from the applied state.

**Durability** (`src/wal.c`): CRC-framed WAL for hard state + entries, torn-tail detection on recovery, atomic snapshot files (tmp + fsync + rename + dir fsync), exclusive `flock` so two processes cannot share a data dir. All writes of one event-loop iteration share **one `fdatasync` (group commit)**, and nothing produced in that iteration (votes, append-acks, client replies) leaves the process before it. A failed write/fsync aborts the process rather than continuing on unknown disk state.

**State machine** (`src/kv.c`): binary-safe hash table; `SET GET DEL EXISTS INCR DECR INCRBY DECRBY CAS DBSIZE FLUSHALL`. Appending `IDEM <client-id> <seq>` to a write makes it **exactly-once across retries and leader changes**: the session table is part of the replicated state and of snapshots, and replays the original reply for a duplicate.

**Server** (`src/server.c`): RESP2 incl. inline commands; replies stay in request order under deep pipelining even though writes and reads complete at different times; a pipelined read observes the writes sent before it on the same connection. Followers answer `-MOVED 0 host:port`, so `redis-cli -c` and cluster-aware clients follow the leader. A write that was interrupted by a leadership change gets `-TIMEOUT ... may or may not have been applied` (honestly indeterminate), and a write whose log slot was overwritten gets `-TRYAGAIN` (definitely not applied).

**Introspection / test hooks**: `INFO` / `RAFT STATUS`, `DEBUG DIGEST` (state hash), `LOCALGET` (deliberately stale local read), `CHAOS ISOLATE <ids>` / `CHAOS HEAL` (drop Raft traffic to/from peers, used to build partitions without root or iptables). Note these are unauthenticated; see Limitations.

## How it is tested

| Layer | What | Where |
|---|---|---|
| Deterministic simulation | 1/2/3/5-node clusters in virtual time over a network that drops, duplicates, delays and reorders messages, partitions, crashes nodes (losing everything not yet synced), compacts and installs snapshots. After every tick it checks **Election Safety, State Machine Safety, Leader Completeness, Log Matching, leader append-only, term monotonicity**; after faults stop it requires a leader, progress and **identical replica state**. Targeted faults: crash a voter right after it votes, crash a leader right after election. | `tests/sim.c`, `make sim` |
| Unit | RESP parser (partial/malformed/binary), KV semantics, exactly-once sessions incl. snapshot round-trip, WAL replay, torn tail, bit-flip, rewrite, lock, snapshot CRC | `tests/unit.c`, `make unit` |
| Integration | real processes: failover and rejoin, full-cluster `kill -9`, no progress without quorum, isolated leader can neither commit nor serve reads, exactly-once through failover, InstallSnapshot for a lagging follower, restart from snapshot + WAL tail, 1/3/5 nodes, pipelining order, protocol robustness | `tests/harness/integration.py`, `make check` |
| Jepsen-style | concurrent clients doing read/write/CAS while a nemesis does `kill -9` of the leader / random node / a majority, SIGSTOP of the leader, leader isolation, split-brain partitions, and power-failure of every node. Every op is timestamped; timeouts are recorded as *indeterminate*. The history is checked for **linearizability** (Wing-Gong-Lowe search, per key) and replicas must converge to identical state. | `tests/harness/jepsen.py`, `make jepsen` |
| Sanitizers | all of the above also run against `make asan` (ASan + UBSan) builds | `make asan` |

"Jepsen-style" means the same method (concurrent history + fault injection + linearizability check), implemented here in Python against real processes; it is not the Clojure Jepsen tool.

### Do the tests have teeth?

A checker that cannot fail proves nothing, so the harnesses were validated:

- **Simulator, mutation tests.** I broke `raft.c` on purpose and ran the simulator: *granting votes without the log up-to-date check* → caught as a Leader Completeness violation (seed 5, t=4s); *not truncating conflicting entries* → caught as a State Machine Safety violation; *follower commit index not capped by the last new entry* → caught (assertion); *not persisting the vote* → caught as **two leaders in one term** (only after adding the targeted "crash a voter right after it votes" fault; random faults alone missed it). *Removing both the no-op and the current-term commit rule* (the Fig. 8 bug) was flagged as a liveness failure rather than the classic lost-commit safety violation, so I make no claim that the simulator reproduces Fig. 8.
- **Jepsen-style, negative control.** `jepsen.py --stale-reads` serves reads from any replica without consensus; the checker reports `VIOLATION` (a read returns nil after the value was written). The checker also has a self-test for stale reads, ghost values, lost acknowledged writes and CAS semantics (`python3 tests/harness/checker.py`).
- **Bugs the tests found in this code** (all fixed): a heartbeat response freed the in-flight window while the real batch was still in transit, which made the leader re-send the same entries on every heartbeat and grew send queues past 64 MB; `buf_reserve` compaction was quadratic on large queues; a pipelined `GET` right behind a `SET` on one connection could miss it; and two bugs in the *test harness* itself (a Leader Completeness check that ignored the commit term, and a probe that never left an isolated old leader).

## Measured results

All numbers below were produced by the scripts in this repo on one 2-core Linux sandbox VM (loopback, **all 3 server processes plus the load generator share those 2 cores**, real `fdatasync` unless stated). They are a reproducible sanity check, not a performance claim for real hardware.

**Verification volume**

- Simulator: **3,000 fresh seeds × 30 s virtual time** (plus 1,500 earlier) with no violation: 27,578 elections, 37,087 node crashes, 3,670 log truncations, 455,794 snapshots taken, 79,303 snapshot installs, 41.7 M messages (2.1 M dropped/partitioned).
- Jepsen-style, 3 nodes, 90 s: **48,427 ops completed, 6 indeterminate, linearizable**; faults: 4× crash-all, 5× kill-majority, 4× kill-random, 6× split partition, 1× leader isolation, 2× leader SIGSTOP; replicas converged to the same digest. 5 nodes with snapshots every 300 entries, 60 s: **36,720 ops, linearizable**. 3 nodes without idempotency tokens (timeouts stay indeterminate), 60 s: **29,460 ops + 47 indeterminate, linearizable**.
- All 11 integration tests and 61 unit checks pass, also under ASan/UBSan; a sanitizer-built cluster survived a fault-injection run with kept logs and zero sanitizer reports.

**Leader failover** (`bench.py failover`, 30 trials, 3 nodes, election timeout 150–300 ms, heartbeat 50 ms, leader killed with SIGKILL):

| | min | median | p95 | max |
|---|---|---|---|---|
| new leader elected | 155 ms | **187 ms** | 255 ms | 264 ms |
| first write committed by the new leader | 158 ms | **191 ms** | 260 ms | 267 ms |

Isolating the leader with `CHAOS` (40 trials): new leader after median 184 ms / max 273 ms; the isolated old leader steps down on its own after ~290–415 ms (CheckQuorum). Failover is bounded below by the election timeout; lowering `--election-min/--election-max` trades it against spurious elections.

**Fault tolerance**: tolerates ⌊(n−1)/2⌋ node failures: 1 of 3, 2 of 5 (tested: 5-node cluster keeps serving after losing 2 nodes; with a majority down it refuses writes rather than acknowledging them, then recovers with no acknowledged write lost).

**Throughput** (`bench.py throughput`, `redis-benchmark`, 32-byte values, 100k random keys, 200k requests, 3 nodes; GET is a **linearizable** ReadIndex read):

| | clients × pipeline | fsync on | fsync off |
|---|---|---|---|
| SET | 50 × 1 | 29.4k/s (p50 1.46 ms) | 82.1k/s |
| SET | 50 × 16 | 139.5k/s (p50 3.9 ms) | 239.8k/s |
| GET | 50 × 1 | 118.4k/s (p50 0.26 ms) | 107.8k/s |
| GET | 50 × 16 | 763k/s | 803k/s |

Unpipelined writes are bounded by one `fdatasync` per batch of concurrent writes (group commit); pipelining amortizes it. Variation between runs on this shared VM is roughly ±15%; the raw output is in `results/`.

## Design notes worth knowing

- **Why messages are held until the WAL sync.** The Raft core only *queues* persistence; the driver does one `fdatasync` per event-loop iteration and only then flushes sockets. That gives group commit and makes "never reply before durable" structural rather than something each handler must remember. The simulator enforces the same discipline (a crash discards un-synced writes *and* un-released messages).
- **Reads.** Linearizable reads cost a heartbeat round but no log entry and no fsync; reads that arrive in the same loop iteration share one round. A leader refuses to serve reads until it has committed an entry of its own term.
- **Stale-leader safety does not depend on clocks.** There are no leases; a deposed leader cannot answer reads because ReadIndex needs a fresh majority acknowledgement (verified by the integration test and by SIGSTOP/partition nemeses).
- **Exactly-once.** `IDEM` tokens are the standard session-table answer to "the client timed out; did my write happen?". Without a token a timed-out write is genuinely indeterminate and the API says so.

## Limitations (what this is not)

- **Static membership**: no joint-consensus / single-server membership changes; the cluster size is fixed at start (max 9).
- **Snapshots are blocking**: the state is serialized in the event loop (no fork/copy-on-write), and InstallSnapshot sends the whole snapshot in one message. Fine for a demo-sized keyspace; a large one would stall the node.
- **Memory-resident data set**, single-threaded, strings only, no expiry/transactions/scan.
- **Session table has no eviction**: one small entry per client id that ever used `IDEM`.
- **No auth/TLS, IPv4 only.** `CHAOS` and `DEBUG` commands are always enabled; they should be compiled out or gated before any real deployment.
- `--no-fsync` is for benchmarking only and is **not crash-safe**.
- Not formally verified (no TLA+/model checking), and the simulator's coverage is statistical. Disk faults other than torn writes and bit flips in the WAL tail (e.g. silent corruption in the middle of an old segment) are not modelled.

## Using the Raft core inside another server

`src/raft.h` is the integration surface: provide the `raft_ops_t` callbacks (`send`, `save_state`, `append`, `truncate`, `apply`, `become`, snapshot load/install), call `raft_tick()` on a timer and `raft_step()` per incoming message, `raft_propose()` for client commands, and follow the durability contract in the header (queue writes → make them durable → `raft_persisted()` → only then release messages). `src/server.c` is a complete reference driver.

## Repository layout

```
src/raft.{c,h}    Raft core            src/wal.{c,h}     WAL + snapshot files
src/kv.{c,h}      state machine        src/resp.{c,h}    RESP parser/encoders
src/server.{c,h}  event loop, glue     src/main.c        CLI
tests/sim.c       deterministic simulation       tests/unit.c   unit tests
tests/harness/    cluster.py, integration.py, jepsen.py, checker.py, bench.py
scripts/cluster.sh  local cluster helper         results/       raw output of the runs above
```

## Resume bullets (every number is measured above)

- Built **raft-kv**, a replicated key-value store in C (~2.9k lines of C, epoll, RESP-compatible) implementing Raft leader election with PreVote and CheckQuorum, majority-commit log replication, a CRC-framed WAL with group-commit fsync, snapshotting/InstallSnapshot, and ReadIndex linearizable reads.
- Measured **median 187 ms (max 264 ms) leader failover** and 191 ms to the first committed write over 30 `kill -9` trials (3 nodes, 150–300 ms election timeout); tolerates f node failures in a 2f+1 cluster (1/3, 2/5).
- Validated correctness with a deterministic fault-injection simulator (3,000 seeds; message loss/duplication/reordering, partitions, crashes that discard unsynced state) checking Election Safety, State Machine Safety, Leader Completeness and Log Matching, and with a Jepsen-style harness (kill -9, SIGSTOP, partitions, full-cluster power loss) whose recorded histories of **48k+ concurrent operations passed a Wing-Gong linearizability check with no lost acknowledged writes**; proved the checkers can fail via injected-bug mutation tests and a negative-control run.
- Reached **~140k writes/s (fsync on) and ~760k linearizable reads/s** pipelined on a shared 2-core VM via group commit and batched ReadIndex; found and fixed a replication-window bug (duplicate batch resends) and a quadratic buffer-compaction bug.

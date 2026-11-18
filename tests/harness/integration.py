#!/usr/bin/env python3
"""Integration tests against real raft-kv processes (kill -9, partitions, restarts)."""
import os
import sys
import time
import traceback

sys.path.insert(0, os.path.dirname(__file__))
from cluster import Client, Cluster, RespError  # noqa: E402

TESTS = []


def test(fn):
    TESTS.append(fn)
    return fn


def expect_error(prefix, fn):
    try:
        fn()
    except RespError as e:
        assert str(e).startswith(prefix), "expected %s..., got %s" % (prefix, e)
        return str(e)
    raise AssertionError("expected an error starting with %s" % prefix)


@test
def basic_replication_and_redirect():
    with Cluster(3) as c:
        c.start_all()
        L = c.wait_leader()
        assert c.call("SET", "a", "1") == "OK"
        assert c.call("INCR", "a") == 2
        assert c.call("GET", "a") == "2"
        # followers refuse writes AND linearizable reads, pointing at the leader
        f = next(i for i in range(1, 4) if i != L)
        cl = c.client(f)
        err = expect_error("MOVED", lambda: cl.call("SET", "x", "y"))
        assert err.endswith(":%d" % c.cports[L - 1]), err
        expect_error("MOVED", lambda: cl.call("GET", "a"))
        cl.close()
        d = c.wait_converged()
        assert len(d) == 3
        # a stale local read on a follower sees the replicated value
        cl = c.client(f)
        assert cl.call("LOCALGET", "a") == "2"
        cl.close()


@test
def pipelining_keeps_reply_order():
    with Cluster(3) as c:
        c.start_all()
        L = c.wait_leader()
        cl = c.client(L, timeout=10)
        cmds = []
        for i in range(500):
            cmds.append(("SET", "k%d" % i, "v%d" % i))
            cmds.append(("GET", "k%d" % i))      # linearizable read right behind its write
            cmds.append(("PING",))               # instantly-ready reply between parked ones
        out = cl.pipeline(cmds)
        for i in range(500):
            assert out[3 * i] == "OK", out[3 * i]
            assert out[3 * i + 1] == "v%d" % i, (i, out[3 * i + 1])
            assert out[3 * i + 2] == "PONG"
        cl.close()


@test
def leader_failover_and_rejoin():
    with Cluster(3) as c:
        c.start_all()
        L = c.wait_leader()
        for i in range(50):
            c.call("SET", "k%d" % i, i)
        t0 = time.time()
        c.kill(L)
        L2 = c.wait_leader(exclude=(L,))
        failover = time.time() - t0
        assert L2 != L
        assert failover < 3.0, failover
        assert c.call("GET", "k49") == "49"            # nothing acknowledged was lost
        c.call("SET", "after", "failover")
        c.start(L)                                     # old leader recovers from its WAL and catches up
        d = c.wait_converged()
        assert len(d) == 3
        st = c.status(L)
        assert st["role"] == "follower", st


@test
def full_cluster_crash_preserves_acknowledged_writes():
    with Cluster(3) as c:
        c.start_all()
        c.wait_leader()
        for i in range(200):
            assert c.call("SET", "k%d" % i, "v%d" % i) == "OK"
        for i in (1, 2, 3):                            # simultaneous power failure
            c.kill(i)
        for i in (1, 2, 3):
            c.start(i)
        c.wait_leader()
        for i in range(0, 200, 7):
            assert c.call("GET", "k%d" % i) == "v%d" % i
        assert c.call("DBSIZE") == 200


@test
def no_progress_without_majority_then_recovery():
    with Cluster(3) as c:
        c.start_all()
        L = c.wait_leader()
        c.call("SET", "x", "1")
        others = [i for i in (1, 2, 3) if i != L]
        c.kill(others[0])
        c.kill(others[1])                              # leader alone: no quorum
        cl = c.client(L, timeout=3)
        try:
            r = cl.call("SET", "x", "2")
            raise AssertionError("write acknowledged without a quorum: %r" % r)
        except RespError as e:
            assert str(e).startswith(("TIMEOUT", "TRYAGAIN", "MOVED")), e
        except (OSError, ConnectionError):
            pass
        cl.close()
        c.start(others[0])
        c.start(others[1])
        c.wait_leader()
        assert c.call("GET", "x") in ("1", "2")       # '2' is allowed: it was never acknowledged
        c.call("SET", "y", "ok")
        assert c.call("GET", "y") == "ok"


@test
def partitioned_leader_cannot_commit_or_serve_reads():
    with Cluster(3) as c:
        c.start_all()
        L = c.wait_leader()
        c.call("SET", "k", "old")
        c.wait_converged()
        others = [i for i in (1, 2, 3) if i != L]
        c.partition([{L}, set(others)])
        # the isolated old leader must neither commit writes nor serve linearizable reads
        cl = c.client(L, timeout=3)
        got = None
        try:
            got = cl.call("SET", "k", "poison")
        except RespError as e:
            assert str(e).startswith(("TIMEOUT", "TRYAGAIN", "MOVED")), e
        except (OSError, ConnectionError):
            pass
        assert got is None, "isolated leader acknowledged a write: %r" % got
        cl.close()
        # the majority side elects a new leader and makes progress
        L2 = c.wait_leader(exclude=(L,), timeout=10)
        assert L2 in others
        cl2 = c.client(L2, timeout=3)
        assert cl2.call("SET", "k", "new") == "OK"
        assert cl2.call("GET", "k") == "new"
        cl2.close()
        # old leader steps down and must not serve a stale read
        time.sleep(0.5)
        cl = c.client(L, timeout=3)
        try:
            r = cl.call("GET", "k")
            raise AssertionError("old leader served a read while partitioned: %r" % r)
        except RespError as e:
            assert str(e).startswith(("MOVED", "TRYAGAIN", "TIMEOUT")), e
        cl.close()
        c.heal()
        d = c.wait_converged()
        assert c.call("GET", "k") == "new"             # 'poison' was never committed and is gone everywhere
        assert len(d) == 3


@test
def exactly_once_across_retries_and_failover():
    with Cluster(3) as c:
        c.start_all()
        c.wait_leader()
        assert c.call("INCR", "n", "IDEM", 42, 1) == 1
        assert c.call("INCR", "n", "IDEM", 42, 1) == 1          # retried: answered from the session table
        L = c.wait_leader()
        c.kill(L)
        c.wait_leader(exclude=(L,))
        assert c.call("INCR", "n", "IDEM", 42, 1) == 1          # ... even through a new leader
        assert c.call("INCR", "n", "IDEM", 42, 2) == 2
        assert c.call("GET", "n") == "2"
        c.start(L)
        c.wait_converged()


@test
def snapshot_and_install_snapshot_for_lagging_follower():
    with Cluster(3, extra=["--snapshot-every", "100"]) as c:
        c.start_all()
        L = c.wait_leader()
        lag = next(i for i in (1, 2, 3) if i != L)
        c.kill(lag)
        cl = c.client(L, timeout=10)
        for base in range(0, 1500, 300):                 # enough writes to compact the log several times
            out = cl.pipeline([("SET", "key%d" % i, "value-%d" % i) for i in range(base, base + 300)])
            assert all(o == "OK" for o in out), [o for o in out if o != "OK"][:3]
        cl.close()
        st = c.status(L)
        assert int(st["snapshot_index"]) > 0, st
        c.start(lag)                                     # its next index is below the leader's snapshot
        c.wait_converged(timeout=20)
        lst = c.status(lag)
        assert int(lst["snapshots_installed"]) >= 1, lst
        assert c.call("DBSIZE") == 1500
        # the follower survives its own restart from snapshot + WAL
        c.kill(lag)
        c.start(lag)
        c.wait_converged(timeout=20)
        cl = c.client(lag)
        assert cl.call("LOCALGET", "key1499") == "value-1499"
        cl.close()


@test
def restart_from_snapshot_and_wal_tail():
    with Cluster(1, extra=["--snapshot-every", "50"]) as c:
        c.start_all()
        c.wait_leader()
        for i in range(260):
            c.call("SET", "k%d" % i, i)
        st = c.status(1)
        assert int(st["snapshot_index"]) > 0
        c.kill(1)
        c.start(1)
        c.wait_leader()
        assert c.call("DBSIZE") == 260
        assert c.call("GET", "k259") == "259"


@test
def sizes_one_and_five():
    for n in (1, 5):
        with Cluster(n) as c:
            c.start_all()
            L = c.wait_leader()
            for i in range(30):
                assert c.call("SET", "k%d" % i, i) == "OK"
            if n == 5:
                c.kill(L)                       # 4/5 left
                L2 = c.wait_leader(exclude=(L,))
                other = next(i for i in range(1, 6) if i not in (L, L2))
                c.kill(other)                   # 3/5 left: still a majority
                c.wait_leader()
                assert c.call("GET", "k29") == "29"
                assert c.call("SET", "z", "1") == "OK"
            assert c.call("GET", "k0") == "0"


@test
def protocol_robustness():
    with Cluster(1) as c:
        c.start_all()
        c.wait_leader()
        cl = c.client(1)
        assert cl.call("PING") == "PONG"
        expect_error("ERR unknown command", lambda: cl.call("NOSUCH"))
        expect_error("ERR wrong number", lambda: cl.call("SET", "onlykey"))
        assert cl.call("SET", "bin", "a\r\nb\x00c") == "OK"
        assert cl.call("GET", "bin") == "a\r\nb\x00c"
        big = "x" * (1 << 20)
        assert cl.call("SET", "big", big) == "OK"
        assert cl.call("GET", "big") == big
        cl.sock.sendall(b"*1\r\n$zz\r\n")                 # malformed => error + close, server stays up
        try:
            cl.read_reply()
        except (RespError, ConnectionError):
            pass
        cl.close()
        cl = c.client(1)
        assert cl.call("PING") == "PONG"
        cl.sock.sendall(b"PING\r\nECHO hi\r\n")           # inline protocol (telnet/netcat)
        assert cl.read_reply() == "PONG" and cl.read_reply() == "hi"
        cl.close()


def main():
    only = sys.argv[1:]
    failed = 0
    for fn in TESTS:
        if only and fn.__name__ not in only:
            continue
        t0 = time.time()
        try:
            fn()
            print("PASS  %-62s %5.1fs" % (fn.__name__, time.time() - t0))
        except Exception:
            failed += 1
            print("FAIL  %-62s %5.1fs" % (fn.__name__, time.time() - t0))
            traceback.print_exc()
    print("\n%d/%d integration tests passed" % (len(TESTS) - failed if not only else len(only) - failed,
                                                len(TESTS) if not only else len(only)))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

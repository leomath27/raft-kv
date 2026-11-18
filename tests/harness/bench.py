#!/usr/bin/env python3
"""Benchmarks: leader-failover time (kill -9) and throughput (redis-benchmark).

  bench.py failover [--trials 20] [--nodes 3]
  bench.py throughput [--nodes 3] [--no-fsync]

Failover is measured two ways, both from the instant SIGKILL returns:
  election  - until a surviving node reports itself leader of a higher term (polled every ~1 ms)
  write     - until a client write is acknowledged by the new leader (committed by a majority)
"""
import argparse
import json
import os
import re
import shutil
import socket
import statistics
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(__file__))
from cluster import Client, Cluster, RespError  # noqa: E402


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(p * (len(xs) - 1))))]


def failover(args):
    res = {"election_ms": [], "write_ms": []}
    with Cluster(args.nodes, extra=["--election-min", str(args.election_min), "--election-max", str(args.election_max),
                                    "--heartbeat", str(args.heartbeat)]) as c:
        c.start_all()
        c.wait_leader()
        for i in range(100):
            c.call("SET", "warm%d" % i, i)
        for trial in range(args.trials):
            L = c.wait_leader()
            c.wait_converged()
            alive = [i for i in range(1, args.nodes + 1) if i != L]
            ok_time = []
            stop = threading.Event()

            def writer():
                target = alive[0]
                cl = None
                n = 0
                while not stop.is_set():
                    try:
                        if cl is None:
                            cl = Client("127.0.0.1", c.cports[target - 1], timeout=0.25)
                        n += 1
                        cl.call("SET", "probe", n)
                        ok_time.append(time.monotonic())
                    except RespError as e:
                        if str(e).startswith("MOVED"):
                            target = c.node_for_port(int(str(e).rsplit(":", 1)[1]))
                        cl and cl.close()
                        cl = None
                        time.sleep(0.002)
                    except (OSError, ConnectionError):
                        cl and cl.close()
                        cl = None
                        target = alive[(alive.index(target) + 1) % len(alive)] if target in alive else alive[0]
                        time.sleep(0.002)

            # connect the status pollers BEFORE the kill so polling is instant
            pollers = {i: c.client(i, timeout=0.5) for i in alive}
            th = threading.Thread(target=writer, daemon=True)
            th.start()
            time.sleep(0.15)
            old_term = int(c.status(L)["term"])
            c.kill(L)
            t_kill = time.monotonic()
            t_elect = None
            while t_elect is None and time.monotonic() - t_kill < 10:
                for i, p in pollers.items():
                    try:
                        st = dict(l.split(":", 1) for l in p.call("INFO").splitlines() if ":" in l and not l.startswith("#"))
                    except (RespError, OSError, ConnectionError):
                        continue
                    if st.get("role") == "leader" and int(st["term"]) > old_term:
                        t_elect = time.monotonic()
                        break
            time.sleep(0.4)
            stop.set()
            th.join(timeout=2)
            for p in pollers.values():
                p.close()
            after = [t for t in ok_time if t > t_kill]
            if t_elect is None or not after:
                print("trial %2d: FAILED to fail over" % trial)
                continue
            e_ms, w_ms = 1000 * (t_elect - t_kill), 1000 * (after[0] - t_kill)
            res["election_ms"].append(e_ms)
            res["write_ms"].append(w_ms)
            print("trial %2d: killed node %d -> new leader in %6.1f ms, first committed write after %6.1f ms" % (trial + 1, L, e_ms, w_ms))
            c.start(L)
            c.wait_converged(timeout=20)
    for k in ("election_ms", "write_ms"):
        xs = res[k]
        print("%-12s n=%d  min %.0f  median %.0f  mean %.0f  p95 %.0f  max %.0f  (ms)" % (
            k, len(xs), min(xs), statistics.median(xs), statistics.mean(xs), pct(xs, 0.95), max(xs)))
    print("config: election timeout %d-%d ms, heartbeat %d ms, %d nodes, 1 killed with SIGKILL per trial" % (
        args.election_min, args.election_max, args.heartbeat, args.nodes))
    out = os.path.join(os.path.dirname(__file__), "..", "..", "results", "failover.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    json.dump(res, open(out, "w"), indent=1)


RB = re.compile(r"^(\w+).*?([\d.]+) requests per second(?:, p50=([\d.]+) msec)?", re.M)


def throughput(args):
    if not shutil.which("redis-benchmark"):
        print("redis-benchmark not found")
        return 1
    rows = []
    for fsync in ([True] if not args.no_fsync_too else [True, False]):
        with Cluster(args.nodes, fsync=fsync) as c:
            c.start_all()
            L = c.wait_leader()
            port = c.cports[L - 1]
            for clients, pipeline in ((50, 1), (50, 16), (200, 16)):
                for t in ("set", "get"):
                    cmd = ["redis-benchmark", "-h", "127.0.0.1", "-p", str(port), "-t", t, "-n", str(args.requests),
                           "-c", str(clients), "-P", str(pipeline), "-r", "100000", "-d", "32", "-q"]
                    out = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
                    text = (out.stdout + out.stderr).replace("\r", "\n")
                    m = None
                    for m in RB.finditer(text):
                        pass
                    if not m:
                        print("could not parse:", text[-300:])
                        continue
                    rps = float(m.group(2))
                    row = dict(fsync=fsync, op=t.upper(), clients=clients, pipeline=pipeline, rps=rps, p50_ms=m.group(3))
                    rows.append(row)
                    print("fsync=%-3s %-3s clients=%-3d pipeline=%-2d  %9.0f req/s  p50=%s ms" % (
                        "on" if fsync else "off", row["op"], clients, pipeline, rps, m.group(3)))
    out = os.path.join(os.path.dirname(__file__), "..", "..", "results", "throughput.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    json.dump(rows, open(out, "w"), indent=1)


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    f = sub.add_parser("failover")
    f.add_argument("--trials", type=int, default=20)
    f.add_argument("--nodes", type=int, default=3)
    f.add_argument("--election-min", type=int, default=150)
    f.add_argument("--election-max", type=int, default=300)
    f.add_argument("--heartbeat", type=int, default=50)
    t = sub.add_parser("throughput")
    t.add_argument("--nodes", type=int, default=3)
    t.add_argument("--requests", type=int, default=100000)
    t.add_argument("--no-fsync-too", action="store_true", help="also run with --no-fsync for comparison")
    a = ap.parse_args()
    return {"failover": failover, "throughput": throughput}[a.cmd](a)


if __name__ == "__main__":
    sys.exit(main())

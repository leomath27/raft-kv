#!/usr/bin/env python3
"""Jepsen-style test for raft-kv.

  * N concurrent clients do read / write / compare-and-swap on a few keys.
  * A nemesis injects faults while they run: kill -9 the leader, kill a random
    node, kill a majority, SIGSTOP the leader (a long GC pause), isolate the
    leader in a minority partition, split the cluster, and crash every node at
    once (power failure).
  * Every operation is recorded with real-time invocation/response stamps.
    Timeouts are recorded as *indeterminate* (they may or may not have taken
    effect) - never as failures.
  * After healing, the history is checked for linearizability per key
    (Wing-Gong-Lowe search), and the replicas are checked for convergence.

A write acknowledged to a client and then lost, a stale read, a read that never
was written, or any other non-linearizable behaviour makes the run FAIL.
"""
import argparse
import json
import os
import random
import socket
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(__file__))
from checker import check_history  # noqa: E402
from cluster import Client, Cluster, RespError  # noqa: E402


def now_ns():
    return time.monotonic_ns()


class History:
    def __init__(self):
        self.lock = threading.Lock()
        self.events = []

    def add(self, **ev):
        ev["time"] = now_ns()
        with self.lock:
            self.events.append(ev)


class Worker(threading.Thread):
    def __init__(self, wid, cluster, hist, keys, stop, idem, op_timeout, seed, stale_reads=False):
        super().__init__(daemon=True)
        self.stale_reads = stale_reads  # negative control: read from ANY node without consensus
        self.wid, self.cluster, self.hist, self.keys, self.stop_ev = wid, cluster, hist, keys, stop
        self.idem, self.op_timeout = idem, op_timeout
        self.cid = 1000 + wid
        self.seq = 0
        self.counter = 0
        self.rng = random.Random(seed * 1000 + wid)
        self.conns = {}
        self.target = self.rng.randint(1, cluster.n)
        self.seen = {k: [] for k in keys}
        self.stats = {"ok": 0, "fail": 0, "info": 0}

    def conn(self, node):
        c = self.conns.get(node)
        if c is None:
            c = self.cluster.client(node, timeout=1.5)
            self.conns[node] = c
        return c

    def drop(self, node):
        c = self.conns.pop(node, None)
        if c:
            c.close()

    def rotate(self):
        self.target = self.target % self.cluster.n + 1

    def execute(self, f, key, value):
        """Returns ('ok', result) | ('info', None). Retries are only done when they cannot
        change the outcome: reads, requests that were definitely not applied (MOVED/TRYAGAIN),
        and writes carrying an idempotency token (exactly-once on the server)."""
        deadline = time.time() + self.op_timeout
        if f != "read":
            self.seq += 1
        idem = ("IDEM", self.cid, self.seq) if (self.idem and f != "read") else ()
        while time.time() < deadline and not self.stop_ev.is_set():
            node = self.rng.randint(1, self.cluster.n) if (self.stale_reads and f == "read") else self.target
            try:
                cl = self.conn(node)
            except OSError:
                self.rotate()
                time.sleep(0.03)
                continue
            try:
                if f == "read":
                    r = cl.call("LOCALGET" if self.stale_reads else "GET", key)
                    return "ok", (None if r is None else int(r))
                if f == "write":
                    cl.call("SET", key, value, *idem)
                    return "ok", None
                old, new = value
                r = cl.call("CAS", key, old, new, *idem)
                return "ok", bool(r)
            except RespError as e:
                m = str(e)
                if m.startswith("MOVED"):
                    self.target = self.cluster.node_for_port(int(m.rsplit(":", 1)[1]))
                    continue
                if m.startswith(("TRYAGAIN", "BUSY")):      # definitely not applied
                    self.rotate()
                    time.sleep(0.03)
                    continue
                if m.startswith("TIMEOUT"):                  # unknown outcome
                    if f == "read" or idem:
                        self.rotate()
                        time.sleep(0.03)
                        continue
                    return "info", None
                raise
            except (OSError, ConnectionError, socket.timeout):
                self.drop(node)
                if f == "read" or idem:
                    self.rotate()
                    time.sleep(0.03)
                    continue
                return "info", None
        return "info", None

    def next_value(self):
        self.counter += 1
        return self.wid * 10_000_000 + self.counter

    def run(self):
        while not self.stop_ev.is_set():
            key = self.rng.choice(self.keys)
            r = self.rng.random()
            if r < 0.40:
                f, arg = "read", None
            elif r < 0.75:
                f, arg = "write", self.next_value()
            else:
                pool = self.seen[key] or [0]
                f, arg = "cas", (self.rng.choice(pool[-6:]), self.next_value())
            self.hist.add(type="invoke", process=self.wid, f=f, key=key, value=arg)
            kind, res = self.execute(f, key, arg)
            if kind == "ok":
                self.hist.add(type="ok", process=self.wid, f=f, key=key, value=res)
                self.stats["ok"] += 1
                if f == "read" and res is not None:
                    self.seen[key].append(res)
                elif f == "write":
                    self.seen[key].append(arg)
                elif f == "cas" and res:
                    self.seen[key].append(arg[1])
            else:
                self.hist.add(type="info", process=self.wid, f=f, key=key, value=None)
                self.stats["info"] += 1
                time.sleep(0.05)
            time.sleep(self.rng.uniform(0, 0.015))
        for c in self.conns.values():
            c.close()


class Probe(threading.Thread):
    """Availability probe: one writer hammering a dedicated key; records success times."""

    def __init__(self, cluster, stop):
        super().__init__(daemon=True)
        self.cluster, self.stop_ev = cluster, stop
        self.ok_times = []
        self.target = 1

    def run(self):
        cl = None
        i = 0
        while not self.stop_ev.is_set():
            try:
                if cl is None:
                    cl = self.cluster.client(self.target, timeout=0.2)
                i += 1
                cl.call("SET", "__probe", i)
                self.ok_times.append(time.monotonic())
                time.sleep(0.005)
            except RespError as e:
                m = str(e)
                if m.startswith("MOVED"):
                    self.target = self.cluster.node_for_port(int(m.rsplit(":", 1)[1]))
                else:  # TRYAGAIN / TIMEOUT: this node cannot serve us (e.g. an isolated old leader) - try another
                    self.target = self.target % self.cluster.n + 1
                    time.sleep(0.01)
                cl.close()
                cl = None
            except (OSError, ConnectionError, socket.timeout):
                if cl:
                    cl.close()
                cl = None
                self.target = self.target % self.cluster.n + 1
                time.sleep(0.01)


class Nemesis(threading.Thread):
    def __init__(self, cluster, stop, kinds, seed, quiet=2.0, fault=2.0):
        super().__init__(daemon=True)
        self.c, self.stop_ev, self.kinds = cluster, stop, kinds
        self.rng = random.Random(seed)
        self.events = []
        self.quiet, self.fault = quiet, fault

    def note(self, kind, **kw):
        self.events.append(dict(t=time.monotonic(), kind=kind, **kw))
        print("  [nemesis +%6.2fs] %s %s" % (time.monotonic() - self.t0, kind, kw or ""), flush=True)

    def sleep(self, a, b):
        end = time.time() + self.rng.uniform(a, b)
        while time.time() < end and not self.stop_ev.is_set():
            time.sleep(0.05)

    def run(self):
        self.t0 = time.monotonic()
        n = self.c.n
        while not self.stop_ev.is_set():
            self.sleep(self.quiet * 0.6, self.quiet * 1.4)
            if self.stop_ev.is_set():
                break
            kind = self.rng.choice(self.kinds)
            L = self.c.leader()
            nodes = list(range(1, n + 1))
            undo = []
            if kind == "kill-leader" and L:
                self.note("kill-leader", node=L, leader_fault=True)
                self.c.kill(L)
                undo.append(lambda L=L: self.c.start(L))
            elif kind == "kill-random":
                v = self.rng.choice(nodes)
                self.note("kill-random", node=v, leader_fault=(v == L))
                self.c.kill(v)
                undo.append(lambda v=v: self.c.start(v))
            elif kind == "kill-majority":
                vs = self.rng.sample(nodes, n // 2 + 1)
                self.note("kill-majority", nodes=vs)
                for v in vs:
                    self.c.kill(v)
                undo.extend(lambda v=v: self.c.start(v) for v in vs)
            elif kind == "pause-leader" and L:
                self.note("pause-leader", node=L, leader_fault=True)
                self.c.pause(L)
                undo.append(lambda L=L: self.c.resume(L))
            elif kind == "partition-leader" and L:
                self.note("partition-leader", node=L, leader_fault=True)
                self.c.partition([{L}, set(nodes) - {L}])
                undo.append(self.c.heal)
            elif kind == "partition-halves":
                s = set(self.rng.sample(nodes, n // 2))
                self.note("partition-halves", minority=sorted(s))
                self.c.partition([s, set(nodes) - s])
                undo.append(self.c.heal)
            elif kind == "crash-all":
                self.note("crash-all")
                for v in nodes:
                    self.c.kill(v)
                undo.extend(lambda v=v: self.c.start(v) for v in nodes)
            else:
                continue
            self.sleep(self.fault * 0.5, self.fault * 1.5)
            for u in undo:
                try:
                    u()
                except Exception as e:  # noqa: BLE001
                    print("  [nemesis] recovery step failed: %r" % (e,))
            self.note("recover", of=kind)


def percentile(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * p))] if xs else float("nan")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=float, default=30)
    ap.add_argument("--clients", type=int, default=6)
    ap.add_argument("--keys", type=int, default=5)
    ap.add_argument("--nodes", type=int, default=3)
    ap.add_argument("--seed", type=int, default=int(time.time()) % 100000)
    ap.add_argument("--no-idem", action="store_true", help="do not use idempotency tokens: timeouts stay indeterminate")
    ap.add_argument("--nemesis", default="kill-leader,kill-random,kill-majority,pause-leader,partition-leader,partition-halves,crash-all")
    ap.add_argument("--fault", type=float, default=2.0, help="mean fault duration (s)")
    ap.add_argument("--quiet", type=float, default=2.0, help="mean healthy interval (s)")
    ap.add_argument("--extra", default="", help="extra server args, e.g. '--snapshot-every 200'")
    ap.add_argument("--stale-reads", action="store_true",
                    help="NEGATIVE CONTROL: serve reads from any replica without consensus; the checker must flag it")
    ap.add_argument("--out", default=None)
    ap.add_argument("--keep", action="store_true")
    a = ap.parse_args()
    print("jepsen: nodes=%d clients=%d keys=%d duration=%.0fs seed=%d idempotent-retries=%s"
          % (a.nodes, a.clients, a.keys, a.duration, a.seed, not a.no_idem))

    keys = ["k%d" % i for i in range(a.keys)]
    cluster = Cluster(a.nodes, extra=a.extra.split() if a.extra else None, keep=a.keep)
    try:
        cluster.start_all()
        cluster.wait_leader()
        stop = threading.Event()
        hist = History()
        workers = [Worker(i + 1, cluster, hist, keys, stop, not a.no_idem, 12.0, a.seed, a.stale_reads) for i in range(a.clients)]
        probe = Probe(cluster, stop)
        nem = Nemesis(cluster, stop, a.nemesis.split(","), a.seed, a.quiet, a.fault)
        t_start = time.monotonic()
        for w in workers:
            w.start()
        probe.start()
        nem.start()
        time.sleep(a.duration)
        stop.set()
        nem.join(timeout=30)
        for w in workers:
            w.join(timeout=30)
        probe.join(timeout=5)
        t_run = time.monotonic() - t_start

        print("healing: restarting every node, removing partitions ...")
        cluster.heal()
        for i in range(1, a.nodes + 1):
            cluster.resume(i)
            if not cluster.alive(i):
                cluster.start(i)
        cluster.heal()
        cluster.wait_leader(timeout=20)
        digests = cluster.wait_converged(timeout=30)

        # final linearizable read of every key, recorded in the history (catches lost acknowledged writes)
        for k in keys:
            hist.add(type="invoke", process=-1, f="read", key=k, value=None)
            v = cluster.call("GET", k)
            hist.add(type="ok", process=-1, f="read", key=k, value=None if v is None else int(v))

        # ---- verdict ----
        t0 = time.monotonic()
        valid, report = check_history(hist.events)
        check_s = time.monotonic() - t0
        n_ok = sum(w.stats["ok"] for w in workers)
        n_info = sum(w.stats["info"] for w in workers)
        crashed = [i for i in range(1, a.nodes + 1) if not cluster.alive(i)]

        # failover = time from a leader-affecting fault to the first successful write afterwards
        fo = []
        fo_by_kind = {}
        for ev in nem.events:
            if ev.get("leader_fault"):
                after = [t for t in probe.ok_times if t > ev["t"]]
                if after:
                    fo.append(after[0] - ev["t"])
                    fo_by_kind.setdefault(ev["kind"], []).append(round(1000 * (after[0] - ev["t"])))
        faults = {}
        for ev in nem.events:
            if ev["kind"] != "recover":
                faults[ev["kind"]] = faults.get(ev["kind"], 0) + 1

        print("\n================ RESULT ================")
        print("operations      : %d completed ok, %d indeterminate (timeouts), %.0f ops/s" % (n_ok, n_info, n_ok / t_run))
        print("faults injected : %s" % (", ".join("%s x%d" % kv for kv in sorted(faults.items())) or "none"))
        if fo:
            print("leader failover : median %.0f ms, p95 %.0f ms, max %.0f ms  (%d leader faults; time to first committed write)"
                  % (1000 * percentile(fo, 0.5), 1000 * percentile(fo, 0.95), 1000 * max(fo), len(fo)))
        for k, v in sorted(fo_by_kind.items()):
            print("   %-18s: %s ms" % (k, v))
        print("replicas        : converged, identical state digest on %d nodes: %s" % (len(digests), list(digests.values())[0]))
        print("linearizability : %s  (%d keys, checked in %.1fs)" % (
            "VALID" if valid else ("UNKNOWN" if report["unknown"] else "VIOLATION"), len(report["keys"]), check_s))
        if not valid:
            for k, r in report["keys"].items():
                if r["result"] is not True:
                    print("   key %s: %s %s" % (k, r["result"], r.get("counterexample", "")))
        if crashed:
            print("WARNING: nodes not running at the end: %s (see logs in %s)" % (crashed, cluster.dir))
        result = dict(seed=a.seed, nodes=a.nodes, duration=t_run, ok=n_ok, indeterminate=n_info, valid=valid,
                      faults=faults, failover_ms=[round(1000 * x) for x in fo], report=report,
                      digest=list(digests.values())[0])
        out = a.out or os.path.join(os.path.dirname(__file__), "..", "..", "results", "jepsen-seed%d.json" % a.seed)
        os.makedirs(os.path.dirname(out), exist_ok=True)
        with open(out, "w") as f:
            json.dump(result, f, indent=1)
        if not valid:
            hp = out.replace(".json", ".history.json")
            with open(hp, "w") as f:
                json.dump(hist.events, f)
            print("history saved to %s" % hp)
        return 0 if (valid and not crashed) else 1
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    sys.exit(main())

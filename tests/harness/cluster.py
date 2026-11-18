"""Cluster manager + minimal RESP client for the integration / Jepsen-style tests.

Real processes, real sockets, real kill -9. Network partitions are injected with
the server's CHAOS command (drops Raft traffic in-process), so no root or
iptables is needed.
"""
import os
import random
import shutil
import signal
import socket
import subprocess
import tempfile
import time

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
BINARY = os.path.join(ROOT, "build", "raft-kv")


class RespError(Exception):
    pass


class Client:
    """Tiny blocking RESP2 client."""

    def __init__(self, host, port, timeout=2.0):
        self.host, self.port, self.timeout = host, port, timeout
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.sock.settimeout(timeout)
        self.buf = b""

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass

    @staticmethod
    def encode(*args):
        out = [b"*%d\r\n" % len(args)]
        for a in args:
            if not isinstance(a, bytes):
                a = str(a).encode()
            out.append(b"$%d\r\n%s\r\n" % (len(a), a))
        return b"".join(out)

    def _fill(self):
        d = self.sock.recv(65536)
        if not d:
            raise ConnectionError("connection closed")
        self.buf += d

    def _line(self):
        while True:
            i = self.buf.find(b"\r\n")
            if i >= 0:
                line, self.buf = self.buf[:i], self.buf[i + 2:]
                return line
            self._fill()

    def _read_n(self, n):
        while len(self.buf) < n + 2:
            self._fill()
        data, self.buf = self.buf[:n], self.buf[n + 2:]
        return data

    def read_reply(self):
        line = self._line()
        t, rest = line[:1], line[1:]
        if t == b"+":
            return rest.decode()
        if t == b"-":
            raise RespError(rest.decode())
        if t == b":":
            return int(rest)
        if t == b"$":
            n = int(rest)
            return None if n < 0 else self._read_n(n).decode("latin-1")
        if t == b"*":
            n = int(rest)
            return None if n < 0 else [self.read_reply() for _ in range(n)]
        raise RespError("bad reply: %r" % line)

    def call(self, *args):
        self.sock.sendall(self.encode(*args))
        return self.read_reply()

    def pipeline(self, cmds):
        self.sock.sendall(b"".join(self.encode(*c) for c in cmds))
        out = []
        for _ in cmds:
            try:
                out.append(self.read_reply())
            except RespError as e:
                out.append(e)
        return out


def free_ports(n):
    socks, ports = [], []
    for _ in range(n):
        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        socks.append(s)
        ports.append(s.getsockname()[1])
    for s in socks:
        s.close()
    return ports


def parse_info(text):
    d = {}
    for line in text.splitlines():
        if ":" in line and not line.startswith("#"):
            k, v = line.split(":", 1)
            d[k] = v
    return d


class Cluster:
    def __init__(self, n=3, extra=None, fsync=True, keep=False, log="warn"):
        self.n = n
        self.extra = list(extra or [])
        self.fsync = fsync
        self.keep = keep
        self.log = log
        self.dir = tempfile.mkdtemp(prefix="raftkv-")
        ports = free_ports(2 * n)
        self.cports = ports[:n]
        self.rports = ports[n:]
        self.procs = {}
        self.paused = set()
        self.groups = None  # current partition (list of sets) so restarts re-apply it
        self.nodes_arg = ",".join("127.0.0.1:%d:%d" % (self.cports[i], self.rports[i]) for i in range(n))
        self.logs = {}

    # ---- lifecycle ----
    def start(self, i, extra=None):
        assert i not in self.procs or self.procs[i].poll() is not None
        args = [BINARY, "--id", str(i), "--nodes", self.nodes_arg, "--dir", os.path.join(self.dir, "n%d" % i),
                "--log", self.log] + self.extra + list(extra or [])
        if not self.fsync:
            args.append("--no-fsync")
        lf = open(os.path.join(self.dir, "n%d.log" % i), "ab")
        self.logs[i] = lf
        self.procs[i] = subprocess.Popen(args, stdout=lf, stderr=lf)
        self._wait_port(i)
        if self.groups:
            self._apply_partition_to(i)

    def _wait_port(self, i, timeout=5.0):
        end = time.time() + timeout
        while time.time() < end:
            if self.procs[i].poll() is not None:
                raise RuntimeError("node %d exited at startup; see %s/n%d.log" % (i, self.dir, i))
            try:
                socket.create_connection(("127.0.0.1", self.cports[i - 1]), timeout=0.2).close()
                return
            except OSError:
                time.sleep(0.02)
        raise RuntimeError("node %d did not open its port" % i)

    def start_all(self):
        for i in range(1, self.n + 1):
            self.start(i)

    def kill(self, i):
        """SIGKILL: the crash case. Nothing is flushed."""
        p = self.procs.get(i)
        if p and p.poll() is None:
            if i in self.paused:
                p.send_signal(signal.SIGCONT)
                self.paused.discard(i)
            p.kill()
            p.wait()

    def stop(self, i):
        p = self.procs.get(i)
        if p and p.poll() is None:
            p.send_signal(signal.SIGTERM)
            try:
                p.wait(timeout=3)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()

    def pause(self, i):
        p = self.procs.get(i)
        if p and p.poll() is None:
            p.send_signal(signal.SIGSTOP)
            self.paused.add(i)

    def resume(self, i):
        p = self.procs.get(i)
        if p and p.poll() is None:
            p.send_signal(signal.SIGCONT)
        self.paused.discard(i)

    def alive(self, i):
        p = self.procs.get(i)
        return p is not None and p.poll() is None and i not in self.paused

    def shutdown(self):
        for i in list(self.procs):
            self.kill(i)
        for lf in self.logs.values():
            lf.close()
        if not self.keep:
            shutil.rmtree(self.dir, ignore_errors=True)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.shutdown()

    # ---- access ----
    def client(self, i, timeout=2.0):
        return Client("127.0.0.1", self.cports[i - 1], timeout)

    def node_for_port(self, port):
        return self.cports.index(port) + 1

    def status(self, i):
        if not self.alive(i):
            return None
        try:
            c = self.client(i, timeout=0.5)
            try:
                return parse_info(c.call("INFO"))
            finally:
                c.close()
        except (OSError, RespError, ConnectionError):
            return None

    def leader(self, quorum_view=False):
        """Highest-term node that reports itself leader (None if none)."""
        best = None
        for i in range(1, self.n + 1):
            st = self.status(i)
            if st and st.get("role") == "leader":
                if best is None or int(st["term"]) > best[1]:
                    best = (i, int(st["term"]))
        return best[0] if best else None

    def wait_leader(self, timeout=10.0, exclude=()):
        end = time.time() + timeout
        while time.time() < end:
            l = self.leader()
            if l and l not in exclude:
                return l
            time.sleep(0.02)
        raise RuntimeError("no leader within %.1fs" % timeout)

    def call(self, *args, timeout=3.0, retries=60):
        """Run a command against the cluster, following MOVED / retrying transient errors."""
        target = self.leader() or 1
        last = None
        for _ in range(retries):
            try:
                c = self.client(target, timeout)
                try:
                    return c.call(*args)
                finally:
                    c.close()
            except RespError as e:
                last = e
                m = str(e)
                if m.startswith("MOVED"):
                    target = self.node_for_port(int(m.rsplit(":", 1)[1]))
                    continue
                if m.startswith(("TRYAGAIN", "TIMEOUT", "BUSY")):
                    time.sleep(0.05)
                    target = self.leader() or target
                    continue
                raise
            except (OSError, ConnectionError) as e:
                last = e
                time.sleep(0.05)
                target = (target % self.n) + 1
        raise RuntimeError("command %r failed: %r" % (args, last))

    # ---- nemesis primitives ----
    def _apply_partition_to(self, i):
        mine = next(g for g in self.groups if i in g)
        blocked = [j for j in range(1, self.n + 1) if j not in mine]
        try:
            c = self.client(i, timeout=1.0)
            c.call("CHAOS", "ISOLATE", *blocked)
            c.close()
        except (OSError, RespError, ConnectionError):
            pass

    def partition(self, groups):
        """groups: list of sets of node ids; nodes in different groups cannot talk."""
        self.groups = [set(g) for g in groups]
        for i in range(1, self.n + 1):
            if self.alive(i):
                self._apply_partition_to(i)

    def heal(self):
        self.groups = None
        for i in range(1, self.n + 1):
            if self.alive(i):
                try:
                    c = self.client(i, timeout=1.0)
                    c.call("CHAOS", "HEAL")
                    c.close()
                except (OSError, RespError, ConnectionError):
                    pass

    def digests(self):
        out = {}
        for i in range(1, self.n + 1):
            if not self.alive(i):
                continue
            try:
                c = self.client(i, timeout=1.0)
                out[i] = c.call("DEBUG", "DIGEST")
                c.close()
            except (OSError, RespError, ConnectionError):
                pass
        return out

    def wait_converged(self, timeout=15.0):
        """All live nodes applied the same index and hold identical state."""
        end = time.time() + timeout
        last = None
        while time.time() < end:
            d = self.digests()
            last = d
            if len(d) == sum(1 for i in range(1, self.n + 1) if self.alive(i)) and len(set(d.values())) == 1:
                return d
            time.sleep(0.1)
        raise RuntimeError("replicas did not converge: %r" % last)

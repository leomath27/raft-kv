"""Linearizability checker for a single-register model (read / write / cas).

Wing & Gong / Lowe search with memoisation ("WGL"), the same family of
algorithm used by Knossos and Porcupine. Keys are independent registers, so a
history is linearizable iff every per-key sub-history is (locality).

Operation outcomes:
  ok    - completed; the observed result is authoritative
  fail  - definitely did not take effect (dropped from the history)
  info  - indeterminate (timeout / lost connection): a write or CAS may have
          taken effect at ANY point after its invocation, or never; an
          indeterminate read says nothing and is dropped.
"""

INIT = None  # a key that was never written reads as nil


class Op:
    __slots__ = ("id", "f", "arg", "res", "call", "ret", "proc")

    def __init__(self, id, f, arg, res, call, ret, proc):
        self.id, self.f, self.arg, self.res, self.call, self.ret, self.proc = id, f, arg, res, call, ret, proc

    def __repr__(self):
        r = "?" if self.ret is None else "ok"
        return "<p%s %s %r -> %r %s [%d..%s]>" % (self.proc, self.f, self.arg, self.res, r, self.call,
                                                 self.ret if self.ret is not None else "inf")


class _E:
    __slots__ = ("t", "is_call", "op", "prev", "next", "match")

    def __init__(self, t, is_call, op):
        self.t, self.is_call, self.op = t, is_call, op
        self.prev = self.next = self.match = None


def _step(state, op):
    """Return (ok, new_state) for linearizing `op` when the register holds `state`."""
    f = op.f
    if f == "read":
        return (state == op.res, state)
    if f == "write":
        return (True, op.arg)
    if f == "cas":
        old, new = op.arg
        if op.ret is None:                      # indeterminate: only the 'applied' branch matters
            return (state == old, new) if state == old else (False, state)
        if op.res:                              # succeeded
            return (True, new) if state == old else (False, state)
        return (state != old, state)            # failed
    raise ValueError(f)


def check_register(ops, max_steps=20_000_000):
    """ops: list of Op for ONE key. Returns (True|False|None, info). None = gave up."""
    ops = [o for o in ops if not (o.ret is None and o.f == "read")]
    if not ops:
        return True, None
    ents = []
    for o in ops:
        c = _E(o.call, True, o)
        ents.append(c)
        if o.ret is not None:
            r = _E(o.ret, False, o)
            c.match = r
            ents.append(r)
    ents.sort(key=lambda e: (e.t, 0 if e.is_call else 1))
    head = _E(-1, False, None)
    prev = head
    for e in ents:
        prev.next = e
        e.prev = prev
        prev = e

    idx = {o.id: i for i, o in enumerate(ops)}
    state = INIT
    linearized = 0
    cache = set()
    stack = []
    best_depth, best_info = -1, None
    steps = 0
    e = head.next
    while head.next is not None:
        steps += 1
        if steps > max_steps:
            return None, "search budget exhausted (history too concurrent)"
        if e is None:      # only indeterminate calls remain: they may simply never have happened
            return True, None
        if e.is_call:
            ok, ns = _step(state, e.op)
            if ok:
                bit = 1 << idx[e.op.id]
                key = (linearized | bit, ns)
                if key not in cache:
                    cache.add(key)
                    stack.append((e, state, linearized))
                    linearized |= bit
                    state = ns
                    # lift the call (and its return) out of the list
                    e.prev.next = e.next
                    if e.next:
                        e.next.prev = e.prev
                    if e.match:
                        e.match.prev.next = e.match.next
                        if e.match.next:
                            e.match.next.prev = e.match.prev
                    e = head.next
                    continue
            e = e.next
        else:
            # a return whose operation is not yet linearized: backtrack
            if len(stack) > best_depth:
                best_depth = len(stack)
                best_info = (e.op, state)
            if not stack:
                return False, best_info
            le, state, linearized = stack.pop()
            # unlift
            if le.match:
                le.match.prev.next = le.match
                if le.match.next:
                    le.match.next.prev = le.match
            le.prev.next = le
            if le.next:
                le.next.prev = le
            e = le.next
    return True, None


def check_history(history):
    """history: list of events {type: invoke|ok|fail|info, process, f, key, value, time}
    value for invoke = argument; for ok = result.  Returns (valid, report_dict)."""
    pending = {}
    per_key = {}
    nid = 0
    for ev in history:
        p = ev["process"]
        if ev["type"] == "invoke":
            pending[p] = ev
        else:
            inv = pending.pop(p, None)
            if inv is None:
                continue
            if ev["type"] == "fail":
                continue
            f, key = inv["f"], inv["key"]
            if f == "read":
                arg, res = None, ev["value"]
            elif f == "write":
                arg, res = inv["value"], None
            else:
                arg, res = inv["value"], ev["value"]
            ret = ev["time"] if ev["type"] == "ok" else None
            nid += 1
            per_key.setdefault(key, []).append(Op(nid, f, arg, res, inv["time"], ret, p))
    for p, inv in pending.items():  # never completed at all: indeterminate
        if inv["f"] == "read":
            continue
        nid += 1
        per_key.setdefault(inv["key"], []).append(Op(nid, inv["f"], inv["value"], None, inv["time"], None, p))
    report = {"keys": {}, "valid": True, "unknown": False}
    for key, ops in sorted(per_key.items()):
        ok, info = check_register(ops)
        report["keys"][key] = {"ops": len(ops), "result": ok}
        if ok is False:
            report["valid"] = False
            report["keys"][key]["counterexample"] = repr(info)
        elif ok is None:
            report["unknown"] = True
    return report["valid"] and not report["unknown"], report


if __name__ == "__main__":  # self-test
    def H(*evs):
        return [dict(type=t, process=p, f=f, key="k", value=v, time=tm) for (t, p, f, v, tm) in evs]

    # sequential, valid
    good = H(("invoke", 1, "write", 1, 1), ("ok", 1, "write", None, 2), ("invoke", 1, "read", None, 3), ("ok", 1, "read", 1, 4))
    assert check_history(good)[0]
    # stale read after an acknowledged write => not linearizable
    bad = H(("invoke", 1, "write", 1, 1), ("ok", 1, "write", None, 2), ("invoke", 2, "read", None, 3), ("ok", 2, "read", None, 4))
    assert not check_history(bad)[0]
    # concurrent write/read: both outcomes allowed
    conc = H(("invoke", 1, "write", 5, 1), ("invoke", 2, "read", None, 2), ("ok", 2, "read", None, 3), ("ok", 1, "write", None, 4))
    assert check_history(conc)[0]
    # an indeterminate write may explain a later read
    info = H(("invoke", 1, "write", 7, 1), ("info", 1, "write", None, 2), ("invoke", 2, "read", None, 10), ("ok", 2, "read", 7, 11))
    assert check_history(info)[0]
    # ... or never have happened
    info2 = H(("invoke", 1, "write", 7, 1), ("info", 1, "write", None, 2), ("invoke", 2, "read", None, 10), ("ok", 2, "read", None, 11))
    assert check_history(info2)[0]
    # a read of a value nobody wrote is invalid
    ghost = H(("invoke", 2, "read", None, 10), ("ok", 2, "read", 9, 11))
    assert not check_history(ghost)[0]
    # CAS semantics
    cas = H(("invoke", 1, "write", 1, 1), ("ok", 1, "write", None, 2),
            ("invoke", 1, "cas", (1, 2), 3), ("ok", 1, "cas", True, 4),
            ("invoke", 1, "cas", (1, 3), 5), ("ok", 1, "cas", False, 6),
            ("invoke", 1, "read", None, 7), ("ok", 1, "read", 2, 8))
    assert check_history(cas)[0]
    badcas = H(("invoke", 1, "write", 1, 1), ("ok", 1, "write", None, 2),
               ("invoke", 1, "cas", (9, 2), 3), ("ok", 1, "cas", True, 4))
    assert not check_history(badcas)[0]
    # lost acknowledged write: write(1) ok, write(2) ok, then read returns 1
    lost = H(("invoke", 1, "write", 1, 1), ("ok", 1, "write", None, 2), ("invoke", 1, "write", 2, 3),
             ("ok", 1, "write", None, 4), ("invoke", 2, "read", None, 5), ("ok", 2, "read", 1, 6))
    assert not check_history(lost)[0]
    print("checker self-test OK")

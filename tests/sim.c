/* sim.c - deterministic fault-injection simulation of the Raft core.
 *
 * Runs 1/3/5-node clusters in virtual time (1 tick = 1 ms) over a lossy,
 * reordering, duplicating, partitionable network with crash/restart that loses
 * all un-synced state. After every tick it checks the Raft safety properties:
 *   - Election Safety:       at most one leader per term
 *   - State Machine Safety:  every node applies the same entry at each index
 *   - Leader Completeness:   a new leader holds every committed entry
 *   - Log Matching:          same index+term => identical log prefix
 *   - Durability discipline: nothing is sent before it is synced; a crash
 *                            discards everything not yet synced
 * and, after the faults stop, liveness: a leader emerges, new proposals commit
 * and all replicas converge to the same state.
 *
 * usage: sim [num_seeds] [first_seed] [duration_ms] */
#include <inttypes.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../src/raft.h"
#include "../src/util.h"

#define MAXN 5
#define MAXIDX 400000

static uint64_t g_seed, T;
static uint64_t g_rng;
static uint64_t R(uint64_t n) { return n ? rng_next(&g_rng) % n : 0; }
static int chance(int pct) { return (int)R(100) < pct; }

static void fail(const char *fmt, ...) {
    va_list ap;
    fprintf(stderr, "\nSIM FAILURE (seed=%" PRIu64 ", t=%" PRIu64 "ms): ", g_seed, T);
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
    fprintf(stderr, "\n");
    exit(1);
}

static uint64_t mix(uint64_t x) {
    x ^= x >> 33; x *= 0xff51afd7ed558ccdull; x ^= x >> 33; x *= 0xc4ceb9fe1a85ec53ull; x ^= x >> 33;
    return x;
}
static uint64_t hbytes(const uint8_t *p, size_t n) {
    uint64_t h = 1469598103934665603ull;
    for (size_t i = 0; i < n; i++) { h ^= p[i]; h *= 1099511628211ull; }
    return h;
}

/* ------------------------------------------------------------------ */
typedef struct { raft_msg_t m; raft_entry_t *ents; uint8_t *snap; uint64_t at; } simmsg_t;
typedef struct { int kind; uint64_t a; int32_t v; raft_entry_t e; } op_t; /* 0 state 1 append 2 trunc */

typedef struct {
    int id, up;
    raft_t *r;
    uint64_t d_term; int32_t d_vote;
    raft_entry_t *d_ents; size_t d_n, d_cap;
    uint64_t d_snap_idx, d_snap_term, d_snap_hash;
    op_t *ops; size_t n_ops, cap_ops;
    simmsg_t *out; size_t n_out, cap_out;
    uint64_t sm_hash, sm_applied;
    uint64_t down_until;
    uint64_t last_term_seen, last_last_index; raft_role_t last_role;
    uint64_t crash_at, crash_down;   /* targeted crash: at a time */
    uint64_t crash_after_sync;       /* targeted crash: right after the next sync releases messages */
} snode_t;

static int N;
static snode_t nodes[MAXN + 1];
static raft_cfg_t cfg;
static simmsg_t *net; static size_t n_net, cap_net;
static int drop_pct, dup_pct;
static int grp[MAXN + 1];
static int blocked[MAXN + 1][MAXN + 1];
static int snap_thresh;
static int g_healed, prop_pct;

/* global truth */
static uint64_t *g_chain, *g_term, *g_dh, *g_cterm; /* g_cterm: term in which idx was first committed */
static uint8_t *g_set;
static uint64_t g_max_applied;
static int *leader_of_term; /* term -> leader id */
#define MAXTERM 100000

/* stats */
static uint64_t st_leaders, st_crashes, st_snaps, st_installs, st_proposed, st_msgs, st_dropped, st_trunc;

static raft_entry_t dup_entry(const raft_entry_t *e) {
    raft_entry_t c = *e;
    c.data = malloc(e->len ? e->len : 1);
    if (e->len) memcpy(c.data, e->data, e->len);
    return c;
}

static simmsg_t deep_copy(const raft_msg_t *m) {
    simmsg_t s;
    memset(&s, 0, sizeof s);
    s.m = *m;
    if (m->n_ents) {
        s.ents = malloc(m->n_ents * sizeof *s.ents);
        for (uint32_t i = 0; i < m->n_ents; i++) s.ents[i] = dup_entry(&m->ents[i]);
        s.m.ents = s.ents;
    }
    if (m->snap_len) {
        s.snap = malloc(m->snap_len);
        memcpy(s.snap, m->snap, m->snap_len);
        s.m.snap = s.snap;
    }
    return s;
}
static void free_simmsg(simmsg_t *s) {
    for (uint32_t i = 0; i < s->m.n_ents; i++) free(s->ents[i].data);
    free(s->ents);
    free(s->snap);
}

/* -------------------- raft ops -------------------- */
static void op_send(void *ctx, const raft_msg_t *m) {
    snode_t *n = ctx;
    /* targeted fault: crash a voter right after its vote leaves the node, so a vote that
     * was not made durable would be forgotten and could be cast a second time */
    if (!g_healed && m->type == MSG_VOTE_RESP && m->granted && !m->prevote && chance(10)) n->crash_after_sync = 1 + R(4);
    if (n->n_out == n->cap_out) { n->cap_out = n->cap_out ? n->cap_out * 2 : 16; n->out = realloc(n->out, n->cap_out * sizeof *n->out); }
    n->out[n->n_out++] = deep_copy(m);
}
static op_t *push_op(snode_t *n) {
    if (n->n_ops == n->cap_ops) { n->cap_ops = n->cap_ops ? n->cap_ops * 2 : 32; n->ops = realloc(n->ops, n->cap_ops * sizeof *n->ops); }
    op_t *o = &n->ops[n->n_ops++];
    memset(o, 0, sizeof *o);
    return o;
}
static void op_save_state(void *ctx, uint64_t term, int32_t vote) {
    snode_t *n = ctx;
    op_t *o = push_op(n); o->kind = 0; o->a = term; o->v = vote;
}
static void op_append(void *ctx, uint64_t idx, const raft_entry_t *e) {
    op_t *o = push_op(ctx); o->kind = 1; o->a = idx; o->e = dup_entry(e);
}
static void op_truncate(void *ctx, uint64_t from) {
    op_t *o = push_op(ctx); o->kind = 2; o->a = from; st_trunc++;
}

static void check_leader_complete(snode_t *n);

static void op_apply(void *ctx, uint64_t idx, const raft_entry_t *e) {
    snode_t *n = ctx;
    if (idx != n->sm_applied + 1) fail("node %d applied %" PRIu64 " after %" PRIu64, n->id, idx, n->sm_applied);
    if (idx >= MAXIDX) fail("log too long for sim");
    uint64_t dh = hbytes(e->data, e->len) ^ e->type;
    uint64_t h = mix(n->sm_hash ^ mix(idx) ^ mix(e->term * 31 + 7) ^ dh);
    n->sm_hash = h;
    n->sm_applied = idx;
    if (g_set[idx]) {
        if (g_chain[idx] != h || g_term[idx] != e->term || g_dh[idx] != dh)
            fail("STATE MACHINE SAFETY: node %d applied a different entry at index %" PRIu64 " (term %" PRIu64 " vs %" PRIu64 ")",
                 n->id, idx, e->term, g_term[idx]);
    } else {
        g_set[idx] = 1; g_chain[idx] = h; g_term[idx] = e->term; g_dh[idx] = dh;
        g_cterm[idx] = raft_term(n->r); /* node's term == committing leader's term at first apply */
    }
    if (idx > g_max_applied) g_max_applied = idx;
}
static void op_become(void *ctx, raft_role_t from, raft_role_t to) {
    snode_t *n = ctx;
    (void)from;
    if (to == RAFT_LEADER) {
        uint64_t t = raft_term(n->r);
        if (t >= MAXTERM) fail("term too large");
        if (leader_of_term[t] && leader_of_term[t] != n->id)
            fail("ELECTION SAFETY: term %" PRIu64 " has leaders %d and %d", t, leader_of_term[t], n->id);
        leader_of_term[t] = n->id;
        st_leaders++;
        check_leader_complete(n);
        /* targeted fault: kill a fresh leader before it has replicated much (Raft paper fig. 8) */
        if (!g_healed && chance(25)) { n->crash_at = T + 1 + R(20); n->crash_down = 20 + R(400); }
    }
}
static int op_load_snapshot(void *ctx, uint8_t **data, size_t *len) {
    snode_t *n = ctx;
    if (raft_snap_index(n->r) != n->d_snap_idx) fail("snapshot index mismatch on node %d", n->id);
    *data = malloc(8);
    for (int i = 0; i < 8; i++) (*data)[i] = (uint8_t)(n->d_snap_hash >> (8 * i));
    *len = 8;
    return 0;
}
static void rebuild_durable_from_raft(snode_t *n) {
    for (size_t i = 0; i < n->d_n; i++) free(n->d_ents[i].data);
    n->d_n = 0;
    uint64_t last = raft_last_index(n->r);
    for (uint64_t i = raft_snap_index(n->r) + 1; i <= last; i++) {
        if (n->d_n == n->d_cap) { n->d_cap = n->d_cap ? n->d_cap * 2 : 64; n->d_ents = realloc(n->d_ents, n->d_cap * sizeof *n->d_ents); }
        n->d_ents[n->d_n++] = dup_entry(raft_entry_at(n->r, i));
    }
    n->d_term = raft_term(n->r);
    n->d_vote = raft_voted_for(n->r);
    for (size_t i = 0; i < n->n_ops; i++) free(n->ops[i].e.data);
    n->n_ops = 0;
}
static void op_install_snapshot(void *ctx, uint64_t idx, uint64_t term, const uint8_t *data, size_t len) {
    snode_t *n = ctx;
    if (len != 8) fail("bad snapshot blob");
    uint64_t h = 0;
    for (int i = 0; i < 8; i++) h |= (uint64_t)data[i] << (8 * i);
    if (g_set[idx] && g_chain[idx] != h) fail("STATE MACHINE SAFETY: snapshot at %" PRIu64 " differs from applied history", idx);
    n->sm_hash = h;
    n->sm_applied = idx;
    n->d_snap_idx = idx; n->d_snap_term = term; n->d_snap_hash = h;
    rebuild_durable_from_raft(n);
    st_installs++;
}

static raft_ops_t make_ops(snode_t *n) {
    raft_ops_t o = {n, op_send, op_save_state, op_append, op_truncate, op_apply, op_become, op_load_snapshot, op_install_snapshot};
    return o;
}

/* -------------------- node lifecycle -------------------- */
static void node_start(snode_t *n) {
    raft_ops_t ops = make_ops(n);
    n->r = raft_new(n->id, N, &cfg, &ops, T, g_seed * 131 + (uint64_t)n->id + T);
    raft_entry_t *es = malloc((n->d_n ? n->d_n : 1) * sizeof *es);
    for (size_t i = 0; i < n->d_n; i++) es[i] = dup_entry(&n->d_ents[i]);
    raft_restore(n->r, n->d_term, n->d_vote, n->d_snap_idx, n->d_snap_term, es, n->d_n);
    free(es);
    n->sm_hash = n->d_snap_hash;
    n->sm_applied = n->d_snap_idx;
    n->up = 1;
    n->last_term_seen = raft_term(n->r);
    n->last_role = RAFT_FOLLOWER;
    n->last_last_index = raft_last_index(n->r);
}
static void node_crash(snode_t *n, uint64_t down_ms) {
    if (!n->up) return;
    raft_free(n->r);
    n->r = NULL;
    n->up = 0;
    for (size_t i = 0; i < n->n_ops; i++) free(n->ops[i].e.data);
    n->n_ops = 0;
    for (size_t i = 0; i < n->n_out; i++) free_simmsg(&n->out[i]);
    n->n_out = 0;
    n->down_until = T + down_ms;
    n->crash_at = n->crash_after_sync = 0;
    st_crashes++;
}

static void net_send(simmsg_t s) {
    st_msgs++;
    if (chance(drop_pct) || grp[s.m.from] != grp[s.m.to] || blocked[s.m.from][s.m.to]) { st_dropped++; free_simmsg(&s); return; }
    int copies = chance(dup_pct) ? 2 : 1;
    for (int c = 0; c < copies; c++) {
        simmsg_t x = c == 0 ? s : deep_copy(&s.m);
        uint64_t d = 1 + R(4);
        if (chance(3)) d += R(300); /* occasional straggler => reordering */
        x.at = T + d;
        if (n_net == cap_net) { cap_net = cap_net ? cap_net * 2 : 256; net = realloc(net, cap_net * sizeof *net); }
        net[n_net++] = x;
    }
}

static void node_sync(snode_t *n) {
    if (!n->up) return;
    for (size_t i = 0; i < n->n_ops; i++) {
        op_t *o = &n->ops[i];
        if (o->kind == 0) { n->d_term = o->a; n->d_vote = o->v; }
        else if (o->kind == 1) {
            if (o->a != n->d_snap_idx + n->d_n + 1) fail("node %d WAL append gap: idx %" PRIu64 " after %" PRIu64, n->id, o->a, n->d_snap_idx + n->d_n);
            if (n->d_n == n->d_cap) { n->d_cap = n->d_cap ? n->d_cap * 2 : 64; n->d_ents = realloc(n->d_ents, n->d_cap * sizeof *n->d_ents); }
            n->d_ents[n->d_n++] = o->e; /* ownership moves */
            o->e.data = NULL;
        } else {
            size_t keep = o->a <= n->d_snap_idx + 1 ? 0 : (size_t)(o->a - n->d_snap_idx - 1);
            for (size_t k = keep; k < n->d_n; k++) free(n->d_ents[k].data);
            if (keep < n->d_n) n->d_n = keep;
        }
        free(o->e.data);
    }
    n->n_ops = 0;
    raft_persisted(n->r);
    for (size_t i = 0; i < n->n_out; i++) net_send(n->out[i]);
    n->n_out = 0;
    if (n->crash_after_sync) { uint64_t d = n->crash_after_sync; n->crash_after_sync = 0; node_crash(n, d); }
}

static void maybe_snapshot(snode_t *n) {
    if (!n->up || n->n_ops || n->n_out) return;
    uint64_t idx = raft_applied(n->r);
    if (idx <= raft_snap_index(n->r) || idx - raft_snap_index(n->r) < (uint64_t)snap_thresh) return;
    if (n->sm_applied != idx) fail("sm/applied mismatch");
    uint64_t t;
    raft_term_at(n->r, idx, &t);
    raft_compact(n->r, idx);
    n->d_snap_idx = idx; n->d_snap_term = t; n->d_snap_hash = n->sm_hash;
    rebuild_durable_from_raft(n);
    st_snaps++;
}

/* -------------------- invariants -------------------- */
static void check_leader_complete(snode_t *n) {
    uint64_t snap = raft_snap_index(n->r), lt = raft_term(n->r);
    for (uint64_t i = 1; i <= g_max_applied; i++) {
        /* Leader Completeness: only entries committed in a term < the leader's term are guaranteed */
        if (!g_set[i] || i <= snap || g_cterm[i] >= lt) continue;
        uint64_t t;
        if (!raft_term_at(n->r, i, &t) || t != g_term[i])
            fail("LEADER COMPLETENESS: leader %d (term %" PRIu64 ") lacks committed entry %" PRIu64 " (term %" PRIu64 ")",
                 n->id, raft_term(n->r), i, g_term[i]);
        const raft_entry_t *e = raft_entry_at(n->r, i);
        if ((hbytes(e->data, e->len) ^ e->type) != g_dh[i]) fail("leader %d has different data at committed index %" PRIu64, n->id, i);
    }
}

static void check_log_matching(void) {
    for (int a = 1; a <= N; a++) for (int b = a + 1; b <= N; b++) {
        if (!nodes[a].up || !nodes[b].up) continue;
        raft_t *x = nodes[a].r, *y = nodes[b].r;
        uint64_t lo = raft_snap_index(x) > raft_snap_index(y) ? raft_snap_index(x) : raft_snap_index(y);
        uint64_t hi = raft_last_index(x) < raft_last_index(y) ? raft_last_index(x) : raft_last_index(y);
        uint64_t first_mismatch = 0, last_equal = 0;
        for (uint64_t i = lo + 1; i <= hi; i++) {
            const raft_entry_t *ex = raft_entry_at(x, i), *ey = raft_entry_at(y, i);
            if (ex->term == ey->term) {
                if (ex->len != ey->len || ex->type != ey->type || (ex->len && memcmp(ex->data, ey->data, ex->len)))
                    fail("LOG MATCHING: nodes %d,%d same term %" PRIu64 " at index %" PRIu64 " but different data", a, b, ex->term, i);
                last_equal = i;
            } else if (!first_mismatch) first_mismatch = i;
        }
        if (first_mismatch && last_equal > first_mismatch)
            fail("LOG MATCHING: nodes %d,%d agree at %" PRIu64 " but differ at earlier index %" PRIu64, a, b, last_equal, first_mismatch);
    }
}

static void check_basic(void) {
    for (int i = 1; i <= N; i++) {
        snode_t *n = &nodes[i];
        if (!n->up) continue;
        uint64_t t = raft_term(n->r);
        if (t < n->last_term_seen) fail("node %d term went backwards", n->id);
        n->last_term_seen = t;
        if (raft_role(n->r) == RAFT_LEADER) {
            if (t < MAXTERM && leader_of_term[t] != n->id) fail("ELECTION SAFETY: node %d leads term %" PRIu64 " but term owner is %d", n->id, t, leader_of_term[t]);
            if (n->last_role == RAFT_LEADER && raft_last_index(n->r) < n->last_last_index) fail("LEADER APPEND-ONLY violated on node %d", n->id);
            n->last_last_index = raft_last_index(n->r);
        }
        n->last_role = raft_role(n->r);
        if (raft_commit(n->r) > raft_last_index(n->r)) fail("commit beyond log on node %d", n->id);
    }
}

/* -------------------- the run -------------------- */
static int leader_node(void) {
    for (int i = 1; i <= N; i++) if (nodes[i].up && raft_role(nodes[i].r) == RAFT_LEADER) return i;
    return 0;
}

static void clean_net(void) {
    drop_pct = 0; dup_pct = 0;
    for (int i = 1; i <= N; i++) { grp[i] = 0; for (int j = 1; j <= N; j++) blocked[i][j] = 0; }
}

static void inject_fault(void) {
    switch (R(9)) {
    case 0: clean_net(); break;
    case 1: drop_pct = 5 + (int)R(35); dup_pct = (int)R(10); break;
    case 2: if (N > 1) { for (int i = 1; i <= N; i++) grp[i] = (int)R(2); } break;
    case 3: if (N > 1) { int v = 1 + (int)R(N); for (int i = 1; i <= N; i++) grp[i] = (i == v); } break;
    case 4: if (N > 1) { int a = 1 + (int)R(N), b = 1 + (int)R(N); if (a != b) blocked[a][b] = 1; } break;
    case 5: node_crash(&nodes[1 + R(N)], 50 + R(1500)); break;
    case 6: { int l = leader_node(); if (l) node_crash(&nodes[l], 50 + R(1500)); break; }
    case 7: if (chance(30)) for (int i = 1; i <= N; i++) node_crash(&nodes[i], 20 + R(300)); break;
    case 8: if (N >= 3) { /* take a follower away long enough that the others compact past it */
                int v = 1 + (int)R(N); node_crash(&nodes[v], 1500 + R(3000)); } break;
    }
}

static void run_seed(uint64_t seed, int n, uint64_t duration) {
    g_seed = seed;
    g_rng = seed * 0x9E3779B97F4A7C15ull + 12345;
    T = 0;
    N = n;
    raft_cfg_default(&cfg);
    cfg.prevote = chance(70);
    if (chance(35)) cfg.election_max_ms = cfg.election_min_ms + 15; /* tight window => many split votes */
    cfg.check_quorum = chance(85);
    prop_pct = chance(50) ? 25 : chance(50) ? 5 : 1;
    snap_thresh = chance(30) ? 1 << 30 : 5 + (int)R(40);
    memset(g_set, 0, MAXIDX);
    memset(leader_of_term, 0, sizeof(int) * MAXTERM);
    g_max_applied = 0;
    n_net = 0;
    clean_net();
    for (int i = 1; i <= N; i++) {
        snode_t *x = &nodes[i];
        memset(x, 0, sizeof *x);
        x->id = i;
        node_start(x);
    }
    uint64_t next_fault = 500 + R(1000);
    uint64_t heal_at = duration - 6000, stop_prop = duration - 2000;
    uint64_t commit_at_heal = 0;
    g_healed = 0;
    uint64_t post_heal_commit_gain = 0;

    for (T = 1; T <= duration; T++) {
        /* faults */
        if (!g_healed && T >= heal_at) {
            g_healed = 1;
            clean_net();
            for (int i = 1; i <= N; i++) { nodes[i].crash_at = nodes[i].crash_after_sync = 0; if (!nodes[i].up) nodes[i].down_until = T; }
            commit_at_heal = g_max_applied;
        }
        if (!g_healed && T >= next_fault) { inject_fault(); next_fault = T + 200 + R(1500); }
        for (int i = 1; i <= N; i++) if (!nodes[i].up && T >= nodes[i].down_until) node_start(&nodes[i]);
        for (int i = 1; i <= N; i++) if (nodes[i].up && nodes[i].crash_at && T >= nodes[i].crash_at) node_crash(&nodes[i], nodes[i].crash_down);

        /* deliver */
        size_t w = 0;
        for (size_t i = 0; i < n_net; i++) {
            simmsg_t *s = &net[i];
            if (s->at > T) { net[w++] = *s; continue; }
            snode_t *dst = &nodes[s->m.to];
            if (dst->up && grp[s->m.from] == grp[s->m.to] && !blocked[s->m.from][s->m.to]) raft_step(dst->r, &s->m, T);
            free_simmsg(s);
        }
        n_net = w;

        /* tick + proposals */
        for (int i = 1; i <= N; i++) if (nodes[i].up) raft_tick(nodes[i].r, T);
        if (T < stop_prop && chance(prop_pct)) {
            int l = leader_node();
            if (l && (g_healed || chance(90))) {
                uint8_t d[12];
                uint64_t rv = rng_next(&g_rng);
                memcpy(d, &rv, 8);
                uint32_t len = 8 + (uint32_t)R(5);
                uint64_t idx, term;
                if (raft_propose(nodes[l].r, ENT_CMD, d, len, &idx, &term) == 0) st_proposed++;
            }
        }
        /* sync (sometimes lagging, so crashes can lose unsynced state) */
        for (int i = 1; i <= N; i++) {
            snode_t *x = &nodes[i];
            if (!x->up) continue;
            if (g_healed || chance(75)) { node_sync(x); maybe_snapshot(x); }
        }
        check_basic();
        if (T % 20 == 0) check_log_matching();
        if (T % 100 == 0) { int l = leader_node(); if (l) check_leader_complete(&nodes[l]); }
    }
    /* final convergence + liveness */
    post_heal_commit_gain = g_max_applied - commit_at_heal;
    int l = leader_node();
    if (!l) fail("LIVENESS: no leader after %dms of healthy network", 6000);
    if (post_heal_commit_gain == 0) fail("LIVENESS: no progress after heal");
    for (int i = 1; i <= N; i++) {
        snode_t *x = &nodes[i];
        if (!x->up) fail("node %d still down after heal", i);
        if (x->sm_applied != nodes[1].sm_applied || x->sm_hash != nodes[1].sm_hash)
            fail("CONVERGENCE: node %d applied=%" PRIu64 " hash=%" PRIx64 " vs node1 applied=%" PRIu64 " hash=%" PRIx64, i,
                 x->sm_applied, x->sm_hash, nodes[1].sm_applied, nodes[1].sm_hash);
    }
    for (int i = 1; i <= N; i++) {
        snode_t *x = &nodes[i];
        if (x->r) raft_free(x->r);
        for (size_t k = 0; k < x->d_n; k++) free(x->d_ents[k].data);
        free(x->d_ents); free(x->ops);
        for (size_t k = 0; k < x->n_out; k++) free_simmsg(&x->out[k]);
        free(x->out);
    }
    for (size_t i = 0; i < n_net; i++) free_simmsg(&net[i]);
    n_net = 0;
}

int main(int argc, char **argv) {
    int seeds = argc > 1 ? atoi(argv[1]) : 200;
    uint64_t first = argc > 2 ? strtoull(argv[2], NULL, 10) : 1;
    uint64_t dur = argc > 3 ? strtoull(argv[3], NULL, 10) : 30000;
    g_log_level = LOG_QUIET;
    g_chain = calloc(MAXIDX, sizeof *g_chain);
    g_term = calloc(MAXIDX, sizeof *g_term);
    g_dh = calloc(MAXIDX, sizeof *g_dh);
    g_cterm = calloc(MAXIDX, sizeof *g_cterm);
    g_set = calloc(MAXIDX, 1);
    leader_of_term = calloc(MAXTERM, sizeof(int));
    static const int sizes[] = {3, 5, 3, 1, 5, 3, 2};
    for (int s = 0; s < seeds; s++) {
        uint64_t seed = first + (uint64_t)s;
        run_seed(seed, sizes[seed % 7], dur);
    }
    printf("OK: %d seeds x %" PRIu64 "ms virtual time; no safety or liveness violation\n", seeds, dur);
    printf("    coverage: leaders elected=%" PRIu64 " crashes=%" PRIu64 " log truncations=%" PRIu64
           " snapshots taken=%" PRIu64 " installed=%" PRIu64 "\n",
           st_leaders, st_crashes, st_trunc, st_snaps, st_installs);
    printf("    messages sent=%" PRIu64 " (dropped/partitioned %" PRIu64 "), proposals accepted=%" PRIu64 "\n", st_msgs, st_dropped, st_proposed);
    return 0;
}

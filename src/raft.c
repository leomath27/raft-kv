#include "raft.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "util.h"

struct raft {
    int id, n;
    raft_cfg_t cfg;
    raft_ops_t ops;

    /* persistent */
    uint64_t term;
    int32_t vote;
    raft_entry_t *ents; /* ents[0] has index snap_index + 1 */
    size_t n_ents, cap_ents;
    uint64_t snap_index, snap_term;

    /* volatile */
    raft_role_t role;
    int leader;
    uint64_t commit, applied, durable;
    uint64_t now;
    uint64_t election_deadline, hb_deadline, quorum_deadline, last_leader_contact;
    uint64_t rng;
    uint8_t votes[RAFT_MAX_NODES + 1];

    /* leader state */
    uint64_t next[RAFT_MAX_NODES + 1], match[RAFT_MAX_NODES + 1];
    uint64_t ack_seq[RAFT_MAX_NODES + 1], last_resp[RAFT_MAX_NODES + 1];
    uint64_t inflight_since[RAFT_MAX_NODES + 1], snap_until[RAFT_MAX_NODES + 1];
    uint8_t inflight[RAFT_MAX_NODES + 1];
    uint64_t inflight_last[RAFT_MAX_NODES + 1]; /* last index of the batch currently in flight to each peer */
    uint64_t bcast_seq;
    uint64_t noop_index;
};

void raft_cfg_default(raft_cfg_t *c) {
    c->election_min_ms = 150;
    c->election_max_ms = 300;
    c->heartbeat_ms = 50;
    c->max_entries_per_msg = 512;
    c->max_bytes_per_msg = 1 << 20;
    c->prevote = 1;
    c->check_quorum = 1;
}

/* ---------------------------------------------------------------- */
/* log helpers                                                       */
/* ---------------------------------------------------------------- */
static uint64_t last_index(const raft_t *r) { return r->snap_index + r->n_ents; }

int raft_term_at(const raft_t *r, uint64_t i, uint64_t *t) {
    if (i == r->snap_index) { *t = r->snap_term; return 1; }
    if (i < r->snap_index || i > last_index(r)) return 0;
    *t = r->ents[i - r->snap_index - 1].term;
    return 1;
}
static uint64_t last_term(const raft_t *r) {
    uint64_t t = 0;
    raft_term_at(r, last_index(r), &t);
    return t;
}
const raft_entry_t *raft_entry_at(const raft_t *r, uint64_t i) {
    if (i <= r->snap_index || i > last_index(r)) return NULL;
    return &r->ents[i - r->snap_index - 1];
}

static void log_push(raft_t *r, uint64_t term, uint8_t type, const uint8_t *data, uint32_t len) {
    if (r->n_ents == r->cap_ents) {
        r->cap_ents = r->cap_ents ? r->cap_ents * 2 : 256;
        r->ents = xrealloc(r->ents, r->cap_ents * sizeof *r->ents);
    }
    raft_entry_t *e = &r->ents[r->n_ents++];
    e->term = term;
    e->type = type;
    e->len = len;
    e->data = xmalloc(len);
    if (len) memcpy(e->data, data, len);
    r->ops.append(r->ops.ctx, last_index(r), e);
}

static void log_truncate_from(raft_t *r, uint64_t from) {
    assert(from > r->snap_index && from <= last_index(r) + 1);
    assert(from > r->commit);
    size_t keep = (size_t)(from - r->snap_index - 1);
    for (size_t i = keep; i < r->n_ents; i++) free(r->ents[i].data);
    r->n_ents = keep;
    if (r->durable > last_index(r)) r->durable = last_index(r);
    r->ops.truncate(r->ops.ctx, from);
}

static int quorum(const raft_t *r) { return r->n / 2 + 1; }

static void send_msg(raft_t *r, raft_msg_t *m) {
    m->from = r->id;
    r->ops.send(r->ops.ctx, m);
}

static void reset_election(raft_t *r) {
    uint64_t span = r->cfg.election_max_ms - r->cfg.election_min_ms;
    r->election_deadline = r->now + r->cfg.election_min_ms + (span ? rng_next(&r->rng) % (span + 1) : 0);
}

const char *raft_role_name(raft_role_t role) {
    switch (role) {
    case RAFT_FOLLOWER: return "follower";
    case RAFT_PRECANDIDATE: return "precandidate";
    case RAFT_CANDIDATE: return "candidate";
    default: return "leader";
    }
}

static void set_role(raft_t *r, raft_role_t role) {
    raft_role_t old = r->role;
    if (old == role) return;
    r->role = role;
    if (r->ops.become) r->ops.become(r->ops.ctx, old, role);
}

/* ---------------------------------------------------------------- */
/* creation / restore                                                */
/* ---------------------------------------------------------------- */
raft_t *raft_new(int id, int n, const raft_cfg_t *cfg, const raft_ops_t *ops, uint64_t now, uint64_t seed) {
    assert(n >= 1 && n <= RAFT_MAX_NODES && id >= 1 && id <= n);
    raft_t *r = xcalloc(1, sizeof *r);
    r->id = id;
    r->n = n;
    r->cfg = *cfg;
    r->ops = *ops;
    r->now = now;
    r->rng = seed * 0x9E3779B97F4A7C15ull + (uint64_t)id * 7919u + 1;
    r->role = RAFT_FOLLOWER;
    reset_election(r);
    return r;
}

void raft_free(raft_t *r) {
    if (!r) return;
    for (size_t i = 0; i < r->n_ents; i++) free(r->ents[i].data);
    free(r->ents);
    free(r);
}

void raft_restore(raft_t *r, uint64_t term, int32_t vote, uint64_t snap_index, uint64_t snap_term,
                  const raft_entry_t *ents, size_t n_ents) {
    r->term = term;
    r->vote = vote;
    r->snap_index = snap_index;
    r->snap_term = snap_term;
    r->n_ents = n_ents;
    r->cap_ents = n_ents ? n_ents : 256;
    r->ents = xmalloc(r->cap_ents * sizeof *r->ents);
    if (n_ents) memcpy(r->ents, ents, n_ents * sizeof *ents);
    r->commit = r->applied = snap_index;
    r->durable = last_index(r);
    reset_election(r);
}

/* ---------------------------------------------------------------- */
/* apply / commit                                                    */
/* ---------------------------------------------------------------- */
static void apply_committed(raft_t *r) {
    while (r->applied < r->commit) {
        r->applied++;
        const raft_entry_t *e = raft_entry_at(r, r->applied);
        assert(e);
        r->ops.apply(r->ops.ctx, r->applied, e);
    }
}

static int cmp_u64_desc(const void *a, const void *b) {
    uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
    return x < y ? 1 : x > y ? -1 : 0;
}

static void maybe_commit(raft_t *r) {
    if (r->role != RAFT_LEADER) return;
    uint64_t idx[RAFT_MAX_NODES];
    int k = 0;
    for (int p = 1; p <= r->n; p++) idx[k++] = (p == r->id) ? r->durable : r->match[p];
    qsort(idx, (size_t)k, sizeof idx[0], cmp_u64_desc);
    uint64_t q = idx[quorum(r) - 1];
    uint64_t t;
    if (q > r->commit && raft_term_at(r, q, &t) && t == r->term) {
        r->commit = q;
        apply_committed(r);
    }
}

void raft_persisted(raft_t *r) {
    r->durable = last_index(r);
    maybe_commit(r);
}

/* ---------------------------------------------------------------- */
/* leader replication                                                */
/* ---------------------------------------------------------------- */
static void send_snapshot(raft_t *r, int p) {
    uint8_t *data = NULL;
    size_t len = 0;
    if (!r->ops.load_snapshot || r->ops.load_snapshot(r->ops.ctx, &data, &len) != 0) return;
    raft_msg_t m;
    memset(&m, 0, sizeof m);
    m.type = MSG_SNAP_REQ;
    m.to = p;
    m.term = r->term;
    m.last_idx = r->snap_index;
    m.last_term = r->snap_term;
    m.snap = data;
    m.snap_len = len;
    send_msg(r, &m);
    free(data);
    r->snap_until[p] = r->now + 1000;
}

static void replicate(raft_t *r, int p) {
    raft_msg_t m;
    memset(&m, 0, sizeof m);
    m.type = MSG_APP_REQ;
    m.to = p;
    m.term = r->term;
    m.commit = r->commit;
    m.seq = r->bcast_seq;
    uint64_t next = r->next[p];
    if (next <= r->snap_index) {
        if (r->now >= r->snap_until[p]) { send_snapshot(r, p); return; }
        /* snapshot in flight: keep the follower's timer fresh and probe */
        m.prev_idx = r->snap_index;
        m.prev_term = r->snap_term;
        send_msg(r, &m);
        return;
    }
    uint64_t pt = 0;
    raft_term_at(r, next - 1, &pt);
    m.prev_idx = next - 1;
    m.prev_term = pt;
    uint64_t last = last_index(r);
    int may = !r->inflight[p] || (r->now - r->inflight_since[p] >= (uint64_t)r->cfg.heartbeat_ms * 3);
    if (may && next <= last) {
        size_t k = 0, bytes = 0;
        while (next + k <= last && k < r->cfg.max_entries_per_msg) {
            const raft_entry_t *e = &r->ents[next + k - r->snap_index - 1];
            if (k > 0 && bytes + e->len > r->cfg.max_bytes_per_msg) break;
            bytes += e->len;
            k++;
        }
        m.ents = &r->ents[next - r->snap_index - 1];
        m.n_ents = (uint32_t)k;
        r->inflight[p] = 1;
        r->inflight_last[p] = next + k - 1;
        r->inflight_since[p] = r->now;
    }
    send_msg(r, &m);
}

static void broadcast(raft_t *r) {
    r->bcast_seq++;
    r->hb_deadline = r->now + r->cfg.heartbeat_ms;
    for (int p = 1; p <= r->n; p++)
        if (p != r->id) replicate(r, p);
}

/* ---------------------------------------------------------------- */
/* role transitions                                                  */
/* ---------------------------------------------------------------- */
static void become_follower(raft_t *r, uint64_t term, int leader) {
    if (term > r->term) {
        r->term = term;
        r->vote = 0;
        r->ops.save_state(r->ops.ctx, r->term, r->vote);
    }
    r->leader = leader;
    reset_election(r);
    set_role(r, RAFT_FOLLOWER);
}

static void become_leader(raft_t *r) {
    r->leader = r->id;
    for (int p = 1; p <= r->n; p++) {
        r->next[p] = last_index(r) + 1;
        r->match[p] = 0;
        r->ack_seq[p] = 0;
        r->last_resp[p] = r->now;
        r->inflight[p] = 0;
        r->snap_until[p] = 0;
    }
    r->quorum_deadline = r->now + r->cfg.election_min_ms;
    set_role(r, RAFT_LEADER);
    log_push(r, r->term, ENT_NOOP, NULL, 0); /* commits prior-term entries */
    r->noop_index = last_index(r);
    broadcast(r);
}

static int count_votes(const raft_t *r) {
    int c = 0;
    for (int p = 1; p <= r->n; p++) c += r->votes[p];
    return c;
}

static void send_vote_reqs(raft_t *r, uint64_t term, int prevote) {
    for (int p = 1; p <= r->n; p++) {
        if (p == r->id) continue;
        raft_msg_t m;
        memset(&m, 0, sizeof m);
        m.type = MSG_VOTE_REQ;
        m.to = p;
        m.term = term;
        m.prevote = (uint8_t)prevote;
        m.last_idx = last_index(r);
        m.last_term = last_term(r);
        send_msg(r, &m);
    }
}

static void start_election(raft_t *r) {
    r->term++;
    r->vote = r->id;
    r->ops.save_state(r->ops.ctx, r->term, r->vote);
    r->leader = 0;
    memset(r->votes, 0, sizeof r->votes);
    r->votes[r->id] = 1;
    reset_election(r);
    set_role(r, RAFT_CANDIDATE);
    LOGD("starting election for term %llu", (unsigned long long)r->term);
    if (count_votes(r) >= quorum(r)) { become_leader(r); return; }
    send_vote_reqs(r, r->term, 0);
}

static void start_prevote(raft_t *r) {
    r->leader = 0;
    memset(r->votes, 0, sizeof r->votes);
    r->votes[r->id] = 1;
    reset_election(r);
    set_role(r, RAFT_PRECANDIDATE);
    if (count_votes(r) >= quorum(r)) { start_election(r); return; }
    send_vote_reqs(r, r->term + 1, 1);
}

static int leader_recent(const raft_t *r) {
    if (r->role == RAFT_LEADER) return 1;
    return r->leader != 0 && r->now - r->last_leader_contact < r->cfg.election_min_ms;
}

static int log_up_to_date(const raft_t *r, uint64_t lidx, uint64_t lterm) {
    uint64_t mt = last_term(r);
    return lterm > mt || (lterm == mt && lidx >= last_index(r));
}

/* ---------------------------------------------------------------- */
/* message handlers                                                  */
/* ---------------------------------------------------------------- */
static void handle_vote_req(raft_t *r, const raft_msg_t *m) {
    raft_msg_t resp;
    memset(&resp, 0, sizeof resp);
    resp.type = MSG_VOTE_RESP;
    resp.to = m->from;
    resp.prevote = m->prevote;
    if (m->prevote) {
        int grant = m->term > r->term && log_up_to_date(r, m->last_idx, m->last_term) && !leader_recent(r);
        resp.granted = (uint8_t)grant;
        resp.term = grant ? m->term : r->term;
        send_msg(r, &resp);
        return;
    }
    resp.term = r->term;
    if (m->term < r->term) { send_msg(r, &resp); return; }
    /* m->term == r->term here (a higher term was adopted in raft_step) */
    int grant = (r->vote == 0 || r->vote == m->from) && log_up_to_date(r, m->last_idx, m->last_term);
    if (grant) {
        r->vote = m->from;
        r->ops.save_state(r->ops.ctx, r->term, r->vote);
        reset_election(r);
    }
    resp.granted = (uint8_t)grant;
    send_msg(r, &resp);
}

static void handle_vote_resp(raft_t *r, const raft_msg_t *m) {
    if (m->prevote) {
        if (r->role != RAFT_PRECANDIDATE) return;
        if (m->granted && m->term == r->term + 1) {
            r->votes[m->from] = 1;
            if (count_votes(r) >= quorum(r)) start_election(r);
        }
        return;
    }
    if (r->role != RAFT_CANDIDATE || m->term != r->term) return;
    if (m->granted) {
        r->votes[m->from] = 1;
        if (count_votes(r) >= quorum(r)) become_leader(r);
    }
}

static void handle_app_req(raft_t *r, const raft_msg_t *m) {
    raft_msg_t resp;
    memset(&resp, 0, sizeof resp);
    resp.type = MSG_APP_RESP;
    resp.to = m->from;
    resp.term = r->term;
    resp.seq = m->seq;
    if (m->term < r->term) { send_msg(r, &resp); return; }
    assert(r->role != RAFT_LEADER); /* two leaders in one term is a safety bug */
    if (r->role != RAFT_FOLLOWER) become_follower(r, r->term, m->from);
    r->leader = m->from;
    r->last_leader_contact = r->now;
    reset_election(r);

    uint64_t prev_idx = m->prev_idx, prev_term = m->prev_term;
    const raft_entry_t *ents = m->ents;
    uint32_t n = m->n_ents;
    if (prev_idx < r->snap_index) { /* the overlap is covered by our snapshot (committed) */
        uint64_t skip = r->snap_index - prev_idx;
        if (skip >= n) { ents = NULL; n = 0; }
        else { ents += skip; n -= (uint32_t)skip; }
        prev_idx = r->snap_index;
        prev_term = r->snap_term;
    }
    uint64_t t;
    if (!raft_term_at(r, prev_idx, &t) || t != prev_term) {
        if (prev_idx > last_index(r)) {
            resp.hint = last_index(r) + 1;
        } else {
            uint64_t i = prev_idx, u;
            while (i > r->snap_index + 1 && raft_term_at(r, i - 1, &u) && u == t) i--;
            resp.hint = i;
        }
        send_msg(r, &resp);
        return;
    }
    for (uint32_t k = 0; k < n; k++) {
        uint64_t idx = prev_idx + 1 + k;
        if (idx <= last_index(r)) {
            uint64_t et = 0;
            raft_term_at(r, idx, &et);
            if (et == ents[k].term) continue;
            log_truncate_from(r, idx);
        }
        log_push(r, ents[k].term, ents[k].type, ents[k].data, ents[k].len);
    }
    uint64_t last_new = prev_idx + n;
    if (m->commit > r->commit) {
        uint64_t nc = m->commit < last_new ? m->commit : last_new;
        if (nc > r->commit) { r->commit = nc; apply_committed(r); }
    }
    resp.success = 1;
    resp.match = last_new;
    send_msg(r, &resp);
}

static void handle_app_resp(raft_t *r, const raft_msg_t *m) {
    if (r->role != RAFT_LEADER || m->term != r->term) return;
    int p = m->from;
    r->last_resp[p] = r->now;
    if (m->seq > r->ack_seq[p]) r->ack_seq[p] = m->seq;
    if (m->success) {
        if (m->match > r->match[p]) r->match[p] = m->match;
        if (r->next[p] < r->match[p] + 1) r->next[p] = r->match[p] + 1;
        /* Only the response that covers the in-flight batch frees the window. A heartbeat's
         * response must not: it would trigger a duplicate send of the batch still in transit. */
        if (m->match >= r->inflight_last[p]) r->inflight[p] = 0;
        maybe_commit(r);
        if (!r->inflight[p] && r->next[p] <= last_index(r)) replicate(r, p);
    } else {
        r->inflight[p] = 0;
        uint64_t nx = m->hint < 1 ? 1 : m->hint;
        if (nx <= r->match[p]) nx = r->match[p] + 1;
        r->next[p] = nx;
        replicate(r, p);
    }
}

static void handle_snap_req(raft_t *r, const raft_msg_t *m) {
    raft_msg_t resp;
    memset(&resp, 0, sizeof resp);
    resp.type = MSG_SNAP_RESP;
    resp.to = m->from;
    resp.term = r->term;
    if (m->term < r->term) { send_msg(r, &resp); return; }
    assert(r->role != RAFT_LEADER);
    if (r->role != RAFT_FOLLOWER) become_follower(r, r->term, m->from);
    r->leader = m->from;
    r->last_leader_contact = r->now;
    reset_election(r);

    uint64_t idx = m->last_idx, t, mine;
    resp.success = 1;
    resp.match = idx;
    if (idx <= r->commit) { send_msg(r, &resp); return; }
    t = m->last_term;
    if (raft_term_at(r, idx, &mine) && mine == t) {
        /* we already hold this entry, so our log matches the leader's through idx */
        r->commit = idx;
        apply_committed(r);
        send_msg(r, &resp);
        return;
    }
    for (size_t i = 0; i < r->n_ents; i++) free(r->ents[i].data);
    r->n_ents = 0;
    r->snap_index = idx;
    r->snap_term = t;
    r->commit = r->applied = idx;
    r->durable = idx;
    r->ops.install_snapshot(r->ops.ctx, idx, t, m->snap, m->snap_len);
    send_msg(r, &resp);
}

static void handle_snap_resp(raft_t *r, const raft_msg_t *m) {
    if (r->role != RAFT_LEADER || m->term != r->term) return;
    int p = m->from;
    r->last_resp[p] = r->now;
    r->inflight[p] = 0;
    if (!m->success) return;
    r->snap_until[p] = 0;
    if (m->match > r->match[p]) r->match[p] = m->match;
    if (r->next[p] < r->match[p] + 1) r->next[p] = r->match[p] + 1;
    maybe_commit(r);
    if (r->next[p] <= last_index(r)) replicate(r, p);
}

void raft_step(raft_t *r, const raft_msg_t *m, uint64_t now) {
    r->now = now;
    if (m->from < 1 || m->from > r->n || m->from == r->id) return;
    if (m->term > r->term) {
        if (m->type == MSG_VOTE_REQ && m->prevote) {
            /* pre-votes never change our term */
        } else if (m->type == MSG_VOTE_RESP && m->prevote && m->granted) {
            /* a granted pre-vote carries the proposed (future) term */
        } else if (m->type == MSG_VOTE_REQ) {
            if (leader_recent(r)) return; /* stickiness: ignore disruptive candidates */
            become_follower(r, m->term, 0);
        } else {
            int ldr = (m->type == MSG_APP_REQ || m->type == MSG_SNAP_REQ) ? m->from : 0;
            become_follower(r, m->term, ldr);
        }
    }
    switch (m->type) {
    case MSG_VOTE_REQ: handle_vote_req(r, m); break;
    case MSG_VOTE_RESP: handle_vote_resp(r, m); break;
    case MSG_APP_REQ: handle_app_req(r, m); break;
    case MSG_APP_RESP: handle_app_resp(r, m); break;
    case MSG_SNAP_REQ: handle_snap_req(r, m); break;
    case MSG_SNAP_RESP: handle_snap_resp(r, m); break;
    default: break;
    }
}

void raft_tick(raft_t *r, uint64_t now) {
    r->now = now;
    if (r->role == RAFT_LEADER) {
        if (now >= r->hb_deadline) broadcast(r);
        if (r->cfg.check_quorum && now >= r->quorum_deadline) {
            r->quorum_deadline = now + r->cfg.election_min_ms;
            int c = 1;
            for (int p = 1; p <= r->n; p++)
                if (p != r->id && now - r->last_resp[p] < r->cfg.election_max_ms) c++;
            if (c < quorum(r)) {
                LOGI("leader lost contact with a majority; stepping down (term %llu)", (unsigned long long)r->term);
                become_follower(r, r->term, 0);
            }
        }
        return;
    }
    if (now >= r->election_deadline) {
        if (r->cfg.prevote) start_prevote(r); else start_election(r);
    }
}

int raft_propose(raft_t *r, uint8_t type, const uint8_t *data, uint32_t len, uint64_t *index, uint64_t *term) {
    if (r->role != RAFT_LEADER) return -1;
    log_push(r, r->term, type, data, len);
    *index = last_index(r);
    *term = r->term;
    for (int p = 1; p <= r->n; p++)
        if (p != r->id && !r->inflight[p]) replicate(r, p);
    return 0;
}

/* ---------------------------------------------------------------- */
/* ReadIndex                                                         */
/* ---------------------------------------------------------------- */
int raft_read_ready(const raft_t *r) { return r->role == RAFT_LEADER && r->commit >= r->noop_index; }

uint64_t raft_read_begin(raft_t *r) {
    broadcast(r);
    return r->bcast_seq;
}

int raft_read_confirmed(const raft_t *r, uint64_t seq) {
    if (r->role != RAFT_LEADER) return 0;
    int c = 1;
    for (int p = 1; p <= r->n; p++)
        if (p != r->id && r->ack_seq[p] >= seq) c++;
    return c >= quorum(r);
}

/* ---------------------------------------------------------------- */
/* compaction                                                        */
/* ---------------------------------------------------------------- */
void raft_compact(raft_t *r, uint64_t index) {
    uint64_t t;
    if (index <= r->snap_index || index > r->applied || !raft_term_at(r, index, &t)) return;
    size_t drop = (size_t)(index - r->snap_index);
    for (size_t i = 0; i < drop; i++) free(r->ents[i].data);
    memmove(r->ents, r->ents + drop, (r->n_ents - drop) * sizeof *r->ents);
    r->n_ents -= drop;
    r->snap_index = index;
    r->snap_term = t;
    if (r->durable < index) r->durable = index;
}

/* ---------------------------------------------------------------- */
/* introspection                                                     */
/* ---------------------------------------------------------------- */
raft_role_t raft_role(const raft_t *r) { return r->role; }
uint64_t raft_term(const raft_t *r) { return r->term; }
int raft_leader(const raft_t *r) { return r->leader; }
int raft_voted_for(const raft_t *r) { return r->vote; }
uint64_t raft_commit(const raft_t *r) { return r->commit; }
uint64_t raft_applied(const raft_t *r) { return r->applied; }
uint64_t raft_last_index(const raft_t *r) { return last_index(r); }
uint64_t raft_snap_index(const raft_t *r) { return r->snap_index; }
uint64_t raft_snap_term(const raft_t *r) { return r->snap_term; }

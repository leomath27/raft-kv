/* raft.h - a transport- and storage-agnostic Raft core.
 *
 * The core is a pure state machine driven by three inputs: raft_tick() (time),
 * raft_step() (an incoming message) and raft_propose() (a client command). All
 * side effects go through raft_ops_t, so the same code runs under the real
 * epoll server and under the deterministic simulator in tests/sim.c.
 *
 * Implemented: leader election with PreVote and leader stickiness, CheckQuorum,
 * log replication with fast conflict backtracking, majority commit (only for
 * current-term entries), durable-index-aware commit, log compaction with
 * InstallSnapshot, and ReadIndex linearizable reads.
 *
 * Durability contract (the driver MUST honor it):
 *   save_state/append/truncate only *queue* a write. The driver must make all
 *   queued writes durable and THEN call raft_persisted() before it releases any
 *   message produced since the last sync. This gives group commit while
 *   guaranteeing we never acknowledge (vote / append-ack / commit) state that
 *   could be lost in a crash.
 */
#ifndef RAFT_H
#define RAFT_H

#include <stddef.h>
#include <stdint.h>

#define RAFT_MAX_NODES 9

typedef enum { RAFT_FOLLOWER = 0, RAFT_PRECANDIDATE, RAFT_CANDIDATE, RAFT_LEADER } raft_role_t;
enum { ENT_NOOP = 0, ENT_CMD = 1 };
enum { MSG_VOTE_REQ = 1, MSG_VOTE_RESP, MSG_APP_REQ, MSG_APP_RESP, MSG_SNAP_REQ, MSG_SNAP_RESP };

typedef struct {
    uint64_t term;
    uint8_t type;
    uint32_t len;
    uint8_t *data;
} raft_entry_t;

/* Node ids are 1..n. A message and everything it points to is BORROWED: the
 * receiver must copy what it keeps; the sender's ops->send must serialize
 * synchronously. */
typedef struct {
    uint8_t type;
    uint8_t prevote;  /* VOTE_REQ / VOTE_RESP */
    uint8_t granted;  /* VOTE_RESP */
    uint8_t success;  /* APP_RESP / SNAP_RESP */
    int32_t from, to;
    uint64_t term;
    uint64_t last_idx, last_term;  /* VOTE_REQ: candidate's last entry. SNAP_REQ: last included */
    uint64_t prev_idx, prev_term;  /* APP_REQ */
    uint64_t commit;               /* APP_REQ: leader commit */
    uint64_t seq;                  /* APP_REQ / APP_RESP: heartbeat round (ReadIndex) */
    uint64_t match;                /* APP_RESP / SNAP_RESP: highest index known replicated */
    uint64_t hint;                 /* APP_RESP reject: where the leader should retry from */
    const raft_entry_t *ents;      /* APP_REQ entries */
    uint32_t n_ents;
    const uint8_t *snap;           /* SNAP_REQ payload */
    size_t snap_len;
} raft_msg_t;

typedef struct {
    void *ctx;
    void (*send)(void *ctx, const raft_msg_t *m);
    void (*save_state)(void *ctx, uint64_t term, int32_t vote);
    void (*append)(void *ctx, uint64_t index, const raft_entry_t *e);
    void (*truncate)(void *ctx, uint64_t from_index); /* drop entries >= from_index */
    void (*apply)(void *ctx, uint64_t index, const raft_entry_t *e);
    void (*become)(void *ctx, raft_role_t from, raft_role_t to);
    /* malloc'd snapshot blob of the state at raft_snap_index(); caller frees */
    int (*load_snapshot)(void *ctx, uint8_t **data, size_t *len);
    void (*install_snapshot)(void *ctx, uint64_t index, uint64_t term, const uint8_t *data, size_t len);
} raft_ops_t;

typedef struct {
    uint32_t election_min_ms, election_max_ms, heartbeat_ms;
    uint32_t max_entries_per_msg, max_bytes_per_msg;
    int prevote, check_quorum;
} raft_cfg_t;

typedef struct raft raft_t;

void raft_cfg_default(raft_cfg_t *c);
raft_t *raft_new(int id, int n_nodes, const raft_cfg_t *cfg, const raft_ops_t *ops, uint64_t now, uint64_t seed);
void raft_free(raft_t *r);

/* Load recovered durable state before the first tick. Takes ownership of the
 * entries' data pointers. The state machine must already reflect snap_index. */
void raft_restore(raft_t *r, uint64_t term, int32_t vote, uint64_t snap_index, uint64_t snap_term,
                  const raft_entry_t *ents, size_t n_ents);

void raft_tick(raft_t *r, uint64_t now);
void raft_step(raft_t *r, const raft_msg_t *m, uint64_t now);

/* Leader only. Returns 0 and the entry's index/term, or -1 if not leader. */
int raft_propose(raft_t *r, uint8_t type, const uint8_t *data, uint32_t len, uint64_t *index, uint64_t *term);

/* Call after the driver made every queued write durable. */
void raft_persisted(raft_t *r);

/* ReadIndex: ready => the leader has committed an entry of its own term.
 * begin() triggers a heartbeat round and returns its sequence; the read may be
 * served once confirmed(seq) is true AND applied >= the commit index captured
 * when the read was admitted. */
int raft_read_ready(const raft_t *r);
uint64_t raft_read_begin(raft_t *r);
int raft_read_confirmed(const raft_t *r, uint64_t seq);

/* The driver has durably snapshotted the state machine at `index`
 * (index <= applied): drop the log prefix. */
void raft_compact(raft_t *r, uint64_t index);

/* Introspection */
raft_role_t raft_role(const raft_t *r);
uint64_t raft_term(const raft_t *r);
int raft_leader(const raft_t *r);
int raft_voted_for(const raft_t *r);
uint64_t raft_commit(const raft_t *r);
uint64_t raft_applied(const raft_t *r);
uint64_t raft_last_index(const raft_t *r);
uint64_t raft_snap_index(const raft_t *r);
uint64_t raft_snap_term(const raft_t *r);
int raft_term_at(const raft_t *r, uint64_t index, uint64_t *term);
const raft_entry_t *raft_entry_at(const raft_t *r, uint64_t index); /* NULL if unavailable */
const char *raft_role_name(raft_role_t r);

#endif

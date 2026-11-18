/* server.c - the node process: a single-threaded epoll event loop that owns
 *   - the RESP client listener (Redis-protocol compatible subset),
 *   - the Raft peer transport (one outgoing TCP connection per peer; every
 *     node both dials and accepts, messages are framed and checksummed by TCP),
 *   - the write-ahead log with group commit, and
 *   - the glue between Raft and the KV state machine.
 *
 * Event-loop iteration:  epoll events -> raft_tick -> serve parked reads ->
 *   fsync WAL once (group commit) -> raft_persisted -> flush sockets.
 * Nothing produced during an iteration reaches the network (or a client)
 * before the iteration's WAL sync, which is what makes votes, append-acks and
 * commit decisions crash-safe. */
#define _GNU_SOURCE
#include "server.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include "kv.h"
#include "resp.h"
#include "util.h"
#include "wal.h"

#define MAX_EVENTS 256
#define MAX_PENDING_WRITES 200000
#define MAX_CLIENT_OUT (256u << 20)
#define MAX_CLIENT_IN (64u << 20)
#define WRITE_TIMEOUT_MS 15000
#define READ_TIMEOUT_MS 5000

enum { K_LISTEN_C, K_LISTEN_R, K_CLIENT, K_PEER_IN, K_PEER_OUT };

typedef struct {
    uint8_t ready;
    buf_t b;
} slot_t;

typedef struct conn {
    int fd, kind;
    uint64_t id;
    buf_t rbuf, wbuf;
    int peer; /* K_PEER_OUT: target node id */
    int connecting, closing, want_out, in_dirty, quit_after_flush;
    /* ordered reply slots so pipelined replies keep request order even when
     * some requests (writes, linearizable reads) complete later */
    slot_t *slots;
    size_t s_cap, s_head, s_len;
    uint64_t head_id;
    uint64_t last_write_index; /* log index of this connection's newest write (read-your-writes) */
} conn_t;

typedef struct { int fd; uint64_t cid, slot, index, term, born; } pw_t;
typedef struct { int fd; uint64_t cid, slot, min_index, read_index, seq, born; int state; buf_t cmd; } pr_t;
typedef struct { struct sockaddr_in addr; conn_t *out; uint64_t retry_at, backoff; } peer_t;

typedef struct server {
    server_cfg_t cfg;
    raft_t *raft;
    kv_t *kv;
    wal_t *wal;
    int ep, lfd_c, lfd_r;
    conn_t **by_fd;
    size_t by_fd_cap;
    uint64_t next_conn_id;
    conn_t **dead; size_t n_dead, cap_dead;
    conn_t **dirty; size_t n_dirty, cap_dirty;
    peer_t peers[RAFT_MAX_NODES + 1];
    uint8_t blocked[RAFT_MAX_NODES + 1]; /* chaos: drop raft traffic to/from these peers */
    pw_t *pw; size_t pw_head, pw_len, pw_cap;
    pr_t *pr; size_t pr_len, pr_cap;
    uint64_t read_batch_seq;
    buf_t tmp, enc;
    resp_cmd_t dcmd; /* scratch for decoding parked reads */
    uint64_t now, start_ms;
    uint64_t st_cmds, st_writes, st_reads, st_elections_won, st_snaps, st_installs, st_redirects;
} server_t;

static volatile sig_atomic_t g_stop;
static void on_signal(int sig) { (void)sig; g_stop = 1; }

static void mark_dirty(server_t *s, conn_t *c) {
    if (c->in_dirty || c->closing) return;
    c->in_dirty = 1;
    if (s->n_dirty == s->cap_dirty) { s->cap_dirty = s->cap_dirty ? s->cap_dirty * 2 : 256; s->dirty = xrealloc(s->dirty, s->cap_dirty * sizeof *s->dirty); }
    s->dirty[s->n_dirty++] = c;
}

/* ------------------------------------------------------------------ */
/* connections                                                         */
/* ------------------------------------------------------------------ */
static void set_nodelay(int fd) { int one = 1; setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one); }

static conn_t *conn_new(server_t *s, int fd, int kind) {
    conn_t *c = xcalloc(1, sizeof *c);
    c->fd = fd;
    c->kind = kind;
    c->id = ++s->next_conn_id;
    buf_init(&c->rbuf);
    buf_init(&c->wbuf);
    if ((size_t)fd >= s->by_fd_cap) {
        size_t n = s->by_fd_cap ? s->by_fd_cap : 1024;
        while (n <= (size_t)fd) n *= 2;
        s->by_fd = xrealloc(s->by_fd, n * sizeof *s->by_fd);
        memset(s->by_fd + s->by_fd_cap, 0, (n - s->by_fd_cap) * sizeof *s->by_fd);
        s->by_fd_cap = n;
    }
    s->by_fd[fd] = c;
    struct epoll_event ev = {.events = EPOLLIN | (kind == K_PEER_OUT ? EPOLLOUT : 0), .data.ptr = c};
    epoll_ctl(s->ep, EPOLL_CTL_ADD, fd, &ev);
    return c;
}

static void conn_close(server_t *s, conn_t *c) {
    if (c->closing) return;
    c->closing = 1;
    epoll_ctl(s->ep, EPOLL_CTL_DEL, c->fd, NULL);
    close(c->fd);
    s->by_fd[c->fd] = NULL;
    if (c->kind == K_PEER_OUT) {
        peer_t *p = &s->peers[c->peer];
        if (p->out == c) p->out = NULL;
        p->retry_at = s->now + p->backoff;
        p->backoff = p->backoff * 2 > 250 ? 250 : p->backoff * 2;
    }
    if (s->n_dead == s->cap_dead) { s->cap_dead = s->cap_dead ? s->cap_dead * 2 : 64; s->dead = xrealloc(s->dead, s->cap_dead * sizeof *s->dead); }
    s->dead[s->n_dead++] = c;
}

static void conn_free(conn_t *c) {
    buf_free(&c->rbuf);
    buf_free(&c->wbuf);
    for (size_t i = 0; i < c->s_len; i++) buf_free(&c->slots[(c->s_head + i) % c->s_cap].b);
    free(c->slots);
    free(c);
}

static conn_t *conn_lookup(server_t *s, int fd, uint64_t id) {
    if (fd < 0 || (size_t)fd >= s->by_fd_cap) return NULL;
    conn_t *c = s->by_fd[fd];
    return (c && c->id == id && !c->closing) ? c : NULL;
}

static void conn_set_out(server_t *s, conn_t *c, int on) {
    if (c->want_out == on) return;
    c->want_out = on;
    struct epoll_event ev = {.events = EPOLLIN | (on ? EPOLLOUT : 0), .data.ptr = c};
    epoll_ctl(s->ep, EPOLL_CTL_MOD, c->fd, &ev);
}

static void conn_write(server_t *s, conn_t *c) {
    while (buf_len(&c->wbuf)) {
        ssize_t n = send(c->fd, buf_data(&c->wbuf), buf_len(&c->wbuf), MSG_NOSIGNAL);
        if (n > 0) { buf_consume(&c->wbuf, (size_t)n); continue; }
        if (n < 0 && errno == EINTR) continue;
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) { conn_set_out(s, c, 1); return; }
        conn_close(s, c);
        return;
    }
    conn_set_out(s, c, 0);
    buf_shrink_if_idle(&c->wbuf, 1u << 20);
    if (c->quit_after_flush) conn_close(s, c);
}

static void flush_all(server_t *s) {
    for (size_t i = 0; i < s->n_dirty; i++) {
        conn_t *c = s->dirty[i];
        c->in_dirty = 0;
        if (!c->closing && !c->connecting) conn_write(s, c);
    }
    s->n_dirty = 0;
}

/* ------------------------------------------------------------------ */
/* client reply slots                                                  */
/* ------------------------------------------------------------------ */
static void slots_flush(server_t *s, conn_t *c) {
    int any = 0;
    while (c->s_len && c->slots[c->s_head].ready) {
        slot_t *sl = &c->slots[c->s_head];
        buf_put(&c->wbuf, buf_data(&sl->b), buf_len(&sl->b));
        buf_free(&sl->b);
        c->s_head = (c->s_head + 1) % c->s_cap;
        c->s_len--;
        c->head_id++;
        any = 1;
    }
    if (any) {
        if (buf_len(&c->wbuf) > MAX_CLIENT_OUT) { conn_close(s, c); return; }
        mark_dirty(s, c);
    }
}

static uint64_t slot_push_pending(conn_t *c) {
    if (c->s_len == c->s_cap) {
        size_t ncap = c->s_cap ? c->s_cap * 2 : 16;
        slot_t *ns = xcalloc(ncap, sizeof *ns);
        for (size_t i = 0; i < c->s_len; i++) ns[i] = c->slots[(c->s_head + i) % c->s_cap];
        free(c->slots);
        c->slots = ns;
        c->s_cap = ncap;
        c->s_head = 0;
    }
    uint64_t id = c->head_id + c->s_len;
    slot_t *sl = &c->slots[(c->s_head + c->s_len) % c->s_cap];
    memset(sl, 0, sizeof *sl);
    c->s_len++;
    return id;
}

static void slot_fill(server_t *s, conn_t *c, uint64_t id, const void *data, size_t len) {
    if (id < c->head_id || id >= c->head_id + c->s_len) return;
    slot_t *sl = &c->slots[(c->s_head + (id - c->head_id)) % c->s_cap];
    if (sl->ready) return;
    buf_put(&sl->b, data, len);
    sl->ready = 1;
    slots_flush(s, c);
}

/* append an immediately-ready reply, preserving order behind parked ones */
static void reply(server_t *s, conn_t *c, const void *data, size_t len) {
    if (c->closing) return;
    if (c->s_len == 0) {
        buf_put(&c->wbuf, data, len);
        if (buf_len(&c->wbuf) > MAX_CLIENT_OUT) { conn_close(s, c); return; }
        mark_dirty(s, c);
    } else {
        uint64_t id = slot_push_pending(c);
        slot_fill(s, c, id, data, len);
    }
}
static void reply_buf(server_t *s, conn_t *c, buf_t *b) { reply(s, c, buf_data(b), buf_len(b)); }
static void reply_err(server_t *s, conn_t *c, const char *msg) {
    buf_t t;
    buf_init(&t);
    resp_error(&t, msg);
    reply_buf(s, c, &t);
    buf_free(&t);
}
static void deliver(server_t *s, int fd, uint64_t cid, uint64_t slot, const void *data, size_t len) {
    conn_t *c = conn_lookup(s, fd, cid);
    if (c) slot_fill(s, c, slot, data, len);
}
static void deliver_err(server_t *s, int fd, uint64_t cid, uint64_t slot, const char *msg) {
    buf_t t;
    buf_init(&t);
    resp_error(&t, msg);
    deliver(s, fd, cid, slot, buf_data(&t), buf_len(&t));
    buf_free(&t);
}

/* ------------------------------------------------------------------ */
/* peer wire format                                                    */
/* ------------------------------------------------------------------ */
static void encode_msg(buf_t *out, const raft_msg_t *m) {
    size_t start = buf_len(out);
    buf_u32(out, 0); /* length placeholder */
    buf_u8(out, m->type);
    buf_u8(out, m->prevote);
    buf_u8(out, m->granted);
    buf_u8(out, m->success);
    buf_u32(out, (uint32_t)m->from);
    buf_u32(out, (uint32_t)m->to);
    buf_u64(out, m->term);
    buf_u64(out, m->last_idx);
    buf_u64(out, m->last_term);
    buf_u64(out, m->prev_idx);
    buf_u64(out, m->prev_term);
    buf_u64(out, m->commit);
    buf_u64(out, m->seq);
    buf_u64(out, m->match);
    buf_u64(out, m->hint);
    buf_u32(out, m->n_ents);
    for (uint32_t i = 0; i < m->n_ents; i++) {
        buf_u64(out, m->ents[i].term);
        buf_u8(out, m->ents[i].type);
        buf_u32(out, m->ents[i].len);
        buf_put(out, m->ents[i].data, m->ents[i].len);
    }
    buf_u32(out, (uint32_t)m->snap_len);
    buf_put(out, m->snap, m->snap_len);
    uint32_t len = (uint32_t)(buf_len(out) - start - 4);
    uint8_t *p = buf_data(out) + start;
    p[0] = len; p[1] = len >> 8; p[2] = len >> 16; p[3] = len >> 24;
}

/* entries[] is allocated by the caller; data pointers alias p */
static int decode_msg(const uint8_t *p, size_t len, raft_msg_t *m, raft_entry_t **ents) {
    rd_t r;
    rd_init(&r, p, len);
    memset(m, 0, sizeof *m);
    *ents = NULL;
    m->type = rd_u8(&r);
    m->prevote = rd_u8(&r);
    m->granted = rd_u8(&r);
    m->success = rd_u8(&r);
    m->from = (int32_t)rd_u32(&r);
    m->to = (int32_t)rd_u32(&r);
    m->term = rd_u64(&r);
    m->last_idx = rd_u64(&r);
    m->last_term = rd_u64(&r);
    m->prev_idx = rd_u64(&r);
    m->prev_term = rd_u64(&r);
    m->commit = rd_u64(&r);
    m->seq = rd_u64(&r);
    m->match = rd_u64(&r);
    m->hint = rd_u64(&r);
    uint32_t n = rd_u32(&r);
    if (r.err || n > (1u << 22)) return -1;
    if (n) {
        *ents = xcalloc(n, sizeof **ents);
        for (uint32_t i = 0; i < n; i++) {
            (*ents)[i].term = rd_u64(&r);
            (*ents)[i].type = rd_u8(&r);
            uint32_t dl = rd_u32(&r);
            (*ents)[i].len = dl;
            (*ents)[i].data = (uint8_t *)rd_bytes(&r, dl);
            if (r.err) { free(*ents); *ents = NULL; return -1; }
        }
    }
    m->ents = *ents;
    m->n_ents = n;
    uint32_t sl = rd_u32(&r);
    m->snap = rd_bytes(&r, sl);
    m->snap_len = sl;
    if (r.err) { free(*ents); *ents = NULL; return -1; }
    return 0;
}

/* ------------------------------------------------------------------ */
/* raft ops                                                            */
/* ------------------------------------------------------------------ */
static void fail_pending_writes(server_t *s, const char *err) {
    while (s->pw_len) {
        pw_t *p = &s->pw[s->pw_head];
        deliver_err(s, p->fd, p->cid, p->slot, err);
        s->pw_head = (s->pw_head + 1) % s->pw_cap;
        s->pw_len--;
    }
}

static void ops_send(void *ctx, const raft_msg_t *m) {
    server_t *s = ctx;
    if (m->to < 1 || m->to > s->cfg.n || s->blocked[m->to]) return;
    peer_t *p = &s->peers[m->to];
    conn_t *c = p->out;
    if (!c || c->connecting || c->closing) return; /* dropped: Raft retransmits via heartbeats */
    if (m->type != MSG_SNAP_REQ && buf_len(&c->wbuf) > (64u << 20)) return;
    encode_msg(&c->wbuf, m);
    mark_dirty(s, c);
}
static void ops_save_state(void *ctx, uint64_t term, int32_t vote) { wal_state(((server_t *)ctx)->wal, term, vote); }
static void ops_append(void *ctx, uint64_t idx, const raft_entry_t *e) { wal_append(((server_t *)ctx)->wal, idx, e); }
static void ops_truncate(void *ctx, uint64_t from) { wal_truncate(((server_t *)ctx)->wal, from); }

static void ops_apply(void *ctx, uint64_t idx, const raft_entry_t *e) {
    server_t *s = ctx;
    buf_t *out = &s->tmp;
    buf_clear(out);
    if (e->type == ENT_CMD) kv_apply(s->kv, e->data, e->len, out);
    while (s->pw_len) {
        pw_t *p = &s->pw[s->pw_head];
        if (p->index > idx) break;
        s->pw_head = (s->pw_head + 1) % s->pw_cap;
        s->pw_len--;
        if (p->index == idx && p->term == e->term && e->type == ENT_CMD)
            deliver(s, p->fd, p->cid, p->slot, buf_data(out), buf_len(out));
        else
            deliver_err(s, p->fd, p->cid, p->slot, "TRYAGAIN entry was overwritten; the write did not take effect");
    }
}

static void ops_become(void *ctx, raft_role_t from, raft_role_t to) {
    server_t *s = ctx;
    LOGI("role %s -> %s (term %llu, leader %d)", raft_role_name(from), raft_role_name(to),
         (unsigned long long)raft_term(s->raft), raft_leader(s->raft));
    if (to == RAFT_LEADER) s->st_elections_won++;
    if (from == RAFT_LEADER)
        fail_pending_writes(s, "TIMEOUT leadership changed; the write may or may not have been applied");
}

static int ops_load_snapshot(void *ctx, uint8_t **data, size_t *len) {
    server_t *s = ctx;
    uint64_t idx, term;
    if (snap_load(s->cfg.dir, &idx, &term, data, len) != 0) return -1;
    if (idx != raft_snap_index(s->raft)) { free(*data); return -1; }
    return 0;
}

static void rewrite_wal_from_raft(server_t *s) {
    uint64_t snap = raft_snap_index(s->raft), last = raft_last_index(s->raft);
    const raft_entry_t *first = last > snap ? raft_entry_at(s->raft, snap + 1) : NULL;
    wal_rewrite(s->wal, raft_term(s->raft), raft_voted_for(s->raft), snap + 1, first, (size_t)(last - snap));
}

static void ops_install_snapshot(void *ctx, uint64_t idx, uint64_t term, const uint8_t *data, size_t len) {
    server_t *s = ctx;
    LOGI("installing snapshot from leader at index %llu (%zu bytes)", (unsigned long long)idx, len);
    if (snap_save(s->cfg.dir, idx, term, data, len, s->cfg.do_fsync) != 0 || kv_load(s->kv, data, len) != 0) {
        fprintf(stderr, "FATAL: cannot install snapshot\n");
        abort();
    }
    rewrite_wal_from_raft(s);
    s->st_installs++;
}

static void maybe_snapshot(server_t *s) {
    if (!s->cfg.snapshot_every) return;
    uint64_t applied = raft_applied(s->raft), snap = raft_snap_index(s->raft);
    if (applied <= snap || applied - snap < s->cfg.snapshot_every || wal_dirty(s->wal)) return;
    uint64_t term;
    if (!raft_term_at(s->raft, applied, &term)) return;
    uint64_t t0 = now_ms();
    buf_t b;
    buf_init(&b);
    kv_serialize(s->kv, &b);
    if (snap_save(s->cfg.dir, applied, term, buf_data(&b), buf_len(&b), s->cfg.do_fsync) != 0) {
        LOGW("snapshot save failed: %s", strerror(errno));
        buf_free(&b);
        return;
    }
    size_t sz = buf_len(&b);
    buf_free(&b);
    raft_compact(s->raft, applied);
    rewrite_wal_from_raft(s);
    s->st_snaps++;
    LOGI("snapshot at index %llu: %zu bytes, %llums", (unsigned long long)applied, sz, (unsigned long long)(now_ms() - t0));
}

/* ------------------------------------------------------------------ */
/* client commands                                                     */
/* ------------------------------------------------------------------ */
static void status_text(server_t *s, buf_t *b) {
    int leader = raft_leader(s->raft);
    int conn = 0;
    for (int p = 1; p <= s->cfg.n; p++) if (p != s->cfg.id && s->peers[p].out && !s->peers[p].out->connecting) conn++;
    buf_printf(b, "# Raft\r\nnode_id:%d\r\nrole:%s\r\nterm:%llu\r\nleader_id:%d\r\n", s->cfg.id,
               raft_role_name(raft_role(s->raft)), (unsigned long long)raft_term(s->raft), leader);
    if (leader > 0) buf_printf(b, "leader_addr:%s:%d\r\n", s->cfg.host[leader - 1], s->cfg.cport[leader - 1]);
    buf_printf(b, "voted_for:%d\r\ncluster_size:%d\r\nconnected_peers:%d\r\ncommit_index:%llu\r\nlast_applied:%llu\r\n"
                  "last_log_index:%llu\r\nsnapshot_index:%llu\r\n",
               raft_voted_for(s->raft), s->cfg.n, conn, (unsigned long long)raft_commit(s->raft),
               (unsigned long long)raft_applied(s->raft), (unsigned long long)raft_last_index(s->raft),
               (unsigned long long)raft_snap_index(s->raft));
    buf_printf(b, "# Keyspace\r\nkeys:%zu\r\n# Stats\r\ncommands:%llu\r\nwrites:%llu\r\nreads:%llu\r\nelections_won:%llu\r\n"
                  "snapshots_taken:%llu\r\nsnapshots_installed:%llu\r\nredirects:%llu\r\nuptime_ms:%llu\r\n",
               kv_size(s->kv), (unsigned long long)s->st_cmds, (unsigned long long)s->st_writes,
               (unsigned long long)s->st_reads, (unsigned long long)s->st_elections_won, (unsigned long long)s->st_snaps,
               (unsigned long long)s->st_installs, (unsigned long long)s->st_redirects,
               (unsigned long long)(s->now - s->start_ms));
}

static void redirect(server_t *s, conn_t *c) {
    s->st_redirects++;
    int leader = raft_leader(s->raft);
    char msg[160];
    if (leader > 0 && leader != s->cfg.id)
        snprintf(msg, sizeof msg, "MOVED 0 %s:%d", s->cfg.host[leader - 1], s->cfg.cport[leader - 1]);
    else
        snprintf(msg, sizeof msg, "TRYAGAIN no leader elected yet");
    reply_err(s, c, msg);
}

static void handle_write(server_t *s, conn_t *c, resp_cmd_t *cmd) {
    if (raft_role(s->raft) != RAFT_LEADER) { redirect(s, c); return; }
    uint64_t cid, seq;
    int argc = kv_strip_idem(cmd->argc, cmd->argv, cmd->lens, &cid, &seq);
    const char *err = kv_check_args(argc, cmd->argv, cmd->lens);
    if (err) { reply_err(s, c, err); return; }
    if (s->pw_len >= MAX_PENDING_WRITES) { reply_err(s, c, "BUSY too many in-flight writes"); return; }
    buf_clear(&s->enc);
    cmd_encode(&s->enc, cmd->argc, cmd->argv, cmd->lens);
    uint64_t idx, term;
    if (raft_propose(s->raft, ENT_CMD, buf_data(&s->enc), (uint32_t)buf_len(&s->enc), &idx, &term) != 0) { redirect(s, c); return; }
    s->st_writes++;
    c->last_write_index = idx;
    uint64_t slot = slot_push_pending(c);
    if (s->pw_len == s->pw_cap) {
        size_t ncap = s->pw_cap ? s->pw_cap * 2 : 1024;
        pw_t *np = xmalloc(ncap * sizeof *np);
        for (size_t i = 0; i < s->pw_len; i++) np[i] = s->pw[(s->pw_head + i) % s->pw_cap];
        free(s->pw);
        s->pw = np;
        s->pw_cap = ncap;
        s->pw_head = 0;
    }
    s->pw[(s->pw_head + s->pw_len) % s->pw_cap] = (pw_t){c->fd, c->id, slot, idx, term, s->now};
    s->pw_len++;
}

static void handle_read(server_t *s, conn_t *c, resp_cmd_t *cmd) {
    if (raft_role(s->raft) != RAFT_LEADER) { redirect(s, c); return; }
    const char *err = kv_check_args(cmd->argc, cmd->argv, cmd->lens);
    if (err) { reply_err(s, c, err); return; }
    if (s->pr_len == s->pr_cap) { s->pr_cap = s->pr_cap ? s->pr_cap * 2 : 256; s->pr = xrealloc(s->pr, s->pr_cap * sizeof *s->pr); }
    pr_t *r = &s->pr[s->pr_len++];
    memset(r, 0, sizeof *r);
    r->fd = c->fd;
    r->cid = c->id;
    r->slot = slot_push_pending(c);
    r->born = s->now;
    r->min_index = c->last_write_index; /* a pipelined read must observe the writes sent before it */
    buf_init(&r->cmd);
    cmd_encode(&r->cmd, cmd->argc, cmd->argv, cmd->lens);
    s->st_reads++;
}

/* Linearizable reads via ReadIndex: admit at the current commit index, confirm
 * leadership with a heartbeat round that starts after admission, wait until the
 * state machine has applied that index, then answer from local state. */
static void reads_pump(server_t *s) {
    if (!s->pr_len) return;
    int leader = raft_role(s->raft) == RAFT_LEADER;
    size_t w = 0;
    for (size_t i = 0; i < s->pr_len; i++) {
        pr_t *r = &s->pr[i];
        int done = 0;
        if (!leader) {
            conn_t *c = conn_lookup(s, r->fd, r->cid);
            if (c) { /* tell the client where to go */
                int ld = raft_leader(s->raft);
                char msg[160];
                if (ld > 0 && ld != s->cfg.id) snprintf(msg, sizeof msg, "MOVED 0 %s:%d", s->cfg.host[ld - 1], s->cfg.cport[ld - 1]);
                else snprintf(msg, sizeof msg, "TRYAGAIN leadership changed");
                deliver_err(s, r->fd, r->cid, r->slot, msg);
            }
            done = 1;
        } else if (s->now - r->born > READ_TIMEOUT_MS) {
            deliver_err(s, r->fd, r->cid, r->slot, "TIMEOUT linearizable read could not be confirmed");
            done = 1;
        } else {
            if (r->state == 0 && raft_read_ready(s->raft)) {
                r->read_index = raft_commit(s->raft);
                if (r->min_index > r->read_index) r->read_index = r->min_index;
                if (!s->read_batch_seq) s->read_batch_seq = raft_read_begin(s->raft);
                r->seq = s->read_batch_seq;
                r->state = 1;
            }
            if (r->state == 1 && raft_applied(s->raft) >= r->read_index && raft_read_confirmed(s->raft, r->seq)) {
                if (cmd_decode(buf_data(&r->cmd), buf_len(&r->cmd), &s->dcmd) == 0) {
                    buf_clear(&s->tmp);
                    kv_exec(s->kv, s->dcmd.argc, s->dcmd.argv, s->dcmd.lens, &s->tmp);
                    deliver(s, r->fd, r->cid, r->slot, buf_data(&s->tmp), buf_len(&s->tmp));
                }
                done = 1;
            }
        }
        if (done) buf_free(&r->cmd);
        else s->pr[w++] = *r;
    }
    s->pr_len = w;
}

static void expire_writes(server_t *s) {
    while (s->pw_len) {
        pw_t *p = &s->pw[s->pw_head];
        if (s->now - p->born <= WRITE_TIMEOUT_MS) break;
        deliver_err(s, p->fd, p->cid, p->slot, "TIMEOUT write not committed in time; it may or may not be applied");
        s->pw_head = (s->pw_head + 1) % s->pw_cap;
        s->pw_len--;
    }
}

static int is(const resp_cmd_t *c, const char *name) { return mem_ieq(c->argv[0], c->lens[0], name); }

static void exec_command(server_t *s, conn_t *c, resp_cmd_t *cmd) {
    s->st_cmds++;
    int kind = kv_cmd_kind(cmd->argv[0], cmd->lens[0]);
    if (kind == CMD_WRITE) { handle_write(s, c, cmd); return; }
    if (kind == CMD_READ) { handle_read(s, c, cmd); return; }
    buf_t b;
    buf_init(&b);
    if (is(cmd, "PING")) {
        if (cmd->argc > 1) resp_bulk(&b, cmd->argv[1], cmd->lens[1]); else resp_simple(&b, "PONG");
    } else if (is(cmd, "ECHO") && cmd->argc == 2) {
        resp_bulk(&b, cmd->argv[1], cmd->lens[1]);
    } else if (is(cmd, "QUIT")) {
        resp_simple(&b, "OK");
        c->quit_after_flush = 1;
    } else if (is(cmd, "INFO") || (is(cmd, "RAFT") && cmd->argc == 2 && mem_ieq(cmd->argv[1], cmd->lens[1], "STATUS"))) {
        buf_t t;
        buf_init(&t);
        status_text(s, &t);
        resp_bulk(&b, buf_data(&t), buf_len(&t));
        buf_free(&t);
    } else if (is(cmd, "COMMAND") || is(cmd, "CONFIG")) {
        resp_array(&b, 0);
    } else if (is(cmd, "SELECT") || is(cmd, "CLIENT")) {
        resp_simple(&b, "OK");
    } else if (is(cmd, "LOCALGET") && cmd->argc == 2) { /* stale read: any node, no consensus */
        const uint8_t *av[2] = {(const uint8_t *)"GET", cmd->argv[1]};
        size_t al[2] = {3, cmd->lens[1]};
        kv_exec(s->kv, 2, av, al, &b);
    } else if (is(cmd, "DEBUG") && cmd->argc == 2 && mem_ieq(cmd->argv[1], cmd->lens[1], "DIGEST")) {
        char t[128];
        int n = snprintf(t, sizeof t, "applied:%llu digest:%016llx keys:%zu", (unsigned long long)raft_applied(s->raft),
                         (unsigned long long)kv_digest(s->kv), kv_size(s->kv));
        resp_bulk(&b, t, (size_t)n);
    } else if (is(cmd, "CHAOS") && cmd->argc >= 2) { /* fault injection for the test harness */
        if (mem_ieq(cmd->argv[1], cmd->lens[1], "HEAL")) {
            memset(s->blocked, 0, sizeof s->blocked);
            resp_simple(&b, "OK");
        } else if (mem_ieq(cmd->argv[1], cmd->lens[1], "ISOLATE")) {
            memset(s->blocked, 0, sizeof s->blocked);
            for (int i = 2; i < cmd->argc; i++) {
                long long id;
                if (parse_ll(cmd->argv[i], cmd->lens[i], &id) == 0 && id >= 1 && id <= s->cfg.n) s->blocked[id] = 1;
            }
            resp_simple(&b, "OK");
        } else resp_error(&b, "ERR CHAOS HEAL | CHAOS ISOLATE <id>...");
    } else {
        char t[128];
        snprintf(t, sizeof t, "ERR unknown command '%.*s'", (int)(cmd->lens[0] > 60 ? 60 : cmd->lens[0]), cmd->argv[0]);
        resp_error(&b, t);
    }
    reply_buf(s, c, &b);
    buf_free(&b);
}

/* ------------------------------------------------------------------ */
/* socket events                                                       */
/* ------------------------------------------------------------------ */
static ssize_t fill_rbuf(server_t *s, conn_t *c) {
    buf_reserve(&c->rbuf, 64 * 1024);
    size_t room = c->rbuf.cap - c->rbuf.off - c->rbuf.len;
    ssize_t n = read(c->fd, c->rbuf.p + c->rbuf.off + c->rbuf.len, room);
    if (n > 0) c->rbuf.len += (size_t)n;
    else if (n == 0) conn_close(s, c);
    else if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) conn_close(s, c);
    return n;
}

static void client_readable(server_t *s, conn_t *c) {
    if (fill_rbuf(s, c) <= 0) return;
    resp_cmd_t cmd;
    cmd_init(&cmd);
    for (;;) {
        if (c->closing || c->quit_after_flush) break;
        size_t used = 0;
        int r = resp_parse(buf_data(&c->rbuf), buf_len(&c->rbuf), &cmd, &used);
        if (r == 0) break;
        if (r < 0) {
            reply_err(s, c, "ERR Protocol error");
            c->quit_after_flush = 1;
            break;
        }
        if (cmd.argc > 0) exec_command(s, c, &cmd);
        buf_consume(&c->rbuf, used);
    }
    cmd_free(&cmd);
    if (!c->closing && buf_len(&c->rbuf) > MAX_CLIENT_IN) conn_close(s, c);
    buf_shrink_if_idle(&c->rbuf, 1u << 20);
}

static void peer_in_readable(server_t *s, conn_t *c) {
    if (fill_rbuf(s, c) <= 0) return;
    for (;;) {
        if (c->closing) return;
        if (buf_len(&c->rbuf) < 4) break;
        rd_t h;
        rd_init(&h, buf_data(&c->rbuf), 4);
        uint32_t len = rd_u32(&h);
        if (len > (1u << 30)) { conn_close(s, c); return; }
        if (buf_len(&c->rbuf) < 4 + (size_t)len) break;
        raft_msg_t m;
        raft_entry_t *ents;
        if (decode_msg(buf_data(&c->rbuf) + 4, len, &m, &ents) != 0) { conn_close(s, c); return; }
        if (m.to == s->cfg.id && m.from >= 1 && m.from <= s->cfg.n && !s->blocked[m.from])
            raft_step(s->raft, &m, s->now);
        free(ents);
        buf_consume(&c->rbuf, 4 + (size_t)len);
    }
    buf_shrink_if_idle(&c->rbuf, 4u << 20);
}

static void accept_loop(server_t *s, conn_t *l) {
    for (;;) {
        int fd = accept4(l->fd, NULL, NULL, SOCK_NONBLOCK);
        if (fd < 0) {
            if (errno == EINTR) continue;
            return;
        }
        set_nodelay(fd);
        conn_new(s, fd, l->kind == K_LISTEN_C ? K_CLIENT : K_PEER_IN);
    }
}

static void peer_connect(server_t *s, int p) {
    peer_t *pe = &s->peers[p];
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (fd < 0) return;
    set_nodelay(fd);
    int r = connect(fd, (struct sockaddr *)&pe->addr, sizeof pe->addr);
    if (r != 0 && errno != EINPROGRESS) {
        close(fd);
        pe->retry_at = s->now + pe->backoff;
        pe->backoff = pe->backoff * 2 > 250 ? 250 : pe->backoff * 2;
        return;
    }
    conn_t *c = conn_new(s, fd, K_PEER_OUT);
    c->peer = p;
    c->connecting = 1;
    pe->out = c;
}

static void peer_connected(server_t *s, conn_t *c) {
    int err = 0;
    socklen_t l = sizeof err;
    getsockopt(c->fd, SOL_SOCKET, SO_ERROR, &err, &l);
    if (err) { conn_close(s, c); return; }
    c->connecting = 0;
    s->peers[c->peer].backoff = 25;
    c->want_out = 1; /* force the MOD below to clear EPOLLOUT */
    conn_set_out(s, c, 0);
    LOGD("connected to peer %d", c->peer);
}

static void handle_event(server_t *s, struct epoll_event *ev) {
    conn_t *c = ev->data.ptr;
    if (c->closing) return;
    uint32_t e = ev->events;
    switch (c->kind) {
    case K_LISTEN_C:
    case K_LISTEN_R:
        accept_loop(s, c);
        break;
    case K_PEER_OUT:
        if (c->connecting) {
            if (e & (EPOLLOUT | EPOLLERR | EPOLLHUP)) peer_connected(s, c);
            return;
        }
        if (e & EPOLLIN) { /* peers never write on our outgoing link: EOF/garbage => reconnect */
            uint8_t sink[512];
            ssize_t n = read(c->fd, sink, sizeof sink);
            if (n == 0 || (n < 0 && errno != EAGAIN && errno != EINTR)) { conn_close(s, c); return; }
        }
        if (e & (EPOLLERR | EPOLLHUP)) { conn_close(s, c); return; }
        if (e & EPOLLOUT) conn_write(s, c);
        break;
    case K_PEER_IN:
        if (e & EPOLLIN) peer_in_readable(s, c);
        if (!c->closing && (e & (EPOLLERR | EPOLLHUP))) conn_close(s, c);
        break;
    case K_CLIENT:
        if (e & EPOLLIN) client_readable(s, c);
        if (!c->closing && (e & EPOLLOUT)) conn_write(s, c);
        if (!c->closing && (e & (EPOLLERR | EPOLLHUP))) conn_close(s, c);
        break;
    }
}

/* ------------------------------------------------------------------ */
/* startup                                                             */
/* ------------------------------------------------------------------ */
static int listen_on(const char *host, int port) {
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (fd < 0) return -1;
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    struct sockaddr_in a;
    memset(&a, 0, sizeof a);
    a.sin_family = AF_INET;
    a.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, host, &a.sin_addr) != 1) {
        struct addrinfo hints = {.ai_family = AF_INET, .ai_socktype = SOCK_STREAM}, *res;
        if (getaddrinfo(host, NULL, &hints, &res) != 0) { close(fd); return -1; }
        a.sin_addr = ((struct sockaddr_in *)res->ai_addr)->sin_addr;
        freeaddrinfo(res);
    }
    if (bind(fd, (struct sockaddr *)&a, sizeof a) != 0 || listen(fd, 512) != 0) {
        fprintf(stderr, "cannot listen on %s:%d: %s\n", host, port, strerror(errno));
        close(fd);
        return -1;
    }
    return fd;
}

static int resolve(const char *host, int port, struct sockaddr_in *out) {
    memset(out, 0, sizeof *out);
    out->sin_family = AF_INET;
    out->sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, host, &out->sin_addr) == 1) return 0;
    struct addrinfo hints = {.ai_family = AF_INET, .ai_socktype = SOCK_STREAM}, *res;
    if (getaddrinfo(host, NULL, &hints, &res) != 0) return -1;
    out->sin_addr = ((struct sockaddr_in *)res->ai_addr)->sin_addr;
    freeaddrinfo(res);
    return 0;
}

int server_run(const server_cfg_t *cfg) {
    server_t *s = xcalloc(1, sizeof *s);
    s->cfg = *cfg;
    g_log_node = cfg->id;
    s->start_ms = s->now = now_ms();
    buf_init(&s->tmp);
    buf_init(&s->enc);
    cmd_init(&s->dcmd);
    s->kv = kv_new();

    mkdir(cfg->dir, 0755);

    /* ---- recover: snapshot, then WAL ---- */
    uint64_t snap_idx = 0, snap_term = 0;
    uint8_t *snap = NULL;
    size_t snap_len = 0;
    int sr = snap_load(cfg->dir, &snap_idx, &snap_term, &snap, &snap_len);
    if (sr < 0) { fprintf(stderr, "snapshot file is corrupt\n"); return 1; }
    if (sr == 0) {
        if (kv_load(s->kv, snap, snap_len) != 0) { fprintf(stderr, "snapshot payload is corrupt\n"); return 1; }
        free(snap);
    }
    wal_state_t st;
    s->wal = wal_open(cfg->dir, snap_idx, cfg->do_fsync, &st);
    if (!s->wal) return 1;

    raft_ops_t ops = {s, ops_send, ops_save_state, ops_append, ops_truncate, ops_apply, ops_become, ops_load_snapshot, ops_install_snapshot};
    s->raft = raft_new(cfg->id, cfg->n, &cfg->raft, &ops, s->now, (uint64_t)s->now ^ ((uint64_t)getpid() << 20));
    raft_restore(s->raft, st.term, st.vote, snap_idx, snap_term, st.ents, st.n);
    free(st.ents);
    LOGI("node %d starting: term=%llu voted_for=%d snapshot_index=%llu log=[%llu..%llu] keys=%zu fsync=%s",
         cfg->id, (unsigned long long)st.term, st.vote, (unsigned long long)snap_idx,
         (unsigned long long)snap_idx + 1, (unsigned long long)raft_last_index(s->raft), kv_size(s->kv),
         cfg->do_fsync ? "on" : "OFF");

    /* ---- sockets ---- */
    s->ep = epoll_create1(0);
    const char *bind_host = cfg->bind[0] ? cfg->bind : cfg->host[cfg->id - 1];
    s->lfd_c = listen_on(bind_host, cfg->cport[cfg->id - 1]);
    s->lfd_r = listen_on(bind_host, cfg->rport[cfg->id - 1]);
    if (s->lfd_c < 0 || s->lfd_r < 0) return 1;
    conn_new(s, s->lfd_c, K_LISTEN_C);
    conn_new(s, s->lfd_r, K_LISTEN_R);
    for (int p = 1; p <= cfg->n; p++) {
        if (p == cfg->id) continue;
        if (resolve(cfg->host[p - 1], cfg->rport[p - 1], &s->peers[p].addr) != 0) {
            fprintf(stderr, "cannot resolve %s\n", cfg->host[p - 1]);
            return 1;
        }
        s->peers[p].backoff = 25;
    }

    signal(SIGPIPE, SIG_IGN);
    struct sigaction sa;
    memset(&sa, 0, sizeof sa);
    sa.sa_handler = on_signal;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    LOGI("listening: clients %s:%d, raft %s:%d", bind_host, cfg->cport[cfg->id - 1], bind_host, cfg->rport[cfg->id - 1]);

    struct epoll_event evs[MAX_EVENTS];
    while (!g_stop) {
        int n = epoll_wait(s->ep, evs, MAX_EVENTS, 5);
        s->now = now_ms();
        s->read_batch_seq = 0;
        for (int i = 0; i < n; i++) handle_event(s, &evs[i]);

        raft_tick(s->raft, s->now);
        for (int p = 1; p <= cfg->n; p++)
            if (p != cfg->id && !s->peers[p].out && s->now >= s->peers[p].retry_at) peer_connect(s, p);
        reads_pump(s);

        if (wal_dirty(s->wal)) { /* group commit: one fdatasync covers every append of this iteration */
            wal_sync(s->wal);
            raft_persisted(s->raft);
        }
        reads_pump(s);
        expire_writes(s);
        maybe_snapshot(s);
        flush_all(s);

        for (size_t i = 0; i < s->n_dead; i++) conn_free(s->dead[i]);
        s->n_dead = 0;
    }
    LOGI("shutting down");
    wal_sync(s->wal);
    wal_close(s->wal);
    return 0;
}

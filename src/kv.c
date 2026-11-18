#include "kv.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ------------------------------------------------------------------ */
/* Chained hash table, binary-safe keys and values                    */
/* ------------------------------------------------------------------ */
typedef struct ent {
    struct ent *next;
    uint64_t h;
    uint32_t klen, vlen;
    uint8_t data[]; /* key then value */
} ent_t;

typedef struct {
    ent_t **b;
    size_t nb, n;
} ht_t;

static uint64_t hash_bytes(const void *p, size_t n) {
    const uint8_t *s = p;
    uint64_t h = 1469598103934665603ull;
    for (size_t i = 0; i < n; i++) { h ^= s[i]; h *= 1099511628211ull; }
    h ^= h >> 32;
    h *= 0x9E3779B97F4A7C15ull;
    h ^= h >> 29;
    return h;
}

static void ht_init(ht_t *t) { memset(t, 0, sizeof *t); }
static void ht_clear(ht_t *t) {
    for (size_t i = 0; i < t->nb; i++)
        for (ent_t *e = t->b[i]; e;) { ent_t *n = e->next; free(e); e = n; }
    free(t->b);
    memset(t, 0, sizeof *t);
}
static void ht_grow(ht_t *t) {
    size_t nnb = t->nb ? t->nb * 2 : 64;
    ent_t **nb = xcalloc(nnb, sizeof *nb);
    for (size_t i = 0; i < t->nb; i++)
        for (ent_t *e = t->b[i]; e;) {
            ent_t *n = e->next;
            size_t j = e->h & (nnb - 1);
            e->next = nb[j];
            nb[j] = e;
            e = n;
        }
    free(t->b);
    t->b = nb;
    t->nb = nnb;
}
static ent_t *ht_find(const ht_t *t, const void *k, size_t kl, uint64_t h) {
    if (!t->nb) return NULL;
    for (ent_t *e = t->b[h & (t->nb - 1)]; e; e = e->next)
        if (e->h == h && e->klen == kl && memcmp(e->data, k, kl) == 0) return e;
    return NULL;
}
static void ht_put(ht_t *t, const void *k, size_t kl, const void *v, size_t vl) {
    uint64_t h = hash_bytes(k, kl);
    ent_t *e = ht_find(t, k, kl, h);
    if (e && e->vlen == vl) { memcpy(e->data + kl, v, vl); return; }
    ent_t *ne = xmalloc(sizeof *ne + kl + vl);
    ne->h = h;
    ne->klen = (uint32_t)kl;
    ne->vlen = (uint32_t)vl;
    memcpy(ne->data, k, kl);
    memcpy(ne->data + kl, v, vl);
    if (e) { /* replace in chain */
        ent_t **pp = &t->b[h & (t->nb - 1)];
        while (*pp != e) pp = &(*pp)->next;
        ne->next = e->next;
        *pp = ne;
        free(e);
        return;
    }
    if (t->n >= t->nb) ht_grow(t);
    size_t j = h & (t->nb - 1);
    ne->next = t->b[j];
    t->b[j] = ne;
    t->n++;
}
static int ht_del(ht_t *t, const void *k, size_t kl) {
    if (!t->nb) return 0;
    uint64_t h = hash_bytes(k, kl);
    ent_t **pp = &t->b[h & (t->nb - 1)];
    for (; *pp; pp = &(*pp)->next) {
        ent_t *e = *pp;
        if (e->h == h && e->klen == kl && memcmp(e->data, k, kl) == 0) {
            *pp = e->next;
            free(e);
            t->n--;
            return 1;
        }
    }
    return 0;
}
static void ht_serialize(const ht_t *t, buf_t *out) {
    buf_u64(out, t->n);
    for (size_t i = 0; i < t->nb; i++)
        for (ent_t *e = t->b[i]; e; e = e->next) {
            buf_u32(out, e->klen);
            buf_put(out, e->data, e->klen);
            buf_u32(out, e->vlen);
            buf_put(out, e->data + e->klen, e->vlen);
        }
}
static int ht_load(ht_t *t, rd_t *r) {
    uint64_t n = rd_u64(r);
    for (uint64_t i = 0; i < n && !r->err; i++) {
        uint32_t kl = rd_u32(r);
        const uint8_t *k = rd_bytes(r, kl);
        uint32_t vl = rd_u32(r);
        const uint8_t *v = rd_bytes(r, vl);
        if (r->err) return -1;
        ht_put(t, k, kl, v, vl);
    }
    return r->err ? -1 : 0;
}
static uint64_t ht_digest(const ht_t *t) {
    uint64_t sum = 0;
    for (size_t i = 0; i < t->nb; i++)
        for (ent_t *e = t->b[i]; e; e = e->next) {
            uint64_t hk = hash_bytes(e->data, e->klen);
            uint64_t hv = hash_bytes(e->data + e->klen, e->vlen);
            sum += hash_bytes(&hk, 8) ^ ((hv << 17) | (hv >> 47));
        }
    return sum + t->n;
}

struct kv {
    ht_t main;
    ht_t sess;       /* key: client id (8 bytes LE); value: seq(8) + cached reply */
    resp_cmd_t scratch;
    buf_t tmp;
};

kv_t *kv_new(void) {
    kv_t *kv = xcalloc(1, sizeof *kv);
    ht_init(&kv->main);
    ht_init(&kv->sess);
    cmd_init(&kv->scratch);
    buf_init(&kv->tmp);
    return kv;
}
void kv_free(kv_t *kv) {
    if (!kv) return;
    ht_clear(&kv->main);
    ht_clear(&kv->sess);
    cmd_free(&kv->scratch);
    buf_free(&kv->tmp);
    free(kv);
}
size_t kv_size(const kv_t *kv) { return kv->main.n; }
uint64_t kv_digest(const kv_t *kv) { return ht_digest(&kv->main) ^ (ht_digest(&kv->sess) * 0x9E3779B97F4A7C15ull); }

/* ------------------------------------------------------------------ */
/* Commands                                                           */
/* ------------------------------------------------------------------ */
int kv_cmd_kind(const uint8_t *n, size_t l) {
    switch (l) {
    case 3:
        if (mem_ieq(n, l, "SET") || mem_ieq(n, l, "DEL") || mem_ieq(n, l, "CAS")) return CMD_WRITE;
        if (mem_ieq(n, l, "GET")) return CMD_READ;
        break;
    case 4:
        if (mem_ieq(n, l, "INCR") || mem_ieq(n, l, "DECR")) return CMD_WRITE;
        break;
    case 6:
        if (mem_ieq(n, l, "INCRBY") || mem_ieq(n, l, "DECRBY")) return CMD_WRITE;
        if (mem_ieq(n, l, "EXISTS") || mem_ieq(n, l, "DBSIZE")) return CMD_READ;
        break;
    case 8:
        if (mem_ieq(n, l, "FLUSHALL")) return CMD_WRITE;
        break;
    }
    return CMD_NONE;
}

int kv_strip_idem(int argc, const uint8_t **argv, const size_t *lens, uint64_t *cid, uint64_t *seq) {
    *cid = *seq = 0;
    if (argc >= 4 && mem_ieq(argv[argc - 3], lens[argc - 3], "IDEM")) {
        uint64_t c, s;
        if (parse_u64(argv[argc - 2], lens[argc - 2], &c) == 0 && parse_u64(argv[argc - 1], lens[argc - 1], &s) == 0 &&
            c != 0) {
            *cid = c;
            *seq = s;
            return argc - 3;
        }
    }
    return argc;
}

#define MAX_KEY (1u << 20)
#define MAX_VAL (8u << 20)

const char *kv_check_args(int argc, const uint8_t **argv, const size_t *lens) {
    const uint8_t *n = argv[0];
    size_t l = lens[0];
    (void)argv;
    if (mem_ieq(n, l, "SET") || mem_ieq(n, l, "INCRBY") || mem_ieq(n, l, "DECRBY")) {
        if (argc != 3) return "ERR wrong number of arguments";
        if (lens[1] > MAX_KEY || lens[2] > MAX_VAL) return "ERR key or value too large";
    } else if (mem_ieq(n, l, "GET") || mem_ieq(n, l, "INCR") || mem_ieq(n, l, "DECR")) {
        if (argc != 2) return "ERR wrong number of arguments";
        if (lens[1] > MAX_KEY) return "ERR key too large";
    } else if (mem_ieq(n, l, "CAS")) {
        if (argc != 4) return "ERR wrong number of arguments (CAS key expected new)";
        if (lens[1] > MAX_KEY || lens[2] > MAX_VAL || lens[3] > MAX_VAL) return "ERR key or value too large";
    } else if (mem_ieq(n, l, "DEL") || mem_ieq(n, l, "EXISTS")) {
        if (argc < 2) return "ERR wrong number of arguments";
    } else if (mem_ieq(n, l, "DBSIZE") || mem_ieq(n, l, "FLUSHALL")) {
        if (argc != 1) return "ERR wrong number of arguments";
    }
    return NULL;
}

static void do_incr(kv_t *kv, const uint8_t *k, size_t kl, long long delta, buf_t *out) {
    ent_t *e = ht_find(&kv->main, k, kl, hash_bytes(k, kl));
    long long cur = 0;
    if (e && parse_ll(e->data + e->klen, e->vlen, &cur) != 0) {
        resp_error(out, "ERR value is not an integer or out of range");
        return;
    }
    long long nv;
    if (__builtin_add_overflow(cur, delta, &nv)) {
        resp_error(out, "ERR increment or decrement would overflow");
        return;
    }
    char tmp[32];
    int n = snprintf(tmp, sizeof tmp, "%lld", nv);
    ht_put(&kv->main, k, kl, tmp, (size_t)n);
    resp_int(out, nv);
}

void kv_exec(kv_t *kv, int argc, const uint8_t **argv, const size_t *lens, buf_t *out) {
    if (argc < 1) { resp_error(out, "ERR empty command"); return; }
    const uint8_t *n = argv[0];
    size_t l = lens[0];
    const char *err = kv_check_args(argc, argv, lens);
    if (err) { resp_error(out, err); return; }

    if (mem_ieq(n, l, "GET")) {
        ent_t *e = ht_find(&kv->main, argv[1], lens[1], hash_bytes(argv[1], lens[1]));
        if (e) resp_bulk(out, e->data + e->klen, e->vlen); else resp_null(out);
    } else if (mem_ieq(n, l, "SET")) {
        ht_put(&kv->main, argv[1], lens[1], argv[2], lens[2]);
        resp_simple(out, "OK");
    } else if (mem_ieq(n, l, "DEL")) {
        long long c = 0;
        for (int i = 1; i < argc; i++) c += ht_del(&kv->main, argv[i], lens[i]);
        resp_int(out, c);
    } else if (mem_ieq(n, l, "EXISTS")) {
        long long c = 0;
        for (int i = 1; i < argc; i++) c += ht_find(&kv->main, argv[i], lens[i], hash_bytes(argv[i], lens[i])) != NULL;
        resp_int(out, c);
    } else if (mem_ieq(n, l, "INCR")) {
        do_incr(kv, argv[1], lens[1], 1, out);
    } else if (mem_ieq(n, l, "DECR")) {
        do_incr(kv, argv[1], lens[1], -1, out);
    } else if (mem_ieq(n, l, "INCRBY") || mem_ieq(n, l, "DECRBY")) {
        long long d;
        if (parse_ll(argv[2], lens[2], &d) != 0) { resp_error(out, "ERR value is not an integer or out of range"); return; }
        if (mem_ieq(n, l, "DECRBY")) {
            if (d == (long long)(-9223372036854775807LL - 1)) { resp_error(out, "ERR overflow"); return; }
            d = -d;
        }
        do_incr(kv, argv[1], lens[1], d, out);
    } else if (mem_ieq(n, l, "CAS")) {
        ent_t *e = ht_find(&kv->main, argv[1], lens[1], hash_bytes(argv[1], lens[1]));
        if (e && e->vlen == lens[2] && memcmp(e->data + e->klen, argv[2], lens[2]) == 0) {
            ht_put(&kv->main, argv[1], lens[1], argv[3], lens[3]);
            resp_int(out, 1);
        } else {
            resp_int(out, 0);
        }
    } else if (mem_ieq(n, l, "DBSIZE")) {
        resp_int(out, (long long)kv->main.n);
    } else if (mem_ieq(n, l, "FLUSHALL")) {
        ht_clear(&kv->main);
        resp_simple(out, "OK");
    } else {
        resp_error(out, "ERR unknown command");
    }
}

/* Replicated-entry application with exactly-once semantics. */
void kv_apply(kv_t *kv, const uint8_t *entry, size_t len, buf_t *out) {
    resp_cmd_t *c = &kv->scratch;
    if (cmd_decode(entry, len, c) != 0 || c->argc < 1) {
        resp_error(out, "ERR corrupt log entry");
        return;
    }
    uint64_t cid, seq;
    int argc = kv_strip_idem(c->argc, c->argv, c->lens, &cid, &seq);
    if (cid == 0) {
        kv_exec(kv, argc, c->argv, c->lens, out);
        return;
    }
    uint8_t key[8];
    for (int i = 0; i < 8; i++) key[i] = (uint8_t)(cid >> (8 * i));
    ent_t *s = ht_find(&kv->sess, key, 8, hash_bytes(key, 8));
    if (s) {
        rd_t r;
        rd_init(&r, s->data + 8, s->vlen);
        uint64_t last = rd_u64(&r);
        if (seq == last) { /* duplicate: replay the original answer */
            buf_put(out, s->data + 8 + 8, s->vlen - 8);
            return;
        }
        if (seq < last) {
            resp_error(out, "ERR stale idempotency sequence");
            return;
        }
    }
    buf_t *t = &kv->tmp;
    buf_clear(t);
    buf_u64(t, seq);
    size_t hdr = buf_len(t);
    kv_exec(kv, argc, c->argv, c->lens, t);
    ht_put(&kv->sess, key, 8, buf_data(t), buf_len(t));
    buf_put(out, buf_data(t) + hdr, buf_len(t) - hdr);
}

void kv_serialize(kv_t *kv, buf_t *out) {
    ht_serialize(&kv->main, out);
    ht_serialize(&kv->sess, out);
}

int kv_load(kv_t *kv, const uint8_t *data, size_t len) {
    ht_clear(&kv->main);
    ht_clear(&kv->sess);
    rd_t r;
    rd_init(&r, data, len);
    if (ht_load(&kv->main, &r) != 0) return -1;
    if (ht_load(&kv->sess, &r) != 0) return -1;
    return 0;
}

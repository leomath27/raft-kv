/* kv.h - the replicated state machine: an in-memory hash table plus a client
 * session table that makes retried commands exactly-once. */
#ifndef KV_H
#define KV_H

#include "resp.h"
#include "util.h"

typedef struct kv kv_t;

enum { CMD_NONE = 0, CMD_READ = 1, CMD_WRITE = 2 };

kv_t *kv_new(void);
void kv_free(kv_t *kv);

int kv_cmd_kind(const uint8_t *name, size_t len);

/* Strip a trailing "IDEM <client-id> <seq>" triple. Returns the new argc
 * (unchanged if the triple is absent or malformed). */
int kv_strip_idem(int argc, const uint8_t **argv, const size_t *lens, uint64_t *cid, uint64_t *seq);

/* Arity/shape validation, done on the leader before anything is replicated.
 * argc must already exclude the IDEM triple. NULL if OK, else "ERR ..." text. */
const char *kv_check_args(int argc, const uint8_t **argv, const size_t *lens);

/* Execute a command directly (no session handling). Used for reads. */
void kv_exec(kv_t *kv, int argc, const uint8_t **argv, const size_t *lens, buf_t *out);

/* Apply a replicated log entry (encoded with cmd_encode), honoring IDEM. */
void kv_apply(kv_t *kv, const uint8_t *entry, size_t len, buf_t *out);

void kv_serialize(kv_t *kv, buf_t *out);
int kv_load(kv_t *kv, const uint8_t *data, size_t len); /* 0 ok */
uint64_t kv_digest(const kv_t *kv);
size_t kv_size(const kv_t *kv);

#endif

/* wal.h - write-ahead log for Raft hard state + entries, and snapshot files.
 *
 * Records are [u32 len][u32 crc32(payload)][payload]; a torn or corrupt tail
 * left by a crash is detected by the CRC and truncated on recovery. Writes are
 * buffered by wal_state/append/truncate and made durable together by one
 * wal_sync() (group commit). */
#ifndef WAL_H
#define WAL_H

#include "raft.h"
#include "util.h"

typedef struct wal wal_t;

typedef struct {
    uint64_t term;
    int32_t vote;
    raft_entry_t *ents; /* heap array; entry data owned by caller afterwards */
    size_t n;
} wal_state_t;

/* Opens (creating if needed) dir/raft.wal, takes an exclusive lock, replays it.
 * Entries at or below snap_index are skipped. Returns NULL on error. */
wal_t *wal_open(const char *dir, uint64_t snap_index, int do_fsync, wal_state_t *st);
void wal_close(wal_t *w);

void wal_state(wal_t *w, uint64_t term, int32_t vote);
void wal_append(wal_t *w, uint64_t index, const raft_entry_t *e);
void wal_truncate(wal_t *w, uint64_t from_index);
int wal_dirty(const wal_t *w);
void wal_sync(wal_t *w); /* aborts the process on I/O failure: unsafe to continue */

/* Atomically replace the log with hard state + the given entries (first_index
 * is the index of ents[0]). Discards any buffered-but-unsynced records: the
 * caller passes the authoritative in-memory state. */
void wal_rewrite(wal_t *w, uint64_t term, int32_t vote, uint64_t first_index, const raft_entry_t *ents, size_t n);

uint64_t wal_bytes_written(const wal_t *w);

int snap_save(const char *dir, uint64_t index, uint64_t term, const uint8_t *data, size_t len, int do_fsync);
/* 0 ok (data malloc'd), 1 no snapshot, -1 corrupt */
int snap_load(const char *dir, uint64_t *index, uint64_t *term, uint8_t **data, size_t *len);

#endif

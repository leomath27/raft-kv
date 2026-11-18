#include "wal.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

enum { REC_STATE = 1, REC_ENTRY = 2, REC_TRUNC = 3 };

struct wal {
    char dir[512];
    int fd;
    int do_fsync;
    buf_t pending;
    uint64_t bytes;
};

static void die(const char *what) {
    fprintf(stderr, "FATAL: %s: %s\n", what, strerror(errno));
    abort();
}

static void fsync_dir(const char *dir) {
    int fd = open(dir, O_RDONLY | O_DIRECTORY);
    if (fd >= 0) { fsync(fd); close(fd); }
}

static void put_record(buf_t *b, const uint8_t *payload, size_t len) {
    buf_u32(b, (uint32_t)len);
    buf_u32(b, crc32_buf(payload, len));
    buf_put(b, payload, len);
}

static void enc_state(buf_t *b, uint64_t term, int32_t vote) {
    buf_t p;
    buf_init(&p);
    buf_u8(&p, REC_STATE);
    buf_u64(&p, term);
    buf_u32(&p, (uint32_t)vote);
    put_record(b, buf_data(&p), buf_len(&p));
    buf_free(&p);
}
static void enc_entry(buf_t *b, uint64_t index, const raft_entry_t *e) {
    buf_t p;
    buf_init(&p);
    buf_u8(&p, REC_ENTRY);
    buf_u64(&p, index);
    buf_u64(&p, e->term);
    buf_u8(&p, e->type);
    buf_u32(&p, e->len);
    buf_put(&p, e->data, e->len);
    put_record(b, buf_data(&p), buf_len(&p));
    buf_free(&p);
}

static void write_all(int fd, const uint8_t *p, size_t n) {
    while (n) {
        ssize_t w = write(fd, p, n);
        if (w < 0) {
            if (errno == EINTR) continue;
            die("wal write");
        }
        p += w;
        n -= (size_t)w;
    }
}

/* Replays the file image; returns the byte offset of the last good record end. */
static size_t replay(const uint8_t *img, size_t size, uint64_t base, wal_state_t *st) {
    size_t off = 0, good = 0;
    size_t cap = 0;
    st->ents = NULL;
    st->n = 0;
    while (off + 8 <= size) {
        rd_t h;
        rd_init(&h, img + off, 8);
        uint32_t len = rd_u32(&h), crc = rd_u32(&h);
        if (len == 0 || len > (1u << 30) || off + 8 + len > size) break;
        const uint8_t *pl = img + off + 8;
        if (crc32_buf(pl, len) != crc) break;
        rd_t r;
        rd_init(&r, pl, len);
        uint8_t type = rd_u8(&r);
        int ok = 1;
        if (type == REC_STATE) {
            uint64_t term = rd_u64(&r);
            int32_t vote = (int32_t)rd_u32(&r);
            if (!r.err) { st->term = term; st->vote = vote; } else ok = 0;
        } else if (type == REC_ENTRY) {
            uint64_t idx = rd_u64(&r), term = rd_u64(&r);
            uint8_t et = rd_u8(&r);
            uint32_t dl = rd_u32(&r);
            const uint8_t *d = rd_bytes(&r, dl);
            if (r.err) ok = 0;
            else if (idx <= base) { /* covered by the snapshot */ }
            else if (idx == base + st->n + 1) {
                if (st->n == cap) {
                    cap = cap ? cap * 2 : 256;
                    st->ents = xrealloc(st->ents, cap * sizeof *st->ents);
                }
                raft_entry_t *e = &st->ents[st->n++];
                e->term = term;
                e->type = et;
                e->len = dl;
                e->data = xmalloc(dl);
                if (dl) memcpy(e->data, d, dl);
            } else ok = 0; /* gap: stop trusting the file here */
        } else if (type == REC_TRUNC) {
            uint64_t from = rd_u64(&r);
            if (r.err) ok = 0;
            else {
                size_t keep = from <= base + 1 ? 0 : (size_t)(from - base - 1);
                if (keep < st->n) {
                    for (size_t i = keep; i < st->n; i++) free(st->ents[i].data);
                    st->n = keep;
                }
            }
        } else ok = 0;
        if (!ok) break;
        off += 8 + len;
        good = off;
    }
    return good;
}

wal_t *wal_open(const char *dir, uint64_t snap_index, int do_fsync, wal_state_t *st) {
    memset(st, 0, sizeof *st);
    wal_t *w = xcalloc(1, sizeof *w);
    snprintf(w->dir, sizeof w->dir, "%s", dir);
    w->do_fsync = do_fsync;
    buf_init(&w->pending);
    char path[600];
    snprintf(path, sizeof path, "%s/raft.wal", dir);
    w->fd = open(path, O_RDWR | O_CREAT, 0644);
    if (w->fd < 0) { fprintf(stderr, "cannot open %s: %s\n", path, strerror(errno)); free(w); return NULL; }
    if (flock(w->fd, LOCK_EX | LOCK_NB) != 0) {
        fprintf(stderr, "%s is locked by another process\n", path);
        close(w->fd);
        free(w);
        return NULL;
    }
    struct stat sb;
    if (fstat(w->fd, &sb) != 0) die("fstat");
    size_t size = (size_t)sb.st_size;
    uint8_t *img = NULL;
    if (size) {
        img = xmalloc(size);
        size_t got = 0;
        while (got < size) {
            ssize_t r = pread(w->fd, img + got, size - got, (off_t)got);
            if (r <= 0) die("wal read");
            got += (size_t)r;
        }
    }
    size_t good = replay(img, size, snap_index, st);
    free(img);
    if (good < size) {
        fprintf(stderr, "wal: discarding %zu bytes of torn/corrupt tail\n", size - good);
        if (ftruncate(w->fd, (off_t)good) != 0) die("ftruncate");
        if (do_fsync) fsync(w->fd);
    }
    lseek(w->fd, (off_t)good, SEEK_SET);
    w->bytes = good;
    fsync_dir(dir);
    return w;
}

void wal_close(wal_t *w) {
    if (!w) return;
    if (buf_len(&w->pending)) wal_sync(w);
    close(w->fd);
    buf_free(&w->pending);
    free(w);
}

void wal_state(wal_t *w, uint64_t term, int32_t vote) { enc_state(&w->pending, term, vote); }
void wal_append(wal_t *w, uint64_t index, const raft_entry_t *e) { enc_entry(&w->pending, index, e); }
void wal_truncate(wal_t *w, uint64_t from) {
    buf_t p;
    buf_init(&p);
    buf_u8(&p, REC_TRUNC);
    buf_u64(&p, from);
    put_record(&w->pending, buf_data(&p), buf_len(&p));
    buf_free(&p);
}
int wal_dirty(const wal_t *w) { return buf_len(&w->pending) > 0; }
uint64_t wal_bytes_written(const wal_t *w) { return w->bytes; }

void wal_sync(wal_t *w) {
    size_t n = buf_len(&w->pending);
    if (!n) return;
    write_all(w->fd, buf_data(&w->pending), n);
    w->bytes += n;
    buf_clear(&w->pending);
    buf_shrink_if_idle(&w->pending, 8u << 20);
    if (w->do_fsync && fdatasync(w->fd) != 0) die("fdatasync");
}

void wal_rewrite(wal_t *w, uint64_t term, int32_t vote, uint64_t first_index, const raft_entry_t *ents, size_t n) {
    char tmp[600], fin[600];
    snprintf(tmp, sizeof tmp, "%s/raft.wal.tmp", w->dir);
    snprintf(fin, sizeof fin, "%s/raft.wal", w->dir);
    int fd = open(tmp, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) die("wal rewrite open");
    buf_t b;
    buf_init(&b);
    enc_state(&b, term, vote);
    for (size_t i = 0; i < n; i++) {
        enc_entry(&b, first_index + i, &ents[i]);
        if (buf_len(&b) > (4u << 20)) { write_all(fd, buf_data(&b), buf_len(&b)); buf_clear(&b); }
    }
    write_all(fd, buf_data(&b), buf_len(&b));
    uint64_t total = 0;
    (void)total;
    if (w->do_fsync && fsync(fd) != 0) die("fsync");
    if (flock(fd, LOCK_EX | LOCK_NB) != 0) die("flock");
    if (rename(tmp, fin) != 0) die("rename");
    if (w->do_fsync) fsync_dir(w->dir);
    struct stat sb;
    if (fstat(fd, &sb) == 0) w->bytes = (uint64_t)sb.st_size;
    close(w->fd); /* releases the old lock */
    w->fd = fd;
    lseek(fd, 0, SEEK_END);
    buf_clear(&w->pending);
    buf_free(&b);
}

/* ---------------- snapshot files ---------------- */
#define SNAP_MAGIC 0x504E5352u /* "RSNP" */

int snap_save(const char *dir, uint64_t index, uint64_t term, const uint8_t *data, size_t len, int do_fsync) {
    char tmp[600], fin[600];
    snprintf(tmp, sizeof tmp, "%s/snapshot.tmp", dir);
    snprintf(fin, sizeof fin, "%s/snapshot.bin", dir);
    buf_t h;
    buf_init(&h);
    buf_u32(&h, SNAP_MAGIC);
    buf_u32(&h, 1);
    buf_u64(&h, index);
    buf_u64(&h, term);
    buf_u64(&h, len);
    uint32_t crc = crc32_update(crc32_buf(buf_data(&h), buf_len(&h)), data, len);
    int fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) { buf_free(&h); return -1; }
    write_all(fd, buf_data(&h), buf_len(&h));
    write_all(fd, data, len);
    uint8_t c[4] = {crc, crc >> 8, crc >> 16, crc >> 24};
    write_all(fd, c, 4);
    if (do_fsync && fsync(fd) != 0) die("fsync snapshot");
    close(fd);
    buf_free(&h);
    if (rename(tmp, fin) != 0) die("rename snapshot");
    if (do_fsync) fsync_dir(dir);
    return 0;
}

int snap_load(const char *dir, uint64_t *index, uint64_t *term, uint8_t **data, size_t *len) {
    char path[600];
    snprintf(path, sizeof path, "%s/snapshot.bin", dir);
    int fd = open(path, O_RDONLY);
    if (fd < 0) return errno == ENOENT ? 1 : -1;
    struct stat sb;
    if (fstat(fd, &sb) != 0 || sb.st_size < 36) { close(fd); return -1; }
    size_t size = (size_t)sb.st_size;
    uint8_t *img = xmalloc(size);
    size_t got = 0;
    while (got < size) {
        ssize_t r = read(fd, img + got, size - got);
        if (r <= 0) { free(img); close(fd); return -1; }
        got += (size_t)r;
    }
    close(fd);
    rd_t r;
    rd_init(&r, img, size);
    uint32_t magic = rd_u32(&r), ver = rd_u32(&r);
    uint64_t idx = rd_u64(&r), t = rd_u64(&r), dl = rd_u64(&r);
    if (r.err || magic != SNAP_MAGIC || ver != 1 || dl != size - 32 - 4) { free(img); return -1; }
    uint32_t crc = crc32_buf(img, size - 4);
    rd_t cr;
    rd_init(&cr, img + size - 4, 4);
    if (rd_u32(&cr) != crc) { free(img); return -1; }
    *data = xmalloc(dl);
    if (dl) memcpy(*data, img + 32, dl);
    *len = dl;
    *index = idx;
    *term = t;
    free(img);
    return 0;
}

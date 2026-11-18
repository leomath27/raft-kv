/* util.h - small shared helpers: byte buffers, CRC32, logging, RNG. */
#ifndef UTIL_H
#define UTIL_H

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>

uint64_t now_ms(void);
uint32_t crc32_update(uint32_t crc, const void *data, size_t n);
static inline uint32_t crc32_buf(const void *d, size_t n) { return crc32_update(0, d, n); }

void *xmalloc(size_t n);
void *xcalloc(size_t n, size_t sz);
void *xrealloc(void *p, size_t n);
char *xstrdup(const char *s);

/* ---- growable byte buffer with a consumed-prefix offset ---- */
typedef struct {
    uint8_t *p;
    size_t off, len, cap;
} buf_t;

void buf_init(buf_t *b);
void buf_free(buf_t *b);
void buf_reserve(buf_t *b, size_t extra);
void buf_put(buf_t *b, const void *d, size_t n);
void buf_putc(buf_t *b, int c);
void buf_u8(buf_t *b, uint8_t v);
void buf_u32(buf_t *b, uint32_t v);
void buf_u64(buf_t *b, uint64_t v);
void buf_printf(buf_t *b, const char *fmt, ...) __attribute__((format(printf, 2, 3)));
void buf_consume(buf_t *b, size_t n);
void buf_clear(buf_t *b);
void buf_shrink_if_idle(buf_t *b, size_t limit);
static inline uint8_t *buf_data(const buf_t *b) { return b->p + b->off; }
static inline size_t buf_len(const buf_t *b) { return b->len; }

/* ---- bounds-checked little-endian reader ---- */
typedef struct {
    const uint8_t *p;
    size_t len, pos;
    int err;
} rd_t;

void rd_init(rd_t *r, const void *p, size_t len);
uint8_t rd_u8(rd_t *r);
uint32_t rd_u32(rd_t *r);
uint64_t rd_u64(rd_t *r);
const uint8_t *rd_bytes(rd_t *r, size_t n);

/* ---- RNG (xorshift64*) ---- */
uint64_t rng_next(uint64_t *state);

/* ---- logging ---- */
enum { LOG_QUIET = 0, LOG_WARN = 1, LOG_INFO = 2, LOG_DEBUG = 3 };
extern int g_log_level;
extern int g_log_node;
void log_msg(int lvl, const char *fmt, ...) __attribute__((format(printf, 2, 3)));
#define LOGW(...) log_msg(LOG_WARN, __VA_ARGS__)
#define LOGI(...) log_msg(LOG_INFO, __VA_ARGS__)
#define LOGD(...) log_msg(LOG_DEBUG, __VA_ARGS__)

#endif

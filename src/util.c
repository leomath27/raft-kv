#include "util.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

uint64_t now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000ull + (uint64_t)ts.tv_nsec / 1000000ull;
}

static uint32_t crc_table[256];
static int crc_ready;

uint32_t crc32_update(uint32_t crc, const void *data, size_t n) {
    if (!crc_ready) {
        for (uint32_t i = 0; i < 256; i++) {
            uint32_t c = i;
            for (int k = 0; k < 8; k++) c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
            crc_table[i] = c;
        }
        crc_ready = 1;
    }
    const uint8_t *p = data;
    crc = ~crc;
    while (n--) crc = crc_table[(crc ^ *p++) & 0xff] ^ (crc >> 8);
    return ~crc;
}

void *xmalloc(size_t n) {
    void *p = malloc(n ? n : 1);
    if (!p) { fprintf(stderr, "out of memory\n"); abort(); }
    return p;
}
void *xcalloc(size_t n, size_t sz) {
    void *p = calloc(n ? n : 1, sz ? sz : 1);
    if (!p) { fprintf(stderr, "out of memory\n"); abort(); }
    return p;
}
void *xrealloc(void *p, size_t n) {
    p = realloc(p, n ? n : 1);
    if (!p) { fprintf(stderr, "out of memory\n"); abort(); }
    return p;
}
char *xstrdup(const char *s) {
    size_t n = strlen(s) + 1;
    char *d = xmalloc(n);
    memcpy(d, s, n);
    return d;
}

/* ---------------- buf_t ---------------- */
void buf_init(buf_t *b) { memset(b, 0, sizeof *b); }
void buf_free(buf_t *b) { free(b->p); memset(b, 0, sizeof *b); }

void buf_reserve(buf_t *b, size_t extra) {
    if (b->off + b->len + extra <= b->cap) return;
    /* Slide the live bytes to the front only when that reclaims at least as much space
     * as it costs to move (off >= len). Otherwise grow geometrically: compacting a big
     * queue to gain a sliver of slack on every append would be quadratic. */
    if (b->off && b->off >= b->len && b->len + extra <= b->cap) {
        memmove(b->p, b->p + b->off, b->len);
        b->off = 0;
        return;
    }
    size_t ncap = b->cap ? b->cap : 256;
    while (ncap < b->len + extra) ncap *= 2;
    uint8_t *np = xmalloc(ncap);
    if (b->len) memcpy(np, b->p + b->off, b->len);
    free(b->p);
    b->p = np;
    b->off = 0;
    b->cap = ncap;
}
void buf_put(buf_t *b, const void *d, size_t n) {
    if (!n) return;
    buf_reserve(b, n);
    memcpy(b->p + b->off + b->len, d, n);
    b->len += n;
}
void buf_putc(buf_t *b, int c) { uint8_t v = (uint8_t)c; buf_put(b, &v, 1); }
void buf_u8(buf_t *b, uint8_t v) { buf_put(b, &v, 1); }
void buf_u32(buf_t *b, uint32_t v) {
    uint8_t t[4] = {v, v >> 8, v >> 16, v >> 24};
    buf_put(b, t, 4);
}
void buf_u64(buf_t *b, uint64_t v) {
    uint8_t t[8];
    for (int i = 0; i < 8; i++) t[i] = (uint8_t)(v >> (8 * i));
    buf_put(b, t, 8);
}
void buf_printf(buf_t *b, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    va_list ap2;
    va_copy(ap2, ap);
    int n = vsnprintf(NULL, 0, fmt, ap);
    va_end(ap);
    if (n > 0) {
        buf_reserve(b, (size_t)n + 1);
        vsnprintf((char *)(b->p + b->off + b->len), (size_t)n + 1, fmt, ap2);
        b->len += (size_t)n;
    }
    va_end(ap2);
}
void buf_consume(buf_t *b, size_t n) {
    if (n >= b->len) { b->off = 0; b->len = 0; return; }
    b->off += n;
    b->len -= n;
}
void buf_clear(buf_t *b) { b->off = 0; b->len = 0; }
void buf_shrink_if_idle(buf_t *b, size_t limit) {
    if (b->len == 0 && b->cap > limit) buf_free(b);
}

/* ---------------- rd_t ---------------- */
void rd_init(rd_t *r, const void *p, size_t len) { r->p = p; r->len = len; r->pos = 0; r->err = 0; }
const uint8_t *rd_bytes(rd_t *r, size_t n) {
    if (r->err || n > r->len - r->pos) { r->err = 1; return NULL; }
    const uint8_t *p = r->p + r->pos;
    r->pos += n;
    return p;
}
uint8_t rd_u8(rd_t *r) { const uint8_t *p = rd_bytes(r, 1); return p ? p[0] : 0; }
uint32_t rd_u32(rd_t *r) {
    const uint8_t *p = rd_bytes(r, 4);
    return p ? (uint32_t)p[0] | (uint32_t)p[1] << 8 | (uint32_t)p[2] << 16 | (uint32_t)p[3] << 24 : 0;
}
uint64_t rd_u64(rd_t *r) {
    const uint8_t *p = rd_bytes(r, 8);
    uint64_t v = 0;
    if (!p) return 0;
    for (int i = 0; i < 8; i++) v |= (uint64_t)p[i] << (8 * i);
    return v;
}

uint64_t rng_next(uint64_t *s) {
    uint64_t x = *s ? *s : 0x9E3779B97F4A7C15ull;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    *s = x;
    return x * 0x2545F4914F6CDD1Dull;
}

/* ---------------- logging ---------------- */
int g_log_level = LOG_INFO;
int g_log_node = 0;

void log_msg(int lvl, const char *fmt, ...) {
    if (lvl > g_log_level) return;
    static uint64_t t0;
    if (!t0) t0 = now_ms();
    uint64_t t = now_ms() - t0;
    static const char *names[] = {"", "WARN", "INFO", "DEBG"};
    fprintf(stderr, "[%4llu.%03llu n%d %s] ", (unsigned long long)(t / 1000),
            (unsigned long long)(t % 1000), g_log_node, names[lvl]);
    va_list ap;
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
    fputc('\n', stderr);
}

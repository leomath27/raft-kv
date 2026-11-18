#include "resp.h"

#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void cmd_init(resp_cmd_t *c) { memset(c, 0, sizeof *c); }
void cmd_free(resp_cmd_t *c) {
    free(c->argv);
    free(c->lens);
    memset(c, 0, sizeof *c);
}
static void cmd_push(resp_cmd_t *c, const uint8_t *p, size_t n) {
    if (c->argc == c->cap) {
        c->cap = c->cap ? c->cap * 2 : 8;
        c->argv = xrealloc(c->argv, (size_t)c->cap * sizeof *c->argv);
        c->lens = xrealloc(c->lens, (size_t)c->cap * sizeof *c->lens);
    }
    c->argv[c->argc] = p;
    c->lens[c->argc] = n;
    c->argc++;
}

/* find "\r\n" starting at i; returns index of '\r' or -1 */
static ssize_t find_crlf(const uint8_t *b, size_t len, size_t i) {
    for (; i + 1 < len; i++)
        if (b[i] == '\r' && b[i + 1] == '\n') return (ssize_t)i;
    return -1;
}

static int parse_len(const uint8_t *p, size_t n, long long *out) { return parse_ll(p, n, out); }

int resp_parse(const uint8_t *buf, size_t len, resp_cmd_t *out, size_t *consumed) {
    out->argc = 0;
    if (len == 0) return 0;
    if (buf[0] == '*') {
        ssize_t e = find_crlf(buf, len, 1);
        if (e < 0) return len > 64 ? -1 : 0;
        long long n;
        if (parse_len(buf + 1, (size_t)e - 1, &n) != 0 || n < 0 || n > RESP_MAX_ARGS) return -1;
        size_t pos = (size_t)e + 2;
        for (long long i = 0; i < n; i++) {
            if (pos >= len) return 0;
            if (buf[pos] != '$') return -1;
            ssize_t e2 = find_crlf(buf, len, pos + 1);
            if (e2 < 0) return (len - pos) > 64 ? -1 : 0;
            long long bl;
            if (parse_len(buf + pos + 1, (size_t)e2 - pos - 1, &bl) != 0 || bl < 0 || bl > RESP_MAX_BULK) return -1;
            size_t start = (size_t)e2 + 2;
            if (start + (size_t)bl + 2 > len) return 0;
            if (buf[start + (size_t)bl] != '\r' || buf[start + (size_t)bl + 1] != '\n') return -1;
            cmd_push(out, buf + start, (size_t)bl);
            pos = start + (size_t)bl + 2;
        }
        *consumed = pos;
        return 1; /* argc may be 0 for "*0\r\n"; caller skips it */
    }
    /* inline command: split on spaces up to newline */
    const uint8_t *nl = memchr(buf, '\n', len);
    if (!nl) return len > 64 * 1024 ? -1 : 0;
    size_t lend = (size_t)(nl - buf);
    size_t cons = lend + 1;
    if (lend > 0 && buf[lend - 1] == '\r') lend--;
    size_t i = 0;
    while (i < lend) {
        while (i < lend && (buf[i] == ' ' || buf[i] == '\t')) i++;
        size_t s = i;
        while (i < lend && buf[i] != ' ' && buf[i] != '\t') i++;
        if (i > s) {
            if (out->argc >= RESP_MAX_ARGS) return -1;
            cmd_push(out, buf + s, i - s);
        }
    }
    *consumed = cons;
    return 1;
}

void cmd_encode(buf_t *out, int argc, const uint8_t **argv, const size_t *lens) {
    buf_u32(out, (uint32_t)argc);
    for (int i = 0; i < argc; i++) {
        buf_u32(out, (uint32_t)lens[i]);
        buf_put(out, argv[i], lens[i]);
    }
}

int cmd_decode(const uint8_t *data, size_t len, resp_cmd_t *out) {
    rd_t r;
    rd_init(&r, data, len);
    out->argc = 0;
    uint32_t argc = rd_u32(&r);
    if (r.err || argc > RESP_MAX_ARGS) return -1;
    for (uint32_t i = 0; i < argc; i++) {
        uint32_t l = rd_u32(&r);
        const uint8_t *p = rd_bytes(&r, l);
        if (r.err) return -1;
        cmd_push(out, p, l);
    }
    return 0;
}

void resp_simple(buf_t *b, const char *s) { buf_putc(b, '+'); buf_put(b, s, strlen(s)); buf_put(b, "\r\n", 2); }
void resp_error(buf_t *b, const char *s) { buf_putc(b, '-'); buf_put(b, s, strlen(s)); buf_put(b, "\r\n", 2); }
void resp_int(buf_t *b, long long v) { buf_printf(b, ":%lld\r\n", v); }
void resp_bulk(buf_t *b, const void *p, size_t n) {
    buf_printf(b, "$%zu\r\n", n);
    buf_put(b, p, n);
    buf_put(b, "\r\n", 2);
}
void resp_null(buf_t *b) { buf_put(b, "$-1\r\n", 5); }
void resp_array(buf_t *b, long long n) { buf_printf(b, "*%lld\r\n", n); }

int parse_ll(const uint8_t *p, size_t n, long long *out) {
    char tmp[32];
    if (n == 0 || n >= sizeof tmp) return -1;
    memcpy(tmp, p, n);
    tmp[n] = 0;
    if (isspace((unsigned char)tmp[0])) return -1;
    char *end;
    errno = 0;
    long long v = strtoll(tmp, &end, 10);
    if (errno || *end) return -1;
    *out = v;
    return 0;
}
int parse_u64(const uint8_t *p, size_t n, uint64_t *out) {
    char tmp[32];
    if (n == 0 || n >= sizeof tmp || p[0] == '-' || p[0] == '+') return -1;
    memcpy(tmp, p, n);
    tmp[n] = 0;
    if (isspace((unsigned char)tmp[0])) return -1;
    char *end;
    errno = 0;
    unsigned long long v = strtoull(tmp, &end, 10);
    if (errno || *end) return -1;
    *out = v;
    return 0;
}
int mem_ieq(const uint8_t *a, size_t alen, const char *lit) {
    size_t n = strlen(lit);
    if (alen != n) return 0;
    for (size_t i = 0; i < n; i++)
        if (tolower(a[i]) != tolower((unsigned char)lit[i])) return 0;
    return 1;
}

/* resp.h - RESP2 request parser (zero-copy) and reply encoders, plus the
 * command encoding used inside replicated log entries. */
#ifndef RESP_H
#define RESP_H

#include "util.h"

#define RESP_MAX_ARGS 4096
#define RESP_MAX_BULK (16 * 1024 * 1024)

typedef struct {
    const uint8_t **argv;
    size_t *lens;
    int argc, cap;
} resp_cmd_t;

void cmd_init(resp_cmd_t *c);
void cmd_free(resp_cmd_t *c);

/* Parse one request from buf. Returns 1 parsed (argv point INTO buf, valid until
 * the buffer is consumed), 0 need more data, -1 protocol error. */
int resp_parse(const uint8_t *buf, size_t len, resp_cmd_t *out, size_t *consumed);

/* Command <-> bytes, used for log entries and parked reads. */
void cmd_encode(buf_t *out, int argc, const uint8_t **argv, const size_t *lens);
int cmd_decode(const uint8_t *data, size_t len, resp_cmd_t *out); /* 0 ok */

/* Reply encoders. */
void resp_simple(buf_t *b, const char *s);
void resp_error(buf_t *b, const char *s); /* s includes the code, e.g. "ERR bad" */
void resp_int(buf_t *b, long long v);
void resp_bulk(buf_t *b, const void *p, size_t n);
void resp_null(buf_t *b);
void resp_array(buf_t *b, long long n);

int parse_ll(const uint8_t *p, size_t n, long long *out);
int parse_u64(const uint8_t *p, size_t n, uint64_t *out);
int mem_ieq(const uint8_t *a, size_t alen, const char *lit); /* case-insensitive equals */

#endif

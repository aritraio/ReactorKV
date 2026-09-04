#include "kvstore/protocol.h"

/* ---- parser state machine ---- */
enum {
    ST_MULTIBULK = 0, /* expect "*<n>\r\n" */
    ST_ARG_LEN,       /* expect "$<len>\r\n" */
    ST_ARG_DATA,      /* expect <len> payload bytes + "\r\n" */
    ST_DONE,
    ST_ERROR,
};

/* Index of the '\r' in the first CRLF at or after offset 0, or SIZE_MAX. */
static size_t find_crlf(const char *s, size_t len) {
    for (size_t i = 0; i + 1 < len; i++) {
        if (s[i] == '\r' && s[i + 1] == '\n') return i;
    }
    return SIZE_MAX;
}

/* Strict non-negative decimal from [s, s+n). -1 on any malformed input.
   The accumulator is capped far below INT64_MAX so the cast is always safe;
   callers compare against KVC_MAX_ARGS / KVC_MAX_BULK_LEN anyway. */
static int64_t parse_uint_line(const char *s, size_t n) {
    if (n == 0) return -1;
    uint64_t acc = 0;
    for (size_t i = 0; i < n; i++) {
        unsigned char c = (unsigned char)s[i];
        if (c < '0' || c > '9') return -1;
        uint64_t d = (uint64_t)(c - '0');
        if (acc > (UINT64_C(1) << 40)) return -1; /* absurdly large */
        acc = acc * 10 + d;
    }
    return (int64_t)acc;
}

/* Drop the first n bytes of the input buffer (used once a step consumes
   them; the buffer therefore always holds only unparsed bytes). */
static void consume(resp_parser *p, size_t n) {
    memmove(p->buf, p->buf + n, p->len - n);
    p->len -= n;
}

static int step_multibulk(resp_parser *p) {
    size_t i = find_crlf(p->buf, p->len);
    if (i == SIZE_MAX) return 0; /* need more data */
    if (p->buf[0] != '*') return -1; /* only multibulk requests supported */
    int64_t n = parse_uint_line(p->buf + 1, i - 1);
    if (n <= 0 || n > KVC_MAX_ARGS) return -1;
    if (p->argv_cap < n) {
        int cap = p->argv_cap ? p->argv_cap : 8;
        while (cap < n) cap *= 2;
        p->argv = kvc_realloc(p->argv, (size_t)cap * sizeof *p->argv);
        p->argvlen = kvc_realloc(p->argvlen, (size_t)cap * sizeof *p->argvlen);
        p->argv_cap = cap;
    }
    p->argc = 0;
    p->pending_args = n;
    consume(p, i + 2);
    p->state = ST_ARG_LEN;
    return 1;
}

static int step_arg_len(resp_parser *p) {
    size_t i = find_crlf(p->buf, p->len);
    if (i == SIZE_MAX) return 0;
    if (p->buf[0] != '$') return -1; /* only bulk args supported */
    int64_t n = parse_uint_line(p->buf + 1, i - 1);
    if (n < 0 || n > KVC_MAX_BULK_LEN) return -1;
    p->bulk_len = n;
    consume(p, i + 2);
    p->state = ST_ARG_DATA;
    return 1;
}

static int step_arg_data(resp_parser *p) {
    if (p->len < (size_t)p->bulk_len + 2) return 0; /* payload + CRLF pending */
    if (p->buf[p->bulk_len] != '\r' || p->buf[p->bulk_len + 1] != '\n') return -1;
    char *arg = kvc_malloc((size_t)p->bulk_len + 1);
    memcpy(arg, p->buf, (size_t)p->bulk_len);
    arg[p->bulk_len] = '\0';
    p->argv[p->argc] = arg;
    p->argvlen[p->argc] = (size_t)p->bulk_len;
    p->argc++;
    consume(p, (size_t)p->bulk_len + 2);
    p->pending_args--;
    if (p->pending_args == 0) {
        p->state = ST_DONE;
        return 1;
    }
    p->state = ST_ARG_LEN;
    return 1;
}

void resp_parser_init(resp_parser *p) {
    memset(p, 0, sizeof *p);
    p->state = ST_MULTIBULK;
}

void resp_parser_reset(resp_parser *p) {
    for (int i = 0; i < p->argc; i++) free(p->argv[i]);
    p->argc = 0;
    p->pending_args = 0;
    p->bulk_len = 0;
    p->error = false;
    p->state = ST_MULTIBULK;
}

/* Full teardown to a pristine parser: like resp_parser_reset, but also
   discards any unparsed input bytes. Used when a connection is closed and
   its kv_conn recycled — stale bytes from the previous connection must not
   leak into the next one. The buffer allocation itself is kept for reuse. */
void resp_parser_clear(resp_parser *p) {
    resp_parser_reset(p);
    p->len = 0;
}

void resp_parser_destroy(resp_parser *p) {
    resp_parser_clear(p); /* frees argv + discards input */
    free(p->argv);
    free(p->argvlen);
    free(p->buf);
    memset(p, 0, sizeof *p);
}

int resp_parser_feed(resp_parser *p, const char *data, size_t len) {
    if (p->error) return -1;
    if (p->state == ST_DONE) resp_parser_reset(p); /* defensive auto-reset */
    if (p->state == ST_ERROR) return -1;

    if (len > 0) {
        if (p->len > KVC_MAX_QUERYBUF || len > KVC_MAX_QUERYBUF - p->len) {
            p->error = true;
            p->state = ST_ERROR;
            return -1;
        }
        size_t need = p->len + len;
        if (need > p->cap) {
            size_t ncap = p->cap ? p->cap : 256;
            while (ncap < need) ncap *= 2;
            p->buf = kvc_realloc(p->buf, ncap);
            p->cap = ncap;
        }
        memcpy(p->buf + p->len, data, len);
        p->len += len;
    }

    for (;;) {
        int rc;
        switch (p->state) {
        case ST_MULTIBULK: rc = step_multibulk(p); break;
        case ST_ARG_LEN:   rc = step_arg_len(p);   break;
        case ST_ARG_DATA:  rc = step_arg_data(p);  break;
        case ST_DONE:
            if (p->argc > 0) {
                /* Lowercase the command name for case-insensitive dispatch. */
                for (char *c = p->argv[0]; c != NULL && *c != '\0'; c++) {
                    if (*c >= 'A' && *c <= 'Z') *c = (char)(*c + ('a' - 'A'));
                }
            }
            return 1;
        default:
            return -1;
        }
        if (rc <= 0) {
            if (rc < 0) {
                p->error = true;
                p->state = ST_ERROR;
            }
            return rc;
        }
    }
}

/* ------------------------------------------------------------------ */
/* Reply writer                                                        */
/* ------------------------------------------------------------------ */

static kvc_err reply_reserve(resp_reply *r, size_t extra) {
    if (extra > SIZE_MAX - r->len) return KVC_ERR_NOMEM;
    size_t need = r->len + extra;
    if (need <= r->cap) return KVC_OK;
    size_t ncap = r->cap ? r->cap : 64;
    while (ncap < need) {
        if (ncap > SIZE_MAX / 2) { ncap = need; break; }
        ncap *= 2;
    }
    r->buf = kvc_realloc(r->buf, ncap);
    r->cap = ncap;
    return KVC_OK;
}

static kvc_err reply_append(resp_reply *r, const char *s, size_t n) {
    KVC_RET_ERR(reply_reserve(r, n));
    memcpy(r->buf + r->len, s, n);
    r->len += n;
    return KVC_OK;
}

void resp_reply_init(resp_reply *r) { memset(r, 0, sizeof *r); }

void resp_reply_destroy(resp_reply *r) {
    free(r->buf);
    r->buf = NULL;
    r->len = r->cap = 0;
}

void resp_reply_clear(resp_reply *r) { r->len = 0; }

kvc_err resp_reply_simple(resp_reply *r, const char *s) {
    size_t sl = strlen(s);
    KVC_RET_ERR(reply_reserve(r, sl + 3));
    r->buf[r->len++] = '+';
    memcpy(r->buf + r->len, s, sl);
    r->len += sl;
    memcpy(r->buf + r->len, "\r\n", 2);
    r->len += 2;
    return KVC_OK;
}

kvc_err resp_reply_error(resp_reply *r, const char *s) {
    size_t sl = strlen(s);
    KVC_RET_ERR(reply_reserve(r, sl + 3));
    r->buf[r->len++] = '-';
    memcpy(r->buf + r->len, s, sl);
    r->len += sl;
    memcpy(r->buf + r->len, "\r\n", 2);
    r->len += 2;
    return KVC_OK;
}

kvc_err resp_reply_integer(resp_reply *r, int64_t v) {
    char tmp[32];
    int n = snprintf(tmp, sizeof tmp, ":%" PRId64 "\r\n", v);
    if (n < 0) return KVC_ERR_IO;
    return reply_append(r, tmp, (size_t)n);
}

kvc_err resp_reply_nil(resp_reply *r) { return reply_append(r, "$-1\r\n", 5); }

kvc_err resp_reply_bulk(resp_reply *r, const char *data, size_t len) {
    if (data == NULL) return resp_reply_nil(r);
    char hdr[40];
    int hn = snprintf(hdr, sizeof hdr, "$%zu\r\n", len);
    if (hn < 0) return KVC_ERR_IO;
    KVC_RET_ERR(reply_reserve(r, (size_t)hn + len + 2));
    memcpy(r->buf + r->len, hdr, (size_t)hn);
    r->len += (size_t)hn;
    memcpy(r->buf + r->len, data, len);
    r->len += len;
    memcpy(r->buf + r->len, "\r\n", 2);
    r->len += 2;
    return KVC_OK;
}

kvc_err resp_reply_mbulk_begin(resp_reply *r, int64_t n) {
    char tmp[32];
    int k = snprintf(tmp, sizeof tmp, "*%" PRId64 "\r\n", n);
    if (k < 0) return KVC_ERR_IO;
    return reply_append(r, tmp, (size_t)k);
}

kvc_err resp_reply_mbulk_bulk(resp_reply *r, const char *data, size_t len) {
    return resp_reply_bulk(r, data, len); /* data == NULL writes nil */
}
#ifndef KVC_PROTOCOL_H
#define KVC_PROTOCOL_H

/*
 * protocol.h — incremental RESP request parser + reply writer.
 *
 * The parser is a byte-driven state machine: feed it whatever the socket
 * delivers and it either completes a request (return 1), needs more bytes
 * (return 0), or rejects the stream (return -1). It never assumes a whole
 * request arrived in one read(), which is exactly what the non-blocking
 * event loop in Phase 2 requires — this parser slots in unchanged.
 *
 * Supported wire format (subset of RESP):
 *   Request: *<argc>\r\n ( $<len>\r\n <bytes>\r\n )*<argc>
 *   Reply:   +<simple>\r\n | -<error>\r\n | :<int>\r\n |
 *            $<len>\r\n<bytes>\r\n | $-1\r\n (nil) | *<n>\r\n ...
 */

#include "common.h"

/* Defensive protocol limits (configurable later). */
#define KVC_MAX_ARGS      1024
#define KVC_MAX_BULK_LEN  (64 * 1024 * 1024) /* 64 MiB per bulk string */
#define KVC_MAX_QUERYBUF  (KVC_MAX_BULK_LEN + 4096)

/* ------------------------------------------------------------------ */
/* Request parser                                                      */
/* ------------------------------------------------------------------ */

typedef struct resp_parser {
    char    *buf;      /* unparsed input bytes (compacted after each request) */
    size_t   len;
    size_t   cap;

    char   **argv;     /* parsed args; NUL-terminated (binary-safe via argvlen) */
    size_t  *argvlen;
    int      argc;
    int      argv_cap; /* allocated slots in argv/argvlen */

    int64_t  pending_args; /* multibulk count still to read */
    int64_t  bulk_len;     /* payload bytes remaining in current bulk */
    int      state;        /* private state machine */
    bool     error;        /* protocol violation seen */
} resp_parser;

void resp_parser_init(resp_parser *p);
void resp_parser_destroy(resp_parser *p);

/* Feed bytes. Returns 1 when a full request is ready (see parser.argc /
   parser.argv / parser.argvlen), 0 when more data is needed, -1 on a
   protocol error (the connection should be closed). May be called with
   data == NULL / len == 0 to re-run the state machine against buffered
   bytes — that is how pipelined requests already buffered are drained
   after resp_parser_reset(). Contract: after a 1, the caller must reset
   the parser before feeding again. */
int resp_parser_feed(resp_parser *p, const char *data, size_t len);

/* Forget the current request. argv strings are freed; the argv array and
   input buffer (including any unparsed tail) are kept — this is what
   allows pipelined drain via feed(NULL, 0). Call after dispatching. */
void resp_parser_reset(resp_parser *p);

/* Full reset to a pristine state: also discards buffered input bytes.
   Call when a connection closes and its parser may be reused by another
   connection (stale bytes must not leak across connections). */
void resp_parser_clear(resp_parser *p);

/* ------------------------------------------------------------------ */
/* Reply writer                                                        */
/* ------------------------------------------------------------------ */

typedef struct resp_reply {
    char  *buf;
    size_t len;
    size_t cap;
} resp_reply;

void resp_reply_init(resp_reply *r);
void resp_reply_destroy(resp_reply *r);
void resp_reply_clear(resp_reply *r); /* reuse buffer for next reply */

kvc_err resp_reply_simple(resp_reply *r, const char *s);
kvc_err resp_reply_error(resp_reply *r, const char *s);
kvc_err resp_reply_integer(resp_reply *r, int64_t v);
/* Bulk string; data == NULL writes the RESP nil ($-1) form. */
kvc_err resp_reply_bulk(resp_reply *r, const char *data, size_t len);
/* RESP nil reply ($-1\r\n). */
kvc_err resp_reply_nil(resp_reply *r);
kvc_err resp_reply_mbulk_begin(resp_reply *r, int64_t n);
/* One element of a multi-bulk reply; data == NULL writes nil. */
kvc_err resp_reply_mbulk_bulk(resp_reply *r, const char *data, size_t len);

#endif /* KVC_PROTOCOL_H */
/* Unit tests for the RESP parser, reply writer, and command dispatch.
   Exercises the store end-to-end through kv_dispatch. */

#include "kvstore/commands.h"
#include "kvstore/protocol.h"
#include "kvstore/store.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;
static int checks = 0;

#define CHECK(cond)                                                        \
    do {                                                                   \
        checks++;                                                          \
        if (!(cond)) {                                                     \
            failures++;                                                    \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond); \
        }                                                                  \
    } while (0)

/* ---- parser tests ---- */

static void test_parse_full(void) {
    resp_parser p;
    resp_parser_init(&p);
    const char *wire = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    CHECK(resp_parser_feed(&p, wire, strlen(wire)) == 1);
    CHECK(p.argc == 3);
    CHECK(strcmp(p.argv[0], "set") == 0); /* lowercased */
    CHECK(strcmp(p.argv[1], "foo") == 0);
    CHECK(p.argvlen[1] == 3);
    CHECK(strcmp(p.argv[2], "bar") == 0);
    resp_parser_destroy(&p);
}

static void test_parse_incremental(void) {
    resp_parser p;
    resp_parser_init(&p);
    const char *wire = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    size_t len = strlen(wire);
    for (size_t i = 0; i < len; i++) {
        int rc = resp_parser_feed(&p, wire + i, 1);
        if (i + 1 < len) {
            CHECK(rc == 0); /* need more data */
        } else {
            CHECK(rc == 1);
        }
    }
    CHECK(p.argc == 3);
    CHECK(strcmp(p.argv[2], "bar") == 0);
    resp_parser_destroy(&p);
}

static void test_parse_binary_arg(void) {
    resp_parser p;
    resp_parser_init(&p);
    /* key = 'a', '\0', 'b' (3 bytes) */
    const char wire[] = "*2\r\n$3\r\nGET\r\n$3\r\na\0b\r\n";
    size_t len = sizeof wire - 1;
    CHECK(resp_parser_feed(&p, wire, len) == 1);
    CHECK(p.argc == 2);
    CHECK(p.argvlen[1] == 3);
    CHECK(p.argv[1][0] == 'a' && p.argv[1][1] == '\0' && p.argv[1][2] == 'b');
    resp_parser_destroy(&p);
}

static void test_parse_errors(void) {
    resp_parser p;
    resp_parser_init(&p);
    /* empty multibulk */
    CHECK(resp_parser_feed(&p, "*0\r\n", strlen("*0\r\n")) == -1);
    resp_parser_destroy(&p);

    resp_parser_init(&p);
    /* arg count beyond KVC_MAX_ARGS */
    CHECK(resp_parser_feed(&p, "*99999999\r\n", strlen("*99999999\r\n")) == -1);
    resp_parser_destroy(&p);

    resp_parser_init(&p);
    /* arg line must be a bulk string ('$') */
    CHECK(resp_parser_feed(&p, "*2\r\n$3\r\nGET\r\nX5\r\n",
                           strlen("*2\r\n$3\r\nGET\r\nX5\r\n")) == -1);
    resp_parser_destroy(&p);

    resp_parser_init(&p);
    /* bulk length beyond KVC_MAX_BULK_LEN */
    CHECK(resp_parser_feed(&p, "*2\r\n$3\r\nGET\r\n$999999999999\r\n",
                           strlen("*2\r\n$3\r\nGET\r\n$999999999999\r\n")) == -1);
    resp_parser_destroy(&p);
}

static void test_pipeline(void) {
    resp_parser p;
    resp_parser_init(&p);
    const char *wire = "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n"
                       "*2\r\n$3\r\nGET\r\n$1\r\na\r\n";
    /* First request completes immediately; second is buffered. */
    CHECK(resp_parser_feed(&p, wire, strlen(wire)) == 1);
    CHECK(p.argc == 3 && strcmp(p.argv[0], "set") == 0);
    resp_parser_reset(&p);
    /* Drain the pipelined request from the buffer. */
    CHECK(resp_parser_feed(&p, NULL, 0) == 1);
    CHECK(p.argc == 2 && strcmp(p.argv[0], "get") == 0);
    resp_parser_destroy(&p);
}

/* Regression: a parser recycled across connections must not carry stale
   buffered bytes into the next connection (Phase 2 conn free list). Feed a
   partial request, simulate conn close with resp_parser_clear, then feed a
   fresh request: it must parse cleanly. */
static void test_parser_clear_on_reuse(void) {
    resp_parser p;
    resp_parser_init(&p);

    /* Connection A: partial command arrives, then the client disconnects
       mid-request (parser holds an incomplete fragment). */
    const char *frag = "*2\r\n$3\r\nGET\r\n$3\r\nfo";
    CHECK(resp_parser_feed(&p, frag, strlen(frag)) == 0);
    CHECK(p.len > 0);
    resp_parser_clear(&p); /* conn close: must drop the fragment */
    CHECK(p.len == 0);

    /* Connection B (same recycled parser): a fresh, complete request. */
    const char *wire = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    CHECK(resp_parser_feed(&p, wire, strlen(wire)) == 1);
    CHECK(p.argc == 3 && strcmp(p.argv[0], "set") == 0);
    CHECK(strcmp(p.argv[1], "foo") == 0 && strcmp(p.argv[2], "bar") == 0);
    resp_parser_destroy(&p);
}

/* ---- reply writer tests ---- */

static void check_reply_bytes(resp_reply *r, const char *want) {
    size_t want_len = strlen(want);
    CHECK(r->len == want_len);
    if (r->len != want_len) {
        fprintf(stderr, "  want %zu bytes, got %zu: [%.*s]\n",
                want_len, r->len, (int)r->len, r->buf);
    } else {
        CHECK(memcmp(r->buf, want, want_len) == 0);
    }
}

static void test_replies(void) {
    resp_reply r;
    resp_reply_init(&r);

    resp_reply_clear(&r);
    resp_reply_simple(&r, "OK");
    check_reply_bytes(&r, "+OK\r\n");

    resp_reply_clear(&r);
    resp_reply_error(&r, "ERR boom");
    check_reply_bytes(&r, "-ERR boom\r\n");

    resp_reply_clear(&r);
    resp_reply_integer(&r, 42);
    check_reply_bytes(&r, ":42\r\n");

    resp_reply_clear(&r);
    resp_reply_integer(&r, -7);
    check_reply_bytes(&r, ":-7\r\n");

    resp_reply_clear(&r);
    resp_reply_bulk(&r, "hi", 2);
    check_reply_bytes(&r, "$2\r\nhi\r\n");

    resp_reply_clear(&r);
    resp_reply_bulk(&r, NULL, 0);
    check_reply_bytes(&r, "$-1\r\n");

    resp_reply_clear(&r);
    resp_reply_mbulk_begin(&r, 2);
    resp_reply_mbulk_bulk(&r, "a", 1);
    resp_reply_mbulk_bulk(&r, NULL, 0);
    check_reply_bytes(&r, "*2\r\n$1\r\na\r\n$-1\r\n");

    resp_reply_destroy(&r);
}

/* ---- dispatch tests (end-to-end through the store) ---- */

static void run(kv_store *s, resp_reply *r, const char *wire) {
    resp_parser p;
    resp_parser_init(&p);
    int rc = resp_parser_feed(&p, wire, strlen(wire));
    CHECK(rc == 1);
    resp_reply_clear(r);
    (void)kv_dispatch(s, p.argc, p.argv, p.argvlen, r);
    resp_parser_destroy(&p);
}

static void test_dispatch(void) {
    kv_store store;
    kv_store_init(&store, 4);
    resp_reply r;
    resp_reply_init(&r);

    run(&store, &r, "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
    check_reply_bytes(&r, "+OK\r\n");

    run(&store, &r, "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
    check_reply_bytes(&r, "$3\r\nbar\r\n");

    run(&store, &r, "*2\r\n$3\r\nGET\r\n$6\r\nnope42\r\n");
    check_reply_bytes(&r, "$-1\r\n");

    run(&store, &r, "*1\r\n$4\r\nPING\r\n");
    check_reply_bytes(&r, "+PONG\r\n");

    /* INCR: 1, then 2 */
    run(&store, &r, "*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n");
    check_reply_bytes(&r, ":1\r\n");
    run(&store, &r, "*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n");
    check_reply_bytes(&r, ":2\r\n");

    /* INCR on a non-integer value */
    run(&store, &r, "*3\r\n$3\r\nSET\r\n$4\r\nword\r\n$3\r\nabc\r\n");
    check_reply_bytes(&r, "+OK\r\n");
    run(&store, &r, "*2\r\n$4\r\nINCR\r\n$4\r\nword\r\n");
    check_reply_bytes(&r, "-ERR value is not an integer or out of range\r\n");

    /* DEL counts removed keys */
    run(&store, &r, "*3\r\n$3\r\nDEL\r\n$3\r\nfoo\r\n$4\r\nword\r\n");
    check_reply_bytes(&r, ":2\r\n");
    run(&store, &r, "*2\r\n$3\r\nDEL\r\n$3\r\nfoo\r\n");
    check_reply_bytes(&r, ":0\r\n");

    /* MGET with hits and misses */
    run(&store, &r, "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n");
    check_reply_bytes(&r, "+OK\r\n");
    run(&store, &r, "*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n");
    check_reply_bytes(&r, "+OK\r\n");
    run(&store, &r, "*4\r\n$4\r\nMGET\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n");
    check_reply_bytes(&r, "*3\r\n$1\r\n1\r\n$1\r\n2\r\n$-1\r\n");

    /* EXPIRE: miss -> 0, hit -> 1, then key still readable */
    run(&store, &r, "*3\r\n$6\r\nEXPIRE\r\n$1\r\nx\r\n$2\r\n10\r\n");
    check_reply_bytes(&r, ":0\r\n");
    run(&store, &r, "*3\r\n$3\r\nSET\r\n$1\r\ne\r\n$1\r\n1\r\n");
    check_reply_bytes(&r, "+OK\r\n");
    run(&store, &r, "*3\r\n$6\r\nEXPIRE\r\n$1\r\ne\r\n$2\r\n10\r\n");
    check_reply_bytes(&r, ":1\r\n");
    run(&store, &r, "*2\r\n$3\r\nGET\r\n$1\r\ne\r\n");
    check_reply_bytes(&r, "$1\r\n1\r\n");

    /* EXPIRE with non-positive TTL deletes the key (Redis semantics) */
    run(&store, &r, "*3\r\n$6\r\nEXPIRE\r\n$1\r\ne\r\n$1\r\n0\r\n");
    check_reply_bytes(&r, ":1\r\n");
    run(&store, &r, "*2\r\n$3\r\nGET\r\n$1\r\ne\r\n");
    check_reply_bytes(&r, "$-1\r\n");

    /* Unknown command + arity errors */
    run(&store, &r, "*1\r\n$5\r\nBOGUS\r\n");
    check_reply_bytes(&r, "-ERR unknown command\r\n");
    run(&store, &r, "*1\r\n$3\r\nGET\r\n");
    check_reply_bytes(&r, "-ERR wrong number of arguments for 'get' command\r\n");

    /* Case-insensitive command name (set z 9 = 3 elements) */
    run(&store, &r, "*3\r\n$3\r\nset\r\n$1\r\nz\r\n$1\r\n9\r\n");
    check_reply_bytes(&r, "+OK\r\n");

    resp_reply_destroy(&r);
    kv_store_destroy(&store);
}

/* Phase 3: an entry whose key+value footprint exceeds the slab allocator's
   1 MiB cap is refused with an error reply rather than silently dropped. */
static void test_dispatch_oversized_value(void) {
    kv_store store;
    kv_store_init(&store, 4);
    resp_reply r;
    resp_reply_init(&r);

    const size_t vlen = 2u * 1024u * 1024u; /* well past the 1 MiB cap */
    const char *prefix = "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$";
    const char *suffix = "\r\n";
    char lenbuf[32];
    int ln = snprintf(lenbuf, sizeof lenbuf, "%zu\r\n", vlen);
    size_t wire_len = strlen(prefix) + (size_t)ln + vlen + strlen(suffix);
    char *wire = malloc(wire_len);
    CHECK(wire != NULL);
    char *p = wire;
    memcpy(p, prefix, strlen(prefix));
    p += strlen(prefix);
    memcpy(p, lenbuf, (size_t)ln);
    p += (size_t)ln;
    memset(p, 'x', vlen);
    p += vlen;
    memcpy(p, suffix, strlen(suffix));

    resp_parser prs;
    resp_parser_init(&prs);
    CHECK(resp_parser_feed(&prs, wire, wire_len) == 1);
    CHECK(prs.argc == 3);
    resp_reply_clear(&r);
    (void)kv_dispatch(&store, prs.argc, prs.argv, prs.argvlen, &r);
    check_reply_bytes(&r, "-ERR out of memory\r\n");
    resp_parser_destroy(&prs);
    free(wire);

    resp_reply_destroy(&r);
    kv_store_destroy(&store);
}

int main(void) {
    test_parse_full();
    test_parse_incremental();
    test_parse_binary_arg();
    test_parse_errors();
    test_pipeline();
    test_parser_clear_on_reuse();
    test_replies();
    test_dispatch();
    test_dispatch_oversized_value();
    printf("test_protocol: %d checks, %d failures\n", checks, failures);
    return failures ? 1 : 0;
}
#include "kvstore/commands.h"

#include <string.h>

typedef struct command {
    const char *name;
    int arity;  /* > 0: exact argc; < 0: minimum argc = -arity */
    bool write; /* true: mutates the store (write lock); false: read lock */
    kvc_err (*handler)(kv_store *s, int argc, char **argv,
                       const size_t *argvlen, resp_reply *out);
} command;

static kvc_err cmd_set(kv_store *s, int argc, char **argv,
                       const size_t *argvlen, resp_reply *out) {
    (void)argc;
    int created = 0;
    kvc_err rc = kv_store_set(s, argv[1], argvlen[1], argv[2], argvlen[2],
                              &created);
    if (rc == KVC_ERR_NOMEM) {
        /* Phase 3: an entry footprint past the slab's 1 MiB cap. */
        return resp_reply_error(out, "ERR out of memory");
    }
    KVC_RET_ERR(rc);
    return resp_reply_simple(out, "OK");
}

static kvc_err cmd_get(kv_store *s, int argc, char **argv,
                       const size_t *argvlen, resp_reply *out) {
    (void)argc;
    const char *val = NULL;
    size_t val_len = 0;
    kvc_err rc = kv_store_get(s, argv[1], argvlen[1], &val, &val_len);
    if (rc == KVC_ERR_NOTFOUND) return resp_reply_nil(out);
    KVC_RET_ERR(rc);
    return resp_reply_bulk(out, val, val_len);
}

static kvc_err cmd_del(kv_store *s, int argc, char **argv,
                       const size_t *argvlen, resp_reply *out) {
    int deleted = 0;
    KVC_RET_ERR(kv_store_del(s, (const char *const *)(argv + 1), argvlen + 1,
                             argc - 1, &deleted));
    return resp_reply_integer(out, deleted);
}

static kvc_err cmd_expire(kv_store *s, int argc, char **argv,
                          const size_t *argvlen, resp_reply *out) {
    (void)argc;
    int64_t secs = 0;
    if (kvc_parse_int64(argv[2], argvlen[2], &secs) != KVC_OK) {
        return resp_reply_error(out, "ERR invalid expire time in 'expire' command");
    }
    if (secs > 0 && secs > (INT64_MAX - kvc_now_ms()) / 1000) {
        return resp_reply_error(out, "ERR invalid expire time in 'expire' command");
    }
    int64_t ttl_ms = secs * 1000;
    int rc = kv_store_expire(s, argv[1], argvlen[1], ttl_ms);
    return resp_reply_integer(out, rc);
}

static kvc_err cmd_incr(kv_store *s, int argc, char **argv,
                        const size_t *argvlen, resp_reply *out) {
    (void)argc;
    int64_t v = 0;
    kvc_err rc = kv_store_incr(s, argv[1], argvlen[1], &v);
    if (rc == KVC_ERR_INVAL) {
        return resp_reply_error(out, "ERR value is not an integer or out of range");
    }
    if (rc == KVC_ERR_NOMEM) {
        return resp_reply_error(out, "ERR out of memory");
    }
    KVC_RET_ERR(rc);
    return resp_reply_integer(out, v);
}

static kvc_err cmd_mget(kv_store *s, int argc, char **argv,
                        const size_t *argvlen, resp_reply *out) {
    KVC_RET_ERR(resp_reply_mbulk_begin(out, argc - 1));
    for (int i = 1; i < argc; i++) {
        const char *val = NULL;
        size_t val_len = 0;
        kvc_err rc = kv_store_get(s, argv[i], argvlen[i], &val, &val_len);
        if (rc == KVC_OK) {
            KVC_RET_ERR(resp_reply_mbulk_bulk(out, val, val_len));
        } else if (rc == KVC_ERR_NOTFOUND) {
            KVC_RET_ERR(resp_reply_mbulk_bulk(out, NULL, 0));
        } else {
            return rc;
        }
    }
    return KVC_OK;
}

static kvc_err cmd_ping(kv_store *s, int argc, char **argv,
                        const size_t *argvlen, resp_reply *out) {
    (void)s; (void)argc; (void)argv; (void)argvlen;
    return resp_reply_simple(out, "PONG");
}

/* INFO — snapshot of the store stats (Phase 4 instrumentation). Returns a
   bulk string of key:value lines, redis-cli INFO-compatible enough. */
static kvc_err cmd_info(kv_store *s, int argc, char **argv,
                        const size_t *argvlen, resp_reply *out) {
    (void)argc; (void)argv; (void)argvlen;
    kv_stats st;
    kv_store_stats(s, &st);
    char buf[512];
    int n = snprintf(buf, sizeof buf,
                     "# Server\r\n"
                     "kvstore_version:0.4.0-phase4\r\n"
                     "# Stats\r\n"
                     "keys:%zu\r\n"
                     "hits:%" PRIu64 "\r\n"
                     "misses:%" PRIu64 "\r\n"
                     "evictions:%" PRIu64 "\r\n"
                     "used_memory:%zu\r\n"
                     "maxmemory:%zu\r\n",
                     st.keys, st.hits, st.misses, st.evictions,
                     st.used_bytes, st.maxmemory);
    if (n < 0) return KVC_ERR_IO;
    return resp_reply_bulk(out, buf, (size_t)n);
}

/* PING is outside the requested command subset but costs nothing and makes
   the server compatible with redis-cli health checks. INFO exposes the
   Phase 4 stats (keys/hits/misses/evictions) for monitoring. */
static const command commands[] = {
    { "set",    3,  true,  cmd_set },
    { "get",    2,  false, cmd_get },
    { "del",   -2,  true,  cmd_del },
    { "expire", 3,  true,  cmd_expire },
    { "incr",   2,  true,  cmd_incr },
    { "mget",  -2,  false, cmd_mget },
    { "ping",   1,  false, cmd_ping },
    { "info",   1,  false, cmd_info },
};

kvc_err kv_dispatch(kv_store *s, int argc, char **argv,
                    const size_t *argvlen, resp_reply *out) {
    if (argc < 1) return KVC_ERR_INVAL;

    const command *cmd = NULL;
    for (size_t i = 0; i < KVC_ARRAY_LEN(commands); i++) {
        if (strcmp(commands[i].name, argv[0]) == 0) {
            cmd = &commands[i];
            break;
        }
    }
    if (cmd == NULL) return resp_reply_error(out, "ERR unknown command");

    int want = cmd->arity > 0 ? cmd->arity : -cmd->arity;
    bool ok = cmd->arity > 0 ? (argc == want) : (argc >= want);
    if (!ok) {
        char msg[128];
        snprintf(msg, sizeof msg,
                 "ERR wrong number of arguments for '%s' command", cmd->name);
        return resp_reply_error(out, msg);
    }

    /* Phase 4: hold the store lock for the whole handler so borrowed
       pointers (GET value buffers) stay valid while the reply is built.
       Read-only commands take the read lock; mutators take the write
       lock, which also serializes them against the expiry worker. */
    if (cmd->write) {
        kv_store_wrlock(s);
    } else {
        kv_store_rdlock(s);
    }
    kvc_err rc = cmd->handler(s, argc, argv, argvlen, out);
    kv_store_unlock(s);
    return rc;
}
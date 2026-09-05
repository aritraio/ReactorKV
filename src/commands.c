#include "kvstore/commands.h"

#include "kvstore/wal.h"

#include <string.h>

typedef struct command {
    const char *name;
    int arity;  /* > 0: exact argc; < 0: minimum argc = -arity */
    bool write; /* true: mutates the store (write lock); false: read lock */
    kvc_err (*handler)(kv_store *s, int argc, char **argv,
                       const size_t *argvlen, resp_reply *out);
} command;

/* Phase 5: append a *successfully applied* mutation to the write-ahead
   log. Handlers call this after the store accepted the write; removals
   the store performs itself (expiry purges, eviction) are logged by the
   store. No-op when no WAL is attached or while startup replay is running
   (records are only appended when serving). On a log write failure the
   reply is set to an error and false is returned — the dataset is ahead
   of the log at that point, which is reported loudly. */
static bool wal_append_cmd(kv_store *s, int argc, char **argv,
                           const size_t *argvlen, resp_reply *out) {
    if (s->wal == NULL || s->loading) return true;
    if (wal_append(s->wal, argc, argv, argvlen) != KVC_OK) {
        (void)resp_reply_error(out, "ERR failed to write WAL (I/O error)");
        return false;
    }
    return true;
}

static kvc_err cmd_set(kv_store *s, int argc, char **argv,
                       const size_t *argvlen, resp_reply *out) {
    (void)argc;
    int created = 0;
    kvc_err rc = kv_store_set(s, argv[1], argvlen[1], argv[2], argvlen[2],
                              &created);
    if (rc == KVC_ERR_NOMEM) {
        /* Phase 3: an entry footprint past the slab's 1 MiB cap, or past
           the maxmemory budget. */
        return resp_reply_error(out, "ERR out of memory");
    }
    KVC_RET_ERR(rc);
    /* SET always changes state (at minimum it clears any prior TTL). */
    if (!wal_append_cmd(s, argc, argv, argvlen, out)) return KVC_OK;
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
    /* The store logs a DEL record per key it actually removed (live or
       expired) — command-level logging would double them. */
    return resp_reply_integer(out, deleted);
}

static kvc_err cmd_expire(kv_store *s, int argc, char **argv,
                          const size_t *argvlen, resp_reply *out) {
    (void)argc;
    int64_t secs = 0;
    if (kvc_parse_int64(argv[2], argvlen[2], &secs) != KVC_OK) {
        return resp_reply_error(out, "ERR invalid expire time in 'expire' command");
    }
    int rc;
    if (secs <= 0) {
        /* Non-positive TTL deletes the key (Redis semantics); the store
           purges it and logs the DEL. */
        rc = kv_store_expire(s, argv[1], argvlen[1], -1);
    } else {
        int64_t now = kvc_now_ms();
        if (secs > (INT64_MAX - now) / 1000) {
            return resp_reply_error(out,
                                    "ERR invalid expire time in 'expire' command");
        }
        int64_t abs = now + secs * 1000;
        rc = kv_store_expireat(s, argv[1], argvlen[1], abs);
        if (rc == 1) {
            /* Phase 5: persist the TTL as an *absolute* PEXPIREAT record so
               a restart hours later does not re-base it. */
            char absbuf[24];
            int n = snprintf(absbuf, sizeof absbuf, "%" PRId64, abs);
            if (n < 0 || n >= (int)sizeof absbuf) return KVC_ERR_IO;
            char *targv[3];
            size_t tlens[3];
            targv[0] = (char *)"pexpireat";
            targv[1] = argv[1];
            targv[2] = absbuf;
            tlens[0] = strlen("pexpireat");
            tlens[1] = argvlen[1];
            tlens[2] = (size_t)n;
            if (!wal_append_cmd(s, 3, targv, tlens, out)) return KVC_OK;
        }
    }
    return resp_reply_integer(out, rc);
}

/* PEXPIREAT key <epoch-ms> — absolute expiry (Redis-compatible). Used both
   as a client command and as the persisted form of EXPIRE (see cmd_expire),
   which is how the WAL loader re-applies it through the ordinary dispatch
   table. A timestamp in the past deletes the key, like Redis. */
static kvc_err cmd_pxexpireat(kv_store *s, int argc, char **argv,
                              const size_t *argvlen, resp_reply *out) {
    (void)argc;
    int64_t abs = 0;
    if (kvc_parse_int64(argv[2], argvlen[2], &abs) != KVC_OK) {
        return resp_reply_error(out,
                                "ERR invalid expire time in 'pexpireat' command");
    }
    int rc = kv_store_expireat(s, argv[1], argvlen[1], abs);
    /* Only a *future* absolute time that was actually set produces its own
       record: past timestamps take the purge path, whose DEL the store
       already logged. (During startup replay of a record whose timestamp
       has since passed, kv_store_expireat stores it verbatim and this
       condition is false, so nothing is double-logged.) */
    if (rc == 1 && abs > kvc_now_ms()) {
        if (!wal_append_cmd(s, argc, argv, argvlen, out)) return KVC_OK;
    }
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
    /* INCR always changes state when it succeeds (create or increment).
       If it had to purge an expired key first, the store already logged
       that DEL before this record, so replay reproduces the create. */
    if (!wal_append_cmd(s, argc, argv, argvlen, out)) return KVC_OK;
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

/* INFO — snapshot of the store stats + persistence state. Returns a bulk
   string of key:value lines, redis-cli INFO-compatible enough. */
static kvc_err cmd_info(kv_store *s, int argc, char **argv,
                        const size_t *argvlen, resp_reply *out) {
    (void)argc; (void)argv; (void)argvlen;
    kv_stats st;
    kv_store_stats(s, &st);
    const wal *w = s->wal; /* replayed/truncated/policy are immutable post-boot */
    char buf[1024];
    int n = snprintf(buf, sizeof buf,
                     "# Server\r\n"
                     "kvstore_version:0.5.0-phase5\r\n"
                     "# Stats\r\n"
                     "keys:%zu\r\n"
                     "hits:%" PRIu64 "\r\n"
                     "misses:%" PRIu64 "\r\n"
                     "evictions:%" PRIu64 "\r\n"
                     "used_memory:%zu\r\n"
                     "maxmemory:%zu\r\n"
                     "# Persistence\r\n"
                     "wal_enabled:%d\r\n",
                     st.keys, st.hits, st.misses, st.evictions,
                     st.used_bytes, st.maxmemory, w != NULL ? 1 : 0);
    if (n < 0) return KVC_ERR_IO;
    if (w != NULL) {
        int m = snprintf(buf + (size_t)n, sizeof buf - (size_t)n,
                         "wal_path:%s\r\n"
                         "appendfsync:%s\r\n"
                         "wal_records_replayed:%" PRIu64 "\r\n"
                         "wal_torn_tail_truncated:%d\r\n",
                         w->path, wal_policy_name(w->policy), w->replayed,
                         w->truncated ? 1 : 0);
        if (m < 0) return KVC_ERR_IO;
        n += m;
    }
    return resp_reply_bulk(out, buf, (size_t)n);
}

/* PING is outside the requested command subset but costs nothing and makes
   the server compatible with redis-cli health checks. INFO exposes the
   Phase 4 stats (keys/hits/misses/evictions) and Phase 5 persistence.
   PEXPIREAT is the persisted absolute form of EXPIRE (also client-usable,
   Redis-compatible). */
static const command commands[] = {
    { "set",      3,  true,  cmd_set },
    { "get",      2,  false, cmd_get },
    { "del",     -2,  true,  cmd_del },
    { "expire",   3,  true,  cmd_expire },
    { "pexpireat", 3, true,  cmd_pxexpireat },
    { "incr",     2,  true,  cmd_incr },
    { "mget",    -2,  false, cmd_mget },
    { "ping",     1,  false, cmd_ping },
    { "info",     1,  false, cmd_info },
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

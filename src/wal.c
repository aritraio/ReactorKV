#include "kvstore/wal.h"

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

/*
 * wal.c — Phase 5 write-ahead log (see wal.h for the design).
 *
 * Appends go through one write() per record (records can be large, e.g. a
 * 1 MiB SET value, so a heap staging buffer is not sized per call — the
 * resp_reply scratch embedded in the struct grows once and is reused).
 * The everysec policy writes to the page cache immediately and lets a
 * background thread fsync ~1/s; ALWAYS fsyncs inside wal_append.
 */

#define WAL_FLUSHER_SLICE_MS 100 /* stop-check granularity (10 per 1 s) */

static kvc_err write_all(int fd, const char *buf, size_t len) {
    size_t off = 0;
    while (off < len) {
        ssize_t n = write(fd, buf + off, len - off);
        if (n < 0) {
            if (errno == EINTR) continue;
            kvc_log(KVC_LOG_ERR, "wal: write() failed: %s", strerror(errno));
            return KVC_ERR_IO;
        }
        off += (size_t)n;
    }
    return KVC_OK;
}

kvc_err wal_sync(wal *w) {
    if (w == NULL || w->fd < 0) return KVC_ERR_IO;
    int rc;
    do {
        rc = fsync(w->fd);
    } while (rc != 0 && errno == EINTR);
    if (rc != 0) {
        kvc_log(KVC_LOG_ERR, "wal: fsync(%s) failed: %s", w->path,
                strerror(errno));
        return KVC_ERR_IO;
    }
    return KVC_OK;
}

const char *wal_policy_name(wal_fsync_policy p) {
    switch (p) {
    case WAL_FSYNC_ALWAYS:   return "always";
    case WAL_FSYNC_EVERYSEC: return "everysec";
    case WAL_FSYNC_NO:       return "no";
    }
    return "?";
}

/* ------------------------------------------------------------------ */
/* everysec flusher thread                                            */
/* ------------------------------------------------------------------ */

static void *flusher_main(void *arg) {
    wal *w = (wal *)arg;
    for (;;) {
        for (int i = 0; i < 1000 / WAL_FLUSHER_SLICE_MS &&
                        !atomic_load_explicit(&w->stop, memory_order_relaxed);
             i++) {
            struct timespec ts = { .tv_sec = 0,
                                   .tv_nsec = WAL_FLUSHER_SLICE_MS * 1000000L };
            while (nanosleep(&ts, &ts) != 0 && errno == EINTR) {}
        }
        if (atomic_load_explicit(&w->stop, memory_order_relaxed)) break;
        if (wal_sync(w) != KVC_OK) {
            kvc_log(KVC_LOG_ERR, "wal: everysec fsync failed on %s", w->path);
        }
    }
    return NULL;
}

/* ------------------------------------------------------------------ */
/* open / close                                                       */
/* ------------------------------------------------------------------ */

kvc_err wal_open(wal *w, const char *path, wal_fsync_policy policy) {
    memset(w, 0, sizeof *w);
    w->fd = -1;
    if (path == NULL || *path == '\0') return KVC_ERR_INVAL;

    int fd = open(path, O_RDWR | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
    if (fd < 0) {
        kvc_log(KVC_LOG_ERR, "wal: open(%s): %s", path, strerror(errno));
        return KVC_ERR_IO;
    }
    w->fd = fd;
    w->path = kvc_strndup(path, strlen(path));
    w->policy = policy;
    if (pthread_mutex_init(&w->lock, NULL) != 0) {
        kvc_log(KVC_LOG_ERR, "wal: pthread_mutex_init: %s", strerror(errno));
        close(fd);
        free(w->path);
        w->path = NULL;
        return KVC_ERR_IO;
    }
    resp_reply_init(&w->enc);
    w->offset = lseek(fd, 0, SEEK_END);
    if (w->offset < 0) {
        kvc_log(KVC_LOG_ERR, "wal: lseek(%s): %s", path, strerror(errno));
        wal_close(w);
        return KVC_ERR_IO;
    }
    w->opened = true;

    if (policy == WAL_FSYNC_EVERYSEC) {
        atomic_init(&w->stop, false);
        if (pthread_create(&w->thread, NULL, flusher_main, w) != 0) {
            kvc_log(KVC_LOG_ERR, "wal: pthread_create(flusher): %s",
                    strerror(errno));
            wal_close(w);
            return KVC_ERR_IO;
        }
        w->flusher_started = true;
    }
    kvc_log(KVC_LOG_INFO, "wal: opened %s (fsync=%s, %lld bytes)",
            path, wal_policy_name(policy), (long long)w->offset);
    return KVC_OK;
}

void wal_close(wal *w) {
    if (w == NULL || !w->opened) return;
    if (w->flusher_started) {
        atomic_store_explicit(&w->stop, true, memory_order_relaxed);
        (void)pthread_join(w->thread, NULL);
        w->flusher_started = false;
    }
    /* A clean shutdown always fsyncs: restart-after-SIGTERM must see every
       acknowledged write even under the `no` policy. */
    if (w->fd >= 0) {
        (void)wal_sync(w);
        close(w->fd);
        w->fd = -1;
    }
    resp_reply_destroy(&w->enc);
    (void)pthread_mutex_destroy(&w->lock);
    free(w->path);
    w->path = NULL;
    kvc_log(KVC_LOG_INFO, "wal: closed (%s, %lld bytes, %" PRIu64
            " records replayed, torn-tail %s)",
            wal_policy_name(w->policy), (long long)w->offset, w->replayed,
            w->truncated ? "yes" : "no");
    w->opened = false;
}

/* ------------------------------------------------------------------ */
/* append                                                             */
/* ------------------------------------------------------------------ */

kvc_err wal_append(wal *w, int argc, char *const *argv,
                   const size_t *argvlen) {
    if (w == NULL || !w->opened) return KVC_ERR_IO;
    if (argc <= 0) return KVC_ERR_INVAL;

    /* Serialize the two append paths (reactor command records + expiry
       worker purge DELs). Held across the fsync under ALWAYS so the ack
       ordering guarantee is unambiguous. */
    (void)pthread_mutex_lock(&w->lock);
    resp_reply_clear(&w->enc);
    kvc_err rc = resp_reply_mbulk_begin(&w->enc, argc);
    for (int i = 0; rc == KVC_OK && i < argc; i++) {
        rc = resp_reply_mbulk_bulk(&w->enc, argv[i], argvlen[i]);
    }
    if (rc == KVC_OK) rc = write_all(w->fd, w->enc.buf, w->enc.len);
    if (rc == KVC_OK) {
        w->offset += (off_t)w->enc.len;
        if (w->policy == WAL_FSYNC_ALWAYS) rc = wal_sync(w);
    }
    (void)pthread_mutex_unlock(&w->lock);
    return rc;
}

/* ------------------------------------------------------------------ */
/* startup crash recovery                                             */
/* ------------------------------------------------------------------ */

kvc_err wal_replay(wal *w, wal_apply_fn apply, void *ctx) {
    if (w == NULL || !w->opened || apply == NULL) return KVC_ERR_IO;
    if (lseek(w->fd, 0, SEEK_SET) < 0) {
        kvc_log(KVC_LOG_ERR, "wal: replay lseek(%s): %s", w->path,
                strerror(errno));
        return KVC_ERR_IO;
    }

    /* After every resp_parser_feed that completes a record, parser.len is
       the count of bytes fed but not yet consumed — all of which belong to
       records after this one — so `fed - parser.len` is the exact end
       offset of the record just completed. Records are never split across
       feed() calls in a way that breaks this (the parser stops at the end
       of each record and is reset before continuing). */
    resp_parser parser;
    resp_parser_init(&parser);

    char buf[64 * 1024];
    size_t fed = 0;
    off_t last_good = 0;
    uint64_t applied = 0;
    kvc_err result = KVC_OK;

    for (;;) {
        ssize_t n = read(w->fd, buf, sizeof buf);
        if (n < 0) {
            if (errno == EINTR) continue;
            kvc_log(KVC_LOG_ERR, "wal: replay read(%s): %s", w->path,
                    strerror(errno));
            result = KVC_ERR_IO;
            break;
        }
        if (n == 0) break; /* EOF */
        fed += (size_t)n;

        int rc = resp_parser_feed(&parser, buf, (size_t)n);
        while (rc == 1) {
            last_good = (off_t)(fed - parser.len);
            kvc_err ar = apply(ctx, parser.argc, parser.argv, parser.argvlen);
            if (ar != KVC_OK) {
                kvc_log(KVC_LOG_ERR,
                        "wal: replay aborted at offset %lld: record rejected",
                        (long long)last_good);
                result = ar;
                goto done;
            }
            applied++;
            resp_parser_reset(&parser);
            rc = resp_parser_feed(&parser, NULL, 0); /* drain the buffer */
        }
        if (rc < 0) {
            kvc_log(KVC_LOG_ERR,
                    "wal: %s corrupt at byte %zu (protocol violation); "
                    "refusing to start",
                    w->path, fed - parser.len);
            result = KVC_ERR_IO;
            goto done;
        }
    }

    /* Everything parsed is applied. Anything past last_good is a record
       torn by a crash (the parser ran out of bytes mid-record): cut it so
       the next append starts at a clean boundary. */
    off_t end = lseek(w->fd, 0, SEEK_END);
    if (end < 0) {
        kvc_log(KVC_LOG_ERR, "wal: replay lseek(%s): %s", w->path,
                strerror(errno));
        result = KVC_ERR_IO;
        goto done;
    }
    if (end > last_good) {
        if (ftruncate(w->fd, last_good) != 0) {
            kvc_log(KVC_LOG_ERR, "wal: truncate(%s): %s", w->path,
                    strerror(errno));
            result = KVC_ERR_IO;
            goto done;
        }
        w->truncated = true;
        kvc_log(KVC_LOG_WARN,
                "wal: %s had a torn tail (%lld bytes); truncated to last "
                "complete record at %lld",
                w->path, (long long)(end - last_good), (long long)last_good);
    }
    w->replayed = applied;
    w->offset = last_good; /* O_APPEND: future writes land after this */

done:
    resp_parser_destroy(&parser);
    if (result == KVC_OK) {
        kvc_log(KVC_LOG_INFO, "wal: replayed %" PRIu64 " records from %s",
                applied, w->path);
    }
    return result;
}

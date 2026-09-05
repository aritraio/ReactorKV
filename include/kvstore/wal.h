#ifndef KVC_WAL_H
#define KVC_WAL_H

/*
 * wal.h — Phase 5 write-ahead log (append-only, RESP-form records).
 *
 * Every mutation that actually changes the dataset is appended to the log
 * as the RESP multibulk array of the command that produced it (SET, INCR,
 * PEXPIREAT, DEL ...). Because RESP is self-framing and length-prefixed,
 * the Phase 1 incremental parser doubles as the startup loader: records
 * are replayed one at a time, and a record torn by a crash (an incomplete
 * trailing array) is detected at EOF and truncated away.
 *
 * Records are the *effective* mutations, not a raw command echo:
 *   - SET / INCR are logged verbatim (their re-execution is deterministic
 *     given the preceding log state);
 *   - EXPIRE key secs is translated to PEXPIREAT key <epoch-ms> at execute
 *     time, so a restart hours later does not re-base the TTL;
 *   - store-driven deletions (DEL on live keys, expiry purges, and
 *     maxmemory evictions) are logged as DEL records so nothing can be
 *     resurrected by a replay.
 *
 * Replay runs with store expiry checks frozen (kv_store "loading" flag):
 * entries whose absolute expiry has already passed are loaded as-is and
 * reclaimed by the active expiry worker once serving starts — the same
 * model Redis uses for AOF loading. Determinism argument (see
 * docs/ARCHITECTURE.md, Phase 5): every deletion is logged, so applying
 * records in order with expiry frozen reproduces the pre-crash dataset
 * exactly; keys that expired but were never purged before the crash are
 * correctly resurrected (the crash pre-empted the purge that was going to
 * remove them) and then reclaimed by the worker.
 *
 * Fsync policies:
 *   ALWAYS   — fsync() inside wal_append(), so an acknowledged write is
 *              durable before the reply is sent.
 *   EVERYSEC — default. Every record is write()n immediately (page cache);
 *              a background flusher thread fsyncs roughly once per second,
 *              bounding crash loss to ~1 s of acknowledged writes.
 *   NO       — no fsync while running; the OS flushes eventually. A clean
 *              wal_close() always fsyncs (cheap, reliable restart).
 *
 * Threading: appends can arrive from two threads — the reactor thread
 * (command records) and the expiry worker thread (DEL records for purged
 * keys). wal_append serializes them with an internal mutex (encode buffer,
 * write, and offset bookkeeping are all covered). The everysec flusher
 * only calls fsync(), which is safe concurrently. Stop = atomic flag +
 * pthread_join inside wal_close().
 */

#include "common.h"
#include "protocol.h"

#include <pthread.h>
#include <stdatomic.h>

typedef enum {
    WAL_FSYNC_ALWAYS = 0,   /* fsync() on every mutating command */
    WAL_FSYNC_EVERYSEC = 1, /* default: background flusher fsyncs ~1/s */
    WAL_FSYNC_NO = 2        /* OS-managed (no explicit fsync while running) */
} wal_fsync_policy;

#define WAL_FSYNC_POLICY_DEFAULT WAL_FSYNC_EVERYSEC

/* Human-readable policy name (INFO / --fsync validation). */
const char *wal_policy_name(wal_fsync_policy p);

typedef struct wal {
    int              fd;      /* O_RDWR|O_CREAT|O_APPEND */
    char            *path;    /* owned copy */
    wal_fsync_policy policy;
    off_t            offset;  /* file length / bytes logged (sole writer) */
    bool             opened;

    /* everysec flusher thread */
    pthread_t        thread;
    atomic_bool      stop;
    bool             flusher_started;

    /* serializes wal_append (reactor + expiry worker both write records) */
    pthread_mutex_t  lock;
    resp_reply       enc;    /* record staging buffer (under lock) */

    /* startup-replay outcome (for logs / INFO) */
    uint64_t         replayed;  /* records applied */
    bool             truncated; /* a torn tail was cut off */
} wal;

/* Open (creating if needed) the log at `path`. policy applies from the
   first append on. For EVERYSEC the flusher thread is started here.
   wal_replay() must run before any wal_append() when the file may already
   hold records from a previous run. */
kvc_err wal_open(wal *w, const char *path, wal_fsync_policy policy);

/* Stop + join the flusher (if any), fsync, close the fd, free the path.
   Safe to call on a zeroed / never-opened wal. */
void wal_close(wal *w);

/* Encode argv as one RESP multibulk record, write() it at the file end,
   and honour the fsync policy. On ALWAYS the fsync completes before this
   returns (so the caller can reply). argv entries need not be
   NUL-terminated — argvlen is authoritative (binary-safe keys/values).
   Only ever called from the reactor thread. */
kvc_err wal_append(wal *w, int argc, char *const *argv,
                   const size_t *argvlen);

/* fsync now. Called by the everysec flusher and wal_close. */
kvc_err wal_sync(wal *w);

/* Startup crash recovery. Replays every complete record from the current
   file offset 0 into `apply` (argc/argv/argvlen are valid only for the
   duration of each call; the callback must copy anything it keeps).
   Returns KVC_ERR_IO on a mid-file protocol violation (real corruption —
   the file is left untouched) or when `apply` fails. On success a torn
   tail record (if any) is truncated off and the fd is positioned for
   appends; w->replayed / w->truncated report what happened. Must be
   called right after wal_open and before any wal_append. */
typedef kvc_err (*wal_apply_fn)(void *ctx, int argc, char **argv,
                                const size_t *argvlen);
kvc_err wal_replay(wal *w, wal_apply_fn apply, void *ctx);

#endif /* KVC_WAL_H */

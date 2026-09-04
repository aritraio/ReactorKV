#ifndef KVC_EXPIRE_H
#define KVC_EXPIRE_H

/*
 * expire.h — active expiry worker thread (Phase 4).
 *
 * The reactor never touches a key again after EXPIRE sets its TTL, so
 * without a background actor expired entries would linger forever. This
 * worker wakes every `interval_ms` and runs one bounded pass of
 * Redis-style probabilistic expiry (kv_store_expire_cycle): it samples a
 * slice of the keyspace with a rotating bucket cursor and purges expired
 * entries, then enforces the maxmemory budget (allkeys-lru).
 *
 * The worker holds the store WRITE lock only for the duration of each
 * bounded pass (kv_store_expire_cycle acquires it), so the reactor is
 * blocked at most for one sampling slice.
 *
 * Shutdown: expire_worker_stop() sets the flag and joins the thread.
 */

#include "common.h"
#include "store.h"

#include <pthread.h>
#include <stdatomic.h>

/* Default cadence (ms) — Redis runs its active cycle at 10 Hz. */
#define KVC_EXPIRE_INTERVAL_MS_DEFAULT 100L
/* Entries sampled per pass; a whole pass over a table smaller than this
   fully sweeps it every interval. */
#define KVC_EXPIRE_SAMPLE_DEFAULT 64u

typedef struct expire_worker {
    pthread_t      thread;
    atomic_bool    stop;   /* set by expire_worker_stop() */
    bool           started;
    kv_store      *store;
    long           interval_ms;
    size_t         sample_limit;
} expire_worker;

/* Spawn the worker. interval_ms <= 0 disables it (returns KVC_OK with
   started == false). */
kvc_err expire_worker_start(expire_worker *w, kv_store *store,
                            long interval_ms, size_t sample_limit);
/* Ask the worker to stop and join it. Safe to call when not started. */
void    expire_worker_stop(expire_worker *w);

#endif /* KVC_EXPIRE_H */

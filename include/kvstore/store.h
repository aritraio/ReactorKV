#ifndef KVC_STORE_H
#define KVC_STORE_H

/*
 * store.h — storage engine facade.
 *
 * This is the seam later phases plug into behind an unchanged command
 * interface:
 *   Phase 3: slab allocator for entry/value storage
 *   Phase 4: LRU eviction + active expiry worker (pthread_rwlock)
 *   Phase 5: WAL for durability
 *
 * Concurrency model (Phase 4)
 * ---------------------------
 * The store is guarded by a single pthread_rwlock:
 *   - GET / MGET / INFO (read-only commands) hold the READ lock for the
 *     whole command handler, which is what keeps pointers borrowed by
 *     kv_store_get() valid while the RESP reply is built from them.
 *   - SET / DEL / EXPIRE / INCR (mutating commands) hold the WRITE lock.
 *   - The active expiry worker (expire.h) holds the WRITE lock for each
 *     bounded expiry/eviction pass.
 *
 * There is exactly ONE reactor thread (Phase 2 architecture), so readers
 * never run concurrently with each other; the rwlock arbitrates the reactor
 * against the background worker. LRU recency updates performed by the
 * reactor under the read lock are therefore safe: no other thread can be
 * touching the recency list while a read lock is held.
 *
 * Locking contract: the data-path functions below (kv_store_set/get/del/
 * expire/incr) do NOT take the lock themselves — the caller must hold it
 * (kv_store_rdlock for reads, kv_store_wrlock for everything that mutates).
 * kv_dispatch and kv_store_expire_cycle acquire the right lock; kv_store_stats
 * is lock-free (all counters are atomic). Tests may call the lock helpers
 * directly.
 */

#include "common.h"
#include "hashmap.h"
#include "lru.h"
#include "slab.h"

#include <pthread.h>
#include <stdatomic.h>

/* Stats snapshot (all fields read with relaxed atomics / internal counters,
   so kv_store_stats needs no lock). */
typedef struct kv_stats {
    size_t   keys;        /* entries currently in the table */
    uint64_t hits;        /* successful GET lookups */
    uint64_t misses;      /* GET lookups that found nothing (incl. expired) */
    uint64_t evictions;   /* entries evicted by maxmemory policy */
    size_t   used_bytes;  /* live entry chunk bytes */
    size_t   maxmemory;   /* configured budget (0 = unlimited) */
} kv_stats;

typedef struct kv_store {
    hashmap          table;
    slab_allocator   slab;      /* entry chunks live in here (Phase 3) */
    pthread_rwlock_t lock;      /* Phase 4: reactor readers vs. worker */
    lru              lru;       /* Phase 4: store-wide recency (head = MRU) */
    _Atomic uint64_t hits;
    _Atomic uint64_t misses;
    _Atomic uint64_t evictions;
    size_t           maxmemory; /* Phase 4: bytes, 0 = unlimited */
    size_t           expire_cursor; /* Phase 4: sweep position (under wrlock) */

    /* Phase 5: write-ahead log (borrowed; the server owns it). When set,
       every mutation that removes an entry (DEL, expiry purge, maxmemory
       eviction) is appended as a DEL record, and command handlers append
       their own records after applying. While `loading` is true (startup
       replay) expiry checks are frozen and no records are appended. */
    struct wal *wal;
    bool         loading; /* Phase 5: replay in progress */
} kv_store;

kvc_err kv_store_init(kv_store *s, size_t nbuckets);
/* Caller must have stopped the expiry worker and released the lock. */
void    kv_store_destroy(kv_store *s);

/* --- locking helpers (see the concurrency contract above) --- */
void kv_store_rdlock(kv_store *s);
void kv_store_wrlock(kv_store *s);
void kv_store_unlock(kv_store *s);

/* --- configuration (set before serving, no lock needed) --- */
void kv_store_set_maxmemory(kv_store *s, size_t bytes);

/* --- data path (caller holds the lock) --- */

/* SET. *created: 1 if a brand-new key, 0 if it overwrote a live key.
   SET clears any prior TTL, per Redis semantics. May evict LRU tails if
   maxmemory is configured and the table is over budget. (write lock) */
kvc_err kv_store_set(kv_store *s, const char *key, size_t key_len,
                     const char *val, size_t val_len, int *created);
/* GET. val/val_len borrow the store's buffer (do not free) and stay valid
   while the read lock is held. Returns KVC_ERR_NOTFOUND if the key is
   absent or expired. Expired-but-present keys are left for the active
   worker to purge (a read never mutates). (read lock) */
kvc_err kv_store_get(kv_store *s, const char *key, size_t key_len,
                     const char **val, size_t *val_len);
/* DEL. *deleted = number of live keys actually removed (0..argc). (write
   lock) */
kvc_err kv_store_del(kv_store *s, const char *const *keys, const size_t *key_lens,
                     int argc, int *deleted);
/* EXPIRE. Returns 1 if the key existed (now has a TTL), 0 otherwise.
   A non-positive ttl_ms deletes the key and returns 1 (Redis semantics).
   (write lock) */
int kv_store_expire(kv_store *s, const char *key, size_t key_len, int64_t ttl_ms);
/* PEXPIREAT: set the expiry to an absolute epoch-ms. Returns 1 if the key
   existed (now has that TTL), 0 otherwise. An absolute time already in
   the past deletes the key (Redis semantics). During startup replay
   (s->loading) the expiry is stored verbatim even when already past — the
   active worker reclaims it after load — so replayed records reproduce
   the pre-crash dataset exactly. (write lock) */
int kv_store_expireat(kv_store *s, const char *key, size_t key_len,
                      int64_t abs_ms);
/* INCR. Strict integer parse; new value in *out. KVC_ERR_INVAL on a
   non-integer value or overflow. A missing key starts at 0. TTL is kept.
   (write lock) */
kvc_err kv_store_incr(kv_store *s, const char *key, size_t key_len, int64_t *out);

/* Lock-free snapshot for stats/monitoring. */
void    kv_store_stats(const kv_store *s, kv_stats *out);

/* One bounded expiry + eviction pass, used by the expiry worker thread and
   by tests. Acquires the write lock itself. `sample_limit` bounds how many
   entries are examined for expired keys per pass (Redis-style sampling with
   a rotating bucket cursor, so all keys are eventually covered). */
void kv_store_expire_cycle(kv_store *s, size_t sample_limit);

/* Attach (or detach with NULL) the write-ahead log. Called by the server
   after startup replay; must not run concurrently with data-path calls. */
void kv_store_set_wal(kv_store *s, struct wal *w);

#endif /* KVC_STORE_H */

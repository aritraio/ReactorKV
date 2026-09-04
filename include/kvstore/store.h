#ifndef KVC_STORE_H
#define KVC_STORE_H

/*
 * store.h — storage engine facade.
 *
 * This is the seam later phases plug into behind an unchanged interface:
 *   Phase 3: slab allocator for entry/value storage
 *   Phase 4: LRU eviction + expiry worker thread (pthread_rwlock)
 *   Phase 5: WAL for durability
 * Phase 1 keeps it a thin wrapper over the hash map with lazy expiration.
 */

#include "common.h"
#include "hashmap.h"
#include "slab.h"

typedef struct kv_store {
    hashmap        table;
    slab_allocator slab;  /* entry chunks live in here (Phase 3) */
    /* Phase 4: lru *lru; pthread_rwlock_t lock;
       Phase 5: wal *wal; */
} kv_store;

kvc_err kv_store_init(kv_store *s, size_t nbuckets);
void    kv_store_destroy(kv_store *s);

/* SET. *created: 1 if a brand-new key, 0 if it overwrote a live key.
   SET clears any prior TTL, per Redis semantics. */
kvc_err kv_store_set(kv_store *s, const char *key, size_t key_len,
                     const char *val, size_t val_len, int *created);
/* GET. val/val_len borrow the store's buffer (do not free). Returns
   KVC_ERR_NOTFOUND if the key is absent or expired (lazy purge). */
kvc_err kv_store_get(kv_store *s, const char *key, size_t key_len,
                     const char **val, size_t *val_len);
/* DEL. *deleted = number of keys actually removed (0..argc). */
kvc_err kv_store_del(kv_store *s, const char *const *keys, const size_t *key_lens,
                     int argc, int *deleted);
/* EXPIRE. Returns 1 if the key existed (now has a TTL), 0 otherwise.
   A non-positive ttl_ms deletes the key and returns 1 (Redis semantics). */
int kv_store_expire(kv_store *s, const char *key, size_t key_len, int64_t ttl_ms);
/* INCR. Strict integer parse; new value in *out. KVC_ERR_INVAL on a
   non-integer value or overflow. A missing key starts at 0. TTL is kept. */
kvc_err kv_store_incr(kv_store *s, const char *key, size_t key_len, int64_t *out);
/* Snapshot stats for monitoring / later benchmarks. */
void    kv_store_stats(const kv_store *s, size_t *keys);

#endif /* KVC_STORE_H */
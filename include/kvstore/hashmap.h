#ifndef KVC_HASHMAP_H
#define KVC_HASHMAP_H

/*
 * hashmap.h — chained hash table mapping binary-safe keys to entries.
 *
 * Design notes
 * ------------
 * - Chaining (singly linked buckets) rather than open addressing: the
 *   entry struct is the unit of ownership everywhere else in the system
 *   (store, LRU), and chaining lets us move entries between buckets during
 *   rehash without touching their storage.
 * - Bucket count is always a power of two, so bucket index = hash & (n-1).
 * - Keys and values are length-prefixed (binary-safe): any byte sequence,
 *   including embedded NULs, is a legal key or value. The payload is NOT
 *   NUL-terminated; key_len/value_len are authoritative.
 * - Phase 3: each kv_entry is a single flexible-payload chunk allocated
 *   from the slab allocator the hashmap was initialized with — the entry
 *   header, key bytes, and value bytes share one chunk (memcached-style),
 *   so SET/DEL never touch the heap and chains point straight at slab
 *   chunks. chunk_sz records the chunk's total size so slab_free can find
 *   the right size class.
 * - Phase 4: kv_entry embeds LRU links (lru_prev/lru_next). When an `lru`
 *   root is attached (kv_store hands its store-wide recency list here),
 *   the hashmap maintains list membership as entries are created,
 *   migrated, deleted, or freed — every entry-lifecycle transition already
 *   lives in this file, so the recency list can never go stale. Without an
 *   attached lru (standalone use, tests) membership is simply not tracked.
 * - count/used_bytes are _Atomic so stats snapshots can be taken without
 *   taking the store lock; they are only ever mutated under the store
 *   write lock.
 */

#include "common.h"
#include "lru.h"
#include "slab.h"

#include <stdatomic.h>
#include <stddef.h>

typedef struct kv_entry kv_entry;

struct kv_entry {
    uint32_t         key_len;      /* key bytes at data[0, key_len) */
    uint32_t         value_len;    /* value bytes at data[key_len, ...) */
    uint32_t         chunk_sz;     /* total slab chunk bytes backing this entry */
    int64_t          expire_at_ms; /* wall-clock expiry; 0 == never */
    struct kv_entry *next;         /* hash-chain link */
    struct kv_entry *lru_prev;     /* Phase 4: recency list (see lru.h) */
    struct kv_entry *lru_next;
    char             data[];       /* key bytes, then value bytes (no NUL) */
};

/* Fixed header bytes before the flexible payload (data[]). */
#define KV_ENTRY_OVERHEAD (offsetof(kv_entry, data))

/* Key and value byte ranges inside the flexible payload. */
#define KV_ENTRY_KEY(e)   ((e)->data)
#define KV_ENTRY_VALUE(e) ((e)->data + (e)->key_len)

typedef struct hashmap {
    kv_entry     **buckets;     /* array of chain heads */
    size_t         nbuckets;    /* power of two */
    _Atomic size_t count;       /* live entries (atomic for lock-free stats) */
    _Atomic size_t used_bytes;  /* live chunk bytes (atomic for lock-free stats) */
    size_t         max_load;    /* rehash when count >= nbuckets * max_load / 100 */
    slab_allocator *slab;       /* entry chunks come from here (borrowed) */
    struct lru    *lru;         /* attached recency list, or NULL (borrowed) */
} hashmap;

/* Entries are allocated from `slab`, which must outlive the hashmap. */
kvc_err hashmap_init(hashmap *h, size_t nbuckets, slab_allocator *slab);
void    hashmap_destroy(hashmap *h);

/* Attach (or detach with NULL) the store-wide recency list. Membership is
   maintained from here on; call before any entries exist. */
void hashmap_set_lru(hashmap *h, struct lru *lru);

kvc_err hashmap_get(const hashmap *h, const char *key, size_t key_len,
                    kv_entry **out);
/* Insert, or overwrite an existing entry with the same key. On overwrite,
   the entry object is reused when the new key+value footprint fits in its
   slab chunk (value bytes updated in place — recency/expiry stay put). A
   value that outgrows the chunk is migrated to a fresh, larger chunk: the
   chain pointer is rewired, expire_at_ms is carried over, and the old
   chunk is returned to the slab. When an lru is attached the surviving
   entry ends up at the recency-list head. */
kvc_err hashmap_set(hashmap *h, const char *key, size_t key_len,
                    const char *value, size_t value_len, kv_entry **out);
/* Unlink and return the entry; the caller must return it to the allocator
   with hashmap_entry_free(). */
kvc_err hashmap_del(hashmap *h, const char *key, size_t key_len, kv_entry **out);
/* Unlink a specific entry object (found by pointer in its bucket chain),
   adjusting count and recency membership. The caller must return it to the
   allocator with hashmap_entry_free(). */
kvc_err hashmap_unlink(hashmap *h, kv_entry *e);

/* Move an entry to the recency-list head (no-op without an attached lru).
   Used by the store on GET hits. */
void hashmap_touch(hashmap *h, kv_entry *e);

size_t      hashmap_count(const hashmap *h);
size_t      hashmap_used_bytes(const hashmap *h);
kv_entry **hashmap_buckets(const hashmap *h, size_t *nbuckets);

/* Visit every live entry. */
typedef void (*hashmap_iter_fn)(kv_entry *e, void *ctx);
void hashmap_foreach(hashmap *h, hashmap_iter_fn fn, void *ctx);

/* Return an entry obtained from hashmap_del()/hashmap_unlink() to its slab
   chunk. */
void hashmap_entry_free(hashmap *h, kv_entry *e);

#endif /* KVC_HASHMAP_H */

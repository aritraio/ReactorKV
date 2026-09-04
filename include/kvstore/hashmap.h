#ifndef KVC_HASHMAP_H
#define KVC_HASHMAP_H

/*
 * hashmap.h — chained hash table mapping binary-safe keys to entries.
 *
 * Design notes
 * ------------
 * - Chaining (singly linked buckets) rather than open addressing: the
 *   entry struct is the unit of ownership everywhere else in the system
 *   (store, later LRU), and chaining lets us move entries between
 *   buckets during rehash without touching their storage.
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
 * - Each entry carries expire_at_ms. The hashmap treats it as opaque; the
 *   store layer interprets it (lazy expiration in Phase 1, an active
 *   expiration worker in Phase 4).
 */

#include "common.h"
#include "slab.h"

typedef struct kv_entry kv_entry;

struct kv_entry {
    uint32_t         key_len;      /* key bytes at data[0, key_len) */
    uint32_t         value_len;    /* value bytes at data[key_len, ...) */
    uint32_t         chunk_sz;     /* total slab chunk bytes backing this entry */
    int64_t          expire_at_ms; /* wall-clock expiry; 0 == never */
    struct kv_entry *next;         /* hash-chain link */
    /* Phase 4 adds: struct kv_entry *lru_prev, *lru_next; */
    char             data[];       /* key bytes, then value bytes (no NUL) */
};

/* Key and value byte ranges inside the flexible payload. */
#define KV_ENTRY_KEY(e)   ((e)->data)
#define KV_ENTRY_VALUE(e) ((e)->data + (e)->key_len)

typedef struct hashmap {
    kv_entry **buckets;        /* array of chain heads */
    size_t     nbuckets;       /* power of two */
    size_t     count;          /* live entries */
    size_t     max_load;       /* rehash when count >= nbuckets * max_load / 100 */
    slab_allocator *slab;      /* entry chunks come from here (borrowed) */
} hashmap;

/* Entries are allocated from `slab`, which must outlive the hashmap. */
kvc_err hashmap_init(hashmap *h, size_t nbuckets, slab_allocator *slab);
/* Frees every remaining entry chunk and the bucket array. */
void    hashmap_destroy(hashmap *h);

kvc_err hashmap_get(const hashmap *h, const char *key, size_t key_len,
                    kv_entry **out);
/* Insert, or overwrite an existing entry with the same key. On overwrite,
   the entry object is reused when the new key+value footprint fits in its
   slab chunk (value bytes updated in place — LRU links and expire_at_ms
   stay put). A value that outgrows the chunk is migrated to a fresh,
   larger chunk: the chain pointer is rewired, expire_at_ms is carried
   over, and the old chunk is returned to the slab. */
kvc_err hashmap_set(hashmap *h, const char *key, size_t key_len,
                    const char *value, size_t value_len, kv_entry **out);
/* Unlink and return the entry; the caller must return it to the allocator
   with hashmap_entry_free(). */
kvc_err hashmap_del(hashmap *h, const char *key, size_t key_len, kv_entry **out);

size_t      hashmap_count(const hashmap *h);
kv_entry **hashmap_buckets(const hashmap *h, size_t *nbuckets);

/* Visit every live entry (used by tests now, the expire sweeper in Phase 4). */
typedef void (*hashmap_iter_fn)(kv_entry *e, void *ctx);
void hashmap_foreach(hashmap *h, hashmap_iter_fn fn, void *ctx);

/* Return an entry obtained from hashmap_del() to its slab chunk. */
void hashmap_entry_free(const hashmap *h, kv_entry *e);

#endif /* KVC_HASHMAP_H */

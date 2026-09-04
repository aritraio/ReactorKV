#include "kvstore/hashmap.h"
#include "kvstore/lru.h"

/* Load factor: rehash when count >= nbuckets * KVC_LOAD_NUM / KVC_LOAD_DEN. */
#define KVC_LOAD_NUM 3
#define KVC_LOAD_DEN 4

/* FNV-1a 64-bit. Simple, fast, adequate for a cache; a DoS-resistant hash
   (e.g. SipHash) can be swapped in later — call sites are localized. */
static uint64_t fnv1a(const char *key, size_t len) {
    uint64_t h = UINT64_C(14695981039346656037);
    for (size_t i = 0; i < len; i++) {
        h ^= (unsigned char)key[i];
        h *= UINT64_C(1099511628211);
    }
    return h;
}

static size_t bucket_index(const hashmap *h, const char *key, size_t key_len) {
    return (size_t)(fnv1a(key, key_len) & (uint64_t)(h->nbuckets - 1));
}

/* Allocate one slab chunk holding the entry header + key + value. Rejects
   footprints larger than the biggest size class with KVC_ERR_NOMEM. */
static kvc_err entry_new(slab_allocator *slab, const char *key, size_t key_len,
                         const char *value, size_t value_len, kv_entry **out) {
    if (key_len > SIZE_MAX - value_len) return KVC_ERR_NOMEM;
    size_t total = KV_ENTRY_OVERHEAD + key_len + value_len;
    size_t chunk_sz = slab_class_size(total); /* 0 when total > 1 MiB */
    if (chunk_sz == 0) return KVC_ERR_NOMEM;
    kv_entry *e = slab_alloc(slab, chunk_sz);
    if (e == NULL) return KVC_ERR_NOMEM;
    e->key_len = (uint32_t)key_len;
    e->value_len = (uint32_t)value_len;
    e->chunk_sz = (uint32_t)chunk_sz;
    e->expire_at_ms = 0;
    e->next = NULL;
    e->lru_prev = NULL;
    e->lru_next = NULL;
    if (key_len > 0) memcpy(e->data, key, key_len);
    if (value_len > 0) memcpy(e->data + key_len, value, value_len);
    *out = e;
    return KVC_OK;
}

kvc_err hashmap_init(hashmap *h, size_t nbuckets, slab_allocator *slab) {
    if (nbuckets == 0) nbuckets = 16;
    size_t n = 1;
    while (n < nbuckets) n <<= 1; /* round up to a power of two */
    h->buckets = kvc_calloc(n, sizeof *h->buckets);
    h->nbuckets = n;
    atomic_init(&h->count, 0);
    atomic_init(&h->used_bytes, 0);
    h->max_load = KVC_LOAD_NUM;
    h->slab = slab;
    h->lru = NULL;
    return KVC_OK;
}

void hashmap_set_lru(hashmap *h, struct lru *lru) {
    h->lru = lru;
}

void hashmap_destroy(hashmap *h) {
    for (size_t i = 0; i < h->nbuckets; i++) {
        kv_entry *e = h->buckets[i];
        while (e != NULL) {
            kv_entry *next = e->next;
            /* LRU links are not unlinked here: the whole store (list root
               included) is being torn down, so the list is never walked
               again. */
            slab_free(h->slab, e, (size_t)e->chunk_sz);
            e = next;
        }
    }
    free(h->buckets);
    h->buckets = NULL;
    h->nbuckets = 0;
    atomic_store(&h->count, 0);
    atomic_store(&h->used_bytes, 0);
}

kvc_err hashmap_get(const hashmap *h, const char *key, size_t key_len,
                    kv_entry **out) {
    size_t idx = bucket_index(h, key, key_len);
    for (kv_entry *e = h->buckets[idx]; e != NULL; e = e->next) {
        if ((size_t)e->key_len == key_len &&
            (key_len == 0 || memcmp(e->data, key, key_len) == 0)) {
            *out = e;
            return KVC_OK;
        }
    }
    *out = NULL;
    return KVC_ERR_NOTFOUND;
}

static kvc_err rehash(hashmap *h, size_t new_nbuckets) {
    kv_entry **nb = kvc_calloc(new_nbuckets, sizeof *nb);
    for (size_t i = 0; i < h->nbuckets; i++) {
        kv_entry *e = h->buckets[i];
        while (e != NULL) {
            kv_entry *next = e->next;
            size_t idx = (size_t)(fnv1a(e->data, (size_t)e->key_len) &
                                  (uint64_t)(new_nbuckets - 1));
            e->next = nb[idx];
            nb[idx] = e;
            e = next;
        }
    }
    free(h->buckets);
    h->buckets = nb;
    h->nbuckets = new_nbuckets;
    return KVC_OK;
}

kvc_err hashmap_set(hashmap *h, const char *key, size_t key_len,
                    const char *value, size_t value_len, kv_entry **out) {
    kv_entry *e = NULL;
    if (hashmap_get(h, key, key_len, &e) == KVC_OK) {
        /* Overwrite. hashmap_get matched, so the key bytes are identical:
           only the value changes. If the new footprint fits in the chunk
           this entry already occupies, update in place and keep the entry
           object. Otherwise migrate to a fresh, larger chunk: the chain
           pointer is rewired, expire_at_ms is carried over, and the old
           chunk is returned to the slab. */
        bool fits;
        if (value_len >= SLAB_MAX_CHUNK) {
            fits = false; /* no chunk is bigger than 1 MiB */
        } else {
            fits = (KV_ENTRY_OVERHEAD + (size_t)e->key_len + value_len <=
                    (size_t)e->chunk_sz);
        }
        if (fits) {
            if (value_len > 0) memcpy(e->data + (size_t)e->key_len, value,
                                      value_len);
            e->value_len = (uint32_t)value_len;
            if (h->lru != NULL) lru_touch(h->lru, e); /* overwrite == access */
            *out = e;
            return KVC_OK;
        }
        /* The value outgrew the chunk: migrate to a fresh, larger chunk. */
        kv_entry *n = NULL;
        KVC_RET_ERR(entry_new(h->slab, key, key_len, value, value_len, &n));
        n->expire_at_ms = e->expire_at_ms;
        n->next = e->next;
        kv_entry **link = &h->buckets[bucket_index(h, key, key_len)];
        while (*link != e) link = &(*link)->next;
        *link = n;
        if (h->lru != NULL) lru_remove(h->lru, e);
        atomic_fetch_sub_explicit(&h->used_bytes, (size_t)e->chunk_sz,
                                  memory_order_relaxed);
        slab_free(h->slab, e, (size_t)e->chunk_sz);
        atomic_fetch_add_explicit(&h->used_bytes, (size_t)n->chunk_sz,
                                  memory_order_relaxed);
        if (h->lru != NULL) lru_push_front(h->lru, n);
        *out = n;
        return KVC_OK;
    }

    KVC_RET_ERR(entry_new(h->slab, key, key_len, value, value_len, &e));
    size_t idx = bucket_index(h, key, key_len);
    e->next = h->buckets[idx];
    h->buckets[idx] = e;
    atomic_fetch_add_explicit(&h->count, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&h->used_bytes, (size_t)e->chunk_sz,
                              memory_order_relaxed);
    if (h->lru != NULL) lru_push_front(h->lru, e);
    if (atomic_load_explicit(&h->count, memory_order_relaxed) >=
        (h->nbuckets * KVC_LOAD_NUM) / KVC_LOAD_DEN) {
        KVC_RET_ERR(rehash(h, h->nbuckets * 2));
    }
    *out = e;
    return KVC_OK;
}

/* Unlink `e` from whichever bucket chain holds it (found by scanning the
   bucket its key hashes to). Bucket chain bookkeeping only. */
static kvc_err unlink_from_bucket(hashmap *h, kv_entry *e) {
    size_t idx = bucket_index(h, e->data, (size_t)e->key_len);
    kv_entry **link = &h->buckets[idx];
    while (*link != NULL) {
        if (*link == e) {
            *link = e->next;
            return KVC_OK;
        }
        link = &(*link)->next;
    }
    return KVC_ERR_NOTFOUND;
}

kvc_err hashmap_del(hashmap *h, const char *key, size_t key_len,
                    kv_entry **out) {
    kv_entry *e = NULL;
    if (hashmap_get(h, key, key_len, &e) != KVC_OK) {
        *out = NULL;
        return KVC_ERR_NOTFOUND;
    }
    KVC_RET_ERR(unlink_from_bucket(h, e));
    if (h->lru != NULL) lru_remove(h->lru, e);
    atomic_fetch_sub_explicit(&h->count, 1, memory_order_relaxed);
    *out = e;
    return KVC_OK;
}

kvc_err hashmap_unlink(hashmap *h, kv_entry *e) {
    if (e == NULL) return KVC_ERR_NOTFOUND;
    KVC_RET_ERR(unlink_from_bucket(h, e));
    if (h->lru != NULL) lru_remove(h->lru, e);
    atomic_fetch_sub_explicit(&h->count, 1, memory_order_relaxed);
    return KVC_OK;
}

void hashmap_touch(hashmap *h, kv_entry *e) {
    if (h->lru != NULL && e != NULL) lru_touch(h->lru, e);
}

size_t hashmap_count(const hashmap *h) {
    return atomic_load_explicit(&h->count, memory_order_relaxed);
}

size_t hashmap_used_bytes(const hashmap *h) {
    return atomic_load_explicit(&h->used_bytes, memory_order_relaxed);
}

kv_entry **hashmap_buckets(const hashmap *h, size_t *nbuckets) {
    *nbuckets = h->nbuckets;
    return h->buckets;
}

void hashmap_entry_free(hashmap *h, kv_entry *e) {
    if (e != NULL) {
        atomic_fetch_sub_explicit(&h->used_bytes, (size_t)e->chunk_sz,
                                  memory_order_relaxed);
        slab_free(h->slab, e, (size_t)e->chunk_sz);
    }
}

void hashmap_foreach(hashmap *h, hashmap_iter_fn fn, void *ctx) {
    for (size_t i = 0; i < h->nbuckets; i++) {
        for (kv_entry *e = h->buckets[i]; e != NULL; e = e->next) {
            fn(e, ctx);
        }
    }
}

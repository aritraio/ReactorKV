#include "kvstore/hashmap.h"

/* Load factor: rehash when count >= nbuckets * KVC_LOAD_NUM / KVC_LOAD_DEN. */
#define KVC_LOAD_NUM 3
#define KVC_LOAD_DEN 4

/* Fixed header bytes before the flexible payload (data[]). Every entry
   chunk is exactly chunk_sz bytes: this header, then key bytes, then
   value bytes. */
#define KVC_ENTRY_OVERHEAD (offsetof(kv_entry, data))

/* FNV-1a 64-bit. Simple, fast, adequate for a cache; Phase 4 may swap in
   a DoS-resistant hash (e.g. SipHash) — the call sites are localized. */
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
    size_t total = KVC_ENTRY_OVERHEAD + key_len + value_len;
    size_t chunk_sz = slab_class_size(total); /* 0 when total > 1 MiB */
    if (chunk_sz == 0) return KVC_ERR_NOMEM;
    kv_entry *e = slab_alloc(slab, chunk_sz);
    if (e == NULL) return KVC_ERR_NOMEM;
    e->key_len = (uint32_t)key_len;
    e->value_len = (uint32_t)value_len;
    e->chunk_sz = (uint32_t)chunk_sz;
    e->expire_at_ms = 0;
    e->next = NULL;
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
    h->count = 0;
    h->max_load = KVC_LOAD_NUM;
    h->slab = slab;
    return KVC_OK;
}

void hashmap_destroy(hashmap *h) {
    for (size_t i = 0; i < h->nbuckets; i++) {
        kv_entry *e = h->buckets[i];
        while (e != NULL) {
            kv_entry *next = e->next;
            slab_free(h->slab, e, (size_t)e->chunk_sz);
            e = next;
        }
    }
    free(h->buckets);
    h->buckets = NULL;
    h->nbuckets = 0;
    h->count = 0;
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
           object (LRU links in Phase 4 and expire_at_ms stay put). */
        bool fits;
        if (value_len >= SLAB_MAX_CHUNK) {
            fits = false; /* no chunk is bigger than 1 MiB */
        } else {
            fits = (KVC_ENTRY_OVERHEAD + (size_t)e->key_len + value_len <=
                    (size_t)e->chunk_sz);
        }
        if (fits) {
            if (value_len > 0) memcpy(e->data + (size_t)e->key_len, value,
                                      value_len);
            e->value_len = (uint32_t)value_len;
            *out = e;
            return KVC_OK;
        }
        /* The value outgrew the chunk: migrate to a fresh, larger chunk,
           carry the opaque expire_at_ms over, rewire the bucket so the
           entry stays findable, and return the old chunk to the slab. */
        kv_entry *n = NULL;
        KVC_RET_ERR(entry_new(h->slab, key, key_len, value, value_len, &n));
        n->expire_at_ms = e->expire_at_ms;
        n->next = e->next;
        kv_entry **link = &h->buckets[bucket_index(h, key, key_len)];
        while (*link != e) link = &(*link)->next;
        *link = n;
        slab_free(h->slab, e, (size_t)e->chunk_sz);
        *out = n;
        return KVC_OK;
    }

    KVC_RET_ERR(entry_new(h->slab, key, key_len, value, value_len, &e));
    size_t idx = bucket_index(h, key, key_len);
    e->next = h->buckets[idx];
    h->buckets[idx] = e;
    h->count++;
    if (h->count >= (h->nbuckets * KVC_LOAD_NUM) / KVC_LOAD_DEN) {
        KVC_RET_ERR(rehash(h, h->nbuckets * 2));
    }
    *out = e;
    return KVC_OK;
}

kvc_err hashmap_del(hashmap *h, const char *key, size_t key_len, kv_entry **out) {
    size_t idx = bucket_index(h, key, key_len);
    kv_entry **link = &h->buckets[idx];
    while (*link != NULL) {
        kv_entry *e = *link;
        if ((size_t)e->key_len == key_len &&
            (key_len == 0 || memcmp(e->data, key, key_len) == 0)) {
            *link = e->next;
            h->count--;
            *out = e;
            return KVC_OK;
        }
        link = &e->next;
    }
    *out = NULL;
    return KVC_ERR_NOTFOUND;
}

size_t hashmap_count(const hashmap *h) { return h->count; }

kv_entry **hashmap_buckets(const hashmap *h, size_t *nbuckets) {
    *nbuckets = h->nbuckets;
    return h->buckets;
}

void hashmap_entry_free(const hashmap *h, kv_entry *e) {
    if (e != NULL) slab_free(h->slab, e, (size_t)e->chunk_sz);
}

void hashmap_foreach(hashmap *h, hashmap_iter_fn fn, void *ctx) {
    for (size_t i = 0; i < h->nbuckets; i++) {
        for (kv_entry *e = h->buckets[i]; e != NULL; e = e->next) {
            fn(e, ctx);
        }
    }
}

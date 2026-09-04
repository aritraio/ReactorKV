#include "kvstore/hashmap.h"

/* Load factor: rehash when count >= nbuckets * KVC_LOAD_NUM / KVC_LOAD_DEN. */
#define KVC_LOAD_NUM 3
#define KVC_LOAD_DEN 4

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

static kv_entry *entry_new(const char *key, size_t key_len,
                           const char *value, size_t value_len) {
    kv_entry *e = kvc_malloc(sizeof *e);
    e->key = kvc_strndup(key, key_len);
    e->key_len = key_len;
    e->value = kvc_strndup(value, value_len);
    e->value_len = value_len;
    e->expire_at_ms = 0;
    e->next = NULL;
    return e;
}

void kv_entry_free(kv_entry *e) {
    if (e == NULL) return;
    free(e->key);
    free(e->value);
    free(e);
}

kvc_err hashmap_init(hashmap *h, size_t nbuckets) {
    if (nbuckets == 0) nbuckets = 16;
    size_t n = 1;
    while (n < nbuckets) n <<= 1; /* round up to a power of two */
    h->buckets = kvc_calloc(n, sizeof *h->buckets);
    h->nbuckets = n;
    h->count = 0;
    h->max_load = KVC_LOAD_NUM;
    return KVC_OK;
}

void hashmap_destroy(hashmap *h) {
    for (size_t i = 0; i < h->nbuckets; i++) {
        kv_entry *e = h->buckets[i];
        while (e != NULL) {
            kv_entry *next = e->next;
            kv_entry_free(e);
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
        if (e->key_len == key_len && memcmp(e->key, key, key_len) == 0) {
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
            size_t idx = (size_t)(fnv1a(e->key, e->key_len) &
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
        /* Overwrite in place: replace the value, keep the entry object. */
        char *nv = kvc_strndup(value, value_len);
        free(e->value);
        e->value = nv;
        e->value_len = value_len;
        *out = e;
        return KVC_OK;
    }
    e = entry_new(key, key_len, value, value_len);
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
        if (e->key_len == key_len && memcmp(e->key, key, key_len) == 0) {
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

void hashmap_foreach(hashmap *h, hashmap_iter_fn fn, void *ctx) {
    for (size_t i = 0; i < h->nbuckets; i++) {
        for (kv_entry *e = h->buckets[i]; e != NULL; e = e->next) {
            fn(e, ctx);
        }
    }
}
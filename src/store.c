#include "kvstore/store.h"

kvc_err kv_store_init(kv_store *s, size_t nbuckets) {
    memset(s, 0, sizeof *s);
    kvc_err rc = slab_allocator_init(&s->slab);
    if (rc != KVC_OK) return rc;
    return hashmap_init(&s->table, nbuckets ? nbuckets : 16, &s->slab);
}

void kv_store_destroy(kv_store *s) {
    hashmap_destroy(&s->table);   /* returns every entry chunk to the slab */
    slab_allocator_destroy(&s->slab); /* munmap all pages */
}

static bool entry_expired(const kv_entry *e) {
    return e->expire_at_ms > 0 && kvc_now_ms() >= e->expire_at_ms;
}

/* Remove a key from the table and return its chunk to the slab (helper
   for lazy expiry). */
static void purge(kv_store *s, const char *key, size_t key_len) {
    kv_entry *gone = NULL;
    (void)hashmap_del(&s->table, key, key_len, &gone);
    hashmap_entry_free(&s->table, gone);
}

kvc_err kv_store_set(kv_store *s, const char *key, size_t key_len,
                     const char *val, size_t val_len, int *created) {
    kv_entry *e = NULL;
    kvc_err rc = hashmap_get(&s->table, key, key_len, &e);
    bool existed = (rc == KVC_OK) && !entry_expired(e);
    KVC_RET_ERR(hashmap_set(&s->table, key, key_len, val, val_len, &e));
    e->expire_at_ms = 0; /* SET clears any prior TTL, per Redis semantics */
    if (created != NULL) *created = existed ? 0 : 1;
    return KVC_OK;
}

kvc_err kv_store_get(kv_store *s, const char *key, size_t key_len,
                     const char **val, size_t *val_len) {
    kv_entry *e = NULL;
    KVC_RET_ERR(hashmap_get(&s->table, key, key_len, &e));
    if (entry_expired(e)) {
        purge(s, key, key_len); /* lazy expiration: purge on access */
        return KVC_ERR_NOTFOUND;
    }
    *val = KV_ENTRY_VALUE(e);
    *val_len = (size_t)e->value_len;
    return KVC_OK;
}

kvc_err kv_store_del(kv_store *s, const char *const *keys, const size_t *key_lens,
                     int argc, int *deleted) {
    int n = 0;
    for (int i = 0; i < argc; i++) {
        kv_entry *e = NULL;
        if (hashmap_get(&s->table, keys[i], key_lens[i], &e) != KVC_OK) continue;
        bool was_live = !entry_expired(e);
        purge(s, keys[i], key_lens[i]);
        if (was_live) n++;
    }
    *deleted = n;
    return KVC_OK;
}

int kv_store_expire(kv_store *s, const char *key, size_t key_len, int64_t ttl_ms) {
    kv_entry *e = NULL;
    if (hashmap_get(&s->table, key, key_len, &e) != KVC_OK) return 0;
    if (entry_expired(e)) {
        purge(s, key, key_len);
        return 0;
    }
    if (ttl_ms <= 0) {
        /* Redis: EXPIRE with a non-positive TTL deletes the key. */
        purge(s, key, key_len);
        return 1;
    }
    int64_t now = kvc_now_ms();
    e->expire_at_ms = (ttl_ms > INT64_MAX - now) ? INT64_MAX : now + ttl_ms;
    return 1;
}

kvc_err kv_store_incr(kv_store *s, const char *key, size_t key_len, int64_t *out) {
    kv_entry *e = NULL;
    kvc_err rc = hashmap_get(&s->table, key, key_len, &e);
    if (rc == KVC_ERR_NOTFOUND || (rc == KVC_OK && entry_expired(e))) {
        if (rc == KVC_OK) purge(s, key, key_len);
        KVC_RET_ERR(hashmap_set(&s->table, key, key_len, "1", 1, &e));
        e->expire_at_ms = 0; /* new key: no TTL */
        *out = 1;
        return KVC_OK;
    }
    if (rc != KVC_OK) return rc;
    int64_t cur = 0;
    if (kvc_parse_int64(KV_ENTRY_VALUE(e), (size_t)e->value_len, &cur) != KVC_OK) {
        return KVC_ERR_INVAL;
    }
    if (cur == INT64_MAX) return KVC_ERR_INVAL; /* increment would overflow */
    int64_t next = cur + 1;
    char buf[24];
    int n = snprintf(buf, sizeof buf, "%" PRId64, next);
    if (n < 0) return KVC_ERR_IO;
    KVC_RET_ERR(hashmap_set(&s->table, key, key_len, buf, (size_t)n, &e));
    /* INCR keeps an existing TTL (only SET clears it): hashmap_set updated
       the value in place when it fit and carried expire_at_ms over when a
       grow forced a fresh chunk, so it is untouched either way. */
    *out = next;
    return KVC_OK;
}

void kv_store_stats(const kv_store *s, size_t *keys) {
    *keys = hashmap_count(&s->table);
}

#include "kvstore/store.h"

#include "kvstore/wal.h"

/* ------------------------------------------------------------------ */
/* Internal helpers                                                    */
/* ------------------------------------------------------------------ */

static bool entry_expired(const kv_store *s, const kv_entry *e) {
    /* Phase 5: during startup replay (`loading`) expiry is frozen — every
       deletion that happened at runtime is already in the log as a DEL, so
       judging entries against the current wall clock here would diverge
       from the pre-crash dataset. The active worker reclaims anything whose
       absolute expiry passed once serving starts. */
    if (s->loading) return false;
    return e->expire_at_ms > 0 && kvc_now_ms() >= e->expire_at_ms;
}

/* Append a single-key DEL record (lowercase command name, matching the
   parser's canonicalization) for a removal the store itself performs:
   expiry purges and maxmemory evictions must be durable too, or replay
   would resurrect them. Silently skipped while loading (the record being
   replayed came from the same log; appending to it mid-load would corrupt
   it) or when no WAL is attached. Logs but does not fail the caller on a
   write error: the mutation has already happened and there is no way to
   roll the store back; the error is surfaced loudly instead. */
static void wal_log_del(kv_store *s, const char *key, size_t key_len) {
    if (s->wal == NULL || s->loading) return;
    char *argv[2];
    argv[0] = (char *)"del";
    argv[1] = (char *)key; /* need not be NUL-terminated: argvlen rules */
    const size_t argvlen[2] = { 3, key_len };
    if (wal_append(s->wal, 2, argv, argvlen) != KVC_OK) {
        kvc_log(KVC_LOG_ERR, "wal: failed to log DEL (key %zu bytes); the "
                "dataset is now ahead of the log", key_len);
    }
}

/* Remove a live-or-expired entry given its key, logging a DEL first, and
   returning its chunk to the slab. Caller must hold the write lock. */
static void purge(kv_store *s, const char *key, size_t key_len) {
    kv_entry *gone = NULL;
    if (hashmap_del(&s->table, key, key_len, &gone) == KVC_OK) {
        wal_log_del(s, key, key_len);
        hashmap_entry_free(&s->table, gone);
    }
}

/* Evict least-recently-used entries until used_bytes <= maxmemory, or only
   one entry remains (a single entry may legitimately exceed the budget: it
   cannot be evicted to make room for itself). Caller holds the write lock.
   maxmemory of 0 disables eviction. Evicted victims are logged as DELs so
   a crash cannot resurrect them on replay. */
static void evict_to_budget(kv_store *s) {
    if (s->maxmemory == 0) return;
    size_t count = hashmap_count(&s->table);
    while (count > 1 && hashmap_used_bytes(&s->table) > s->maxmemory) {
        kv_entry *victim = lru_tail(&s->lru); /* least recently used */
        if (victim == NULL) break; /* defensive: table empty */
        wal_log_del(s, KV_ENTRY_KEY(victim), (size_t)victim->key_len);
        if (hashmap_unlink(&s->table, victim) != KVC_OK) break;
        hashmap_entry_free(&s->table, victim);
        atomic_fetch_add_explicit(&s->evictions, 1, memory_order_relaxed);
        count = hashmap_count(&s->table);
    }
}

/* ------------------------------------------------------------------ */
/* Lifecycle / configuration                                           */
/* ------------------------------------------------------------------ */

kvc_err kv_store_init(kv_store *s, size_t nbuckets) {
    memset(s, 0, sizeof *s);
    kvc_err rc = slab_allocator_init(&s->slab);
    if (rc != KVC_OK) return rc;
    rc = hashmap_init(&s->table, nbuckets ? nbuckets : 16, &s->slab);
    if (rc != KVC_OK) return rc;
    lru_init(&s->lru);
    hashmap_set_lru(&s->table, &s->lru);
    if (pthread_rwlock_init(&s->lock, NULL) != 0) return KVC_ERR_IO;
    atomic_init(&s->hits, 0);
    atomic_init(&s->misses, 0);
    atomic_init(&s->evictions, 0);
    s->maxmemory = 0; /* unlimited by default */
    s->expire_cursor = 0;
    s->wal = NULL;
    s->loading = false;
    return KVC_OK;
}

void kv_store_destroy(kv_store *s) {
    hashmap_destroy(&s->table); /* returns every entry chunk to the slab */
    slab_allocator_destroy(&s->slab); /* munmap all pages */
    (void)pthread_rwlock_destroy(&s->lock);
}

void kv_store_rdlock(kv_store *s) { (void)pthread_rwlock_rdlock(&s->lock); }
void kv_store_wrlock(kv_store *s) { (void)pthread_rwlock_wrlock(&s->lock); }
void kv_store_unlock(kv_store *s) { (void)pthread_rwlock_unlock(&s->lock); }

void kv_store_set_maxmemory(kv_store *s, size_t bytes) {
    s->maxmemory = bytes;
}

void kv_store_set_wal(kv_store *s, struct wal *w) {
    s->wal = w;
}

/* ------------------------------------------------------------------ */
/* Data path (callers hold the lock)                                   */
/* ------------------------------------------------------------------ */

kvc_err kv_store_set(kv_store *s, const char *key, size_t key_len,
                     const char *val, size_t val_len, int *created) {
    kv_entry *e = NULL;
    kvc_err rc = hashmap_get(&s->table, key, key_len, &e);
    bool existed = (rc == KVC_OK) && !entry_expired(s, e);
    /* Refuse an entry that can never fit even alone (mirrors the 1 MiB
       slab cap, now budget-driven): past the largest slab class (need == 0)
       or, under maxmemory, when the write requires a fresh chunk bigger
       than the whole budget. An in-place overwrite never allocates, so it
       is always allowed. */
    if (s->maxmemory > 0) {
        size_t need = (size_t)slab_class_size(KV_ENTRY_OVERHEAD + key_len +
                                              val_len);
        bool in_place = existed && need <= (size_t)e->chunk_sz;
        if (need == 0 || (need > s->maxmemory && !in_place)) {
            return KVC_ERR_NOMEM;
        }
    }
    KVC_RET_ERR(hashmap_set(&s->table, key, key_len, val, val_len, &e));
    e->expire_at_ms = 0; /* SET clears any prior TTL, per Redis semantics */
    if (created != NULL) *created = existed ? 0 : 1;
    evict_to_budget(s);
    return KVC_OK;
}

kvc_err kv_store_get(kv_store *s, const char *key, size_t key_len,
                     const char **val, size_t *val_len) {
    kv_entry *e = NULL;
    if (hashmap_get(&s->table, key, key_len, &e) != KVC_OK) {
        atomic_fetch_add_explicit(&s->misses, 1, memory_order_relaxed);
        return KVC_ERR_NOTFOUND;
    }
    if (entry_expired(s, e)) {
        /* Logically gone. A read never mutates, so the expired chunk is
           left for the active expiry worker to purge. */
        atomic_fetch_add_explicit(&s->misses, 1, memory_order_relaxed);
        return KVC_ERR_NOTFOUND;
    }
    hashmap_touch(&s->table, e); /* recency: head of the LRU list */
    atomic_fetch_add_explicit(&s->hits, 1, memory_order_relaxed);
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
        bool was_live = !entry_expired(s, e);
        purge(s, keys[i], key_lens[i]); /* logs the DEL itself */
        if (was_live) n++;
    }
    *deleted = n;
    return KVC_OK;
}

int kv_store_expire(kv_store *s, const char *key, size_t key_len, int64_t ttl_ms) {
    if (ttl_ms <= 0) {
        /* Redis: EXPIRE with a non-positive TTL deletes the key. Route
           through expireat with an absolute time already in the past. */
        return kv_store_expireat(s, key, key_len, kvc_now_ms());
    }
    int64_t now = kvc_now_ms();
    int64_t abs = (ttl_ms > INT64_MAX - now) ? INT64_MAX : now + ttl_ms;
    return kv_store_expireat(s, key, key_len, abs);
}

int kv_store_expireat(kv_store *s, const char *key, size_t key_len,
                      int64_t abs_ms) {
    kv_entry *e = NULL;
    if (hashmap_get(&s->table, key, key_len, &e) != KVC_OK) return 0;

    if (s->loading) {
        /* Replay: set the recorded absolute expiry verbatim. Even an expiry
           already in the past must be stored, not acted on — the record
           was generated when the key was live, and any later deletion of it
           is itself a later record in the log. The worker cleans up after
           load. */
        e->expire_at_ms = abs_ms;
        return 1;
    }
    if (entry_expired(s, e)) {
        purge(s, key, key_len); /* logs DEL; mirrors EXPIRE on an expired key */
        return 0;
    }
    if (abs_ms <= kvc_now_ms()) {
        /* An expiry timestamp in the past: the key is deleted now (Redis
           PEXPIREAT semantics). The purge logs the DEL. */
        purge(s, key, key_len);
        return 1;
    }
    e->expire_at_ms = abs_ms;
    return 1;
}

kvc_err kv_store_incr(kv_store *s, const char *key, size_t key_len, int64_t *out) {
    kv_entry *e = NULL;
    kvc_err rc = hashmap_get(&s->table, key, key_len, &e);
    if (rc == KVC_ERR_NOTFOUND || (rc == KVC_OK && entry_expired(s, e))) {
        if (rc == KVC_OK) purge(s, key, key_len); /* logs the DEL */
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
    evict_to_budget(s); /* a new key (or a migrated grow) may add memory */
    return KVC_OK;
}

void kv_store_stats(const kv_store *s, kv_stats *out) {
    out->keys = hashmap_count(&s->table);
    out->hits = atomic_load_explicit(&s->hits, memory_order_relaxed);
    out->misses = atomic_load_explicit(&s->misses, memory_order_relaxed);
    out->evictions =
        atomic_load_explicit(&s->evictions, memory_order_relaxed);
    out->used_bytes = hashmap_used_bytes(&s->table);
    out->maxmemory = s->maxmemory;
}

/* One active-expiry pass: examine up to `sample_limit` entries using a
   rotating cursor over buckets (Redis-style bounded sampling — keys that
   are never looked up still get purged), then enforce the memory budget.
   Caller must hold the write lock. Purged entries are logged as DELs. */
static void expire_cycle_locked(kv_store *s, size_t sample_limit) {
    size_t nbuckets = 0;
    kv_entry **buckets = hashmap_buckets(&s->table, &nbuckets);
    if (nbuckets == 0) return;

    int64_t now = kvc_now_ms();
    size_t cursor = s->expire_cursor % nbuckets;
    size_t examined = 0;
    size_t buckets_seen = 0;

    while (examined < sample_limit && buckets_seen < nbuckets) {
        kv_entry *e = buckets[cursor];
        while (e != NULL && examined < sample_limit) {
            examined++;
            kv_entry *next = e->next;
            if (e->expire_at_ms > 0 && now >= e->expire_at_ms) {
                wal_log_del(s, KV_ENTRY_KEY(e), (size_t)e->key_len);
                if (hashmap_unlink(&s->table, e) == KVC_OK) {
                    hashmap_entry_free(&s->table, e);
                }
            }
            e = next;
        }
        cursor = (cursor + 1) % nbuckets;
        buckets_seen++;
    }
    s->expire_cursor = cursor;
    evict_to_budget(s);
}

void kv_store_expire_cycle(kv_store *s, size_t sample_limit) {
    kv_store_wrlock(s);
    expire_cycle_locked(s, sample_limit);
    kv_store_unlock(s);
}

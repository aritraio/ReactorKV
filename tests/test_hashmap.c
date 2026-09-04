/* Unit tests for the chained hash table, backed by the Phase 3 slab
   allocator (entries are single flexible chunks in slab pages). Build and
   run via `make test`. Valgrind-clean: every chunk is returned to the slab
   and every slab page is munmap'd on destroy. */

#include "kvstore/hashmap.h"
#include "kvstore/slab.h"

#include <stdio.h>
#include <string.h>

static int failures = 0;
static int checks = 0;

#define CHECK(cond)                                                        \
    do {                                                                   \
        checks++;                                                          \
        if (!(cond)) {                                                     \
            failures++;                                                    \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond); \
        }                                                                  \
    } while (0)

/* Every test pairs a hashmap with the slab allocator that backs it. */
typedef struct env {
    slab_allocator slab;
    hashmap        h;
} env;

static void env_init(env *env, size_t nbuckets) {
    CHECK(slab_allocator_init(&env->slab) == KVC_OK);
    CHECK(hashmap_init(&env->h, nbuckets, &env->slab) == KVC_OK);
}

static void env_destroy(env *env) {
    hashmap_destroy(&env->h);
    slab_allocator_destroy(&env->slab);
}

static void test_basic(void) {
    env e;
    env_init(&e, 4);

    kv_entry *ent = NULL;
    CHECK(hashmap_get(&e.h, "foo", 3, &ent) == KVC_ERR_NOTFOUND);

    CHECK(hashmap_set(&e.h, "foo", 3, "bar", 3, &ent) == KVC_OK);
    CHECK(hashmap_count(&e.h) == 1);
    CHECK(ent->key_len == 3 && memcmp(KV_ENTRY_KEY(ent), "foo", 3) == 0);
    CHECK(ent->value_len == 3 && memcmp(KV_ENTRY_VALUE(ent), "bar", 3) == 0);

    CHECK(hashmap_get(&e.h, "foo", 3, &ent) == KVC_OK);
    CHECK(memcmp(KV_ENTRY_VALUE(ent), "bar", 3) == 0);

    /* Overwrite that still fits the entry's slab chunk reuses the entry
       object (important for LRU links in Phase 4) and updates the value. */
    kv_entry *before = ent;
    CHECK(hashmap_set(&e.h, "foo", 3, "baz!", 4, &ent) == KVC_OK);
    CHECK(ent == before);
    CHECK(ent->value_len == 4 && memcmp(KV_ENTRY_VALUE(ent), "baz!", 4) == 0);
    CHECK(hashmap_count(&e.h) == 1);

    CHECK(hashmap_del(&e.h, "foo", 3, &ent) == KVC_OK);
    CHECK(ent == before);
    hashmap_entry_free(&e.h, ent);
    CHECK(hashmap_count(&e.h) == 0);
    CHECK(hashmap_get(&e.h, "foo", 3, &ent) == KVC_ERR_NOTFOUND);

    env_destroy(&e);
}

/* A value that outgrows the entry's slab chunk must migrate to a fresh,
   larger chunk — the chain is rewired, the entry stays findable, and the
   opaque expire_at_ms survives (INCR/SET TTL semantics depend on it). */
static void test_overwrite_grows_across_class(void) {
    env e;
    env_init(&e, 4);

    char big[200];
    memset(big, 'a', sizeof big);

    /* Header is 48 B in Phase 4 (LRU links added), so 48+1+40 = 89 B fits
       the 128 B class. */
    kv_entry *first = NULL;
    CHECK(hashmap_set(&e.h, "k", 1, big, 40, &first) == KVC_OK);
    first->expire_at_ms = 123456789; /* simulate a TTL set by the store */

    /* Growth that still fits the 128-byte chunk stays in place
       (48+1+70 = 119 <= 128). */
    kv_entry *ent = NULL;
    CHECK(hashmap_set(&e.h, "k", 1, big, 70, &ent) == KVC_OK);
    CHECK(ent == first);
    CHECK(ent->value_len == 70);

    /* Growth past the chunk (48+1+90 = 139 > 128) forces a migration to a
       fresh entry. */
    CHECK(hashmap_set(&e.h, "k", 1, big, 90, &ent) == KVC_OK);
    CHECK(ent != first);
    CHECK(ent->value_len == 90);
    CHECK(ent->expire_at_ms == 123456789); /* TTL carried across the move */
    CHECK(hashmap_count(&e.h) == 1);

    /* The new entry is findable and the old one is gone from the table. */
    CHECK(hashmap_get(&e.h, "k", 1, &ent) == KVC_OK);
    CHECK(ent->value_len == 90 && memcmp(KV_ENTRY_VALUE(ent), big, 90) == 0);

    env_destroy(&e);
}

static void test_binary_safe_keys(void) {
    env e;
    env_init(&e, 4);

    /* key "a\0b" (3 bytes, embedded NUL) */
    const char key[3] = { 'a', '\0', 'b' };
    kv_entry *ent = NULL;
    CHECK(hashmap_set(&e.h, key, 3, "v1", 2, &ent) == KVC_OK);

    /* A NUL-terminated "a" must NOT match the 3-byte key. */
    CHECK(hashmap_get(&e.h, "a", 1, &ent) == KVC_ERR_NOTFOUND);
    CHECK(hashmap_get(&e.h, key, 3, &ent) == KVC_OK);
    CHECK(ent->value_len == 2 && memcmp(KV_ENTRY_VALUE(ent), "v1", 2) == 0);

    env_destroy(&e);
}

static void test_rehash_and_many(void) {
    env e;
    env_init(&e, 4); /* forces several doubling rehashes */

    char key[32], val[32];
    enum { N = 10000 };
    for (int i = 0; i < N; i++) {
        snprintf(key, sizeof key, "key%06d", i);
        snprintf(val, sizeof val, "value%06d", i);
        kv_entry *ent = NULL;
        CHECK(hashmap_set(&e.h, key, strlen(key), val, strlen(val), &ent) == KVC_OK);
    }
    CHECK(hashmap_count(&e.h) == (size_t)N);

    for (int i = 0; i < N; i++) {
        snprintf(key, sizeof key, "key%06d", i);
        snprintf(val, sizeof val, "value%06d", i);
        kv_entry *ent = NULL;
        CHECK(hashmap_get(&e.h, key, strlen(key), &ent) == KVC_OK);
        CHECK(ent->value_len == strlen(val) &&
              memcmp(KV_ENTRY_VALUE(ent), val, strlen(val)) == 0);
    }

    env_destroy(&e);
}

struct count_ctx { size_t n; };
static void count_cb(kv_entry *ent, void *ctx) {
    (void)ent;
    ((struct count_ctx *)ctx)->n++;
}

static void test_foreach(void) {
    env e;
    env_init(&e, 4);
    for (int i = 0; i < 50; i++) {
        char key[16];
        snprintf(key, sizeof key, "k%d", i);
        kv_entry *ent = NULL;
        hashmap_set(&e.h, key, strlen(key), "x", 1, &ent);
    }
    struct count_ctx ctx = { 0 };
    hashmap_foreach(&e.h, count_cb, &ctx);
    CHECK(ctx.n == 50);
    env_destroy(&e);
}

int main(void) {
    test_basic();
    test_overwrite_grows_across_class();
    test_binary_safe_keys();
    test_rehash_and_many();
    test_foreach();
    printf("test_hashmap: %d checks, %d failures\n", checks, failures);
    return failures ? 1 : 0;
}

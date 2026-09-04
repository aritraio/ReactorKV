/* Unit tests for the chained hash table. Build and run via `make test`.
   Valgrind-clean: every entry is freed on destroy. */

#include "kvstore/hashmap.h"

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

static void test_basic(void) {
    hashmap h;
    CHECK(hashmap_init(&h, 4) == KVC_OK);

    kv_entry *e = NULL;
    CHECK(hashmap_get(&h, "foo", 3, &e) == KVC_ERR_NOTFOUND);

    CHECK(hashmap_set(&h, "foo", 3, "bar", 3, &e) == KVC_OK);
    CHECK(hashmap_count(&h) == 1);
    CHECK(e->key_len == 3 && memcmp(e->key, "foo", 3) == 0);
    CHECK(e->value_len == 3 && memcmp(e->value, "bar", 3) == 0);

    CHECK(hashmap_get(&h, "foo", 3, &e) == KVC_OK);
    CHECK(memcmp(e->value, "bar", 3) == 0);

    /* Overwrite reuses the entry object and replaces the value. */
    kv_entry *before = e;
    CHECK(hashmap_set(&h, "foo", 3, "baz!", 4, &e) == KVC_OK);
    CHECK(e == before);
    CHECK(e->value_len == 4 && memcmp(e->value, "baz!", 4) == 0);
    CHECK(hashmap_count(&h) == 1);

    CHECK(hashmap_del(&h, "foo", 3, &e) == KVC_OK);
    CHECK(e == before);
    kv_entry_free(e);
    CHECK(hashmap_count(&h) == 0);
    CHECK(hashmap_get(&h, "foo", 3, &e) == KVC_ERR_NOTFOUND);

    hashmap_destroy(&h);
}

static void test_binary_safe_keys(void) {
    hashmap h;
    hashmap_init(&h, 4);

    /* key "a\0b" (3 bytes, embedded NUL) */
    const char key[3] = { 'a', '\0', 'b' };
    kv_entry *e = NULL;
    CHECK(hashmap_set(&h, key, 3, "v1", 2, &e) == KVC_OK);

    /* A NUL-terminated "a" must NOT match the 3-byte key. */
    CHECK(hashmap_get(&h, "a", 1, &e) == KVC_ERR_NOTFOUND);
    CHECK(hashmap_get(&h, key, 3, &e) == KVC_OK);
    CHECK(e->value_len == 2 && memcmp(e->value, "v1", 2) == 0);

    hashmap_destroy(&h);
}

static void test_rehash_and_many(void) {
    hashmap h;
    hashmap_init(&h, 4); /* forces several doubling rehashes */

    char key[32], val[32];
    enum { N = 10000 };
    for (int i = 0; i < N; i++) {
        snprintf(key, sizeof key, "key%06d", i);
        snprintf(val, sizeof val, "value%06d", i);
        kv_entry *e = NULL;
        CHECK(hashmap_set(&h, key, strlen(key), val, strlen(val), &e) == KVC_OK);
    }
    CHECK(hashmap_count(&h) == (size_t)N);

    for (int i = 0; i < N; i++) {
        snprintf(key, sizeof key, "key%06d", i);
        snprintf(val, sizeof val, "value%06d", i);
        kv_entry *e = NULL;
        CHECK(hashmap_get(&h, key, strlen(key), &e) == KVC_OK);
        CHECK(e->value_len == strlen(val) && memcmp(e->value, val, strlen(val)) == 0);
    }

    hashmap_destroy(&h);
}

struct count_ctx { size_t n; };
static void count_cb(kv_entry *e, void *ctx) {
    (void)e;
    ((struct count_ctx *)ctx)->n++;
}

static void test_foreach(void) {
    hashmap h;
    hashmap_init(&h, 4);
    for (int i = 0; i < 50; i++) {
        char key[16];
        snprintf(key, sizeof key, "k%d", i);
        kv_entry *e = NULL;
        hashmap_set(&h, key, strlen(key), "x", 1, &e);
    }
    struct count_ctx ctx = { 0 };
    hashmap_foreach(&h, count_cb, &ctx);
    CHECK(ctx.n == 50);
    hashmap_destroy(&h);
}

int main(void) {
    test_basic();
    test_binary_safe_keys();
    test_rehash_and_many();
    test_foreach();
    printf("test_hashmap: %d checks, %d failures\n", checks, failures);
    return failures ? 1 : 0;
}
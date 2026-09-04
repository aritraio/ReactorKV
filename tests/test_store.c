/* Store-level Phase 4 tests: maxmemory/allkeys-lru eviction, hit/miss/
   eviction counters, and the bounded active-expiry cycle (kv_store_expire_
   cycle). The full worker thread is exercised separately in test_expire.c;
   these tests call the store data path directly (single-threaded, so no
   lock is needed — the store's locking contract only matters once a second
   thread is in play). */

#include "kvstore/store.h"

#include <stdio.h>
#include <string.h>
#include <time.h>

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

static void msleep(long ms) {
    struct timespec ts = { .tv_sec = ms / 1000,
                           .tv_nsec = (ms % 1000) * 1000000L };
    while (nanosleep(&ts, &ts) != 0 && errno == EINTR) {}
}

static void set_str(kv_store *s, const char *key, const char *val) {
    int created = -1;
    CHECK(kv_store_set(s, key, strlen(key), val, strlen(val), &created) ==
          KVC_OK);
}

static void check_get(kv_store *s, const char *key, const char *expect) {
    const char *val = NULL;
    size_t val_len = 0;
    kvc_err rc = kv_store_get(s, key, strlen(key), &val, &val_len);
    if (expect == NULL) {
        CHECK(rc == KVC_ERR_NOTFOUND);
        return;
    }
    CHECK(rc == KVC_OK);
    if (rc == KVC_OK) {
        CHECK(val_len == strlen(expect));
        CHECK(memcmp(val, expect, val_len) == 0);
    }
}

/* ------------------------------------------------------------------ */
/* maxmemory + allkeys-lru                                            */
/* ------------------------------------------------------------------ */

/* Entries with 3-byte keys and 8-byte values fit the 64 B slab class
   (KV_ENTRY_OVERHEAD + 3 + 8 <= 64), so each live entry is exactly
   used_bytes = 64. A budget of 640 B therefore holds exactly 10 entries. */
static void test_eviction_lru(void) {
    kv_store s;
    kv_store_init(&s, 16);
    kv_store_set_maxmemory(&s, 640);

    for (int i = 0; i < 10; i++) {
        char k[8], v[16];
        snprintf(k, sizeof k, "k%02d", i);
        snprintf(v, sizeof v, "v%08d", i);
        set_str(&s, k, v);
    }
    kv_stats st;
    kv_store_stats(&s, &st);
    CHECK(st.keys == 10);         /* exactly at budget: nothing evicted */
    CHECK(st.evictions == 0);
    CHECK(st.used_bytes == 640);

    /* Touch k00 (moves it to the LRU head), then keep inserting: each new
       key pushes used over budget and the least-recently-used entry — the
       one untouched longest — must go first. */
    check_get(&s, "k00", "v00000000");
    for (int i = 10; i < 15; i++) {
        char k[8], v[16];
        snprintf(k, sizeof k, "k%02d", i);
        snprintf(v, sizeof v, "v%08d", i);
        set_str(&s, k, v);
    }
    kv_store_stats(&s, &st);
    CHECK(st.keys == 10);
    CHECK(st.evictions == 5);     /* k01..k05 evicted in LRU order */
    CHECK(st.used_bytes == 640);

    check_get(&s, "k00", "v00000000"); /* touched: survives */
    check_get(&s, "k01", NULL);        /* evicted */
    check_get(&s, "k05", NULL);        /* evicted */
    check_get(&s, "k14", "v00000014"); /* newest: survives */

    /* A single request whose footprint exceeds the whole budget is refused
       up front (can never fit) rather than evicting everything for it. */
    char big[700];
    memset(big, 'x', sizeof big);
    CHECK(kv_store_set(&s, "big", 3, big, sizeof big, NULL) == KVC_ERR_NOMEM);

    /* Without a budget the same write is fine. */
    kv_store_set_maxmemory(&s, 0);
    int created = -1;
    CHECK(kv_store_set(&s, "big", 3, big, sizeof big, &created) == KVC_OK);
    kv_store_stats(&s, &st);
    CHECK(st.keys == 11);
    CHECK(st.used_bytes > 640);

    kv_store_destroy(&s);
}

/* A value that outgrows its chunk migrates to a larger slab class; the
   store must still satisfy the same budget afterwards. Values of 700 B
   give a footprint ~760 B -> the 1024 B class. */
static void test_eviction_with_growth(void) {
    kv_store s;
    kv_store_init(&s, 16);
    kv_store_set_maxmemory(&s, 3 * 1024); /* room for exactly 3 chunks */

    char v[701]; /* 700 'a' bytes + NUL */
    memset(v, 'a', 700);
    v[700] = '\0';
    for (int i = 0; i < 6; i++) {
        char k[8];
        snprintf(k, sizeof k, "k%02d", i);
        set_str(&s, k, v); /* 6 * 1024 = 6144 B >> 3072 budget */
    }
    kv_stats st;
    kv_store_stats(&s, &st);
    CHECK(st.keys == 3);          /* k03, k04, k05 survive */
    CHECK(st.used_bytes == 3072);
    CHECK(st.evictions == 3);

    /* Growing an existing value in place (still within its 1024 B chunk)
       must not evict it — no new memory is needed. */
    char grow[301];
    memset(grow, 'b', 300);
    grow[300] = '\0';
    set_str(&s, "k05", grow);     /* 48+3+300 = 351 <= 1024 chunk */
    check_get(&s, "k05", grow);   /* live, updated in place */
    kv_store_stats(&s, &st);
    CHECK(st.keys == 3);
    CHECK(st.evictions == 3);     /* in-place grow caused no eviction */

    /* Re-inserting an evicted key needs a fresh 1024 B chunk: the budget is
       exceeded again, so the LRU tail (k03, untouched since insert) goes
       first, keeping the table at exactly 3 entries. */
    char fresh[701];
    memset(fresh, 'c', 700);
    fresh[700] = '\0';
    set_str(&s, "k00", fresh);
    kv_store_stats(&s, &st);
    CHECK(st.keys == 3);          /* k00, k04, k05 */
    CHECK(st.evictions == 4);
    CHECK(st.used_bytes == 3072);
    check_get(&s, "k00", fresh);
    check_get(&s, "k03", NULL);  /* evicted to make room */
    check_get(&s, "k04", v);     /* still holds its original 'a' value */

    kv_store_destroy(&s);
}

/* Overwriting an existing live key must not count as a new key, and must
   not allocate: the surviving entry keeps its chunk. */
static void test_overwrite_no_growth(void) {
    kv_store s;
    kv_store_init(&s, 16);
    kv_store_set_maxmemory(&s, 512);

    set_str(&s, "a", "1");
    set_str(&s, "b", "2");
    int created = -1;
    const char v15[] = "0123456789abcde"; /* 15 B: 48+1+15 = 64 B chunk */
    CHECK(kv_store_set(&s, "a", 1, v15, sizeof v15 - 1, &created) == KVC_OK);
    CHECK(created == 0);          /* overwrite, not create */
    kv_stats st;
    kv_store_stats(&s, &st);
    CHECK(st.keys == 2);
    /* a's longer value still fits its 64 B chunk: no new memory used. */
    CHECK(st.used_bytes == 128);
    CHECK(st.evictions == 0);

    check_get(&s, "a", v15);
    kv_store_destroy(&s);
}

/* ------------------------------------------------------------------ */
/* Expiry: lazy reads + the active purge cycle                        */
/* ------------------------------------------------------------------ */

static void test_expired_get_and_cycle(void) {
    kv_store s;
    kv_store_init(&s, 16);

    set_str(&s, "live", "yes");
    set_str(&s, "doomed", "soon");
    CHECK(kv_store_expire(&s, "doomed", 6, 80) == 1); /* 80 ms TTL */

    msleep(150); /* let the TTL lapse */

    /* A read of the expired key misses (logically gone) but must not
       mutate the table: the entry lingers until the active cycle purges it
       (a read never evicts). */
    const char *val = NULL;
    size_t val_len = 0;
    CHECK(kv_store_get(&s, "doomed", 6, &val, &val_len) == KVC_ERR_NOTFOUND);
    kv_stats st;
    kv_store_stats(&s, &st);
    CHECK(st.keys == 2);
    CHECK(st.misses == 1);
    CHECK(st.hits == 0);

    check_get(&s, "live", "yes");

    /* The bounded expiry cycle sweeps the table and reclaims the chunk. */
    kv_store_expire_cycle(&s, 1000);
    kv_store_stats(&s, &st);
    CHECK(st.keys == 1);
    CHECK(st.used_bytes == 64);   /* only "live" remains */
    CHECK(st.evictions == 0);     /* expiry is not eviction */

    check_get(&s, "live", "yes");
    check_get(&s, "doomed", NULL);

    /* EXPIRE on a missing key, and negative-TTL delete semantics. */
    CHECK(kv_store_expire(&s, "nope", 4, 1000) == 0);
    set_str(&s, "bye", "now");
    CHECK(kv_store_expire(&s, "bye", 3, -1) == 1); /* deletes */
    check_get(&s, "bye", NULL);
    kv_store_stats(&s, &st);
    CHECK(st.keys == 1);

    kv_store_destroy(&s);
}

/* Stats counters: hits/misses from the data path, keys across del. */
static void test_counters(void) {
    kv_store s;
    kv_store_init(&s, 16);

    set_str(&s, "a", "1");
    check_get(&s, "a", "1");
    check_get(&s, "a", "1");
    check_get(&s, "missing", NULL);

    const char *keys[] = { "a", "b" };
    const size_t lens[] = { 1, 1 };
    int deleted = -1;
    kv_store_del(&s, keys, lens, 2, &deleted);
    CHECK(deleted == 1);

    kv_stats st;
    kv_store_stats(&s, &st);
    CHECK(st.keys == 0);
    CHECK(st.hits == 2);
    CHECK(st.misses == 1);

    kv_store_destroy(&s);
}

int main(void) {
    test_eviction_lru();
    test_eviction_with_growth();
    test_overwrite_no_growth();
    test_expired_get_and_cycle();
    test_counters();
    printf("test_store: %d checks, %d failures\n", checks, failures);
    return failures ? 1 : 0;
}

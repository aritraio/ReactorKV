/* Phase 4 expiry-worker tests.
 *
 * 1. test_worker_purges_expired: keys given short TTLs are purged by the
 *    background worker (the reactor never looks at them again), while keys
 *    without TTLs survive.
 * 2. test_worker_disabled: interval <= 0 keeps the worker off.
 * 3. test_concurrent_maxmemory: a reactor-style thread holds the store
 *    read/write locks while the worker runs its cycles concurrently — the
 *    ThreadSanitizer scenario for the Phase 4 locking model. Verifies the
 *    maxmemory budget is enforced with the worker live.
 *
 * These binaries link the store core + pthread; run under `make tsan` too.
 */

#include "kvstore/expire.h"
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

/* ------------------------------------------------------------------ */

static void test_worker_purges_expired(void) {
    kv_store s;
    kv_store_init(&s, 16);

    /* k000..k099 die in 120 ms; k100..k119 live forever. */
    enum { N_DOOMED = 100, N_PERM = 20 };
    for (int i = 0; i < N_DOOMED + N_PERM; i++) {
        char k[8];
        snprintf(k, sizeof k, "k%03d", i);
        set_str(&s, k, "v");
        if (i < N_DOOMED) {
            CHECK(kv_store_expire(&s, k, strlen(k), 120) == 1);
        }
    }
    kv_stats st;
    kv_store_stats(&s, &st);
    CHECK(st.keys == N_DOOMED + N_PERM);

    expire_worker w;
    CHECK(expire_worker_start(&w, &s, 20, 0) == KVC_OK);

    /* Poll (lock-free stats snapshot) until the doomed keys are gone. */
    int purged = 0;
    for (int i = 0; i < 200; i++) { /* up to ~4 s */
        msleep(20);
        kv_store_stats(&s, &st);
        if (st.keys <= N_PERM) {
            purged = 1;
            break;
        }
    }
    expire_worker_stop(&w);
    CHECK(purged); /* the worker reclaimed the expired keys on its own */

    /* A final manual cycle makes the count exact (no stragglers). */
    for (int i = 0; i < 3; i++) kv_store_expire_cycle(&s, 100000);
    kv_store_stats(&s, &st);
    CHECK(st.keys == N_PERM);
    CHECK(st.evictions == 0); /* expiry is not eviction */

    /* Spot-check a purged key and a permanent one. */
    const char *val = NULL;
    size_t val_len = 0;
    CHECK(kv_store_get(&s, "k000", 4, &val, &val_len) == KVC_ERR_NOTFOUND);
    CHECK(kv_store_get(&s, "k100", 4, &val, &val_len) == KVC_OK);

    kv_store_destroy(&s);
}

static void test_worker_disabled(void) {
    kv_store s;
    kv_store_init(&s, 16);
    expire_worker w;
    CHECK(expire_worker_start(&w, &s, 0, 64) == KVC_OK); /* disabled */
    expire_worker_stop(&w); /* no-op: never started */
    kv_store_destroy(&s);
}

/* ------------------------------------------------------------------ */
/* Concurrency: reactor-style writer + reader vs. the worker           */
/* ------------------------------------------------------------------ */

static void test_concurrent_maxmemory(void) {
    kv_store s;
    kv_store_init(&s, 256);
    kv_store_set_maxmemory(&s, 4 * 1024 * 1024); /* 4 MiB budget */

    expire_worker w;
    CHECK(expire_worker_start(&w, &s, 5, 0) == KVC_OK);

    /* Deterministic pseudo-random walk: 4000 distinct keys, values sized
       2048 / 5120 / 9000 B (chunk classes 2048 / 8192 / 16384). Total
       demand >> budget, so the LRU policy must evict continuously while
       the worker runs its own cycles. */
    enum { ITER = 4000 };
    const size_t sizes[3] = { 2048, 5120, 9000 };
    char buf[9000];
    for (int i = 0; i < ITER; i++) {
        memset(buf, 'a' + (i % 26), sizes[i % 3]);

        char key[16];
        snprintf(key, sizeof key, "k%06d", i);
        kv_store_wrlock(&s);
        int created = -1;
        kv_store_set(&s, key, strlen(key), buf, sizes[i % 3], &created);
        kv_store_unlock(&s);

        /* Every so often read a recent key back under the read lock — the
           value pointer is only valid while the lock is held, mirroring how
           the reactor builds replies. */
        if (i % 8 == 0 && i >= 3) {
            int j = i - 3;
            char pkey[16];
            snprintf(pkey, sizeof pkey, "k%06d", j);
            const char *val = NULL;
            size_t val_len = 0;
            kv_store_rdlock(&s);
            kvc_err rc = kv_store_get(&s, pkey, strlen(pkey), &val, &val_len);
            if (rc == KVC_OK) {
                CHECK(val_len == sizes[j % 3]); /* may be evicted: skip if so */
            }
            kv_store_unlock(&s);
        }
    }

    expire_worker_stop(&w);

    kv_stats st;
    kv_store_stats(&s, &st);
    CHECK(st.evictions > 0);          /* budget forced real eviction */
    CHECK(st.used_bytes <= 4 * 1024 * 1024); /* budget enforced (smallest
                                                chunk is 2048 B, far under
                                                the budget, so eviction can
                                                always trim to it) */
    CHECK(st.keys > 0 && st.keys < ITER);

    kv_store_destroy(&s);
}

int main(void) {
    test_worker_purges_expired();
    test_worker_disabled();
    test_concurrent_maxmemory();
    printf("test_expire: %d checks, %d failures\n", checks, failures);
    return failures ? 1 : 0;
}

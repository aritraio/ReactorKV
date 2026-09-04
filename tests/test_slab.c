/* Unit tests for the Phase 3 slab allocator: size-class selection, 16-byte
   chunk alignment, lazy page growth across the free-list boundary, free
   chunk reuse, stats, the 1 MiB cap, and clean teardown. Build and run via
   `make test`. Valgrind-clean: destroy() munmap's every page and frees
   every page node. */

#include "kvstore/slab.h"

#include <stdio.h>
#include <stdint.h>
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

#define CHECK_ALIGNED(p)                                                   \
    do {                                                                   \
        void *_p = (p);                                                    \
        checks++;                                                          \
        if (_p == NULL || ((uintptr_t)_p & 15u) != 0) {                    \
            failures++;                                                    \
            fprintf(stderr, "FAIL %s:%d: %s not 16-byte aligned\n",       \
                    __FILE__, __LINE__, #p);                              \
        }                                                                  \
    } while (0)

static void test_class_selection(void) {
    CHECK(slab_class_size(0) == 64);
    CHECK(slab_class_size(1) == 64);
    CHECK(slab_class_size(64) == 64);
    CHECK(slab_class_size(65) == 128);
    CHECK(slab_class_size(100) == 128);
    CHECK(slab_class_size(128) == 128);
    CHECK(slab_class_size(129) == 256);
    CHECK(slab_class_size(512) == 512);
    CHECK(slab_class_size(65536) == 65536);
    CHECK(slab_class_size(1048576) == 1048576);
    CHECK(slab_class_size(1048577) == 0); /* past the largest class */
    CHECK(slab_class_size(SIZE_MAX) == 0);
}

static void test_alloc_alignment(void) {
    slab_allocator sa;
    CHECK(slab_allocator_init(&sa) == KVC_OK);

    /* One allocation per class (requesting just past each boundary, so
       every class is exercised) plus a mid-range request. */
    const size_t probes[] = { 1, 64, 65, 300, 2048, 2049, 100000,
                              65537, 1048576 };
    void *ptrs[sizeof probes / sizeof probes[0]];
    for (size_t i = 0; i < sizeof probes / sizeof probes[0]; i++) {
        ptrs[i] = slab_alloc(&sa, probes[i]);
        CHECK(ptrs[i] != NULL);
        CHECK_ALIGNED(ptrs[i]);
    }
    for (size_t i = 0; i < sizeof probes / sizeof probes[0]; i++) {
        slab_free(&sa, ptrs[i], probes[i]);
    }
    slab_allocator_destroy(&sa);
}

/* Allocations of the same class must reuse freed chunks (LIFO free list). */
static void test_free_list_reuse(void) {
    slab_allocator sa;
    slab_allocator_init(&sa);

    void *a = slab_alloc(&sa, 64);
    void *b = slab_alloc(&sa, 64);
    CHECK(a != NULL && b != NULL && a != b);
    CHECK_ALIGNED(a);
    CHECK_ALIGNED(b);
    slab_free(&sa, a, 64);
    slab_free(&sa, b, 64);
    void *c = slab_alloc(&sa, 64);
    CHECK(c == b); /* LIFO: the most recently freed chunk comes back first */
    slab_free(&sa, c, 64);
    slab_allocator_destroy(&sa);
}

/* The free list of a small class holds one page worth of chunks; draining
   it must lazily map a second page instead of failing. */
static void test_lazy_page_growth_and_stats(void) {
    slab_allocator sa;
    slab_allocator_init(&sa);

    size_t mapped = 0, used = 0;
    slab_stats(&sa, &mapped, &used);
    CHECK(mapped == 0 && used == 0);

    enum { N = 20000 }; /* 64 B chunks: 16384 fit in the first 1 MiB page */
    void *ptrs[N];
    for (int i = 0; i < N; i++) {
        ptrs[i] = slab_alloc(&sa, 64);
        CHECK(ptrs[i] != NULL);
        CHECK_ALIGNED(ptrs[i]);
        /* Live chunks must be writable end-to-end. */
        memset(ptrs[i], (int)(i & 0xff), 64);
    }
    slab_stats(&sa, &mapped, &used);
    CHECK(mapped >= 2 * 1024 * 1024);   /* crossed into a second page */
    CHECK(used == (size_t)N * 64);

    for (int i = 0; i < N; i++) slab_free(&sa, ptrs[i], 64);
    slab_stats(&sa, &mapped, &used);
    CHECK(used == 0);                   /* every chunk returned */
    CHECK(mapped >= 2 * 1024 * 1024);   /* pages stay mapped for reuse */

    /* The freed chunks are reused: another full round allocates without
       mapping any new pages. */
    for (int i = 0; i < N; i++) {
        ptrs[i] = slab_alloc(&sa, 64);
        CHECK(ptrs[i] != NULL);
    }
    size_t mapped2 = 0, used2 = 0;
    slab_stats(&sa, &mapped2, &used2);
    CHECK(mapped2 == mapped);
    CHECK(used2 == (size_t)N * 64);
    for (int i = 0; i < N; i++) slab_free(&sa, ptrs[i], 64);

    slab_allocator_destroy(&sa);
}

/* Allocations past 1 MiB must be rejected, not rounded to a bigger class. */
static void test_size_cap(void) {
    slab_allocator sa;
    slab_allocator_init(&sa);

    void *p = slab_alloc(&sa, SLAB_MAX_CHUNK);
    CHECK(p != NULL);
    CHECK_ALIGNED(p);
    slab_free(&sa, p, SLAB_MAX_CHUNK);

    CHECK(slab_alloc(&sa, SLAB_MAX_CHUNK + 1) == NULL);
    slab_allocator_destroy(&sa);
}

/* destroy() must cope with live chunks still outstanding. */
static void test_destroy_with_live_chunks(void) {
    slab_allocator sa;
    slab_allocator_init(&sa);

    void *ptrs[300];
    for (size_t i = 0; i < 300; i++) {
        size_t sz = (i % 3 == 0) ? 64 : (i % 3 == 1) ? 4096 : 65536;
        ptrs[i] = slab_alloc(&sa, sz);
        CHECK(ptrs[i] != NULL);
    }
    /* Free only every other chunk, then tear down with the rest live. */
    for (size_t i = 0; i < 300; i += 2) {
        size_t sz = (i % 3 == 0) ? 64 : (i % 3 == 1) ? 4096 : 65536;
        slab_free(&sa, ptrs[i], sz);
    }
    slab_allocator_destroy(&sa);

    /* A fresh allocator after destroy starts from zero. */
    CHECK(slab_allocator_init(&sa) == KVC_OK);
    size_t mapped = 99, used = 99;
    slab_stats(&sa, &mapped, &used);
    CHECK(mapped == 0 && used == 0);
    slab_allocator_destroy(&sa);
}

int main(void) {
    test_class_selection();
    test_alloc_alignment();
    test_free_list_reuse();
    test_lazy_page_growth_and_stats();
    test_size_cap();
    test_destroy_with_live_chunks();
    printf("test_slab: %d checks, %d failures\n", checks, failures);
    return failures ? 1 : 0;
}

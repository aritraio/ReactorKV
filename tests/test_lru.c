/* Unit tests for the Phase 4 embedded doubly linked LRU list (lru.h/lru.c).
   The list manipulates links only — it never allocates or frees entries —
   so entries here are plain heap kv_entry structs (sizeof excludes the
   flexible payload array, which the list never touches). */

#include "kvstore/hashmap.h"
#include "kvstore/lru.h"

#include <stdio.h>
#include <stdlib.h>
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

static kv_entry *mk_entry(void) {
    kv_entry *e = calloc(1, sizeof *e);
    if (e == NULL) {
        fprintf(stderr, "FAIL calloc\n");
        exit(1);
    }
    return e;
}

static void check_list_consistency(const lru *l, int expect_n) {
    int n = 0;
    const kv_entry *cur = l->head;
    const kv_entry *prev = NULL;
    while (cur != NULL) {
        n++;
        CHECK(cur->lru_prev == prev); /* back links line up */
        prev = cur;
        cur = cur->lru_next;
    }
    CHECK(prev == l->tail); /* last node is the tail */
    CHECK(n == expect_n);
    if (expect_n == 0) CHECK(l->head == NULL && l->tail == NULL);
}

static void test_push_front(void) {
    lru l;
    lru_init(&l);
    kv_entry *a = mk_entry();
    kv_entry *b = mk_entry();
    kv_entry *c = mk_entry();

    lru_push_front(&l, a);
    check_list_consistency(&l, 1);
    CHECK(l.head == a && l.tail == a);

    lru_push_front(&l, b);
    lru_push_front(&l, c);
    check_list_consistency(&l, 3);
    CHECK(l.head == c && l.tail == a); /* c (MRU) ... a (LRU) */

    free(a); free(b); free(c);
}

static void test_touch(void) {
    lru l;
    lru_init(&l);
    kv_entry *a = mk_entry();
    kv_entry *b = mk_entry();
    kv_entry *c = mk_entry();
    lru_push_front(&l, a);
    lru_push_front(&l, b);
    lru_push_front(&l, c);
    /* Order: c, b, a. */

    lru_touch(&l, a); /* touching the LRU tail moves it to the head */
    check_list_consistency(&l, 3);
    CHECK(l.head == a && l.tail == b);

    lru_touch(&l, a); /* touching the head is a no-op */
    check_list_consistency(&l, 3);
    CHECK(l.head == a && l.tail == b);

    free(a); free(b); free(c);
}

static void test_remove(void) {
    lru l;
    lru_init(&l);
    kv_entry *a = mk_entry();
    kv_entry *b = mk_entry();
    kv_entry *c = mk_entry();
    lru_push_front(&l, a);
    lru_push_front(&l, b);
    lru_push_front(&l, c);
    /* Order: c, b, a. */

    lru_remove(&l, b); /* middle */
    check_list_consistency(&l, 2);
    CHECK(l.head == c && l.tail == a);
    CHECK(b->lru_prev == NULL && b->lru_next == NULL); /* unlinked */

    lru_remove(&l, c); /* head */
    check_list_consistency(&l, 1);
    CHECK(l.head == a && l.tail == a);

    lru_remove(&l, a); /* sole remaining node */
    check_list_consistency(&l, 0);
    CHECK(lru_tail(&l) == NULL);

    free(a); free(b); free(c);
}

/* push/remove interleaving must leave the list correct (prev/next wiring). */
static void test_interleave(void) {
    lru l;
    lru_init(&l);
    enum { N = 8 };
    kv_entry *e[N];
    for (int i = 0; i < N; i++) e[i] = mk_entry();

    for (int i = 0; i < N; i++) lru_push_front(&l, e[i]);
    check_list_consistency(&l, N);

    /* Reorder by touching pairs, removing some along the way. */
    lru_touch(&l, e[0]);
    lru_remove(&l, e[3]);
    lru_touch(&l, e[5]);
    lru_remove(&l, e[7]);
    lru_touch(&l, e[1]);
    check_list_consistency(&l, N - 2);
    CHECK(l.head == e[1]);
    CHECK(lru_tail(&l) == e[6] || lru_tail(&l) == e[4] || lru_tail(&l) == e[2]);

    for (int i = 0; i < N; i++) {
        if (i != 3 && i != 7) {
            lru_remove(&l, e[i]);
            CHECK(e[i]->lru_prev == NULL && e[i]->lru_next == NULL);
        }
    }
    check_list_consistency(&l, 0);
    CHECK(lru_tail(&l) == NULL);

    for (int i = 0; i < N; i++) free(e[i]);
}

int main(void) {
    test_push_front();
    test_touch();
    test_remove();
    test_interleave();
    printf("test_lru: %d checks, %d failures\n", checks, failures);
    return failures ? 1 : 0;
}

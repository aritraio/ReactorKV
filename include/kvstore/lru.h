#ifndef KVC_LRU_H
#define KVC_LRU_H

/*
 * lru.h — embedded doubly linked LRU list (Phase 4).
 *
 * Recency is tracked *inside* each kv_entry (lru_prev / lru_next fields in
 * hashmap.h), so the list costs no per-entry allocations and moves are O(1).
 * Head is the most recently used entry; tail is the least recently used one.
 *
 * The list only manipulates links; it never allocates or frees entries.
 * Ownership of the link state follows entry lifetime: kv_store hands its
 * `lru` root to the hashmap at init, and the hashmap maintains membership as
 * entries are created, migrated, deleted, or freed (all entry-lifecycle
 * transitions already live in hashmap.c).  The store layer touches the list
 * only to implement policy: move-to-head on GET and pop-tail on eviction.
 *
 * Concurrency: NOT thread-safe.  The store rwlock serializes access (reads
 * on GET, writes everywhere else), so all list mutations happen under that
 * lock.  See store.h for the locking contract.
 */

#include "hashmap.h" /* kv_entry (lru_prev / lru_next live here) */

typedef struct lru {
    struct kv_entry *head; /* most recently used */
    struct kv_entry *tail; /* least recently used */
} lru;

void lru_init(lru *l);

/* Insert `e` at the head.  `e` must not already be linked. */
void lru_push_front(lru *l, kv_entry *e);

/* Move an already-linked `e` to the head (no-op if already head). */
void lru_touch(lru *l, kv_entry *e);

/* Unlink `e` (no free).  `e` must be linked. */
void lru_remove(lru *l, kv_entry *e);

/* Least-recently-used entry, or NULL when empty. */
kv_entry *lru_tail(const lru *l);

#endif /* KVC_LRU_H */

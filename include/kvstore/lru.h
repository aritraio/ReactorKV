#ifndef KVC_LRU_H
#define KVC_LRU_H

/*
 * lru.h — embedded doubly linked LRU list (Phase 4).
 *
 * Recency is tracked *inside* each kv_entry (lru_prev / lru_next fields in
 * hashmap.h), so the list costs no per-entry allocations and moves are O(1).
 * Head is the most recently used entry; tail is the least recently used.
 *
 * The list only manipulates links — it never allocates or frees entries.
 * Membership follows entry lifetime: the store attaches its `lru` root to
 * the hashmap (hashmap_set_lru), and the hashmap maintains membership as
 * entries are created, migrated, deleted, or freed.  The store touches the
 * list on GET hits and pops the tail when evicting.
 *
 * The kv_entry definition lives in hashmap.h; only the struct tag is used
 * here so the two headers stay acyclic (no typedef duplication).
 */

struct kv_entry;

typedef struct lru {
    struct kv_entry *head; /* most recently used */
    struct kv_entry *tail; /* least recently used */
} lru;

void lru_init(lru *l);

/* Insert `e` at the head.  `e` must not already be linked. */
void lru_push_front(lru *l, struct kv_entry *e);

/* Move an already-linked `e` to the head (no-op if already head). */
void lru_touch(lru *l, struct kv_entry *e);

/* Unlink `e` (does not free it).  `e` must be linked. */
void lru_remove(lru *l, struct kv_entry *e);

/* Least-recently-used entry, or NULL when empty. */
struct kv_entry *lru_tail(const lru *l);

#endif /* KVC_LRU_H */

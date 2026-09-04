#include "kvstore/lru.h"

void lru_init(lru *l) {
    l->head = NULL;
    l->tail = NULL;
}

void lru_push_front(lru *l, kv_entry *e) {
    e->lru_prev = NULL;
    e->lru_next = l->head;
    if (l->head != NULL) {
        l->head->lru_prev = e;
    } else {
        l->tail = e; /* list was empty */
    }
    l->head = e;
}

void lru_remove(lru *l, kv_entry *e) {
    if (e->lru_prev != NULL) {
        e->lru_prev->lru_next = e->lru_next;
    } else {
        l->head = e->lru_next;
    }
    if (e->lru_next != NULL) {
        e->lru_next->lru_prev = e->lru_prev;
    } else {
        l->tail = e->lru_prev;
    }
    e->lru_prev = NULL;
    e->lru_next = NULL;
}

void lru_touch(lru *l, kv_entry *e) {
    if (l->head == e) return; /* already most recent */
    lru_remove(l, e);
    lru_push_front(l, e);
}

kv_entry *lru_tail(const lru *l) {
    return l->tail;
}

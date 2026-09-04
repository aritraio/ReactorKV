#ifndef KVC_SLAB_H
#define KVC_SLAB_H

/*
 * slab.h — fixed-size-class slab allocator (Phase 3).
 *
 * Replaces per-entry heap allocations in the hot path with chunks carved
 * from mmap'd slab pages. Requests are rounded up to one of SLAB_CLASS_COUNT
 * power-of-two size classes; each class owns a singly linked free list of
 * chunks (the free-list pointer lives inside the free chunk itself) and a
 * linked list of the pages it has mapped. Pages are grown lazily: a class
 * only mmap's a new page when its free list runs out, and slab_destroy
 * walks the page lists so every mapping is munmap'd cleanly.
 *
 * Concurrency: not thread-safe. The reactor is single-threaded through
 * Phase 3; Phase 4 serializes the store with a rwlock before touching it.
 *
 * Chunk alignment: pages come from mmap (page-aligned) and every chunk
 * size is a power of two >= 64, so every chunk is 16-byte aligned.
 *
 * Note on class count: the Phase plan documents "SLAB_CLASS_COUNT 16",
 * but 64 B .. 1 MiB contains 15 powers of two (2^6 .. 2^20). The size
 * classes below are the source of truth, so SLAB_CLASS_COUNT is 15.
 */

#include "common.h"

/* Size classes: powers of two from 64 B to 1 MiB. */
#define SLAB_MIN_CHUNK 64u
#define SLAB_MAX_CHUNK (1024u * 1024u)
#define SLAB_CLASS_COUNT 15

/* Each slab page mmap's roughly this many bytes of chunks. 1 MiB is a
   large multiple of any OS page size and keeps mmap churn negligible;
   the largest class (1 MiB chunks) maps exactly one chunk per page. */
#define SLAB_PAGE_TARGET (1024u * 1024u)

/* One mmap'd region of chunks, plus the bookkeeping needed to tear it
   down. Pages are kept on a per-class singly linked list. */
typedef struct slab_page {
    struct slab_page *next;   /* next page of the same class */
    void             *mem;    /* mmap'd chunk area (page-aligned) */
    size_t            mem_len;/* bytes mapped (munmap needs the length) */
} slab_page;

typedef struct slab_class {
    size_t     chunk_size;      /* bytes per chunk (a power of two) */
    size_t     chunks_per_page; /* chunks carved out of each new page */
    void      *free_head;       /* free list; next pointer stored in chunk */
    slab_page *pages;           /* pages of this class, for teardown */
    size_t     total_chunks;    /* chunks ever handed to the free list */
    size_t     free_chunks;     /* chunks currently on the free list */
} slab_class;

struct slab_allocator {
    slab_class classes[SLAB_CLASS_COUNT];
};

typedef struct slab_allocator slab_allocator;

kvc_err slab_allocator_init(slab_allocator *sa);
/* munmap every page of every class and free page bookkeeping. */
void    slab_allocator_destroy(slab_allocator *sa);

/* Allocate a chunk of at least `size` bytes from the smallest class that
   fits; grows the class by mapping a new page only when its free list is
   empty. Returns NULL if `size` exceeds the largest class or mmap fails.
   The caller must free with slab_free(sa, ptr, size) using the SAME size. */
void   *slab_alloc(slab_allocator *sa, size_t size);

/* Return a chunk to its class's free list. `size` must equal the size
   passed to slab_alloc (it selects the class — the allocator never scans
   page lists on the free path). */
void    slab_free(slab_allocator *sa, void *ptr, size_t size);

/* Aggregate stats: total mmap'd bytes and bytes of live (non-free) chunks
   (chunk sizes, so internal fragmentation is included). */
void    slab_stats(const slab_allocator *sa, size_t *total_mapped,
                   size_t *total_used);

/* The smallest class size that can hold `size` bytes — i.e. the exact
   chunk size slab_alloc will grant for a request of `size`. Returns 0
   when `size` exceeds the largest class. Callers that must know a
   chunk's size up front (e.g. the store, to record the size for free and
   to decide whether an overwrite fits in place) use this. */
size_t  slab_class_size(size_t size);

#endif /* KVC_SLAB_H */

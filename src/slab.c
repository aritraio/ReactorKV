#include "kvstore/slab.h"

/* The build pins -D_POSIX_C_SOURCE=200809L, under which the libc headers
   hide the anonymous-mapping flags (non-POSIX): MAP_ANONYMOUS on glibc,
   MAP_ANON on Darwin. Re-expose the platform's flag before any system
   header so pages stay truly anonymous (newer macOS rejects /dev/zero
   mappings, so that classic fallback is not viable). */
#if defined(__APPLE__)
#define _DARWIN_C_SOURCE 1
#elif defined(__linux__)
#define _DEFAULT_SOURCE 1
#endif

#include "kvstore/slab.h"

#include <sys/mman.h>

#ifndef MAP_ANONYMOUS
#define MAP_ANONYMOUS MAP_ANON
#endif

/* Chunk sizes are SLAB_MIN_CHUNK << i for i in [0, SLAB_CLASS_COUNT). */
static size_t chunk_size(int idx) {
    return (size_t)SLAB_MIN_CHUNK << (unsigned)idx;
}

/* Index of the smallest class with chunk size >= `size`, or -1 when
   `size` exceeds the largest class. A request of 0 is served by the
   smallest class. */
static int class_index(size_t size) {
    if (size > SLAB_MAX_CHUNK) return -1;
    int idx = 0;
    size_t cs = SLAB_MIN_CHUNK;
    while (size > cs && idx < SLAB_CLASS_COUNT - 1) {
        cs <<= 1;
        idx++;
    }
    return idx;
}

size_t slab_class_size(size_t size) {
    int idx = class_index(size);
    return idx < 0 ? 0 : chunk_size(idx);
}

kvc_err slab_allocator_init(slab_allocator *sa) {
    memset(sa, 0, sizeof *sa);
    for (int i = 0; i < SLAB_CLASS_COUNT; i++) {
        slab_class *c = &sa->classes[i];
        c->chunk_size = chunk_size(i);
        /* Round up so each page carries at least SLAB_PAGE_TARGET bytes
           (every class size divides a power of two exactly, so pages are
           an exact multiple of chunk_size: no wasted tail). */
        c->chunks_per_page =
            (SLAB_PAGE_TARGET + c->chunk_size - 1) / c->chunk_size;
    }
    return KVC_OK;
}

/* mmap a fresh page of chunks for class `c`, push every chunk onto the
   free list (ascending addresses, so allocation walks memory forward),
   and record the page for teardown. Returns NULL if mmap fails. */
static slab_page *class_grow(slab_class *c) {
    size_t len = c->chunk_size * c->chunks_per_page;
    void *mem = mmap(NULL, len, PROT_READ | PROT_WRITE,
                     MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (mem == MAP_FAILED) {
        kvc_log(KVC_LOG_ERR, "mmap(%zu) failed: %s", len, strerror(errno));
        return NULL;
    }
    slab_page *pg = kvc_calloc(1, sizeof *pg);
    pg->mem = mem;
    pg->mem_len = len;
    pg->next = c->pages;
    c->pages = pg;

    /* Carve chunks high-to-low while pushing onto the front of the free
       list, so the resulting list runs low-to-high. Free chunks are only
       touched on the free path, so storing the next pointer at offset 0
       never clobbers live payloads. */
    for (size_t i = c->chunks_per_page; i > 0; i--) {
        void *chunk = (unsigned char *)mem + (i - 1) * c->chunk_size;
        *(void **)chunk = c->free_head;
        c->free_head = chunk;
    }
    c->total_chunks += c->chunks_per_page;
    c->free_chunks += c->chunks_per_page;
    return pg;
}

void *slab_alloc(slab_allocator *sa, size_t size) {
    int idx = class_index(size);
    if (idx < 0) return NULL; /* size > largest class: rejected */
    slab_class *c = &sa->classes[idx];
    if (c->free_head == NULL) {
        if (class_grow(c) == NULL) return NULL;
    }
    void *chunk = c->free_head;
    c->free_head = *(void **)chunk;
    c->free_chunks--;
    return chunk;
}

void slab_free(slab_allocator *sa, void *ptr, size_t size) {
    if (ptr == NULL) return;
    int idx = class_index(size);
    if (idx < 0) {
        /* Misuse: the size does not map to any class. Silently dropping
           the chunk would leak it; log loudly instead. */
        kvc_log(KVC_LOG_ERR, "slab_free: size %zu has no size class", size);
        return;
    }
    slab_class *c = &sa->classes[idx];
    *(void **)ptr = c->free_head;
    c->free_head = ptr;
    c->free_chunks++;
}

void slab_allocator_destroy(slab_allocator *sa) {
    for (int i = 0; i < SLAB_CLASS_COUNT; i++) {
        slab_class *c = &sa->classes[i];
        slab_page *pg = c->pages;
        while (pg != NULL) {
            slab_page *next = pg->next;
            if (pg->mem != NULL) {
                (void)munmap(pg->mem, pg->mem_len);
            }
            free(pg);
            pg = next;
        }
        c->pages = NULL;
        c->free_head = NULL;
        c->total_chunks = 0;
        c->free_chunks = 0;
    }
}

void slab_stats(const slab_allocator *sa, size_t *total_mapped,
                size_t *total_used) {
    size_t mapped = 0;
    size_t used = 0;
    for (int i = 0; i < SLAB_CLASS_COUNT; i++) {
        const slab_class *c = &sa->classes[i];
        for (const slab_page *pg = c->pages; pg != NULL; pg = pg->next) {
            mapped += pg->mem_len;
        }
        size_t live = c->total_chunks - c->free_chunks;
        used += live * c->chunk_size;
    }
    *total_mapped = mapped;
    *total_used = used;
}

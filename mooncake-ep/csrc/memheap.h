#ifndef _IBGDA_MEMHEAP_H_
#define _IBGDA_MEMHEAP_H_

#include <stddef.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdalign.h>

#include "os.h"

struct memheap {
    size_t size;
    pthread_mutex_t lock;
    size_t allocated;
};

static inline struct memheap *memheap_create(size_t size) {
    struct memheap *heap = (struct memheap *)malloc(sizeof(struct memheap));
    if (!heap) {
        return NULL;
    }
    heap->size = size;
    heap->allocated = 0;
    mutex_init(&heap->lock);
    return heap;
}

static inline void memheap_destroy(struct memheap *heap) {
    if (heap) {
        mutex_destroy(&heap->lock);
        free(heap);
    }
}

static inline size_t memheap_aligned_alloc(struct memheap *heap, size_t size,
                                           size_t align) {
    if (size == 0) {
        return 0;  // No allocation for zero size
    }
    if (align == 0 || (align & (align - 1)) != 0) {
        errno = EINVAL;  // Invalid alignment
        return -1;
    }
    size_t ret = -1;
    mutex_lock(&heap->lock);
    size_t offset = heap->allocated;
    if (offset & (align - 1)) {
        offset = (offset | (align - 1)) + 1;
    }
    if (offset + size <= heap->size) {
        ret = offset;
        heap->allocated = offset + size;
    } else {
        errno = ENOMEM;  // Not enough memory
    }
    mutex_unlock(&heap->lock);
    return ret;
}

static inline size_t memheap_alloc(struct memheap *heap, size_t size) {
    size_t align = size & -size;
    if (align > alignof(max_align_t)) {
        align = alignof(max_align_t);
    }
    return memheap_aligned_alloc(heap, size, align);
}

static inline void memheap_free(struct memheap *heap, size_t offset) {
    // currently no-op
}

#endif

#ifndef _IBGDA_MEMHEAP_H_
#define _IBGDA_MEMHEAP_H_

#include <stddef.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdalign.h>
#include <stdbool.h>
#include <errno.h>

#include "os.h"

#define MEMHEAP_MAX_ALLOCATIONS 1024

struct memheap_allocation {
    size_t offset;
    size_t size;
    bool used;
};

struct memheap {
    size_t size;
    pthread_mutex_t lock;
    size_t allocated;
    struct memheap_allocation allocs[MEMHEAP_MAX_ALLOCATIONS];
    int alloc_count;
};

static inline struct memheap* memheap_create(size_t size) {
    struct memheap* heap = (struct memheap*)malloc(sizeof(struct memheap));
    if (!heap) {
        return NULL;
    }
    heap->size = size;
    heap->allocated = 0;
    heap->alloc_count = 0;
    mutex_init(&heap->lock);
    return heap;
}

static inline void memheap_destroy(struct memheap* heap) {
    if (heap) {
        mutex_destroy(&heap->lock);
        free(heap);
    }
}

static inline size_t memheap_aligned_alloc(struct memheap* heap, size_t size,
                                           size_t align) {
    if (size == 0) {
        return (size_t)-1;  // No allocation for zero size
    }
    if (align == 0 || (align & (align - 1)) != 0) {
        errno = EINVAL;  // Invalid alignment
        return (size_t)-1;
    }

    mutex_lock(&heap->lock);

    size_t ret = (size_t)-1;

    for (int i = 0; i < heap->alloc_count; i++) {
        if (!heap->allocs[i].used) {
            size_t offset = heap->allocs[i].offset;
            size_t block_size = heap->allocs[i].size;

            size_t aligned_offset = offset;
            if (aligned_offset & (align - 1)) {
                aligned_offset = (aligned_offset | (align - 1)) + 1;
            }

            if (aligned_offset + size <= offset + block_size) {
                if (aligned_offset > offset) {
                    int new_idx = heap->alloc_count;
                    if (new_idx < MEMHEAP_MAX_ALLOCATIONS) {
                        heap->allocs[new_idx].offset = offset;
                        heap->allocs[new_idx].size = aligned_offset - offset;
                        heap->allocs[new_idx].used = false;
                        heap->alloc_count++;
                    }
                }

                if (aligned_offset + size < offset + block_size) {
                    int new_idx = heap->alloc_count;
                    if (new_idx < MEMHEAP_MAX_ALLOCATIONS) {
                        heap->allocs[new_idx].offset = aligned_offset + size;
                        heap->allocs[new_idx].size =
                            offset + block_size - (aligned_offset + size);
                        heap->allocs[new_idx].used = false;
                        heap->alloc_count++;
                    }
                }

                heap->allocs[i].offset = aligned_offset;
                heap->allocs[i].size = size;
                heap->allocs[i].used = true;

                ret = aligned_offset;
                heap->allocated += size;
                break;
            }
        }
    }

    if (ret == (size_t)-1) {
        size_t offset = heap->allocated;
        if (offset & (align - 1)) {
            offset = (offset | (align - 1)) + 1;
        }
        if (offset + size <= heap->size) {
            ret = offset;

            if (heap->alloc_count < MEMHEAP_MAX_ALLOCATIONS) {
                heap->allocs[heap->alloc_count].offset = offset;
                heap->allocs[heap->alloc_count].size = size;
                heap->allocs[heap->alloc_count].used = true;
                heap->alloc_count++;
            }

            heap->allocated = offset + size;
        } else {
            errno = ENOMEM;
        }
    }

    mutex_unlock(&heap->lock);
    return ret;
}

static inline size_t memheap_alloc(struct memheap* heap, size_t size) {
    size_t align = size & -size;
    if (align > alignof(max_align_t)) {
        align = alignof(max_align_t);
    }
    if (align < 8) align = 8;
    return memheap_aligned_alloc(heap, size, align);
}

static inline void memheap_free(struct memheap* heap, size_t offset) {
    if (!heap || offset == (size_t)-1) {
        return;
    }

    mutex_lock(&heap->lock);

    for (int i = 0; i < heap->alloc_count; i++) {
        if (heap->allocs[i].used && heap->allocs[i].offset == offset) {
            heap->allocs[i].used = false;
            heap->allocated -= heap->allocs[i].size;
            break;
        }
    }

    mutex_unlock(&heap->lock);
}

#endif
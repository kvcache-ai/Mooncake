#ifndef _IBGDA_MEMHEAP_H_
#define _IBGDA_MEMHEAP_H_

#include <stddef.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdalign.h>
#include <stdbool.h>
#include <errno.h>

#include "os.h"

struct memheap_block {
    size_t size;           
    bool used;               
    struct memheap_block *next;
    struct memheap_block *prev; 
};

struct memheap {
    size_t size;                   
    size_t allocated;               
    pthread_mutex_t lock;           
    void *base;                      
    struct memheap_block *free_list;
};

#define MEMHEAP_ALIGN(size, align) \
    (((size) + (align) - 1) & ~((align) - 1))

#define MEMHEAP_DEFAULT_ALIGN sizeof(max_align_t)

static inline struct memheap *memheap_create(size_t size) {
    if (size < sizeof(struct memheap_block) * 2) {
        return NULL;
    }
    
    struct memheap *heap = (struct memheap *)malloc(sizeof(struct memheap));
    if (!heap) {
        return NULL;
    }

    heap->base = malloc(size);
    if (!heap->base) {
        free(heap);
        return NULL;
    }
    
    heap->size = size;
    heap->allocated = 0;
    mutex_init(&heap->lock);

    heap->free_list = (struct memheap_block *)heap->base;
    heap->free_list->size = size;
    heap->free_list->used = false;
    heap->free_list->next = NULL;
    heap->free_list->prev = NULL;
    
    return heap;
}

static inline void memheap_destroy(struct memheap *heap) {
    if (heap) {
        mutex_destroy(&heap->lock);
        if (heap->base) {
            free(heap->base);
        }
        free(heap);
    }
}

static inline void memheap_coalesce(struct memheap *heap) {
    struct memheap_block *curr = heap->free_list;
    
    while (curr && curr->next) {
        char *curr_end = (char *)curr + curr->size;
        struct memheap_block *next_block = (struct memheap_block *)curr_end;
        
        if (next_block == curr->next) {
            curr->size += curr->next->size;
            curr->next = curr->next->next;
            if (curr->next) {
                curr->next->prev = curr;
            }
        } else {
            curr = curr->next;
        }
    }
}

static inline void *memheap_offset_to_ptr(struct memheap *heap, size_t offset) {
    if (offset == (size_t)-1 || offset >= heap->size) {
        return NULL;
    }
    return (void *)((char *)heap->base + offset);
}

static inline size_t memheap_ptr_to_offset(struct memheap *heap, void *ptr) {
    if (ptr < heap->base || ptr >= (void *)((char *)heap->base + heap->size)) {
        return (size_t)-1;
    }
    return (size_t)((char *)ptr - (char *)heap->base);
}

static inline size_t memheap_aligned_alloc(struct memheap *heap, size_t size,
                                           size_t align) {
    if (size == 0) {
        return (size_t)-1;
    }

    if (align == 0 || (align & (align - 1)) != 0) {
        errno = EINVAL;
        return (size_t)-1;
    }

    if (align < MEMHEAP_DEFAULT_ALIGN) {
        align = MEMHEAP_DEFAULT_ALIGN;
    }

    size_t total_size = size + sizeof(struct memheap_block);
    
    mutex_lock(&heap->lock);
    
    struct memheap_block *best_fit = NULL;
    size_t min_waste = SIZE_MAX;

    struct memheap_block *curr = heap->free_list;
    while (curr) {
        if (!curr->used) {
            char *data_start = (char *)curr + sizeof(struct memheap_block);
            uintptr_t addr = (uintptr_t)data_start;
            uintptr_t aligned_addr = (addr + align - 1) & ~(align - 1);
            size_t padding = aligned_addr - addr;
            
            size_t required_size = total_size + padding;
            
            if (curr->size >= required_size) {
                size_t waste = curr->size - required_size;
                if (waste < min_waste) {
                    min_waste = waste;
                    best_fit = curr;
                }
            }
        }
        curr = curr->next;
    }
    
    size_t result_offset = (size_t)-1;
    
    if (best_fit) {
        char *data_start = (char *)best_fit + sizeof(struct memheap_block);
        uintptr_t addr = (uintptr_t)data_start;
        uintptr_t aligned_addr = (addr + align - 1) & ~(align - 1);
        size_t padding = aligned_addr - addr;

        struct memheap_block *alloc_block = (struct memheap_block *)(aligned_addr - sizeof(struct memheap_block));

        if (padding >= sizeof(struct memheap_block)) {
            struct memheap_block *padding_block = (struct memheap_block *)((char *)best_fit + padding);
            padding_block->size = best_fit->size - padding;
            padding_block->used = false;
            padding_block->next = best_fit->next;
            padding_block->prev = best_fit->prev;

            if (padding_block->prev) {
                padding_block->prev->next = padding_block;
            }
            if (padding_block->next) {
                padding_block->next->prev = padding_block;
            }
            if (best_fit == heap->free_list) {
                heap->free_list = padding_block;
            }

            best_fit->size = padding;
            best_fit->used = false;

            best_fit = padding_block;
            alloc_block = (struct memheap_block *)((char *)best_fit + sizeof(struct memheap_block));
            aligned_addr = (uintptr_t)((char *)best_fit + sizeof(struct memheap_block));
            padding = 0;
        }
        
        if (best_fit != alloc_block) {
            size_t alloc_size = total_size;
            size_t remaining_size = best_fit->size - padding - alloc_size;
            
            if (remaining_size >= sizeof(struct memheap_block)) {
                struct memheap_block *remaining = (struct memheap_block *)((char *)alloc_block + alloc_size);
                remaining->size = remaining_size;
                remaining->used = false;
                remaining->next = best_fit->next;
                remaining->prev = best_fit->prev;
                
                if (remaining->prev) {
                    remaining->prev->next = remaining;
                }
                if (remaining->next) {
                    remaining->next->prev = remaining;
                }
                if (best_fit == heap->free_list) {
                    heap->free_list = remaining;
                }
                
                alloc_block->size = alloc_size;
                alloc_block->used = true;
            } else {
                alloc_block->size = best_fit->size - padding;
                alloc_block->used = true;

                if (best_fit->prev) {
                    best_fit->prev->next = best_fit->next;
                }
                if (best_fit->next) {
                    best_fit->next->prev = best_fit->prev;
                }
                if (best_fit == heap->free_list) {
                    heap->free_list = best_fit->next;
                }
            }
        } else {
            best_fit->used = true;
            
            if (best_fit->prev) {
                best_fit->prev->next = best_fit->next;
            }
            if (best_fit->next) {
                best_fit->next->prev = best_fit->prev;
            }
            if (best_fit == heap->free_list) {
                heap->free_list = best_fit->next;
            }
            
            if (padding > 0) {
                struct memheap_block *padding_block = (struct memheap_block *)((char *)best_fit);
                padding_block->size = padding;
                padding_block->used = false;
                padding_block->next = heap->free_list;
                padding_block->prev = NULL;
                if (heap->free_list) {
                    heap->free_list->prev = padding_block;
                }
                heap->free_list = padding_block;
                
                alloc_block = (struct memheap_block *)((char *)best_fit + padding);
                alloc_block->size = best_fit->size - padding;
                alloc_block->used = true;
            }
        }
        
        void *ptr = (void *)((char *)alloc_block + sizeof(struct memheap_block));
        result_offset = memheap_ptr_to_offset(heap, ptr);
        heap->allocated += alloc_block->size;
    } else {
        errno = ENOMEM;
    }
    
    mutex_unlock(&heap->lock);
    return result_offset;
}

static inline size_t memheap_alloc(struct memheap *heap, size_t size) {
    return memheap_aligned_alloc(heap, size, MEMHEAP_DEFAULT_ALIGN);
}

static inline void memheap_free(struct memheap *heap, size_t offset) {
    if (!heap || offset == (size_t)-1 || offset >= heap->size) {
        return;
    }
    
    mutex_lock(&heap->lock);
    
    void *ptr = memheap_offset_to_ptr(heap, offset);
    struct memheap_block *block = (struct memheap_block *)((char *)ptr - sizeof(struct memheap_block));
    
    if ((void *)block >= heap->base && 
        (void *)((char *)block + block->size) <= (void *)((char *)heap->base + heap->size) &&
        block->used) {
        
        block->used = false;
        heap->allocated -= block->size;
        
        block->next = heap->free_list;
        block->prev = NULL;
        if (heap->free_list) {
            heap->free_list->prev = block;
        }
        heap->free_list = block;
        
        memheap_coalesce(heap);
    }
    
    mutex_unlock(&heap->lock);
}

static inline size_t memheap_allocated_size(struct memheap *heap) {
    size_t allocated;
    mutex_lock(&heap->lock);
    allocated = heap->allocated;
    mutex_unlock(&heap->lock);
    return allocated;
}

static inline size_t memheap_total_size(struct memheap *heap) {
    return heap->size;
}

#endif
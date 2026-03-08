#ifndef _IBGDA_MEMHEAP_H_
#define _IBGDA_MEMHEAP_H_

#include <stddef.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdalign.h>
#include <stdbool.h>
#include <errno.h>
#include <string.h>

#include "os.h"

/* 内存块头部结构 */
struct memheap_block {
    size_t size;                    /* 块大小（包括头部） */
    bool used;                      /* 是否已使用 */
    struct memheap_block *next;      /* 下一个块（用于遍历） */
};

/* 内存堆结构 */
struct memheap {
    size_t size;                     /* 总大小 */
    size_t allocated;                /* 已分配大小（统计用） */
    pthread_mutex_t lock;             /* 互斥锁 */
    void *base;                       /* 内存基址 */
    struct memheap_block *first_block; /* 第一个内存块 */
};

/* 获取块的数据区起始地址 */
static inline void *memheap_block_data(struct memheap_block *block) {
    return (void *)((char *)block + sizeof(struct memheap_block));
}

/* 获取数据区对应的块头部 */
static inline struct memheap_block *memheap_block_from_data(void *data) {
    return (struct memheap_block *)((char *)data - sizeof(struct memheap_block));
}

/* 对齐宏 */
#define MEMHEAP_ALIGN(size, align) \
    (((size) + (align) - 1) & ~((align) - 1))

/* 默认对齐 */
#define MEMHEAP_DEFAULT_ALIGN sizeof(max_align_t)

static inline struct memheap *memheap_create(size_t size) {
    /* 确保至少能容纳一个块 */
    if (size < sizeof(struct memheap_block) + 1) {
        return NULL;
    }
    
    struct memheap *heap = (struct memheap *)malloc(sizeof(struct memheap));
    if (!heap) {
        return NULL;
    }
    
    /* 分配实际内存池 */
    heap->base = malloc(size);
    if (!heap->base) {
        free(heap);
        return NULL;
    }
    
    heap->size = size;
    heap->allocated = 0;
    mutex_init(&heap->lock);
    
    /* 初始化：创建一个空闲块覆盖整个内存池 */
    heap->first_block = (struct memheap_block *)heap->base;
    heap->first_block->size = size;
    heap->first_block->used = false;
    heap->first_block->next = NULL;
    
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

/* 查找合适的空闲块（首次适应算法） */
static inline struct memheap_block *memheap_find_block(struct memheap *heap, 
                                                       size_t size, 
                                                       size_t align) {
    struct memheap_block *curr = heap->first_block;
    
    while (curr) {
        if (!curr->used) {
            /* 计算在这个块中，数据区对齐后的起始地址 */
            void *data_start = memheap_block_data(curr);
            uintptr_t addr = (uintptr_t)data_start;
            uintptr_t aligned_addr = (addr + align - 1) & ~(align - 1);
            size_t padding = aligned_addr - addr;
            
            /* 检查是否足够空间（包括对齐填充） */
            if (curr->size >= size + padding + sizeof(struct memheap_block)) {
                return curr;
            }
        }
        curr = curr->next;
    }
    
    return NULL;
}

/* 分割内存块 */
static inline void memheap_split_block(struct memheap *heap,
                                       struct memheap_block *block,
                                       size_t size,
                                       size_t align) {
    /* 计算对齐后的数据区起始地址 */
    void *data_start = memheap_block_data(block);
    uintptr_t addr = (uintptr_t)data_start;
    uintptr_t aligned_addr = (addr + align - 1) & ~(align - 1);
    size_t padding = aligned_addr - addr;
    
    /* 如果有填充，创建填充块 */
    if (padding >= sizeof(struct memheap_block)) {
        struct memheap_block *padding_block = (struct memheap_block *)((char *)block + padding);
        padding_block->size = block->size - padding;
        padding_block->used = false;
        padding_block->next = block->next;
        
        block->size = padding;
        block->next = padding_block;
        block = padding_block;
        
        /* 重新计算对齐 */
        data_start = memheap_block_data(block);
        addr = (uintptr_t)data_start;
        aligned_addr = (addr + align - 1) & ~(align - 1);
        padding = aligned_addr - addr;
    }
    
    /* 计算分配块的位置 */
    struct memheap_block *alloc_block = (struct memheap_block *)((char *)block + padding);
    size_t alloc_size = size + sizeof(struct memheap_block);
    size_t remaining_size = block->size - padding - alloc_size;
    
    if (remaining_size >= sizeof(struct memheap_block)) {
        /* 有剩余空间，创建剩余块 */
        struct memheap_block *remaining = (struct memheap_block *)((char *)alloc_block + alloc_size);
        remaining->size = remaining_size;
        remaining->used = false;
        remaining->next = block->next;
        
        /* 设置分配块 */
        alloc_block->size = alloc_size;
        alloc_block->used = true;
        alloc_block->next = remaining;
        
        /* 更新前一个块的next指针 */
        if (block == heap->first_block) {
            heap->first_block = alloc_block;
        } else {
            struct memheap_block *prev = heap->first_block;
            while (prev && prev->next != block) {
                prev = prev->next;
            }
            if (prev) {
                prev->next = alloc_block;
            }
        }
    } else {
        /* 剩余空间太小，整个块都给分配块 */
        alloc_block->size = block->size - padding;
        alloc_block->used = true;
        alloc_block->next = block->next;
        
        /* 更新前一个块的next指针 */
        if (block == heap->first_block) {
            heap->first_block = alloc_block;
        } else {
            struct memheap_block *prev = heap->first_block;
            while (prev && prev->next != block) {
                prev = prev->next;
            }
            if (prev) {
                prev->next = alloc_block;
            }
        }
    }
}

/* 偏移量转指针 */
static inline void *memheap_offset_to_ptr(struct memheap *heap, size_t offset) {
    if (offset == (size_t)-1 || offset >= heap->size) {
        return NULL;
    }
    return (void *)((char *)heap->base + offset);
}

/* 指针转偏移量 */
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
    
    /* 检查对齐参数 */
    if (align == 0 || (align & (align - 1)) != 0) {
        errno = EINVAL;
        return (size_t)-1;
    }
    
    /* 确保对齐至少为默认对齐 */
    if (align < MEMHEAP_DEFAULT_ALIGN) {
        align = MEMHEAP_DEFAULT_ALIGN;
    }
    
    mutex_lock(&heap->lock);
    
    /* 查找合适的空闲块 */
    struct memheap_block *block = memheap_find_block(heap, size, align);
    size_t result_offset = (size_t)-1;
    
    if (block) {
        /* 分割块并分配 */
        memheap_split_block(heap, block, size, align);
        
        /* 找到分配的块 */
        struct memheap_block *curr = heap->first_block;
        while (curr) {
            if (curr->used) {
                void *data = memheap_block_data(curr);
                /* 验证这是我们要找的块 */
                result_offset = memheap_ptr_to_offset(heap, data);
                heap->allocated += curr->size;
                break;
            }
            curr = curr->next;
        }
    } else {
        errno = ENOMEM;
    }
    
    mutex_unlock(&heap->lock);
    return result_offset;
}

static inline size_t memheap_alloc(struct memheap *heap, size_t size) {
    return memheap_aligned_alloc(heap, size, MEMHEAP_DEFAULT_ALIGN);
}

/* 合并相邻的空闲块 */
static inline void memheap_coalesce(struct memheap *heap) {
    struct memheap_block *curr = heap->first_block;
    
    while (curr && curr->next) {
        if (!curr->used && !curr->next->used) {
            /* 合并相邻的空闲块 */
            curr->size += curr->next->size;
            curr->next = curr->next->next;
        } else {
            curr = curr->next;
        }
    }
}

static inline void memheap_free(struct memheap *heap, size_t offset) {
    if (!heap || offset == (size_t)-1 || offset >= heap->size) {
        return;
    }
    
    mutex_lock(&heap->lock);
    
    /* 获取数据区指针 */
    void *ptr = memheap_offset_to_ptr(heap, offset);
    
    /* 获取块头部 */
    struct memheap_block *block = memheap_block_from_data(ptr);
    
    /* 简单的验证：检查指针范围 */
    if ((void *)block >= heap->base && 
        (void *)((char *)block + block->size) <= (void *)((char *)heap->base + heap->size)) {
        
        if (block->used) {
            block->used = false;
            heap->allocated -= block->size;
            
            /* 合并相邻的空闲块 */
            memheap_coalesce(heap);
        }
    }
    
    mutex_unlock(&heap->lock);
}

/* 获取已分配大小 */
static inline size_t memheap_allocated_size(struct memheap *heap) {
    size_t allocated;
    mutex_lock(&heap->lock);
    allocated = heap->allocated;
    mutex_unlock(&heap->lock);
    return allocated;
}

/* 获取总大小 */
static inline size_t memheap_total_size(struct memheap *heap) {
    return heap->size;
}

#endif
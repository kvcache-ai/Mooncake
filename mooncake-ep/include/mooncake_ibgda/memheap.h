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

#define MEMHEAP_MAX_BLOCKS 1024  /* 最大块数量 */

/* 内存块记录 */
struct memheap_block_record {
    size_t offset;  /* 块起始偏移 */
    size_t size;    /* 块大小 */
    bool used;      /* 是否已使用 */
};

/* 内存堆结构 */
struct memheap {
    size_t size;                          /* 总大小 */
    pthread_mutex_t lock;                  /* 互斥锁 */
    size_t allocated;                      /* 已分配大小（统计用） */
    struct memheap_block_record blocks[MEMHEAP_MAX_BLOCKS];  /* 块记录数组 */
    int block_count;                        /* 当前块数量 */
};

static inline struct memheap *memheap_create(size_t size) {
    struct memheap *heap = (struct memheap *)malloc(sizeof(struct memheap));
    if (!heap) {
        return NULL;
    }
    
    heap->size = size;
    heap->allocated = 0;
    heap->block_count = 1;  /* 初始一个空闲块 */
    heap->blocks[0].offset = 0;
    heap->blocks[0].size = size;
    heap->blocks[0].used = false;
    
    mutex_init(&heap->lock);
    return heap;
}

static inline void memheap_destroy(struct memheap *heap) {
    if (heap) {
        mutex_destroy(&heap->lock);
        free(heap);
    }
}

/* 查找合适的空闲块（首次适应） */
static inline int memheap_find_free_block(struct memheap *heap, size_t size, 
                                          size_t align, size_t *offset) {
    for (int i = 0; i < heap->block_count; i++) {
        if (!heap->blocks[i].used) {
            size_t block_offset = heap->blocks[i].offset;
            size_t block_size = heap->blocks[i].size;
            
            /* 计算对齐后的偏移 */
            size_t aligned_offset = block_offset;
            if (aligned_offset & (align - 1)) {
                aligned_offset = (aligned_offset | (align - 1)) + 1;
            }
            
            /* 计算对齐需要的额外空间 */
            size_t padding = aligned_offset - block_offset;
            
            /* 检查是否足够 */
            if (padding + size <= block_size) {
                *offset = aligned_offset;
                return i;
            }
        }
    }
    return -1;
}

/* 插入新的块记录 */
static inline void memheap_insert_block(struct memheap *heap, int index,
                                        size_t offset, size_t size, bool used) {
    if (heap->block_count >= MEMHEAP_MAX_BLOCKS) {
        return;  /* 超过最大块数，忽略 */
    }
    
    /* 向后移动元素 */
    for (int i = heap->block_count; i > index; i--) {
        heap->blocks[i] = heap->blocks[i - 1];
    }
    
    /* 插入新块 */
    heap->blocks[index].offset = offset;
    heap->blocks[index].size = size;
    heap->blocks[index].used = used;
    heap->block_count++;
}

/* 删除块记录 */
static inline void memheap_remove_block(struct memheap *heap, int index) {
    /* 向前移动元素 */
    for (int i = index; i < heap->block_count - 1; i++) {
        heap->blocks[i] = heap->blocks[i + 1];
    }
    heap->block_count--;
}

/* 合并相邻的空闲块 */
static inline void memheap_merge_blocks(struct memheap *heap) {
    for (int i = 0; i < heap->block_count - 1; i++) {
        if (!heap->blocks[i].used && !heap->blocks[i + 1].used) {
            /* 检查是否相邻 */
            if (heap->blocks[i].offset + heap->blocks[i].size == 
                heap->blocks[i + 1].offset) {
                /* 合并 */
                heap->blocks[i].size += heap->blocks[i + 1].size;
                memheap_remove_block(heap, i + 1);
                i--;  /* 重新检查这个位置 */
            }
        }
    }
}

static inline size_t memheap_aligned_alloc(struct memheap *heap, size_t size,
                                           size_t align) {
    if (size == 0) {
        return (size_t)-1;  /* 使用-1表示无效偏移 */
    }
    
    if (align == 0 || (align & (align - 1)) != 0) {
        errno = EINVAL;
        return (size_t)-1;
    }
    
    mutex_lock(&heap->lock);
    
    size_t offset = (size_t)-1;
    int block_index = memheap_find_free_block(heap, size, align, &offset);
    
    if (block_index >= 0) {
        size_t block_offset = heap->blocks[block_index].offset;
        size_t block_size = heap->blocks[block_index].size;
        size_t padding = offset - block_offset;
        
        if (padding > 0) {
            /* 有对齐填充，在分配块前插入一个填充块 */
            memheap_insert_block(heap, block_index, block_offset, padding, false);
            block_index++;  /* 分配块索引后移 */
        }
        
        if (padding + size < block_size) {
            /* 分配后还有剩余空间，在分配块后插入剩余块 */
            size_t remaining_offset = offset + size;
            size_t remaining_size = block_size - padding - size;
            memheap_insert_block(heap, block_index + 1, remaining_offset, 
                                 remaining_size, false);
        }
        
        /* 标记分配块为已使用 */
        heap->blocks[block_index].offset = offset;
        heap->blocks[block_index].size = size;
        heap->blocks[block_index].used = true;
        
        heap->allocated += size;
    } else {
        errno = ENOMEM;
    }
    
    mutex_unlock(&heap->lock);
    return offset;
}

static inline size_t memheap_alloc(struct memheap *heap, size_t size) {
    /* 简单的对齐策略：使用默认对齐 */
    return memheap_aligned_alloc(heap, size, alignof(max_align_t));
}

static inline void memheap_free(struct memheap *heap, size_t offset) {
    if (!heap || offset == (size_t)-1 || offset >= heap->size) {
        return;
    }
    
    mutex_lock(&heap->lock);
    
    /* 查找对应的块 */
    for (int i = 0; i < heap->block_count; i++) {
        if (heap->blocks[i].used && heap->blocks[i].offset == offset) {
            heap->blocks[i].used = false;
            heap->allocated -= heap->blocks[i].size;
            
            /* 合并相邻的空闲块 */
            memheap_merge_blocks(heap);
            break;
        }
    }
    
    mutex_unlock(&heap->lock);
}

/* 调试函数：打印内存块状态 */
static inline void memheap_dump(struct memheap *heap) {
    mutex_lock(&heap->lock);
    printf("Memory heap (size=%zu, allocated=%zu, blocks=%d):\n", 
           heap->size, heap->allocated, heap->block_count);
    for (int i = 0; i < heap->block_count; i++) {
        printf("  Block %d: offset=%zu, size=%zu, %s\n", i,
               heap->blocks[i].offset, heap->blocks[i].size,
               heap->blocks[i].used ? "used" : "free");
    }
    mutex_unlock(&heap->lock);
}

#endif
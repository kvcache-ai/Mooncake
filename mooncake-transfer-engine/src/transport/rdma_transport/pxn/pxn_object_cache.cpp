// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#include "transport/rdma_transport/pxn/pxn_object_cache.h"

#include "transport/rdma_transport/pxn/pxn_types.h"

namespace mooncake {
namespace pxn {

// ============================================================
// PxnCopyWorkerCache
// ============================================================

PxnCopyWorkerCache::PxnCopyWorkerCache() {
    pxn_cache_.resize(kPxnCopyWorkerCacheCapacity);
}

PxnCopyWorkerCache::~PxnCopyWorkerCache() {
    for (uint64_t idx = pxn_tail_; idx != pxn_head_; ++idx) {
        delete pxn_cache_[idx % kPxnCopyWorkerCacheCapacity];
    }
}

PxnCopyWorker* PxnCopyWorkerCache::PxnAllocate() {
    PxnCopyWorker* cw;
    bool from_cache;
    if (pxn_head_ - pxn_tail_ == 0) {
        cw = new PxnCopyWorker();
        from_cache = false;
    } else {
        cw = pxn_cache_[pxn_tail_ % kPxnCopyWorkerCacheCapacity];
        pxn_tail_++;
        from_cache = true;
    }

    cw->pxn_rdma_workers.clear();
    cw->pxn_offsets_in_slot.clear();
    cw->pxn_seg_ranges.clear();
    cw->pxn_cumulative_size = 0;
    cw->pxn_slot_pool = nullptr;
    cw->pxn_state.store(PxnCopyWorker::kPending, std::memory_order_relaxed);
    cw->pxn_refcount.store(0, std::memory_order_relaxed);
    cw->pxn_task_refcount.store(0, std::memory_order_relaxed);
    cw->pxn_cascade_next = nullptr;
    cw->pxn_from_cache = from_cache;
    return cw;
}

void PxnCopyWorkerCache::PxnDeallocate(PxnCopyWorker* cw) {
    if (cw == nullptr) return;
    for (auto *range : cw->pxn_seg_ranges) {
        PxnGetSegRangeCache().PxnDeallocate(range);
    }
    cw->pxn_seg_ranges.clear();
    if (pxn_head_ - pxn_tail_ == kPxnCopyWorkerCacheCapacity) {
        delete cw;
        return;
    }
    pxn_cache_[pxn_head_ % kPxnCopyWorkerCacheCapacity] = cw;
    pxn_head_++;
}

thread_local static PxnCopyWorkerCache tl_pxn_copy_worker_cache;

PxnCopyWorkerCache& PxnGetCopyWorkerCache() {
    return tl_pxn_copy_worker_cache;
}

// ============================================================
// PxnSegRangeCache
// ============================================================

PxnSegRangeCache::PxnSegRangeCache() {
    pxn_cache_.resize(kPxnSegRangeCacheCapacity);
}

PxnSegRangeCache::~PxnSegRangeCache() {
    for (uint64_t idx = pxn_tail_; idx != pxn_head_; ++idx) {
        delete pxn_cache_[idx % kPxnSegRangeCacheCapacity];
    }
}

PxnSegRange* PxnSegRangeCache::PxnAllocate() {
    PxnSegRange* range;
    if (pxn_head_ - pxn_tail_ == 0) {
        range = new PxnSegRange();
    } else {
        range = pxn_cache_[pxn_tail_ % kPxnSegRangeCacheCapacity];
        pxn_tail_++;
    }
    range->pxn_src_start = 0;
    range->pxn_src_end = 0;
    range->pxn_offset_in_slot = 0;
    range->pxn_from_cache = true;
    return range;
}

void PxnSegRangeCache::PxnDeallocate(PxnSegRange* range) {
    if (range == nullptr) return;
    if (pxn_head_ - pxn_tail_ == kPxnSegRangeCacheCapacity) {
        delete range;
        return;
    }
    pxn_cache_[pxn_head_ % kPxnSegRangeCacheCapacity] = range;
    pxn_head_++;
}

thread_local static PxnSegRangeCache tl_pxn_seg_range_cache;

PxnSegRangeCache& PxnGetSegRangeCache() {
    return tl_pxn_seg_range_cache;
}

}  // namespace pxn
}  // namespace mooncake

// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#ifndef MOONCAKE_PXN_OBJECT_CACHE_H_
#define MOONCAKE_PXN_OBJECT_CACHE_H_

#include <cstddef>
#include <cstdint>
#include <vector>

namespace mooncake {
namespace pxn {

struct PxnCopyWorker;
struct PxnSegRange;

// ============================================================
// PxnCopyWorkerCache
// ============================================================
class PxnCopyWorkerCache {
   public:
    static constexpr size_t kPxnCopyWorkerCacheCapacity = 4096;

    PxnCopyWorkerCache();
    ~PxnCopyWorkerCache();

    PxnCopyWorkerCache(const PxnCopyWorkerCache&) = delete;
    PxnCopyWorkerCache& operator=(const PxnCopyWorkerCache&) = delete;

    PxnCopyWorker* PxnAllocate();
    void PxnDeallocate(PxnCopyWorker* cw);

   private:
    std::vector<PxnCopyWorker*> pxn_cache_;
    uint64_t pxn_head_ = 0;
    uint64_t pxn_tail_ = 0;
};

PxnCopyWorkerCache& PxnGetCopyWorkerCache();

// ============================================================
// PxnSegRangeCache
// ============================================================
class PxnSegRangeCache {
   public:
    static constexpr size_t kPxnSegRangeCacheCapacity = 4096;

    PxnSegRangeCache();
    ~PxnSegRangeCache();

    PxnSegRangeCache(const PxnSegRangeCache&) = delete;
    PxnSegRangeCache& operator=(const PxnSegRangeCache&) = delete;

    PxnSegRange* PxnAllocate();
    void PxnDeallocate(PxnSegRange* range);

   private:
    std::vector<PxnSegRange*> pxn_cache_;
    uint64_t pxn_head_ = 0;
    uint64_t pxn_tail_ = 0;
};

PxnSegRangeCache& PxnGetSegRangeCache();

}  // namespace pxn
}  // namespace mooncake

#endif  // MOONCAKE_PXN_OBJECT_CACHE_H_

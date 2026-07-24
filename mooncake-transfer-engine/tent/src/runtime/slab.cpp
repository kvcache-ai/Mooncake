// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "tent/runtime/slab.h"

#include <atomic>
#include <unordered_map>

namespace mooncake {
namespace tent {

static std::atomic<int> g_next_slab_index(0);
thread_local std::unordered_map<int, SlabBase::ThreadLocal> tl_slice_set;

SlabBase::SlabBase(size_t block_size) : block_size_(block_size) {
    slab_index_ = g_next_slab_index.fetch_add(1);
}

void *SlabBase::allocate() {
    // Fast path: check if destroyed without lock
    if (destroyed_.load(std::memory_order_acquire)) {
        return nullptr;
    }

    // Try thread-local cache first (lock-free fast path)
    auto &tl_slice = tl_slice_set[slab_index_];
    void *object = nullptr;
    if (tl_slice.dequeue(object)) return object;

    // Slow path: cache is empty, need to acquire lock for refill
    std::lock_guard<std::mutex> global_lock(mutex_);

    // Check again after acquiring lock (destructor might have run)
    if (destroyed_.load(std::memory_order_relaxed)) {
        return nullptr;
    }

    // Try thread-local cache again (another thread might have refilled)
    if (tl_slice.dequeue(object)) return object;

    // Refill from global free list
    while (!tl_slice.full() && !free_list_.empty()) {
        tl_slice.enqueue(free_list_.front());
        free_list_.pop_front();
    }

    // Allocate new slab if needed
    if (tl_slice.empty()) {
        void *slab = malloc(kAllocateBatchSize * block_size_);
        if (!slab) return nullptr;
        alloc_list_.push_back(slab);
        for (size_t i = 0; i < kAllocateBatchSize; ++i)
            tl_slice.enqueue((char *)slab + i * block_size_);
    }

    tl_slice.dequeue(object);
    return object;
}

void SlabBase::deallocate(void *object) {
    if (!object) return;

    // Fast path: check if destroyed without lock
    if (destroyed_.load(std::memory_order_acquire)) {
        return;
    }

    auto &tl_slice = tl_slice_set[slab_index_];

    // Lock-free fast path: try to return to thread-local cache if not full
    if (!tl_slice.full()) {
        tl_slice.enqueue(object);
        return;
    }

    // Slow path: cache is full, need to flush to global list
    std::lock_guard<std::mutex> global_lock(mutex_);

    // Check again after acquiring lock
    if (destroyed_.load(std::memory_order_relaxed)) {
        return;
    }

    // Flush thread-local cache to global free list
    while (!tl_slice.empty()) {
        void *obj = nullptr;
        tl_slice.dequeue(obj);
        free_list_.push_back(obj);
    }
    tl_slice.enqueue(object);
}

}  // namespace tent
}  // namespace mooncake
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
    auto &tl_slice = tl_slice_set[slab_index_];
    void *object = nullptr;
    if (tl_slice.dequeue(object)) return object;

    std::lock_guard<std::mutex> global_lock(mutex_);
    while (!tl_slice.full() && !free_list_.empty()) {
        tl_slice.enqueue(free_list_.front());
        free_list_.pop_front();
    }

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
    auto &tl_slice = tl_slice_set[slab_index_];
    if (tl_slice.full()) {
        std::lock_guard<std::mutex> global_lock(mutex_);
        while (!tl_slice.empty()) {
            void *object = nullptr;
            tl_slice.dequeue(object);
            free_list_.push_back(object);
        }
    }
    tl_slice.enqueue(object);
}

}  // namespace tent
}  // namespace mooncake
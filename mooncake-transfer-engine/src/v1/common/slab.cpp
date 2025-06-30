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

#include "v1/common/slab.h"

#include <unordered_map>

namespace mooncake {
namespace v1 {

thread_local std::unordered_map<SlabBase *, SlabBase::ThreadLocal> tl_slice_set;

void *SlabBase::allocate() {
    auto &tl_slice = tl_slice_set[this];
    if (!tl_slice.free_list.empty()) {
        void *object = tl_slice.free_list.front();
        tl_slice.free_list.pop_front();
        return object;
    }

    std::lock_guard<std::mutex> global_lock(mutex_);
    if (free_list_.size() >= kAllocateBatchSize) {
        tl_slice.free_list.splice(
            tl_slice.free_list.end(), free_list_, free_list_.begin(),
            std::next(free_list_.begin(), kAllocateBatchSize));
    } else {
        tl_slice.free_list.splice(tl_slice.free_list.end(), free_list_);
    }

    if (tl_slice.free_list.empty()) {
        void *slab = malloc(kAllocateBatchSize * block_size_);
        if (!slab) return nullptr;
        alloc_list_.push_back(slab);
        for (size_t i = 0; i < kAllocateBatchSize; ++i)
            tl_slice.free_list.push_back((char *)slab + i * block_size_);
    }

    void *object = tl_slice.free_list.front();
    tl_slice.free_list.pop_front();
    return object;
}

void SlabBase::deallocate(void *object) {
    auto &tl_slice = tl_slice_set[this];
    tl_slice.free_list.push_back(object);
    if (tl_slice.free_list.size() > kMaxFreeListSizeInThread &&
        mutex_.try_lock()) {
        while (!tl_slice.free_list.empty()) {
            auto target = tl_slice.free_list.back();
            tl_slice.free_list.pop_back();
            free_list_.push_back(target);
        }
        mutex_.unlock();
    }
}

}  // namespace v1
}  // namespace mooncake
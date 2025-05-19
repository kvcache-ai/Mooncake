// Copyright 2025 KVCache.AI
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

#include "transport_v1/rdma/slice.h"

#include <cassert>

namespace mooncake {
namespace v1 {

thread_local RdmaSliceStorage::ThreadLocal tl_slice;

RdmaSliceStorage& RdmaSliceStorage::Get() {
    static RdmaSliceStorage g_slice;
    return g_slice;
}

RdmaSliceStorage::RdmaSliceStorage() {}

RdmaSliceStorage::~RdmaSliceStorage() {
    for (auto entry : alloc_list_) delete entry;
    alloc_list_.clear();
    free_list_.clear();
}

RdmaSlice* RdmaSliceStorage::allocate() {
    if (!tl_slice.free_list.empty()) {
        RdmaSlice* slice = tl_slice.free_list.front();
        tl_slice.free_list.pop_front();
        return slice;
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
        RdmaSlice* slices = new RdmaSlice[kAllocateBatchSize];
        alloc_list_.push_back(slices);
        for (size_t i = 0; i < kAllocateBatchSize; ++i)
            tl_slice.free_list.push_back(&slices[i]);
    }

    RdmaSlice* slice = tl_slice.free_list.front();
    tl_slice.free_list.pop_front();
    return slice;
}

void RdmaSliceStorage::deallocate(RdmaSlice* slice) {
    tl_slice.free_list.push_back(slice);
    if (tl_slice.free_list.size() > kMaxFreeListSizeInThread && mutex_.try_lock()) {
        while (!tl_slice.free_list.empty()) {
            RdmaSlice* slice = tl_slice.free_list.back();
            tl_slice.free_list.pop_back();
            free_list_.push_back(slice);
        }
        mutex_.unlock();
    }
}

}  // namespace v1
}  // namespace mooncake
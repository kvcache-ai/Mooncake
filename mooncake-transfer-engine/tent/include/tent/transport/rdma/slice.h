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

#ifndef TENT_SLICE_H
#define TENT_SLICE_H

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <list>
#include <mutex>
#include <new>
#include <thread>
#include <type_traits>
#include <vector>

#include "tent/runtime/transport.h"
#include "tent/runtime/slab.h"

namespace mooncake {
namespace tent {
struct RdmaSlice;

struct RdmaSliceList {
    RdmaSlice* first = nullptr;
    int num_slices = 0;
};

struct RdmaTask {
    int num_slices;
    Request request;
    volatile TransferStatusEnum status_word;
    volatile size_t transferred_bytes;
    volatile int success_slices;
    volatile int resolved_slices;
    volatile TransferStatusEnum first_error = PENDING;
};

class RdmaEndPoint;

struct RdmaSlice {
    void* source_addr = nullptr;
    uint64_t target_addr = 0;
    size_t length = 0;

    RdmaTask* task = nullptr;
    RdmaSlice* next = nullptr;

    uint32_t source_lkey = 0;
    uint32_t target_rkey = 0;
    int source_dev_id = -1;
    int target_dev_id = -1;

    RdmaEndPoint* ep_weak_ptr = nullptr;
    TransferStatusEnum word = TransferStatusEnum::INITIAL;
    int qp_index = 0;
    int retry_count = 0;
    bool failed = false;
    uint64_t enqueue_ts = 0;
    uint64_t submit_ts = 0;
};

using RdmaSliceStorage = Slab<RdmaSlice>;

static inline void updateSliceStatus(RdmaSlice* slice,
                                     TransferStatusEnum status) {
    if (status == PENDING) return;
    RdmaTask* task = slice->task;
    if (!__sync_bool_compare_and_swap(&slice->word, PENDING, status)) return;
    if (status == COMPLETED) {
        __sync_fetch_and_add(&task->transferred_bytes, slice->length);
        __sync_fetch_and_add(&task->success_slices, 1);
    } else {
        __sync_bool_compare_and_swap(&task->first_error, PENDING, status);
    }
    int resolved = __sync_add_and_fetch(&task->resolved_slices, 1);
    if (resolved >= task->num_slices) {
        TransferStatusEnum final_st = (task->success_slices == task->num_slices)
                                          ? COMPLETED
                                          : task->first_error;
        if (final_st == PENDING) final_st = FAILED;
        __sync_bool_compare_and_swap(&task->status_word, PENDING, final_st);
    }
}

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_SLICE_H
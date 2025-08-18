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

#ifndef RDMA_SLICE_H_
#define RDMA_SLICE_H_

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

#include "v1/transport/transport.h"
#include "v1/memory/slab.h"

namespace mooncake {
namespace v1 {
struct RdmaSlice;

struct RdmaSliceList {
    RdmaSlice *first = nullptr;
    int num_slices = 0;
};

struct RdmaTask {
    int num_slices;
    Request request;
    volatile TransferStatusEnum status_word;
    volatile size_t transferred_bytes;
    uint64_t padding1[8];

    volatile int success_slices = 0;
    volatile int failed_slices = 0;
    uint64_t padding2[8];
};

struct RdmaSlice {
    void *source_addr = nullptr;
    uint64_t target_addr = 0;
    size_t length = 0;

    RdmaTask *task = nullptr;
    RdmaSlice *next = nullptr;

    uint32_t source_lkey = 0;
    uint32_t target_rkey = 0;
    int source_dev_id = -1;
    int target_dev_id = -1;

    int retry_count = 0;
    volatile int *endpoint_quota = nullptr;
    bool failed = false;
};

using RdmaSliceStorage = Slab<RdmaSlice>;

}  // namespace v1
}  // namespace mooncake

#endif  // RDMA_SLICE_H_
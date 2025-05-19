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

#include "transport_v1/transport.h"

namespace mooncake {
namespace v1 {
struct RdmaSlice;

struct RdmaSliceList {
    RdmaSlice *first = nullptr;
    int num_slices = 0;
};

struct RdmaTask {
    int num_slices;
    Transport::Request request;
    Transport::TransferStatus status;
    uint64_t padding1[8];

    volatile int finish_slices = 0;  // including success or failed
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

class RdmaSliceStorage {
   public:
    static RdmaSliceStorage &Get();

   public:
    RdmaSliceStorage();

    ~RdmaSliceStorage();

    RdmaSlice *allocate();

    void deallocate(RdmaSlice *slice);

    struct ThreadLocal {
        std::list<RdmaSlice *> free_list;
    };

   private:
    std::mutex mutex_;
    std::list<RdmaSlice *> free_list_;
    std::list<RdmaSlice *> alloc_list_;

    static const size_t kMaxFreeListSizeInThread = 128;
    static const size_t kAllocateBatchSize = 128;

    RdmaSliceStorage(const RdmaSliceStorage &) = delete;
    RdmaSliceStorage &operator=(const RdmaSliceStorage &) = delete;
};
}  // namespace v1
}  // namespace mooncake

#endif  // RDMA_SLICE_H_
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

#ifndef CUFILE_DESC_POOL_H_
#define CUFILE_DESC_POOL_H_

#include <cufile.h>

#include <atomic>
#include <bitset>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <vector>

#include "transfer_engine.h"

namespace mooncake {
class CUFileDescPool {
   public:
    explicit CUFileDescPool();
    ~CUFileDescPool();

    CUFileDescPool(const CUFileDescPool &) = delete;
    CUFileDescPool &operator=(const CUFileDescPool &) = delete;
    CUFileDescPool(CUFileDescPool &&) = delete;

    int allocCUfileDesc(size_t batch_size);  // ret: (desc_idx, start_idx)

    int pushParams(int idx, CUfileIOParams_t &io_params);

    int submitBatch(int idx);

    CUfileIOEvents_t getTransferStatus(int idx, int slice_id);

    int getSliceNum(int idx);

    int freeCUfileDesc(int idx);

   private:
    static const size_t MAX_NR_CUFILE_DESC = 16;
    static const size_t MAX_CUFILE_BATCH_SIZE = 128;
    thread_local static int thread_index;
    static std::atomic<int> index_counter;
    // 1. indicates whether a file descriptor is available
    std::atomic<uint64_t> occupied_[MAX_NR_CUFILE_DESC];
    // 2. cufile desc array
    CUfileBatchHandle_t handle_[MAX_NR_CUFILE_DESC];
    // 3. start idx
    int start_idx_[MAX_NR_CUFILE_DESC];
    // 4. IO Params and IO Status
    std::vector<CUfileIOParams_t> io_params_[MAX_NR_CUFILE_DESC];
    std::vector<CUfileIOEvents_t> io_events_[MAX_NR_CUFILE_DESC];

    RWSpinlock mutex_;
};

}  // namespace mooncake

#endif
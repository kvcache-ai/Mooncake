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

#include "transport/nvmeof_transport/cufile_desc_pool.h"

#include <bits/stdint-uintn.h>

#include <atomic>
#include <cstddef>
#include <mutex>

#include "cufile.h"
#include "transfer_engine.h"
#include "transport/nvmeof_transport/cufile_context.h"

namespace mooncake {
thread_local int CUFileDescPool::thread_index = -1;
std::atomic<int> CUFileDescPool::index_counter(0);

CUFileDescPool::CUFileDescPool() {
    for (size_t i = 0; i < MAX_NR_CUFILE_DESC; ++i) {
        handle_[i] = NULL;
        io_params_[i].reserve(MAX_CUFILE_BATCH_SIZE);
        io_events_[i].resize(MAX_CUFILE_BATCH_SIZE);
        start_idx_[i] = 0;
        occupied_[i].store(0, std::memory_order_relaxed);
        CUFILE_CHECK(cuFileBatchIOSetUp(&handle_[i], MAX_CUFILE_BATCH_SIZE));
    }
}

CUFileDescPool::~CUFileDescPool() {
    for (size_t i = 0; i < MAX_NR_CUFILE_DESC; ++i) {
        cuFileBatchIODestroy(handle_[i]);
    }
}

int CUFileDescPool::allocCUfileDesc(size_t batch_size) {
    if (batch_size > MAX_CUFILE_BATCH_SIZE) {
        LOG(ERROR) << "Batch Size Exceeds Max CUFile Batch Size";
        return -1;
    }
    if (thread_index == -1) {
        thread_index = index_counter.fetch_add(1);
    }

    int idx = thread_index % MAX_NR_CUFILE_DESC;
    uint64_t old = 0;
    if (!occupied_[idx].compare_exchange_strong(old, thread_index)) {
        LOG(INFO) << "No Batch Descriptor Available ";
        return -1;
    }
    return idx;
}

int CUFileDescPool::pushParams(int idx, CUfileIOParams_t &io_params) {
    auto &params = io_params_[idx];
    if (params.size() >= params.capacity()) {
        return -1;
    }
    params.push_back(io_params);
    return 0;
}

int CUFileDescPool::submitBatch(int idx) {
    auto &params = io_params_[idx];
    // LOG(INFO) << "submit " << idx;
    CUFILE_CHECK(cuFileBatchIOSubmit(handle_[idx],
                                     params.size() - start_idx_[idx],
                                     params.data() + start_idx_[idx], 0));
    start_idx_[idx] = params.size();
    return 0;
}

CUfileIOEvents_t CUFileDescPool::getTransferStatus(int idx, int slice_id) {
    unsigned nr = io_params_[idx].size();
    // TODO: optimize this & fix start
    CUFILE_CHECK(cuFileBatchIOGetStatus(handle_[idx], 0, &nr,
                                        io_events_[idx].data(), NULL));
    return io_events_[idx][slice_id];
}

int CUFileDescPool::getSliceNum(int idx) {
    auto &params = io_params_[idx];
    return params.size();
}

int CUFileDescPool::freeCUfileDesc(int idx) {
    occupied_[idx].store(0, std::memory_order_relaxed);
    io_params_[idx].clear();
    start_idx_[idx] = 0;
    // memset(io_events_[idx].data(), 0, io_events_[idx].size() *
    // sizeof(CUfileIOEvents_t));
    return 0;
}
}  // namespace mooncake
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

#include <glog/logging.h>

#include <cstddef>
#include <mutex>

#include "cufile.h"
#include "transport/nvmeof_transport/cufile_context.h"

namespace mooncake {

CUFileDescPool::CUFileDescPool(size_t max_batch_size)
    : max_batch_size_(max_batch_size) {
    // Initialize descriptor array
    for (size_t i = 0; i < MAX_NR_DESC; ++i) {
        descs_[i] = nullptr;
        occupied_[i].store(false, std::memory_order_relaxed);
    }
}

CUFileDescPool::~CUFileDescPool() {
    // Clean up all handles in the pool
    std::lock_guard<std::mutex> lock(handle_pool_lock_);
    for (auto *batch_handle : handle_pool_) {
        cuFileBatchIODestroy(batch_handle->handle);
        delete batch_handle;
    }
    handle_pool_.clear();

    // Clean up all descriptors
    for (size_t i = 0; i < MAX_NR_DESC; ++i) {
        if (descs_[i] != nullptr) {
            delete descs_[i];
            descs_[i] = nullptr;
        }
    }
}

int CUFileDescPool::allocCUfileDesc(size_t batch_size) {
    if (batch_size > max_batch_size_) {
        LOG(ERROR) << "Batch Size " << batch_size
                   << " Exceeds Max CUFile Batch Size " << max_batch_size_;
        return -1;
    }

    RWSpinlock::WriteGuard guard(mutex_);

    // Find a free slot
    int idx = -1;
    for (size_t i = 0; i < MAX_NR_DESC; ++i) {
        bool expected = false;
        if (occupied_[i].compare_exchange_strong(expected, true,
                                                   std::memory_order_acq_rel)) {
            idx = i;
            break;
        }
    }

    if (idx < 0) {
        LOG(ERROR) << "No Batch Descriptor Available";
        return -1;
    }

    // Create new descriptor with independent io_params/io_events
    auto *desc = new CUFileBatchDesc();

    // Get or create BatchHandle from pool (lazy loading)
    BatchHandle* batch_handle = nullptr;
    {
        std::lock_guard<std::mutex> lock(handle_pool_lock_);
        if (!handle_pool_.empty()) {
            batch_handle = handle_pool_.back();
            handle_pool_.pop_back();
        }
    }

    // If pool is empty, create new handle (expensive operation)
    if (!batch_handle) {
        batch_handle = new BatchHandle();
        batch_handle->max_nr = max_batch_size_;
        // cuFileBatchIOSetUp is time-costly, so we reuse handles
        CUFILE_CHECK(cuFileBatchIOSetUp(&batch_handle->handle, max_batch_size_));
    }

    desc->batch_handle = batch_handle;
    desc->io_params.clear();
    desc->io_params.reserve(max_batch_size_);
    desc->io_events.resize(max_batch_size_);
    desc->slice_count = 0;

    descs_[idx] = desc;
    return idx;
}

int CUFileDescPool::pushParams(int idx, CUfileIOParams_t &io_params) {
    if (idx < 0 || idx >= (int)MAX_NR_DESC || descs_[idx] == nullptr) {
        LOG(ERROR) << "Invalid descriptor index: " << idx;
        return -1;
    }

    auto *desc = descs_[idx];
    if (desc->io_params.size() >= desc->io_params.capacity()) {
        LOG(ERROR) << "Descriptor " << idx << " is full";
        return -1;
    }

    desc->io_params.push_back(io_params);
    return 0;
}

int CUFileDescPool::submitBatch(int idx) {
    if (idx < 0 || idx >= (int)MAX_NR_DESC || descs_[idx] == nullptr) {
        LOG(ERROR) << "Invalid descriptor index: " << idx;
        return -1;
    }

    auto *desc = descs_[idx];
    if (desc->io_params.empty()) {
        LOG(WARNING) << "Submitting empty batch for descriptor " << idx;
        return 0;
    }

    // Submit all params in this descriptor
    CUFILE_CHECK(cuFileBatchIOSubmit(desc->batch_handle->handle,
                                     desc->io_params.size(),
                                     desc->io_params.data(), 0));
    return 0;
}

CUfileIOEvents_t CUFileDescPool::getTransferStatus(int idx, int slice_id) {
    if (idx < 0 || idx >= (int)MAX_NR_DESC || descs_[idx] == nullptr) {
        LOG(ERROR) << "Invalid descriptor index: " << idx;
        CUfileIOEvents_t event;
        event.status = CUFILE_FAILED;
        event.ret = -1;
        return event;
    }

    auto *desc = descs_[idx];
    if (slice_id < 0 || slice_id >= (int)desc->io_params.size()) {
        LOG(ERROR) << "Invalid slice_id " << slice_id << " for descriptor " << idx
                   << " (size: " << desc->io_params.size() << ")";
        CUfileIOEvents_t event;
        event.status = CUFILE_FAILED;
        event.ret = -1;
        return event;
    }

    unsigned nr = desc->io_params.size();
    CUFILE_CHECK(cuFileBatchIOGetStatus(desc->batch_handle->handle, nr, &nr,
                                        desc->io_events.data(), nullptr));

    return desc->io_events[slice_id];
}

int CUFileDescPool::getSliceNum(int idx) {
    if (idx < 0 || idx >= (int)MAX_NR_DESC || descs_[idx] == nullptr) {
        LOG(ERROR) << "Invalid descriptor index: " << idx;
        return -1;
    }

    return descs_[idx]->io_params.size();
}

int CUFileDescPool::freeCUfileDesc(int idx) {
    if (idx < 0 || idx >= (int)MAX_NR_DESC || descs_[idx] == nullptr) {
        LOG(ERROR) << "Invalid descriptor index: " << idx;
        return -1;
    }

    auto *desc = descs_[idx];

    // Return the handle to pool for reuse (avoid expensive cuFileBatchIODestroy)
    // Note: Caller should ensure all IOs are completed before calling free
    {
        std::lock_guard<std::mutex> lock(handle_pool_lock_);
        handle_pool_.push_back(desc->batch_handle);
    }

    // Delete the descriptor (each allocation gets a fresh one)
    delete desc;
    descs_[idx] = nullptr;
    occupied_[idx].store(false, std::memory_order_relaxed);

    return 0;
}

CUFileBatchDesc* CUFileDescPool::getDesc(int idx) {
    if (idx < 0 || idx >= (int)MAX_NR_DESC) {
        return nullptr;
    }
    return descs_[idx];
}

}  // namespace mooncake
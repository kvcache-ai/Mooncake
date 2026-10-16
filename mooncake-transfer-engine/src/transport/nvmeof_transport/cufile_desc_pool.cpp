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
    }
}

CUFileDescPool::~CUFileDescPool() {
    // First, collect and destroy batch_handles from allocated descriptors
    for (size_t i = 0; i < MAX_NR_DESC; ++i) {
        if (descs_[i] != nullptr) {
            destroyDesc(descs_[i]);
            descs_[i] = nullptr;
        }
    }

    // Then clean up any remaining handles in the pool
    std::lock_guard<std::mutex> lock(handle_pool_lock_);
    for (auto* batch_handle : handle_pool_) {
        cuFileBatchIODestroy(batch_handle->handle);
        delete batch_handle;
    }
    handle_pool_.clear();
    for (auto* desc : quarantined_descs_) {
        destroyDesc(desc);
    }
    quarantined_descs_.clear();
}

int CUFileDescPool::allocCUfileDesc(size_t batch_size) {
    if (batch_size > max_batch_size_) {
        LOG(ERROR) << "Batch Size " << batch_size
                   << " Exceeds Max CUFile Batch Size " << max_batch_size_;
        return -1;
    }

    RWSpinlock::WriteGuard guard(mutex_);

    // Find a free slot (nullptr = free)
    int idx = -1;
    for (size_t i = 0; i < MAX_NR_DESC; ++i) {
        if (descs_[i] == nullptr) {
            idx = i;
            break;
        }
    }

    if (idx < 0) {
        LOG(ERROR) << "No Batch Descriptor Available";
        return -1;
    }

    // Create new descriptor with independent io_params/io_events
    auto* desc = new CUFileBatchDesc();

    // Get or create BatchHandle from pool (lazy loading)
    BatchHandle* batch_handle = nullptr;
    {
        std::lock_guard<std::mutex> lock(handle_pool_lock_);
        if (!handle_pool_.empty()) {
            batch_handle = handle_pool_.back();
            handle_pool_.pop_back();
        }
    }

    try {
        // If pool is empty or handle size mismatch, create new handle
        // (expensive operation)
        if (!batch_handle || batch_handle->max_nr != max_batch_size_) {
            // Destroy mismatched handle if exists
            if (batch_handle) {
                cuFileBatchIODestroy(batch_handle->handle);
                delete batch_handle;
                batch_handle = nullptr;
            }

            auto new_batch_handle = std::make_unique<BatchHandle>();
            new_batch_handle->max_nr = max_batch_size_;
            // cuFileBatchIOSetUp is time-costly, so we reuse handles
            CUFILE_CHECK(
                cuFileBatchIOSetUp(&new_batch_handle->handle, max_batch_size_));
            batch_handle = new_batch_handle.release();
        }

        desc->batch_handle = batch_handle;
        desc->io_params.clear();
        desc->io_params.reserve(max_batch_size_);
        desc->io_events.clear();
        desc->io_events.reserve(max_batch_size_);
        desc->polled_events.resize(max_batch_size_);
        desc->reusable = true;

        descs_[idx] = desc;
        return idx;
    } catch (...) {
        // Clean up on exception to avoid memory leaks
        delete desc;
        if (batch_handle) {
            cuFileBatchIODestroy(batch_handle->handle);
            delete batch_handle;
        }
        throw;  // Re-throw to caller
    }
}

int CUFileDescPool::pushParams(int idx, const CUfileIOParams_t& io_params) {
    RWSpinlock::WriteGuard guard(mutex_);
    if (idx < 0 || idx >= (int)MAX_NR_DESC || descs_[idx] == nullptr) {
        LOG(ERROR) << "Invalid descriptor index: " << idx;
        return -1;
    }

    auto* desc = descs_[idx];
    if (desc->io_params.size() >= max_batch_size_) {
        LOG(ERROR) << "Descriptor " << idx << " is full";
        return -1;
    }

    CUfileIOParams_t params = io_params;
    const size_t slice_id = desc->io_params.size();
    params.cookie =
        reinterpret_cast<void*>(static_cast<uintptr_t>(slice_id + 1));
    desc->io_params.push_back(params);
    desc->io_events.push_back(CUfileIOEvents_t{
        .cookie = params.cookie, .status = CUFILE_WAITING, .ret = 0});
    return 0;
}

int CUFileDescPool::submitBatch(int idx) {
    RWSpinlock::WriteGuard guard(mutex_);
    if (idx < 0 || idx >= (int)MAX_NR_DESC || descs_[idx] == nullptr) {
        LOG(ERROR) << "Invalid descriptor index: " << idx;
        return -1;
    }

    auto* desc = descs_[idx];
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
    if (!updateBatchStatus(idx)) {
        return failedEvent();
    }
    return getCachedTransferStatus(idx, slice_id);
}

bool CUFileDescPool::updateBatchStatus(int idx) {
    RWSpinlock::WriteGuard guard(mutex_);
    if (idx < 0 || idx >= (int)MAX_NR_DESC || descs_[idx] == nullptr) {
        LOG(ERROR) << "Invalid descriptor index: " << idx;
        return false;
    }

    return updateBatchStatus(descs_[idx], idx);
}

CUfileIOEvents_t CUFileDescPool::getCachedTransferStatus(int idx,
                                                         int slice_id) {
    RWSpinlock::ReadGuard guard(mutex_);
    if (idx < 0 || idx >= (int)MAX_NR_DESC || descs_[idx] == nullptr) {
        LOG(ERROR) << "Invalid descriptor index: " << idx;
        return failedEvent();
    }

    auto* desc = descs_[idx];
    if (slice_id < 0 || slice_id >= (int)desc->io_params.size()) {
        LOG(ERROR) << "Invalid slice_id " << slice_id << " for descriptor "
                   << idx << " (size: " << desc->io_params.size() << ")";
        return failedEvent();
    }

    return desc->io_events[slice_id];
}

bool CUFileDescPool::updateBatchStatus(CUFileBatchDesc* desc, int idx) {
    unsigned nr = desc->io_params.size();
    if (desc->polled_events.size() < nr) {
        LOG(ERROR) << "Completion buffer is too small for descriptor " << idx;
        return false;
    }

    CUfileError_t rc =
        cuFileBatchIOGetStatus(desc->batch_handle->handle, 0, &nr,
                               desc->polled_events.data(), nullptr);
    if (rc.err != CU_FILE_SUCCESS) {
        LOG(WARNING) << "cuFileBatchIOGetStatus failed for descriptor " << idx
                     << ": " << cuFileGetErrorString(rc);
        return false;
    }

    for (unsigned i = 0; i < nr; ++i) {
        const auto& event = desc->polled_events[i];
        if (!cachePolledEvent(desc->io_events, event)) {
            LOG(ERROR) << "Invalid completion cookie "
                       << reinterpret_cast<uintptr_t>(event.cookie)
                       << " for descriptor " << idx;
        }
    }

    return true;
}

bool CUFileDescPool::cachePolledEvent(std::vector<CUfileIOEvents_t>& io_events,
                                      const CUfileIOEvents_t& event) {
    const uintptr_t cookie = reinterpret_cast<uintptr_t>(event.cookie);
    if (cookie == 0 || cookie > io_events.size()) return false;
    io_events[cookie - 1] = event;
    return true;
}

bool CUFileDescPool::isTerminalStatus(CUfileStatus_t status) {
    return status != CUFILE_WAITING && status != CUFILE_PENDING;
}

CUfileIOEvents_t CUFileDescPool::failedEvent() {
    CUfileIOEvents_t event = {};
    event.status = CUFILE_FAILED;
    event.ret = -1;
    return event;
}

void CUFileDescPool::destroyDesc(CUFileBatchDesc* desc) {
    cuFileBatchIODestroy(desc->batch_handle->handle);
    delete desc->batch_handle;
    delete desc;
}

void CUFileDescPool::cleanupQuarantinedDescs() {
    auto it = quarantined_descs_.begin();
    while (it != quarantined_descs_.end()) {
        auto* desc = *it;
        updateBatchStatus(desc, -1);

        bool all_terminal = true;
        for (const auto& event : desc->io_events) {
            if (!isTerminalStatus(event.status)) {
                all_terminal = false;
                break;
            }
        }

        if (all_terminal) {
            destroyDesc(desc);
            it = quarantined_descs_.erase(it);
        } else {
            ++it;
        }
    }
}

bool CUFileDescPool::cancelBatch(int idx) {
    RWSpinlock::WriteGuard guard(mutex_);
    if (idx < 0 || idx >= (int)MAX_NR_DESC || descs_[idx] == nullptr) {
        LOG(ERROR) << "Invalid descriptor index: " << idx;
        return false;
    }

    CUfileError_t rc = cuFileBatchIOCancel(descs_[idx]->batch_handle->handle);
    if (rc.err != CU_FILE_SUCCESS) {
        LOG(WARNING) << "cuFileBatchIOCancel failed for descriptor " << idx
                     << ": " << cuFileGetErrorString(rc);
        return false;
    }
    return true;
}

void CUFileDescPool::markUnreusable(int idx) {
    RWSpinlock::WriteGuard guard(mutex_);
    if (idx < 0 || idx >= (int)MAX_NR_DESC || descs_[idx] == nullptr) {
        LOG(ERROR) << "Invalid descriptor index: " << idx;
        return;
    }
    descs_[idx]->reusable = false;
}

int CUFileDescPool::getSliceNum(int idx) {
    RWSpinlock::ReadGuard guard(mutex_);
    if (idx < 0 || idx >= (int)MAX_NR_DESC || descs_[idx] == nullptr) {
        LOG(ERROR) << "Invalid descriptor index: " << idx;
        return -1;
    }

    return descs_[idx]->io_params.size();
}

int CUFileDescPool::freeCUfileDesc(int idx) {
    RWSpinlock::WriteGuard guard(mutex_);
    if (idx < 0 || idx >= (int)MAX_NR_DESC || descs_[idx] == nullptr) {
        LOG(ERROR) << "Invalid descriptor index: " << idx;
        return -1;
    }

    auto* desc = descs_[idx];
    const bool reusable = desc->reusable;

    // Reusable descriptors are safe to recycle only after the caller observed
    // all IOs complete. Non-reusable descriptors may still be referenced by
    // cuFile after a bounded failure cleanup, so keep their handle and params
    // quarantined until a later poll observes terminal status for every IO.
    // Return the handle to pool for reuse (avoid expensive
    // cuFileBatchIODestroy)
    {
        std::lock_guard<std::mutex> lock(handle_pool_lock_);
        if (reusable) {
            handle_pool_.push_back(desc->batch_handle);
        } else {
            quarantined_descs_.push_back(desc);
        }
    }

    if (reusable) {
        delete desc;
    }
    descs_[idx] = nullptr;
    cleanupQuarantinedDescs();

    return 0;
}

CUFileBatchDesc* CUFileDescPool::getDesc(int idx) {
    RWSpinlock::ReadGuard guard(mutex_);
    if (idx < 0 || idx >= (int)MAX_NR_DESC) {
        return nullptr;
    }
    return descs_[idx];
}

}  // namespace mooncake

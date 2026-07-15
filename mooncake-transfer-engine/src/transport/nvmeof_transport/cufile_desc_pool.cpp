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
#include <utility>

#include "cufile.h"
#include "transport/nvmeof_transport/cufile_context.h"

namespace mooncake {

namespace {

class NativeCUFileBatchAPI : public CUFileBatchAPI {
   public:
    CUfileError_t setUp(CUfileBatchHandle_t* batch_handle,
                        unsigned nr) override {
        return cuFileBatchIOSetUp(batch_handle, nr);
    }

    CUfileError_t submit(CUfileBatchHandle_t batch_handle, unsigned nr,
                         CUfileIOParams_t* params, unsigned flags) override {
        return cuFileBatchIOSubmit(batch_handle, nr, params, flags);
    }

    CUfileError_t getStatus(CUfileBatchHandle_t batch_handle, unsigned min_nr,
                            unsigned* nr, CUfileIOEvents_t* events,
                            struct timespec* timeout) override {
        return cuFileBatchIOGetStatus(batch_handle, min_nr, nr, events,
                                      timeout);
    }

    CUfileError_t cancel(CUfileBatchHandle_t batch_handle) override {
        return cuFileBatchIOCancel(batch_handle);
    }

    void destroy(CUfileBatchHandle_t batch_handle) override {
        cuFileBatchIODestroy(batch_handle);
    }
};

}  // namespace

CUFileDescPool::CUFileDescPool(size_t max_batch_size)
    : CUFileDescPool(max_batch_size, std::make_shared<NativeCUFileBatchAPI>()) {
}

CUFileDescPool::CUFileDescPool(size_t max_batch_size,
                               std::shared_ptr<CUFileBatchAPI> batch_api)
    : max_batch_size_(max_batch_size), batch_api_(std::move(batch_api)) {
    // Initialize descriptor array
    for (size_t i = 0; i < MAX_NR_DESC; ++i) {
        descs_[i] = nullptr;
    }
}

CUFileDescPool::~CUFileDescPool() {
    // First, collect and destroy batch_handles from allocated descriptors
    for (size_t i = 0; i < MAX_NR_DESC; ++i) {
        if (descs_[i] != nullptr) {
            batch_api_->destroy(descs_[i]->batch_handle->handle);
            delete descs_[i]->batch_handle;
            delete descs_[i];
            descs_[i] = nullptr;
        }
    }

    // Then clean up any remaining handles in the pool
    std::lock_guard<std::mutex> lock(handle_pool_lock_);
    for (auto* batch_handle : handle_pool_) {
        batch_api_->destroy(batch_handle->handle);
        delete batch_handle;
    }
    handle_pool_.clear();
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
                batch_api_->destroy(batch_handle->handle);
                delete batch_handle;
                batch_handle = nullptr;
            }

            auto new_batch_handle = std::make_unique<BatchHandle>();
            new_batch_handle->max_nr = max_batch_size_;
            // cuFileBatchIOSetUp is time-costly, so we reuse handles
            auto result =
                batch_api_->setUp(&new_batch_handle->handle, max_batch_size_);
            CUFILE_CHECK(result);
            batch_handle = new_batch_handle.release();
        }

        desc->batch_handle = batch_handle;
        desc->io_params.clear();
        desc->io_params.reserve(max_batch_size_);
        desc->io_events.clear();
        desc->io_events.reserve(max_batch_size_);
        desc->polled_events.resize(max_batch_size_);
        desc->cancel_requested.clear();
        desc->cancel_requested.reserve(max_batch_size_);

        descs_[idx] = desc;
        return idx;
    } catch (...) {
        // Clean up on exception to avoid memory leaks
        delete desc;
        if (batch_handle) {
            batch_api_->destroy(batch_handle->handle);
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
    if (desc->failure_seen) {
        LOG(ERROR) << "Descriptor " << idx
                   << " cannot accept parameters while failure cleanup is "
                      "in progress";
        return -1;
    }
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
    desc->cancel_requested.push_back(false);
    return 0;
}

int CUFileDescPool::submitBatch(int idx) {
    RWSpinlock::WriteGuard guard(mutex_);
    if (idx < 0 || idx >= (int)MAX_NR_DESC || descs_[idx] == nullptr) {
        LOG(ERROR) << "Invalid descriptor index: " << idx;
        return -1;
    }

    auto* desc = descs_[idx];
    if (desc->failure_seen) {
        LOG(ERROR) << "Descriptor " << idx
                   << " cannot submit while failure cleanup is in progress";
        return -1;
    }
    if (desc->submitted_count == desc->io_params.size()) {
        LOG(WARNING) << "Submitting empty batch for descriptor " << idx;
        return 0;
    }

    const auto submit_count = desc->io_params.size() - desc->submitted_count;
    const auto result =
        batch_api_->submit(desc->batch_handle->handle, submit_count,
                           desc->io_params.data() + desc->submitted_count, 0);

    // cuFile does not guarantee that a failed batch submission accepted no
    // requests. Keep the attempted range alive and quarantine the descriptor
    // so status polling can conservatively cancel and drain it.
    desc->submitted_count = desc->io_params.size();
    if (result.err != CU_FILE_SUCCESS) {
        desc->failure_seen = true;
        LOG(ERROR) << "Failed to submit descriptor " << idx << ": "
                   << cuFileGetErrorString(result);
        return -1;
    }
    return 0;
}

int CUFileDescPool::discardUnsubmittedParams(int idx) {
    RWSpinlock::WriteGuard guard(mutex_);
    if (idx < 0 || idx >= (int)MAX_NR_DESC || descs_[idx] == nullptr) {
        LOG(ERROR) << "Invalid descriptor index: " << idx;
        return -1;
    }

    auto* desc = descs_[idx];
    desc->io_params.resize(desc->submitted_count);
    desc->io_events.resize(desc->submitted_count);
    desc->cancel_requested.resize(desc->submitted_count);
    return 0;
}

bool CUFileDescPool::isAcceptingSubmissions(int idx) {
    RWSpinlock::ReadGuard guard(mutex_);
    if (idx < 0 || idx >= (int)MAX_NR_DESC || descs_[idx] == nullptr) {
        return false;
    }
    return !descs_[idx]->failure_seen;
}

CUFileBatchPollResult CUFileDescPool::pollBatch(int idx,
                                                CUFileBatchSnapshot& snapshot) {
    RWSpinlock::WriteGuard guard(mutex_);
    if (idx < 0 || idx >= (int)MAX_NR_DESC || descs_[idx] == nullptr) {
        LOG(ERROR) << "Invalid descriptor index: " << idx;
        return CUFileBatchPollResult::kError;
    }

    auto* desc = descs_[idx];
    auto cancel_active = [&]() {
        if (!desc->failure_seen || desc->cancel_attempted) return;

        bool has_active = false;
        for (size_t i = 0; i < desc->submitted_count; ++i) {
            if (!isTerminal(desc->io_events[i].status)) {
                desc->cancel_requested[i] = true;
                has_active = true;
            }
        }
        if (!has_active) return;

        // Set the flag before entering cuFile so an error cannot cause a retry.
        desc->cancel_attempted = true;
        auto result = batch_api_->cancel(desc->batch_handle->handle);
        if (result.err != CU_FILE_SUCCESS) {
            LOG(ERROR) << "Failed to cancel descriptor " << idx << ": "
                       << cuFileGetErrorString(result)
                       << "; continuing to drain completions";
        }
    };

    unsigned nr = desc->submitted_count;
    if (desc->polled_events.size() < nr) {
        LOG(ERROR) << "Completion buffer is too small for descriptor " << idx;
        return CUFileBatchPollResult::kError;
    }
    if (nr > 0) {
        auto result =
            batch_api_->getStatus(desc->batch_handle->handle, 0, &nr,
                                  desc->polled_events.data(), nullptr);
        if (result.err != CU_FILE_SUCCESS) {
            if (result.err == CU_FILE_INTERNAL_BATCH_GETSTATUS_ERROR) {
                cancel_active();
                LOG(WARNING)
                    << "Failed to poll descriptor " << idx << ": "
                    << cuFileGetErrorString(result) << "; retrying later";
                return CUFileBatchPollResult::kRetry;
            }

            // Other errors leave the descriptor state unknown, so quarantine
            // it and attempt cancellation.
            desc->failure_seen = true;
            cancel_active();
            LOG(ERROR) << "Failed to poll descriptor " << idx << ": "
                       << cuFileGetErrorString(result);
            return CUFileBatchPollResult::kError;
        }
    }

    for (unsigned i = 0; i < nr; ++i) {
        const auto& event = desc->polled_events[i];
        const uintptr_t cookie = reinterpret_cast<uintptr_t>(event.cookie);
        if (cookie == 0 || cookie > desc->submitted_count ||
            !cachePolledEvent(desc->io_events, event)) {
            LOG(ERROR) << "Invalid completion cookie "
                       << reinterpret_cast<uintptr_t>(event.cookie)
                       << " for descriptor " << idx;
        }
    }

    bool has_active = false;
    for (size_t i = 0; i < desc->submitted_count; ++i) {
        const auto status = desc->io_events[i].status;
        if (!isTerminal(status)) {
            has_active = true;
        } else if (status != CUFILE_COMPLETE) {
            desc->failure_seen = true;
        }
    }

    if (has_active) cancel_active();

    snapshot.io_events.assign(desc->io_events.begin(),
                              desc->io_events.begin() + desc->submitted_count);
    snapshot.cancel_requested.assign(
        desc->cancel_requested.begin(),
        desc->cancel_requested.begin() + desc->submitted_count);
    snapshot.failure_seen = desc->failure_seen;
    snapshot.all_terminal = !has_active;
    return CUFileBatchPollResult::kSuccess;
}

bool CUFileDescPool::cachePolledEvent(std::vector<CUfileIOEvents_t>& io_events,
                                      const CUfileIOEvents_t& event) {
    const uintptr_t cookie = reinterpret_cast<uintptr_t>(event.cookie);
    if (cookie == 0 || cookie > io_events.size()) return false;
    auto& cached = io_events[cookie - 1];
    if (!isTerminal(cached.status)) cached = event;
    return true;
}

bool CUFileDescPool::isTerminal(CUfileStatus_t status) {
    return status != CUFILE_WAITING && status != CUFILE_PENDING;
}

bool CUFileDescPool::allTerminal(const CUFileBatchDesc& desc) {
    for (size_t i = 0; i < desc.submitted_count; ++i) {
        if (!isTerminal(desc.io_events[i].status)) return false;
    }
    return true;
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
    BatchHandle* retired_handle = nullptr;
    {
        RWSpinlock::WriteGuard guard(mutex_);
        if (idx < 0 || idx >= (int)MAX_NR_DESC || descs_[idx] == nullptr) {
            LOG(ERROR) << "Invalid descriptor index: " << idx;
            return -1;
        }

        auto* desc = descs_[idx];
        if (!allTerminal(*desc)) {
            LOG(ERROR) << "Descriptor " << idx
                       << " cannot be freed while I/O is active";
            return -1;
        }

        if (desc->failure_seen) {
            retired_handle = desc->batch_handle;
        } else {
            std::lock_guard<std::mutex> lock(handle_pool_lock_);
            handle_pool_.push_back(desc->batch_handle);
        }

        // Delete the descriptor (each allocation gets a fresh one)
        delete desc;
        descs_[idx] = nullptr;
    }

    if (retired_handle) {
        batch_api_->destroy(retired_handle->handle);
        delete retired_handle;
    }

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

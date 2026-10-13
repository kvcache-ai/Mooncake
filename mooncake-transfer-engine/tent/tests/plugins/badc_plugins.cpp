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

// The loopback transport with one rule of the conformance suite
// (transport_conformance_test.cpp) broken, built once per NONCONFORMING_<v>
// as libtent_xport_bad<v>.so (v = c4, c5, c5b, c6, c7, c8). The suite must
// fail the case the rule belongs to on it and pass the others.

#include <algorithm>
#include <map>
#include <mutex>
#include <set>

#include "../../plugins/transport/loopback/loopback_transport.h"
#include "tent/transport_plugin.hpp"

namespace mooncake {
namespace tent {
namespace {

class NonconformingTransport : public LoopbackTransport {
   public:
    using LoopbackTransport::LoopbackTransport;

#if defined(NONCONFORMING_C4)
    // Submits request by request, so a submission refused at its third
    // request keeps the two before it.
    Status submitTransferTasks(
        SubBatchRef batch, const std::vector<Request>& request_list) override {
        for (const auto& request : request_list)
            CHECK_STATUS(
                LoopbackTransport::submitTransferTasks(batch, {request}));
        return Status::OK();
    }
#elif defined(NONCONFORMING_C5)
    // The last task of a submission of 16 or more never finishes on its own
    // (cancelling it still works).
    Status submitTransferTasks(
        SubBatchRef batch, const std::vector<Request>& request_list) override {
        const int first = static_cast<int>(batch->size());
        CHECK_STATUS(
            LoopbackTransport::submitTransferTasks(batch, request_list));
        if (request_list.size() >= 16) {
            std::lock_guard<std::mutex> lock(mutex_);
            stuck_[batch] = {first + int(request_list.size()) - 1, false};
        }
        return Status::OK();
    }

    Status getTransferStatus(SubBatchRef batch, int task_id,
                             TransferStatus& state) override {
        CHECK_STATUS(
            LoopbackTransport::getTransferStatus(batch, task_id, state));
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = stuck_.find(batch);
        if (it != stuck_.end() && it->second.first == task_id)
            state = {it->second.second ? CANCELED : PENDING, 0};
        return Status::OK();
    }

    Status cancelTransferTask(SubBatchRef batch, int task_id) override {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto it = stuck_.find(batch);
            if (it != stuck_.end() && it->second.first == task_id)
                it->second.second = true;
        }
        return LoopbackTransport::cancelTransferTask(batch, task_id);
    }

    Status freeSubBatch(SubBatchRef& batch) override {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stuck_.erase(batch);
        }
        return LoopbackTransport::freeSubBatch(batch);
    }

   private:
    std::mutex mutex_;
    std::map<SubBatchRef, std::pair<int, bool> > stuck_;  // task, cancelled
#elif defined(NONCONFORMING_C5B)
    // A completed task reports no bytes moved.
    Status getTransferStatus(SubBatchRef batch, int task_id,
                             TransferStatus& state) override {
        CHECK_STATUS(
            LoopbackTransport::getTransferStatus(batch, task_id, state));
        if (state.s == COMPLETED) state.transferred_bytes = 0;
        return Status::OK();
    }
#elif defined(NONCONFORMING_C6)
    // Hands out the notifications it has queued newest first.
    Status receiveNotification(
        std::vector<Notification>& notify_list) override {
        CHECK_STATUS(LoopbackTransport::receiveNotification(notify_list));
        std::reverse(notify_list.begin(), notify_list.end());
        return Status::OK();
    }
#elif defined(NONCONFORMING_C7)
    // A cancelled task stays PENDING.
    Status cancelTransferTask(SubBatchRef batch, int task_id) override {
        CHECK_STATUS(LoopbackTransport::cancelTransferTask(batch, task_id));
        std::lock_guard<std::mutex> lock(mutex_);
        cancelled_[batch].insert(task_id);
        return Status::OK();
    }

    Status getTransferStatus(SubBatchRef batch, int task_id,
                             TransferStatus& state) override {
        CHECK_STATUS(
            LoopbackTransport::getTransferStatus(batch, task_id, state));
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = cancelled_.find(batch);
        if (it != cancelled_.end() && it->second.count(task_id))
            state = {PENDING, 0};
        return Status::OK();
    }

    Status freeSubBatch(SubBatchRef& batch) override {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            cancelled_.erase(batch);
        }
        return LoopbackTransport::freeSubBatch(batch);
    }

   private:
    std::mutex mutex_;
    std::map<SubBatchRef, std::set<int> > cancelled_;
#elif defined(NONCONFORMING_C8)
    using LoopbackTransport::addMemoryBuffer;

    // Keeps only its own tag on a buffer, dropping the tags of the
    // transports registered before it, so no other transport can take over.
    Status addMemoryBuffer(BufferDesc& desc,
                           const MemoryOptions& options) override {
        CHECK_STATUS(LoopbackTransport::addMemoryBuffer(desc, options));
        desc.transports = {desc.transports.back()};
        return Status::OK();
    }
#else
#error "define one of NONCONFORMING_C4, C5, C5B, C6, C7 or C8"
#endif
};

}  // namespace
}  // namespace tent
}  // namespace mooncake

#define NONCONFORMING_FLAGS                                            \
    TENT_XPORT_FLAG_SAME_MACHINE_ONLY | TENT_XPORT_FLAG_NOTIFICATION | \
        TENT_XPORT_FLAG_CANCELLATION

#if defined(NONCONFORMING_C4)
TENT_DEFINE_TRANSPORT_PLUGIN_WITH_HOST(badc4,
                                       mooncake::tent::NonconformingTransport,
                                       mooncake::tent::UNSPEC,
                                       NONCONFORMING_FLAGS,
                                       TENT_XPORT_CAP_DRAM_TO_DRAM)
#elif defined(NONCONFORMING_C5)
TENT_DEFINE_TRANSPORT_PLUGIN_WITH_HOST(badc5,
                                       mooncake::tent::NonconformingTransport,
                                       mooncake::tent::UNSPEC,
                                       NONCONFORMING_FLAGS,
                                       TENT_XPORT_CAP_DRAM_TO_DRAM)
#elif defined(NONCONFORMING_C5B)
TENT_DEFINE_TRANSPORT_PLUGIN_WITH_HOST(badc5b,
                                       mooncake::tent::NonconformingTransport,
                                       mooncake::tent::UNSPEC,
                                       NONCONFORMING_FLAGS,
                                       TENT_XPORT_CAP_DRAM_TO_DRAM)
#elif defined(NONCONFORMING_C6)
TENT_DEFINE_TRANSPORT_PLUGIN_WITH_HOST(badc6,
                                       mooncake::tent::NonconformingTransport,
                                       mooncake::tent::UNSPEC,
                                       NONCONFORMING_FLAGS,
                                       TENT_XPORT_CAP_DRAM_TO_DRAM)
#elif defined(NONCONFORMING_C7)
TENT_DEFINE_TRANSPORT_PLUGIN_WITH_HOST(badc7,
                                       mooncake::tent::NonconformingTransport,
                                       mooncake::tent::UNSPEC,
                                       NONCONFORMING_FLAGS,
                                       TENT_XPORT_CAP_DRAM_TO_DRAM)
#elif defined(NONCONFORMING_C8)
TENT_DEFINE_TRANSPORT_PLUGIN_WITH_HOST(badc8,
                                       mooncake::tent::NonconformingTransport,
                                       mooncake::tent::UNSPEC,
                                       NONCONFORMING_FLAGS,
                                       TENT_XPORT_CAP_DRAM_TO_DRAM)
#endif

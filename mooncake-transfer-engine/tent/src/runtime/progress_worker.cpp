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

#include "tent/runtime/progress_worker.h"

#include "tent/common/status.h"
#include "tent/runtime/transfer_engine_impl.h"

namespace mooncake {
namespace tent {

ProgressWorker::ProgressWorker(TransferEngineImpl* impl) : impl_(impl) {}

ProgressWorker::~ProgressWorker() { stop(); }

void ProgressWorker::start() {
    if (running_.exchange(true, std::memory_order_acq_rel)) return;
    thread_ = std::thread(&ProgressWorker::runner, this);
}

void ProgressWorker::stop() {
    if (!running_.exchange(false, std::memory_order_acq_rel)) return;
    {
        std::lock_guard<std::mutex> lk(mu_);
        // Drop pending work; outstanding batches will be reaped via the
        // user thread's freeBatch path.
        order_.clear();
        queued_.clear();
    }
    cv_.notify_all();
    if (thread_.joinable()) thread_.join();
}

void ProgressWorker::notifyBatchMaybeReady(BatchID batch_id) {
    if (!batch_id) return;
    if (!running_.load(std::memory_order_acquire)) return;
    {
        std::lock_guard<std::mutex> lk(mu_);
        if (!queued_.insert(batch_id).second) return;
        order_.push_back(batch_id);
    }
    cv_.notify_one();
}

void ProgressWorker::runner() {
    while (true) {
        BatchID batch_id = 0;
        {
            std::unique_lock<std::mutex> lk(mu_);
            cv_.wait(lk, [&] {
                return !running_.load(std::memory_order_acquire) ||
                       !order_.empty();
            });
            if (!running_.load(std::memory_order_acquire)) return;
            batch_id = order_.front();
            order_.pop_front();
            queued_.erase(batch_id);
        }
        // progressBatch acquires the engine's progress_mutex_ and silently
        // returns InvalidArgument if the batch was freed before we got here.
        // PENDING means "kick again later"; the next notify wakes us up.
        // Terminal states leave the batch alone — freeBatch on the user
        // thread is responsible for reclamation.
        TransferStatus s;
        (void)impl_->progressBatch(batch_id, s);
    }
}

}  // namespace tent
}  // namespace mooncake

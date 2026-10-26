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

#include <chrono>
#include <vector>

#include "tent/common/status.h"
#include "tent/runtime/transfer_engine_impl.h"

namespace mooncake {
namespace tent {

ProgressWorker::ProgressWorker(TransferEngineImpl* impl,
                               std::chrono::microseconds fallback_interval)
    : impl_(impl), fallback_interval_(fallback_interval) {}

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
        polling_.clear();
        queue_ready_ = false;
    }
    cv_.notify_all();
    if (thread_.joinable()) thread_.join();
}

void ProgressWorker::notifyBatchMaybeReady(BatchID batch_id,
                                           bool poll_until_terminal) {
    if (!batch_id) return;
    if (!running_.load(std::memory_order_acquire)) return;
    {
        std::lock_guard<std::mutex> lk(mu_);
        if (poll_until_terminal) polling_.insert(batch_id);
        if (!queued_.insert(batch_id).second) return;
        order_.push_back(batch_id);
    }
    cv_.notify_one();
}

void ProgressWorker::notifyRuntimeQueueReady() {
    if (!running_.load(std::memory_order_acquire)) return;
    {
        std::lock_guard<std::mutex> lk(mu_);
        queue_ready_ = true;
    }
    cv_.notify_one();
}

void ProgressWorker::runner() {
    // An explicit zero disables runtime-queue scans, but must not strand a
    // transport that requires polling to produce its only completion wake.
    const auto interval = fallback_interval_.count() > 0
                              ? fallback_interval_
                              : std::chrono::microseconds(50000);
    auto next_fallback = std::chrono::steady_clock::now() + interval;
    auto progress = [this](BatchID id) {
        TransferStatus status{PENDING, 0};
        auto result = impl_->progressBatch(id, status);
        bool retired = result.ok() && status.s != PENDING;
        if (!result.ok()) {
            // InvalidArgument can also mean a live batch's transport is not
            // ready yet (for example while appending a new sub-batch). Only
            // actual removal from the engine retires polling on an error.
            std::lock_guard<std::recursive_mutex> batch_lk(
                impl_->progressLockFor(id));
            retired = !impl_->isBatchAlive(id);
        }
        if (retired) {
            std::lock_guard<std::mutex> lk(mu_);
            // A concurrent submit can reuse this batch after progressBatch
            // releases the engine lock. Preserve its newly queued wake.
            if (!queued_.count(id)) polling_.erase(id);
        }
    };
    while (true) {
        BatchID batch_id = 0;
        bool queue_ready = false;
        bool fallback_due = false;
        bool queue_active = impl_->hasActiveRuntimeQueue();
        std::vector<BatchID> polling_due;
        {
            std::unique_lock<std::mutex> lk(mu_);
            if (!polling_.empty() ||
                (queue_active && fallback_interval_.count() > 0)) {
                cv_.wait_until(lk, next_fallback, [&] {
                    return !running_.load(std::memory_order_acquire) ||
                           queue_ready_ || !order_.empty();
                });
            } else {
                cv_.wait(lk, [&] {
                    return !running_.load(std::memory_order_acquire) ||
                           queue_ready_ || !order_.empty();
                });
            }
            if (!running_.load(std::memory_order_acquire)) return;
            const auto now = std::chrono::steady_clock::now();
            if (now >= next_fallback) {
                fallback_due = true;
                polling_due.assign(polling_.begin(), polling_.end());
                next_fallback = now + interval;
            }
            queue_ready = queue_ready_;
            queue_ready_ = false;
            if (!order_.empty()) {
                batch_id = order_.front();
                order_.pop_front();
                queued_.erase(batch_id);
            }
        }
        // lazyFreeBatch reports the first batch it could not reclaim and logs
        // the skip itself (rate-limited); the worker only sweeps, so the
        // status is intentionally dropped at these call sites.
        if (queue_ready) {
            (void)impl_->progressRuntimeQueue();
            (void)impl_->lazyFreeBatch();
        }
        // progressBatch checks batch liveness under the engine's progress lock.
        // Errors alone do not establish that a batch was freed.
        // Event-driven transports wait for their next notification; polling
        // transports remain in polling_ until terminal or explicitly freed.
        // Terminal states are observed here; lazyFreeBatch reclaims a batch
        // only if freeBatch has already marked it free_requested.
        if (batch_id) {
            progress(batch_id);
            (void)impl_->progressRuntimeQueue();
            (void)impl_->lazyFreeBatch();
        }
        // Keep an absolute deadline so a stream of wakes for other batches
        // cannot indefinitely postpone these polls.
        for (auto id : polling_due) {
            if (id != batch_id) progress(id);
        }
        if (fallback_due) {
            if (queue_active && fallback_interval_.count() > 0)
                (void)impl_->progressRuntimeQueue();
            (void)impl_->lazyFreeBatch();
        }
    }
}

}  // namespace tent
}  // namespace mooncake

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

#ifndef PROGRESS_WORKER_H_
#define PROGRESS_WORKER_H_

#include <atomic>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <thread>
#include <unordered_set>

#include "tent/common/types.h"

namespace mooncake {
namespace tent {

class TransferEngineImpl;

// Event-driven progress worker for issue #2116. When the engine is configured
// with enable_progress_worker=true, transports (or test hooks) call
// notifyBatchMaybeReady to wake this worker, which then drives one
// progressBatch step per notification. This decouples failover/resubmit from
// the caller polling loop, so integrators that turn off
// enable_auto_failover_on_poll do not need to spin a polling thread of their
// own to keep failover progressing.
class ProgressWorker {
   public:
    explicit ProgressWorker(TransferEngineImpl* impl);
    ~ProgressWorker();

    ProgressWorker(const ProgressWorker&) = delete;
    ProgressWorker& operator=(const ProgressWorker&) = delete;

    void start();

    // Idempotent. Signals the worker thread to exit and joins it. After stop()
    // returns, notifyBatchMaybeReady becomes a no-op.
    void stop();

    // Safe from any thread. De-duplicates: enqueueing a batch that is already
    // queued is a no-op. No-op if the worker has been stopped or never
    // started.
    void notifyBatchMaybeReady(BatchID batch_id);

    // Safe from any thread. Coalesces multiple runtime queue wakes into one
    // bounded refill step.
    void notifyRuntimeQueueReady();

   private:
    void runner();

    TransferEngineImpl* impl_;
    std::atomic<bool> running_{false};
    std::thread thread_;

    std::mutex mu_;
    std::condition_variable cv_;
    std::unordered_set<BatchID> queued_;
    std::deque<BatchID> order_;
    bool queue_ready_{false};
};

}  // namespace tent
}  // namespace mooncake

#endif  // PROGRESS_WORKER_H_

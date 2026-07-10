// Copyright 2025 Huawei Technologies Co., Ltd
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

#ifndef ASCEND_SLICE_DISPATCHER_H
#define ASCEND_SLICE_DISPATCHER_H

#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

#include <acl/acl.h>
#include "transport/transport.h"
#include "utils.h"

namespace mooncake {

class TransferExecutorBase;

/**
 * Dispatcher interface: groups slices and enqueues to execution threads.
 * Depends on TransferExecutorBase for processing.
 */
class ISliceDispatcher {
   public:
    virtual void enqueue(std::vector<Transport::Slice *> slice_list) = 0;
    virtual void stop() = 0;
    virtual ~ISliceDispatcher() = default;
};

/**
 * Default dispatcher: groups by target_id, enqueues to shared thread pool.
 * Sets ACL context before invoking executor. Thread pool size is parsed from
 * ASCEND_THREAD_POOL_SIZE; when use_buffer_pool (single-thread only), uses 1.
 */
class DefaultSliceDispatcher : public ISliceDispatcher {
   public:
    DefaultSliceDispatcher(
        TransferExecutorBase *transfer_executor,
        const std::vector<aclrtContext> &local_engine_contexts);
    void enqueue(std::vector<Transport::Slice *> slice_list) override;
    void stop() override;

   private:
    size_t resolveThreadPoolSize() const;

    TransferExecutorBase *transfer_executor_;
    std::unique_ptr<AscendThreadPool> thread_pool_;
    std::vector<aclrtContext> local_engine_contexts_;
};

/**
 * RoCE dummy-real dispatcher: one thread per ADXL engine, groups by
 * (engine_idx, target_id), dispatches to engine's dedicated queue.
 */
class RoceDummyRealSliceDispatcher : public ISliceDispatcher {
   public:
    RoceDummyRealSliceDispatcher(
        TransferExecutorBase *transfer_executor,
        const std::vector<aclrtContext> &local_engine_contexts);
    ~RoceDummyRealSliceDispatcher() override;
    void enqueue(std::vector<Transport::Slice *> slice_list) override;
    void stop() override;

   private:
    void engineWorker(size_t engine_idx);

    struct SliceTask {
        std::vector<Transport::Slice *> slices;
    };
    size_t num_engines_;
    std::vector<aclrtContext> local_engine_contexts_;
    TransferExecutorBase *transfer_executor_;
    std::vector<std::thread> engine_threads_;
    std::vector<std::queue<SliceTask>> engine_queues_;
    std::vector<std::mutex> queue_mutexes_;
    std::vector<std::condition_variable> queue_cvs_;
    std::atomic<bool> running_{true};
};

}  // namespace mooncake

#endif  // ASCEND_SLICE_DISPATCHER_H

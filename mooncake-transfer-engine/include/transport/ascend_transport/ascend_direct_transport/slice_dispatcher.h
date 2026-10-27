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
    virtual void enqueue(std::vector<Transport::Slice*> slice_list) = 0;
    virtual void stop() = 0;
    virtual ~ISliceDispatcher() = default;
};

/**
 * Default dispatcher: groups by target_id, enqueues to a transfer thread pool.
 * Sets ACL context before invoking executor. Each transfer pool's size is
 * parsed from ASCEND_THREAD_POOL_SIZE and defaults to 16. Buffer-pool mode
 * uses 1. Device copies run on a separate pool of 6 threads.
 * One local device keeps one transfer pool and one memcpy pool. Both pin to
 * the shared NPU CPU affinity when that device resolves a cpulist.
 * Several local devices (standalone) keep one transfer pool and one memcpy
 * pool per device. Each pool pins to that device's NUMA cpulist. Non-fabric
 * host allocation for a device runs on that device's transfer pool.
 */
class DefaultSliceDispatcher : public ISliceDispatcher {
   public:
    DefaultSliceDispatcher(
        TransferExecutorBase* transfer_executor,
        const std::vector<aclrtContext>& local_engine_contexts,
        const std::vector<int32_t>& logic_device_ids);
    void enqueue(std::vector<Transport::Slice*> slice_list) override;
    void stop() override;

   private:
    size_t resolveThreadPoolSize() const;
    void enqueueShared(std::vector<Transport::Slice*> slice_list);
    void enqueuePerDevice(std::vector<Transport::Slice*> slice_list);

    TransferExecutorBase* transfer_executor_;
    std::unique_ptr<AscendThreadPool> thread_pool_;
    std::unique_ptr<AscendThreadPool> memcpy_thread_pool_;
    // Standalone, one entry per logic device. Empty means the shared pools.
    std::vector<std::unique_ptr<AscendThreadPool>> device_thread_pools_;
    std::vector<std::unique_ptr<AscendThreadPool>> device_memcpy_pools_;
    std::vector<aclrtContext> local_engine_contexts_;
};

/**
 * RoCE dummy-real dispatcher: one thread per ADXL engine, groups by
 * (engine_idx, target_id), dispatches to engine's dedicated queue.
 * Device copies run on a separate pool of 6 threads. Each engine thread
 * pins to that NPU's affinity CPUs. The memcpy pool pins only when every
 * NPU shares one CPU set.
 */
class RoceDummyRealSliceDispatcher : public ISliceDispatcher {
   public:
    RoceDummyRealSliceDispatcher(
        TransferExecutorBase* transfer_executor,
        const std::vector<aclrtContext>& local_engine_contexts,
        const std::vector<int32_t>& logic_device_ids);
    ~RoceDummyRealSliceDispatcher() override;
    void enqueue(std::vector<Transport::Slice*> slice_list) override;
    void stop() override;

   private:
    void engineWorker(size_t engine_idx);

    struct SliceTask {
        std::vector<Transport::Slice*> slices;
    };
    size_t num_engines_;
    std::vector<aclrtContext> local_engine_contexts_;
    TransferExecutorBase* transfer_executor_;
    std::vector<std::thread> engine_threads_;
    std::vector<std::queue<SliceTask>> engine_queues_;
    std::vector<std::mutex> queue_mutexes_;
    std::vector<std::condition_variable> queue_cvs_;
    std::unique_ptr<AscendThreadPool> memcpy_thread_pool_;
    // Aligned with engine index. Empty means that engine thread is not pinned.
    std::vector<std::vector<int>> engine_affinity_cpus_;
    std::atomic<bool> running_{true};
};

}  // namespace mooncake

#endif  // ASCEND_SLICE_DISPATCHER_H

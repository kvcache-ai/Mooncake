// Copyright 2026 KVCache.AI
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

#ifndef ASCEND_AFFINITY_H_
#define ASCEND_AFFINITY_H_

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace mooncake {

// CPU sets for memcpy pools and transfer pools. Affinity is the NPU's NUMA
// node. shared_cpus is that node's cpulist when every local NPU is on the same
// node; a single shared pool uses it and otherwise stays unpinned.
// per_device_cpus is aligned with the logic-id list. It is filled for devices
// whose NUMA node can be read, and the unread devices are spread over the
// machine's CPU nodes when the plan holds more than one device and at least two
// nodes are usable. Standalone pools use that list, one pool per device,
// including when the NUMA nodes differ.
struct AscendAffinityPlan {
    std::vector<int> shared_cpus;
    std::vector<std::vector<int>> per_device_cpus;
};

// Resolve each logic id to its NUMA node's cpulist. Never throws. Devices
// whose NUMA node cannot be read are assigned by
// AllocateUnreadDevicesToNumaNodes, so the per-device pools do not all end up
// on the process start node, and are left empty when there are fewer than two
// usable nodes.
AscendAffinityPlan BuildAscendAffinityPlan(
    const std::vector<int32_t>& logic_device_ids);

// CPU nodes this process may run on, as (node id, cpulist) pairs in node order.
// The cpulists are intersected with the process cpuset. Exposed for tests and
// used by BuildAscendAffinityPlan.
const std::vector<std::pair<int, std::vector<int>>>& NumaNodeCpuLists();

// Internal helper exposed for unit tests. Assigns each unread device to a NUMA
// node, preferring the node that currently holds the fewest devices and
// breaking ties by node id (nodes are in node order), then returns the chosen
// node index for each device. Returns an empty vector when there are fewer
// than two usable nodes, because spreading then has nothing to spread over.
std::vector<size_t> AllocateUnreadDevicesToNumaNodes(
    size_t device_count,
    const std::vector<std::pair<int, std::vector<int>>>& nodes);

// Bind this thread to cpus. Empty cpus leave the thread on the default
// scheduler and return false without a log. A failed pthread_setaffinity_np
// logs and returns false.
bool TryBindCurrentThread(const std::vector<int>& cpus);

/**
 * @brief A simple thread pool for executing tasks asynchronously.
 *
 * This thread pool maintains a fixed number of worker threads that
 * execute tasks from a shared queue until the pool is stopped.
 */
class AscendThreadPool {
   public:
    /**
     * @brief Constructs a AscendThreadPool with the specified number of
     * threads.
     * @param num_threads The number of worker threads to create.
     * @param affinity_cpus CPUs to pin each worker to. Empty keeps the
     * default scheduler. A failed pin on a worker also keeps the default.
     */
    // thread_name_prefix plus the worker index must fit in 15 characters,
    // the pthread name limit.
    explicit AscendThreadPool(size_t num_threads,
                              std::string thread_name_prefix = "ascend-wk-",
                              std::vector<int> affinity_cpus = {});

    /**
     * @brief Destructor that stops the thread pool and waits for all threads.
     */
    ~AscendThreadPool();

    /**
     * @brief Stops the thread pool and waits for all threads to finish.
     *
     * No new tasks will be accepted after stopping. Tasks already
     * in the queue will be processed before shutdown completes.
     */
    void stop();

    /**
     * @brief Checks if the thread pool is running.
     * @return true if the pool is running, false otherwise.
     */
    bool isRunning() const;

    /**
     * @brief Returns the number of worker threads.
     */
    size_t size() const { return workers_.size(); }

    /**
     * @brief Enqueues a task to be executed by the thread pool.
     * @tparam F The callable type.
     * @tparam Args The argument types for the callable.
     * @param f The callable to execute.
     * @param args The arguments to pass to the callable.
     *
     * If the thread pool has been stopped, the task will not be enqueued.
     */
    template <class F, class... Args>
    void enqueue(F&& f, Args&&... args);

    // Queues task while the pool is running. Returns false if the pool has
    // stopped and the task was not queued.
    bool tryEnqueue(std::function<void()> task);

   private:
    std::vector<std::thread> workers_;
    std::queue<std::function<void()>> tasks_;
    std::mutex queue_mutex_;
    std::condition_variable condition_;
    std::atomic<bool> running_{false};
    std::vector<int> affinity_cpus_;
};

template <class F, class... Args>
void AscendThreadPool::enqueue(F&& f, Args&&... args) {
    auto task = std::make_shared<std::function<void()>>(
        [f = std::forward<F>(f),
         ... args = std::forward<Args>(args)]() mutable {
            std::invoke(f, args...);
        });
    {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        if (!running_) {
            return;
        }
        tasks_.emplace([task] { (*task)(); });
    }
    condition_.notify_one();
}

inline bool AscendThreadPool::tryEnqueue(std::function<void()> task) {
    {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        if (!running_) {
            return false;
        }
        tasks_.emplace(std::move(task));
    }
    condition_.notify_one();
    return true;
}

// ---------------------------------------------------------------------------
// Allocation placement: the transport publishes the CPU-affinity-bound pools
// that should run host-memory allocation (first touch), and the allocator runs
// each allocation through RunAllocationOnAffinityPool(). Both classic and TENT
// publish their transport thread pools here, so allocation placement uses the
// same per-NPU NUMA plan as the transfer pools.
// ---------------------------------------------------------------------------

// Pools published by one transport. `per_device` is indexed by the allocator's
// device slot (0..device_count-1); a missing or empty entry falls back to
// `shared`, and no runner at all means "run on the caller".
struct AllocationAffinityPools {
    std::function<void(std::function<void()>)> shared;
    std::vector<std::function<void(std::function<void()>)>> per_device;
};

// Adapt an AscendThreadPool to the runner interface. Tries the pool first so
// the allocation runs on a worker already bound to the target CPUs; when the
// pool is null or stopped, runs inline so allocation never fails because of
// placement.
inline std::function<void(std::function<void()>)> MakeAllocationRunner(
    AscendThreadPool* pool) {
    return [pool](std::function<void()> task) {
        if (pool == nullptr || !pool->tryEnqueue(std::move(task))) {
            task();
        }
    };
}

// Replace the transport's published pools. owner identifies the publisher; a
// later Clear from another owner is ignored, so shutdown of one transport
// cannot remove another transport's pools.
void PublishAllocationAffinityPools(const void* owner,
                                    AllocationAffinityPools pools);
void ClearAllocationAffinityPools(const void* owner);

// Run fn on the pool published for device_slot and wait. A negative slot uses
// the shared pool. With no published pool, or when fn is already running on a
// published pool, it runs inline.
void RunAllocationOnAffinityPool(int32_t device_slot, std::function<void()> fn);

}  // namespace mooncake
#endif  // ASCEND_AFFINITY_H_

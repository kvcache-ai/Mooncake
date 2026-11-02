// Copyright 2026 Huawei Technologies Co., Ltd
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

#ifndef ASCEND_DIRECT_UTIL_H
#define ASCEND_DIRECT_UTIL_H

#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#include <acl/acl.h>

namespace mooncake {

#define CHECK_ACL(call)                                       \
    do {                                                      \
        auto err = (call);                                    \
        if (err != ACL_ERROR_NONE) {                          \
            LOG(ERROR) << "Call acl failed, "                 \
                       << "detail: " << aclGetRecentErrMsg(); \
            return -1;                                        \
        }                                                     \
    } while (0)

#define MAKE_GUARD(var, callback) \
    const ::mooncake::ScopeGuard const_guard_##var(callback)

#define MAKE_DISMISSABLE_GUARD(var, callback) \
    ::mooncake::ScopeGuard make_guard_##var(callback)
#define DISMISS_GUARD(var) make_guard_##var.Dismiss()

class ScopeGuard {
   public:
    ScopeGuard(const ScopeGuard&) = delete;
    ScopeGuard& operator=(const ScopeGuard&) = delete;

    explicit ScopeGuard(const std::function<void()>& on_exit_scope)
        : on_exit_scope_(on_exit_scope) {}

    ~ScopeGuard() {
        if (!dismissed_) {
            if (on_exit_scope_ != nullptr) {
                try {
                    on_exit_scope_();
                } catch (std::bad_function_call&) {
                    // no need log error
                } catch (...) {
                    // no need log error
                }
            }
        }
    }

    void Dismiss() { dismissed_ = true; }

   private:
    std::function<void()> on_exit_scope_;
    bool dismissed_ = false;
};

std::string GenAdxlEngineName(const std::string& ip, const uint64_t port);

uint16_t FindAdxlListenPort(int32_t base_port, int32_t device_id);

// True when HCCL_INTRA_ROCE_ENABLE=1 or ASCEND_GLOBAL_RESOURCE_CONFIG sets
// comm_resource_config.protocol_desc to a roce:* entry (string or array).
bool IsRoceModeEnabled();

// Parse ASCEND_GLOBAL_RESOURCE_CONFIG JSON for roce protocol_desc only.
bool HasRoceProtocolDescInGlobalResourceConfig(const char* config_str);

// True when protocol_desc is "hccs:device", or an array containing that
// entry. "hccs:host" and "roce:*" do not match. config_str is the already
// role-resolved ASCEND_GLOBAL_RESOURCE_CONFIG.
bool HasHccsDeviceProtocolDescInGlobalResourceConfig(const char* config_str);

// True when protocol_desc is "roce:host", or an array containing that entry,
// on this object or on its "store" subtree. "roce:device" does not match.
// The store subtree is included because host memory is allocated after TE
// init, when ascend_store_te_init has already been cleared.
bool HasRoceHostProtocolDescInGlobalResourceConfig(const char* config_str);

// Resolve the effective ASCEND_GLOBAL_RESOURCE_CONFIG for the current TE role.
// Schema: the top-level object is the default config (passed verbatim to adxl
// for normal/P2P TEs); an optional "store" sub-object overrides it for a
// Store-init TE (globalConfig().ascend_store_te_init == true).
//   - null/empty or no "store" key  -> returned verbatim (backward compatible)
//   - has "store" and store-init TE  -> the "store" sub-object, serialized
//   - has "store" and normal TE      -> root with "store" removed, serialized
// A Store TE with no "store" key falls back to the default config.
std::string ResolveAscendGlobalResourceConfig(const char* config_str);

// TE support for configuring HIXL's comm_resource_config.nic_topo_path in the
// (already role-resolved) GlobalResourceConfig: fills it from Mooncake's
// MC_CUSTOM_TOPO_JSON path when the HIXL option is empty. Rules:
//   - MC_CUSTOM_TOPO_JSON unset/empty                    -> returned unchanged
//   - nic_topo_path already set (flat or nested, string, non-empty)
//                                                       -> returned unchanged
//   - otherwise the MC_CUSTOM_TOPO_JSON path is written under the nested
//     "comm_resource_config" object when one exists (any stale flat key is
//     dropped), otherwise as the flat key
//     "comm_resource_config.nic_topo_path". An empty input yields a minimal
//     {"comm_resource_config.nic_topo_path":"..."} object.
// A non-empty but unparsable input is returned unchanged (HIXL rejects it).
std::string InjectNicTopoPathFromCustomTopoEnvTe(
    const std::string& resolved_config);

int SetDeviceAndGetContext(int32_t device_id, aclrtContext* out_context);

// CPU sets for memcpy pools and transfer pools. Affinity is the NPU's NUMA
// node. shared_cpus is that node's cpulist when every local NPU is on the
// same node; a single shared pool uses it and otherwise stays unpinned.
// per_device_cpus is aligned with the logic-id list. Standalone pools use
// that list, one pool per device, including when the NUMA nodes differ.
struct AscendAffinityPlan {
    std::vector<int> shared_cpus;
    std::vector<std::vector<int>> per_device_cpus;
};

// Resolve each logic id to its NUMA node's cpulist. Never throws.
AscendAffinityPlan BuildAscendAffinityPlan(
    const std::vector<int32_t>& logic_device_ids);

// Bind this thread to cpus. Empty cpus leave the thread on the default
// scheduler and return false without a log. A failed pthread_setaffinity_np
// logs and returns false.
bool TryBindCurrentThread(const std::vector<int>& cpus);

// Publish the affinity-bound pool that should run non-fabric host allocation.
// owner is the pool pointer. A later clear from another owner is ignored.
void SetBoundAffinityRunner(const void* owner,
                            std::function<void(std::function<void()>)> runner);
void ClearBoundAffinityRunner(const void* owner);

// One runner per ACL logic device id. runners[i] runs work for device i.
// owner identifies the publisher. A later clear from another owner is ignored.
void SetPerDeviceBoundAffinityRunners(
    const void* owner,
    std::vector<std::function<void(std::function<void()>)>> runners);
void ClearPerDeviceBoundAffinityRunners(const void* owner);

// Run fn on the bound pool and wait. With no pool, or when already on that
// pool, fn runs on the caller. A stopped pool also runs fn on the caller.
void RunOnBoundAffinityThread(std::function<void()> fn);

// Run fn on the pool published for device_id and wait. A missing per-device
// runner uses the shared runner from SetBoundAffinityRunner. With neither,
// or when already on a bound pool, fn runs on the caller.
void RunOnDeviceBoundAffinityThread(int32_t device_id,
                                    std::function<void()> fn);

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

}  // namespace mooncake
#endif  // ASCEND_DIRECT_UTIL_H

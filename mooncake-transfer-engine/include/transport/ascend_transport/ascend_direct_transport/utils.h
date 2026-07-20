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
    ScopeGuard(const ScopeGuard &) = delete;
    ScopeGuard &operator=(const ScopeGuard &) = delete;

    explicit ScopeGuard(const std::function<void()> &on_exit_scope)
        : on_exit_scope_(on_exit_scope) {}

    ~ScopeGuard() {
        if (!dismissed_) {
            if (on_exit_scope_ != nullptr) {
                try {
                    on_exit_scope_();
                } catch (std::bad_function_call &) {
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

std::string GenAdxlEngineName(const std::string &ip, const uint64_t port);

uint16_t FindAdxlListenPort(int32_t base_port, int32_t device_id);

int SetDeviceAndGetContext(int32_t device_id, aclrtContext *out_context);

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
     */
    explicit AscendThreadPool(size_t num_threads);

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
     * @brief Enqueues a task to be executed by the thread pool.
     * @tparam F The callable type.
     * @tparam Args The argument types for the callable.
     * @param f The callable to execute.
     * @param args The arguments to pass to the callable.
     *
     * If the thread pool has been stopped, the task will not be enqueued.
     */
    template <class F, class... Args>
    void enqueue(F &&f, Args &&...args);

   private:
    std::vector<std::thread> workers_;
    std::queue<std::function<void()>> tasks_;
    std::mutex queue_mutex_;
    std::condition_variable condition_;
    std::atomic<bool> running_{false};
};

template <class F, class... Args>
void AscendThreadPool::enqueue(F &&f, Args &&...args) {
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

}  // namespace mooncake
#endif  // ASCEND_DIRECT_UTIL_H
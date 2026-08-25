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

#ifndef TENT_HIGH_PERFORMANCE_TCP_WORKERS_H_
#define TENT_HIGH_PERFORMANCE_TCP_WORKERS_H_

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

#include "tent/common/status.h"

namespace mooncake {
namespace tent {

// Worker substrate for the future datacenter-only TCP path.
//
// The existing TcpTransport intentionally remains RPC/ylt based.  This class
// has no RPC dependency: a future high-performance transport can attach one
// socket/connection cache and one I/O context to every WorkerContext, then
// submit affinity-preserving work through submitToWorker().
class HighPerformanceTcpWorkers {
   public:
    struct Config {
        size_t worker_count{16};
        // Bound is per worker. The total admitted-but-not-running work is at
        // most worker_count * queue_capacity.
        size_t queue_capacity{256};
    };

    using Task = std::function<void(size_t worker_id)>;

    HighPerformanceTcpWorkers();
    explicit HighPerformanceTcpWorkers(Config config);
    ~HighPerformanceTcpWorkers();

    HighPerformanceTcpWorkers(const HighPerformanceTcpWorkers&) = delete;
    HighPerformanceTcpWorkers& operator=(const HighPerformanceTcpWorkers&) =
        delete;

    // Starts a fixed set of workers. A stopped instance is deliberately not
    // restartable: future workers will own sockets, and reopening them under
    // the same object would make connection lifetime ambiguous.
    Status start();
    Status stop();

    // Picks a worker round-robin, falling back to another worker if the first
    // candidate queue is full. Use submitToWorker for connection affinity.
    Status submit(Task task);
    Status submitToWorker(size_t worker_id, Task task);

    bool running() const { return running_.load(std::memory_order_acquire); }
    size_t workerCount() const { return config_.worker_count; }

   private:
    struct WorkerContext {
        std::mutex mutex;
        std::condition_variable cv;
        std::queue<Task> queue;
        std::thread thread;
        bool stopping{false};
    };

    Status enqueue(size_t worker_id, Task task);
    void workerLoop(size_t worker_id);

    Config config_;
    std::vector<std::unique_ptr<WorkerContext>> workers_;
    std::atomic<bool> running_{false};
    std::atomic<size_t> next_worker_{0};
    std::mutex lifecycle_mutex_;
    bool started_{false};
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_HIGH_PERFORMANCE_TCP_WORKERS_H_

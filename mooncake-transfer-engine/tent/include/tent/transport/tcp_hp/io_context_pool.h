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

#ifndef TCP_HP_IO_CONTEXT_POOL_H_
#define TCP_HP_IO_CONTEXT_POOL_H_

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

#include <asio/io_context.hpp>
#include <asio/executor_work_guard.hpp>

namespace mooncake {
namespace tent {
namespace tcp_hp {

// A pool of io_context instances, each running on its own thread.
// Phase 1: pool_size=1 (single-threaded, same as existing).
// Phase 2: pool_size=N for multi-threaded I/O.
class IoContextPool {
   public:
    explicit IoContextPool(size_t pool_size);
    ~IoContextPool();

    // Start all worker threads. Must be called once after construction.
    void start();

    // Stop all io_contexts and join threads. Safe to call multiple times.
    void stop();

    // Round-robin: get the next io_context for distributing work.
    asio::io_context& getNextContext();

    // Get a specific io_context by index (e.g., index 0 for acceptor).
    asio::io_context& getContext(size_t index);

    size_t size() const { return entries_.size(); }

   private:
    struct Entry {
        std::unique_ptr<asio::io_context> context;
        std::unique_ptr<
            asio::executor_work_guard<asio::io_context::executor_type>>
            work_guard;
        std::thread thread;
    };

    std::vector<Entry> entries_;
    std::atomic<size_t> next_index_{0};
    bool running_ = false;
};

}  // namespace tcp_hp
}  // namespace tent
}  // namespace mooncake

#endif  // TCP_HP_IO_CONTEXT_POOL_H_

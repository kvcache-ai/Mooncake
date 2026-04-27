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

#include "tent/transport/tcp_hp/io_context_pool.h"

#include <glog/logging.h>

namespace mooncake {
namespace tent {
namespace tcp_hp {

IoContextPool::IoContextPool(size_t pool_size) {
    CHECK_GT(pool_size, 0u) << "IoContextPool size must be > 0";
    entries_.resize(pool_size);
    for (auto& entry : entries_) {
        entry.context = std::make_unique<asio::io_context>();
        entry.work_guard = std::make_unique<
            asio::executor_work_guard<asio::io_context::executor_type>>(
            entry.context->get_executor());
    }
}

IoContextPool::~IoContextPool() { stop(); }

void IoContextPool::start() {
    if (running_) return;
    running_ = true;
    for (auto& entry : entries_) {
        entry.thread = std::thread([&ctx = *entry.context]() {
            try {
                ctx.run();
            } catch (const std::exception& e) {
                LOG(ERROR) << "IoContextPool worker exception: " << e.what();
            }
        });
    }
    LOG(INFO) << "IoContextPool started with " << entries_.size() << " thread(s)";
}

void IoContextPool::stop() {
    if (!running_) return;
    running_ = false;
    for (auto& entry : entries_) {
        entry.work_guard.reset();
    }
    for (auto& entry : entries_) {
        entry.context->stop();
    }
    for (auto& entry : entries_) {
        if (entry.thread.joinable()) entry.thread.join();
    }
}

asio::io_context& IoContextPool::getNextContext() {
    size_t idx =
        next_index_.fetch_add(1, std::memory_order_relaxed) % entries_.size();
    return *entries_[idx].context;
}

asio::io_context& IoContextPool::getContext(size_t index) {
    CHECK_LT(index, entries_.size());
    return *entries_[index].context;
}

}  // namespace tcp_hp
}  // namespace tent
}  // namespace mooncake

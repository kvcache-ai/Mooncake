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

#include "tent/transport/tcp_hp/bounce_buffer_pool.h"

#include <glog/logging.h>

namespace mooncake {
namespace tent {
namespace tcp_hp {

BounceBufferPool::BounceBufferPool(size_t buffer_size, size_t initial_count,
                                   size_t max_count)
    : buffer_size_(buffer_size), max_count_(max_count) {
    CHECK_GT(buffer_size, 0u);
    free_list_.reserve(initial_count);
    for (size_t i = 0; i < initial_count; ++i) {
        free_list_.push_back(new char[buffer_size]);
    }
    total_allocated_ = initial_count;
    LOG(INFO) << "BounceBufferPool: pre-allocated " << initial_count
              << " x " << buffer_size << " byte buffers";
}

BounceBufferPool::~BounceBufferPool() {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto* buf : free_list_) {
        delete[] buf;
    }
    if (free_list_.size() != total_allocated_) {
        LOG(WARNING) << "BounceBufferPool: destroyed with "
                     << (total_allocated_ - free_list_.size())
                     << " buffers still in use";
    }
    free_list_.clear();
    total_allocated_ = 0;
}

char* BounceBufferPool::acquire() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!free_list_.empty()) {
        char* buf = free_list_.back();
        free_list_.pop_back();
        return buf;
    }
    // Pool empty — try to allocate a new buffer.
    if (max_count_ > 0 && total_allocated_ >= max_count_) {
        return nullptr;  // Hard limit reached.
    }
    ++total_allocated_;
    return new char[buffer_size_];
}

void BounceBufferPool::release(char* buf) {
    if (!buf) return;
    std::lock_guard<std::mutex> lock(mutex_);
    free_list_.push_back(buf);
}

size_t BounceBufferPool::total_allocated() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return total_allocated_;
}

size_t BounceBufferPool::free_count() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return free_list_.size();
}

}  // namespace tcp_hp
}  // namespace tent
}  // namespace mooncake

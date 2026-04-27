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

#ifndef TCP_HP_BOUNCE_BUFFER_POOL_H_
#define TCP_HP_BOUNCE_BUFFER_POOL_H_

#include <cstddef>
#include <mutex>
#include <vector>

namespace mooncake {
namespace tent {
namespace tcp_hp {

// Pre-allocated pool of fixed-size DRAM buffers used as bounce buffers
// for GPU<->TCP transfers. Eliminates per-chunk malloc/free overhead.
//
// Thread-safe: acquire/release can be called from any thread.
class BounceBufferPool {
   public:
    // buffer_size:    size of each buffer (typically = chunk_size)
    // initial_count:  number of buffers to pre-allocate
    // max_count:      hard cap on total buffers (0 = unlimited)
    BounceBufferPool(size_t buffer_size, size_t initial_count,
                     size_t max_count);
    ~BounceBufferPool();

    // Non-copyable, non-movable
    BounceBufferPool(const BounceBufferPool&) = delete;
    BounceBufferPool& operator=(const BounceBufferPool&) = delete;

    // Acquire a buffer. Returns nullptr if max_count reached.
    char* acquire();

    // Return a buffer to the pool.
    void release(char* buf);

    size_t buffer_size() const { return buffer_size_; }
    size_t total_allocated() const;
    size_t free_count() const;

   private:
    size_t buffer_size_;
    size_t max_count_;
    mutable std::mutex mutex_;
    std::vector<char*> free_list_;
    size_t total_allocated_ = 0;
};

}  // namespace tcp_hp
}  // namespace tent
}  // namespace mooncake

#endif  // TCP_HP_BOUNCE_BUFFER_POOL_H_

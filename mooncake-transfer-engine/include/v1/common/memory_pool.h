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

#ifndef MEMORY_POOL_H_
#define MEMORY_POOL_H_

#include <list>
#include <mutex>

namespace mooncake {
namespace v1 {

template <class T>
class MemoryPool {
   public:
    static MemoryPool &Get();

   public:
    MemoryPool() {}

    ~MemoryPool() {
        for (auto entry : alloc_list_) delete entry;
        alloc_list_.clear();
        free_list_.clear();
    }

    T *allocate();

    void deallocate(T *&object);

    struct ThreadLocal {
        std::list<T> free_list;
    };

   private:
    std::mutex mutex_;
    std::list<T *> free_list_;
    std::list<T *> alloc_list_;

    static const size_t kMaxFreeListSizeInThread = 128;
    static const size_t kAllocateBatchSize = 128;

    MemoryPool(const MemoryPool &) = delete;
    MemoryPool &operator=(const MemoryPool &) = delete;
};
}  // namespace v1
}  // namespace mooncake

#endif  // MEMORY_POOL_H_
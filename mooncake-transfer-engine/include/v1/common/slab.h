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

#ifndef SLAB_H_
#define SLAB_H_

#include <list>
#include <mutex>

namespace mooncake {
namespace v1 {

class SlabBase {
   public:
    SlabBase(size_t block_size) : block_size_(block_size) {}

    ~SlabBase() {
        for (auto entry : alloc_list_) free(entry);
        alloc_list_.clear();
        free_list_.clear();
    }

    void *allocate();

    void deallocate(void *object);

    struct ThreadLocal {
        std::list<void *> free_list;
    };

   private:
    const size_t block_size_;
    std::mutex mutex_;
    std::list<void *> free_list_;
    std::list<void *> alloc_list_;

    static const size_t kMaxFreeListSizeInThread = 128;
    static const size_t kAllocateBatchSize = 128;
};

template <class T>
class Slab {
   public:
    static Slab &Get() {
        static Slab<T> g_slice;
        return g_slice;
    }

   public:
    Slab() : base(sizeof(T)) {}

    virtual ~Slab() {}

    T *allocate() { return (T *)base.allocate(); }

    void deallocate(T *object) { return base.deallocate(object); }

   private:
    SlabBase base;
};
}  // namespace v1
}  // namespace mooncake

#endif  // SLAB_H_
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
#include <array>
#include <cstdlib>

namespace mooncake {
namespace tent {

class SlabBase {
   public:
    SlabBase(size_t block_size);

    ~SlabBase() {
        for (auto entry : alloc_list_) free(entry);
        alloc_list_.clear();
        free_list_.clear();
    }

    void *allocate();

    void deallocate(void *object);

    static const size_t kMaxFreeListSizeInThread = 128;
    static const size_t kAllocateBatchSize = 128;

    struct ThreadLocal {
       public:
        bool enqueue(void *item) {
            if (full()) return false;
            buffer[tail] = item;
            tail = (tail + 1) % kMaxFreeListSizeInThread;
            ++count;
            return true;
        }

        bool dequeue(void *&item) {
            if (empty()) return false;
            item = buffer[head];
            head = (head + 1) % kMaxFreeListSizeInThread;
            --count;
            return true;
        }

        bool empty() const { return count == 0; }
        bool full() const { return count == kMaxFreeListSizeInThread; }
        size_t size() const { return count; }

       private:
        std::array<void *, kMaxFreeListSizeInThread> buffer;
        int head = 0, tail = 0, count = 0;
    };

   private:
    const size_t block_size_;
    int slab_index_;
    std::mutex mutex_;
    std::list<void *> free_list_;
    std::list<void *> alloc_list_;
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

    T *allocate() {
        auto ptr = (T *)base.allocate();
        if (ptr) new (ptr) T();
        return ptr;
    }

    void deallocate(T *object) {
        object->~T();
        return base.deallocate(object);
    }

   private:
    SlabBase base;
};
}  // namespace tent
}  // namespace mooncake

#endif  // SLAB_H_
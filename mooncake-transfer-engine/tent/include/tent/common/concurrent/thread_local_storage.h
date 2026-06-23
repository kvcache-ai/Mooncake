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

#ifndef TENT_THREAD_LOCAL_STORAGE_H
#define TENT_THREAD_LOCAL_STORAGE_H

#include <atomic>
#include <cstdint>
#include <functional>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>

namespace mooncake {
namespace tent {

// Per-thread storage that also supports iterating every live thread's value
// via forEach (e.g. to aggregate or clean up across worker threads). Each
// object keeps independent per-thread values. The destructor frees this
// object's per-thread holders, so get()/forEach must not run concurrently with
// the object's own destruction (the usual "no use during destruction" rule).
template <typename T>
class ThreadLocalStorage {
   public:
    ThreadLocalStorage() : id_(nextId()) {}

    ~ThreadLocalStorage() {
        // This object owns the per-thread holders created for it (one per
        // thread that called get()). Detach them under the lock, then destroy
        // them outside it so ~InstanceHolder can re-acquire global_mutex_
        // without deadlocking.
        std::unordered_set<InstanceHolder*> holders;
        {
            std::lock_guard<std::mutex> lock(global_mutex_);
            holders.swap(instances_);
        }
        for (auto* holder : holders) delete holder;
    }

    // Disable copy/move
    ThreadLocalStorage(const ThreadLocalStorage&) = delete;
    ThreadLocalStorage& operator=(const ThreadLocalStorage&) = delete;

    // Access the thread-local instance for THIS object (lock-free fast path).
    T& get() {
        // Per-thread, per-object storage. Keyed by this object's unique id
        // (not by `this`) so a recycled address cannot alias a destroyed
        // object's leftover holder. The map is per type T per thread.
        static thread_local std::unordered_map<uint64_t, InstanceHolder*>
            holders;
        auto& holder = holders[id_];
        if (!holder) {
            holder = new InstanceHolder(this);
        }
        return holder->value;
    }

    // Safe iteration over all live threads' instances of THIS object.
    void forEach(const std::function<void(T&)>& fn) {
        std::lock_guard<std::mutex> lock(global_mutex_);
        for (auto* inst : instances_) {
            fn(inst->value);
        }
    }

   private:
    struct InstanceHolder {
        T value{};
        ThreadLocalStorage<T>* owner;

        InstanceHolder(ThreadLocalStorage<T>* owner) : owner(owner) {
            std::lock_guard<std::mutex> lock(owner->global_mutex_);
            owner->instances_.insert(this);
        }

        ~InstanceHolder() {
            std::lock_guard<std::mutex> lock(owner->global_mutex_);
            owner->instances_.erase(this);
        }
    };

    static uint64_t nextId() {
        static std::atomic<uint64_t> counter{0};
        return counter.fetch_add(1, std::memory_order_relaxed);
    }

    // Unique id used to key the per-thread holder map, stable for this
    // object's lifetime and never reused.
    const uint64_t id_;

    // All live thread instances created for THIS object.
    std::unordered_set<InstanceHolder*> instances_;
    std::mutex global_mutex_;
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_THREAD_LOCAL_STORAGE_H
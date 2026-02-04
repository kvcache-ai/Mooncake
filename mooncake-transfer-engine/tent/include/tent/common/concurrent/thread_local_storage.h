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

#include <functional>
#include <mutex>
#include <thread>
#include <unordered_set>

namespace mooncake {
namespace tent {

template <typename T>
class ThreadLocalStorage {
   public:
    ThreadLocalStorage() = default;
    ~ThreadLocalStorage() = default;

    // Disable copy/move
    ThreadLocalStorage(const ThreadLocalStorage&) = delete;
    ThreadLocalStorage& operator=(const ThreadLocalStorage&) = delete;

    // Access the thread-local instance (lock-free)
    T& get() {
        if (!instance_) {
            instance_ = new InstanceHolder(this);
        }
        return instance_->value;
    }

    // Safe iteration over all instances (with locking)
    void forEach(const std::function<void(T&)>& fn) {
        std::lock_guard<std::mutex> lock(global_mutex_);
        for (auto* inst : instances_) {
            fn(inst->value);
        }
    }

   private:
    struct InstanceHolder {
        T value;
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

    // Thread-local pointer to the per-thread instance
    thread_local static InstanceHolder* instance_;

    // Global list of all thread instances
    std::unordered_set<InstanceHolder*> instances_;
    std::mutex global_mutex_;
};

// Definition of thread_local variable (must be outside the class)
template <typename T>
thread_local typename ThreadLocalStorage<T>::InstanceHolder*
    ThreadLocalStorage<T>::instance_ = nullptr;

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_THREAD_LOCAL_STORAGE_H
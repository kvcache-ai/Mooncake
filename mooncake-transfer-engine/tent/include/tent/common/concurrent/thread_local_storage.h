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
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

namespace mooncake {
namespace tent {

namespace detail {
// Hoisted out of the class template so ids are unique across ALL
// ThreadLocalStorage instantiations, not just within one T. The per-thread
// maps are per-T, so per-T uniqueness would suffice today — global
// uniqueness removes the aliasing hazard if the per-thread state is ever
// shared across instantiations.
inline uint64_t nextThreadLocalStorageId() {
    static std::atomic<uint64_t> counter{1};
    return counter.fetch_add(1, std::memory_order_relaxed);
}
}  // namespace detail

// Per-instance thread-local storage (#2717).
//
// Each (ThreadLocalStorage instance, thread) pair owns a distinct T. The
// previous implementation kept one `thread_local` slot per template
// instantiation, so all instances of ThreadLocalStorage<T> in a process
// aliased each other's per-thread state, and its heap-allocated holders were
// never destroyed (the deregistration path was unreachable).
//
// Lifetime rules:
// - A thread's T values are destroyed when the thread exits.
// - Destroying the storage does not destroy other threads' values; they are
//   orphaned (at most one T per thread per destroyed storage) and reclaimed
//   at those threads' exit. Instance ids are process-monotonic and never
//   reused, so an orphaned value can never be served to a new instance that
//   happens to reuse the same address.
// - Both teardown orders are safe: a thread exiting while the owner is alive
//   deregisters its value from the owner's registry; an owner destroyed
//   while threads are alive marks the jointly-owned control block dead, and
//   those threads skip deregistration at exit. Orphans are additionally
//   swept opportunistically: the next first-use get() of ANY instance on
//   that thread reclaims all of the thread's dead-owner values, so orphan
//   count stays bounded by live instances even under storage churn.
//
// Precondition: callers must keep the storage alive across every get() /
// forEach() call (the usual member-of-owner pattern satisfies this); the
// destructor may run concurrently only with other threads' exits, not with
// their accesses.
//
// Concurrency:
// - get() is lock-free after the first call per (instance, thread): one
//   thread_local access plus an id compare on the hot path.
// - forEach() runs under the registry mutex and visits exactly the values of
//   live registered threads. It synchronizes registry membership only — if
//   owning threads mutate their T concurrently, the callback observes those
//   fields with whatever synchronization T itself provides (unchanged from
//   the previous implementation).
template <typename T>
class ThreadLocalStorage {
   public:
    ThreadLocalStorage() = default;

    ~ThreadLocalStorage() {
        std::lock_guard<std::mutex> lock(control_->mutex);
        control_->owner_alive.store(false, std::memory_order_release);
        control_->values.clear();
    }

    ThreadLocalStorage(const ThreadLocalStorage&) = delete;
    ThreadLocalStorage& operator=(const ThreadLocalStorage&) = delete;

    // Access the calling thread's instance, constructing it on first use.
    T& get() {
        ThreadState& state = threadState();
        if (state.cached_id == id_) return *state.cached_value;
        auto it = state.nodes.find(id_);
        if (it == state.nodes.end()) {
            // First use of this instance on this thread — the cold path.
            // Piggyback a sweep of values whose owners are gone, so a
            // long-lived thread does not accumulate one orphan per
            // destroyed storage it ever touched (their T contents would
            // otherwise stay pinned until thread exit).
            sweepDeadNodes(state);
            it = state.nodes.try_emplace(id_).first;
            ThreadNode& node = it->second;
            node.control = control_;
            // The caller holds a reference to *this, so the owner is alive
            // and registration cannot race ~ThreadLocalStorage's clear().
            std::lock_guard<std::mutex> lock(control_->mutex);
            control_->values.insert(&node.value);
        }
        state.cached_id = id_;
        state.cached_value = &it->second.value;
        return *state.cached_value;
    }

    // Safe iteration over the values of all live threads that have called
    // get() on this instance.
    void forEach(const std::function<void(T&)>& fn) {
        std::lock_guard<std::mutex> lock(control_->mutex);
        for (T* value : control_->values) {
            fn(*value);
        }
    }

   private:
    // Shared between the owner and every thread node so that whichever side
    // is torn down last still has a valid registry (or a dead flag) to look
    // at.
    struct ControlBlock {
        std::mutex mutex;
        std::unordered_set<T*> values;
        // Atomic so the orphan sweep can test liveness without taking the
        // mutex of every node it scans.
        std::atomic<bool> owner_alive{true};
    };

    struct ThreadNode {
        T value{};
        std::shared_ptr<ControlBlock> control;

        ThreadNode() = default;
        ThreadNode(const ThreadNode&) = delete;
        ThreadNode& operator=(const ThreadNode&) = delete;

        ~ThreadNode() {
            if (!control) return;
            std::lock_guard<std::mutex> lock(control->mutex);
            if (control->owner_alive.load(std::memory_order_acquire))
                control->values.erase(&value);
        }
    };

    struct ThreadState {
        // Node-based map: ThreadNode addresses are stable across rehash,
        // which the registry and the one-entry cache below rely on.
        std::unordered_map<uint64_t, ThreadNode> nodes;
        // One-entry cache so the common get() is a single thread_local
        // access plus a compare. Entries live until thread exit, so the
        // cached pointer cannot dangle while the id matches.
        uint64_t cached_id = 0;  // ids start at 1; 0 never matches
        T* cached_value = nullptr;
    };

    static void sweepDeadNodes(ThreadState& state) {
        for (auto it = state.nodes.begin(); it != state.nodes.end();) {
            if (!it->second.control->owner_alive.load(
                    std::memory_order_acquire)) {
                if (state.cached_value == &it->second.value) {
                    state.cached_id = 0;
                    state.cached_value = nullptr;
                }
                // ~ThreadNode sees the dead owner and skips the registry.
                it = state.nodes.erase(it);
            } else {
                ++it;
            }
        }
    }

    static ThreadState& threadState() {
        thread_local ThreadState state;
        return state;
    }

    const uint64_t id_ = detail::nextThreadLocalStorageId();
    std::shared_ptr<ControlBlock> control_ = std::make_shared<ControlBlock>();
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_THREAD_LOCAL_STORAGE_H

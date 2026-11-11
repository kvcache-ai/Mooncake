// Copyright 2026 KVCache.AI
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

#include <cstdint>
#include <exception>
#include <functional>
#include <future>
#include <mutex>
#include <utility>
#include <vector>

#include "ascend_affinity.h"

// One process-wide slot where the active Ascend transport publishes the pools
// that should run host-memory allocation. The allocation entry point
// (ascend_allocator.cpp) only knows "shared" or "device slot N"; this file owns
// the pool selection, the inline fallback and the owner-aware clear.
namespace mooncake {
namespace {

struct PublishedPools {
    std::mutex mu;
    const void* owner = nullptr;
    AllocationAffinityPools pools;
};

PublishedPools& Registry() {
    static PublishedPools registry;
    return registry;
}

// Set while a task is executing on a published pool. This is what prevents an
// allocation issued from inside a pool worker from enqueueing onto the same
// pool and deadlocking on itself.
thread_local bool on_published_pool = false;

void RunAndWait(const std::function<void(std::function<void()>)>& runner,
                std::function<void()> fn) {
    if (!runner) {
        fn();
        return;
    }
    std::promise<void> done;
    auto wait = done.get_future();
    runner([&fn, &done] {
        on_published_pool = true;
        try {
            fn();
            on_published_pool = false;
            done.set_value();
        } catch (...) {
            on_published_pool = false;
            done.set_exception(std::current_exception());
        }
    });
    wait.get();
}

}  // namespace

void PublishAllocationAffinityPools(const void* owner,
                                    AllocationAffinityPools pools) {
    auto& registry = Registry();
    std::lock_guard<std::mutex> lock(registry.mu);
    registry.owner = owner;
    registry.pools = std::move(pools);
}

void ClearAllocationAffinityPools(const void* owner) {
    auto& registry = Registry();
    std::lock_guard<std::mutex> lock(registry.mu);
    if (registry.owner != owner) {
        return;
    }
    registry.owner = nullptr;
    registry.pools = AllocationAffinityPools{};
}

void RunAllocationOnAffinityPool(int32_t device_slot,
                                 std::function<void()> fn) {
    if (!fn) {
        return;
    }
    if (on_published_pool) {
        fn();
        return;
    }

    std::function<void(std::function<void()>)> runner;
    {
        auto& registry = Registry();
        std::lock_guard<std::mutex> lock(registry.mu);
        if (device_slot >= 0) {
            const auto index = static_cast<size_t>(device_slot);
            if (index < registry.pools.per_device.size()) {
                runner = registry.pools.per_device[index];
            }
        }
        if (!runner) {
            runner = registry.pools.shared;
        }
    }
    RunAndWait(runner, std::move(fn));
}

}  // namespace mooncake

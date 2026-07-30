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

// Regression tests for #2717: ThreadLocalStorage used one `thread_local`
// slot per template instantiation (all instances aliased each other's
// per-thread state) and never destroyed its per-thread holders (the
// deregistration path was unreachable). These tests pin the per-instance
// semantics and both teardown orders; run them under ASAN/LSAN to verify
// the holder-leak fix and the owner-destroyed-first ordering.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <memory>
#include <thread>
#include <vector>

#include "tent/common/concurrent/thread_local_storage.h"

namespace mooncake {
namespace tent {

namespace {
struct Cache {
    int value = 0;
};

size_t countValues(ThreadLocalStorage<Cache>& storage) {
    size_t n = 0;
    storage.forEach([&](Cache&) { n++; });
    return n;
}
}  // namespace

// The original aliasing bug: two instances in the same thread must own
// distinct per-thread values.
TEST(ThreadLocalStorageTest, InstancesDoNotAlias) {
    ThreadLocalStorage<Cache> a;
    ThreadLocalStorage<Cache> b;
    a.get().value = 42;
    EXPECT_EQ(b.get().value, 0);
    EXPECT_NE(&a.get(), &b.get());
    b.get().value = 7;
    EXPECT_EQ(a.get().value, 42);
}

// Thread exits while the owner is alive: the value deregisters (forEach no
// longer sees it) and its memory is reclaimed (LSAN would flag the previous
// implementation, which leaked one holder per thread per instantiation).
TEST(ThreadLocalStorageTest, ThreadExitDeregistersAndReclaims) {
    ThreadLocalStorage<Cache> storage;
    storage.get().value = 1;  // main thread's value
    EXPECT_EQ(countValues(storage), 1u);

    std::thread t([&] {
        storage.get().value = 2;
        EXPECT_EQ(countValues(storage), 2u);
    });
    t.join();

    EXPECT_EQ(countValues(storage), 1u);  // worker's value deregistered
    EXPECT_EQ(storage.get().value, 1);    // main's value untouched
}

// Owner destroyed while a using thread is still alive: the thread's later
// exit must not touch the dead owner (the jointly-owned control block keeps
// the registry memory valid; ASAN pins this ordering).
TEST(ThreadLocalStorageTest, OwnerDestroyedBeforeThreadExitIsSafe) {
    std::atomic<bool> used{false};
    std::atomic<bool> release{false};
    auto storage = std::make_unique<ThreadLocalStorage<Cache>>();
    std::thread t([&] {
        storage->get().value = 7;
        used.store(true);
        while (!release.load()) std::this_thread::yield();
        // Thread exit here runs the node destructor against a dead owner.
    });
    while (!used.load()) std::this_thread::yield();
    storage.reset();  // owner gone first
    release.store(true);
    t.join();
}

// Instance ids are never reused: a new storage that may occupy the same
// address as a destroyed one must not see the old orphaned value.
TEST(ThreadLocalStorageTest, DestroyedInstanceStateIsNotResurrected) {
    for (int round = 0; round < 8; ++round) {
        auto storage = std::make_unique<ThreadLocalStorage<Cache>>();
        EXPECT_EQ(storage->get().value, 0) << "round " << round;
        storage->get().value = 100 + round;
    }
}

// forEach synchronizes registry membership: it sees exactly the values of
// live threads that used this instance.
TEST(ThreadLocalStorageTest, ForEachVisitsExactlyLiveRegisteredValues) {
    ThreadLocalStorage<Cache> storage;
    constexpr int kThreads = 8;
    std::atomic<int> ready{0};
    std::atomic<bool> release{false};
    std::vector<std::thread> threads;
    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([&, i] {
            storage.get().value = i + 1;
            ready++;
            while (!release.load()) std::this_thread::yield();
        });
    }
    while (ready.load() < kThreads) std::this_thread::yield();

    int sum = 0;
    size_t n = 0;
    storage.forEach([&](Cache& c) {
        sum += c.value;
        n++;
    });
    EXPECT_EQ(n, (size_t)kThreads);
    EXPECT_EQ(sum, kThreads * (kThreads + 1) / 2);

    release.store(true);
    for (auto& t : threads) t.join();
    EXPECT_EQ(countValues(storage), 0u);
}

// Concurrent churn: threads exercising get() across shared storages while
// other storages are created/destroyed, with forEach mixed in. Run under
// TSAN/ASAN for the full effect; asserts basic integrity without them.
TEST(ThreadLocalStorageTest, ConcurrentChurnStress) {
    constexpr int kThreads = 8;
    constexpr int kIterations = 2000;
    ThreadLocalStorage<Cache> shared_a;
    ThreadLocalStorage<Cache> shared_b;
    std::atomic<uint64_t> failures{0};
    std::vector<std::thread> threads;
    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([&, i] {
            for (int iter = 0; iter < kIterations; ++iter) {
                shared_a.get().value = i;
                shared_b.get().value = -i;
                if (shared_a.get().value != i) failures++;
                if (shared_b.get().value != -i) failures++;
                // Thread-private storages churn creation/destruction.
                ThreadLocalStorage<Cache> ephemeral;
                ephemeral.get().value = iter;
                if (ephemeral.get().value != iter) failures++;
                if (iter % 64 == 0) {
                    shared_a.forEach([](Cache&) {});
                }
            }
        });
    }
    for (auto& t : threads) t.join();
    EXPECT_EQ(failures.load(), 0u);
    EXPECT_EQ(countValues(shared_a), 0u);
}

namespace {
struct Counted {
    static std::atomic<int> live;
    int value = 0;
    Counted() { live.fetch_add(1, std::memory_order_relaxed); }
    ~Counted() { live.fetch_sub(1, std::memory_order_relaxed); }
};
std::atomic<int> Counted::live{0};
}  // namespace

// Storage churn on a long-lived thread: values of destroyed storages must be
// swept by the next first-use get() rather than accumulating until thread
// exit (one orphaned T — pinning its contents — per destroyed storage).
TEST(ThreadLocalStorageTest, OrphanedValuesSweptOnInstanceChurn) {
    constexpr int kRounds = 1000;
    for (int i = 0; i < kRounds; ++i) {
        ThreadLocalStorage<Counted> storage;
        storage.get().value = i;
        // Previous round's orphan must have been swept by this round's
        // first-use get(): at most this round's value plus one not-yet-swept
        // orphan may be alive.
        ASSERT_LE(Counted::live.load(), 2) << "round " << i;
    }
}

// Informational: hot-path cost of get(). The remote-desc cache consults this
// on every transfer submit, so the common case must stay a thread_local
// access plus a compare.
TEST(ThreadLocalStorageTest, HotPathMicrobench) {
    ThreadLocalStorage<Cache> storage;
    storage.get().value = 1;
    constexpr uint64_t kOps = 20'000'000;
    volatile int sink = 0;
    auto t0 = std::chrono::steady_clock::now();
    for (uint64_t i = 0; i < kOps; ++i) {
        sink += storage.get().value;
    }
    auto t1 = std::chrono::steady_clock::now();
    double ns =
        (double)std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0)
            .count() /
        (double)kOps;
    printf("get_hot_path_ns_per_op %.2f\n", ns);
    (void)sink;
#if defined(__SANITIZE_THREAD__) || defined(__SANITIZE_ADDRESS__)
    constexpr bool kSanitized = true;
#elif defined(__has_feature)
#if __has_feature(thread_sanitizer) || __has_feature(address_sanitizer)
    constexpr bool kSanitized = true;
#else
    constexpr bool kSanitized = false;
#endif
#else
    constexpr bool kSanitized = false;
#endif
    // Wall-clock assertions flake under sanitizers (TSAN alone is ~14x);
    // elsewhere keep a loose ceiling that still catches syscall- or
    // contention-class regressions on the hot path.
    if (!kSanitized) {
        EXPECT_LT(ns, 100.0);
    }
}

}  // namespace tent
}  // namespace mooncake

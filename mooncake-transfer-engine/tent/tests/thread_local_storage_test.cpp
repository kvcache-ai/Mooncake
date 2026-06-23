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

#include "tent/common/concurrent/thread_local_storage.h"

#include <gtest/gtest.h>

#include <thread>

namespace mooncake {
namespace tent {
namespace {

// Two ThreadLocalStorage<T> objects of the same type must keep independent
// per-thread values. (Before the per-object fix, the second object aliased the
// first because the thread-local pointer was a per-type static.)
TEST(ThreadLocalStorageTest, DistinctObjectsDoNotAlias) {
    ThreadLocalStorage<int> a, b;
    a.get() = 111;
    EXPECT_EQ(b.get(), 0);
    b.get() = 222;
    EXPECT_EQ(a.get(), 111);
    EXPECT_EQ(b.get(), 222);
}

// forEach must visit only the holders that belong to that object.
TEST(ThreadLocalStorageTest, ForEachVisitsOnlyOwnInstances) {
    ThreadLocalStorage<int> a, b;
    a.get() = 5;
    b.get() = 9;
    int cnt_a = 0, sum_a = 0;
    a.forEach([&](int& v) {
        ++cnt_a;
        sum_a += v;
    });
    int cnt_b = 0, sum_b = 0;
    b.forEach([&](int& v) {
        ++cnt_b;
        sum_b += v;
    });
    EXPECT_EQ(cnt_a, 1);
    EXPECT_EQ(sum_a, 5);
    EXPECT_EQ(cnt_b, 1);
    EXPECT_EQ(sum_b, 9);
}

// Each thread gets its own value; forEach aggregates across live threads.
TEST(ThreadLocalStorageTest, PerThreadIsolationAndCrossThreadForEach) {
    ThreadLocalStorage<int> s;
    s.get() = 1;
    std::thread t([&] {
        s.get() = 2;
        EXPECT_EQ(s.get(), 2);
    });
    t.join();
    EXPECT_EQ(s.get(), 1);
    int cnt = 0, total = 0;
    s.forEach([&](int& v) {
        ++cnt;
        total += v;
    });
    EXPECT_EQ(cnt, 2);
    EXPECT_EQ(total, 3);
}

// A freshly created object must not observe a destroyed object's leftover
// value, even if the allocator recycles the same address/storage.
TEST(ThreadLocalStorageTest, FreshObjectDoesNotAliasDestroyedOne) {
    {
        ThreadLocalStorage<int> a;
        a.get() = 77;
    }
    ThreadLocalStorage<int> b;
    EXPECT_EQ(b.get(), 0);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake

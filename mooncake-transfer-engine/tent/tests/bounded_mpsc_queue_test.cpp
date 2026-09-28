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
//
// Regression coverage for issue #3637: a full queue used to spin push()
// forever, so a worker re-enqueueing its own slices could wedge the only
// consumer. try_push must report fullness instead, and the worker-local
// overflow must drain before the shared queue so parked entries cannot
// starve behind contending producers.

#include "tent/common/concurrent/bounded_mpsc_queue.h"

#include <gtest/gtest.h>

#include <vector>

namespace mooncake {
namespace tent {
namespace {

// The queue is generic over T and only touches num_slices, so a stand-in keeps
// this test buildable without the RDMA transport headers.
struct SliceList {
    void* first = nullptr;
    int num_slices = 0;
};

using Queue = BoundedMPSCQueue<SliceList, 8>;

SliceList entry(int n) {
    SliceList list;
    list.num_slices = n;
    return list;
}

TEST(BoundedMPSCQueueTest, TryPushReportsFullInsteadOfSpinning) {
    Queue queue;
    for (int i = 0; i < 8; ++i) {
        auto item = entry(1);
        ASSERT_TRUE(queue.try_push(item));
    }
    auto extra = entry(1);
    EXPECT_FALSE(queue.try_push(extra));
}

TEST(BoundedMPSCQueueTest, PopDrainsInOrderAfterFull) {
    Queue queue;
    for (int i = 1; i <= 8; ++i) {
        auto item = entry(i);
        ASSERT_TRUE(queue.try_push(item));
    }
    for (int i = 1; i <= 8; ++i) {
        EXPECT_EQ(queue.pop().num_slices, i);
    }
    // Empty pop returns a zero-initialized entry.
    EXPECT_EQ(queue.pop().num_slices, 0);
}

TEST(BoundedMPSCQueueTest, ParkedEntryIsDrainedBeforeContendingProducers) {
    // Model the production drain order: the worker consumes its local overflow
    // before popping the shared queue, so a parked retry makes progress even
    // while producers keep refilling freed slots (review feedback on #3683).
    Queue queue;
    for (int i = 0; i < 8; ++i) {
        auto item = entry(1);
        ASSERT_TRUE(queue.try_push(item));
    }
    std::vector<SliceList> overflow;
    auto parked = entry(7);
    ASSERT_FALSE(queue.try_push(parked));
    overflow.push_back(parked);

    // First tick drains the shared queue fully; a producer instantly refills.
    std::vector<SliceList> first_batch;
    queue.pop(first_batch);
    ASSERT_EQ(first_batch.size(), 8u);
    auto fresh = entry(9);
    ASSERT_TRUE(queue.try_push(fresh));

    // Second tick: overflow first, then the shared queue.
    std::vector<SliceList> batch;
    for (auto it = overflow.begin(); it != overflow.end();) {
        batch.push_back(*it);
        it = overflow.erase(it);
    }
    queue.pop(batch);
    ASSERT_EQ(batch.size(), 2u);
    EXPECT_EQ(batch[0].num_slices, 7);
    EXPECT_EQ(batch[1].num_slices, 9);
    EXPECT_TRUE(overflow.empty());
}

TEST(BoundedMPSCQueueTest, PushStillAcceptsAndEmptyPushIsANoop) {
    Queue queue;
    auto empty = entry(0);
    queue.push(empty);  // returns immediately for an empty list
    auto item = entry(3);
    queue.push(item);
    EXPECT_EQ(queue.pop().num_slices, 3);
    EXPECT_EQ(queue.pop().num_slices, 0);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake

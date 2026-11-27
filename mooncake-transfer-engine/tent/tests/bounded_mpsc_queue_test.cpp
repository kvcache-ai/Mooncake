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

TEST(BoundedMPSCQueueTest, RejectingSecondQueueLeavesOnlyCanceledSlots) {
    Queue first;
    Queue full;
    auto item = entry(1);
    for (int i = 0; i < 8; ++i) ASSERT_TRUE(full.try_push(item));
    for (int i = 0; i < 24; ++i) {
        Queue::Reservation admitted, rejected;
        ASSERT_TRUE(first.try_reserve(admitted));
        EXPECT_FALSE(first.has_ready());
        ASSERT_FALSE(full.try_reserve(rejected));
        first.cancel_reservation(admitted);
        EXPECT_TRUE(first.has_ready());
        first.discard_empty_entries();
        EXPECT_FALSE(first.has_ready());
    }

    Queue::Reservation reserved;
    ASSERT_TRUE(first.try_reserve(reserved));
    first.commit(reserved, item);
    first.discard_empty_entries();
    ASSERT_EQ(first.pop().num_slices, 1);
}

TEST(BoundedMPSCQueueTest, TryPopSkipsCanceledReservationsBeforeReadyEntry) {
    Queue queue;
    Queue::Reservation first_reserved;
    Queue::Reservation second_reserved;
    ASSERT_TRUE(queue.try_reserve(first_reserved));
    ASSERT_TRUE(queue.try_reserve(second_reserved));
    auto item = entry(4);
    ASSERT_TRUE(queue.try_push(item));

    SliceList out;
    // An unpublished reservation at head blocks dequeuing later entries.
    EXPECT_FALSE(queue.try_pop(out));

    queue.cancel_reservation(first_reserved);
    queue.cancel_reservation(second_reserved);
    ASSERT_TRUE(queue.try_pop(out));
    EXPECT_EQ(out.num_slices, 4);

    // Trailing canceled reservations alone still report empty.
    Queue::Reservation trailing_reserved;
    ASSERT_TRUE(queue.try_reserve(trailing_reserved));
    queue.cancel_reservation(trailing_reserved);
    EXPECT_FALSE(queue.try_pop(out));
}

}  // namespace
}  // namespace tent
}  // namespace mooncake

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

#include <chrono>
#include <condition_variable>
#include <future>
#include <mutex>
#include <thread>
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

TEST(BoundedMPSCQueueTest, ReservationHidesEntryUntilCommit) {
    Queue queue;
    Queue::Reservation reservation;
    ASSERT_TRUE(queue.try_reserve(reservation));

    EXPECT_EQ(queue.pop().num_slices, 0);
    auto item = entry(5);
    queue.commit(reservation, item);
    EXPECT_EQ(queue.pop().num_slices, 5);
}

TEST(BoundedMPSCQueueTest, CancelledReservationPublishesOnlyEmptyEntry) {
    Queue first;
    Queue full;
    for (int i = 0; i < 8; ++i) {
        auto item = entry(1);
        ASSERT_TRUE(full.try_push(item));
    }

    Queue::Reservation first_reservation;
    Queue::Reservation rejected_reservation;
    ASSERT_TRUE(first.try_reserve(first_reservation));
    ASSERT_FALSE(full.try_reserve(rejected_reservation));

    // A failed later reservation rolls back the earlier queue without ever
    // publishing the real batch entry.
    first.cancel_reservation(first_reservation);
    EXPECT_EQ(first.pop().num_slices, 0);
    EXPECT_EQ(first.pop().num_slices, 0);
}

TEST(BoundedMPSCQueueTest, ReadinessTracksPublicationAtTheHead) {
    Queue queue;
    Queue::Reservation first;
    Queue::Reservation second;
    EXPECT_FALSE(queue.has_ready());
    ASSERT_TRUE(queue.try_reserve(first));
    ASSERT_TRUE(queue.try_reserve(second));
    queue.commit(second, entry(7));
    EXPECT_FALSE(queue.has_ready());

    queue.cancel_reservation(first);
    EXPECT_TRUE(queue.has_ready());
    EXPECT_EQ(queue.pop().num_slices, 0);
    EXPECT_TRUE(queue.has_ready());
    EXPECT_EQ(queue.pop().num_slices, 7);
    EXPECT_FALSE(queue.has_ready());
}

TEST(BoundedMPSCQueueTest, RejectionsPastCapacityCanDrainIdleQueue) {
    Queue first;
    Queue full;
    for (int i = 0; i < 8; ++i) {
        auto item = entry(1);
        ASSERT_TRUE(full.try_push(item));
    }

    // Model a worker with no real inflight slices. Rejection must not fill
    // its queue with tombstones, even after several complete capacity wraps.
    const int inflight_slices = 0;
    for (int attempt = 0; attempt < 32; ++attempt) {
        Queue::Reservation first_reservation;
        Queue::Reservation rejected_reservation;
        ASSERT_TRUE(first.try_reserve(first_reservation));
        ASSERT_FALSE(full.try_reserve(rejected_reservation));
        first.cancel_reservation(first_reservation);

        ASSERT_TRUE(inflight_slices > 0 || first.has_ready());
        std::vector<SliceList> drained;
        first.pop(drained);
        ASSERT_EQ(drained.size(), 1u);
        EXPECT_EQ(drained[0].num_slices, 0);
        EXPECT_FALSE(first.has_ready());
    }

    // Once the later queue has room, an ordinary admission can still proceed.
    full.pop();
    Queue::Reservation first_reservation;
    Queue::Reservation later_reservation;
    ASSERT_TRUE(first.try_reserve(first_reservation));
    ASSERT_TRUE(full.try_reserve(later_reservation));
    first.commit(first_reservation, entry(3));
    full.commit(later_reservation, entry(3));
    EXPECT_EQ(first.pop().num_slices, 3);
}

TEST(BoundedMPSCQueueTest, CancelledReservationWakesZeroInflightConsumer) {
    Queue queue;
    Queue::Reservation reservation;
    ASSERT_TRUE(queue.try_reserve(reservation));
    std::mutex mutex;
    std::condition_variable cv;
    std::promise<void> consumer_started;
    std::vector<SliceList> drained;
    bool woke = false;
    const int inflight_slices = 0;

    std::thread consumer([&]() {
        std::unique_lock<std::mutex> lock(mutex);
        consumer_started.set_value();
        woke = cv.wait_for(lock, std::chrono::seconds(2), [&]() {
            return inflight_slices > 0 || queue.has_ready();
        });
        if (woke) queue.pop(drained);
    });
    consumer_started.get_future().wait();

    queue.cancel_reservation(reservation);
    {
        // Serialize with the consumer's transition into wait, as admitBatch
        // does, so a notification cannot be lost between its check and sleep.
        std::lock_guard<std::mutex> lock(mutex);
        cv.notify_all();
    }
    consumer.join();

    ASSERT_TRUE(woke);
    ASSERT_EQ(drained.size(), 1u);
    EXPECT_EQ(drained[0].num_slices, 0);
    EXPECT_FALSE(queue.has_ready());
}

TEST(BoundedMPSCQueueTest, DiscardEmptyEntriesWithoutSendQuota) {
    Queue queue;
    const bool can_send = false;
    std::vector<SliceList> dispatched;

    // Match the worker's drain-before-quota order. Repeated cancellation must
    // not exhaust the queue when sending is disabled for this priority.
    for (int attempt = 0; attempt < 32; ++attempt) {
        Queue::Reservation reservation;
        ASSERT_TRUE(queue.try_reserve(reservation));
        queue.cancel_reservation(reservation);
        ASSERT_TRUE(queue.has_ready());

        queue.discard_empty_entries();
        if (can_send) queue.pop(dispatched);
        EXPECT_FALSE(queue.has_ready());
        EXPECT_TRUE(dispatched.empty());
    }
}

TEST(BoundedMPSCQueueTest, DiscardEmptyEntriesStopsAtRealHead) {
    Queue queue;
    Queue::Reservation first;
    Queue::Reservation real;
    Queue::Reservation last;
    ASSERT_TRUE(queue.try_reserve(first));
    ASSERT_TRUE(queue.try_reserve(real));
    ASSERT_TRUE(queue.try_reserve(last));
    queue.cancel_reservation(first);
    queue.commit(real, entry(7));
    queue.cancel_reservation(last);

    queue.discard_empty_entries();
    ASSERT_TRUE(queue.has_ready());
    EXPECT_EQ(queue.pop().num_slices, 7);
    ASSERT_TRUE(queue.has_ready());
    queue.discard_empty_entries();
    EXPECT_FALSE(queue.has_ready());
}

TEST(BoundedMPSCQueueTest, DiscardEmptyEntriesPreservesUnpublishedHead) {
    Queue queue;
    queue.discard_empty_entries();  // An empty queue is also a no-op.
    Queue::Reservation first;
    Queue::Reservation canceled;
    Queue::Reservation real;
    ASSERT_TRUE(queue.try_reserve(first));
    ASSERT_TRUE(queue.try_reserve(canceled));
    ASSERT_TRUE(queue.try_reserve(real));
    queue.cancel_reservation(canceled);
    queue.commit(real, entry(9));

    queue.discard_empty_entries();
    EXPECT_FALSE(queue.has_ready());
    queue.cancel_reservation(first);
    queue.discard_empty_entries();
    ASSERT_TRUE(queue.has_ready());
    EXPECT_EQ(queue.pop().num_slices, 9);
    EXPECT_FALSE(queue.has_ready());
}

TEST(BoundedMPSCQueueTest, DiscardEmptyEntriesReturnsEveryQueueSlot) {
    Queue queue;
    for (int i = 0; i < 8; ++i) {
        Queue::Reservation reservation;
        ASSERT_TRUE(queue.try_reserve(reservation));
        queue.cancel_reservation(reservation);
    }

    queue.discard_empty_entries();
    EXPECT_FALSE(queue.has_ready());
    for (int i = 1; i <= 8; ++i) {
        auto item = entry(i);
        ASSERT_TRUE(queue.try_push(item));
    }
    auto extra = entry(9);
    EXPECT_FALSE(queue.try_push(extra));
    for (int i = 1; i <= 8; ++i) {
        EXPECT_EQ(queue.pop().num_slices, i);
    }
    EXPECT_FALSE(queue.has_ready());
}

}  // namespace
}  // namespace tent
}  // namespace mooncake

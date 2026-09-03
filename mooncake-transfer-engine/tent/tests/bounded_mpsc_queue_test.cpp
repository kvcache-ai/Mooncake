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

// Regression for issue #3636 (production/initial-submit path). Models the
// admission contract of Workers::submit(): when the target worker queue is
// full the initial submit — which runs on the caller's thread — must report
// the rejection WITHOUT blocking and WITHOUT counting the slices as inflight,
// so RdmaTransport::submitTransferTasks() can cancel the task and terminate
// the batch instead of leaving it PENDING forever or spinning. The prior code
// called the blocking push() and always returned OK, so this sequence would
// wedge the caller.
TEST(BoundedMPSCQueueTest, InitialSubmitRejectionTerminatesBatch) {
    Queue queue;
    for (int i = 0; i < 8; ++i) {
        auto item = entry(1);
        ASSERT_TRUE(queue.try_push(item));  // queue now at capacity
    }

    // Mirror Workers::submit(): admit via try_push, only bump inflight on
    // success, and surface a TooManyRequests-equivalent failure otherwise.
    long inflight = 0;
    auto submit = [&](SliceList& list) -> bool {
        if (!queue.try_push(list)) return false;  // admission rejected
        inflight += list.num_slices;
        return true;
    };

    auto rejected = entry(4);
    const bool admitted = submit(rejected);

    // The batch neither pends nor spins: submit returns a hard failure and no
    // phantom inflight count is left behind for the rejected slices.
    EXPECT_FALSE(admitted);
    EXPECT_EQ(inflight, 0);

    // Once the consumer drains one slot, a retry of the same batch succeeds —
    // proving the rejection is transient backpressure, not a lost batch.
    EXPECT_EQ(queue.pop().num_slices, 1);
    EXPECT_TRUE(submit(rejected));
    EXPECT_EQ(inflight, 4);
}

// Regression for issue #3661 (atomic-admission follow-up). catyans pointed out
// that a submit-then-cancel loop is unsafe: Workers::cancel() is asynchronous
// and cannot recall a slice already posted to a QP, so admitting workers [0, i)
// and then rejecting worker i leaves live posted WRs racing the engine's
// fallback and a later freeSubBatch(). Workers::admitBatch() closes this by
// checking EVERY target worker queue for room BEFORE pushing any list: if any
// is full, none is pushed, so no worker ever observes — and therefore never
// posts — a partially-admitted batch.
//
// This models admitBatch() with the SliceList stand-in: 4 worker queues, one
// pre-saturated, a batch with a list for each worker. It asserts the pre-check
// rejects and that ZERO lists were pushed to ANY queue (nothing to post, drain,
// or cancel), which is the property the async-cancel hazard needed.
TEST(BoundedMPSCQueueTest, AtomicAdmissionPushesNothingWhenAnyQueueFull) {
    constexpr int kNumWorkers = 4;
    std::vector<Queue> worker_queues(kNumWorkers);

    // Saturate worker 2's queue so the batch cannot be fully admitted.
    constexpr int kFullWorker = 2;
    for (int i = 0; i < 8; ++i) {
        auto filler = entry(1);
        ASSERT_TRUE(worker_queues[kFullWorker].try_push(filler));
    }
    // The other three start empty.
    for (int w = 0; w < kNumWorkers; ++w) {
        if (w == kFullWorker) continue;
        ASSERT_TRUE(worker_queues[w].has_free_slot());
    }

    // One non-empty list per worker (the round-robin scatter fills every
    // worker in the general case).
    std::vector<SliceList> lists(kNumWorkers);
    for (int w = 0; w < kNumWorkers; ++w) lists[w] = entry(2);

    // Model admitBatch(): phase 1 pre-check across all target queues.
    auto pre_check_ok = [&]() -> bool {
        for (int w = 0; w < kNumWorkers; ++w) {
            if (lists[w].num_slices == 0) continue;
            if (!worker_queues[w].has_free_slot()) return false;
        }
        return true;
    };

    const bool admitted = pre_check_ok();
    EXPECT_FALSE(admitted);  // worker 2 is full → whole batch rejected

    // The core invariant: because the pre-check failed, phase 2 never runs, so
    // NONE of the batch's lists were pushed. The empty workers are still empty
    // (occupancy unchanged) and the full worker holds only its 8 fillers — no
    // batch slice sits in any queue to be posted or later freed.
    for (int w = 0; w < kNumWorkers; ++w) {
        if (w == kFullWorker) continue;
        // Empty queue: a pop yields the default (num_slices == 0).
        EXPECT_EQ(worker_queues[w].pop().num_slices, 0)
            << "worker " << w << " received a list despite batch rejection";
    }
}

// Companion: when every target queue has room, admitBatch commits all lists and
// counts their slices inflight — the success path must not regress.
TEST(BoundedMPSCQueueTest, AtomicAdmissionCommitsAllWhenEveryQueueHasRoom) {
    constexpr int kNumWorkers = 4;
    std::vector<Queue> worker_queues(kNumWorkers);
    std::vector<SliceList> lists(kNumWorkers);
    for (int w = 0; w < kNumWorkers; ++w) lists[w] = entry(3);

    // Phase 1 pre-check passes (all empty), phase 2 commits every list.
    bool pre_ok = true;
    for (int w = 0; w < kNumWorkers; ++w)
        pre_ok = pre_ok && worker_queues[w].has_free_slot();
    ASSERT_TRUE(pre_ok);

    long inflight = 0;
    for (int w = 0; w < kNumWorkers; ++w) {
        ASSERT_TRUE(worker_queues[w].try_push(lists[w]));
        inflight += lists[w].num_slices;
    }

    EXPECT_EQ(inflight, kNumWorkers * 3);
    for (int w = 0; w < kNumWorkers; ++w)
        EXPECT_EQ(worker_queues[w].pop().num_slices, 3);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake

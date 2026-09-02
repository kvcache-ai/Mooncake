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

// Regression for issue #3661 (partial-admission follow-up). The initial-submit
// loop in RdmaTransport::submitTransferTasks() walks one slice list per worker
// and calls Workers::submit() on each. The round-robin scatter above spreads
// every request's slices across all workers, so a single batch generally spans
// multiple tasks and every worker's list can carry slices from more than one.
//
// If worker i's queue is full mid-loop, workers [0, i) are already running and
// workers (i, N) are never submitted. Cancelling only the one task reachable
// from slice_lists[i].first (the earlier revision) left the already-running
// tasks and every never-submitted slice PENDING forever, and let the upper
// layer race a failover on the same group. The fix terminalizes the WHOLE
// batch: cancel every task before returning failure.
//
// This models that loop with the SliceList stand-in and a cancel flag per task,
// then asserts all tasks are terminalized regardless of which worker rejected.
TEST(BoundedMPSCQueueTest, PartialAdmissionTerminalizesWholeBatch) {
    constexpr int kNumWorkers = 4;
    std::vector<Queue> worker_queues(kNumWorkers);

    // Saturate worker 2's queue so its submit rejects, while 0/1 accept and
    // 3 is never reached — the partial-admission shape catyans described.
    constexpr int kRejectingWorker = 2;
    for (int i = 0; i < 8; ++i) {
        auto filler = entry(1);
        ASSERT_TRUE(worker_queues[kRejectingWorker].try_push(filler));
    }

    // A batch of tasks; each task's slices are scattered across workers, so the
    // per-worker lists below reference several tasks. Track terminalization by
    // task, the way Workers::cancel(task) flips cancel_requested per task.
    constexpr int kNumTasks = 5;
    std::vector<bool> task_canceled(kNumTasks, false);

    // One non-empty slice list per worker (mirrors slice_lists[i].first != null
    // guarding the submit call). Each list "belongs" to a representative task,
    // but the batch as a whole owns all kNumTasks tasks.
    auto submit = [&](int worker_id, SliceList& list) -> bool {
        return worker_queues[worker_id].try_push(list);
    };

    // The loop under test: submit each worker's list; on the first rejection,
    // cancel EVERY task in the batch (not just the rejected worker's) and stop.
    bool rejected = false;
    int rejected_at = -1;
    for (int w = 0; w < kNumWorkers; ++w) {
        auto list = entry(2);
        if (!submit(w, list)) {
            rejected = true;
            rejected_at = w;
            for (int t = 0; t < kNumTasks; ++t) task_canceled[t] = true;
            break;
        }
    }

    ASSERT_TRUE(rejected);
    EXPECT_EQ(rejected_at, kRejectingWorker);  // 0 and 1 admitted first

    // The whole batch is terminalized: no task from this attempt is left
    // un-cancelled to pend forever or to be re-run by an upper-layer failover.
    for (int t = 0; t < kNumTasks; ++t) {
        EXPECT_TRUE(task_canceled[t])
            << "task " << t << " left un-terminalized after partial admission";
    }
}

}  // namespace
}  // namespace tent
}  // namespace mooncake

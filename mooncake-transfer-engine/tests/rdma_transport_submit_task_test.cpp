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

// Regression test for RdmaTransport::submitTransferTask()'s
// "memory region not registered" (!found_device) error path: a slice that
// already succeeded device selection and was queued into the function-local
// slices_to_post accumulator must not be deallocated a second time when a
// later slice in the same call fails device selection, since it is already
// owned (and will be freed exactly once) by its TransferTask::slice_list.
//
// This exercises the real, unmodified RdmaTransport::submitTransferTask()
// entry point. It avoids needing a real RDMA device by using a bare
// RdmaContext (construct() never called): submitTransferTask() only checks
// RdmaContext::active(), which defaults to true, for slices that fail device
// selection.
//
// Every request here is a *single* TransferTask whose length spans two
// slice_size blocks: the first offset resolves inside the registered
// buffer (succeeds, queued into slices_to_post), and the second offset
// starts exactly at the buffer's end (fails every retry). This reproduces
// the bug with a single task instead of a task pair, so the only slices
// touching ThreadLocalSliceCache belong to the one task under test -- no
// second task's own slice churns through the shared cache and disturbs the
// recycling queue's ordering.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "config.h"
#include "rdma_test_peers.h"
#include "transfer_metadata.h"
#include "transport/rdma_transport/rdma_context.h"
#include "transport/rdma_transport/rdma_transport.h"

using namespace mooncake;

namespace {

using SegmentDesc = TransferMetadata::SegmentDesc;
using BufferDesc = TransferMetadata::BufferDesc;

// Test seam: completes every flushed slice immediately via markSuccess()
// (the real CQE path), standing in for hardware. Lets submitTransferTask()
// flush paths run end-to-end in this deviceless fixture, where the base
// class would dereference its null worker_pool_.
class FakeCompletionContext : public RdmaContext {
   public:
    using RdmaContext::RdmaContext;

    int submitPostSend(
        const std::vector<Transport::Slice *> &slice_list) override {
        posted_ += slice_list.size();
        for (auto *slice : slice_list) slice->markSuccess();
        return 0;
    }

    size_t posted_ = 0;
};

class SubmitTransferTaskTest : public ::testing::Test {
   protected:
    static constexpr uint64_t kBufferAddr = 0x10000;

    std::shared_ptr<TransferMetadata> metadata_;
    std::unique_ptr<RdmaTransport> transport_;
    std::shared_ptr<FakeCompletionContext> context_;
    uint64_t block_size_ = 0;

    void SetUp() override {
        block_size_ = globalConfig().slice_size;

        metadata_ = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
        transport_ = std::make_unique<RdmaTransport>();
        RdmaTransportTestPeer::bindMetadata(*transport_, metadata_,
                                            "unit-test-server:1234");

        // construct() is never called: no real device is opened. active()
        // defaults to true, and the only other context call these tests
        // reach, submitPostSend(), is overridden by FakeCompletionContext.
        context_ = std::make_shared<FakeCompletionContext>(*transport_,
                                                           "mlx5_unit_test");
        RdmaTransportTestPeer::addContext(*transport_, context_);

        auto desc = std::make_shared<SegmentDesc>();
        desc->name = "unit-test-server:1234";
        desc->protocol = "rdma";
        BufferDesc buffer;
        buffer.name = "cpu:0";
        buffer.addr = kBufferAddr;
        // Exactly one slice_size: a request longer than this has its first
        // slice land fully inside the buffer and its second slice start
        // exactly at the (unregistered) byte past the end.
        buffer.length = block_size_;
        buffer.lkey = {1};
        buffer.rkey = {1};
        desc->buffers.push_back(buffer);
        ASSERT_EQ(
            desc->topology.parse(R"({"cpu:0": [["mlx5_unit_test"], []]})"), 0);
        metadata_->addLocalSegment(LOCAL_SEGMENT_ID, desc->name,
                                   std::move(desc));
    }

    // Submits one task whose request spans 2 slice_size blocks starting at
    // kBufferAddr. The first slice (offset 0) resolves inside the
    // registered buffer and succeeds; the second slice (offset
    // block_size_) starts past the buffer's end and fails every retry, so
    // the call always returns early via the !found_device branch.
    //
    // `req`/`batch` are owned by the caller and left alive on return, so
    // the caller controls when the task's slices go back to the cache.
    // The task gets a valid batch_id: the fast-fail path's markFailed()
    // dereferences toBatchDesc(batch_id) under USE_EVENT_DRIVEN_COMPLETION
    // (production sets it in submitTransfer(); tests calling
    // submitTransferTask() directly must too).
    void submitFailingTask(Transport::TransferRequest &req,
                           Transport::BatchDesc &batch) {
        batch.batch_size = 1;
        batch.task_list.resize(1);
        auto &task = batch.task_list[0];
        task.batch_id = reinterpret_cast<Transport::BatchID>(&batch);

        req.opcode = Transport::TransferRequest::WRITE;
        req.source = reinterpret_cast<void *>(kBufferAddr);
        req.length = 2 * block_size_;
        req.target_id = LOCAL_SEGMENT_ID;
        req.target_offset = 0;
        task.request = &req;

        auto status = transport_->submitTransferTask({&task});
        EXPECT_FALSE(status.ok());
        EXPECT_TRUE(status.IsAddressNotRegistered());
        // slice_list[0]: the first slice, in-buffer, queued into
        // slices_to_post; fast-failed by the abort path today, double-freed
        // by the old bug.
        // slice_list[1]: the second slice, out-of-buffer, the one that
        // fails device selection; never added to slices_to_post, counted
        // and fast-failed by the abort path.
        ASSERT_EQ(task.slice_list.size(), 2u);
    }
};

// task1's first slice is queued for reuse twice in a row, with nothing else
// touching the cache in between: once by the buggy manual deallocate()
// inside submitTransferTask() (while task1 is still alive), and again by
// task1's own ~TransferTask() moments later. The recycling cache now holds
// two entries for that single object.
//
// task2's own call then draws from that corrupted cache twice in a row (its
// offset-0 slice, then its offset-block_size slice): with a correctly
// single-registered cache, task1's two slices (each freed exactly once)
// supply exactly two distinct pool entries, so task2's two independent
// draws come back as two distinct Slice objects -- one reusing task1's
// first slice, the other reusing its second. With the duplicate
// registration, both draws instead return task1's original object --
// proving it was freed more than once, purely by pointer comparison, with
// no forced crash or process teardown required. EXPECT_EQ(task2[0], original)
// is not itself the bug signal (a *single* legitimate free of `original`
// would also make the very next allocation reuse it); it exists to
// document that task2 is
// indeed the one hitting the corrupted state before the discriminating
// check below.
TEST_F(SubmitTransferTaskTest, FoundDeviceFailureFreesSliceTwice) {
    Transport::Slice *original = nullptr;
    {
        Transport::TransferRequest req1;
        Transport::BatchDesc batch1;
        submitFailingTask(req1, batch1);
        original = batch1.task_list[0].slice_list[0];
        // batch1 (and with it task1) destroyed here: ~TransferTask()
        // deallocate()s both of its slices, exactly once each. Under the
        // old bug, the manual deallocate() inside submitTransferTask() had
        // already returned `original` to the cache, so this destruction
        // registered it a second time -- with nothing else in between,
        // since the task's second (failed) slice was never added to
        // slices_to_post and so was never touched by that cleanup.
    }

    Transport::TransferRequest req2;
    Transport::BatchDesc batch2;
    submitFailingTask(req2, batch2);
    auto &task2 = batch2.task_list[0];

    EXPECT_EQ(task2.slice_list[0], original)
        << "sanity check: the cache should have at least one legitimate "
           "reuse of `original` queued up regardless of the bug";
    EXPECT_NE(task2.slice_list[0], task2.slice_list[1])
        << "task2's two slices cover disjoint offsets of the same request "
           "and must never be backed by the same Slice object; "
           "submitTransferTask() double-freed task1's slice on its "
           "!found_device error path, so the recycling cache handed the "
           "exact same, still-conceptually-owned pointer out twice in a "
           "row for what should have been two independent allocations";
}

// A slice already queued in slices_to_post when a later slice of the same
// task fails device selection must be fast-failed via markFailed(): not
// posted (the caller retries the whole task) and not left PENDING (the
// task would never converge and the batch never be freeable). The task
// must be immediately queryable as FAILED, with no RDMA completion
// involved.
TEST_F(SubmitTransferTaskTest,
       FoundDeviceFailureFastFailsQueuedSliceOfSameTask) {
    Transport::TransferRequest req;
    Transport::BatchDesc batch;
    submitFailingTask(req, batch);

    auto &task = batch.task_list[0];
    // slice_list[0]: in-buffer, device selection succeeded, was queued into
    // slices_to_post -- must be fast-failed, not left PENDING.
    // slice_list[1]: the slice that failed device selection -- must be
    // counted into slice_count and fast-failed too, so every slice in
    // slice_list is counted and terminal and, under
    // USE_EVENT_DRIVEN_COMPLETION, its markFailed() drives the task's
    // completion bookkeeping.
    EXPECT_EQ(task.slice_list[0]->status, Transport::Slice::FAILED);
    EXPECT_EQ(task.slice_list[1]->status, Transport::Slice::FAILED);
    EXPECT_EQ(task.slice_count, 2u);
    EXPECT_EQ(task.failed_slice_count, 2u);
    EXPECT_EQ(task.success_slice_count, 0u);
    EXPECT_TRUE(task.submit_failed);

    Transport::TransferStatus ts;
    auto batch_id = reinterpret_cast<Transport::BatchID>(&batch);
    auto get_ret = transport_->getTransferStatus(batch_id, 0, ts);
    ASSERT_TRUE(get_ret.ok());
    EXPECT_EQ(ts.s, Transport::TransferStatusEnum::FAILED);
    EXPECT_TRUE(task.is_finished);

    // The batch-vector overload shares the same submit_failed check and
    // must agree with the single-task overload above.
    std::vector<Transport::TransferStatus> batch_status;
    auto batch_get_ret = transport_->getTransferStatus(batch_id, batch_status);
    ASSERT_TRUE(batch_get_ret.ok());
    ASSERT_EQ(batch_status.size(), 1u);
    EXPECT_EQ(batch_status[0].s, Transport::TransferStatusEnum::FAILED);
}

// A task whose very first slice fails device selection: nothing of its own
// was ever queued into slices_to_post, so before the abort the task's
// accounting is entirely empty (slice_count == 0). The abort path must
// count and fail that first slice like any other, leaving the task with
// one counted, terminal slice and an immediately queryable FAILED --
// under USE_EVENT_DRIVEN_COMPLETION this is also what lets the failing
// slice's markFailed() drive the task's completion bookkeeping, since
// there is no other slice to do it.
TEST_F(SubmitTransferTaskTest, FirstSliceDeviceSelectionFailureFailsTask) {
    Transport::BatchDesc batch;
    batch.batch_size = 1;
    batch.task_list.resize(1);
    auto &task = batch.task_list[0];
    // See submitFailingTask() for why batch_id must be valid here.
    task.batch_id = reinterpret_cast<Transport::BatchID>(&batch);

    // Entirely outside the registered buffer: the first (and only) slice
    // fails every device-selection retry.
    Transport::TransferRequest req;
    req.opcode = Transport::TransferRequest::WRITE;
    req.source = reinterpret_cast<void *>(kBufferAddr + 2 * block_size_);
    req.length = block_size_;
    req.target_id = LOCAL_SEGMENT_ID;
    req.target_offset = 0;
    task.request = &req;

    auto status = transport_->submitTransferTask({&task});
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.IsAddressNotRegistered());

    // Nothing was ever queued, so nothing was flushed.
    EXPECT_EQ(context_->posted_, 0u);
    ASSERT_EQ(task.slice_list.size(), 1u);
    EXPECT_EQ(task.slice_list[0]->status, Transport::Slice::FAILED);
    EXPECT_EQ(task.slice_count, 1u);
    EXPECT_EQ(task.failed_slice_count, 1u);
    EXPECT_EQ(task.success_slice_count, 0u);
    EXPECT_TRUE(task.submit_failed);

    Transport::TransferStatus ts;
    auto batch_id = reinterpret_cast<Transport::BatchID>(&batch);
    ASSERT_TRUE(transport_->getTransferStatus(batch_id, 0, ts).ok());
    EXPECT_EQ(ts.s, Transport::TransferStatusEnum::FAILED);
    EXPECT_EQ(ts.transferred_bytes, 0u);
    EXPECT_TRUE(task.is_finished);
}

// A task after the failing one in the same call is never processed: its
// slice_count stays 0, which the slice counters alone would misreport as
// COMPLETED. submit_failed must mark it FAILED so the caller knows to
// resubmit it.
TEST_F(SubmitTransferTaskTest,
       SiblingTaskAfterFailureIsReportedFailedNotSilentlyCompleted) {
    Transport::BatchDesc batch;
    batch.batch_size = 2;
    batch.task_list.resize(2);
    // See submitFailingTask() for why batch_id must be valid here.
    for (auto &task : batch.task_list)
        task.batch_id = reinterpret_cast<Transport::BatchID>(&batch);

    Transport::TransferRequest req1;
    req1.opcode = Transport::TransferRequest::WRITE;
    req1.source = reinterpret_cast<void *>(kBufferAddr);
    req1.length = 2 * block_size_;  // triggers !found_device on its 2nd slice
    req1.target_id = LOCAL_SEGMENT_ID;
    req1.target_offset = 0;
    batch.task_list[0].request = &req1;

    // On its own this would be a perfectly valid, fully in-buffer request.
    // It never gets a chance to run in this call because task 0 fails first.
    Transport::TransferRequest req2;
    req2.opcode = Transport::TransferRequest::WRITE;
    req2.source = reinterpret_cast<void *>(kBufferAddr);
    req2.length = block_size_;
    req2.target_id = LOCAL_SEGMENT_ID;
    req2.target_offset = 0;
    batch.task_list[1].request = &req2;

    auto status = transport_->submitTransferTask(
        {&batch.task_list[0], &batch.task_list[1]});
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.IsAddressNotRegistered());

    auto &task2 = batch.task_list[1];
    EXPECT_TRUE(task2.slice_list.empty());
    EXPECT_EQ(task2.slice_count, 0u);
    EXPECT_TRUE(task2.submit_failed);

    Transport::TransferStatus ts;
    auto batch_id = reinterpret_cast<Transport::BatchID>(&batch);
    auto get_ret = transport_->getTransferStatus(batch_id, 1, ts);
    ASSERT_TRUE(get_ret.ok());
    EXPECT_EQ(ts.s, Transport::TransferStatusEnum::FAILED)
        << "task2 was never attempted in this call; without submit_failed, "
           "slice_count == 0 trivially satisfies success_slice_count + "
           "failed_slice_count == slice_count and this would be "
           "misreported as COMPLETED";
    EXPECT_TRUE(task2.is_finished);

    // The batch-vector overload must agree for both tasks: task 0 (the one
    // that actually hit !found_device) and task 1 (the untouched sibling).
    std::vector<Transport::TransferStatus> batch_status;
    auto batch_get_ret = transport_->getTransferStatus(batch_id, batch_status);
    ASSERT_TRUE(batch_get_ret.ok());
    ASSERT_EQ(batch_status.size(), 2u);
    EXPECT_EQ(batch_status[0].s, Transport::TransferStatusEnum::FAILED);
    EXPECT_EQ(batch_status[1].s, Transport::TransferStatusEnum::FAILED);
}

// Mixed-outcome batch in one live submitTransferTask() call: task1 (fully
// in-buffer) is queued, kept by failQueuedSlicesForTask() when task2 fails
// on its second slice, flushed to the FakeCompletionContext and completed;
// task3 is never reached. getTransferStatus() must report COMPLETED /
// FAILED / FAILED, with task1 untouched by the siblings' failure.
TEST_F(SubmitTransferTaskTest, MixedBatchReportsPerTaskStatusAfterFailure) {
    Transport::BatchDesc batch;
    batch.batch_size = 3;
    batch.task_list.resize(3);
    // See submitFailingTask() for why batch_id must be valid here.
    for (auto &task : batch.task_list)
        task.batch_id = reinterpret_cast<Transport::BatchID>(&batch);
    auto &task1 = batch.task_list[0];
    auto &task2 = batch.task_list[1];
    auto &task3 = batch.task_list[2];

    Transport::TransferRequest req1;
    req1.opcode = Transport::TransferRequest::WRITE;
    req1.source = reinterpret_cast<void *>(kBufferAddr);
    req1.length = block_size_;  // fully in-buffer: succeeds
    req1.target_id = LOCAL_SEGMENT_ID;
    req1.target_offset = 0;
    task1.request = &req1;
    Transport::TransferRequest req2 = req1;
    req2.length = 2 * block_size_;  // fails on its second slice
    task2.request = &req2;
    Transport::TransferRequest req3 = req1;  // valid, but never reached
    task3.request = &req3;

    auto status = transport_->submitTransferTask({&task1, &task2, &task3});
    EXPECT_TRUE(status.IsAddressNotRegistered());

    // task1's queued slice was kept and flushed to the fake context, not
    // fast-failed alongside task2's.
    EXPECT_EQ(context_->posted_, 1u);
    ASSERT_EQ(task1.slice_list.size(), 1u);
    EXPECT_EQ(task1.slice_list[0]->status, Transport::Slice::SUCCESS);
    EXPECT_FALSE(task1.submit_failed);

    ASSERT_EQ(task2.slice_list.size(), 2u);
    EXPECT_EQ(task2.slice_list[0]->status, Transport::Slice::FAILED);
    EXPECT_EQ(task2.slice_list[1]->status, Transport::Slice::FAILED);
    EXPECT_TRUE(task2.submit_failed);
    EXPECT_TRUE(task3.slice_list.empty());
    EXPECT_TRUE(task3.submit_failed);

    auto batch_id = reinterpret_cast<Transport::BatchID>(&batch);
    std::vector<Transport::TransferStatus> batch_status;
    ASSERT_TRUE(transport_->getTransferStatus(batch_id, batch_status).ok());
    ASSERT_EQ(batch_status.size(), 3u);
    EXPECT_EQ(batch_status[0].s, Transport::TransferStatusEnum::COMPLETED);
    EXPECT_EQ(batch_status[0].transferred_bytes, block_size_);
    EXPECT_EQ(batch_status[1].s, Transport::TransferStatusEnum::FAILED);
    EXPECT_EQ(batch_status[2].s, Transport::TransferStatusEnum::FAILED);
    EXPECT_TRUE(task1.is_finished);

    Transport::TransferStatus ts;
    ASSERT_TRUE(transport_->getTransferStatus(batch_id, 0, ts).ok());
    EXPECT_EQ(ts.s, Transport::TransferStatusEnum::COMPLETED);
    ASSERT_TRUE(transport_->getTransferStatus(batch_id, 1, ts).ok());
    EXPECT_EQ(ts.s, Transport::TransferStatusEnum::FAILED);
    ASSERT_TRUE(transport_->getTransferStatus(batch_id, 2, ts).ok());
    EXPECT_EQ(ts.s, Transport::TransferStatusEnum::FAILED);
}

#ifdef USE_EVENT_DRIVEN_COMPLETION
// Event-driven completion for the same mixed-outcome batch: a waiter
// blocked on the batch's completion_cv must be woken by the abort path
// itself -- no getTransferStatus() polling involved -- with every task
// accounted for in finished_task_count: task1 via its slice's
// markSuccess(), task2 via its slices' markFailed() (including the
// device-selection-failed slice, counted and failed by
// failTaskSubmission()), task3 via the sibling bookkeeping in
// failTaskSubmission() (it has no slice to drive
// check_batch_completion()).
TEST_F(SubmitTransferTaskTest, EventDrivenMixedBatchNotifiesWaiter) {
    Transport::BatchDesc batch;
    batch.batch_size = 3;
    batch.task_list.resize(3);
    for (auto &task : batch.task_list)
        task.batch_id = reinterpret_cast<Transport::BatchID>(&batch);
    auto &task1 = batch.task_list[0];
    auto &task2 = batch.task_list[1];
    auto &task3 = batch.task_list[2];

    Transport::TransferRequest req1;
    req1.opcode = Transport::TransferRequest::WRITE;
    req1.source = reinterpret_cast<void *>(kBufferAddr);
    req1.length = block_size_;  // fully in-buffer: succeeds
    req1.target_id = LOCAL_SEGMENT_ID;
    req1.target_offset = 0;
    task1.request = &req1;
    Transport::TransferRequest req2 = req1;
    req2.length = 2 * block_size_;  // fails on its second slice
    task2.request = &req2;
    Transport::TransferRequest req3 = req1;  // valid, but never reached
    task3.request = &req3;

    // Start the waiter before submitting, mirroring
    // TransferEngineOperationState::wait_for_completion(): predicate
    // re-checked under completion_mutex, so either notify order is safe.
    // The 10s cap turns a missing notification into a test failure
    // instead of a hang.
    std::atomic<bool> woken{false};
    std::thread waiter([&] {
        std::unique_lock<std::mutex> lock(batch.completion_mutex);
        woken = batch.completion_cv.wait_for(
            lock, std::chrono::seconds(10),
            [&] { return batch.is_finished.load(std::memory_order_relaxed); });
    });

    auto status = transport_->submitTransferTask({&task1, &task2, &task3});
    EXPECT_TRUE(status.IsAddressNotRegistered());
    waiter.join();

    EXPECT_TRUE(woken.load()) << "waiter on completion_cv was not notified";
    EXPECT_EQ(batch.finished_task_count.load(), 3u);
    EXPECT_TRUE(batch.is_finished.load());
    EXPECT_TRUE(batch.has_failure.load());

    // Per-task terminal states, all driven by the event chain itself:
    EXPECT_TRUE(task1.is_finished);
    EXPECT_EQ(task1.completed_slice_count, 1u);
    EXPECT_TRUE(task2.is_finished);
    EXPECT_EQ(task2.completed_slice_count, 2u);
    EXPECT_TRUE(task3.is_finished);
    EXPECT_EQ(task3.completed_slice_count, 0u);
}

// Event-driven completion when the very first slice fails device
// selection: the task's only counted slice is the one failTaskSubmission()
// counted and failed, so its markFailed() alone must complete the task and
// the batch -- the waiter is notified with no polling involved, and the
// subsequent status query agrees with the event outcome.
TEST_F(SubmitTransferTaskTest, EventDrivenFirstSliceFailureNotifiesWaiter) {
    Transport::BatchDesc batch;
    batch.batch_size = 1;
    batch.task_list.resize(1);
    auto &task = batch.task_list[0];
    // See submitFailingTask() for why batch_id must be valid here.
    task.batch_id = reinterpret_cast<Transport::BatchID>(&batch);

    // Entirely outside the registered buffer: the first (and only) slice
    // fails every device-selection retry.
    Transport::TransferRequest req;
    req.opcode = Transport::TransferRequest::WRITE;
    req.source = reinterpret_cast<void *>(kBufferAddr + 2 * block_size_);
    req.length = block_size_;
    req.target_id = LOCAL_SEGMENT_ID;
    req.target_offset = 0;
    task.request = &req;

    std::atomic<bool> woken{false};
    std::thread waiter([&] {
        std::unique_lock<std::mutex> lock(batch.completion_mutex);
        woken = batch.completion_cv.wait_for(
            lock, std::chrono::seconds(10),
            [&] { return batch.is_finished.load(std::memory_order_relaxed); });
    });

    auto status = transport_->submitTransferTask({&task});
    EXPECT_TRUE(status.IsAddressNotRegistered());
    waiter.join();

    EXPECT_TRUE(woken.load()) << "waiter on completion_cv was not notified";
    EXPECT_EQ(batch.finished_task_count.load(), 1u);
    EXPECT_TRUE(batch.is_finished.load());
    EXPECT_TRUE(batch.has_failure.load());
    // Set by the event chain itself, before any status poll:
    EXPECT_TRUE(task.is_finished);
    EXPECT_EQ(task.completed_slice_count, 1u);

    Transport::TransferStatus ts;
    auto batch_id = reinterpret_cast<Transport::BatchID>(&batch);
    ASSERT_TRUE(transport_->getTransferStatus(batch_id, 0, ts).ok());
    EXPECT_EQ(ts.s, Transport::TransferStatusEnum::FAILED);
    EXPECT_EQ(ts.transferred_bytes, 0u);
}
#endif  // USE_EVENT_DRIVEN_COMPLETION

}  // namespace

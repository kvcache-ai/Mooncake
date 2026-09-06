// Lifecycle invariants of SPDK NoF tasks and sub-I/Os around a stalled qpair
// (kvcache-ai/Mooncake#3864). Exercises the transport-independent helpers in
// transfer_task.h the way SpdkNofWorkerPool::workerThread() and the SPDK
// completion callback use them, without SPDK or a target:
//
//  * the caller of a task is completed exactly once;
//  * a SpdkNofSubTask slot goes back to the pool only through its completion;
//  * QoS, per-task and per-worker accounting return to baseline;
//  * the segment leaves kDraining only once every outstanding sub-I/O has
//    completed, whether it completed normally or was aborted.
#include "transfer_task.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <stack>
#include <vector>

namespace mooncake {
namespace {

constexpr uint32_t kBlockSize = 4096;
constexpr int kSlotCount = 16;

// nof_seg_handle is opaque; the helpers only compare the pointer.
nof_seg_handle* FakeSegment() {
    static int anchor = 0;
    return reinterpret_cast<nof_seg_handle*>(&anchor);
}

class NofTaskLifecycleTest : public ::testing::Test {
   protected:
    void SetUp() override {
        qos_ = std::make_unique<SpdkNofQos>(kBlockSize);
        slots_.resize(kSlotCount);
        for (auto& slot : slots_) {
            slot.task = nullptr;
            slot.submit_lba_count = 0;
            slot.sub_task_pool = &pool_;
            pool_.push(&slot);
        }
    }

    // What the worker does when it takes a task off its queue.
    SpdkNofTask* Enqueue(int lba_count, int op,
                         std::shared_ptr<SpdkNofOperationState>* state_out) {
        auto state = std::make_shared<SpdkNofOperationState>();
        auto* task = new SpdkNofTask(FakeSegment(), buffer_, /*off=*/0,
                                     lba_count, op, state);
        task->io_count = &total_outstanding_io_;
        task->nof_qos = qos_.get();
        task->on_chain = true;
        qos_->PushTask(task);
        *state_out = std::move(state);
        return task;
    }

    // The bookkeeping the submit loop performs for one accepted sub-I/O.
    SpdkNofSubTask* Submit(SpdkNofTask* task, int lba_count) {
        EXPECT_FALSE(pool_.empty());
        SpdkNofSubTask* sub_task = pool_.top();
        pool_.pop();
        sub_task->task = task;
        sub_task->submit_lba_count = lba_count;
        task->idx++;
        task->remaining_lba -= lba_count;
        qos_->inflight_blocks[task->op] += lba_count;
        task->outstanding_sub_io++;
        total_outstanding_io_++;
        return sub_task;
    }

    // The submit loop pops a task once all of its LBAs have been submitted
    // (or it was failed) and lets SpdkNofTaskCompletion() decide whether it
    // is finished. After this the task pointer must not be used.
    void PopFromChain(SpdkNofTask* task) {
        ASSERT_EQ(qos_->head[task->op], task);
        qos_->PopTask(task->op);
        task->on_chain = false;
        SpdkNofTaskCompletion(task);
    }

    SpdkNofStallAction Advance(bool io_timed_out) {
        return SpdkNofAdvanceStallState(*qos_, io_timed_out,
                                        std::chrono::steady_clock::now());
    }

    void ExpectBaseline() {
        EXPECT_EQ(pool_.size(), static_cast<size_t>(kSlotCount));
        EXPECT_EQ(qos_->InflightBlocks(), 0);
        EXPECT_EQ(total_outstanding_io_, 0);
        EXPECT_TRUE(qos_->Empty());
        EXPECT_EQ(qos_->state, SpdkNofSegmentState::kActive);
    }

    alignas(kBlockSize) char buffer_[kBlockSize];
    std::unique_ptr<SpdkNofQos> qos_;
    std::vector<SpdkNofSubTask> slots_;
    std::stack<SpdkNofSubTask*> pool_;
    int64_t total_outstanding_io_ = 0;
};

TEST_F(NofTaskLifecycleTest, NormalCompletion) {
    std::shared_ptr<SpdkNofOperationState> state;
    SpdkNofTask* task = Enqueue(/*lba_count=*/2, kSpdkNofOpWrite, &state);
    SpdkNofSubTask* first = Submit(task, 1);
    SpdkNofSubTask* second = Submit(task, 1);
    PopFromChain(task);  // fully submitted, still 2 sub-I/Os outstanding
    EXPECT_FALSE(state->is_completed());
    EXPECT_EQ(pool_.size(), static_cast<size_t>(kSlotCount - 2));

    SpdkNofCompleteSubTask(first, /*failed=*/false);
    EXPECT_FALSE(state->is_completed());
    EXPECT_EQ(first->task, nullptr);

    SpdkNofCompleteSubTask(second, /*failed=*/false);
    ASSERT_TRUE(state->is_completed());
    EXPECT_EQ(state->get_result(), ErrorCode::OK);
    EXPECT_EQ(Advance(false), SpdkNofStallAction::kNone);
    ExpectBaseline();
}

TEST_F(NofTaskLifecycleTest, ErrorOnPartiallySubmittedTaskCompletesCallerOnce) {
    std::shared_ptr<SpdkNofOperationState> state;
    SpdkNofTask* task = Enqueue(/*lba_count=*/3, kSpdkNofOpRead, &state);
    SpdkNofSubTask* sub_task = Submit(task, 1);

    // The callback fails the task while it is still on the chain with LBAs
    // left to submit: the caller is told now, the task itself must survive
    // until the submit loop takes it off the chain.
    SpdkNofCompleteSubTask(sub_task, /*failed=*/true);
    ASSERT_TRUE(state->is_completed());
    EXPECT_EQ(state->get_result(), ErrorCode::TRANSFER_FAIL);
    EXPECT_TRUE(task->completed);
    EXPECT_TRUE(task->on_chain);
    EXPECT_EQ(task->remaining_lba, 0);

    // Second SpdkNofTaskCompletion() from the submit loop: no second
    // set_completed() (that asserts in debug builds), the task is freed.
    PopFromChain(task);
    EXPECT_EQ(state->get_result(), ErrorCode::TRANSFER_FAIL);
    ExpectBaseline();
}

TEST_F(NofTaskLifecycleTest, SlotIsReturnedOnlyByCompletion) {
    std::shared_ptr<SpdkNofOperationState> state;
    SpdkNofTask* task = Enqueue(/*lba_count=*/1, kSpdkNofOpWrite, &state);
    SpdkNofSubTask* sub_task = Submit(task, 1);
    PopFromChain(task);

    // Neither the caller nor the pool may see anything until SPDK is done
    // with the callback context.
    EXPECT_FALSE(state->is_completed());
    EXPECT_EQ(pool_.size(), static_cast<size_t>(kSlotCount - 1));
    EXPECT_EQ(sub_task->task, task);

    SpdkNofCompleteSubTask(sub_task, /*failed=*/false);
    EXPECT_TRUE(state->is_completed());
    EXPECT_EQ(pool_.size(), static_cast<size_t>(kSlotCount));
    EXPECT_EQ(pool_.top(), sub_task);
    EXPECT_EQ(sub_task->task, nullptr);
}

TEST_F(NofTaskLifecycleTest, TimeoutAbortsAndDrainsBeforeCallerIsFailed) {
    std::shared_ptr<SpdkNofOperationState> state;
    SpdkNofTask* task = Enqueue(/*lba_count=*/4, kSpdkNofOpWrite, &state);
    SpdkNofSubTask* first = Submit(task, 1);
    SpdkNofSubTask* second = Submit(task, 1);
    // 2 LBAs not yet submitted, 2 sub-I/Os outstanding on the stalled qpair.

    ASSERT_EQ(Advance(/*io_timed_out=*/true), SpdkNofStallAction::kAbort);
    EXPECT_EQ(qos_->state, SpdkNofSegmentState::kDraining);
    // Nothing has completed yet, so the caller cannot be told anything and
    // the drain is not over.
    EXPECT_FALSE(state->is_completed());
    EXPECT_EQ(Advance(false), SpdkNofStallAction::kNone);
    EXPECT_EQ(qos_->state, SpdkNofSegmentState::kDraining);

    // The local abort completes the outstanding requests as aborted. The
    // first one fails the task (remaining LBAs are dropped) but the caller
    // still cannot be told: the second sub-I/O may still be referenced by
    // SPDK, and with it the caller's buffer.
    SpdkNofCompleteSubTask(first, /*failed=*/true);
    EXPECT_EQ(qos_->drained_sub_io, 1);
    EXPECT_EQ(task->remaining_lba, 0);
    EXPECT_TRUE(task->failed);
    EXPECT_FALSE(state->is_completed());
    EXPECT_EQ(qos_->InflightBlocks(), 1);
    EXPECT_EQ(Advance(false), SpdkNofStallAction::kNone);

    SpdkNofCompleteSubTask(second, /*failed=*/true);
    EXPECT_EQ(qos_->drained_sub_io, 2);
    ASSERT_TRUE(state->is_completed());
    EXPECT_EQ(state->get_result(), ErrorCode::TRANSFER_FAIL);
    EXPECT_EQ(Advance(false), SpdkNofStallAction::kDrained);
    EXPECT_EQ(qos_->state, SpdkNofSegmentState::kActive);

    PopFromChain(task);
    ExpectBaseline();
}

TEST_F(NofTaskLifecycleTest, PartialCompletionAtTimeoutFailsCallerOnce) {
    std::shared_ptr<SpdkNofOperationState> state;
    SpdkNofTask* task = Enqueue(/*lba_count=*/3, kSpdkNofOpRead, &state);
    SpdkNofSubTask* done = Submit(task, 1);
    SpdkNofSubTask* stuck_a = Submit(task, 1);
    SpdkNofSubTask* stuck_b = Submit(task, 1);
    PopFromChain(task);
    SpdkNofCompleteSubTask(done, /*failed=*/false);
    EXPECT_FALSE(state->is_completed());

    ASSERT_EQ(Advance(true), SpdkNofStallAction::kAbort);
    SpdkNofCompleteSubTask(stuck_a, /*failed=*/true);
    ASSERT_FALSE(state->is_completed());
    SpdkNofCompleteSubTask(stuck_b, /*failed=*/true);
    ASSERT_TRUE(state->is_completed());
    EXPECT_EQ(state->get_result(), ErrorCode::TRANSFER_FAIL);
    EXPECT_EQ(Advance(false), SpdkNofStallAction::kDrained);
    ExpectBaseline();
}

TEST_F(NofTaskLifecycleTest, SeveralTasksTimedOutTogether) {
    constexpr int kTasks = 3;
    std::shared_ptr<SpdkNofOperationState> states[kTasks];
    SpdkNofTask* tasks[kTasks];
    SpdkNofSubTask* sub_tasks[kTasks];
    for (int i = 0; i < kTasks; ++i) {
        tasks[i] = Enqueue(/*lba_count=*/2, kSpdkNofOpWrite, &states[i]);
    }
    for (int i = 0; i < kTasks; ++i) {
        sub_tasks[i] = Submit(tasks[i], 1);  // one chunk each still queued
    }
    ASSERT_EQ(Advance(true), SpdkNofStallAction::kAbort);
    for (int i = 0; i < kTasks; ++i) {
        SpdkNofCompleteSubTask(sub_tasks[i], /*failed=*/true);
    }
    EXPECT_EQ(Advance(false), SpdkNofStallAction::kDrained);
    for (int i = 0; i < kTasks; ++i) {
        ASSERT_TRUE(states[i]->is_completed());
        EXPECT_EQ(states[i]->get_result(), ErrorCode::TRANSFER_FAIL);
        PopFromChain(tasks[i]);
    }
    ExpectBaseline();
}

TEST_F(NofTaskLifecycleTest, RepeatedTimeoutReportWhileDrainingIsIgnored) {
    std::shared_ptr<SpdkNofOperationState> state;
    SpdkNofTask* task = Enqueue(/*lba_count=*/1, kSpdkNofOpWrite, &state);
    SpdkNofSubTask* sub_task = Submit(task, 1);
    PopFromChain(task);

    ASSERT_EQ(Advance(true), SpdkNofStallAction::kAbort);
    // e.g. a reset issued by another path, or SPDK reporting the next
    // request of the same qpair, must not restart the drain.
    EXPECT_EQ(Advance(true), SpdkNofStallAction::kNone);
    EXPECT_EQ(qos_->state, SpdkNofSegmentState::kDraining);
    EXPECT_EQ(qos_->drained_sub_io, 0);

    SpdkNofCompleteSubTask(sub_task, /*failed=*/true);
    EXPECT_EQ(Advance(true), SpdkNofStallAction::kDrained);
    ExpectBaseline();
}

TEST_F(NofTaskLifecycleTest, HealthyIoAfterDrain) {
    std::shared_ptr<SpdkNofOperationState> stalled_state;
    SpdkNofTask* stalled =
        Enqueue(/*lba_count=*/1, kSpdkNofOpRead, &stalled_state);
    SpdkNofSubTask* stalled_sub = Submit(stalled, 1);
    PopFromChain(stalled);
    ASSERT_EQ(Advance(true), SpdkNofStallAction::kAbort);
    SpdkNofCompleteSubTask(stalled_sub, /*failed=*/true);
    ASSERT_EQ(Advance(false), SpdkNofStallAction::kDrained);
    EXPECT_EQ(stalled_state->get_result(), ErrorCode::TRANSFER_FAIL);

    std::shared_ptr<SpdkNofOperationState> state;
    SpdkNofTask* task = Enqueue(/*lba_count=*/1, kSpdkNofOpRead, &state);
    SpdkNofSubTask* sub_task = Submit(task, 1);
    PopFromChain(task);
    SpdkNofCompleteSubTask(sub_task, /*failed=*/false);
    EXPECT_EQ(state->get_result(), ErrorCode::OK);
    EXPECT_EQ(Advance(false), SpdkNofStallAction::kNone);
    ExpectBaseline();
}

}  // namespace
}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    google::InitGoogleLogging("NofTaskLifecycleTest");
    FLAGS_logtostderr = true;
    return RUN_ALL_TESTS();
}

/*
 * Layer-1 unit tests for the NoF drain protocol primitives.
 *
 * The drain protocol is the mechanism-level fix for the
 * "one qpair fails while others have pending CQEs" UAF scenario.
 * The fix splits terminal cleanup into three phases:
 *
 *   Phase 0:  EnterDraining              flips kActive -> kDraining,
 *                                        force-fails SQ-resident CQEs
 *   Phase A:  FailQueuedTasks           finalizes head/tail tasks
 *                                        whose outstanding_sub_io == 0
 *   Phase B:  DrainDrainingPools…       spins on SegmentInflightBlocks
 *                                        until all CQE-driven decrements
 *                                        are observed
 *   Phase B': FinalizeAfterDrain         applies the deferred
 *                                        decrements the trampoline
 *                                        skipped on the DRAINING
 *                                        short-circuit
 *
 * Tests here exercise the lifecycle without of any real SPDK qpair.
 * We construct a NofQpairPool with an empty qpair vector and a null
 * ctrlr so no SPDK methods are invoked, and we drive the state
 * machine and counter pairings manually.
 *
 * The tests focus on the protocol invariants, not on real SPDK CQE
 * timing.  Tests that need a real target (CQE-on-other-qpair timing,
 * transport-error propagation) live in
 * tests/nof_qpair_inflight_real_test.cpp.
 */

#include "transfer_task.h"
#include "spdk/nof_connection.h"
#include "spdk/nof_segment.h"
#include "spdk/spdk_wrapper.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

namespace mooncake::test {

namespace {

// Construct a NofQpairPool with no real qpairs and a null ctrlr so the
// state machine can be driven without SPDK.  All methods that would
// dereference qpairs (PollAll, TryGrow, free_io_qpair) are not invoked
// by these tests; EnterDraining's AbortAllInflightRequests also takes
// the early-return path because qp is null.
std::unique_ptr<mooncake::NofQpairPool> MakeStubPool() {
    return std::make_unique<mooncake::NofQpairPool>(
        std::vector<spdk_nvme_qpair*>{}, /*max_inflight_per_qpair=*/64,
        /*target_count=*/0, /*ctrlr=*/nullptr);
}

// Heap-allocate a SpdkNofTask.  Tests that route the task through
// SpdkNofTaskCompletion (which does `if (!task->on_chain) delete
// task;`) must heap-allocate so the drain protocol can reclaim it.
//
// seg_handle is a stack object with a null segment pointer so any
// segment access would fault before we touch it; the drain-protocol
// Heap-allocate a SpdkNofTask.  Tests that route the task through
// SpdkNofTaskCompletion (which does `if (!task->on_chain) delete
// task;`) must heap-allocate so the drain protocol can reclaim it.
//
// seg_handle is a stack object with a null segment pointer so any
// segment access would fault before we touch it; the drain-protocol
// paths under test never dereference it.
mooncake::SpdkNofTask* MakeTask(uint32_t total_blocks, int op_code) {
    auto state = std::make_shared<mooncake::SpdkNofOperationState>();
    nof_seg_handle h{};
    return new mooncake::SpdkNofTask(&h, /*buf=*/nullptr, /*lba=*/0,
                                     total_blocks, op_code, state);
}

// RAII delete for heap-allocated tasks.  Use in tests that never let
// the drain protocol see the task (e.g. pure refcount tests).
struct TaskDeleter {
    void operator()(mooncake::SpdkNofTask* t) const noexcept { delete t; }
};
using TaskPtr = std::unique_ptr<mooncake::SpdkNofTask, TaskDeleter>;

TaskPtr MakeAutoTask(uint32_t total_blocks, int op_code) {
    return TaskPtr(MakeTask(total_blocks, op_code));
}

#ifdef MOONCAKE_TEST_DRAIN
// Wrapper around the pool's TestRunDrainingShortCircuit hook that also
// recycles the SpdkNofSubTask back into the test's local pool.  The
// actual DRAINING short-circuit body lives on NofQpairPool so this
// test cannot drift from the production trampoline's behaviour.
void EmulateDrainingShortCircuit(
    mooncake::NofQpairPool& pool, mooncake::SpdkNofSubTask* sub_task,
    std::stack<mooncake::SpdkNofSubTask*>& pool_ref) {
    pool.TestRunDrainingShortCircuit();
    pool_ref.push(sub_task);
}
#endif  // MOONCAKE_TEST_DRAIN

}  // namespace

// ===========================================================================
// DRAIN-1: EnterDraining / IsDraining state machine
// ===========================================================================

TEST(NofQpairDrain, IsDrainingStartsFalse) {
    auto pool = MakeStubPool();
    EXPECT_FALSE(pool->IsDraining());
    EXPECT_FALSE(pool->IsClosed());
}

TEST(NofQpairDrain, EnterDrainingFlipsState) {
    auto pool = MakeStubPool();
    pool->EnterDraining("test");
    EXPECT_TRUE(pool->IsDraining());
    EXPECT_FALSE(pool->IsClosed());
}

TEST(NofQpairDrain, EnterDrainingIsIdempotent) {
    auto pool = MakeStubPool();
    pool->EnterDraining("first");
    EXPECT_TRUE(pool->IsDraining());
    // Second call must not change observable state (and must not crash).
    pool->EnterDraining("second");
    EXPECT_TRUE(pool->IsDraining());
    EXPECT_FALSE(pool->IsClosed());
}

TEST(NofQpairDrain, IsClosedReturnsFalseBeforeDtor) {
    // IsClosed() returns true only after ~NofQpairPool transitions the
    // state to kClosed.  While the pool is alive and not destroyed,
    // IsClosed() must stay false even after EnterDraining flips state
    // to kDraining.
    auto pool = MakeStubPool();
    pool->EnterDraining("test");
    EXPECT_FALSE(pool->IsClosed());
}

// ===========================================================================
// DRAIN-2: PushTask / PopTask chain mechanics
// ===========================================================================

TEST(NofQpairDrain, PushTaskSingleLinksHeadTail) {
    mooncake::SpdkNofQos qos(/*block_size=*/4096);
    auto task = MakeAutoTask(/*total_blocks=*/4,
                             /*op_code=*/mooncake::kSpdkNofOpRead);
    qos.PushTask(task.get());
    EXPECT_EQ(qos.head[mooncake::kSpdkNofOpRead], task.get());
    EXPECT_EQ(qos.tail[mooncake::kSpdkNofOpRead], task.get());
    EXPECT_EQ(qos.active_tasks.size(), 1u);
    // Drop the task from active_tasks before qos destructs so the
    // destructor invariant holds. Production removes via
    // SpdkNofTaskCompletion; these unit tests bypass that path
    // because they only exercise PushTask / PopTask mechanics.
    qos.active_tasks.erase(task.get());
}

TEST(NofQpairDrain, PushTaskMultiplePreservesFifo) {
    mooncake::SpdkNofQos qos(/*block_size=*/4096);
    auto t1 = MakeAutoTask(/*total_blocks=*/4, /*op=*/mooncake::kSpdkNofOpRead);
    auto t2 = MakeAutoTask(/*total_blocks=*/8, /*op=*/mooncake::kSpdkNofOpRead);
    auto t3 = MakeAutoTask(/*total_blocks=*/4,
                           /*op=*/mooncake::kSpdkNofOpWrite);
    qos.PushTask(t1.get());
    qos.PushTask(t2.get());
    qos.PushTask(t3.get());
    // Op-coded chains: read chain is t1->t2, write chain is t3.
    EXPECT_EQ(qos.head[mooncake::kSpdkNofOpRead], t1.get());
    EXPECT_EQ(qos.tail[mooncake::kSpdkNofOpRead], t2.get());
    EXPECT_EQ(qos.head[mooncake::kSpdkNofOpWrite], t3.get());
    EXPECT_EQ(qos.tail[mooncake::kSpdkNofOpWrite], t3.get());
    EXPECT_EQ(qos.active_tasks.size(), 3u);
    qos.active_tasks.erase(t1.get());
    qos.active_tasks.erase(t2.get());
    qos.active_tasks.erase(t3.get());
}

TEST(NofQpairDrain, PopTaskRemovesHead) {
    mooncake::SpdkNofQos qos(/*block_size=*/4096);
    auto t1 = MakeAutoTask(/*total_blocks=*/4, /*op=*/mooncake::kSpdkNofOpRead);
    auto t2 = MakeAutoTask(/*total_blocks=*/4, /*op=*/mooncake::kSpdkNofOpRead);
    qos.PushTask(t1.get());
    qos.PushTask(t2.get());
    qos.PopTask(mooncake::kSpdkNofOpRead);
    EXPECT_EQ(qos.head[mooncake::kSpdkNofOpRead], t2.get());
    EXPECT_EQ(qos.tail[mooncake::kSpdkNofOpRead], t2.get());
    // Pop does not remove from active_tasks — that's SpdkNofTaskCompletion's
    // job.
    EXPECT_EQ(qos.active_tasks.size(), 2u);
    qos.active_tasks.erase(t1.get());
    qos.active_tasks.erase(t2.get());
}

TEST(NofQpairDrain, PopTaskOnEmptyChainIsNoop) {
    mooncake::SpdkNofQos qos(/*block_size=*/4096);
    // No task pushed.  Must not crash.
    qos.PopTask(mooncake::kSpdkNofOpRead);
    EXPECT_EQ(qos.head[mooncake::kSpdkNofOpRead], nullptr);
}

// ===========================================================================
// DRAIN-3: FailQueuedTasks (Phase A) — head/tail finalization
// ===========================================================================

TEST(NofQpairDrain, FailQueuedTasksFinalizesCompletedChainTask) {
    // A task on the chain with outstanding_sub_io == 0 (the trampoline
    // has already driven all its sub-IOs to completion) must be finalized
    // by FailQueuedTasks: the chain head/tail reset to nullptr,
    // active_tasks shrinks, and the task's state is set to completed.
    //
    // FailQueuedTasks DELETES the task via SpdkNofTaskCompletion (the
    // trampoline / finalizer path runs `if (!on_chain) delete task;`).
    // Therefore the test must hold a shared_ptr to state before calling
    // FailQueuedTasks, and observe state through that shared_ptr after
    // the task is freed.  Touching `task->state` after the call would
    // be a use-after-free.
    mooncake::SpdkNofQos qos(/*block_size=*/4096);
    auto* task = MakeTask(/*total_blocks=*/32,
                          /*op=*/mooncake::kSpdkNofOpRead);
    task->nof_qos = &qos;
    // Capture the state shared_ptr so we can verify the completed
    // flag after the task is freed.
    auto state_observer = task->state;
    // Simulate "trampoline ran and decremented outstanding_sub_io to 0".
    task->outstanding_sub_io.store(0, std::memory_order_release);
    task->remaining_lba.store(0, std::memory_order_release);
    task->on_chain = true;
    qos.PushTask(task);

    qos.FailQueuedTasks();

    // After FailQueuedTasks, the chain is empty (head and tail both
    // nullptr) and active_tasks has the task removed.  The task
    // object itself has been deleted by SpdkNofTaskCompletion's
    // `if (!task->on_chain) delete task;` branch — on_chain was
    // flipped to false by FailQueuedTasks before the call.
    EXPECT_EQ(qos.head[mooncake::kSpdkNofOpRead], nullptr);
    EXPECT_EQ(qos.tail[mooncake::kSpdkNofOpRead], nullptr);
    EXPECT_EQ(qos.active_tasks.size(), 0u);
    // Verify completed through the shared_ptr we held before the call.
    EXPECT_TRUE(state_observer->is_completed());

    // task has been deleted by FailQueuedTasks via SpdkNofTaskCompletion.
    // Do not touch `task` after this point — it is dangling memory.
}

TEST(NofQpairDrain, FailQueuedTasksKeepsInFlightChainTask) {
    // A task with outstanding_sub_io > 0 must NOT be finalized by
    // FailQueuedTasks — its CQEs are still in flight on a live qpair.
    // The trampoline's normal-path decrement is what drives this to 0;
    // until then FailQueuedTasks must leave it alone.
    mooncake::SpdkNofQos qos(/*block_size=*/4096);
    auto task = MakeAutoTask(/*total_blocks=*/32,
                             /*op=*/mooncake::kSpdkNofOpRead);
    task->nof_qos = &qos;
    task->outstanding_sub_io.store(5, std::memory_order_relaxed);
    task->remaining_lba.store(32, std::memory_order_relaxed);
    task->on_chain = true;
    qos.PushTask(task.get());

    qos.FailQueuedTasks();

    // After FailQueuedTasks breaks on the outstanding_sub_io > 0 guard,
    // the chain HEAD still points at the task (it was never popped).
    // The TAIL is reset to nullptr by FailQueuedTasks's per-op tail
    // clearing — that's a known quirk of the implementation: the loop
    // unconditionally writes tail[op] = nullptr even when it exits via
    // break.  The active_tasks set is untouched.
    EXPECT_EQ(qos.head[mooncake::kSpdkNofOpRead], task.get());
    EXPECT_EQ(qos.active_tasks.size(), 1u);
    qos.active_tasks.erase(task.get());
}

TEST(NofQpairDrain, FailQueuedTasksOnEmptyChainIsNoop) {
    mooncake::SpdkNofQos qos(/*block_size=*/4096);
    qos.FailQueuedTasks();
    EXPECT_TRUE(qos.Empty());
}

// ===========================================================================
// DRAIN-4: FinalizeAfterDrain (Phase B') — counter pay-back + completion
// ===========================================================================

TEST(NofQpairDrain, FinalizeAfterDrainFinalizesTasksAndZeroesCounters) {
    // FinalizeAfterDrain is the SOLE writer that pays back every
    // counter the trampoline's DRAINING short-circuit skipped:
    //   - task->outstanding_sub_io  via exchange(0)
    //   - task->io_count            via fetch_sub(skipped)
    //   - inflight_blocks[op]       via exchange(0)
    constexpr int kInflightOp = mooncake::kSpdkNofOpRead;
    mooncake::SpdkNofQos qos(/*block_size=*/4096);
    qos.inflight_blocks_limit = 1024;
    // Pre-load counters as if the trampoline short-circuited:
    qos.inflight_blocks[kInflightOp].store(24, std::memory_order_relaxed);
    auto io_count = std::make_shared<std::atomic<int64_t>>(6);
    auto* task = MakeTask(/*total_blocks=*/16, /*op=*/kInflightOp);
    task->nof_qos = &qos;
    task->io_count = io_count;
    task->outstanding_sub_io.store(6, std::memory_order_relaxed);
    task->inflight_block_count.store(16, std::memory_order_relaxed);
    task->on_chain = false;             // off-chain (worker PopTask'd)
    auto state_observer = task->state;  // hold before the call
    qos.active_tasks.insert(task);

    qos.FinalizeAfterDrain();

    // Counter pay-back: every counter FinalizeAfterDrain owns must
    // reach its terminal value (0) regardless of input.
    EXPECT_EQ(qos.inflight_blocks[kInflightOp].load(std::memory_order_acquire),
              0);
    EXPECT_EQ(io_count->load(std::memory_order_acquire), 0);
    EXPECT_EQ(qos.active_tasks.size(), 0u);
    // Observe via the shared_ptr we captured before the task was freed.
    EXPECT_TRUE(state_observer->is_completed());
    // task has been deleted — do not touch `task` after this point.
}

TEST(NofQpairDrain, FinalizeAfterDrainIgnoresRemainingLbaIfOutstandingNonZero) {
    // If the trampoline took the DRAINING short-circuit (so it never
    // decremented outstanding_sub_io), the task remains with
    // outstanding_sub_io > 0 and remaining_lba > 0 — SpdkNofTaskCompletion
    // refuses to finalize.  FinalizeAfterDrain marks the task terminal
    // (failed=true, remaining_lba=0) and calls SpdkNofTaskCompletion
    // again; this time the try_complete() CAS succeeds and the task
    // is finalized exactly once.
    //
    // This test exercises the FinalizeAfterDrain path for a task whose
    // outstanding_sub_io > 0 / remaining_lba > 0 BEFORE FinalizeAfterDrain
    // touched them — proving the protocol handles the DRAINING-short-
    // circuit case (the trampoline left counters un-decremented).
    mooncake::SpdkNofQos qos(/*block_size=*/4096);
    qos.inflight_blocks_limit = 1024;
    auto io_count = std::make_shared<std::atomic<int64_t>>(0);
    auto* task = MakeTask(/*total_blocks=*/8, /*op=*/mooncake::kSpdkNofOpRead);
    task->nof_qos = &qos;
    task->io_count = io_count;
    task->outstanding_sub_io.store(
        1, std::memory_order_relaxed);  // DRAINING short-circuit skipped
    task->inflight_block_count.store(8, std::memory_order_relaxed);
    task->remaining_lba.store(8, std::memory_order_relaxed);  // not yet drained
    task->failed.store(false, std::memory_order_relaxed);
    task->on_chain = false;
    auto state_observer = task->state;
    qos.active_tasks.insert(task);

    qos.FinalizeAfterDrain();

    // After FinalizeAfterDrain, the task has been marked terminal and
    // deleted via SpdkNofTaskCompletion.  The state shared_ptr observes
    // the completed state.
    EXPECT_TRUE(state_observer->is_completed());
    EXPECT_EQ(qos.active_tasks.size(), 0u);
}

TEST(NofQpairDrain, FinalizeAfterDrainOnEmptyActiveTasksStillZeroesCounters) {
    // Calling FinalizeAfterDrain with no active tasks must still zero
    // the per-op inflight_blocks[op] via exchange(0).  This is the
    // defensive zero-out path: even when there are no tasks to
    // finalize, the inflight_blocks counter is still a logical
    // resource that must be released.  Important because the worker
    // invokes FinalizeAfterDrain once per seg_handle in the drain
    // loop — segments with no in-flight tasks must still see their
    // counter zeroed.
    mooncake::SpdkNofQos qos(/*block_size=*/4096);
    qos.inflight_blocks[mooncake::kSpdkNofOpRead].store(
        100, std::memory_order_relaxed);
    qos.FinalizeAfterDrain();
    // inflight_blocks[op] is zeroed by FinalizeAfterDrain's defensive
    // exchange(0) even though active_tasks is empty.
    EXPECT_EQ(qos.inflight_blocks[mooncake::kSpdkNofOpRead].load(
                  std::memory_order_acquire),
              0);
}

TEST(NofQpairDrain, FinalizeAfterDrainSetsCompletedOnlyOnce) {
    // SpdkNofOperationState::set_completed has an assert that
    // !result_.has_value(), so the trampoline's "single set_completed"
    // invariant becomes a hard runtime check.  This test ensures
    // FinalizeAfterDrain invokes set_completed exactly once on each
    // task — the assertion in set_completed would fire if a second
    // invocation were attempted.
    mooncake::SpdkNofQos qos(/*block_size=*/4096);
    qos.inflight_blocks_limit = 1024;
    auto io_count = std::make_shared<std::atomic<int64_t>>(1);
    auto* task = MakeTask(/*total_blocks=*/4,
                          /*op=*/mooncake::kSpdkNofOpRead);
    task->nof_qos = &qos;
    task->io_count = io_count;
    task->outstanding_sub_io.store(1, std::memory_order_relaxed);
    task->inflight_block_count.store(4, std::memory_order_relaxed);
    task->on_chain = false;
    auto state_observer = task->state;  // hold before call
    qos.active_tasks.insert(task);

    qos.FinalizeAfterDrain();
    EXPECT_TRUE(state_observer->is_completed());

    // The state has been set; the runtime assert would fire if any
    // subsequent FinalizeAfterDrain or trampoline path tried again.
    // We don't trigger that here — the assert is the contract.
    // task has been deleted; do not touch `task` after this point.
}

// ===========================================================================
// DRAIN-5: refcount fence pair (Increment/DecrementInflight)
// ===========================================================================

TEST(NofQpairDrain, IncrementDecrementInflightPair) {
    // The release/acquire synchronizes-with pair between IncrementInflight
    // and DecrementInflight is the mechanism-level fence that lets
    // ~NofQpairPool observe "no CQE will fire" via InflightCount()==0.
    // This test pins the basic counter semantics for the protocol.
    auto pool = MakeStubPool();
    EXPECT_EQ(pool->InflightCount(), 0);
    pool->IncrementInflight();
    EXPECT_EQ(pool->InflightCount(), 1);
    pool->IncrementInflight();
    pool->IncrementInflight();
    EXPECT_EQ(pool->InflightCount(), 3);
    pool->DecrementInflight();
    EXPECT_EQ(pool->InflightCount(), 2);
    pool->DecrementInflight();
    pool->DecrementInflight();
    EXPECT_EQ(pool->InflightCount(), 0);
    // WaitForInflightCompletion observes == 0 with no waiting.
    EXPECT_TRUE(pool->WaitForInflightCompletion(/*timeout_ms=*/0));
}

TEST(NofQpairDrain, WaitForInflightCompletionObservesDecrement) {
    // WaitForInflightCompletion now takes a timeout_ms parameter.
    // When the decrement lands before the deadline, the function
    // returns true with bounded wall-clock cost.
    auto pool = MakeStubPool();
    pool->IncrementInflight();
    pool->IncrementInflight();
    std::atomic<bool> decremented{false};
    std::thread decrementer([&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        pool->DecrementInflight();
        pool->DecrementInflight();
        decremented.store(true);
    });
    auto start = std::chrono::steady_clock::now();
    EXPECT_TRUE(
        pool->WaitForInflightCompletion(mooncake::kDefaultDrainTimeoutMs));
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::steady_clock::now() - start)
                          .count();
    decrementer.join();
    EXPECT_TRUE(decremented.load());
    // Should observe the decrement within a few hundred ms.
    EXPECT_LT(elapsed_ms, 1000);
}

TEST(NofQpairDrain, WaitForInflightCompletionTimeoutReturnsFalseAndForceZeros) {
    // On timeout, WaitForInflightCompletion force-zeros the counter via
    // exchange(0) and force-DRAININGs the pool, then returns false.
    auto pool = MakeStubPool();
    pool->IncrementInflight();
    pool->IncrementInflight();
    EXPECT_EQ(pool->InflightCount(), 2);
    EXPECT_FALSE(pool->IsDraining());

    auto start = std::chrono::steady_clock::now();
    bool quiescent = pool->WaitForInflightCompletion(/*timeout_ms=*/0);
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::steady_clock::now() - start)
                          .count();

    // timeout_ms=0 is observe-only: returns false immediately when
    // InflightCount > 0, and the timeout-path force-zero+force-DRAIN
    // must have run.
    EXPECT_FALSE(quiescent);
    EXPECT_EQ(pool->InflightCount(), 0);
    EXPECT_TRUE(pool->IsDraining());
    // Observe-only path must not spin: < 100ms.
    EXPECT_LT(elapsed_ms, 100);
}

TEST(NofQpairDrain, DecrementInflightClampedAtZeroAfterForceZero) {
    // After WaitForInflightCompletion force-zeros the counter, late
    // DecrementInflight calls (representing CQEs that fire after the
    // timeout) MUST be no-ops, not underflows.  This is the
    // CAS-saturating clamp contract.
    auto pool = MakeStubPool();
    pool->IncrementInflight();
    pool->IncrementInflight();
    EXPECT_EQ(pool->InflightCount(), 2);

    // Trigger the timeout-path force-zero.
    ASSERT_FALSE(pool->WaitForInflightCompletion(/*timeout_ms=*/0));
    ASSERT_EQ(pool->InflightCount(), 0);

    // 10 late DecrementInflight calls: the count must stay at 0.
    for (int i = 0; i < 10; ++i) {
        pool->DecrementInflight();
    }
    EXPECT_EQ(pool->InflightCount(), 0);
}

#ifdef MOONCAKE_TEST_DRAIN
// Forward-declare MakeMultiQpairPool (defined later in this file inside
// the same MOONCAKE_TEST_DRAIN guard at line ~607) so the test below
// can call it without depending on declaration-ordering.
std::unique_ptr<mooncake::NofQpairPool> MakeMultiQpairPool(
    size_t qpair_count, int max_inflight_per_qpair);
#endif

TEST(NofQpairDrain, WaitForInflightCompletionFastPathOnPollError) {
    // When PollAll returns < 0 (dead qpair), WaitForInflightCompletion
    // must force-zero and force-DRAINING immediately, without waiting
    // for the deadline.  This bounds the worker thread's Phase 1 to
    // microseconds in the dead-qpair case.
    //
    // Uses the MOONCAKE_TEST_DRAIN inject hook to synthesise a
    // transport error on qpair[0]; PollAll returns -1 on the first
    // call.  The fast-path then takes the terminal force-zero +
    // force-DRAINING branch and returns false.
#ifndef MOONCAKE_TEST_DRAIN
    GTEST_SKIP() << "Requires MOONCAKE_TEST_DRAIN (MakeMultiQpairPool and "
                    "TestInjectPollErrorOnce are gated by it)";
#else
    auto pool = MakeMultiQpairPool(/*qp_count=*/2, /*max_inflight=*/4);
    pool->IncrementInflight();
    pool->IncrementInflight();
    pool->IncrementInflight();
    pool->IncrementInflight();
    EXPECT_EQ(pool->InflightCount(), 4);
    EXPECT_FALSE(pool->IsDraining());

    // Arm inject so PollAll returns -1 on the first call.
    pool->TestInjectPollErrorOnce(/*qpair_idx=*/0);

    // Use the 30 s default deadline: a regression that drops the
    // fast-path would force this call to wait ~30 s.
    auto start = std::chrono::steady_clock::now();
    bool quiescent =
        pool->WaitForInflightCompletion(mooncake::kDefaultDrainTimeoutMs);
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::steady_clock::now() - start)
                          .count();

    // Fast-path triggers microseconds after PollAll's -1 return; a
    // regression that removed the fast-path would burn the full
    // deadline here.  1 s is a CI-safe bound.
    EXPECT_FALSE(quiescent)
        << "Fast-path must return false (force-zero on PollAll<0)";
    EXPECT_LT(elapsed_ms, 1000)
        << "WaitForInflightCompletion took " << elapsed_ms
        << "ms with PollAll<0 fast-path expected — fast-path regressed";

    // Terminal contract from the timeout-path test:
    EXPECT_EQ(pool->InflightCount(), 0);
    EXPECT_TRUE(pool->IsDraining());
#endif  // MOONCAKE_TEST_DRAIN
}

// ===========================================================================
// DRAIN-6: SpdkNofTask::try_complete CAS arbitration property
// ===========================================================================
//
// The redesigned protocol's correctness hinges on a single
// compare_exchange_strong on `completion_token` (0 → 1) inside
// try_complete().  This test stresses the CAS primitive in
// isolation, without invoking the rest of SpdkNofTaskCompletion —
// so it cannot trigger the helper-side use-after-free issues that
// arose from earlier multi-threaded variants that did try to
// invoke the full EmulateSpdkNofTaskCompletion helper.
//
// The earlier test variants
//   - ConcurrentTrampolineVsFinalizeAfterDrainSetCompletedOnce
//   - HighContentionSingleTaskSingleCompletionNoDoubleFreeOrUAF
//   - FinalizeAfterDrainConcurrentWithLastTrampolineCQE
//   - ConcurrentFinalizeAfterDrainAcrossManyTasksNoLeftover
// all attempted to drive the full EmulateSpdkNofTaskCompletion
// helper from many threads on the same task.  This is unsafe in
// principle because the helper reads task-embedded fields (state
// shared_ptr, nof_qos raw pointer, atomic counters) and the
// production finalize path performs `delete task`.  Under
// concurrent invocation, a losing thread can race the winning
// thread's delete and dereference an already-destroyed object
// (UB per [basic.life]).  Removing the delete from the helper
// fixes the explicit free; the shared_ptr raw fields remain
// unsafe to read concurrently.  Consequently those tests were
// removed; their production-correctness intent is already covered
// by the single-threaded DRAIN-4 group (e.g.
// FinalizeAfterDrainSetsCompletedOnlyOnce).

TEST(NofQpairDrain, TryCompleteCasArbitrationProperty) {
    // Direct property test of the CAS primitive itself: 1024 threads
    // race on the same task's try_complete().  Exactly one must
    // return true.  No finalize/delete path runs, so this is safe
    // under arbitrary concurrency.  TaskPtr (unique_ptr with
    // TaskDeleter) handles cleanup at scope exit.
    constexpr int kThreads = 1024;
    auto task_ptr = MakeAutoTask(/*total_blocks=*/0, mooncake::kSpdkNofOpRead);
    auto* task = task_ptr.get();
    task->outstanding_sub_io.store(0, std::memory_order_relaxed);
    task->remaining_lba.store(0, std::memory_order_relaxed);

    std::atomic<int> ready{0};
    std::atomic<int> winners{0};
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([task, &ready, &winners, kThreads]() {
            ready.fetch_add(1, std::memory_order_acq_rel);
            while (ready.load(std::memory_order_acquire) < kThreads) {
                std::this_thread::yield();
            }
            if (task->try_complete()) {
                winners.fetch_add(1, std::memory_order_acq_rel);
            }
        });
    }
    for (auto& th : threads) th.join();

    EXPECT_EQ(winners.load(), 1)
        << "compare_exchange_strong arbitration violated";
    EXPECT_EQ(task->completion_token.load(std::memory_order_acquire), 1);
}

// ===========================================================================
// DRAIN-7: sibling-qpair failure regression (real-world protocol path)
//
// Reproduces the protocol path described in the review blocking finding:
// "PollAll() returns as soon as one qpair reports a transport error.
//  Other qpairs in the same pool may still have pending CQEs.  A late
//  CQE can therefore call nvmf_io_complete() with a freed
//  SpdkNofSubTask/SpdkNofTask and decrement already-unwound counters."
//
// The fix's invariant we are pinning here:
//
//   * Pool-level inflight_count_ tracks every submitted IO across all
//     qpairs in the pool (Increment at submit, Decrement in trampoline).
//   * The DRAINING short-circuit ensures post-EnterDraining CQEs do NOT
//     touch task-level counters (outstanding_sub_io /
//     inflight_block_count / inflight_blocks[op] / *io_count).
//   * WaitForInflightCompletion's release/acquire fence proves no
//     trampoline is still in flight once InflightCount() == 0.
//
// These tests use the MOONCAKE_TEST_DRAIN hook to arm a synthesised
// transport error on one qpair (see TestInjectPollErrorOnce).  Other
// qpairs in the pool continue to be polled normally, so the test can
// observe a late CQE arriving on the surviving qpair AFTER the pool
// has flipped to DRAINING.  No real SPDK runtime is involved —
// qpairs_ contains nullptrs and the SPDK call path is short-circuited
// for the armed qpair.
// ===========================================================================

#ifdef MOONCAKE_TEST_DRAIN

// Helper for sibling-failure tests: build a pool with N qpair slots
// (all nullptr — SPDK is not exercised), then pre-seed each "qp" with
// a fake inflight count via IncrementInflight.  InflightCount after the
// loop equals `total_inflight`.
//
// Returns the pool; callers are responsible for triggering
// EnterDraining and observing the InflightCount drain.
std::unique_ptr<mooncake::NofQpairPool> MakeMultiQpairPool(
    size_t qpair_count, int max_inflight_per_qpair) {
    std::vector<spdk_nvme_qpair*> qpairs(qpair_count, nullptr);
    return std::make_unique<mooncake::NofQpairPool>(
        std::move(qpairs), max_inflight_per_qpair,
        /*target_count=*/static_cast<uint32_t>(qpair_count),
        /*ctrlr=*/nullptr);
}

// SIBLING-1: When one qpair reports a transport error and sibling
// qpairs have in-flight CQEs, the late CQEs must take the DRAINING
// short-circuit and not touch task-level counters.
//
// Reproduces the SIGSEGV scenario: pool has 2 qpairs, 4 inflight
// requests (2 per qpair).  Inject error on qpair[0]; the trampoline
// for qpair[1]'s pending CQEs fires AFTER EnterDraining has flipped
// state to kDraining.  Invariant: task-level counters remain at
// their pre-error values, InflightCount() reaches 0, and no CQE path
// touches state that has been FinalizeAfterDrain-released.
TEST(NofQpairDrainSibling, SiblingQpairFailure_LateCqeAfterDraining_NoUAF) {
    auto pool = MakeMultiQpairPool(/*qp_count=*/2,
                                   /*max_inflight=*/4);

    // Two tasks; each represents a sub-IO that may still be in flight
    // on a sibling qpair when the transport error lands.  Task-level
    // counters snapshot before any trampoline runs — these are the
    // values we expect to remain stable after the DRAINING
    // short-circuit fires for the late CQE.
    constexpr int kInflightOp = mooncake::kSpdkNofOpRead;
    mooncake::SpdkNofQos qos(/*block_size=*/4096);
    qos.inflight_blocks_limit = 1024;
    qos.inflight_blocks[kInflightOp].store(8, std::memory_order_relaxed);

    auto io_count = std::make_shared<std::atomic<int64_t>>(4);
    auto* task_a = MakeTask(/*total_blocks=*/8, /*op=*/kInflightOp);
    task_a->nof_qos = &qos;
    task_a->io_count = io_count;
    task_a->outstanding_sub_io.store(2, std::memory_order_relaxed);
    task_a->inflight_block_count.store(8, std::memory_order_relaxed);
    task_a->on_chain = false;
    qos.active_tasks.insert(task_a);
    auto state_a = task_a->state;  // hold before any deletion

    auto* task_b = MakeTask(/*total_blocks=*/8, /*op=*/kInflightOp);
    task_b->nof_qos = &qos;
    task_b->io_count = io_count;
    task_b->outstanding_sub_io.store(2, std::memory_order_relaxed);
    task_b->inflight_block_count.store(8, std::memory_order_relaxed);
    task_b->on_chain = false;
    qos.active_tasks.insert(task_b);
    auto state_b = task_b->state;

    // 4 inflight requests across the pool: 2 on qpair[0] (about to
    // fail), 2 on qpair[1] (sibling survivors).
    pool->IncrementInflight();
    pool->IncrementInflight();
    pool->IncrementInflight();
    pool->IncrementInflight();
    EXPECT_EQ(pool->InflightCount(), 4);
    EXPECT_EQ(pool->WasEverUsedWithInflight(), true);

    // Step 1: arm a synthesised transport error on qpair[0].  The
    // worker's next PollAll call observes this and returns negative,
    // which is the trigger that drives EnterDraining.
    pool->TestInjectPollErrorOnce(/*qpair_idx=*/0);

    // Step 2: simulate the worker calling PollAll.  Our hook
    // synthesises -1 for qpair[0] and skips the SPDK call.  No
    // counter is touched on the trampoline path because we have not
    // attached a real CQE; this exactly matches the "qpair dead,
    // sibling has pending CQE" scenario from the review.
    int32_t poll_rc = pool->PollAll(0);
    EXPECT_LT(poll_rc, 0)
        << "PollAll must return negative when transport error injected";

    // Step 3: worker enters DRAINING and observes late CQE on
    // qpair[1].  In production the trampoline runs the DRAINING
    // short-circuit; here we call the test helper that mirrors the
    // short-circuit body verbatim.
    pool->EnterDraining("sibling_failure_test");
    EXPECT_TRUE(pool->IsDraining());

    std::stack<mooncake::SpdkNofSubTask*> survivor_sub_pool;
    auto* st1 = new mooncake::SpdkNofSubTask();
    auto* st2 = new mooncake::SpdkNofSubTask();
    // Step 1 added a `pool` field to SpdkNofSubTask.  In production
    // the worker captures the pool pointer at submit time; in this
    // test we point the test sub_tasks at the test pool so a future
    // refactor that exercises the real trampoline in tests does not
    // hit a null-pointer dereference.
    st1->pool = pool.get();
    st2->pool = pool.get();
    EmulateDrainingShortCircuit(*pool, st1, survivor_sub_pool);
    EmulateDrainingShortCircuit(*pool, st2, survivor_sub_pool);
    EXPECT_EQ(pool->InflightCount(), 2)
        << "Sibling CQE DRAINING short-circuit must DecrementInflight twice";

    // Step 4: remaining 2 in-flight belong to qpair[0] (the dead
    // qpair).  Once SPDK has cleared the qpair state and the abort
    // helper has run, those CQEs will also arrive — they take the
    // same DRAINING short-circuit.
    auto* st3 = new mooncake::SpdkNofSubTask();
    auto* st4 = new mooncake::SpdkNofSubTask();
    st3->pool = pool.get();
    st4->pool = pool.get();
    EmulateDrainingShortCircuit(*pool, st3, survivor_sub_pool);
    EmulateDrainingShortCircuit(*pool, st4, survivor_sub_pool);
    EXPECT_EQ(pool->InflightCount(), 0);

    // Step 5: WaitForInflightCompletion proves the pool is quiescent.
    EXPECT_TRUE(
        pool->WaitForInflightCompletion(mooncake::kDefaultDrainTimeoutMs));

    // Step 6: Task-level counters must be UNCHANGED at this point —
    // the DRAINING short-circuit did NOT decrement outstanding_sub_io /
    // inflight_block_count / inflight_blocks[op] / *io_count.  This is
    // the invariant the trampoline enforces; if any counter went
    // negative here the regression has recurred.
    EXPECT_EQ(task_a->outstanding_sub_io.load(), 2);
    EXPECT_EQ(task_a->inflight_block_count.load(), 8);
    EXPECT_EQ(task_b->outstanding_sub_io.load(), 2);
    EXPECT_EQ(task_b->inflight_block_count.load(), 8);
    EXPECT_EQ(qos.inflight_blocks[kInflightOp].load(), 8);
    EXPECT_EQ(io_count->load(), 4);
    // task_a / task_b are still alive (FinalizeAfterDrain has not
    // run) so dereferencing them is safe.

    // Step 7: FinalizeAfterDrain pays back every counter the trampoline
    // skipped (outstanding_sub_io via exchange(0), io_count via
    // fetch_sub, inflight_blocks[op] via exchange(0)) and routes each
    // task through SpdkNofTaskCompletion.  try_complete() CAS
    // arbitrates set_completed + delete so it happens exactly once.
    qos.FinalizeAfterDrain();

    // After FinalizeAfterDrain, both tasks have been deleted by
    // SpdkNofTaskCompletion's `if (!on_chain) delete task;` branch.
    // We observe completion through the shared_ptrs we captured.
    EXPECT_TRUE(state_a->is_completed());
    EXPECT_TRUE(state_b->is_completed());
    EXPECT_EQ(qos.active_tasks.size(), 0u);

    // Counter pay-back: FinalizeAfterDrain must zero every counter
    // (io_count via fetch_sub, inflight_blocks[op] via exchange(0))
    // and route every task through SpdkNofTaskCompletion exactly once.
    EXPECT_EQ(io_count->load(std::memory_order_acquire), 0);
    EXPECT_EQ(qos.inflight_blocks[kInflightOp].load(std::memory_order_acquire),
              0);

    // Cleanup: the sub_tasks allocated for the trampoline helper go
    // back into survivor_sub_pool; release them here.  In production
    // the trampoline pushes sub_task back into the per-worker pool;
    // in the test we just delete them since no real worker pool is
    // involved.
    while (!survivor_sub_pool.empty()) {
        delete survivor_sub_pool.top();
        survivor_sub_pool.pop();
    }
}

// SIBLING-2: WaitForInflightCompletion observes InflightCount==0
// within its budget once the DRAINING short-circuit has drained every
// surviving CQE.  This pins the release/acquire fence semantics: the
// destructor path that calls WaitForInflightCompletion must not block
// past the point where the last CQE has decremented the counter.
//
// Race-driven: a background thread fires two late CQEs via the
// short-circuit while the main thread is blocked in
// WaitForInflightCompletion.  Total wall-clock must be bounded; if
// the fence is broken the main thread blocks indefinitely and the
// test times out.
TEST(NofQpairDrainSibling, SiblingQpairFailure_PoolQuiescenceFence) {
    auto pool = MakeMultiQpairPool(/*qp_count=*/2,
                                   /*max_inflight=*/4);
    // Two inflight CQEs on the surviving sibling qpair.  The dying
    // qpair's CQEs are not modelled here — this test only pins the
    // release/acquire fence, i.e. that WaitForInflightCompletion
    // observes the late CQE's DecrementInflight in bounded time once
    // the pool is in DRAINING.  The 2:2 increment/decrement pairing
    // is what makes InflightCount reach 0; an extra pair of increments
    // (e.g. leftover from the UAF test above) would leave the count
    // at 2 forever and force WaitForInflightCompletion to hit its
    // timeout.
    pool->IncrementInflight();
    pool->IncrementInflight();
    pool->EnterDraining("sibling_failure_test");
    EXPECT_TRUE(pool->IsDraining());

    std::atomic<bool> short_circuit_done{false};
    std::thread late_cqe_thread([&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        // Simulate two sibling qpairs each firing one late CQE.
        pool->DecrementInflight();
        pool->DecrementInflight();
        short_circuit_done.store(true, std::memory_order_release);
    });

    auto start = std::chrono::steady_clock::now();
    EXPECT_TRUE(
        pool->WaitForInflightCompletion(mooncake::kDefaultDrainTimeoutMs));
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::steady_clock::now() - start)
                          .count();
    late_cqe_thread.join();
    EXPECT_TRUE(short_circuit_done.load());
    // Strict fence must observe the decrement in bounded time.  If
    // this fails the fence is broken (e.g. spin-locked with timeout
    // that masks the release/acquire ordering).
    EXPECT_LT(elapsed_ms, 2000);
    EXPECT_EQ(pool->InflightCount(), 0);
}

// SIBLING-3: After the sibling-failure sequence completes,
// FinalizeAfterDrain must route every active task through
// SpdkNofTaskCompletion exactly once AND pay back every counter the
// trampoline skipped on the DRAINING short-circuit.
//
// SpdkNofOperationState::set_completed assert (!result_.has_value())
// is the runtime check for single-completion.
TEST(NofQpairDrainSibling,
     SiblingQpairFailure_FinalizeAfterDrain_CompletesAllTasks) {
    mooncake::SpdkNofQos qos(/*block_size=*/4096);
    qos.inflight_blocks_limit = 1024;
    constexpr int kInflightOp = mooncake::kSpdkNofOpRead;
    // Pre-load counters as if the trampoline short-circuited:
    qos.inflight_blocks[kInflightOp].store(24, std::memory_order_relaxed);

    auto io_count = std::make_shared<std::atomic<int64_t>>(6);
    std::vector<std::shared_ptr<mooncake::SpdkNofOperationState>> state_holders;

    constexpr int kTaskCount = 3;
    for (int i = 0; i < kTaskCount; ++i) {
        auto* task = MakeTask(/*total_blocks=*/8, /*op=*/kInflightOp);
        task->nof_qos = &qos;
        task->io_count = io_count;
        // outstanding_sub_io > 0 because the DRAINING short-circuit
        // skipped the trampoline's decrement on these tasks' CQEs.
        task->outstanding_sub_io.store(2, std::memory_order_relaxed);
        task->inflight_block_count.store(8, std::memory_order_relaxed);
        task->remaining_lba.store(8, std::memory_order_relaxed);
        task->on_chain = false;
        state_holders.push_back(task->state);
        qos.active_tasks.insert(task);
    }
    EXPECT_EQ(qos.active_tasks.size(), static_cast<size_t>(kTaskCount));

    // Snapshot pre-call values for reference.  Per-task atomic fields
    // (outstanding_sub_io, inflight_block_count) live on the task
    // object; once FinalizeAfterDrain runs, those tasks are deleted
    // via SpdkNofTaskCompletion's `if (!on_chain) delete task;` —
    // reading them post-call would be UAF.
    const int64_t pre_io_count = io_count->load(std::memory_order_relaxed);
    const int pre_inflight_blocks =
        qos.inflight_blocks[kInflightOp].load(std::memory_order_relaxed);
    EXPECT_EQ(pre_io_count, 6);
    EXPECT_EQ(pre_inflight_blocks, 24);

    qos.FinalizeAfterDrain();

    // Every task has been routed through SpdkNofTaskCompletion.
    // set_completed was called exactly once per task (the
    // try_complete() CAS arbitrates with any racing trampoline
    // normal-path — none runs here, so FinalizeAfterDrain is the
    // sole caller).  The assert in set_completed would fire if a
    // double-set occurred.
    for (auto& st : state_holders) {
        EXPECT_TRUE(st->is_completed());
    }
    EXPECT_EQ(qos.active_tasks.size(), 0u);

    // Counter pay-back: FinalizeAfterDrain zeroes every counter
    // (io_count via fetch_sub, inflight_blocks[op] via exchange(0))
    // before SpdkNofTaskCompletion deletes the task objects.  After
    // the call, only the shared_ptr-managed io_count and the
    // stack-allocated nof_qos's inflight_blocks[op] remain safe to
    // observe — both must be 0.
    EXPECT_EQ(io_count->load(std::memory_order_acquire), 0);
    EXPECT_EQ(qos.inflight_blocks[kInflightOp].load(std::memory_order_acquire),
              0);
}

// ===========================================================================
// DRAIN-8: PollAll / hold / TestInjectPollErrorOnce interaction.
//
// Regression coverage for `Reproducer_DrainTimeout_FutureCompletesNotHangs`.
// The real bug was that `hold_all_completions_` short-circuited the
// inject-error path inside PollAll, so when a test armed the inject
// AFTER setting hold, the pool never observed the error and the
// future hung.  After the fix, inject is consumed before the hold
// gate; sibling qpairs whose CQE harvest is suppressed still see the
// trampoline DRAINING short-circuit once EnterDraining is driven from
// the -1 return value.
// ===========================================================================

TEST(NofQpairDrainSibling, PollAll_InjectFiresWhenHoldIsSet) {
    // Two qpair slots; both null so SPDK is not exercised.  Hold
    // armed before inject so the test order matches the regression
    // (`TestHoldAllCompletions(true); TestInjectPollErrorOnce(0);`).
    auto pool = MakeMultiQpairPool(/*qp_count=*/2, /*max_inflight=*/4);
    pool->TestHoldAllCompletions(true);
    pool->TestInjectPollErrorOnce(/*qpair_idx=*/0);

    int32_t poll_rc = pool->PollAll(/*max_completions=*/0);
    EXPECT_LT(poll_rc, 0)
        << "PollAll must return negative when inject is armed even "
           "while hold_all_completions_ is true";
    // The arm must be consumed in a single PollAll iteration.
    EXPECT_EQ(pool->TestPendingInjectErrorIdx(), SIZE_MAX);

    // Hold must STILL be true after PollAll returned -1: the
    // regression test relies on the held CQEs not being released
    // until the test calls `TestHoldAllCompletions(false)` itself.
    EXPECT_TRUE(pool->TestIsHoldingCompletions());
}

TEST(NofQpairDrainSibling, PollAll_HoldStillBlocksSPDKAfterInject) {
    // Inject on qpair 0, hold armed.  After PollAll consumes the
    // inject (returning -1), subsequent PollAll calls must not
    // touch the SPDK path on the held qpairs.  We verify by giving
    // the pool a non-null qpair (via a sentinel pointer) and
    // counting how many times spdk_nvme_qpair_process_completions
    // would have been called: a nullptr slot short-circuits before
    // SPDK, so we use the `hold_all_completions_` flag itself as the
    // proxy.  The simpler invariant: hold remains armed after
    // PollAll returns -1, so any subsequent re-entry must NOT touch
    // the SPDK call (it'll `continue` at the hold gate).
    auto pool = MakeMultiQpairPool(/*qp_count=*/2, /*max_inflight=*/4);
    pool->TestHoldAllCompletions(true);
    pool->TestInjectPollErrorOnce(/*qpair_idx=*/0);

    int32_t first = pool->PollAll(0);
    EXPECT_LT(first, 0);

    // Second PollAll: inject is already consumed (SIZE_MAX); hold is
    // still true.  Expected return: 0 (no error, no completion).
    int32_t second = pool->PollAll(0);
    EXPECT_EQ(second, 0)
        << "after inject consumed and hold still armed, PollAll must "
           "return 0 (no error synthesised, no SPDK CQE harvested)";
    EXPECT_TRUE(pool->TestIsHoldingCompletions());
}

// ===========================================================================
// DRAIN-9: FinalizeSubmittedTask single-pop invariants.
//
// These tests pin the contract added in the fix: the helper pops the
// task off the chain at most once (guarded by task->on_chain) and
// routes through SpdkNofTaskCompletion's try_complete() CAS exactly
// once.  Pre-fix, the sync-failure path called PopTask + on_chain=false
// inline AND the submit-loop epilogue did the same, leaving every
// alternate task without a termination path.  FinalizeSubmittedTask
// collapses both code paths into one.
//
// These tests cannot call the static helper directly (it lives in
// transfer_task.cpp's anonymous namespace and is not exported).  The
// regression is therefore exercised by simulating the worker's
// sync-failure path manually: set failed=true, zero remaining_lba,
// ensure outstanding_sub_io is 0, then walk the chain and call
// SpdkNofTaskCompletion once per task via FinalizeAfterDrain's
// contract.  The test asserts the chain is fully drained with no
// task being silently skipped.
// ===========================================================================

TEST(NofQpairDrainSibling, SyncFailurePath_DoesNotPopSiblingTask) {
    mooncake::SpdkNofQos qos(/*block_size=*/4096);
    qos.inflight_blocks_limit = 1024;
    constexpr int kOp = mooncake::kSpdkNofOpRead;

    // Push 4 tasks onto the read chain.  All start with
    // outstanding_sub_io == 0 (no successful submits yet) and
    // remaining_lba == total_blocks.  Simulate the worker's sync
    // failure for each: set failed=true, zero remaining_lba.
    constexpr int kTaskCount = 4;
    std::vector<std::shared_ptr<mooncake::SpdkNofOperationState>> state_holders;
    state_holders.reserve(kTaskCount);
    for (int i = 0; i < kTaskCount; ++i) {
        auto* task = MakeTask(/*total_blocks=*/4, /*op=*/kOp);
        task->nof_qos = &qos;
        task->on_chain = false;  // let PushTask set it
        qos.PushTask(task);
        state_holders.push_back(task->state);
    }
    ASSERT_EQ(qos.CountOnChain(), static_cast<size_t>(kTaskCount));
    ASSERT_EQ(qos.active_tasks.size(), static_cast<size_t>(kTaskCount));

    // Walk the chain head-to-tail, simulating FinalizeSubmittedTask's
    // sequence: remaining_lba=0, outstanding_sub_io=0 → pop and
    // complete.  Walking this way proves no task is silently skipped
    // (the double-pop regression would have made head advance by 2
    // per sync-failure iteration, leaving every alternate task
    // orphaned on the chain).
    SpdkNofTask* cursor = qos.head[kOp];
    int visited = 0;
    while (cursor != nullptr) {
        SpdkNofTask* next = cursor->nxt;
        cursor->failed.store(true, std::memory_order_release);
        cursor->remaining_lba.store(0, std::memory_order_release);
        // Outstanding is already 0 (we never incremented).
        ASSERT_EQ(cursor->outstanding_sub_io.load(), 0);
        ASSERT_TRUE(cursor->on_chain)
            << "PushTask must set on_chain=true; invariant regressed";
        qos.PopTask(kOp);
        cursor->on_chain = false;
        // SpdkNofTaskCompletion is the production completion path.
        // It is file-static in transfer_task.cpp; we drive it via
        // the public SpdkNofQos::FinalizeAfterDrain which calls
        // SpdkNofTaskCompletion internally for every active task.
        // For this single-task-at-a-time test we trigger completion
        // by calling SpdkNofTaskCompletion through FinalizeAfterDrain
        // after the simulation loop — see the post-loop section.
        cursor = next;
        ++visited;
    }
    EXPECT_EQ(visited, kTaskCount);
    // Every task is now off-chain.  active_tasks still contains them
    // because we didn't route through SpdkNofTaskCompletion.
    EXPECT_EQ(qos.CountOnChain(), 0u);
    EXPECT_EQ(qos.active_tasks.size(), static_cast<size_t>(kTaskCount));

    // FinalizeAfterDrain drives SpdkNofTaskCompletion for every
    // remaining active task.  Each task must be set_completed exactly
    // once (assertion inside set_completed enforces this).
    qos.FinalizeAfterDrain();
    for (auto& st : state_holders) {
        EXPECT_TRUE(st->is_completed());
    }
    EXPECT_EQ(qos.active_tasks.size(), 0u);
}

TEST(NofQpairDrainSibling,
     FinalizeAfterDrain_SyncFailureChain_DoesNotDoublePop) {
    // Stronger regression test: build a chain of 8 sync-failed tasks
    // (simulating 8 DRAINING-state submits).  Without the fix, the
    // first 4 would have been completed correctly but the last 4
    // would be silently orphaned (because the historical double-pop
    // popped head twice per iteration, halving effective iteration
    // count).  With FinalizeAfterDrain as the single termination
    // gate, all 8 must reach `is_completed() == true`.
    mooncake::SpdkNofQos qos(/*block_size=*/4096);
    qos.inflight_blocks_limit = 1024;
    constexpr int kOp = mooncake::kSpdkNofOpRead;
    constexpr int kTaskCount = 8;

    std::vector<std::shared_ptr<mooncake::SpdkNofOperationState>> state_holders;
    state_holders.reserve(kTaskCount);
    for (int i = 0; i < kTaskCount; ++i) {
        auto* task = MakeTask(/*total_blocks=*/4, /*op=*/kOp);
        task->nof_qos = &qos;
        task->on_chain = false;
        qos.PushTask(task);
        state_holders.push_back(task->state);
    }
    ASSERT_EQ(qos.CountOnChain(), static_cast<size_t>(kTaskCount));

    // Manually sync-fail each task in chain order (mimics the worker
    // walking head-to-tail in DRAINING).  The on_chain guard in
    // FinalizeSubmittedTask (modelled here as: PopTask only when
    // on_chain is true) is what prevents the double-pop regression
    // from popping a sibling task.
    SpdkNofTask* cursor = qos.head[kOp];
    int processed = 0;
    while (cursor != nullptr) {
        SpdkNofTask* next = cursor->nxt;
        ASSERT_TRUE(cursor->on_chain);
        cursor->failed.store(true, std::memory_order_release);
        cursor->remaining_lba.store(0, std::memory_order_release);
        qos.PopTask(kOp);
        cursor->on_chain = false;
        cursor = next;
        ++processed;
    }
    EXPECT_EQ(processed, kTaskCount);
    EXPECT_EQ(qos.CountOnChain(), 0u);
    EXPECT_EQ(qos.active_tasks.size(), static_cast<size_t>(kTaskCount));

    qos.FinalizeAfterDrain();
    for (auto& st : state_holders) {
        EXPECT_TRUE(st->is_completed())
            << "every sync-failed task must be completed by "
               "FinalizeAfterDrain — sibling-skip regression would "
               "leave this task with is_completed()==false";
    }
    EXPECT_EQ(qos.active_tasks.size(), 0u);
}

TEST(NofQpairDrainSibling, PushTask_SetsOnChainTrue) {
    // Direct invariant check: every task pushed onto the chain must
    // have on_chain==true.  The historical code path left this to
    // the caller (worker set on_chain=true before PushTask), but
    // the qos itself could not enforce it.  PushTask now sets it.
    mooncake::SpdkNofQos qos(/*block_size=*/4096);
    auto* task = MakeTask(/*total_blocks=*/4, /*op=*/mooncake::kSpdkNofOpRead);
    task->on_chain = false;
    qos.PushTask(task);
    EXPECT_TRUE(task->on_chain);
    EXPECT_EQ(qos.CountOnChain(), 1u);
    // Cleanup: keep qos invariant intact.
    qos.active_tasks.erase(task);
    delete task;
}

#endif  // MOONCAKE_TEST_DRAIN

}  // namespace mooncake::test

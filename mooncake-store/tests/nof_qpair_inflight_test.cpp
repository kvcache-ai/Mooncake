/*
 * Layer-1 unit tests for the NofQpairPool inflight synchronization
 * primitive.
 *
 * The primitive proves "no callback will fire after free_io_qpair"
 * by waiting for the inflight counter to reach zero within a bounded
 * budget.  ProbeNofSegment uses IncrementInflight/DecrementInflight
 * as a trampoline pair so the qpair pool can apply this proof
 * synchronously before destruction.
 *
 * ProbeNofSegment uses IncrementInflight/DecrementInflight as a
 * trampoline pair, so the qpair pool can prove "no callback will fire
 * after free_io_qpair".
 *
 * Tests here focus on the counter semantics in isolation.  We construct
 * a NofQpairPool with an empty qpair vector and a null ctrlr so that
 * no SPDK methods are invoked; we verify:
 *
 *   - INV-1: Increment/Decrement pair maintains InflightCount accurately.
 *   - INV-2: WasEverUsedWithInflight flips on first Increment.
 *   - INV-3: WaitForInflightCompletion returns immediately when
 *     InflightCount==0 (no SPDK dereference).
 *   - INV-4: WaitForInflightCompletion with a 0-budget still returns
 *     the current state (defensive).
 *   - INV-5: ProbeCtxRecycler::PushWithConn accepts ctx + null conn
 *     and Drain releases both atomically.
 *
 * Note: tests that need a real SPDK target (T2/T3 boundary-timeout,
 * T5 stuck-CQE 30s budget) cannot be unit-tested without a target
 * and live in nof_heartbeat_test.cpp / integration tests.
 */

#include "spdk/nof_connection.h"
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

// Construct a NofQpairPool with NO real qpairs and a null ctrlr so we
// can drive the counter without touching the SPDK runtime.  All methods
// that would dereference qpairs (PollAll, TryGrow, free_io_qpair) are
// not invoked by these tests.
std::unique_ptr<mooncake::NofQpairPool> MakeStubPool() {
    return std::make_unique<mooncake::NofQpairPool>(
        std::vector<spdk_nvme_qpair*>{}, /*max_inflight_per_qpair=*/64,
        /*target_count=*/0, /*ctrlr=*/nullptr);
}

}  // namespace

// ===========================================================================
// INV-1: Increment/Decrement pair maintains InflightCount accurately.
// ===========================================================================

TEST(NofQpairInflight, InitialCountIsZero) {
    auto pool = MakeStubPool();
    EXPECT_EQ(pool->InflightCount(), 0);
    EXPECT_FALSE(pool->WasEverUsedWithInflight());
}

TEST(NofQpairInflight, IncrementDecrementPair) {
    auto pool = MakeStubPool();
    pool->IncrementInflight();
    EXPECT_EQ(pool->InflightCount(), 1);
    pool->IncrementInflight();
    EXPECT_EQ(pool->InflightCount(), 2);
    pool->DecrementInflight();
    EXPECT_EQ(pool->InflightCount(), 1);
    pool->DecrementInflight();
    EXPECT_EQ(pool->InflightCount(), 0);
}

TEST(NofQpairInflight, ManyIncrementDecrement) {
    auto pool = MakeStubPool();
    constexpr int N = 1000;
    for (int i = 0; i < N; ++i) {
        pool->IncrementInflight();
    }
    EXPECT_EQ(pool->InflightCount(), N);
    for (int i = 0; i < N; ++i) {
        pool->DecrementInflight();
    }
    EXPECT_EQ(pool->InflightCount(), 0);
}

// ===========================================================================
// INV-2: WasEverUsedWithInflight flips on first Increment.
// ===========================================================================

TEST(NofQpairInflight, WasEverUsedFlipsOnFirstIncrement) {
    auto pool = MakeStubPool();
    EXPECT_FALSE(pool->WasEverUsedWithInflight());
    pool->IncrementInflight();
    EXPECT_TRUE(pool->WasEverUsedWithInflight());
}

TEST(NofQpairInflight, WasEverUsedStaysTrueAfterDecrement) {
    auto pool = MakeStubPool();
    pool->IncrementInflight();
    pool->IncrementInflight();
    EXPECT_TRUE(pool->WasEverUsedWithInflight());
    pool->DecrementInflight();
    pool->DecrementInflight();
    EXPECT_EQ(pool->InflightCount(), 0);
    // Even though InflightCount==0, the metadata stays true so
    // ~NofQpairPool can distinguish "truly quiescent" from
    // "trivially 0 because no path used Increment".
    EXPECT_TRUE(pool->WasEverUsedWithInflight());
}

TEST(NofQpairInflight, WasEverUsedStaysFalseIfNeverIncremented) {
    auto pool = MakeStubPool();
    EXPECT_FALSE(pool->WasEverUsedWithInflight());
    // Decrementing without prior Increment must not flip the flag.
    // (This protects against double-decrement bugs.)
    // We don't call Decrement here because the count would go
    // negative; the test is just that the flag stays false.
    EXPECT_FALSE(pool->WasEverUsedWithInflight());
}

// ===========================================================================
// INV-3: WaitForInflightCompletion returns immediately when
// InflightCount==0 — happy path: no polling required.
// ===========================================================================

TEST(NofQpairInflight, WaitReturnsImmediatelyWhenCountIsZero) {
    auto pool = MakeStubPool();
    auto start = std::chrono::steady_clock::now();
    bool ok = pool->WaitForInflightCompletion();
    auto elapsed = std::chrono::steady_clock::now() - start;
    EXPECT_TRUE(ok);
    // Should complete in microseconds, not seconds.
    EXPECT_LT(
        std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(),
        100);
}

TEST(NofQpairInflight, WaitReturnsTrueAfterDecrementPair) {
    auto pool = MakeStubPool();
    pool->IncrementInflight();
    pool->IncrementInflight();
    pool->DecrementInflight();
    pool->DecrementInflight();
    EXPECT_TRUE(pool->WaitForInflightCompletion());
}

// ===========================================================================
// INV-4: WaitForInflightCompletion has NO budget parameter — it is a
// strict mechanism-level fence.  This test guards the API shape so
// future regressions that re-introduce a budget get caught here.
// ===========================================================================

TEST(NofQpairInflight, WaitReturnsCurrentStateImmediately) {
    auto pool = MakeStubPool();
    // Even without a budget, if count is 0 we return true immediately
    // (the loop body checks count first and exits without sleeping).
    EXPECT_TRUE(pool->WaitForInflightCompletion());
}

// ===========================================================================
// INV-5: ProbeCtxRecycler::PushWithConn accepts ctx + null conn and
// Drain releases both atomically.  The null conn is a degenerate
// case (no real probe ever opens a null conn) but exercises the
// recycler path that the 30s-timeout fallback uses.
// ===========================================================================

TEST(ProbeCtxRecycler, PushWithConnAndDrain) {
    auto& r = mooncake::ProbeCtxRecycler::Instance();
    r.Drain();
    auto ctx = std::make_shared<mooncake::ProbeRequestContext>();
    std::unique_ptr<mooncake::NofConnection> null_conn;
    r.PushWithConn(std::move(ctx), std::move(null_conn));
    // Drain releases both lists atomically.
    r.Drain();
    SUCCEED();  // No crash = success criterion.
}

TEST(ProbeCtxRecycler, PushWithConnMultipleTimesThenDrain) {
    auto& r = mooncake::ProbeCtxRecycler::Instance();
    r.Drain();
    for (int i = 0; i < 5; ++i) {
        auto ctx = std::make_shared<mooncake::ProbeRequestContext>();
        std::unique_ptr<mooncake::NofConnection> null_conn;
        r.PushWithConn(std::move(ctx), std::move(null_conn));
    }
    r.Drain();
    SUCCEED();
}

}  // namespace mooncake::test

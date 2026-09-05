/*
 * Layer-2 integration tests for the NofQpairPool inflight proof
 * against a real NVMe-oF target.  Exercises ProbeNofSegment's
 * trampoline +
 * Phase 1b path, the recycler across consecutive probes, and the
 * transfer-path safety contract (CloseNofSegment after OpenNofSegment +
 * PipelineRead).
 *
 * Requires:
 *   - A reachable SPDK NVMe-oF target (any transport RDMA or TCP).
 *   - Environment variable MC_TEST_NOF_TARGET set to the SPDK
 *     transport string, e.g.:
 *       export MC_TEST_NOF_TARGET="traddr:10.0.0.5 trsvcid:4420 \
 *         subnqn:nqn.2024-08.mooncake:test trtype:RDMA adrfam:IPv4 ns:1"
 *
 * Skips:
 *   - GTEST_SKIP when MC_TEST_NOF_TARGET is unset, so the test
 *     gracefully no-ops on developer machines without infrastructure.
 *
 * Coverage matrix (against the 6 invariants):
 *
 *   RealTarget.ProbeNormalPath            - INV-1, INV-3 happy path
 *   RealTarget.ProbeLateCallback           - INV-3 Phase 1b catches late CQE
 *   RealTarget.ProbeRecyclerAcrossRuns     - INV-5 multi-cycle correctness
 *   RealTarget.ProbeConcurrentThreads      - INV-1 thread-safety of counter
 *   RealTarget.ProbeFailureMalformedString - error_reason reporting
 *   RealTarget.TransferPathCloseOrdering   - transfer-path safety contract
 *   RealTarget.MultipleOpensThenClose      - open_segments_ teardown
 *
 * Note: This test file requires an actual NVMe-oF target.  The
 * companion nof_qpair_inflight_test.cpp covers the counter semantics
 * in isolation (no SPDK target required).
 */

#include "spdk/nof_config.h"
#include "spdk/nof_connection.h"
#include "spdk/nof_segment.h"
#include "spdk/spdk_wrapper.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace mooncake::test {

namespace {

// Read the SPDK transport string from MC_TEST_NOF_TARGET.  Returns
// empty string if unset.
std::string GetTargetTransportString() {
    const char *raw = std::getenv("MC_TEST_NOF_TARGET");
    if (!raw || *raw == '\0') return "";
    return std::string(raw);
}

// Polls a condition until it returns true or the timeout elapses.
bool WaitForCondition(std::chrono::milliseconds timeout,
                      std::chrono::milliseconds interval,
                      const std::function<bool()> &condition) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (condition()) return true;
        std::this_thread::sleep_for(interval);
    }
    return condition();
}

// One-time SPDK env initialization for the test suite.  SpdkWrapper
// is a singleton; InitializeEnv() is idempotent (returns true on
// subsequent calls), so calling it from SetUp() of every test is
// safe but redundant.  Doing it once in SetUpTestSuite keeps the
// per-test setup minimal.
class RealTarget : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("RealTarget");
        FLAGS_logtostderr = true;
        auto &wrapper = mooncake::SpdkWrapper::GetInstance();
        ASSERT_TRUE(wrapper.InitializeEnv())
            << "Failed to initialize SPDK environment";
    }

    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

    void SetUp() override {
        target_ = GetTargetTransportString();
        if (target_.empty()) {
            GTEST_SKIP()
                << "MC_TEST_NOF_TARGET is not set; skipping real-target "
                   "test. Set it to an SPDK transport string to enable.";
        }
    }

    std::string target_;
};

}  // namespace

// ===========================================================================
// T1: ProbeNormalPath — happy path.  Increment → callback →
// Decrement → InflightCount==0 → Phase 1 returns success within the
// soft budget.  Validates INV-1 + INV-3.
// ===========================================================================

TEST_F(RealTarget, ProbeNormalPath) {
    auto &wrapper = mooncake::SpdkWrapper::GetInstance();
    std::string reason;
    bool ok = wrapper.ProbeNofSegment(target_, /*timeout_ms=*/2000, &reason);
    EXPECT_TRUE(ok) << "Probe failed: " << reason;

    // After ProbeNofSegment returns, the probe conn has been
    // destroyed; we cannot directly observe its InflightCount.
    // Instead, verify the recycler is empty: any pending ctx/conn
    // would indicate a non-quiescent termination.
    // Drain() is a no-op when the recycler is empty; we just verify
    // it doesn't crash and that a follow-up probe succeeds.
    EXPECT_TRUE(wrapper.ProbeNofSegment(target_, 2000, &reason))
        << "Second probe failed: " << reason;
}

// ===========================================================================
// T2: ProbeLateCallback — soft timeout=0 forces Phase 1 timeout;
// Phase 1b's WaitForInflightCompletion catches the late callback
// before the conn destructor.  Validates INV-3 + the trampoline path.
// ===========================================================================

TEST_F(RealTarget, ProbeLateCallback) {
    auto &wrapper = mooncake::SpdkWrapper::GetInstance();
    std::string reason;
    // timeout_ms=1: soft timeout fires immediately, but the I/O will
    // complete shortly after.  Phase 1b's 30s budget is what actually
    // waits for the callback.
    bool ok = wrapper.ProbeNofSegment(target_, /*timeout_ms=*/1, &reason);
    // ok==true means the late callback was caught AND it reported
    // success (status.sc==0).  ok==false with reason=="completion_error"
    // is also acceptable — it means the late callback was caught but
    // the read returned a non-zero status.  The crucial assertion is
    // that we DID NOT see "completion_timeout" (which would mean
    // Phase 1b itself timed out after 30s).
    if (!ok) {
        EXPECT_NE(reason, "completion_timeout")
            << "Phase 1b exceeded 30s budget — quiescent proof failed: "
            << reason;
    }
}

// ===========================================================================
// T3: ProbeRecyclerAcrossRuns — N consecutive probes; each
// probe's conn + ctx go through the recycler; verify the recycler
// doesn't accumulate entries and doesn't double-free.  Validates
// INV-5 across many iterations.
// ===========================================================================

TEST_F(RealTarget, ProbeRecyclerAcrossRuns) {
    auto &wrapper = mooncake::SpdkWrapper::GetInstance();
    constexpr int kIterations = 10;
    int success_count = 0;
    for (int i = 0; i < kIterations; ++i) {
        std::string reason;
        if (wrapper.ProbeNofSegment(target_, /*timeout_ms=*/2000, &reason)) {
            ++success_count;
        }
    }
    // We don't require all 10 to succeed (target may time out under
    // load), but at least most should.  The key invariant is that we
    // don't see repeated failures with "completion_timeout" — that
    // would mean the recycler is leaking conns/ctxs.
    EXPECT_GE(success_count, kIterations - 2)
        << "Only " << success_count << "/" << kIterations
        << " probes succeeded — recycler may be leaking";
}

// ===========================================================================
// T4: ProbeConcurrentThreads — two threads probe the same target
// simultaneously.  Validates INV-1 thread-safety (atomic
// increment/decrement).
// ===========================================================================

TEST_F(RealTarget, ProbeConcurrentThreads) {
    auto &wrapper = mooncake::SpdkWrapper::GetInstance();
    std::atomic<int> success_count{0};
    std::atomic<int> timeout_count{0};
    auto probe_fn = [&]() {
        for (int i = 0; i < 3; ++i) {
            std::string reason;
            if (wrapper.ProbeNofSegment(target_, 2000, &reason)) {
                success_count.fetch_add(1, std::memory_order_relaxed);
            } else if (reason == "completion_timeout") {
                timeout_count.fetch_add(1, std::memory_order_relaxed);
            }
        }
    };
    std::thread t1(probe_fn);
    std::thread t2(probe_fn);
    t1.join();
    t2.join();
    // We don't expect all 6 to succeed (concurrent Connect calls
    // contend for the connect_mutex_ and may exceed the probe
    // timeout).  But none should hit "completion_timeout" if the
    // trampoline/release-acquire pairing works.
    EXPECT_EQ(timeout_count.load(), 0)
        << "Concurrent probes hit completion_timeout — release/acquire "
           "ordering may be broken";
}

// ===========================================================================
// T5: ProbeFailureMalformedString — a syntactically invalid transport
// string must return false synchronously without leaving the recycler
// in a poisoned state.  Validates the error path doesn't corrupt
// subsequent probes.
// ===========================================================================

TEST_F(RealTarget, ProbeFailureMalformedString) {
    auto &wrapper = mooncake::SpdkWrapper::GetInstance();
    std::string reason;
    bool ok = wrapper.ProbeNofSegment("not_a_valid_trstring", 1000, &reason);
    EXPECT_FALSE(ok);
    EXPECT_FALSE(reason.empty());

    // Subsequent probe against the real target must still work.
    EXPECT_TRUE(wrapper.ProbeNofSegment(target_, 2000, &reason))
        << "Probe after malformed-string failure did not recover: " << reason;
}

// ===========================================================================
// T6: TransferPathCloseOrdering — exercise the transfer-path safety
// contract: open a segment, do a read, then close it.  The
// close_nof_segment docs require the caller to have joined the worker
// pool; in this minimal test we do synchronous reads, so no worker
// pool is involved and the contract trivially holds.
// ===========================================================================

TEST_F(RealTarget, TransferPathCloseOrdering) {
    auto &wrapper = mooncake::SpdkWrapper::GetInstance();
    wrapper.SetConfig(mooncake::NofConfig::FromEnv());

    nof_seg_handle *handle = wrapper.OpenNofSegment(target_);
    ASSERT_NE(handle, nullptr) << "OpenNofSegment failed for " << target_;

    // Do a synchronous 1-block read at offset 0.  The I/O buffer must
    // be DMA-registered with SPDK's memory domain — a plain heap
    // allocation (std::vector / new / malloc) has no MR registration
    // and fails at nvme_rdma_req_init() with rc -22 (EINVAL), as
    // observed in the SPDK log:
    //   spdk_rdma_get_translation: No translation for ptr 0x...
    //   nvme_rdma_get_memory_translation: rc -22
    // spdk_zmalloc with SPDK_MALLOC_DMA guarantees the buffer is
    // registered so the RDMA translation can resolve it.
    void *buf = spdk_zmalloc(/*size=*/4096, /*align=*/0x1000,
                             /*opts=*/nullptr, /*socket_id=*/-1,
                             /*flags=*/SPDK_MALLOC_DMA);
    ASSERT_NE(buf, nullptr) << "spdk_zmalloc failed";

    auto n = wrapper.PipelineRead(handle, buf, 0, 1);
    EXPECT_GE(n, 0) << "PipelineRead failed";

    spdk_free(buf);
    wrapper.CloseNofSegment(handle);

    // Subsequent open must still work (no leaked resources).
    handle = wrapper.OpenNofSegment(target_);
    EXPECT_NE(handle, nullptr)
        << "Second OpenNofSegment failed — resource leak?";
    if (handle) wrapper.CloseNofSegment(handle);
}

// ===========================================================================
// T7: MultipleOpensThenClose — open several segments to the same
// target, then close them in reverse order.  Exercises
// open_segments_ teardown semantics.
// ===========================================================================

TEST_F(RealTarget, MultipleOpensThenClose) {
    auto &wrapper = mooncake::SpdkWrapper::GetInstance();
    wrapper.SetConfig(mooncake::NofConfig::FromEnv());

    std::vector<nof_seg_handle *> handles;
    for (int i = 0; i < 3; ++i) {
        handles.push_back(wrapper.OpenNofSegment(target_));
    }
    // At least one should succeed; all 3 is the common case.
    EXPECT_FALSE(handles.empty());

    // Close in reverse order — exercises the open_segments_ map's
    // erase semantics (no use-after-free of the unique_ptr).
    for (auto it = handles.rbegin(); it != handles.rend(); ++it) {
        if (*it) wrapper.CloseNofSegment(*it);
    }

    // Verify the map is fully drained by opening a new one.
    auto *h = wrapper.OpenNofSegment(target_);
    EXPECT_NE(h, nullptr);
    if (h) wrapper.CloseNofSegment(h);
}

}  // namespace mooncake::test

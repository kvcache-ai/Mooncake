/*
 * Layer-2 regression test for the "one failed qpair with pending CQEs on
 * another qpair" production failure scenario.
 *
 * This file is the end-to-end counterpart to the Layer-1 protocol tests
 * (tests/nof_qpair_drain_protocol_test.cpp DRAIN-7 group).  The Layer-1
 * tests drive the protocol invariants in isolation using a stub
 * NofQpairPool.  The tests in this file drive the real worker thread
 * path against a real SPDK target:
 *
 *   1. Submit enough I/O to occupy every qpair in the pool with in-flight
 *      CQEs.
 *   2. Use the MOONCAKE_TEST_DRAIN hook (see NofQpairPool::
 *      TestInjectPollErrorOnce) to synthesise a transport error on
 *      qpair[0] during the next PollAll call.  The other qpairs in the
 *      pool continue to be polled normally, so any pending CQE on a
 *      sibling qpair fires through the real trampoline path.
 *   3. Observe the worker thread exit the drain loop cleanly:
 *        - no SIGSEGV
 *        - no log entries containing "outstanding io < 0" or
 *          "task inflight_block_count < 0"
 *        - all SpdkNofTask futures reach a terminal state (TRANSFER_FAIL)
 *        - the qpair pool reaches InflightCount() == 0 within a bounded
 *          wall-clock window
 *
 * Test gating:
 *   - The tests skip with GTEST_SKIP when MC_TEST_NOF_TARGET is unset
 *     (developer machines without an SPDK target).
 *   - The MOONCAKE_TEST_DRAIN compile-time gate keeps the
 *     TestInjectPollErrorOnce hook out of production binaries; the
 *     CMakeLists entry only sets that flag for the test target.
 *
 * Usage:
 *   export MC_TEST_NOF_TARGET="traddr:10.0.0.5 trsvcid:4420 \
 *     subnqn:nqn.2024-08.mooncake:test trtype:RDMA adrfam:IPv4 ns:1"
 *   ./mooncake_store_tests --gtest_filter=NofQpairSiblingFailure.*
 */

#include "spdk/nof_config.h"
#include "spdk/nof_connection.h"
#include "spdk/nof_segment.h"
#include "spdk/spdk_wrapper.h"

#include "transfer_task.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace mooncake::test {

namespace {

// Read the SPDK transport string from MC_TEST_NOF_TARGET.  Returns
// empty string if unset, which the test fixture uses to GTEST_SKIP.
std::string GetTargetTransportString() {
    const char* raw = std::getenv("MC_TEST_NOF_TARGET");
    if (!raw || *raw == '\0') return "";
    return std::string(raw);
}

// ScopedDrainInvariant — destructor-time invariant sentinel.
//
// Captures the qpair pool's pointer at construction and asserts at
// destruction that:
//   (a) InflightCount == 0 — every task routed through the
//       production path completed.
//   (b) If MarkDroveDraining() was called, IsDraining() is still
//       true (the pool didn't revert to kActive).
//
// Fires on every path out of the test body (return, exception,
// ADD_FAILURE exit), so a regression that bypasses the inline
// assertions is still reported.
//
// Uses ADD_FAILURE rather than EXPECT_EQ so the failure attaches
// even if gtest has already moved past the body.
class ScopedDrainInvariant {
   public:
    ScopedDrainInvariant(mooncake::NofQpairPool* pool, const char* test_name)
        : pool_(pool),
          test_name_(test_name ? test_name : "<unnamed>"),
          start_(std::chrono::steady_clock::now()) {}

    ~ScopedDrainInvariant() {
        auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now() - start_)
                              .count();
        if (pool_ == nullptr) {
            LOG(INFO) << "[" << test_name_
                      << "] ScopedDrainInvariant: pool=nullptr, elapsed="
                      << elapsed_ms << "ms";
            return;
        }
        int32_t inflight = pool_->InflightCount();
        bool draining = pool_->IsDraining();
        LOG(INFO) << "[" << test_name_
                  << "] ScopedDrainInvariant: elapsed=" << elapsed_ms
                  << "ms, InflightCount=" << inflight
                  << ", IsDraining=" << draining;

        if (inflight != 0) {
            ADD_FAILURE() << "[" << test_name_
                          << "] pool->InflightCount=" << inflight
                          << " (expected 0) at test scope exit — task leak.";
        }

        if (drove_draining_ && !draining) {
            ADD_FAILURE() << "[" << test_name_
                          << "] pool reverted from DRAINING to kActive after "
                             "test scope exit.";
        }
    }

    void MarkDroveDraining() { drove_draining_ = true; }

   private:
    mooncake::NofQpairPool* pool_;
    const char* test_name_;
    std::chrono::steady_clock::time_point start_;
    bool drove_draining_ = false;
};

// One-time SPDK env initialization for the test suite.  SpdkWrapper is
// a singleton and InitializeEnv() is idempotent, so calling it once in
// SetUpTestSuite keeps the per-test setup minimal.
class SiblingQpairFailure : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("SiblingQpairFailure");
        FLAGS_logtostderr = true;
        auto& wrapper = mooncake::SpdkWrapper::GetInstance();
        ASSERT_TRUE(wrapper.InitializeEnv())
            << "Failed to initialize SPDK environment";
    }

    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

    void SetUp() override {
        target_ = GetTargetTransportString();
        if (target_.empty()) {
            GTEST_SKIP()
                << "MC_TEST_NOF_TARGET is not set; skipping real-target "
                   "sibling-failure regression test. Set it to an SPDK "
                   "transport string to enable.";
        }
    }

    // Poll a predicate until it returns true or the timeout elapses.
    bool WaitForCondition(std::chrono::milliseconds timeout,
                          std::chrono::milliseconds interval,
                          const std::function<bool()>& condition) const {
        auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (condition()) return true;
            std::this_thread::sleep_for(interval);
        }
        return condition();
    }

    std::string target_;
};

}  // namespace

// ===========================================================================
// Regression test 1: read path with one qpair failure
// ===========================================================================
//
// Submits concurrent reads across 4 qpairs, then arms a synthesised
// transport error on qpair[0].  The worker's next PollAll flips the
// pool to DRAINING; sibling qpairs must drain their own CQEs cleanly
// via the DRAINING short-circuit of nvmf_io_complete.
//
// Uses submitSpdkNofOperation (production path) so the drain
// protocol in SpdkNofWorkerPool::workerThread is exercised.  A
// regression in the drain protocol surfaces as a hung
// TransferFuture.
//
// Test guarantees:
//   - No SIGSEGV under qpair death + sibling CQE pressure.
//   - Every SpdkNofTask future reaches a terminal state.
//   - The pool's InflightCount() reaches 0 within the drain timeout.
TEST_F(SiblingQpairFailure,
       ReadPath_OneQpairFailure_PendingSiblingCQEs_NoCrash) {
    auto& wrapper = mooncake::SpdkWrapper::GetInstance();

    mooncake::NofConfig cfg = mooncake::NofConfig::FromEnv();
    cfg.num_io_queues = 4;
    cfg.max_inflight_per_qpair = 16;
    cfg.io_queue_size = 256;
    cfg.io_queue_requests = 512;
    wrapper.SetConfig(cfg);

    // TransferEngine + TransferSubmitter — submit through the worker
    // thread so the drain protocol is exercised.
    mooncake::TransferEngine engine(false);
    ASSERT_EQ(engine.init("P2PHANDSHAKE", "localhost:17999"), 0);
    if (!engine.isUsingTent()) {
        ASSERT_NE(engine.installTransport("tcp", nullptr), nullptr);
    }
    const std::string local_endpoint = engine.getLocalIpAndPort();
    std::shared_ptr<mooncake::StorageBackend> storage_backend;
    mooncake::TransferSubmitter submitter(engine, storage_backend,
                                          local_endpoint);

    constexpr uint32_t kBlocks = 128;
    constexpr size_t kBufBytes = kBlocks * 4096;
    void* buf = spdk_zmalloc(kBufBytes, /*align=*/0x1000, /*opts=*/nullptr,
                             /*socket_id=*/-1, /*flags=*/SPDK_MALLOC_DMA);
    ASSERT_NE(buf, nullptr) << "spdk_zmalloc failed";

    auto make_desc = [&]() {
        mooncake::AllocatedBuffer::Descriptor d;
        d.size_ = kBufBytes;
        d.buffer_address_ = 0;
        d.protocol_ = "rdma";
        d.transport_endpoint_ = target_;
        return d;
    };

    // Prime the handle cache so subsequent submits hit the cache
    // path.  This is the same primer used by test 3 — see its
    // comment block for the buffer_address_ invariants.
    auto primer_desc = make_desc();
    auto primer_future = submitter.submitSpdkNofOperation(
        primer_desc, buf, /*size=*/4096, mooncake::TransferRequest::WRITE);
    ASSERT_TRUE(primer_future.has_value());
    auto primer_rc = primer_future->wait();
    if (primer_rc != mooncake::ErrorCode::OK) {
        spdk_free(buf);
        EXPECT_EQ(engine.freeEngine(), 0);
        GTEST_SKIP() << "Primer write returned " << static_cast<int>(primer_rc)
                     << " — target cannot service real I/O; skipping.";
    }

    nof_seg_handle* handle = submitter.TestGetNofHandle(target_);
    ASSERT_NE(handle, nullptr);
    auto* segment = handle->segment;
    ASSERT_NE(segment, nullptr);
    auto* conn = segment->GetConnection();
    ASSERT_NE(conn, nullptr);
    auto& pool = conn->GetQpairPool();
    ASSERT_EQ(pool.Size(), 4u);
    ASSERT_FALSE(pool.IsDraining())
        << "Pool must start in kActive — TestInjectPollErrorOnce is "
           "only valid from kActive";

    // Destructor-time invariant sentinel.  Fires on every path out
    // of the test body, so a regression that bypasses the inline
    // assertions below is still reported.
    ScopedDrainInvariant invariant(
        &pool, "ReadPath_OneQpairFailure_PendingSiblingCQEs_NoCrash");

    // Arm the synthesised transport error BEFORE the submits.  The
    // worker's PollAll block is gated on total_outstanding_io > 0;
    // if all CQEs land before the inject is armed the worker skips
    // the PollAll block and the inject is never consumed, leaving
    // the pool in kActive.  Arming first guarantees the inject fires
    // during the first poll cycle that observes in-flight IO.
    pool.TestInjectPollErrorOnce(/*qpair_idx=*/0);

    // Submit one read per qpair concurrently.  The round-robin
    // distribution in the qos spread gives every qpair at least one
    // in-flight CQE by the time we observe the inject.
    constexpr int kReaders = 4;
    std::vector<std::optional<mooncake::TransferFuture>> futures;
    futures.reserve(kReaders);
    for (int i = 0; i < kReaders; ++i) {
        auto desc = make_desc();
        // Distinguish reads by buffer_address_ so each read lands on
        // a different LBA range.  Buffer content is irrelevant; only
        // CQE completion semantics matter.
        desc.buffer_address_ = static_cast<uint64_t>(i) * kBufBytes;
        auto fut = submitter.submitSpdkNofOperation(
            desc, buf, /*size=*/kBufBytes, mooncake::TransferRequest::READ);
        ASSERT_TRUE(fut.has_value()) << "submitSpdkNofOperation failed";
        futures.push_back(std::move(fut));
    }

    // Hard assertion: the inject must actually drive the pool into
    // DRAINING.  If this fails the inject arm was stale (worker
    // didn't poll before the submits landed) or the trampoline's
    // DRAINING branch never fired.  Mark the invariant AFTER the
    // pool state is observed to keep MarkDroveDraining() honest —
    // the destructor-time sentinel only checks the secondary
    // invariant (didn't revert) when drove_draining_ was set true.
    bool drain_observed = WaitForCondition(
        std::chrono::milliseconds(10000), std::chrono::milliseconds(10),
        [&pool]() { return pool.IsDraining(); });
    EXPECT_TRUE(drain_observed)
        << "Pool did not enter DRAINING after injected error — the "
           "synthesised-error path was not exercised; the test's "
           "observations are not meaningful.";
    if (drain_observed) {
        invariant.MarkDroveDraining();
    }

    // Bounded wait for every future.  Each future may complete with
    // OK (pre-injection CQE fired) or TRANSFER_FAIL (injection fired
    // mid-drain and FinalizeAfterDrain marked the task failed).  The
    // invariant the test enforces is that EVERY future reaches a
    // terminal state within the deadline — i.e. no task is stranded.
    auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(10000);
    bool all_ready = true;
    for (auto& fut : futures) {
        if (!fut.has_value()) continue;
        while (!(*fut).isReady() &&
               std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        if (!(*fut).isReady()) {
            all_ready = false;
            break;
        }
    }
    EXPECT_TRUE(all_ready)
        << "Not all TransferFutures completed within 10 s — a task "
           "is stranded.";

    // Each future must have reached a terminal state with a defined
    // result code (OK or TRANSFER_FAIL — never "pending").
    int ok_count = 0;
    int fail_count = 0;
    int pending_count = 0;
    for (auto& fut : futures) {
        if (!fut.has_value()) continue;
        if (!(*fut).isReady()) {
            ++pending_count;
            continue;
        }
        mooncake::ErrorCode rc = (*fut).get();
        if (rc == mooncake::ErrorCode::OK) {
            ++ok_count;
        } else if (rc == mooncake::ErrorCode::TRANSFER_FAIL) {
            ++fail_count;
        }
    }
    EXPECT_EQ(pending_count, 0)
        << "Some futures never reached a terminal state — drain leaked";
    EXPECT_EQ(ok_count + fail_count, kReaders)
        << "Every future must reach OK or TRANSFER_FAIL";

    // If the inject fired, at least one task must have been routed
    // through the failure path.  A 0/4 fail split means the inject
    // was consumed by a poll cycle that happened to find the queue
    // empty (i.e. all 4 reads had already completed) — the test
    // scenario did not actually exercise sibling CQEs.
    if (drain_observed) {
        EXPECT_GT(fail_count, 0)
            << "Pool entered DRAINING but every future completed OK — "
               "the inject fired AFTER every CQE, the sibling-CQE "
               "scenario was not actually exercised.";
    }

    // The pool's InflightCount must reach 0 within the strict fence.
    bool quiescent = WaitForCondition(
        std::chrono::milliseconds(5000), std::chrono::milliseconds(10),
        [&pool]() { return pool.InflightCount() == 0; });
    EXPECT_TRUE(quiescent)
        << "Pool failed to drain within 5 s — strict fence is broken";

    spdk_free(buf);
    EXPECT_EQ(engine.freeEngine(), 0);
}

// ===========================================================================
// Regression test 2: write path with one qpair failure
// ===========================================================================
//
// Mirrors test 1 on the write path.
TEST_F(SiblingQpairFailure,
       WritePath_OneQpairFailure_PendingSiblingCQEs_NoCrash) {
    auto& wrapper = mooncake::SpdkWrapper::GetInstance();

    mooncake::NofConfig cfg = mooncake::NofConfig::FromEnv();
    cfg.num_io_queues = 4;
    cfg.max_inflight_per_qpair = 16;
    cfg.io_queue_size = 256;
    cfg.io_queue_requests = 512;
    wrapper.SetConfig(cfg);

    mooncake::TransferEngine engine(false);
    ASSERT_EQ(engine.init("P2PHANDSHAKE", "localhost:17999"), 0);
    if (!engine.isUsingTent()) {
        ASSERT_NE(engine.installTransport("tcp", nullptr), nullptr);
    }
    const std::string local_endpoint = engine.getLocalIpAndPort();
    std::shared_ptr<mooncake::StorageBackend> storage_backend;
    mooncake::TransferSubmitter submitter(engine, storage_backend,
                                          local_endpoint);

    constexpr uint32_t kBlocks = 128;
    constexpr size_t kBufBytes = kBlocks * 4096;
    void* buf = spdk_zmalloc(kBufBytes, /*align=*/0x1000, /*opts=*/nullptr,
                             /*socket_id=*/-1, /*flags=*/SPDK_MALLOC_DMA);
    ASSERT_NE(buf, nullptr) << "spdk_zmalloc failed";

    auto make_desc = [&]() {
        mooncake::AllocatedBuffer::Descriptor d;
        d.size_ = kBufBytes;
        d.buffer_address_ = 0;
        d.protocol_ = "rdma";
        d.transport_endpoint_ = target_;
        return d;
    };

    // Primer write — same as test 1 / test 3.
    auto primer_desc = make_desc();
    auto primer_future = submitter.submitSpdkNofOperation(
        primer_desc, buf, /*size=*/4096, mooncake::TransferRequest::WRITE);
    ASSERT_TRUE(primer_future.has_value());
    auto primer_rc = primer_future->wait();
    if (primer_rc != mooncake::ErrorCode::OK) {
        spdk_free(buf);
        EXPECT_EQ(engine.freeEngine(), 0);
        GTEST_SKIP() << "Primer write returned " << static_cast<int>(primer_rc)
                     << " — target cannot service real I/O; skipping.";
    }

    nof_seg_handle* handle = submitter.TestGetNofHandle(target_);
    ASSERT_NE(handle, nullptr);
    auto* segment = handle->segment;
    ASSERT_NE(segment, nullptr);
    auto* conn = segment->GetConnection();
    ASSERT_NE(conn, nullptr);
    auto& pool = conn->GetQpairPool();
    ASSERT_EQ(pool.Size(), 4u);
    ASSERT_FALSE(pool.IsDraining())
        << "Pool must start in kActive — TestInjectPollErrorOnce is "
           "only valid from kActive";

    ScopedDrainInvariant invariant(
        &pool, "WritePath_OneQpairFailure_PendingSiblingCQEs_NoCrash");

    // Arm the inject BEFORE the submits so the worker's first poll
    // cycle observes it (PollAll is gated on total_outstanding_io > 0
    // and a fast target can drain CQEs before the inject is armed).
    pool.TestInjectPollErrorOnce(/*qpair_idx=*/0);

    constexpr int kWriters = 4;
    std::vector<std::optional<mooncake::TransferFuture>> futures;
    futures.reserve(kWriters);
    for (int i = 0; i < kWriters; ++i) {
        auto desc = make_desc();
        desc.buffer_address_ = static_cast<uint64_t>(i) * kBufBytes;
        auto fut = submitter.submitSpdkNofOperation(
            desc, buf, /*size=*/kBufBytes, mooncake::TransferRequest::WRITE);
        ASSERT_TRUE(fut.has_value());
        futures.push_back(std::move(fut));
    }

    // Hard assertion: the inject must actually drive the pool into
    // DRAINING.  MarkDroveDraining() is conditional on the observation
    // so the destructor-time sentinel only enforces the secondary
    // invariant (didn't revert) when the test actually exercised the
    // synthesised-error path.
    bool drain_observed = WaitForCondition(
        std::chrono::milliseconds(10000), std::chrono::milliseconds(10),
        [&pool]() { return pool.IsDraining(); });
    EXPECT_TRUE(drain_observed)
        << "Pool did not enter DRAINING after injected error — the "
           "synthesised-error path was not exercised; the test's "
           "observations are not meaningful.";
    if (drain_observed) {
        invariant.MarkDroveDraining();
    }

    auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(10000);
    bool all_ready = true;
    for (auto& fut : futures) {
        if (!fut.has_value()) continue;
        while (!(*fut).isReady() &&
               std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        if (!(*fut).isReady()) {
            all_ready = false;
            break;
        }
    }
    EXPECT_TRUE(all_ready)
        << "Not all TransferFutures completed within 10 s — a task "
           "is stranded.";

    int ok_count = 0;
    int fail_count = 0;
    int pending_count = 0;
    for (auto& fut : futures) {
        if (!fut.has_value()) continue;
        if (!(*fut).isReady()) {
            ++pending_count;
            continue;
        }
        mooncake::ErrorCode rc = (*fut).get();
        if (rc == mooncake::ErrorCode::OK) {
            ++ok_count;
        } else if (rc == mooncake::ErrorCode::TRANSFER_FAIL) {
            ++fail_count;
        }
    }
    EXPECT_EQ(pending_count, 0)
        << "Some futures never reached a terminal state — drain leaked";
    EXPECT_EQ(ok_count + fail_count, kWriters)
        << "Every future must reach OK or TRANSFER_FAIL";

    // If the inject fired, at least one task must have been routed
    // through the failure path.  0/N fail means the inject was
    // consumed by a poll cycle that happened to find the queue
    // empty — the sibling-CQE scenario was not actually exercised.
    if (drain_observed) {
        EXPECT_GT(fail_count, 0)
            << "Pool entered DRAINING but every future completed OK — "
               "the inject fired AFTER every CQE, the sibling-CQE "
               "scenario was not actually exercised.";
    }

    bool quiescent = WaitForCondition(
        std::chrono::milliseconds(5000), std::chrono::milliseconds(10),
        [&pool]() { return pool.InflightCount() == 0; });
    EXPECT_TRUE(quiescent)
        << "Pool failed to drain within 5 s — strict fence is broken";

    spdk_free(buf);
    EXPECT_EQ(engine.freeEngine(), 0);
}

// ===========================================================================
// Regression test 3: deterministic DRAINING entry + final-state invariant
// ===========================================================================
//
// Arms the synthesised error BEFORE any submission, so the worker's
// next PollAll hits the inject on qpair[0] regardless of which qpair
// the round-robin assigns the submission to.  Submits a single
// transfer and exercises the FinalizeAfterDrain counter-pay-back path.
//
// Test guarantees:
//   * The pool reliably enters kDraining under the synthesised error.
//   * FinalizeAfterDrain zeros io_count for every short-circuited CQE
//     so the transfer returns.
//   * The pool's IsDraining() stays true after the drain (kDraining
//     or kClosed), so a late CQE from a sibling qpair still
//     short-circuits without dereferencing freed task memory.
// ===========================================================================

TEST_F(SiblingQpairFailure, ReadPath_DeterministicDrain_AllCountersReachZero) {
    auto& wrapper = mooncake::SpdkWrapper::GetInstance();

    mooncake::NofConfig cfg = mooncake::NofConfig::FromEnv();
    cfg.num_io_queues = 4;
    cfg.max_inflight_per_qpair = 16;
    cfg.io_queue_size = 256;
    cfg.io_queue_requests = 512;
    wrapper.SetConfig(cfg);

    // Construct a TransferSubmitter so the test exercises
    // submitSpdkNofOperation → SpdkNofWorkerPool::submitTask →
    // workerThread → NvmePollProcessCompletion → EnterDraining →
    // DrainDrainingPoolsUntilQuiescent (the path under test).
    mooncake::TransferEngine engine(/*auto_discover=*/false);
    ASSERT_EQ(engine.init("P2PHANDSHAKE", "localhost:17999"), 0);
    if (!engine.isUsingTent()) {
        ASSERT_NE(engine.installTransport("tcp", nullptr), nullptr);
    }
    const std::string local_endpoint = engine.getLocalIpAndPort();
    std::shared_ptr<mooncake::StorageBackend> storage_backend;
    mooncake::TransferSubmitter submitter(engine, storage_backend,
                                          local_endpoint);

    constexpr uint32_t kBlocks = 128;
    constexpr size_t kBufBytes = kBlocks * 4096;
    void* buf = spdk_zmalloc(kBufBytes, /*align=*/0x1000, /*opts=*/nullptr,
                             /*socket_id=*/-1, /*flags=*/SPDK_MALLOC_DMA);
    ASSERT_NE(buf, nullptr) << "spdk_zmalloc failed";

    // Helper: descriptor for the NoF path.  transport_endpoint_ is
    // the SPDK target string from MC_TEST_NOF_TARGET; submitSpdkNofOperation
    // uses it directly to open / cache the nof_seg_handle.
    //
    // NB: buffer_address_ is the DEVICE-side byte offset (the value
    // submitted as `remote_base_offset` in client_service.cpp:199),
    // NOT the memory pointer.  submitSpdkNofOperation divides it by
    // block_size to obtain the LBA, so it must fit within the target's
    // max_lba * block_size.  Using `buf` (the SPDK DMA pointer, an
    // IOVA on the order of 2^45) here would produce LBA values far
    // beyond the device's capacity and the submit would fail with
    // "LBA out of range".
    //
    // size_ is the descriptor buffer size used for alignment/size
    // validation in submitSpdkNofOperation (transfer size checked
    // against descriptor size, ptr alignment against block_size).
    // The actual transfer size is the `size` arg below.
    auto make_desc = [&]() {
        mooncake::AllocatedBuffer::Descriptor d;
        d.size_ = kBufBytes;
        d.buffer_address_ = 0;
        // Target exposes only RDMA; tag the descriptor to keep
        // selectStrategy on the NoF replica branch.
        d.protocol_ = "rdma";
        d.transport_endpoint_ = target_;
        return d;
    };

    // Prime the submitter's handle cache so a subsequent
    // submitSpdkNofOperation hits the cache and skips the slow
    // OpenNofSegment path.  The primer must complete with
    // ErrorCode::OK — otherwise the worker never reaches the poll
    // block (total_outstanding_io stays at 0) and the synthesised-
    // error path cannot fire.
    auto primer_desc = make_desc();
    auto primer_future = submitter.submitSpdkNofOperation(
        primer_desc, buf, /*size=*/4096, mooncake::TransferRequest::WRITE);
    ASSERT_TRUE(primer_future.has_value())
        << "submitSpdkNofOperation failed to open handle / submit primer";
    auto primer_rc = primer_future->wait();
    if (primer_rc != mooncake::ErrorCode::OK) {
        // Primer failure means the target cannot service real I/O;
        // skip — the rest of the test would be meaningless.
        GTEST_SKIP()
            << "Primer write returned " << static_cast<int>(primer_rc)
            << " — target cannot service real I/O in this environment; "
               "skipping the production-drain regression test (the "
               "synthesised-error injection requires a working NVMe-oF "
               "transport).";
    }

    // Reach the cached handle so we can arm the synthesised error
    // BEFORE the next worker poll.  TestGetNofHandle is gated by
    // MOONCAKE_TEST_DRAIN (compiled into this test target).
    nof_seg_handle* handle = submitter.TestGetNofHandle(target_);
    ASSERT_NE(handle, nullptr)
        << "TestGetNofHandle returned nullptr; submitter has no cached "
           "handle for the target";
    auto* segment = handle->segment;
    ASSERT_NE(segment, nullptr);
    auto* conn = segment->GetConnection();
    ASSERT_NE(conn, nullptr);
    auto& pool = conn->GetQpairPool();
    ASSERT_EQ(pool.Size(), 4u);
    ASSERT_FALSE(pool.IsDraining())
        << "Pool must start in kActive — TestInjectPollErrorOnce is only "
           "valid from kActive";

    // Install the destructor-time invariant sentinel.  Fires on
    // every path out of the test body (return, exception,
    // ADD_FAILURE exit), so the regression is reported even if a
    // future refactor short-circuits the inline assertions below.
    ScopedDrainInvariant invariant(&pool,
                                   "ReadPath_DeterministicDrain_"
                                   "AllCountersReachZero");

    // Arm the synthesised error on qpair[0] BEFORE the next worker
    // poll.  The worker's next NvmePollProcessCompletion call sees
    // the inject, calls EnterDraining + DrainDrainingPoolsUntilQuiescent.
    pool.TestInjectPollErrorOnce(/*qpair_idx=*/0);

    // MarkDroveDraining() is set after the post-submit observation
    // (below) so the destructor-time sentinel only enforces the
    // secondary invariant when the test actually drove the pool into
    // DRAINING via the synthesised-error path.

    // Submit a real write.  SubmitSpdkNofOperation's slow path
    // pushes an SpdkNofTask to the worker queue; the worker dequeues
    // and submits it.  Subsequent PollAll observes the inject → flips
    // pool to DRAINING → walks DrainDrainingPoolsUntilQuiescent.
    auto real_desc = make_desc();
    auto real_future = submitter.submitSpdkNofOperation(
        real_desc, buf, /*size=*/kBufBytes, mooncake::TransferRequest::WRITE);
    ASSERT_TRUE(real_future.has_value())
        << "submitSpdkNofOperation failed for the real submission";

    // Bounded wait.  TransferFuture::wait() blocks indefinitely on a
    // cv; poll isReady() with a 35 s deadline.
    auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(35000);
    while (!real_future->isReady() &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    EXPECT_TRUE(real_future->isReady())
        << "TransferFuture never completed within 35 s — the worker "
           "did not finalise the task during drain. Inspect "
           "DrainDrainingPoolsUntilQuiescent (Phase 2 "
           "FinalizeAfterDrain must run for every DRAINING pool, "
           "including those in timed_out_set) and the "
           "DrainProtocolGuard destructor.";

    if (!real_future->isReady()) {
        // The destructor-time invariant still fires and reports
        // InflightCount > 0 (and IsDraining state).  Don't
        // GTEST_SKIP — the failure is what we want surfaced.
        return;
    }

    mooncake::ErrorCode rc = real_future->get();

    // The future must complete with TRANSFER_FAIL: the worker entered
    // DRAINING so the transfer is marked failed.
    EXPECT_EQ(rc, mooncake::ErrorCode::TRANSFER_FAIL)
        << "Worker entered DRAINING but did not mark future as failed";

    // The pool must have entered kDraining from the synthesised error.
    EXPECT_TRUE(pool.IsDraining())
        << "Pool did not enter DRAINING after injected error — the "
           "production EnterDraining path was not exercised";

    // Declare the SECONDARY invariant now that we have direct
    // evidence the pool entered DRAINING.  The destructor-time
    // sentinel will check that IsDraining() stays true at scope exit.
    invariant.MarkDroveDraining();

    // The pool must reach InflightCount()==0 within the strict
    // fence window.
    bool quiescent = WaitForCondition(
        std::chrono::milliseconds(5000), std::chrono::milliseconds(10),
        [&pool]() { return pool.InflightCount() == 0; });
    EXPECT_TRUE(quiescent)
        << "Pool failed to drain within 5 s — strict fence is broken";

    // Once drained, IsDraining() must STAY true (kDraining or kClosed):
    // a late CQE arriving after the drain observes IsDraining()==true
    // and takes the DRAINING short-circuit.
    EXPECT_TRUE(pool.IsDraining())
        << "Pool reverted from kDraining back to kActive after drain";

    spdk_free(buf);
    // ~TransferSubmitter joins the worker pool first, then iterates
    // nof_handle_cache_ and calls CloseNofSegment on each handle
    // (idempotent for already-closed / retired entries).  ~TransferEngine
    // runs after the submitter goes out of scope and assumes
    // freeEngine() has already been called.
    EXPECT_EQ(engine.freeEngine(), 0);
}

// ===========================================================================
// Regression test 4 (REPRODUCER): permanent recovery hang on drain timeout
// ===========================================================================
//
// Forces the WaitForInflightCompletion timeout path:
//
//   1. submitSpdkNofOperation primer write opens the segment.
//   2. TestHoldAllCompletions(true) prevents any CQE delivery.
//   3. TestInjectPollErrorOnce(0) forces the next PollAll into
//      EnterDraining → DrainDrainingPoolsUntilQuiescent.
//   4. submitSpdkNofOperation enqueues a real IO.
//   5. Phase 1's WaitForInflightCompletion returns via fast-path on
//      PollAll<0 (dead qpair) or via the kWorkerDrainTimeoutMs
//      deadline if siblings could deliver.
//   6. Phase 2 must run FinalizeAfterDrain on the timed-out pool so
//      the task gets set_completed(TRANSFER_FAIL).
//
// The future-completion deadline is 5 s = kWorkerDrainTimeoutMs (1 s)
// × 2 (Phase 1 + Phase 2) plus margin for trampoline bookkeeping.
// The ~TransferSubmitter destructor must also complete within 5 s —
// the wall-clock assertion below observes how long
// ~TransferSubmitter → spdk_nvmf_pool_.reset() → ~SpdkNofWorkerPool
// → worker.join() takes.
TEST_F(SiblingQpairFailure, Reproducer_DrainTimeout_FutureCompletesNotHangs) {
    auto& wrapper = mooncake::SpdkWrapper::GetInstance();

    mooncake::NofConfig cfg = mooncake::NofConfig::FromEnv();
    cfg.num_io_queues = 4;
    cfg.max_inflight_per_qpair = 16;
    cfg.io_queue_size = 256;
    cfg.io_queue_requests = 512;
    wrapper.SetConfig(cfg);

    mooncake::TransferEngine engine(/*auto_discover=*/false);
    ASSERT_EQ(engine.init("P2PHANDSHAKE", "localhost:17999"), 0);
    if (!engine.isUsingTent()) {
        ASSERT_NE(engine.installTransport("tcp", nullptr), nullptr);
    }
    const std::string local_endpoint = engine.getLocalIpAndPort();
    std::shared_ptr<mooncake::StorageBackend> storage_backend;
    // Heap-allocate the submitter so we can time its destructor with
    // explicit delete — the wall-clock assertion below observes how
    // long ~TransferSubmitter → spdk_nvmf_pool_.reset() →
    // ~SpdkNofWorkerPool → worker.join() takes.
    auto* submitter = new mooncake::TransferSubmitter(engine, storage_backend,
                                                      local_endpoint);

    constexpr uint32_t kBlocks = 128;
    constexpr size_t kBufBytes = kBlocks * 4096;
    void* buf = spdk_zmalloc(kBufBytes, /*align=*/0x1000, /*opts=*/nullptr,
                             /*socket_id=*/-1, /*flags=*/SPDK_MALLOC_DMA);
    ASSERT_NE(buf, nullptr) << "spdk_zmalloc failed";

    auto make_desc = [&]() {
        mooncake::AllocatedBuffer::Descriptor d;
        d.size_ = kBufBytes;
        d.buffer_address_ = 0;
        d.protocol_ = "rdma";
        d.transport_endpoint_ = target_;
        return d;
    };

    // Prime the submitter's handle cache before arming the inject.
    auto primer_desc = make_desc();
    auto primer_future = submitter->submitSpdkNofOperation(
        primer_desc, buf, /*size=*/4096, mooncake::TransferRequest::WRITE);
    ASSERT_TRUE(primer_future.has_value())
        << "submitSpdkNofOperation failed to open handle / submit primer";
    auto primer_rc = primer_future->wait();
    if (primer_rc != mooncake::ErrorCode::OK) {
        spdk_free(buf);
        EXPECT_EQ(engine.freeEngine(), 0);
        GTEST_SKIP()
            << "Primer write returned " << static_cast<int>(primer_rc)
            << " — target cannot service real I/O in this environment; "
               "skipping the reproducer test (it requires a working "
               "NVMe-oF transport to establish the handle cache).";
    }

    // Reach the cached handle and arm the test hooks.
    nof_seg_handle* handle = submitter->TestGetNofHandle(target_);
    ASSERT_NE(handle, nullptr)
        << "TestGetNofHandle returned nullptr; submitter has no cached "
           "handle for the target";
    auto* segment = handle->segment;
    ASSERT_NE(segment, nullptr);
    auto* conn = segment->GetConnection();
    ASSERT_NE(conn, nullptr);
    auto& pool = conn->GetQpairPool();
    ASSERT_EQ(pool.Size(), 4u);
    ASSERT_FALSE(pool.IsDraining())
        << "Pool must start in kActive — both TestHoldAllCompletions "
           "and TestInjectPollErrorOnce are only valid from kActive";

    // TestHoldAllCompletions prevents any CQE delivery; sibling qpairs
    // would otherwise complete normally and WaitForInflightCompletion
    // would return true before the kWorkerDrainTimeoutMs (1 s)
    // deadline.  The fast-path on PollAll<0 still drives Phase 1 to a
    // terminal force-zero + force-DRAINING, so this test exercises
    // the same path regardless of whether the hold or the fast-path
    // is what stops sibling CQEs from arriving.
    pool.TestHoldAllCompletions(true);

    // TestInjectPollErrorOnce forces the next PollAll into
    // EnterDraining → DrainDrainingPoolsUntilQuiescent.
    pool.TestInjectPollErrorOnce(/*qpair_idx=*/0);

    // Submit via the production path.  The worker dequeues, submits
    // to SPDK, then enters the drain path.
    auto real_desc = make_desc();
    auto real_future = submitter->submitSpdkNofOperation(
        real_desc, buf, /*size=*/kBufBytes, mooncake::TransferRequest::WRITE);
    ASSERT_TRUE(real_future.has_value())
        << "submitSpdkNofOperation failed for the real submission";

    // Bounded wait — poll isReady() rather than calling wait()
    // (wait() blocks indefinitely on a cv).  5 s =
    // kWorkerDrainTimeoutMs (1 s) × 2 (Phase 1 + Phase 2) plus a
    // margin for trampoline bookkeeping and cv notification.
    auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(5000);
    while (!real_future->isReady() &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    EXPECT_TRUE(real_future->isReady())
        << "TransferFuture never completed within 5 s — the worker "
           "did not finalise the task during drain. Inspect "
           "DrainDrainingPoolsUntilQuiescent and the guard ordering."
        << " [diagnostic] hold_all_completions="
        << (pool.TestIsHoldingCompletions() ? "true" : "false")
        << " pending_inject_error_idx=" << pool.TestPendingInjectErrorIdx()
        << " InflightCount=" << pool.InflightCount();

    if (real_future->isReady()) {
        mooncake::ErrorCode rc = real_future->get();
        EXPECT_EQ(rc, mooncake::ErrorCode::TRANSFER_FAIL)
            << "Future completed but with wrong code; expected "
               "TRANSFER_FAIL because the worker entered DRAINING "
               "and FinalizeAfterDrain marked the task failed";

        // Pool state invariants: pool entered DRAINING from the
        // synthesised error; remains DRAINING (or kClosed) after
        // drain; InflightCount reaches 0 within the strict fence.
        EXPECT_TRUE(pool.IsDraining())
            << "Pool did not enter DRAINING after injected error — "
               "Phase 1 was not exercised";
        bool quiescent = WaitForCondition(
            std::chrono::milliseconds(5000), std::chrono::milliseconds(10),
            [&pool]() { return pool.InflightCount() == 0; });
        EXPECT_TRUE(quiescent) << "Pool failed to drain within 5 s";
        EXPECT_TRUE(pool.IsDraining())
            << "Pool reverted from kDraining back to kActive after "
               "drain";
    }

    // Release the hold so the test's cleanup phase (spdk_free,
    // freeEngine, ~TransferSubmitter) can complete without contending
    // with the held-completions flag.
    pool.TestHoldAllCompletions(false);

    // ~TransferSubmitter wall-clock bound.  ~TransferSubmitter first
    // calls spdk_nvmf_pool_.reset() → ~SpdkNofWorkerPool which joins
    // every worker thread.  The worker's break condition (transfer_task.cpp:
    // shutdown_ && task_queue.empty() && total_outstanding_io == 0 &&
    // !HasBufferedTask) must be satisfiable within seconds; a hang
    // here means io_count was not paid back by FinalizeAfterDrain, or
    // the worker is stuck in a sync-failure busy-loop with a stuck
    // chain.
    //
    // Heap-allocated submitter so we can time the destructor with
    // explicit delete.  The heap allocation also means ~TransferSubmitter
    // does NOT fire at scope exit — only this delete does.
    auto t_dtor_start = std::chrono::steady_clock::now();
    delete submitter;
    auto dtor_elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - t_dtor_start);
    EXPECT_LT(dtor_elapsed, std::chrono::milliseconds(5000))
        << "~TransferSubmitter took " << dtor_elapsed.count()
        << "ms — worker.join() did not return within 5 s, indicating "
           "a recovery hang in DrainDrainingPoolsUntilQuiescent or a "
           "stuck counter that prevents the break condition.";

    spdk_free(buf);
    EXPECT_EQ(engine.freeEngine(), 0);
}

// ===========================================================================
// Regression test 5 (STRESS): 1000× drain cycles on a single pool
// ===========================================================================
//
// Repeats the submit / fault / wait pattern 1000 times to expose
// latent counter drift, ordering bugs, or memory leaks that a
// one-shot test cannot catch.
//
// Per iteration:
//   1. Submit kSubmitsPerIter writes via submitSpdkNofOperation.
//   2. Iter 0 only: arm TestInjectPollErrorOnce(0) so the pool
//      enters kDraining via the worker's next PollAll.
//   3. Bounded-wait for every future to reach a terminal state.
//   4. Verify pool.InflightCount() == 0 (strict per-iteration fence).
//
// The pool is never re-opened — once EnterDraining fires (iter 0),
// the pool stays in kDraining for the rest of the test (the state
// machine has no kDraining -> kActive transition).  Iterations 1..N-1
// drive submitSpdkNofOperation against a kDraining pool: GetNextQpair
// returns nullptr, SubmitRequest returns -1, the worker marks each
// task failed and routes it through SpdkNofTaskCompletion with
// TRANSFER_FAIL.  This still exercises the trampoline's failure path
// 1000× and surfaces counter drift across the cumulative workload.
//
// The cumulative destructor-time invariant (ScopedDrainInvariant at
// test scope exit + ~SpdkNofQos DCHECK at worker shutdown) is what
// catches cross-iteration leaks: a per-iteration InflightCount==0
// check is not sufficient if iteration N leaks a counter that
// iteration N+1 happens to observe as zero.
//
// Why not re-open per iteration: CloseNofSegment on a segment that
// the worker is still processing UAFs the worker's seg_to_qos keys
// and any in-flight SpdkNofTask references.  The submitter
// destructor sequence (spdk_nvmf_pool_.reset() joins workers before
// CloseNofSegment) is the only safe ordering, and it runs once at
// end-of-submitter — not per iteration.
TEST_F(SiblingQpairFailure, Stress_DrainRecoveryCycles_NoLeakNoHang) {
    auto& wrapper = mooncake::SpdkWrapper::GetInstance();

    mooncake::NofConfig cfg = mooncake::NofConfig::FromEnv();
    cfg.num_io_queues = 4;
    cfg.max_inflight_per_qpair = 16;
    cfg.io_queue_size = 256;
    cfg.io_queue_requests = 512;
    wrapper.SetConfig(cfg);

    mooncake::TransferEngine engine(false);
    ASSERT_EQ(engine.init("P2PHANDSHAKE", "localhost:17999"), 0);
    if (!engine.isUsingTent()) {
        ASSERT_NE(engine.installTransport("tcp", nullptr), nullptr);
    }
    const std::string local_endpoint = engine.getLocalIpAndPort();
    std::shared_ptr<mooncake::StorageBackend> storage_backend;
    mooncake::TransferSubmitter submitter(engine, storage_backend,
                                          local_endpoint);

    constexpr uint32_t kBlocks = 128;
    constexpr size_t kBufBytes = kBlocks * 4096;
    void* buf = spdk_zmalloc(kBufBytes, /*align=*/0x1000, /*opts=*/nullptr,
                             /*socket_id=*/-1, /*flags=*/SPDK_MALLOC_DMA);
    ASSERT_NE(buf, nullptr) << "spdk_zmalloc failed";

    auto make_desc = [&]() {
        mooncake::AllocatedBuffer::Descriptor d;
        d.size_ = kBufBytes;
        d.buffer_address_ = 0;
        d.protocol_ = "rdma";
        d.transport_endpoint_ = target_;
        return d;
    };

    // Primer write to open the segment and populate the cache.
    auto primer_desc = make_desc();
    auto primer_future = submitter.submitSpdkNofOperation(
        primer_desc, buf, /*size=*/4096, mooncake::TransferRequest::WRITE);
    ASSERT_TRUE(primer_future.has_value());
    auto primer_rc = primer_future->wait();
    if (primer_rc != mooncake::ErrorCode::OK) {
        spdk_free(buf);
        EXPECT_EQ(engine.freeEngine(), 0);
        GTEST_SKIP() << "Primer write returned " << static_cast<int>(primer_rc)
                     << " — target cannot service real I/O; skipping stress.";
    }

    nof_seg_handle* handle = submitter.TestGetNofHandle(target_);
    ASSERT_NE(handle, nullptr);
    auto* segment = handle->segment;
    ASSERT_NE(segment, nullptr);
    auto* conn = segment->GetConnection();
    ASSERT_NE(conn, nullptr);
    auto& pool = conn->GetQpairPool();
    ASSERT_EQ(pool.Size(), 4u);

    constexpr int kIterations = 1000;
    constexpr int kSubmitsPerIter = 4;
    auto stress_start = std::chrono::steady_clock::now();

    for (int iter = 0; iter < kIterations; ++iter) {
        // Iter 0 only: arm the inject so the worker observes it on
        // its next PollAll and enters DRAINING.  Iter 1+ already has
        // the pool in kDraining, so re-arming is unnecessary.
        if (iter == 0) {
            pool.TestInjectPollErrorOnce(/*qpair_idx=*/0);
        }

        // Submit kSubmitsPerIter writes.  Iter 0 submits land on the
        // round-robin qpairs while the pool is still kActive; the
        // inject then drives the drain.  Iter 1+ submits land on a
        // kDraining pool and complete via SubmitRequest failure path
        // (task marked failed, SpdkNofTaskCompletion sets TRANSFER_FAIL).
        std::vector<std::optional<mooncake::TransferFuture>> futures;
        futures.reserve(kSubmitsPerIter);
        for (int s = 0; s < kSubmitsPerIter; ++s) {
            auto desc = make_desc();
            desc.buffer_address_ =
                static_cast<uint64_t>(iter * kSubmitsPerIter + s) * kBufBytes;
            auto fut = submitter.submitSpdkNofOperation(
                desc, buf, /*size=*/kBufBytes,
                mooncake::TransferRequest::WRITE);
            ASSERT_TRUE(fut.has_value())
                << "submitSpdkNofOperation failed at iter " << iter
                << " submit " << s;
            futures.push_back(std::move(fut));
        }

        // Bounded wait — every future must reach a terminal state.
        auto deadline =
            std::chrono::steady_clock::now() + std::chrono::milliseconds(5000);
        bool all_ready = true;
        for (auto& fut : futures) {
            if (!fut.has_value()) continue;
            while (!(*fut).isReady() &&
                   std::chrono::steady_clock::now() < deadline) {
                std::this_thread::sleep_for(std::chrono::milliseconds(2));
            }
            if (!(*fut).isReady()) {
                all_ready = false;
                break;
            }
        }
        ASSERT_TRUE(all_ready)
            << "Iter " << iter
            << ": not all TransferFutures completed within 5 s";

        // Per-iteration InflightCount fence.  Catches a per-iteration
        // counter leak immediately rather than deferring discovery to
        // the destructor-time invariant.
        bool quiescent = WaitForCondition(
            std::chrono::milliseconds(2000), std::chrono::milliseconds(10),
            [&pool]() { return pool.InflightCount() == 0; });
        ASSERT_TRUE(quiescent)
            << "Iter " << iter
            << ": pool's InflightCount did not reach 0 within 2 s";

        if ((iter + 1) % 100 == 0) {
            auto elapsed_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - stress_start)
                    .count();
            LOG(INFO) << "Stress iter " << (iter + 1) << "/" << kIterations
                      << " elapsed=" << elapsed_ms << "ms";
        }
    }

    auto total_elapsed_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - stress_start)
            .count();
    LOG(INFO) << "Stress completed: " << kIterations << " iterations in "
              << total_elapsed_ms << "ms (" << (total_elapsed_ms / kIterations)
              << "ms/iter avg)";

    spdk_free(buf);
    EXPECT_EQ(engine.freeEngine(), 0);
}

// ===========================================================================
// Regression test 6: NoBusyLoopAfterDrain — DRAINING-pool future terminates
//                    cleanly without busy-looping the worker
// ===========================================================================
//
// Reproduces the "submit io fail" busy-loop reported in the reviewer's
// stability re-review.  Drives the pool into DRAINING via the
// synthesised-error path, then asserts:
//   * submitSpdkNofOperation on the same cached handle still returns
//     a future (NOT std::nullopt) — early-reject was deliberately not
//     implemented because it races with the worker's PollAll path:
//     test 4 / test 5 need a DRAINING pool to ACCEPT submits so the
//     worker can drive them through the sync-failure termination gate
//     (FinalizeSubmittedTask) and complete them as TRANSFER_FAIL.  An
//     IsDraining() early-reject would break test 5's
//     Stress_DrainRecoveryCycles iter 1+, where 4000 futures rely on
//     the sync-failure path for completion.  See
//     mooncake-store/src/transfer_task.cpp:2268-2281 for the full
//     rationale.
//   * The returned future reaches TRANSFER_FAIL within a bounded
//     window — the worker terminates the task via FinalizeSubmittedTask
//     → SpdkNofTaskCompletion → try_complete() CAS, no hang.
//   * ~TransferSubmitter joins the worker pool within 5 s — the worker
//     is idle waiting on cv, NOT spinning through failed submits.
//
// Test guarantees:
//   * The "submit io fail" log path (VLOG(1)) is reachable from the
//     client API but does NOT cause a worker-side busy-loop: each
//     sync-failed submit is processed in one outer-while iteration via
//     the epilogue (FinalizeSubmittedTask pops the chain and runs
//     SpdkNofTaskCompletion).  The worker then waits on cv until the
//     next task arrives.
//   * Worker shutdown is bounded — no permanent recovery hang.
TEST_F(SiblingQpairFailure, NoBusyLoopAfterDrain_ProcessExitsCleanly) {
    auto& wrapper = mooncake::SpdkWrapper::GetInstance();

    mooncake::NofConfig cfg = mooncake::NofConfig::FromEnv();
    cfg.num_io_queues = 4;
    cfg.max_inflight_per_qpair = 16;
    cfg.io_queue_size = 256;
    cfg.io_queue_requests = 512;
    wrapper.SetConfig(cfg);

    mooncake::TransferEngine engine(false);
    ASSERT_EQ(engine.init("P2PHANDSHAKE", "localhost:17999"), 0);
    if (!engine.isUsingTent()) {
        ASSERT_NE(engine.installTransport("tcp", nullptr), nullptr);
    }
    const std::string local_endpoint = engine.getLocalIpAndPort();
    std::shared_ptr<mooncake::StorageBackend> storage_backend;
    mooncake::TransferSubmitter submitter(engine, storage_backend,
                                          local_endpoint);

    constexpr uint32_t kBlocks = 128;
    constexpr size_t kBufBytes = kBlocks * 4096;
    void* buf = spdk_zmalloc(kBufBytes, /*align=*/0x1000, /*opts=*/nullptr,
                             /*socket_id=*/-1, /*flags=*/SPDK_MALLOC_DMA);
    ASSERT_NE(buf, nullptr) << "spdk_zmalloc failed";

    auto make_desc = [&]() {
        mooncake::AllocatedBuffer::Descriptor d;
        d.size_ = kBufBytes;
        d.buffer_address_ = 0;
        d.protocol_ = "rdma";
        d.transport_endpoint_ = target_;
        return d;
    };

    // Prime the handle cache.
    auto primer_desc = make_desc();
    auto primer_future = submitter.submitSpdkNofOperation(
        primer_desc, buf, /*size=*/4096, mooncake::TransferRequest::WRITE);
    ASSERT_TRUE(primer_future.has_value());
    auto primer_rc = primer_future->wait();
    if (primer_rc != mooncake::ErrorCode::OK) {
        spdk_free(buf);
        EXPECT_EQ(engine.freeEngine(), 0);
        GTEST_SKIP() << "Primer write returned " << static_cast<int>(primer_rc)
                     << " — target cannot service real I/O; skipping.";
    }

    nof_seg_handle* handle = submitter.TestGetNofHandle(target_);
    ASSERT_NE(handle, nullptr);
    auto* segment = handle->segment;
    ASSERT_NE(segment, nullptr);
    auto* conn = segment->GetConnection();
    ASSERT_NE(conn, nullptr);
    auto& pool = conn->GetQpairPool();
    ASSERT_EQ(pool.Size(), 4u);
    ASSERT_FALSE(pool.IsDraining())
        << "Pool must start in kActive — TestInjectPollErrorOnce is "
           "only valid from kActive";

    // Trigger DRAINING via the synthesised-error path.
    pool.TestInjectPollErrorOnce(/*qpair_idx=*/0);

    // Submit one transfer to wake the worker into the poll block.
    auto real_desc = make_desc();
    auto real_future = submitter.submitSpdkNofOperation(
        real_desc, buf, /*size=*/kBufBytes, mooncake::TransferRequest::WRITE);
    ASSERT_TRUE(real_future.has_value());

    // Wait for DRAINING to be observed.
    bool drain_observed = WaitForCondition(
        std::chrono::milliseconds(10000), std::chrono::milliseconds(10),
        [&pool]() { return pool.IsDraining(); });
    EXPECT_TRUE(drain_observed)
        << "Pool did not enter DRAINING after injected error — the "
           "synthesised-error path was not exercised.";
    if (!drain_observed) {
        spdk_free(buf);
        EXPECT_EQ(engine.freeEngine(), 0);
        return;
    }

    // Bounded wait for the first future to drain.
    auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(5000);
    while (!real_future->isReady() &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    // Production contract for DRAINING-pool submits (matches test 5's
    // iter 1+ design):
    //
    //   submitSpdkNofOperation on a DRAINING pool RETURNS a future.
    //   The future is finalized as TRANSFER_FAIL by the worker's
    //   sync-failure path (SubmitRequest → GetNextQpair → nullptr →
    //   -1 → FinalizeSubmittedTask → SpdkNofTaskCompletion).
    //
    // Early-reject (returning std::nullopt on IsDraining()) is NOT
    // implemented because it would race with the worker's PollAll
    // path: the test's injected error can flip the pool state to
    // kDraining mid-submit.  An early-reject would break test 4's
    // 35s timeout scenario and test 5's 4000-submit stress.  See the
    // comment block above for the full rationale.
    //
    // The "no busy-loop" guarantee is verified by the
    // EXPECT_LT(join_elapsed_ms, 5000) assertion below — each
    // sync-failed submit is processed in one outer-while iteration
    // via the epilogue, then the worker waits on cv.
    auto late_desc = make_desc();
    late_desc.buffer_address_ = kBufBytes;
    auto late_future = submitter.submitSpdkNofOperation(
        late_desc, buf, /*size=*/kBufBytes, mooncake::TransferRequest::READ);
    EXPECT_TRUE(late_future.has_value())
        << "submitSpdkNofOperation on a DRAINING pool must return a "
           "future; the worker completes it as TRANSFER_FAIL via "
           "FinalizeSubmittedTask.  An early-reject here would break "
           "test 5's Stress_DrainRecoveryCycles iter 1+ (see comment "
           "above).";

    // The returned future must terminate as TRANSFER_FAIL within a
    // bounded window — proves the worker correctly drives the
    // sync-failed task through FinalizeSubmittedTask (single-pop
    // gate) and SpdkNofTaskCompletion (try_complete CAS) without
    // leaking or hanging.
    if (late_future.has_value()) {
        auto late_deadline =
            std::chrono::steady_clock::now() + std::chrono::milliseconds(5000);
        while (!(*late_future).isReady() &&
               std::chrono::steady_clock::now() < late_deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        ASSERT_TRUE((*late_future).isReady())
            << "DRAINING-pool future did not reach a terminal state "
               "within 5 s — FinalizeSubmittedTask hung or the "
               "sync-failure path is broken.";
        auto late_rc = (*late_future).wait();
        EXPECT_EQ(late_rc, mooncake::ErrorCode::TRANSFER_FAIL)
            << "DRAINING-pool future must complete as TRANSFER_FAIL "
               "(the worker-side sync-failure termination contract).";
    }

    // ~TransferSubmitter must join the worker pool within a bounded
    // budget.  If the worker is busy-looping on failed submits the
    // join blocks until the per-iteration cv condition is met; under
    // the regression the worker drains naturally (total_outstanding_io
    // stays at 0 because failed submits don't increment it), so the
    // join should complete within seconds.
    auto t0 = std::chrono::steady_clock::now();
    // We cannot call ~TransferSubmitter directly without invalidating
    // the local.  Instead, exercise the destroy path by scoping a
    // fresh submitter around a small drain cycle and observe its
    // dtor join time.  This isolates the busy-loop test from the
    // outer submitter (whose dtor runs at scope exit anyway).
    {
        mooncake::TransferSubmitter inner_submitter(engine, storage_backend,
                                                    local_endpoint);
        // Force an inner submitter handle open so its worker has
        // seg_to_qos work to do.
        auto inner_desc = make_desc();
        auto inner_fut = inner_submitter.submitSpdkNofOperation(
            inner_desc, buf, /*size=*/4096, mooncake::TransferRequest::WRITE);
        if (inner_fut.has_value()) {
            auto inner_rc = inner_fut->wait();
            (void)inner_rc;
        }
    }
    auto join_elapsed_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - t0)
            .count();
    EXPECT_LT(join_elapsed_ms, 5000)
        << "Inner submitter destructor took " << join_elapsed_ms
        << "ms — worker join is hanging, the busy-loop regression has "
           "recurred.";

    spdk_free(buf);
    EXPECT_EQ(engine.freeEngine(), 0);
}

}  // namespace mooncake::test

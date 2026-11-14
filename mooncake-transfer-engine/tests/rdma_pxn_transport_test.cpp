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

// Hardware-free unit tests for the PXN runtime integration with
// RdmaTransport, RdmaContext, and WorkerPool.
//
// This file is the home for host-side PXN tests that do not need an RDMA
// device, a GPU, or a running metadata server. New PXN unit tests should be
// added here (or alongside, following the same fixture pattern) rather than
// in the broader rdma_transport_test family, so the PXN surface stays easy
// to run on every CI runner.
//
// Coverage:
//   1. PxnRuntimeManager null-safety.
//      RdmaTransport owns a PxnRuntimeManager that is only constructed
//      inside install(). Code paths that run before install() (test
//      fixtures that build an RdmaContext or WorkerPool directly over a
//      bare transport, and RdmaContext::construct() when a device fails
//      to open) used to call through engine().PxnRuntimeMgr()
//      unconditionally, dereferencing an empty unique_ptr. Under
//      -DUSE_PXN=ON that was an unconditional segfault. The accessor now
//      returns a raw pointer and callers null-check it, so an uninstalled
//      transport is a well-defined no-op.
//   2. PxnRouteSlicesToShards shard-key dispatch.
//      Each CopyWorker shell must land on shard
//        (target_id * 10007 + peer_device_id + pxn_src_gpu_id) %
//        pxn_worker_thread_count
//      and the per-shard scratch inside the router must be thread_local so
//      concurrent initiators cannot clobber each other's buckets.

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <glog/logging.h>

#include "rdma_test_peers.h"
#include "transfer_metadata.h"
#include "transport/rdma_transport/rdma_context.h"
#include "transport/rdma_transport/rdma_transport.h"
#include "transport/rdma_transport/worker_pool.h"
#ifdef MOONCAKE_ENABLE_PXN
#include "transport/rdma_transport/pxn/pxn_object_cache.h"
#include "transport/rdma_transport/pxn/pxn_runtime_manager.h"
#include "transport/rdma_transport/pxn/pxn_types.h"
#include "transport/rdma_transport/pxn/pxn_worker_pool_state.h"
#endif

namespace mooncake {

namespace {

class RdmaPxnTransportTest : public ::testing::Test {
   protected:
    void SetUp() override {
        // Device-open failures below are expected and noisy; keep the gtest
        // output readable the same way worker_pool_rail_state_test does.
        previous_min_log_level_ = FLAGS_minloglevel;
        FLAGS_minloglevel = google::GLOG_FATAL;

        metadata_ = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
        transport_ = std::make_unique<RdmaTransport>();
        RdmaTransportTestPeer::bindMetadata(*transport_, metadata_,
                                            "pxn-disabled-runtime-test");
    }

    void TearDown() override {
        transport_.reset();
        FLAGS_minloglevel = previous_min_log_level_;
    }

    int previous_min_log_level_ = 0;
    std::shared_ptr<TransferMetadata> metadata_;
    std::unique_ptr<RdmaTransport> transport_;
};

// install() is what news PxnRuntimeManager. A bare transport (as used by
// several test fixtures) must therefore return a null manager pointer rather
// than crashing on the previous reference-returning accessor.
TEST_F(RdmaPxnTransportTest, UninstalledTransportHasNoPxnRuntime) {
#ifdef MOONCAKE_ENABLE_PXN
    EXPECT_EQ(transport_->PxnRuntimeMgr(), nullptr);
#else
    GTEST_SKIP() << "USE_PXN=OFF: PxnRuntimeMgr() is unavailable";
#endif
}

// RdmaContext::construct() is expected to fail here (device name "unused"
// does not exist on any host). The PXN block inside construct() must honor
// that failure path: with no runtime manager, no attempt is made to allocate
// per-context PXN resources, and construct() returns without touching a null
// manager.
TEST_F(RdmaPxnTransportTest, RdmaContextConstructSurvivesUninstalledEngine) {
    auto context = std::make_unique<RdmaContext>(*transport_, "unused");
    // Expected to fail - no such device - but must not crash.
    context->construct();
    SUCCEED();
}

// The pre-fix segfault manifested here: WorkerPool's constructor dereferenced
// engine().PxnRuntimeMgr() unconditionally. With the fix, pxn_state_ stays
// null when there is no runtime manager and the pool behaves like a USE_PXN=OFF
// build for the rest of its lifetime.
TEST_F(RdmaPxnTransportTest, WorkerPoolCtorSkipsPxnWhenRuntimeAbsent) {
    auto context = std::make_unique<RdmaContext>(*transport_, "unused");
    context->construct();
    auto pool = std::make_unique<WorkerPool>(*context);
#ifdef MOONCAKE_ENABLE_PXN
    EXPECT_EQ(pool->getPxnState(), nullptr);
#endif
    // Reset in dependency order so no worker outlives its context.
    pool.reset();
    context.reset();
    SUCCEED();
}

}  // namespace

#ifdef MOONCAKE_ENABLE_PXN

namespace {

// Builds a PxnRuntimeManager + manually-initialized PxnWorkerPoolState pair
// so PxnRouteSlicesToShards can be exercised without an RDMA device or a
// real RdmaContext. The topology-manager build inside the manager ctor is
// harmless when the HCA names do not resolve to real devices; the routing
// path reached by these tests never touches the manager's device state.
class RdmaPxnRouteSlicesTest : public ::testing::Test {
   protected:
    static constexpr int kWorkerCount = 8;

    void SetUp() override {
        previous_min_log_level_ = FLAGS_minloglevel;
        FLAGS_minloglevel = google::GLOG_FATAL;

        pxn_runtime_ = std::make_unique<pxn::PxnRuntimeManager>(
            std::vector<std::string>{"mlx5_0", "mlx5_1"},
            /*IbRelaxOrderEn=*/false);

        state_ = std::make_unique<pxn::PxnWorkerPoolState>(/*numa=*/-1);
        state_->pxn_worker_thread_count = kWorkerCount;
        state_->pxn_slice_queue =
            std::vector<std::vector<void *>>(kWorkerCount);
        state_->pxn_slice_queue_count =
            std::vector<std::atomic<uint64_t>>(kWorkerCount);
        state_->pxn_slice_queue_lock = std::vector<TicketLock>(kWorkerCount);
        for (int i = 0; i < kWorkerCount; ++i) {
            state_->pxn_slice_queue_count[i].store(0,
                                                   std::memory_order_relaxed);
        }
    }

    void TearDown() override {
        // ~PxnWorkerPoolState reclaims each shell's PxnCopyWorker back to
        // the global cache, then clears pxn_slice_queue. Nothing else to do.
        state_.reset();
        pxn_runtime_.reset();
        for (auto *shell : owned_shells_) delete shell;
        owned_shells_.clear();
        FLAGS_minloglevel = previous_min_log_level_;
    }

    // Allocates a CopyWorker shell with the routing fields the shard key
    // depends on. The shell owns the CW indirectly: on teardown
    // ~PxnWorkerPoolState walks pxn_slice_queue and returns each CW to the
    // cache via PxnReclaimCwFromShell.
    Transport::Slice *makeShell(uint64_t target_id, int peer_device_id,
                                int src_gpu_id) {
        auto *cw = pxn::PxnGetCopyWorkerCache().PxnAllocate();
        cw->peer_device_id = peer_device_id;
        cw->pxn_src_gpu_id = src_gpu_id;
        cw->pxn_rdma_workers_cnt = 1;

        auto *shell = new Transport::Slice();
        shell->target_id = target_id;
        shell->pxn_info.is_copy_worker = true;
        shell->pxn_info.pxn_copy_worker = cw;
        owned_shells_.push_back(shell);
        return shell;
    }

    static size_t expectedShard(uint64_t target_id, int peer_device_id,
                                int src_gpu_id) {
        return (static_cast<size_t>(target_id) * 10007 +
                static_cast<size_t>(peer_device_id) +
                static_cast<size_t>(src_gpu_id)) %
               static_cast<size_t>(kWorkerCount);
    }

    int previous_min_log_level_ = 0;
    std::unique_ptr<pxn::PxnRuntimeManager> pxn_runtime_;
    std::unique_ptr<pxn::PxnWorkerPoolState> state_;
    std::vector<Transport::Slice *> owned_shells_;
};

// A single shell lands on exactly the shard predicted by the public
// shard-key formula and no other queue is touched.
TEST_F(RdmaPxnRouteSlicesTest, SingleSliceLandsOnDeterministicShard) {
    const uint64_t target = 1;
    const int peer = 2;
    const int gpu = 3;

    auto *shell = makeShell(target, peer, gpu);
    std::vector<Transport::Slice *> slices{shell};
    uint64_t submitted = 0;

    ASSERT_EQ(
        pxn_runtime_->PxnRouteSlicesToShards(state_.get(), slices, submitted),
        0);

    const size_t expected = expectedShard(target, peer, gpu);
    for (int i = 0; i < kWorkerCount; ++i) {
        if (i == static_cast<int>(expected)) {
            EXPECT_EQ(state_->pxn_slice_queue[i].size(), 1u);
            EXPECT_EQ(state_->pxn_slice_queue[i].front(), shell);
            EXPECT_EQ(state_->pxn_slice_queue_count[i].load(
                          std::memory_order_relaxed),
                      1u);
        } else {
            EXPECT_TRUE(state_->pxn_slice_queue[i].empty());
        }
    }
    EXPECT_EQ(submitted, 1u);
}

// Many unique keys fan out: every shell is placed on its predicted shard
// and nothing is dropped or duplicated.
TEST_F(RdmaPxnRouteSlicesTest, DistinctKeysLandOnPredictedShards) {
    std::vector<Transport::Slice *> slices;
    const int kNumSlices = 64;
    for (int i = 0; i < kNumSlices; ++i) {
        slices.push_back(makeShell(/*target=*/i, /*peer=*/i % 4,
                                   /*gpu=*/i % 8));
    }
    uint64_t submitted = 0;

    ASSERT_EQ(
        pxn_runtime_->PxnRouteSlicesToShards(state_.get(), slices, submitted),
        0);

    uint64_t total = 0;
    for (int i = 0; i < kWorkerCount; ++i) {
        total += state_->pxn_slice_queue[i].size();
        EXPECT_EQ(
            state_->pxn_slice_queue[i].size(),
            state_->pxn_slice_queue_count[i].load(std::memory_order_relaxed));
    }
    EXPECT_EQ(total, static_cast<uint64_t>(kNumSlices));
    EXPECT_EQ(submitted, static_cast<uint64_t>(kNumSlices));

    for (int i = 0; i < kNumSlices; ++i) {
        const size_t shard = expectedShard(i, i % 4, i % 8);
        const auto &q = state_->pxn_slice_queue[shard];
        EXPECT_NE(std::find(q.begin(), q.end(), slices[i]), q.end())
            << "shell " << i << " missing from shard " << shard;
    }
}

// Identical keys collapse onto the same shard; the function neither hashes
// pointer identity nor otherwise spreads a repeated key.
TEST_F(RdmaPxnRouteSlicesTest, IdenticalKeysCollapseOntoSameShard) {
    std::vector<Transport::Slice *> slices;
    const int kRepeats = 10;
    for (int i = 0; i < kRepeats; ++i) {
        slices.push_back(makeShell(/*target=*/42, /*peer=*/5, /*gpu=*/6));
    }
    uint64_t submitted = 0;

    ASSERT_EQ(
        pxn_runtime_->PxnRouteSlicesToShards(state_.get(), slices, submitted),
        0);

    const size_t expected = expectedShard(42, 5, 6);
    for (int i = 0; i < kWorkerCount; ++i) {
        if (i == static_cast<int>(expected)) {
            EXPECT_EQ(state_->pxn_slice_queue[i].size(),
                      static_cast<size_t>(kRepeats));
        } else {
            EXPECT_TRUE(state_->pxn_slice_queue[i].empty());
        }
    }
    EXPECT_EQ(submitted, static_cast<uint64_t>(kRepeats));
}

// Concurrent initiators share the state but must not share the per-shard
// scratch buffer: it is a thread_local inside PxnRouteSlicesToShards. If
// that regressed to a plain state field (an earlier revision did), one
// thread's bucket would overwrite the other's and the total count would
// drop below kNumSlicesPerThread * 2.
TEST_F(RdmaPxnRouteSlicesTest, ThreadLocalScratchHoldsAcrossInitiators) {
    constexpr int kNumSlicesPerThread = 128;
    std::vector<Transport::Slice *> slices_a;
    std::vector<Transport::Slice *> slices_b;
    for (int i = 0; i < kNumSlicesPerThread; ++i) {
        slices_a.push_back(makeShell(/*target=*/i, /*peer=*/0, /*gpu=*/0));
        slices_b.push_back(
            makeShell(/*target=*/i + 10'000, /*peer=*/1, /*gpu=*/1));
    }

    uint64_t submitted_a = 0;
    uint64_t submitted_b = 0;
    std::atomic<int> ready{0};

    auto route = [&](const std::vector<Transport::Slice *> &slices,
                     uint64_t &submitted) {
        ready.fetch_add(1, std::memory_order_acq_rel);
        while (ready.load(std::memory_order_acquire) < 2) {
            // Spin: start both initiators as close together as possible so
            // the scratch-reuse hazard, if it reappears, is observable.
        }
        EXPECT_EQ(pxn_runtime_->PxnRouteSlicesToShards(state_.get(), slices,
                                                       submitted),
                  0);
    };

    std::thread ta(route, std::cref(slices_a), std::ref(submitted_a));
    std::thread tb(route, std::cref(slices_b), std::ref(submitted_b));
    ta.join();
    tb.join();

    EXPECT_EQ(submitted_a, static_cast<uint64_t>(kNumSlicesPerThread));
    EXPECT_EQ(submitted_b, static_cast<uint64_t>(kNumSlicesPerThread));

    uint64_t total = 0;
    for (int i = 0; i < kWorkerCount; ++i) {
        total += state_->pxn_slice_queue[i].size();
    }
    EXPECT_EQ(total, static_cast<uint64_t>(kNumSlicesPerThread) * 2);
}

}  // namespace

#endif  // MOONCAKE_ENABLE_PXN

}  // namespace mooncake

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

// Rail-state regression tests for issue #3299. A local completion fault
// (mlx5 "local length error" and friends) retires the endpoint and hands the
// slice to another local RNIC, which rebuilds its own endpoint to the same
// peer NIC. Nothing used to bound that teardown/rebuild cycle, so a recurring
// fault turned into a reconnect storm. markRailFailed() now counts those
// faults, and kRailErrorThreshold consecutive ones pause the path.
//
// The rail monitor is plain per-worker-pool state, so these tests need no RDMA
// device: the pool is built over a context whose device never opens.

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>

#include <glog/logging.h>

#include "transport/rdma_transport/rdma_context.h"
#include "transport/rdma_transport/rdma_transport.h"
#include "transport/rdma_transport/worker_pool.h"

#if defined(__has_feature)
#define MC_HAS_FEATURE(x) __has_feature(x)
#else
#define MC_HAS_FEATURE(x) 0
#endif
#if defined(__SANITIZE_ADDRESS__) || MC_HAS_FEATURE(address_sanitizer) || \
    MC_HAS_FEATURE(leak_sanitizer)
#include <sanitizer/lsan_interface.h>
#define MC_LSAN_IGNORE_OBJECT(p) __lsan_ignore_object(p)

// Suppress false positives from libnuma.so's process-wide static cache
// allocated in numa_node_to_cpus() which is intentionally retained until exit.
extern "C" __attribute__((weak, visibility("default"))) const char *
__lsan_default_suppressions() {
    return "leak:libnuma.so\n";
}
#else
#define MC_LSAN_IGNORE_OBJECT(p) ((void)(p))
#endif

using namespace mooncake;

namespace mooncake {

class WorkerPoolTestPeer {
   public:
    static void markRailFailed(WorkerPool &pool, const std::string &path,
                               bool immediate_pause) {
        pool.markRailFailed(path, immediate_pause);
    }

    static bool isRailAvailable(WorkerPool &pool, const std::string &path) {
        return pool.isRailAvailable(path);
    }

    // Moves the recorded error timestamp back so the decay window can be
    // exercised without sleeping through it.
    static void ageLastError(WorkerPool &pool, const std::string &path,
                             uint64_t age_ns) {
        std::lock_guard<std::mutex> lock(pool.rail_state_lock_);
        pool.rail_states_[path].last_error_ns -= age_ns;
    }

    static int errorThreshold() { return WorkerPool::kRailErrorThreshold; }

    static uint64_t errorWindowNs() { return WorkerPool::kRailErrorWindowNs; }
};

}  // namespace mooncake

namespace {

constexpr const char *kPeerA = "10.0.0.1@mlx5_bond_0";
constexpr const char *kPeerB = "10.0.0.1@mlx5_bond_1";

class WorkerPoolRailStateTest : public ::testing::Test {
   protected:
    void SetUp() override {
        // monitorWorker busy-polls epoll_wait() on an invalid fd for the whole
        // lifetime of this pool because the context was never opened. Silence
        // it so gtest failures stay readable.
        previous_min_log_level_ = FLAGS_minloglevel;
        FLAGS_minloglevel = google::GLOG_FATAL;

        // Intentional leak: ~RdmaTransport dereferences metadata_, which is
        // null until install(). We only need it as RdmaContext's owner, same
        // as rdma_endpoint_state_test.
        transport_ = new RdmaTransport();
        MC_LSAN_IGNORE_OBJECT(transport_);
        context_ = std::make_unique<RdmaContext>(*transport_, "unused");
        // Always fails, on any host, because no device is named "unused". It
        // still creates the endpoint store, which monitorWorker's reclaim tick
        // needs to exist.
        context_->construct();
        worker_pool_ = std::make_unique<WorkerPool>(*context_);
    }

    void TearDown() override {
        worker_pool_.reset();
        context_.reset();
        FLAGS_minloglevel = previous_min_log_level_;
    }

    void failRail(const std::string &path, bool immediate_pause = false) {
        WorkerPoolTestPeer::markRailFailed(*worker_pool_, path,
                                           immediate_pause);
    }

    bool railAvailable(const std::string &path) {
        return WorkerPoolTestPeer::isRailAvailable(*worker_pool_, path);
    }

    int previous_min_log_level_ = 0;
    RdmaTransport *transport_ = nullptr;
    std::unique_ptr<RdmaContext> context_;
    std::unique_ptr<WorkerPool> worker_pool_;
};

TEST_F(WorkerPoolRailStateTest, UnknownRailIsAvailable) {
    EXPECT_TRUE(railAvailable(kPeerA));
}

// A single local fault must stay free: the endpoint is rebuilt and the slice
// retried, exactly as before.
TEST_F(WorkerPoolRailStateTest, FailuresBelowThresholdDoNotPauseRail) {
    for (int i = 0; i < WorkerPoolTestPeer::errorThreshold() - 1; ++i) {
        failRail(kPeerA);
        EXPECT_TRUE(railAvailable(kPeerA))
            << "paused after " << (i + 1) << " error(s)";
    }
}

// The regression: without this, a recurring local fault re-handshakes the same
// peer NIC forever because nothing on the local path ever pauses it.
TEST_F(WorkerPoolRailStateTest, RepeatedFailuresPauseRail) {
    for (int i = 0; i < WorkerPoolTestPeer::errorThreshold(); ++i)
        failRail(kPeerA);

    EXPECT_FALSE(railAvailable(kPeerA));
}

// Remote-path handling is unchanged: one error with immediate_pause set still
// pauses the rail right away.
TEST_F(WorkerPoolRailStateTest, ImmediatePauseStillPausesOnFirstError) {
    failRail(kPeerA, /*immediate_pause=*/true);

    EXPECT_FALSE(railAvailable(kPeerA));
}

// Errors spread further apart than the window are not consecutive, so a
// healthy rail cannot accumulate its way to a pause over a long process life.
TEST_F(WorkerPoolRailStateTest, StaleErrorsDoNotAccumulate) {
    const int threshold = WorkerPoolTestPeer::errorThreshold();
    for (int round = 0; round < 3; ++round) {
        for (int i = 0; i < threshold - 1; ++i) failRail(kPeerA);
        ASSERT_TRUE(railAvailable(kPeerA));
        WorkerPoolTestPeer::ageLastError(
            *worker_pool_, kPeerA, WorkerPoolTestPeer::errorWindowNs() + 1);
    }

    for (int i = 0; i < threshold - 1; ++i) failRail(kPeerA);
    EXPECT_TRUE(railAvailable(kPeerA));
}

// Pausing one peer RNIC must not take the peer's other RNIC with it, otherwise
// a single bad path would strand every transfer to that server.
TEST_F(WorkerPoolRailStateTest, RailsArePausedIndependently) {
    for (int i = 0; i < WorkerPoolTestPeer::errorThreshold(); ++i)
        failRail(kPeerA);

    EXPECT_FALSE(railAvailable(kPeerA));
    EXPECT_TRUE(railAvailable(kPeerB));
}

}  // namespace

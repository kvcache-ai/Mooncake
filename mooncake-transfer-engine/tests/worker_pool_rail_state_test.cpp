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
// The rail monitor and local-WC context breaker are plain per-worker-pool
// state, so these tests need no RDMA device: the pool is built over a context
// whose device never opens.

#include <gtest/gtest.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>

#include <glog/logging.h>

#include "rdma_test_peers.h"
#include "transfer_metadata_plugin.h"
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

#ifdef __linux__
namespace {
std::function<GidRefreshResult()> gid_probe;
}

// Wrap the device probe, leaving the actual WorkerPool recovery path intact.
extern "C" GidRefreshResult
wrapRefreshCurrentGid(RdmaContext *, std::string *, std::string *) asm(
    "__wrap__ZN8mooncake11RdmaContext17refreshCurrentGidEPNSt7__cxx1112basic_"
    "stringIcSt11char_traitsIcESaIcEEES7_");
extern "C" GidRefreshResult wrapRefreshCurrentGid(RdmaContext *, std::string *,
                                                  std::string *) {
    return gid_probe ? gid_probe() : GidRefreshResult::FAILED;
}
#endif

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

    // These tests only read and write rail state, so the pool's threads have
    // nothing to do. Left running they busy-poll: the context never opened, so
    // monitorWorker spins on a failing epoll_wait() for the whole suite. Same
    // shutdown as ~WorkerPool, ordered so a worker cannot miss the wakeup and
    // park for its full 1s timeout; the destructor then sees workers_running_
    // already false and does not join twice.
    static void stopWorkers(WorkerPool &pool) {
        if (!pool.workers_running_) return;
        pool.workers_running_.store(false);
        pool.cond_var_.notify_all();
        for (auto &entry : pool.worker_thread_) entry.join();
    }

    static void complete(WorkerPool &pool, Transport::Slice &slice,
                         ibv_wc_status status) {
        ibv_wc wc{};
        wc.wr_id = reinterpret_cast<uint64_t>(&slice);
        wc.status = status;
        pool.processCompletions(0, {wc});
    }

    static void completeBatch(WorkerPool &pool, const std::vector<ibv_wc> &wc) {
        pool.processCompletions(0, wc);
    }

    static int errorThreshold() { return WorkerPool::kRailErrorThreshold; }

    static uint64_t errorWindowNs() { return WorkerPool::kRailErrorWindowNs; }

    static void setContextActive(WorkerPool &pool, bool active) {
        pool.context_.set_active(active);
    }

    static bool contextActive(WorkerPool &pool) {
        return pool.context_.active();
    }

    static bool markLocalContextFailure(WorkerPool &pool) {
        return pool.markLocalContextFailure();
    }

    static void markContextSuccess(WorkerPool &pool) {
        pool.markContextSuccess();
    }

    static int contextFailureCount(WorkerPool &pool) {
        return pool.context_failure_count_.load(std::memory_order_relaxed);
    }

    static uint64_t breakerReactivateAfter(WorkerPool &pool) {
        std::lock_guard<std::mutex> lock(pool.context_state_lock_);
        return pool.breaker_reactivate_after_ns_;
    }

    static bool tryReactivateContext(WorkerPool &pool, uint64_t now_ns) {
        return pool.tryReactivateContext(now_ns);
    }

    static void setRecoveryActivateAfter(WorkerPool &pool,
                                         uint64_t activate_after_ns) {
        pool.recovery_activate_after_ns_.store(activate_after_ns,
                                               std::memory_order_relaxed);
    }

    static void contextEvent(WorkerPool &pool, ibv_event_type event) {
        pool.processContextEventForTest(event);
    }

    static uint64_t recoveryActivateAfter(WorkerPool &pool) {
        return pool.recovery_activate_after_ns_.load(std::memory_order_relaxed);
    }

    static void recoverContext(WorkerPool &pool) {
        pool.maybeActivateRecoveredContext();
    }

    static int contextFailureThreshold() {
        return WorkerPool::kLocalCompletionFailureThreshold;
    }
};

}  // namespace mooncake

namespace {

constexpr const char *kPeerA = "10.0.0.1@mlx5_bond_0";
constexpr const char *kPeerB = "10.0.0.1@mlx5_bond_1";

class WorkerPoolRailStateTest : public ::testing::Test {
   protected:
    void SetUp() override {
        // The context below is expected to fail to open, and that path is
        // noisy. Silence it so gtest failures stay readable.
        previous_min_log_level_ = FLAGS_minloglevel;
        FLAGS_minloglevel = google::GLOG_FATAL;

        metadata_ = std::make_shared<TransferMetadata>(P2PHANDSHAKE);
        transport_ = std::make_unique<RdmaTransport>();
        RdmaTransportTestPeer::bindMetadata(*transport_, metadata_,
                                            "rail-state-test");
        context_ = std::make_unique<RdmaContext>(*transport_, "unused");
        // Always fails, on any host, because no device is named "unused". It
        // still creates the endpoint store, which the monitor tick can touch
        // in the window before it is stopped below.
        context_->construct();
        worker_pool_ = std::make_unique<WorkerPool>(*context_);
        WorkerPoolTestPeer::stopWorkers(*worker_pool_);
    }

    void TearDown() override {
#ifdef __linux__
        gid_probe = {};
#endif
        worker_pool_.reset();
        context_.reset();
        transport_.reset();
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
    std::shared_ptr<TransferMetadata> metadata_;
    std::unique_ptr<RdmaTransport> transport_;
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

// Exercise the submit path that used to mistake one peer's unavailable rails
// for a local RNIC failure, not just the rail-pausing helper.
TEST_F(WorkerPoolRailStateTest,
       AllRailsUnavailableSubmitsDoNotDeactivateContext) {
    constexpr SegmentID target_id = 3559;
    constexpr uint64_t buffer_addr = 0x10000;
    auto desc = std::make_shared<TransferMetadata::SegmentDesc>();
    desc->name = "10.0.0.1";
    desc->protocol = "rdma";
    desc->devices.push_back({"mlx5_bond_0", 1, "", ""});
    desc->devices.push_back({"mlx5_bond_1", 1, "", ""});
    TransferMetadata::BufferDesc buffer;
    buffer.name = "cpu:0";
    buffer.addr = buffer_addr;
    buffer.length = 4096;
    buffer.rkey = {11, 22};
    desc->buffers.push_back(buffer);
    ASSERT_EQ(desc->topology.parse(
                  R"({"cpu:0": [["mlx5_bond_0", "mlx5_bond_1"], []]})"),
              0);
    metadata_->addLocalSegment(target_id, "10.0.0.1", std::move(desc));
    WorkerPoolTestPeer::setContextActive(*worker_pool_, true);
    Transport::TransferTask task;
    task.batch_id = transport_->allocateBatchID(1);
    for (int i = 0; i < WorkerPoolTestPeer::contextFailureThreshold(); ++i) {
        failRail(kPeerA, /*immediate_pause=*/true);
        failRail(kPeerB, /*immediate_pause=*/true);
        Transport::Slice slice{};
        slice.task = &task;
        slice.target_id = target_id;
        slice.rdma.dest_addr = buffer_addr;
        slice.length = 64;
        ASSERT_EQ(worker_pool_->submitPostSend({&slice}), 0);
        EXPECT_EQ(slice.status, Transport::Slice::FAILED);
        // Device selection succeeded, so the failure was unavailable rails.
        EXPECT_TRUE(slice.rdma.dest_rkey == 11 || slice.rdma.dest_rkey == 22);
        EXPECT_EQ(WorkerPoolTestPeer::contextFailureCount(*worker_pool_), 0);
        EXPECT_TRUE(WorkerPoolTestPeer::contextActive(*worker_pool_));
    }
    EXPECT_EQ(task.failed_slice_count,
              WorkerPoolTestPeer::contextFailureThreshold());
    EXPECT_TRUE(transport_->freeBatchID(task.batch_id).ok());

    EXPECT_TRUE(WorkerPoolTestPeer::contextActive(*worker_pool_));
    EXPECT_EQ(WorkerPoolTestPeer::contextFailureCount(*worker_pool_), 0);
}

TEST_F(WorkerPoolRailStateTest,
       ExhaustedRemoteFailureRefreshesNextP2PSubmission) {
    // Drive the real completion and next-submission paths without a verbs
    // device. Only the peer metadata exchange uses a loopback TCP daemon.
    WorkerPoolTestPeer::setContextActive(*worker_pool_, true);
    TransferMetadata server(P2PHANDSHAKE);
    int fd = -1;
    const auto port = findAvailableTcpPort(fd);
    ASSERT_GT(port, 0);
    const std::string name = "127.0.0.1:" + std::to_string(port);
    auto desc = std::make_shared<TransferMetadata::SegmentDesc>();
    desc->name = name;
    desc->protocol = "rdma";
    desc->rdma_server_name = "192.0.2.1:12345";
    desc->devices.push_back(
        {"mlx5_remote", 1, "00000000000000000000ffff7f000001", ""});
    TransferMetadata::BufferDesc buffer;
    buffer.name = "cpu:0";
    buffer.addr = 0x10000;
    buffer.length = 4096;
    buffer.rkey = {11};
    buffer.lkey = {1};
    desc->buffers.push_back(buffer);
    ASSERT_EQ(desc->topology.parse(R"({"cpu:0": [["mlx5_remote"], []]})"), 0);
    ASSERT_EQ(server.addLocalSegment(
                  LOCAL_SEGMENT_ID, name,
                  std::make_shared<TransferMetadata::SegmentDesc>(*desc)),
              0);
    ASSERT_EQ(metadata_->addLocalSegment(
                  LOCAL_SEGMENT_ID, "client",
                  std::make_shared<TransferMetadata::SegmentDesc>(*desc)),
              0);
    TransferMetadata::RpcMetaDesc rpc;
    rpc.ip_or_host_name = "127.0.0.1";
    rpc.rpc_port = port;
    rpc.sockfd = fd;
    ASSERT_EQ(server.addRpcMetaEntry(name, rpc), 0);
    const auto id = metadata_->getSegmentID(name);
    auto cached = metadata_->getSegmentDescByID(id);
    ASSERT_TRUE(cached);
    ASSERT_EQ(cached->buffers[0].rkey[0], 11u);

    ASSERT_EQ(server.removeLocalMemoryBuffer(
                  reinterpret_cast<void *>(buffer.addr), false),
              0);
    buffer.rkey = {22};
    ASSERT_EQ(server.addLocalMemoryBuffer(buffer, false), 0);
    ASSERT_EQ(metadata_->getSegmentDescByID(id), cached);

    Transport::TransferTask task;
    task.batch_id = transport_->allocateBatchID(1);
    Transport::Slice failed{};
    failed.task = &task;
    failed.target_id = id;
    failed.peer_nic_path = MakeNicPath(desc->rdma_server_name, "mlx5_remote");
    failed.rdma.dest_addr = buffer.addr;
    failed.rdma.dest_rkey = 11;
    failed.length = 64;
    failed.rdma.max_retry_cnt = 1;
    WorkerPoolTestPeer::complete(*worker_pool_, failed, IBV_WC_REM_ACCESS_ERR);
    EXPECT_EQ(failed.status, Transport::Slice::FAILED);
    EXPECT_EQ(failed.rdma.retry_cnt, 1);

    Transport::Slice next{};
    next.task = &task;
    next.target_id = id;
    next.rdma.dest_addr = buffer.addr;
    next.length = 64;
    ASSERT_EQ(worker_pool_->submitPostSend({&next}), 0);
    EXPECT_NE(next.status, Transport::Slice::FAILED);
    EXPECT_EQ(next.rdma.dest_rkey, 22u);
    // Also keep the existing redispatch path working with the default budget.
    ASSERT_EQ(server.removeLocalMemoryBuffer(
                  reinterpret_cast<void *>(buffer.addr), false),
              0);
    buffer.rkey = {33};
    ASSERT_EQ(server.addLocalMemoryBuffer(buffer, false), 0);
    next.rdma.max_retry_cnt = globalConfig().retry_cnt;
    ASSERT_GT(next.rdma.max_retry_cnt, 1);
    WorkerPoolTestPeer::complete(*worker_pool_, next, IBV_WC_REM_ACCESS_ERR);
    EXPECT_NE(next.status, Transport::Slice::FAILED);
    EXPECT_EQ(next.rdma.retry_cnt, 1);
    EXPECT_EQ(next.rdma.dest_rkey, 33u);

    EXPECT_EQ(metadata_->getSegmentID(name), id);
    ASSERT_EQ(metadata_->getRpcMetaEntry(desc->rdma_server_name, rpc), 0);
    EXPECT_EQ(rpc.ip_or_host_name, "127.0.0.1");
    EXPECT_EQ(rpc.rpc_port, port);
    EXPECT_TRUE(transport_->freeBatchID(task.batch_id).ok());
}

class UnavailableMetadataStorage : public MetadataStoragePlugin {
   public:
    int reads = 0;
    bool get(const std::string &, Json::Value &) override {
        ++reads;
        return false;
    }
    bool set(const std::string &, const Json::Value &) override { return true; }
    bool remove(const std::string &) override { return true; }
};

class RailTestMetadata : public TransferMetadata {
   public:
    explicit RailTestMetadata(std::shared_ptr<MetadataStoragePlugin> plugin)
        : TransferMetadata(std::move(plugin)) {}
};

TEST_F(WorkerPoolRailStateTest,
       CompletionBurstDoesNotFetchMetadataForRailSelection) {
    auto plugin = std::make_shared<UnavailableMetadataStorage>();
    metadata_ = std::make_shared<RailTestMetadata>(plugin);
    RdmaTransportTestPeer::bindMetadata(*transport_, metadata_,
                                        "rail-state-test");
    WorkerPoolTestPeer::setContextActive(*worker_pool_, true);
    constexpr SegmentID id = 3561;
    auto desc = std::make_shared<TransferMetadata::SegmentDesc>();
    desc->name = "10.0.0.1";
    desc->protocol = "rdma";
    desc->devices = {{"mlx5_bond_0", 1, "", ""}, {"mlx5_bond_1", 1, "", ""}};
    TransferMetadata::BufferDesc buffer;
    buffer.addr = 0x10000;
    buffer.length = 4096;
    buffer.rkey = {11, 22};
    desc->buffers.push_back(buffer);
    ASSERT_EQ(metadata_->addLocalSegment(
                  id, desc->name,
                  std::make_shared<TransferMetadata::SegmentDesc>(*desc)),
              0);
    Transport::TransferTask task;
    task.batch_id = transport_->allocateBatchID(1);
    Transport::Slice slices[2]{};
    std::vector<ibv_wc> completions;
    for (auto &slice : slices) {
        slice.task = &task;
        slice.target_id = id;
        slice.peer_nic_path = kPeerA;
        slice.rdma.dest_addr = buffer.addr;
        slice.length = 64;
        slice.rdma.max_retry_cnt = 1;
        ibv_wc wc{};
        wc.wr_id = reinterpret_cast<uint64_t>(&slice);
        wc.status = IBV_WC_RETRY_EXC_ERR;
        completions.push_back(wc);
    }
    WorkerPoolTestPeer::completeBatch(*worker_pool_, completions);
    EXPECT_EQ(plugin->reads, 0);
    EXPECT_TRUE(railAvailable(kPeerB));
    EXPECT_EQ(task.failed_slice_count, 2);
    // The burst leaves one peer-scoped request, consumed at transfer lookup.
    EXPECT_EQ(metadata_->getSegmentDescForTransfer(id), nullptr);
    EXPECT_EQ(plugin->reads, 1);
    EXPECT_TRUE(transport_->freeBatchID(task.batch_id).ok());
}

TEST_F(WorkerPoolRailStateTest,
       ExhaustedLocalAndFlushErrorsKeepRemoteMetadata) {
    WorkerPoolTestPeer::setContextActive(*worker_pool_, true);
    constexpr SegmentID id = 3560;
    auto desc = std::make_shared<TransferMetadata::SegmentDesc>();
    desc->name = "peer";
    desc->protocol = "rdma";
    ASSERT_EQ(
        metadata_->addLocalSegment(
            id, "peer", std::make_shared<TransferMetadata::SegmentDesc>(*desc)),
        0);
    auto cached = metadata_->getSegmentDescByID(id);
    Transport::TransferTask task;
    task.batch_id = transport_->allocateBatchID(1);
    for (auto status : {IBV_WC_WR_FLUSH_ERR, IBV_WC_LOC_LEN_ERR}) {
        Transport::Slice slice{};
        slice.task = &task;
        slice.target_id = id;
        slice.peer_nic_path = kPeerA;
        slice.rdma.max_retry_cnt = 1;
        WorkerPoolTestPeer::complete(*worker_pool_, slice, status);
        EXPECT_EQ(slice.status, Transport::Slice::FAILED);
        EXPECT_EQ(metadata_->getSegmentDescByID(id), cached);
    }
    EXPECT_TRUE(transport_->freeBatchID(task.batch_id).ok());
}

TEST_F(WorkerPoolRailStateTest, LocalFailuresTripContextAtThreshold) {
    WorkerPoolTestPeer::setContextActive(*worker_pool_, true);
    const int threshold = WorkerPoolTestPeer::contextFailureThreshold();
    for (int i = 0; i < threshold - 1; ++i) {
        EXPECT_FALSE(
            WorkerPoolTestPeer::markLocalContextFailure(*worker_pool_));
        EXPECT_TRUE(WorkerPoolTestPeer::contextActive(*worker_pool_));
    }

    EXPECT_TRUE(WorkerPoolTestPeer::markLocalContextFailure(*worker_pool_));
    EXPECT_FALSE(WorkerPoolTestPeer::contextActive(*worker_pool_));
    EXPECT_NE(WorkerPoolTestPeer::breakerReactivateAfter(*worker_pool_), 0u);
}

TEST_F(WorkerPoolRailStateTest, SuccessResetsLocalFailureStreak) {
    WorkerPoolTestPeer::setContextActive(*worker_pool_, true);
    const int threshold = WorkerPoolTestPeer::contextFailureThreshold();
    for (int i = 0; i < threshold - 1; ++i)
        ASSERT_FALSE(
            WorkerPoolTestPeer::markLocalContextFailure(*worker_pool_));

    WorkerPoolTestPeer::markContextSuccess(*worker_pool_);
    EXPECT_EQ(WorkerPoolTestPeer::contextFailureCount(*worker_pool_), 0);

    for (int i = 0; i < threshold - 1; ++i)
        EXPECT_FALSE(
            WorkerPoolTestPeer::markLocalContextFailure(*worker_pool_));
    EXPECT_TRUE(WorkerPoolTestPeer::contextActive(*worker_pool_));
}

TEST_F(WorkerPoolRailStateTest, BreakerReactivatesAtDeadline) {
    WorkerPoolTestPeer::setContextActive(*worker_pool_, true);
    for (int i = 0; i < WorkerPoolTestPeer::contextFailureThreshold(); ++i)
        WorkerPoolTestPeer::markLocalContextFailure(*worker_pool_);
    const uint64_t deadline =
        WorkerPoolTestPeer::breakerReactivateAfter(*worker_pool_);
    ASSERT_NE(deadline, 0u);

    EXPECT_FALSE(
        WorkerPoolTestPeer::tryReactivateContext(*worker_pool_, deadline - 1));
    EXPECT_FALSE(WorkerPoolTestPeer::contextActive(*worker_pool_));
    EXPECT_TRUE(
        WorkerPoolTestPeer::tryReactivateContext(*worker_pool_, deadline));
    EXPECT_TRUE(WorkerPoolTestPeer::contextActive(*worker_pool_));
    EXPECT_EQ(WorkerPoolTestPeer::contextFailureCount(*worker_pool_), 0);
    EXPECT_EQ(WorkerPoolTestPeer::breakerReactivateAfter(*worker_pool_), 0u);
}

TEST_F(WorkerPoolRailStateTest, GidRecoveryBlocksBreakerDeadline) {
    WorkerPoolTestPeer::setContextActive(*worker_pool_, true);
    for (int i = 0; i < WorkerPoolTestPeer::contextFailureThreshold(); ++i)
        WorkerPoolTestPeer::markLocalContextFailure(*worker_pool_);
    const uint64_t deadline =
        WorkerPoolTestPeer::breakerReactivateAfter(*worker_pool_);
    ASSERT_NE(deadline, 0u);

    WorkerPoolTestPeer::setRecoveryActivateAfter(*worker_pool_, 1);
    EXPECT_FALSE(
        WorkerPoolTestPeer::tryReactivateContext(*worker_pool_, deadline));
    EXPECT_FALSE(WorkerPoolTestPeer::contextActive(*worker_pool_));

    WorkerPoolTestPeer::setRecoveryActivateAfter(*worker_pool_, 0);
    EXPECT_TRUE(
        WorkerPoolTestPeer::tryReactivateContext(*worker_pool_, deadline));
}

TEST_F(WorkerPoolRailStateTest, FatalEventOwnershipCancelsBreakerDeadline) {
    WorkerPoolTestPeer::setContextActive(*worker_pool_, true);
    for (int i = 0; i < WorkerPoolTestPeer::contextFailureThreshold(); ++i)
        WorkerPoolTestPeer::markLocalContextFailure(*worker_pool_);
    ASSERT_NE(WorkerPoolTestPeer::breakerReactivateAfter(*worker_pool_), 0u);

    WorkerPoolTestPeer::contextEvent(*worker_pool_, IBV_EVENT_DEVICE_FATAL);
    EXPECT_EQ(WorkerPoolTestPeer::breakerReactivateAfter(*worker_pool_), 0u);
    EXPECT_EQ(WorkerPoolTestPeer::contextFailureCount(*worker_pool_), 0);
    EXPECT_FALSE(WorkerPoolTestPeer::markLocalContextFailure(*worker_pool_));
    EXPECT_EQ(WorkerPoolTestPeer::breakerReactivateAfter(*worker_pool_), 0u);
    EXPECT_FALSE(
        WorkerPoolTestPeer::tryReactivateContext(*worker_pool_, UINT64_MAX));
    EXPECT_FALSE(WorkerPoolTestPeer::contextActive(*worker_pool_));
}

TEST_F(WorkerPoolRailStateTest,
       RedundantPortActivePreservesLocalFailureStreak) {
    WorkerPoolTestPeer::setContextActive(*worker_pool_, true);
    for (int i = 0; i < WorkerPoolTestPeer::contextFailureThreshold() - 1; ++i)
        WorkerPoolTestPeer::markLocalContextFailure(*worker_pool_);

    WorkerPoolTestPeer::contextEvent(*worker_pool_, IBV_EVENT_PORT_ACTIVE);
    EXPECT_EQ(WorkerPoolTestPeer::contextFailureCount(*worker_pool_), 31);
    EXPECT_EQ(WorkerPoolTestPeer::recoveryActivateAfter(*worker_pool_), 0u);
    EXPECT_TRUE(WorkerPoolTestPeer::markLocalContextFailure(*worker_pool_));
    WorkerPoolTestPeer::recoverContext(*worker_pool_);
    EXPECT_FALSE(WorkerPoolTestPeer::contextActive(*worker_pool_));
    EXPECT_NE(WorkerPoolTestPeer::breakerReactivateAfter(*worker_pool_), 0u);
}

#ifdef __linux__
TEST_F(WorkerPoolRailStateTest, GidProbeOwnsRecoveryThroughoutIoAndRetry) {
    WorkerPoolTestPeer::setContextActive(*worker_pool_, true);
    for (int i = 0; i < WorkerPoolTestPeer::contextFailureThreshold(); ++i)
        WorkerPoolTestPeer::markLocalContextFailure(*worker_pool_);

    WorkerPoolTestPeer::contextEvent(*worker_pool_, IBV_EVENT_PORT_ACTIVE);
    EXPECT_EQ(WorkerPoolTestPeer::breakerReactivateAfter(*worker_pool_), 0u);
    EXPECT_EQ(WorkerPoolTestPeer::contextFailureCount(*worker_pool_), 32);
    EXPECT_NE(WorkerPoolTestPeer::recoveryActivateAfter(*worker_pool_), 0u);
    int probe_count = 0;
    gid_probe = [&] {
        ++probe_count;
        EXPECT_NE(WorkerPoolTestPeer::recoveryActivateAfter(*worker_pool_), 0u);
        EXPECT_FALSE(WorkerPoolTestPeer::contextActive(*worker_pool_));
        // A poller can still deliver late CQEs while the monitor probes.
        for (int i = 0; i < WorkerPoolTestPeer::contextFailureThreshold(); ++i)
            EXPECT_FALSE(
                WorkerPoolTestPeer::markLocalContextFailure(*worker_pool_));
        EXPECT_EQ(WorkerPoolTestPeer::breakerReactivateAfter(*worker_pool_),
                  0u);
        EXPECT_FALSE(WorkerPoolTestPeer::tryReactivateContext(*worker_pool_,
                                                              UINT64_MAX));
        return probe_count == 1 ? GidRefreshResult::FAILED
                                : GidRefreshResult::UNCHANGED;
    };

    WorkerPoolTestPeer::setRecoveryActivateAfter(*worker_pool_, 1);
    WorkerPoolTestPeer::recoverContext(*worker_pool_);
    EXPECT_EQ(probe_count, 1);
    EXPECT_FALSE(WorkerPoolTestPeer::contextActive(*worker_pool_));
    EXPECT_NE(WorkerPoolTestPeer::recoveryActivateAfter(*worker_pool_), 0u);
    EXPECT_EQ(WorkerPoolTestPeer::breakerReactivateAfter(*worker_pool_), 0u);

    WorkerPoolTestPeer::setRecoveryActivateAfter(*worker_pool_, 1);
    WorkerPoolTestPeer::recoverContext(*worker_pool_);
    EXPECT_EQ(probe_count, 2);
    EXPECT_TRUE(WorkerPoolTestPeer::contextActive(*worker_pool_));
    EXPECT_EQ(WorkerPoolTestPeer::recoveryActivateAfter(*worker_pool_), 0u);
    EXPECT_EQ(WorkerPoolTestPeer::contextFailureCount(*worker_pool_), 0);
}

TEST_F(WorkerPoolRailStateTest, FatalEventCancelsPendingGidProbe) {
    WorkerPoolTestPeer::setContextActive(*worker_pool_, false);
    WorkerPoolTestPeer::contextEvent(*worker_pool_, IBV_EVENT_PORT_ACTIVE);
    WorkerPoolTestPeer::setRecoveryActivateAfter(*worker_pool_, 1);
    WorkerPoolTestPeer::contextEvent(*worker_pool_, IBV_EVENT_PORT_ERR);
    gid_probe = [] {
        ADD_FAILURE() << "Fatal event must cancel the pending probe";
        return GidRefreshResult::UNCHANGED;
    };
    WorkerPoolTestPeer::recoverContext(*worker_pool_);
    EXPECT_FALSE(WorkerPoolTestPeer::contextActive(*worker_pool_));
    EXPECT_EQ(WorkerPoolTestPeer::recoveryActivateAfter(*worker_pool_), 0u);
    EXPECT_FALSE(
        WorkerPoolTestPeer::tryReactivateContext(*worker_pool_, UINT64_MAX));
}
#endif

}  // namespace

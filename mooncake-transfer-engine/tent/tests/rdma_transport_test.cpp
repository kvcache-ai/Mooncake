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

#include <gtest/gtest.h>
#include <infiniband/verbs.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "tent/common/config.h"
#include "tent/common/types.h"
#include "tent/transfer_engine.h"
#include "tent/transport/rdma/connect_pause_tracker.h"
#include "tent/transport/rdma/params.h"
#include "tent/transport/rdma/rdma_gid_probe.h"
#include "tent/transport/rdma/rdma_transport.h"

namespace mooncake {
namespace tent {
namespace {

bool hasRdmaDevice() {
    int count = 0;
    ibv_device** devices = ibv_get_device_list(&count);
    const bool available = devices != nullptr && count > 0;
    if (devices) ibv_free_device_list(devices);
    return available;
}

class ChildProcessGuard {
   public:
    ChildProcessGuard(pid_t pid, int stop_fd) : pid_(pid), stop_fd_(stop_fd) {}

    ~ChildProcessGuard() {
        if (pid_ <= 0) return;
        close(stop_fd_);
        (void)waitpid(pid_, nullptr, 0);
    }

    int finish() {
        close(stop_fd_);
        int status = 0;
        (void)waitpid(pid_, &status, 0);
        pid_ = -1;
        stop_fd_ = -1;
        return status;
    }

    int reap() {
        int status = 0;
        (void)waitpid(pid_, &status, 0);
        close(stop_fd_);
        pid_ = -1;
        stop_fd_ = -1;
        return status;
    }

   private:
    pid_t pid_;
    int stop_fd_;
};

std::shared_ptr<Config> makeRdmaConfig() {
    auto config = std::make_shared<Config>();
    config->set("metadata_type", "p2p");
    config->set("metadata_servers", "P2PHANDSHAKE");
    config->set("transports/rdma/enable", true);
    config->set("transports/rdma/num_lanes", 1);
    config->set("transports/rdma/endpoint/max_qp_wr", 1);
    config->set("transports/tcp/enable", false);
    config->set("transports/shm/enable", false);
    return config;
}

bool waitBatchDone(TransferEngine& engine, BatchID batch) {
    TransferStatus status;
    for (int i = 0; i < 10000; ++i) {
        auto result = engine.getTransferStatus(batch, status);
        if (!result.ok() || status.s == TransferStatusEnum::FAILED)
            return false;
        if (status.s == TransferStatusEnum::COMPLETED) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return false;
}

AutoGidCandidate makeGidCandidate(int gid_index, uint32_t gid_type,
                                  bool has_network_device,
                                  bool is_ipv4_mapped,
                                  bool is_link_local_ipv6,
                                  bool is_overlay_network = false,
                                  bool is_overlay_ipv4 = false,
                                  bool is_null_gid = false,
                                  bool query_succeeded = true,
                                  std::string gid = "") {
    AutoGidCandidate candidate;
    candidate.gid_index = gid_index;
    candidate.gid =
        gid.empty() ? "gid-" + std::to_string(gid_index) : std::move(gid);
    candidate.gid_type = gid_type;
    candidate.has_network_device = has_network_device;
    candidate.is_ipv4_mapped = is_ipv4_mapped;
    candidate.is_link_local_ipv6 = is_link_local_ipv6;
    candidate.is_overlay_network = is_overlay_network;
    candidate.is_overlay_ipv4 = is_overlay_ipv4;
    candidate.is_null_gid = is_null_gid;
    candidate.query_succeeded = query_succeeded;
    return candidate;
}

TEST(RdmaParamsTest, DefaultsKeepLaneCountsAligned) {
    RdmaParams params;

    EXPECT_EQ(params.num_lanes, 6);
    EXPECT_EQ(params.device.num_cq_list, params.num_lanes);
    EXPECT_EQ(params.endpoint.qp_mul_factor, params.num_lanes);
    EXPECT_EQ(params.workers.num_workers, params.num_lanes);
    EXPECT_EQ(params.endpoint.path_mtu, IBV_MTU_4096);
    EXPECT_EQ(params.endpoint.conn_pause_ttl_ms, 1000);
    EXPECT_EQ(params.device.auto_gid_max_retries, 2);
    EXPECT_TRUE(params.endpoint.mlx5_qp_udp_sports.empty());
    EXPECT_FALSE(params.endpoint.mlx5_qp_lag_port_balance);
    EXPECT_FALSE(params.workers.track_posted_slices);
}

TEST(RdmaGidProbeTest, PrefersNetworkBackedRoutableCandidate) {
    std::vector<AutoGidCandidate> candidates = {
        makeGidCandidate(0, IBV_GID_TYPE_ROCE_V2, false, true, false),
        makeGidCandidate(1, IBV_GID_TYPE_ROCE_V2, true, true, false),
    };

    auto selection = selectBestAutoGidCandidate(candidates);
    ASSERT_TRUE(selection.has_value());
    EXPECT_EQ(selection->gid_index, 1);
    EXPECT_EQ(selection->candidate_class,
              AutoGidCandidateClass::kNetworkRoutable);
}

TEST(RdmaGidProbeTest, DemotesLinkLocalBehindRoutableNetworkCandidate) {
    std::vector<AutoGidCandidate> candidates = {
        makeGidCandidate(0, IBV_GID_TYPE_ROCE_V2, true, false, true),
        makeGidCandidate(1, IBV_GID_TYPE_ROCE_V2, true, true, false),
    };

    auto selection = selectBestAutoGidCandidate(candidates);
    ASSERT_TRUE(selection.has_value());
    EXPECT_EQ(selection->gid_index, 1);
    EXPECT_EQ(selection->candidate_class,
              AutoGidCandidateClass::kNetworkRoutable);
}

TEST(RdmaGidProbeTest, DemotesOverlayCandidateBehindNormalNetworkCandidate) {
    std::vector<AutoGidCandidate> candidates = {
        makeGidCandidate(0, IBV_GID_TYPE_ROCE_V2, true, true, false, true),
        makeGidCandidate(1, IBV_GID_TYPE_ROCE_V2, true, true, false),
    };

    auto selection = selectBestAutoGidCandidate(candidates);
    ASSERT_TRUE(selection.has_value());
    EXPECT_EQ(selection->gid_index, 1);
}

TEST(RdmaGidProbeTest,
     PrefersNoNetworkRoutableOverDegradedNetworkBackedCandidate) {
    std::vector<AutoGidCandidate> candidates = {
        makeGidCandidate(0, IBV_GID_TYPE_ROCE_V2, true, false, true),
        makeGidCandidate(1, IBV_GID_TYPE_ROCE_V2, false, true, false),
    };

    auto selection = selectBestAutoGidCandidate(candidates);
    ASSERT_TRUE(selection.has_value());
    EXPECT_EQ(selection->gid_index, 1);
    EXPECT_EQ(selection->candidate_class,
              AutoGidCandidateClass::kNoNetworkRoutable);
}

TEST(RdmaGidProbeTest, KeepsNoNetworkFallbackAsLastResort) {
    std::vector<AutoGidCandidate> candidates = {
        makeGidCandidate(3, IBV_GID_TYPE_ROCE_V2, false, true, false),
    };

    auto selection = selectBestAutoGidCandidate(candidates);
    ASSERT_TRUE(selection.has_value());
    EXPECT_EQ(selection->gid_index, 3);
    EXPECT_EQ(selection->candidate_class,
              AutoGidCandidateClass::kNoNetworkRoutable);
}

TEST(RdmaGidProbeTest, FallsBackToFirstNonzeroCandidateWhenNeeded) {
    std::vector<AutoGidCandidate> candidates = {
        makeGidCandidate(0, IBV_GID_TYPE_ROCE_V1, false, false, false),
        makeGidCandidate(2, IBV_GID_TYPE_ROCE_V1, false, false, false),
    };

    auto selection = selectBestAutoGidCandidate(candidates);
    ASSERT_TRUE(selection.has_value());
    EXPECT_EQ(selection->gid_index, 0);
    EXPECT_EQ(selection->candidate_class,
              AutoGidCandidateClass::kFallbackNonzero);
}

TEST(RdmaGidProbeTest, DoesNotTreatIbCandidateAsLinkLocalIpv6Penalty) {
    std::vector<AutoGidCandidate> candidates = {
        makeGidCandidate(0, IBV_GID_TYPE_IB, true, false, true),
        makeGidCandidate(1, IBV_GID_TYPE_ROCE_V2, false, true, false),
    };

    auto selection = selectBestAutoGidCandidate(candidates);
    ASSERT_TRUE(selection.has_value());
    EXPECT_EQ(selection->gid_index, 0);
    EXPECT_EQ(selection->candidate_class,
              AutoGidCandidateClass::kNetworkRoutable);
}

TEST(RdmaGidProbeTest, SkipsInvalidAndNullCandidates) {
    std::vector<AutoGidCandidate> candidates = {
        makeGidCandidate(0, IBV_GID_TYPE_ROCE_V2, true, true, false, false,
                         false, false, false),
        makeGidCandidate(1, IBV_GID_TYPE_ROCE_V2, true, true, false, false,
                         false, true),
        makeGidCandidate(2, IBV_GID_TYPE_ROCE_V2, true, true, false),
    };

    auto selection = selectBestAutoGidCandidate(candidates);
    ASSERT_TRUE(selection.has_value());
    EXPECT_EQ(selection->gid_index, 2);
}

TEST(RdmaGidProbeTest, KeepsStableOrderingWithinSameCandidateClass) {
    std::vector<AutoGidCandidate> candidates = {
        makeGidCandidate(3, IBV_GID_TYPE_ROCE_V2, true, true, false),
        makeGidCandidate(1, IBV_GID_TYPE_ROCE_V2, true, true, false),
    };

    auto ranked = rankAutoGidCandidates(candidates);
    ASSERT_EQ(ranked.size(), 2u);
    EXPECT_EQ(ranked[0].gid_index, 1);
    EXPECT_EQ(ranked[1].gid_index, 3);
}

TEST(RdmaGidProbeTest, ReprobeStillPicksBestCandidateFromFreshSnapshot) {
    std::vector<AutoGidCandidate> candidates = {
        makeGidCandidate(1, IBV_GID_TYPE_ROCE_V2, true, true, false),
        makeGidCandidate(3, IBV_GID_TYPE_ROCE_V2, false, true, false),
    };

    auto selection = selectBestAutoGidCandidate(candidates);
    ASSERT_TRUE(selection.has_value());
    EXPECT_EQ(selection->gid_index, 1);
    EXPECT_EQ(selection->candidate_class,
              AutoGidCandidateClass::kNetworkRoutable);
}

TEST(RdmaGidProbeTest, ReprobeDetectsSameIndexGidRefresh) {
    std::vector<AutoGidCandidate> candidates = {
        makeGidCandidate(1, IBV_GID_TYPE_ROCE_V2, true, true, false, false,
                         false, false, true, "00:11:22"),
        makeGidCandidate(3, IBV_GID_TYPE_ROCE_V2, false, true, false),
    };

    auto selection = reselectAutoGidCandidate(candidates, 1, "00:11:21");
    ASSERT_TRUE(selection.has_value());
    EXPECT_EQ(selection->gid_index, 1);
    EXPECT_EQ(selection->gid, "00:11:22");
    EXPECT_EQ(selection->candidate_class,
              AutoGidCandidateClass::kNetworkRoutable);
}

TEST(RdmaGidProbeTest, ReprobeSkipsRetryWhenBestSelectionDidNotChange) {
    std::vector<AutoGidCandidate> candidates = {
        makeGidCandidate(1, IBV_GID_TYPE_ROCE_V2, true, true, false, false,
                         false, false, true, "00:11:22"),
    };

    auto selection = reselectAutoGidCandidate(candidates, 1, "00:11:22");
    EXPECT_FALSE(selection.has_value());
}

TEST(RdmaGidProbeTest, ReprobeSkipsAlreadyTriedCandidates) {
    std::vector<AutoGidCandidate> candidates = {
        makeGidCandidate(1, IBV_GID_TYPE_ROCE_V2, true, true, false, false,
                         false, false, true, "00:11:22"),
        makeGidCandidate(3, IBV_GID_TYPE_ROCE_V2, false, true, false, false,
                         false, false, true, "00:11:33"),
    };

    auto selection = reselectAutoGidCandidate(
        candidates, 1, "00:11:21", {{1, "00:11:22"}});
    ASSERT_TRUE(selection.has_value());
    EXPECT_EQ(selection->gid_index, 3);
    EXPECT_EQ(selection->gid, "00:11:33");
}

TEST(RdmaGidProbeTest, DetectsSameIndexGidByteChangesAsSelectionChanges) {
    EXPECT_TRUE(didAutoGidSelectionChange(1, "00:11:22", 1, "00:11:23"));
    EXPECT_FALSE(didAutoGidSelectionChange(1, "00:11:22", 1, "00:11:22"));
}

TEST(RdmaGidProbeTest, HandshakeRetryRespectsConfiguredRetryBudget) {
    EXPECT_TRUE(shouldAttemptAutoGidHandshakeRetry(true, 0, 2, true, EINVAL));
    EXPECT_TRUE(shouldAttemptAutoGidHandshakeRetry(true, 1, 2, true, EINVAL));
    EXPECT_FALSE(shouldAttemptAutoGidHandshakeRetry(false, 0, 2, true, EINVAL));
    EXPECT_FALSE(shouldAttemptAutoGidHandshakeRetry(true, 2, 2, true, EINVAL));
    EXPECT_FALSE(shouldAttemptAutoGidHandshakeRetry(true, 0, 0, true, EINVAL));
}

TEST(RdmaGidProbeTest, HandshakeRetryOnlyTriggersForRtrEinval) {
    EXPECT_FALSE(shouldAttemptAutoGidHandshakeRetry(true, 0, 2, false, EINVAL));
    EXPECT_FALSE(shouldAttemptAutoGidHandshakeRetry(true, 0, 2, true, ENOENT));
}

TEST(RdmaGidProbeTest, RetryActionRequiresObservedOrReprobedChange) {
    EXPECT_EQ(decideAutoGidRetryAction(false, 1, "00:11:22", 1, "00:11:22"),
              AutoGidRetryAction::kDoNotRetry);
    EXPECT_EQ(decideAutoGidRetryAction(true, 1, "00:11:22", 1, "00:11:23"),
              AutoGidRetryAction::kRetryWithReprobedGid);
    EXPECT_EQ(decideAutoGidRetryAction(false, 1, "00:11:22", 1, "00:11:23"),
              AutoGidRetryAction::kRetryWithObservedChange);
}

TEST(RdmaGidProbeTest, PrefersPrivateRangeV4OverLinkLocal) {
    std::vector<AutoGidCandidate> candidates = {
        makeGidCandidate(0, IBV_GID_TYPE_ROCE_V1, true, false, true),
        makeGidCandidate(1, IBV_GID_TYPE_ROCE_V2, true, false, true),
        makeGidCandidate(2, IBV_GID_TYPE_ROCE_V1, true, true, false, false,
                         true),
        makeGidCandidate(3, IBV_GID_TYPE_ROCE_V2, true, true, false, false,
                         true),
    };

    auto selection = selectBestAutoGidCandidate(candidates);
    ASSERT_TRUE(selection.has_value());
    EXPECT_EQ(selection->gid_index, 3);
    EXPECT_EQ(selection->candidate_class,
              AutoGidCandidateClass::kNetworkPrivateV4);
}

TEST(RdmaGidProbeTest, RoutableV4StillOutranksPrivateRangeV4) {
    std::vector<AutoGidCandidate> candidates = {
        makeGidCandidate(1, IBV_GID_TYPE_ROCE_V2, true, true, false, false,
                         true),
        makeGidCandidate(5, IBV_GID_TYPE_ROCE_V2, true, true, false),
    };

    auto selection = selectBestAutoGidCandidate(candidates);
    ASSERT_TRUE(selection.has_value());
    EXPECT_EQ(selection->gid_index, 5);
    EXPECT_EQ(selection->candidate_class,
              AutoGidCandidateClass::kNetworkRoutable);
}

TEST(RdmaGidProbeTest, OverlayInterfaceStaysDemotedBelowPrivateRangeV4) {
    std::vector<AutoGidCandidate> candidates = {
        makeGidCandidate(1, IBV_GID_TYPE_ROCE_V2, true, true, false, true,
                         true),
        makeGidCandidate(4, IBV_GID_TYPE_ROCE_V2, true, true, false, false,
                         true),
    };

    auto selection = selectBestAutoGidCandidate(candidates);
    ASSERT_TRUE(selection.has_value());
    EXPECT_EQ(selection->gid_index, 4);
    EXPECT_EQ(selection->candidate_class,
              AutoGidCandidateClass::kNetworkPrivateV4);
}

TEST(RdmaGidProbeTest, NetworkLinkLocalStillOutranksNoNetworkPrivateV4) {
    std::vector<AutoGidCandidate> candidates = {
        makeGidCandidate(1, IBV_GID_TYPE_ROCE_V2, true, false, true),
        makeGidCandidate(3, IBV_GID_TYPE_ROCE_V2, false, true, false, false,
                         true),
    };

    auto selection = selectBestAutoGidCandidate(candidates);
    ASSERT_TRUE(selection.has_value());
    EXPECT_EQ(selection->gid_index, 1);
    EXPECT_EQ(selection->candidate_class,
              AutoGidCandidateClass::kNetworkDegraded);
}

TEST(RdmaGidProbeTest, ClassPriorityAndNameTablesAreComplete) {
    const AutoGidCandidateClass all[] = {
        AutoGidCandidateClass::kNetworkRoutable,
        AutoGidCandidateClass::kNoNetworkRoutable,
        AutoGidCandidateClass::kNetworkPrivateV4,
        AutoGidCandidateClass::kNetworkDegraded,
        AutoGidCandidateClass::kNoNetworkPrivateV4,
        AutoGidCandidateClass::kNoNetworkDegraded,
        AutoGidCandidateClass::kFallbackNonzero,
    };
    int expected_priority = 0;
    std::set<std::string> names;
    for (auto cls : all) {
        EXPECT_EQ(autoGidCandidateClassPriority(cls), expected_priority++);
        std::string name = autoGidCandidateClassToString(cls);
        EXPECT_NE(name, "unknown");
        EXPECT_TRUE(names.insert(name).second)
            << "duplicate class name: " << name;
    }
}

TEST(RdmaGidProbeTest, LinkLocalOnlyKeepsLowestIndexTieBreak) {
    std::vector<AutoGidCandidate> candidates = {
        makeGidCandidate(1, IBV_GID_TYPE_ROCE_V2, true, false, true),
        makeGidCandidate(2, IBV_GID_TYPE_ROCE_V2, true, false, true),
    };

    auto selection = selectBestAutoGidCandidate(candidates);
    ASSERT_TRUE(selection.has_value());
    EXPECT_EQ(selection->gid_index, 1);
    EXPECT_EQ(selection->candidate_class,
              AutoGidCandidateClass::kNetworkDegraded);
}

struct FakeClock {
    std::atomic<uint64_t> now{0};
    uint64_t operator()() const { return now.load(std::memory_order_relaxed); }
};

ConnectPauseTracker makeTracker(std::shared_ptr<FakeClock> clk) {
    return ConnectPauseTracker([clk] { return (*clk)(); });
}

TEST(ConnectPauseTrackerTest, UnknownPeerNotPaused) {
    auto clk = std::make_shared<FakeClock>();
    auto tracker = makeTracker(clk);

    EXPECT_FALSE(tracker.isPaused("10.0.0.1:1234"));
}

TEST(ConnectPauseTrackerTest, PausesUntilExpiryAndPrunes) {
    uint64_t now = 100;
    ConnectPauseTracker tracker([&] { return now; });

    EXPECT_FALSE(tracker.isPaused("peer-a"));
    tracker.pauseFor("peer-a", 50);
    tracker.pauseFor("peer-b", 100);
    EXPECT_TRUE(tracker.isPaused("peer-a"));
    EXPECT_TRUE(tracker.isPaused("peer-b"));

    now = 151;
    EXPECT_FALSE(tracker.isPaused("peer-a"));
    EXPECT_TRUE(tracker.isPaused("peer-b"));
    tracker.prune();
    EXPECT_FALSE(tracker.isPaused("peer-a"));
    EXPECT_TRUE(tracker.isPaused("peer-b"));

    now = 201;
    tracker.prune();
    EXPECT_FALSE(tracker.isPaused("peer-a"));
    EXPECT_FALSE(tracker.isPaused("peer-b"));
}

TEST(ConnectPauseTrackerTest, LaterPauseExtendsExistingDeadline) {
    uint64_t now = 100;
    ConnectPauseTracker tracker([&] { return now; });

    tracker.pauseFor("peer-a", 50);
    now = 120;
    tracker.pauseFor("peer-a", 100);

    now = 151;
    EXPECT_TRUE(tracker.isPaused("peer-a"));
    now = 221;
    EXPECT_FALSE(tracker.isPaused("peer-a"));
}

TEST(ConnectPauseTrackerTest, RepeatedChecksDoNotExtendHardDeadline) {
    auto clk = std::make_shared<FakeClock>();
    auto tracker = makeTracker(clk);
    const std::string peer = "10.0.0.1:1234";
    tracker.pauseFor(peer, 1000);

    for (uint64_t now = 100; now < 1000; now += 100) {
        clk->now = now;
        EXPECT_TRUE(tracker.isPaused(peer));
    }

    clk->now = 1000;
    EXPECT_FALSE(tracker.isPaused(peer));
}

TEST(ConnectPauseTrackerTest, RefreshExtendsWindow) {
    auto clk = std::make_shared<FakeClock>();
    auto tracker = makeTracker(clk);
    const std::string peer = "peer";
    tracker.pauseFor(peer, 100);
    clk->now = 50;
    tracker.pauseFor(peer, 150);

    clk->now = 150;
    EXPECT_TRUE(tracker.isPaused(peer));
    clk->now = 200;
    EXPECT_FALSE(tracker.isPaused(peer));
}

TEST(ConnectPauseTrackerTest, PruneDropsOnlyExpired) {
    auto clk = std::make_shared<FakeClock>();
    auto tracker = makeTracker(clk);
    tracker.pauseFor("a", 100);
    tracker.pauseFor("b", 300);

    clk->now = 200;
    tracker.prune();

    EXPECT_FALSE(tracker.isPaused("a"));
    EXPECT_TRUE(tracker.isPaused("b"));
}

TEST(ConnectPauseTrackerTest, PerPeerIndependent) {
    auto clk = std::make_shared<FakeClock>();
    auto tracker = makeTracker(clk);
    tracker.pauseFor("a", 100);
    tracker.pauseFor("b", 1000);

    clk->now = 150;
    EXPECT_FALSE(tracker.isPaused("a"));
    EXPECT_TRUE(tracker.isPaused("b"));
}

TEST(ConnectPauseTrackerTest, ConcurrentAccessIsRaceFree) {
    auto clk = std::make_shared<FakeClock>();
    auto tracker = makeTracker(clk);
    constexpr int kIters = 5000;
    constexpr uint64_t kFarFuture = 1ull << 40;
    std::vector<std::thread> threads;

    for (int i = 0; i < 4; ++i) {
        threads.emplace_back([&tracker, i] {
            std::string server = "peer" + std::to_string(i % 3);
            for (int k = 0; k < kIters; ++k) {
                tracker.pauseFor(server, kFarFuture);
            }
        });
    }
    for (int i = 0; i < 4; ++i) {
        threads.emplace_back([&tracker] {
            for (int k = 0; k < kIters; ++k) {
                (void)tracker.isPaused("peer0");
            }
        });
    }
    threads.emplace_back([&tracker] {
        for (int k = 0; k < kIters; ++k) {
            tracker.prune();
        }
    });

    for (auto& thread : threads) thread.join();
    SUCCEED();
}

TEST(RdmaSubBatchTest, ReportsTaskCount) {
    RdmaSubBatch batch;
    batch.max_size = 8;

    EXPECT_EQ(batch.size(), 0);
    batch.task_list.push_back(nullptr);
    batch.task_list.push_back(nullptr);
    EXPECT_EQ(batch.size(), 2);
}

TEST(RdmaTransportIntegrationTest, WriteThenReadAcrossProcesses) {
    if (!hasRdmaDevice()) GTEST_SKIP() << "no RDMA device detected";

    constexpr size_t kDataLength = 4 * 1024 * 1024;
    constexpr size_t kCancelTaskCount = 16;
    constexpr size_t kCancelStride = 8 * 1024 * 1024;
    constexpr size_t kBufferLength = kCancelTaskCount * kCancelStride;
    int ready_pipe[2];
    int stop_pipe[2];
    ASSERT_EQ(pipe(ready_pipe), 0);
    ASSERT_EQ(pipe(stop_pipe), 0);

    pid_t child = fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
        close(ready_pipe[0]);
        close(stop_pipe[1]);

        TransferEngine server(makeRdmaConfig());
        if (!server.available()) _exit(2);
        std::vector<uint8_t> buffer(kBufferLength);
        if (!server.registerLocalMemory(buffer.data(), buffer.size()).ok())
            _exit(3);

        const std::string segment = server.getSegmentName();
        uint32_t length = static_cast<uint32_t>(segment.size());
        if (write(ready_pipe[1], &length, sizeof(length)) != sizeof(length))
            _exit(4);
        if (write(ready_pipe[1], segment.data(), length) !=
            static_cast<ssize_t>(length))
            _exit(5);

        char stop = 0;
        const ssize_t stop_result = read(stop_pipe[0], &stop, 1);
        (void)stop_result;
        (void)server.unregisterLocalMemory(buffer.data(), buffer.size());
        _exit(0);
    }

    close(ready_pipe[1]);
    close(stop_pipe[0]);
    ChildProcessGuard child_guard(child, stop_pipe[1]);

    uint32_t segment_length = 0;
    ssize_t received =
        read(ready_pipe[0], &segment_length, sizeof(segment_length));
    if (received != static_cast<ssize_t>(sizeof(segment_length))) {
        const int status = child_guard.reap();
        GTEST_SKIP() << "RDMA server initialization failed, child status "
                     << status;
    }
    std::string server_segment(segment_length, '\0');
    ASSERT_EQ(read(ready_pipe[0], server_segment.data(), segment_length),
              static_cast<ssize_t>(segment_length));

    TransferEngine client(makeRdmaConfig());
    ASSERT_TRUE(client.available());
    std::vector<uint8_t> buffer(kBufferLength);
    for (size_t i = 0; i < kDataLength; ++i) {
        buffer[i] = static_cast<uint8_t>((i * 31) & 0xff);
    }
    ASSERT_TRUE(client.registerLocalMemory(buffer.data(), buffer.size()).ok());

    SegmentID segment = 0;
    Status result;
    for (int i = 0; i < 100; ++i) {
        result = client.openSegment(segment, server_segment);
        if (result.ok()) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_TRUE(result.ok()) << result.ToString();

    SegmentInfo info;
    ASSERT_TRUE(client.getSegmentInfo(segment, info).ok());
    ASSERT_FALSE(info.buffers.empty());

    Request request{};
    request.opcode = Request::WRITE;
    request.source = buffer.data();
    request.target_id = segment;
    request.target_offset = info.buffers[0].base;
    request.length = kDataLength;
    request.transport_hint = RDMA;

    BatchID batch = client.allocateBatch(1);
    ASSERT_TRUE(client.submitTransfer(batch, {request}).ok());
    ASSERT_TRUE(waitBatchDone(client, batch));
    ASSERT_TRUE(client.freeBatch(batch).ok());

    request.opcode = Request::READ;
    request.source = buffer.data() + kDataLength;
    batch = client.allocateBatch(1);
    ASSERT_TRUE(client.submitTransfer(batch, {request}).ok());
    ASSERT_TRUE(waitBatchDone(client, batch));
    ASSERT_TRUE(client.freeBatch(batch).ok());
    EXPECT_EQ(
        std::memcmp(buffer.data(), buffer.data() + kDataLength, kDataLength),
        0);

    // Keep one QP/worker and one outstanding WR so the tail task remains in
    // the worker's unposted set long enough to exercise real cancellation.
    std::vector<Request> cancel_requests;
    cancel_requests.reserve(kCancelTaskCount);
    for (size_t i = 0; i < kCancelTaskCount; ++i) {
        Request cancel_request{};
        cancel_request.opcode = Request::WRITE;
        cancel_request.source = buffer.data() + i * kCancelStride;
        cancel_request.target_id = segment;
        cancel_request.target_offset = info.buffers[0].base + i * kCancelStride;
        cancel_request.length = kDataLength;
        cancel_request.transport_hint = RDMA;
        cancel_requests.push_back(cancel_request);
    }

    batch = client.allocateBatch(kCancelTaskCount);
    ASSERT_TRUE(client.submitTransfer(batch, cancel_requests).ok());
    const size_t cancel_task_id = kCancelTaskCount - 1;
    ASSERT_TRUE(client.cancelTransfer(batch, cancel_task_id).ok());
    ASSERT_TRUE(client.cancelTransfer(batch, cancel_task_id).ok());

    std::vector<TransferStatus> statuses;
    for (int poll = 0; poll < 10000; ++poll) {
        ASSERT_TRUE(client.getTransferStatus(batch, statuses).ok());
        if (std::all_of(statuses.begin(), statuses.end(),
                        [](const TransferStatus& task_status) {
                            return task_status.s != TransferStatusEnum::PENDING;
                        }))
            break;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_EQ(statuses.size(), kCancelTaskCount);
    for (size_t i = 0; i < cancel_task_id; ++i) {
        EXPECT_EQ(statuses[i].s, TransferStatusEnum::COMPLETED);
    }
    EXPECT_EQ(statuses[cancel_task_id].s, TransferStatusEnum::CANCELED);
    EXPECT_LE(statuses[cancel_task_id].transferred_bytes, kDataLength);
    ASSERT_TRUE(client.freeBatch(batch).ok());

    EXPECT_TRUE(client.closeSegment(segment).ok());
    EXPECT_TRUE(
        client.unregisterLocalMemory(buffer.data(), buffer.size()).ok());

    const int status = child_guard.finish();
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 0);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake

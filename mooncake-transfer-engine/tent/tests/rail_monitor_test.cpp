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

#include <atomic>
#include <chrono>
#include <map>
#include <thread>

#include "tent/common/config.h"
#include "tent/runtime/segment.h"
#include "tent/transport/rdma/rail_monitor.h"

namespace mooncake {
namespace tent {
namespace {

// ---------------------------------------------------------------------------
// Helper: build a minimal single-NIC Topology via Topology::parse().
//
// JSON keys expected by parse(): "nics" / "mems"
// NicType enum:  NIC_RDMA=0, NIC_TCP=1, NIC_UNKNOWN=2
// MemType enum:  MEM_HOST=0, MEM_CUDA=1, ...
// device_list keys: "rank0", "rank1", ...
// ---------------------------------------------------------------------------

static std::shared_ptr<Topology> makeSingleNicTopology(const std::string& nic,
                                                       int numa_node = 0) {
    // One RDMA NIC (type=0) and one CUDA mem region (type=1) referencing it.
    auto json_str = R"({
        "nics": [{"name": ")" +
                    nic + R"(", "type": 0, "numa_node": )" +
                    std::to_string(numa_node) + R"(}],
        "mems": [{
            "name": "cuda0",
            "type": 1,
            "numa_node": )" +
                    std::to_string(numa_node) +
                    R"(,
            "device_list": {"rank0": [0]}
        }]
    })";
    auto topo = std::make_shared<Topology>();
    auto status = topo->parse(json_str);
    if (!status.ok()) {
        ADD_FAILURE() << "Topology::parse failed: " << status.ToString();
    }
    return topo;
}

static SegmentDescRef makeSingleNicSegment(const std::string& nic,
                                           int numa_node = 0) {
    auto desc = std::make_shared<SegmentDesc>();
    desc->type = SegmentType::Memory;
    auto& topology = std::get<MemorySegmentDesc>(desc->detail).topology;
    auto json_str = R"({
        "nics": [{"name": ")" +
                    nic + R"(", "type": 0, "numa_node": )" +
                    std::to_string(numa_node) + R"(}],
        "mems": [{
            "name": "cuda0",
            "type": 1,
            "numa_node": )" +
                    std::to_string(numa_node) +
                    R"(,
            "device_list": {"rank0": [0]}
        }]
    })";
    auto status = topology.parse(json_str);
    if (!status.ok()) {
        ADD_FAILURE() << "Topology::parse failed: " << status.ToString();
    }
    return desc;
}

static std::shared_ptr<Topology> makeTwoNicTopology(const std::string& first,
                                                    const std::string& second) {
    auto json_str = R"({
        "nics": [
            {"name": ")" +
                    first + R"(", "type": 0, "numa_node": 0},
            {"name": ")" +
                    second + R"(", "type": 0, "numa_node": 0}
        ],
        "mems": [{
            "name": "host0",
            "type": 0,
            "numa_node": 0,
            "device_list": {"rank0": [0, 1]}
        }]
    })";
    auto topo = std::make_shared<Topology>();
    auto status = topo->parse(json_str);
    if (!status.ok()) {
        ADD_FAILURE() << "Topology::parse failed: " << status.ToString();
    }
    return topo;
}

// A single simulated client retains the admission of each outstanding rail.
// Denied admission must not overwrite an earlier request's token.
class RailClient {
   public:
    explicit RailClient(RailMonitor& rail) : rail_(rail) {}
    bool admit(int local, int remote) {
        uint64_t token;
        if (!rail_.admit(local, remote, token)) return false;
        tokens_[{local, remote}] = token;
        return true;
    }
    void markFailed(int local, int remote) {
        ensureAdmission(local, remote);
        rail_.markFailed(local, remote, tokens_[{local, remote}]);
    }
    void markRecovered(int local, int remote) {
        ensureAdmission(local, remote);
        rail_.markRecovered(local, remote, tokens_[{local, remote}]);
    }
    void cancelProbe(int local, int remote) {
        rail_.cancelProbe(local, remote, tokens_[{local, remote}]);
    }

   private:
    void ensureAdmission(int local, int remote) {
        // A healthy rail can supply the current normal admission. A paused
        // rail must retain the existing trial (or deliberately stale token).
        if (rail_.isAvailable(local, remote)) admit(local, remote);
    }
    RailMonitor& rail_;
    std::map<std::pair<int, int>, uint64_t> tokens_;
};

// Shared CQs can run completion updates on a different worker while the
// owner selects another path. Run under TSan as well as ordinary regression.
TEST(RailMonitorConcurrencyTest, CompletionAndAdmissionShareStateSafely) {
    auto local = makeTwoNicTopology("local0", "local1");
    auto remote = makeTwoNicTopology("remote0", "remote1");
    RailMonitor rail;
    RailClient client(rail);
    Config conf;
    conf.set(RailMonitor::kCfgErrorThreshold, 1);
    conf.set(RailMonitor::kCfgProbeIntervalSecs, 0);
    ASSERT_TRUE(rail.load(local, remote, "", &conf).ok());
    std::atomic<bool> start{false};
    std::thread completion([&] {
        while (!start.load(std::memory_order_acquire))
            std::this_thread::yield();
        for (int i = 0; i < 2000; ++i) {
            uint64_t admission;
            if (rail.admit(0, 0, admission)) {
                if (i % 2)
                    rail.markFailed(0, 0, admission);
                else
                    rail.markRecovered(0, 0, admission);
            }
        }
    });
    start.store(true, std::memory_order_release);
    for (int i = 0; i < 2000; ++i) {
        client.admit(0, 0);
        rail.probeInFlight(0, 0);
        client.cancelProbe(0, 0);
        rail.findBestRemoteDevice(0, 0);
        rail.findBestRemoteDevice(0, -1);
        rail.findBestRemoteDevice(0, 2);
        EXPECT_EQ(rail.localDeviceMask(*remote->getMemEntry(0)), 3u);
        EXPECT_TRUE(rail.isAvailable(1, 1));
        EXPECT_TRUE(rail.load(local, remote, "", &conf).ok());
    }
    completion.join();
    client.cancelProbe(0, 0);
    ASSERT_TRUE(client.admit(0, 0));
    client.markRecovered(0, 0);
    EXPECT_TRUE(rail.isAvailable(0, 0));
    EXPECT_FALSE(rail.probeInFlight(0, 0));
}

TEST(RailMonitorConfigTest, CustomJsonOverridesAutomaticPeerMapping) {
    auto local = makeTwoNicTopology("local0", "local1");
    auto remote = makeTwoNicTopology("remote0", "remote1");
    const std::string rail_json = R"({
        "all": [
            {"local": "local0", "remote": "remote1"},
            {"local": "local1", "remote": "remote0"}
        ],
        "direct": [
            {"local": "local0", "remote": "remote1"},
            {"local": "local1", "remote": "remote0"}
        ]
    })";

    RailMonitor rail;
    ASSERT_TRUE(rail.load(local, remote, rail_json, nullptr).ok());
    EXPECT_EQ(rail.findBestRemoteDevice(/*local_nic=*/0, /*remote_numa=*/0), 1);
    EXPECT_EQ(rail.findBestRemoteDevice(/*local_nic=*/1, /*remote_numa=*/0), 0);
    EXPECT_TRUE(rail.isAvailable(/*local_nic=*/0, /*remote_nic=*/1));
    EXPECT_FALSE(rail.isAvailable(/*local_nic=*/0, /*remote_nic=*/0));
}

TEST(RailMonitorConfigTest, UnknownRemoteNumaSkipsUnavailableDirectRail) {
    auto local = makeTwoNicTopology("local0", "local1");
    auto remote = makeTwoNicTopology("remote0", "remote1");
    const std::string rail_json = R"({
        "all": [
            {"local": "local0", "remote": "remote1"},
            {"local": "local0", "remote": "remote0"},
            {"local": "local1", "remote": "remote0"}
        ],
        "direct": [
            {"local": "local0", "remote": "remote1"},
            {"local": "local1", "remote": "remote0"}
        ]
    })";

    RailMonitor rail;
    RailClient client(rail);
    ASSERT_TRUE(rail.load(local, remote, rail_json, nullptr).ok());
    for (int i = 0; i < 3; ++i)
        client.markFailed(/*local_nic=*/0, /*remote_nic=*/1);

    EXPECT_FALSE(rail.isAvailable(/*local_nic=*/0, /*remote_nic=*/1));
    EXPECT_TRUE(rail.isAvailable(/*local_nic=*/0, /*remote_nic=*/0));
    EXPECT_EQ(rail.findBestRemoteDevice(/*local_nic=*/0, /*remote_numa=*/-1),
              0);
}

TEST(RailMonitorConfigTest, AllocationMaskUsesConfiguredPairsAndTargetMemory) {
    auto local = makeTwoNicTopology("local0", "local1");
    auto remote = makeTwoNicTopology("remote0", "remote1");
    RailMonitor rail;
    ASSERT_TRUE(rail.load(local, remote, R"({"all":[
        {"local":"local0","remote":"remote1"},
        {"local":"local1","remote":"remote0"}]})")
                    .ok());
    Topology::MemEntry target;
    target.device_list[1] = {1};
    EXPECT_EQ(rail.localDeviceMask(target), 1ULL);
    target.device_list[1] = {0};
    EXPECT_EQ(rail.localDeviceMask(target), 2ULL);
    target.device_list[0] = {1};
    EXPECT_EQ(rail.localDeviceMask(target), 3ULL);
    target.device_list[0] = {-1, 99};
    target.device_list[1].clear();
    EXPECT_EQ(rail.localDeviceMask(target), 0ULL);
}

TEST(RailMonitorConfigTest, AllocationMaskIsPeerSpecific) {
    auto local = makeTwoNicTopology("local0", "local1");
    const std::string config = R"({"all":[
        {"local":"local0","remote":"peerA"},
        {"local":"local1","remote":"peerB"}]})";
    RailMonitor a, b;
    auto peer_a = makeSingleNicTopology("peerA");
    auto peer_b = makeSingleNicTopology("peerB");
    ASSERT_TRUE(a.load(local, peer_a, config).ok());
    ASSERT_TRUE(b.load(local, peer_b, config).ok());
    EXPECT_EQ(a.localDeviceMask(*peer_a->getMemEntry(0)), 1ULL);
    EXPECT_EQ(b.localDeviceMask(*peer_b->getMemEntry(0)), 2ULL);
}

TEST(RailMonitorConfigTest, AllocationMaskPreservesDefaultAndEmptyRails) {
    auto local = makeTwoNicTopology("local0", "local1");
    auto remote = makeSingleNicTopology("remote");
    for (const auto& config : {std::string{}, std::string{"invalid-json"},
                               std::string{R"({"all":[],"direct":[]})"}}) {
        SCOPED_TRACE(config);
        RailMonitor rail;
        ASSERT_TRUE(rail.load(local, remote, config).ok());
        EXPECT_EQ(rail.localDeviceMask(*remote->getMemEntry(0)),
                  config.find("all") == std::string::npos ? 3ULL : 0ULL);
    }
}

TEST(RailMonitorConfigTest, AllocationMaskDoesNotConsumeRecoveryProbe) {
    auto local = makeSingleNicTopology("local");
    auto remote = makeSingleNicTopology("remote");
    Config config;
    config.set(RailMonitor::kCfgErrorThreshold, 1);
    config.set(RailMonitor::kCfgProbeIntervalSecs, 0);
    RailMonitor rail;
    RailClient client(rail);
    ASSERT_TRUE(rail.load(local, remote, "", &config).ok());
    client.markFailed(0, 0);
    ASSERT_FALSE(rail.isAvailable(0, 0));
    for (int i = 0; i < 3; ++i)
        EXPECT_EQ(rail.localDeviceMask(*remote->getMemEntry(0)), 1ULL);
    EXPECT_TRUE(client.admit(0, 0));
    EXPECT_EQ(rail.localDeviceMask(*remote->getMemEntry(0)), 1ULL);
    EXPECT_FALSE(client.admit(0, 0));
}

// Build a 2-NIC topology (mlx5_a, mlx5_b) with per-NIC NUMA nodes, so the two
// sides can disagree on which NUMA a same-named NIC sits in — the asymmetric
// (overlay) situation from #2467.
static std::shared_ptr<Topology> makeNamedNumaTopology(const std::string& n0,
                                                       int numa0,
                                                       const std::string& n1,
                                                       int numa1) {
    auto json_str =
        R"({
        "nics": [
            {"name": ")" +
        n0 + R"(", "type": 0, "numa_node": )" + std::to_string(numa0) + R"(},
            {"name": ")" +
        n1 + R"(", "type": 0, "numa_node": )" + std::to_string(numa1) + R"(}
        ],
        "mems": [{
            "name": "host0",
            "type": 0,
            "numa_node": 0,
            "device_list": {"rank0": [0, 1]}
        }]
    })";
    auto topo = std::make_shared<Topology>();
    auto status = topo->parse(json_str);
    if (!status.ok()) {
        ADD_FAILURE() << "Topology::parse failed: " << status.ToString();
    }
    return topo;
}

// ---------------------------------------------------------------------------
// Cross-NUMA mapping must prefer a same-name remote device over a positional
// (i % remote_cnt) pick, so a local NIC is not routed to an unrelated remote
// NIC on a different physical/overlay network (issues #2758/#2467).
//
// Setup (asymmetric NUMA, as in #2467's overlay case):
//   local : mlx5_x @ NUMA 0 (idx0), mlx5_y @ NUMA 1 (idx1)
//   remote: mlx5_y @ NUMA 0 (idx0), mlx5_x @ NUMA 1 (idx1)
// Local mlx5_y sits in NUMA 1; its same-name remote mlx5_y sits in NUMA 0.
// Querying local mlx5_y (idx1) for the remote NUMA-0 domain is cross-NUMA and
// must pick the same-name remote mlx5_y (remote idx0). The positional bug would
// instead pick remote_devices[NUMA0][i]. With only one device in that domain
// they coincide, so we make the discriminating assertion below.
// ---------------------------------------------------------------------------

TEST(RailMonitorConcurrencyTest, LateCompletionsCannotResolveAnotherProbe) {
    auto topo = makeNamedNumaTopology("mlx5_a", 3, "mlx5_b", 7);
    for (int cooldown : {0, 30}) {
        SCOPED_TRACE(cooldown);  // Half-open trial and open-phase probe.
        RailMonitor rail;
        Config conf;
        conf.set(RailMonitor::kCfgErrorThreshold, 1);
        conf.set(RailMonitor::kCfgProbeIntervalSecs, 0);
        conf.set(RailMonitor::kCfgCooldownSecs, cooldown);
        ASSERT_TRUE(rail.load(topo, topo, "", &conf).ok());
        uint64_t ordinary, probe, denied = 123;
        ASSERT_TRUE(rail.admit(1, 1, ordinary));
        rail.markFailed(1, 1, ordinary);
        ASSERT_TRUE(rail.admit(1, 1, probe));
        ASSERT_NE(ordinary, probe);
        std::thread late([&] {
            for (int i = 0; i < 1000; ++i) {
                rail.cancelProbe(1, 1, ordinary);
                rail.markFailed(1, 1, ordinary);
                rail.markRecovered(1, 1, ordinary);
            }
        });
        for (int i = 0; i < 1000; ++i) {
            EXPECT_TRUE(rail.probeInFlight(1, 1));
            EXPECT_FALSE(rail.admit(1, 1, denied));
            EXPECT_EQ(denied, 0u);
            EXPECT_EQ(rail.findBestRemoteDevice(1, -1), 0);
            EXPECT_TRUE(rail.load(topo, topo, "", &conf).ok());
        }
        late.join();
        rail.cancelProbe(1, 1, probe);
        uint64_t replacement;
        ASSERT_TRUE(rail.admit(1, 1, replacement));
        ASSERT_NE(probe, replacement);
        rail.cancelProbe(1, 1, probe);
        rail.markFailed(1, 1, probe);
        rail.markRecovered(1, 1, probe);
        EXPECT_TRUE(rail.probeInFlight(1, 1));
        rail.markRecovered(1, 1, replacement);
        EXPECT_TRUE(rail.isAvailable(1, 1));
        EXPECT_EQ(rail.findBestRemoteDevice(1, -1), 1);
        rail.markFailed(1, 1, replacement);
        EXPECT_TRUE(rail.isAvailable(1, 1));  // Duplicate old completion.
    }
}

TEST(RailMonitorConcurrencyTest, OnlyOneConcurrentAdmissionOwnsRecovery) {
    auto topo = makeSingleNicTopology("mlx5_0");
    RailMonitor rail;
    Config conf;
    conf.set(RailMonitor::kCfgErrorThreshold, 1);
    conf.set(RailMonitor::kCfgProbeIntervalSecs, 0);
    ASSERT_TRUE(rail.load(topo, topo, "", &conf).ok());
    uint64_t ordinary;
    ASSERT_TRUE(rail.admit(0, 0, ordinary));
    rail.markFailed(0, 0, ordinary);
    std::atomic<bool> start{false};
    uint64_t admissions[8]{};
    std::vector<std::thread> threads;
    for (int i = 0; i < 8; ++i)
        threads.emplace_back([&, i] {
            while (!start.load(std::memory_order_acquire))
                std::this_thread::yield();
            rail.admit(0, 0, admissions[i]);
            rail.cancelProbe(0, 0, ordinary);
        });
    start.store(true, std::memory_order_release);
    for (auto& thread : threads) thread.join();
    int owners = 0;
    uint64_t winner = 0;
    for (auto token : admissions)
        if (token) {
            ++owners;
            winner = token;
        }
    ASSERT_EQ(owners, 1);
    rail.markRecovered(0, 0, winner);
    EXPECT_TRUE(rail.isAvailable(0, 0));
}

TEST(RailMonitorLoadTest, ReloadCannotReuseAnOldAdmission) {
    auto local = makeSingleNicTopology("local");
    auto old_remote = makeSingleNicTopology("old");
    auto new_remote = makeSingleNicTopology("new");
    RailMonitor rail;
    Config conf;
    conf.set(RailMonitor::kCfgErrorThreshold, 1);
    conf.set(RailMonitor::kCfgProbeIntervalSecs, 0);
    ASSERT_TRUE(rail.load(local, old_remote, "", &conf).ok());
    uint64_t old;
    ASSERT_TRUE(rail.admit(0, 0, old));
    ASSERT_TRUE(rail.load(local, new_remote, "", &conf).ok());
    rail.markFailed(0, 0, old);
    EXPECT_TRUE(rail.isAvailable(0, 0));
    uint64_t current, probe;
    ASSERT_TRUE(rail.admit(0, 0, current));
    rail.markFailed(0, 0, current);
    ASSERT_TRUE(rail.admit(0, 0, probe));
    rail.markRecovered(0, 0, old);
    rail.cancelProbe(0, 0, old);
    EXPECT_TRUE(rail.probeInFlight(0, 0));
    rail.markRecovered(0, 0, probe);
    EXPECT_TRUE(rail.isAvailable(0, 0));
}

TEST(RailMonitorCrossNumaTest, CrossNumaPrefersSameNameDevice) {
    // local NUMA-1 domain has one NIC: mlx5_y (idx1).
    auto local = makeNamedNumaTopology("mlx5_x", 0, "mlx5_y", 1);
    // remote NUMA-0 domain: mlx5_y (idx0); remote NUMA-1 domain: mlx5_x (idx1).
    auto remote = makeNamedNumaTopology("mlx5_y", 0, "mlx5_x", 1);
    RailMonitor rail;
    ASSERT_TRUE(rail.load(local, remote).ok());
    ASSERT_TRUE(rail.ready());

    // local mlx5_y (idx1, NUMA 1) reaching the remote NUMA-0 domain: the only
    // same-name device is remote mlx5_y at idx0. Must map there.
    EXPECT_EQ(rail.findBestRemoteDevice(/*local_nic=*/1, /*remote_numa=*/0), 0);

    // local mlx5_x (idx0, NUMA 0) reaching remote NUMA-1 domain: same-name
    // remote mlx5_x is at idx1. Must map there, not positionally to idx0.
    EXPECT_EQ(rail.findBestRemoteDevice(/*local_nic=*/0, /*remote_numa=*/1), 1);

    // With unknown target NUMA (wildcard memory location), the explicit
    // same-name direct rail remains the best peer across all remote NUMA
    // domains.
    EXPECT_EQ(rail.findBestRemoteDevice(/*local_nic=*/0, /*remote_numa=*/-1),
              1);
}

TEST(RailMonitorCrossNumaTest, UnknownRemoteNumaPrefersDirectPeer) {
    // The same-name peers are on the same NUMA node, but another remote NIC
    // occupies the first remote NUMA domain. A wildcard target has no NUMA
    // preference, so its mapping must still use the direct same-name peer.
    auto local = makeNamedNumaTopology("mlx5_x", 1, "mlx5_y", 0);
    auto remote = makeNamedNumaTopology("mlx5_x", 1, "mlx5_y", 0);
    RailMonitor rail;
    ASSERT_TRUE(rail.load(local, remote).ok());

    EXPECT_EQ(rail.findBestRemoteDevice(/*local_nic=*/0, /*remote_numa=*/-1),
              0);
}

// ---------------------------------------------------------------------------
// markRecovered resets error_count so failures start accumulating fresh
// ---------------------------------------------------------------------------

TEST(RailMonitorRecoverTest, RecoverResetsErrorCount) {
    auto local = makeSingleNicTopology("mlx5_0");
    auto remote = makeSingleNicTopology("mlx5_1");
    RailMonitor rail;
    RailClient client(rail);
    ASSERT_TRUE(rail.load(local, remote).ok());
    ASSERT_TRUE(rail.ready());

    // Initially available
    EXPECT_TRUE(rail.isAvailable(0, 0));

    // One failure — not yet past default threshold (3)
    client.markFailed(0, 0);
    EXPECT_TRUE(rail.isAvailable(0, 0));  // error_count=1, not paused

    // A successful transfer — reset error_count back to 0
    client.markRecovered(0, 0);
    EXPECT_TRUE(rail.isAvailable(0, 0));

    // Failure again — counter starts fresh from 0, one hit is not enough
    client.markFailed(0, 0);
    EXPECT_TRUE(rail.isAvailable(0, 0));
}

// ---------------------------------------------------------------------------
// markRecovered un-pauses a rail that reached the failure threshold
// ---------------------------------------------------------------------------

TEST(RailMonitorRecoverTest, RecoverUnpausesPausedRail) {
    auto local = makeSingleNicTopology("mlx5_0");
    auto remote = makeSingleNicTopology("mlx5_1");
    RailMonitor rail;
    RailClient client(rail);
    Config cfg;
    cfg.set(RailMonitor::kCfgProbeIntervalSecs, 0);
    ASSERT_TRUE(rail.load(local, remote, "", &cfg).ok());

    // Drive error_count to the default threshold (3) to trigger pause
    for (int i = 0; i < 3; ++i) client.markFailed(0, 0);
    EXPECT_FALSE(rail.isAvailable(0, 0))
        << "Rail should be paused after 3 failures";

    // A successful transfer proves the path is live — should un-pause
    // immediately
    ASSERT_TRUE(client.admit(0, 0));
    client.markRecovered(0, 0);
    EXPECT_TRUE(rail.isAvailable(0, 0))
        << "Rail should be available after recovery";
}

// ---------------------------------------------------------------------------
// markRecovered on an unknown NIC pair is a no-op (no crash / no assert)
// ---------------------------------------------------------------------------

TEST(RailMonitorRecoverTest, RecoverUnknownPairIsNoop) {
    auto local = makeSingleNicTopology("mlx5_0");
    auto remote = makeSingleNicTopology("mlx5_1");
    RailMonitor rail;
    RailClient client(rail);
    ASSERT_TRUE(rail.load(local, remote).ok());

    // NIC IDs 5 and 7 are not in the topology — must not crash
    EXPECT_NO_FATAL_FAILURE(client.markRecovered(5, 7));
}

TEST(RailMonitorLifetimeTest, KeepsSegmentSnapshotsAliveForFailureUpdates) {
    auto local = makeSingleNicSegment("mlx5_0");
    auto remote = makeSingleNicSegment("mlx5_1");
    std::weak_ptr<SegmentDesc> weak_local = local;
    std::weak_ptr<SegmentDesc> weak_remote = remote;
    auto* local_topology = &local->getMemory().topology;
    auto* remote_topology = &remote->getMemory().topology;

    {
        RailMonitor rail;
        RailClient client(rail);
        ASSERT_TRUE(
            rail.load(std::shared_ptr<const Topology>(local, local_topology),
                      std::shared_ptr<const Topology>(remote, remote_topology))
                .ok());
        local.reset();
        remote.reset();

        EXPECT_FALSE(weak_local.expired());
        EXPECT_FALSE(weak_remote.expired());
        EXPECT_NO_FATAL_FAILURE({
            for (int i = 0; i < 3; ++i) client.markFailed(0, 0);
        });
        EXPECT_FALSE(rail.isAvailable(0, 0));
    }

    EXPECT_TRUE(weak_local.expired());
    EXPECT_TRUE(weak_remote.expired());
}

// ---------------------------------------------------------------------------
// After recovery, findBestRemoteDevice maps back to the (only) available rail
// ---------------------------------------------------------------------------

TEST(RailMonitorRecoverTest, FindBestAfterRecovery) {
    auto local = makeSingleNicTopology("mlx5_0");
    auto remote = makeSingleNicTopology("mlx5_1");
    RailMonitor rail;
    RailClient client(rail);
    Config cfg;
    cfg.set(RailMonitor::kCfgProbeIntervalSecs, 0);
    ASSERT_TRUE(rail.load(local, remote, "", &cfg).ok());

    // Pause the only available rail
    for (int i = 0; i < 3; ++i) client.markFailed(0, 0);
    EXPECT_FALSE(rail.isAvailable(0, 0));

    // Recovery must rebuild best_mapping so findBestRemoteDevice works again
    ASSERT_TRUE(client.admit(0, 0));
    client.markRecovered(0, 0);
    EXPECT_TRUE(rail.isAvailable(0, 0));
    int best = rail.findBestRemoteDevice(/*local_nic=*/0, /*remote_numa=*/0);
    EXPECT_EQ(best, 0) << "Recovered rail should be the best remote device";
}

// ---------------------------------------------------------------------------
// After recovery, the cooldown on the next pause must start from the
// configured initial value, not a doubled value left over from the
// previous cycle.
//
// Uses error_threshold=1 and cooldown=1s so each single failure triggers
// a pause. If cooldown is correctly reset on recovery, the second pause
// expires in ~1s; if the cooldown had carried over (bug), the second
// pause would expire in ~2s.
// ---------------------------------------------------------------------------

// Segment metadata is copy-on-write: a new Topology object with the same
// NIC/memory wiring must not rebuild rail_states_ (that would reset
// error_count) and must not treat the reload as a first-time config log.
TEST(RailMonitorLoadTest, SameLayoutReloadPreservesErrorCount) {
    auto local1 = makeSingleNicTopology("mlx5_0");
    auto remote1 = makeSingleNicTopology("mlx5_1");
    auto local2 = makeSingleNicTopology("mlx5_0");
    auto remote2 = makeSingleNicTopology("mlx5_1");
    Config cfg;
    RailMonitor rail;
    RailClient client(rail);
    ASSERT_TRUE(rail.load(local1, remote1, "", &cfg).ok());

    client.markFailed(0, 0);
    client.markFailed(0, 0);
    EXPECT_TRUE(rail.isAvailable(0, 0))
        << "Two failures are below the default threshold of 3";

    ASSERT_TRUE(rail.load(local2, remote2, "", &cfg).ok());
    client.markFailed(0, 0);
    EXPECT_FALSE(rail.isAvailable(0, 0))
        << "COW snapshot refresh must not reset rail error_count";
}

TEST(RailMonitorLoadTest, DifferentLayoutRebuildsMapping) {
    auto local = makeSingleNicTopology("mlx5_0");
    auto remote_old = makeSingleNicTopology("mlx5_1");
    auto remote_new = makeSingleNicTopology("mlx5_2");
    RailMonitor rail;
    RailClient client(rail);
    ASSERT_TRUE(rail.load(local, remote_old).ok());
    for (int i = 0; i < 3; ++i) client.markFailed(0, 0);
    EXPECT_FALSE(rail.isAvailable(0, 0));

    ASSERT_TRUE(rail.load(local, remote_new).ok());
    EXPECT_TRUE(rail.isAvailable(0, 0))
        << "A real topology change must rebuild rails from a clean state";
}

TEST(RailMonitorRecoverTest, CooldownDoesNotCarryOverAfterRecovery) {
    auto local = makeSingleNicTopology("mlx5_0");
    auto remote = makeSingleNicTopology("mlx5_1");

    Config cfg;
    cfg.set(RailMonitor::kCfgErrorThreshold, 1);      // pause on first failure
    cfg.set(RailMonitor::kCfgErrorWindowSecs, 60);    // wide: no window resets
    cfg.set(RailMonitor::kCfgCooldownSecs, 1);        // small initial cooldown
    cfg.set(RailMonitor::kCfgProbeIntervalSecs, 60);  // disable probing

    RailMonitor rail;
    RailClient client(rail);
    ASSERT_TRUE(rail.load(local, remote, "", &cfg).ok());

    // First pause cycle: single failure arms resume_time at now+1s.
    client.markFailed(0, 0);
    EXPECT_FALSE(rail.isAvailable(0, 0));

    // A successful half-open trial resets the next pause to 1s.
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    ASSERT_TRUE(client.admit(0, 0));
    client.markRecovered(0, 0);
    EXPECT_TRUE(rail.isAvailable(0, 0));

    // Second pause cycle: single failure must arm resume_time at now+1s.
    client.markFailed(0, 0);
    EXPECT_FALSE(rail.isAvailable(0, 0));

    // Wait 1.5s: longer than the initial 1s cooldown, shorter than the
    // 2s value the bug would produce. If cooldown was correctly reset on
    // recovery, the cooldown expired and admit() admits a Half-Open trial;
    // if it carried over, admit() stays false until ~2s elapses.
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
    EXPECT_TRUE(client.admit(0, 0))
        << "After recovery, the next pause must use the initial cooldown "
           "(1s); staying paused (admit=false) past 1.5s indicates cooldown "
           "carried over from the previous cycle.";
}

// ---------------------------------------------------------------------------
// Defect A: a burst of failures within one error window must not escalate the
// cooldown. Previously cooldown doubled on every markFailed call, so N error
// WQEs in 10s pushed a 1s pause to the 300s cap, forcing a multi-minute
// TCP fallback after the peer had already recovered. Now the cooldown is set
// once when a fresh pause arms; errors arriving while already paused are
// no-ops.
//
// Probing is disabled (probe_interval large) so a *cooling* rail's admit()
// returns false (now < resume_time, no probe) while an *expired* rail's
// admit() returns true (Half-Open trial). This discriminates the cooldown
// DURATION; the in-flight path is exercised separately by
// InFlightProbeBlocksSecondAdmit with the default probe_interval.
//
// error_threshold=1, cooldown=1s. 8 rapid markFailed calls must arm resume_time
// at now+1s, not now+256s.
// ---------------------------------------------------------------------------

TEST(RailMonitorBurstTest, BurstFailuresDoNotEscalateCooldown) {
    auto local = makeSingleNicTopology("mlx5_0");
    auto remote = makeSingleNicTopology("mlx5_1");

    Config cfg;
    cfg.set(RailMonitor::kCfgErrorThreshold, 1);
    cfg.set(RailMonitor::kCfgErrorWindowSecs, 60);
    cfg.set(RailMonitor::kCfgCooldownSecs, 1);
    cfg.set(RailMonitor::kCfgProbeIntervalSecs, 60);  // disable probing

    RailMonitor rail;
    RailClient client(rail);
    ASSERT_TRUE(rail.load(local, remote, "", &cfg).ok());

    // A burst of 8 failures within the error window. With the old per-error
    // doubling, cooldown would be 1->2->4->...->256s (capped 300). With the
    // fix, only the first failure arms the pause at +1s; the rest are no-ops.
    for (int i = 0; i < 8; ++i) client.markFailed(0, 0);
    EXPECT_FALSE(rail.isAvailable(0, 0));
    EXPECT_FALSE(client.admit(0, 0))
        << "Cooling rail must not admit (no probe)";

    // 1.5s > 1s initial cooldown, far below any escalated value. If the burst
    // had escalated, the rail would still be cooling and admit()=false here.
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
    EXPECT_TRUE(client.admit(0, 0))
        << "A single failure burst must not escalate the cooldown past the "
           "initial 1s; staying paused (admit=false) past 1.5s indicates "
           "per-error doubling.";
}

// ---------------------------------------------------------------------------
// Half-Open: escalation happens on the trial RESULT, not because the clock
// fired. A Half-Open trial failure (markFailed while half_open) escalates the
// cooldown and re-arms; a trial success (markRecovered) closes the rail.
//
// Probing disabled so a cooling rail's admit()=false while an expired rail's
// admit()=true (trial). error_threshold=1, cooldown=1s.
// Cycle 1: pause 1s, expire -> trial; trial fails -> escalate 1->2s re-arm.
// Cycle 2: 2s cooldown, expire -> trial.
// ---------------------------------------------------------------------------

TEST(RailMonitorHalfOpenTest, TrialFailureEscalatesCooldown) {
    auto local = makeSingleNicTopology("mlx5_0");
    auto remote = makeSingleNicTopology("mlx5_1");

    Config cfg;
    cfg.set(RailMonitor::kCfgErrorThreshold, 1);
    cfg.set(RailMonitor::kCfgErrorWindowSecs, 60);
    cfg.set(RailMonitor::kCfgCooldownSecs, 1);
    cfg.set(RailMonitor::kCfgProbeIntervalSecs, 60);  // disable probing

    RailMonitor rail;
    RailClient client(rail);
    ASSERT_TRUE(rail.load(local, remote, "", &cfg).ok());

    // Cycle 1: single failure arms a 1s pause.
    client.markFailed(0, 0);
    EXPECT_FALSE(rail.isAvailable(0, 0));
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    ASSERT_TRUE(client.admit(0, 0)) << "Cycle 1 cooldown (1s) expired -> trial";

    // The trial fails: escalate 1->2s and re-arm.
    client.markFailed(0, 0);
    EXPECT_FALSE(rail.isAvailable(0, 0));
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    EXPECT_FALSE(client.admit(0, 0))
        << "Cycle 2 must use the escalated 2s cooldown; 1.1s is not enough.";
    std::this_thread::sleep_for(std::chrono::milliseconds(1400));
    EXPECT_TRUE(client.admit(0, 0))
        << "Cycle 2 (2s) expired -> trial by ~2.5s.";
}

// ---------------------------------------------------------------------------
// A successful Half-Open trial closes the rail and resets backoff, so the
// next failure starts from the initial cooldown, not a doubled leftover.
//
// Probing disabled. Pause, expire -> trial, trial succeeds (markRecovered).
// The next pause must use 1s, not 2s.
// ---------------------------------------------------------------------------

TEST(RailMonitorHalfOpenTest, TrialSuccessResetsBackoff) {
    auto local = makeSingleNicTopology("mlx5_0");
    auto remote = makeSingleNicTopology("mlx5_1");

    Config cfg;
    cfg.set(RailMonitor::kCfgErrorThreshold, 1);
    cfg.set(RailMonitor::kCfgErrorWindowSecs, 60);
    cfg.set(RailMonitor::kCfgCooldownSecs, 1);
    cfg.set(RailMonitor::kCfgProbeIntervalSecs, 60);

    RailMonitor rail;
    RailClient client(rail);
    ASSERT_TRUE(rail.load(local, remote, "", &cfg).ok());

    // Pause, let it expire to Half-Open, then the trial succeeds.
    client.markFailed(0, 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    ASSERT_TRUE(client.admit(0, 0)) << "Half-Open trial admitted";
    client.markRecovered(0, 0);
    EXPECT_TRUE(rail.isAvailable(0, 0)) << "Trial success must close the rail";

    // Next pause must use the initial 1s cooldown (backoff was reset), not 2s.
    client.markFailed(0, 0);
    EXPECT_FALSE(rail.isAvailable(0, 0));
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
    EXPECT_TRUE(client.admit(0, 0))
        << "After a successful trial, the next pause must use the initial 1s "
           "cooldown; staying paused (admit=false) past 1.5s indicates backoff "
           "carried over.";
}

// ---------------------------------------------------------------------------
// Expiry admits exactly ONE trial, not a full reopen. The second caller is
// refused while the trial is in flight, so a still-dead peer is not flooded
// with every slice -- the storm the breaker exists to prevent.
//
// Probing disabled. Pause 1s, expire, admit()=true (trial), admit()=false.
// ---------------------------------------------------------------------------

TEST(RailMonitorHalfOpenTest, ExpiryAdmitsOneTrialNotFullReopen) {
    auto local = makeSingleNicTopology("mlx5_0");
    auto remote = makeSingleNicTopology("mlx5_1");

    Config cfg;
    cfg.set(RailMonitor::kCfgErrorThreshold, 1);
    cfg.set(RailMonitor::kCfgErrorWindowSecs, 60);
    cfg.set(RailMonitor::kCfgCooldownSecs, 1);
    cfg.set(RailMonitor::kCfgProbeIntervalSecs, 60);

    RailMonitor rail;
    RailClient client(rail);
    ASSERT_TRUE(rail.load(local, remote, "", &cfg).ok());

    client.markFailed(0, 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    EXPECT_TRUE(client.admit(0, 0)) << "Expiry must admit one trial";
    EXPECT_FALSE(client.admit(0, 0))
        << "After the trial is admitted, the next caller must be refused; "
           "expiry must not reopen all traffic.";
}

// ---------------------------------------------------------------------------
// In-flight tracking (the bug the reviewer flagged): with the DEFAULT
// probe_interval (1s), admit() must block a second probe while the first is
// on the wire, and clear the flag on completion so a new probe can arm.
// This test uses the default probe_interval, NOT 60s, so it exercises the
// in-flight path directly.
// ---------------------------------------------------------------------------

TEST(RailMonitorProbeTest, InFlightProbeBlocksSecondAdmit) {
    auto local = makeSingleNicTopology("mlx5_0");
    auto remote = makeSingleNicTopology("mlx5_1");

    Config cfg;
    cfg.set(RailMonitor::kCfgErrorThreshold, 1);
    cfg.set(RailMonitor::kCfgErrorWindowSecs, 60);
    cfg.set(RailMonitor::kCfgCooldownSecs, 10);  // long cooldown
    // DEFAULT probe_interval (1s) -- do NOT set kCfgProbeIntervalSecs.

    RailMonitor rail;
    RailClient client(rail);
    ASSERT_TRUE(rail.load(local, remote, "", &cfg).ok());

    client.markFailed(0, 0);  // pause 10s; last_probe_time=now (deferral)
    // Wait one probe_interval so the first probe is eligible.
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    EXPECT_TRUE(client.admit(0, 0))
        << "Open-phase probe must arm after interval";
    EXPECT_FALSE(client.admit(0, 0))
        << "A second probe must be blocked while the first is in flight";

    // The probe succeeds: reopen Closed and clear the in-flight flag.
    client.markRecovered(0, 0);
    EXPECT_TRUE(rail.isAvailable(0, 0));

    // Re-pause and arm another probe: the flag was cleared, so a new probe
    // arms.
    client.markFailed(0, 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    EXPECT_TRUE(client.admit(0, 0))
        << "After completion clears the flag, a new probe must arm";
    EXPECT_FALSE(client.admit(0, 0)) << "Again blocked while in flight";
}

// ---------------------------------------------------------------------------
// cancelProbe reverts an armed probe/trial when the slice never reaches the
// wire (pre-wire failure). After cancelProbe the rail must re-admit a trial.
// ---------------------------------------------------------------------------

TEST(RailMonitorProbeTest, CancelProbeRevertsArmedTrial) {
    auto local = makeSingleNicTopology("mlx5_0");
    auto remote = makeSingleNicTopology("mlx5_1");

    Config cfg;
    cfg.set(RailMonitor::kCfgErrorThreshold, 1);
    cfg.set(RailMonitor::kCfgErrorWindowSecs, 60);
    cfg.set(RailMonitor::kCfgCooldownSecs, 1);
    cfg.set(RailMonitor::kCfgProbeIntervalSecs, 60);  // disable probing

    RailMonitor rail;
    RailClient client(rail);
    ASSERT_TRUE(rail.load(local, remote, "", &cfg).ok());

    client.markFailed(0, 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    ASSERT_TRUE(client.admit(0, 0)) << "Expiry -> trial admitted";
    EXPECT_FALSE(client.admit(0, 0)) << "Trial in flight blocks the second";

    // The slice failed pre-wire: cancel the trial. The rail reverts to
    // expired-Open and re-admits a fresh trial.
    client.cancelProbe(0, 0);
    EXPECT_TRUE(client.admit(0, 0)) << "cancelProbe must allow a fresh trial";
    EXPECT_FALSE(client.admit(0, 0)) << "Re-armed trial is again in flight";

    // cancelProbe is a no-op when no probe is in flight.
    client.markRecovered(0, 0);
    client.cancelProbe(0, 0);
    EXPECT_TRUE(rail.isAvailable(0, 0)) << "cancelProbe no-op on a Closed rail";
}

}  // namespace
}  // namespace tent
}  // namespace mooncake

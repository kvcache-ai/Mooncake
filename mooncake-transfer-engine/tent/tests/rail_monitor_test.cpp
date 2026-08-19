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

#include <chrono>
#include <thread>

#include "tent/common/config.h"
#include "tent/transport/rdma/rail_monitor.h"
#include "tent/runtime/topology.h"

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
    ASSERT_TRUE(rail.load(local.get(), remote.get(), rail_json, nullptr).ok());
    EXPECT_EQ(rail.findBestRemoteDevice(/*local_nic=*/0, /*remote_numa=*/0), 1);
    EXPECT_EQ(rail.findBestRemoteDevice(/*local_nic=*/1, /*remote_numa=*/0), 0);
    EXPECT_TRUE(rail.available(/*local_nic=*/0, /*remote_nic=*/1));
    EXPECT_FALSE(rail.available(/*local_nic=*/0, /*remote_nic=*/0));
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

TEST(RailMonitorCrossNumaTest, CrossNumaPrefersSameNameDevice) {
    // local NUMA-1 domain has one NIC: mlx5_y (idx1).
    auto local = makeNamedNumaTopology("mlx5_x", 0, "mlx5_y", 1);
    // remote NUMA-0 domain: mlx5_y (idx0); remote NUMA-1 domain: mlx5_x (idx1).
    auto remote = makeNamedNumaTopology("mlx5_y", 0, "mlx5_x", 1);
    RailMonitor rail;
    ASSERT_TRUE(rail.load(local.get(), remote.get()).ok());
    ASSERT_TRUE(rail.ready());

    // local mlx5_y (idx1, NUMA 1) reaching the remote NUMA-0 domain: the only
    // same-name device is remote mlx5_y at idx0. Must map there.
    EXPECT_EQ(rail.findBestRemoteDevice(/*local_nic=*/1, /*remote_numa=*/0), 0);

    // local mlx5_x (idx0, NUMA 0) reaching remote NUMA-1 domain: same-name
    // remote mlx5_x is at idx1. Must map there, not positionally to idx0.
    EXPECT_EQ(rail.findBestRemoteDevice(/*local_nic=*/0, /*remote_numa=*/1), 1);
}

// ---------------------------------------------------------------------------
// markRecovered resets error_count so failures start accumulating fresh
// ---------------------------------------------------------------------------

TEST(RailMonitorRecoverTest, RecoverResetsErrorCount) {
    auto local = makeSingleNicTopology("mlx5_0");
    auto remote = makeSingleNicTopology("mlx5_1");
    RailMonitor rail;
    ASSERT_TRUE(rail.load(local.get(), remote.get()).ok());
    ASSERT_TRUE(rail.ready());

    // Initially available
    EXPECT_TRUE(rail.available(0, 0));

    // One failure — not yet past default threshold (3)
    rail.markFailed(0, 0);
    EXPECT_TRUE(rail.available(0, 0));  // error_count=1, not paused

    // A successful transfer — reset error_count back to 0
    rail.markRecovered(0, 0);
    EXPECT_TRUE(rail.available(0, 0));

    // Failure again — counter starts fresh from 0, one hit is not enough
    rail.markFailed(0, 0);
    EXPECT_TRUE(rail.available(0, 0));
}

// ---------------------------------------------------------------------------
// markRecovered un-pauses a rail that reached the failure threshold
// ---------------------------------------------------------------------------

TEST(RailMonitorRecoverTest, RecoverUnpausesPausedRail) {
    auto local = makeSingleNicTopology("mlx5_0");
    auto remote = makeSingleNicTopology("mlx5_1");
    RailMonitor rail;
    ASSERT_TRUE(rail.load(local.get(), remote.get()).ok());

    // Drive error_count to the default threshold (3) to trigger pause
    for (int i = 0; i < 3; ++i) rail.markFailed(0, 0);
    EXPECT_FALSE(rail.available(0, 0))
        << "Rail should be paused after 3 failures";

    // A successful transfer proves the path is live — should un-pause
    // immediately
    rail.markRecovered(0, 0);
    EXPECT_TRUE(rail.available(0, 0))
        << "Rail should be available after recovery";
}

// ---------------------------------------------------------------------------
// markRecovered on an unknown NIC pair is a no-op (no crash / no assert)
// ---------------------------------------------------------------------------

TEST(RailMonitorRecoverTest, RecoverUnknownPairIsNoop) {
    auto local = makeSingleNicTopology("mlx5_0");
    auto remote = makeSingleNicTopology("mlx5_1");
    RailMonitor rail;
    ASSERT_TRUE(rail.load(local.get(), remote.get()).ok());

    // NIC IDs 5 and 7 are not in the topology — must not crash
    EXPECT_NO_FATAL_FAILURE(rail.markRecovered(5, 7));
}

// ---------------------------------------------------------------------------
// After recovery, findBestRemoteDevice maps back to the (only) available rail
// ---------------------------------------------------------------------------

TEST(RailMonitorRecoverTest, FindBestAfterRecovery) {
    auto local = makeSingleNicTopology("mlx5_0");
    auto remote = makeSingleNicTopology("mlx5_1");
    RailMonitor rail;
    ASSERT_TRUE(rail.load(local.get(), remote.get()).ok());

    // Pause the only available rail
    for (int i = 0; i < 3; ++i) rail.markFailed(0, 0);
    EXPECT_FALSE(rail.available(0, 0));

    // Recovery must rebuild best_mapping so findBestRemoteDevice works again
    rail.markRecovered(0, 0);
    EXPECT_TRUE(rail.available(0, 0));
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

TEST(RailMonitorRecoverTest, CooldownDoesNotCarryOverAfterRecovery) {
    auto local = makeSingleNicTopology("mlx5_0");
    auto remote = makeSingleNicTopology("mlx5_1");

    Config cfg;
    cfg.set(RailMonitor::kCfgErrorThreshold, 1);    // pause on first failure
    cfg.set(RailMonitor::kCfgErrorWindowSecs, 60);  // wide: no window resets
    cfg.set(RailMonitor::kCfgCooldownSecs, 1);      // small initial cooldown

    RailMonitor rail;
    ASSERT_TRUE(rail.load(local.get(), remote.get(), "", &cfg).ok());

    // First pause cycle: single failure arms resume_time at now+1s.
    rail.markFailed(0, 0);
    EXPECT_FALSE(rail.available(0, 0));

    // Recover: must clear st.cooldown so the next pause uses 1s again,
    // not the 1s left over from cycle 1 (which would double to 2s).
    rail.markRecovered(0, 0);
    EXPECT_TRUE(rail.available(0, 0));

    // Second pause cycle: single failure must arm resume_time at now+1s.
    rail.markFailed(0, 0);
    EXPECT_FALSE(rail.available(0, 0));

    // Wait 1.5s: longer than the initial 1s cooldown, shorter than the
    // 2s value the bug would produce. If cooldown was correctly reset on
    // recovery, available() returns true; if it carried over, available()
    // stays false until ~2s elapses.
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
    EXPECT_TRUE(rail.available(0, 0))
        << "After recovery, the next pause must use the initial cooldown "
           "(1s); staying paused past 1.5s indicates cooldown carried over "
           "from the previous cycle.";
}

}  // namespace
}  // namespace tent
}  // namespace mooncake

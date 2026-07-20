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

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
    ASSERT_TRUE(rail.load(local, remote).ok());
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
    ASSERT_TRUE(rail.load(local, remote).ok());
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
    ASSERT_TRUE(rail.load(local, remote).ok());

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
    ASSERT_TRUE(rail.load(local, remote).ok());

    // NIC IDs 5 and 7 are not in the topology — must not crash
    EXPECT_NO_FATAL_FAILURE(rail.markRecovered(5, 7));
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
        ASSERT_TRUE(
            rail.load(std::shared_ptr<const Topology>(local, local_topology),
                      std::shared_ptr<const Topology>(remote, remote_topology))
                .ok());
        local.reset();
        remote.reset();

        EXPECT_FALSE(weak_local.expired());
        EXPECT_FALSE(weak_remote.expired());
        EXPECT_NO_FATAL_FAILURE({
            for (int i = 0; i < 3; ++i) rail.markFailed(0, 0);
        });
        EXPECT_FALSE(rail.available(0, 0));
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
    ASSERT_TRUE(rail.load(local, remote).ok());

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
    ASSERT_TRUE(rail.load(local1, remote1, "", &cfg).ok());

    rail.markFailed(0, 0);
    rail.markFailed(0, 0);
    EXPECT_TRUE(rail.available(0, 0))
        << "Two failures are below the default threshold of 3";

    ASSERT_TRUE(rail.load(local2, remote2, "", &cfg).ok());
    rail.markFailed(0, 0);
    EXPECT_FALSE(rail.available(0, 0))
        << "COW snapshot refresh must not reset rail error_count";
}

TEST(RailMonitorLoadTest, DifferentLayoutRebuildsMapping) {
    auto local = makeSingleNicTopology("mlx5_0");
    auto remote_old = makeSingleNicTopology("mlx5_1");
    auto remote_new = makeSingleNicTopology("mlx5_2");
    RailMonitor rail;
    ASSERT_TRUE(rail.load(local, remote_old).ok());
    for (int i = 0; i < 3; ++i) rail.markFailed(0, 0);
    EXPECT_FALSE(rail.available(0, 0));

    ASSERT_TRUE(rail.load(local, remote_new).ok());
    EXPECT_TRUE(rail.available(0, 0))
        << "A real topology change must rebuild rails from a clean state";
}

TEST(RailMonitorRecoverTest, CooldownDoesNotCarryOverAfterRecovery) {
    auto local = makeSingleNicTopology("mlx5_0");
    auto remote = makeSingleNicTopology("mlx5_1");

    Config cfg;
    cfg.set(RailMonitor::kCfgErrorThreshold, 1);    // pause on first failure
    cfg.set(RailMonitor::kCfgErrorWindowSecs, 60);  // wide: no window resets
    cfg.set(RailMonitor::kCfgCooldownSecs, 1);      // small initial cooldown

    RailMonitor rail;
    ASSERT_TRUE(rail.load(local, remote, "", &cfg).ok());

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

// ---------------------------------------------------------------------------
// Defect A: a burst of failures within one error window must not escalate the
// cooldown. Previously cooldown doubled on every markFailed call, so N error
// WQEs in 10s pushed a 1s pause to the 300s cap, forcing a multi-minute
// TCP fallback after the peer had already recovered. Now the cooldown is set
// once when a fresh pause arms; errors arriving while already paused are
// no-ops.
//
// error_threshold=1, cooldown=1s, probe_interval disabled (60s). 8 rapid
// markFailed calls must arm resume_time at now+1s, not now+256s.
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
    ASSERT_TRUE(rail.load(local, remote, "", &cfg).ok());

    // A burst of 8 failures within the error window. With the old per-error
    // doubling, cooldown would be 1->2->4->...->256s (capped 300). With the
    // fix, only the first failure arms the pause at +1s; the rest are no-ops.
    for (int i = 0; i < 8; ++i) rail.markFailed(0, 0);
    EXPECT_FALSE(rail.available(0, 0));

    // 1.5s > 1s initial cooldown, far below any escalated value. If the burst
    // had escalated, the rail would still be paused here.
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
    EXPECT_TRUE(rail.available(0, 0))
        << "A single failure burst must not escalate the cooldown past the "
           "initial 1s; staying paused past 1.5s indicates per-error doubling.";
}

// ---------------------------------------------------------------------------
// Core fix: when the cooldown timer expires, available() must NOT reopen the
// rail to all traffic. It admits exactly ONE trial transfer (Half-Open) and
// refuses the next caller, so a still-dead peer is not slammed with every
// slice — that was the storm the breaker exists to prevent, previously
// recurring at 30s/60s/120s intervals.
//
// error_threshold=1, cooldown=1s, probe_interval disabled (60s) so the
// Half-Open trial admits exactly one and does not re-admit within the test.
// ---------------------------------------------------------------------------

TEST(RailMonitorHalfOpenTest, ExpiryAdmitsOneTrialNotFullReopen) {
    auto local = makeSingleNicTopology("mlx5_0");
    auto remote = makeSingleNicTopology("mlx5_1");

    Config cfg;
    cfg.set(RailMonitor::kCfgErrorThreshold, 1);
    cfg.set(RailMonitor::kCfgErrorWindowSecs, 60);
    cfg.set(RailMonitor::kCfgCooldownSecs, 1);
    cfg.set(RailMonitor::kCfgProbeIntervalSecs, 60);  // one trial, no re-admit

    RailMonitor rail;
    ASSERT_TRUE(rail.load(local, remote, "", &cfg).ok());

    rail.markFailed(0, 0);
    EXPECT_FALSE(rail.available(0, 0));

    // Cooldown (1s) expires. The first caller gets the single Half-Open trial.
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    EXPECT_TRUE(rail.available(0, 0)) << "Expiry must admit one trial";

    // The very next caller must NOT get through — the rail is Half-Open, not
    // fully reopened. A full reopen (the old bug) would return true here and
    // flood a still-dead peer.
    EXPECT_FALSE(rail.available(0, 0))
        << "After the trial is admitted, subsequent callers must be refused; "
           "expiry must not reopen all traffic.";
}

// ---------------------------------------------------------------------------
// Half-Open trial failure escalates the cooldown and re-arms the pause.
// Escalation happens on the PROBE/TRIAL RESULT, not because the clock fired
// (elapsed time does not prove the path is healthy).
//
// error_threshold=1, cooldown=1s, probe disabled. Cycle 1: pause 1s, expire,
// trial fails -> escalate 1->2s. Cycle 2: 2s cooldown, expire, trial admitted.
// ---------------------------------------------------------------------------

TEST(RailMonitorHalfOpenTest, TrialFailureEscalatesCooldown) {
    auto local = makeSingleNicTopology("mlx5_0");
    auto remote = makeSingleNicTopology("mlx5_1");

    Config cfg;
    cfg.set(RailMonitor::kCfgErrorThreshold, 1);
    cfg.set(RailMonitor::kCfgErrorWindowSecs, 60);
    cfg.set(RailMonitor::kCfgCooldownSecs, 1);
    cfg.set(RailMonitor::kCfgProbeIntervalSecs,
            60);  // disable exploratory probes

    RailMonitor rail;
    ASSERT_TRUE(rail.load(local, remote, "", &cfg).ok());

    // Cycle 1: single failure arms a 1s pause.
    rail.markFailed(0, 0);
    EXPECT_FALSE(rail.available(0, 0));
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    ASSERT_TRUE(rail.available(0, 0)) << "Cycle 1 cooldown (1s) must expire";

    // The trial admitted at expiry fails: escalation 1->2s, re-arm.
    rail.markFailed(0, 0);
    EXPECT_FALSE(rail.available(0, 0));
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    EXPECT_FALSE(rail.available(0, 0))
        << "Cycle 2 must use the escalated 2s cooldown; 1.1s is not enough.";
    std::this_thread::sleep_for(std::chrono::milliseconds(1400));
    EXPECT_TRUE(rail.available(0, 0)) << "Cycle 2 (2s) must expire by ~2.5s.";
}

// ---------------------------------------------------------------------------
// Half-Open trial success closes the rail and resets backoff, so the next
// failure starts from the initial cooldown, not a doubled leftover.
//
// error_threshold=1, cooldown=1s, probe disabled. Pause, expire to Half-Open,
// trial succeeds (markRecovered). The next pause must use 1s, not 2s.
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
    ASSERT_TRUE(rail.load(local, remote, "", &cfg).ok());

    // Pause, let it expire to Half-Open, then the trial succeeds.
    rail.markFailed(0, 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    ASSERT_TRUE(rail.available(0, 0)) << "Half-Open trial admitted";
    rail.markRecovered(0, 0);
    EXPECT_TRUE(rail.available(0, 0)) << "Trial success must close the rail";

    // Next pause must use the initial 1s cooldown (backoff was reset), not 2s
    // that a leftover-from-Half-Open value would produce.
    rail.markFailed(0, 0);
    EXPECT_FALSE(rail.available(0, 0));
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
    EXPECT_TRUE(rail.available(0, 0))
        << "After a successful trial, the next pause must use the initial 1s "
           "cooldown; staying paused past 1.5s indicates backoff carried over.";
}

// ---------------------------------------------------------------------------
// Defect B: while a rail is paused, available() admits one transfer every
// probe_interval as a recovery probe. A probe that succeeds (simulated via
// markRecovered) un-pauses early, instead of waiting out the full cooldown.
// ---------------------------------------------------------------------------

TEST(RailMonitorProbeTest, ProbeAdmitsTransferDuringCooldown) {
    auto local = makeSingleNicTopology("mlx5_0");
    auto remote = makeSingleNicTopology("mlx5_1");

    Config cfg;
    cfg.set(RailMonitor::kCfgErrorThreshold, 1);
    cfg.set(RailMonitor::kCfgErrorWindowSecs, 60);
    cfg.set(RailMonitor::kCfgCooldownSecs, 10);  // long cooldown
    cfg.set(RailMonitor::kCfgProbeIntervalSecs, 1);

    RailMonitor rail;
    ASSERT_TRUE(rail.load(local, remote, "", &cfg).ok());

    rail.markFailed(0, 0);
    // Immediately: probe throttled (last_probe_time armed to now on pause).
    EXPECT_FALSE(rail.available(0, 0));

    // After one probe_interval, available() admits a probe transfer.
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    EXPECT_TRUE(rail.available(0, 0))
        << "Probe must fire after probe_interval to test peer recovery.";

    // The probe slice succeeds (simulated): markRecovered clears the pause
    // well before the 10s cooldown would have elapsed.
    rail.markRecovered(0, 0);
    EXPECT_TRUE(rail.available(0, 0));
}

}  // namespace
}  // namespace tent
}  // namespace mooncake

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

// Unit tests for the DeviceSelector's per-NIC inflight accounting, the
// selector half of the charge/release contract the worker relies on:
//   * allocate charges each slice exactly the bytes it carries, so release
//     by slice length balances every NIC;
//   * chargeDevice / release are a matched pair on the device named, and a
//     charge against an untracked device fails without touching anything;
//   * a completion with no latency sample (cancel, timeout, failed CQ) moves
//     no EWMA; a successful one updates it with length / attempt latency;
//   * baseline (round-robin) mode never touches inflight, so it cannot
//     underflow.
// The slice-level half -- which device a slice's charge follows across a
// fallback or a retry -- lives in rdma_transport_test.cpp
// (RdmaWorkersChargeTest), where a Workers can be built without hardware.

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "tent/common/types.h"
#include "tent/runtime/topology.h"
#include "tent/transport/rdma/quota.h"

namespace mooncake {
namespace tent {
namespace {

constexpr uint64_t kNotFound = std::numeric_limits<uint64_t>::max();

// Two RDMA NICs (dev 0, 1) reachable from host location "cpu:0".
std::shared_ptr<Topology> MakeTopology() {
    static const char* kJson = R"json(
    {
      "nics": [
        {"name": "mlx5_0", "pci_bus_id": "0000:01:00.0", "type": 0,
         "numa_node": 0},
        {"name": "mlx5_1", "pci_bus_id": "0000:02:00.0", "type": 0,
         "numa_node": 0}
      ],
      "mems": [
        {"name": "cpu:0", "numa_node": 0,
         "device_list": {"rank0": [0, 1]}}
      ]
    })json";
    auto topo = std::make_shared<Topology>();
    EXPECT_TRUE(topo->parse(kJson).ok());
    return topo;
}

std::unique_ptr<DeviceSelector> MakeSelector(bool smart) {
    auto sel = std::make_unique<DeviceSelector>();
    auto topo = MakeTopology();
    EXPECT_TRUE(sel->loadTopology(topo).ok());
    sel->setSmartSelection(smart);
    return sel;
}

uint64_t InflightOf(DeviceSelector& sel, const std::string& name) {
    std::vector<NicLoadStats> stats;
    sel.getNicLoadStats(stats);
    for (const auto& s : stats)
        if (s.device_name == name) return s.inflight_bytes;
    return kNotFound;
}

uint64_t TotalInflight(DeviceSelector& sel) {
    std::vector<NicLoadStats> stats;
    sel.getNicLoadStats(stats);
    uint64_t total = 0;
    for (const auto& s : stats) total += s.inflight_bytes;
    return total;
}

double EwmaOf(DeviceSelector& sel, const std::string& name) {
    std::vector<NicLoadStats> stats;
    sel.getNicLoadStats(stats);
    for (const auto& s : stats)
        if (s.device_name == name) return s.ewma_bandwidth_bps;
    return -1.0;
}

}  // namespace

// ---- allocate charges what each slice carries; release by length balances

TEST(QuotaAccounting, AllocateChargesEachSliceItsOwnLength) {
    auto sel = MakeSelector(/*smart=*/true);
    std::vector<int> dev_ids;
    // 1000 bytes in 250-byte blocks: four slices of 250.
    ASSERT_TRUE(sel->allocate(1000, /*num_slices=*/4, /*slice_bytes=*/250,
                              "cpu:0", dev_ids)
                    .ok());
    ASSERT_EQ(dev_ids.size(), 4u);
    EXPECT_EQ(TotalInflight(*sel), 1000u);

    for (int dev : dev_ids) ASSERT_TRUE(sel->release(dev, 250, 0.0).ok());
    EXPECT_EQ(TotalInflight(*sel), 0u);
}

TEST(QuotaAccounting, ATrailingShortSliceIsChargedItsRealLength) {
    auto sel = MakeSelector(/*smart=*/true);
    std::vector<int> dev_ids;
    // 1000 bytes in 300-byte blocks: 300, 300, 300 and a 100-byte tail.
    ASSERT_TRUE(sel->allocate(1000, /*num_slices=*/4, /*slice_bytes=*/300,
                              "cpu:0", dev_ids)
                    .ok());
    ASSERT_EQ(dev_ids.size(), 4u);
    EXPECT_EQ(TotalInflight(*sel), 1000u);  // not 4 * 300

    for (size_t i = 0; i < dev_ids.size(); ++i)
        ASSERT_TRUE(sel->release(dev_ids[i], i == 3 ? 100 : 300, 0.0).ok());
    EXPECT_EQ(InflightOf(*sel, "mlx5_0"), 0u);
    EXPECT_EQ(InflightOf(*sel, "mlx5_1"), 0u);
}

// ---- chargeDevice / release are a matched pair on the device named ----

TEST(QuotaAccounting, ChargeDeviceAndReleaseBalance) {
    auto sel = MakeSelector(/*smart=*/true);
    ASSERT_TRUE(sel->chargeDevice(1, 256).ok());
    EXPECT_EQ(InflightOf(*sel, "mlx5_0"), 0u);
    EXPECT_EQ(InflightOf(*sel, "mlx5_1"), 256u);

    ASSERT_TRUE(sel->release(1, 256, 0.0).ok());
    EXPECT_EQ(TotalInflight(*sel), 0u);
}

TEST(QuotaAccounting, ChargeOnAnUntrackedDeviceFailsAndChangesNothing) {
    auto sel = MakeSelector(/*smart=*/true);
    EXPECT_FALSE(sel->chargeDevice(999, 4096).ok());
    EXPECT_EQ(TotalInflight(*sel), 0u);
    EXPECT_FALSE(sel->release(999, 4096, 0.0).ok());
    EXPECT_EQ(TotalInflight(*sel), 0u);
}

// ---- EWMA is only learned from successful completions ----

TEST(QuotaAccounting, NonSuccessCompletionDoesNotMoveEwma) {
    auto sel = MakeSelector(/*smart=*/true);
    const double before = EwmaOf(*sel, "mlx5_0");
    ASSERT_TRUE(sel->chargeDevice(0, 4096).ok());

    // cancel / timeout / failed CQ all release with latency <= 0.
    ASSERT_TRUE(sel->release(0, 4096, 0.0).ok());

    EXPECT_EQ(EwmaOf(*sel, "mlx5_0"), before);  // unchanged
    EXPECT_EQ(InflightOf(*sel, "mlx5_0"), 0u);
}

TEST(QuotaAccounting, SuccessUpdatesEwmaWithLengthOverLatency) {
    auto sel = MakeSelector(/*smart=*/true);
    const double before = EwmaOf(*sel, "mlx5_0");

    const uint64_t length = 100'000'000;  // 100 MB
    const double latency = 0.001;         // seconds (attempt latency)
    ASSERT_TRUE(sel->chargeDevice(0, length).ok());

    ASSERT_TRUE(sel->release(0, length, latency).ok());

    // observed_bw = length / latency; EWMA = a*old + (1-a)*observed, a = 0.01.
    const double observed = static_cast<double>(length) / latency;  // 1e11 B/s
    const double alpha = sel->getSchedulingParams().bandwidth_learning_rate;
    const double expected = alpha * before + (1.0 - alpha) * observed;
    EXPECT_NEAR(EwmaOf(*sel, "mlx5_0"), expected, expected * 1e-6);
    EXPECT_NE(EwmaOf(*sel, "mlx5_0"), before);
}

// ---- baseline (round-robin) mode never tracks inflight ----

TEST(QuotaAccounting, BaselineModeKeepsInflightZero) {
    auto sel = MakeSelector(/*smart=*/false);
    std::vector<int> dev_ids;
    ASSERT_TRUE(sel->allocate(4096, 8, 512, "cpu:0", dev_ids).ok());
    ASSERT_EQ(dev_ids.size(), 8u);
    EXPECT_EQ(TotalInflight(*sel), 0u);  // baseline charges nothing

    // Neither a re-charge nor a release moves (or underflows) inflight.
    ASSERT_TRUE(sel->chargeDevice(0, 512).ok());
    EXPECT_EQ(TotalInflight(*sel), 0u);
    for (int dev : dev_ids) ASSERT_TRUE(sel->release(dev, 512, 0.001).ok());
    EXPECT_EQ(InflightOf(*sel, "mlx5_0"), 0u);  // not ~2^64 (no underflow)
    EXPECT_EQ(InflightOf(*sel, "mlx5_1"), 0u);
}

}  // namespace tent
}  // namespace mooncake

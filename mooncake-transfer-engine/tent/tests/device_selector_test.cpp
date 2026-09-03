// Copyright 2025 KVCache.AI
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
//
// DeviceSelector bandwidth calibration: the per-device link speed must drive
// the EWMA seed and clamp instead of a one-size-fits-all constant.

#include "tent/transport/rdma/quota.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

namespace mooncake {
namespace tent {
namespace {

constexpr int kDev = 0;
constexpr uint64_t kMiB = 1ull << 20;

std::shared_ptr<Topology> oneRdmaNic() {
    auto topo = std::make_shared<Topology>();
    Topology::NicEntry nic;
    nic.name = "mlx5_0";
    nic.type = Topology::NIC_RDMA;
    nic.numa_node = 0;
    topo->nic_list_.push_back(nic);
    return topo;
}

// A selector over one RDMA NIC, with scheduling params applied the way
// Workers does it (topology first, then params).
std::unique_ptr<DeviceSelector> makeSelector(
    const DeviceSelector::SchedulingParams& params = {}) {
    auto topo = oneRdmaNic();
    auto sel = std::make_unique<DeviceSelector>();
    EXPECT_TRUE(sel->loadTopology(topo).ok());
    sel->setSchedulingParams(params);
    return sel;
}

// Feed one completion whose observed bandwidth is `bytes_per_sec`.
void observe(DeviceSelector& sel, double bytes_per_sec) {
    ASSERT_TRUE(sel.release(kDev, kMiB, kMiB / bytes_per_sec).ok());
}

TEST(DeviceSelectorBandwidthTest, LinkSpeedSeedsEwma) {
    auto sel = makeSelector();
    ASSERT_TRUE(sel->setDeviceBandwidth(kDev, 25.0).ok());
    // 25 Gbps = 3.125 GB/s, before a single byte has moved.
    EXPECT_DOUBLE_EQ(sel->getAggregateEwmaBandwidth(), 3.125e9);
}

TEST(DeviceSelectorBandwidthTest, UnknownLinkSpeedUsesConfiguredDefault) {
    DeviceSelector::SchedulingParams params;
    params.default_bandwidth_gbps = 100.0;
    auto sel = makeSelector(params);
    // 0 = the transport could not read the port speed.
    ASSERT_TRUE(sel->setDeviceBandwidth(kDev, 0.0).ok());
    EXPECT_DOUBLE_EQ(sel->getAggregateEwmaBandwidth(), 12.5e9);
}

TEST(DeviceSelectorBandwidthTest, OutOfRangeLinkSpeedUsesConfiguredDefault) {
    DeviceSelector::SchedulingParams params;
    params.default_bandwidth_gbps = 100.0;
    params.min_bandwidth_gbps = 10.0;
    params.max_bandwidth_gbps = 800.0;
    auto sel = makeSelector(params);
    ASSERT_TRUE(sel->setDeviceBandwidth(kDev, 5.0).ok());
    EXPECT_DOUBLE_EQ(sel->getAggregateEwmaBandwidth(), 12.5e9);
    ASSERT_TRUE(sel->setDeviceBandwidth(kDev, 1600.0).ok());
    EXPECT_DOUBLE_EQ(sel->getAggregateEwmaBandwidth(), 12.5e9);
}

TEST(DeviceSelectorBandwidthTest, UnknownDeviceIsRejected) {
    auto sel = makeSelector();
    EXPECT_FALSE(sel->setDeviceBandwidth(7, 100.0).ok());
}

// The bug this file exists for: with theoretical bandwidth fixed at 400 Gbps
// the EWMA floor was 5 GB/s everywhere, so a 25G link (3.1 GB/s measured)
// could never learn its real rate.
TEST(DeviceSelectorBandwidthTest, EwmaLearnsRealRateOn25GLink) {
    auto sel = makeSelector();
    ASSERT_TRUE(sel->setDeviceBandwidth(kDev, 25.0).ok());
    const double measured = 3.1e9;
    for (int i = 0; i < 64; ++i) observe(*sel, measured);
    EXPECT_NEAR(sel->getAggregateEwmaBandwidth(), measured, measured * 0.02);
}

TEST(DeviceSelectorBandwidthTest, EwmaClampScalesWithLinkSpeed) {
    auto sel = makeSelector();
    ASSERT_TRUE(sel->setDeviceBandwidth(kDev, 25.0).ok());
    // One absurd sample: the ceiling must be 10x the 25G link, not 10x 400G.
    observe(*sel, 1e13);
    EXPECT_DOUBLE_EQ(sel->getAggregateEwmaBandwidth(), 31.25e9);
    // And absurdly slow ones: the floor is 0.1x the 25G link.
    observe(*sel, 1e3);
    observe(*sel, 1e3);
    EXPECT_DOUBLE_EQ(sel->getAggregateEwmaBandwidth(), 0.3125e9);
}

// ---------------------------------------------------------------------------
// Availability: a NIC that cannot carry traffic (context never constructed,
// construct() failed, port down) must not be a selection candidate and must
// not count toward the aggregate the admission queue reads.
// ---------------------------------------------------------------------------

constexpr int kDev0 = 0;
constexpr int kDev1 = 1;

// Two RDMA NICs, both rank-0 for "cpu:0".
std::shared_ptr<Topology> twoRdmaNics() {
    auto topo = std::make_shared<Topology>();
    for (const char* name : {"mlx5_0", "mlx5_1"}) {
        Topology::NicEntry nic;
        nic.name = name;
        nic.type = Topology::NIC_RDMA;
        nic.numa_node = 0;
        topo->nic_list_.push_back(nic);
    }
    Topology::MemEntry mem;
    mem.name = "cpu:0";
    mem.type = Topology::MEM_HOST;
    mem.numa_node = 0;
    mem.device_list[0] = {kDev0, kDev1};
    topo->mem_list_.push_back(mem);
    return topo;
}

std::unique_ptr<DeviceSelector> makeTwoNicSelector(
    const DeviceSelector::SchedulingParams& params = {}) {
    auto topo = twoRdmaNics();
    auto sel = std::make_unique<DeviceSelector>();
    EXPECT_TRUE(sel->loadTopology(topo).ok());
    sel->setSchedulingParams(params);
    EXPECT_TRUE(sel->setDeviceBandwidth(kDev0, 100.0).ok());
    EXPECT_TRUE(sel->setDeviceBandwidth(kDev1, 100.0).ok());
    return sel;
}

// Allocate `rounds` single-slice requests and return how many landed on each
// device, releasing each so inflight never skews the next choice.
std::pair<int, int> countAllocations(DeviceSelector& sel, int rounds) {
    std::pair<int, int> hits{0, 0};
    for (int i = 0; i < rounds; ++i) {
        int dev = -1;
        if (!sel.allocate(kMiB, "cpu:0", dev).ok()) continue;
        if (dev == kDev0) ++hits.first;
        if (dev == kDev1) ++hits.second;
        sel.release(dev, kMiB, 0.0);
    }
    return hits;
}

TEST(DeviceSelectorAvailabilityTest, DevicesStartAvailable) {
    auto sel = makeTwoNicSelector();
    EXPECT_TRUE(sel->isDeviceAvailable(kDev0));
    EXPECT_TRUE(sel->isDeviceAvailable(kDev1));
    EXPECT_FALSE(sel->isDeviceAvailable(7));  // unknown device
    EXPECT_FALSE(sel->setDeviceAvailable(7, false).ok());
}

TEST(DeviceSelectorAvailabilityTest, UnavailableDeviceLeavesAggregate) {
    auto sel = makeTwoNicSelector();
    EXPECT_DOUBLE_EQ(sel->getAggregateEwmaBandwidth(), 25e9);
    ASSERT_TRUE(sel->setDeviceAvailable(kDev1, false).ok());
    EXPECT_DOUBLE_EQ(sel->getAggregateEwmaBandwidth(), 12.5e9);
    ASSERT_TRUE(sel->setDeviceAvailable(kDev1, true).ok());
    EXPECT_DOUBLE_EQ(sel->getAggregateEwmaBandwidth(), 25e9);
}

// getNicLoadStats() is the load signal upper layers score NICs with
// (#2996, for #2516). A NIC that cannot carry traffic must not appear in it
// at all: its seed is meaningless and, with zero inflight, would look like
// the best NIC of the lot -- the same bad pick the aggregate avoids.
TEST(DeviceSelectorAvailabilityTest, UnavailableDeviceLeavesLoadStats) {
    auto sel = makeTwoNicSelector();
    std::vector<NicLoadStats> stats;
    ASSERT_TRUE(sel->getNicLoadStats(stats).ok());
    ASSERT_EQ(stats.size(), 2u);

    ASSERT_TRUE(sel->setDeviceAvailable(kDev1, false).ok());
    stats.clear();
    ASSERT_TRUE(sel->getNicLoadStats(stats).ok());
    ASSERT_EQ(stats.size(), 1u);
    EXPECT_EQ(stats[0].device_name, "mlx5_0");

    ASSERT_TRUE(sel->setDeviceAvailable(kDev1, true).ok());
    stats.clear();
    ASSERT_TRUE(sel->getNicLoadStats(stats).ok());
    EXPECT_EQ(stats.size(), 2u);
}

TEST(DeviceSelectorAvailabilityTest, UnavailableDeviceIsNeverAllocated) {
    auto sel = makeTwoNicSelector();
    auto both = countAllocations(*sel, 64);
    ASSERT_GT(both.first, 0);
    ASSERT_GT(both.second, 0);  // sanity: both are picked when available

    ASSERT_TRUE(sel->setDeviceAvailable(kDev1, false).ok());
    auto smart = countAllocations(*sel, 64);
    EXPECT_EQ(smart.first, 64);
    EXPECT_EQ(smart.second, 0);

    sel->setSmartSelection(false);  // round-robin path has its own loop
    auto rr = countAllocations(*sel, 64);
    EXPECT_EQ(rr.first, 64);
    EXPECT_EQ(rr.second, 0);
}

TEST(DeviceSelectorAvailabilityTest, AllDevicesUnavailable) {
    auto sel = makeTwoNicSelector();
    ASSERT_TRUE(sel->setDeviceAvailable(kDev0, false).ok());
    ASSERT_TRUE(sel->setDeviceAvailable(kDev1, false).ok());
    int dev = -1;
    EXPECT_FALSE(sel->allocate(kMiB, "cpu:0", dev).ok());
    // No usable bandwidth: the admission queue must not drop on it.
    EXPECT_LT(sel->getAggregateEwmaBandwidth(), 0.0);
}

TEST(DeviceSelectorAvailabilityTest, PriorityFallbackKeepsUnavailableOut) {
    // Priority filtering can reject every candidate, after which
    // buildCandidates falls back to "all devices". That fallback must still
    // honor availability.
    DeviceSelector::SchedulingParams params;
    params.enable_priority_filtering = true;
    params.local_rotation_interval_us = 0;  // fixed priorities: dev0=0, dev1=1
    auto sel = makeTwoNicSelector(params);
    ASSERT_TRUE(sel->setDeviceAvailable(kDev1, false).ok());
    // Device priorities are 0 and 1, so request priority 2 rejects both in
    // the first pass; the fallback must then offer dev0 only.
    for (int i = 0; i < 16; ++i) {
        std::vector<int> dev_ids;
        ASSERT_TRUE(sel->allocate(kMiB, 1, kMiB, "cpu:0", dev_ids, 2).ok());
        ASSERT_EQ(dev_ids, std::vector<int>{kDev0});
        sel->release(kDev0, kMiB, 0.0);
    }
    ASSERT_TRUE(sel->setDeviceAvailable(kDev0, false).ok());
    std::vector<int> dev_ids;
    EXPECT_FALSE(sel->allocate(kMiB, 1, kMiB, "cpu:0", dev_ids, 2).ok());
}

// The reviewer's scenario for a link that renegotiates lower: the learned
// EWMA and the clamp derived from the old speed must both be replaced.
TEST(DeviceSelectorBandwidthTest, ReseedOnLinkSpeedChangeReleasesStaleClamp) {
    auto sel = makeSelector();
    ASSERT_TRUE(sel->setDeviceBandwidth(kDev, 400.0).ok());
    for (int i = 0; i < 64; ++i) observe(*sel, 45e9);
    ASSERT_NEAR(sel->getAggregateEwmaBandwidth(), 45e9, 45e9 * 0.02);

    ASSERT_TRUE(sel->setDeviceBandwidth(kDev, 25.0).ok());
    EXPECT_DOUBLE_EQ(sel->getAggregateEwmaBandwidth(), 3.125e9);
    for (int i = 0; i < 64; ++i) observe(*sel, 3.1e9);
    // With the 400G clamp still in place this would sit at the 5 GB/s floor.
    EXPECT_NEAR(sel->getAggregateEwmaBandwidth(), 3.1e9, 3.1e9 * 0.02);
}

// What one device has been charged and not yet released.
TEST(DeviceSelectorInflightTest, InflightBytesPerDevice) {
    auto topo = oneRdmaNic();
    Topology::MemEntry mem;
    mem.name = "cpu:0";
    mem.type = Topology::MEM_HOST;
    mem.numa_node = 0;
    mem.device_list[0].push_back(kDev);
    topo->mem_list_.push_back(mem);
    DeviceSelector sel;
    ASSERT_TRUE(sel.loadTopology(topo).ok());

    EXPECT_EQ(sel.getInflightBytes(kDev), 0u);
    int chosen = -1;
    ASSERT_TRUE(sel.allocate(kMiB, "cpu:0", chosen).ok());
    ASSERT_EQ(chosen, kDev);
    EXPECT_EQ(sel.getInflightBytes(kDev), kMiB);
    EXPECT_EQ(sel.getInflightBytes(7), 0u);  // unknown device
    ASSERT_TRUE(sel.release(kDev, kMiB, 0.0).ok());
    EXPECT_EQ(sel.getInflightBytes(kDev), 0u);
}

// Smart-mode multi-path allocation must charge each device the bytes of the
// slices it was assigned -- the caller cuts slices as min(block, remaining),
// and release() returns exactly that -- not ceil(total / n) per slice, which
// would drift per device (and wrap a device that receives less than it
// released).
TEST(DeviceSelectorInflightTest, MultiPathChargesActualSliceBytes) {
    auto topo = oneRdmaNic();
    Topology::NicEntry nic;
    nic.name = "mlx5_1";
    nic.type = Topology::NIC_RDMA;
    nic.numa_node = 0;
    topo->nic_list_.push_back(nic);
    Topology::MemEntry mem;
    mem.name = "cpu:0";
    mem.type = Topology::MEM_HOST;
    mem.numa_node = 0;
    mem.device_list[0].push_back(0);
    mem.device_list[0].push_back(1);
    topo->mem_list_.push_back(mem);
    DeviceSelector sel;
    ASSERT_TRUE(sel.loadTopology(topo).ok());
    DeviceSelector::SchedulingParams params;
    params.score_jitter_range = 0.0;
    sel.setSchedulingParams(params);

    // 1,000,001 B in 64 KiB blocks: 15 full slices and one of 16,961 B;
    // ceil(total / 16) = 62,501 would charge 1,000,016 in total.
    constexpr uint64_t kTotal = 1'000'001;
    constexpr uint64_t kBlock = 65536;
    constexpr uint32_t kSlices = 16;
    std::vector<int> ids;
    ASSERT_TRUE(
        sel.allocate(kTotal, kSlices, kBlock, "cpu:0", ids, PRIO_HIGH, ~0ULL)
            .ok());
    ASSERT_EQ(ids.size(), kSlices);

    uint64_t expected[2] = {0, 0};
    for (uint32_t i = 0; i < kSlices; ++i) {
        ASSERT_TRUE(ids[i] == 0 || ids[i] == 1);
        expected[ids[i]] += std::min<uint64_t>(kBlock, kTotal - i * kBlock);
    }
    EXPECT_EQ(sel.getInflightBytes(0), expected[0]);
    EXPECT_EQ(sel.getInflightBytes(1), expected[1]);

    for (uint32_t i = 0; i < kSlices; ++i) {
        const uint64_t len = std::min<uint64_t>(kBlock, kTotal - i * kBlock);
        ASSERT_TRUE(sel.release(ids[i], len, 0.0).ok());
    }
    EXPECT_EQ(sel.getInflightBytes(0), 0u);
    EXPECT_EQ(sel.getInflightBytes(1), 0u);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake

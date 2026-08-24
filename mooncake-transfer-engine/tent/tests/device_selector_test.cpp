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

#include <memory>

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

}  // namespace
}  // namespace tent
}  // namespace mooncake

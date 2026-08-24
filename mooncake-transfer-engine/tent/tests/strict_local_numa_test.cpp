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

#include <memory>
#include <string>
#include <vector>

#include "tent/common/types.h"
#include "tent/runtime/topology.h"
#include "tent/transport/rdma/quota.h"

namespace mooncake {
namespace tent {
namespace {

// cuda:0 owns a same-NUMA rail (dev 0 in rank0) and a cross-NUMA rail
// (dev 1 in the last rank). cuda:1 has no local rail; its only device is the
// cross-NUMA dev 1 in the last rank.
constexpr const char* kTwoTierTopology = R"json(
{
  "nics": [
    {"name": "mlx5_local", "type": 0, "numa_node": 0},
    {"name": "mlx5_remote", "type": 0, "numa_node": 1}
  ],
  "mems": [
    {"name": "cuda:0", "type": 1, "numa_node": 0,
     "device_list": {"rank0": [0], "rank2": [1]}},
    {"name": "cuda:1", "type": 1, "numa_node": 1,
     "device_list": {"rank2": [1]}}
  ]
}
)json";

std::shared_ptr<Topology> makeTopology() {
    auto topo = std::make_shared<Topology>();
    EXPECT_TRUE(topo->parse(kTwoTierTopology).ok());
    return topo;
}

std::unique_ptr<DeviceSelector> makeSelector(std::shared_ptr<Topology> topo,
                                             bool strict, bool smart) {
    auto selector = std::make_unique<DeviceSelector>();
    EXPECT_TRUE(selector->loadTopology(topo).ok());
    DeviceSelector::SchedulingParams params;
    params.strict_local_numa = strict;
    // Isolate NUMA behavior from QoS priority rotation.
    params.enable_priority_filtering = false;
    selector->setSchedulingParams(params);
    selector->setSmartSelection(smart);
    return selector;
}

TEST(StrictLocalNumaTest, SmartModeNeverPicksCrossNumaWhenLocalExists) {
    auto selector =
        makeSelector(makeTopology(), /*strict=*/true, /*smart=*/true);
    for (int i = 0; i < 64; ++i) {
        std::vector<int> devs;
        ASSERT_TRUE(selector->allocate(4096, 4, 1024, "cuda:0", devs).ok());
        ASSERT_FALSE(devs.empty());
        for (int dev : devs) EXPECT_EQ(dev, 0) << "iteration " << i;
    }
}

TEST(StrictLocalNumaTest, SmartModeReturnsDeviceNotFoundWithoutLocalNic) {
    auto selector =
        makeSelector(makeTopology(), /*strict=*/true, /*smart=*/true);
    std::vector<int> devs;
    auto status = selector->allocate(4096, 4, 1024, "cuda:1", devs);
    EXPECT_TRUE(status.IsDeviceNotFound()) << status.ToString();
    EXPECT_TRUE(devs.empty());
}

TEST(StrictLocalNumaTest, NonStrictModeFallsBackToCrossNuma) {
    auto selector =
        makeSelector(makeTopology(), /*strict=*/false, /*smart=*/true);
    std::vector<int> devs;
    ASSERT_TRUE(selector->allocate(4096, 4, 1024, "cuda:1", devs).ok());
    ASSERT_FALSE(devs.empty());
    for (int dev : devs) EXPECT_EQ(dev, 1);
}

TEST(StrictLocalNumaTest, BaselineModeExcludesCrossNuma) {
    auto selector =
        makeSelector(makeTopology(), /*strict=*/true, /*smart=*/false);
    for (int i = 0; i < 16; ++i) {
        std::vector<int> devs;
        ASSERT_TRUE(selector->allocate(4096, 4, 1024, "cuda:0", devs).ok());
        ASSERT_FALSE(devs.empty());
        for (int dev : devs) EXPECT_EQ(dev, 0);
    }
    std::vector<int> devs;
    EXPECT_TRUE(
        selector->allocate(4096, 4, 1024, "cuda:1", devs).IsDeviceNotFound());
}

TEST(StrictLocalNumaTest, BaselineNonStrictFallsBackToCrossNuma) {
    auto selector =
        makeSelector(makeTopology(), /*strict=*/false, /*smart=*/false);
    std::vector<int> devs;
    ASSERT_TRUE(selector->allocate(4096, 4, 1024, "cuda:1", devs).ok());
    ASSERT_FALSE(devs.empty());
    for (int dev : devs) EXPECT_EQ(dev, 1);
}

// When the only local rail is masked out, the empty-candidate fallback must not
// reintroduce the cross-NUMA rail under strict mode.
TEST(StrictLocalNumaTest, FallbackDoesNotReintroduceCrossNuma) {
    const uint64_t mask = ~(1ULL << 0);  // exclude the local NIC (dev 0)

    auto strict = makeSelector(makeTopology(), /*strict=*/true, /*smart=*/true);
    std::vector<int> devs;
    auto status =
        strict->allocate(4096, 4, 1024, "cuda:0", devs, PRIO_HIGH, mask);
    EXPECT_TRUE(status.IsDeviceNotFound()) << status.ToString();
    EXPECT_TRUE(devs.empty());

    // The same mask without strict mode still reaches the remote NIC, proving
    // the mask alone does not block dev 1.
    auto relaxed =
        makeSelector(makeTopology(), /*strict=*/false, /*smart=*/true);
    std::vector<int> relaxed_devs;
    ASSERT_TRUE(
        relaxed
            ->allocate(4096, 4, 1024, "cuda:0", relaxed_devs, PRIO_HIGH, mask)
            .ok());
    ASSERT_FALSE(relaxed_devs.empty());
    for (int dev : relaxed_devs) EXPECT_EQ(dev, 1);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake

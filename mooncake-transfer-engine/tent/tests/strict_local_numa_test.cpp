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

#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

#include "tent/common/config.h"
#include "tent/common/types.h"
#include "tent/runtime/topology.h"
#include "tent/transport/rdma/quota.h"

namespace mooncake {
namespace tent {
namespace {

// cuda:0 (NUMA 0): local dev 0 + remote dev 1. cuda:1 (NUMA 2): only remote
// dev 1.
constexpr const char* kTwoTierTopology = R"json(
{
  "nics": [
    {"name": "mlx5_local", "type": 0, "numa_node": 0},
    {"name": "mlx5_remote", "type": 0, "numa_node": 1}
  ],
  "mems": [
    {"name": "cuda:0", "type": 1, "numa_node": 0,
     "device_list": {"rank0": [0], "rank2": [1]}},
    {"name": "cuda:1", "type": 1, "numa_node": 2,
     "device_list": {"rank2": [1]}}
  ]
}
)json";

// Unknown NIC or memory NUMA (-1) must stay selectable.
constexpr const char* kUnknownNumaTopology = R"json(
{
  "nics": [
    {"name": "mlx5_known", "type": 0, "numa_node": 1},
    {"name": "mlx5_bond_0", "type": 0, "numa_node": -1}
  ],
  "mems": [
    {"name": "cuda:0", "type": 1, "numa_node": 0,
     "device_list": {"rank2": [1]}},
    {"name": "*", "type": 4, "numa_node": -1,
     "device_list": {"rank2": [0, 1]}}
  ]
}
)json";

// Remote NIC in rank 0 (ROCm / MemoryProber closest-PCIe shape).
constexpr const char* kRemoteNicInFirstRankTopology = R"json(
{
  "nics": [
    {"name": "mlx5_close_remote", "type": 0, "numa_node": 1},
    {"name": "mlx5_far_local", "type": 0, "numa_node": 0}
  ],
  "mems": [
    {"name": "hip:0", "type": 2, "numa_node": 0,
     "device_list": {"rank0": [0], "rank1": [1]}}
  ]
}
)json";

std::shared_ptr<Topology> makeTopology(const char* json = kTwoTierTopology) {
    auto topo = std::make_shared<Topology>();
    EXPECT_TRUE(topo->parse(json).ok());
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

// Masking the local NIC must not fall back to the remote one under strict mode.
TEST(StrictLocalNumaTest, FallbackDoesNotReintroduceCrossNuma) {
    const uint64_t mask = ~(1ULL << 0);  // exclude the local NIC (dev 0)

    auto strict = makeSelector(makeTopology(), /*strict=*/true, /*smart=*/true);
    std::vector<int> devs;
    auto status =
        strict->allocate(4096, 4, 1024, "cuda:0", devs, PRIO_HIGH, mask);
    EXPECT_TRUE(status.IsDeviceNotFound()) << status.ToString();
    EXPECT_TRUE(devs.empty());

    // Same mask without strict still reaches the remote NIC.
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

TEST(StrictLocalNumaTest, UnknownNicNumaIsNotExcluded) {
    for (bool smart : {true, false}) {
        auto selector = makeSelector(makeTopology(kUnknownNumaTopology),
                                     /*strict=*/true, smart);
        std::vector<int> devs;
        auto status = selector->allocate(4096, 4, 1024, "cuda:0", devs);
        ASSERT_TRUE(status.ok()) << status.ToString() << ", smart=" << smart;
        ASSERT_FALSE(devs.empty());
        for (int dev : devs) EXPECT_EQ(dev, 1);
    }
}

TEST(StrictLocalNumaTest, UnknownMemoryNumaIsNotExcluded) {
    for (bool smart : {true, false}) {
        auto selector = makeSelector(makeTopology(kUnknownNumaTopology),
                                     /*strict=*/true, smart);
        std::vector<int> devs;
        auto status =
            selector->allocate(4096, 4, 1024, kWildcardLocation, devs);
        ASSERT_TRUE(status.ok()) << status.ToString() << ", smart=" << smart;
        EXPECT_EQ(devs.size(), 4u);
    }
}

TEST(StrictLocalNumaTest, RemoteNicPromotedToFirstRankIsStillExcluded) {
    for (bool smart : {true, false}) {
        auto selector =
            makeSelector(makeTopology(kRemoteNicInFirstRankTopology),
                         /*strict=*/true, smart);
        std::vector<int> devs;
        auto status = selector->allocate(4096, 4, 1024, "hip:0", devs);
        ASSERT_TRUE(status.ok()) << status.ToString() << ", smart=" << smart;
        ASSERT_FALSE(devs.empty());
        // dev 0 is closest by PCIe but on another NUMA node.
        for (int dev : devs) EXPECT_EQ(dev, 1) << "smart=" << smart;
    }
}

// Priority matrix has no NUMA ids, so every NIC stays selectable.
TEST(StrictLocalNumaTest, PriorityMatrixTopologyKeepsSoftPenalty) {
    auto topo = std::make_shared<Topology>();
    ASSERT_TRUE(topo->parsePriorityMatrix(
                        R"json({"cpu:0": [["mlx5_0"], ["mlx5_1"]]})json")
                    .ok());

    for (bool smart : {true, false}) {
        auto selector = makeSelector(topo, /*strict=*/true, smart);
        std::vector<int> devs;
        auto status = selector->allocate(4096, 4, 1024, "cpu:0", devs);
        ASSERT_TRUE(status.ok()) << status.ToString() << ", smart=" << smart;
        EXPECT_EQ(devs.size(), 4u);
    }
}

TEST(StrictLocalNumaTest, IsCrossNumaRequiresBothSidesKnown) {
    auto topo = makeTopology(kUnknownNumaTopology);
    const auto* cuda0 = topo->getMemEntry("cuda:0");
    const auto* wildcard = topo->getMemEntry(kWildcardLocation);
    ASSERT_NE(cuda0, nullptr);
    ASSERT_NE(wildcard, nullptr);

    EXPECT_TRUE(topo->isCrossNuma(*cuda0, 0));   // both known, differ
    EXPECT_FALSE(topo->isCrossNuma(*cuda0, 1));  // NIC unknown
    EXPECT_FALSE(topo->isCrossNuma(*wildcard, 0));
    EXPECT_FALSE(topo->isCrossNuma(*wildcard, 1));
    EXPECT_FALSE(topo->isCrossNuma(*cuda0, 42));
}

class StrictLocalNumaEnvTest : public ::testing::Test {
   protected:
    // Isolate from an ambient MC_TENT_CONF.
    void SetUp() override { unsetenv("MC_TENT_CONF"); }

    void TearDown() override {
        unsetenv("MC_STRICT_LOCAL_NUMA");
        unsetenv("MC_IB_PORT");
    }

    static bool loadStrictFlag(const char* env_value, bool default_value) {
        setenv("MC_STRICT_LOCAL_NUMA", env_value, 1);
        Config config;
        ConfigHelper helper;
        EXPECT_TRUE(helper.loadFromEnv(config).ok());
        return config.get("transports/rdma/strict_local_numa", default_value);
    }
};

TEST_F(StrictLocalNumaEnvTest, AcceptsNumericAndTextualBooleans) {
    for (const char* on : {"1", "true", "TRUE", "yes", "on"}) {
        EXPECT_TRUE(loadStrictFlag(on, false)) << "value: " << on;
    }
    for (const char* off : {"0", "false", "no", "off"}) {
        EXPECT_FALSE(loadStrictFlag(off, true)) << "value: " << off;
    }
}

// Unparsable env must not overwrite an existing JSON value.
TEST_F(StrictLocalNumaEnvTest, UnrecognizedValueKeepsConfiguredValue) {
    setenv("MC_STRICT_LOCAL_NUMA", "maybe", 1);
    ConfigHelper helper;

    Config preset;
    preset.set("transports/rdma/strict_local_numa", true);
    ASSERT_TRUE(helper.loadFromEnv(preset).ok());
    EXPECT_TRUE(preset.get("transports/rdma/strict_local_numa", false));

    Config unset;
    ASSERT_TRUE(helper.loadFromEnv(unset).ok());
    EXPECT_FALSE(unset.get("transports/rdma/strict_local_numa", false));
}

// setFromString() must still store integer env vars as integers.
TEST_F(StrictLocalNumaEnvTest, IntegerEnvVarsAreUnaffected) {
    setenv("MC_IB_PORT", "1", 1);
    Config config;
    ConfigHelper helper;
    ASSERT_TRUE(helper.loadFromEnv(config).ok());
    EXPECT_EQ(config.get("transports/rdma/device/port", -1), 1);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake

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

#include "pci_topology.h"
#include "topology.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <unordered_set>
#include <vector>

using mooncake::classifyGpuNicPath;
using mooncake::HcaPartition;
using mooncake::InfinibandDevice;
using mooncake::normalizePciBusId;
using mooncake::PciPathType;
using mooncake::selectPreferredHcas;

// ---------------------------------------------------------------------------
// normalizePciBusId tests
// ---------------------------------------------------------------------------

TEST(NormalizePciBusIdTest, LowercaseConversion) {
    EXPECT_EQ(normalizePciBusId("0000:03:00.0"), "0000:03:00.0");
    EXPECT_EQ(normalizePciBusId("0000:03:00.0"), "0000:03:00.0");
    EXPECT_EQ(normalizePciBusId("0000:AB:CD.0"), "0000:ab:cd.0");
}

TEST(NormalizePciBusIdTest, StripLeadingZerosFrom8DigitDomain) {
    // CUDA returns 8-digit domain, sysfs uses 4-digit
    EXPECT_EQ(normalizePciBusId("00000008:06:00.0"), "0008:06:00.0");
    EXPECT_EQ(normalizePciBusId("00000000:03:00.0"), "0000:03:00.0");
    EXPECT_EQ(normalizePciBusId("00000018:06:00.0"), "0018:06:00.0");
}

TEST(NormalizePciBusIdTest, AlreadyNormalized4Digit) {
    // 4-digit domain should pass through unchanged
    EXPECT_EQ(normalizePciBusId("0008:06:00.0"), "0008:06:00.0");
    EXPECT_EQ(normalizePciBusId("0000:03:00.1"), "0000:03:00.1");
}

TEST(NormalizePciBusIdTest, NonZeroLeadingDigitsPreserved) {
    // If leading digits are non-zero, they should not be stripped
    EXPECT_EQ(normalizePciBusId("10000008:06:00.0"), "10000008:06:00.0");
}

TEST(NormalizePciBusIdTest, MixedCaseWithDomainStrip) {
    EXPECT_EQ(normalizePciBusId("00000008:AB:CD.0"), "0008:ab:cd.0");
}

// ---------------------------------------------------------------------------
// classifyGpuNicPath tests
// ---------------------------------------------------------------------------

// Helper: build an unordered_set from a vector for the gpu_ancestors param.
static std::unordered_set<std::string> toSet(
    const std::vector<std::string> &v) {
    return {v.begin(), v.end()};
}

// Scenario: GPU and NIC on different NUMA nodes -> SYS
TEST(ClassifyGpuNicPathTest, DifferentNuma) {
    std::vector<std::string> gpu_chain = {"0008:06:00.0", "0008:00:00.0"};
    std::vector<std::string> nic_chain = {"0010:03:00.0", "0010:00:00.0"};
    auto [path_type, hops] =
        classifyGpuNicPath(gpu_chain, toSet(gpu_chain), /*gpu_numa=*/0,
                           /*nic_numa=*/1, nic_chain);
    EXPECT_EQ(path_type, PciPathType::SYS);
    EXPECT_EQ(hops, -1);
}

// Scenario: Same PCI switch (total hops <= 2) -> PIX
TEST(ClassifyGpuNicPathTest, SamePciSwitch_PIX) {
    // GPU: switch -> gpu
    // NIC: switch -> nic
    // Common ancestor is "0008:01:00.0" (the switch)
    std::vector<std::string> gpu_chain = {"0008:02:00.0", "0008:01:00.0"};
    std::vector<std::string> nic_chain = {"0008:03:00.0", "0008:01:00.0"};
    auto [path_type, hops] =
        classifyGpuNicPath(gpu_chain, toSet(gpu_chain), /*gpu_numa=*/0,
                           /*nic_numa=*/0, nic_chain);
    EXPECT_EQ(path_type, PciPathType::PIX);
    EXPECT_EQ(hops, 2);  // 1 gpu hop + 1 nic hop
}

// Scenario: Crossing PCI bridges (total hops 3-4) -> PXB
TEST(ClassifyGpuNicPathTest, CrossingBridges_PXB) {
    // Common ancestor is "0008:00:00.0", gpu is 2 hops away, nic is 2 hops
    std::vector<std::string> gpu_chain = {"0008:04:00.0", "0008:02:00.0",
                                          "0008:00:00.0"};
    std::vector<std::string> nic_chain = {"0008:05:00.0", "0008:03:00.0",
                                          "0008:00:00.0"};
    auto [path_type, hops] =
        classifyGpuNicPath(gpu_chain, toSet(gpu_chain), /*gpu_numa=*/0,
                           /*nic_numa=*/0, nic_chain);
    EXPECT_EQ(path_type, PciPathType::PXB);
    EXPECT_EQ(hops, 4);  // 2 + 2
}

// Scenario: Crossing host bridge (total hops > 4) -> PHB
TEST(ClassifyGpuNicPathTest, HostBridge_PHB) {
    // Deep chains with common ancestor far up
    std::vector<std::string> gpu_chain = {"0008:06:00.0", "0008:05:00.0",
                                          "0008:04:00.0", "0008:00:00.0"};
    std::vector<std::string> nic_chain = {"0008:09:00.0", "0008:08:00.0",
                                          "0008:07:00.0", "0008:00:00.0"};
    auto [path_type, hops] =
        classifyGpuNicPath(gpu_chain, toSet(gpu_chain), /*gpu_numa=*/0,
                           /*nic_numa=*/0, nic_chain);
    EXPECT_EQ(path_type, PciPathType::PHB);
    EXPECT_EQ(hops, 6);  // 3 + 3
}

// Scenario: Same NUMA but no common PCI ancestor -> NODE
TEST(ClassifyGpuNicPathTest, SameNuma_NoCommonAncestor_NODE) {
    // GPU and NIC on completely separate PCI domains (like GB200)
    std::vector<std::string> gpu_chain = {"0008:06:00.0", "0008:00:00.0"};
    std::vector<std::string> nic_chain = {"0006:01:00.0", "0006:00:00.0"};
    auto [path_type, hops] =
        classifyGpuNicPath(gpu_chain, toSet(gpu_chain), /*gpu_numa=*/0,
                           /*nic_numa=*/0, nic_chain);
    EXPECT_EQ(path_type, PciPathType::NODE);
    // All NODE NICs use a fixed hop count (0) so chain length doesn't matter
    EXPECT_EQ(hops, 0);
}

// Scenario: GPU and NIC behind different root ports on the same root bus.
// The root bus (e.g. "pci0008:00") is the common ancestor -> PHB.
TEST(ClassifyGpuNicPathTest, SameRootBus_DifferentRootPorts_PHB) {
    // GPU behind root port 0008:00:00.0
    std::vector<std::string> gpu_chain = {"0008:06:00.0", "0008:01:00.0",
                                          "0008:00:00.0", "pci0008:00"};
    // NIC behind root port 0008:00:01.0 on the same root bus
    std::vector<std::string> nic_chain = {"0008:04:00.0", "0008:03:00.0",
                                          "0008:00:01.0", "pci0008:00"};
    auto [path_type, hops] =
        classifyGpuNicPath(gpu_chain, toSet(gpu_chain), /*gpu_numa=*/0,
                           /*nic_numa=*/0, nic_chain);
    // Common ancestor is pci0008:00 at gpu_hops=3, nic_hops=3 -> total 6 -> PHB
    EXPECT_EQ(path_type, PciPathType::PHB);
    EXPECT_EQ(hops, 6);
}

// Scenario: GPU and NIC on different root buses (different PCI domains),
// root bus included in chain but no match -> still NODE.
TEST(ClassifyGpuNicPathTest, DifferentRootBuses_StillNODE) {
    std::vector<std::string> gpu_chain = {"0008:06:00.0", "0008:00:00.0",
                                          "pci0008:00"};
    std::vector<std::string> nic_chain = {"0006:01:00.0", "0006:00:00.0",
                                          "pci0006:00"};
    auto [path_type, hops] =
        classifyGpuNicPath(gpu_chain, toSet(gpu_chain), /*gpu_numa=*/0,
                           /*nic_numa=*/0, nic_chain);
    EXPECT_EQ(path_type, PciPathType::NODE);
    EXPECT_EQ(hops, 0);
}

// Scenario: Different NUMA, no common ancestor -> SYS (not NODE)
TEST(ClassifyGpuNicPathTest, DifferentNuma_NoCommonAncestor_SYS) {
    std::vector<std::string> gpu_chain = {"0008:06:00.0", "0008:00:00.0"};
    std::vector<std::string> nic_chain = {"0016:01:00.0", "0016:00:00.0"};
    auto [path_type, hops] =
        classifyGpuNicPath(gpu_chain, toSet(gpu_chain), /*gpu_numa=*/0,
                           /*nic_numa=*/1, nic_chain);
    EXPECT_EQ(path_type, PciPathType::SYS);
    EXPECT_EQ(hops, -1);
}

// Scenario: GPU NUMA unknown (-1) should not short-circuit to SYS
TEST(ClassifyGpuNicPathTest, UnknownGpuNuma_FallsThrough) {
    std::vector<std::string> gpu_chain = {"0008:06:00.0", "0008:00:00.0"};
    std::vector<std::string> nic_chain = {"0008:03:00.0", "0008:00:00.0"};
    auto [path_type, hops] =
        classifyGpuNicPath(gpu_chain, toSet(gpu_chain), /*gpu_numa=*/-1,
                           /*nic_numa=*/0, nic_chain);
    // Should still find common ancestor and classify by hops
    EXPECT_EQ(path_type, PciPathType::PIX);
    EXPECT_EQ(hops, 2);
}

// Scenario: Both NUMA unknown, no common ancestor -> SYS (not NODE)
TEST(ClassifyGpuNicPathTest, BothNumaUnknown_NoCommonAncestor_SYS) {
    std::vector<std::string> gpu_chain = {"0008:06:00.0", "0008:00:00.0"};
    std::vector<std::string> nic_chain = {"0006:01:00.0", "0006:00:00.0"};
    auto [path_type, hops] =
        classifyGpuNicPath(gpu_chain, toSet(gpu_chain), /*gpu_numa=*/-1,
                           /*nic_numa=*/-1, nic_chain);
    // Can't confirm same NUMA, so falls to SYS
    EXPECT_EQ(path_type, PciPathType::SYS);
    EXPECT_EQ(hops, -1);
}

// ---------------------------------------------------------------------------
// Selection logic tests — verify that the preferred/avail partition works
// correctly for multi-NIC topologies like GB200.
// ---------------------------------------------------------------------------

// Thin adapter that unpacks the (hca, chain) pair-list used by these tests
// into the parallel-vector form expected by selectPreferredHcas, so the
// tests exercise the production selection logic directly.
struct NicSelection {
    std::vector<std::string> preferred;
    std::vector<std::string> avail;
};

static NicSelection selectPreferredNics(
    const std::vector<std::string> &gpu_ancestor_chain, int gpu_numa_node,
    const std::vector<std::pair<InfinibandDevice, std::vector<std::string>>>
        &hcas_with_chains) {
    std::vector<InfinibandDevice> all_hca;
    std::vector<std::vector<std::string>> nic_chains;
    all_hca.reserve(hcas_with_chains.size());
    nic_chains.reserve(hcas_with_chains.size());
    for (const auto &[hca, chain] : hcas_with_chains) {
        all_hca.push_back(hca);
        nic_chains.push_back(chain);
    }

    HcaPartition partition = selectPreferredHcas(
        gpu_ancestor_chain, gpu_numa_node, all_hca, nic_chains);
    return NicSelection{.preferred = std::move(partition.preferred_hca),
                        .avail = std::move(partition.avail_hca)};
}

// GB200-like topology: GPUs and NICs on separate PCI domains, same NUMA.
// All same-NUMA NICs with equal chain length should be preferred together.
TEST(NicSelectionTest, GB200_AllSameNumaNicsPreferred) {
    // GPU 0: domain 0008, NUMA 0, chain length 3
    std::vector<std::string> gpu_chain = {"0008:06:00.0", "0008:01:00.0",
                                          "0008:00:00.0"};

    // 4 NICs on NUMA 0 with equal chain lengths (domain 0000, 0002)
    // 4 NICs on NUMA 1 (should be SYS)
    using HcaWithChain =
        std::pair<mooncake::InfinibandDevice, std::vector<std::string>>;
    std::vector<HcaWithChain> hcas = {
        {{"mlx5_0", "0000:03:00.0", 0, 1024.0},
         {"0000:03:00.0", "0000:02:00.0", "0000:00:00.0"}},
        {{"mlx5_1", "0000:03:00.1", 0, 1024.0},
         {"0000:03:00.1", "0000:02:00.0", "0000:00:00.0"}},
        {{"mlx5_2", "0002:03:00.0", 0, 1024.0},
         {"0002:03:00.0", "0002:02:00.0", "0002:00:00.0"}},
        {{"mlx5_3", "0002:03:00.1", 0, 1024.0},
         {"0002:03:00.1", "0002:02:00.0", "0002:00:00.0"}},
        {{"mlx5_5", "0010:03:00.0", 1, 1024.0},
         {"0010:03:00.0", "0010:02:00.0", "0010:00:00.0"}},
        {{"mlx5_6", "0010:03:00.1", 1, 1024.0},
         {"0010:03:00.1", "0010:02:00.0", "0010:00:00.0"}},
        {{"mlx5_7", "0012:03:00.0", 1, 1024.0},
         {"0012:03:00.0", "0012:02:00.0", "0012:00:00.0"}},
        {{"mlx5_8", "0012:03:00.1", 1, 1024.0},
         {"0012:03:00.1", "0012:02:00.0", "0012:00:00.0"}},
    };

    auto result = selectPreferredNics(gpu_chain, /*gpu_numa=*/0, hcas);

    // All 4 NUMA-0 NICs should be preferred (equal chain length = equal hops)
    std::sort(result.preferred.begin(), result.preferred.end());
    ASSERT_EQ(result.preferred.size(), 4u);
    EXPECT_EQ(result.preferred[0], "mlx5_0");
    EXPECT_EQ(result.preferred[1], "mlx5_1");
    EXPECT_EQ(result.preferred[2], "mlx5_2");
    EXPECT_EQ(result.preferred[3], "mlx5_3");

    // All 4 NUMA-1 NICs should be avail (SYS)
    std::sort(result.avail.begin(), result.avail.end());
    ASSERT_EQ(result.avail.size(), 4u);
    EXPECT_EQ(result.avail[0], "mlx5_5");
    EXPECT_EQ(result.avail[1], "mlx5_6");
    EXPECT_EQ(result.avail[2], "mlx5_7");
    EXPECT_EQ(result.avail[3], "mlx5_8");
}

// GB200-like topology: front-end NIC (Gen4, 512 GT/s*width) vs backend NICs
// (Gen5, 1024 GT/s*width).  All are NODE on the same NUMA, but the bandwidth
// tiebreaker should filter out the slower front-end NIC.
TEST(NicSelectionTest, GB200_BandwidthFiltersOutFrontEndNic) {
    std::vector<std::string> gpu_chain = {"0008:06:00.0", "0008:01:00.0",
                                          "0008:00:00.0"};

    using HcaWithChain =
        std::pair<mooncake::InfinibandDevice, std::vector<std::string>>;
    std::vector<HcaWithChain> hcas = {
        // Front-end NIC: Gen4 x16 = 512 on NUMA 0
        {{"mlx5_4", "0006:01:00.0", 0, 512.0},
         {"0006:01:00.0", "0006:00:00.0"}},
        // Backend NICs: Gen5 x16 = 1024 on NUMA 0
        {{"mlx5_0", "0000:03:00.0", 0, 1024.0},
         {"0000:03:00.0", "0000:02:00.0", "0000:00:00.0"}},
        {{"mlx5_1", "0000:03:00.1", 0, 1024.0},
         {"0000:03:00.1", "0000:02:00.0", "0000:00:00.0"}},
    };

    auto result = selectPreferredNics(gpu_chain, /*gpu_numa=*/0, hcas);

    // mlx5_0 and mlx5_1 (1024) are preferred; mlx5_4 (512) goes to avail
    std::sort(result.preferred.begin(), result.preferred.end());
    ASSERT_EQ(result.preferred.size(), 2u);
    EXPECT_EQ(result.preferred[0], "mlx5_0");
    EXPECT_EQ(result.preferred[1], "mlx5_1");
    ASSERT_EQ(result.avail.size(), 1u);
    EXPECT_EQ(result.avail[0], "mlx5_4");
}

// When all NICs have the same bandwidth, all same-NUMA NODE NICs should
// still be preferred together (no bandwidth differentiation).
TEST(NicSelectionTest, GB200_EqualBandwidth_AllPreferred) {
    std::vector<std::string> gpu_chain = {"0008:06:00.0", "0008:01:00.0",
                                          "0008:00:00.0"};

    using HcaWithChain =
        std::pair<mooncake::InfinibandDevice, std::vector<std::string>>;
    std::vector<HcaWithChain> hcas = {
        {{"mlx5_4", "0006:01:00.0", 0, 1024.0},
         {"0006:01:00.0", "0006:00:00.0"}},
        {{"mlx5_0", "0000:03:00.0", 0, 1024.0},
         {"0000:03:00.0", "0000:02:00.0", "0000:00:00.0"}},
        {{"mlx5_1", "0000:03:00.1", 0, 1024.0},
         {"0000:03:00.1", "0000:02:00.0", "0000:00:00.0"}},
    };

    auto result = selectPreferredNics(gpu_chain, /*gpu_numa=*/0, hcas);

    std::sort(result.preferred.begin(), result.preferred.end());
    ASSERT_EQ(result.preferred.size(), 3u);
    EXPECT_EQ(result.preferred[0], "mlx5_0");
    EXPECT_EQ(result.preferred[1], "mlx5_1");
    EXPECT_EQ(result.preferred[2], "mlx5_4");
    EXPECT_TRUE(result.avail.empty());
}

// Shared PCI ancestor: all NICs under same switch should be PIX together
TEST(NicSelectionTest, AllNicsUnderSameSwitch) {
    std::vector<std::string> gpu_chain = {"0008:03:00.0", "0008:01:00.0"};

    using HcaWithChain =
        std::pair<mooncake::InfinibandDevice, std::vector<std::string>>;
    std::vector<HcaWithChain> hcas = {
        {{"mlx5_0", "0008:04:00.0", 0, 1024.0},
         {"0008:04:00.0", "0008:01:00.0"}},
        {{"mlx5_1", "0008:05:00.0", 0, 1024.0},
         {"0008:05:00.0", "0008:01:00.0"}},
        {{"mlx5_2", "0008:06:00.0", 0, 1024.0},
         {"0008:06:00.0", "0008:01:00.0"}},
    };

    auto result = selectPreferredNics(gpu_chain, /*gpu_numa=*/0, hcas);

    // All NICs are PIX with 2 hops -> all preferred
    ASSERT_EQ(result.preferred.size(), 3u);
    EXPECT_TRUE(result.avail.empty());
}

// Mixed path types: PIX NIC preferred over NODE NICs
TEST(NicSelectionTest, PIX_PreferredOverNODE) {
    std::vector<std::string> gpu_chain = {"0008:03:00.0", "0008:01:00.0"};

    using HcaWithChain =
        std::pair<mooncake::InfinibandDevice, std::vector<std::string>>;
    std::vector<HcaWithChain> hcas = {
        // PIX: same switch as GPU
        {{"mlx5_0", "0008:04:00.0", 0, 1024.0},
         {"0008:04:00.0", "0008:01:00.0"}},
        // NODE: same NUMA, different PCI domain
        {{"mlx5_1", "0006:01:00.0", 0, 1024.0},
         {"0006:01:00.0", "0006:00:00.0"}},
        // SYS: different NUMA
        {{"mlx5_2", "0010:01:00.0", 1, 1024.0},
         {"0010:01:00.0", "0010:00:00.0"}},
    };

    auto result = selectPreferredNics(gpu_chain, /*gpu_numa=*/0, hcas);

    ASSERT_EQ(result.preferred.size(), 1u);
    EXPECT_EQ(result.preferred[0], "mlx5_0");
    ASSERT_EQ(result.avail.size(), 2u);
}

// H100-like topology: GPU and some NICs share the same root bus (different
// root ports -> PHB), while other NICs are on a separate root bus (NODE).
// PHB NICs must be preferred over NODE NICs.
TEST(NicSelectionTest, PHB_PreferredOverNODE_SharedRootBus) {
    // GPU behind root port 0008:00:00.0 on root bus pci0008:00
    std::vector<std::string> gpu_chain = {"0008:06:00.0", "0008:01:00.0",
                                          "0008:00:00.0", "pci0008:00"};

    using HcaWithChain =
        std::pair<mooncake::InfinibandDevice, std::vector<std::string>>;
    std::vector<HcaWithChain> hcas = {
        // PHB: same root bus, different root port
        {{"mlx5_0", "0008:04:00.0", 0, 1024.0},
         {"0008:04:00.0", "0008:03:00.0", "0008:00:01.0", "pci0008:00"}},
        // NODE: different root bus, same NUMA
        {{"mlx5_1", "0006:01:00.0", 0, 1024.0},
         {"0006:01:00.0", "0006:00:00.0", "pci0006:00"}},
        // NODE: different root bus, same NUMA
        {{"mlx5_2", "0002:03:00.0", 0, 1024.0},
         {"0002:03:00.0", "0002:00:00.0", "pci0002:00"}},
    };

    auto result = selectPreferredNics(gpu_chain, /*gpu_numa=*/0, hcas);

    // mlx5_0 is PHB (shares root bus), mlx5_1/2 are NODE -> PHB wins
    ASSERT_EQ(result.preferred.size(), 1u);
    EXPECT_EQ(result.preferred[0], "mlx5_0");
    ASSERT_EQ(result.avail.size(), 2u);
}

// ---------------------------------------------------------------------------
// Fallback path tests — verify that after disabling a preferred NIC,
// avail_hca NICs are still reachable and would have passed DMA-BUF
// validation (i.e., they appear in the topology for that GPU).
// ---------------------------------------------------------------------------

// Simulate the fallback scenario: preferred NIC is disabled, selection
// falls back to avail_hca.  The avail NIC must appear in the GPU's
// topology entry so that DMA-BUF validation (which now checks both
// preferred and avail lists) covers it.
TEST(FallbackPathTest, DisabledPreferredFallsBackToAvail) {
    // Build a topology via Topology::parse with known preferred/avail.
    mooncake::Topology topology;
    std::string json = R"({
        "cuda:0": [["mlx5_0"], ["mlx5_1", "mlx5_2"]],
        "cpu:0":  [["mlx5_0", "mlx5_1", "mlx5_2"], []]
    })";
    topology.parse(json);

    // Before disable: cuda:0 preferred = [mlx5_0], avail = [mlx5_1, mlx5_2]
    auto matrix = topology.getMatrix();
    ASSERT_EQ(matrix["cuda:0"].preferred_hca.size(), 1u);
    EXPECT_EQ(matrix["cuda:0"].preferred_hca[0], "mlx5_0");
    ASSERT_EQ(matrix["cuda:0"].avail_hca.size(), 2u);

    // Disable the preferred NIC (simulating a runtime failure)
    topology.disableDevice("mlx5_0");

    // After disable: preferred is empty, avail still has mlx5_1 and mlx5_2
    matrix = topology.getMatrix();
    EXPECT_TRUE(matrix["cuda:0"].preferred_hca.empty());
    ASSERT_EQ(matrix["cuda:0"].avail_hca.size(), 2u);

    // selectDevice should still return a valid device index (not -1)
    // since avail_hca entries are present.  retry_count=1 forces avail path.
    int device = topology.selectDevice("cuda:0", 1);
    EXPECT_GE(device, 0);
}

// Verify that an avail NIC appears in the topology for the GPU entry,
// meaning DMA-BUF validation would have covered it (since validation now
// checks both preferred_hca and avail_hca).
TEST(FallbackPathTest, AvailNicInTopologyForDmaBufCoverage) {
    mooncake::Topology topology;
    // mlx5_2 is avail for cuda:0 and cuda:1
    std::string json = R"({
        "cuda:0": [["mlx5_0"], ["mlx5_2"]],
        "cuda:1": [["mlx5_1"], ["mlx5_2"]],
        "cpu:0":  [["mlx5_0", "mlx5_1", "mlx5_2"], []]
    })";
    topology.parse(json);

    auto matrix = topology.getMatrix();

    // mlx5_2 must be in avail_hca for both cuda:0 and cuda:1,
    // confirming DMA-BUF validation covers the fallback path.
    auto &cuda0_avail = matrix["cuda:0"].avail_hca;
    EXPECT_NE(std::find(cuda0_avail.begin(), cuda0_avail.end(), "mlx5_2"),
              cuda0_avail.end());

    auto &cuda1_avail = matrix["cuda:1"].avail_hca;
    EXPECT_NE(std::find(cuda1_avail.begin(), cuda1_avail.end(), "mlx5_2"),
              cuda1_avail.end());

    // After disabling both preferred NICs, mlx5_2 should still be selectable
    topology.disableDevice("mlx5_0");
    topology.disableDevice("mlx5_1");

    int device0 = topology.selectDevice("cuda:0", 1);
    int device1 = topology.selectDevice("cuda:1", 1);
    EXPECT_GE(device0, 0);
    EXPECT_GE(device1, 0);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

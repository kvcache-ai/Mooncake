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
#include <unistd.h>

#include <algorithm>
#include <filesystem>
#include <memory>
#include <vector>

#include "tent/platform/mlu_topology.h"
#include "tent/transport/rdma/quota.h"

namespace mooncake::tent {
namespace {
class MluTopologyTest : public ::testing::Test {
   protected:
    void SetUp() override {
        char pattern[] = "/tmp/tent-mlu-topology-XXXXXX";
        auto* dir = mkdtemp(pattern);
        ASSERT_NE(dir, nullptr);
        root = dir;
        std::filesystem::create_directory(root / "pci");
        mem.name = "mlu:0";
        mem.type = Topology::MEM_MLU;
        mem.numa_node = 0;
        mem.pci_bus_id = "0000:03:00.0";
        link(mem.pci_bus_id, "pci0000:00/0000:00:01.0/0000:03:00.0");
    }
    void TearDown() override {
        std::error_code error;
        if (!root.empty()) std::filesystem::remove_all(root, error);
    }
    void link(const std::string& id, const std::string& path) {
        auto target = root / "devices" / path;
        std::filesystem::create_directories(target);
        std::filesystem::create_directory_symlink(target, root / "pci" / id);
    }
    void nic(const std::string& id, int numa, const std::string& path = "") {
        if (!path.empty()) link(id, path);
        Topology::NicEntry entry;
        entry.name = "nic" + std::to_string(nics.size());
        entry.pci_bus_id = id;
        entry.numa_node = numa;
        entry.type = Topology::NIC_RDMA;
        nics.push_back(entry);
    }
    void rank() {
        mlu_detail::rankNics(mem, nics, root / "pci");
        std::vector<int> ids;
        for (const auto& rank : mem.device_list)
            ids.insert(ids.end(), rank.begin(), rank.end());
        std::sort(ids.begin(), ids.end());
        ASSERT_EQ(ids.size(), nics.size());
        for (size_t i = 0; i < ids.size(); ++i) EXPECT_EQ(ids[i], i);
    }
    void expect(std::vector<int> first, std::vector<int> second,
                std::vector<int> third) {
        EXPECT_EQ(mem.device_list[0], first);
        EXPECT_EQ(mem.device_list[1], second);
        EXPECT_EQ(mem.device_list[2], third);
    }
    std::unique_ptr<DeviceSelector> selector(bool smart, bool strict = false) {
        auto topo = std::make_shared<Topology>();
        topo->nic_list_ = nics;
        topo->mem_list_ = {mem};
        auto result = std::make_unique<DeviceSelector>();
        EXPECT_TRUE(result->loadTopology(topo).ok());
        DeviceSelector::SchedulingParams params;
        params.enable_priority_filtering = false;
        params.score_jitter_range = 0;
        params.strict_local_numa = strict;
        result->setSchedulingParams(params);
        result->setSmartSelection(smart);
        return result;
    }
    std::filesystem::path root;
    Topology::MemEntry mem;
    std::vector<Topology::NicEntry> nics;
};

TEST_F(MluTopologyTest, SeparatesClosestSameNumaAndRemote) {
    nic("0000:04:00.0", 0, "pci0000:00/0000:00:01.0/0000:04:00.0");
    nic("0000:06:00.0", 0, "pci0000:00/0000:00:02.0/0000:06:00.0");
    nic("0000:84:00.0", 1, "pci0000:80/0000:80:01.0/0000:84:00.0");
    rank();
    expect({0}, {1}, {2});
}

TEST_F(MluTopologyTest, PreservesEqualDistanceNicsAndInputOrder) {
    nic("0000:04:00.1", 0, "pci0000:00/0000:00:01.0/0000:04:00.1");
    nic("0000:04:00.0", 0, "pci0000:00/0000:00:01.0/0000:04:00.0");
    rank();
    expect({0, 1}, {}, {});
}

TEST_F(MluTopologyTest, ChoosesNearestWhenNoSiblingNicExists) {
    nic("0000:84:00.0", 1, "pci0000:80/0000:80:01.0/0000:84:00.0");
    nic("0000:06:00.0", 0, "pci0000:00/0000:00:02.0/0000:06:00.0");
    rank();
    expect({1}, {}, {0});
}

TEST_F(MluTopologyTest, MissingAndBrokenPathsNeverBeatKnownDistance) {
    nic("0000:05:00.0", 0);
    nic("0000:07:00.0", 1);
    std::filesystem::create_symlink(root / "missing",
                                    root / "pci" / "0000:07:00.0");
    nic("0000:06:00.0", 0, "pci0000:00/0000:00:02.0/0000:06:00.0");
    rank();
    expect({2}, {0}, {1});
}

TEST_F(MluTopologyTest, EmptyNicBusIdDoesNotResolveToSysfsRoot) {
    nic("", 0);
    nic("0000:06:00.0", 0, "pci0000:00/0000:00:02.0/0000:06:00.0");
    rank();
    expect({1}, {0}, {});
}

TEST_F(MluTopologyTest, MissingDevicePathFallsBackToNuma) {
    mem.pci_bus_id = "0000:ff:00.0";
    nic("0000:04:00.0", 0, "pci0000:00/0000:00:01.0/0000:04:00.0");
    nic("0000:84:00.0", 1);
    rank();
    expect({0}, {}, {1});
}

TEST_F(MluTopologyTest, AllUnknownDistancesFallBackToNuma) {
    nic("0000:05:00.0", 0);
    nic("0000:85:00.0", 1);
    nic("", -1);
    rank();
    expect({0}, {}, {1, 2});
}

TEST_F(MluTopologyTest, UnknownNumaDoesNotCreateFalseAffinity) {
    mem.numa_node = -1;
    nic("0000:05:00.0", -1);
    nic("0000:85:00.0", 1);
    rank();
    expect({}, {}, {0, 1});
}

TEST_F(MluTopologyTest, KnownDistanceStillWorksWithoutNuma) {
    mem.numa_node = -1;
    nic("0000:04:00.0", -1, "pci0000:00/0000:00:01.0/0000:04:00.0");
    nic("0000:06:00.0", -1, "pci0000:00/0000:00:02.0/0000:06:00.0");
    rank();
    expect({0}, {}, {1});
}

TEST_F(MluTopologyTest, NormalizesHexCase) {
    link("0000:ab:00.0", "pci0000:a0/0000:a0:01.0/0000:ab:00.0");
    mem.pci_bus_id = "0000:AB:00.0";
    nic("0000:ac:00.0", 0, "pci0000:a0/0000:a0:01.0/0000:ac:00.0");
    nics[0].pci_bus_id = "0000:AC:00.0";
    rank();
    expect({0}, {}, {});
}

TEST_F(MluTopologyTest, DeepLocalSwitchBeatsShallowRemoteRoot) {
    nic("0000:08:00.0", 0,
        "pci0000:00/0000:00:02.0/0000:05:00.0/0000:06:00.0/"
        "0000:07:00.0/0000:08:00.0");
    nic("0000:80:00.0", 1, "pci0000:80/0000:80:00.0");
    rank();
    expect({0}, {}, {1});
}

TEST_F(MluTopologyTest, DifferentRootsFallBackToNumaNotDirectoryDepth) {
    nic("0000:48:00.0", 0,
        "pci0000:40/0000:40:01.0/0000:45:00.0/0000:46:00.0/"
        "0000:47:00.0/0000:48:00.0");
    nic("0000:80:00.0", 1, "pci0000:80/0000:80:00.0");
    rank();
    expect({0}, {}, {1});
}

TEST_F(MluTopologyTest, RemoteRootDoesNotDisplaceUnresolvedLocalNic) {
    nic("0000:05:00.0", 0);
    nic("0000:80:00.0", 1, "pci0000:80/0000:80:00.0");
    rank();
    expect({0}, {}, {1});
}

TEST_F(MluTopologyTest, UnknownNumaAcrossRootsDoesNotInventAffinity) {
    mem.numa_node = -1;
    nic("0000:48:00.0", -1, "pci0000:40/0000:40:01.0/0000:48:00.0");
    nic("0000:80:00.0", -1, "pci0000:80/0000:80:00.0");
    rank();
    expect({}, {}, {0, 1});
}

TEST_F(MluTopologyTest, RemoteOnlyNicsBothCarrySlicesAndBalanceQuota) {
    nic("0000:40:00.0", 1, "pci0000:40/0000:40:00.0");
    nic("0000:80:00.0", 2, "pci0000:80/0000:80:00.0");
    rank();
    expect({}, {}, {0, 1});
    for (bool smart : {false, true}) {
        auto sel = selector(smart);
        for (int batch = 0; batch < 105; ++batch) {
            std::vector<int> ids;
            ASSERT_TRUE(sel->allocate(16 * 4096, 16, 4096, mem.name, ids).ok());
            ASSERT_EQ(ids.size(), 16u);
            EXPECT_GT(std::count(ids.begin(), ids.end(), 0), 0);
            EXPECT_GT(std::count(ids.begin(), ids.end(), 1), 0);
            for (int id : ids) ASSERT_TRUE(sel->release(id, 4096, 0).ok());
            EXPECT_EQ(sel->getInflightBytes(0), 0u);
            EXPECT_EQ(sel->getInflightBytes(1), 0u);
        }
    }
}

TEST_F(MluTopologyTest, UnavailableClosestNicFallsBackAndCanReturn) {
    nic("0000:04:00.0", 0, "pci0000:00/0000:00:01.0/0000:04:00.0");
    nic("0000:80:00.0", 1, "pci0000:80/0000:80:00.0");
    rank();
    expect({0}, {}, {1});
    for (bool smart : {false, true}) {
        auto sel = selector(smart);
        ASSERT_TRUE(sel->setDeviceAvailable(0, false).ok());
        int id = -1;
        ASSERT_TRUE(sel->allocate(4096, mem.name, id).ok());
        EXPECT_EQ(id, 1);
        ASSERT_TRUE(sel->release(id, 4096, 0).ok());
        // The caller's mask remains authoritative during fallback.
        EXPECT_TRUE(sel->allocate(4096, mem.name, id, PRIO_HIGH, 1ULL)
                        .IsDeviceNotFound());
        ASSERT_TRUE(sel->setDeviceAvailable(0, true).ok());
        ASSERT_TRUE(sel->allocate(4096, mem.name, id, PRIO_HIGH, 1ULL).ok());
        EXPECT_EQ(id, 0);
        ASSERT_TRUE(sel->release(id, 4096, 0).ok());
        EXPECT_EQ(sel->getInflightBytes(0), 0u);
        EXPECT_EQ(sel->getInflightBytes(1), 0u);
    }
}

TEST_F(MluTopologyTest, StrictNumaRejectsKnownRemoteButAllowsUnknown) {
    nic("0000:80:00.0", 1, "pci0000:80/0000:80:00.0");
    for (int numa : {0, -1}) {
        mem.numa_node = numa;
        rank();
        for (bool smart : {false, true}) {
            auto sel = selector(smart, true);
            int id = -1;
            auto status = sel->allocate(4096, mem.name, id);
            if (numa >= 0) {
                EXPECT_TRUE(status.IsDeviceNotFound());
            } else {
                ASSERT_TRUE(status.ok());
                EXPECT_EQ(id, 0);
                ASSERT_TRUE(sel->release(id, 4096, 0).ok());
            }
            EXPECT_EQ(sel->getInflightBytes(0), 0u);
        }
    }
}

TEST_F(MluTopologyTest, RanksOnlyFilteredNicsAndReplacesPreviousRanks) {
    nic("0000:04:00.0", 0, "pci0000:00/0000:00:01.0/0000:04:00.0");
    nic("0000:06:00.0", 0, "pci0000:00/0000:00:02.0/0000:06:00.0");
    rank();
    expect({0}, {1}, {});
    nics.erase(nics.begin());
    rank();
    expect({0}, {}, {});
    nics.clear();
    rank();
    expect({}, {}, {});
}

TEST_F(MluTopologyTest, IndependentDevicesPreferTheirOwnSwitch) {
    nic("0000:04:00.0", 0, "pci0000:00/0000:00:01.0/0000:04:00.0");
    nic("0000:06:00.0", 0, "pci0000:00/0000:00:02.0/0000:06:00.0");
    rank();
    expect({0}, {1}, {});
    link("0000:07:00.0", "pci0000:00/0000:00:02.0/0000:07:00.0");
    mem.name = "mlu:1";
    mem.pci_bus_id = "0000:07:00.0";
    rank();
    expect({1}, {0}, {});
    Topology topology;
    topology.nic_list_ = nics;
    topology.mem_list_.push_back(mem);
    Topology restored;
    ASSERT_TRUE(restored.parse(topology.toString()).ok());
    auto* entry = restored.getMemEntry("mlu:1");
    ASSERT_NE(entry, nullptr);
    for (size_t i = 0; i < Topology::DevicePriorityRanks; ++i)
        EXPECT_EQ(entry->device_list[i], mem.device_list[i]);
}

TEST(MluPciDistanceTest, ComparesWholeComponents) {
    EXPECT_EQ(mlu_detail::pciDistance("/sys/devices/pci0000:00/a",
                                      "/sys/devices/pci0000:00/b"),
              0);
    EXPECT_EQ(mlu_detail::pciDistance("/sys/devices/pci0000:00/a",
                                      "/sys/devices/pci0000:01/b"),
              -1);
    EXPECT_EQ(mlu_detail::pciDistance("/sys/devices/pci0000:00/a/b",
                                      "/sys/devices/pci0000:00/c/d"),
              2);
    EXPECT_EQ(mlu_detail::pciDistance("", "/sys/devices/pci0000:00/a"), -1);
}

TEST(MluPciDistanceTest, RecognizesRootBelowHostController) {
    EXPECT_EQ(mlu_detail::pciDistance(
                  "/sys/devices/platform/4010000000.pcie/pci0000:00/a/b",
                  "/sys/devices/platform/4010000000.pcie/pci0000:00/a/c"),
              0);
    EXPECT_EQ(mlu_detail::pciDistance(
                  "/sys/devices/platform/4010000000.pcie/pci0000:00/a",
                  "/sys/devices/platform/4010000000.pcie/pci0000:01/b"),
              -1);
    EXPECT_EQ(mlu_detail::pciDistance("/sys/devices/virtual/a",
                                      "/sys/devices/virtual/b"),
              -1);
}
}  // namespace
}  // namespace mooncake::tent

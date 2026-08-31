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
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>

#include "tent/common/config.h"
#include "tent/common/types.h"
#include "tent/runtime/topology.h"

namespace mooncake {
namespace tent {
namespace {

class TempTopologyFile {
   public:
    explicit TempTopologyFile(const std::string& content) {
        auto unique_name =
            "tent-topo-" +
            std::to_string(
                std::chrono::steady_clock::now().time_since_epoch().count()) +
            ".json";
        path_ = std::filesystem::temp_directory_path() / unique_name;
        std::ofstream ofs(path_);
        ofs << content;
    }

    ~TempTopologyFile() {
        std::error_code ec;
        std::filesystem::remove(path_, ec);
    }

    std::string path() const { return path_.string(); }

   private:
    std::filesystem::path path_;
};

TEST(TopologyPriorityMatrixTest, ParsesClassicMatrixRanksAndWildcard) {
    constexpr const char* kMatrix = R"json(
        {
          "cpu:0": [["mlx5_0"], ["mlx5_1"]],
          "cpu:1": [["mlx5_1"], ["mlx5_0"]],
          "cuda:0": [["mlx5_0"], ["mlx5_1"]]
        }
    )json";

    Topology topology;
    ASSERT_TRUE(topology.parsePriorityMatrix(kMatrix).ok());

    ASSERT_EQ(topology.getNicCount(), 2u);
    EXPECT_EQ(topology.getNicName(0), "mlx5_0");
    EXPECT_EQ(topology.getNicName(1), "mlx5_1");
    EXPECT_EQ(topology.getNicType(0), Topology::NIC_RDMA);

    const auto* cpu0 = topology.getMemEntry("cpu:0");
    ASSERT_NE(cpu0, nullptr);
    EXPECT_EQ(cpu0->type, Topology::MEM_HOST);
    ASSERT_EQ(cpu0->device_list[0].size(), 1u);
    EXPECT_EQ(cpu0->device_list[0][0], 0);
    ASSERT_EQ(cpu0->device_list[1].size(), 1u);
    EXPECT_EQ(cpu0->device_list[1][0], 1);
    EXPECT_TRUE(cpu0->device_list[2].empty());

    const auto* cpu1 = topology.getMemEntry("cpu:1");
    ASSERT_NE(cpu1, nullptr);
    ASSERT_EQ(cpu1->device_list[0].size(), 1u);
    EXPECT_EQ(cpu1->device_list[0][0], 1);
    ASSERT_EQ(cpu1->device_list[1].size(), 1u);
    EXPECT_EQ(cpu1->device_list[1][0], 0);

    const auto* cuda0 = topology.getMemEntry("cuda:0");
    ASSERT_NE(cuda0, nullptr);
    EXPECT_EQ(cuda0->type, Topology::MEM_CUDA);

    const auto* wildcard = topology.getMemEntry(kWildcardLocation);
    ASSERT_NE(wildcard, nullptr);
    ASSERT_EQ(wildcard->device_list[0].size(), 2u);
    EXPECT_EQ(wildcard->device_list[0][0], 0);
    EXPECT_EQ(wildcard->device_list[0][1], 1);
    ASSERT_EQ(wildcard->device_list[1].size(), 2u);
    EXPECT_EQ(wildcard->device_list[1][0], 1);
    EXPECT_EQ(wildcard->device_list[1][1], 0);
}

TEST(TopologyPriorityMatrixTest, RejectsMalformedMatrixEntries) {
    Topology topology;
    EXPECT_FALSE(topology.parsePriorityMatrix("").ok());
    EXPECT_FALSE(
        topology.parsePriorityMatrix(R"({"cpu:0": [["mlx5_0"]]})").ok());
    EXPECT_FALSE(topology.parsePriorityMatrix(R"({"cpu:0": "mlx5_0"})").ok());
    EXPECT_FALSE(
        topology.parsePriorityMatrix(R"({"cpu:0": [[1], ["mlx5_1"]]})").ok());
}

// "segments" is the location type used for NUMA-segmented host DRAM (e.g.
// "segments:4096:0,1" = page_size 4096, NUMA nodes 0 and 1), produced by the
// store's buildSegmentsLocation() for multi-NIC cross-NUMA buffers. It is
// host memory and must map to MEM_HOST; otherwise transport selection treats
// it as MEM_UNKNOWN and fails with UNSPEC, breaking transfers to multi-NUMA
// store segments (the store allocates NUMA-segmented buffers when multiple
// NIC NUMA nodes are present).
TEST(TopologyPriorityMatrixTest, SegmentsLocationTreatedAsHostMemory) {
    EXPECT_EQ(memTypeFromLocation("segments:4096:0,1"), Topology::MEM_HOST);
    EXPECT_EQ(memTypeFromLocation("segments:4096:0"), Topology::MEM_HOST);
    EXPECT_EQ(memTypeFromLocation("cpu:0"), Topology::MEM_HOST);
    EXPECT_EQ(memTypeFromLocation("cuda:0"), Topology::MEM_CUDA);
}

TEST(TopologyPriorityMatrixTest, AmdGpuLocationUsesHipPrefixAndRocmAlias) {
    EXPECT_EQ(memTypeFromLocation("hip:0"), Topology::MEM_ROCM);
    EXPECT_EQ(memTypeFromLocation("rocm:3"), Topology::MEM_ROCM);
    EXPECT_EQ(makeAmdGpuLocation(2), "hip:2");
    EXPECT_TRUE(isAmdGpuLocationType("hip"));
    EXPECT_TRUE(isAmdGpuLocationType("rocm"));
    EXPECT_FALSE(isAmdGpuLocationType("cuda"));
    EXPECT_EQ(canonicalizeLocation("rocm:1"), "hip:1");
    EXPECT_EQ(canonicalizeLocation("hip:1"), "hip:1");
    EXPECT_EQ(canonicalizeLocation("cpu:0"), "cpu:0");

    Topology topology;
    ASSERT_TRUE(topology
                    .parsePriorityMatrix(R"json({
          "hip:0": [["mlx5_0"], ["mlx5_1"]],
          "rocm:1": [["mlx5_1"], ["mlx5_0"]]
        })json")
                    .ok());

    const auto* hip0 = topology.getMemEntry("hip:0");
    ASSERT_NE(hip0, nullptr);
    EXPECT_EQ(hip0->type, Topology::MEM_ROCM);

    // The legacy "rocm:1" key is stored under its canonical name, and both
    // the canonical and the legacy name resolve to the same entry.
    const auto* rocm1 = topology.getMemEntry("rocm:1");
    ASSERT_NE(rocm1, nullptr);
    EXPECT_EQ(rocm1->name, "hip:1");
    EXPECT_EQ(rocm1->type, Topology::MEM_ROCM);
    EXPECT_EQ(topology.getMemEntry("hip:1"), rocm1);
}

// Regression test: a matrix configured with only the legacy "rocm:N" key
// must be reachable via the canonical runtime name, so the configured NIC
// priorities are not silently ignored by exact-name lookups.
TEST(TopologyPriorityMatrixTest,
     LegacyRocmMatrixKeyResolvesViaCanonicalHipName) {
    Topology topology;
    ASSERT_TRUE(topology
                    .parsePriorityMatrix(R"json({
          "rocm:0": [["mlx5_0"], ["mlx5_1"]]
        })json")
                    .ok());

    // Stored under the canonical name only.
    const auto* hip0 = topology.getMemEntry("hip:0");
    ASSERT_NE(hip0, nullptr);
    EXPECT_EQ(hip0->name, "hip:0");
    EXPECT_EQ(hip0->type, Topology::MEM_ROCM);

    // Exact-id lookups with the canonical name resolve to the entry and
    // carry the configured per-GPU NIC priorities.
    const auto mem_id = topology.getMemId("hip:0");
    EXPECT_GE(mem_id, 0);
    const auto* by_id = topology.getMemEntry(mem_id);
    ASSERT_NE(by_id, nullptr);
    ASSERT_EQ(by_id->device_list[0].size(), 1u);
    EXPECT_EQ(topology.getNicName(by_id->device_list[0][0]), "mlx5_0");
    ASSERT_EQ(by_id->device_list[1].size(), 1u);
    EXPECT_EQ(topology.getNicName(by_id->device_list[1][0]), "mlx5_1");
}

TEST(TopologyPriorityMatrixTest, RejectsConflictingHipAndRocmKeys) {
    Topology topology;
    // "rocm:0" and "hip:0" refer to the same device; specifying both is a
    // config error rather than a silent last-wins.
    EXPECT_FALSE(topology
                     .parsePriorityMatrix(R"json({
          "hip:0": [["mlx5_0"], ["mlx5_1"]],
          "rocm:0": [["mlx5_1"], ["mlx5_0"]]
        })json")
                     .ok());
}

TEST(TopologyPriorityMatrixTest, ParseCustomRoutesNativeFormat) {
    constexpr const char* kNative = R"json(
        {
          "nics": [{"name": "mlx5_0", "type": 0, "numa_node": 0}],
          "mems": [{
            "name": "cpu:0",
            "type": 0,
            "numa_node": 0,
            "device_list": {"rank0": [0]}
          }]
        }
    )json";

    Topology topology;
    ASSERT_TRUE(topology.parseCustomTopology(kNative).ok());
    ASSERT_EQ(topology.getNicCount(), 1u);
    EXPECT_EQ(topology.getNicName(0), "mlx5_0");
    EXPECT_NE(topology.getMemEntry("cpu:0"), nullptr);
    // Native path does not synthesize wildcard.
    EXPECT_EQ(topology.getMemEntry(kWildcardLocation), nullptr);
}

TEST(TopologyPriorityMatrixTest, ParseCustomRoutesClassicMatrix) {
    Topology topology;
    ASSERT_TRUE(
        topology.parseCustomTopology(R"({"cpu:0": [["erdma_0"],["erdma_1"]]})")
            .ok());
    EXPECT_EQ(topology.getNicCount(), 2u);
    EXPECT_NE(topology.getMemEntry(kWildcardLocation), nullptr);
}

TEST(TopologyPriorityMatrixTest, LoadFromConfigInlineMatrix) {
    Config conf;
    ASSERT_TRUE(conf.load(R"json({
      "topology": {
        "priority_matrix": {
          "cpu:0": [["mlx5_0"], ["mlx5_1"]]
        },
        "custom_json_path": "/nonexistent/should_be_ignored.json"
      }
    })json")
                    .ok());

    Topology topology;
    ASSERT_TRUE(topology.loadFromConfig(conf, {}).ok());
    EXPECT_EQ(topology.getNicName(0), "mlx5_0");
    EXPECT_EQ(topology.getNicName(1), "mlx5_1");
    EXPECT_NE(topology.getMemEntry("cpu:0"), nullptr);
}

TEST(TopologyPriorityMatrixTest, LoadFromConfigPathFile) {
    TempTopologyFile file(R"({"cpu:0": [["mlx5_2"], ["mlx5_3"]]})");
    Config conf;
    conf.set("topology/custom_json_path", file.path());

    Topology topology;
    ASSERT_TRUE(topology.loadFromConfig(conf, {}).ok());
    EXPECT_EQ(topology.getNicName(0), "mlx5_2");
    EXPECT_EQ(topology.getNicName(1), "mlx5_3");
}

TEST(TopologyPriorityMatrixTest, LoadFromConfigInlinePreferredOverPath) {
    TempTopologyFile file(R"({"cpu:0": [["from_file"], []]})");
    Config conf;
    ASSERT_TRUE(conf.load(R"json({
      "topology": {
        "priority_matrix": {
          "cpu:0": [["from_inline"], []]
        }
      }
    })json")
                    .ok());
    conf.set("topology/custom_json_path", file.path());

    Topology topology;
    ASSERT_TRUE(topology.loadFromConfig(conf, {}).ok());
    EXPECT_EQ(topology.getNicName(0), "from_inline");
}

TEST(TopologyPriorityMatrixTest, LoadFromConfigInvalidInlineFallsBack) {
    Config conf;
    ASSERT_TRUE(conf.load(R"json({
      "topology": {
        "priority_matrix": {
          "cpu:0": ["bad"]
        }
      }
    })json")
                    .ok());

    Topology topology;
    // Empty platform list → discover yields empty topology.
    ASSERT_TRUE(topology.loadFromConfig(conf, {}).ok());
    EXPECT_TRUE(topology.empty());
}

TEST(ConfigDumpSubtreeTest, DumpsNestedPriorityMatrix) {
    Config conf;
    ASSERT_TRUE(conf.load(R"json({
      "topology": {
        "priority_matrix": {
          "cpu:0": [["mlx5_0"], []]
        }
      }
    })json")
                    .ok());

    std::string dumped;
    ASSERT_TRUE(conf.dumpSubtree("topology/priority_matrix", &dumped));
    Topology topology;
    ASSERT_TRUE(topology.parsePriorityMatrix(dumped).ok());
    EXPECT_EQ(topology.getNicName(0), "mlx5_0");
}

TEST(ConfigEnvMappingTest, CustomTopoJsonEnvLoadsPath) {
    const char* name = "MC_CUSTOM_TOPO_JSON";
    const char* old = std::getenv(name);
    std::string old_value = old ? old : "";
    setenv(name, "/tmp/mooncake-nic-priority-matrix.json", 1);

    Config config;
    ASSERT_TRUE(ConfigHelper().loadFromEnv(config).ok());
    EXPECT_EQ(config.get("topology/custom_json_path", ""),
              "/tmp/mooncake-nic-priority-matrix.json");

    if (old) {
        setenv(name, old_value.c_str(), 1);
    } else {
        unsetenv(name);
    }
}

}  // namespace
}  // namespace tent
}  // namespace mooncake

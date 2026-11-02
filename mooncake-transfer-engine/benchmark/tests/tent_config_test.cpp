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

#include "tent_backend.h"

#include <gtest/gtest.h>
#include <numa.h>

namespace mooncake::tent {

std::shared_ptr<Config> loadConfig();

namespace {

class TENTConfigTest : public testing::Test {
   protected:
    void SetUp() override {
        saved_transport_ = XferBenchConfig::xport_type;
        saved_segment_ = XferBenchConfig::seg_type;
        saved_mix_ = XferBenchConfig::seg_type_mix;
        saved_backend_ = XferBenchConfig::backend;
        saved_numa_ = XferBenchConfig::local_numa_id;
        XferBenchConfig::seg_type_mix.clear();
        XferBenchConfig::seg_type = "DRAM";
        XferBenchConfig::backend = "tent";
        XferBenchConfig::local_numa_id = -1;
    }

    void TearDown() override {
        XferBenchConfig::xport_type = saved_transport_;
        XferBenchConfig::seg_type = saved_segment_;
        XferBenchConfig::seg_type_mix = saved_mix_;
        XferBenchConfig::backend = saved_backend_;
        XferBenchConfig::local_numa_id = saved_numa_;
    }

    static int usableNumaNode() {
        if (numa_available() < 0 || !numa_all_nodes_ptr) return -1;
        for (int node = numa_max_node(); node >= 0; --node) {
            long long free_bytes = 0;
            if (numa_bitmask_isbitset(numa_all_nodes_ptr, node) &&
                numa_node_size64(node, &free_bytes) > 0)
                return node;
        }
        return -1;
    }

    std::string saved_transport_, saved_segment_, saved_mix_, saved_backend_;
    int saved_numa_ = -1;
};

TEST_F(TENTConfigTest, NetworkSelectionRetainsMluHostCopies) {
    for (const auto* transport : {"tcp", "hp_tcp"}) {
        SCOPED_TRACE(transport);
        XferBenchConfig::xport_type = transport;
        for (const auto* mix : {"", " DRAM, VRAM "}) {
            SCOPED_TRACE(mix);
            XferBenchConfig::seg_type = "VRAM";
            XferBenchConfig::seg_type_mix = mix;
            auto config = loadConfig();
            EXPECT_TRUE(config->get(
                std::string("transports/") + transport + "/enable", false));
            EXPECT_FALSE(config->get("transports/rdma/enable", true));
#ifdef USE_MLU
            EXPECT_TRUE(config->get("transports/mlu/enable", false));
#else
            EXPECT_FALSE(config->contains("transports/mlu/enable"));
#endif
        }
    }
}

TEST_F(TENTConfigTest, DramOnlyDoesNotEnableMluHostCopies) {
    XferBenchConfig::xport_type = "tcp";
    XferBenchConfig::seg_type = "DRAM";
    EXPECT_FALSE(loadConfig()->contains("transports/mlu/enable"));
    XferBenchConfig::seg_type = "VRAM";
    XferBenchConfig::seg_type_mix = "dram";
    EXPECT_FALSE(loadConfig()->contains("transports/mlu/enable"));
}

TEST_F(TENTConfigTest, AutomaticSelectionLeavesTransportsUnset) {
    XferBenchConfig::xport_type.clear();
    XferBenchConfig::seg_type = "VRAM";
    XferBenchConfig::seg_type_mix = "dram,vram";
    auto config = loadConfig();
    EXPECT_FALSE(config->contains("transports/mlu/enable"));
    EXPECT_FALSE(config->contains("transports/tcp/enable"));
    EXPECT_FALSE(config->contains("transports/hp_tcp/enable"));
}

TEST_F(TENTConfigTest, NumaFlagDefaultsToAllNodes) {
    std::string value;
    ASSERT_TRUE(gflags::GetCommandLineOption("local_numa_id", &value));
    EXPECT_EQ(value, "-1");
    std::string error;
    EXPECT_TRUE(XferBenchConfig::validateLocalNuma(error)) << error;
    int start_idx = 7, num_buffers = 7;
    ASSERT_TRUE(
        XferBenchConfig::resolveDramNumaRange(start_idx, num_buffers, error))
        << error;
    EXPECT_EQ(start_idx, 0);
    EXPECT_EQ(num_buffers, numa_num_configured_nodes());
    // The default must not restrict classic or VRAM-only configurations.
    XferBenchConfig::backend = "classic";
    XferBenchConfig::seg_type = "VRAM";
    EXPECT_TRUE(XferBenchConfig::validateLocalNuma(error)) << error;
}

TEST_F(TENTConfigTest, ExplicitNumaSelectsExactlyOneBuffer) {
    const int node = usableNumaNode();
    if (node < 0) GTEST_SKIP() << "No usable NUMA node";
    XferBenchConfig::local_numa_id = node;
    std::string error;
    ASSERT_TRUE(XferBenchConfig::validateLocalNuma(error)) << error;
    int start_idx = 0, num_buffers = 0;
    ASSERT_TRUE(
        XferBenchConfig::resolveDramNumaRange(start_idx, num_buffers, error))
        << error;
    EXPECT_EQ(start_idx, node);
    EXPECT_EQ(num_buffers, 1);
}

TEST_F(TENTConfigTest, NumaSelectionRejectsInvalidIds) {
    std::string error;
    int start_idx = 7, num_buffers = 7;
    for (int node : {-2, std::numeric_limits<int>::max()}) {
        SCOPED_TRACE(node);
        XferBenchConfig::local_numa_id = node;
        EXPECT_FALSE(XferBenchConfig::validateLocalNuma(error));
        EXPECT_FALSE(error.empty());
        EXPECT_FALSE(XferBenchConfig::resolveDramNumaRange(start_idx,
                                                           num_buffers, error));
        EXPECT_EQ(start_idx, 0);
        EXPECT_EQ(num_buffers, 0);
    }
}

TEST_F(TENTConfigTest, NumaSelectionRejectsClassicAndVramOnly) {
    XferBenchConfig::local_numa_id = 0;
    XferBenchConfig::backend = "classic";
    std::string error;
    EXPECT_FALSE(XferBenchConfig::validateLocalNuma(error));
    EXPECT_NE(error.find("--backend=tent"), std::string::npos);
    XferBenchConfig::backend = "tent";
    XferBenchConfig::seg_type = "VRAM";
    EXPECT_FALSE(XferBenchConfig::validateLocalNuma(error));
    EXPECT_NE(error.find("requires DRAM"), std::string::npos);
    XferBenchConfig::seg_type = "DRAM";
    XferBenchConfig::seg_type_mix = " VRAM ";
    EXPECT_FALSE(XferBenchConfig::validateLocalNuma(error));
    EXPECT_NE(error.find("requires DRAM"), std::string::npos);
}

TEST_F(TENTConfigTest, NumaSelectionSupportsMixedDramSegments) {
    const int node = usableNumaNode();
    if (node < 0) GTEST_SKIP() << "No usable NUMA node";
    XferBenchConfig::local_numa_id = node;
    XferBenchConfig::seg_type = "VRAM";
    XferBenchConfig::seg_type_mix = " VRAM, dRaM ";
    std::string error;
    EXPECT_TRUE(XferBenchConfig::validateLocalNuma(error)) << error;
    XferBenchConfig::seg_type = "dram";
    XferBenchConfig::seg_type_mix = " , ";
    EXPECT_TRUE(XferBenchConfig::validateLocalNuma(error)) << error;
}

}  // namespace
}  // namespace mooncake::tent

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

namespace mooncake::tent {

std::shared_ptr<Config> loadConfig();

namespace {

class TENTConfigTest : public testing::Test {
   protected:
    void SetUp() override {
        saved_transport_ = XferBenchConfig::xport_type;
        saved_segment_ = XferBenchConfig::seg_type;
        saved_mix_ = XferBenchConfig::seg_type_mix;
        XferBenchConfig::seg_type_mix.clear();
    }

    void TearDown() override {
        XferBenchConfig::xport_type = saved_transport_;
        XferBenchConfig::seg_type = saved_segment_;
        XferBenchConfig::seg_type_mix = saved_mix_;
    }

    std::string saved_transport_, saved_segment_, saved_mix_;
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

}  // namespace
}  // namespace mooncake::tent

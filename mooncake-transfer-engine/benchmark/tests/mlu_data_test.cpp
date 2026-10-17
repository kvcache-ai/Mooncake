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

#include "utils.h"

#include <gtest/gtest.h>
#include <thread>

namespace mooncake::tent {
namespace {

class MluDataTest : public testing::Test {
   protected:
    void SetUp() override {
        unsigned int count = 0;
        if (cnrtGetDeviceCount(&count) != cnrtSuccess || count == 0)
            GTEST_SKIP() << "No MLU device visible";
        ASSERT_EQ(cnrtSetDevice(0), cnrtSuccess);
        ASSERT_EQ(cnrtMalloc(&device_, kSize + kOffset), cnrtSuccess);
        saved_transport_ = XferBenchConfig::xport_type;
        saved_check_ = XferBenchConfig::check_consistency;
    }

    void TearDown() override {
        if (!device_) return;
        EXPECT_EQ(cnrtFree(device_), cnrtSuccess);
        XferBenchConfig::xport_type = saved_transport_;
        XferBenchConfig::check_consistency = saved_check_;
    }

    static constexpr size_t kSize = 65539;
    static constexpr size_t kOffset = 17;
    void* device_ = nullptr;
    std::string saved_transport_;
    bool saved_check_ = false;
};

TEST_F(MluDataTest, InteriorDeviceDataMatchesHostPatternOnFreshWorker) {
    auto* ptr = static_cast<uint8_t*>(device_) + kOffset;
    std::thread worker([&] {
        std::vector<uint8_t> expected(kSize), actual(kSize);
        EXPECT_FALSE(isMluMemory(expected.data()));
        ASSERT_TRUE(isMluMemory(ptr));
        for (const auto* transport : {"tcp", "hp_tcp"}) {
            XferBenchConfig::xport_type = transport;
            for (bool check : {false, true}) {
                XferBenchConfig::check_consistency = check;
                fillData(expected.data(), kSize, 37);
                fillData(ptr, kSize, 37);
                ASSERT_EQ(
                    cnrtMemcpy(actual.data(), ptr, kSize, cnrtMemcpyDevToHost),
                    cnrtSuccess);
                EXPECT_EQ(actual, expected);
                verifyData(ptr, kSize, 37);
            }
        }
    });
    worker.join();
}

using MluDataDeathTest = MluDataTest;

TEST_F(MluDataDeathTest, RejectsCorruptedDeviceData) {
    testing::FLAGS_gtest_death_test_style = "threadsafe";
    XferBenchConfig::xport_type = "tcp";
    XferBenchConfig::check_consistency = true;
    fillData(device_, kSize, 37);
    ASSERT_EQ(cnrtMemset(static_cast<uint8_t*>(device_) + kOffset, 38, 1),
              cnrtSuccess);
    EXPECT_DEATH(verifyData(device_, kSize, 37), "Inconsistent data detected");
}

}  // namespace
}  // namespace mooncake::tent

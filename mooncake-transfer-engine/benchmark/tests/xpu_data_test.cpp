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
#include "tent/runtime/platform.h"

#include <gtest/gtest.h>

namespace mooncake::tent {
namespace {

constexpr size_t kSize = 65539;
constexpr size_t kOffset = 17;

void* allocateXpu() {
    MemoryOptions options;
    options.location = "xpu:0";
    void* ptr = nullptr;
    if (!Platform::getLoader().allocate(&ptr, kSize + kOffset, options).ok())
        return nullptr;
    return ptr;
}

class XpuDataTest : public testing::Test {
   protected:
    void SetUp() override {
        device_ = allocateXpu();
        if (!device_) GTEST_SKIP() << "No XPU device visible";
        saved_transport_ = XferBenchConfig::xport_type;
        saved_check_ = XferBenchConfig::check_consistency;
    }

    void TearDown() override {
        if (!device_) return;
        EXPECT_TRUE(Platform::getLoader().free(device_, kSize + kOffset).ok());
        XferBenchConfig::xport_type = saved_transport_;
        XferBenchConfig::check_consistency = saved_check_;
    }

    void* device_ = nullptr;
    std::string saved_transport_;
    bool saved_check_ = false;
};

TEST_F(XpuDataTest, InteriorDeviceDataMatchesHostPattern) {
    auto* ptr = static_cast<uint8_t*>(device_) + kOffset;
    for (const auto* transport : {"tcp", "hp_tcp"}) {
        XferBenchConfig::xport_type = transport;
        for (bool check : {false, true}) {
            XferBenchConfig::check_consistency = check;
            std::vector<uint8_t> expected(kSize), actual(kSize);
            EXPECT_FALSE(isXpuMemory(expected.data()));
            ASSERT_TRUE(isXpuMemory(ptr));
            fillData(expected.data(), kSize, 37);
            fillData(ptr, kSize, 37);
            ASSERT_TRUE(
                Platform::getLoader().copy(actual.data(), ptr, kSize).ok());
            EXPECT_EQ(actual, expected);
            verifyData(ptr, kSize, 37);
        }
    }
}

TEST(XpuDataDeathTest, RejectsCorruptedDeviceData) {
    void* probe = allocateXpu();
    if (!probe) GTEST_SKIP() << "No XPU device visible";
    ASSERT_TRUE(Platform::getLoader().free(probe, kSize + kOffset).ok());
    testing::FLAGS_gtest_death_test_style = "threadsafe";
    EXPECT_DEATH(
        ([] {
            // Create the device allocation in the child process.
            void* device = allocateXpu();
            LOG_ASSERT(device) << "XPU allocation failed";
            XferBenchConfig::xport_type = "tcp";
            XferBenchConfig::check_consistency = true;
            fillData(device, kSize, 37);
            uint8_t bad = 38;
            LOG_ASSERT(
                Platform::getLoader()
                    .copy(static_cast<uint8_t*>(device) + kOffset, &bad, 1)
                    .ok());
            verifyData(device, kSize, 37);
        }()),
        "Inconsistent data detected");
}

}  // namespace
}  // namespace mooncake::tent

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
#include <limits>
#include <string>

#include "adaptive_congestion_control_config.h"

namespace mooncake::adaptive_cc {
namespace {

class AdaptiveCongestionControlConfigTest : public ::testing::Test {
   protected:
    void SetUp() override { clear(); }
    void TearDown() override { clear(); }

    static void clear() {
        unsetenv("MC_ADAPTIVE_CC_MODE");
        unsetenv("MC_ADAPTIVE_CC_MIN_WINDOW_BYTES");
        unsetenv("MC_ADAPTIVE_CC_MAX_WINDOW_BYTES");
        unsetenv("MC_ADAPTIVE_CC_TARGET_DRAIN_US");
    }
};

TEST_F(AdaptiveCongestionControlConfigTest, DefaultsToOff) {
    const ConfigLoadResult result = loadConfigFromEnvironment();
    EXPECT_TRUE(result.valid);
    EXPECT_EQ(result.config.mode, Mode::kOff);
}

TEST_F(AdaptiveCongestionControlConfigTest, LoadsValidValues) {
    ASSERT_EQ(setenv("MC_ADAPTIVE_CC_MODE", "enforce", 1), 0);
    ASSERT_EQ(setenv("MC_ADAPTIVE_CC_MIN_WINDOW_BYTES", "1048576", 1), 0);
    ASSERT_EQ(setenv("MC_ADAPTIVE_CC_MAX_WINDOW_BYTES", "8388608", 1), 0);
    ASSERT_EQ(setenv("MC_ADAPTIVE_CC_TARGET_DRAIN_US", "250", 1), 0);

    const ConfigLoadResult result = loadConfigFromEnvironment();
    ASSERT_TRUE(result.valid);
    EXPECT_EQ(result.config.mode, Mode::kEnforce);
    EXPECT_EQ(result.config.min_window_bytes, 1'048'576u);
    EXPECT_EQ(result.config.max_window_bytes, 8'388'608u);
    EXPECT_EQ(result.config.target_drain_time_ns, 250'000u);
}

TEST_F(AdaptiveCongestionControlConfigTest, AcceptsObserveMode) {
    ASSERT_EQ(setenv("MC_ADAPTIVE_CC_MODE", "observe", 1), 0);
    const ConfigLoadResult result = loadConfigFromEnvironment();
    EXPECT_TRUE(result.valid);
    EXPECT_EQ(result.config.mode, Mode::kObserve);
}

TEST_F(AdaptiveCongestionControlConfigTest, InvalidModeFailsClosed) {
    ASSERT_EQ(setenv("MC_ADAPTIVE_CC_MODE", "enabled", 1), 0);
    const ConfigLoadResult result = loadConfigFromEnvironment();
    EXPECT_FALSE(result.valid);
    EXPECT_EQ(result.config.mode, Mode::kOff);
    EXPECT_EQ(result.error, "MC_ADAPTIVE_CC_MODE");
}

TEST_F(AdaptiveCongestionControlConfigTest, InvalidNumberFailsClosed) {
    ASSERT_EQ(setenv("MC_ADAPTIVE_CC_MODE", "enforce", 1), 0);
    ASSERT_EQ(setenv("MC_ADAPTIVE_CC_MIN_WINDOW_BYTES", "1MB", 1), 0);
    const ConfigLoadResult result = loadConfigFromEnvironment();
    EXPECT_FALSE(result.valid);
    EXPECT_EQ(result.config.mode, Mode::kOff);
    EXPECT_EQ(result.error, "MC_ADAPTIVE_CC_MIN_WINDOW_BYTES");
}

TEST_F(AdaptiveCongestionControlConfigTest, InvalidWindowRangeFailsClosed) {
    ASSERT_EQ(setenv("MC_ADAPTIVE_CC_MODE", "enforce", 1), 0);
    ASSERT_EQ(setenv("MC_ADAPTIVE_CC_MIN_WINDOW_BYTES", "4096", 1), 0);
    ASSERT_EQ(setenv("MC_ADAPTIVE_CC_MAX_WINDOW_BYTES", "1024", 1), 0);
    const ConfigLoadResult result = loadConfigFromEnvironment();
    EXPECT_FALSE(result.valid);
    EXPECT_EQ(result.config.mode, Mode::kOff);
    EXPECT_EQ(result.error, "adaptive congestion window range");
}

TEST_F(AdaptiveCongestionControlConfigTest, DrainTimeOverflowFailsClosed) {
    ASSERT_EQ(setenv("MC_ADAPTIVE_CC_MODE", "enforce", 1), 0);
    const std::string value =
        std::to_string(std::numeric_limits<uint64_t>::max());
    ASSERT_EQ(setenv("MC_ADAPTIVE_CC_TARGET_DRAIN_US", value.c_str(), 1), 0);
    const ConfigLoadResult result = loadConfigFromEnvironment();
    EXPECT_FALSE(result.valid);
    EXPECT_EQ(result.config.mode, Mode::kOff);
    EXPECT_EQ(result.error, "MC_ADAPTIVE_CC_TARGET_DRAIN_US");
}

}  // namespace
}  // namespace mooncake::adaptive_cc

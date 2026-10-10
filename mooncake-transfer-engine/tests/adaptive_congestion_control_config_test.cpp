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

#include <array>
#include <cstdlib>
#include <limits>
#include <string>

#include "adaptive_congestion_control_config.h"

namespace mooncake::adaptive_congestion_control {
namespace {

class AdaptiveCongestionControlConfigTest : public ::testing::Test {
   protected:
    void SetUp() override { clear(); }
    void TearDown() override { clear(); }

    static void clear() {
        unsetenv("MC_ADAPTIVE_CONGESTION_CONTROL_MODE");
        unsetenv("MC_ADAPTIVE_CONGESTION_CONTROL_MIN_WINDOW_BYTES");
        unsetenv("MC_ADAPTIVE_CONGESTION_CONTROL_MAX_WINDOW_BYTES");
        unsetenv("MC_ADAPTIVE_CONGESTION_CONTROL_TARGET_DRAIN_US");
        unsetenv("MC_ADAPTIVE_CONGESTION_CONTROL_HIGH_PRESSURE_EPOCHS");
        unsetenv("MC_ADAPTIVE_CONGESTION_CONTROL_LOW_PRESSURE_EPOCHS");
        unsetenv("MC_ADAPTIVE_CONGESTION_CONTROL_HARD_ERROR_THRESHOLD");
        unsetenv("MC_ADAPTIVE_CONGESTION_CONTROL_COOLDOWN_MS");
        unsetenv("MC_ADAPTIVE_CONGESTION_CONTROL_PROBE_WINDOW_BYTES");
    }
};

TEST_F(AdaptiveCongestionControlConfigTest, DefaultsToOff) {
    const ConfigLoadResult result = loadConfigFromEnvironment();
    EXPECT_TRUE(result.valid);
    EXPECT_EQ(result.config.mode, Mode::kOff);
}

TEST_F(AdaptiveCongestionControlConfigTest, LoadsValidValues) {
    ASSERT_EQ(setenv("MC_ADAPTIVE_CONGESTION_CONTROL_MODE", "enforce", 1), 0);
    ASSERT_EQ(
        setenv("MC_ADAPTIVE_CONGESTION_CONTROL_MIN_WINDOW_BYTES", "1048576", 1),
        0);
    ASSERT_EQ(
        setenv("MC_ADAPTIVE_CONGESTION_CONTROL_MAX_WINDOW_BYTES", "8388608", 1),
        0);
    ASSERT_EQ(
        setenv("MC_ADAPTIVE_CONGESTION_CONTROL_TARGET_DRAIN_US", "250", 1), 0);

    const ConfigLoadResult result = loadConfigFromEnvironment();
    ASSERT_TRUE(result.valid);
    EXPECT_EQ(result.config.mode, Mode::kEnforce);
    EXPECT_EQ(result.config.min_window_bytes, 1'048'576u);
    EXPECT_EQ(result.config.max_window_bytes, 8'388'608u);
    EXPECT_EQ(result.config.target_drain_time_ns, 250'000u);
}

TEST_F(AdaptiveCongestionControlConfigTest, AcceptsObserveMode) {
    ASSERT_EQ(setenv("MC_ADAPTIVE_CONGESTION_CONTROL_MODE", "observe", 1), 0);
    const ConfigLoadResult result = loadConfigFromEnvironment();
    EXPECT_TRUE(result.valid);
    EXPECT_EQ(result.config.mode, Mode::kObserve);
}

TEST_F(AdaptiveCongestionControlConfigTest, InvalidModeFailsClosed) {
    ASSERT_EQ(setenv("MC_ADAPTIVE_CONGESTION_CONTROL_MODE", "enabled", 1), 0);
    const ConfigLoadResult result = loadConfigFromEnvironment();
    EXPECT_FALSE(result.valid);
    EXPECT_EQ(result.config.mode, Mode::kOff);
    EXPECT_EQ(result.error, "MC_ADAPTIVE_CONGESTION_CONTROL_MODE");
}

TEST_F(AdaptiveCongestionControlConfigTest, InvalidNumberFailsClosed) {
    ASSERT_EQ(setenv("MC_ADAPTIVE_CONGESTION_CONTROL_MODE", "enforce", 1), 0);
    ASSERT_EQ(
        setenv("MC_ADAPTIVE_CONGESTION_CONTROL_MIN_WINDOW_BYTES", "1MB", 1), 0);
    const ConfigLoadResult result = loadConfigFromEnvironment();
    EXPECT_FALSE(result.valid);
    EXPECT_EQ(result.config.mode, Mode::kOff);
    EXPECT_EQ(result.error, "MC_ADAPTIVE_CONGESTION_CONTROL_MIN_WINDOW_BYTES");
}

TEST_F(AdaptiveCongestionControlConfigTest, InvalidWindowRangeFailsClosed) {
    ASSERT_EQ(setenv("MC_ADAPTIVE_CONGESTION_CONTROL_MODE", "enforce", 1), 0);
    ASSERT_EQ(
        setenv("MC_ADAPTIVE_CONGESTION_CONTROL_MIN_WINDOW_BYTES", "4096", 1),
        0);
    ASSERT_EQ(
        setenv("MC_ADAPTIVE_CONGESTION_CONTROL_MAX_WINDOW_BYTES", "1024", 1),
        0);
    const ConfigLoadResult result = loadConfigFromEnvironment();
    EXPECT_FALSE(result.valid);
    EXPECT_EQ(result.config.mode, Mode::kOff);
    EXPECT_EQ(result.error, "adaptive congestion window range");
}

TEST_F(AdaptiveCongestionControlConfigTest, DrainTimeOverflowFailsClosed) {
    ASSERT_EQ(setenv("MC_ADAPTIVE_CONGESTION_CONTROL_MODE", "enforce", 1), 0);
    const std::string value =
        std::to_string(std::numeric_limits<uint64_t>::max());
    ASSERT_EQ(setenv("MC_ADAPTIVE_CONGESTION_CONTROL_TARGET_DRAIN_US",
                     value.c_str(), 1),
              0);
    const ConfigLoadResult result = loadConfigFromEnvironment();
    EXPECT_FALSE(result.valid);
    EXPECT_EQ(result.config.mode, Mode::kOff);
    EXPECT_EQ(result.error, "MC_ADAPTIVE_CONGESTION_CONTROL_TARGET_DRAIN_US");
}

TEST_F(AdaptiveCongestionControlConfigTest, LoadsRecoveryThresholdOverrides) {
    ASSERT_EQ(setenv("MC_ADAPTIVE_CONGESTION_CONTROL_MODE", "enforce", 1), 0);
    ASSERT_EQ(
        setenv("MC_ADAPTIVE_CONGESTION_CONTROL_HIGH_PRESSURE_EPOCHS", "4", 1),
        0);
    ASSERT_EQ(
        setenv("MC_ADAPTIVE_CONGESTION_CONTROL_LOW_PRESSURE_EPOCHS", "5", 1),
        0);
    ASSERT_EQ(
        setenv("MC_ADAPTIVE_CONGESTION_CONTROL_HARD_ERROR_THRESHOLD", "6", 1),
        0);
    ASSERT_EQ(setenv("MC_ADAPTIVE_CONGESTION_CONTROL_COOLDOWN_MS", "1500", 1),
              0);
    ASSERT_EQ(setenv("MC_ADAPTIVE_CONGESTION_CONTROL_PROBE_WINDOW_BYTES",
                     "131072", 1),
              0);

    const ConfigLoadResult result = loadConfigFromEnvironment();
    ASSERT_TRUE(result.valid) << result.error;
    EXPECT_EQ(result.config.high_pressure_epochs, 4u);
    EXPECT_EQ(result.config.low_pressure_epochs, 5u);
    EXPECT_EQ(result.config.hard_error_threshold, 6u);
    EXPECT_EQ(result.config.cooldown_ns, 1'500'000'000u);
    EXPECT_EQ(result.config.probe_window_bytes, 131'072u);
}

TEST_F(AdaptiveCongestionControlConfigTest,
       RejectsInvalidRecoveryThresholdOverrides) {
    constexpr std::array<const char*, 5> names = {
        "MC_ADAPTIVE_CONGESTION_CONTROL_HIGH_PRESSURE_EPOCHS",
        "MC_ADAPTIVE_CONGESTION_CONTROL_LOW_PRESSURE_EPOCHS",
        "MC_ADAPTIVE_CONGESTION_CONTROL_HARD_ERROR_THRESHOLD",
        "MC_ADAPTIVE_CONGESTION_CONTROL_COOLDOWN_MS",
        "MC_ADAPTIVE_CONGESTION_CONTROL_PROBE_WINDOW_BYTES"};
    ASSERT_EQ(setenv("MC_ADAPTIVE_CONGESTION_CONTROL_MODE", "enforce", 1), 0);
    for (const char* name : names) {
        for (const char* invalid_value : {"0", "-1", "not-a-number"}) {
            SCOPED_TRACE(std::string(name) + "=" + invalid_value);
            ASSERT_EQ(setenv(name, invalid_value, 1), 0);
            const ConfigLoadResult result = loadConfigFromEnvironment();
            EXPECT_FALSE(result.valid);
            EXPECT_EQ(result.config.mode, Mode::kOff);
            EXPECT_EQ(result.error, name);
            ASSERT_EQ(unsetenv(name), 0);
        }
    }
}

TEST_F(AdaptiveCongestionControlConfigTest, RejectsEpochCounterOverflow) {
    ASSERT_EQ(setenv("MC_ADAPTIVE_CONGESTION_CONTROL_MODE", "enforce", 1), 0);
    constexpr std::array<const char*, 3> names = {
        "MC_ADAPTIVE_CONGESTION_CONTROL_HIGH_PRESSURE_EPOCHS",
        "MC_ADAPTIVE_CONGESTION_CONTROL_LOW_PRESSURE_EPOCHS",
        "MC_ADAPTIVE_CONGESTION_CONTROL_HARD_ERROR_THRESHOLD"};
    for (const char* name : names) {
        SCOPED_TRACE(name);
        ASSERT_EQ(setenv(name, "4294967296", 1), 0);
        const ConfigLoadResult result = loadConfigFromEnvironment();
        EXPECT_FALSE(result.valid);
        EXPECT_EQ(result.error, name);
        ASSERT_EQ(unsetenv(name), 0);
    }
}

TEST_F(AdaptiveCongestionControlConfigTest, RejectsCooldownOverflow) {
    ASSERT_EQ(setenv("MC_ADAPTIVE_CONGESTION_CONTROL_MODE", "enforce", 1), 0);
    const std::string value =
        std::to_string(std::numeric_limits<uint64_t>::max());
    ASSERT_EQ(
        setenv("MC_ADAPTIVE_CONGESTION_CONTROL_COOLDOWN_MS", value.c_str(), 1),
        0);
    const ConfigLoadResult result = loadConfigFromEnvironment();
    EXPECT_FALSE(result.valid);
    EXPECT_EQ(result.error, "MC_ADAPTIVE_CONGESTION_CONTROL_COOLDOWN_MS");
}

TEST_F(AdaptiveCongestionControlConfigTest, RejectsOversizedProbeWindow) {
    ASSERT_EQ(setenv("MC_ADAPTIVE_CONGESTION_CONTROL_MODE", "enforce", 1), 0);
    ASSERT_EQ(
        setenv("MC_ADAPTIVE_CONGESTION_CONTROL_MIN_WINDOW_BYTES", "4096", 1),
        0);
    ASSERT_EQ(
        setenv("MC_ADAPTIVE_CONGESTION_CONTROL_PROBE_WINDOW_BYTES", "8192", 1),
        0);
    const ConfigLoadResult result = loadConfigFromEnvironment();
    EXPECT_FALSE(result.valid);
    EXPECT_EQ(result.error, "adaptive congestion probe window range");
}

TEST_F(AdaptiveCongestionControlConfigTest,
       SmallMinWindowWithoutProbeOverrideRemainsValid) {
    ASSERT_EQ(
        setenv("MC_ADAPTIVE_CONGESTION_CONTROL_MIN_WINDOW_BYTES", "4096", 1),
        0);
    const ConfigLoadResult result = loadConfigFromEnvironment();
    ASSERT_TRUE(result.valid);
    EXPECT_EQ(result.config.probe_window_bytes, 64u << 10);
    DomainState domain(result.config);
    EXPECT_EQ(snapshot(domain).window_bytes, 4096u);
}

}  // namespace
}  // namespace mooncake::adaptive_congestion_control

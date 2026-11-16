// Copyright 2024 KVCache.AI
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

#include "config.h"

namespace mooncake {
namespace {

// --- MC_PKEY_INDEX (stoi with try-catch, range 0-65535) ---

class PkeyIndexEnvTest : public ::testing::Test {
   protected:
    void TearDown() override {
        ::unsetenv("MC_PKEY_INDEX");
        ::unsetenv("MC_AUTO_GID_MAX_RETRIES");
        ::unsetenv("MC_IB_SL");
        ::unsetenv("MC_TE_METADATA_REFRESH_INTERVAL_SECONDS");
    }
};

TEST_F(PkeyIndexEnvTest, DefaultIsZeroWhenUnset) {
    ::unsetenv("MC_PKEY_INDEX");
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.pkey_index, 0);
}

TEST_F(PkeyIndexEnvTest, ValidOverrideIsApplied) {
    ASSERT_EQ(::setenv("MC_PKEY_INDEX", "7", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.pkey_index, 7);
}

TEST_F(PkeyIndexEnvTest, MaxBoundaryIsApplied) {
    ASSERT_EQ(::setenv("MC_PKEY_INDEX", "65535", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.pkey_index, 65535);
}

TEST_F(PkeyIndexEnvTest, OutOfRangeIsIgnored) {
    ASSERT_EQ(::setenv("MC_PKEY_INDEX", "70000", 1), 0);
    GlobalConfig config;
    config.pkey_index = 3;  // sentinel preserved when env var is rejected
    loadGlobalConfig(config);
    EXPECT_EQ(config.pkey_index, 3);
}

TEST_F(PkeyIndexEnvTest, NegativeIsIgnored) {
    ASSERT_EQ(::setenv("MC_PKEY_INDEX", "-1", 1), 0);
    GlobalConfig config;
    config.pkey_index = 5;
    loadGlobalConfig(config);
    EXPECT_EQ(config.pkey_index, 5);
}

TEST_F(PkeyIndexEnvTest, NonNumericKeepsDefault) {
    ASSERT_EQ(::setenv("MC_PKEY_INDEX", "abc", 1), 0);
    GlobalConfig config;
    config.pkey_index = 9;
    loadGlobalConfig(config);
    EXPECT_EQ(config.pkey_index, 9);
}

TEST_F(PkeyIndexEnvTest, EmptyStringKeepsDefault) {
    ASSERT_EQ(::setenv("MC_PKEY_INDEX", "", 1), 0);
    GlobalConfig config;
    config.pkey_index = 4;
    loadGlobalConfig(config);
    EXPECT_EQ(config.pkey_index, 4);
}

TEST_F(PkeyIndexEnvTest, IbSlDefaultsToMinusOneWhenUnset) {
    ::unsetenv("MC_IB_SL");
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.ib_service_level, -1);
}

TEST_F(PkeyIndexEnvTest, IbSlValidOverrideIsApplied) {
    ASSERT_EQ(::setenv("MC_IB_SL", "3", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.ib_service_level, 3);
}

TEST_F(PkeyIndexEnvTest, IbSlMinBoundaryIsApplied) {
    ASSERT_EQ(::setenv("MC_IB_SL", "0", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.ib_service_level, 0);
}

TEST_F(PkeyIndexEnvTest, IbSlMaxBoundaryIsApplied) {
    ASSERT_EQ(::setenv("MC_IB_SL", "15", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.ib_service_level, 15);
}

TEST_F(PkeyIndexEnvTest, IbSlOutOfRangeIsIgnored) {
    ASSERT_EQ(::setenv("MC_IB_SL", "16", 1), 0);
    GlobalConfig config;
    config.ib_service_level = 7;  // sentinel preserved when env var is rejected
    loadGlobalConfig(config);
    EXPECT_EQ(config.ib_service_level, 7);
}

TEST_F(PkeyIndexEnvTest, IbSlNegativeIsIgnored) {
    ASSERT_EQ(::setenv("MC_IB_SL", "-1", 1), 0);
    GlobalConfig config;
    config.ib_service_level = 5;
    loadGlobalConfig(config);
    EXPECT_EQ(config.ib_service_level, 5);
}

TEST_F(PkeyIndexEnvTest, IbSlNonNumericKeepsDefault) {
    ASSERT_EQ(::setenv("MC_IB_SL", "abc", 1), 0);
    GlobalConfig config;
    config.ib_service_level = 9;
    loadGlobalConfig(config);
    EXPECT_EQ(config.ib_service_level, 9);
}

// --- ValidatePortRange (pure function) ---

TEST(ValidatePortRangeTest, ValidRangePassesThrough) {
    auto [min_p, max_p] = ValidatePortRange(15000, 17000, 15000, 17000);
    EXPECT_EQ(min_p, 15000);
    EXPECT_EQ(max_p, 17000);
}

TEST(ValidatePortRangeTest, CustomValidRange) {
    auto [min_p, max_p] = ValidatePortRange(2000, 3000, 15000, 17000);
    EXPECT_EQ(min_p, 2000);
    EXPECT_EQ(max_p, 3000);
}

TEST(ValidatePortRangeTest, MinGreaterThanMaxFallsBack) {
    auto [min_p, max_p] = ValidatePortRange(5000, 4000, 15000, 17000);
    EXPECT_EQ(min_p, 15000);
    EXPECT_EQ(max_p, 17000);
}

TEST(ValidatePortRangeTest, WellKnownPortRejected) {
    auto [min_p, max_p] = ValidatePortRange(80, 443, 15000, 17000);
    EXPECT_EQ(min_p, 15000);
    EXPECT_EQ(max_p, 17000);
}

TEST(ValidatePortRangeTest, EphemeralPortRejected) {
    auto [min_p, max_p] = ValidatePortRange(32768, 40000, 15000, 17000);
    EXPECT_EQ(min_p, 15000);
    EXPECT_EQ(max_p, 17000);
}

TEST(ValidatePortRangeTest, BoundaryJustAboveEphemeral) {
    auto [min_p, max_p] = ValidatePortRange(61000, 65000, 15000, 17000);
    EXPECT_EQ(min_p, 61000);
    EXPECT_EQ(max_p, 65000);
}

TEST(ValidatePortRangeTest, MaxPort65535IsValid) {
    auto [min_p, max_p] = ValidatePortRange(61000, 65535, 15000, 17000);
    EXPECT_EQ(min_p, 61000);
    EXPECT_EQ(max_p, 65535);
}

TEST(ValidatePortRangeTest, FirstValidPort1024) {
    auto [min_p, max_p] = ValidatePortRange(1024, 2000, 15000, 17000);
    EXPECT_EQ(min_p, 1024);
    EXPECT_EQ(max_p, 2000);
}

TEST(ValidatePortRangeTest, LastBeforeEphemeral32767) {
    auto [min_p, max_p] = ValidatePortRange(1024, 32767, 15000, 17000);
    EXPECT_EQ(min_p, 1024);
    EXPECT_EQ(max_p, 32767);
}

TEST(ValidatePortRangeTest, AboveMaxRejected) {
    auto [min_p, max_p] = ValidatePortRange(61000, 65536, 15000, 17000);
    EXPECT_EQ(min_p, 15000);
    EXPECT_EQ(max_p, 17000);
}

}  // namespace
}  // namespace mooncake

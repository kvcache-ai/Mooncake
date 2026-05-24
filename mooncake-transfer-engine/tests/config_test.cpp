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

class ConfigEnvTest : public ::testing::Test {
   protected:
    void TearDown() override {
        ::unsetenv("MC_PKEY_INDEX");
        ::unsetenv("MC_IB_ENABLE_ECE");
    }
};

TEST_F(ConfigEnvTest, DefaultPkeyIndexIsZeroWhenUnset) {
    ::unsetenv("MC_PKEY_INDEX");
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.pkey_index, 0);
}

TEST_F(ConfigEnvTest, ValidPkeyIndexOverrideIsApplied) {
    ASSERT_EQ(::setenv("MC_PKEY_INDEX", "7", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.pkey_index, 7);
}

TEST_F(ConfigEnvTest, MaxPkeyIndexBoundaryIsApplied) {
    ASSERT_EQ(::setenv("MC_PKEY_INDEX", "65535", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.pkey_index, 65535);
}

TEST_F(ConfigEnvTest, OutOfRangePkeyIndexIsIgnored) {
    ASSERT_EQ(::setenv("MC_PKEY_INDEX", "70000", 1), 0);
    GlobalConfig config;
    config.pkey_index = 3;  // sentinel preserved when env var is rejected
    loadGlobalConfig(config);
    EXPECT_EQ(config.pkey_index, 3);
}

TEST_F(ConfigEnvTest, NegativePkeyIndexIsIgnored) {
    ASSERT_EQ(::setenv("MC_PKEY_INDEX", "-1", 1), 0);
    GlobalConfig config;
    config.pkey_index = 5;
    loadGlobalConfig(config);
    EXPECT_EQ(config.pkey_index, 5);
}

TEST_F(ConfigEnvTest, NonNumericPkeyIndexKeepsDefault) {
    ASSERT_EQ(::setenv("MC_PKEY_INDEX", "abc", 1), 0);
    GlobalConfig config;
    config.pkey_index = 9;
    loadGlobalConfig(config);
    EXPECT_EQ(config.pkey_index, 9);
}

TEST_F(ConfigEnvTest, EmptyPkeyIndexStringKeepsDefault) {
    ASSERT_EQ(::setenv("MC_PKEY_INDEX", "", 1), 0);
    GlobalConfig config;
    config.pkey_index = 4;
    loadGlobalConfig(config);
    EXPECT_EQ(config.pkey_index, 4);
}

TEST_F(ConfigEnvTest, EceIsEnabledByDefault) {
    ::unsetenv("MC_IB_ENABLE_ECE");
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_TRUE(config.ib_enable_ece);
}

TEST_F(ConfigEnvTest, EceCanBeEnabled) {
    ASSERT_EQ(::setenv("MC_IB_ENABLE_ECE", "1", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_TRUE(config.ib_enable_ece);
}

TEST_F(ConfigEnvTest, EceAcceptsBooleanStrings) {
    ASSERT_EQ(::setenv("MC_IB_ENABLE_ECE", "true", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_TRUE(config.ib_enable_ece);

    ASSERT_EQ(::setenv("MC_IB_ENABLE_ECE", "false", 1), 0);
    config.ib_enable_ece = true;
    loadGlobalConfig(config);
    EXPECT_FALSE(config.ib_enable_ece);
}

TEST_F(ConfigEnvTest, InvalidEceValueIsIgnored) {
    ASSERT_EQ(::setenv("MC_IB_ENABLE_ECE", "maybe", 1), 0);
    GlobalConfig config;
    config.ib_enable_ece = true;
    loadGlobalConfig(config);
    EXPECT_TRUE(config.ib_enable_ece);
}

}  // namespace
}  // namespace mooncake

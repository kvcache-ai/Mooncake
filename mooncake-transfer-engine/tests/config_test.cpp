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

class PkeyIndexEnvTest : public ::testing::Test {
   protected:
    void TearDown() override {
        ::unsetenv("MC_PKEY_INDEX");
        ::unsetenv("MC_AUTO_GID_MAX_RETRIES");
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

TEST_F(PkeyIndexEnvTest, AutoGidRetriesDefaultsToTwoWhenUnset) {
    ::unsetenv("MC_AUTO_GID_MAX_RETRIES");
    GlobalConfig config;
    config.auto_gid_max_retries = 2;
    loadGlobalConfig(config);
    EXPECT_EQ(config.auto_gid_max_retries, 2);
}

TEST_F(PkeyIndexEnvTest, AutoGidRetriesAcceptsValidOverride) {
    ASSERT_EQ(::setenv("MC_AUTO_GID_MAX_RETRIES", "0", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.auto_gid_max_retries, 0);
}

TEST_F(PkeyIndexEnvTest, AutoGidRetriesRejectsOutOfRangeOverride) {
    ASSERT_EQ(::setenv("MC_AUTO_GID_MAX_RETRIES", "99", 1), 0);
    GlobalConfig config;
    config.auto_gid_max_retries = 5;
    loadGlobalConfig(config);
    EXPECT_EQ(config.auto_gid_max_retries, 5);
}

// MC_QP_DRAIN_TIMEOUT_MS bounds the spin for a disconnected endpoint's
// outstanding WRs to flush before the next reconnect reuses/destroys its QPs.
// Unlike most knobs, 0 is a valid value (disables the wait); the range is
// capped at 1000ms because the drain spins with a spinlock held. These cases
// pin down that 0 is accepted while garbage/negative/out-of-range/trailing
// junk preserve the default -- a typo must not silently disable or mis-set the
// safety wait.
class QpDrainTimeoutEnvTest : public ::testing::Test {
   protected:
    void TearDown() override { ::unsetenv("MC_QP_DRAIN_TIMEOUT_MS"); }
};

TEST_F(QpDrainTimeoutEnvTest, DefaultIsFiftyWhenUnset) {
    ::unsetenv("MC_QP_DRAIN_TIMEOUT_MS");
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.qp_drain_timeout_ms, 50);
}

TEST_F(QpDrainTimeoutEnvTest, ValidOverrideIsApplied) {
    // Use a value distinct from the default so the test fails if the override
    // is silently ignored and the default is kept instead.
    ASSERT_EQ(::setenv("MC_QP_DRAIN_TIMEOUT_MS", "30", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.qp_drain_timeout_ms, 30);
}

TEST_F(QpDrainTimeoutEnvTest, ZeroIsAcceptedAndDisablesWait) {
    ASSERT_EQ(::setenv("MC_QP_DRAIN_TIMEOUT_MS", "0", 1), 0);
    GlobalConfig config;
    config.qp_drain_timeout_ms = 50;  // sentinel must be overwritten by 0
    loadGlobalConfig(config);
    EXPECT_EQ(config.qp_drain_timeout_ms, 0);
}

TEST_F(QpDrainTimeoutEnvTest, MaxBoundaryIsApplied) {
    ASSERT_EQ(::setenv("MC_QP_DRAIN_TIMEOUT_MS", "1000", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.qp_drain_timeout_ms, 1000);
}

TEST_F(QpDrainTimeoutEnvTest, OutOfRangeIsIgnored) {
    ASSERT_EQ(::setenv("MC_QP_DRAIN_TIMEOUT_MS", "1001", 1), 0);
    GlobalConfig config;
    config.qp_drain_timeout_ms = 7;  // sentinel preserved when rejected
    loadGlobalConfig(config);
    EXPECT_EQ(config.qp_drain_timeout_ms, 7);
}

TEST_F(QpDrainTimeoutEnvTest, NegativeIsIgnored) {
    ASSERT_EQ(::setenv("MC_QP_DRAIN_TIMEOUT_MS", "-1", 1), 0);
    GlobalConfig config;
    config.qp_drain_timeout_ms = 11;
    loadGlobalConfig(config);
    EXPECT_EQ(config.qp_drain_timeout_ms, 11);
}

TEST_F(QpDrainTimeoutEnvTest, NonNumericKeepsDefault) {
    ASSERT_EQ(::setenv("MC_QP_DRAIN_TIMEOUT_MS", "abc", 1), 0);
    GlobalConfig config;
    config.qp_drain_timeout_ms = 13;  // a typo must NOT silently disable
    loadGlobalConfig(config);
    EXPECT_EQ(config.qp_drain_timeout_ms, 13);
}

TEST_F(QpDrainTimeoutEnvTest, EmptyStringKeepsDefault) {
    ASSERT_EQ(::setenv("MC_QP_DRAIN_TIMEOUT_MS", "", 1), 0);
    GlobalConfig config;
    config.qp_drain_timeout_ms = 17;
    loadGlobalConfig(config);
    EXPECT_EQ(config.qp_drain_timeout_ms, 17);
}

}  // namespace
}  // namespace mooncake

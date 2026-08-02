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

TEST_F(PkeyIndexEnvTest, TeMetadataRefreshIntervalDefaultsToZeroWhenUnset) {
    ::unsetenv("MC_TE_METADATA_REFRESH_INTERVAL_SECONDS");
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.te_metadata_refresh_interval_seconds, 0);
}

TEST_F(PkeyIndexEnvTest, TeMetadataRefreshIntervalAcceptsValidOverride) {
    ASSERT_EQ(::setenv("MC_TE_METADATA_REFRESH_INTERVAL_SECONDS", "5", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.te_metadata_refresh_interval_seconds, 5);
}

TEST_F(PkeyIndexEnvTest, TeMetadataRefreshIntervalAcceptsZeroAsDisabled) {
    ASSERT_EQ(::setenv("MC_TE_METADATA_REFRESH_INTERVAL_SECONDS", "0", 1), 0);
    GlobalConfig config;
    config.te_metadata_refresh_interval_seconds = 123;
    loadGlobalConfig(config);
    EXPECT_EQ(config.te_metadata_refresh_interval_seconds, 0);
}

TEST_F(PkeyIndexEnvTest, TeMetadataRefreshIntervalRejectsNegativeOverride) {
    ASSERT_EQ(::setenv("MC_TE_METADATA_REFRESH_INTERVAL_SECONDS", "-1", 1), 0);
    GlobalConfig config;
    config.te_metadata_refresh_interval_seconds = 123;
    loadGlobalConfig(config);
    EXPECT_EQ(config.te_metadata_refresh_interval_seconds, 123);
}

TEST_F(PkeyIndexEnvTest, TeMetadataRefreshIntervalRejectsNonNumericOverride) {
    ASSERT_EQ(::setenv("MC_TE_METADATA_REFRESH_INTERVAL_SECONDS", "abc", 1), 0);
    GlobalConfig config;
    config.te_metadata_refresh_interval_seconds = 456;
    loadGlobalConfig(config);
    EXPECT_EQ(config.te_metadata_refresh_interval_seconds, 456);
}

// MC_CONN_PAUSE_TTL_MS arms the active-connect circuit-breaker: after an
// endpoint to a peer is torn down, active reconnection to that peer's address
// is paused for this many ms so the CQ poller isn't blocked re-handshaking a
// gone peer. 0 disables (and is the default); the range is capped at 600000ms.
// As with the other knobs, a typo / out-of-range value must preserve the
// default rather than silently change behavior.
class ConnPauseTtlEnvTest : public ::testing::Test {
   protected:
    void TearDown() override { ::unsetenv("MC_CONN_PAUSE_TTL_MS"); }
};

TEST_F(ConnPauseTtlEnvTest, DefaultIsZeroWhenUnset) {
    ::unsetenv("MC_CONN_PAUSE_TTL_MS");
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.conn_pause_ttl_ms, 0);
}

TEST_F(ConnPauseTtlEnvTest, ValidOverrideIsApplied) {
    ASSERT_EQ(::setenv("MC_CONN_PAUSE_TTL_MS", "5000", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.conn_pause_ttl_ms, 5000);
}

TEST_F(ConnPauseTtlEnvTest, ZeroIsAcceptedAndDisables) {
    ASSERT_EQ(::setenv("MC_CONN_PAUSE_TTL_MS", "0", 1), 0);
    GlobalConfig config;
    config.conn_pause_ttl_ms = 99;  // sentinel must be overwritten by 0
    loadGlobalConfig(config);
    EXPECT_EQ(config.conn_pause_ttl_ms, 0);
}

TEST_F(ConnPauseTtlEnvTest, MaxBoundaryIsApplied) {
    ASSERT_EQ(::setenv("MC_CONN_PAUSE_TTL_MS", "600000", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.conn_pause_ttl_ms, 600000);
}

TEST_F(ConnPauseTtlEnvTest, OutOfRangeIsIgnored) {
    ASSERT_EQ(::setenv("MC_CONN_PAUSE_TTL_MS", "600001", 1), 0);
    GlobalConfig config;
    config.conn_pause_ttl_ms = 7;  // sentinel preserved when rejected
    loadGlobalConfig(config);
    EXPECT_EQ(config.conn_pause_ttl_ms, 7);
}

TEST_F(ConnPauseTtlEnvTest, NegativeIsIgnored) {
    ASSERT_EQ(::setenv("MC_CONN_PAUSE_TTL_MS", "-1", 1), 0);
    GlobalConfig config;
    config.conn_pause_ttl_ms = 11;
    loadGlobalConfig(config);
    EXPECT_EQ(config.conn_pause_ttl_ms, 11);
}

TEST_F(ConnPauseTtlEnvTest, NonNumericKeepsDefault) {
    ASSERT_EQ(::setenv("MC_CONN_PAUSE_TTL_MS", "abc", 1), 0);
    GlobalConfig config;
    config.conn_pause_ttl_ms = 13;  // a typo must NOT silently change behavior
    loadGlobalConfig(config);
    EXPECT_EQ(config.conn_pause_ttl_ms, 13);
}

TEST_F(ConnPauseTtlEnvTest, NumericSuffixKeepsDefault) {
    ASSERT_EQ(::setenv("MC_CONN_PAUSE_TTL_MS", "5000s", 1), 0);
    GlobalConfig config;
    config.conn_pause_ttl_ms = 15;
    loadGlobalConfig(config);
    EXPECT_EQ(config.conn_pause_ttl_ms, 15);
}

TEST_F(ConnPauseTtlEnvTest, EmptyStringKeepsDefault) {
    ASSERT_EQ(::setenv("MC_CONN_PAUSE_TTL_MS", "", 1), 0);
    GlobalConfig config;
    config.conn_pause_ttl_ms = 17;
    loadGlobalConfig(config);
    EXPECT_EQ(config.conn_pause_ttl_ms, 17);
}

}  // namespace
}  // namespace mooncake

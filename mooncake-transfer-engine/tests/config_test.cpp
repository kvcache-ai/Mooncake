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

// --- MC_NUM_CQ_PER_CTX (atoi, range 1-255) ---

class NumCqEnvTest : public ::testing::Test {
   protected:
    void TearDown() override { ::unsetenv("MC_NUM_CQ_PER_CTX"); }
};

TEST_F(NumCqEnvTest, DefaultIsOneWhenUnset) {
    ::unsetenv("MC_NUM_CQ_PER_CTX");
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.num_cq_per_ctx, 1u);
}

TEST_F(NumCqEnvTest, ValidOverride) {
    ASSERT_EQ(::setenv("MC_NUM_CQ_PER_CTX", "4", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.num_cq_per_ctx, 4u);
}

TEST_F(NumCqEnvTest, ZeroIsRejected) {
    ASSERT_EQ(::setenv("MC_NUM_CQ_PER_CTX", "0", 1), 0);
    GlobalConfig config;
    config.num_cq_per_ctx = 1;
    loadGlobalConfig(config);
    EXPECT_EQ(config.num_cq_per_ctx, 1u);
}

TEST_F(NumCqEnvTest, OverMaxIsRejected) {
    ASSERT_EQ(::setenv("MC_NUM_CQ_PER_CTX", "256", 1), 0);
    GlobalConfig config;
    config.num_cq_per_ctx = 1;
    loadGlobalConfig(config);
    EXPECT_EQ(config.num_cq_per_ctx, 1u);
}

TEST_F(NumCqEnvTest, AlsoSetsJfcAndJfce) {
    ASSERT_EQ(::setenv("MC_NUM_CQ_PER_CTX", "8", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.num_jfc_per_ctx, 8u);
    EXPECT_EQ(config.num_jfce_per_ctx, 8u);
}

// --- MC_SLICE_SIZE (atoi to size_t, val > 0) ---

class SliceSizeEnvTest : public ::testing::Test {
   protected:
    void TearDown() override { ::unsetenv("MC_SLICE_SIZE"); }
};

TEST_F(SliceSizeEnvTest, DefaultIs65536) {
    ::unsetenv("MC_SLICE_SIZE");
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.slice_size, 65536u);
}

TEST_F(SliceSizeEnvTest, ValidOverride) {
    ASSERT_EQ(::setenv("MC_SLICE_SIZE", "131072", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.slice_size, 131072u);
}

TEST_F(SliceSizeEnvTest, ZeroIsRejected) {
    ASSERT_EQ(::setenv("MC_SLICE_SIZE", "0", 1), 0);
    GlobalConfig config;
    config.slice_size = 65536;
    loadGlobalConfig(config);
    EXPECT_EQ(config.slice_size, 65536u);
}

// --- MC_LOG_LEVEL (string match) ---

class LogLevelEnvTest : public ::testing::Test {
   protected:
    void TearDown() override { ::unsetenv("MC_LOG_LEVEL"); }
};

TEST_F(LogLevelEnvTest, InfoLevel) {
    ASSERT_EQ(::setenv("MC_LOG_LEVEL", "INFO", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.log_level, google::INFO);
    EXPECT_FALSE(config.trace);
}

TEST_F(LogLevelEnvTest, WarningLevel) {
    ASSERT_EQ(::setenv("MC_LOG_LEVEL", "WARNING", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.log_level, google::WARNING);
}

TEST_F(LogLevelEnvTest, ErrorLevel) {
    ASSERT_EQ(::setenv("MC_LOG_LEVEL", "ERROR", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.log_level, google::ERROR);
}

TEST_F(LogLevelEnvTest, TraceEnablesTrace) {
    ASSERT_EQ(::setenv("MC_LOG_LEVEL", "TRACE", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.log_level, google::INFO);
    EXPECT_TRUE(config.trace);
}

// --- MC_DISABLE_METACACHE (presence check) ---

class MetacacheEnvTest : public ::testing::Test {
   protected:
    void TearDown() override { ::unsetenv("MC_DISABLE_METACACHE"); }
};

TEST_F(MetacacheEnvTest, DefaultEnabled) {
    ::unsetenv("MC_DISABLE_METACACHE");
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_TRUE(config.metacache);
}

TEST_F(MetacacheEnvTest, DisabledWhenSet) {
    ASSERT_EQ(::setenv("MC_DISABLE_METACACHE", "1", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_FALSE(config.metacache);
}

// --- MC_IB_TC (stoi with try-catch, range 0-255) ---

class IbTrafficClassEnvTest : public ::testing::Test {
   protected:
    void TearDown() override { ::unsetenv("MC_IB_TC"); }
};

TEST_F(IbTrafficClassEnvTest, DefaultIsNegativeOne) {
    ::unsetenv("MC_IB_TC");
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.ib_traffic_class, -1);
}

TEST_F(IbTrafficClassEnvTest, ValidOverride) {
    ASSERT_EQ(::setenv("MC_IB_TC", "106", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.ib_traffic_class, 106);
}

TEST_F(IbTrafficClassEnvTest, ZeroIsValid) {
    ASSERT_EQ(::setenv("MC_IB_TC", "0", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.ib_traffic_class, 0);
}

TEST_F(IbTrafficClassEnvTest, OverMaxIsRejected) {
    ASSERT_EQ(::setenv("MC_IB_TC", "256", 1), 0);
    GlobalConfig config;
    config.ib_traffic_class = -1;
    loadGlobalConfig(config);
    EXPECT_EQ(config.ib_traffic_class, -1);
}

TEST_F(IbTrafficClassEnvTest, NonNumericKeepsDefault) {
    ASSERT_EQ(::setenv("MC_IB_TC", "xyz", 1), 0);
    GlobalConfig config;
    config.ib_traffic_class = -1;
    loadGlobalConfig(config);
    EXPECT_EQ(config.ib_traffic_class, -1);
}

// --- MC_ENDPOINT_STORE_TYPE (string enum) ---

class EndpointStoreTypeEnvTest : public ::testing::Test {
   protected:
    void TearDown() override { ::unsetenv("MC_ENDPOINT_STORE_TYPE"); }
};

TEST_F(EndpointStoreTypeEnvTest, DefaultIsSieve) {
    ::unsetenv("MC_ENDPOINT_STORE_TYPE");
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.endpoint_store_type, EndpointStoreType::SIEVE);
}

TEST_F(EndpointStoreTypeEnvTest, FifoOverride) {
    ASSERT_EQ(::setenv("MC_ENDPOINT_STORE_TYPE", "FIFO", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.endpoint_store_type, EndpointStoreType::FIFO);
}

TEST_F(EndpointStoreTypeEnvTest, InvalidIsRejected) {
    ASSERT_EQ(::setenv("MC_ENDPOINT_STORE_TYPE", "LRU", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.endpoint_store_type, EndpointStoreType::SIEVE);
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

}  // namespace
}  // namespace mooncake

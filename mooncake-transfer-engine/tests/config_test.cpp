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

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <thread>

#include "config.h"
#include "transport/batch_registration.h"

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

// MC_IB_PORT names the RDMA port opened on every device. Port numbers are
// 1-based, so 0 is not a "disable" value: it makes ibv_query_port fail on
// every device and takes the whole topology down.
class IbPortEnvTest : public ::testing::Test {
   protected:
    void TearDown() override { ::unsetenv("MC_IB_PORT"); }
};

TEST_F(IbPortEnvTest, DefaultIsPortOneWhenUnset) {
    ::unsetenv("MC_IB_PORT");
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.port, 1);
}

TEST_F(IbPortEnvTest, ValidOverrideIsApplied) {
    ASSERT_EQ(::setenv("MC_IB_PORT", "2", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.port, 2);
}

TEST_F(IbPortEnvTest, ZeroIsRejected) {
    ASSERT_EQ(::setenv("MC_IB_PORT", "0", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.port, 1);
}

TEST_F(IbPortEnvTest, OutOfRangeIsRejected) {
    ASSERT_EQ(::setenv("MC_IB_PORT", "256", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.port, 1);
}

TEST_F(IbPortEnvTest, NegativeIsRejected) {
    ASSERT_EQ(::setenv("MC_IB_PORT", "-1", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.port, 1);
}

TEST_F(IbPortEnvTest, NonNumericIsRejected) {
    ASSERT_EQ(::setenv("MC_IB_PORT", "abc", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.port, 1);
}

// MC_MAX_CONCURRENT_REG_MR caps how many buffers registerLocalMemoryBatch()
// registers at once; 0 (the default) means unbounded. 0 is therefore also what
// a silent atol() fallback would produce on a typo, which would read as "the
// knob was honored and asked for no cap" -- the opposite of what the operator
// wanted. So a typo must be rejected loudly and leave the field untouched.
class MaxConcurrentRegMrEnvTest : public ::testing::Test {
   protected:
    void TearDown() override { ::unsetenv("MC_MAX_CONCURRENT_REG_MR"); }
};

TEST_F(MaxConcurrentRegMrEnvTest, UnboundedWhenUnset) {
    ::unsetenv("MC_MAX_CONCURRENT_REG_MR");
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.max_concurrent_reg_mr, 0u);
}

TEST_F(MaxConcurrentRegMrEnvTest, ValidOverrideIsApplied) {
    ASSERT_EQ(::setenv("MC_MAX_CONCURRENT_REG_MR", "8", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.max_concurrent_reg_mr, 8u);
}

TEST_F(MaxConcurrentRegMrEnvTest, ExplicitZeroSelectsUnbounded) {
    ASSERT_EQ(::setenv("MC_MAX_CONCURRENT_REG_MR", "0", 1), 0);
    GlobalConfig config;
    config.max_concurrent_reg_mr = 99;  // sentinel must be overwritten by 0
    loadGlobalConfig(config);
    EXPECT_EQ(config.max_concurrent_reg_mr, 0u);
}

TEST_F(MaxConcurrentRegMrEnvTest, OneIsAcceptedAndSerializes) {
    ASSERT_EQ(::setenv("MC_MAX_CONCURRENT_REG_MR", "1", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.max_concurrent_reg_mr, 1u);
}

TEST_F(MaxConcurrentRegMrEnvTest, NegativeIsIgnored) {
    ASSERT_EQ(::setenv("MC_MAX_CONCURRENT_REG_MR", "-1", 1), 0);
    GlobalConfig config;
    config.max_concurrent_reg_mr = 11;
    loadGlobalConfig(config);
    EXPECT_EQ(config.max_concurrent_reg_mr, 11u);
}

TEST_F(MaxConcurrentRegMrEnvTest, NonNumericKeepsDefault) {
    ASSERT_EQ(::setenv("MC_MAX_CONCURRENT_REG_MR", "abc", 1), 0);
    GlobalConfig config;
    config.max_concurrent_reg_mr = 13;
    loadGlobalConfig(config);
    EXPECT_EQ(config.max_concurrent_reg_mr, 13u);
}

TEST_F(MaxConcurrentRegMrEnvTest, NumericSuffixKeepsDefault) {
    ASSERT_EQ(::setenv("MC_MAX_CONCURRENT_REG_MR", "8x", 1), 0);
    GlobalConfig config;
    config.max_concurrent_reg_mr = 15;
    loadGlobalConfig(config);
    EXPECT_EQ(config.max_concurrent_reg_mr, 15u);
}

TEST_F(MaxConcurrentRegMrEnvTest, EmptyStringKeepsDefault) {
    ASSERT_EQ(::setenv("MC_MAX_CONCURRENT_REG_MR", "", 1), 0);
    GlobalConfig config;
    config.max_concurrent_reg_mr = 17;
    loadGlobalConfig(config);
    EXPECT_EQ(config.max_concurrent_reg_mr, 17u);
}

class MaxConcurrentRegMrScope {
   public:
    explicit MaxConcurrentRegMrScope(size_t value)
        : previous_(globalConfig().max_concurrent_reg_mr) {
        globalConfig().max_concurrent_reg_mr = value;
    }

    ~MaxConcurrentRegMrScope() {
        globalConfig().max_concurrent_reg_mr = previous_;
    }

   private:
    size_t previous_;
};

void updatePeak(std::atomic<size_t>& peak, size_t current) {
    size_t observed = peak.load();
    while (observed < current &&
           !peak.compare_exchange_weak(observed, current)) {
    }
}

TEST(BatchRegistrationTest, RespectsConfiguredWorkerLimit) {
    MaxConcurrentRegMrScope limit(3);
    std::atomic<size_t> active{0};
    std::atomic<size_t> peak{0};
    std::atomic<size_t> completed{0};

    int ret = runBoundedRegMrBatch(24, [&](size_t) {
        size_t current = active.fetch_add(1) + 1;
        updatePeak(peak, current);
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        active.fetch_sub(1);
        completed.fetch_add(1);
        return 0;
    });

    EXPECT_EQ(ret, 0);
    EXPECT_EQ(completed.load(), 24u);
    EXPECT_LE(peak.load(), 3u);
}

TEST(BatchRegistrationTest, LimitOneRunsSerially) {
    MaxConcurrentRegMrScope limit(1);
    std::atomic<size_t> active{0};
    std::atomic<size_t> peak{0};

    int ret = runBoundedRegMrBatch(8, [&](size_t) {
        size_t current = active.fetch_add(1) + 1;
        updatePeak(peak, current);
        std::this_thread::yield();
        active.fetch_sub(1);
        return 0;
    });

    EXPECT_EQ(ret, 0);
    EXPECT_EQ(peak.load(), 1u);
}

TEST(BatchRegistrationTest, AttemptsEveryItemAndReturnsAnError) {
    MaxConcurrentRegMrScope limit(4);
    std::atomic<size_t> completed{0};

    int ret = runBoundedRegMrBatch(12, [&](size_t index) {
        completed.fetch_add(1);
        return index == 7 ? -7 : 0;
    });

    EXPECT_EQ(ret, -7);
    EXPECT_EQ(completed.load(), 12u);
}

class EfaNicSelectionEnvTest : public ::testing::Test {
   protected:
    void TearDown() override { ::unsetenv("MC_EFA_NIC_SELECTION"); }
};

TEST_F(EfaNicSelectionEnvTest, DefaultsToAll) {
    ::unsetenv("MC_EFA_NIC_SELECTION");
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.efa_nic_selection, EfaNicSelection::ALL);
}

TEST_F(EfaNicSelectionEnvTest, LocalIsApplied) {
    ASSERT_EQ(::setenv("MC_EFA_NIC_SELECTION", "local", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.efa_nic_selection, EfaNicSelection::LOCAL);
}

TEST_F(EfaNicSelectionEnvTest, AllIsAcceptedExplicitly) {
    ASSERT_EQ(::setenv("MC_EFA_NIC_SELECTION", "all", 1), 0);
    GlobalConfig config;
    config.efa_nic_selection = EfaNicSelection::LOCAL;  // must be overwritten
    loadGlobalConfig(config);
    EXPECT_EQ(config.efa_nic_selection, EfaNicSelection::ALL);
}

TEST_F(EfaNicSelectionEnvTest, CaseIsIgnored) {
    ASSERT_EQ(::setenv("MC_EFA_NIC_SELECTION", "LOCAL", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.efa_nic_selection, EfaNicSelection::LOCAL);
}

TEST_F(EfaNicSelectionEnvTest, UnknownValueKeepsDefault) {
    // A typo must not silently pick a policy: registering buffers on the wrong
    // NIC set is a correctness-adjacent surprise, not a perf knob.
    ASSERT_EQ(::setenv("MC_EFA_NIC_SELECTION", "topology", 1), 0);
    GlobalConfig config;
    config.efa_nic_selection = EfaNicSelection::LOCAL;
    loadGlobalConfig(config);
    EXPECT_EQ(config.efa_nic_selection, EfaNicSelection::LOCAL);
}

TEST_F(EfaNicSelectionEnvTest, EmptyStringKeepsDefault) {
    ASSERT_EQ(::setenv("MC_EFA_NIC_SELECTION", "", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.efa_nic_selection, EfaNicSelection::ALL);
}

// max_wr_from_env distinguishes "the operator asked for this depth" from "this
// is the compiled-in default".  The EFA transport needs that distinction: with
// no override it adopts the provider's per-device transmit queue depth, which
// no fixed default can match (2048 on p6-b300, 4096 on p5).  A rejected value
// must NOT set the flag, or a typo would be treated as a deliberate override.
class MaxWrEnvTest : public ::testing::Test {
   protected:
    void TearDown() override { ::unsetenv("MC_MAX_WR"); }
};

TEST_F(MaxWrEnvTest, NotFromEnvWhenUnset) {
    ::unsetenv("MC_MAX_WR");
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_FALSE(config.max_wr_from_env);
    EXPECT_EQ(config.max_wr, 256u);  // default preserved for RDMA
}

TEST_F(MaxWrEnvTest, ValidOverrideSetsFlag) {
    ASSERT_EQ(::setenv("MC_MAX_WR", "2048", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_TRUE(config.max_wr_from_env);
    EXPECT_EQ(config.max_wr, 2048u);
}

TEST_F(MaxWrEnvTest, RejectedValueDoesNotSetFlag) {
    // 0 is out of range.  The value is ignored, so the EFA transport must
    // still treat this as "no override" and track the provider's depth.
    ASSERT_EQ(::setenv("MC_MAX_WR", "0", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_FALSE(config.max_wr_from_env);
    EXPECT_EQ(config.max_wr, 256u);
}

TEST_F(MaxWrEnvTest, NonNumericDoesNotSetFlag) {
    ASSERT_EQ(::setenv("MC_MAX_WR", "abc", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_FALSE(config.max_wr_from_env);
    EXPECT_EQ(config.max_wr, 256u);
}

TEST_F(MaxWrEnvTest, OutOfRangeDoesNotSetFlag) {
    ASSERT_EQ(::setenv("MC_MAX_WR", "70000", 1), 0);  // > UINT16_MAX
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_FALSE(config.max_wr_from_env);
    EXPECT_EQ(config.max_wr, 256u);
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
    config.num_cq_per_ctx = 42;
    loadGlobalConfig(config);
    EXPECT_EQ(config.num_cq_per_ctx, 42u);
}

TEST_F(NumCqEnvTest, OverMaxIsRejected) {
    ASSERT_EQ(::setenv("MC_NUM_CQ_PER_CTX", "256", 1), 0);
    GlobalConfig config;
    config.num_cq_per_ctx = 42;
    loadGlobalConfig(config);
    EXPECT_EQ(config.num_cq_per_ctx, 42u);
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
    config.slice_size = 99999;
    loadGlobalConfig(config);
    EXPECT_EQ(config.slice_size, 99999u);
}

TEST_F(SliceSizeEnvTest, NegativeWrapsToLargeValue) {
    ASSERT_EQ(::setenv("MC_SLICE_SIZE", "-100", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    // atoi("-100") returns -100, cast to size_t wraps to a large value.
    // val > 0 passes, so config.slice_size gets the wrapped value.
    // This documents existing (buggy) behavior.
    EXPECT_NE(config.slice_size, 65536u);
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

TEST_F(LogLevelEnvTest, InvalidLevelKeepsDefault) {
    ASSERT_EQ(::setenv("MC_LOG_LEVEL", "INVALID", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.log_level, google::INFO);
    EXPECT_FALSE(config.trace);
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
    config.endpoint_store_type = EndpointStoreType::FIFO;
    loadGlobalConfig(config);
    EXPECT_EQ(config.endpoint_store_type, EndpointStoreType::FIFO);
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

// --- MC_MTU (valid values only; invalid is logged and ignored) ---

class MtuEnvTest : public ::testing::Test {
   protected:
    void TearDown() override { ::unsetenv("MC_MTU"); }
};

TEST_F(MtuEnvTest, DefaultIs4096) {
    ::unsetenv("MC_MTU");
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.mtu_length, IBV_MTU_4096);
}

TEST_F(MtuEnvTest, Mtu512) {
    ASSERT_EQ(::setenv("MC_MTU", "512", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.mtu_length, IBV_MTU_512);
}

TEST_F(MtuEnvTest, Mtu1024) {
    ASSERT_EQ(::setenv("MC_MTU", "1024", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.mtu_length, IBV_MTU_1024);
}

TEST_F(MtuEnvTest, Mtu2048) {
    ASSERT_EQ(::setenv("MC_MTU", "2048", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.mtu_length, IBV_MTU_2048);
}

TEST_F(MtuEnvTest, Mtu4096) {
    ASSERT_EQ(::setenv("MC_MTU", "4096", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.mtu_length, IBV_MTU_4096);
}

TEST_F(MtuEnvTest, InvalidIsIgnored) {
    ASSERT_EQ(::setenv("MC_MTU", "1500", 1), 0);
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_EQ(config.mtu_length, IBV_MTU_4096);
}

// --- Path MTU negotiation (issue #3868) ---

class MtuLengthScope {
   public:
    explicit MtuLengthScope(ibv_mtu value)
        : previous_(globalConfig().mtu_length) {
        globalConfig().mtu_length = value;
    }

    ~MtuLengthScope() { globalConfig().mtu_length = previous_; }

   private:
    ibv_mtu previous_;
};

TEST(PathMtuTest, MtuLengthToBytesCoversEveryEnumerant) {
    EXPECT_EQ(mtuLengthToBytes(IBV_MTU_256), 256u);
    EXPECT_EQ(mtuLengthToBytes(IBV_MTU_512), 512u);
    EXPECT_EQ(mtuLengthToBytes(IBV_MTU_1024), 1024u);
    EXPECT_EQ(mtuLengthToBytes(IBV_MTU_2048), 2048u);
    EXPECT_EQ(mtuLengthToBytes(IBV_MTU_4096), 4096u);
    EXPECT_EQ(mtuLengthToBytes(static_cast<ibv_mtu>(0)), 0u);
}

TEST(PathMtuTest, LocalPathMtuIsCappedByConfiguredMtu) {
    MtuLengthScope scope(IBV_MTU_1024);
    EXPECT_EQ(localPathMtu(IBV_MTU_4096), IBV_MTU_1024);
    // A port slower than MC_MTU still wins: the hardware cannot do more.
    EXPECT_EQ(localPathMtu(IBV_MTU_512), IBV_MTU_512);
}

// The regression: a 4096-MTU port talking to a 1024-MTU peer must program the
// RC QP with the peer's smaller MTU, otherwise its READ/WRITE packets exceed
// what the peer accepts.
TEST(PathMtuTest, NegotiateTakesTheSmallerOfBothPeers) {
    EXPECT_EQ(negotiatePathMtu(IBV_MTU_4096, 1024), IBV_MTU_1024);
    EXPECT_EQ(negotiatePathMtu(IBV_MTU_1024, 4096), IBV_MTU_1024);
    EXPECT_EQ(negotiatePathMtu(IBV_MTU_2048, 2048), IBV_MTU_2048);
}

TEST(PathMtuTest, NegotiateKeepsLocalMtuWhenPeerDoesNotAdvertise) {
    // Older peers omit the field; unknown lengths are treated the same way.
    EXPECT_EQ(negotiatePathMtu(IBV_MTU_2048, 0), IBV_MTU_2048);
    EXPECT_EQ(negotiatePathMtu(IBV_MTU_2048, 1500), IBV_MTU_2048);
}

}  // namespace
}  // namespace mooncake

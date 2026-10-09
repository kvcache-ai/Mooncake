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

#include <cstdlib>
#include <deque>
#include <vector>

#include <gtest/gtest.h>

#include "config.h"
#include "transport/rdma_transport/rdma_posted_fifo.h"

using mooncake::collectPostedFifo;
using mooncake::DrainedSliceStatus;
using mooncake::drainedSliceStatus;
using mooncake::shouldSignalRdmaWr;
using mooncake::useRdmaPostedFifo;
using mooncake::wrRetiredByCqe;

TEST(ShouldSignalRdmaWr, IntervalOneSignalsEveryWr) {
    EXPECT_TRUE(shouldSignalRdmaWr(0, 1, 256, false));
    EXPECT_TRUE(shouldSignalRdmaWr(17, 1, 256, false));
    EXPECT_TRUE(shouldSignalRdmaWr(0, 0, 256, false));
}

TEST(ShouldSignalRdmaWr, IntervalSignalsOnBoundaryAndChainTail) {
    EXPECT_FALSE(shouldSignalRdmaWr(0, 32, 256, false));
    EXPECT_FALSE(shouldSignalRdmaWr(30, 32, 256, false));
    EXPECT_TRUE(shouldSignalRdmaWr(31, 32, 256, false));
    EXPECT_TRUE(shouldSignalRdmaWr(0, 32, 256, true));
}

TEST(ShouldSignalRdmaWr, HalfSqCapPreventsUnsignaledDeadlock) {
    // 4 unsignaled (count=4) hits max_wr/2 when max_wr=8.
    EXPECT_TRUE(shouldSignalRdmaWr(3, 32, 8, false));
    EXPECT_FALSE(shouldSignalRdmaWr(2, 32, 8, false));
}

TEST(CollectPostedFifo, SuccessPopsPrefixThroughSignaled) {
    int a, b, c, d;
    std::deque<int *> q{&a, &b, &c, &d};
    std::vector<int *> out;
    EXPECT_EQ(collectPostedFifo(q, &c, out), 3u);
    ASSERT_EQ(out.size(), 3u);
    EXPECT_EQ(out[0], &a);
    EXPECT_EQ(out[1], &b);
    EXPECT_EQ(out[2], &c);
    ASSERT_EQ(q.size(), 1u);
    EXPECT_EQ(q.front(), &d);
}

TEST(CollectPostedFifo, ErrorLeavesTheTailForLaterCqes) {
    int a, b, c, d;
    std::deque<int *> q{&a, &b, &c, &d};
    std::vector<int *> out;
    EXPECT_EQ(collectPostedFifo(q, &b, out), 2u);
    ASSERT_EQ(out.size(), 2u);
    EXPECT_EQ(out[0], &a);
    EXPECT_EQ(out[1], &b);
    ASSERT_EQ(q.size(), 2u);
    EXPECT_EQ(q.front(), &c);
    EXPECT_EQ(q.back(), &d);
}

TEST(CollectPostedFifo, MissingSignaledLeavesQueueUntouched) {
    int a, b, missing;
    std::deque<int *> q{&a, &b};
    std::vector<int *> out;
    EXPECT_EQ(collectPostedFifo(q, &missing, out), 0u);
    EXPECT_TRUE(out.empty());
    EXPECT_EQ(q.size(), 2u);
}

TEST(DrainedSliceStatus, SuccessCqePrefixIsSuccess) {
    EXPECT_EQ(drainedSliceStatus(true, false, false, 0, 3),
              DrainedSliceStatus::kSuccess);
    EXPECT_EQ(drainedSliceStatus(true, false, false, 2, 3),
              DrainedSliceStatus::kKeepCqe);
    EXPECT_EQ(drainedSliceStatus(true, false, false, 0, 1),
              DrainedSliceStatus::kKeepCqe);
}

TEST(DrainedSliceStatus, FirstNonFlushErrorPrefixIsSuccess) {
    EXPECT_EQ(drainedSliceStatus(false, false, false, 0, 3),
              DrainedSliceStatus::kSuccess);
    EXPECT_EQ(drainedSliceStatus(false, false, false, 1, 3),
              DrainedSliceStatus::kSuccess);
    EXPECT_EQ(drainedSliceStatus(false, false, false, 2, 3),
              DrainedSliceStatus::kKeepCqe);
}

TEST(DrainedSliceStatus, FlushCqePrefixIsFlushErr) {
    EXPECT_EQ(drainedSliceStatus(false, true, false, 0, 3),
              DrainedSliceStatus::kFlushErr);
    EXPECT_EQ(drainedSliceStatus(false, true, false, 2, 3),
              DrainedSliceStatus::kKeepCqe);
}

TEST(DrainedSliceStatus, AlreadyErroredPrefixIsFlushErr) {
    EXPECT_EQ(drainedSliceStatus(false, false, true, 0, 3),
              DrainedSliceStatus::kFlushErr);
    EXPECT_EQ(drainedSliceStatus(false, true, true, 0, 2),
              DrainedSliceStatus::kFlushErr);
}

TEST(DrainedSliceStatus, SequenceSuccessFirstErrorThenFlush) {
    // One QP, three CQEs: success, first real error, then flush after error.
    struct Event {
        bool success;
        bool flush;
        size_t drained_n;
    };
    const Event events[] = {
        {true, false, 2},
        {false, false, 2},
        {false, true, 2},
    };
    bool qp_errored = false;
    std::vector<DrainedSliceStatus> got;
    for (const auto &e : events) {
        const bool already = qp_errored;
        for (size_t j = 0; j < e.drained_n; ++j) {
            got.push_back(drainedSliceStatus(e.success, e.flush, already, j,
                                             e.drained_n));
        }
        if (!e.success) qp_errored = true;
    }
    ASSERT_EQ(got.size(), 6u);
    EXPECT_EQ(got[0], DrainedSliceStatus::kSuccess);
    EXPECT_EQ(got[1], DrainedSliceStatus::kKeepCqe);
    EXPECT_EQ(got[2], DrainedSliceStatus::kSuccess);
    EXPECT_EQ(got[3], DrainedSliceStatus::kKeepCqe);
    EXPECT_EQ(got[4], DrainedSliceStatus::kFlushErr);
    EXPECT_EQ(got[5], DrainedSliceStatus::kKeepCqe);
    EXPECT_TRUE(qp_errored);
}

TEST(UseRdmaPostedFifo, IntervalOneSkipsFifo) {
    EXPECT_FALSE(useRdmaPostedFifo(1));
    EXPECT_FALSE(useRdmaPostedFifo(0));
    EXPECT_TRUE(useRdmaPostedFifo(2));
    EXPECT_TRUE(useRdmaPostedFifo(32));
}

TEST(WrRetiredByCqe, IntervalOneEqualsNonNullCqeCount) {
    EXPECT_EQ(wrRetiredByCqe(false, false, 99), 0u);
    EXPECT_EQ(wrRetiredByCqe(false, true, 99), 1u);
    const bool has_completed[] = {true, true, false, true};
    size_t retired = 0;
    for (bool has : has_completed)
        retired += wrRetiredByCqe(/*use_fifo=*/false, has, /*drained_n=*/4);
    EXPECT_EQ(retired, 3u);
}

TEST(WrRetiredByCqe, FifoHitRetiresPrefixStaleRetiresZero) {
    EXPECT_EQ(wrRetiredByCqe(true, true, 0), 0u);
    EXPECT_EQ(wrRetiredByCqe(true, true, 4), 4u);
    EXPECT_EQ(wrRetiredByCqe(true, false, 4), 0u);
}

class SignalIntervalEnvTest : public ::testing::Test {
   protected:
    void TearDown() override { ::unsetenv("MC_RDMA_SIGNAL_INTERVAL"); }
};

TEST_F(SignalIntervalEnvTest, DefaultIsLegacyEveryWr) {
    ::unsetenv("MC_RDMA_SIGNAL_INTERVAL");
    mooncake::GlobalConfig config;
    mooncake::loadGlobalConfig(config);
    EXPECT_EQ(config.rdma_signal_interval, 1);
}

TEST_F(SignalIntervalEnvTest, ValidOverride) {
    ASSERT_EQ(::setenv("MC_RDMA_SIGNAL_INTERVAL", "32", 1), 0);
    mooncake::GlobalConfig config;
    mooncake::loadGlobalConfig(config);
    EXPECT_EQ(config.rdma_signal_interval, 32);
}

TEST_F(SignalIntervalEnvTest, RejectedValueKeepsDefault) {
    ASSERT_EQ(::setenv("MC_RDMA_SIGNAL_INTERVAL", "0", 1), 0);
    mooncake::GlobalConfig config;
    mooncake::loadGlobalConfig(config);
    EXPECT_EQ(config.rdma_signal_interval, 1);
}

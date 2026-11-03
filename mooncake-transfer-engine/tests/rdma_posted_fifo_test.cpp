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
using mooncake::rdmaSignalPeriod;
using mooncake::shouldSignalRdmaWr;

TEST(RdmaSignalPeriod, IntervalOneIsEveryWr) {
    EXPECT_EQ(rdmaSignalPeriod(1, 256), 1);
    EXPECT_EQ(rdmaSignalPeriod(0, 256), 1);
}

TEST(ShouldSignalRdmaWr, SignalsOnPeriodAndChainTail) {
    const int period = rdmaSignalPeriod(32, 256);
    EXPECT_EQ(period, 32);
    EXPECT_FALSE(shouldSignalRdmaWr(0, 40, period));
    EXPECT_FALSE(shouldSignalRdmaWr(30, 40, period));
    EXPECT_TRUE(shouldSignalRdmaWr(31, 40, period));
    EXPECT_TRUE(shouldSignalRdmaWr(39, 40, period));
}

TEST(ShouldSignalRdmaWr, PeriodCappedAtHalfSq) {
    const int period = rdmaSignalPeriod(32, 8);
    EXPECT_EQ(period, 4);
    EXPECT_TRUE(shouldSignalRdmaWr(3, 8, period));
    EXPECT_FALSE(shouldSignalRdmaWr(2, 8, period));
    EXPECT_TRUE(shouldSignalRdmaWr(7, 8, period));
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

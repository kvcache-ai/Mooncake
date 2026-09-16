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
using mooncake::shouldSignalRdmaWr;

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
    EXPECT_EQ(collectPostedFifo(q, &c, false, out), 3u);
    ASSERT_EQ(out.size(), 3u);
    EXPECT_EQ(out[0], &a);
    EXPECT_EQ(out[1], &b);
    EXPECT_EQ(out[2], &c);
    ASSERT_EQ(q.size(), 1u);
    EXPECT_EQ(q.front(), &d);
}

TEST(CollectPostedFifo, ErrorDrainsTheWholeQpWindow) {
    int a, b, c, d;
    std::deque<int *> q{&a, &b, &c, &d};
    std::vector<int *> out;
    EXPECT_EQ(collectPostedFifo(q, &b, true, out), 4u);
    EXPECT_TRUE(q.empty());
    ASSERT_EQ(out.size(), 4u);
    EXPECT_EQ(out[3], &d);
}

TEST(CollectPostedFifo, MissingSignaledLeavesQueueUntouched) {
    int a, b, missing;
    std::deque<int *> q{&a, &b};
    std::vector<int *> out;
    EXPECT_EQ(collectPostedFifo(q, &missing, false, out), 0u);
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

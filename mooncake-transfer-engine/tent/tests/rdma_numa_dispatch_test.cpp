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

#include <algorithm>
#include <array>
#include <vector>

#include "tent/transport/rdma/numa_dispatch.h"

namespace mooncake::tent {
namespace {

TEST(RdmaNumaDispatch, EachAlternatingNicUsesEveryLocalWorker) {
    const std::array<int, 6> nodes{0, 1, 0, 1, 0, 1};
    for (int devices : {1, 2, 3, 6}) {
        RdmaNumaLaneCursor cursor(nodes.size(), true);
        std::vector<std::array<int, 6>> loads(devices);
        for (int i = 0; i < devices * 30; ++i) {
            const int dev = i % devices;
            const int node = dev % 2;
            const int lane = cursor.select(dev, node, i % nodes.size(),
                                           [&](int w) { return nodes[w]; });
            ASSERT_GE(lane, 0);
            ASSERT_LT(lane, static_cast<int>(nodes.size()));
            ASSERT_EQ(nodes[lane], node);
            ++loads[dev][lane];
        }
        for (int dev = 0; dev < devices; ++dev) {
            for (int lane = 0; lane < static_cast<int>(nodes.size()); ++lane)
                EXPECT_EQ(loads[dev][lane], nodes[lane] == dev % 2 ? 10 : 0)
                    << "devices=" << devices << " device=" << dev
                    << " lane=" << lane;
        }
    }
}

TEST(RdmaNumaDispatch, HandlesSparseNodesAndUnevenWorkerCounts) {
    const std::array<int, 5> nodes{2, -1, 7, 2, 7};
    RdmaNumaLaneCursor cursor(nodes.size(), true);
    for (int i = 0; i < 20; ++i) {
        const int node = i % 2 ? 7 : 2;
        const int lane = cursor.select(i % 2, node, i % nodes.size(),
                                       [&](int w) { return nodes[w]; });
        ASSERT_GE(lane, 0);
        ASSERT_LT(lane, static_cast<int>(nodes.size()));
        EXPECT_EQ(nodes[lane], node);
    }
}

TEST(RdmaNumaDispatch, FallsBackForUnknownOrUnavailablePlacement) {
    const std::array<int, 3> nodes{2, -1, 2};
    RdmaNumaLaneCursor cursor(nodes.size(), true);
    const auto node = [&](int w) { return nodes[w]; };
    EXPECT_EQ(cursor.select(-1, 2, 1, node), 1);
    EXPECT_EQ(cursor.select(64, 2, 2, node), 2);
    EXPECT_EQ(cursor.select(0, -1, 0, node), 0);
    EXPECT_EQ(cursor.select(0, 7, 1, node), 1);
}

TEST(RdmaNumaDispatch, CanUsePlacementPublishedAfterAnEarlierFallback) {
    std::array<int, 3> nodes{-1, -1, -1};
    RdmaNumaLaneCursor cursor(nodes.size(), true);
    const auto node = [&](int w) { return nodes[w]; };
    EXPECT_EQ(cursor.select(63, 7, 0, node), 0);
    nodes[2] = 7;
    EXPECT_EQ(cursor.select(63, 7, 0, node), 2);
}

TEST(RdmaNumaDispatch, WorksWithMoreThanSixtyFourWorkers) {
    RdmaNumaLaneCursor cursor(129, true);
    const auto node = [](int w) { return w == 128 ? 7 : -1; };
    EXPECT_EQ(cursor.select(63, 7, 0, node), 128);
}

TEST(RdmaNumaDispatch, DisabledModePreservesTheOrdinaryLaneCursor) {
    RdmaNumaLaneCursor cursor(6, false);
    EXPECT_FALSE(cursor.enabled());
    for (int i = 0; i < 30; ++i) {
        const int fallback = i % 6;
        EXPECT_EQ(
            cursor.select(i % 2, 0, fallback, [](int w) { return w % 2; }),
            fallback);
    }
}

}  // namespace
}  // namespace mooncake::tent

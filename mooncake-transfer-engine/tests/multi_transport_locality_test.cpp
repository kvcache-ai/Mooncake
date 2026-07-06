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

#include <string>

#include "multi_transport_locality.h"

using namespace mooncake;

TEST(MultiTransportLocalityTest, SegmentHostStripsPort) {
    EXPECT_EQ(segmentHost("10.0.0.1:12345"), "10.0.0.1");
    EXPECT_EQ(segmentHost("node-a:8000"), "node-a");
    // No port: whole string is the host.
    EXPECT_EQ(segmentHost("node-a"), "node-a");
    EXPECT_EQ(segmentHost(""), "");
}

TEST(MultiTransportLocalityTest, HipReachableForSameHostDifferentPort) {
    // Two engines co-located on one host share the host, differ only in port.
    EXPECT_TRUE(isHipReachableTarget("10.0.0.1:20000", "10.0.0.1:20001"));
    EXPECT_TRUE(isHipReachableTarget("node-a:8000", "node-a:9000"));
}

TEST(MultiTransportLocalityTest, HipReachableForIdenticalName) {
    EXPECT_TRUE(isHipReachableTarget("node-a:8000", "node-a:8000"));
}

TEST(MultiTransportLocalityTest, HipNotReachableForRemoteHost) {
    // Cross-host target: hip (GPU IPC) is not usable, must fall back to rdma.
    EXPECT_FALSE(isHipReachableTarget("10.0.0.2:20000", "10.0.0.1:20000"));
    EXPECT_FALSE(isHipReachableTarget("node-b:8000", "node-a:8000"));
}

TEST(MultiTransportLocalityTest, HandlesMissingPort) {
    // Locality decision still works when one side has no explicit port.
    EXPECT_TRUE(isHipReachableTarget("node-a", "node-a:8000"));
    EXPECT_FALSE(isHipReachableTarget("node-b", "node-a:8000"));
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

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

#include <cstdint>
#include <limits>

#include "tent_polling.h"

namespace mooncake::tent {
namespace {

TEST(TentPolling, PreservesSmallBatchesAndLowConcurrency) {
    EXPECT_FALSE(useTentPollingBackoff(true, 32, 262144, 1));
    EXPECT_FALSE(useTentPollingBackoff(true, 32, 262144, 31));
    EXPECT_FALSE(useTentPollingBackoff(true, 1, 262144, 32));
    EXPECT_FALSE(useTentPollingBackoff(true, 4, 1048576, 32));
}

TEST(TentPolling, BoundsConcurrentLargeHostRdmaWaits) {
    EXPECT_FALSE(useTentPollingBackoff(true, 8, 262144, 32));
    EXPECT_FALSE(useTentPollingBackoff(true, 31, 262144, 32));
    EXPECT_TRUE(useTentPollingBackoff(true, 32, 262144, 32));
    EXPECT_TRUE(useTentPollingBackoff(true, 32, 1048576, 8));
    EXPECT_TRUE(useTentPollingBackoff(true, 32, 8388608, 1));
}

TEST(TentPolling, LeavesDeviceAndOtherTransportWaitsHot) {
    EXPECT_FALSE(useTentPollingBackoff(false, 32, 262144, 32));
}

TEST(TentPolling, HandlesEmptyAndOverflowSizedPayloads) {
    constexpr auto max = std::numeric_limits<uint64_t>::max();
    EXPECT_FALSE(useTentPollingBackoff(true, 32, 0, max));
    EXPECT_FALSE(useTentPollingBackoff(true, 32, max, 0));
    EXPECT_TRUE(useTentPollingBackoff(true, 32, max, max));
    EXPECT_TRUE(useTentPollingBackoff(true, 32, 1, 8388608));
    EXPECT_FALSE(useTentPollingBackoff(true, 32, 1, 8388607));
}

}  // namespace
}  // namespace mooncake::tent

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

#include "transport/rdma_transport/rdma_context.h"

namespace mooncake {

TEST(RdmaAtomicDepthTest, UsesTheDeviceCapabilityWhenLowerThanDefault) {
    EXPECT_EQ(clampRdAtomicDepth(4), 4);
    EXPECT_EQ(clampRdAtomicDepth(8), 8);
    EXPECT_EQ(clampRdAtomicDepth(15), 15);
}

TEST(RdmaAtomicDepthTest, KeepsHistoricalDefaultWhenDeviceIsMoreCapable) {
    EXPECT_EQ(clampRdAtomicDepth(kIdealRdAtomicDepth), kIdealRdAtomicDepth);
    EXPECT_EQ(clampRdAtomicDepth(32), kIdealRdAtomicDepth);
}

TEST(RdmaAtomicDepthTest, FloorsAtOneForDevicesWithoutReadSupport) {
    // A device that reports 0 has no single-sided READ support; programming 1
    // keeps ibv_modify_qp() as the loud failure point instead of every READ.
    EXPECT_EQ(clampRdAtomicDepth(0), 1);
    EXPECT_EQ(clampRdAtomicDepth(-1), 1);
}

}  // namespace mooncake

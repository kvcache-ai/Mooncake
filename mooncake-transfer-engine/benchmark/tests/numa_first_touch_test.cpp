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

#include "numa_first_touch.h"

#include <gtest/gtest.h>

#ifdef __linux__
#include <sched.h>
#include <unistd.h>
#endif

namespace mooncake {
namespace tent {
namespace {

TEST(NumaMapsParse, EmptyAndNoise) {
    EXPECT_EQ(parseNumaMapsDominantNode(""), -1);
    EXPECT_EQ(parseNumaMapsDominantNode("numa_maps unavailable"), -1);
    EXPECT_EQ(parseNumaMapsDominantNode("not found in numa_maps"), -1);
    EXPECT_EQ(parseNumaMapsDominantNode("7fff000 default anon=262144"), -1);
}

TEST(NumaMapsParse, SingleNode) {
    EXPECT_EQ(parseNumaMapsDominantNode(
                  "7fbdbbfff000 default anon=262144 dirty=262144 N0=262144 "
                  "kernelpagesize_kB=4"),
              0);
    EXPECT_EQ(parseNumaMapsDominantNode(
                  "7fbd7bfff000 default anon=262144 dirty=262144 N1=262144 "
                  "kernelpagesize_kB=4"),
              1);
}

TEST(NumaMapsParse, MixedNodesPicksLargest) {
    EXPECT_EQ(parseNumaMapsDominantNode(
                  "7fe483fff000 default anon=262144 dirty=262144 N0=240640 "
                  "N1=21504 kernelpagesize_kB=4"),
              0);
    EXPECT_EQ(parseNumaMapsDominantNode("addr default N1=100 N0=20 N2=90"), 1);
}

TEST(NumaMapsParse, IgnoresEmbeddedN) {
    EXPECT_EQ(parseNumaMapsDominantNode("kernelpagesize_kB=4 file=/N0=9"), -1);
}

#ifdef __linux__
TEST(CpuAffinityMask, SaveRestoreRoundTrip) {
    CpuAffinityMask saved;
    ASSERT_TRUE(saved.saveCurrent());
    ASSERT_TRUE(saved.valid());
    ASSERT_TRUE(saved.restore());
}

TEST(CpuAffinityMask, PinToContainedCpuThenRestore) {
    CpuAffinityGuard guard;
    ASSERT_TRUE(guard.saved());
    int pick = -1;
    const long nproc = sysconf(_SC_NPROCESSORS_CONF);
    for (int cpu = 0; cpu < nproc && cpu < 4096; ++cpu) {
        if (guard.mask().containsCpu(cpu)) {
            pick = cpu;
            break;
        }
    }
    ASSERT_GE(pick, 0);
    ASSERT_TRUE(guard.mask().pinToCpu(pick));
    ASSERT_TRUE(guard.mask().restore());
}

TEST(CpuAffinityMask, RefusesPinWhenSaveFailedDoesNotApply) {
    CpuAffinityMask empty;
    EXPECT_FALSE(empty.valid());
    EXPECT_FALSE(empty.containsCpu(0));
    EXPECT_FALSE(empty.restore());
}

TEST(PickAllowedCpuOnNode, UnreachableNodeIsNegative) {
    CpuAffinityGuard guard;
    if (!guard.saved()) GTEST_SKIP() << "cannot read affinity";
    EXPECT_EQ(pickAllowedCpuOnNode(-1, guard.mask()), -1);
}
#endif

}  // namespace
}  // namespace tent
}  // namespace mooncake

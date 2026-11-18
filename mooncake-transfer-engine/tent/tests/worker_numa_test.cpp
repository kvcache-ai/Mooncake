// Copyright 2025 KVCache.AI
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
#include <numa.h>
#include <sched.h>

#include <thread>
#include "tent/transport/rdma/worker_numa.h"

namespace mooncake {
namespace tent {
namespace {

TEST(WorkerNumaConfigTest, DefaultsPreserveExistingBehavior) {
    Config conf;
    std::vector<int> nodes{9};
    bool local = true;
    ASSERT_TRUE(parseWorkerNumaConfig(conf, 2, nodes, local).ok());
    EXPECT_TRUE(nodes.empty());
    EXPECT_FALSE(local);
}

TEST(WorkerNumaConfigTest, SupportsSingleNodeAndPerWorkerPlacement) {
    for (const auto& expected : {std::vector<int>{0}, std::vector<int>{0, 1},
                                 std::vector<int>{1, 1}}) {
        Config conf;
        conf.set("transports/rdma/workers/numa_nodes", expected);
        conf.set("transports/rdma/workers/prefer_local", true);
        std::vector<int> nodes;
        bool local = false;
        ASSERT_TRUE(parseWorkerNumaConfig(conf, 2, nodes, local).ok());
        EXPECT_EQ(nodes, expected);
        EXPECT_TRUE(local);
    }
}

TEST(WorkerNumaConfigTest, RejectsMalformedNodeMapsWithoutPartialUpdate) {
    for (const json& invalid :
         {json("0"), json(0), json::array({0, 1, 2}), json::array({-1}),
          json::array({0.5}), json::array({"0"}), json::array({true}),
          json::array({18446744073709551615ULL})}) {
        Config conf;
        conf.set("transports/rdma/workers/numa_nodes", invalid);
        std::vector<int> nodes{7};
        bool local = false;
        EXPECT_FALSE(parseWorkerNumaConfig(conf, 2, nodes, local).ok());
        EXPECT_EQ(nodes, (std::vector<int>{7}));
        EXPECT_FALSE(local);
    }
}

TEST(WorkerNumaConfigTest, RejectsNonBooleanPolicy) {
    Config conf;
    conf.set("transports/rdma/workers/prefer_local", "true");
    std::vector<int> nodes;
    bool local = false;
    EXPECT_FALSE(parseWorkerNumaConfig(conf, 2, nodes, local).ok());
}

TEST(WorkerNumaDispatchTest, EmptyLocalSetPreservesGlobalRoundRobin) {
    for (size_t sequence = 0; sequence < 30; ++sequence)
        EXPECT_EQ(selectSubmissionWorker(sequence, 3, {}), sequence % 3);
}

TEST(WorkerNumaDispatchTest, BalancesOnlyAmongLocalWorkers) {
    const std::vector<int> local{1, 3};
    int counts[4] = {};
    for (size_t sequence = 1; sequence <= 64; ++sequence)
        ++counts[selectSubmissionWorker(sequence, 4, local)];
    EXPECT_EQ(counts[0], 0);
    EXPECT_EQ(counts[1], 32);
    EXPECT_EQ(counts[2], 0);
    EXPECT_EQ(counts[3], 32);
}

TEST(WorkerNumaDispatchTest, SingleLocalWorkerAndChangedLocalSet) {
    EXPECT_EQ(selectSubmissionWorker(9, 4, {2}), 2);
    // No thread-local cached route survives a change of submitter locality.
    EXPECT_EQ(selectSubmissionWorker(9, 4, {0, 2}), 2);
    EXPECT_EQ(selectSubmissionWorker(9, 4, {1, 3}), 3);
    EXPECT_EQ(selectSubmissionWorker(9, 4, {}), 1);
}

TEST(WorkerNumaBindingTest, InvalidNodeDoesNotChangeAffinity) {
    cpu_set_t before, after;
    ASSERT_EQ(pthread_getaffinity_np(pthread_self(), sizeof(before), &before),
              0);
    EXPECT_NE(bindRdmaWorkerToNode(-1), 0);
    EXPECT_NE(bindRdmaWorkerToNode(std::numeric_limits<int>::max()), 0);
    ASSERT_EQ(pthread_getaffinity_np(pthread_self(), sizeof(after), &after), 0);
    EXPECT_TRUE(CPU_EQUAL(&before, &after));
}

TEST(WorkerNumaBindingTest, RespectsInheritedSingleCpuAffinity) {
    if (numa_available() < 0) GTEST_SKIP() << "NUMA unavailable";
    // A separate thread ensures the test runner's affinity is never changed.
    std::thread worker([] {
        const int cpu = sched_getcpu();
        ASSERT_GE(cpu, 0);
        ASSERT_LT(cpu, CPU_SETSIZE);
        const int node = numa_node_of_cpu(cpu);
        ASSERT_GE(node, 0);
        cpu_set_t single;
        CPU_ZERO(&single);
        CPU_SET(cpu, &single);
        ASSERT_EQ(
            pthread_setaffinity_np(pthread_self(), sizeof(single), &single), 0);
        ASSERT_EQ(bindRdmaWorkerToNode(node), 0);
        cpu_set_t actual;
        ASSERT_EQ(
            pthread_getaffinity_np(pthread_self(), sizeof(actual), &actual), 0);
        EXPECT_TRUE(CPU_EQUAL(&single, &actual));
        for (int other = 0; other <= numa_max_node(); ++other) {
            if (other != node &&
                numa_bitmask_isbitset(numa_all_nodes_ptr, other)) {
                EXPECT_NE(bindRdmaWorkerToNode(other), 0);
                ASSERT_EQ(pthread_getaffinity_np(pthread_self(), sizeof(actual),
                                                 &actual),
                          0);
                EXPECT_TRUE(CPU_EQUAL(&single, &actual));
            }
        }
    });
    worker.join();
}

}  // namespace
}  // namespace tent
}  // namespace mooncake

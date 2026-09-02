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

#include "tent/runtime/capability_graph.h"

namespace mooncake {
namespace tent {
namespace {

void enable(CapabilityGraphInput& input, TransportType type,
            const Capabilities& caps) {
    input.transports[type].enabled = true;
    input.transports[type].caps = caps;
}

Capabilities crossDramOnly() {
    Capabilities caps;
    caps.cross_node_transfer = true;
    caps.dram_to_dram = true;
    return caps;
}

Capabilities localGpuCopy() {
    Capabilities caps;
    caps.local_stage_executor = true;
    caps.dram_to_gpu = true;
    caps.gpu_to_dram = true;
    caps.gpu_to_gpu = true;
    return caps;
}

TEST(CapabilityGraphTest, DirectRdmaPathPreferredForHostMemory) {
    CapabilityGraphInput input;
    input.local_memory_type = MTYPE_CPU;
    input.remote_memory_type = MTYPE_CPU;
    input.local_location = "cpu:0";
    input.remote_location = "cpu:1";
    input.server_addr = "server";
    enable(input, RDMA, crossDramOnly());

    auto path = CapabilityPathSynthesizer::synthesize(input);

    ASSERT_TRUE(path.found);
    EXPECT_TRUE(path.direct);
    EXPECT_EQ(path.cross_transport, RDMA);
    EXPECT_TRUE(path.local_stage_location.empty());
    EXPECT_TRUE(path.remote_stage_location.empty());
    EXPECT_EQ(path.edges.size(), 1u);
}

TEST(CapabilityGraphTest, KeepsMultipleDirectCandidates) {
    CapabilityGraphInput input;
    input.local_memory_type = MTYPE_CPU;
    input.remote_memory_type = MTYPE_CPU;
    input.local_location = "cpu:0";
    input.remote_location = "cpu:1";
    input.server_addr = "server";
    enable(input, RDMA, crossDramOnly());
    enable(input, TCP, crossDramOnly());

    auto path = CapabilityPathSynthesizer::synthesize(input);

    ASSERT_TRUE(path.found);
    EXPECT_TRUE(path.direct);
    EXPECT_EQ(path.cross_transport, RDMA);
    ASSERT_EQ(path.candidates.size(), 2u);
    EXPECT_NE(path.selected_candidate_index,
              std::numeric_limits<size_t>::max());

    bool saw_rdma = false;
    bool saw_tcp = false;
    for (const auto& candidate : path.candidates) {
        EXPECT_TRUE(candidate.direct);
        EXPECT_EQ(candidate.edges.size(), 1u);
        saw_rdma |= candidate.cross_transport == RDMA;
        saw_tcp |= candidate.cross_transport == TCP;
    }
    EXPECT_TRUE(saw_rdma);
    EXPECT_TRUE(saw_tcp);
}

TEST(CapabilityGraphTest, TransportWeightsCanPreferTcpDirectOverRdma) {
    CapabilityGraphInput input;
    input.local_memory_type = MTYPE_CPU;
    input.remote_memory_type = MTYPE_CPU;
    input.local_location = "cpu:0";
    input.remote_location = "cpu:1";
    input.server_addr = "server";
    enable(input, RDMA, crossDramOnly());
    enable(input, TCP, crossDramOnly());

    PathSynthesisOptions options;
    options.transport_cost[RDMA] = 10.0;
    options.transport_cost[TCP] = 1.0;

    auto path = CapabilityPathSynthesizer::synthesize(input, options);

    ASSERT_TRUE(path.found);
    EXPECT_TRUE(path.direct);
    EXPECT_EQ(path.cross_transport, TCP);
    ASSERT_EQ(path.candidates.size(), 2u);
    EXPECT_EQ(path.candidates[path.selected_candidate_index].cross_transport,
              TCP);
}

TEST(CapabilityGraphTest, StagesGpuTransferThroughHostWhenRdmaLacksGpuDirect) {
    CapabilityGraphInput input;
    input.local_memory_type = MTYPE_CUDA;
    input.remote_memory_type = MTYPE_CUDA;
    input.local_location = "cuda:0";
    input.remote_location = "cuda:1";
    input.server_addr = "server";
    input.local_stage_candidates.push_back({"cpu:0", MTYPE_CPU});
    input.remote_stage_candidates.push_back({"cpu:1", MTYPE_CPU});
    enable(input, RDMA, crossDramOnly());
    enable(input, NVLINK, localGpuCopy());

    auto path = CapabilityPathSynthesizer::synthesize(input);

    ASSERT_TRUE(path.found);
    EXPECT_FALSE(path.direct);
    EXPECT_EQ(path.cross_transport, RDMA);
    EXPECT_EQ(path.local_stage_location, "cpu:0");
    EXPECT_EQ(path.remote_stage_location, "cpu:1");
    ASSERT_EQ(path.edges.size(), 3u);
    EXPECT_EQ(path.edges[0].transport, NVLINK);
    EXPECT_EQ(path.edges[1].transport, RDMA);
    EXPECT_EQ(path.edges[2].transport, NVLINK);
    ASSERT_NE(path.selected_candidate_index,
              std::numeric_limits<size_t>::max());
    const auto& selected = path.candidates[path.selected_candidate_index];
    EXPECT_EQ(selected.direct, path.direct);
    EXPECT_EQ(selected.cross_transport, path.cross_transport);
    EXPECT_EQ(selected.local_stage_location, path.local_stage_location);
    EXPECT_EQ(selected.remote_stage_location, path.remote_stage_location);
}

TEST(CapabilityGraphTest, ChoosesStagedPathOverSlowDirectTcp) {
    CapabilityGraphInput input;
    input.local_memory_type = MTYPE_CUDA;
    input.remote_memory_type = MTYPE_CUDA;
    input.local_location = "cuda:0";
    input.remote_location = "cuda:1";
    input.server_addr = "server";
    input.local_stage_candidates.push_back({"cpu:0", MTYPE_CPU});
    input.remote_stage_candidates.push_back({"cpu:1", MTYPE_CPU});
    enable(input, RDMA, crossDramOnly());
    enable(input, NVLINK, localGpuCopy());

    Capabilities tcp;
    tcp.cross_node_transfer = true;
    tcp.dram_to_dram = true;
    tcp.dram_to_gpu = true;
    tcp.gpu_to_dram = true;
    tcp.gpu_to_gpu = true;
    enable(input, TCP, tcp);

    auto path = CapabilityPathSynthesizer::synthesize(input);

    ASSERT_TRUE(path.found);
    EXPECT_FALSE(path.direct);
    EXPECT_EQ(path.cross_transport, RDMA);
    EXPECT_EQ(path.local_stage_location, "cpu:0");
    EXPECT_EQ(path.remote_stage_location, "cpu:1");
    EXPECT_GE(path.candidates.size(), 2u);

    bool saw_direct_tcp = false;
    bool saw_staged_rdma = false;
    for (const auto& candidate : path.candidates) {
        saw_direct_tcp |= candidate.direct && candidate.cross_transport == TCP;
        saw_staged_rdma |= !candidate.direct &&
                           candidate.cross_transport == RDMA &&
                           candidate.local_stage_location == "cpu:0" &&
                           candidate.remote_stage_location == "cpu:1";
    }
    EXPECT_TRUE(saw_direct_tcp);
    EXPECT_TRUE(saw_staged_rdma);
}

TEST(CapabilityGraphTest, IntentOptionsCanPreferDirectPath) {
    CapabilityGraphInput input;
    input.local_memory_type = MTYPE_CUDA;
    input.remote_memory_type = MTYPE_CUDA;
    input.local_location = "cuda:0";
    input.remote_location = "cuda:1";
    input.server_addr = "server";
    input.local_stage_candidates.push_back({"cpu:0", MTYPE_CPU});
    input.remote_stage_candidates.push_back({"cpu:1", MTYPE_CPU});
    enable(input, RDMA, crossDramOnly());
    enable(input, NVLINK, localGpuCopy());

    Capabilities tcp;
    tcp.cross_node_transfer = true;
    tcp.dram_to_dram = true;
    tcp.dram_to_gpu = true;
    tcp.gpu_to_dram = true;
    tcp.gpu_to_gpu = true;
    enable(input, TCP, tcp);

    PathSynthesisOptions options;
    options.staged_path_penalty = 20.0;

    auto path = CapabilityPathSynthesizer::synthesize(input, options);

    ASSERT_TRUE(path.found);
    EXPECT_TRUE(path.direct);
    EXPECT_EQ(path.cross_transport, TCP);
}

TEST(CapabilityGraphTest, UsesRemoteStageForHostToGpuTransfer) {
    CapabilityGraphInput input;
    input.local_memory_type = MTYPE_CPU;
    input.remote_memory_type = MTYPE_CUDA;
    input.local_location = "cpu:0";
    input.remote_location = "cuda:1";
    input.server_addr = "server";
    input.remote_stage_candidates.push_back({"cpu:1", MTYPE_CPU});
    enable(input, RDMA, crossDramOnly());
    enable(input, NVLINK, localGpuCopy());

    auto path = CapabilityPathSynthesizer::synthesize(input);

    ASSERT_TRUE(path.found);
    EXPECT_FALSE(path.direct);
    EXPECT_EQ(path.cross_transport, RDMA);
    EXPECT_TRUE(path.local_stage_location.empty());
    EXPECT_EQ(path.remote_stage_location, "cpu:1");
    ASSERT_EQ(path.edges.size(), 2u);
    EXPECT_EQ(path.edges[0].transport, RDMA);
    EXPECT_EQ(path.edges[1].transport, NVLINK);
}

TEST(CapabilityGraphTest, StagesTpuHbmThroughHostMemory) {
    CapabilityGraphInput input;
    input.local_memory_type = MTYPE_TPU;
    input.remote_memory_type = MTYPE_TPU;
    input.local_location = "tpu:0";
    input.remote_location = "tpu:1";
    input.server_addr = "server";
    input.local_stage_candidates.push_back({"cpu:0", MTYPE_CPU});
    input.remote_stage_candidates.push_back({"cpu:1", MTYPE_CPU});
    enable(input, TCP, crossDramOnly());

    Capabilities tpu;
    tpu.local_stage_executor = true;
    tpu.gpu_to_dram = true;
    tpu.dram_to_gpu = true;
    enable(input, TPU, tpu);

    auto path = CapabilityPathSynthesizer::synthesize(input);

    ASSERT_TRUE(path.found);
    EXPECT_FALSE(path.direct);
    EXPECT_EQ(path.cross_transport, TCP);
    EXPECT_EQ(path.local_stage_location, "cpu:0");
    EXPECT_EQ(path.remote_stage_location, "cpu:1");
}

TEST(CapabilityGraphTest, SynthesizesCudaStagesForMnnvlHostTransfer) {
    CapabilityGraphInput input;
    input.local_memory_type = MTYPE_CPU;
    input.remote_memory_type = MTYPE_CPU;
    input.local_location = "cpu:0";
    input.remote_location = "cpu:1";
    input.server_addr = "server";
    input.local_stage_candidates.push_back({"cuda:0", MTYPE_CUDA});
    input.remote_stage_candidates.push_back({"cuda:1", MTYPE_CUDA});

    Capabilities mnnvl;
    mnnvl.cross_node_transfer = true;
    mnnvl.dram_to_gpu = true;
    mnnvl.gpu_to_gpu = true;
    enable(input, MNNVL, mnnvl);
    enable(input, NVLINK, localGpuCopy());

    auto path = CapabilityPathSynthesizer::synthesize(input);

    ASSERT_TRUE(path.found);
    EXPECT_FALSE(path.direct);
    EXPECT_EQ(path.cross_transport, MNNVL);
    EXPECT_TRUE(path.local_stage_location == "cuda:0" ||
                path.remote_stage_location == "cuda:1");
}

TEST(CapabilityGraphTest, ReportsNoFeasiblePathWithoutLocalExecutors) {
    CapabilityGraphInput input;
    input.local_memory_type = MTYPE_CUDA;
    input.remote_memory_type = MTYPE_CUDA;
    input.local_location = "cuda:0";
    input.remote_location = "cuda:1";
    input.server_addr = "server";
    input.local_stage_candidates.push_back({"cpu:0", MTYPE_CPU});
    input.remote_stage_candidates.push_back({"cpu:1", MTYPE_CPU});
    enable(input, RDMA, crossDramOnly());

    auto path = CapabilityPathSynthesizer::synthesize(input);

    EXPECT_FALSE(path.found);
    EXPECT_FALSE(path.direct);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake

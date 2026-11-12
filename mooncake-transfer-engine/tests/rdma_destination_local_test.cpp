// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
#include <gtest/gtest.h>

#include "config.h"
#include "cuda_alike.h"
#include "topology.h"
#include "transport/rdma_transport/rdma_transport.h"

namespace mooncake {
namespace {
// Topology keys GPU memory by the build's GPU_PREFIX (cuda:, hip:, ...); the
// tests write "gpu:" and substitute it.
std::string gpus(std::string text) {
    static const std::string kPlaceholder = "gpu:";
    for (size_t at = 0; (at = text.find(kPlaceholder, at)) != std::string::npos;
         at += GPU_PREFIX.size())
        text.replace(at, kPlaceholder.size(), GPU_PREFIX);
    return text;
}

std::string pin(const Topology &topology, const std::string &location) {
    const int index = topology.pinnedDevice(gpus(location));
    return index < 0 ? "" : topology.getHcaList().at(index);
}

TEST(DestinationPinning, CanonicalizesPreferredSetsAndOrdersGpuOrdinals) {
    Topology topology;
    ASSERT_EQ(topology.parse(gpus(R"({
        "gpu:10":[["rail1","rail0"],[]],
        "gpu:2":[["rail0","rail1","rail0"],[]],
        "gpu:0":[["rail1","rail0"],[]],
        "cpu:0":[["rail0"],[]],
        "gpu:bad":[["rail0"],[]]
    })")),
              0);
    EXPECT_EQ(pin(topology, "gpu:0"), "rail0");
    EXPECT_EQ(pin(topology, "gpu:2"), "rail1");
    EXPECT_EQ(pin(topology, "gpu:10"), "rail0");
    EXPECT_EQ(pin(topology, "cpu:0"), "");
    EXPECT_EQ(pin(topology, "gpu:bad"), "");
    EXPECT_EQ(pin(topology, "gpu:99"), "");
}

TEST(DestinationPinning, KeepsSinglePreferredNicAndDoesNotPinFallbackOnlyGpu) {
    Topology topology;
    ASSERT_EQ(topology.parse(gpus(R"({"gpu:0":[["rail1"],["rail0"]],
                                      "gpu:1":[[],["rail0"]]})")),
              0);
    EXPECT_EQ(pin(topology, "gpu:0"), "rail1");
    EXPECT_EQ(pin(topology, "gpu:1"), "");
    topology.clear();
    EXPECT_EQ(pin(topology, "gpu:0"), "");
    ASSERT_EQ(topology.parse(gpus(R"({"gpu:0":[["newrail"],[]]})")), 0);
    EXPECT_EQ(pin(topology, "gpu:0"), "newrail");
}

TEST(DestinationPinning, DisabledNicCannotRemainPinned) {
    Topology topology;
    ASSERT_EQ(topology.parse(gpus(R"({"gpu:0":[["rail0","rail0","rail1"],[]],
                                      "gpu:1":[["rail0","rail1"],[]]})")),
              0);
    ASSERT_EQ(topology.disableDevice("rail0"), 0);
    EXPECT_EQ(pin(topology, "gpu:0"), "rail1");
    EXPECT_EQ(pin(topology, "gpu:1"), "rail1");
    ASSERT_EQ(topology.disableDevice("rail1"), 0);
    EXPECT_EQ(pin(topology, "gpu:0"), "");
    EXPECT_EQ(pin(topology, "gpu:1"), "");
}

class DestinationRail : public ::testing::Test {
   protected:
    void SetUp() override {
        old_local = globalConfig().enable_dest_local_rail;
        old_affinity = globalConfig().enable_dest_device_affinity;
        globalConfig().enable_dest_local_rail = true;
        globalConfig().enable_dest_device_affinity = true;
        ASSERT_EQ(peer.topology.parse(gpus(R"({"gpu:0":[["rail0","rail1"],[]],
                                                "gpu:1":[["rail0","rail1"],[]]})")),
                  0);
        TransferMetadata::BufferDesc buffer;
        buffer.addr = 4096;
        buffer.length = 1024;
        buffer.name = gpus("gpu:1");
        // Device selection skips HCAs without keys for the buffer.
        buffer.lkey = {1, 2};
        buffer.rkey = {1, 2};
        peer.buffers.push_back(buffer);
    }
    void TearDown() override {
        globalConfig().enable_dest_local_rail = old_local;
        globalConfig().enable_dest_device_affinity = old_affinity;
    }
    std::string hint(Transport::TransferRequest::OpCode opcode =
                         Transport::TransferRequest::WRITE,
                     uint64_t offset = 4096, size_t length = 32) {
        return RdmaTransport::destinationLocalHca(&peer, opcode, offset,
                                                  length);
    }
    TransferMetadata::SegmentDesc peer;
    bool old_local, old_affinity;
};

TEST_F(DestinationRail, PicksDestinationGpuRailAndUsesItOnSender) {
    EXPECT_EQ(hint(), "rail1");
    TransferMetadata::SegmentDesc local;
    ASSERT_EQ(local.topology.parse(gpus(R"({"gpu:0":[["rail0"],["rail1"]]})")),
              0);
    local.buffers = peer.buffers;
    local.buffers[0].name = gpus("gpu:0");
    int buffer = -1, device = -1;
    ASSERT_EQ(
        RdmaTransport::selectDevice(&local, 4096, 32, hint(), buffer, device),
        0);
    EXPECT_EQ(local.topology.getHcaList().at(device), "rail1");
    // A host without the named rail retains ordinary local selection.
    ASSERT_EQ(local.topology.parse(gpus(R"({"gpu:0":[["otherrail"],[]]})")), 0);
    ASSERT_EQ(
        RdmaTransport::selectDevice(&local, 4096, 32, hint(), buffer, device),
        0);
    EXPECT_EQ(local.topology.getHcaList().at(device), "otherrail");
}

TEST_F(DestinationRail, KeepsReadHostAndUnknownRangesOnNormalPolicy) {
    EXPECT_EQ(hint(Transport::TransferRequest::READ), "");
    EXPECT_EQ(hint(Transport::TransferRequest::WRITE, 0), "");
    EXPECT_EQ(hint(Transport::TransferRequest::WRITE, 4096, 0), "");
    EXPECT_EQ(hint(Transport::TransferRequest::WRITE, 5000, 512), "");
    EXPECT_EQ(RdmaTransport::destinationLocalHca(
                  nullptr, Transport::TransferRequest::WRITE, 4096, 32),
              "");
    peer.buffers[0].name = "cpu:0";
    EXPECT_EQ(hint(), "");
}

TEST_F(DestinationRail, FollowsOneGpuAcrossAdjacentMemoryRegions) {
    // A receiver that splits one allocation by MC_MAX_MR_SIZE registers
    // adjacent MRs; a WRITE crossing their boundary still targets one GPU.
    TransferMetadata::BufferDesc next = peer.buffers[0];
    next.addr = 5120;
    peer.buffers.push_back(next);
    EXPECT_EQ(hint(Transport::TransferRequest::WRITE, 5000, 512), "rail1");
    peer.buffers[1].name = gpus("gpu:0");
    EXPECT_EQ(hint(Transport::TransferRequest::WRITE, 5000, 512), "");
}

TEST_F(DestinationRail, RequiresBothOptInAndDestinationAffinity) {
    globalConfig().enable_dest_local_rail = false;
    EXPECT_EQ(hint(), "");
    globalConfig().enable_dest_local_rail = true;
    globalConfig().enable_dest_device_affinity = false;
    EXPECT_EQ(hint(), "");
}

TEST(DestinationRailConfig, ParsesOptInAndRejectsMissingAffinity) {
    ::setenv("MC_ENABLE_DEST_LOCAL_RAIL", "true", 1);
    ::unsetenv("MC_ENABLE_DEST_DEVICE_AFFINITY");
    GlobalConfig config;
    loadGlobalConfig(config);
    EXPECT_FALSE(config.enable_dest_local_rail);
    ::setenv("MC_ENABLE_DEST_DEVICE_AFFINITY", "1", 1);
    loadGlobalConfig(config);
    EXPECT_TRUE(config.enable_dest_local_rail);
    ::setenv("MC_ENABLE_DEST_LOCAL_RAIL", "false", 1);
    loadGlobalConfig(config);
    EXPECT_FALSE(config.enable_dest_local_rail);
    ::unsetenv("MC_ENABLE_DEST_LOCAL_RAIL");
    ::unsetenv("MC_ENABLE_DEST_DEVICE_AFFINITY");
}
}  // namespace
}  // namespace mooncake

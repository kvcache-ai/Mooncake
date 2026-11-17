// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
#include <gtest/gtest.h>

#include <cstdlib>
#include <optional>
#include <string>
#include <string_view>

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

class DestinationNic : public ::testing::Test {
   protected:
    void SetUp() override {
        ASSERT_EQ(peer.topology.parse(gpus(R"({"gpu:0":[["rail0"],["rail1"]],
                                                "gpu:1":[["rail1"],["rail0"]]})")),
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
    // The destination NIC for [offset, offset + length); "!" when no single
    // peer MR covers the range.
    std::string nic(uint64_t offset = 4096, size_t length = 32) {
        std::string_view name;
        int buffer = -1, device = -1;
        if (RdmaTransport::selectDestinationNic(&peer, offset, length, name,
                                                buffer, device))
            return "!";
        return std::string(name);
    }
    TransferMetadata::SegmentDesc peer;
};

TEST_F(DestinationNic, NamesThePeersPickAndTheSenderUsesIt) {
    EXPECT_EQ(nic(), "rail1");
    TransferMetadata::SegmentDesc local;
    ASSERT_EQ(local.topology.parse(gpus(R"({"gpu:0":[["rail0"],["rail1"]]})")),
              0);
    local.buffers = peer.buffers;
    local.buffers[0].name = gpus("gpu:0");
    int buffer = -1, device = -1;
    ASSERT_EQ(
        RdmaTransport::selectDevice(&local, 4096, 32, nic(), buffer, device),
        0);
    EXPECT_EQ(local.topology.getHcaList().at(device), "rail1");
    // A sender without an HCA of that name keeps ordinary selection.
    ASSERT_EQ(local.topology.parse(gpus(R"({"gpu:0":[["otherrail"],[]]})")), 0);
    ASSERT_EQ(
        RdmaTransport::selectDevice(&local, 4096, 32, nic(), buffer, device),
        0);
    EXPECT_EQ(local.topology.getHcaList().at(device), "otherrail");
}

TEST_F(DestinationNic, IsEmptyForHostMemory) {
    peer.buffers[0].name = "cpu:0";
    EXPECT_EQ(nic(), "");
}

TEST_F(DestinationNic, FailsWhenNoSinglePeerMrCoversTheRange) {
    EXPECT_EQ(nic(0, 32), "!");
    EXPECT_EQ(nic(5000, 512), "!");
    // A receiver that splits one allocation by MC_MAX_MR_SIZE registers
    // adjacent MRs; a range crossing them is left to per-slice selection.
    TransferMetadata::BufferDesc next = peer.buffers[0];
    next.addr = 5120;
    peer.buffers.push_back(next);
    EXPECT_EQ(nic(5000, 512), "!");
    EXPECT_EQ(nic(5120, 512), "rail1");
}

TEST_F(DestinationNic, KeepsThePeerPickForTheSameMr) {
    ASSERT_EQ(peer.topology.parse(gpus(R"({"gpu:1":[["rail0","rail1"],[]]})")),
              0);
    std::string_view first;
    int buffer = -1, device = -1;
    ASSERT_EQ(RdmaTransport::selectDestinationNic(&peer, 4096, 32, first,
                                                  buffer, device),
              0);
    ASSERT_FALSE(first.empty());
    for (int i = 0; i < 32; ++i) {
        std::string_view name;
        int next_buffer = -1, next_device = -1;
        ASSERT_EQ(RdmaTransport::selectDestinationNic(&peer, 4096 + i, 32, name,
                                                      next_buffer, next_device,
                                                      buffer, device),
                  0);
        EXPECT_EQ(name, first);
        EXPECT_EQ(next_device, device);
    }
}

class EnvVar {
   public:
    EnvVar(const char *name, const char *value) : name_(name) {
        if (const char *old = std::getenv(name)) old_ = old;
        if (value)
            ::setenv(name, value, 1);
        else
            ::unsetenv(name);
    }
    ~EnvVar() {
        if (old_)
            ::setenv(name_, old_->c_str(), 1);
        else
            ::unsetenv(name_);
    }

   private:
    const char *name_;
    std::optional<std::string> old_;
};

GlobalConfig loadedConfig() {
    GlobalConfig config;
    loadGlobalConfig(config);
    return config;
}

TEST(DestinationNicConfig, ImpliesDestinationAffinity) {
    EnvVar affinity("MC_ENABLE_DEST_DEVICE_AFFINITY", nullptr);
    EnvVar peer_affinity("MC_ENABLE_HCA_PEER_AFFINITY", nullptr);
    {
        EnvVar knob("MC_RDMA_WRITE_PREFER_DEST_NIC", "true");
        const auto config = loadedConfig();
        EXPECT_TRUE(config.rdma_write_prefer_dest_nic);
        EXPECT_TRUE(config.enable_dest_device_affinity);
    }
    {
        EnvVar knob("MC_RDMA_WRITE_PREFER_DEST_NIC", "false");
        const auto config = loadedConfig();
        EXPECT_FALSE(config.rdma_write_prefer_dest_nic);
        EXPECT_FALSE(config.enable_dest_device_affinity);
    }
    const auto config = loadedConfig();
    EXPECT_FALSE(config.rdma_write_prefer_dest_nic);
}

TEST(DestinationNicConfig, YieldsToHcaPeerAffinity) {
    EnvVar affinity("MC_ENABLE_DEST_DEVICE_AFFINITY", nullptr);
    EnvVar peer_affinity("MC_ENABLE_HCA_PEER_AFFINITY", "1");
    EnvVar knob("MC_RDMA_WRITE_PREFER_DEST_NIC", "1");
    const auto config = loadedConfig();
    EXPECT_FALSE(config.rdma_write_prefer_dest_nic);
    EXPECT_FALSE(config.enable_dest_device_affinity);
    EXPECT_TRUE(config.enable_hca_peer_affinity);
}

TEST(DestinationNicConfig, AllThreeKeepTheAffinityClashAsIs) {
    // The option yields to HCA peer affinity first; the existing clash between
    // the two affinities then turns both off, as without the option.
    EnvVar affinity("MC_ENABLE_DEST_DEVICE_AFFINITY", "1");
    EnvVar peer_affinity("MC_ENABLE_HCA_PEER_AFFINITY", "1");
    EnvVar knob("MC_RDMA_WRITE_PREFER_DEST_NIC", "1");
    const auto config = loadedConfig();
    EXPECT_FALSE(config.rdma_write_prefer_dest_nic);
    EXPECT_FALSE(config.enable_dest_device_affinity);
    EXPECT_FALSE(config.enable_hca_peer_affinity);
}
}  // namespace
}  // namespace mooncake

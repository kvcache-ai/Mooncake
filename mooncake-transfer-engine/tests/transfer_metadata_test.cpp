// Copyright 2024 KVCache.AI
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

#include "transfer_metadata.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <sys/time.h>

#include <cstdlib>

#include "transport/transport.h"

using namespace mooncake;

namespace mooncake {

class TransferMetadataTest : public ::testing::Test {
   protected:
    void SetUp() override {
        // initialize glog
        google::InitGoogleLogging("TransferMetadataTest");
        FLAGS_logtostderr = 1;  // output to stdout

        const char* env = std::getenv("MC_METADATA_SERVER");
        if (env)
            metadata_server = env;
        else
            metadata_server = metadata_server;
        LOG(INFO) << "metadata_server: " << metadata_server;

        env = std::getenv("MC_LOCAL_SERVER_NAME");
        if (env)
            local_server_name = env;
        else
            local_server_name = "127.0.0.2:12345";
        LOG(INFO) << "local_server_name: " << local_server_name;

        metadata_client = std::make_unique<TransferMetadata>(metadata_server);
    }
    void TearDown() override {
        // clean up glog
        google::ShutdownGoogleLogging();
    }
    std::unique_ptr<TransferMetadata> metadata_client;
    std::string metadata_server;
    std::string local_server_name;

    static TransferMetadata::SegmentDesc MakeRdmaSegment(
        const std::string& name) {
        TransferMetadata::SegmentDesc desc;
        desc.name = name;
        desc.protocol = "rdma";
        desc.tcp_data_port = 1234;

        TransferMetadata::DeviceDesc device;
        device.name = "mlx5_0";
        device.lid = 1;
        device.gid = "0000:0000:0000:0000:0000:ffff:7f00:0001";
        desc.devices.push_back(device);

        TransferMetadata::BufferDesc buffer;
        buffer.name = "buffer-0";
        buffer.addr = 0x100000;
        buffer.length = 4096;
        buffer.lkey.push_back(11);
        buffer.rkey.push_back(22);
        desc.buffers.push_back(buffer);
        return desc;
    }
};

// add and search LocalSegmentMeta
TEST_F(TransferMetadataTest, LocalSegmentTest) {
    auto segment_des = std::make_shared<TransferMetadata::SegmentDesc>();
    segment_des->name = "test_server";
    segment_des->protocol = "rdma";
    TransferMetadata::SegmentID segment_id = 1111111;
    std::string segment_name = "test_segment";
    int re = metadata_client->addLocalSegment(segment_id, segment_name,
                                              std::move(segment_des));
    ASSERT_EQ(re, 0);
    auto des = metadata_client->getSegmentDescByName(segment_name);
    ASSERT_EQ(des, segment_des);
    ASSERT_EQ(des->metadata_version, 1);
    des = metadata_client->getSegmentDescByID(segment_id, false);
    ASSERT_EQ(des, segment_des);
    auto id = metadata_client->getSegmentID(segment_name);
    ASSERT_EQ(id, segment_id);
    re = metadata_client->removeLocalSegment(segment_name);
    ASSERT_EQ(re, 0);
}

TEST_F(TransferMetadataTest, LocalSegmentPreservesExplicitVersion) {
    auto segment_des = std::make_shared<TransferMetadata::SegmentDesc>(
        MakeRdmaSegment("explicit_versions"));
    segment_des->metadata_version = 8;
    segment_des->buffers[0].state =
        TransferMetadata::BufferDesc::STATE_DRAINING;

    ASSERT_EQ(metadata_client->addLocalSegment(2222222, "explicit_segment",
                                               std::move(segment_des)),
              0);

    auto desc = metadata_client->getSegmentDescByID(2222222);
    ASSERT_TRUE(desc);
    ASSERT_EQ(desc->metadata_version, 8);
    ASSERT_EQ(desc->buffers.size(), 1);
    ASSERT_EQ(desc->buffers[0].state,
              TransferMetadata::BufferDesc::STATE_DRAINING);
}

// add and remove LocalMemoryBufferMeta
TEST_F(TransferMetadataTest, LocalMemoryBufferTest) {
    auto segment_des = std::make_shared<TransferMetadata::SegmentDesc>();
    segment_des->name = "test_localMemory";
    segment_des->protocol = "rdma";
    int re = metadata_client->addLocalSegment(
        LOCAL_SEGMENT_ID, "test_local_segment", std::move(segment_des));
    ASSERT_EQ(re, 0);
    auto local_desc = metadata_client->getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_TRUE(local_desc);
    auto metadata_version = local_desc->metadata_version;
    uint64_t addr = 0;
    for (int i = 0; i < 10; ++i) {
        TransferMetadata::BufferDesc buffer_des;
        buffer_des.addr = addr + i * 2048;
        buffer_des.length = 1024;
        re = metadata_client->addLocalMemoryBuffer(buffer_des, false);
        ASSERT_EQ(re, 0);
    }
    local_desc = metadata_client->getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_TRUE(local_desc);
    ASSERT_EQ(local_desc->metadata_version, metadata_version);
    ASSERT_EQ(local_desc->buffers.size(), 10);
    for (const auto& buffer : local_desc->buffers) {
        ASSERT_EQ(buffer.state, TransferMetadata::BufferDesc::STATE_READY);
    }
    ASSERT_EQ(metadata_client->updateLocalSegmentDesc(), 0);
    local_desc = metadata_client->getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_TRUE(local_desc);
    ASSERT_GT(local_desc->metadata_version, metadata_version);
    addr = 1000;
    re = metadata_client->removeLocalMemoryBuffer((void*)addr, false);
    ASSERT_EQ(re, ERR_ADDRESS_NOT_REGISTERED);
    auto before_remove_metadata_version = local_desc->metadata_version;
    for (int i = 9; i > 0; --i) {
        addr = i * 2048;
        re = metadata_client->removeLocalMemoryBuffer((void*)addr, false);
        ASSERT_EQ(re, 0);
    }
    local_desc = metadata_client->getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_TRUE(local_desc);
    ASSERT_EQ(local_desc->metadata_version, before_remove_metadata_version);
    ASSERT_EQ(local_desc->buffers.size(), 1);
    re = metadata_client->removeLocalSegment("test_local_segment");
    ASSERT_EQ(re, 0);
}

TEST_F(TransferMetadataTest, LocalMemoryBufferDeregisterWithMetadata) {
    auto segment_des = std::make_shared<TransferMetadata::SegmentDesc>();
    segment_des->name = "test_metadata_deregister";
    segment_des->protocol = "rdma";
    ASSERT_EQ(metadata_client->addLocalSegment(
                  LOCAL_SEGMENT_ID, "test_metadata_deregister",
                  std::move(segment_des)),
              0);

    TransferMetadata::BufferDesc buffer_des;
    buffer_des.addr = 4096;
    buffer_des.length = 1024;
    ASSERT_EQ(metadata_client->addLocalMemoryBuffer(buffer_des, false), 0);

    auto local_desc = metadata_client->getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_TRUE(local_desc);
    auto metadata_version = local_desc->metadata_version;

    ASSERT_EQ(metadata_client->removeLocalMemoryBuffer((void*)4096, true), 0);
    local_desc = metadata_client->getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_TRUE(local_desc);
    ASSERT_GT(local_desc->metadata_version, metadata_version);
    ASSERT_TRUE(local_desc->buffers.empty());
}

TEST_F(TransferMetadataTest, SegmentDescJsonRoundTripIncludesReliabilityFields) {
    auto desc = MakeRdmaSegment("json_round_trip");
    desc.metadata_version = 42;
    desc.buffers[0].state = TransferMetadata::BufferDesc::STATE_READY;

    Json::Value json;
    ASSERT_EQ(metadata_client->encodeSegmentDesc(desc, json), 0);
    ASSERT_EQ(json["metadata_version"].asUInt64(), desc.metadata_version);
    ASSERT_EQ(json["buffers"][0]["state"].asString(), desc.buffers[0].state);

    auto decoded = metadata_client->decodeSegmentDesc(json, desc.name);
    ASSERT_TRUE(decoded);
    ASSERT_EQ(decoded->metadata_version, desc.metadata_version);
    ASSERT_EQ(decoded->buffers.size(), 1);
    ASSERT_EQ(decoded->buffers[0].state, desc.buffers[0].state);
}

TEST_F(TransferMetadataTest, LegacySegmentDescJsonDecodesAsReady) {
    auto desc = MakeRdmaSegment("legacy_json");
    Json::Value json;
    ASSERT_EQ(metadata_client->encodeSegmentDesc(desc, json), 0);
    json.removeMember("metadata_version");
    json["buffers"][0].removeMember("state");

    auto decoded = metadata_client->decodeSegmentDesc(json, desc.name);
    ASSERT_TRUE(decoded);
    ASSERT_EQ(decoded->metadata_version, 0);
    ASSERT_EQ(decoded->buffers.size(), 1);
    ASSERT_EQ(decoded->buffers[0].state,
              TransferMetadata::BufferDesc::STATE_READY);
}

TEST_F(TransferMetadataTest, SegmentCacheTracksMetadataVersionChanges) {
    auto desc_v1 = std::make_shared<TransferMetadata::SegmentDesc>(
        MakeRdmaSegment("remote_segment"));
    desc_v1->metadata_version = 1;

    metadata_client->updateSegmentCacheEntry(3333333, "remote_segment",
                                             desc_v1);
    ASSERT_EQ(metadata_client->segmentMetadataVersionChangeCount(), 0);

    auto desc_v2 = std::make_shared<TransferMetadata::SegmentDesc>(*desc_v1);
    desc_v2->metadata_version = 2;
    metadata_client->updateSegmentCacheEntry(3333333, "remote_segment",
                                             desc_v2);
    ASSERT_EQ(metadata_client->segmentMetadataVersionChangeCount(), 1);

    auto desc_v3 = std::make_shared<TransferMetadata::SegmentDesc>(*desc_v2);
    desc_v3->metadata_version = 3;
    metadata_client->updateSegmentCacheEntry(3333333, "remote_segment",
                                             desc_v3);
    ASSERT_EQ(metadata_client->segmentMetadataVersionChangeCount(), 2);
}

TEST_F(TransferMetadataTest, SegmentCacheIgnoresNullDescriptor) {
    metadata_client->updateSegmentCacheEntry(4444444, "null_remote_segment",
                                             nullptr);

    auto desc = metadata_client->getSegmentDescByID(4444444);
    ASSERT_FALSE(desc);
}

// add, get and remove RPCMetaEntryMeta
TEST_F(TransferMetadataTest, RpcMetaEntryTest) {
    if (metadata_server == P2PHANDSHAKE) {
        GTEST_SKIP() << "P2P RPC metadata requires a local listening socket";
    }
    auto hostname_port = parseHostNameWithPort(local_server_name);
    TransferMetadata::RpcMetaDesc desc;
    desc.ip_or_host_name = hostname_port.first.c_str();
    desc.rpc_port = hostname_port.second;
    int re = metadata_client->addRpcMetaEntry("test_server", desc);
    ASSERT_EQ(re, 0);
    TransferMetadata::RpcMetaDesc desc1;
    re = metadata_client->getRpcMetaEntry("test_server", desc1);
    ASSERT_EQ(desc.ip_or_host_name, desc1.ip_or_host_name);
    ASSERT_EQ(desc.rpc_port, desc1.rpc_port);
    re = metadata_client->removeRpcMetaEntry("test_server");
    ASSERT_EQ(re, 0);
}

}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

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

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <sys/time.h>

#include <cstdlib>

#include "transport/transport.h"
#include "transfer_metadata.h"

using namespace mooncake;

//default value is used for local test
DEFINE_string(metadata_server, "127.0.0.1:2379", "etcd server host address");
DEFINE_string(local_server, "127.0.0.2:12345", "Local server name for segment discovery");

namespace mooncake {

class TransferMetadataTest : public ::testing::Test {
   protected:
    void SetUp() override {
        // initialize glog
        google::InitGoogleLogging("TransferMetadataTest");
        FLAGS_logtostderr = 1;  // output to stdout
        metadata_client = std::make_unique<TransferMetadata>(FLAGS_metadata_server);
    }
    void TearDown() override {
        // clean up glog
        google::ShutdownGoogleLogging();
    }
    std::unique_ptr<TransferMetadata> metadata_client;
};

//add and search LocalSegmentMeta
TEST_F(TransferMetadataTest, LocalSegmentTest) {
    auto segment_des = std::make_shared<TransferMetadata::SegmentDesc>();
    segment_des->name="test_server";
    segment_des->protocol="rdma";
    TransferMetadata::SegmentID segment_id=1111111;
    std::string segment_name="test_segment";
    int re = metadata_client->addLocalSegment(segment_id, segment_name, std::move(segment_des));
    ASSERT_EQ(re, 0);
    auto des = metadata_client->getSegmentDescByName(segment_name);
    ASSERT_EQ(des,segment_des);
    des = metadata_client->getSegmentDescByID(segment_id, false);
    ASSERT_EQ(des,segment_des);
    auto id = metadata_client->getSegmentID(segment_name);
    ASSERT_EQ(id, segment_id);
}

// add and remove LocalMemoryBufferMeta
TEST_F(TransferMetadataTest, LocalMemoryBufferTest) {
    auto segment_des = std::make_shared<TransferMetadata::SegmentDesc>();
    segment_des->name="test_localMemery";
    segment_des->protocol="rdma";
    int re = metadata_client->addLocalSegment(LOCAL_SEGMENT_ID, "test_local_segment", std::move(segment_des));
    ASSERT_EQ(re, 0);
    uint64_t addr=0;
    for (int i = 0; i < 10; ++i) {
        TransferMetadata::BufferDesc buffer_des;
        buffer_des.addr= addr + i*2048;
        buffer_des.length=1024;
        re = metadata_client->addLocalMemoryBuffer(buffer_des, false);
        ASSERT_EQ(re, 0);
    }
    addr=1000;
    re = metadata_client->removeLocalMemoryBuffer((void*)addr, false);
    ASSERT_EQ(re, ERR_ADDRESS_NOT_REGISTERED);
    for (int i = 9; i > 0; --i) {
        addr = i*2048;
        re = metadata_client->removeLocalMemoryBuffer((void*)addr, false);
        ASSERT_EQ(re, 0);
    }
}

//add, get and remove RPCMetaEntryMeta
TEST_F(TransferMetadataTest, RpcMetaEntryTest) {
    auto hostname_port = parseHostNameWithPort(FLAGS_local_server);
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

}// namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
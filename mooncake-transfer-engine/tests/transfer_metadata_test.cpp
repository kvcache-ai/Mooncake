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
            metadata_server = "http://127.0.0.1:8080/metadata";
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
};

// add and search LocalSegmentMeta
TEST_F(TransferMetadataTest, LocalSegmentTest) {
    auto segment_des = std::make_shared<TransferMetadata::SegmentDesc>();
    segment_des->name = "test_segment";
    segment_des->addProtocol("rdma");
    segment_des->tcp_data_port = 12345;
    TransferMetadata::SegmentID segment_id = 1111111;
    std::string segment_name = "test_segment";
    int re = metadata_client->addLocalSegment(segment_id, segment_name,
                                              std::move(segment_des));
    ASSERT_EQ(re, 0);
    auto des = metadata_client->getSegmentDescByName(segment_name);
    ASSERT_EQ(des, segment_des);
    des = metadata_client->getSegmentDescByID(segment_id, false);
    ASSERT_EQ(des, segment_des);
    auto id = metadata_client->getSegmentID(segment_name);
    ASSERT_EQ(id, segment_id);
    re = metadata_client->removeLocalSegment(segment_name);
    ASSERT_EQ(re, 0);
}

// Test multiple protocols support
TEST_F(TransferMetadataTest, MultipleProtocolsTest) {
    auto segment_des = std::make_shared<TransferMetadata::SegmentDesc>();
    segment_des->name = "test_multi_protocol";
    segment_des->addProtocol("rdma");
    segment_des->addProtocol("tcp");
    segment_des->addProtocol("nvlink");
    segment_des->tcp_data_port = 12346;

    // Verify protocols were added correctly
    ASSERT_TRUE(segment_des->hasProtocol("rdma"));
    ASSERT_TRUE(segment_des->hasProtocol("tcp"));
    ASSERT_TRUE(segment_des->hasProtocol("nvlink"));
    ASSERT_FALSE(segment_des->hasProtocol("invalid"));

    // Verify first protocol is rdma
    ASSERT_EQ(segment_des->getFirstProtocol(), "rdma");

    // Test remove protocol
    segment_des->removeProtocol("tcp");
    ASSERT_FALSE(segment_des->hasProtocol("tcp"));
    ASSERT_TRUE(segment_des->hasProtocol("rdma"));
    ASSERT_TRUE(segment_des->hasProtocol("nvlink"));

    TransferMetadata::SegmentID segment_id = 2222222;
    std::string segment_name = "test_multi_protocol";
    int re = metadata_client->addLocalSegment(segment_id, segment_name,
                                              std::move(segment_des));
    ASSERT_EQ(re, 0);

    // Verify the segment can be retrieved
    auto des = metadata_client->getSegmentDescByName(segment_name);
    ASSERT_NE(des, nullptr);
    ASSERT_EQ(des->getFirstProtocol(), "rdma");
    ASSERT_TRUE(des->hasProtocol("rdma"));
    ASSERT_TRUE(des->hasProtocol("nvlink"));
    ASSERT_FALSE(des->hasProtocol("tcp"));

    re = metadata_client->removeLocalSegment(segment_name);
    ASSERT_EQ(re, 0);
}

// add and remove LocalMemoryBufferMeta
TEST_F(TransferMetadataTest, LocalMemoryBufferTest) {
    auto segment_des = std::make_shared<TransferMetadata::SegmentDesc>();
    segment_des->name = "test_localMemory";
    segment_des->addProtocol("rdma");
    int re = metadata_client->addLocalSegment(
        LOCAL_SEGMENT_ID, "test_local_segment", std::move(segment_des));
    ASSERT_EQ(re, 0);
    uint64_t addr = 0;
    for (int i = 0; i < 10; ++i) {
        TransferMetadata::BufferDesc buffer_des;
        buffer_des.addr = addr + i * 2048;
        buffer_des.length = 1024;
        re = metadata_client->addLocalMemoryBuffer(buffer_des, false);
        ASSERT_EQ(re, 0);
    }
    addr = 1000;
    re = metadata_client->removeLocalMemoryBuffer((void*)addr, false);
    ASSERT_EQ(re, ERR_ADDRESS_NOT_REGISTERED);
    for (int i = 9; i > 0; --i) {
        addr = i * 2048;
        re = metadata_client->removeLocalMemoryBuffer((void*)addr, false);
        ASSERT_EQ(re, 0);
    }
    re = metadata_client->removeLocalSegment("test_local_segment");
    ASSERT_EQ(re, 0);
}

// add, get and remove RPCMetaEntryMeta
TEST_F(TransferMetadataTest, RpcMetaEntryTest) {
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

// Test that multiple transports can share the same segment
TEST_F(TransferMetadataTest, MultipleTransportSegmentTest) {
    // Create a segment with rdma protocol first
    auto segment_des = std::make_shared<TransferMetadata::SegmentDesc>();
    segment_des->name = "test_shared_segment";
    segment_des->addProtocol("rdma");
    segment_des->tcp_data_port = 12347;

    TransferMetadata::SegmentID segment_id = 3333333;
    std::string segment_name = "test_shared_segment";
    int re = metadata_client->addLocalSegment(segment_id, segment_name,
                                              std::move(segment_des));
    ASSERT_EQ(re, 0);

    // Verify segment exists
    ASSERT_TRUE(metadata_client->hasLocalSegment(segment_name));
    auto existing_desc = metadata_client->getLocalSegmentDesc(segment_name);
    ASSERT_NE(existing_desc, nullptr);
    ASSERT_TRUE(existing_desc->hasProtocol("rdma"));
    ASSERT_FALSE(existing_desc->hasProtocol("tcp"));
    ASSERT_FALSE(existing_desc->hasProtocol("nvlink"));

    // Simulate adding tcp protocol to existing segment
    existing_desc->addProtocol("tcp");
    existing_desc->tcp_data_port = 12348;
    ASSERT_TRUE(existing_desc->hasProtocol("rdma"));
    ASSERT_TRUE(existing_desc->hasProtocol("tcp"));
    ASSERT_FALSE(existing_desc->hasProtocol("nvlink"));

    // Simulate adding nvlink protocol to existing segment
    existing_desc->addProtocol("nvlink");
    ASSERT_TRUE(existing_desc->hasProtocol("rdma"));
    ASSERT_TRUE(existing_desc->hasProtocol("tcp"));
    ASSERT_TRUE(existing_desc->hasProtocol("nvlink"));

    // Verify the segment still has all protocols
    auto retrieved_desc = metadata_client->getLocalSegmentDesc(segment_name);
    ASSERT_NE(retrieved_desc, nullptr);
    ASSERT_TRUE(retrieved_desc->hasProtocol("rdma"));
    ASSERT_TRUE(retrieved_desc->hasProtocol("tcp"));
    ASSERT_TRUE(retrieved_desc->hasProtocol("nvlink"));

    re = metadata_client->removeLocalSegment(segment_name);
    ASSERT_EQ(re, 0);
}

// Test protocol encoding/decoding consistency
TEST_F(TransferMetadataTest, ProtocolEncodingDecodingTest) {
    // Create a segment with multiple protocols
    auto original_desc = std::make_shared<TransferMetadata::SegmentDesc>();
    original_desc->name = "test_encoding_decoding";
    original_desc->addProtocol("rdma");
    original_desc->addProtocol("tcp");
    original_desc->addProtocol("nvlink");
    original_desc->tcp_data_port = 12349;

    // Add some buffer data
    TransferMetadata::BufferDesc buffer;
    buffer.name = "test_buffer";
    buffer.addr = 0x1000;
    buffer.length = 1024;
    buffer.rkey.push_back(123);
    buffer.lkey.push_back(456);
    buffer.shm_name = "test_shm";
    original_desc->buffers.push_back(buffer);

    // Add the segment to local storage
    TransferMetadata::SegmentID segment_id = 4444444;
    std::string segment_name = "test_encoding_decoding";
    int re = metadata_client->addLocalSegment(segment_id, segment_name,
                                              std::move(original_desc));
    ASSERT_EQ(re, 0);

    // Update the segment descriptor (this will trigger encoding)
    auto local_desc = metadata_client->getLocalSegmentDesc(segment_name);
    ASSERT_NE(local_desc, nullptr);
    re = metadata_client->updateSegmentDesc(segment_name, *local_desc);
    ASSERT_EQ(re, 0);

    // Retrieve the segment descriptor (this will trigger decoding)
    auto retrieved_desc = metadata_client->getSegmentDescByName(segment_name);
    ASSERT_NE(retrieved_desc, nullptr);

    // Verify all protocols are correctly preserved
    ASSERT_TRUE(retrieved_desc->hasProtocol("rdma"));
    ASSERT_TRUE(retrieved_desc->hasProtocol("tcp"));
    ASSERT_TRUE(retrieved_desc->hasProtocol("nvlink"));
    ASSERT_EQ(retrieved_desc->protocols.size(), 3);

    // Verify buffer data is correctly preserved
    ASSERT_EQ(retrieved_desc->buffers.size(), 1);
    ASSERT_EQ(retrieved_desc->buffers[0].name, "test_buffer");
    ASSERT_EQ(retrieved_desc->buffers[0].addr, 0x1000);
    ASSERT_EQ(retrieved_desc->buffers[0].length, 1024);
    ASSERT_EQ(retrieved_desc->buffers[0].rkey.size(), 1);
    ASSERT_EQ(retrieved_desc->buffers[0].lkey.size(), 1);
    ASSERT_EQ(retrieved_desc->buffers[0].rkey[0], 123);
    ASSERT_EQ(retrieved_desc->buffers[0].lkey[0], 456);
    ASSERT_EQ(retrieved_desc->buffers[0].shm_name, "test_shm");

    // Clean up
    re = metadata_client->removeLocalSegment(segment_name);
    ASSERT_EQ(re, 0);
}

// Test early protocol validation
TEST_F(TransferMetadataTest, EarlyProtocolValidationTest) {
    // Test case 1: No protocols - should fail early
    auto no_protocol_desc = std::make_shared<TransferMetadata::SegmentDesc>();
    no_protocol_desc->name = "test_no_protocol";
    no_protocol_desc->tcp_data_port = 12350;

    TransferMetadata::SegmentID segment_id = 5555555;
    std::string segment_name = "test_no_protocol";
    int re = metadata_client->addLocalSegment(segment_id, segment_name,
                                              std::move(no_protocol_desc));
    ASSERT_EQ(re, 0);

    // Try to update segment with no protocols - should fail
    auto local_desc = metadata_client->getLocalSegmentDesc(segment_name);
    ASSERT_NE(local_desc, nullptr);
    re = metadata_client->updateSegmentDesc(segment_name, *local_desc);
    ASSERT_NE(re, 0);  // Should fail due to no supported protocols

    // Clean up
    re = metadata_client->removeLocalSegment(segment_name);
    ASSERT_EQ(re, 0);

    // Test case 2: Single valid protocol - should succeed
    auto single_protocol_desc =
        std::make_shared<TransferMetadata::SegmentDesc>();
    single_protocol_desc->name = "test_single_protocol";
    single_protocol_desc->addProtocol("rdma");
    single_protocol_desc->tcp_data_port = 12351;

    segment_id = 6666666;
    segment_name = "test_single_protocol";
    re = metadata_client->addLocalSegment(segment_id, segment_name,
                                          std::move(single_protocol_desc));
    ASSERT_EQ(re, 0);

    // Update segment with single protocol - should succeed
    local_desc = metadata_client->getLocalSegmentDesc(segment_name);
    ASSERT_NE(local_desc, nullptr);
    re = metadata_client->updateSegmentDesc(segment_name, *local_desc);
    ASSERT_EQ(re, 0);  // Should succeed

    // Clean up
    re = metadata_client->removeLocalSegment(segment_name);
    ASSERT_EQ(re, 0);

    // Test case 3: Multiple valid protocols - should succeed
    auto multi_protocol_desc =
        std::make_shared<TransferMetadata::SegmentDesc>();
    multi_protocol_desc->name = "test_multi_protocol";
    multi_protocol_desc->addProtocol("rdma");
    multi_protocol_desc->addProtocol("tcp");
    multi_protocol_desc->addProtocol("nvlink");
    multi_protocol_desc->tcp_data_port = 12352;

    segment_id = 7777777;
    segment_name = "test_multi_protocol";
    re = metadata_client->addLocalSegment(segment_id, segment_name,
                                          std::move(multi_protocol_desc));
    ASSERT_EQ(re, 0);

    // Update segment with multiple protocols - should succeed
    local_desc = metadata_client->getLocalSegmentDesc(segment_name);
    ASSERT_NE(local_desc, nullptr);
    re = metadata_client->updateSegmentDesc(segment_name, *local_desc);
    ASSERT_EQ(re, 0);  // Should succeed

    // Clean up
    re = metadata_client->removeLocalSegment(segment_name);
    ASSERT_EQ(re, 0);
}

// Test unified buffer processing
TEST_F(TransferMetadataTest, UnifiedBufferProcessingTest) {
    // Create a segment with multiple protocols that have different buffer
    // requirements
    auto segment_desc = std::make_shared<TransferMetadata::SegmentDesc>();
    segment_desc->name = "test_unified_buffer";
    segment_desc->addProtocol("rdma");
    segment_desc->addProtocol("nvlink");
    segment_desc->addProtocol("cxl");
    segment_desc->tcp_data_port = 12353;

    // Add buffers with different protocol-specific fields
    TransferMetadata::BufferDesc rdma_buffer;
    rdma_buffer.name = "rdma_buffer";
    rdma_buffer.addr = 0x2000;
    rdma_buffer.length = 2048;
    rdma_buffer.rkey.push_back(789);
    rdma_buffer.lkey.push_back(101);
    segment_desc->buffers.push_back(rdma_buffer);

    TransferMetadata::BufferDesc nvlink_buffer;
    nvlink_buffer.name = "nvlink_buffer";
    nvlink_buffer.addr = 0x3000;
    nvlink_buffer.length = 4096;
    nvlink_buffer.shm_name = "nvlink_shm";
    segment_desc->buffers.push_back(nvlink_buffer);

    TransferMetadata::BufferDesc cxl_buffer;
    cxl_buffer.name = "cxl_buffer";
    cxl_buffer.addr = 0x4000;
    cxl_buffer.length = 8192;
    cxl_buffer.offset = 0x1000;
    segment_desc->buffers.push_back(cxl_buffer);

    // Add the segment to local storage
    TransferMetadata::SegmentID segment_id = 8888888;
    std::string segment_name = "test_unified_buffer";
    int re = metadata_client->addLocalSegment(segment_id, segment_name,
                                              std::move(segment_desc));
    ASSERT_EQ(re, 0);

    // Update the segment descriptor (this will trigger encoding)
    auto local_desc = metadata_client->getLocalSegmentDesc(segment_name);
    ASSERT_NE(local_desc, nullptr);
    re = metadata_client->updateSegmentDesc(segment_name, *local_desc);
    ASSERT_EQ(re, 0);

    // Retrieve the segment descriptor (this will trigger decoding)
    auto retrieved_desc = metadata_client->getSegmentDescByName(segment_name);
    ASSERT_NE(retrieved_desc, nullptr);

    // Verify all protocols are correctly preserved
    ASSERT_TRUE(retrieved_desc->hasProtocol("rdma"));
    ASSERT_TRUE(retrieved_desc->hasProtocol("nvlink"));
    ASSERT_TRUE(retrieved_desc->hasProtocol("cxl"));
    ASSERT_EQ(retrieved_desc->protocols.size(), 3);

    // Verify all buffers are correctly processed in a single traversal
    ASSERT_EQ(retrieved_desc->buffers.size(), 3);

    // Verify RDMA buffer
    ASSERT_EQ(retrieved_desc->buffers[0].name, "rdma_buffer");
    ASSERT_EQ(retrieved_desc->buffers[0].addr, 0x2000);
    ASSERT_EQ(retrieved_desc->buffers[0].length, 2048);
    ASSERT_EQ(retrieved_desc->buffers[0].rkey.size(), 1);
    ASSERT_EQ(retrieved_desc->buffers[0].lkey.size(), 1);
    ASSERT_EQ(retrieved_desc->buffers[0].rkey[0], 789);
    ASSERT_EQ(retrieved_desc->buffers[0].lkey[0], 101);

    // Verify NVLink buffer
    ASSERT_EQ(retrieved_desc->buffers[1].name, "nvlink_buffer");
    ASSERT_EQ(retrieved_desc->buffers[1].addr, 0x3000);
    ASSERT_EQ(retrieved_desc->buffers[1].length, 4096);
    ASSERT_EQ(retrieved_desc->buffers[1].shm_name, "nvlink_shm");

    // Verify CXL buffer
    ASSERT_EQ(retrieved_desc->buffers[2].name, "cxl_buffer");
    ASSERT_EQ(retrieved_desc->buffers[2].addr, 0x4000);
    ASSERT_EQ(retrieved_desc->buffers[2].length, 8192);
    ASSERT_EQ(retrieved_desc->buffers[2].offset, 0x1000);

    // Clean up
    re = metadata_client->removeLocalSegment(segment_name);
    ASSERT_EQ(re, 0);
}

// Test error handling for corrupted segment descriptors
TEST_F(TransferMetadataTest, ErrorHandlingTest) {
    // Test case 1: Segment with no protocols - should fail during update
    auto no_protocol_desc = std::make_shared<TransferMetadata::SegmentDesc>();
    no_protocol_desc->name = "test_no_protocol_error";
    no_protocol_desc->tcp_data_port = 12354;
    // Note: no protocols added

    TransferMetadata::SegmentID segment_id = 9999999;
    std::string segment_name = "test_no_protocol_error";
    int re = metadata_client->addLocalSegment(segment_id, segment_name,
                                              std::move(no_protocol_desc));
    ASSERT_EQ(re, 0);

    // Try to update segment with no protocols - should fail
    auto local_desc = metadata_client->getLocalSegmentDesc(segment_name);
    ASSERT_NE(local_desc, nullptr);
    re = metadata_client->updateSegmentDesc(segment_name, *local_desc);
    ASSERT_NE(re, 0);  // Should fail due to no supported protocols

    // Clean up
    re = metadata_client->removeLocalSegment(segment_name);
    ASSERT_EQ(re, 0);

    // Test case 2: Segment with invalid protocol - should fail during update
    auto invalid_protocol_desc =
        std::make_shared<TransferMetadata::SegmentDesc>();
    invalid_protocol_desc->name = "test_invalid_protocol_error";
    invalid_protocol_desc->addProtocol("invalid_protocol");
    invalid_protocol_desc->tcp_data_port = 12355;

    segment_id = 10101010;
    segment_name = "test_invalid_protocol_error";
    re = metadata_client->addLocalSegment(segment_id, segment_name,
                                          std::move(invalid_protocol_desc));
    ASSERT_EQ(re, 0);

    // Try to update segment with invalid protocol - should fail
    local_desc = metadata_client->getLocalSegmentDesc(segment_name);
    ASSERT_NE(local_desc, nullptr);
    re = metadata_client->updateSegmentDesc(segment_name, *local_desc);
    ASSERT_NE(re, 0);  // Should fail due to unsupported protocol

    // Clean up
    re = metadata_client->removeLocalSegment(segment_name);
    ASSERT_EQ(re, 0);

    // Test case 3: Valid segment for comparison
    auto valid_desc = std::make_shared<TransferMetadata::SegmentDesc>();
    valid_desc->name = "test_valid_error";
    valid_desc->addProtocol("rdma");
    valid_desc->tcp_data_port = 12356;

    // Add valid RDMA buffer
    TransferMetadata::BufferDesc valid_buffer;
    valid_buffer.name = "valid_buffer";
    valid_buffer.addr = 0x7000;
    valid_buffer.length = 1024;
    valid_buffer.rkey.push_back(123);
    valid_buffer.lkey.push_back(456);
    valid_desc->buffers.push_back(valid_buffer);

    segment_id = 11111111;
    segment_name = "test_valid_error";
    re = metadata_client->addLocalSegment(segment_id, segment_name,
                                          std::move(valid_desc));
    ASSERT_EQ(re, 0);

    // Update valid segment - should succeed
    local_desc = metadata_client->getLocalSegmentDesc(segment_name);
    ASSERT_NE(local_desc, nullptr);
    re = metadata_client->updateSegmentDesc(segment_name, *local_desc);
    ASSERT_EQ(re, 0);  // Should succeed

    // Retrieve and verify valid segment
    auto retrieved_desc = metadata_client->getSegmentDescByName(segment_name);
    ASSERT_NE(retrieved_desc, nullptr);
    ASSERT_TRUE(retrieved_desc->hasProtocol("rdma"));
    ASSERT_EQ(retrieved_desc->buffers.size(), 1);
    ASSERT_EQ(retrieved_desc->buffers[0].name, "valid_buffer");
    ASSERT_EQ(retrieved_desc->buffers[0].addr, 0x7000);
    ASSERT_EQ(retrieved_desc->buffers[0].length, 1024);
    ASSERT_EQ(retrieved_desc->buffers[0].rkey.size(), 1);
    ASSERT_EQ(retrieved_desc->buffers[0].lkey.size(), 1);
    ASSERT_EQ(retrieved_desc->buffers[0].rkey[0], 123);
    ASSERT_EQ(retrieved_desc->buffers[0].lkey[0], 456);

    // Clean up
    re = metadata_client->removeLocalSegment(segment_name);
    ASSERT_EQ(re, 0);
}

// Test various protocol combinations
TEST_F(TransferMetadataTest, MultiProtocolCombinationTest) {
    // Test case 1: RDMA + TCP combination
    auto rdma_tcp_desc = std::make_shared<TransferMetadata::SegmentDesc>();
    rdma_tcp_desc->name = "test_rdma_tcp";
    rdma_tcp_desc->addProtocol("rdma");
    rdma_tcp_desc->addProtocol("tcp");
    rdma_tcp_desc->tcp_data_port = 12360;

    // Add RDMA-specific device
    TransferMetadata::DeviceDesc rdma_device;
    rdma_device.name = "rdma_device";
    rdma_device.lid = 1;
    rdma_device.gid = "fe80::1";
    rdma_tcp_desc->devices.push_back(rdma_device);

    // Add buffers for both protocols
    TransferMetadata::BufferDesc rdma_buffer;
    rdma_buffer.name = "rdma_buffer";
    rdma_buffer.addr = 0x8000;
    rdma_buffer.length = 1024;
    rdma_buffer.rkey.push_back(111);
    rdma_buffer.lkey.push_back(222);
    rdma_tcp_desc->buffers.push_back(rdma_buffer);

    TransferMetadata::BufferDesc tcp_buffer;
    tcp_buffer.name = "tcp_buffer";
    tcp_buffer.addr = 0x9000;
    tcp_buffer.length = 2048;
    rdma_tcp_desc->buffers.push_back(tcp_buffer);

    TransferMetadata::SegmentID segment_id = 12121212;
    std::string segment_name = "test_rdma_tcp";
    int re = metadata_client->addLocalSegment(segment_id, segment_name,
                                              std::move(rdma_tcp_desc));
    ASSERT_EQ(re, 0);

    // Update and retrieve
    auto local_desc = metadata_client->getLocalSegmentDesc(segment_name);
    ASSERT_NE(local_desc, nullptr);
    re = metadata_client->updateSegmentDesc(segment_name, *local_desc);
    ASSERT_EQ(re, 0);

    auto retrieved_desc = metadata_client->getSegmentDescByName(segment_name);
    ASSERT_NE(retrieved_desc, nullptr);

    // Verify RDMA + TCP combination
    ASSERT_TRUE(retrieved_desc->hasProtocol("rdma"));
    ASSERT_TRUE(retrieved_desc->hasProtocol("tcp"));
    ASSERT_EQ(retrieved_desc->protocols.size(), 2);
    ASSERT_EQ(retrieved_desc->devices.size(), 1);
    ASSERT_EQ(retrieved_desc->devices[0].name, "rdma_device");
    ASSERT_EQ(retrieved_desc->devices[0].lid, 1);
    ASSERT_EQ(retrieved_desc->devices[0].gid, "fe80::1");
    ASSERT_EQ(retrieved_desc->buffers.size(), 2);

    // Clean up
    re = metadata_client->removeLocalSegment(segment_name);
    ASSERT_EQ(re, 0);

    // Test case 2: Ascend + CXL combination
    auto ascend_cxl_desc = std::make_shared<TransferMetadata::SegmentDesc>();
    ascend_cxl_desc->name = "test_ascend_cxl";
    ascend_cxl_desc->addProtocol("ascend");
    ascend_cxl_desc->addProtocol("cxl");
    ascend_cxl_desc->tcp_data_port = 12361;

    // Add Ascend-specific rank info
    ascend_cxl_desc->rank_info.rankId = 12345;
    ascend_cxl_desc->rank_info.hostIp = "192.168.1.100";
    ascend_cxl_desc->rank_info.hostPort = 8080;
    ascend_cxl_desc->rank_info.deviceLogicId = 1;
    ascend_cxl_desc->rank_info.devicePhyId = 2;
    ascend_cxl_desc->rank_info.deviceType = 5;
    ascend_cxl_desc->rank_info.deviceIp = "192.168.1.101";
    ascend_cxl_desc->rank_info.devicePort = 9090;
    ascend_cxl_desc->rank_info.pid = 1234;

    // Add CXL-specific fields
    ascend_cxl_desc->cxl_name = "cxl_device";
    ascend_cxl_desc->cxl_base_addr = 0x10000;

    // Add Ascend device
    TransferMetadata::DeviceDesc ascend_device;
    ascend_device.name = "ascend_device";
    ascend_device.lid = 2;
    ascend_cxl_desc->devices.push_back(ascend_device);

    // Add buffers for both protocols
    TransferMetadata::BufferDesc ascend_buffer;
    ascend_buffer.name = "ascend_buffer";
    ascend_buffer.addr = 0xA000;
    ascend_buffer.length = 1024;
    ascend_cxl_desc->buffers.push_back(ascend_buffer);

    TransferMetadata::BufferDesc cxl_buffer;
    cxl_buffer.name = "cxl_buffer";
    cxl_buffer.addr = 0xB000;
    cxl_buffer.length = 2048;
    cxl_buffer.offset = 0x2000;
    ascend_cxl_desc->buffers.push_back(cxl_buffer);

    segment_id = 13131313;
    segment_name = "test_ascend_cxl";
    re = metadata_client->addLocalSegment(segment_id, segment_name,
                                          std::move(ascend_cxl_desc));
    ASSERT_EQ(re, 0);

    // Update and retrieve
    local_desc = metadata_client->getLocalSegmentDesc(segment_name);
    ASSERT_NE(local_desc, nullptr);
    re = metadata_client->updateSegmentDesc(segment_name, *local_desc);
    ASSERT_EQ(re, 0);

    retrieved_desc = metadata_client->getSegmentDescByName(segment_name);
    ASSERT_NE(retrieved_desc, nullptr);

    // Verify Ascend + CXL combination
    ASSERT_TRUE(retrieved_desc->hasProtocol("ascend"));
    ASSERT_TRUE(retrieved_desc->hasProtocol("cxl"));
    ASSERT_EQ(retrieved_desc->protocols.size(), 2);
    ASSERT_EQ(retrieved_desc->devices.size(), 1);
    ASSERT_EQ(retrieved_desc->devices[0].name, "ascend_device");
    ASSERT_EQ(retrieved_desc->devices[0].lid, 2);
    ASSERT_EQ(retrieved_desc->rank_info.rankId, 12345);
    ASSERT_EQ(retrieved_desc->rank_info.hostIp, "192.168.1.100");
    ASSERT_EQ(retrieved_desc->rank_info.hostPort, 8080);
    ASSERT_EQ(retrieved_desc->rank_info.deviceLogicId, 1);
    ASSERT_EQ(retrieved_desc->rank_info.devicePhyId, 2);
    ASSERT_EQ(retrieved_desc->rank_info.deviceType, 5);
    ASSERT_EQ(retrieved_desc->rank_info.deviceIp, "192.168.1.101");
    ASSERT_EQ(retrieved_desc->rank_info.devicePort, 9090);
    ASSERT_EQ(retrieved_desc->rank_info.pid, 1234);
    ASSERT_EQ(retrieved_desc->cxl_name, "cxl_device");
    ASSERT_EQ(retrieved_desc->cxl_base_addr, 0x10000);
    ASSERT_EQ(retrieved_desc->buffers.size(), 2);

    // Clean up
    re = metadata_client->removeLocalSegment(segment_name);
    ASSERT_EQ(re, 0);
}

// Test boundary conditions and edge cases
TEST_F(TransferMetadataTest, BoundaryConditionTest) {
    // Test case 1: Empty buffer list
    auto empty_buffers_desc = std::make_shared<TransferMetadata::SegmentDesc>();
    empty_buffers_desc->name = "test_empty_buffers";
    empty_buffers_desc->addProtocol("rdma");
    empty_buffers_desc->tcp_data_port = 12362;
    // Note: no buffers added

    TransferMetadata::SegmentID segment_id = 14141414;
    std::string segment_name = "test_empty_buffers";
    int re = metadata_client->addLocalSegment(segment_id, segment_name,
                                              std::move(empty_buffers_desc));
    ASSERT_EQ(re, 0);

    // Update and retrieve
    auto local_desc = metadata_client->getLocalSegmentDesc(segment_name);
    ASSERT_NE(local_desc, nullptr);
    re = metadata_client->updateSegmentDesc(segment_name, *local_desc);
    ASSERT_EQ(re, 0);

    auto retrieved_desc = metadata_client->getSegmentDescByName(segment_name);
    ASSERT_NE(retrieved_desc, nullptr);
    ASSERT_TRUE(retrieved_desc->hasProtocol("rdma"));
    ASSERT_EQ(retrieved_desc->buffers.size(), 0);  // Should have empty buffers

    // Clean up
    re = metadata_client->removeLocalSegment(segment_name);
    ASSERT_EQ(re, 0);

    // Test case 2: Empty device list
    auto empty_devices_desc = std::make_shared<TransferMetadata::SegmentDesc>();
    empty_devices_desc->name = "test_empty_devices";
    empty_devices_desc->addProtocol("rdma");
    empty_devices_desc->tcp_data_port = 12363;
    // Note: no devices added

    TransferMetadata::BufferDesc buffer;
    buffer.name = "test_buffer";
    buffer.addr = 0xC000;
    buffer.length = 1024;
    buffer.rkey.push_back(333);
    buffer.lkey.push_back(444);
    empty_devices_desc->buffers.push_back(buffer);

    segment_id = 15151515;
    segment_name = "test_empty_devices";
    re = metadata_client->addLocalSegment(segment_id, segment_name,
                                          std::move(empty_devices_desc));
    ASSERT_EQ(re, 0);

    // Update and retrieve
    local_desc = metadata_client->getLocalSegmentDesc(segment_name);
    ASSERT_NE(local_desc, nullptr);
    re = metadata_client->updateSegmentDesc(segment_name, *local_desc);
    ASSERT_EQ(re, 0);

    retrieved_desc = metadata_client->getSegmentDescByName(segment_name);
    ASSERT_NE(retrieved_desc, nullptr);
    ASSERT_TRUE(retrieved_desc->hasProtocol("rdma"));
    ASSERT_EQ(retrieved_desc->devices.size(), 0);  // Should have empty devices
    ASSERT_EQ(retrieved_desc->buffers.size(), 1);

    // Clean up
    re = metadata_client->removeLocalSegment(segment_name);
    ASSERT_EQ(re, 0);

    // Test case 3: Maximum values
    auto max_values_desc = std::make_shared<TransferMetadata::SegmentDesc>();
    max_values_desc->name = "test_max_values";
    max_values_desc->addProtocol("rdma");
    max_values_desc->addProtocol("cxl");
    max_values_desc->tcp_data_port = 65535;  // Max port number

    TransferMetadata::BufferDesc max_buffer;
    max_buffer.name = "max_buffer";
    max_buffer.addr = 0xFFFFFFFFFFFFFFFF;    // Max address
    max_buffer.length = 0xFFFFFFFFFFFFFFFF;  // Max length
    max_buffer.rkey.push_back(0xFFFFFFFF);   // Max rkey
    max_buffer.lkey.push_back(0xFFFFFFFF);   // Max lkey
    max_values_desc->buffers.push_back(max_buffer);

    TransferMetadata::BufferDesc cxl_max_buffer;
    cxl_max_buffer.name = "cxl_max_buffer";
    cxl_max_buffer.addr = 0xFFFFFFFFFFFFFFFF;    // Max address
    cxl_max_buffer.length = 0xFFFFFFFFFFFFFFFF;  // Max length
    cxl_max_buffer.offset = 0xFFFFFFFFFFFFFFFF;  // Max offset
    max_values_desc->buffers.push_back(cxl_max_buffer);

    // Set maximum rank info values
    max_values_desc->rank_info.rankId = 0xFFFFFFFFFFFFFFFF;
    max_values_desc->rank_info.hostPort = 0xFFFFFFFFFFFFFFFF;
    max_values_desc->rank_info.deviceLogicId = 0xFFFFFFFFFFFFFFFF;
    max_values_desc->rank_info.devicePhyId = 0xFFFFFFFFFFFFFFFF;
    max_values_desc->rank_info.deviceType = 0xFFFFFFFFFFFFFFFF;
    max_values_desc->rank_info.devicePort = 0xFFFFFFFFFFFFFFFF;
    max_values_desc->rank_info.pid = 0xFFFFFFFFFFFFFFFF;

    segment_id = 16161616;
    segment_name = "test_max_values";
    re = metadata_client->addLocalSegment(segment_id, segment_name,
                                          std::move(max_values_desc));
    ASSERT_EQ(re, 0);

    // Update and retrieve
    local_desc = metadata_client->getLocalSegmentDesc(segment_name);
    ASSERT_NE(local_desc, nullptr);
    re = metadata_client->updateSegmentDesc(segment_name, *local_desc);
    ASSERT_EQ(re, 0);

    retrieved_desc = metadata_client->getSegmentDescByName(segment_name);
    ASSERT_NE(retrieved_desc, nullptr);
    ASSERT_TRUE(retrieved_desc->hasProtocol("rdma"));
    ASSERT_TRUE(retrieved_desc->hasProtocol("cxl"));
    ASSERT_EQ(retrieved_desc->tcp_data_port, 65535);
    ASSERT_EQ(retrieved_desc->buffers.size(), 2);
    ASSERT_EQ(retrieved_desc->buffers[0].addr, 0xFFFFFFFFFFFFFFFF);
    ASSERT_EQ(retrieved_desc->buffers[0].length, 0xFFFFFFFFFFFFFFFF);
    ASSERT_EQ(retrieved_desc->buffers[0].rkey[0], 0xFFFFFFFF);
    ASSERT_EQ(retrieved_desc->buffers[0].lkey[0], 0xFFFFFFFF);
    ASSERT_EQ(retrieved_desc->buffers[1].addr, 0xFFFFFFFFFFFFFFFF);
    ASSERT_EQ(retrieved_desc->buffers[1].length, 0xFFFFFFFFFFFFFFFF);
    ASSERT_EQ(retrieved_desc->buffers[1].offset, 0xFFFFFFFFFFFFFFFF);
    ASSERT_EQ(retrieved_desc->rank_info.rankId, 0xFFFFFFFFFFFFFFFF);
    ASSERT_EQ(retrieved_desc->rank_info.hostPort, 0xFFFFFFFFFFFFFFFF);
    ASSERT_EQ(retrieved_desc->rank_info.deviceLogicId, 0xFFFFFFFFFFFFFFFF);
    ASSERT_EQ(retrieved_desc->rank_info.devicePhyId, 0xFFFFFFFFFFFFFFFF);
    ASSERT_EQ(retrieved_desc->rank_info.deviceType, 0xFFFFFFFFFFFFFFFF);
    ASSERT_EQ(retrieved_desc->rank_info.devicePort, 0xFFFFFFFFFFFFFFFF);
    ASSERT_EQ(retrieved_desc->rank_info.pid, 0xFFFFFFFFFFFFFFFF);

    // Clean up
    re = metadata_client->removeLocalSegment(segment_name);
    ASSERT_EQ(re, 0);
}

}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
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
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <thread>

#include "config.h"
#include "transport/transport.h"
#include "transfer_metadata_plugin.h"

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
    des = metadata_client->getSegmentDescByID(segment_id, false);
    ASSERT_EQ(des, segment_des);
    auto id = metadata_client->getSegmentID(segment_name);
    ASSERT_EQ(id, segment_id);
    re = metadata_client->removeLocalSegment(segment_name);
    ASSERT_EQ(re, 0);
}

// add and remove LocalMemoryBufferMeta
TEST_F(TransferMetadataTest, LocalMemoryBufferTest) {
    auto segment_des = std::make_shared<TransferMetadata::SegmentDesc>();
    segment_des->name = "test_localMemory";
    segment_des->protocol = "rdma";
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

namespace {

struct ScopedMetadataRefreshConfig {
    uint64_t old_interval_seconds;
    bool old_metacache;

    ScopedMetadataRefreshConfig(uint64_t interval_seconds, bool metacache)
        : old_interval_seconds(
              globalConfig().te_metadata_refresh_interval_seconds),
          old_metacache(globalConfig().metacache) {
        globalConfig().te_metadata_refresh_interval_seconds = interval_seconds;
        globalConfig().metacache = metacache;
    }

    ~ScopedMetadataRefreshConfig() {
        globalConfig().te_metadata_refresh_interval_seconds =
            old_interval_seconds;
        globalConfig().metacache = old_metacache;
    }
};

TransferMetadata::BufferDesc makeRdmaBufferDesc(uint64_t addr) {
    TransferMetadata::BufferDesc buffer_desc;
    buffer_desc.name = "buffer";
    buffer_desc.addr = addr;
    buffer_desc.length = 1024;
    buffer_desc.lkey.push_back(1);
    buffer_desc.rkey.push_back(2);
    return buffer_desc;
}

std::shared_ptr<TransferMetadata::SegmentDesc> makeRdmaSegmentDesc(
    const std::string& name, uint64_t addr) {
    auto segment_desc = std::make_shared<TransferMetadata::SegmentDesc>();
    segment_desc->name = name;
    segment_desc->protocol = "rdma";
    segment_desc->tcp_data_port = 0;

    TransferMetadata::DeviceDesc device_desc;
    device_desc.name = "mlx5_0";
    device_desc.lid = 1;
    device_desc.gid = "00000000000000000000ffff7f000001";
    segment_desc->devices.push_back(device_desc);

    segment_desc->buffers.push_back(makeRdmaBufferDesc(addr));
    return segment_desc;
}

}  // namespace

TEST(TransferMetadataPollingTest, PollingRefreshesCachedRemoteSegmentDesc) {
    constexpr uint64_t kInitialAddr = 0x1000;
    constexpr uint64_t kUpdatedAddr = 0x2000;

    ScopedMetadataRefreshConfig restore(1, true);
    TransferMetadata server(P2PHANDSHAKE);
    TransferMetadata client(P2PHANDSHAKE);

    int sockfd = -1;
    const uint16_t port = findAvailableTcpPort(sockfd);
    ASSERT_GT(port, 0);
    const std::string remote_segment_name = "127.0.0.1:" + std::to_string(port);

    ASSERT_EQ(server.addLocalSegment(
                  LOCAL_SEGMENT_ID, remote_segment_name,
                  makeRdmaSegmentDesc(remote_segment_name, kInitialAddr)),
              0);
    TransferMetadata::RpcMetaDesc rpc_desc;
    rpc_desc.ip_or_host_name = "127.0.0.1";
    rpc_desc.rpc_port = port;
    rpc_desc.sockfd = sockfd;
    ASSERT_EQ(server.addRpcMetaEntry(remote_segment_name, rpc_desc), 0);

    ASSERT_EQ(
        client.addLocalSegment(LOCAL_SEGMENT_ID, "127.0.0.1:0",
                               makeRdmaSegmentDesc("127.0.0.1:0", 0x3000)),
        0);

    const auto segment_id = client.getSegmentID(remote_segment_name);
    ASSERT_NE(segment_id, static_cast<TransferMetadata::SegmentID>(-1));
    auto cached_desc = client.getSegmentDescByID(segment_id);
    ASSERT_TRUE(cached_desc);
    ASSERT_EQ(cached_desc->buffers[0].addr, kInitialAddr);

    ASSERT_EQ(server.removeLocalMemoryBuffer(
                  reinterpret_cast<void*>(kInitialAddr), false),
              0);
    ASSERT_EQ(
        server.addLocalMemoryBuffer(makeRdmaBufferDesc(kUpdatedAddr), false),
        0);

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (std::chrono::steady_clock::now() < deadline) {
        cached_desc = client.getSegmentDescByID(segment_id);
        ASSERT_TRUE(cached_desc);
        if (!cached_desc->buffers.empty() &&
            cached_desc->buffers[0].addr == kUpdatedAddr) {
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }

    FAIL() << "TE metadata refresh polling did not refresh cached descriptor";
}

TEST(HandshakeFrameTest, ValidFrameRoundTrips) {
    int fds[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);

    ASSERT_EQ(writeString(fds[0], HandShakeRequestType::Metadata,
                          "{\"name\":\"segment\"}"),
              0);
    auto [type, payload] = readString(fds[1]);
    EXPECT_EQ(type, HandShakeRequestType::Metadata);
    EXPECT_EQ(payload, "{\"name\":\"segment\"}");

    close(fds[0]);
    close(fds[1]);
}

TEST(HandshakeFrameTest, ValidTypedFrameWithTlsLikeNativeEndianLength) {
    int fds[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);

    // 790 is encoded as 0x16 0x03 0x00 ... on little-endian machines, which
    // collides with the first two bytes of a TLS ClientHello record. It is
    // still a valid native-endian handshake frame length.
    const std::string payload(789, 'x');
    ASSERT_EQ(writeString(fds[0], HandShakeRequestType::Metadata, payload), 0);

    auto [type, read_payload] = readString(fds[1]);
    EXPECT_EQ(type, HandShakeRequestType::Metadata);
    EXPECT_EQ(read_payload, payload);

    close(fds[0]);
    close(fds[1]);
}

TEST(HandshakeFrameTest, OldProtocolFrameStillWorks) {
    int fds[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);

    const std::string old_payload = "{\"name\":\"segment\"}";
    uint64_t old_length = old_payload.size();
    ASSERT_EQ(writeFully(fds[0], &old_length, sizeof(old_length)),
              static_cast<ssize_t>(sizeof(old_length)));
    ASSERT_EQ(writeFully(fds[0], old_payload.data(), old_payload.size()),
              static_cast<ssize_t>(old_payload.size()));

    auto [type, payload] = readString(fds[1]);
    EXPECT_EQ(type, HandShakeRequestType::OldProtocol);
    EXPECT_EQ(payload, old_payload);

    close(fds[0]);
    close(fds[1]);
}

TEST(HandshakeFrameTest, RejectsHttpProbe) {
    int fds[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);

    const std::string request = "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n";
    ASSERT_EQ(writeFully(fds[0], request.data(), request.size()),
              static_cast<ssize_t>(request.size()));

    auto [type, payload] = readString(fds[1]);
    EXPECT_EQ(type, HandShakeRequestType::Invalid);
    EXPECT_TRUE(payload.empty());

    close(fds[0]);
    close(fds[1]);
}

TEST(HandshakeFrameTest, RejectsTlsProbe) {
    int fds[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);

    const uint8_t client_hello_prefix[] = {0x16, 0x03, 0x01, 0x05,
                                           0xc2, 0x01, 0x00, 0x05};

    ASSERT_EQ(
        writeFully(fds[0], client_hello_prefix, sizeof(client_hello_prefix)),
        static_cast<ssize_t>(sizeof(client_hello_prefix)));

    auto [read_type, payload] = readString(fds[1]);
    EXPECT_EQ(read_type, HandShakeRequestType::Invalid);
    EXPECT_TRUE(payload.empty());

    close(fds[0]);
    close(fds[1]);
}

TEST(HandshakeFrameTest, RejectsInvalidLength) {
    int fds[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);

    const uint64_t oversized_length = kMaxHandshakeMaxLength + 1;
    ASSERT_EQ(writeFully(fds[0], &oversized_length, sizeof(oversized_length)),
              static_cast<ssize_t>(sizeof(oversized_length)));

    auto [oversized_type, oversized_payload] = readString(fds[1]);
    EXPECT_EQ(oversized_type, HandShakeRequestType::Invalid);
    EXPECT_TRUE(oversized_payload.empty());

    close(fds[0]);
    close(fds[1]);

    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);

    const uint64_t zero_length = 0;
    ASSERT_EQ(writeFully(fds[0], &zero_length, sizeof(zero_length)),
              static_cast<ssize_t>(sizeof(zero_length)));

    auto [zero_type, zero_payload] = readString(fds[1]);
    EXPECT_EQ(zero_type, HandShakeRequestType::Invalid);
    EXPECT_TRUE(zero_payload.empty());

    close(fds[0]);
    close(fds[1]);
}

}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

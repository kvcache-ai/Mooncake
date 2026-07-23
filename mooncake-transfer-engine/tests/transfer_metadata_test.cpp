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

#include <chrono>
#include <cstdlib>
#include <map>
#include <memory>
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

        const char *env = std::getenv("MC_METADATA_SERVER");
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
    re = metadata_client->removeLocalMemoryBuffer((void *)addr, false);
    ASSERT_EQ(re, ERR_ADDRESS_NOT_REGISTERED);
    for (int i = 9; i > 0; --i) {
        addr = i * 2048;
        re = metadata_client->removeLocalMemoryBuffer((void *)addr, false);
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
    const std::string &name, uint64_t addr) {
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
                  reinterpret_cast<void *>(kInitialAddr), false),
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

namespace {

// Hardware-free in-memory MetadataStoragePlugin. get() returns false for keys
// absent from the store (a removed/unmounted segment) and can be made to fail
// a bounded number of times via failNext() to reproduce a transient backend
// blip. No RDMA/CUDA/etcd is required.
class FakeMetadataStoragePlugin : public MetadataStoragePlugin {
   public:
    bool get(const std::string &key, Json::Value &value) override {
        auto fit = forced_failures_.find(key);
        if (fit != forced_failures_.end() && fit->second > 0) {
            --fit->second;
            return false;  // transient blip: the key may still be in store_
        }
        auto it = store_.find(key);
        if (it == store_.end()) return false;
        value = it->second;
        return true;
    }
    bool set(const std::string &key, const Json::Value &value) override {
        store_[key] = value;
        return true;
    }
    bool remove(const std::string &key) override {
        store_.erase(key);
        return true;
    }

    // Simulate the master-side cleanup of a peer's segment key (unmount or
    // client expiry): the key vanishes from the backend, so every later
    // get() returns false.
    void drop(const std::string &key) { store_.erase(key); }

    // Inject n transient get() failures for key without removing the key,
    // modelling a curl timeout / etcd blip that resolves next sync cycle.
    void failNext(const std::string &key, int n) { forced_failures_[key] = n; }

   private:
    std::map<std::string, Json::Value> store_;
    std::map<std::string, int> forced_failures_;
};

// Exposes the protected storage-plugin injection seam to hardware-free tests.
class TestableTransferMetadata : public TransferMetadata {
   public:
    explicit TestableTransferMetadata(
        std::shared_ptr<MetadataStoragePlugin> storage_plugin)
        : TransferMetadata(std::move(storage_plugin)) {}
};

}  // namespace

class TransferMetadataStaleSegmentInvalidationTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging(
            "TransferMetadataStaleSegmentInvalidationTest");
        FLAGS_logtostderr = 1;
    }
    void TearDown() override { google::ShutdownGoogleLogging(); }

    // Publish a remote rdma segment to the fake backend and cache it locally
    // (as a non-local id), returning the cached descriptor so callers can
    // assert on cache state.
    std::shared_ptr<TransferMetadata::SegmentDesc> seedRemoteSegment(
        TransferMetadata &client, const std::string &name) {
        auto desc = makeRdmaSegmentDesc(name, 0x1000);
        EXPECT_EQ(client.updateSegmentDesc(name, *desc), 0);
        EXPECT_NE(client.getSegmentID(name),
                  static_cast<TransferMetadata::SegmentID>(-1));
        return client.getSegmentDescByName(name);
    }
};

// Red on master / green on branch: a cached remote segment whose backend key
// is removed (peer unmount) is invalidated only after the consecutive-failure
// streak reaches the threshold -- not on the first failure. On master there
// is no invalidation branch, so the stale entry is never erased and the final
// expectation fails.
TEST_F(TransferMetadataStaleSegmentInvalidationTest,
       ErasesStaleEntryAfterConsecutiveFailures) {
    // No background refresh thread (interval == 0) with the cache-hit fast path
    // on (metacache == true): getSegmentDescByName reflects the local cache,
    // and we drive syncSegmentCache explicitly to count failures precisely.
    ScopedMetadataRefreshConfig restore(0, true);

    auto plugin = std::make_shared<FakeMetadataStoragePlugin>();
    TestableTransferMetadata client(plugin);

    const std::string name = "B";
    ASSERT_TRUE(seedRemoteSegment(client, name));

    // Peer B unmounts: the master removes mooncake/ram/B; every get() fails.
    plugin->drop("mooncake/ram/B");

    // First failed sync: streak is 1, below the threshold -> retain the entry,
    // guarding against a single transient blip purging the whole cache.
    ASSERT_EQ(client.syncSegmentCache(""), 0);
    EXPECT_NE(client.getSegmentDescByName(name), nullptr);

    // Second consecutive failed sync: streak reaches the threshold -> the
    // stale cached entry is invalidated. Red-on-master: master never erases,
    // so getSegmentDescByName keeps returning the cached descriptor.
    ASSERT_EQ(client.syncSegmentCache(""), 0);
    EXPECT_EQ(client.getSegmentDescByName(name), nullptr);
}

// Transient-failure guard: a segment that fails once and then succeeds on the
// next cycle must NOT be invalidated -- the successful fetch resets its
// streak. Guards against an erase-on-first-failure regression.
TEST_F(TransferMetadataStaleSegmentInvalidationTest,
       TransientFailureIsRetained) {
    ScopedMetadataRefreshConfig restore(0, true);

    auto plugin = std::make_shared<FakeMetadataStoragePlugin>();
    TestableTransferMetadata client(plugin);

    const std::string name = "C";
    ASSERT_TRUE(seedRemoteSegment(client, name));

    // One transient get() failure: the key still lives in the backend, so the
    // segment is genuinely alive and must not count toward removal.
    plugin->failNext("mooncake/ram/C", 1);

    ASSERT_EQ(client.syncSegmentCache(""), 0);
    EXPECT_NE(client.getSegmentDescByName(name), nullptr);

    // Next cycle the blip clears: get() succeeds again, the streak resets to
    // zero, and the cached entry survives.
    ASSERT_EQ(client.syncSegmentCache(""), 0);
    EXPECT_NE(client.getSegmentDescByName(name), nullptr);
}

}  // namespace mooncake

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

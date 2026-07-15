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
#include <condition_variable>
#include <cstdlib>
#include <future>
#include <mutex>
#include <thread>

#include "config.h"
#include "transport/transport.h"
#include "transfer_metadata_plugin.h"

using namespace mooncake;

namespace mooncake {

class TransferMetadataTestPeer {
   public:
    static int EncodeSegmentDesc(TransferMetadata& metadata,
                                 const TransferMetadata::SegmentDesc& desc,
                                 Json::Value& encoded) {
        return metadata.encodeSegmentDesc(desc, encoded);
    }

    static std::shared_ptr<TransferMetadata::SegmentDesc> DecodeSegmentDesc(
        TransferMetadata& metadata, Json::Value& encoded,
        const std::string& segment_name) {
        return metadata.decodeSegmentDesc(encoded, segment_name);
    }

    static void SetStoragePlugin(
        TransferMetadata& metadata,
        std::shared_ptr<MetadataStoragePlugin> storage_plugin) {
        metadata.p2p_handshake_mode_ = false;
        metadata.storage_plugin_ = std::move(storage_plugin);
    }
};

class FailOnceMetadataStoragePlugin : public MetadataStoragePlugin {
   public:
    bool get(const std::string&, Json::Value& value) override {
        value = last_successful_value;
        return has_successful_value;
    }

    bool set(const std::string&, const Json::Value& value) override {
        ++set_calls;
        last_attempted_value = value;
        if (fail_next_set) {
            fail_next_set = false;
            return false;
        }
        last_successful_value = value;
        has_successful_value = true;
        return true;
    }

    bool remove(const std::string&) override { return true; }

    int set_calls = 0;
    bool fail_next_set = false;
    bool has_successful_value = false;
    Json::Value last_attempted_value;
    Json::Value last_successful_value;
};

class BlockingFailOnceMetadataStoragePlugin : public MetadataStoragePlugin {
   public:
    bool get(const std::string&, Json::Value& value) override {
        std::lock_guard<std::mutex> lock(mutex_);
        value = last_successful_value;
        return has_successful_value;
    }

    bool set(const std::string&, const Json::Value& value) override {
        std::unique_lock<std::mutex> lock(mutex_);
        ++set_calls;
        last_attempted_value = value;
        if (block_and_fail_next_set_) {
            block_and_fail_next_set_ = false;
            blocked_ = true;
            condition_.notify_all();
            condition_.wait(lock, [this] { return release_blocked_set_; });
            release_blocked_set_ = false;
            return false;
        }
        last_successful_value = value;
        has_successful_value = true;
        return true;
    }

    bool remove(const std::string&) override { return true; }

    void BlockAndFailNextSet() {
        std::lock_guard<std::mutex> lock(mutex_);
        block_and_fail_next_set_ = true;
        blocked_ = false;
    }

    void WaitUntilSetIsBlocked() {
        std::unique_lock<std::mutex> lock(mutex_);
        condition_.wait(lock, [this] { return blocked_; });
    }

    void ReleaseBlockedSet() {
        std::lock_guard<std::mutex> lock(mutex_);
        release_blocked_set_ = true;
        condition_.notify_all();
    }

    int SetCalls() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return set_calls;
    }

    Json::Value LastSuccessfulValue() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return last_successful_value;
    }

   private:
    mutable std::mutex mutex_;
    std::condition_variable condition_;
    int set_calls = 0;
    bool block_and_fail_next_set_ = false;
    bool blocked_ = false;
    bool release_blocked_set_ = false;
    bool has_successful_value = false;
    Json::Value last_attempted_value;
    Json::Value last_successful_value;
};

template <typename T>
concept HasMemoryKind = requires(T value) { value.memory_kind; };
template <typename T>
concept HasNumaNode = requires(T value) { value.numa_node; };
template <typename T>
concept HasFabricDomain = requires(T value) { value.fabric_domain_id; };
template <typename T>
concept HasGeneration = requires(T value) { value.generation; };

static_assert(!HasMemoryKind<TransferMetadata::SegmentDesc>);
static_assert(!HasNumaNode<TransferMetadata::SegmentDesc>);
static_assert(!HasFabricDomain<TransferMetadata::SegmentDesc>);
static_assert(!HasGeneration<TransferMetadata::SegmentDesc>);
static_assert(!HasMemoryKind<TransferMetadata::BufferDesc>);
static_assert(!HasNumaNode<TransferMetadata::BufferDesc>);
static_assert(!HasFabricDomain<TransferMetadata::BufferDesc>);
static_assert(!HasGeneration<TransferMetadata::BufferDesc>);

TEST(TransferMetadataSchemaTest, NvlinkDescriptorMatchesV1GoldenJson) {
    TransferMetadata metadata(P2PHANDSHAKE);
    // Freeze the NVLink JSON contract inherited from the current origin/main.
    // Compare parsed values so key order and whitespace do not become an
    // accidental ABI.
    TransferMetadata::SegmentDesc descriptor{};
    descriptor.name = "provider:12345";
    descriptor.protocol = "nvlink";
    descriptor.tcp_data_port = 0;
    descriptor.tcp_proto_version = 1;

    TransferMetadata::BufferDesc buffer{};
    buffer.name = descriptor.name;
    buffer.addr = 0x100000000ULL;
    buffer.length = 0x20000ULL;
    buffer.shm_name = "fabric-handle-v1";
    descriptor.buffers.push_back(buffer);

    Json::Value encoded;
    ASSERT_EQ(TransferMetadataTestPeer::EncodeSegmentDesc(metadata, descriptor,
                                                          encoded),
              0);
    ASSERT_TRUE(encoded.isMember("timestamp"));
    ASSERT_TRUE(encoded["timestamp"].isString());
    ASSERT_FALSE(encoded["timestamp"].asString().empty());
    encoded["timestamp"] = "<timestamp>";

    Json::Value expected;
    expected["name"] = descriptor.name;
    expected["protocol"] = descriptor.protocol;
    expected["tcp_data_port"] = descriptor.tcp_data_port;
    expected["tcp_proto_version"] = descriptor.tcp_proto_version;
    expected["timestamp"] = "<timestamp>";
    Json::Value expected_buffers(Json::arrayValue);
    Json::Value expected_buffer;
    expected_buffer["name"] = buffer.name;
    expected_buffer["addr"] = static_cast<Json::UInt64>(buffer.addr);
    expected_buffer["length"] = static_cast<Json::UInt64>(buffer.length);
    expected_buffer["shm_name"] = buffer.shm_name;
    expected_buffers.append(expected_buffer);
    expected["buffers"] = expected_buffers;

    EXPECT_EQ(encoded, expected)
        << "NVLink SegmentDesc wire fields changed from the V1 golden schema";
    for (const char* forbidden :
         {"memory_kind", "numa_node", "fabric_domain_id", "generation"}) {
        EXPECT_FALSE(encoded.isMember(forbidden));
        EXPECT_FALSE(encoded["buffers"][0].isMember(forbidden));
    }

    Json::Value baseline_golden = expected;
    auto decoded = TransferMetadataTestPeer::DecodeSegmentDesc(
        metadata, baseline_golden, descriptor.name);
    ASSERT_NE(decoded, nullptr);
    EXPECT_EQ(decoded->name, descriptor.name);
    EXPECT_EQ(decoded->protocol, descriptor.protocol);
    EXPECT_EQ(decoded->tcp_data_port, descriptor.tcp_data_port);
    EXPECT_EQ(decoded->tcp_proto_version, descriptor.tcp_proto_version);
    ASSERT_EQ(decoded->buffers.size(), 1U);
    EXPECT_EQ(decoded->buffers[0].name, buffer.name);
    EXPECT_EQ(decoded->buffers[0].addr, buffer.addr);
    EXPECT_EQ(decoded->buffers[0].length, buffer.length);
    EXPECT_EQ(decoded->buffers[0].shm_name, buffer.shm_name);

    Json::Value reencoded;
    ASSERT_EQ(TransferMetadataTestPeer::EncodeSegmentDesc(metadata, *decoded,
                                                          reencoded),
              0);
    ASSERT_TRUE(reencoded.isMember("timestamp"));
    ASSERT_TRUE(reencoded["timestamp"].isString());
    ASSERT_FALSE(reencoded["timestamp"].asString().empty());
    reencoded["timestamp"] = "<timestamp>";
    EXPECT_EQ(reencoded, expected);
}

TEST(TransferMetadataAdditionTest,
     FailedRemoteUpdateRestoresLocalDescriptorForRetry) {
    TransferMetadata metadata(P2PHANDSHAKE);
    auto storage = std::make_shared<FailOnceMetadataStoragePlugin>();
    TransferMetadataTestPeer::SetStoragePlugin(metadata, storage);

    auto segment = std::make_shared<TransferMetadata::SegmentDesc>();
    segment->name = "provider:add-retry";
    segment->protocol = "nvlink";
    auto original_segment = segment;
    ASSERT_EQ(metadata.addLocalSegment(LOCAL_SEGMENT_ID, segment->name,
                                       std::move(segment)),
              0);
    ASSERT_EQ(metadata.updateLocalSegmentDesc(), 0);
    ASSERT_TRUE(storage->has_successful_value);
    ASSERT_EQ(storage->last_successful_value["buffers"].size(), 0U);

    TransferMetadata::BufferDesc buffer{};
    buffer.name = original_segment->name;
    buffer.addr = 0x100000000ULL;
    buffer.length = 0x20000ULL;
    buffer.shm_name = "fabric-handle-add-retry";

    storage->fail_next_set = true;
    EXPECT_EQ(metadata.addLocalMemoryBuffer(buffer, true), ERR_METADATA);
    EXPECT_EQ(storage->set_calls, 2);
    EXPECT_EQ(storage->last_attempted_value["buffers"].size(), 1U);
    EXPECT_EQ(storage->last_successful_value["buffers"].size(), 0U);

    auto local = metadata.getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_NE(local, nullptr);
    EXPECT_EQ(local, original_segment)
        << "failed addition must restore the previous descriptor snapshot";
    EXPECT_TRUE(local->buffers.empty());

    EXPECT_EQ(metadata.addLocalMemoryBuffer(buffer, true), 0);
    EXPECT_EQ(storage->set_calls, 3);
    ASSERT_EQ(storage->last_successful_value["buffers"].size(), 1U);
    EXPECT_EQ(storage->last_successful_value["buffers"][0]["addr"].asUInt64(),
              buffer.addr);
    local = metadata.getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_NE(local, nullptr);
    ASSERT_EQ(local->buffers.size(), 1U);
    EXPECT_EQ(local->buffers[0].addr, buffer.addr);
}

TEST(TransferMetadataAdditionTest,
     FailedRemoteUpdateSerializesConcurrentCowMutationAndRetry) {
    using namespace std::chrono_literals;

    TransferMetadata metadata(P2PHANDSHAKE);
    auto storage = std::make_shared<BlockingFailOnceMetadataStoragePlugin>();
    TransferMetadataTestPeer::SetStoragePlugin(metadata, storage);

    auto segment = std::make_shared<TransferMetadata::SegmentDesc>();
    segment->name = "provider:add-concurrent";
    segment->protocol = "nvlink";
    TransferMetadata::BufferDesc first{};
    first.name = segment->name;
    first.addr = 0x100000000ULL;
    first.length = 0x20000ULL;
    first.shm_name = "fabric-handle-first";
    segment->buffers.push_back(first);
    ASSERT_EQ(metadata.addLocalSegment(LOCAL_SEGMENT_ID, segment->name,
                                       std::move(segment)),
              0);
    ASSERT_EQ(metadata.updateLocalSegmentDesc(), 0);

    TransferMetadata::BufferDesc second{};
    second.name = first.name;
    second.addr = 0x200000000ULL;
    second.length = 0x40000ULL;
    second.shm_name = "fabric-handle-second";
    TransferMetadata::BufferDesc third{};
    third.name = first.name;
    third.addr = 0x300000000ULL;
    third.length = 0x60000ULL;
    third.shm_name = "fabric-handle-third";

    storage->BlockAndFailNextSet();
    auto failed_add = std::async(std::launch::async, [&] {
        return metadata.addLocalMemoryBuffer(second, true);
    });
    storage->WaitUntilSetIsBlocked();

    std::promise<void> concurrent_add_started_promise;
    auto concurrent_add_started = concurrent_add_started_promise.get_future();
    auto concurrent_add = std::async(std::launch::async, [&] {
        concurrent_add_started_promise.set_value();
        return metadata.addLocalMemoryBuffer(third, false);
    });
    concurrent_add_started.wait();
    EXPECT_EQ(concurrent_add.wait_for(50ms), std::future_status::timeout)
        << "concurrent COW mutation must wait for addition "
           "publication/rollback";

    storage->ReleaseBlockedSet();
    EXPECT_EQ(failed_add.get(), ERR_METADATA);
    EXPECT_EQ(concurrent_add.get(), 0);

    auto local = metadata.getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_NE(local, nullptr);
    ASSERT_EQ(local->buffers.size(), 2U);
    EXPECT_EQ(local->buffers[0].addr, first.addr);
    EXPECT_EQ(local->buffers[1].addr, third.addr);

    EXPECT_EQ(metadata.addLocalMemoryBuffer(second, true), 0);
    EXPECT_EQ(storage->SetCalls(), 3);
    Json::Value published = storage->LastSuccessfulValue();
    ASSERT_EQ(published["buffers"].size(), 3U);
    EXPECT_EQ(published["buffers"][0]["addr"].asUInt64(), first.addr);
    EXPECT_EQ(published["buffers"][1]["addr"].asUInt64(), third.addr);
    EXPECT_EQ(published["buffers"][2]["addr"].asUInt64(), second.addr);

    local = metadata.getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_NE(local, nullptr);
    ASSERT_EQ(local->buffers.size(), 3U);
    EXPECT_EQ(local->buffers[0].addr, first.addr);
    EXPECT_EQ(local->buffers[1].addr, third.addr);
    EXPECT_EQ(local->buffers[2].addr, second.addr);
}

TEST(TransferMetadataRemovalTest,
     FailedRemoteUpdateRestoresLocalDescriptorForRetry) {
    TransferMetadata metadata(P2PHANDSHAKE);
    auto storage = std::make_shared<FailOnceMetadataStoragePlugin>();
    TransferMetadataTestPeer::SetStoragePlugin(metadata, storage);

    auto segment = std::make_shared<TransferMetadata::SegmentDesc>();
    segment->name = "provider:12345";
    segment->protocol = "nvlink";
    TransferMetadata::BufferDesc buffer{};
    buffer.name = segment->name;
    buffer.addr = 0x100000000ULL;
    buffer.length = 0x20000ULL;
    buffer.shm_name = "fabric-handle-v1";
    segment->buffers.push_back(buffer);
    auto original_segment = segment;
    ASSERT_EQ(metadata.addLocalSegment(LOCAL_SEGMENT_ID, segment->name,
                                       std::move(segment)),
              0);

    ASSERT_EQ(metadata.updateLocalSegmentDesc(), 0);
    ASSERT_TRUE(storage->has_successful_value);
    ASSERT_EQ(storage->last_successful_value["buffers"].size(), 1U);

    storage->fail_next_set = true;
    EXPECT_EQ(metadata.removeLocalMemoryBuffer(
                  reinterpret_cast<void*>(buffer.addr), true),
              ERR_METADATA);
    EXPECT_EQ(storage->set_calls, 2);
    EXPECT_EQ(storage->last_attempted_value["buffers"].size(), 0U);
    EXPECT_EQ(storage->last_successful_value["buffers"].size(), 1U);

    auto local = metadata.getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_NE(local, nullptr);
    EXPECT_EQ(local, original_segment)
        << "failed deletion must restore the previous descriptor snapshot";
    ASSERT_EQ(local->buffers.size(), 1U);
    EXPECT_EQ(local->buffers[0].addr, buffer.addr)
        << "failed deletion must remain retryable in the local descriptor";

    EXPECT_EQ(metadata.removeLocalMemoryBuffer(
                  reinterpret_cast<void*>(buffer.addr), true),
              0);
    EXPECT_EQ(storage->set_calls, 3);
    EXPECT_EQ(storage->last_successful_value["buffers"].size(), 0U);
    local = metadata.getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_NE(local, nullptr);
    EXPECT_TRUE(local->buffers.empty());
}

TEST(TransferMetadataRemovalTest,
     FailedRemoteUpdateSerializesConcurrentCowMutationAndRetry) {
    using namespace std::chrono_literals;

    TransferMetadata metadata(P2PHANDSHAKE);
    auto storage = std::make_shared<BlockingFailOnceMetadataStoragePlugin>();
    TransferMetadataTestPeer::SetStoragePlugin(metadata, storage);

    auto segment = std::make_shared<TransferMetadata::SegmentDesc>();
    segment->name = "provider:concurrent";
    segment->protocol = "nvlink";
    TransferMetadata::BufferDesc first{};
    first.name = segment->name;
    first.addr = 0x100000000ULL;
    first.length = 0x20000ULL;
    first.shm_name = "fabric-handle-first";
    segment->buffers.push_back(first);
    ASSERT_EQ(metadata.addLocalSegment(LOCAL_SEGMENT_ID, segment->name,
                                       std::move(segment)),
              0);
    ASSERT_EQ(metadata.updateLocalSegmentDesc(), 0);

    TransferMetadata::BufferDesc second{};
    second.name = first.name;
    second.addr = 0x200000000ULL;
    second.length = 0x40000ULL;
    second.shm_name = "fabric-handle-second";

    storage->BlockAndFailNextSet();
    auto remove = std::async(std::launch::async, [&] {
        return metadata.removeLocalMemoryBuffer(
            reinterpret_cast<void*>(first.addr), true);
    });
    storage->WaitUntilSetIsBlocked();

    std::promise<void> add_started_promise;
    auto add_started = add_started_promise.get_future();
    auto add = std::async(std::launch::async, [&] {
        add_started_promise.set_value();
        return metadata.addLocalMemoryBuffer(second, false);
    });
    add_started.wait();
    EXPECT_EQ(add.wait_for(50ms), std::future_status::timeout)
        << "concurrent COW mutation must wait for removal publication/rollback";

    storage->ReleaseBlockedSet();
    EXPECT_EQ(remove.get(), ERR_METADATA);
    EXPECT_EQ(add.get(), 0);

    auto local = metadata.getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_NE(local, nullptr);
    ASSERT_EQ(local->buffers.size(), 2U);
    EXPECT_EQ(local->buffers[0].addr, first.addr);
    EXPECT_EQ(local->buffers[1].addr, second.addr);

    EXPECT_EQ(metadata.removeLocalMemoryBuffer(
                  reinterpret_cast<void*>(first.addr), true),
              0);
    EXPECT_EQ(storage->SetCalls(), 3);
    Json::Value published = storage->LastSuccessfulValue();
    ASSERT_EQ(published["buffers"].size(), 1U);
    EXPECT_EQ(published["buffers"][0]["addr"].asUInt64(), second.addr);

    local = metadata.getSegmentDescByID(LOCAL_SEGMENT_ID);
    ASSERT_NE(local, nullptr);
    ASSERT_EQ(local->buffers.size(), 1U);
    EXPECT_EQ(local->buffers[0].addr, second.addr);
}

TEST(TransferTaskSubmissionFailureTest, ZeroSliceFailureIsExplicitlyTerminal) {
    Transport::BatchDesc batch;
    batch.batch_size = 1;
    batch.id = reinterpret_cast<Transport::BatchID>(&batch);
    batch.task_list.resize(1);
    auto& task = batch.task_list.front();
    task.batch_id = batch.id;

    Transport::markSubmissionFailed(task);
    Transport::markSubmissionFailed(task);

    EXPECT_TRUE(task.submission_failed);
    EXPECT_TRUE(task.is_finished);
    EXPECT_EQ(task.slice_count, 0);
    EXPECT_TRUE(batch.has_failure.load());
#ifdef USE_EVENT_DRIVEN_COMPLETION
    EXPECT_TRUE(batch.is_finished.load());
    EXPECT_EQ(batch.finished_task_count.load(), 1);
#endif
}

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

}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

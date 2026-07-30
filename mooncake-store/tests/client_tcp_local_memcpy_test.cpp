#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "allocator.h"
#include "client_service.h"
#include "test_server_helpers.h"
#include "types.h"
#include "utils.h"

namespace mooncake {
namespace testing {

enum class HandshakeMode { P2P, Metadata };

namespace {

class EnvGuard {
   public:
    explicit EnvGuard(const char* key) : key_(key) {
        if (const char* value = std::getenv(key_)) {
            old_value_ = value;
        }
    }

    ~EnvGuard() {
        if (old_value_.has_value()) {
            setenv(key_, old_value_->c_str(), 1);
        } else {
            unsetenv(key_);
        }
    }

   private:
    const char* key_;
    std::optional<std::string> old_value_;
};

class VLogGuard {
   public:
    explicit VLogGuard(int new_v) : old_v_(FLAGS_v) { FLAGS_v = new_v; }
    ~VLogGuard() { FLAGS_v = old_v_; }

   private:
    int old_v_;
};

class StrategyCaptureSink : public google::LogSink {
   public:
    void send(google::LogSeverity severity, const char* full_filename,
              const char* base_filename, int line, const struct ::tm* tm_time,
              const char* message, size_t message_len) override {
        (void)severity;
        (void)full_filename;
        (void)base_filename;
        (void)line;
        (void)tm_time;

        std::string msg(message, message_len);
        constexpr char kPrefix[] = "Using transfer strategy: ";
        size_t pos = msg.find(kPrefix);
        if (pos == std::string::npos) {
            return;
        }

        captured_strategy_ = msg.substr(pos + std::strlen(kPrefix));
        TrimInPlace(captured_strategy_);
    }

    const std::string& strategy() const { return captured_strategy_; }

   private:
    static void TrimInPlace(std::string& value) {
        auto begin = value.find_first_not_of(" \t\n\r");
        if (begin == std::string::npos) {
            value.clear();
            return;
        }
        auto end = value.find_last_not_of(" \t\n\r");
        value = value.substr(begin, end - begin + 1);
    }

    std::string captured_strategy_;
};

struct ClientRuntime {
    std::shared_ptr<Client> client;
    std::unique_ptr<SimpleAllocator> io_allocator;
    void* segment_ptr = nullptr;
    size_t segment_size = 0;
    std::string host_name;
};

struct PreparedObject {
    std::string key;
    std::string payload;
    QueryResult query_result;
    Replica::Descriptor replica;
};

Replica::Descriptor FindFirstCompleteMemoryReplica(const QueryResult& result) {
    for (const auto& replica : result.replicas) {
        if (replica.status == ReplicaStatus::COMPLETE &&
            replica.is_memory_replica()) {
            return replica;
        }
    }
    throw std::runtime_error("No complete memory replica found");
}

bool PutHotKeyHelper(LocalHotCache& cache, const std::string& key,
                     const Slice& slice) {
    if (key.empty() || slice.ptr == nullptr || slice.size == 0) {
        return false;
    }
    if (cache.TouchHotKey(key)) {
        return true;
    }

    HotMemBlock* block = cache.GetFreeBlock();
    if (block == nullptr) {
        return false;
    }
    if (slice.size > block->size) {
        block->key_.clear();
        cache.PutHotKey(block);
        return false;
    }

    std::memcpy(block->addr, slice.ptr, slice.size);
    block->size = slice.size;
    block->key_ = key;
    return cache.PutHotKey(block);
}

const char* HandshakeModeName(HandshakeMode mode) {
    switch (mode) {
        case HandshakeMode::P2P:
            return "P2P";
        case HandshakeMode::Metadata:
            return "Metadata";
    }
    return "Unknown";
}

}  // namespace

class TcpLocalMemcpyAutoEnableTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("TcpLocalMemcpyAutoEnableTest");
        FLAGS_logtostderr = 1;
    }

    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

    void SetUp() override {
        memcpy_guard_ = std::make_unique<EnvGuard>("MC_STORE_MEMCPY");
        vlog_guard_ = std::make_unique<VLogGuard>(1);
        unsetenv("MC_STORE_MEMCPY");

        InProcMasterConfig config;
        config.http_metadata_port = getFreeTcpPort();
        ASSERT_TRUE(master_.Start(config));
        master_address_ = master_.master_address();
        metadata_url_ = master_.metadata_url();
    }

    void TearDown() override {
        CleanupRuntime(remote_runtime_);
        CleanupRuntime(runtime_);
        master_.Stop();
    }

    ClientRuntime CreateRuntime(const std::string& host_name,
                                const std::string& metadata_conn) {
        ClientRuntime runtime;
        runtime.host_name = host_name;

        auto client_opt = Client::Create(host_name, metadata_conn, "tcp",
                                         std::nullopt, master_address_);
        EXPECT_TRUE(client_opt.has_value())
            << "Failed to create client for host " << host_name
            << ", metadata=" << metadata_conn;
        if (!client_opt.has_value()) {
            return runtime;
        }

        runtime.client = client_opt.value();
        runtime.io_allocator =
            std::make_unique<SimpleAllocator>(16 * 1024 * 1024);
        auto reg = runtime.client->RegisterLocalMemory(
            runtime.io_allocator->getBase(), 16 * 1024 * 1024, "cpu:0", false,
            false);
        EXPECT_TRUE(reg.has_value())
            << "RegisterLocalMemory failed: " << toString(reg.error());

        runtime.segment_size = 64 * 1024 * 1024;
        runtime.segment_ptr =
            allocate_buffer_allocator_memory(runtime.segment_size);
        EXPECT_NE(runtime.segment_ptr, nullptr);
        if (runtime.segment_ptr == nullptr) {
            return runtime;
        }

        auto mount = runtime.client->MountSegment(runtime.segment_ptr,
                                                  runtime.segment_size, "tcp");
        EXPECT_TRUE(mount.has_value())
            << "MountSegment failed: " << toString(mount.error());
        if (!mount.has_value()) {
            free_memory("", runtime.segment_ptr);
            runtime.segment_ptr = nullptr;
        }

        return runtime;
    }

    void CleanupRuntime(ClientRuntime& runtime) {
        if (runtime.client && runtime.segment_ptr) {
            auto unmount = runtime.client->UnmountSegment(runtime.segment_ptr,
                                                          runtime.segment_size);
            EXPECT_TRUE(unmount.has_value())
                << "UnmountSegment failed: " << toString(unmount.error());
        }

        runtime.client.reset();
        runtime.io_allocator.reset();

        if (runtime.segment_ptr) {
            free_memory("", runtime.segment_ptr);
            runtime.segment_ptr = nullptr;
        }
        runtime.segment_size = 0;
        runtime.host_name.clear();
    }

    PreparedObject PrepareLocalObject(const std::string& key,
                                      const std::string& payload) {
        return PrepareObjectOnRuntime(runtime_, key, payload);
    }

    PreparedObject PrepareObjectOnRuntime(ClientRuntime& runtime,
                                          const std::string& key,
                                          const std::string& payload) {
        void* write_buf = runtime.io_allocator->allocate(payload.size());
        EXPECT_NE(write_buf, nullptr);
        std::memcpy(write_buf, payload.data(), payload.size());

        std::vector<Slice> write_slices;
        write_slices.emplace_back(Slice{write_buf, payload.size()});

        ReplicateConfig config;
        config.replica_num = 1;
        config.preferred_segment = runtime.host_name;

        auto put = runtime.client->Put(key, write_slices, config);
        runtime.io_allocator->deallocate(write_buf, payload.size());
        EXPECT_TRUE(put.has_value()) << "Put failed: " << toString(put.error());

        auto query = runtime.client->Query(key);
        EXPECT_TRUE(query.has_value())
            << "Query failed: " << toString(query.error());

        auto replica = FindFirstCompleteMemoryReplica(query.value());

        return PreparedObject{key, payload, std::move(query.value()), replica};
    }

    void LogReplicaDiagnostics(const std::string& label,
                               const Replica::Descriptor& replica) {
        const auto& endpoint = replica.get_memory_descriptor()
                                   .buffer_descriptor.transport_endpoint_;
        LOG(INFO) << label << " replica endpoint=" << endpoint;
        LOG(INFO) << label << " client transport endpoint="
                  << runtime_.client->GetTransportEndpoint();
        LOG(INFO) << label << " is local replica="
                  << runtime_.client->IsReplicaOnLocalMemory(replica);
    }

    InProcMaster master_;
    std::string master_address_;
    std::string metadata_url_;
    ClientRuntime runtime_;
    ClientRuntime remote_runtime_;
    std::unique_ptr<EnvGuard> memcpy_guard_;
    std::unique_ptr<VLogGuard> vlog_guard_;
};

TEST_F(TcpLocalMemcpyAutoEnableTest, P2PLocalReplicaUsesLocalMemcpy) {
    runtime_ = CreateRuntime("localhost", "P2PHANDSHAKE");
    ASSERT_TRUE(runtime_.client != nullptr);

    auto prepared =
        PrepareLocalObject("p2p_local_memcpy_key", "hello-local-memcpy");
    LogReplicaDiagnostics("[P2P]", prepared.replica);

    StrategyCaptureSink sink;
    google::AddLogSink(&sink);

    std::vector<char> out(prepared.payload.size(), '\0');
    std::vector<Slice> read_slices;
    read_slices.emplace_back(Slice{out.data(), out.size()});

    auto get =
        runtime_.client->Get(prepared.key, prepared.query_result, read_slices);

    google::RemoveLogSink(&sink);

    ASSERT_TRUE(get.has_value()) << "Get failed: " << toString(get.error());
    ASSERT_EQ(std::memcmp(out.data(), prepared.payload.data(), out.size()), 0);
    ASSERT_TRUE(runtime_.client->IsReplicaOnLocalMemory(prepared.replica));

    const auto& replica_endpoint = prepared.replica.get_memory_descriptor()
                                       .buffer_descriptor.transport_endpoint_;
    EXPECT_EQ(replica_endpoint, runtime_.client->GetTransportEndpoint());
    EXPECT_EQ(sink.strategy(), "LOCAL_MEMCPY");
}

TEST_F(TcpLocalMemcpyAutoEnableTest, MetadataLocalReplicaUsesLocalMemcpy) {
    runtime_ = CreateRuntime("localhost", metadata_url_);
    ASSERT_TRUE(runtime_.client != nullptr);

    auto prepared =
        PrepareLocalObject("metadata_local_memcpy_key", "hello-local-memcpy");
    LogReplicaDiagnostics("[METADATA]", prepared.replica);

    const auto& replica_endpoint = prepared.replica.get_memory_descriptor()
                                       .buffer_descriptor.transport_endpoint_;
    const auto client_endpoint = runtime_.client->GetTransportEndpoint();
    const bool is_local =
        runtime_.client->IsReplicaOnLocalMemory(prepared.replica);

    StrategyCaptureSink sink;
    google::AddLogSink(&sink);

    std::vector<char> out(prepared.payload.size(), '\0');
    std::vector<Slice> read_slices;
    read_slices.emplace_back(Slice{out.data(), out.size()});

    auto get =
        runtime_.client->Get(prepared.key, prepared.query_result, read_slices);

    google::RemoveLogSink(&sink);

    ASSERT_TRUE(get.has_value()) << "Get failed: " << toString(get.error());
    ASSERT_EQ(std::memcmp(out.data(), prepared.payload.data(), out.size()), 0);
    ASSERT_TRUE(is_local);
    EXPECT_NE(replica_endpoint, client_endpoint);
    EXPECT_EQ(sink.strategy(), "LOCAL_MEMCPY");
}

TEST_F(TcpLocalMemcpyAutoEnableTest,
       P2PRemoteReplicaOnSameTcpHostUsesTransferEngine) {
    remote_runtime_ = CreateRuntime("127.0.0.1:18001", "P2PHANDSHAKE");
    runtime_ = CreateRuntime("127.0.0.1:18002", "P2PHANDSHAKE");
    ASSERT_TRUE(remote_runtime_.client != nullptr);
    ASSERT_TRUE(runtime_.client != nullptr);

    auto prepared = PrepareObjectOnRuntime(
        remote_runtime_, "p2p_remote_same_host_key", "hello-remote-transfer");
    LogReplicaDiagnostics("[P2P_REMOTE]", prepared.replica);

    const auto& replica_endpoint = prepared.replica.get_memory_descriptor()
                                       .buffer_descriptor.transport_endpoint_;
    const auto client_endpoint = runtime_.client->GetTransportEndpoint();
    ASSERT_NE(replica_endpoint, client_endpoint);
    ASSERT_FALSE(runtime_.client->IsReplicaOnLocalMemory(prepared.replica));

    StrategyCaptureSink sink;
    google::AddLogSink(&sink);

    std::vector<char> out(prepared.payload.size(), '\0');
    std::vector<Slice> read_slices;
    read_slices.emplace_back(Slice{out.data(), out.size()});

    auto get =
        runtime_.client->Get(prepared.key, prepared.query_result, read_slices);

    google::RemoveLogSink(&sink);

    ASSERT_TRUE(get.has_value()) << "Get failed: " << toString(get.error());
    ASSERT_EQ(std::memcmp(out.data(), prepared.payload.data(), out.size()), 0);
    EXPECT_EQ(sink.strategy(), "TRANSFER_ENGINE");
}

TEST_F(TcpLocalMemcpyAutoEnableTest,
       MetadataRemoteReplicaOnSameTcpHostUsesTransferEngine) {
    remote_runtime_ = CreateRuntime("metadata-host-a", metadata_url_);
    runtime_ = CreateRuntime("metadata-host-b", metadata_url_);
    ASSERT_TRUE(remote_runtime_.client != nullptr);
    ASSERT_TRUE(runtime_.client != nullptr);

    auto prepared =
        PrepareObjectOnRuntime(remote_runtime_, "metadata_remote_same_host_key",
                               "hello-metadata-remote-transfer");
    LogReplicaDiagnostics("[METADATA_REMOTE]", prepared.replica);

    const auto& replica_endpoint = prepared.replica.get_memory_descriptor()
                                       .buffer_descriptor.transport_endpoint_;
    const auto client_endpoint = runtime_.client->GetTransportEndpoint();
    ASSERT_EQ(replica_endpoint, remote_runtime_.host_name);
    ASSERT_NE(replica_endpoint, runtime_.host_name);
    ASSERT_NE(replica_endpoint, client_endpoint);
    ASSERT_FALSE(runtime_.client->IsReplicaOnLocalMemory(prepared.replica));

    StrategyCaptureSink sink;
    google::AddLogSink(&sink);

    std::vector<char> out(prepared.payload.size(), '\0');
    std::vector<Slice> read_slices;
    read_slices.emplace_back(Slice{out.data(), out.size()});

    auto get =
        runtime_.client->Get(prepared.key, prepared.query_result, read_slices);

    google::RemoveLogSink(&sink);

    ASSERT_TRUE(get.has_value()) << "Get failed: " << toString(get.error());
    ASSERT_EQ(std::memcmp(out.data(), prepared.payload.data(), out.size()), 0);
    EXPECT_EQ(sink.strategy(), "TRANSFER_ENGINE");
}

class HotCacheRedirectStrategyTest
    : public TcpLocalMemcpyAutoEnableTest,
      public ::testing::WithParamInterface<HandshakeMode> {};

TEST_P(HotCacheRedirectStrategyTest, CacheHitUsesLocalMemcpy) {
    EnvGuard cache_size_guard("MC_STORE_LOCAL_HOT_CACHE_SIZE");
    setenv("MC_STORE_LOCAL_HOT_CACHE_SIZE", "33554432", 1);  // 32MB

    const HandshakeMode mode = GetParam();
    const bool is_p2p = mode == HandshakeMode::P2P;
    const std::string metadata_conn = is_p2p ? "P2PHANDSHAKE" : metadata_url_;
    const std::string host_name =
        is_p2p ? "localhost" : "metadata-hot-cache-host";

    runtime_ = CreateRuntime(host_name, metadata_conn);
    ASSERT_TRUE(runtime_.client != nullptr) << HandshakeModeName(mode);
    ASSERT_TRUE(runtime_.client->IsHotCacheEnabled())
        << HandshakeModeName(mode);

    const std::string key = is_p2p ? "p2p_hot_cache_redirect_key"
                                   : "metadata_hot_cache_redirect_key";
    const std::string payload =
        is_p2p ? "p2p-hot-cache-data" : "metadata-hot-cache-data";

    Slice cache_slice{const_cast<char*>(payload.data()), payload.size()};
    ASSERT_TRUE(
        PutHotKeyHelper(*runtime_.client->GetHotCache(), key, cache_slice));
    ASSERT_TRUE(runtime_.client->GetHotCache()->HasHotKey(key));

    Replica::Descriptor replica;
    replica.id = is_p2p ? 1 : 2;
    replica.status = ReplicaStatus::COMPLETE;
    MemoryDescriptor mem_desc;
    mem_desc.buffer_descriptor.transport_endpoint_ = "remote:9999";
    mem_desc.buffer_descriptor.buffer_address_ = 0;
    mem_desc.buffer_descriptor.size_ = payload.size();
    replica.descriptor_variant = mem_desc;

    ASSERT_FALSE(runtime_.client->IsReplicaOnLocalMemory(replica))
        << HandshakeModeName(mode);

    std::vector<Replica::Descriptor> replicas;
    replicas.emplace_back(replica);
    QueryResult query_result(
        std::move(replicas),
        std::chrono::steady_clock::now() + std::chrono::seconds(60));

    StrategyCaptureSink sink;
    google::AddLogSink(&sink);

    std::vector<char> out(payload.size(), '\0');
    std::vector<Slice> read_slices;
    read_slices.emplace_back(Slice{out.data(), out.size()});

    auto get = runtime_.client->Get(key, query_result, read_slices);

    google::RemoveLogSink(&sink);

    ASSERT_TRUE(get.has_value()) << "Get failed: " << toString(get.error());
    ASSERT_EQ(std::memcmp(out.data(), payload.data(), out.size()), 0);
    EXPECT_EQ(sink.strategy(), "LOCAL_MEMCPY");
}

INSTANTIATE_TEST_SUITE_P(
    AllModes, HotCacheRedirectStrategyTest,
    ::testing::Values(HandshakeMode::P2P, HandshakeMode::Metadata),
    [](const ::testing::TestParamInfo<HandshakeMode>& info) {
        switch (info.param) {
            case HandshakeMode::P2P:
                return "P2P";
            case HandshakeMode::Metadata:
                return "Metadata";
        }
        return "Unknown";
    });

}  // namespace testing
}  // namespace mooncake
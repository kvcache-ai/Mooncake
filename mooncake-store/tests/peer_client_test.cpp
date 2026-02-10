#include <glog/logging.h>
#include <gtest/gtest.h>
#include <json/json.h>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <async_simple/coro/Lazy.h>
#include <async_simple/coro/SyncAwait.h>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "client_rpc_service.h"
#include "client_rpc_types.h"
#include "data_manager.h"
#include "peer_client.h"
#include "tiered_cache/tiered_backend.h"
#include "transfer_engine.h"
#include "types.h"

namespace mooncake {

// Helper function to parse JSON string
static bool parseJsonString(const std::string& json_str, Json::Value& value,
                            std::string* error_msg = nullptr) {
    Json::CharReaderBuilder builder;
    std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    std::string errs;

    bool success = reader->parse(
        json_str.data(), json_str.data() + json_str.size(), &value, &errs);
    if (!success && error_msg) {
        *error_msg = errs;
    }
    return success;
}

// Test fixture for PeerClient tests.
// Sets up a coro_rpc_server with ClientRpcService registered,
// and a PeerClient connected to the server.
//
// Error propagation: PeerClient forwards server-side error codes
// (INVALID_PARAMS, INTERNAL_ERROR, INVALID_KEY) transparently.
// RPC_FAIL is only returned when the RPC transport itself fails
// (e.g., client not connected).
class PeerClientTest : public ::testing::Test {
   protected:
    static constexpr uint16_t kTestPort = 50051;

    void SetUp() override {
        google::InitGoogleLogging("PeerClientTest");
        FLAGS_logtostderr = 1;

        // Create a minimal TransferEngine
        transfer_engine_ = std::make_shared<TransferEngine>(false);

        // Create TieredBackend with DRAM tier configuration
        std::string json_config_str = R"({
            "tiers": [
                {
                    "type": "DRAM",
                    "capacity": 1073741824,
                    "priority": 10,
                    "tags": ["fast", "local"],
                    "allocator_type": "OFFSET"
                }
            ]
        })";
        Json::Value config;
        ASSERT_TRUE(parseJsonString(json_config_str, config));

        tiered_backend_ = std::make_unique<TieredBackend>();
        auto init_result = tiered_backend_->Init(config, nullptr, nullptr);
        ASSERT_TRUE(init_result.has_value())
            << "Failed to initialize TieredBackend: " << init_result.error();

        // Verify tier was created successfully
        auto tier_views = tiered_backend_->GetTierViews();
        ASSERT_EQ(tier_views.size(), 1)
            << "Expected 1 tier, got " << tier_views.size();
        saved_tier_id_ = tier_views[0].id;

        // Create DataManager
        data_manager_ = std::make_unique<DataManager>(
            std::move(tiered_backend_), transfer_engine_);

        // Create ClientRpcService
        rpc_service_ = std::make_unique<ClientRpcService>(*data_manager_);

        // Start coro_rpc_server
        server_ = std::make_unique<coro_rpc::coro_rpc_server>(
            /*thread_num=*/1, kTestPort);
        RegisterClientRpcService(*server_, *rpc_service_);

        server_thread_ = std::thread([this]() {
            auto ec = server_->start();
            if (ec) {
                LOG(ERROR) << "Server start failed: " << ec.message();
            }
        });

        // Wait for server to be ready
        std::this_thread::sleep_for(std::chrono::milliseconds(200));

        // Create and connect PeerClient
        peer_client_ = std::make_unique<PeerClient>();
        std::string endpoint =
            "127.0.0.1:" + std::to_string(kTestPort);
        auto connect_result = peer_client_->Connect(endpoint);
        ASSERT_TRUE(connect_result.has_value())
            << "PeerClient::Connect failed";
    }

    void TearDown() override {
        peer_client_.reset();
        if (server_) {
            server_->stop();
        }
        if (server_thread_.joinable()) {
            server_thread_.join();
        }
        server_.reset();
        rpc_service_.reset();
        data_manager_.reset();
        tiered_backend_.reset();
        transfer_engine_.reset();
        google::ShutdownGoogleLogging();
    }

    // Helper: Get tier ID from backend
    std::optional<UUID> GetTierId() {
        if (saved_tier_id_.has_value()) {
            return saved_tier_id_;
        }
        return std::nullopt;
    }

    // Helper: Create test data buffer
    std::unique_ptr<char[]> StringToBuffer(const std::string& str) {
        auto buffer = std::make_unique<char[]>(str.size());
        std::memcpy(buffer.get(), str.data(), str.size());
        return buffer;
    }

    // Helper: Create a valid RemoteBufferDesc
    RemoteBufferDesc CreateBufferDesc(const std::string& segment_name,
                                      uintptr_t addr, uint64_t size) {
        RemoteBufferDesc desc;
        desc.segment_name = segment_name;
        desc.addr = addr;
        desc.size = size;
        return desc;
    }

    std::unique_ptr<DataManager> data_manager_;
    std::unique_ptr<TieredBackend> tiered_backend_;
    std::shared_ptr<TransferEngine> transfer_engine_;
    std::unique_ptr<ClientRpcService> rpc_service_;
    std::unique_ptr<coro_rpc::coro_rpc_server> server_;
    std::thread server_thread_;
    std::unique_ptr<PeerClient> peer_client_;
    std::optional<UUID> saved_tier_id_;
};

// ============================================================================
// Connect Tests
// ============================================================================

TEST_F(PeerClientTest, ConnectSuccess) {
    // Connect was already called in SetUp; verify that peer_client_ is usable
    // by making an RPC call that reaches the server.
    RemoteReadRequest request;
    request.key = "connect_test_key";
    request.dest_buffers.push_back(
        CreateBufferDesc("test_segment", 0x1000, 100));

    auto result = peer_client_->ReadRemoteData(request);
    // The RPC reaches the server but fails because the key doesn't exist.
    // The important thing is that we get a server-side error, not RPC_FAIL.
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_KEY);
}

// ============================================================================
// AsyncReadRemoteData Tests
// ============================================================================

TEST_F(PeerClientTest, AsyncReadRemoteDataKeyNotFound) {
    RemoteReadRequest request;
    request.key = "non_existent_key";
    request.dest_buffers.push_back(
        CreateBufferDesc("test_segment", 0x1000, 100));

    auto result = async_simple::coro::syncAwait(
        peer_client_->AsyncReadRemoteData(request));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_KEY);
}

TEST_F(PeerClientTest, AsyncReadRemoteDataEmptyKey) {
    RemoteReadRequest request;
    request.key = "";  // Empty key
    request.dest_buffers.push_back(
        CreateBufferDesc("test_segment", 0x1000, 100));

    auto result = async_simple::coro::syncAwait(
        peer_client_->AsyncReadRemoteData(request));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, AsyncReadRemoteDataEmptyBuffers) {
    RemoteReadRequest request;
    request.key = "test_key";
    // Empty dest_buffers

    auto result = async_simple::coro::syncAwait(
        peer_client_->AsyncReadRemoteData(request));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, AsyncReadRemoteDataInvalidBufferZeroSize) {
    RemoteReadRequest request;
    request.key = "test_key";
    request.dest_buffers.push_back(
        CreateBufferDesc("test_segment", 0x1000, 0));  // Zero size

    auto result = async_simple::coro::syncAwait(
        peer_client_->AsyncReadRemoteData(request));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, AsyncReadRemoteDataInvalidBufferNullAddr) {
    RemoteReadRequest request;
    request.key = "test_key";
    request.dest_buffers.push_back(
        CreateBufferDesc("test_segment", 0, 100));  // Null address

    auto result = async_simple::coro::syncAwait(
        peer_client_->AsyncReadRemoteData(request));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, AsyncReadRemoteDataWithExistingKey) {
    // First, put some data via DataManager
    const std::string key = "async_read_key";
    const std::string test_data = "Hello, Async!";
    auto buffer = StringToBuffer(test_data);
    auto put_result =
        data_manager_->Put(key, std::move(buffer), test_data.size());
    ASSERT_TRUE(put_result.has_value()) << "Put failed";

    // Create read request
    RemoteReadRequest request;
    request.key = key;
    request.dest_buffers.push_back(
        CreateBufferDesc("test_segment", 0x1000, test_data.size()));

    // RPC reaches the server; DataManager finds the key but TransferEngine
    // is not fully initialized, so the transfer fails with INTERNAL_ERROR.
    auto result = async_simple::coro::syncAwait(
        peer_client_->AsyncReadRemoteData(request));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INTERNAL_ERROR);
}

// ============================================================================
// AsyncWriteRemoteData Tests
// ============================================================================

TEST_F(PeerClientTest, AsyncWriteRemoteDataEmptyKey) {
    RemoteWriteRequest request;
    request.key = "";  // Empty key
    request.src_buffers.push_back(
        CreateBufferDesc("test_segment", 0x1000, 100));

    auto result = async_simple::coro::syncAwait(
        peer_client_->AsyncWriteRemoteData(request));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, AsyncWriteRemoteDataEmptyBuffers) {
    RemoteWriteRequest request;
    request.key = "test_key";
    // Empty src_buffers

    auto result = async_simple::coro::syncAwait(
        peer_client_->AsyncWriteRemoteData(request));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, AsyncWriteRemoteDataInvalidBufferZeroSize) {
    RemoteWriteRequest request;
    request.key = "test_key";
    request.src_buffers.push_back(
        CreateBufferDesc("test_segment", 0x1000, 0));  // Zero size

    auto result = async_simple::coro::syncAwait(
        peer_client_->AsyncWriteRemoteData(request));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, AsyncWriteRemoteDataInvalidBufferNullAddr) {
    RemoteWriteRequest request;
    request.key = "test_key";
    request.src_buffers.push_back(
        CreateBufferDesc("test_segment", 0, 100));  // Null address

    auto result = async_simple::coro::syncAwait(
        peer_client_->AsyncWriteRemoteData(request));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, AsyncWriteRemoteDataValidRequest) {
    RemoteWriteRequest request;
    request.key = "async_write_key";
    request.src_buffers.push_back(
        CreateBufferDesc("test_segment", 0x1000, 100));
    request.target_tier_id = std::nullopt;

    // RPC reaches the server; TransferEngine is not fully initialized,
    // so the write fails with INTERNAL_ERROR.
    auto result = async_simple::coro::syncAwait(
        peer_client_->AsyncWriteRemoteData(request));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INTERNAL_ERROR);
}

TEST_F(PeerClientTest, AsyncWriteRemoteDataWithTierId) {
    auto tier_id = GetTierId();
    ASSERT_TRUE(tier_id.has_value()) << "No tier available";

    RemoteWriteRequest request;
    request.key = "async_write_key_with_tier";
    request.src_buffers.push_back(
        CreateBufferDesc("test_segment", 0x1000, 100));
    request.target_tier_id = tier_id;

    auto result = async_simple::coro::syncAwait(
        peer_client_->AsyncWriteRemoteData(request));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INTERNAL_ERROR);
}

// ============================================================================
// Sync ReadRemoteData Tests (wrappers around async)
// ============================================================================

TEST_F(PeerClientTest, SyncReadRemoteDataKeyNotFound) {
    RemoteReadRequest request;
    request.key = "non_existent_key";
    request.dest_buffers.push_back(
        CreateBufferDesc("test_segment", 0x1000, 100));

    auto result = peer_client_->ReadRemoteData(request);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_KEY);
}

TEST_F(PeerClientTest, SyncReadRemoteDataEmptyKey) {
    RemoteReadRequest request;
    request.key = "";  // Empty key
    request.dest_buffers.push_back(
        CreateBufferDesc("test_segment", 0x1000, 100));

    auto result = peer_client_->ReadRemoteData(request);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, SyncReadRemoteDataEmptyBuffers) {
    RemoteReadRequest request;
    request.key = "test_key";
    // Empty dest_buffers

    auto result = peer_client_->ReadRemoteData(request);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, SyncReadRemoteDataWithExistingKey) {
    // Put data first
    const std::string key = "sync_read_key";
    const std::string test_data = "Hello, Sync Read!";
    auto buffer = StringToBuffer(test_data);
    auto put_result =
        data_manager_->Put(key, std::move(buffer), test_data.size());
    ASSERT_TRUE(put_result.has_value()) << "Put failed";

    RemoteReadRequest request;
    request.key = key;
    request.dest_buffers.push_back(
        CreateBufferDesc("test_segment", 0x1000, test_data.size()));

    auto result = peer_client_->ReadRemoteData(request);
    ASSERT_FALSE(result.has_value());
    // INTERNAL_ERROR: RPC reached server, key found, but TransferEngine
    // not initialized so transfer fails.
    EXPECT_EQ(result.error(), ErrorCode::INTERNAL_ERROR);
}

// ============================================================================
// Sync WriteRemoteData Tests (wrappers around async)
// ============================================================================

TEST_F(PeerClientTest, SyncWriteRemoteDataEmptyKey) {
    RemoteWriteRequest request;
    request.key = "";  // Empty key
    request.src_buffers.push_back(
        CreateBufferDesc("test_segment", 0x1000, 100));

    auto result = peer_client_->WriteRemoteData(request);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, SyncWriteRemoteDataEmptyBuffers) {
    RemoteWriteRequest request;
    request.key = "test_key";
    // Empty src_buffers

    auto result = peer_client_->WriteRemoteData(request);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, SyncWriteRemoteDataValidRequest) {
    RemoteWriteRequest request;
    request.key = "sync_write_key";
    request.src_buffers.push_back(
        CreateBufferDesc("test_segment", 0x1000, 100));
    request.target_tier_id = std::nullopt;

    auto result = peer_client_->WriteRemoteData(request);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INTERNAL_ERROR);
}

// ============================================================================
// BatchReadRemoteData Tests
// ============================================================================

TEST_F(PeerClientTest, BatchReadRemoteDataKeysNotFound) {
    BatchRemoteReadRequest request;
    request.keys.push_back("batch_key1");
    request.keys.push_back("batch_key2");
    request.dest_buffers_list.push_back(
        {CreateBufferDesc("segment1", 0x1000, 100)});
    request.dest_buffers_list.push_back(
        {CreateBufferDesc("segment2", 0x2000, 100)});

    auto results = peer_client_->BatchReadRemoteData(request);
    ASSERT_EQ(results.size(), 2);

    // Both should fail (keys don't exist)
    ASSERT_FALSE(results[0].has_value());
    EXPECT_EQ(results[0].error(), ErrorCode::INVALID_KEY);
    ASSERT_FALSE(results[1].has_value());
    EXPECT_EQ(results[1].error(), ErrorCode::INVALID_KEY);
}

TEST_F(PeerClientTest, BatchReadRemoteDataWithExistingKeys) {
    // Put test data
    const std::string key1 = "batch_read_key1";
    const std::string key2 = "batch_read_key2";
    const std::string test_data = "batch_data";

    auto buffer1 = StringToBuffer(test_data);
    auto put_result1 =
        data_manager_->Put(key1, std::move(buffer1), test_data.size());
    ASSERT_TRUE(put_result1.has_value());

    auto buffer2 = StringToBuffer(test_data);
    auto put_result2 =
        data_manager_->Put(key2, std::move(buffer2), test_data.size());
    ASSERT_TRUE(put_result2.has_value());

    BatchRemoteReadRequest request;
    request.keys.push_back(key1);
    request.keys.push_back(key2);
    request.dest_buffers_list.push_back(
        {CreateBufferDesc("segment1", 0x1000, test_data.size())});
    request.dest_buffers_list.push_back(
        {CreateBufferDesc("segment2", 0x2000, test_data.size())});

    auto results = peer_client_->BatchReadRemoteData(request);
    ASSERT_EQ(results.size(), 2);

    // Both should fail with INTERNAL_ERROR (TransferEngine not initialized)
    ASSERT_FALSE(results[0].has_value());
    EXPECT_EQ(results[0].error(), ErrorCode::INTERNAL_ERROR);
    ASSERT_FALSE(results[1].has_value());
    EXPECT_EQ(results[1].error(), ErrorCode::INTERNAL_ERROR);
}

TEST_F(PeerClientTest, BatchReadRemoteDataEmptyRequest) {
    BatchRemoteReadRequest request;
    // Empty keys and buffers

    auto results = peer_client_->BatchReadRemoteData(request);
    ASSERT_EQ(results.size(), 0);
}

// ============================================================================
// BatchWriteRemoteData Tests
// ============================================================================

TEST_F(PeerClientTest, BatchWriteRemoteDataValidRequests) {
    BatchRemoteWriteRequest request;
    request.keys.push_back("batch_write_key1");
    request.keys.push_back("batch_write_key2");
    request.src_buffers_list.push_back(
        {CreateBufferDesc("segment1", 0x1000, 100)});
    request.src_buffers_list.push_back(
        {CreateBufferDesc("segment2", 0x2000, 100)});
    request.target_tier_ids.push_back(std::nullopt);
    request.target_tier_ids.push_back(std::nullopt);

    auto results = peer_client_->BatchWriteRemoteData(request);
    ASSERT_EQ(results.size(), 2);

    // Both should fail with INTERNAL_ERROR (TransferEngine not initialized)
    ASSERT_FALSE(results[0].has_value());
    EXPECT_EQ(results[0].error(), ErrorCode::INTERNAL_ERROR);
    ASSERT_FALSE(results[1].has_value());
    EXPECT_EQ(results[1].error(), ErrorCode::INTERNAL_ERROR);
}

TEST_F(PeerClientTest, BatchWriteRemoteDataWithTierIds) {
    auto tier_id = GetTierId();
    ASSERT_TRUE(tier_id.has_value()) << "No tier available";

    BatchRemoteWriteRequest request;
    request.keys.push_back("batch_write_tier_key1");
    request.keys.push_back("batch_write_tier_key2");
    request.src_buffers_list.push_back(
        {CreateBufferDesc("segment1", 0x1000, 100)});
    request.src_buffers_list.push_back(
        {CreateBufferDesc("segment2", 0x2000, 100)});
    request.target_tier_ids.push_back(tier_id);
    request.target_tier_ids.push_back(tier_id);

    auto results = peer_client_->BatchWriteRemoteData(request);
    ASSERT_EQ(results.size(), 2);

    // Both should fail with INTERNAL_ERROR (TransferEngine not initialized)
    ASSERT_FALSE(results[0].has_value());
    EXPECT_EQ(results[0].error(), ErrorCode::INTERNAL_ERROR);
    ASSERT_FALSE(results[1].has_value());
    EXPECT_EQ(results[1].error(), ErrorCode::INTERNAL_ERROR);
}

TEST_F(PeerClientTest, BatchWriteRemoteDataEmptyRequest) {
    BatchRemoteWriteRequest request;
    // Empty keys, buffers, and tier_ids

    auto results = peer_client_->BatchWriteRemoteData(request);
    ASSERT_EQ(results.size(), 0);
}

// ============================================================================
// Not Connected Error Tests
// ============================================================================

TEST_F(PeerClientTest, AsyncReadWithoutConnect) {
    // Create a new PeerClient without calling Connect
    PeerClient unconnected_client;

    RemoteReadRequest request;
    request.key = "test_key";
    request.dest_buffers.push_back(
        CreateBufferDesc("test_segment", 0x1000, 100));

    auto result = async_simple::coro::syncAwait(
        unconnected_client.AsyncReadRemoteData(request));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::RPC_FAIL);
}

TEST_F(PeerClientTest, AsyncWriteWithoutConnect) {
    // Create a new PeerClient without calling Connect
    PeerClient unconnected_client;

    RemoteWriteRequest request;
    request.key = "test_key";
    request.src_buffers.push_back(
        CreateBufferDesc("test_segment", 0x1000, 100));

    auto result = async_simple::coro::syncAwait(
        unconnected_client.AsyncWriteRemoteData(request));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::RPC_FAIL);
}

TEST_F(PeerClientTest, SyncReadWithoutConnect) {
    PeerClient unconnected_client;

    RemoteReadRequest request;
    request.key = "test_key";
    request.dest_buffers.push_back(
        CreateBufferDesc("test_segment", 0x1000, 100));

    auto result = unconnected_client.ReadRemoteData(request);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::RPC_FAIL);
}

TEST_F(PeerClientTest, SyncWriteWithoutConnect) {
    PeerClient unconnected_client;

    RemoteWriteRequest request;
    request.key = "test_key";
    request.src_buffers.push_back(
        CreateBufferDesc("test_segment", 0x1000, 100));

    auto result = unconnected_client.WriteRemoteData(request);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::RPC_FAIL);
}

}  // namespace mooncake

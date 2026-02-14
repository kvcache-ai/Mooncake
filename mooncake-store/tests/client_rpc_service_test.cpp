#include <glog/logging.h>
#include <gtest/gtest.h>
#include <json/json.h>
#include <memory>
#include <string>
#include <vector>

#include "client_rpc_service.h"
#include "client_rpc_types.h"
#include "data_manager.h"
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

// Test fixture for ClientRpcService tests
class ClientRpcServiceTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("ClientRpcServiceTest");
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
    }

    void TearDown() override {
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
    std::optional<UUID> saved_tier_id_;
};

// ============================================================================
// ReadRemoteData Tests
// ============================================================================

// Test ReadRemoteData - success case (without initialized TransferEngine)
TEST_F(ClientRpcServiceTest, ReadRemoteDataSuccess) {
    // First, put some data
    const std::string key = "test_read_key";
    const std::string test_data = "Hello, World!";
    auto buffer = StringToBuffer(test_data);
    auto put_result =
        data_manager_->Put(key, std::move(buffer), test_data.size());
    ASSERT_TRUE(put_result.has_value()) << "Put failed";

    // Create read request with valid buffers
    RemoteReadRequest request;
    request.key = key;
    request.dest_buffers.push_back(
        CreateBufferDesc("test_segment", 0x1000, test_data.size()));

    // ReadRemoteData will fail because TransferEngine is not fully initialized
    // (no metadata connection). This is expected in unit test environment.
    auto result = rpc_service_->ReadRemoteData(request);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INTERNAL_ERROR);
}

// Test ReadRemoteData - empty key
TEST_F(ClientRpcServiceTest, ReadRemoteDataEmptyKey) {
    RemoteReadRequest request;
    request.key = "";  // Empty key
    request.dest_buffers.push_back(
        CreateBufferDesc("test_segment", 0x1000, 100));

    auto result = rpc_service_->ReadRemoteData(request);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

// Test ReadRemoteData - empty destination buffers
TEST_F(ClientRpcServiceTest, ReadRemoteDataEmptyBuffers) {
    RemoteReadRequest request;
    request.key = "test_key";
    // Empty dest_buffers

    auto result = rpc_service_->ReadRemoteData(request);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

// Test ReadRemoteData - invalid buffer (zero size)
TEST_F(ClientRpcServiceTest, ReadRemoteDataInvalidBufferZeroSize) {
    RemoteReadRequest request;
    request.key = "test_key";
    request.dest_buffers.push_back(
        CreateBufferDesc("test_segment", 0x1000, 0));  // Zero size

    auto result = rpc_service_->ReadRemoteData(request);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

// Test ReadRemoteData - invalid buffer (null address)
TEST_F(ClientRpcServiceTest, ReadRemoteDataInvalidBufferNullAddr) {
    RemoteReadRequest request;
    request.key = "test_key";
    request.dest_buffers.push_back(
        CreateBufferDesc("test_segment", 0, 100));  // Null address

    auto result = rpc_service_->ReadRemoteData(request);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

// Test ReadRemoteData - key not found
TEST_F(ClientRpcServiceTest, ReadRemoteDataKeyNotFound) {
    RemoteReadRequest request;
    request.key = "non_existent_key";
    request.dest_buffers.push_back(
        CreateBufferDesc("test_segment", 0x1000, 100));

    auto result = rpc_service_->ReadRemoteData(request);
    ASSERT_FALSE(result.has_value());
}

// ============================================================================
// WriteRemoteData Tests
// ============================================================================

// Test WriteRemoteData - success case
TEST_F(ClientRpcServiceTest, WriteRemoteDataSuccess) {
    RemoteWriteRequest request;
    request.key = "test_write_key";
    request.src_buffers.push_back(
        CreateBufferDesc("test_segment", 0x1000, 100));
    request.target_tier_id = std::nullopt;

    // WriteRemoteData will fail because TransferEngine is not fully initialized
    // (no metadata connection). This is expected in unit test environment.
    auto result = rpc_service_->WriteRemoteData(request);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INTERNAL_ERROR);
}

// Test WriteRemoteData - empty key
TEST_F(ClientRpcServiceTest, WriteRemoteDataEmptyKey) {
    RemoteWriteRequest request;
    request.key = "";  // Empty key
    request.src_buffers.push_back(
        CreateBufferDesc("test_segment", 0x1000, 100));

    auto result = rpc_service_->WriteRemoteData(request);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

// Test WriteRemoteData - empty source buffers
TEST_F(ClientRpcServiceTest, WriteRemoteDataEmptyBuffers) {
    RemoteWriteRequest request;
    request.key = "test_key";
    // Empty src_buffers

    auto result = rpc_service_->WriteRemoteData(request);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

// Test WriteRemoteData - invalid buffer (zero size)
TEST_F(ClientRpcServiceTest, WriteRemoteDataInvalidBufferZeroSize) {
    RemoteWriteRequest request;
    request.key = "test_key";
    request.src_buffers.push_back(
        CreateBufferDesc("test_segment", 0x1000, 0));  // Zero size

    auto result = rpc_service_->WriteRemoteData(request);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

// Test WriteRemoteData - invalid buffer (null address)
TEST_F(ClientRpcServiceTest, WriteRemoteDataInvalidBufferNullAddr) {
    RemoteWriteRequest request;
    request.key = "test_key";
    request.src_buffers.push_back(
        CreateBufferDesc("test_segment", 0, 100));  // Null address

    auto result = rpc_service_->WriteRemoteData(request);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

// Test WriteRemoteData - with tier_id
TEST_F(ClientRpcServiceTest, WriteRemoteDataWithTierId) {
    auto tier_id = GetTierId();

    ASSERT_TRUE(tier_id.has_value()) << "No tier available";

    RemoteWriteRequest request;
    request.key = "test_write_key_with_tier";
    request.src_buffers.push_back(
        CreateBufferDesc("test_segment", 0x1000, 100));
    request.target_tier_id = tier_id;

    // Will fail because TransferEngine is not fully initialized
    auto result = rpc_service_->WriteRemoteData(request);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INTERNAL_ERROR);
}

}  // namespace mooncake

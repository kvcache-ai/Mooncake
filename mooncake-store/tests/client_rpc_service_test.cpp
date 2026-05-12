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
#include "utils/common.h"
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
        auto init_result = InitTieredBackendForTest(*tiered_backend_, config);
        ASSERT_TRUE(init_result.has_value())
            << "Failed to initialize TieredBackend: " << init_result.error();

        // Verify tier was created successfully
        auto tier_views = tiered_backend_->GetTierViews();
        ASSERT_EQ(tier_views.size(), 1)
            << "Expected 1 tier, got " << tier_views.size();
        saved_tier_id_ = tier_views[0].id;

        // Create DataManager (MEMCPY mode: no TE endpoint available in unit
        // tests)
        LocalTransferConfig transfer_config;
        transfer_config.mode = LocalTransferMode::MEMCPY;
        transfer_config.local_memcpy_async_worker_num = 32;
        data_manager_ = std::make_unique<DataManager>(
            std::move(tiered_backend_), transfer_engine_,
            /*lock_shard_count=*/1024, transfer_config);

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
    RemoteBufferDesc CreateBufferDesc(const std::string& segment_endpoint,
                                      uintptr_t addr, uint64_t size) {
        RemoteBufferDesc desc;
        desc.segment_endpoint = segment_endpoint;
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
    std::vector<Slice> put_slices{{buffer.get(), test_data.size()}};
    auto put_result = data_manager_->Put(key, put_slices);
    ASSERT_TRUE(put_result.has_value()) << "Put failed";
    put_result.value()->Wait();

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
    EXPECT_EQ(result.error(), ErrorCode::OBJECT_NOT_FOUND);
}

// ============================================================================
// PinKey / UnPinKey (forward read control plane)
// ============================================================================

TEST_F(ClientRpcServiceTest, PinKeyEmptyKey) {
    PinKeyRequest req;
    req.key = "";
    req.target_tier_id = std::nullopt;

    auto result = rpc_service_->PinKey(req);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(ClientRpcServiceTest, PinKeyObjectNotFound) {
    PinKeyRequest req;
    req.key = "rpc_svc_pin_missing_key";
    req.target_tier_id = std::nullopt;

    auto result = rpc_service_->PinKey(req);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::OBJECT_NOT_FOUND);
}

TEST_F(ClientRpcServiceTest, PinKeyAfterPutThenUnPin) {
    const std::string key = "rpc_svc_pin_after_put";
    const std::string blob = "payload";
    auto buf = StringToBuffer(blob);
    std::vector<Slice> slices{{buf.get(), blob.size()}};
    auto put = data_manager_->Put(key, slices);
    ASSERT_TRUE(put.has_value()) << "Put failed";
    put.value()->Wait();

    PinKeyRequest pin_req;
    pin_req.key = key;
    pin_req.target_tier_id = std::nullopt;

    auto pin_res = rpc_service_->PinKey(pin_req);
    ASSERT_TRUE(pin_res.has_value())
        << "PinKey failed: " << static_cast<int>(pin_res.error());
    EXPECT_GT(pin_res->remote_buffer.size, 0u);
    EXPECT_NE(pin_res->pin_token.first, 0u);
    EXPECT_NE(pin_res->pin_token.second, 0u);

    // TransferEngine is not initialized in this unit test, so no TE read
    // occurs; UnPinKey only drives DataManager pin refcount.
    UnPinKeyRequest unpin;
    unpin.key = key;
    unpin.pin_token = pin_res->pin_token;
    auto unpin_res = rpc_service_->UnPinKey(unpin);
    ASSERT_TRUE(unpin_res.has_value())
        << "UnPinKey failed: " << static_cast<int>(unpin_res.error());
}

TEST_F(ClientRpcServiceTest, PinKeyTwiceSameTokenThenUnpinTwice) {
    const std::string key = "rpc_svc_pin_twice_ref";
    const std::string blob = "ref";
    auto buf = StringToBuffer(blob);
    std::vector<Slice> slices{{buf.get(), blob.size()}};
    auto put = data_manager_->Put(key, slices);
    ASSERT_TRUE(put.has_value());
    put.value()->Wait();

    PinKeyRequest pin_req;
    pin_req.key = key;
    pin_req.target_tier_id = std::nullopt;

    auto first = rpc_service_->PinKey(pin_req);
    ASSERT_TRUE(first.has_value())
        << "first PinKey failed: " << static_cast<int>(first.error());
    auto second = rpc_service_->PinKey(pin_req);
    ASSERT_TRUE(second.has_value())
        << "second PinKey failed: " << static_cast<int>(second.error());

    EXPECT_EQ(first->pin_token, second->pin_token);

    // TransferEngine is not initialized in this unit test, so no TE read
    // occurs; UnPinKey only drives DataManager pin refcount.
    UnPinKeyRequest unpin;
    unpin.key = key;
    unpin.pin_token = first->pin_token;
    auto u1 = rpc_service_->UnPinKey(unpin);
    ASSERT_TRUE(u1.has_value())
        << "first UnPinKey failed: " << static_cast<int>(u1.error());

    auto u2 = rpc_service_->UnPinKey(unpin);
    ASSERT_TRUE(u2.has_value())
        << "second UnPinKey failed: " << static_cast<int>(u2.error());
}

TEST_F(ClientRpcServiceTest, PinKeyAfterUnpinNewToken) {
    const std::string key = "rpc_svc_pin_new_token_after_unpin";
    const std::string blob = "tok";
    auto buf = StringToBuffer(blob);
    std::vector<Slice> slices{{buf.get(), blob.size()}};
    auto put = data_manager_->Put(key, slices);
    ASSERT_TRUE(put.has_value());
    put.value()->Wait();

    PinKeyRequest pin_req;
    pin_req.key = key;
    pin_req.target_tier_id = std::nullopt;

    auto pin1 = rpc_service_->PinKey(pin_req);
    ASSERT_TRUE(pin1.has_value())
        << "first PinKey failed: " << static_cast<int>(pin1.error());

    // TransferEngine is not initialized in this unit test, so no TE read
    // occurs; UnPinKey only drives DataManager pin refcount.
    UnPinKeyRequest unpin1;
    unpin1.key = key;
    unpin1.pin_token = pin1->pin_token;
    auto un1 = rpc_service_->UnPinKey(unpin1);
    ASSERT_TRUE(un1.has_value())
        << "first UnPinKey failed: " << static_cast<int>(un1.error());

    auto pin2 = rpc_service_->PinKey(pin_req);
    ASSERT_TRUE(pin2.has_value())
        << "second PinKey after unpin failed: "
        << static_cast<int>(pin2.error());
    EXPECT_NE(pin1->pin_token, pin2->pin_token);

    UnPinKeyRequest unpin2;
    unpin2.key = key;
    unpin2.pin_token = pin2->pin_token;
    auto un2 = rpc_service_->UnPinKey(unpin2);
    ASSERT_TRUE(un2.has_value())
        << "second UnPinKey failed: " << static_cast<int>(un2.error());
}

TEST_F(ClientRpcServiceTest, UnPinKeyEmptyKey) {
    UnPinKeyRequest req;
    req.key = "";
    req.pin_token = {1, 2};

    auto result = rpc_service_->UnPinKey(req);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(ClientRpcServiceTest, UnPinKeyZeroToken) {
    UnPinKeyRequest req;
    req.key = "rpc_svc_unpin_zero";
    req.pin_token = {0, 0};

    auto result = rpc_service_->UnPinKey(req);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(ClientRpcServiceTest, UnPinKeyWrongTokenAfterPin) {
    const std::string key = "rpc_svc_unpin_wrong_token";
    const std::string blob = "x";
    auto buf = StringToBuffer(blob);
    std::vector<Slice> slices{{buf.get(), blob.size()}};
    auto put = data_manager_->Put(key, slices);
    ASSERT_TRUE(put.has_value());
    put.value()->Wait();

    PinKeyRequest pin_req;
    pin_req.key = key;
    pin_req.target_tier_id = std::nullopt;
    auto pin_res = rpc_service_->PinKey(pin_req);
    ASSERT_TRUE(pin_res.has_value());

    UnPinKeyRequest bad;
    bad.key = key;
    bad.pin_token = pin_res->pin_token;
    bad.pin_token.first += 1;
    auto bad_res = rpc_service_->UnPinKey(bad);
    ASSERT_FALSE(bad_res.has_value());
    EXPECT_EQ(bad_res.error(), ErrorCode::INVALID_READ);

    UnPinKeyRequest ok;
    ok.key = key;
    ok.pin_token = pin_res->pin_token;
    auto ok_res = rpc_service_->UnPinKey(ok);
    ASSERT_TRUE(ok_res.has_value())
        << "UnPinKey with correct token failed: "
        << static_cast<int>(ok_res.error());
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

// ============================================================================
// WriteRevoke
// ============================================================================

TEST_F(ClientRpcServiceTest, WriteRevokeInvalidKey) {
    WriteRevokeRequest request;
    request.key = "";
    request.pending_write_token = {1, 0};

    auto result = rpc_service_->WriteRevoke(request);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(ClientRpcServiceTest, WriteRevokeInvalidZeroToken) {
    const std::string key = "rpc_svc_revoke_zero_token";
    WriteRevokeRequest request;
    request.key = key;
    request.pending_write_token = {0, 0};

    auto result = rpc_service_->WriteRevoke(request);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(ClientRpcServiceTest, WriteRevokeIdempotentNoPendingRecord) {
    const std::string key = "rpc_svc_revoke_no_pending";
    WriteRevokeRequest request;
    request.key = key;
    request.pending_write_token = {100, 200};

    auto result = rpc_service_->WriteRevoke(request);
    ASSERT_TRUE(result.has_value())
        << "WriteRevoke on missing pending should be OK (idempotent)";
}

TEST_F(ClientRpcServiceTest, WriteRevokeAfterPreWrite) {
    auto tier_id = GetTierId();
    ASSERT_TRUE(tier_id.has_value()) << "No tier available";

    const std::string key = "rpc_svc_revoke_after_prewrite";
    PreWriteRequest pre;
    pre.key = key;
    pre.size_bytes = 256;
    pre.target_tier_id = tier_id;

    auto pre_res = rpc_service_->PreWrite(pre);
    ASSERT_TRUE(pre_res.has_value())
        << "PreWrite failed: " << static_cast<int>(pre_res.error());

    WriteRevokeRequest revoke;
    revoke.key = key;
    revoke.pending_write_token = pre_res->pending_write_token;
    auto rev_res = rpc_service_->WriteRevoke(revoke);
    ASSERT_TRUE(rev_res.has_value())
        << "WriteRevoke failed: " << static_cast<int>(rev_res.error());

    auto again = rpc_service_->WriteRevoke(revoke);
    ASSERT_TRUE(again.has_value())
        << "Second WriteRevoke on same key/token should be idempotent OK";
}

TEST_F(ClientRpcServiceTest, WriteRevokeTokenMismatch) {
    auto tier_id = GetTierId();
    ASSERT_TRUE(tier_id.has_value()) << "No tier available";

    const std::string key = "rpc_svc_revoke_token_mismatch";
    PreWriteRequest pre;
    pre.key = key;
    pre.size_bytes = 64;
    pre.target_tier_id = tier_id;

    auto pre_res = rpc_service_->PreWrite(pre);
    ASSERT_TRUE(pre_res.has_value());

    UUID wrong_token = pre_res->pending_write_token;
    wrong_token.first += 1;

    WriteRevokeRequest bad;
    bad.key = key;
    bad.pending_write_token = wrong_token;
    auto bad_res = rpc_service_->WriteRevoke(bad);
    ASSERT_FALSE(bad_res.has_value());
    EXPECT_EQ(bad_res.error(), ErrorCode::INVALID_WRITE);

    WriteRevokeRequest good;
    good.key = key;
    good.pending_write_token = pre_res->pending_write_token;
    auto good_res = rpc_service_->WriteRevoke(good);
    ASSERT_TRUE(good_res.has_value());
}

}  // namespace mooncake

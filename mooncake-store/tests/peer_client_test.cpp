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
        LocalTransferConfig local_transfer_config;
        local_transfer_config.mode = LocalTransferMode::MEMCPY;
        local_transfer_config.local_memcpy_async_worker_num = 32;
        data_manager_ = std::make_unique<DataManager>(
            std::move(tiered_backend_), transfer_engine_,
            /*lock_shard_count=*/1024, local_transfer_config);

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
        std::string endpoint = "127.0.0.1:" + std::to_string(kTestPort);
        auto connect_result = peer_client_->Connect(endpoint);
        ASSERT_TRUE(connect_result.has_value()) << "PeerClient::Connect failed";
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
    EXPECT_EQ(result.error(), ErrorCode::OBJECT_NOT_FOUND);
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
    EXPECT_EQ(result.error(), ErrorCode::OBJECT_NOT_FOUND);
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
    std::vector<Slice> put_slices{{buffer.get(), test_data.size()}};
    auto put_result = data_manager_->Put(key, put_slices);
    ASSERT_TRUE(put_result.has_value()) << "Put failed";
    put_result.value()->Wait();

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
// PinKey / UnPinKey (async) — forward read control plane over PeerClient
// ============================================================================

TEST_F(PeerClientTest, AsyncPinKeyEmptyKey) {
    PinKeyRequest req;
    req.key = "";
    req.target_tier_id = std::nullopt;

    auto result =
        async_simple::coro::syncAwait(peer_client_->AsyncPinKey(req));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, AsyncPinKeyKeyNotFound) {
    PinKeyRequest req;
    req.key = "peer_async_pin_missing_key";
    req.target_tier_id = std::nullopt;

    auto result =
        async_simple::coro::syncAwait(peer_client_->AsyncPinKey(req));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::OBJECT_NOT_FOUND);
}

TEST_F(PeerClientTest, AsyncPinKeyAfterPut) {
    const std::string key = "peer_async_pin_after_put";
    const std::string blob = "payload";
    auto buf = StringToBuffer(blob);
    std::vector<Slice> slices{{buf.get(), blob.size()}};
    auto put = data_manager_->Put(key, slices);
    ASSERT_TRUE(put.has_value()) << "Put failed";
    put.value()->Wait();

    PinKeyRequest req;
    req.key = key;
    req.target_tier_id = std::nullopt;

    auto pin_res =
        async_simple::coro::syncAwait(peer_client_->AsyncPinKey(req));
    ASSERT_TRUE(pin_res.has_value())
        << "AsyncPinKey failed: " << static_cast<int>(pin_res.error());
    EXPECT_GT(pin_res->remote_buffer.size, 0u);
    EXPECT_NE(pin_res->read_operation_id.first, 0u);
    EXPECT_NE(pin_res->read_operation_id.second, 0u);

    // TransferEngine is not initialized in this unit test, so no TE read
    // occurs; UnPinKey only drives DataManager pin refcount via RPC.
    UnPinKeyRequest unpin;
    unpin.key = key;
    unpin.read_operation_id = pin_res->read_operation_id;
    auto unpin_res =
        async_simple::coro::syncAwait(peer_client_->AsyncUnPinKey(unpin));
    ASSERT_TRUE(unpin_res.has_value())
        << "AsyncUnPinKey failed: " << static_cast<int>(unpin_res.error());
}

TEST_F(PeerClientTest, AsyncPinKeyTwiceSameTokenThenUnpinTwice) {
    const std::string key = "peer_async_pin_twice_ref";
    const std::string blob = "ref";
    auto buf = StringToBuffer(blob);
    std::vector<Slice> slices{{buf.get(), blob.size()}};
    auto put = data_manager_->Put(key, slices);
    ASSERT_TRUE(put.has_value());
    put.value()->Wait();

    PinKeyRequest pin_req;
    pin_req.key = key;
    pin_req.target_tier_id = std::nullopt;

    auto first =
        async_simple::coro::syncAwait(peer_client_->AsyncPinKey(pin_req));
    ASSERT_TRUE(first.has_value())
        << "first AsyncPinKey failed: " << static_cast<int>(first.error());
    auto second =
        async_simple::coro::syncAwait(peer_client_->AsyncPinKey(pin_req));
    ASSERT_TRUE(second.has_value())
        << "second AsyncPinKey failed: " << static_cast<int>(second.error());

    EXPECT_EQ(first->read_operation_id, second->read_operation_id);

    // TransferEngine is not initialized in this unit test, so no TE read
    // occurs; UnPinKey only drives DataManager pin refcount via RPC.
    UnPinKeyRequest unpin;
    unpin.key = key;
    unpin.read_operation_id = first->read_operation_id;
    auto u1 =
        async_simple::coro::syncAwait(peer_client_->AsyncUnPinKey(unpin));
    ASSERT_TRUE(u1.has_value())
        << "first AsyncUnPinKey failed: " << static_cast<int>(u1.error());

    auto u2 =
        async_simple::coro::syncAwait(peer_client_->AsyncUnPinKey(unpin));
    ASSERT_TRUE(u2.has_value())
        << "second AsyncUnPinKey failed: " << static_cast<int>(u2.error());
}

TEST_F(PeerClientTest, AsyncPinKeyAfterUnpinNewToken) {
    const std::string key = "peer_async_pin_new_token_after_unpin";
    const std::string blob = "tok";
    auto buf = StringToBuffer(blob);
    std::vector<Slice> slices{{buf.get(), blob.size()}};
    auto put = data_manager_->Put(key, slices);
    ASSERT_TRUE(put.has_value());
    put.value()->Wait();

    PinKeyRequest pin_req;
    pin_req.key = key;
    pin_req.target_tier_id = std::nullopt;

    auto pin1 =
        async_simple::coro::syncAwait(peer_client_->AsyncPinKey(pin_req));
    ASSERT_TRUE(pin1.has_value())
        << "first AsyncPinKey failed: " << static_cast<int>(pin1.error());

    // TransferEngine is not initialized in this unit test, so no TE read
    // occurs; UnPinKey only drives DataManager pin refcount via RPC.
    UnPinKeyRequest unpin1;
    unpin1.key = key;
    unpin1.read_operation_id = pin1->read_operation_id;
    auto un1 =
        async_simple::coro::syncAwait(peer_client_->AsyncUnPinKey(unpin1));
    ASSERT_TRUE(un1.has_value())
        << "first AsyncUnPinKey failed: " << static_cast<int>(un1.error());

    auto pin2 =
        async_simple::coro::syncAwait(peer_client_->AsyncPinKey(pin_req));
    ASSERT_TRUE(pin2.has_value())
        << "second AsyncPinKey after unpin failed: "
        << static_cast<int>(pin2.error());
    EXPECT_NE(pin1->read_operation_id, pin2->read_operation_id);

    UnPinKeyRequest unpin2;
    unpin2.key = key;
    unpin2.read_operation_id = pin2->read_operation_id;
    auto un2 =
        async_simple::coro::syncAwait(peer_client_->AsyncUnPinKey(unpin2));
    ASSERT_TRUE(un2.has_value())
        << "second AsyncUnPinKey failed: " << static_cast<int>(un2.error());
}

TEST_F(PeerClientTest, AsyncUnPinKeyEmptyKey) {
    UnPinKeyRequest req;
    req.key = "";
    req.read_operation_id = {1, 2};

    auto result =
        async_simple::coro::syncAwait(peer_client_->AsyncUnPinKey(req));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, AsyncUnPinKeyZeroToken) {
    const std::string key = "peer_async_unpin_zero_token";
    UnPinKeyRequest req;
    req.key = key;
    req.read_operation_id = {0, 0};

    auto result =
        async_simple::coro::syncAwait(peer_client_->AsyncUnPinKey(req));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, AsyncUnPinKeyWrongTokenAfterPin) {
    const std::string key = "peer_async_unpin_wrong_token";
    const std::string blob = "x";
    auto buf = StringToBuffer(blob);
    std::vector<Slice> slices{{buf.get(), blob.size()}};
    auto put = data_manager_->Put(key, slices);
    ASSERT_TRUE(put.has_value());
    put.value()->Wait();

    PinKeyRequest pin_req;
    pin_req.key = key;
    pin_req.target_tier_id = std::nullopt;
    auto pin_res =
        async_simple::coro::syncAwait(peer_client_->AsyncPinKey(pin_req));
    ASSERT_TRUE(pin_res.has_value());

    UnPinKeyRequest bad;
    bad.key = key;
    bad.read_operation_id = pin_res->read_operation_id;
    bad.read_operation_id.first += 1;
    auto bad_res =
        async_simple::coro::syncAwait(peer_client_->AsyncUnPinKey(bad));
    ASSERT_FALSE(bad_res.has_value());
    EXPECT_EQ(bad_res.error(), ErrorCode::INVALID_READ);

    UnPinKeyRequest ok;
    ok.key = key;
    ok.read_operation_id = pin_res->read_operation_id;
    auto ok_res =
        async_simple::coro::syncAwait(peer_client_->AsyncUnPinKey(ok));
    ASSERT_TRUE(ok_res.has_value())
        << "AsyncUnPinKey with correct token failed: "
        << static_cast<int>(ok_res.error());
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
// PreWrite / WriteCommit / WriteRevoke (async)
// ============================================================================

TEST_F(PeerClientTest, AsyncPreWriteEmptyKey) {
    PreWriteRequest pre;
    pre.key = "";
    pre.size_bytes = 64;
    // target_tier_id optional; invalid key fails before tier selection.

    auto result = async_simple::coro::syncAwait(
        peer_client_->AsyncPreWrite(pre));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, AsyncPreWriteZeroSize) {
    const std::string key = "peer_async_pre_zero_size";
    PreWriteRequest pre;
    pre.key = key;
    pre.size_bytes = 0;

    auto result = async_simple::coro::syncAwait(
        peer_client_->AsyncPreWrite(pre));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, AsyncPreWriteValidRequest) {
    auto tier_id = GetTierId();
    ASSERT_TRUE(tier_id.has_value()) << "No tier available";

    const std::string key = "peer_async_pre_valid";
    PreWriteRequest pre;
    pre.key = key;
    pre.size_bytes = 256;
    pre.target_tier_id = tier_id;

    auto result = async_simple::coro::syncAwait(
        peer_client_->AsyncPreWrite(pre));
    ASSERT_TRUE(result.has_value())
        << "AsyncPreWrite failed: " << static_cast<int>(result.error());
    EXPECT_GT(result->remote_buffer.size, 0u);
    EXPECT_NE(result->write_operation_id.first, 0u);
    EXPECT_NE(result->write_operation_id.second, 0u);

    WriteRevokeRequest revoke;
    revoke.key = key;
    revoke.write_operation_id = result->write_operation_id;
    auto rev = async_simple::coro::syncAwait(
        peer_client_->AsyncWriteRevoke(revoke));
    ASSERT_TRUE(rev.has_value())
        << "Cleanup AsyncWriteRevoke failed: " << static_cast<int>(rev.error());
}

TEST_F(PeerClientTest, AsyncPreWriteWhenObjectAlreadyExists) {
    const std::string key = "peer_async_pre_key_exists";
    const std::string blob = "existing";
    auto buffer = StringToBuffer(blob);
    std::vector<Slice> put_slices{{buffer.get(), blob.size()}};
    auto put_result = data_manager_->Put(key, put_slices);
    ASSERT_TRUE(put_result.has_value()) << "Put failed";
    put_result.value()->Wait();

    PreWriteRequest pre;
    pre.key = key;
    pre.size_bytes = 128;

    auto result = async_simple::coro::syncAwait(
        peer_client_->AsyncPreWrite(pre));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::OBJECT_ALREADY_EXISTS);
}

TEST_F(PeerClientTest, AsyncWriteCommitAfterPreWrite) {
    auto tier_id = GetTierId();
    ASSERT_TRUE(tier_id.has_value()) << "No tier available";

    const std::string key = "peer_async_commit_after_pre";
    PreWriteRequest pre;
    pre.key = key;
    pre.size_bytes = 256;
    pre.target_tier_id = tier_id;

    auto pre_res =
        async_simple::coro::syncAwait(peer_client_->AsyncPreWrite(pre));
    ASSERT_TRUE(pre_res.has_value())
        << "AsyncPreWrite failed: " << static_cast<int>(pre_res.error());

    // TransferEngine is not initialized in this unit test, so no real data-plane
    // write occurs. Assume the access side has already filled the buffer via TE;
    // this case only checks WriteCommit RPC / metadata outcome.
    WriteCommitRequest commit;
    commit.key = key;
    commit.write_operation_id = pre_res->write_operation_id;
    auto commit_res = async_simple::coro::syncAwait(
        peer_client_->AsyncWriteCommit(commit));
    ASSERT_TRUE(commit_res.has_value())
        << "AsyncWriteCommit failed: " << static_cast<int>(commit_res.error());
}

TEST_F(PeerClientTest, AsyncWriteCommitEmptyKey) {
    WriteCommitRequest commit;
    commit.key = "";
    commit.write_operation_id = {1, 2};

    auto result = async_simple::coro::syncAwait(
        peer_client_->AsyncWriteCommit(commit));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, AsyncWriteCommitZeroToken) {
    const std::string key = "peer_async_commit_zero_token";
    WriteCommitRequest commit;
    commit.key = key;
    commit.write_operation_id = {0, 0};

    auto result = async_simple::coro::syncAwait(
        peer_client_->AsyncWriteCommit(commit));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, AsyncWriteCommitTokenMismatchAfterPreWrite) {
    auto tier_id = GetTierId();
    ASSERT_TRUE(tier_id.has_value()) << "No tier available";

    const std::string key = "peer_async_commit_token_bad";
    PreWriteRequest pre;
    pre.key = key;
    pre.size_bytes = 128;
    pre.target_tier_id = tier_id;

    auto pre_res =
        async_simple::coro::syncAwait(peer_client_->AsyncPreWrite(pre));
    ASSERT_TRUE(pre_res.has_value());

    WriteCommitRequest commit;
    commit.key = key;
    UUID wrong_token = pre_res->write_operation_id;
    wrong_token.first += 1;
    commit.write_operation_id = wrong_token;

    auto bad = async_simple::coro::syncAwait(
        peer_client_->AsyncWriteCommit(commit));
    ASSERT_FALSE(bad.has_value());
    EXPECT_EQ(bad.error(), ErrorCode::INVALID_WRITE);

    WriteRevokeRequest revoke;
    revoke.key = key;
    revoke.write_operation_id = pre_res->write_operation_id;
    auto rev = async_simple::coro::syncAwait(
        peer_client_->AsyncWriteRevoke(revoke));
    ASSERT_TRUE(rev.has_value());
}

TEST_F(PeerClientTest, AsyncWriteRevokeEmptyKey) {
    WriteRevokeRequest request;
    request.key = "";
    request.write_operation_id = {1, 2};

    auto result = async_simple::coro::syncAwait(
        peer_client_->AsyncWriteRevoke(request));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, AsyncWriteRevokeZeroToken) {
    const std::string key = "peer_async_revoke_zero_token";
    WriteRevokeRequest request;
    request.key = key;
    request.write_operation_id = {0, 0};

    auto result = async_simple::coro::syncAwait(
        peer_client_->AsyncWriteRevoke(request));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, AsyncPreWriteThenWriteRevokeIdempotent) {
    auto tier_id = GetTierId();
    ASSERT_TRUE(tier_id.has_value()) << "No tier available";

    const std::string key = "peer_async_revoke_after_prewrite";
    PreWriteRequest pre;
    pre.key = key;
    pre.size_bytes = 256;
    pre.target_tier_id = tier_id;

    auto pre_res =
        async_simple::coro::syncAwait(peer_client_->AsyncPreWrite(pre));
    ASSERT_TRUE(pre_res.has_value())
        << "AsyncPreWrite failed: " << static_cast<int>(pre_res.error());

    WriteRevokeRequest revoke;
    revoke.key = key;
    revoke.write_operation_id = pre_res->write_operation_id;
    auto rev_res = async_simple::coro::syncAwait(
        peer_client_->AsyncWriteRevoke(revoke));
    ASSERT_TRUE(rev_res.has_value())
        << "AsyncWriteRevoke failed: " << static_cast<int>(rev_res.error());

    auto idem =
        async_simple::coro::syncAwait(peer_client_->AsyncWriteRevoke(revoke));
    ASSERT_TRUE(idem.has_value())
        << "Second AsyncWriteRevoke should be idempotent OK";
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
    EXPECT_EQ(result.error(), ErrorCode::OBJECT_NOT_FOUND);
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
    std::vector<Slice> put_slices{{buffer.get(), test_data.size()}};
    auto put_result = data_manager_->Put(key, put_slices);
    ASSERT_TRUE(put_result.has_value()) << "Put failed";
    put_result.value()->Wait();

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
// Sync PinKey / UnPinKey (wrappers around async)
// ============================================================================

TEST_F(PeerClientTest, SyncPinKeyEmptyKey) {
    PinKeyRequest req;
    req.key = "";
    req.target_tier_id = std::nullopt;

    auto result = peer_client_->PinKey(req);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, SyncPinKeyAfterPut) {
    const std::string key = "peer_sync_pin_after_put";
    const std::string blob = "sync-payload";
    auto buf = StringToBuffer(blob);
    std::vector<Slice> slices{{buf.get(), blob.size()}};
    auto put = data_manager_->Put(key, slices);
    ASSERT_TRUE(put.has_value());
    put.value()->Wait();

    PinKeyRequest req;
    req.key = key;
    req.target_tier_id = std::nullopt;

    auto pin_res = peer_client_->PinKey(req);
    ASSERT_TRUE(pin_res.has_value())
        << "PinKey failed: " << static_cast<int>(pin_res.error());
    EXPECT_GT(pin_res->remote_buffer.size, 0u);

    // TransferEngine is not initialized in this unit test, so no TE read
    // occurs; UnPinKey only drives DataManager pin refcount via RPC.
    UnPinKeyRequest unpin;
    unpin.key = key;
    unpin.read_operation_id = pin_res->read_operation_id;
    auto unpin_res = peer_client_->UnPinKey(unpin);
    ASSERT_TRUE(unpin_res.has_value())
        << "UnPinKey failed: " << static_cast<int>(unpin_res.error());
}

TEST_F(PeerClientTest, SyncPinKeyTwiceSameTokenThenUnpinTwice) {
    const std::string key = "peer_sync_pin_twice_ref";
    const std::string blob = "ref";
    auto buf = StringToBuffer(blob);
    std::vector<Slice> slices{{buf.get(), blob.size()}};
    auto put = data_manager_->Put(key, slices);
    ASSERT_TRUE(put.has_value());
    put.value()->Wait();

    PinKeyRequest pin_req;
    pin_req.key = key;
    pin_req.target_tier_id = std::nullopt;

    auto first = peer_client_->PinKey(pin_req);
    ASSERT_TRUE(first.has_value())
        << "first PinKey failed: " << static_cast<int>(first.error());
    auto second = peer_client_->PinKey(pin_req);
    ASSERT_TRUE(second.has_value())
        << "second PinKey failed: " << static_cast<int>(second.error());

    EXPECT_EQ(first->read_operation_id, second->read_operation_id);

    // TransferEngine is not initialized in this unit test, so no TE read
    // occurs; UnPinKey only drives DataManager pin refcount via RPC.
    UnPinKeyRequest unpin;
    unpin.key = key;
    unpin.read_operation_id = first->read_operation_id;
    auto u1 = peer_client_->UnPinKey(unpin);
    ASSERT_TRUE(u1.has_value())
        << "first UnPinKey failed: " << static_cast<int>(u1.error());

    auto u2 = peer_client_->UnPinKey(unpin);
    ASSERT_TRUE(u2.has_value())
        << "second UnPinKey failed: " << static_cast<int>(u2.error());
}

TEST_F(PeerClientTest, SyncPinKeyAfterUnpinNewToken) {
    const std::string key = "peer_sync_pin_new_token_after_unpin";
    const std::string blob = "tok";
    auto buf = StringToBuffer(blob);
    std::vector<Slice> slices{{buf.get(), blob.size()}};
    auto put = data_manager_->Put(key, slices);
    ASSERT_TRUE(put.has_value());
    put.value()->Wait();

    PinKeyRequest pin_req;
    pin_req.key = key;
    pin_req.target_tier_id = std::nullopt;

    auto pin1 = peer_client_->PinKey(pin_req);
    ASSERT_TRUE(pin1.has_value())
        << "first PinKey failed: " << static_cast<int>(pin1.error());

    // TransferEngine is not initialized in this unit test, so no TE read
    // occurs; UnPinKey only drives DataManager pin refcount via RPC.
    UnPinKeyRequest unpin1;
    unpin1.key = key;
    unpin1.read_operation_id = pin1->read_operation_id;
    auto un1 = peer_client_->UnPinKey(unpin1);
    ASSERT_TRUE(un1.has_value())
        << "first UnPinKey failed: " << static_cast<int>(un1.error());

    auto pin2 = peer_client_->PinKey(pin_req);
    ASSERT_TRUE(pin2.has_value())
        << "second PinKey after unpin failed: "
        << static_cast<int>(pin2.error());
    EXPECT_NE(pin1->read_operation_id, pin2->read_operation_id);

    UnPinKeyRequest unpin2;
    unpin2.key = key;
    unpin2.read_operation_id = pin2->read_operation_id;
    auto un2 = peer_client_->UnPinKey(unpin2);
    ASSERT_TRUE(un2.has_value())
        << "second UnPinKey failed: " << static_cast<int>(un2.error());
}

TEST_F(PeerClientTest, SyncUnPinKeyZeroToken) {
    UnPinKeyRequest req;
    req.key = "peer_sync_unpin_zero";
    req.read_operation_id = {0, 0};

    auto result = peer_client_->UnPinKey(req);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
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
// Sync PreWrite / WriteCommit / WriteRevoke
// ============================================================================

TEST_F(PeerClientTest, SyncPreWriteEmptyKey) {
    PreWriteRequest pre;
    pre.key = "";
    pre.size_bytes = 32;

    auto result = peer_client_->PreWrite(pre);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, SyncWriteCommitEmptyKey) {
    WriteCommitRequest commit;
    commit.key = "";
    commit.write_operation_id = {5, 6};

    auto result = peer_client_->WriteCommit(commit);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, SyncWriteCommitAfterPreWrite) {
    auto tier_id = GetTierId();
    ASSERT_TRUE(tier_id.has_value()) << "No tier available";

    const std::string key = "peer_sync_commit_after_pre";
    PreWriteRequest pre;
    pre.key = key;
    pre.size_bytes = 128;
    pre.target_tier_id = tier_id;

    auto pre_res = peer_client_->PreWrite(pre);
    ASSERT_TRUE(pre_res.has_value())
        << "PreWrite failed: " << static_cast<int>(pre_res.error());

    // TransferEngine is not initialized in this unit test, so no real data-plane
    // write occurs. Assume the access side has already filled the buffer via TE;
    // this case only checks WriteCommit RPC / metadata outcome.
    WriteCommitRequest commit;
    commit.key = key;
    commit.write_operation_id = pre_res->write_operation_id;
    auto commit_res = peer_client_->WriteCommit(commit);
    ASSERT_TRUE(commit_res.has_value())
        << "WriteCommit failed: " << static_cast<int>(commit_res.error());
}

TEST_F(PeerClientTest, SyncWriteRevokeEmptyKey) {
    WriteRevokeRequest request;
    request.key = "";
    request.write_operation_id = {3, 4};

    auto result = peer_client_->WriteRevoke(request);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, SyncWriteRevokeAfterPreWrite) {
    auto tier_id = GetTierId();
    ASSERT_TRUE(tier_id.has_value()) << "No tier available";

    const std::string key = "peer_sync_revoke_after_prewrite";
    PreWriteRequest pre;
    pre.key = key;
    pre.size_bytes = 128;
    pre.target_tier_id = tier_id;

    auto pre_res = peer_client_->PreWrite(pre);
    ASSERT_TRUE(pre_res.has_value())
        << "PreWrite failed: " << static_cast<int>(pre_res.error());

    WriteRevokeRequest revoke;
    revoke.key = key;
    revoke.write_operation_id = pre_res->write_operation_id;
    auto rev_res = peer_client_->WriteRevoke(revoke);
    ASSERT_TRUE(rev_res.has_value())
        << "WriteRevoke failed: " << static_cast<int>(rev_res.error());
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

TEST_F(PeerClientTest, AsyncPinKeyWithoutConnect) {
    PeerClient unconnected_client;

    PinKeyRequest req;
    req.key = "k";
    req.target_tier_id = std::nullopt;

    auto result =
        async_simple::coro::syncAwait(unconnected_client.AsyncPinKey(req));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::RPC_FAIL);
}

TEST_F(PeerClientTest, AsyncUnPinKeyWithoutConnect) {
    PeerClient unconnected_client;

    UnPinKeyRequest req;
    req.key = "k";
    req.read_operation_id = {1, 1};

    auto result =
        async_simple::coro::syncAwait(unconnected_client.AsyncUnPinKey(req));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::RPC_FAIL);
}

TEST_F(PeerClientTest, SyncPinKeyWithoutConnect) {
    PeerClient unconnected_client;

    PinKeyRequest req;
    req.key = "k";
    req.target_tier_id = std::nullopt;

    auto result = unconnected_client.PinKey(req);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::RPC_FAIL);
}

TEST_F(PeerClientTest, SyncUnPinKeyWithoutConnect) {
    PeerClient unconnected_client;

    UnPinKeyRequest req;
    req.key = "k";
    req.read_operation_id = {1, 1};

    auto result = unconnected_client.UnPinKey(req);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::RPC_FAIL);
}

TEST_F(PeerClientTest, AsyncWriteRevokeWithoutConnect) {
    PeerClient unconnected_client;

    WriteRevokeRequest request;
    request.key = "test_key";
    request.write_operation_id = {1, 1};

    auto result = async_simple::coro::syncAwait(
        unconnected_client.AsyncWriteRevoke(request));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::RPC_FAIL);
}

TEST_F(PeerClientTest, SyncWriteRevokeWithoutConnect) {
    PeerClient unconnected_client;

    WriteRevokeRequest request;
    request.key = "test_key";
    request.write_operation_id = {2, 2};

    auto result = unconnected_client.WriteRevoke(request);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::RPC_FAIL);
}

}  // namespace mooncake

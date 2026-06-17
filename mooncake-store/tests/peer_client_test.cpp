#include <glog/logging.h>
#include <gtest/gtest.h>

#include <future>
#include <memory>
#include <csignal>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <async_simple/coro/SyncAwait.h>

#include "peer_client_process_test_helper.h"
#include "peer_client.h"

namespace mooncake {

class PeerClientTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("PeerClientTest");
        FLAGS_logtostderr = 1;
        StartConnectedServer();
    }

    void TearDown() override {
        peer_client_.reset();
        server_process_.Stop();
        CleanupStateFile();
        google::ShutdownGoogleLogging();
    }

    void StartConnectedServer(
        const std::optional<std::string>& pre_put_key = std::nullopt,
        const std::optional<std::string>& pre_put_data = std::nullopt) {
        peer_client_.reset();
        server_process_.Stop();
        CleanupStateFile();

        state_file_path_ = testing::MakeTempStateFilePath();
        ASSERT_TRUE(
            server_process_.Start(pre_put_key, pre_put_data, state_file_path_))
            << "Failed to start rpc-server child process";

        peer_client_ = std::make_unique<PeerClient>();
        auto connect_result = peer_client_->Connect(server_process_.endpoint());
        ASSERT_TRUE(connect_result.has_value()) << "PeerClient::Connect failed";
    }

    void RestartServerWithPrePut(const std::string& key,
                                 const std::string& data) {
        StartConnectedServer(key, data);
    }

    std::optional<UUID> GetTierId() const {
        if (!state_file_path_.has_value()) {
            return std::nullopt;
        }
        return testing::ReadTierIdFromStateFile(*state_file_path_);
    }

    RemoteBufferDesc CreateBufferDesc(const std::string& endpoint,
                                      uintptr_t addr, uint64_t size) const {
        RemoteBufferDesc desc;
        desc.segment_endpoint = endpoint;
        desc.addr = addr;
        desc.size = size;
        return desc;
    }

    RemoteReadRequest MakeReadRequest(const std::string& key, uintptr_t addr,
                                      uint64_t size) const {
        RemoteReadRequest request;
        request.key = key;
        request.dest_buffers.push_back(
            CreateBufferDesc("test_segment", addr, size));
        return request;
    }

    RemoteWriteRequest MakeWriteRequest(const std::string& key, uintptr_t addr,
                                        uint64_t size) const {
        RemoteWriteRequest request;
        request.key = key;
        request.src_buffers.push_back(
            CreateBufferDesc("test_segment", addr, size));
        return request;
    }

    PinKeyRequest MakePinRequest(const std::string& key) const {
        PinKeyRequest req;
        req.key = key;
        req.target_tier_id = std::nullopt;
        return req;
    }

    UnPinKeyRequest MakeUnpinRequest(const std::string& key, UUID token) const {
        UnPinKeyRequest req;
        req.key = key;
        req.read_operation_id = token;
        return req;
    }

    PreWriteRequest MakePreWriteRequest(
        const std::string& key, size_t size,
        std::optional<UUID> tier = std::nullopt) const {
        PreWriteRequest req;
        req.key = key;
        req.size_bytes = size;
        req.target_tier_id = tier;
        return req;
    }

    void ExpectKeyExists(const std::string& key) {
        auto pin_res = peer_client_->PinKey(MakePinRequest(key));
        ASSERT_TRUE(pin_res.has_value()) << "Expected key to exist: " << key;

        auto unpin_res = peer_client_->UnPinKey(
            MakeUnpinRequest(key, pin_res->read_operation_id));
        ASSERT_TRUE(unpin_res.has_value())
            << "Failed to unpin verification key: " << key;
    }

   private:
    void CleanupStateFile() {
        if (!state_file_path_.has_value()) {
            return;
        }
        std::error_code ec;
        std::filesystem::remove(*state_file_path_, ec);
        state_file_path_.reset();
    }

   protected:
    std::unique_ptr<PeerClient> peer_client_;
    testing::ScopedPeerClientRpcServerProcess server_process_;
    std::optional<std::string> state_file_path_;
};

// ============================================================================
// Connect Tests
// ============================================================================

TEST_F(PeerClientTest, ConnectSuccess) {
    auto result = peer_client_->ReadRemoteData(
        MakeReadRequest("connect_test_key", 0x1000, 100));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::OBJECT_NOT_FOUND);
}

// ============================================================================
// AsyncReadRemoteData Tests
// ============================================================================

TEST_F(PeerClientTest, AsyncReadRemoteDataKeyNotFound) {
    auto result =
        async_simple::coro::syncAwait(peer_client_->AsyncReadRemoteData(
            MakeReadRequest("non_existent_key", 0x1000, 100)));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::OBJECT_NOT_FOUND);
}

TEST_F(PeerClientTest, AsyncReadRemoteDataEmptyKey) {
    auto result = async_simple::coro::syncAwait(
        peer_client_->AsyncReadRemoteData(MakeReadRequest("", 0x1000, 100)));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, AsyncReadRemoteDataEmptyBuffers) {
    RemoteReadRequest request;
    request.key = "test_key";
    auto result = async_simple::coro::syncAwait(
        peer_client_->AsyncReadRemoteData(request));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, AsyncReadRemoteDataInvalidBufferZeroSize) {
    auto result =
        async_simple::coro::syncAwait(peer_client_->AsyncReadRemoteData(
            MakeReadRequest("test_key", 0x1000, 0)));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, AsyncReadRemoteDataInvalidBufferNullAddr) {
    auto result = async_simple::coro::syncAwait(
        peer_client_->AsyncReadRemoteData(MakeReadRequest("test_key", 0, 100)));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, AsyncReadRemoteDataWithExistingKey) {
    const std::string key = "async_read_key";
    const std::string data = "Hello, Async!";
    RestartServerWithPrePut(key, data);

    auto result =
        async_simple::coro::syncAwait(peer_client_->AsyncReadRemoteData(
            MakeReadRequest(key, 0x1000, data.size())));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INTERNAL_ERROR);
}

// ============================================================================
// PinKey / UnPinKey (async)
// ============================================================================

TEST_F(PeerClientTest, AsyncPinKeyEmptyKey) {
    auto result = async_simple::coro::syncAwait(
        peer_client_->AsyncPinKey(MakePinRequest("")));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, AsyncPinKeyKeyNotFound) {
    auto result = async_simple::coro::syncAwait(peer_client_->AsyncPinKey(
        MakePinRequest("peer_async_pin_missing_key")));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::OBJECT_NOT_FOUND);
}

TEST_F(PeerClientTest, AsyncPinKeyAfterPut) {
    const std::string key = "peer_async_pin_after_put";
    RestartServerWithPrePut(key, "payload");

    auto pin_res = async_simple::coro::syncAwait(
        peer_client_->AsyncPinKey(MakePinRequest(key)));
    ASSERT_TRUE(pin_res.has_value())
        << "AsyncPinKey failed: " << static_cast<int>(pin_res.error());
    EXPECT_GT(pin_res->remote_buffer.size, 0u);
    EXPECT_NE(pin_res->read_operation_id.first, 0u);
    EXPECT_NE(pin_res->read_operation_id.second, 0u);

    auto unpin_res = async_simple::coro::syncAwait(peer_client_->AsyncUnPinKey(
        MakeUnpinRequest(key, pin_res->read_operation_id)));
    ASSERT_TRUE(unpin_res.has_value())
        << "AsyncUnPinKey failed: " << static_cast<int>(unpin_res.error());
}

TEST_F(PeerClientTest, AsyncPinKeyTwiceSameTokenThenUnpinTwice) {
    const std::string key = "peer_async_pin_twice_ref";
    RestartServerWithPrePut(key, "ref");

    auto first = async_simple::coro::syncAwait(
        peer_client_->AsyncPinKey(MakePinRequest(key)));
    ASSERT_TRUE(first.has_value())
        << "first AsyncPinKey failed: " << static_cast<int>(first.error());
    auto second = async_simple::coro::syncAwait(
        peer_client_->AsyncPinKey(MakePinRequest(key)));
    ASSERT_TRUE(second.has_value())
        << "second AsyncPinKey failed: " << static_cast<int>(second.error());
    EXPECT_EQ(first->read_operation_id, second->read_operation_id);

    auto u1 = async_simple::coro::syncAwait(peer_client_->AsyncUnPinKey(
        MakeUnpinRequest(key, first->read_operation_id)));
    ASSERT_TRUE(u1.has_value())
        << "first AsyncUnPinKey failed: " << static_cast<int>(u1.error());
    auto u2 = async_simple::coro::syncAwait(peer_client_->AsyncUnPinKey(
        MakeUnpinRequest(key, first->read_operation_id)));
    ASSERT_TRUE(u2.has_value())
        << "second AsyncUnPinKey failed: " << static_cast<int>(u2.error());
}

TEST_F(PeerClientTest, ConcurrentAsyncPinKeySameKey) {
    const std::string key = "peer_concurrent_async_pin";
    RestartServerWithPrePut(key, "concurrent_pin_data");

    std::promise<void> start_promise;
    auto start_future = start_promise.get_future().share();
    std::mutex results_mutex;
    std::vector<tl::expected<PinKeyResponse, ErrorCode>> results(2);

    auto run_pin = [&](size_t index) {
        start_future.wait();
        auto result = async_simple::coro::syncAwait(
            peer_client_->AsyncPinKey(MakePinRequest(key)));
        std::lock_guard<std::mutex> lock(results_mutex);
        results[index] = std::move(result);
    };

    std::thread first(run_pin, 0);
    std::thread second(run_pin, 1);
    start_promise.set_value();
    first.join();
    second.join();

    for (size_t i = 0; i < results.size(); ++i) {
        ASSERT_TRUE(results[i].has_value())
            << "AsyncPinKey thread " << i
            << " failed: " << static_cast<int>(results[i].error());
    }
    EXPECT_EQ(results[0]->read_operation_id, results[1]->read_operation_id);

    auto u1 = async_simple::coro::syncAwait(peer_client_->AsyncUnPinKey(
        MakeUnpinRequest(key, results[0]->read_operation_id)));
    ASSERT_TRUE(u1.has_value())
        << "first AsyncUnPinKey failed: " << static_cast<int>(u1.error());
    auto u2 = async_simple::coro::syncAwait(peer_client_->AsyncUnPinKey(
        MakeUnpinRequest(key, results[0]->read_operation_id)));
    ASSERT_TRUE(u2.has_value())
        << "second AsyncUnPinKey failed: " << static_cast<int>(u2.error());
}

TEST_F(PeerClientTest, AsyncPinKeyAfterUnpinNewToken) {
    const std::string key = "peer_async_pin_new_token_after_unpin";
    RestartServerWithPrePut(key, "tok");

    auto pin1 = async_simple::coro::syncAwait(
        peer_client_->AsyncPinKey(MakePinRequest(key)));
    ASSERT_TRUE(pin1.has_value())
        << "first AsyncPinKey failed: " << static_cast<int>(pin1.error());

    auto un1 = async_simple::coro::syncAwait(peer_client_->AsyncUnPinKey(
        MakeUnpinRequest(key, pin1->read_operation_id)));
    ASSERT_TRUE(un1.has_value())
        << "first AsyncUnPinKey failed: " << static_cast<int>(un1.error());

    auto pin2 = async_simple::coro::syncAwait(
        peer_client_->AsyncPinKey(MakePinRequest(key)));
    ASSERT_TRUE(pin2.has_value()) << "second AsyncPinKey after unpin failed: "
                                  << static_cast<int>(pin2.error());
    EXPECT_NE(pin1->read_operation_id, pin2->read_operation_id);

    auto un2 = async_simple::coro::syncAwait(peer_client_->AsyncUnPinKey(
        MakeUnpinRequest(key, pin2->read_operation_id)));
    ASSERT_TRUE(un2.has_value())
        << "second AsyncUnPinKey failed: " << static_cast<int>(un2.error());
}

TEST_F(PeerClientTest, AsyncUnPinKeyEmptyKey) {
    auto result = async_simple::coro::syncAwait(
        peer_client_->AsyncUnPinKey(MakeUnpinRequest("", {1, 2})));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, AsyncUnPinKeyZeroToken) {
    auto result = async_simple::coro::syncAwait(peer_client_->AsyncUnPinKey(
        MakeUnpinRequest("peer_async_unpin_zero_token", {0, 0})));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, AsyncUnPinKeyWrongTokenAfterPin) {
    const std::string key = "peer_async_unpin_wrong_token";
    RestartServerWithPrePut(key, "x");

    auto pin_res = async_simple::coro::syncAwait(
        peer_client_->AsyncPinKey(MakePinRequest(key)));
    ASSERT_TRUE(pin_res.has_value());

    auto bad_token = pin_res->read_operation_id;
    bad_token.first += 1;
    auto bad_res = async_simple::coro::syncAwait(
        peer_client_->AsyncUnPinKey(MakeUnpinRequest(key, bad_token)));
    ASSERT_FALSE(bad_res.has_value());
    EXPECT_EQ(bad_res.error(), ErrorCode::INVALID_READ);

    auto ok_res = async_simple::coro::syncAwait(peer_client_->AsyncUnPinKey(
        MakeUnpinRequest(key, pin_res->read_operation_id)));
    ASSERT_TRUE(ok_res.has_value())
        << "AsyncUnPinKey with correct token failed: "
        << static_cast<int>(ok_res.error());
}

// ============================================================================
// AsyncWriteRemoteData Tests
// ============================================================================

TEST_F(PeerClientTest, AsyncWriteRemoteDataEmptyKey) {
    auto result = async_simple::coro::syncAwait(
        peer_client_->AsyncWriteRemoteData(MakeWriteRequest("", 0x1000, 100)));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, AsyncWriteRemoteDataEmptyBuffers) {
    RemoteWriteRequest request;
    request.key = "test_key";
    auto result = async_simple::coro::syncAwait(
        peer_client_->AsyncWriteRemoteData(request));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, AsyncWriteRemoteDataInvalidBufferZeroSize) {
    auto result =
        async_simple::coro::syncAwait(peer_client_->AsyncWriteRemoteData(
            MakeWriteRequest("test_key", 0x1000, 0)));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, AsyncWriteRemoteDataInvalidBufferNullAddr) {
    auto result =
        async_simple::coro::syncAwait(peer_client_->AsyncWriteRemoteData(
            MakeWriteRequest("test_key", 0, 100)));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, AsyncWriteRemoteDataValidRequest) {
    auto request = MakeWriteRequest("async_write_key", 0x1000, 100);
    request.target_tier_id = std::nullopt;
    auto result = async_simple::coro::syncAwait(
        peer_client_->AsyncWriteRemoteData(request));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INTERNAL_ERROR);
}

TEST_F(PeerClientTest, AsyncWriteRemoteDataWithTierId) {
    auto tier_id = GetTierId();
    ASSERT_TRUE(tier_id.has_value()) << "No tier available";

    auto request = MakeWriteRequest("async_write_key_with_tier", 0x1000, 100);
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
    auto result = async_simple::coro::syncAwait(
        peer_client_->AsyncPreWrite(MakePreWriteRequest("", 64)));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, AsyncPreWriteZeroSize) {
    auto result = async_simple::coro::syncAwait(peer_client_->AsyncPreWrite(
        MakePreWriteRequest("peer_async_pre_zero_size", 0)));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, AsyncPreWriteValidRequest) {
    auto tier_id = GetTierId();
    ASSERT_TRUE(tier_id.has_value()) << "No tier available";

    const std::string key = "peer_async_pre_valid";
    auto result = async_simple::coro::syncAwait(
        peer_client_->AsyncPreWrite(MakePreWriteRequest(key, 256, tier_id)));
    ASSERT_TRUE(result.has_value())
        << "AsyncPreWrite failed: " << static_cast<int>(result.error());
    EXPECT_GT(result->remote_buffer.size, 0u);
    EXPECT_NE(result->write_operation_id.first, 0u);
    EXPECT_NE(result->write_operation_id.second, 0u);

    WriteRevokeRequest revoke;
    revoke.key = key;
    revoke.write_operation_id = result->write_operation_id;
    auto rev =
        async_simple::coro::syncAwait(peer_client_->AsyncWriteRevoke(revoke));
    ASSERT_TRUE(rev.has_value())
        << "Cleanup AsyncWriteRevoke failed: " << static_cast<int>(rev.error());
}

TEST_F(PeerClientTest, AsyncPreWriteWhenObjectAlreadyExists) {
    const std::string key = "peer_async_pre_key_exists";
    RestartServerWithPrePut(key, "existing");

    auto result = async_simple::coro::syncAwait(
        peer_client_->AsyncPreWrite(MakePreWriteRequest(key, 128)));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::OBJECT_ALREADY_EXISTS);
}

TEST_F(PeerClientTest, ConcurrentAsyncPreWriteSameKey) {
    auto tier_id = GetTierId();
    ASSERT_TRUE(tier_id.has_value()) << "No tier available";

    const std::string key = "peer_concurrent_async_prewrite";
    auto pre = MakePreWriteRequest(key, 256, tier_id);

    std::promise<void> start_promise;
    auto start_future = start_promise.get_future().share();
    std::mutex results_mutex;
    std::vector<tl::expected<PreWriteResponse, ErrorCode>> results(2);

    auto run_prewrite = [&](size_t index) {
        start_future.wait();
        auto result =
            async_simple::coro::syncAwait(peer_client_->AsyncPreWrite(pre));
        std::lock_guard<std::mutex> lock(results_mutex);
        results[index] = std::move(result);
    };

    std::thread first(run_prewrite, 0);
    std::thread second(run_prewrite, 1);
    start_promise.set_value();
    first.join();
    second.join();

    int ok_count = 0;
    int replica_processing_count = 0;
    std::optional<PreWriteResponse> winner;
    for (const auto& result : results) {
        if (result.has_value()) {
            ++ok_count;
            winner = result.value();
        } else if (result.error() == ErrorCode::REPLICA_IS_PROCESSING) {
            ++replica_processing_count;
        }
    }
    ASSERT_EQ(ok_count, 1) << "Exactly one AsyncPreWrite should succeed";
    ASSERT_EQ(replica_processing_count, 1)
        << "The other AsyncPreWrite should be REPLICA_IS_PROCESSING";
    ASSERT_TRUE(winner.has_value());

    WriteCommitRequest commit;
    commit.key = key;
    commit.write_operation_id = winner->write_operation_id;
    auto commit_res =
        async_simple::coro::syncAwait(peer_client_->AsyncWriteCommit(commit));
    ASSERT_TRUE(commit_res.has_value()) << "Winner WriteCommit failed: "
                                        << static_cast<int>(commit_res.error());
}

TEST_F(PeerClientTest, AsyncWriteCommitAfterPreWrite) {
    auto tier_id = GetTierId();
    ASSERT_TRUE(tier_id.has_value()) << "No tier available";

    const std::string key = "peer_async_commit_after_pre";
    auto pre_res = async_simple::coro::syncAwait(
        peer_client_->AsyncPreWrite(MakePreWriteRequest(key, 256, tier_id)));
    ASSERT_TRUE(pre_res.has_value())
        << "AsyncPreWrite failed: " << static_cast<int>(pre_res.error());

    WriteCommitRequest commit;
    commit.key = key;
    commit.write_operation_id = pre_res->write_operation_id;
    auto commit_res =
        async_simple::coro::syncAwait(peer_client_->AsyncWriteCommit(commit));
    ASSERT_TRUE(commit_res.has_value())
        << "AsyncWriteCommit failed: " << static_cast<int>(commit_res.error());

    ExpectKeyExists(key);
}

TEST_F(PeerClientTest, AsyncWriteCommitEmptyKey) {
    WriteCommitRequest commit;
    commit.key = "";
    commit.write_operation_id = {1, 2};
    auto result =
        async_simple::coro::syncAwait(peer_client_->AsyncWriteCommit(commit));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, AsyncWriteCommitZeroToken) {
    WriteCommitRequest commit;
    commit.key = "peer_async_commit_zero_token";
    commit.write_operation_id = {0, 0};
    auto result =
        async_simple::coro::syncAwait(peer_client_->AsyncWriteCommit(commit));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, AsyncWriteCommitTokenMismatchAfterPreWrite) {
    auto tier_id = GetTierId();
    ASSERT_TRUE(tier_id.has_value()) << "No tier available";

    const std::string key = "peer_async_commit_token_bad";
    auto pre_res = async_simple::coro::syncAwait(
        peer_client_->AsyncPreWrite(MakePreWriteRequest(key, 128, tier_id)));
    ASSERT_TRUE(pre_res.has_value());

    auto wrong_token = pre_res->write_operation_id;
    wrong_token.first += 1;
    WriteCommitRequest commit;
    commit.key = key;
    commit.write_operation_id = wrong_token;
    auto bad =
        async_simple::coro::syncAwait(peer_client_->AsyncWriteCommit(commit));
    ASSERT_FALSE(bad.has_value());
    EXPECT_EQ(bad.error(), ErrorCode::INVALID_WRITE);

    WriteRevokeRequest revoke;
    revoke.key = key;
    revoke.write_operation_id = pre_res->write_operation_id;
    auto rev =
        async_simple::coro::syncAwait(peer_client_->AsyncWriteRevoke(revoke));
    ASSERT_TRUE(rev.has_value());
}

TEST_F(PeerClientTest, AsyncWriteRevokeEmptyKey) {
    WriteRevokeRequest request;
    request.key = "";
    request.write_operation_id = {1, 2};
    auto result =
        async_simple::coro::syncAwait(peer_client_->AsyncWriteRevoke(request));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, AsyncWriteRevokeZeroToken) {
    WriteRevokeRequest request;
    request.key = "peer_async_revoke_zero_token";
    request.write_operation_id = {0, 0};
    auto result =
        async_simple::coro::syncAwait(peer_client_->AsyncWriteRevoke(request));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, AsyncWriteRevokeClearsPending) {
    auto tier_id = GetTierId();
    ASSERT_TRUE(tier_id.has_value()) << "No tier available";

    const std::string key = "peer_async_revoke_clears_pending";
    auto pre = MakePreWriteRequest(key, 256, tier_id);
    auto pre_res =
        async_simple::coro::syncAwait(peer_client_->AsyncPreWrite(pre));
    ASSERT_TRUE(pre_res.has_value())
        << "First AsyncPreWrite failed: " << static_cast<int>(pre_res.error());

    auto blocked_pre =
        async_simple::coro::syncAwait(peer_client_->AsyncPreWrite(pre));
    ASSERT_FALSE(blocked_pre.has_value());
    EXPECT_EQ(blocked_pre.error(), ErrorCode::REPLICA_IS_PROCESSING);

    WriteRevokeRequest revoke;
    revoke.key = key;
    revoke.write_operation_id = pre_res->write_operation_id;
    auto rev_res =
        async_simple::coro::syncAwait(peer_client_->AsyncWriteRevoke(revoke));
    ASSERT_TRUE(rev_res.has_value())
        << "AsyncWriteRevoke failed: " << static_cast<int>(rev_res.error());

    auto retry_pre =
        async_simple::coro::syncAwait(peer_client_->AsyncPreWrite(pre));
    ASSERT_TRUE(retry_pre.has_value()) << "AsyncPreWrite after revoke failed: "
                                       << static_cast<int>(retry_pre.error());

    WriteRevokeRequest cleanup;
    cleanup.key = key;
    cleanup.write_operation_id = retry_pre->write_operation_id;
    auto cleanup_res =
        async_simple::coro::syncAwait(peer_client_->AsyncWriteRevoke(cleanup));
    ASSERT_TRUE(cleanup_res.has_value())
        << "Cleanup AsyncWriteRevoke failed: "
        << static_cast<int>(cleanup_res.error());
}

// ============================================================================
// Sync ReadRemoteData Tests
// ============================================================================

TEST_F(PeerClientTest, SyncReadRemoteDataKeyNotFound) {
    auto result = peer_client_->ReadRemoteData(
        MakeReadRequest("non_existent_key", 0x1000, 100));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::OBJECT_NOT_FOUND);
}

TEST_F(PeerClientTest, SyncReadRemoteDataEmptyKey) {
    auto result =
        peer_client_->ReadRemoteData(MakeReadRequest("", 0x1000, 100));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, SyncReadRemoteDataEmptyBuffers) {
    RemoteReadRequest request;
    request.key = "test_key";
    auto result = peer_client_->ReadRemoteData(request);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, SyncReadRemoteDataWithExistingKey) {
    const std::string key = "sync_read_key";
    const std::string data = "Hello, Sync Read!";
    RestartServerWithPrePut(key, data);

    auto result =
        peer_client_->ReadRemoteData(MakeReadRequest(key, 0x1000, data.size()));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INTERNAL_ERROR);
}

// ============================================================================
// Sync PinKey / UnPinKey
// ============================================================================

TEST_F(PeerClientTest, SyncPinKeyEmptyKey) {
    auto result = peer_client_->PinKey(MakePinRequest(""));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, SyncPinKeyAfterPut) {
    const std::string key = "peer_sync_pin_after_put";
    RestartServerWithPrePut(key, "sync-payload");

    auto pin_res = peer_client_->PinKey(MakePinRequest(key));
    ASSERT_TRUE(pin_res.has_value())
        << "PinKey failed: " << static_cast<int>(pin_res.error());
    EXPECT_GT(pin_res->remote_buffer.size, 0u);

    auto unpin_res = peer_client_->UnPinKey(
        MakeUnpinRequest(key, pin_res->read_operation_id));
    ASSERT_TRUE(unpin_res.has_value())
        << "UnPinKey failed: " << static_cast<int>(unpin_res.error());
}

TEST_F(PeerClientTest, SyncPinKeyTwiceSameTokenThenUnpinTwice) {
    const std::string key = "peer_sync_pin_twice_ref";
    RestartServerWithPrePut(key, "ref");

    auto first = peer_client_->PinKey(MakePinRequest(key));
    ASSERT_TRUE(first.has_value())
        << "first PinKey failed: " << static_cast<int>(first.error());
    auto second = peer_client_->PinKey(MakePinRequest(key));
    ASSERT_TRUE(second.has_value())
        << "second PinKey failed: " << static_cast<int>(second.error());
    EXPECT_EQ(first->read_operation_id, second->read_operation_id);

    auto u1 =
        peer_client_->UnPinKey(MakeUnpinRequest(key, first->read_operation_id));
    ASSERT_TRUE(u1.has_value())
        << "first UnPinKey failed: " << static_cast<int>(u1.error());
    auto u2 =
        peer_client_->UnPinKey(MakeUnpinRequest(key, first->read_operation_id));
    ASSERT_TRUE(u2.has_value())
        << "second UnPinKey failed: " << static_cast<int>(u2.error());
}

TEST_F(PeerClientTest, SyncPinKeyAfterUnpinNewToken) {
    const std::string key = "peer_sync_pin_new_token_after_unpin";
    RestartServerWithPrePut(key, "tok");

    auto pin1 = peer_client_->PinKey(MakePinRequest(key));
    ASSERT_TRUE(pin1.has_value())
        << "first PinKey failed: " << static_cast<int>(pin1.error());

    auto un1 =
        peer_client_->UnPinKey(MakeUnpinRequest(key, pin1->read_operation_id));
    ASSERT_TRUE(un1.has_value())
        << "first UnPinKey failed: " << static_cast<int>(un1.error());

    auto pin2 = peer_client_->PinKey(MakePinRequest(key));
    ASSERT_TRUE(pin2.has_value()) << "second PinKey after unpin failed: "
                                  << static_cast<int>(pin2.error());
    EXPECT_NE(pin1->read_operation_id, pin2->read_operation_id);

    auto un2 =
        peer_client_->UnPinKey(MakeUnpinRequest(key, pin2->read_operation_id));
    ASSERT_TRUE(un2.has_value())
        << "second UnPinKey failed: " << static_cast<int>(un2.error());
}

TEST_F(PeerClientTest, SyncUnPinKeyZeroToken) {
    auto result = peer_client_->UnPinKey(
        MakeUnpinRequest("peer_sync_unpin_zero", {0, 0}));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

// ============================================================================
// Sync WriteRemoteData Tests
// ============================================================================

TEST_F(PeerClientTest, SyncWriteRemoteDataEmptyKey) {
    auto result =
        peer_client_->WriteRemoteData(MakeWriteRequest("", 0x1000, 100));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, SyncWriteRemoteDataEmptyBuffers) {
    RemoteWriteRequest request;
    request.key = "test_key";
    auto result = peer_client_->WriteRemoteData(request);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, SyncWriteRemoteDataValidRequest) {
    auto request = MakeWriteRequest("sync_write_key", 0x1000, 100);
    request.target_tier_id = std::nullopt;
    auto result = peer_client_->WriteRemoteData(request);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INTERNAL_ERROR);
}

// ============================================================================
// Sync PreWrite / WriteCommit / WriteRevoke
// ============================================================================

TEST_F(PeerClientTest, SyncPreWriteEmptyKey) {
    auto result = peer_client_->PreWrite(MakePreWriteRequest("", 32));
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
    auto pre_res =
        peer_client_->PreWrite(MakePreWriteRequest(key, 128, tier_id));
    ASSERT_TRUE(pre_res.has_value())
        << "PreWrite failed: " << static_cast<int>(pre_res.error());

    WriteCommitRequest commit;
    commit.key = key;
    commit.write_operation_id = pre_res->write_operation_id;
    auto commit_res = peer_client_->WriteCommit(commit);
    ASSERT_TRUE(commit_res.has_value())
        << "WriteCommit failed: " << static_cast<int>(commit_res.error());

    ExpectKeyExists(key);
}

TEST_F(PeerClientTest, SyncWriteRevokeEmptyKey) {
    WriteRevokeRequest request;
    request.key = "";
    request.write_operation_id = {3, 4};
    auto result = peer_client_->WriteRevoke(request);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(PeerClientTest, SyncWriteRevokeClearsPending) {
    auto tier_id = GetTierId();
    ASSERT_TRUE(tier_id.has_value()) << "No tier available";

    const std::string key = "peer_sync_revoke_clears_pending";
    auto pre = MakePreWriteRequest(key, 128, tier_id);
    auto pre_res = peer_client_->PreWrite(pre);
    ASSERT_TRUE(pre_res.has_value())
        << "First PreWrite failed: " << static_cast<int>(pre_res.error());

    auto blocked_pre = peer_client_->PreWrite(pre);
    ASSERT_FALSE(blocked_pre.has_value());
    EXPECT_EQ(blocked_pre.error(), ErrorCode::REPLICA_IS_PROCESSING);

    WriteRevokeRequest revoke;
    revoke.key = key;
    revoke.write_operation_id = pre_res->write_operation_id;
    auto rev_res = peer_client_->WriteRevoke(revoke);
    ASSERT_TRUE(rev_res.has_value())
        << "WriteRevoke failed: " << static_cast<int>(rev_res.error());

    auto retry_pre = peer_client_->PreWrite(pre);
    ASSERT_TRUE(retry_pre.has_value()) << "PreWrite after revoke failed: "
                                       << static_cast<int>(retry_pre.error());

    WriteRevokeRequest cleanup;
    cleanup.key = key;
    cleanup.write_operation_id = retry_pre->write_operation_id;
    auto cleanup_res = peer_client_->WriteRevoke(cleanup);
    ASSERT_TRUE(cleanup_res.has_value())
        << "Cleanup WriteRevoke failed: "
        << static_cast<int>(cleanup_res.error());
}

// ============================================================================
// Not Connected Error Tests
// ============================================================================

TEST_F(PeerClientTest, AsyncReadWithoutConnect) {
    PeerClient unconnected_client;
    auto result =
        async_simple::coro::syncAwait(unconnected_client.AsyncReadRemoteData(
            MakeReadRequest("test_key", 0x1000, 100)));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::RPC_FAIL);
}

TEST_F(PeerClientTest, AsyncWriteWithoutConnect) {
    PeerClient unconnected_client;
    auto result =
        async_simple::coro::syncAwait(unconnected_client.AsyncWriteRemoteData(
            MakeWriteRequest("test_key", 0x1000, 100)));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::RPC_FAIL);
}

TEST_F(PeerClientTest, SyncReadWithoutConnect) {
    PeerClient unconnected_client;
    auto result = unconnected_client.ReadRemoteData(
        MakeReadRequest("test_key", 0x1000, 100));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::RPC_FAIL);
}

TEST_F(PeerClientTest, SyncWriteWithoutConnect) {
    PeerClient unconnected_client;
    auto result = unconnected_client.WriteRemoteData(
        MakeWriteRequest("test_key", 0x1000, 100));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::RPC_FAIL);
}

TEST_F(PeerClientTest, AsyncPinKeyWithoutConnect) {
    PeerClient unconnected_client;
    auto result = async_simple::coro::syncAwait(
        unconnected_client.AsyncPinKey(MakePinRequest("k")));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::RPC_FAIL);
}

TEST_F(PeerClientTest, AsyncUnPinKeyWithoutConnect) {
    PeerClient unconnected_client;
    auto result = async_simple::coro::syncAwait(
        unconnected_client.AsyncUnPinKey(MakeUnpinRequest("k", {1, 1})));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::RPC_FAIL);
}

TEST_F(PeerClientTest, SyncPinKeyWithoutConnect) {
    PeerClient unconnected_client;
    auto result = unconnected_client.PinKey(MakePinRequest("k"));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::RPC_FAIL);
}

TEST_F(PeerClientTest, SyncUnPinKeyWithoutConnect) {
    PeerClient unconnected_client;
    auto result = unconnected_client.UnPinKey(MakeUnpinRequest("k", {1, 1}));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::RPC_FAIL);
}

TEST_F(PeerClientTest, AsyncPreWriteWithoutConnect) {
    PeerClient unconnected_client;
    auto result = async_simple::coro::syncAwait(
        unconnected_client.AsyncPreWrite(MakePreWriteRequest("test_key", 256)));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::RPC_FAIL);
}

TEST_F(PeerClientTest, AsyncWriteCommitWithoutConnect) {
    PeerClient unconnected_client;
    WriteCommitRequest commit;
    commit.key = "test_key";
    commit.write_operation_id = {1, 1};
    auto result = async_simple::coro::syncAwait(
        unconnected_client.AsyncWriteCommit(commit));
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

TEST_F(PeerClientTest, SyncPreWriteWithoutConnect) {
    PeerClient unconnected_client;
    auto result =
        unconnected_client.PreWrite(MakePreWriteRequest("test_key", 256));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::RPC_FAIL);
}

TEST_F(PeerClientTest, SyncWriteCommitWithoutConnect) {
    PeerClient unconnected_client;
    WriteCommitRequest commit;
    commit.key = "test_key";
    commit.write_operation_id = {2, 2};
    auto result = unconnected_client.WriteCommit(commit);
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

int main(int argc, char** argv) {
    mooncake::testing::SetPeerClientTestBinaryPath(argv[0]);
    if (auto child_exit =
            mooncake::testing::MaybeRunPeerClientTestChildProcess(argc, argv);
        child_exit.has_value()) {
        return *child_exit;
    }

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

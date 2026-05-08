#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <thread>
#include <chrono>
#include <vector>

#include "real_client.h"
#include "test_server_helpers.h"

DEFINE_string(protocol, "tcp", "Transfer protocol: rdma|tcp");
DEFINE_string(device_name, "", "Device name to use, valid if protocol=rdma");

namespace mooncake {
namespace testing {

class AsyncBatchGetTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("AsyncBatchGetTest");
        FLAGS_logtostderr = 1;
    }

    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

    void SetUp() override {
        if (getenv("PROTOCOL")) FLAGS_protocol = getenv("PROTOCOL");
        if (getenv("DEVICE_NAME")) FLAGS_device_name = getenv("DEVICE_NAME");

        py_client_ = RealClient::create();

        ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder().build()))
            << "Failed to start in-proc master";
        master_address_ = master_.master_address();

        const std::string rdma_devices = (FLAGS_protocol == std::string("rdma"))
                                             ? FLAGS_device_name
                                             : std::string("");
        ASSERT_EQ(py_client_->setup_real("localhost:17813", "P2PHANDSHAKE",
                                         16 * 1024 * 1024, 16 * 1024 * 1024,
                                         FLAGS_protocol, rdma_devices,
                                         master_address_),
                  0);
    }

    void TearDown() override {
        if (py_client_) {
            py_client_->tearDownAll();
        }
        master_.Stop();
    }

    // Helper: put a key with data
    void PutKey(const std::string& key, const std::string& data) {
        std::span<const char> span(data.data(), data.size());
        ReplicateConfig config;
        config.replica_num = 1;
        int rc = py_client_->put(key, span, config);
        ASSERT_EQ(rc, 0) << "Put failed for key: " << key;
    }

    // Helper: register a buffer and get its pointer
    void* RegisterBuffer(size_t size) {
        void* buf = malloc(size);
        EXPECT_NE(buf, nullptr);
        int rc = py_client_->register_buffer(buf, size);
        EXPECT_EQ(rc, 0);
        return buf;
    }

    void UnregisterAndFree(void* buf) {
        py_client_->unregister_buffer(buf);
        free(buf);
    }

    std::shared_ptr<RealClient> py_client_;
    InProcMaster master_;
    std::string master_address_;
};

// ============================================================================
// Two-phase API tests (batch_get_submit / batch_get_complete)
// ============================================================================

TEST_F(AsyncBatchGetTest, TwoPhase_BasicSingleKey) {
    const std::string key = "async_test_key_1";
    const std::string data = "hello async world";
    PutKey(key, data);

    const size_t buf_size = 1024;
    void* buf = RegisterBuffer(buf_size);

    std::vector<std::string> keys = {key};
    std::vector<void*> buffers = {buf};
    std::vector<size_t> sizes = {buf_size};

    auto state = py_client_->batch_get_submit(keys, buffers, sizes);
    ASSERT_FALSE(state.pending_transfers.empty())
        << "Should have at least one pending transfer";

    auto results = py_client_->batch_get_complete(state);
    ASSERT_EQ(results.size(), 1u);
    EXPECT_EQ(results[0], 0) << "Transfer should succeed";

    // Verify data
    EXPECT_EQ(memcmp(buf, data.data(), data.size()), 0);

    UnregisterAndFree(buf);
}

TEST_F(AsyncBatchGetTest, TwoPhase_MultipleBatchKeys) {
    const int num_keys = 5;
    std::vector<std::string> keys;
    std::vector<std::string> data_vec;
    for (int i = 0; i < num_keys; i++) {
        keys.push_back("async_batch_key_" + std::to_string(i));
        data_vec.push_back("data_for_key_" + std::to_string(i));
        PutKey(keys[i], data_vec[i]);
    }

    const size_t buf_size = 1024;
    std::vector<void*> buffers;
    std::vector<size_t> sizes;
    for (int i = 0; i < num_keys; i++) {
        buffers.push_back(RegisterBuffer(buf_size));
        sizes.push_back(buf_size);
    }

    auto state = py_client_->batch_get_submit(keys, buffers, sizes);
    ASSERT_FALSE(state.pending_transfers.empty());

    auto results = py_client_->batch_get_complete(state);
    ASSERT_EQ(results.size(), static_cast<size_t>(num_keys));

    for (int i = 0; i < num_keys; i++) {
        EXPECT_EQ(results[i], 0) << "Key " << i << " should succeed";
        EXPECT_EQ(memcmp(buffers[i], data_vec[i].data(), data_vec[i].size()), 0)
            << "Data mismatch for key " << i;
    }

    for (auto buf : buffers) UnregisterAndFree(buf);
}

TEST_F(AsyncBatchGetTest, TwoPhase_NonExistentKey) {
    const size_t buf_size = 1024;
    void* buf = RegisterBuffer(buf_size);

    std::vector<std::string> keys = {"nonexistent_key_xyz"};
    std::vector<void*> buffers = {buf};
    std::vector<size_t> sizes = {buf_size};

    auto state = py_client_->batch_get_submit(keys, buffers, sizes);
    // All keys failed in submit, pending_transfers should be empty
    EXPECT_TRUE(state.pending_transfers.empty());

    UnregisterAndFree(buf);
}

TEST_F(AsyncBatchGetTest, TwoPhase_MixedExistAndNonExist) {
    const std::string exist_key = "async_exist_key";
    const std::string exist_data = "exist_data";
    PutKey(exist_key, exist_data);

    const size_t buf_size = 1024;
    void* buf1 = RegisterBuffer(buf_size);
    void* buf2 = RegisterBuffer(buf_size);

    std::vector<std::string> keys = {exist_key, "nonexistent_key_abc"};
    std::vector<void*> buffers = {buf1, buf2};
    std::vector<size_t> sizes = {buf_size, buf_size};

    auto state = py_client_->batch_get_submit(keys, buffers, sizes);
    // At least the existing key should have a transfer
    ASSERT_FALSE(state.pending_transfers.empty());

    auto results = py_client_->batch_get_complete(state);
    ASSERT_EQ(results.size(), 2u);
    EXPECT_EQ(results[0], 0) << "Existing key should succeed";
    EXPECT_EQ(results[1], -1) << "Non-existent key should fail";

    EXPECT_EQ(memcmp(buf1, exist_data.data(), exist_data.size()), 0);

    UnregisterAndFree(buf1);
    UnregisterAndFree(buf2);
}

TEST_F(AsyncBatchGetTest, TwoPhase_ConsistencyWithSync) {
    const std::string key = "async_sync_compare_key";
    const std::string data = "compare_this_data";
    PutKey(key, data);

    const size_t buf_size = 1024;

    // Sync path
    void* sync_buf = RegisterBuffer(buf_size);
    auto sync_results =
        py_client_->batch_get_into({key}, {sync_buf}, {buf_size});
    ASSERT_EQ(sync_results.size(), 1u);
    ASSERT_GT(sync_results[0], 0);

    // Async path
    void* async_buf = RegisterBuffer(buf_size);
    auto state = py_client_->batch_get_submit({key}, {async_buf}, {buf_size});
    ASSERT_FALSE(state.pending_transfers.empty());
    auto async_results = py_client_->batch_get_complete(state);
    ASSERT_EQ(async_results.size(), 1u);
    EXPECT_EQ(async_results[0], 0);

    // Compare data
    EXPECT_EQ(memcmp(sync_buf, async_buf, data.size()), 0)
        << "Sync and async paths should produce identical data";

    UnregisterAndFree(sync_buf);
    UnregisterAndFree(async_buf);
}

// ============================================================================
// AsyncGetContext tests
// ============================================================================

TEST_F(AsyncBatchGetTest, Context_BasicSubmitWaitAny) {
    const std::string key = "ctx_basic_key";
    const std::string data = "ctx_basic_data";
    PutKey(key, data);

    auto ctx = py_client_->create_async_context(4);
    ASSERT_NE(ctx, nullptr);

    const size_t buf_size = 1024;
    void* buf = RegisterBuffer(buf_size);

    auto token = ctx->submit({key}, {buf}, {buf_size});
    ASSERT_NE(token, AsyncGetContext::INVALID_TOKEN);
    EXPECT_EQ(ctx->in_flight(), 1u);

    auto [ret_token, results] = ctx->wait_any();
    EXPECT_EQ(ret_token, token);
    EXPECT_EQ(ctx->in_flight(), 0u);
    ASSERT_EQ(results.size(), 1u);
    EXPECT_EQ(results[0], 0);
    EXPECT_EQ(memcmp(buf, data.data(), data.size()), 0);

    UnregisterAndFree(buf);
}

TEST_F(AsyncBatchGetTest, Context_MultipleConcurrentSubmits) {
    const int num_ops = 4;
    std::vector<std::string> keys;
    std::vector<std::string> data_vec;
    for (int i = 0; i < num_ops; i++) {
        keys.push_back("ctx_concurrent_" + std::to_string(i));
        data_vec.push_back("concurrent_data_" + std::to_string(i));
        PutKey(keys[i], data_vec[i]);
    }

    auto ctx = py_client_->create_async_context(4);
    const size_t buf_size = 1024;

    std::vector<void*> buffers;
    std::map<AsyncGetContext::Token, int> token_to_idx;

    // Submit all at once
    for (int i = 0; i < num_ops; i++) {
        void* buf = RegisterBuffer(buf_size);
        buffers.push_back(buf);
        auto token = ctx->submit({keys[i]}, {buf}, {buf_size});
        ASSERT_NE(token, AsyncGetContext::INVALID_TOKEN)
            << "Submit " << i << " should succeed";
        token_to_idx[token] = i;
    }
    EXPECT_EQ(ctx->in_flight(), static_cast<size_t>(num_ops));

    // Wait for all completions (in any order)
    for (int i = 0; i < num_ops; i++) {
        auto [token, results] = ctx->wait_any();
        ASSERT_NE(token, AsyncGetContext::INVALID_TOKEN);
        ASSERT_EQ(results.size(), 1u);
        EXPECT_EQ(results[0], 0);

        int idx = token_to_idx[token];
        EXPECT_EQ(
            memcmp(buffers[idx], data_vec[idx].data(), data_vec[idx].size()), 0)
            << "Data mismatch for op " << idx;
    }
    EXPECT_EQ(ctx->in_flight(), 0u);

    for (auto buf : buffers) UnregisterAndFree(buf);
}

TEST_F(AsyncBatchGetTest, Context_MaxConcurrencyEnforced) {
    const int max_conc = 2;
    auto ctx = py_client_->create_async_context(max_conc);

    // Put enough keys
    for (int i = 0; i < 3; i++) {
        PutKey("ctx_max_" + std::to_string(i), "data_" + std::to_string(i));
    }

    const size_t buf_size = 1024;
    std::vector<void*> buffers;
    for (int i = 0; i < 3; i++) {
        buffers.push_back(RegisterBuffer(buf_size));
    }

    // Submit up to max
    auto t1 = ctx->submit({"ctx_max_0"}, {buffers[0]}, {buf_size});
    EXPECT_NE(t1, AsyncGetContext::INVALID_TOKEN);
    auto t2 = ctx->submit({"ctx_max_1"}, {buffers[1]}, {buf_size});
    EXPECT_NE(t2, AsyncGetContext::INVALID_TOKEN);

    // Third should be rejected
    auto t3 = ctx->submit({"ctx_max_2"}, {buffers[2]}, {buf_size});
    EXPECT_EQ(t3, AsyncGetContext::INVALID_TOKEN)
        << "Should reject when at max concurrency";

    // Drain one
    auto [token, results] = ctx->wait_any();
    EXPECT_NE(token, AsyncGetContext::INVALID_TOKEN);

    // Now submit should succeed again
    auto t4 = ctx->submit({"ctx_max_2"}, {buffers[2]}, {buf_size});
    EXPECT_NE(t4, AsyncGetContext::INVALID_TOKEN);

    // Drain remaining
    ctx->wait_any();
    ctx->wait_any();
    EXPECT_EQ(ctx->in_flight(), 0u);

    for (auto buf : buffers) UnregisterAndFree(buf);
}

TEST_F(AsyncBatchGetTest, Context_NonExistentKeyReturnsErrorViaWaitAny) {
    auto ctx = py_client_->create_async_context(4);

    const size_t buf_size = 1024;
    void* buf = RegisterBuffer(buf_size);

    // All keys fail, but submit still returns a valid token
    auto token = ctx->submit({"no_such_key_ever"}, {buf}, {buf_size});
    EXPECT_NE(token, AsyncGetContext::INVALID_TOKEN)
        << "Should return valid token even when all keys fail";
    EXPECT_EQ(ctx->in_flight(), 1u);

    // Errors are reported via wait_any
    auto [ret_token, results] = ctx->wait_any();
    EXPECT_EQ(ret_token, token);
    ASSERT_EQ(results.size(), 1u);
    EXPECT_EQ(results[0], -1)
        << "Non-existent key should report error in results";
    EXPECT_EQ(ctx->in_flight(), 0u);

    UnregisterAndFree(buf);
}

TEST_F(AsyncBatchGetTest, Context_DestroyWithInFlight) {
    const std::string key = "ctx_destroy_key";
    PutKey(key, "destroy_data");

    const size_t buf_size = 1024;
    void* buf = RegisterBuffer(buf_size);

    {
        auto ctx = py_client_->create_async_context(4);
        auto token = ctx->submit({key}, {buf}, {buf_size});
        ASSERT_NE(token, AsyncGetContext::INVALID_TOKEN);
        // Destroy ctx without calling wait_any — should not crash or hang
    }

    UnregisterAndFree(buf);
}

}  // namespace testing
}  // namespace mooncake

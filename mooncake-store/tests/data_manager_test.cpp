#include <glog/logging.h>
#include <gtest/gtest.h>
#include <json/json.h>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <cstring>
#include <unordered_map>
#include <chrono>
#include <mutex>

#define private public
#define protected public
#include "data_manager.h"
#include "tiered_cache/tiered_backend.h"
#include "utils/common.h"
#include "transfer_engine.h"
#include "types.h"
#include "utils.h"
#undef protected
#undef private

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

// Test fixture for DataManager tests
class DataManagerTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("DataManagerTest");
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

        const size_t metadata_shard_count =
            GetEnvOr<size_t>("MOONCAKE_DM_LOCK_SHARD_COUNT", 1024);
        tiered_backend_ = std::make_unique<TieredBackend>(metadata_shard_count);
        // transfer_engine_ is nullptr when initializing tiered_backend_
        // only for local access test
        auto init_result = InitTieredBackendForTest(*tiered_backend_, config);
        ASSERT_TRUE(init_result.has_value())
            << "Failed to initialize TieredBackend: " << init_result.error();

        // Verify tier was created successfully
        auto tier_views = tiered_backend_->GetTierViews();
        ASSERT_EQ(tier_views.size(), 1)
            << "Expected 1 tier, got " << tier_views.size();
        saved_tier_id_ = tier_views[0].id;

        // Create DataManager in MEMCPY mode so Put/Get work without RDMA.
        LocalTransferConfig transfer_config;
        transfer_config.mode = LocalTransferMode::MEMCPY;
        transfer_config.local_memcpy_async_worker_num = 32;

        // Save raw pointer before move for tests that need direct backend
        // access
        TieredBackend* backend_raw_ptr = tiered_backend_.get();
        data_manager_ = std::make_unique<DataManager>(
            std::move(tiered_backend_), transfer_engine_, metadata_shard_count,
            transfer_config);
        // Keep the raw pointer accessible for tests that need direct backend
        // access Note: The backend is now owned by DataManager, but tests can
        // still use it
        tiered_backend_raw_ptr_ = backend_raw_ptr;
    }

    void TearDown() override {
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
    std::unique_ptr<char[]> CreateTestData(
        size_t size, const std::string& pattern = "test") {
        auto buffer = std::make_unique<char[]>(size);
        for (size_t i = 0; i < size; ++i) {
            buffer[i] = pattern[i % pattern.size()];
        }
        return buffer;
    }

    // Helper: Convert string to unique_ptr<char[]> for Put
    std::unique_ptr<char[]> StringToBuffer(const std::string& str) {
        auto buffer = std::make_unique<char[]>(str.size());
        std::memcpy(buffer.get(), str.data(), str.size());
        return buffer;
    }

    // Helper: Put data via public API, returns success/failure synchronously.
    tl::expected<void, ErrorCode> DoPut(const std::string& key, void* ptr,
                                        size_t size,
                                        DataManager* dm = nullptr) {
        if (!dm) dm = data_manager_.get();
        std::vector<Slice> slices = {{ptr, size}};
        auto handle = dm->Put(key, slices);
        if (!handle.has_value()) return tl::make_unexpected(handle.error());
        return handle.value()->Wait();
    }

    // Helper: Get data via public API into a caller-supplied buffer.
    // Returns true on success.
    bool DoGet(const std::string& key, void* buf, size_t size,
               DataManager* dm = nullptr) {
        if (!dm) dm = data_manager_.get();
        std::vector<Slice> slices = {{buf, size}};
        auto handle = dm->Get(key, slices);
        if (!handle.has_value()) return false;
        return handle.value().task_handle->Wait().has_value();
    }

    // Wrappers for private Transfer methods (DataManagerTest is a friend).
    // Internally resolve the key to an AllocationHandle so test bodies
    // never call GetHandle directly.

    std::unique_ptr<DataManager> data_manager_;
    std::unique_ptr<TieredBackend> tiered_backend_;
    TieredBackend* tiered_backend_raw_ptr_ =
        nullptr;  // Raw pointer for tests that need direct backend access
    std::shared_ptr<TransferEngine> transfer_engine_;
    std::optional<UUID> saved_tier_id_;
};

// Test Put operation - success case
TEST_F(DataManagerTest, PutSuccess) {
    const std::string key = "test_key";
    const std::string test_data = "Hello, World!";
    const size_t data_size = test_data.size();

    auto buffer = StringToBuffer(test_data);
    auto result = DoPut(key, buffer.get(), data_size);

    ASSERT_TRUE(result.has_value())
        << "Put failed with error: " << toString(result.error());

    // Verify the key exists and data is readable
    ASSERT_TRUE(data_manager_->Exist(key)) << "Key should exist after Put";

    auto read_buf = std::make_unique<char[]>(data_size);
    ASSERT_TRUE(DoGet(key, read_buf.get(), data_size))
        << "Get failed after Put";
    EXPECT_EQ(std::string(read_buf.get(), data_size), test_data);
}

// Test Put operation - allocation failure (by using huge size)
TEST_F(DataManagerTest, PutAllocationFailure) {
    const std::string key = "test_key";
    // Try to allocate more than available capacity (1GB)
    const size_t huge_size = 2ULL * 1024 * 1024 * 1024;  // 2GB

    auto test_data = CreateTestData(1024);
    auto result = DoPut(key, test_data.get(), huge_size);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::NO_AVAILABLE_HANDLE);
}

// Test Put with tier_id (public Put doesn't target a specific tier; just
// verify the key is stored and readable after a plain Put).
TEST_F(DataManagerTest, PutWithTierId) {
    const std::string key = "test_key_with_tier";
    const std::string test_data = "Test data with tier";

    auto buffer = StringToBuffer(test_data);
    auto result = DoPut(key, buffer.get(), test_data.size());

    ASSERT_TRUE(result.has_value()) << "Put failed";

    // Verify we can get it back
    ASSERT_TRUE(data_manager_->Exist(key));
}

// Test Get operation - success case
TEST_F(DataManagerTest, GetSuccess) {
    const std::string key = "test_get_key";
    const std::string test_data = "Test data for Get";

    // First, put the data
    auto buffer = StringToBuffer(test_data);
    auto put_result = DoPut(key, buffer.get(), test_data.size());
    ASSERT_TRUE(put_result.has_value()) << "Put failed in Get test";

    // Then, get it and verify content
    auto read_buf = std::make_unique<char[]>(test_data.size());
    ASSERT_TRUE(DoGet(key, read_buf.get(), test_data.size())) << "Get failed";
    EXPECT_EQ(std::string(read_buf.get(), test_data.size()), test_data);
}

// Test Get operation - key not found
TEST_F(DataManagerTest, GetKeyNotFound) {
    const std::string key = "non_existent_key";

    ASSERT_FALSE(data_manager_->Exist(key));
}

// Test Get with tier_id (public Put doesn't target a specific tier; verify
// key is stored and readable after a plain Put).
TEST_F(DataManagerTest, GetWithTierId) {
    const std::string key = "test_get_tier_key";
    const std::string test_data = "Test data";

    auto buffer = StringToBuffer(test_data);
    auto put_result = DoPut(key, buffer.get(), test_data.size());
    ASSERT_TRUE(put_result.has_value());

    auto read_buf = std::make_unique<char[]>(test_data.size());
    ASSERT_TRUE(DoGet(key, read_buf.get(), test_data.size()));
    EXPECT_EQ(std::string(read_buf.get(), test_data.size()), test_data);
}

// Test Delete operation - success case
TEST_F(DataManagerTest, DeleteSuccess) {
    const std::string key = "test_delete_key";
    const std::string test_data = "Test data for Delete";

    // First, put the data
    auto buffer = StringToBuffer(test_data);
    auto put_result = DoPut(key, buffer.get(), test_data.size());
    ASSERT_TRUE(put_result.has_value());

    // Then, delete it
    auto delete_result = data_manager_->Delete(key);

    ASSERT_TRUE(delete_result.has_value())
        << "Delete failed with error: " << toString(delete_result.error());

    // Verify it's deleted
    ASSERT_FALSE(data_manager_->Exist(key));
}

// Test Delete operation - key not found
TEST_F(DataManagerTest, DeleteKeyNotFound) {
    const std::string key = "non_existent_key";

    auto result = data_manager_->Delete(key);

    ASSERT_FALSE(result.has_value())
        << "Delete should fail for non-existent key";
    // Verify it returns appropriate error code
    EXPECT_NE(result.error(), ErrorCode::OK);
}

// Test Delete with tier_id
TEST_F(DataManagerTest, DeleteWithTierId) {
    const std::string key = "test_delete_tier_key";
    const std::string test_data = "Test data";

    auto buffer = StringToBuffer(test_data);
    auto put_result = DoPut(key, buffer.get(), test_data.size());
    ASSERT_TRUE(put_result.has_value());

    auto delete_result = data_manager_->Delete(key);
    ASSERT_TRUE(delete_result.has_value())
        << "Delete failed with error: " << toString(delete_result.error());

    ASSERT_FALSE(data_manager_->Exist(key));
}

// BuildRemoteBufferDesc: null allocation buffer is an internal error.
TEST_F(DataManagerTest, BuildRemoteBufferDescRejectsNullBuffer) {
    auto alloc_result = tiered_backend_raw_ptr_->Allocate(64, GetTierId());
    ASSERT_TRUE(alloc_result.has_value())
        << "Allocate failed: " << toString(alloc_result.error());

    AllocationHandle handle = alloc_result.value();
    auto saved_buffer = std::move(handle->loc.data.buffer);
    ASSERT_EQ(handle->loc.data.buffer, nullptr);

    auto desc_result = data_manager_->BuildRemoteBufferDesc(handle);
    ASSERT_FALSE(desc_result.has_value());
    EXPECT_EQ(desc_result.error(), ErrorCode::INTERNAL_ERROR);

    // Restore buffer so AllocationEntry destructor can release via tier.
    handle->loc.data.buffer = std::move(saved_buffer);
}

// Test PreWrite: concurrent PreWrite should be rejected by a pending lease.
TEST_F(DataManagerTest, PreWriteRejectsConcurrentLease) {
    const std::string key = "prewrite_lifecycle_key";
    auto prewrite_result = data_manager_->PreWrite(key, 256, GetTierId());
    ASSERT_TRUE(prewrite_result.has_value())
        << "PreWrite failed: " << toString(prewrite_result.error());

    auto second_prewrite = data_manager_->PreWrite(key, 256, GetTierId());
    ASSERT_FALSE(second_prewrite.has_value());
    EXPECT_EQ(second_prewrite.error(), ErrorCode::REPLICA_IS_PROCESSING);

    const auto kctx = data_manager_->BuildKeyCtx(key);
    auto& shard = data_manager_->GetPendingWriteShard(kctx);
    {
        std::shared_lock shard_lock(shard.mutex);
        auto it = shard.existed_operation_key_map.find(key);
        ASSERT_NE(it, shard.existed_operation_key_map.end());
        EXPECT_EQ(it->second.write_operation_id,
                  prewrite_result->write_operation_id);
    }
}

// Test WriteCommit: successful commit should erase the pending write record.
TEST_F(DataManagerTest, WriteCommitErasesPendingWriteRecord) {
    const std::string key = "write_commit_erases_record_key";
    auto prewrite_result = data_manager_->PreWrite(key, 256, GetTierId());
    ASSERT_TRUE(prewrite_result.has_value())
        << "PreWrite failed: " << toString(prewrite_result.error());

    auto commit_result =
        data_manager_->WriteCommit(key, prewrite_result->write_operation_id);
    ASSERT_TRUE(commit_result.has_value())
        << "WriteCommit failed: " << toString(commit_result.error());
    EXPECT_TRUE(data_manager_->Exist(key));

    const auto kctx = data_manager_->BuildKeyCtx(key);
    auto& shard = data_manager_->GetPendingWriteShard(kctx);
    {
        std::shared_lock shard_lock(shard.mutex);
        EXPECT_EQ(shard.existed_operation_key_map.count(key), 0U);
    }
}

// WriteCommit without a pending write (e.g. duplicate commit) is an error.
TEST_F(DataManagerTest, WriteCommitWithoutPendingWriteFails) {
    const std::string key = "write_commit_no_pending_key";
    auto prewrite_result = data_manager_->PreWrite(key, 256, GetTierId());
    ASSERT_TRUE(prewrite_result.has_value());

    auto first_commit =
        data_manager_->WriteCommit(key, prewrite_result->write_operation_id);
    ASSERT_TRUE(first_commit.has_value());

    auto second_commit =
        data_manager_->WriteCommit(key, prewrite_result->write_operation_id);
    ASSERT_FALSE(second_commit.has_value());
    EXPECT_EQ(second_commit.error(), ErrorCode::OBJECT_NOT_FOUND);
}

// Test WriteCommit: token mismatch should fail without erasing the record.
TEST_F(DataManagerTest, WriteCommitTokenMismatchKeepsPendingWriteRecord) {
    const std::string key = "write_commit_token_mismatch_key";
    auto prewrite_result = data_manager_->PreWrite(key, 256, GetTierId());
    ASSERT_TRUE(prewrite_result.has_value())
        << "PreWrite failed: " << toString(prewrite_result.error());

    UUID wrong_token = prewrite_result->write_operation_id;
    wrong_token.first += 1;
    auto wrong_commit = data_manager_->WriteCommit(key, wrong_token);
    ASSERT_FALSE(wrong_commit.has_value());
    EXPECT_EQ(wrong_commit.error(), ErrorCode::INVALID_WRITE);

    const auto kctx = data_manager_->BuildKeyCtx(key);
    auto& shard = data_manager_->GetPendingWriteShard(kctx);
    {
        std::shared_lock shard_lock(shard.mutex);
        auto it = shard.existed_operation_key_map.find(key);
        ASSERT_NE(it, shard.existed_operation_key_map.end());
        EXPECT_EQ(it->second.write_operation_id,
                  prewrite_result->write_operation_id);
    }
}

// Test Pin/Unpin: ref_count increments on PinKey and reaches zero on final
// UnPinKey.
TEST_F(DataManagerTest, PinKeyTracksRefCountUntilFinalUnpin) {
    const std::string key = "pin_ref_count_key";
    const std::string test_data = "Pin key ref count payload";

    auto buffer = StringToBuffer(test_data);
    ASSERT_TRUE(DoPut(key, buffer.get(), test_data.size()).has_value());

    auto first_pin = data_manager_->PinKey(key, GetTierId());
    ASSERT_TRUE(first_pin.has_value())
        << "First PinKey failed: " << toString(first_pin.error());

    auto second_pin = data_manager_->PinKey(key, GetTierId());
    ASSERT_TRUE(second_pin.has_value())
        << "Second PinKey failed: " << toString(second_pin.error());
    EXPECT_EQ(first_pin->read_operation_id, second_pin->read_operation_id);

    const auto kctx = data_manager_->BuildKeyCtx(key);
    auto& shard = data_manager_->GetPinnedKeyShard(kctx);
    {
        std::shared_lock shard_lock(shard.mutex);
        auto it = shard.existed_operation_key_map.find(key);
        ASSERT_NE(it, shard.existed_operation_key_map.end());
        EXPECT_EQ(it->second.ref_count, 2U);
    }

    auto first_unpin =
        data_manager_->UnPinKey(key, first_pin->read_operation_id);
    ASSERT_TRUE(first_unpin.has_value())
        << "First UnPinKey failed: " << toString(first_unpin.error());
    {
        std::shared_lock shard_lock(shard.mutex);
        auto it = shard.existed_operation_key_map.find(key);
        ASSERT_NE(it, shard.existed_operation_key_map.end());
        EXPECT_EQ(it->second.ref_count, 1U);
    }

    auto second_unpin =
        data_manager_->UnPinKey(key, second_pin->read_operation_id);
    ASSERT_TRUE(second_unpin.has_value())
        << "Second UnPinKey failed: " << toString(second_unpin.error());
    {
        std::shared_lock shard_lock(shard.mutex);
        EXPECT_EQ(shard.existed_operation_key_map.count(key), 0U);
    }
}

// Test UnPinKey: token mismatch should fail without erasing or decrementing the
// record.
TEST_F(DataManagerTest, UnPinKeyTokenMismatchKeepsPinnedRecord) {
    const std::string key = "unpin_token_mismatch_key";
    const std::string test_data = "Unpin token mismatch payload";

    auto buffer = StringToBuffer(test_data);
    ASSERT_TRUE(DoPut(key, buffer.get(), test_data.size()).has_value());

    auto pin_result = data_manager_->PinKey(key, GetTierId());
    ASSERT_TRUE(pin_result.has_value())
        << "PinKey failed: " << toString(pin_result.error());

    UUID wrong_token = pin_result->read_operation_id;
    wrong_token.first += 1;
    auto wrong_unpin = data_manager_->UnPinKey(key, wrong_token);
    ASSERT_FALSE(wrong_unpin.has_value());
    EXPECT_EQ(wrong_unpin.error(), ErrorCode::INVALID_READ);

    const auto kctx = data_manager_->BuildKeyCtx(key);
    auto& shard = data_manager_->GetPinnedKeyShard(kctx);
    {
        std::shared_lock shard_lock(shard.mutex);
        auto it = shard.existed_operation_key_map.find(key);
        ASSERT_NE(it, shard.existed_operation_key_map.end());
        EXPECT_EQ(it->second.ref_count, 1U);
        EXPECT_EQ(it->second.read_operation_id, pin_result->read_operation_id);
    }
}

// Test WriteCommit: lease expiry causes commit failure and allows subsequent
// PreWrite.
TEST_F(DataManagerTest,
       WriteCommitFailsAfterLeaseExpiryAndAllowsRetryPreWrite) {
    const std::string key = "expired_prewrite_key";
    auto prewrite_result = data_manager_->PreWrite(key, 512, GetTierId());
    ASSERT_TRUE(prewrite_result.has_value())
        << "PreWrite failed: " << toString(prewrite_result.error());

    const auto kctx = data_manager_->BuildKeyCtx(key);
    auto& shard = data_manager_->GetPendingWriteShard(kctx);
    {
        std::unique_lock shard_lock(shard.mutex);
        auto it = shard.existed_operation_key_map.find(key);
        ASSERT_NE(it, shard.existed_operation_key_map.end());
        const auto expired_deadline =
            std::chrono::steady_clock::now() - std::chrono::milliseconds(1);
        it->second.deadline = expired_deadline;
        it->second.list_it->second = expired_deadline;
    }

    auto commit_result =
        data_manager_->WriteCommit(key, prewrite_result->write_operation_id);
    ASSERT_FALSE(commit_result.has_value());
    EXPECT_EQ(commit_result.error(), ErrorCode::LEASE_EXPIRED);

    {
        std::shared_lock shard_lock(shard.mutex);
        EXPECT_EQ(shard.existed_operation_key_map.count(key), 0U);
    }

    auto retry_prewrite = data_manager_->PreWrite(key, 512, GetTierId());
    ASSERT_TRUE(retry_prewrite.has_value())
        << "PreWrite should succeed after expired record is cleaned: "
        << toString(retry_prewrite.error());
}

// Test UnPinKey: lease expiry causes unpin failure and cleans up the pin
// record.
TEST_F(DataManagerTest,
       ExpiredPinnedLeaseBlocksUnpinButDeleteCanProceedAfterCleanup) {
    const std::string key = "expired_pin_key";
    const std::string test_data = "Expired pin payload";

    auto buffer = StringToBuffer(test_data);
    ASSERT_TRUE(DoPut(key, buffer.get(), test_data.size()).has_value());

    auto pin_result = data_manager_->PinKey(key, GetTierId());
    ASSERT_TRUE(pin_result.has_value())
        << "PinKey failed: " << toString(pin_result.error());

    const auto kctx = data_manager_->BuildKeyCtx(key);
    auto& shard = data_manager_->GetPinnedKeyShard(kctx);
    {
        std::unique_lock shard_lock(shard.mutex);
        auto it = shard.existed_operation_key_map.find(key);
        ASSERT_NE(it, shard.existed_operation_key_map.end());
        const auto expired_deadline =
            std::chrono::steady_clock::now() - std::chrono::milliseconds(1);
        it->second.deadline = expired_deadline;
        it->second.list_it->second = expired_deadline;
    }

    auto unpin_result =
        data_manager_->UnPinKey(key, pin_result->read_operation_id);
    ASSERT_FALSE(unpin_result.has_value());
    EXPECT_EQ(unpin_result.error(), ErrorCode::LEASE_EXPIRED);

    {
        std::shared_lock shard_lock(shard.mutex);
        EXPECT_EQ(shard.existed_operation_key_map.count(key), 0U);
    }
}

// Test concurrent Put operations
TEST_F(DataManagerTest, ConcurrentPut) {
    const int num_keys = 10;
    std::vector<std::string> keys;
    for (int i = 0; i < num_keys; ++i) {
        keys.push_back("concurrent_key_" + std::to_string(i));
    }

    // Put all keys concurrently
    std::vector<tl::expected<void, ErrorCode>> results(num_keys);
    std::vector<std::thread> threads;

    for (int i = 0; i < num_keys; ++i) {
        threads.emplace_back([this, &keys, &results, i]() {
            std::string data = "data_" + std::to_string(i);
            auto buffer = StringToBuffer(data);
            std::vector<Slice> slices = {{buffer.get(), data.size()}};
            auto handle = data_manager_->Put(keys[i], slices);
            results[i] = handle.has_value()
                             ? handle.value()->Wait()
                             : tl::make_unexpected(handle.error());
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // Verify all succeeded
    for (int i = 0; i < num_keys; ++i) {
        ASSERT_TRUE(results[i].has_value())
            << "Put failed for key: " << keys[i];
    }

    // Verify all can be retrieved
    for (int i = 0; i < num_keys; ++i) {
        ASSERT_TRUE(data_manager_->Exist(keys[i]))
            << "Key not found after Put: " << keys[i];
    }
}

// Test Put-Get-Delete sequence
TEST_F(DataManagerTest, PutGetDeleteSequence) {
    const std::string key = "sequence_test_key";
    const std::string test_data = "Sequence test data";

    // Put
    auto buffer = StringToBuffer(test_data);
    auto put_result = DoPut(key, buffer.get(), test_data.size());
    ASSERT_TRUE(put_result.has_value());

    // Get and verify
    auto read_buf = std::make_unique<char[]>(test_data.size());
    ASSERT_TRUE(DoGet(key, read_buf.get(), test_data.size()));
    EXPECT_EQ(std::string(read_buf.get(), test_data.size()), test_data);

    // Delete
    auto delete_result = data_manager_->Delete(key);
    ASSERT_TRUE(delete_result.has_value())
        << "Delete failed with error: " << toString(delete_result.error());

    // Verify deleted
    ASSERT_FALSE(data_manager_->Exist(key));
}

// ========== ReadRemoteData/WriteRemoteData Tests ==========

// Test ReadRemoteData with non-existent key
TEST_F(DataManagerTest, ReadRemoteDataKeyNotFound) {
    const std::string key = "non_existent_key";

    std::vector<RemoteBufferDesc> dest_buffers = {{"segment1", 0x1000, 1024}};

    auto result = data_manager_->ReadRemoteData(key, dest_buffers);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::OBJECT_NOT_FOUND);
}

// Test ReadRemoteData with empty buffers
TEST_F(DataManagerTest, ReadRemoteDataEmptyBuffers) {
    const std::string key = "test_key";
    const std::string test_data = "Hello";

    auto buffer = StringToBuffer(test_data);
    ASSERT_TRUE(DoPut(key, buffer.get(), test_data.size()).has_value());

    std::vector<RemoteBufferDesc> empty_buffers;
    auto result = data_manager_->ReadRemoteData(key, empty_buffers);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

// Test WriteRemoteData with invalid buffer (zero size)
TEST_F(DataManagerTest, WriteRemoteDataInvalidBuffer) {
    const std::string key = "test_key";

    std::vector<RemoteBufferDesc> invalid_buffers = {
        {"segment1", 0x1000, 0}  // Zero size
    };

    auto result = data_manager_->WriteRemoteData(key, invalid_buffers);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

// Test WriteRemoteData with invalid buffer (null address)
TEST_F(DataManagerTest, WriteRemoteDataNullAddress) {
    const std::string key = "test_key";

    std::vector<RemoteBufferDesc> invalid_buffers = {
        {"segment1", 0, 1024}  // Null address
    };

    auto result = data_manager_->WriteRemoteData(key, invalid_buffers);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

// Test WriteRemoteData with empty buffers
TEST_F(DataManagerTest, WriteRemoteDataEmptyBuffers) {
    const std::string key = "test_key";

    std::vector<RemoteBufferDesc> empty_buffers;
    auto result = data_manager_->WriteRemoteData(key, empty_buffers);

    // Empty buffers should be handled - total_size will be 0
    // This is a valid edge case that should fail at allocation
    ASSERT_FALSE(result.has_value());
}

// Test large data storage and retrieval
TEST_F(DataManagerTest, LargeDataStorage) {
    const std::string key = "large_data_key";
    const size_t large_size = 10 * 1024 * 1024;  // 10MB

    auto large_data = CreateTestData(large_size, "LARGE");

    auto put_result = DoPut(key, large_data.get(), large_size);
    ASSERT_TRUE(put_result.has_value());

    auto read_buf = std::make_unique<char[]>(large_size);
    ASSERT_TRUE(DoGet(key, read_buf.get(), large_size));

    // Verify data integrity (sample check)
    for (size_t i = 0; i < 100; ++i) {
        EXPECT_EQ(read_buf[i], "LARGE"[i % 5]);
    }
}

// Test multiple scatter-gather buffers
TEST_F(DataManagerTest, MultipleScatterGatherBuffers) {
    const std::string key = "scatter_gather_key";
    const std::string test_data = "ScatterGatherTestData";

    auto buffer = StringToBuffer(test_data);
    ASSERT_TRUE(DoPut(key, buffer.get(), test_data.size()).has_value());
    ASSERT_TRUE(data_manager_->Exist(key));

    // Test with multiple buffers in different segments (total size >= 21)
    std::vector<RemoteBufferDesc> multi_segment_buffers = {
        {"segment_a", 0x1000, 7},
        {"segment_b", 0x2000, 7},
        {"segment_c", 0x3000, 7}};

    // Verify all parameters are valid (no empty segment names, non-zero sizes,
    // valid addresses)
    for (const auto& buf : multi_segment_buffers) {
        EXPECT_FALSE(buf.segment_endpoint.empty());
        EXPECT_GT(buf.size, 0);
        EXPECT_NE(buf.addr, 0);
    }

    // Calculate total size
    size_t total_size = 0;
    for (const auto& buf : multi_segment_buffers) {
        total_size += buf.size;
    }
    EXPECT_GE(total_size, test_data.size());

    // Note: Actual TransferDataToRemote would require real TransferEngine setup
    // with registered segments This test validates the scatter-gather parameter
    // structure is correct
}

// Test concurrent read operations
TEST_F(DataManagerTest, ConcurrentReadOperations) {
    const std::string key = "concurrent_read_key";
    const std::string test_data = "ConcurrentTestData";

    auto buffer = StringToBuffer(test_data);
    ASSERT_TRUE(DoPut(key, buffer.get(), test_data.size()).has_value());

    const int num_threads = 10;
    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([this, &key, &success_count, &test_data]() {
            auto read_buf = std::make_unique<char[]>(test_data.size());
            if (DoGet(key, read_buf.get(), test_data.size())) {
                success_count++;
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(success_count, num_threads);
}

// Test data integrity across multiple operations
TEST_F(DataManagerTest, DataIntegrityAcrossOperations) {
    const std::string key = "integrity_test_key";
    const std::string original_data = "IntegrityTest_123456789";

    // Store original data
    auto buffer1 = StringToBuffer(original_data);
    ASSERT_TRUE(DoPut(key, buffer1.get(), original_data.size()).has_value());

    // Retrieve and verify
    {
        auto read_buf = std::make_unique<char[]>(original_data.size());
        ASSERT_TRUE(DoGet(key, read_buf.get(), original_data.size()));
        EXPECT_EQ(std::string(read_buf.get(), original_data.size()),
                  original_data);
    }

    // Delete the data
    ASSERT_TRUE(data_manager_->Delete(key).has_value());

    // Verify deletion
    ASSERT_FALSE(data_manager_->Exist(key));

    // Store new data with same key
    const std::string new_data = "NewDataForIntegrityCheck";
    auto buffer2 = StringToBuffer(new_data);
    ASSERT_TRUE(DoPut(key, buffer2.get(), new_data.size()).has_value());

    // Retrieve and verify new data
    {
        auto read_buf = std::make_unique<char[]>(new_data.size());
        ASSERT_TRUE(DoGet(key, read_buf.get(), new_data.size()));
        EXPECT_EQ(std::string(read_buf.get(), new_data.size()), new_data);
    }
}

// Test various key pattern formats
TEST_F(DataManagerTest, KeyPatternVariations) {
    const std::string test_data = "KeyPatternTestData";

    // Test different key formats
    std::vector<std::string> test_keys = {
        "simple_key",
        "key/with/slashes",
        "key.with.dots",
        "key-with-dashes",
        "key_with_underscores",
        "MixedCaseKey",
        "key:with:colons",
        "key123numeric456",
        "a",                    // Single character key
        std::string(100, 'x'),  // Long key (100 chars)
        "key with spaces",
        "key\twith\ttabs",
        "unicode_键_key"};

    for (const auto& key : test_keys) {
        auto buffer = StringToBuffer(test_data);
        auto put_result = DoPut(key, buffer.get(), test_data.size());

        // Put should succeed for all valid string keys
        ASSERT_TRUE(put_result.has_value()) << "Put failed for key: " << key;

        // Get should return the same data
        auto read_buf = std::make_unique<char[]>(test_data.size());
        ASSERT_TRUE(DoGet(key, read_buf.get(), test_data.size()))
            << "Get failed for key: " << key;
        EXPECT_EQ(std::string(read_buf.get(), test_data.size()), test_data)
            << "Data mismatch for key: " << key;

        // Delete should succeed
        ASSERT_TRUE(data_manager_->Delete(key).has_value())
            << "Delete failed for key: " << key;
    }
}

// Test memory release verification through repeated allocations
TEST_F(DataManagerTest, MemoryReleaseVerification) {
    const size_t data_size = 1024 * 1024;  // 1MB per allocation
    const int iterations = 50;             // 50MB total if memory not released

    for (int i = 0; i < iterations; ++i) {
        std::string key = "memory_test_key_" + std::to_string(i);
        auto buffer = CreateTestData(data_size, "MEM");

        // Put data
        auto put_result = DoPut(key, buffer.get(), data_size);
        ASSERT_TRUE(put_result.has_value()) << "Put failed at iteration " << i;

        // Verify data exists
        ASSERT_TRUE(data_manager_->Exist(key))
            << "Key missing at iteration " << i;

        // Delete data to release memory
        ASSERT_TRUE(data_manager_->Delete(key).has_value())
            << "Delete failed at iteration " << i;

        // Verify deletion
        EXPECT_FALSE(data_manager_->Exist(key));
    }

    // Final verification: allocate one more time to ensure memory was properly
    // released
    std::string final_key = "final_memory_test";
    auto final_buffer = CreateTestData(data_size, "FIN");
    ASSERT_TRUE(DoPut(final_key, final_buffer.get(), data_size).has_value());
    ASSERT_TRUE(data_manager_->Delete(final_key).has_value());
}

// Test multi-buffer validation for scatter-gather operations
TEST_F(DataManagerTest, MultiBufferValidation) {
    const std::string key = "multi_buffer_key";
    const std::string test_data =
        "MultiBufferValidationTestData123456";  // 35 bytes

    auto buffer = StringToBuffer(test_data);
    ASSERT_TRUE(DoPut(key, buffer.get(), test_data.size()).has_value());

    // Test case 1: Multiple valid buffers with exact total size
    {
        std::vector<RemoteBufferDesc> exact_buffers = {
            {"seg1", 0x1000, 10},
            {"seg2", 0x2000, 10},
            {"seg3", 0x3000, 10},
            {"seg4", 0x4000, 5}  // Total = 35 bytes
        };
        size_t total = 0;
        for (const auto& b : exact_buffers) total += b.size;
        EXPECT_EQ(total, test_data.size());
    }

    // Test case 2: Multiple valid buffers with excess capacity
    {
        std::vector<RemoteBufferDesc> excess_buffers = {
            {"seg1", 0x1000, 20}, {"seg2", 0x2000, 20}, {"seg3", 0x3000, 20}
            // Total = 60 bytes > 35 bytes
        };
        size_t total = 0;
        for (const auto& b : excess_buffers) total += b.size;
        EXPECT_GT(total, test_data.size());
    }

    // Test case 3: Mixed buffer with one invalid (empty segment name) - should
    // fail validation
    {
        std::vector<RemoteBufferDesc> mixed_invalid = {
            {"seg1", 0x1000, 10},
            {"", 0x2000, 10},  // Invalid: empty segment
            {"seg3", 0x3000, 15}};
        auto result = data_manager_->ReadRemoteData(key, mixed_invalid);
        EXPECT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
    }

    // Test case 4: Mixed buffer with one invalid (zero size) - should fail
    // validation
    {
        std::vector<RemoteBufferDesc> zero_size_buffer = {
            {"seg1", 0x1000, 10},
            {"seg2", 0x2000, 0},  // Invalid: zero size
            {"seg3", 0x3000, 25}};
        auto result = data_manager_->ReadRemoteData(key, zero_size_buffer);
        EXPECT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
    }

    // Test case 5: Valid buffers but TransferEngine not initialized
    // This simulates the actual transfer flow up to the point where
    // TransferEngine would perform RDMA operations
    {
        std::vector<RemoteBufferDesc> valid_buffers = {
            {"seg1", 0x1000, 20}, {"seg2", 0x2000, 15}
            // Total = 35 bytes, exact match
        };
        size_t total = 0;
        for (const auto& b : valid_buffers) total += b.size;
        EXPECT_EQ(total, test_data.size());

        // This will fail with INTERNAL_ERROR because TransferEngine is not
        // fully initialized (no metadata/RDMA connection in test environment).
        // In production with proper TransferEngine setup, this would succeed.
        auto result = data_manager_->ReadRemoteData(key, valid_buffers);
        EXPECT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), ErrorCode::INTERNAL_ERROR);
    }
}

// Test TransferDataToRemote with actual data preparation
// This test validates the complete flow including data buffer handling
TEST_F(DataManagerTest, TransferDataToRemoteDataPreparation) {
    const std::string key = "transfer_prep_key";
    const std::string test_data =
        "DataTransferPreparationTest12345678901";  // 38 bytes

    // Store test data
    auto buffer = StringToBuffer(test_data);
    ASSERT_TRUE(DoPut(key, buffer.get(), test_data.size()).has_value());

    // Verify we can read the data back
    {
        auto read_buf = std::make_unique<char[]>(test_data.size());
        ASSERT_TRUE(DoGet(key, read_buf.get(), test_data.size()));
        EXPECT_EQ(std::string(read_buf.get(), test_data.size()), test_data);
    }

    // Prepare scatter-gather destination buffers
    // Simulate splitting data across multiple remote segments
    std::vector<RemoteBufferDesc> scatter_buffers = {
        {"segment_A", 0x1000, 15},  // First 15 bytes
        {"segment_B", 0x2000, 13},  // Next 13 bytes
        {"segment_C", 0x3000, 10}   // Last 10 bytes, total = 38
    };

    // Verify total size matches
    size_t total_size = 0;
    for (const auto& buf : scatter_buffers) {
        total_size += buf.size;
    }
    EXPECT_EQ(total_size, test_data.size());

    // Attempt transfer (will fail due to uninitialized TransferEngine in test
    // env)
    auto transfer_result = data_manager_->ReadRemoteData(key, scatter_buffers);

    EXPECT_FALSE(transfer_result.has_value());
    EXPECT_EQ(transfer_result.error(), ErrorCode::INTERNAL_ERROR);
}

// Test TransferDataFromRemote validation
TEST_F(DataManagerTest, TransferDataFromRemoteValidation) {
    const std::string key = "transfer_from_key";

    // Test invalid parameters
    {
        // Empty segment name
        std::vector<RemoteBufferDesc> invalid_buffers = {{"", 0x1000, 50}};
        auto result = data_manager_->WriteRemoteData(key, invalid_buffers);
        EXPECT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
    }

    {
        // Zero size
        std::vector<RemoteBufferDesc> invalid_buffers = {{"seg1", 0x1000, 0}};
        auto result = data_manager_->WriteRemoteData(key, invalid_buffers);
        EXPECT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
    }

    {
        // Valid parameters but TransferEngine not initialized
        std::vector<RemoteBufferDesc> valid_buffers = {{"seg1", 0x1000, 30},
                                                       {"seg2", 0x2000, 20}};
        auto result = data_manager_->WriteRemoteData(key, valid_buffers);
        EXPECT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), ErrorCode::INTERNAL_ERROR);
    }
}

// Test concurrent delete operations for thread safety
TEST_F(DataManagerTest, ConcurrentDeleteOperations) {
    const int num_keys = 20;
    std::vector<std::string> keys;

    // Create keys and put data
    for (int i = 0; i < num_keys; ++i) {
        std::string key = "concurrent_delete_key_" + std::to_string(i);
        keys.push_back(key);
        std::string data = "data_for_deletion_" + std::to_string(i);
        auto buffer = StringToBuffer(data);
        ASSERT_TRUE(DoPut(key, buffer.get(), data.size()).has_value());
    }

    // Verify all keys exist
    for (const auto& key : keys) {
        ASSERT_TRUE(data_manager_->Exist(key));
    }

    // Delete all keys concurrently
    std::vector<std::thread> threads;
    std::atomic<int> delete_success_count{0};

    for (int i = 0; i < num_keys; ++i) {
        threads.emplace_back([this, &keys, &delete_success_count, i]() {
            if (data_manager_->Delete(keys[i]).has_value()) {
                delete_success_count++;
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(delete_success_count, num_keys);

    // Verify all keys are deleted
    for (const auto& key : keys) {
        EXPECT_FALSE(data_manager_->Exist(key));
    }
}

// Test repeated Put operations on the same key (overwrite behavior)
TEST_F(DataManagerTest, RepeatedPutSameKey) {
    const std::string key = "repeated_put_key";

    // First Put
    const std::string data1 = "FirstData";
    auto buffer1 = StringToBuffer(data1);
    ASSERT_TRUE(DoPut(key, buffer1.get(), data1.size()).has_value());

    // Verify first data
    {
        auto read_buf = std::make_unique<char[]>(data1.size());
        ASSERT_TRUE(DoGet(key, read_buf.get(), data1.size()));
        EXPECT_EQ(std::string(read_buf.get(), data1.size()), data1);
    }

    // Second Put with different data (overwrite)
    const std::string data2 = "SecondDataLonger";
    auto buffer2 = StringToBuffer(data2);
    auto put_result2 = DoPut(key, buffer2.get(), data2.size());

    // The behavior depends on implementation - it may fail or succeed
    // If it succeeds, verify the new data
    if (put_result2.has_value()) {
        ASSERT_TRUE(data_manager_->Exist(key));
    }

    // Third Put with shorter data
    const std::string data3 = "Short";
    auto buffer3 = StringToBuffer(data3);
    // Delete first to ensure clean state
    data_manager_->Delete(key);
    ASSERT_TRUE(DoPut(key, buffer3.get(), data3.size()).has_value());

    {
        auto read_buf = std::make_unique<char[]>(data3.size());
        ASSERT_TRUE(DoGet(key, read_buf.get(), data3.size()));
        EXPECT_EQ(std::string(read_buf.get(), data3.size()), data3);
    }
}

// Test boundary conditions with various data sizes
TEST_F(DataManagerTest, BoundaryConditionTests) {
    // Test 1: Single byte data
    {
        const std::string key = "single_byte_key";
        const std::string single_byte = "X";
        auto buffer = StringToBuffer(single_byte);
        ASSERT_TRUE(DoPut(key, buffer.get(), 1).has_value());
        ASSERT_TRUE(data_manager_->Exist(key));
    }

    // Test 2: Exact power-of-two sizes
    {
        const std::string key = "power_of_two_key";
        const size_t size = 64 * 1024;  // 64KB
        auto buffer = CreateTestData(size, "P2");
        ASSERT_TRUE(DoPut(key, buffer.get(), size).has_value());
        ASSERT_TRUE(data_manager_->Exist(key));
    }

    // Test 3: Non-power-of-two sizes
    {
        const std::string key = "non_power_of_two_key";
        const size_t size = 100 * 1024 + 512;  // 100.5KB
        auto buffer = CreateTestData(size, "NP2");
        ASSERT_TRUE(DoPut(key, buffer.get(), size).has_value());
        ASSERT_TRUE(data_manager_->Exist(key));
    }
}

// Test lock contention with 1025 concurrent Put operations
// Since there are only 1024 lock shards, at least 2 keys will map to the same
// lock
TEST_F(DataManagerTest, LockContentionTest) {
    const int num_keys = 1025;
    // Use the same initialization logic as DataManager
    const size_t kLockShardCount =
        GetEnvOr<size_t>("MOONCAKE_DM_LOCK_SHARD_COUNT", 1024);

    std::vector<std::string> keys;
    std::unordered_map<size_t, int> lock_usage_count;

    // Generate 1025 unique keys and calculate their lock indices
    for (int i = 0; i < num_keys; ++i) {
        std::string key = "contention_key_" + std::to_string(i);
        keys.push_back(key);

        // Calculate which lock shard this key maps to (same logic as
        // DataManager)
        size_t hash = std::hash<std::string>{}(key);
        size_t lock_index = hash % kLockShardCount;
        lock_usage_count[lock_index]++;
    }

    // Find locks with contention (multiple keys mapping to same lock)
    int contended_locks_num = 0;
    int used_locks_num = 0;
    for (const auto& [lock_idx, count] : lock_usage_count) {
        if (count > 1) {
            contended_locks_num++;
        }
        used_locks_num++;
    }

    // Log contention statistics
    LOG(INFO) << "=== Lock Contention Statistics ===";
    LOG(INFO) << "Total keys: " << num_keys;
    LOG(INFO) << "Total lock shards: " << kLockShardCount;
    LOG(INFO) << "Locks with contention (multiple keys): "
              << contended_locks_num;
    LOG(INFO) << "Contention ratio: "
              << (100.0 * (num_keys - used_locks_num) / num_keys) << "%";

    // Perform concurrent Put operations
    std::vector<tl::expected<void, ErrorCode>> results(num_keys);
    std::vector<std::thread> threads;
    std::mutex log_mutex;  // For thread-safe logging

    LOG(INFO) << "Starting " << num_keys << " concurrent Put operations...";
    auto start_time = std::chrono::steady_clock::now();

    for (int i = 0; i < num_keys; ++i) {
        threads.emplace_back([this, &keys, &results, i, &lock_usage_count,
                              kLockShardCount, &log_mutex]() {
            std::string data = "data_" + std::to_string(i);
            auto buffer = StringToBuffer(data);

            // Calculate lock index for this key
            size_t hash = std::hash<std::string>{}(keys[i]);
            size_t lock_index = hash % kLockShardCount;

            // Log if this key is in a contended lock
            bool is_contended = lock_usage_count[lock_index] > 1;

            std::vector<Slice> slices = {{buffer.get(), data.size()}};
            auto handle = data_manager_->Put(keys[i], slices);
            results[i] = handle.has_value()
                             ? handle.value()->Wait()
                             : tl::make_unexpected(handle.error());

            if (!results[i].has_value() && is_contended) {
                std::lock_guard<std::mutex> lock(log_mutex);
                LOG(ERROR) << "[CONTENTION FAILURE] Key: " << keys[i]
                           << " failed with error: "
                           << toString(results[i].error());
            }
        });
    }

    // Wait for all threads
    for (auto& t : threads) {
        t.join();
    }

    auto end_time = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                        end_time - start_time)
                        .count();

    LOG(INFO) << "All " << num_keys << " Put operations completed in "
              << duration << " ms";

    // Verify all operations succeeded
    int success_count = 0;
    int failure_count = 0;
    for (int i = 0; i < num_keys; ++i) {
        if (results[i].has_value()) {
            success_count++;
        } else {
            failure_count++;
            LOG(ERROR) << "Put failed for key: " << keys[i]
                       << ", error: " << toString(results[i].error());
        }
    }

    LOG(INFO) << "=== Operation Results ===";
    LOG(INFO) << "Success: " << success_count << "/" << num_keys;
    LOG(INFO) << "Failure: " << failure_count << "/" << num_keys;
    LOG(INFO) << "Success rate: " << (100.0 * success_count / num_keys) << "%";

    // All operations should succeed even with lock contention
    EXPECT_EQ(success_count, num_keys)
        << "All Put operations should succeed even with lock contention";
    EXPECT_EQ(failure_count, 0)
        << "No operations should fail due to lock contention";

    // Verify all keys can be retrieved
    LOG(INFO) << "Verifying all keys can be retrieved...";
    int retrieved_count = 0;
    for (int i = 0; i < num_keys; ++i) {
        if (data_manager_->Exist(keys[i])) {
            retrieved_count++;
        } else {
            LOG(ERROR) << "Key missing after Put: " << keys[i];
        }
    }

    LOG(INFO) << "Retrieved: " << retrieved_count << "/" << num_keys;
    EXPECT_EQ(retrieved_count, num_keys)
        << "All keys should be retrievable after Put";
}

// Test concurrent Get and Delete operations to verify that removing read locks
// from DataManager::Get is safe. The TieredBackend's internal locking and
// shared_ptr reference counting should ensure thread safety.
TEST_F(DataManagerTest, ConcurrentGetAndDelete) {
    const int num_keys = 50;
    std::vector<std::string> keys;

    // Setup: Create keys and put data
    for (int i = 0; i < num_keys; ++i) {
        std::string key = "concurrent_get_delete_key_" + std::to_string(i);
        keys.push_back(key);
        std::string data = "test_data_" + std::to_string(i);
        auto buffer = StringToBuffer(data);
        ASSERT_TRUE(DoPut(key, buffer.get(), data.size()).has_value());
    }

    std::atomic<int> successful_gets{0};
    std::atomic<int> successful_deletes{0};
    std::atomic<int> expected_not_found{0};
    std::vector<std::thread> threads;

    // Launch concurrent Get and Delete operations on the same keys
    for (int i = 0; i < num_keys; ++i) {
        // Get thread
        threads.emplace_back(
            [this, &keys, &successful_gets, &expected_not_found, i]() {
                if (data_manager_->Exist(keys[i])) {
                    successful_gets++;
                } else {
                    // Key may have been deleted by the delete thread
                    expected_not_found++;
                }
            });

        // Delete thread
        threads.emplace_back([this, &keys, &successful_deletes, i]() {
            if (data_manager_->Delete(keys[i]).has_value()) {
                successful_deletes++;
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    LOG(INFO) << "=== Concurrent Get/Delete Results ===";
    LOG(INFO) << "Successful Gets: " << successful_gets;
    LOG(INFO) << "Successful Deletes: " << successful_deletes;
    LOG(INFO) << "Expected Not Found: " << expected_not_found;

    // All deletes should succeed (each key deleted exactly once)
    EXPECT_EQ(successful_deletes.load(), num_keys);
    // Gets + NotFound should equal num_keys (each key accessed exactly once)
    EXPECT_EQ(successful_gets.load() + expected_not_found.load(), num_keys);
}

// ============================================================================
// Batch Transfer Tests - Testing batch submission and unified waiting
// ============================================================================

// Test WriteRemoteData atomicity: all batches failed should not commit
TEST_F(DataManagerTest, WriteRemoteDataAllBatchesFailedNoCommit) {
    const std::string key = "all_failed_no_commit_key";

    // Create source buffers with multiple segments
    // These will fail because TransferEngine is not initialized
    std::vector<RemoteBufferDesc> src_buffers = {
        {"segment_fail_1", 0x1000, 20},
        {"segment_fail_2", 0x2000, 20},
        {"segment_fail_3", 0x3000, 20}  // Total = 60
    };

    // Attempt write - should fail because TransferEngine is not initialized
    auto write_result = data_manager_->WriteRemoteData(key, src_buffers);

    // Should fail with INTERNAL_ERROR
    EXPECT_FALSE(write_result.has_value());
    EXPECT_EQ(write_result.error(), ErrorCode::INTERNAL_ERROR);

    // Verify key was NOT committed (should not exist)
    EXPECT_FALSE(data_manager_->Exist(key));

    LOG(INFO) << "WriteRemoteData all-failed test: key correctly not committed";
}

// Test TransferDataFromRemote return value
// This tests the expected<void, ErrorCode> return type behavior
TEST_F(DataManagerTest, TransferDataFromRemoteReturnValue) {
    const std::string key = "all_failed_flag_test";
    const size_t data_size = 50;

    // Subtest 1: Invalid segment_name (empty), should fail validation
    {
        std::vector<RemoteBufferDesc> invalid_buffers = {{"", 0x1000, 50}};
        auto result = data_manager_->WriteRemoteData(key, invalid_buffers);
        EXPECT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
    }

    // Subtest 2: TransferEngine is not initialized
    {
        std::vector<RemoteBufferDesc> valid_buffers = {{"seg1", 0x1000, 50}};
        auto result = data_manager_->WriteRemoteData(key, valid_buffers);
        EXPECT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), ErrorCode::INTERNAL_ERROR);
    }

    // Subtest 3: total_src_size less than data_size (buffers too small)
    // This tests the underlying TransferDataFromRemote/SubmitTeTransferInternal
    // logic by bypassing the automatic allocation of WriteRemoteData.
    {
        auto alloc_result = data_manager_->tiered_backend_->Allocate(data_size);
        ASSERT_TRUE(alloc_result.has_value());
        auto handle = alloc_result.value();

        std::vector<RemoteBufferDesc> too_small = {{"seg2", 0x2000, 10}};
        auto result = data_manager_->TransferDataFromRemote(handle, too_small);
        EXPECT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
    }

    // Subtest 4: Key already exists, should return OBJECT_ALREADY_EXISTS
    {
        // First, create the key
        auto buffer = std::make_unique<char[]>(data_size);
        ASSERT_TRUE(DoPut(key, buffer.get(), data_size).has_value());

        // Then, try to write it again via WriteRemoteData
        std::vector<RemoteBufferDesc> valid_buffers = {{"seg2", 0x2000, 50}};
        auto result = data_manager_->WriteRemoteData(key, valid_buffers);
        EXPECT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), ErrorCode::OBJECT_ALREADY_EXISTS);
    }
}

// Test: Destination buffers total size (60) is less than source data size (62)
TEST_F(DataManagerTest, TransferDataToRemoteDestBuffersTooSmall) {
    const std::string key = "dest_too_small_key";
    // Source data is 62 bytes
    const std::string test_data =
        "1234567890123456789012345678901234567890123456789012345678901"
        "2";  // 62 bytes

    // Store test data
    auto buffer = StringToBuffer(test_data);
    ASSERT_TRUE(DoPut(key, buffer.get(), test_data.size()).has_value());

    // Destination buffers total 60 bytes (< 62 source bytes)
    std::vector<RemoteBufferDesc> dest_buffers = {
        {"seg_a", 0x1000, 15},
        {"seg_b", 0x2000, 15},
        {"seg_c", 0x3000, 15},
        {"seg_d", 0x4000, 15}  // Total = 60
    };

    // Try transfer: should return INVALID_PARAMS because total dest buffer <
    // source
    auto transfer_result = data_manager_->ReadRemoteData(key, dest_buffers);

    EXPECT_FALSE(transfer_result.has_value());
    EXPECT_EQ(transfer_result.error(), ErrorCode::INVALID_PARAMS);

    LOG(INFO) << "Tested destination buffer total size (" << 15 * 4
              << ") < source data size (62), got error_code="
              << static_cast<int>(transfer_result.error());
}

// Test real RDMA loopback transfer (requires RDMA hardware and metadata server)
// This test initializes TransferEngine with RDMA support and performs actual
// RDMA data transfer on the same node
TEST_F(DataManagerTest, RealRDMALoopbackTransfer) {
    // Check environment variables
    const char* metadata_addr = std::getenv("MC_METADATA_ADDR");
    const char* local_hostname = std::getenv("MC_LOCAL_HOSTNAME");

    if (!metadata_addr || !local_hostname) {
        GTEST_SKIP() << "Skipping real RDMA test: MC_METADATA_ADDR and "
                        "MC_LOCAL_HOSTNAME environment variables not set. "
                        "Set these to enable real RDMA transfer testing.";
    }

    LOG(INFO) << "=== Starting Real RDMA Loopback Transfer Test ===";
    LOG(INFO) << "Metadata address: " << metadata_addr;
    LOG(INFO) << "Local hostname: " << local_hostname;

    // Create TransferEngine with RDMA support
    auto rdma_transfer_engine = std::make_shared<TransferEngine>(true);
    int init_result =
        rdma_transfer_engine->init(metadata_addr, local_hostname, "", 12350);

    if (init_result != 0) {
        GTEST_SKIP() << "Failed to initialize TransferEngine (error code: "
                     << init_result
                     << "). RDMA environment may not be configured.";
    }
    LOG(INFO) << "TransferEngine initialized successfully";

    // Allocate memory for RDMA transfer (single buffer, use different regions)
    // Note: allocate_buffer_allocator_memory requires minimum 16MB
    const size_t test_size = 16 * 1024 * 1024;  // 16MB (minimum required)
    void* rdma_buffer = allocate_buffer_allocator_memory(test_size * 2);

    if (!rdma_buffer) {
        GTEST_SKIP() << "Failed to allocate memory for RDMA test";
    }

    // Use first half as source reference, second half as destination
    void* dst_area = static_cast<char*>(rdma_buffer) + test_size;

    // Fill destination area with zeros (will be overwritten by RDMA transfer)
    std::memset(dst_area, 0, test_size);

    LOG(INFO) << "Allocated RDMA buffer (" << test_size * 2 << " bytes)";

    // Register memory with TransferEngine using "cpu:0" as location hint
    int reg_result = rdma_transfer_engine->registerLocalMemory(
        rdma_buffer, test_size * 2, "cpu:0");

    if (reg_result != 0) {
        free_memory("", rdma_buffer);
        GTEST_SKIP() << "Failed to register memory with TransferEngine. "
                     << "RDMA device may not be available.";
    }
    LOG(INFO) << "Memory registered for RDMA access";

    // Create TieredBackend and DataManager with RDMA-enabled TransferEngine
    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "DRAM",
                "capacity": 1073741824,
                "priority": 10,
                "allocator_type": "OFFSET"
            }
        ]
    })";
    Json::Value config;
    ASSERT_TRUE(parseJsonString(json_config_str, config));

    auto rdma_tiered_backend = std::make_unique<TieredBackend>();
    // Pass TransferEngine to TieredBackend so DRAM memory gets registered for
    // RDMA
    auto init_backend_result = rdma_tiered_backend->Init(
        config, rdma_transfer_engine.get(), nullptr, nullptr, nullptr);
    ASSERT_TRUE(init_backend_result.has_value())
        << "Failed to initialize TieredBackend";

    LocalTransferConfig local_transfer_config;
    local_transfer_config.mode = LocalTransferMode::MEMCPY;
    auto rdma_data_manager = std::make_unique<DataManager>(
        std::move(rdma_tiered_backend), rdma_transfer_engine, 1024,
        local_transfer_config);
    LOG(INFO) << "DataManager created with RDMA-enabled TransferEngine";

    // Store data in DataManager (this will be the source for RDMA transfer)
    const std::string key = "rdma_real_test_key";
    const std::string pattern = "RDMA_REAL_TEST_PATTERN_";
    auto data_copy = std::make_unique<char[]>(test_size);
    for (size_t i = 0; i < test_size; i++) {
        data_copy[i] = pattern[i % pattern.size()];
    }

    ASSERT_TRUE(DoPut(key, data_copy.get(), test_size, rdma_data_manager.get()))
        << "Failed to put data into DataManager";
    ASSERT_TRUE(rdma_data_manager->Exist(key)) << "Key not found after Put";
    LOG(INFO) << "Data stored in DataManager";

    // Use hostname as segment name (this is how TransferEngine registers
    // segments) The destination address must be within the registered memory
    // region
    std::string segment_name = std::string(local_hostname);
    std::vector<RemoteBufferDesc> dest_buffers = {
        {segment_name, reinterpret_cast<uint64_t>(dst_area), test_size}};

    LOG(INFO) << "Attempting real RDMA transfer to segment: " << segment_name;
    LOG(INFO) << "Destination address: 0x" << std::hex
              << reinterpret_cast<uint64_t>(dst_area) << std::dec;

    // Perform RDMA transfer via DataManager
    auto transfer_result = rdma_data_manager->ReadRemoteData(key, dest_buffers);

    if (!transfer_result.has_value()) {
        LOG(WARNING) << "RDMA transfer failed: "
                     << toString(transfer_result.error());
        rdma_transfer_engine->unregisterLocalMemory(rdma_buffer);
        free_memory("", rdma_buffer);
        GTEST_SKIP()
            << "RDMA transfer failed, may need additional configuration";
    }

    LOG(INFO) << "RDMA transfer completed!";

    // Verify data - compare with expected pattern
    bool data_matches = true;
    for (size_t i = 0; i < test_size; i++) {
        if (static_cast<char*>(dst_area)[i] != pattern[i % pattern.size()]) {
            data_matches = false;
            LOG(ERROR) << "Data mismatch at byte " << i << ": expected '"
                       << pattern[i % pattern.size()] << "' got '"
                       << static_cast<char*>(dst_area)[i] << "'";
            break;
        }
    }

    if (data_matches) {
        LOG(INFO) << "✓ Data verification PASSED: " << test_size
                  << " bytes transferred correctly via RDMA!";
    }

    EXPECT_TRUE(data_matches) << "RDMA transferred data does not match source";

    // Cleanup
    rdma_transfer_engine->unregisterLocalMemory(rdma_buffer);
    free_memory("", rdma_buffer);

    LOG(INFO) << "=== Real RDMA Loopback Transfer Test Completed ===";
}

// Test multiple batch transfer with all segments valid
// This test verifies batch submission and unified waiting with multiple
// segments
TEST_F(DataManagerTest, RealRDMAMultiBatchTransfer) {
    // Check environment variables
    const char* metadata_addr = std::getenv("MC_METADATA_ADDR");
    const char* local_hostname = std::getenv("MC_LOCAL_HOSTNAME");

    if (!metadata_addr || !local_hostname) {
        GTEST_SKIP() << "Skipping real RDMA test: MC_METADATA_ADDR and "
                        "MC_LOCAL_HOSTNAME environment variables not set. "
                        "Set these to enable real RDMA transfer testing.";
    }

    LOG(INFO) << "=== Starting Real RDMA Multi-Batch Partial Transfer Test ===";
    LOG(INFO) << "Metadata address: " << metadata_addr;
    LOG(INFO) << "Local hostname: " << local_hostname;

    // Create TransferEngine with RDMA support
    auto rdma_transfer_engine = std::make_shared<TransferEngine>(true);
    int init_result =
        rdma_transfer_engine->init(metadata_addr, local_hostname, "", 12350);

    if (init_result != 0) {
        GTEST_SKIP() << "Failed to initialize TransferEngine (error code: "
                     << init_result
                     << "). RDMA environment may not be configured.";
    }
    LOG(INFO) << "TransferEngine initialized successfully";

    // Allocate memory for RDMA transfer - larger buffer to accommodate multiple
    // segments
    // Note: allocate_buffer_allocator_memory requires minimum 16MB
    const size_t segment_size = 4 * 1024 * 1024;  // 4MB per segment
    const int num_segments = 4;                   // 4 segments
    const size_t total_buffer_size =
        segment_size * num_segments * 2;  // Double for source + dest
    void* rdma_buffer = allocate_buffer_allocator_memory(total_buffer_size);

    if (!rdma_buffer) {
        GTEST_SKIP() << "Failed to allocate memory for RDMA test";
    }

    // Use first half as source reference, second half as destination areas
    void* src_base = rdma_buffer;
    void* dst_base = static_cast<char*>(rdma_buffer) + (total_buffer_size / 2);

    // Fill destination areas with zeros (will be overwritten by RDMA transfer)
    std::memset(dst_base, 0, total_buffer_size / 2);

    LOG(INFO) << "Allocated RDMA buffer (" << total_buffer_size << " bytes)";
    LOG(INFO) << "Source base: 0x" << std::hex
              << reinterpret_cast<uint64_t>(src_base) << std::dec;
    LOG(INFO) << "Destination base: 0x" << std::hex
              << reinterpret_cast<uint64_t>(dst_base) << std::dec;

    // Register memory with TransferEngine using "cpu:0" as location hint
    int reg_result = rdma_transfer_engine->registerLocalMemory(
        rdma_buffer, total_buffer_size, "cpu:0");

    if (reg_result != 0) {
        free_memory("", rdma_buffer);
        GTEST_SKIP() << "Failed to register memory with TransferEngine. "
                     << "RDMA device may not be available.";
    }
    LOG(INFO) << "Memory registered for RDMA access";

    // Create TieredBackend and DataManager with RDMA-enabled TransferEngine
    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "DRAM",
                "capacity": 1073741824,
                "priority": 10,
                "allocator_type": "OFFSET"
            }
        ]
    })";
    Json::Value config;
    ASSERT_TRUE(parseJsonString(json_config_str, config));

    auto rdma_tiered_backend = std::make_unique<TieredBackend>();
    // Pass TransferEngine to TieredBackend so DRAM memory gets registered for
    // RDMA
    auto init_backend_result = rdma_tiered_backend->Init(
        config, rdma_transfer_engine.get(), nullptr, nullptr, nullptr);
    ASSERT_TRUE(init_backend_result.has_value())
        << "Failed to initialize TieredBackend";

    LocalTransferConfig local_transfer_config;
    local_transfer_config.mode = LocalTransferMode::MEMCPY;
    auto rdma_data_manager = std::make_unique<DataManager>(
        std::move(rdma_tiered_backend), rdma_transfer_engine, 1024,
        local_transfer_config);
    LOG(INFO) << "DataManager created with RDMA-enabled TransferEngine";

    // Store data in DataManager (this will be the source for RDMA transfer)
    const std::string key = "rdma_multi_batch_test_key";
    const std::string pattern = "RDMA_MULTI_BATCH_PATTERN_";
    const size_t total_data_size = segment_size * num_segments;  // 16MB total
    auto data_copy = std::make_unique<char[]>(total_data_size);
    for (size_t i = 0; i < total_data_size; i++) {
        data_copy[i] = pattern[i % pattern.size()];
    }

    ASSERT_TRUE(
        DoPut(key, data_copy.get(), total_data_size, rdma_data_manager.get()))
        << "Failed to put data into DataManager";
    ASSERT_TRUE(rdma_data_manager->Exist(key)) << "Key not found after Put";
    LOG(INFO) << "Data stored in DataManager (" << total_data_size << " bytes)";

    // Use hostname as segment name (this is how TransferEngine registers
    // segments)
    std::string segment_name = std::string(local_hostname);

    // Create multiple destination buffers across different memory regions
    // This simulates scatter-gather with multiple batches
    std::vector<RemoteBufferDesc> dest_buffers;
    for (int i = 0; i < num_segments; ++i) {
        void* dst_segment = static_cast<char*>(dst_base) + (i * segment_size);
        dest_buffers.push_back({segment_name,
                                reinterpret_cast<uint64_t>(dst_segment),
                                segment_size});
        LOG(INFO) << "Segment " << i << ": address=0x" << std::hex
                  << reinterpret_cast<uint64_t>(dst_segment) << std::dec
                  << ", size=" << segment_size;
    }

    LOG(INFO) << "Attempting multi-batch RDMA transfer with " << num_segments
              << " segments";

    // Perform RDMA transfer via DataManager
    // This will submit all batches first, then wait for all to complete
    auto transfer_result = rdma_data_manager->ReadRemoteData(key, dest_buffers);

    if (!transfer_result.has_value()) {
        LOG(WARNING) << "RDMA multi-batch transfer failed: "
                     << toString(transfer_result.error());
        rdma_transfer_engine->unregisterLocalMemory(rdma_buffer);
        free_memory("", rdma_buffer);
        GTEST_SKIP()
            << "RDMA transfer failed, may need additional configuration";
    }

    LOG(INFO) << "RDMA multi-batch transfer completed!";

    // Verify data for each segment - compare with expected pattern
    bool all_segments_match = true;
    for (int i = 0; i < num_segments; ++i) {
        void* dst_segment = static_cast<char*>(dst_base) + (i * segment_size);
        bool segment_matches = true;

        // Verify first 100 bytes and last 100 bytes of each segment
        for (size_t j = 0; j < 100 && j < segment_size; j++) {
            size_t offset_in_total = (i * segment_size) + j;
            char expected = pattern[offset_in_total % pattern.size()];
            if (static_cast<char*>(dst_segment)[j] != expected) {
                segment_matches = false;
                LOG(ERROR) << "Segment " << i << " mismatch at byte " << j
                           << ": expected '" << expected << "' got '"
                           << static_cast<char*>(dst_segment)[j] << "'";
                break;
            }
        }

        if (segment_matches && segment_size > 200) {
            // Check last 100 bytes
            for (size_t j = segment_size - 100; j < segment_size; j++) {
                size_t offset_in_total = (i * segment_size) + j;
                char expected = pattern[offset_in_total % pattern.size()];
                if (static_cast<char*>(dst_segment)[j] != expected) {
                    segment_matches = false;
                    LOG(ERROR) << "Segment " << i << " mismatch at byte " << j
                               << ": expected '" << expected << "' got '"
                               << static_cast<char*>(dst_segment)[j] << "'";
                    break;
                }
            }
        }

        if (!segment_matches) {
            all_segments_match = false;
            LOG(ERROR) << "Segment " << i << " data verification FAILED";
        } else {
            LOG(INFO) << "✓ Segment " << i << " data verification PASSED";
        }
    }

    if (all_segments_match) {
        LOG(INFO) << "✓ All " << num_segments
                  << " segments transferred correctly via RDMA!";
    }

    EXPECT_TRUE(all_segments_match)
        << "RDMA transferred data does not match source for all segments";

    // Cleanup
    rdma_transfer_engine->unregisterLocalMemory(rdma_buffer);
    free_memory("", rdma_buffer);

    LOG(INFO) << "=== Real RDMA Multi-Batch Transfer Test Completed ===";
}

// Test ReadRemoteData with invalid segment - should fail
// This test verifies that any segment failure results in error
TEST_F(DataManagerTest, RealRDMAMultiBatchPartialFailure) {
    // Check environment variables
    const char* metadata_addr = std::getenv("MC_METADATA_ADDR");
    const char* local_hostname = std::getenv("MC_LOCAL_HOSTNAME");

    if (!metadata_addr || !local_hostname) {
        GTEST_SKIP() << "Skipping real RDMA test: MC_METADATA_ADDR and "
                        "MC_LOCAL_HOSTNAME environment variables not set. "
                        "Set these to enable real RDMA transfer testing.";
    }

    LOG(INFO) << "=== Starting Real RDMA Multi-Batch Partial Failure Test ===";
    LOG(INFO) << "Metadata address: " << metadata_addr;
    LOG(INFO) << "Local hostname: " << local_hostname;

    // Create TransferEngine with RDMA support
    auto rdma_transfer_engine = std::make_shared<TransferEngine>(true);
    int init_result =
        rdma_transfer_engine->init(metadata_addr, local_hostname, "", 12350);

    if (init_result != 0) {
        GTEST_SKIP() << "Failed to initialize TransferEngine (error code: "
                     << init_result
                     << "). RDMA environment may not be configured.";
    }
    LOG(INFO) << "TransferEngine initialized successfully";

    // Allocate memory for RDMA transfer
    const size_t segment_size = 4 * 1024 * 1024;  // 4MB per segment
    const int num_segments = 3;  // 3 segments: 2 valid, 1 invalid
    const size_t total_buffer_size = segment_size * num_segments * 2;
    void* rdma_buffer = allocate_buffer_allocator_memory(total_buffer_size);

    if (!rdma_buffer) {
        GTEST_SKIP() << "Failed to allocate memory for RDMA test";
    }

    void* dst_base = static_cast<char*>(rdma_buffer) + (total_buffer_size / 2);
    std::memset(dst_base, 0, total_buffer_size / 2);

    LOG(INFO) << "Allocated RDMA buffer (" << total_buffer_size << " bytes)";

    // Register memory with TransferEngine
    int reg_result = rdma_transfer_engine->registerLocalMemory(
        rdma_buffer, total_buffer_size, "cpu:0");

    if (reg_result != 0) {
        free_memory("", rdma_buffer);
        GTEST_SKIP() << "Failed to register memory with TransferEngine. "
                     << "RDMA device may not be available.";
    }
    LOG(INFO) << "Memory registered for RDMA access";

    // Create TieredBackend and DataManager
    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "DRAM",
                "capacity": 1073741824,
                "priority": 10,
                "allocator_type": "OFFSET"
            }
        ]
    })";
    Json::Value config;
    ASSERT_TRUE(parseJsonString(json_config_str, config));

    auto rdma_tiered_backend = std::make_unique<TieredBackend>();
    auto init_backend_result = rdma_tiered_backend->Init(
        config, rdma_transfer_engine.get(), nullptr, nullptr, nullptr);
    ASSERT_TRUE(init_backend_result.has_value())
        << "Failed to initialize TieredBackend";

    LocalTransferConfig local_transfer_config;
    local_transfer_config.mode = LocalTransferMode::MEMCPY;
    auto rdma_data_manager = std::make_unique<DataManager>(
        std::move(rdma_tiered_backend), rdma_transfer_engine, 1024,
        local_transfer_config);
    LOG(INFO) << "DataManager created with RDMA-enabled TransferEngine";

    // Store test data
    const std::string key = "rdma_partial_failure_test_key";
    const std::string pattern = "RDMA_PARTIAL_FAIL_PATTERN_";
    const size_t total_data_size = segment_size * num_segments;
    auto data_copy = std::make_unique<char[]>(total_data_size);
    for (size_t i = 0; i < total_data_size; i++) {
        data_copy[i] = pattern[i % pattern.size()];
    }

    ASSERT_TRUE(
        DoPut(key, data_copy.get(), total_data_size, rdma_data_manager.get()))
        << "Failed to put data into DataManager";
    ASSERT_TRUE(rdma_data_manager->Exist(key)) << "Key not found after Put";
    LOG(INFO) << "Data stored in DataManager (" << total_data_size << " bytes)";

    std::string valid_segment = std::string(local_hostname);
    std::string invalid_segment = "non_existent_segment_12345";

    // Create destination buffers: 2 valid segments, 1 invalid segment
    // This simulates partial batch failure scenario
    std::vector<RemoteBufferDesc> dest_buffers = {
        {valid_segment, reinterpret_cast<uint64_t>(dst_base), segment_size},
        {valid_segment,
         reinterpret_cast<uint64_t>(static_cast<char*>(dst_base) +
                                    segment_size),
         segment_size},
        {invalid_segment,
         reinterpret_cast<uint64_t>(static_cast<char*>(dst_base) +
                                    (2 * segment_size)),
         segment_size}  // This segment will fail to open
    };

    LOG(INFO) << "Attempting multi-batch transfer with partial failure:";
    LOG(INFO) << "  Segment 0: " << valid_segment << " (valid)";
    LOG(INFO) << "  Segment 1: " << valid_segment << " (valid)";
    LOG(INFO) << "  Segment 2: " << invalid_segment << " (invalid - will fail)";

    auto transfer_result = rdma_data_manager->ReadRemoteData(key, dest_buffers);

    EXPECT_FALSE(transfer_result.has_value());
    EXPECT_EQ(transfer_result.error(), ErrorCode::TRANSFER_FAIL);

    // Cleanup
    rdma_transfer_engine->unregisterLocalMemory(rdma_buffer);
    free_memory("", rdma_buffer);

    LOG(INFO) << "=== Real RDMA Multi-Batch Partial Failure Test Completed ===";
}

// Test WriteRemoteData with multiple batch transfer
// This test verifies batch submission and unified waiting for WriteRemoteData
TEST_F(DataManagerTest, RealRDMAMultiBatchWriteRemoteData) {
    // Check environment variables
    const char* metadata_addr = std::getenv("MC_METADATA_ADDR");
    const char* local_hostname = std::getenv("MC_LOCAL_HOSTNAME");

    if (!metadata_addr || !local_hostname) {
        GTEST_SKIP() << "Skipping real RDMA test: MC_METADATA_ADDR and "
                        "MC_LOCAL_HOSTNAME environment variables not set. "
                        "Set these to enable real RDMA transfer testing.";
    }

    LOG(INFO) << "=== Starting Real RDMA Multi-Batch WriteRemoteData Test ===";
    LOG(INFO) << "Metadata address: " << metadata_addr;
    LOG(INFO) << "Local hostname: " << local_hostname;

    // Create TransferEngine with RDMA support
    auto rdma_transfer_engine = std::make_shared<TransferEngine>(true);
    int init_result =
        rdma_transfer_engine->init(metadata_addr, local_hostname, "", 12350);

    if (init_result != 0) {
        GTEST_SKIP() << "Failed to initialize TransferEngine (error code: "
                     << init_result
                     << "). RDMA environment may not be configured.";
    }
    LOG(INFO) << "TransferEngine initialized successfully";

    // Allocate memory for RDMA transfer - larger buffer to accommodate multiple
    // segments
    const size_t segment_size = 4 * 1024 * 1024;  // 4MB per segment
    const int num_segments = 4;                   // 4 segments
    const size_t total_buffer_size =
        segment_size * num_segments * 2;  // Double for source + dest
    void* rdma_buffer = allocate_buffer_allocator_memory(total_buffer_size);

    if (!rdma_buffer) {
        GTEST_SKIP() << "Failed to allocate memory for RDMA test";
    }

    // Use first half as source data, second half as destination areas
    void* src_base = rdma_buffer;
    void* dst_base = static_cast<char*>(rdma_buffer) + (total_buffer_size / 2);

    // Fill source areas with test pattern
    const std::string pattern = "RDMA_WRITE_MULTI_BATCH_PATTERN_";
    const size_t total_data_size = segment_size * num_segments;  // 16MB total
    for (size_t i = 0; i < total_data_size; i++) {
        static_cast<char*>(src_base)[i] = pattern[i % pattern.size()];
    }

    LOG(INFO) << "Allocated RDMA buffer (" << total_buffer_size << " bytes)";
    LOG(INFO) << "Source base: 0x" << std::hex
              << reinterpret_cast<uint64_t>(src_base) << std::dec;
    LOG(INFO) << "Destination base: 0x" << std::hex
              << reinterpret_cast<uint64_t>(dst_base) << std::dec;

    // Register memory with TransferEngine using "cpu:0" as location hint
    int reg_result = rdma_transfer_engine->registerLocalMemory(
        rdma_buffer, total_buffer_size, "cpu:0");

    if (reg_result != 0) {
        free_memory("", rdma_buffer);
        GTEST_SKIP() << "Failed to register memory with TransferEngine. "
                     << "RDMA device may not be available.";
    }
    LOG(INFO) << "Memory registered for RDMA access";

    // Create TieredBackend and DataManager with RDMA-enabled TransferEngine
    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "DRAM",
                "capacity": 1073741824,
                "priority": 10,
                "allocator_type": "OFFSET"
            }
        ]
    })";
    Json::Value config;
    ASSERT_TRUE(parseJsonString(json_config_str, config));

    auto rdma_tiered_backend = std::make_unique<TieredBackend>();
    auto init_backend_result = rdma_tiered_backend->Init(
        config, rdma_transfer_engine.get(), nullptr, nullptr, nullptr);
    ASSERT_TRUE(init_backend_result.has_value())
        << "Failed to initialize TieredBackend";

    LocalTransferConfig local_transfer_config;
    local_transfer_config.mode = LocalTransferMode::MEMCPY;
    auto rdma_data_manager = std::make_unique<DataManager>(
        std::move(rdma_tiered_backend), rdma_transfer_engine, 1024,
        local_transfer_config);
    LOG(INFO) << "DataManager created with RDMA-enabled TransferEngine";

    // Use hostname as segment name (this is how TransferEngine registers
    // segments)
    std::string segment_name = std::string(local_hostname);

    // Create multiple source buffers across different memory regions
    // This simulates scatter-gather with multiple batches for WriteRemoteData
    std::vector<RemoteBufferDesc> src_buffers;
    for (int i = 0; i < num_segments; ++i) {
        void* src_segment = static_cast<char*>(src_base) + (i * segment_size);
        src_buffers.push_back({segment_name,
                               reinterpret_cast<uint64_t>(src_segment),
                               segment_size});
        LOG(INFO) << "Source segment " << i << ": address=0x" << std::hex
                  << reinterpret_cast<uint64_t>(src_segment) << std::dec
                  << ", size=" << segment_size;
    }

    const std::string key = "rdma_multi_batch_write_test_key";
    LOG(INFO) << "Attempting multi-batch WriteRemoteData with " << num_segments
              << " segments";

    // Perform RDMA write via DataManager
    // This will submit all batches first, then wait for all to complete
    auto write_result = rdma_data_manager->WriteRemoteData(key, src_buffers);

    if (!write_result.has_value()) {
        LOG(WARNING) << "RDMA multi-batch WriteRemoteData failed: "
                     << toString(write_result.error());
        rdma_transfer_engine->unregisterLocalMemory(rdma_buffer);
        free_memory("", rdma_buffer);
        GTEST_SKIP()
            << "RDMA transfer failed, may need additional configuration";
    }

    LOG(INFO) << "RDMA multi-batch WriteRemoteData completed!";

    // Verify data was written correctly by reading it back via public Get
    auto read_buf = std::make_unique<char[]>(total_data_size);
    ASSERT_TRUE(
        DoGet(key, read_buf.get(), total_data_size, rdma_data_manager.get()))
        << "Failed to get written data";
    const char* stored_data = read_buf.get();
    size_t stored_size = total_data_size;

    // Verify data integrity - compare with expected pattern
    bool all_segments_match = true;
    for (size_t i = 0; i < stored_size; i++) {
        char expected = pattern[i % pattern.size()];
        if (stored_data[i] != expected) {
            all_segments_match = false;
            LOG(ERROR) << "Data mismatch at byte " << i << ": expected '"
                       << expected << "' got '" << stored_data[i] << "'";
            // Only log first few mismatches
            if (i > 100) {
                break;
            }
        }
    }

    if (all_segments_match) {
        LOG(INFO) << "✓ All " << num_segments
                  << " segments written correctly via RDMA WriteRemoteData!";
    }

    EXPECT_TRUE(all_segments_match)
        << "RDMA written data does not match source pattern";

    // Cleanup
    rdma_transfer_engine->unregisterLocalMemory(rdma_buffer);
    free_memory("", rdma_buffer);

    LOG(INFO) << "=== Real RDMA Multi-Batch WriteRemoteData Test Completed ===";
}

// Test WriteRemoteData with invalid segment - should fail and not commit
// This test verifies that any segment failure results in error and no commit
TEST_F(DataManagerTest, RealRDMAMultiBatchWriteRemoteDataPartialFailure) {
    // Check environment variables
    const char* metadata_addr = std::getenv("MC_METADATA_ADDR");
    const char* local_hostname = std::getenv("MC_LOCAL_HOSTNAME");

    if (!metadata_addr || !local_hostname) {
        GTEST_SKIP() << "Skipping real RDMA test: MC_METADATA_ADDR and "
                        "MC_LOCAL_HOSTNAME environment variables not set. "
                        "Set these to enable real RDMA transfer testing.";
    }

    LOG(INFO) << "=== Starting Real RDMA Multi-Batch WriteRemoteData Partial "
                 "Failure Test ===";
    LOG(INFO) << "Metadata address: " << metadata_addr;
    LOG(INFO) << "Local hostname: " << local_hostname;

    // Create TransferEngine with RDMA support
    auto rdma_transfer_engine = std::make_shared<TransferEngine>(true);
    int init_result =
        rdma_transfer_engine->init(metadata_addr, local_hostname, "", 12350);

    if (init_result != 0) {
        GTEST_SKIP() << "Failed to initialize TransferEngine (error code: "
                     << init_result
                     << "). RDMA environment may not be configured.";
    }
    LOG(INFO) << "TransferEngine initialized successfully";

    const size_t segment_size = 4 * 1024 * 1024;  // 4MB per segment
    const int num_segments = 3;                   // 2 valid + 1 invalid
    const size_t total_buffer_size =
        segment_size * num_segments * 2;  // Double for source + dest
    void* rdma_buffer = allocate_buffer_allocator_memory(total_buffer_size);

    if (!rdma_buffer) {
        GTEST_SKIP() << "Failed to allocate memory for RDMA test";
    }

    void* src_base = rdma_buffer;
    const std::string pattern = "RDMA_WRITE_PARTIAL_FAIL_PATTERN_";
    const size_t total_data_size = segment_size * num_segments;

    for (size_t i = 0; i < total_data_size; i++) {
        static_cast<char*>(src_base)[i] = pattern[i % pattern.size()];
    }

    LOG(INFO) << "Allocated RDMA buffer (" << total_buffer_size << " bytes)";

    // Register memory with TransferEngine
    int reg_result = rdma_transfer_engine->registerLocalMemory(
        rdma_buffer, total_buffer_size, "cpu:0");

    if (reg_result != 0) {
        free_memory("", rdma_buffer);
        GTEST_SKIP() << "Failed to register memory with TransferEngine. "
                     << "RDMA device may not be available.";
    }
    LOG(INFO) << "Memory registered for RDMA access";

    // Create TieredBackend and DataManager
    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "DRAM",
                "capacity": 1073741824,
                "priority": 10,
                "allocator_type": "OFFSET"
            }
        ]
    })";
    Json::Value config;
    ASSERT_TRUE(parseJsonString(json_config_str, config));

    auto rdma_tiered_backend = std::make_unique<TieredBackend>();
    auto init_backend_result = rdma_tiered_backend->Init(
        config, rdma_transfer_engine.get(), nullptr, nullptr, nullptr);
    ASSERT_TRUE(init_backend_result.has_value())
        << "Failed to initialize TieredBackend";

    LocalTransferConfig local_transfer_config;
    local_transfer_config.mode = LocalTransferMode::MEMCPY;
    auto rdma_data_manager = std::make_unique<DataManager>(
        std::move(rdma_tiered_backend), rdma_transfer_engine, 1024,
        local_transfer_config);
    LOG(INFO) << "DataManager created with RDMA-enabled TransferEngine";

    std::string valid_segment = std::string(local_hostname);
    std::string invalid_segment = "non_existent_segment_12345";
    const std::string key = "rdma_partial_failure_write_test_key";

    std::vector<RemoteBufferDesc> src_buffers = {
        {valid_segment, reinterpret_cast<uint64_t>(src_base), segment_size},
        {valid_segment,
         reinterpret_cast<uint64_t>(static_cast<char*>(src_base) +
                                    segment_size),
         segment_size},
        {invalid_segment,
         reinterpret_cast<uint64_t>(static_cast<char*>(src_base) +
                                    (2 * segment_size)),
         segment_size}};

    LOG(INFO) << "Attempting WriteRemoteData with partial failure:";
    LOG(INFO) << "  Segment 0: " << valid_segment << " (valid)";
    LOG(INFO) << "  Segment 1: " << valid_segment << " (valid)";
    LOG(INFO) << "  Segment 2: " << invalid_segment << " (invalid - will fail)";

    auto write_result = rdma_data_manager->WriteRemoteData(key, src_buffers);

    EXPECT_FALSE(write_result.has_value());
    EXPECT_EQ(write_result.error(), ErrorCode::TRANSFER_FAIL);

    // Verify key was NOT committed (any segment failure should prevent commit)
    EXPECT_FALSE(rdma_data_manager->Exist(key));

    // Cleanup
    rdma_transfer_engine->unregisterLocalMemory(rdma_buffer);
    free_memory("", rdma_buffer);

    LOG(INFO)
        << "=== Real RDMA Multi-Batch WriteRemoteData Partial Failure Test "
           "Completed ===";
}

}  // namespace mooncake

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

        tiered_backend_ = std::make_unique<TieredBackend>();
        // transfer_engine_ is nullptr when initializing tiered_backend_
        // only for local access test
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

    std::unique_ptr<DataManager> data_manager_;
    std::unique_ptr<TieredBackend> tiered_backend_;
    std::shared_ptr<TransferEngine> transfer_engine_;
    std::optional<UUID> saved_tier_id_;
};

// Test Put operation - success case
TEST_F(DataManagerTest, PutSuccess) {
    const std::string key = "test_key";
    const std::string test_data = "Hello, World!";
    const size_t data_size = test_data.size();

    auto buffer = StringToBuffer(test_data);
    auto result = data_manager_->Put(key, std::move(buffer), data_size);

    ASSERT_TRUE(result.has_value())
        << "Put failed with error: " << toString(result.error());

    // Verify the key exists in the backend
    auto get_result = data_manager_->Get(key);
    ASSERT_TRUE(get_result.has_value()) << "Get failed after Put";

    // DFX: Verify handle and buffer validity
    auto handle = get_result.value();
    ASSERT_NE(handle, nullptr) << "Handle should not be null";
    ASSERT_NE(handle->loc.data.buffer, nullptr) << "Buffer should not be null";
    ASSERT_NE(handle->loc.tier, nullptr) << "Tier pointer should not be null";

    EXPECT_EQ(handle->loc.data.buffer->size(), data_size);
}

// Test Put operation - allocation failure (by using huge size)
TEST_F(DataManagerTest, PutAllocationFailure) {
    const std::string key = "test_key";
    // Try to allocate more than available capacity (1GB)
    const size_t huge_size = 2ULL * 1024 * 1024 * 1024;  // 2GB

    auto test_data = CreateTestData(1024);
    auto result = data_manager_->Put(key, std::move(test_data), huge_size);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::NO_AVAILABLE_HANDLE);
}

// Test Put with tier_id
TEST_F(DataManagerTest, PutWithTierId) {
    const std::string key = "test_key_with_tier";
    const std::string test_data = "Test data with tier";
    auto tier_id = GetTierId();

    ASSERT_TRUE(tier_id.has_value()) << "No tier available";

    auto buffer = StringToBuffer(test_data);
    auto result =
        data_manager_->Put(key, std::move(buffer), test_data.size(), tier_id);

    ASSERT_TRUE(result.has_value()) << "Put with tier_id failed";

    // Verify we can get it back
    auto get_result = data_manager_->Get(key, tier_id);
    ASSERT_TRUE(get_result.has_value());
}

// Test Get operation - success case
TEST_F(DataManagerTest, GetSuccess) {
    const std::string key = "test_get_key";
    const std::string test_data = "Test data for Get";

    // First, put the data
    auto buffer = StringToBuffer(test_data);
    auto put_result =
        data_manager_->Put(key, std::move(buffer), test_data.size());
    ASSERT_TRUE(put_result.has_value()) << "Put failed in Get test";

    // Then, get it
    auto get_result = data_manager_->Get(key);

    ASSERT_TRUE(get_result.has_value())
        << "Get failed with error: " << toString(get_result.error());
    ASSERT_NE(get_result.value()->loc.data.buffer, nullptr)
        << "Buffer should not be null";
    EXPECT_EQ(get_result.value()->loc.data.buffer->size(), test_data.size());
}

// Test Get operation - key not found
TEST_F(DataManagerTest, GetKeyNotFound) {
    const std::string key = "non_existent_key";

    auto result = data_manager_->Get(key);

    ASSERT_FALSE(result.has_value());
    // TieredBackend returns INVALID_KEY for not found keys
    EXPECT_EQ(result.error(), ErrorCode::INVALID_KEY);
}

// Test Get with tier_id
TEST_F(DataManagerTest, GetWithTierId) {
    const std::string key = "test_get_tier_key";
    const std::string test_data = "Test data";
    auto tier_id = GetTierId();

    ASSERT_TRUE(tier_id.has_value()) << "No tier available";

    // Put with specific tier
    auto buffer = StringToBuffer(test_data);
    auto put_result =
        data_manager_->Put(key, std::move(buffer), test_data.size(), tier_id);
    ASSERT_TRUE(put_result.has_value());

    // Get with same tier
    auto get_result = data_manager_->Get(key, tier_id);
    ASSERT_TRUE(get_result.has_value());
    ASSERT_NE(get_result.value()->loc.data.buffer, nullptr)
        << "Buffer should not be null";
    EXPECT_EQ(get_result.value()->loc.data.buffer->size(), test_data.size());
}

// Test Delete operation - success case
TEST_F(DataManagerTest, DeleteSuccess) {
    const std::string key = "test_delete_key";
    const std::string test_data = "Test data for Delete";

    // First, put the data
    auto buffer = StringToBuffer(test_data);
    auto put_result =
        data_manager_->Put(key, std::move(buffer), test_data.size());
    ASSERT_TRUE(put_result.has_value());

    // Then, delete it
    auto delete_result = data_manager_->Delete(key);

    ASSERT_TRUE(delete_result.has_value())
        << "Delete failed with error: " << toString(delete_result.error());

    // Verify it's deleted
    auto get_result = data_manager_->Get(key);
    ASSERT_FALSE(get_result.has_value());
    EXPECT_EQ(get_result.error(), ErrorCode::INVALID_KEY);
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
    auto tier_id = GetTierId();

    ASSERT_TRUE(tier_id.has_value()) << "No tier available";

    // Put with specific tier
    auto buffer = StringToBuffer(test_data);
    auto put_result =
        data_manager_->Put(key, std::move(buffer), test_data.size(), tier_id);
    ASSERT_TRUE(put_result.has_value());

    // Delete with same tier
    auto delete_result = data_manager_->Delete(key, tier_id);
    ASSERT_TRUE(delete_result.has_value())
        << "Delete failed with error: " << toString(delete_result.error());

    // Verify it's deleted
    auto get_result = data_manager_->Get(key, tier_id);
    ASSERT_FALSE(get_result.has_value());
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
            results[i] =
                data_manager_->Put(keys[i], std::move(buffer), data.size());
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
        auto get_result = data_manager_->Get(keys[i]);
        ASSERT_TRUE(get_result.has_value())
            << "Get failed for key: " << keys[i];
    }
}

// Test Put-Get-Delete sequence
TEST_F(DataManagerTest, PutGetDeleteSequence) {
    const std::string key = "sequence_test_key";
    const std::string test_data = "Sequence test data";

    // Put
    auto buffer = StringToBuffer(test_data);
    auto put_result =
        data_manager_->Put(key, std::move(buffer), test_data.size());
    ASSERT_TRUE(put_result.has_value());

    // Get
    auto get_result = data_manager_->Get(key);
    ASSERT_TRUE(get_result.has_value());
    ASSERT_NE(get_result.value()->loc.data.buffer, nullptr)
        << "Buffer should not be null";
    EXPECT_EQ(get_result.value()->loc.data.buffer->size(), test_data.size());

    // Delete
    auto delete_result = data_manager_->Delete(key);
    ASSERT_TRUE(delete_result.has_value())
        << "Delete failed with error: " << toString(delete_result.error());

    // Verify deleted
    auto get_after_delete = data_manager_->Get(key);
    ASSERT_FALSE(get_after_delete.has_value());
}

// Test lock contention with 1025 concurrent Put operations
// Since there are only 1024 lock shards, at least 2 keys will map to the same
// lock
TEST_F(DataManagerTest, LockContentionTest) {
    const int num_keys = 1025;
    const size_t kLockShardCount = data_manager_->GetLockShardCount();

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

            results[i] =
                data_manager_->Put(keys[i], std::move(buffer), data.size());

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
        auto get_result = data_manager_->Get(keys[i]);
        if (get_result.has_value()) {
            retrieved_count++;
        } else {
            LOG(ERROR) << "Get failed for key: " << keys[i]
                       << ", error: " << toString(get_result.error());
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
        ASSERT_TRUE(data_manager_->Put(key, std::move(buffer), data.size())
                        .has_value());
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
                auto result = data_manager_->Get(keys[i]);
                if (result.has_value()) {
                    successful_gets++;
                    // Verify the handle is valid and data is accessible
                    EXPECT_NE(result.value()->loc.data.buffer, nullptr);
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

// Test that AllocationHandle (shared_ptr) keeps data alive even after deletion
// from DataManager. This verifies the reference counting mechanism works
// correctly without the read lock in DataManager::Get.
TEST_F(DataManagerTest, HandleKeepsDataAliveAfterDelete) {
    const std::string key = "handle_lifetime_test_key";
    const std::string test_data = "HandleLifetimeTestData_1234567890";

    // Put data
    auto buffer = StringToBuffer(test_data);
    ASSERT_TRUE(data_manager_->Put(key, std::move(buffer), test_data.size())
                    .has_value());

    // Get handle (increases ref count)
    auto handle_result = data_manager_->Get(key);
    ASSERT_TRUE(handle_result.has_value());
    auto handle = handle_result.value();

    // Verify handle is valid
    ASSERT_NE(handle, nullptr);
    ASSERT_NE(handle->loc.data.buffer, nullptr);
    EXPECT_EQ(handle->loc.data.buffer->size(), test_data.size());

    // Delete the key from DataManager
    ASSERT_TRUE(data_manager_->Delete(key).has_value());

    // Verify key is no longer accessible via DataManager
    auto get_after_delete = data_manager_->Get(key);
    EXPECT_FALSE(get_after_delete.has_value());
    EXPECT_EQ(get_after_delete.error(), ErrorCode::INVALID_KEY);

    // But the handle we obtained earlier should still be valid!
    // This is because shared_ptr reference counting keeps the data alive
    ASSERT_NE(handle->loc.data.buffer, nullptr)
        << "Handle should still be valid after key deletion";
    EXPECT_EQ(handle->loc.data.buffer->size(), test_data.size())
        << "Data size should be unchanged";

    // Verify data content is still intact
    char* data_ptr = reinterpret_cast<char*>(handle->loc.data.buffer->data());
    std::string retrieved_data(data_ptr, test_data.size());
    EXPECT_EQ(retrieved_data, test_data)
        << "Data content should be unchanged after key deletion";

    LOG(INFO) << "Handle successfully kept data alive after deletion";
}

}  // namespace mooncake

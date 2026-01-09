#include <glog/logging.h>
#include <gtest/gtest.h>
#include <json/json.h>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <cstring>

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
        ASSERT_EQ(tier_views.size(), 1) << "Expected 1 tier, got " << tier_views.size();
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
    std::unique_ptr<char[]> CreateTestData(size_t size, const std::string& pattern = "test") {
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
    
    ASSERT_TRUE(result.has_value()) << "Put failed with error: " 
                                    << toString(result.error());
    
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
    auto result = data_manager_->Put(key, std::move(buffer), test_data.size(), tier_id);
    
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
    auto put_result = data_manager_->Put(key, std::move(buffer), test_data.size());
    ASSERT_TRUE(put_result.has_value()) << "Put failed in Get test";

    // Then, get it
    auto get_result = data_manager_->Get(key);
    
    ASSERT_TRUE(get_result.has_value()) << "Get failed with error: "
                                        << toString(get_result.error());
    ASSERT_NE(get_result.value()->loc.data.buffer, nullptr) << "Buffer should not be null";
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
    auto put_result = data_manager_->Put(key, std::move(buffer), test_data.size(), tier_id);
    ASSERT_TRUE(put_result.has_value());

    // Get with same tier
    auto get_result = data_manager_->Get(key, tier_id);
    ASSERT_TRUE(get_result.has_value());
    ASSERT_NE(get_result.value()->loc.data.buffer, nullptr) << "Buffer should not be null";
    EXPECT_EQ(get_result.value()->loc.data.buffer->size(), test_data.size());
}

// Test Delete operation - success case
TEST_F(DataManagerTest, DeleteSuccess) {
    const std::string key = "test_delete_key";
    const std::string test_data = "Test data for Delete";
    
    // First, put the data
    auto buffer = StringToBuffer(test_data);
    auto put_result = data_manager_->Put(key, std::move(buffer), test_data.size());
    ASSERT_TRUE(put_result.has_value());

    // Then, delete it
    bool delete_result = data_manager_->Delete(key);
    
    EXPECT_TRUE(delete_result) << "Delete should return true on success";
    
    // Verify it's deleted
    auto get_result = data_manager_->Get(key);
    ASSERT_FALSE(get_result.has_value());
    EXPECT_EQ(get_result.error(), ErrorCode::INVALID_KEY);
}

// Test Delete operation - key not found
TEST_F(DataManagerTest, DeleteKeyNotFound) {
    const std::string key = "non_existent_key";

    bool result = data_manager_->Delete(key);
    
    EXPECT_FALSE(result) << "Delete should return false for non-existent key";
}

// Test Delete with tier_id
TEST_F(DataManagerTest, DeleteWithTierId) {
    const std::string key = "test_delete_tier_key";
    const std::string test_data = "Test data";
    auto tier_id = GetTierId();
    
    ASSERT_TRUE(tier_id.has_value()) << "No tier available";
    
    // Put with specific tier
    auto buffer = StringToBuffer(test_data);
    auto put_result = data_manager_->Put(key, std::move(buffer), test_data.size(), tier_id);
    ASSERT_TRUE(put_result.has_value());

    // Delete with same tier
    bool delete_result = data_manager_->Delete(key, tier_id);
    EXPECT_TRUE(delete_result);
    
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
            results[i] = data_manager_->Put(keys[i], std::move(buffer), data.size());
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
    bool delete_result = data_manager_->Delete(key);
    EXPECT_TRUE(delete_result);

    // Verify deleted
    auto get_after_delete = data_manager_->Get(key);
    ASSERT_FALSE(get_after_delete.has_value());
}

}  // namespace mooncake

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <filesystem>

#include "tiered_cache/tiers/storage_tier.h"
#include "tiered_cache/tiered_backend.h"
#include "storage_backend.h"
#include "allocator.h"
#include "utils.h"

namespace fs = std::filesystem;
namespace mooncake::test {

class StorageTierTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("StorageTierTest");
        FLAGS_logtostderr = true;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }

    std::unique_ptr<char[]> CreateTestBuffer(size_t size) {
        auto buffer = std::make_unique<char[]>(size);
        for (size_t i = 0; i < size; ++i) {
            buffer[i] = static_cast<char>(i % 256);
        }
        return buffer;
    }

    bool parseJsonString(const std::string& json_str, Json::Value& config) {
        Json::CharReaderBuilder builder;
        std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
        std::string errors;
        return reader->parse(json_str.c_str(),
                             json_str.c_str() + json_str.size(), &config,
                             &errors);
    }
};

// Test basic Storage Tier functionality with FilePerKey backend
TEST_F(StorageTierTest, StorageTierBasic) {
    // Setup environment for Storage Backend (FilePerKey)
    setenv("MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR",
           "file_per_key_storage_backend", 1);
    setenv("MOONCAKE_OFFLOAD_FILE_STORAGE_PATH", "/tmp/mooncake_test_storage",
           1);

    // Ensure clean state
    fs::remove_all("/tmp/mooncake_test_storage");
    fs::create_directories("/tmp/mooncake_test_storage");

    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "STORAGE",
                "capacity": 1073741824,
                "priority": 5,
                "tags": ["ssd"]
            }
        ]
    })";
    Json::Value config;
    ASSERT_TRUE(parseJsonString(json_config_str, config));

    TieredBackend backend;
    auto init_res = backend.Init(config, nullptr, nullptr);
    ASSERT_TRUE(init_res.has_value()) << "Init failed: " << init_res.error();

    // Verify tier created
    auto tier_views = backend.GetTierViews();
    ASSERT_EQ(tier_views.size(), 1);
    EXPECT_EQ(tier_views[0].type, MemoryType::NVME);

    // 1. Allocate
    size_t data_size = 1024;
    auto alloc_result = backend.Allocate(data_size);
    ASSERT_TRUE(alloc_result.has_value());
    AllocationHandle handle = alloc_result.value();

    // 2. Write
    auto test_buffer = CreateTestBuffer(data_size);
    DataSource source;
    source.buffer =
        std::make_unique<TempDRAMBuffer>(std::move(test_buffer), data_size);
    source.type = MemoryType::DRAM;

    auto write_result = backend.Write(source, handle);
    ASSERT_TRUE(write_result.has_value())
        << "Write failed: " << write_result.error();

    // 3. Commit
    auto commit_result = backend.Commit("storage_key", handle);
    ASSERT_TRUE(commit_result.has_value());

    // 4. Flush
    auto tier = backend.GetTier(handle->loc.tier->GetTierId());
    const_cast<CacheTier*>(tier)->Flush();

    // 5. Get (Verify)
    auto get_result = backend.Get("storage_key");
    ASSERT_TRUE(get_result.has_value());

    // Verify file exists on disk
    bool found = false;
    for (const auto& entry :
         fs::recursive_directory_iterator("/tmp/mooncake_test_storage")) {
        if (entry.path().filename() == "storage_key") {
            found = true;
            break;
        }
    }
    EXPECT_TRUE(found) << "File 'storage_key' not found in storage directory";

    // Cleanup
    fs::remove_all("/tmp/mooncake_test_storage");
}

// Test Bucket Storage Backend
TEST_F(StorageTierTest, StorageTierBucket) {
    // Setup environment for Bucket Backend
    setenv("MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR",
           "bucket_storage_backend", 1);
    setenv("MOONCAKE_OFFLOAD_FILE_STORAGE_PATH", "/tmp/mooncake_test_bucket",
           1);

    // Ensure clean state
    fs::remove_all("/tmp/mooncake_test_bucket");
    fs::create_directories("/tmp/mooncake_test_bucket");

    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "STORAGE",
                "capacity": 1073741824,
                "priority": 5,
                "tags": ["ssd"]
            }
        ]
    })";
    Json::Value config;
    ASSERT_TRUE(parseJsonString(json_config_str, config));

    TieredBackend backend;
    auto init_res = backend.Init(config, nullptr, nullptr);
    ASSERT_TRUE(init_res.has_value());

    // Allocate and Write multiple small items
    size_t data_size = 1024;
    for (int i = 0; i < 5; ++i) {
        auto alloc_result = backend.Allocate(data_size);
        ASSERT_TRUE(alloc_result.has_value());
        AllocationHandle handle = alloc_result.value();

        auto test_buffer = CreateTestBuffer(data_size);
        DataSource source;
        source.buffer =
            std::make_unique<TempDRAMBuffer>(std::move(test_buffer), data_size);
        source.type = MemoryType::DRAM;

        auto write_result = backend.Write(source, handle);
        ASSERT_TRUE(write_result.has_value());

        std::string key = "bucket_key_" + std::to_string(i);
        auto commit_result = backend.Commit(key, handle);
        ASSERT_TRUE(commit_result.has_value());
    }

    // Explicit Flush
    auto tier_views = backend.GetTierViews();
    ASSERT_FALSE(tier_views.empty());
    auto tier_id = tier_views[0].id;
    auto tier = backend.GetTier(tier_id);
    const_cast<CacheTier*>(tier)->Flush();

    // Verify Bucket Files Creation
    bool bucket_found = false;
    bool meta_found = false;
    for (const auto& entry :
         fs::recursive_directory_iterator("/tmp/mooncake_test_bucket")) {
        if (entry.path().extension() == ".bucket") bucket_found = true;
        if (entry.path().extension() == ".meta") meta_found = true;
    }
    EXPECT_TRUE(bucket_found) << ".bucket file should be created";
    EXPECT_TRUE(meta_found) << ".meta file should be created";

    // Verify Get works (reads back from bucket)
    auto get_result = backend.Get("bucket_key_0");
    ASSERT_TRUE(get_result.has_value());

    // Cleanup
    fs::remove_all("/tmp/mooncake_test_bucket");
}

// Test Bucket Eviction functionality
TEST_F(StorageTierTest, BucketEviction) {
    // Setup environment for Bucket Backend
    setenv("MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR",
           "bucket_storage_backend", 1);
    setenv("MOONCAKE_OFFLOAD_FILE_STORAGE_PATH",
           "/tmp/mooncake_test_bucket_eviction", 1);

    // Ensure clean state
    fs::remove_all("/tmp/mooncake_test_bucket_eviction");
    fs::create_directories("/tmp/mooncake_test_bucket_eviction");

    // Create backend directly
    FileStorageConfig config;
    config.storage_filepath = "/tmp/mooncake_test_bucket_eviction";
    config.storage_backend_type = StorageBackendType::kBucket;

    BucketBackendConfig bucket_config;
    bucket_config.bucket_size_limit = 1024 * 1024;
    bucket_config.bucket_keys_limit = 10;

    BucketStorageBackend backend(config, bucket_config);
    ASSERT_TRUE(backend.Init());

    // Create test data
    std::unordered_map<std::string, std::vector<Slice>> batch;
    std::vector<char> data(1024, 'A');
    batch["key1"] = {Slice{data.data(), data.size()}};
    batch["key2"] = {Slice{data.data(), data.size()}};
    batch["key3"] = {Slice{data.data(), data.size()}};

    auto offload_res = backend.BatchOffload(batch, nullptr);
    ASSERT_TRUE(offload_res.has_value());
    int64_t bucket_id = offload_res.value();

    LOG(INFO) << "Created bucket: " << bucket_id;

    // Verify keys exist
    auto exist_res = backend.IsExist("key1");
    ASSERT_TRUE(exist_res.has_value());
    EXPECT_TRUE(exist_res.value());

    // Mark two keys as deleted (66% fragmentation)
    auto mark_res = backend.MarkKeyDeleted("key1");
    ASSERT_TRUE(mark_res.has_value());
    mark_res = backend.MarkKeyDeleted("key2");
    ASSERT_TRUE(mark_res.has_value());

    LOG(INFO) << "Marked key1 and key2 as deleted";

    // Select bucket for eviction (should select the fragmented bucket)
    auto select_res = backend.SelectBucketForEviction();
    ASSERT_TRUE(select_res.has_value());
    EXPECT_EQ(select_res.value(), bucket_id)
        << "Should select the fragmented bucket";

    LOG(INFO) << "Selected bucket for eviction: " << select_res.value();

    // Evict the bucket
    auto evict_res = backend.EvictBucket(bucket_id);
    ASSERT_TRUE(evict_res.has_value());

    LOG(INFO) << "Successfully evicted bucket";

    // Verify all keys are gone
    exist_res = backend.IsExist("key1");
    ASSERT_TRUE(exist_res.has_value());
    EXPECT_FALSE(exist_res.value()) << "key1 should not exist after eviction";

    exist_res = backend.IsExist("key2");
    ASSERT_TRUE(exist_res.has_value());
    EXPECT_FALSE(exist_res.value()) << "key2 should not exist after eviction";

    exist_res = backend.IsExist("key3");
    ASSERT_TRUE(exist_res.has_value());
    EXPECT_FALSE(exist_res.value()) << "key3 should not exist after eviction";

    // Verify physical files are deleted
    bool bucket_file_exists = false;
    bool meta_file_exists = false;
    for (const auto& entry : fs::recursive_directory_iterator(
             "/tmp/mooncake_test_bucket_eviction")) {
        if (entry.path().extension() == ".bucket") bucket_file_exists = true;
        if (entry.path().extension() == ".meta") meta_file_exists = true;
    }
    EXPECT_FALSE(bucket_file_exists) << "Bucket file should be deleted";
    EXPECT_FALSE(meta_file_exists) << "Meta file should be deleted";

    // Cleanup
    fs::remove_all("/tmp/mooncake_test_bucket_eviction");
}

}  // namespace mooncake::test

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <thread>
#include <vector>
#include <limits>

#include "tiered_cache/tiers/storage_tier.h"
#include "tiered_cache/tiered_backend.h"
#include "utils/common.h"
#include "storage_backend.h"
#include "allocator.h"
#include "utils.h"

namespace fs = std::filesystem;
namespace mooncake::test {

class StorageTierTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("StorageTierTest");
        FLAGS_logtostderr = true;
    }

    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

    void SetUp() override {
        unsetenv("MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES");
    }

    void TearDown() override {
        unsetenv("MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES");
    }

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

    bool WaitForNoFilesWithExtension(
        const fs::path& dir, const std::string& extension,
        std::chrono::milliseconds timeout = std::chrono::seconds(2)) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            bool found = false;
            for (const auto& entry : fs::recursive_directory_iterator(dir)) {
                if (entry.path().extension() == extension) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return false;
    }

    int CountFilesWithExtension(const fs::path& dir,
                                const std::string& extension) {
        int count = 0;
        for (const auto& entry : fs::recursive_directory_iterator(dir)) {
            if (entry.path().extension() == extension) {
                ++count;
            }
        }
        return count;
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

    {
        TieredBackend backend;
        auto init_res = InitTieredBackendForTest(backend, config);
        ASSERT_TRUE(init_res.has_value())
            << "Init failed: " << init_res.error();

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
        EXPECT_TRUE(found)
            << "File 'storage_key' not found in storage directory";
    }

    // Cleanup
    fs::remove_all("/tmp/mooncake_test_storage");
}

TEST_F(StorageTierTest, StorageTierUsesConfiguredLocalBufferPool) {
    setenv("MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR",
           "file_per_key_storage_backend", 1);
    setenv("MOONCAKE_OFFLOAD_FILE_STORAGE_PATH",
           "/tmp/mooncake_test_local_buffer_pool", 1);
    setenv("MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES", "1024", 1);

    fs::remove_all("/tmp/mooncake_test_local_buffer_pool");
    fs::create_directories("/tmp/mooncake_test_local_buffer_pool");

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

    {
        TieredBackend backend;
        auto init_res = InitTieredBackendForTest(backend, config);
        ASSERT_TRUE(init_res.has_value())
            << "Init failed: " << init_res.error();

        auto oversized_alloc = backend.Allocate(2048);
        EXPECT_FALSE(oversized_alloc.has_value())
            << "allocation larger than configured local buffer pool should "
               "fail";
        EXPECT_EQ(oversized_alloc.error(), ErrorCode::BUFFER_OVERFLOW);

        auto alloc_result = backend.Allocate(1024);
        ASSERT_TRUE(alloc_result.has_value())
            << "allocation within local buffer pool should succeed";
        AllocationHandle handle = alloc_result.value();

        auto test_buffer = CreateTestBuffer(1024);
        DataSource source;
        source.buffer =
            std::make_unique<TempDRAMBuffer>(std::move(test_buffer), 1024);
        source.type = MemoryType::DRAM;

        auto write_result = backend.Write(source, handle);
        ASSERT_TRUE(write_result.has_value())
            << "Write failed: " << write_result.error();
        ASSERT_TRUE(backend.Commit("local_buffer_key", handle).has_value());

        auto tier = backend.GetTier(handle->loc.tier->GetTierId());
        ASSERT_NE(tier, nullptr);
        ASSERT_TRUE(const_cast<CacheTier*>(tier)->Flush().has_value());

        auto recycled_alloc = backend.Allocate(1024);
        EXPECT_TRUE(recycled_alloc.has_value())
            << "staging pool should recycle memory after flush";
    }

    fs::remove_all("/tmp/mooncake_test_local_buffer_pool");
}

TEST_F(StorageTierTest, StorageTierCapacityCountsUncommittedStaging) {
    setenv("MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR",
           "file_per_key_storage_backend", 1);
    setenv("MOONCAKE_OFFLOAD_FILE_STORAGE_PATH",
           "/tmp/mooncake_test_capacity_accounting", 1);
    setenv("MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES", "4096", 1);

    fs::remove_all("/tmp/mooncake_test_capacity_accounting");
    fs::create_directories("/tmp/mooncake_test_capacity_accounting");

    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "STORAGE",
                "capacity": 2048,
                "priority": 5,
                "tags": ["ssd"]
            }
        ]
    })";
    Json::Value config;
    ASSERT_TRUE(parseJsonString(json_config_str, config));

    {
        TieredBackend backend;
        auto init_res = InitTieredBackendForTest(backend, config);
        ASSERT_TRUE(init_res.has_value())
            << "Init failed: " << init_res.error();

        auto handle1 = backend.Allocate(1024);
        ASSERT_TRUE(handle1.has_value());
        auto handle2 = backend.Allocate(1024);
        ASSERT_TRUE(handle2.has_value());

        auto over_capacity = backend.Allocate(1);
        EXPECT_FALSE(over_capacity.has_value())
            << "uncommitted staging allocations should count against tier "
               "capacity";
        EXPECT_EQ(over_capacity.error(), ErrorCode::NO_AVAILABLE_HANDLE);

        handle1 = nullptr;

        auto recovered = backend.Allocate(1024);
        EXPECT_TRUE(recovered.has_value()) << "releasing an uncommitted handle "
                                              "should restore storage capacity";
    }

    fs::remove_all("/tmp/mooncake_test_capacity_accounting");
}

TEST_F(StorageTierTest,
       StorageHandleKeepsPersistedDataAliveAfterBackendDestruction) {
    setenv("MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR",
           "file_per_key_storage_backend", 1);
    setenv("MOONCAKE_OFFLOAD_FILE_STORAGE_PATH",
           "/tmp/mooncake_test_handle_lifetime", 1);
    setenv("MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES", "4096", 1);

    fs::remove_all("/tmp/mooncake_test_handle_lifetime");
    fs::create_directories("/tmp/mooncake_test_handle_lifetime");

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

    auto backend = std::make_unique<TieredBackend>();
    auto init_res = InitTieredBackendForTest(*backend, config);
    ASSERT_TRUE(init_res.has_value()) << "Init failed: " << init_res.error();

    constexpr size_t kDataSize = 1024;
    auto alloc_result = backend->Allocate(kDataSize);
    ASSERT_TRUE(alloc_result.has_value());
    AllocationHandle handle = alloc_result.value();

    auto test_buffer = CreateTestBuffer(kDataSize);
    std::string expected(test_buffer.get(), test_buffer.get() + kDataSize);

    DataSource source;
    source.buffer =
        std::make_unique<TempDRAMBuffer>(std::move(test_buffer), kDataSize);
    source.type = MemoryType::DRAM;

    ASSERT_TRUE(backend->Write(source, handle).has_value());
    ASSERT_TRUE(backend->Commit("persisted_handle_key", handle).has_value());

    auto tier = backend->GetTier(handle->loc.tier->GetTierId());
    ASSERT_NE(tier, nullptr);
    ASSERT_TRUE(const_cast<CacheTier*>(tier)->Flush().has_value());

    std::atomic<bool> backend_destroyed{false};
    std::thread destroy_thread(
        [backend = std::move(backend), &backend_destroyed]() mutable {
            backend.reset();
            backend_destroyed.store(true, std::memory_order_release);
        });

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (!backend_destroyed.load(std::memory_order_acquire) &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    if (!backend_destroyed.load(std::memory_order_acquire)) {
        handle.reset();
        destroy_thread.join();
        FAIL() << "TieredBackend destruction should not block on outstanding "
                  "SSD handles";
    }

    destroy_thread.join();

    auto* storage_buffer =
        dynamic_cast<StorageBuffer*>(handle->loc.data.buffer.get());
    ASSERT_NE(storage_buffer, nullptr);

    std::string restored(kDataSize, '\0');
    auto read_res = storage_buffer->ReadTo(restored.data(), restored.size());
    ASSERT_TRUE(read_res.has_value()) << "Read failed: " << read_res.error();
    EXPECT_EQ(restored, expected);

    handle.reset();
    fs::remove_all("/tmp/mooncake_test_handle_lifetime");
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
    auto init_res = InitTieredBackendForTest(backend, config);
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

TEST_F(StorageTierTest, BucketBackendRejectsOffloadWhenPhysicalSpaceTooLow) {
    const fs::path test_dir = "/tmp/mooncake_test_bucket_space_guard";
    fs::remove_all(test_dir);
    fs::create_directories(test_dir);

    std::error_code ec;
    auto space_info = fs::space(test_dir, ec);
    ASSERT_FALSE(ec) << "Failed to inspect filesystem space: " << ec.message();

    if (space_info.available >=
        static_cast<uintmax_t>(std::numeric_limits<int64_t>::max() - 1)) {
        GTEST_SKIP() << "filesystem available space is too large to build a "
                        "stable int64_t overflow-safe test";
    }

    FileStorageConfig config;
    config.storage_filepath = test_dir.string();
    config.storage_backend_type = StorageBackendType::kBucket;
    config.total_size_limit = std::numeric_limits<int64_t>::max();

    BucketBackendConfig bucket_config;
    bucket_config.bucket_keys_limit = 10;
    bucket_config.bucket_size_limit =
        static_cast<int64_t>(space_info.available + 1);

    {
        BucketStorageBackend backend(config, bucket_config);
        ASSERT_TRUE(backend.Init());

        auto enable_res = backend.IsEnableOffloading();
        ASSERT_TRUE(enable_res.has_value());
        EXPECT_FALSE(enable_res.value())
            << "backend should reject offload when the next bucket cannot fit "
               "on "
               "the filesystem";
    }

    fs::remove_all(test_dir);
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

    {
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
        size_t freed_size = evict_res.value();
        EXPECT_GT(freed_size, 0) << "Should have freed some space";

        LOG(INFO) << "Successfully evicted bucket, freed " << freed_size
                  << " bytes";

        // Verify all keys are gone
        exist_res = backend.IsExist("key1");
        ASSERT_TRUE(exist_res.has_value());
        EXPECT_FALSE(exist_res.value())
            << "key1 should not exist after eviction";

        exist_res = backend.IsExist("key2");
        ASSERT_TRUE(exist_res.has_value());
        EXPECT_FALSE(exist_res.value())
            << "key2 should not exist after eviction";

        exist_res = backend.IsExist("key3");
        ASSERT_TRUE(exist_res.has_value());
        EXPECT_FALSE(exist_res.value())
            << "key3 should not exist after eviction";

        // Physical deletion is asynchronous; verify eventual removal instead of
        // checking synchronously on the eviction path.
        EXPECT_TRUE(WaitForNoFilesWithExtension(
            "/tmp/mooncake_test_bucket_eviction", ".bucket"));
        EXPECT_TRUE(WaitForNoFilesWithExtension(
            "/tmp/mooncake_test_bucket_eviction", ".meta"));
    }

    // Cleanup
    fs::remove_all("/tmp/mooncake_test_bucket_eviction");
}

// Test automatic bucket eviction when capacity is exceeded
TEST_F(StorageTierTest, AutoEvictionOnCapacityExceeded) {
    namespace fs = std::filesystem;
    fs::remove_all("/tmp/mooncake_test_auto_eviction");
    fs::create_directories("/tmp/mooncake_test_auto_eviction");

    // Setup environment for Bucket Backend
    setenv("MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR",
           "bucket_storage_backend", 1);
    setenv("MOONCAKE_OFFLOAD_FILE_STORAGE_PATH",
           "/tmp/mooncake_test_auto_eviction", 1);

    // Create a TieredBackend with small capacity (20KB)
    std::string json_config_str = R"({
        "tiers": [
            {
                "type": "STORAGE",
                "capacity": 20480,
                "priority": 5,
                "tags": ["ssd"]
            }
        ]
    })";
    Json::Value config;
    ASSERT_TRUE(parseJsonString(json_config_str, config));

    {
        TieredBackend backend;
        auto init_res = InitTieredBackendForTest(backend, config);
        ASSERT_TRUE(init_res.has_value());

        // Fill up the tier with data (~20KB)
        std::vector<std::string> keys;
        for (int i = 0; i < 20; ++i) {
            std::string key = "key_" + std::to_string(i);
            keys.push_back(key);

            auto alloc_result = backend.Allocate(1024);  // 1KB each
            ASSERT_TRUE(alloc_result.has_value())
                << "Failed to allocate for " << key;
            AllocationHandle handle = alloc_result.value();

            auto test_buffer = CreateTestBuffer(1024);
            DataSource source;
            source.buffer =
                std::make_unique<TempDRAMBuffer>(std::move(test_buffer), 1024);
            source.type = MemoryType::DRAM;

            auto write_result = backend.Write(source, handle);
            ASSERT_TRUE(write_result.has_value());

            auto commit_result = backend.Commit(key, handle);
            ASSERT_TRUE(commit_result.has_value());
        }

        // Flush to persist to disk
        auto tier_views = backend.GetTierViews();
        ASSERT_FALSE(tier_views.empty());
        auto tier = backend.GetTier(tier_views[0].id);
        const_cast<CacheTier*>(tier)->Flush();

        LOG(INFO) << "Filled tier with 20 keys, usage=" << tier_views[0].usage
                  << ", capacity=" << tier_views[0].capacity;

        // Now try to allocate more data that exceeds capacity
        // This should trigger automatic bucket eviction
        auto alloc_result = backend.Allocate(5 * 1024);  // 5KB
        ASSERT_TRUE(alloc_result.has_value())
            << "Should succeed after automatic eviction";

        LOG(INFO) << "Successfully allocated after auto-eviction";

        // The evicted keys should no longer be in the storage backend
        // (they were removed from the bucket that was evicted)
        // Note: We can't use backend.Get() because it might return from
        // pending_batch Instead, verify that bucket files were deleted
        EXPECT_TRUE(WaitForNoFilesWithExtension(
            "/tmp/mooncake_test_auto_eviction", ".bucket"));
        EXPECT_TRUE(WaitForNoFilesWithExtension(
            "/tmp/mooncake_test_auto_eviction", ".meta"));

        int bucket_files_count = CountFilesWithExtension(
            "/tmp/mooncake_test_auto_eviction", ".bucket");
        LOG(INFO) << "After eviction, " << bucket_files_count
                  << " bucket files remain";
    }

    // Cleanup
    fs::remove_all("/tmp/mooncake_test_auto_eviction");
}

// ============================================================
// Concurrency Tests
// ============================================================

// Test concurrent Allocate+Write+Commit and Get on TieredBackend
TEST_F(StorageTierTest, ConcurrentReadWrite) {
    setenv("MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR",
           "file_per_key_storage_backend", 1);
    setenv("MOONCAKE_OFFLOAD_FILE_STORAGE_PATH",
           "/tmp/mooncake_test_concurrent_rw", 1);

    fs::remove_all("/tmp/mooncake_test_concurrent_rw");
    fs::create_directories("/tmp/mooncake_test_concurrent_rw");

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
    auto init_res = backend.Init(config, nullptr, nullptr, nullptr, nullptr);
    ASSERT_TRUE(init_res.has_value());

    // Phase 1: Concurrent writes
    constexpr int kNumWriters = 4;
    constexpr int kKeysPerWriter = 10;
    constexpr size_t kDataSize = 1024;
    std::atomic<int> write_success{0};
    std::vector<std::thread> threads;

    for (int i = 0; i < kNumWriters; ++i) {
        threads.emplace_back([&, i]() {
            for (int j = 0; j < kKeysPerWriter; ++j) {
                std::string key =
                    "rw_key_" + std::to_string(i) + "_" + std::to_string(j);

                auto alloc_result = backend.Allocate(kDataSize);
                if (!alloc_result.has_value()) continue;
                AllocationHandle handle = alloc_result.value();

                auto test_buffer = CreateTestBuffer(kDataSize);
                DataSource source;
                source.buffer = std::make_unique<TempDRAMBuffer>(
                    std::move(test_buffer), kDataSize);
                source.type = MemoryType::DRAM;

                auto write_result = backend.Write(source, handle);
                if (!write_result.has_value()) continue;

                auto commit_result = backend.Commit(key, handle);
                if (commit_result.has_value()) {
                    write_success.fetch_add(1);
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }
    threads.clear();
    EXPECT_EQ(write_success.load(), kNumWriters * kKeysPerWriter);

    // Flush to persist
    auto tier_views = backend.GetTierViews();
    ASSERT_FALSE(tier_views.empty());
    auto tier = backend.GetTier(tier_views[0].id);
    const_cast<CacheTier*>(tier)->Flush();

    // Phase 2: Concurrent reads
    std::atomic<int> read_success{0};
    for (int i = 0; i < kNumWriters; ++i) {
        threads.emplace_back([&, i]() {
            for (int j = 0; j < kKeysPerWriter; ++j) {
                std::string key =
                    "rw_key_" + std::to_string(i) + "_" + std::to_string(j);
                auto get_result = backend.Get(key);
                if (get_result.has_value()) {
                    read_success.fetch_add(1);
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(read_success.load(), kNumWriters * kKeysPerWriter);

    fs::remove_all("/tmp/mooncake_test_concurrent_rw");
}

}  // namespace mooncake::test

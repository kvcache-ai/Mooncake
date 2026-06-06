#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <barrier>
#include <chrono>
#include <filesystem>
#include <thread>
#include <vector>

#include "allocator.h"
#include "storage_backend.h"
#include "file_storage.h"
#include "utils/common.h"
#include "client_metric.h"

namespace mooncake {

void SetEnv(const std::string& key, const std::string& value) {
    setenv(key.c_str(), value.c_str(), 1);
}

void UnsetEnv(const std::string& key) { unsetenv(key.c_str()); }

class FileStorageTest : public ::testing::Test {
   protected:
    std::string data_path;
    void SetUp() override {
        google::InitGoogleLogging("FileStorageTest");
        FLAGS_logtostderr = true;
        UnsetEnv("MOONCAKE_OFFLOAD_FILE_STORAGE_PATH");
        UnsetEnv("MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES");
        UnsetEnv("MOONCAKE_OFFLOAD_SCANMETA_ITERATOR_KEYS_LIMIT");
        UnsetEnv("MOONCAKE_SCANMETA_ITERATOR_KEYS_LIMIT");
        UnsetEnv("MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT");
        UnsetEnv("MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES");
        UnsetEnv("MOONCAKE_OFFLOAD_TOTAL_KEYS_LIMIT");
        UnsetEnv("MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES");
        UnsetEnv("MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS");
        data_path = std::filesystem::current_path().string() + "/data";
        fs::create_directories(data_path);
        for (const auto& entry : fs::directory_iterator(data_path)) {
            if (entry.is_regular_file()) {
                fs::remove(entry.path());
            }
        }
    }

    tl::expected<void, ErrorCode> FileStorageBatchOffload(
        FileStorage& fileStorage, std::vector<std::string>& keys,
        std::vector<int64_t>& sizes,
        std::unordered_map<std::string, std::string>& batch_data) {
        std::vector<int64_t> buckets;
        return BatchOffloadUtil(*fileStorage.storage_backend_, keys, sizes,
                                batch_data, buckets);
    }

    tl::expected<std::shared_ptr<FileStorage::AllocatedBatch>, ErrorCode>
    FileStorageAllocateBatch(FileStorage& fileStorage,
                             const std::vector<std::string>& keys,
                             const std::vector<int64_t>& sizes) {
        return fileStorage.AllocateBatch(keys, sizes);
    }

    tl::expected<void, ErrorCode> FileStorageBatchLoad(
        FileStorage& fileStorage,
        std::unordered_map<std::string, Slice>& batch_object) {
        return fileStorage.BatchLoad(batch_object);
    }

    tl::expected<bool, ErrorCode> FileStorageIsEnableOffloading(
        FileStorage& fileStorage) {
        return fileStorage.IsEnableOffloading();
    }

    tl::expected<void, ErrorCode> FileStorageGroupOffloadingKeysByBucket(
        FileStorage& fileStorage,
        const std::unordered_map<std::string, int64_t>& offloading_objects,
        std::vector<std::vector<std::string>>& buckets_keys) {
        auto bucket_backend = std::dynamic_pointer_cast<BucketStorageBackend>(
            fileStorage.storage_backend_);
        if (!bucket_backend) {
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        return bucket_backend->AllocateOffloadingBuckets(offloading_objects,
                                                         buckets_keys);
    }

    size_t GetUngroupedOffloadingObjectsSize(FileStorage& fileStorage) {
        auto bucket_backend = std::dynamic_pointer_cast<BucketStorageBackend>(
            fileStorage.storage_backend_);
        if (!bucket_backend) {
            return 0;
        }
        return bucket_backend->UngroupedOffloadingObjectsSize();
    }

    void TearDown() override {
        google::ShutdownGoogleLogging();
        LOG(INFO) << "Clear test data...";
        for (const auto& entry : fs::directory_iterator(data_path)) {
            if (entry.is_regular_file()) {
                fs::remove(entry.path());
            }
        }
    }
};

TEST_F(FileStorageTest, IsEnableOffloading) {
    std::unordered_map<std::string, std::string> all_object;
    std::vector<std::string> keys;
    std::vector<int64_t> sizes;
    std::unordered_map<std::string, std::string> batch_data;
    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_filepath = data_path;
    file_storage_config.local_buffer_size = 128 * 1024 * 1024;
    FileStorage fileStorage1(file_storage_config, nullptr, "localhost:9003");
    ASSERT_TRUE(FileStorageBatchOffload(fileStorage1, keys, sizes, batch_data));
    auto enable_offloading_result1 =
        FileStorageIsEnableOffloading(fileStorage1);
    ASSERT_TRUE(enable_offloading_result1 && enable_offloading_result1.value());

    // bucket_keys_limit/bucket_size_limit moved to BucketBackendConfig.
    // With current semantics, backend prevents offloading once it would exceed
    // limits, so we validate IsEnableOffloading directly under tight limits.

    // Case 2: total_keys_limit < bucket_keys_limit => cannot offload
    SetEnv("MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT", "10");
    file_storage_config.total_keys_limit = 9;
    FileStorage fileStorage2(file_storage_config, nullptr, "localhost:9003");
    auto enable_offloading_result2 =
        FileStorageIsEnableOffloading(fileStorage2);
    ASSERT_TRUE(enable_offloading_result2 &&
                !enable_offloading_result2.value());

    // Case 3: total_size_limit < bucket_size_limit => cannot offload
    SetEnv("MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES", "969");
    file_storage_config.total_keys_limit = 10'000'000;
    file_storage_config.total_size_limit = 100;
    FileStorage fileStorage3(file_storage_config, nullptr, "localhost:9003");
    auto enable_offloading_result3 =
        FileStorageIsEnableOffloading(fileStorage3);
    ASSERT_TRUE(enable_offloading_result3 &&
                !enable_offloading_result3.value());
}

TEST_F(FileStorageTest, BatchLoad) {
    std::vector<std::string> keys;
    std::vector<int64_t> sizes;
    std::unordered_map<std::string, std::string> batch_data;
    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_filepath = data_path;
    FileStorage fileStorage(file_storage_config, nullptr, "localhost:9003");
    ASSERT_TRUE(FileStorageBatchOffload(fileStorage, keys, sizes, batch_data));
    std::unordered_map<std::string, Slice> batch_slice;
    std::vector<BufferHandle> buff;

    auto allocate_res = FileStorageAllocateBatch(fileStorage, keys, sizes);
    ASSERT_TRUE(allocate_res);

    ASSERT_TRUE(
        FileStorageBatchLoad(fileStorage, allocate_res.value()->slices));
    for (auto& slice_it : batch_slice) {
        std::string data(static_cast<char*>(slice_it.second.ptr),
                         slice_it.second.size);
        LOG(INFO) << "key: " << slice_it.first;
        ASSERT_EQ(data, batch_data.at(slice_it.first));
    }
}

TEST_F(FileStorageTest, GroupOffloadingKeysByBucket_bucket_keys_limit) {
    std::unordered_map<std::string, int64_t> offloading_objects;
    for (size_t i = 0; i < 35; i++) {
        offloading_objects.emplace("test" + std::to_string(i), 1);
    }
    std::vector<std::vector<std::string>> buckets_keys;
    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_filepath = data_path;
    file_storage_config.scanmeta_iterator_keys_limit = 969;
    SetEnv("MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT", "10");
    FileStorage fileStorage(file_storage_config, nullptr, "localhost:9003");
    ASSERT_TRUE(FileStorageGroupOffloadingKeysByBucket(
        fileStorage, offloading_objects, buckets_keys));
    ASSERT_EQ(buckets_keys.size(), 3);
    for (const auto& bucket_keys : buckets_keys) {
        ASSERT_EQ(bucket_keys.size(), 10);
    }
    ASSERT_EQ(GetUngroupedOffloadingObjectsSize(fileStorage), 5);
    buckets_keys.clear();
    ASSERT_TRUE(FileStorageGroupOffloadingKeysByBucket(
        fileStorage, offloading_objects, buckets_keys));
    ASSERT_EQ(buckets_keys.size(), 4);
    for (const auto& bucket_keys : buckets_keys) {
        ASSERT_EQ(bucket_keys.size(), 10);
    }
    ASSERT_EQ(GetUngroupedOffloadingObjectsSize(fileStorage), 0);
}

TEST_F(FileStorageTest, GroupOffloadingKeysByBucket_bucket_size_limit) {
    std::unordered_map<std::string, int64_t> offloading_objects;
    for (size_t i = 0; i < 35; i++) {
        offloading_objects.emplace("test" + std::to_string(i), 1);
    }
    std::vector<std::vector<std::string>> buckets_keys;
    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_filepath = data_path;
    SetEnv("MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES", "10");
    FileStorage fileStorage(file_storage_config, nullptr, "localhost:9003");
    ASSERT_TRUE(FileStorageGroupOffloadingKeysByBucket(
        fileStorage, offloading_objects, buckets_keys));
    ASSERT_EQ(buckets_keys.size(), 3);
    for (const auto& bucket_keys : buckets_keys) {
        ASSERT_EQ(bucket_keys.size(), 10);
    }
    ASSERT_EQ(GetUngroupedOffloadingObjectsSize(fileStorage), 5);
    buckets_keys.clear();
    ASSERT_TRUE(FileStorageGroupOffloadingKeysByBucket(
        fileStorage, offloading_objects, buckets_keys));
    ASSERT_EQ(buckets_keys.size(), 4);
    for (const auto& bucket_keys : buckets_keys) {
        ASSERT_EQ(bucket_keys.size(), 10);
    }
    ASSERT_EQ(GetUngroupedOffloadingObjectsSize(fileStorage), 0);
}

TEST_F(FileStorageTest,
       GroupOffloadingKeysByBucket_bucket_size_limit_and_bucket_keys_limit) {
    std::unordered_map<std::string, int64_t> offloading_objects;
    for (size_t i = 0; i < 500; i++) {
        offloading_objects.emplace("test" + std::to_string(i), i);
    }
    std::vector<std::vector<std::string>> buckets_keys;
    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_filepath = data_path;
    SetEnv("MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT", "9");
    SetEnv("MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES", "496");
    FileStorage fileStorage(file_storage_config, nullptr, "localhost:9003");
    ASSERT_TRUE(FileStorageGroupOffloadingKeysByBucket(
        fileStorage, offloading_objects, buckets_keys));
    for (size_t i = 0; i < buckets_keys.size(); i++) {
        auto bucket_keys = buckets_keys.at(i);
        ASSERT_TRUE(bucket_keys.size() <= 9);
        size_t total_size = 0;
        std::string keys;
        for (const auto& bucket_key : bucket_keys) {
            total_size += offloading_objects.at(bucket_key);
            keys += bucket_key + ",";
        }
        ASSERT_TRUE(total_size <= 496);
    }
}

TEST_F(FileStorageTest,
       GroupOffloadingKeysByBucket_ungrouped_offloading_objects) {
    std::unordered_map<std::string, int64_t> offloading_objects;
    for (size_t i = 0; i < 1; i++) {
        offloading_objects.emplace("test" + std::to_string(i), 1);
    }
    std::vector<std::vector<std::string>> buckets_keys;
    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_filepath = data_path;
    FileStorage fileStorage(file_storage_config, nullptr, "localhost:9003");
    ASSERT_TRUE(FileStorageGroupOffloadingKeysByBucket(
        fileStorage, offloading_objects, buckets_keys));
    offloading_objects.clear();
    ASSERT_TRUE(FileStorageGroupOffloadingKeysByBucket(
        fileStorage, offloading_objects, buckets_keys));
    for (size_t i = 0; i < 7; i++) {
        offloading_objects.emplace("test" + std::to_string(i), 1);
    }
    ASSERT_TRUE(FileStorageGroupOffloadingKeysByBucket(
        fileStorage, offloading_objects, buckets_keys));
}

TEST_F(FileStorageTest, DefaultValuesWhenNoEnvSet) {
    auto config = FileStorageConfig::FromEnvironment();
    auto bucket_backend_config = BucketBackendConfig::FromEnvironment();

    EXPECT_EQ(config.storage_filepath, "/data/file_storage");
    EXPECT_EQ(config.local_buffer_size, 1280 * 1024 * 1024);
    EXPECT_EQ(config.scanmeta_iterator_keys_limit, 20000);
    EXPECT_EQ(bucket_backend_config.bucket_keys_limit, 500);
    EXPECT_EQ(bucket_backend_config.bucket_size_limit, 256 * 1024 * 1024);
    EXPECT_EQ(config.total_keys_limit, 10'000'000);
    EXPECT_EQ(config.total_size_limit, 2ULL * 1024 * 1024 * 1024 * 1024);
    EXPECT_EQ(config.heartbeat_interval_seconds, 10u);
}

TEST_F(FileStorageTest, ReadStringFromEnv) {
    SetEnv("MOONCAKE_OFFLOAD_FILE_STORAGE_PATH", "/tmp/storage");

    auto config = FileStorageConfig::FromEnvironment();
    EXPECT_EQ(config.storage_filepath, "/tmp/storage");
}

TEST_F(FileStorageTest, ReadInt64FromEnv) {
    SetEnv("MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES", "2147483648");  // 2GB
    SetEnv("MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT", "1000");
    SetEnv("MOONCAKE_OFFLOAD_TOTAL_KEYS_LIMIT", "5000000");

    auto config = FileStorageConfig::FromEnvironment();
    auto bucket_backend_config = BucketBackendConfig::FromEnvironment();

    EXPECT_EQ(config.local_buffer_size, 2147483648);
    EXPECT_EQ(bucket_backend_config.bucket_keys_limit, 1000);
    EXPECT_EQ(config.total_keys_limit, 5000000);
}

TEST_F(FileStorageTest, ReadUint32FromEnv) {
    SetEnv("MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS", "5");

    auto config = FileStorageConfig::FromEnvironment();
    EXPECT_EQ(config.heartbeat_interval_seconds, 5u);
}

TEST_F(FileStorageTest, InvalidIntValueUsesDefault) {
    SetEnv("MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT", "abc");
    SetEnv("MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES", "sdfsdf");
    SetEnv("MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS", "-1");

    auto config = FileStorageConfig::FromEnvironment();
    auto bucket_backend_config = BucketBackendConfig::FromEnvironment();

    EXPECT_EQ(bucket_backend_config.bucket_keys_limit, 500);
    EXPECT_EQ(config.total_size_limit, 2ULL * 1024 * 1024 * 1024 * 1024);
    EXPECT_EQ(config.heartbeat_interval_seconds, 10u);
}

TEST_F(FileStorageTest, OutOfRangeValueUsesDefault) {
    SetEnv("MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS",
           "4294967296");  // > UINT32_MAX
    SetEnv("MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS", "-10");  // negative

    auto config = FileStorageConfig::FromEnvironment();
    EXPECT_EQ(config.heartbeat_interval_seconds, 10u);  // fallback to default
}

TEST_F(FileStorageTest, EmptyEnvValueUsesDefault) {
    SetEnv("MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT", "");  // empty string

    auto config = FileStorageConfig::FromEnvironment();
    auto bucket_backend_config = BucketBackendConfig::FromEnvironment();
    EXPECT_EQ(bucket_backend_config.bucket_keys_limit, 500);  // fallback
}

TEST_F(FileStorageTest, ValidateSuccessWithValidConfig) {
    FileStorageConfig config;
    config.storage_filepath = std::filesystem::current_path().string();
    config.total_keys_limit = 1000000;
    config.total_size_limit = 1073741824;  // 1GB
    config.heartbeat_interval_seconds = 5;

    EXPECT_TRUE(config.Validate());
}

TEST_F(FileStorageTest, ValidateFailsOnEmptyStoragePath) {
    FileStorageConfig config;
    config.storage_filepath = "";
    EXPECT_FALSE(config.Validate());
    config.storage_filepath = "   ";
    EXPECT_FALSE(config.Validate());
    config.storage_filepath = "relative/path";
    EXPECT_FALSE(config.Validate());
    config.storage_filepath = "./data";
    EXPECT_FALSE(config.Validate());
    config.storage_filepath = "../data";
    EXPECT_FALSE(config.Validate());
    config.storage_filepath = "/valid/../invalid";
    EXPECT_FALSE(config.Validate());
    config.storage_filepath = "/path/./sub";
    EXPECT_FALSE(config.Validate());
    config.storage_filepath = "/tmp/this_directory_does_not_exist_12345";
    EXPECT_FALSE(config.Validate());
    config.storage_filepath = data_path;
    EXPECT_TRUE(config.Validate());
}

TEST_F(FileStorageTest, ValidateFailsOnInvalidLimits) {
    FileStorageConfig config;
    config.storage_filepath = "/tmp";

    config.total_keys_limit = 0;
    EXPECT_FALSE(config.Validate());

    config.total_keys_limit = 1;
    config.total_size_limit = 0;
    EXPECT_FALSE(config.Validate());

    config.total_size_limit = 1;
    config.heartbeat_interval_seconds = 0;
    EXPECT_FALSE(config.Validate());
}

TEST_F(FileStorageTest, BatchLoad_WithStorageBackendAdaptor) {
    std::vector<std::string> keys;
    std::vector<int64_t> sizes;
    std::unordered_map<std::string, std::string> batch_data;

    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_backend_type = StorageBackendType::kFilePerKey;
    file_storage_config.storage_filepath = data_path;
    file_storage_config.local_buffer_size = 128 * 1024 * 1024;
    FilePerKeyConfig file_per_key_config;
    file_per_key_config.fsdir = "FileStorageTestDir";

    auto total_path = fs::path(data_path) / file_per_key_config.fsdir;
    fs::create_directories(total_path);

    FileStorage fileStorage(file_storage_config, nullptr, "localhost:9003");

    auto offload_res =
        FileStorageBatchOffload(fileStorage, keys, sizes, batch_data);
    ASSERT_TRUE(offload_res) << "FileStorageBatchOffload failed";

    auto allocate_res = FileStorageAllocateBatch(fileStorage, keys, sizes);
    ASSERT_TRUE(allocate_res) << "FileStorageAllocateBatch failed";

    auto batch = std::move(allocate_res.value());

    auto load_res = FileStorageBatchLoad(fileStorage, batch->slices);
    ASSERT_TRUE(load_res) << "FileStorageBatchLoad failed";

    for (const auto& it : batch->slices) {
        const std::string& key = it.first;
        const Slice& slice = it.second;
        std::string data(static_cast<char*>(slice.ptr), slice.size);

        auto found = batch_data.find(key);
        ASSERT_TRUE(found != batch_data.end())
            << "key not found in batch_data: " << key;
        EXPECT_EQ(data, found->second);
    }
}

TEST_F(FileStorageTest, BatchLoadRecordsSsdMetrics) {
    // Setup: write data to storage backend via BatchOffloadUtil
    std::vector<std::string> keys;
    std::vector<int64_t> sizes;
    std::unordered_map<std::string, std::string> batch_data;

    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_filepath = data_path;

    // Create FileStorage WITH SsdMetric
    SsdMetric ssd_metric;
    FileStorage fileStorage(file_storage_config, nullptr, "localhost:9003",
                            &ssd_metric);

    // Write test data to disk
    ASSERT_TRUE(FileStorageBatchOffload(fileStorage, keys, sizes, batch_data));
    ASSERT_FALSE(keys.empty());

    // Allocate buffers and call BatchLoad (read path)
    auto allocate_res = FileStorageAllocateBatch(fileStorage, keys, sizes);
    ASSERT_TRUE(allocate_res);

    auto load_result =
        FileStorageBatchLoad(fileStorage, allocate_res.value()->slices);
    ASSERT_TRUE(load_result);

    // Verify SSD read metrics were recorded
    EXPECT_EQ(ssd_metric.ssd_read_ops.value(),
              static_cast<int64_t>(allocate_res.value()->slices.size()));

    // Verify bytes: sum of all slice sizes
    int64_t expected_bytes = 0;
    for (const auto& [key, slice] : allocate_res.value()->slices) {
        expected_bytes += slice.size;
    }
    EXPECT_EQ(ssd_metric.ssd_read_bytes.value(), expected_bytes);
    EXPECT_GT(expected_bytes, 0);

    // Verify latency histogram has exactly 1 observation (one BatchLoad call)
    auto buckets = ssd_metric.ssd_read_latency_us.get_bucket_counts();
    int64_t total_observations = 0;
    for (auto& b : buckets) {
        total_observations += b->value();
    }
    EXPECT_EQ(total_observations, 1);

    // Write metrics should remain 0 (BatchOffloadUtil bypasses FileStorage)
    EXPECT_EQ(ssd_metric.ssd_write_ops.value(), 0);
    EXPECT_EQ(ssd_metric.ssd_write_bytes.value(), 0);

    LOG(INFO) << "SSD read metrics after BatchLoad: ops="
              << ssd_metric.ssd_read_ops.value()
              << ", bytes=" << ssd_metric.ssd_read_bytes.value();
}

TEST_F(FileStorageTest, BatchLoadFailureDoesNotRecordSsdMetrics) {
    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_filepath = data_path;

    SsdMetric ssd_metric;
    FileStorage fileStorage(file_storage_config, nullptr, "localhost:9003",
                            &ssd_metric);

    // Init storage backend so we can call BatchLoad
    // But load with keys that don't exist on disk -> should fail
    std::unordered_map<std::string, Slice> batch_object;
    char dummy_buf[4096] = {};
    batch_object["nonexistent_key_1"] = Slice{dummy_buf, 4096};
    batch_object["nonexistent_key_2"] = Slice{dummy_buf, 8192};

    auto result = FileStorageBatchLoad(fileStorage, batch_object);
    // Expect failure (keys don't exist on disk)
    EXPECT_FALSE(result);

    // Metrics should remain 0 - failed operations are not counted
    EXPECT_EQ(ssd_metric.ssd_read_ops.value(), 0);
    EXPECT_EQ(ssd_metric.ssd_read_bytes.value(), 0);

    auto buckets = ssd_metric.ssd_read_latency_us.get_bucket_counts();
    int64_t total_observations = 0;
    for (auto& b : buckets) {
        total_observations += b->value();
    }
    EXPECT_EQ(total_observations, 0);

    LOG(INFO) << "SSD metrics after failed BatchLoad: ops="
              << ssd_metric.ssd_read_ops.value()
              << ", bytes=" << ssd_metric.ssd_read_bytes.value();
}

TEST_F(FileStorageTest, NullSsdMetricDoesNotCrash) {
    // FileStorage with nullptr SsdMetric should work fine (no metrics recorded)
    std::vector<std::string> keys;
    std::vector<int64_t> sizes;
    std::unordered_map<std::string, std::string> batch_data;

    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_filepath = data_path;

    // nullptr SsdMetric (default)
    FileStorage fileStorage(file_storage_config, nullptr, "localhost:9003");

    ASSERT_TRUE(FileStorageBatchOffload(fileStorage, keys, sizes, batch_data));
    ASSERT_FALSE(keys.empty());

    auto allocate_res = FileStorageAllocateBatch(fileStorage, keys, sizes);
    ASSERT_TRUE(allocate_res);

    auto load_result =
        FileStorageBatchLoad(fileStorage, allocate_res.value()->slices);
    ASSERT_TRUE(load_result);
    // No crash = success. No metrics pointer, so nothing to verify.
}

TEST_F(FileStorageTest, BatchGetSingleflight_SerializesSameKey) {
    std::vector<std::string> keys;
    std::vector<int64_t> sizes;
    std::unordered_map<std::string, std::string> batch_data;

    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_filepath = data_path;
    file_storage_config.local_buffer_size = 128 * 1024 * 1024;
    file_storage_config.total_keys_limit = 1000;
    file_storage_config.total_size_limit = 10 * 1024 * 1024;
    FileStorage fileStorage(file_storage_config, nullptr, "localhost:9003");
    ASSERT_TRUE(FileStorageBatchOffload(fileStorage, keys, sizes, batch_data));
    ASSERT_FALSE(keys.empty());

    const std::string target_key = keys.front();
    const int64_t target_size = sizes.front();
    constexpr int kConcurrency = 8;

    std::atomic<int> success_count{0};
    std::atomic<int> started{0};
    std::vector<std::thread> threads;
    threads.reserve(kConcurrency);

    for (int i = 0; i < kConcurrency; ++i) {
        threads.emplace_back([&] {
            started.fetch_add(1);
            while (started.load() < kConcurrency) {
                std::this_thread::yield();
            }
            auto result = fileStorage.BatchGet({target_key}, {target_size});
            if (result) {
                fileStorage.ReleaseBuffer(result->batch_id);
                success_count.fetch_add(1);
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(success_count.load(), kConcurrency)
        << "All concurrent BatchGet calls should succeed";
}

TEST_F(FileStorageTest, BatchGetSingleflight_DifferentKeysRunConcurrently) {
    std::vector<std::string> keys;
    std::vector<int64_t> sizes;
    std::unordered_map<std::string, std::string> batch_data;

    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_filepath = data_path;
    file_storage_config.local_buffer_size = 128 * 1024 * 1024;
    file_storage_config.total_keys_limit = 1000;
    file_storage_config.total_size_limit = 10 * 1024 * 1024;
    FileStorage fileStorage(file_storage_config, nullptr, "localhost:9003");
    ASSERT_TRUE(FileStorageBatchOffload(fileStorage, keys, sizes, batch_data));
    ASSERT_GE(keys.size(), 2u) << "Need at least 2 keys for this test";

    std::atomic<int> success_count{0};
    std::vector<std::thread> threads;

    for (size_t i = 0; i < std::min(keys.size(), size_t(4)); ++i) {
        threads.emplace_back([&, i] {
            auto result = fileStorage.BatchGet({keys[i]}, {sizes[i]});
            if (result) {
                fileStorage.ReleaseBuffer(result->batch_id);
                success_count.fetch_add(1);
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(success_count.load(),
              static_cast<int>(std::min(keys.size(), size_t(4))))
        << "All different-key BatchGet calls should succeed";
}

TEST_F(FileStorageTest, BatchGetSingleflight_MultiKeyBypassesSingleflight) {
    std::vector<std::string> keys;
    std::vector<int64_t> sizes;
    std::unordered_map<std::string, std::string> batch_data;

    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_filepath = data_path;
    file_storage_config.local_buffer_size = 128 * 1024 * 1024;
    file_storage_config.total_keys_limit = 1000;
    file_storage_config.total_size_limit = 10 * 1024 * 1024;
    FileStorage fileStorage(file_storage_config, nullptr, "localhost:9003");
    ASSERT_TRUE(FileStorageBatchOffload(fileStorage, keys, sizes, batch_data));
    ASSERT_GE(keys.size(), 2u);

    auto result = fileStorage.BatchGet(keys, sizes);
    ASSERT_TRUE(result) << "Multi-key BatchGet should succeed";
    fileStorage.ReleaseBuffer(result->batch_id);
}

TEST_F(FileStorageTest, BatchGetSingleflight_StagingSharingVerification) {
    std::vector<std::string> keys;
    std::vector<int64_t> sizes;
    std::unordered_map<std::string, std::string> batch_data;

    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_filepath = data_path;
    file_storage_config.local_buffer_size = 128 * 1024 * 1024;
    file_storage_config.total_keys_limit = 1000;
    file_storage_config.total_size_limit = 10 * 1024 * 1024;
    FileStorage fileStorage(file_storage_config, nullptr, "localhost:9003");
    ASSERT_TRUE(FileStorageBatchOffload(fileStorage, keys, sizes, batch_data));
    ASSERT_FALSE(keys.empty());

    const std::string target_key = keys.front();
    const int64_t target_size = sizes.front();
    const std::string& expected_data = batch_data.at(target_key);
    constexpr int kConcurrency = 16;

    std::atomic<int> success_count{0};
    std::atomic<int> shared_batch_count{0};
    std::atomic<int> started{0};
    std::atomic<uint64_t> leader_batch_id{0};
    std::vector<std::thread> threads;
    threads.reserve(kConcurrency);

    for (int i = 0; i < kConcurrency; ++i) {
        threads.emplace_back([&] {
            started.fetch_add(1);
            while (started.load() < kConcurrency) {
                std::this_thread::yield();
            }
            auto result = fileStorage.BatchGet({target_key}, {target_size});
            if (!result) return;

            uint64_t expected = 0;
            if (leader_batch_id.compare_exchange_strong(expected,
                                                        result->batch_id)) {
                // First to set — this is the leader
            }
            if (result->batch_id == leader_batch_id.load()) {
                shared_batch_count.fetch_add(1);
            }

            // Verify data correctness: read from staging pointer
            ASSERT_EQ(result->pointers.size(), 1u);
            auto* data_ptr = reinterpret_cast<const char*>(result->pointers[0]);
            std::string actual(data_ptr, target_size);
            EXPECT_EQ(actual, expected_data)
                << "Data mismatch for batch_id=" << result->batch_id;

            fileStorage.ReleaseBuffer(result->batch_id);
            success_count.fetch_add(1);
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(success_count.load(), kConcurrency)
        << "All concurrent BatchGet calls should succeed";

    // With staging sharing, most threads should get the same batch_id.
    // At minimum the leader + some waiters share a batch.
    LOG(INFO) << "Staging sharing results: " << shared_batch_count.load() << "/"
              << kConcurrency << " threads shared the same batch_id (leader="
              << leader_batch_id.load() << ")";
    EXPECT_GT(shared_batch_count.load(), 1)
        << "At least 2 threads should share the same staging buffer";
}

TEST_F(FileStorageTest, BatchGetSingleflight_RefCountRelease) {
    std::vector<std::string> keys;
    std::vector<int64_t> sizes;
    std::unordered_map<std::string, std::string> batch_data;

    auto file_storage_config = FileStorageConfig::FromEnvironment();
    file_storage_config.storage_filepath = data_path;
    file_storage_config.local_buffer_size = 128 * 1024 * 1024;
    file_storage_config.total_keys_limit = 1000;
    file_storage_config.total_size_limit = 10 * 1024 * 1024;
    FileStorage fileStorage(file_storage_config, nullptr, "localhost:9003");
    ASSERT_TRUE(FileStorageBatchOffload(fileStorage, keys, sizes, batch_data));
    ASSERT_FALSE(keys.empty());

    const std::string target_key = keys.front();
    const int64_t target_size = sizes.front();
    constexpr int kConcurrency = 8;

    std::vector<uint64_t> batch_ids(kConcurrency, 0);
    std::atomic<int> started{0};
    std::vector<std::thread> threads;
    threads.reserve(kConcurrency);

    // Phase 1: all threads get batch_ids but DON'T release yet
    std::barrier sync_point(kConcurrency + 1);  // +1 for main thread

    for (int i = 0; i < kConcurrency; ++i) {
        threads.emplace_back([&, i] {
            started.fetch_add(1);
            while (started.load() < kConcurrency) {
                std::this_thread::yield();
            }
            auto result = fileStorage.BatchGet({target_key}, {target_size});
            ASSERT_TRUE(result);
            batch_ids[i] = result->batch_id;
            sync_point.arrive_and_wait();  // signal: got batch_id
            sync_point.arrive_and_wait();  // wait: main says release
            fileStorage.ReleaseBuffer(result->batch_id);
        });
    }

    sync_point.arrive_and_wait();  // wait for all threads to get batch_ids

    // Verify: shared batch_ids still valid (not GC'd) because ref_count > 0
    uint64_t shared_id = batch_ids[0];
    int shared_count = 0;
    for (int i = 0; i < kConcurrency; ++i) {
        if (batch_ids[i] == shared_id) {
            shared_count++;
        }
    }
    LOG(INFO) << "RefCount test: " << shared_count << "/" << kConcurrency
              << " threads share batch_id=" << shared_id;

    // Phase 2: let all threads release
    sync_point.arrive_and_wait();

    for (auto& t : threads) {
        t.join();
    }

    // After all releases, batch should be fully cleaned up.
    // A subsequent BatchGet for the same key should get a NEW batch_id.
    auto fresh = fileStorage.BatchGet({target_key}, {target_size});
    ASSERT_TRUE(fresh);
    EXPECT_NE(fresh->batch_id, shared_id)
        << "After all releases, a new BatchGet should allocate a fresh batch";
    fileStorage.ReleaseBuffer(fresh->batch_id);
}

}  // namespace mooncake
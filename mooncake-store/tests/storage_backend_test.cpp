#include "storage_backend.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstddef>
#include <filesystem>
#include <iostream>
#include <ranges>

#include "allocator.h"
#include "utils.h"
#include "utils/common.h"

namespace fs = std::filesystem;
namespace mooncake::test {
class StorageBackendTest : public ::testing::Test {
   protected:
    std::string data_path;
    void SetUp() override {
        google::InitGoogleLogging("StorageBackendTest");
        FLAGS_logtostderr = true;
        data_path = std::filesystem::current_path().string() + "/data";
        fs::create_directories(data_path);
        for (const auto& entry : fs::directory_iterator(data_path)) {
            if (entry.is_regular_file()) {
                fs::remove(entry.path());
            }
        }
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

TEST_F(StorageBackendTest, StorageBackendAll) {
    std::shared_ptr<SimpleAllocator> client_buffer_allocator =
        std::make_shared<SimpleAllocator>(128 * 1024 * 1024);
    FileStorageConfig config;
    config.storage_filepath = data_path;
    BucketStorageBackend storage_backend(config);

    ASSERT_TRUE(storage_backend.Init());
    ASSERT_TRUE(fs::directory_iterator(data_path) == fs::directory_iterator{});
    ASSERT_TRUE(!storage_backend.Init());
    std::unordered_map<std::string, std::string> test_data;
    std::vector<std::string> keys;
    std::vector<int64_t> sizes;
    std::vector<int64_t> buckets;
    ASSERT_TRUE(
        BatchOffloadUtil(storage_backend, keys, sizes, test_data, buckets));

    std::unordered_map<std::string, StorageObjectMetadata>
        batche_object_metadata;
    auto batch_query_object_result_two =
        storage_backend.BatchQuery(keys, batche_object_metadata);
    ASSERT_TRUE(batch_query_object_result_two);
    ASSERT_EQ(batche_object_metadata.size(), test_data.size());
    for (const auto& keys_it : test_data) {
        auto metadata = batche_object_metadata[keys_it.first];
        ASSERT_EQ(keys_it.second.size(), metadata.data_size);
    }

    std::unordered_map<std::string, Slice> batche_object;

    for (auto test_data_it : test_data) {
        void* buffer =
            client_buffer_allocator->allocate(test_data_it.second.size());
        batche_object.emplace(test_data_it.first,
                              Slice{buffer, test_data_it.second.size()});
    }

    auto batch_load_object_result = storage_backend.BatchLoad(batche_object);
    ASSERT_TRUE(batch_load_object_result);
    ASSERT_EQ(batche_object.size(), test_data.size());
    for (const auto& test_data_it : test_data) {
        auto is_exist_object_result =
            storage_backend.IsExist(test_data_it.first);
        ASSERT_TRUE(is_exist_object_result);
        ASSERT_TRUE(is_exist_object_result.value());
        auto object_it = batche_object.find(test_data_it.first);
        ASSERT_TRUE(object_it != batche_object.end());
        char* buf = new char[object_it->second.size + 1];
        buf[object_it->second.size] = '\0';
        memcpy(buf, object_it->second.ptr, object_it->second.size);
        auto data = std::string(buf);
        ASSERT_EQ(data, test_data_it.second);
        delete[] buf;
    }
}
TEST_F(StorageBackendTest, BucketScan) {
    std::shared_ptr<SimpleAllocator> client_buffer_allocator =
        std::make_shared<SimpleAllocator>(128 * 1024 * 1024);
    FileStorageConfig config;
    config.storage_filepath = data_path;
    BucketStorageBackend storage_backend(config);
    ASSERT_TRUE(storage_backend.Init());
    ASSERT_TRUE(!storage_backend.Init());
    std::vector<std::string> keys;
    std::vector<int64_t> sizes;
    std::vector<int64_t> buckets;
    std::unordered_map<std::string, std::string> batch_data;
    ASSERT_TRUE(
        BatchOffloadUtil(storage_backend, keys, sizes, batch_data, buckets));
    std::vector<std::string> scan_keys;
    std::vector<StorageObjectMetadata> scan_metadatas;
    std::vector<int64_t> scan_buckets;
    auto res = storage_backend.BucketScan(0, scan_keys, scan_metadatas,
                                          scan_buckets, 10);
    ASSERT_TRUE(res);
    ASSERT_EQ(res.value(), buckets.at(1));
    for (size_t i = 0; i < scan_keys.size(); i++) {
        ASSERT_EQ(scan_metadatas[i].data_size,
                  batch_data.at(scan_keys[i]).size());
        ASSERT_EQ(scan_metadatas[i].key_size, scan_keys[i].size());
    }
    ASSERT_EQ(scan_buckets.size(), 1);

    ASSERT_EQ(scan_buckets.at(0), buckets.at(0));
    scan_keys.clear();
    scan_metadatas.clear();
    scan_buckets.clear();
    res = storage_backend.BucketScan(0, scan_keys, scan_metadatas, scan_buckets,
                                     45);
    ASSERT_TRUE(res);

    ASSERT_EQ(res.value(), buckets.at(4));
    ASSERT_EQ(scan_buckets.size(), 4);
    for (int i = 0; i < 4; i++) {
        ASSERT_EQ(scan_buckets.at(i), buckets.at(i));
    }

    scan_keys.clear();
    scan_metadatas.clear();
    scan_buckets.clear();
    res = storage_backend.BucketScan(buckets.at(4), scan_keys, scan_metadatas,
                                     scan_buckets, 45);
    ASSERT_TRUE(res);

    ASSERT_EQ(res.value(), buckets.at(8));
    ASSERT_EQ(scan_buckets.size(), 4);
    for (int i = 0; i < 4; i++) {
        ASSERT_EQ(scan_buckets.at(i), buckets.at(i + 4));
    }

    scan_keys.clear();
    scan_metadatas.clear();
    scan_buckets.clear();
    res = storage_backend.BucketScan(buckets.at(9), scan_keys, scan_metadatas,
                                     scan_buckets, 45);

    ASSERT_TRUE(res);
    ASSERT_EQ(res.value(), 0);
    ASSERT_EQ(scan_buckets.size(), 1);
    ASSERT_EQ(scan_buckets.at(0), buckets.at(9));

    scan_keys.clear();
    scan_metadatas.clear();
    scan_buckets.clear();
    res = storage_backend.BucketScan(buckets.at(9) + 10, scan_keys,
                                     scan_metadatas, scan_buckets, 45);
    ASSERT_TRUE(res);
    ASSERT_EQ(res.value(), 0);
    ASSERT_EQ(scan_buckets.size(), 0);

    scan_keys.clear();
    scan_metadatas.clear();
    scan_buckets.clear();
    res = storage_backend.BucketScan(0, scan_keys, scan_metadatas, scan_buckets,
                                     8);
    ASSERT_TRUE(!res);
    ASSERT_EQ(res.error(), ErrorCode::KEYS_EXCEED_BUCKET_LIMIT);
    ASSERT_EQ(scan_buckets.size(), 0);
    ASSERT_EQ(scan_keys.size(), 0);
    ASSERT_EQ(scan_metadatas.size(), 0);
}

TEST_F(StorageBackendTest, InitializeWithValidStart) {
    BucketIdGenerator gen(100);
    EXPECT_EQ(gen.CurrentId(), 100);
    EXPECT_EQ(gen.NextId(), 101);
    EXPECT_EQ(gen.NextId(), 102);
}

TEST_F(StorageBackendTest, InitializeWithInvalidStart_UseTimestampFallback) {
    auto time = time_gen();
    int64_t expected = (time << 12) | 0;
    BucketIdGenerator gen(
        BucketIdGenerator::INIT_NEW_START_ID);  // invalid start
    LOG(INFO) << "expected is: " << expected << " gen is: " << gen.CurrentId();
    EXPECT_TRUE(expected <= gen.CurrentId());
}

TEST_F(StorageBackendTest, NextIdReturnsNewValue) {
    BucketIdGenerator gen(10);

    EXPECT_EQ(gen.NextId(), 11);  // Returns the new value: old + 1 = 11
    EXPECT_EQ(gen.NextId(), 12);
    EXPECT_EQ(gen.CurrentId(), 12);
}

TEST_F(StorageBackendTest, IdsAreMonotonicallyIncreasing) {
    BucketIdGenerator gen(100);

    int64_t id1 = gen.NextId();  // 101
    int64_t id2 = gen.NextId();  // 102
    int64_t id3 = gen.NextId();  // 103

    EXPECT_LT(id1, id2);
    EXPECT_LT(id2, id3);
    EXPECT_EQ(id1 + 1, id2);
    EXPECT_EQ(id2 + 1, id3);
}

TEST_F(StorageBackendTest, Concurrency_UniquenessAndNoDuplicates) {
    const int num_threads = 4;
    const int iterations_per_thread = 1000;

    BucketIdGenerator gen(1);
    std::vector<std::thread> threads;
    std::vector<int64_t> all_ids;
    std::mutex mutex;

    auto worker = [&gen, &all_ids, &mutex] {
        for (int i = 0; i < iterations_per_thread; ++i) {
            int64_t id = gen.NextId();  // Returns the next ID (new value)
            {
                std::lock_guard<std::mutex> lock(mutex);
                all_ids.push_back(id);
            }
        }
    };

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(worker);
    }

    for (auto& t : threads) {
        t.join();
    }

    // Check uniqueness: no duplicate IDs should exist
    std::set<int64_t> unique_ids(all_ids.begin(), all_ids.end());
    EXPECT_EQ(unique_ids.size(), all_ids.size())
        << "Duplicate IDs detected in concurrent execution!";
}

TEST_F(StorageBackendTest, CurrentIdReturnsLatestValue) {
    BucketIdGenerator gen(50);

    EXPECT_EQ(gen.CurrentId(), 50);
    EXPECT_EQ(gen.NextId(), 51);
    EXPECT_EQ(gen.CurrentId(), 51);
    EXPECT_EQ(gen.NextId(), 52);
    EXPECT_EQ(gen.CurrentId(), 52);
}

TEST_F(StorageBackendTest, LargeNumberOfIds_NoOverflowInLifetime) {
    BucketIdGenerator gen(1000);
    int64_t last_id = 1000;

    for (int i = 0; i < 100000; ++i) {
        int64_t id = gen.NextId();
        EXPECT_EQ(id, last_id + 1);  // Each ID increments by exactly 1
        last_id = id;
    }

    EXPECT_GE(last_id, 101000);  // Should have increased by at least 100,000
}

TEST_F(StorageBackendTest, OrphanedBucketFileCleanup) {
    std::string data_path = std::filesystem::current_path().string() + "/data";
    fs::create_directories(data_path);

    // Clean up any existing files
    for (const auto& entry : fs::directory_iterator(data_path)) {
        if (entry.is_regular_file()) {
            fs::remove(entry.path());
        }
    }

    FileStorageConfig config;
    config.storage_filepath = data_path;
    // Create a valid bucket with data and metadata
    BucketStorageBackend storage_backend(config);
    ASSERT_TRUE(storage_backend.Init());

    std::shared_ptr<SimpleAllocator> client_buffer_allocator =
        std::make_shared<SimpleAllocator>(128 * 1024 * 1024);

    // Create one valid bucket
    std::unordered_map<std::string, std::vector<Slice>> batched_slices;
    std::string key = "test_key";
    std::string data = "test_data_content";
    void* buffer = client_buffer_allocator->allocate(data.size());
    memcpy(buffer, data.data(), data.size());
    batched_slices.emplace(key, std::vector<Slice>{Slice{buffer, data.size()}});

    auto result = storage_backend.BatchOffload(
        batched_slices, [](const std::vector<std::string>& keys,
                           std::vector<StorageObjectMetadata>& metadatas) {
            return ErrorCode::OK;
        });
    ASSERT_TRUE(result);
    int64_t valid_bucket_id = result.value();

    // Manually create an orphaned bucket file (simulate crash scenario)
    // The orphaned file will have a different ID and no corresponding .meta
    // file
    int64_t orphaned_bucket_id = valid_bucket_id + 1000;
    std::string orphaned_bucket_path =
        data_path + "/" + std::to_string(orphaned_bucket_id) + ".bucket";

    // Write some data to the orphaned file
    std::ofstream orphan_file(orphaned_bucket_path, std::ios::binary);
    ASSERT_TRUE(orphan_file.is_open());
    std::string orphan_content =
        "This is orphaned bucket data without metadata";
    orphan_file.write(orphan_content.data(), orphan_content.size());
    orphan_file.close();

    // Verify the orphaned file exists
    ASSERT_TRUE(fs::exists(orphaned_bucket_path));

    // Create a second orphaned file with another ID
    int64_t orphaned_bucket_id_2 = valid_bucket_id + 2000;
    std::string orphaned_bucket_path_2 =
        data_path + "/" + std::to_string(orphaned_bucket_id_2) + ".bucket";
    std::ofstream orphan_file_2(orphaned_bucket_path_2, std::ios::binary);
    ASSERT_TRUE(orphan_file_2.is_open());
    orphan_file_2.write(orphan_content.data(), orphan_content.size());
    orphan_file_2.close();

    // Verify both orphaned files exist
    ASSERT_TRUE(fs::exists(orphaned_bucket_path));
    ASSERT_TRUE(fs::exists(orphaned_bucket_path_2));

    // Count files before cleanup
    int file_count_before = 0;
    for (const auto& entry : fs::directory_iterator(data_path)) {
        if (entry.is_regular_file()) {
            file_count_before++;
        }
    }
    // Should have: 1 valid .bucket + 1 valid .meta + 2 orphaned .bucket = 4
    ASSERT_EQ(file_count_before, 4);

    // Re-initialize the storage backend (orphan cleanup enabled by default now)
    // This should trigger orphan cleanup
    BucketStorageBackend storage_backend_2(config);
    auto init_result = storage_backend_2.Init();
    ASSERT_TRUE(init_result);

    // Verify the orphaned files were removed
    ASSERT_FALSE(fs::exists(orphaned_bucket_path))
        << "Orphaned bucket file should have been cleaned up during Init()";
    ASSERT_FALSE(fs::exists(orphaned_bucket_path_2))
        << "Second orphaned bucket file should have been cleaned up during "
           "Init()";

    // Verify the valid bucket's files still exist
    std::string valid_bucket_path =
        data_path + "/" + std::to_string(valid_bucket_id) + ".bucket";
    std::string valid_meta_path =
        data_path + "/" + std::to_string(valid_bucket_id) + ".meta";
    ASSERT_TRUE(fs::exists(valid_bucket_path))
        << "Valid bucket file should still exist";
    ASSERT_TRUE(fs::exists(valid_meta_path))
        << "Valid bucket metadata file should still exist";

    // Count files after cleanup
    int file_count_after = 0;
    for (const auto& entry : fs::directory_iterator(data_path)) {
        if (entry.is_regular_file()) {
            file_count_after++;
        }
    }
    // Should have only: 1 valid .bucket + 1 valid .meta = 2
    ASSERT_EQ(file_count_after, 2);

    // Verify the valid data can still be loaded
    auto is_exist = storage_backend_2.IsExist(key);
    ASSERT_TRUE(is_exist);
    ASSERT_TRUE(is_exist.value());
}

}  // namespace mooncake::test

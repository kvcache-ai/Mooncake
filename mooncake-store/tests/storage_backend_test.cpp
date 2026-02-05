#include "storage_backend.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <iostream>
#include <ranges>
#include <thread>
#include <atomic>
#include <mutex>
#include <fcntl.h>
#include <unistd.h>
#include <ylt/util/tl/expected.hpp>

#include "allocator.h"
#include "utils.h"
#include "utils/common.h"

namespace fs = std::filesystem;
namespace mooncake::test {

class StorageBackendTest : public ::testing::Test {
   protected:
    std::string data_path;

    // Helper function to test partial success behavior for any
    // StorageBackendInterface. Uses deterministic failure injection on a batch
    // of keys to ensure that some writes succeed and exactly one write fails,
    // then verifies that results and stored data match.
    static void TestPartialSuccessBehavior(StorageBackendInterface& backend,
                                           const std::string& backend_name) {
        // Set up test failure predicate: fail only "key2"
        // This provides deterministic failure injection for testing partial
        // success.
        backend.SetTestFailurePredicate([](const std::string& key) {
            return key == "key2";  // Fail only this key
        });

        // Create a batch with 3 keys:
        // - key1: should succeed
        // - key2: should fail (test failure predicate)
        // - key3: should succeed
        std::unordered_map<std::string, std::vector<Slice>> batch;
        std::vector<std::unique_ptr<char[]>> buffers;
        std::vector<std::string> keys = {"key1", "key2", "key3"};

        for (size_t i = 0; i < keys.size(); ++i) {
            std::string value(1024, static_cast<char>('a' + i));
            auto buf = std::make_unique<char[]>(value.size());
            std::memcpy(buf.get(), value.data(), value.size());
            batch.emplace(keys[i],
                          std::vector<Slice>{Slice{buf.get(), value.size()}});
            buffers.push_back(std::move(buf));
        }

        // Execute batch - should have partial success (2 succeed, 1 fails)
        auto offload_res = backend.BatchOffload(
            batch,
            [](const std::vector<std::string>&,
               std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; });

        // Verify partial success: should have exactly 2 successes, 1 failure
        // (deterministic via test failure predicate)
        ASSERT_TRUE(offload_res.has_value())
            << backend_name
            << ": BatchOffload should return success count even with partial "
               "failures";
        EXPECT_EQ(offload_res.value(), 2)
            << backend_name
            << ": Should have exactly 2 successful keys (key1 and key3)";

        // Verify return count matches actual keys written
        std::vector<tl::expected<bool, ErrorCode>> exists;
        for (const auto& key : keys) {
            exists.push_back(backend.IsExist(key));
        }

        ASSERT_TRUE(exists[0].has_value() && exists[1].has_value() &&
                    exists[2].has_value());

        int successful_keys = 0;
        for (const auto& exist : exists) {
            if (exist.value()) successful_keys++;
        }

        EXPECT_EQ(successful_keys, offload_res.value())
            << backend_name
            << ": Return count should match number of keys that exist";
        EXPECT_EQ(successful_keys, 2)
            << backend_name
            << ": Should have exactly 2 keys written (key1 and key3)";

        // Verify specific keys: key1 and key3 succeed, key2 fails
        EXPECT_TRUE(exists[0].value())
            << backend_name << ": key1 should have succeeded";
        EXPECT_FALSE(exists[1].value())
            << backend_name
            << ": key2 should have failed (test failure predicate)";
        EXPECT_TRUE(exists[2].value())
            << backend_name << ": key3 should have succeeded";
    }

    void SetUp() override {
        google::InitGoogleLogging("StorageBackendTest");
        FLAGS_logtostderr = true;
        data_path = std::filesystem::current_path().string() + "/data";
        // Remove all leftover files and subdirectories from previous runs
        if (fs::exists(data_path)) {
            for (const auto& entry : fs::directory_iterator(data_path)) {
                std::error_code ec;
                if (entry.is_regular_file()) {
                    fs::remove(entry.path(), ec);
                    if (ec) {
                        LOG(WARNING) << "Failed to remove file '"
                                     << entry.path() << "': " << ec.message();
                    }
                } else if (entry.is_directory()) {
                    fs::remove_all(entry.path(), ec);
                    if (ec) {
                        LOG(WARNING) << "Failed to remove directory '"
                                     << entry.path() << "': " << ec.message();
                    }
                }
            }
        }
        fs::create_directories(data_path);
    }

    void TearDown() override {
        google::ShutdownGoogleLogging();
        LOG(INFO) << "Clear test data...";
        // Clean up all test files and subdirectories
        if (fs::exists(data_path)) {
            for (const auto& entry : fs::directory_iterator(data_path)) {
                std::error_code ec;
                if (entry.is_regular_file()) {
                    fs::remove(entry.path(), ec);
                    if (ec) {
                        LOG(WARNING) << "Failed to remove file '"
                                     << entry.path() << "': " << ec.message();
                    }
                } else if (entry.is_directory()) {
                    fs::remove_all(entry.path(), ec);
                    if (ec) {
                        LOG(WARNING) << "Failed to remove directory '"
                                     << entry.path() << "': " << ec.message();
                    }
                }
            }
        }
    }
};

TEST_F(StorageBackendTest, StorageBackendAll) {
    std::shared_ptr<SimpleAllocator> client_buffer_allocator =
        std::make_shared<SimpleAllocator>(128 * 1024 * 1024);
    FileStorageConfig config;
    BucketBackendConfig bucket_config;
    config.storage_filepath = data_path;
    BucketStorageBackend storage_backend(config, bucket_config);

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
    BucketBackendConfig bucket_config;
    BucketStorageBackend storage_backend(config, bucket_config);
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
    BucketBackendConfig bucket_config;
    // Create a valid bucket with data and metadata
    BucketStorageBackend storage_backend(config, bucket_config);
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
    BucketStorageBackend storage_backend_2(config, bucket_config);
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

TEST_F(StorageBackendTest, AdaptorBatchOffloadAndBatchLoad) {
    FileStorageConfig cfg;

    cfg.storage_filepath = data_path;
    FilePerKeyConfig file_per_key_config;
    file_per_key_config.fsdir = "file_per_key_dir_offload_load";
    file_per_key_config.enable_eviction = false;

    StorageBackendAdaptor adaptor(cfg, file_per_key_config);
    ASSERT_TRUE(adaptor.Init());
    ASSERT_TRUE(
        adaptor.ScanMeta([](const std::vector<std::string>& keys,
                            std::vector<StorageObjectMetadata>& metadatas) {
            return ErrorCode::OK;
        }));

    std::unordered_map<std::string, std::string> test_data = {
        {"simple-key", "hello world"},
        {"key/with/invalid:chars", "value-2"},
    };

    std::unordered_map<std::string, std::vector<Slice>> batch_object;
    std::vector<std::unique_ptr<char[]>> offload_buffers;

    for (auto& [key, value] : test_data) {
        auto buf = std::make_unique<char[]>(value.size());
        std::memcpy(buf.get(), value.data(), value.size());

        std::vector<Slice> slices;
        slices.emplace_back(
            Slice{buf.get(), static_cast<size_t>(value.size())});
        batch_object.emplace(key, std::move(slices));

        offload_buffers.push_back(std::move(buf));
    }

    auto offload_result = adaptor.BatchOffload(
        batch_object,
        [](const std::vector<std::string>&,
           std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; });
    ASSERT_TRUE(offload_result);

    auto exist_simple = adaptor.IsExist("simple-key");
    ASSERT_TRUE(exist_simple);
    EXPECT_TRUE(exist_simple.value());

    auto exist_not = adaptor.IsExist("not-exist-key");
    ASSERT_TRUE(exist_not);
    EXPECT_FALSE(exist_not.value());

    std::unordered_map<std::string, Slice> load_slices;
    std::vector<std::unique_ptr<char[]>> load_buffers;

    for (auto& [key, value] : test_data) {
        auto buf = std::make_unique<char[]>(value.size());
        load_slices.emplace(
            key, Slice{buf.get(), static_cast<size_t>(value.size())});
        load_buffers.push_back(std::move(buf));
    }

    auto load_result = adaptor.BatchLoad(load_slices);
    ASSERT_TRUE(load_result);

    for (auto& [key, value] : test_data) {
        auto it = load_slices.find(key);
        ASSERT_NE(it, load_slices.end());

        std::string loaded(static_cast<char*>(it->second.ptr), it->second.size);
        EXPECT_EQ(loaded, value);
    }
}

TEST_F(StorageBackendTest, AdaptorBatchOffload_PartialSuccess) {
    FileStorageConfig cfg;
    cfg.storage_filepath = data_path;
    cfg.total_size_limit = 50 * 1024;  // Small quota: 50KB
    cfg.total_keys_limit = 1000;
    FilePerKeyConfig file_per_key_config;
    file_per_key_config.fsdir = "file_per_key_partial_success";
    file_per_key_config.enable_eviction = true;  // Enable eviction

    StorageBackendAdaptor adaptor(cfg, file_per_key_config);
    ASSERT_TRUE(adaptor.Init());

    // Must call ScanMeta first when eviction is enabled
    auto scan_res = adaptor.ScanMeta(
        [](const std::vector<std::string>&,
           std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; });
    ASSERT_TRUE(scan_res);

    // Test partial success with deterministic failure injection
    StorageBackendTest::TestPartialSuccessBehavior(adaptor,
                                                   "StorageBackendAdaptor");
}

//-----------------------------------------------------------------------------

TEST_F(StorageBackendTest, AdaptorBatchOffloadEmptyShouldFail) {
    FileStorageConfig cfg;
    cfg.storage_filepath = data_path + "/";

    FilePerKeyConfig file_per_key_config;
    file_per_key_config.fsdir = "file_per_key_dir_offload_empty";
    file_per_key_config.enable_eviction = false;

    StorageBackendAdaptor adaptor(cfg, file_per_key_config);
    ASSERT_TRUE(adaptor.Init());
    ASSERT_TRUE(
        adaptor.ScanMeta([](const std::vector<std::string>& keys,
                            std::vector<StorageObjectMetadata>& metadatas) {
            return ErrorCode::OK;
        }));

    std::unordered_map<std::string, std::vector<Slice>> empty_batch;

    auto res = adaptor.BatchOffload(
        empty_batch,
        [](const std::vector<std::string>&,
           std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; });

    EXPECT_FALSE(res);
    EXPECT_EQ(res.error(), ErrorCode::INVALID_KEY);
}

TEST_F(StorageBackendTest, AdaptorScanMetaAndIsEnableOffloading) {
    FileStorageConfig cfg;
    cfg.storage_filepath = data_path + "/";
    cfg.total_keys_limit = 10;
    cfg.total_size_limit = 1024 * 1024;

    FilePerKeyConfig file_per_key_config;
    file_per_key_config.fsdir = "file_per_key_dir_is_enable_offloading";
    file_per_key_config.enable_eviction = false;

    StorageBackendAdaptor adaptor(cfg, file_per_key_config);
    ASSERT_TRUE(adaptor.Init());

    auto enable_before = adaptor.IsEnableOffloading();
    EXPECT_FALSE(enable_before);
    EXPECT_EQ(enable_before.error(), ErrorCode::INTERNAL_ERROR);

    // New behavior: must call ScanMeta once before BatchOffload when eviction
    // is disabled, otherwise meta_scanned_ is false and BatchOffload is
    // rejected.
    auto scan_init_res = adaptor.ScanMeta(
        [](const std::vector<std::string>&,
           std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; });
    ASSERT_TRUE(scan_init_res);

    auto enable_empty = adaptor.IsEnableOffloading();
    ASSERT_TRUE(enable_empty);
    EXPECT_TRUE(enable_empty.value());

    std::unordered_map<std::string, std::string> test_data = {
        {"k1", std::string(128, 'a')},
        {"k2", std::string(256, 'b')},
    };

    std::unordered_map<std::string, std::vector<Slice>> batch_object;
    std::vector<std::unique_ptr<char[]>> offload_buffers;

    for (auto& [key, value] : test_data) {
        auto buf = std::make_unique<char[]>(value.size());
        std::memcpy(buf.get(), value.data(), value.size());

        std::vector<Slice> slices;
        slices.emplace_back(
            Slice{buf.get(), static_cast<size_t>(value.size())});
        batch_object.emplace(key, std::move(slices));

        offload_buffers.push_back(std::move(buf));
    }

    auto offload_result = adaptor.BatchOffload(
        batch_object,
        [](const std::vector<std::string>&,
           std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; });
    ASSERT_TRUE(offload_result);

    auto enable_after_write = adaptor.IsEnableOffloading();
    ASSERT_TRUE(enable_after_write);
    EXPECT_TRUE(enable_after_write.value());

    // Verify scan results via a fresh adaptor instance to avoid double-counting
    // totals if ScanMeta is called again on the same adaptor.
    StorageBackendAdaptor restart_adaptor(cfg, file_per_key_config);
    ASSERT_TRUE(restart_adaptor.Init());

    std::vector<std::string> scan_keys;
    std::vector<StorageObjectMetadata> scan_metas;

    auto scan_result = restart_adaptor.ScanMeta(
        [&](const std::vector<std::string>& keys,
            std::vector<StorageObjectMetadata>& metas) {
            scan_keys.insert(scan_keys.end(), keys.begin(), keys.end());
            scan_metas.insert(scan_metas.end(), metas.begin(), metas.end());
            return ErrorCode::OK;
        });
    ASSERT_TRUE(scan_result);

    EXPECT_EQ(scan_keys.size(), test_data.size());
    EXPECT_EQ(scan_metas.size(), test_data.size());

    auto enable_after = restart_adaptor.IsEnableOffloading();
    ASSERT_TRUE(enable_after);
    EXPECT_TRUE(enable_after.value());

    FileStorageConfig strict_cfg = cfg;
    strict_cfg.total_keys_limit = 1;
    strict_cfg.total_size_limit = 1;

    StorageBackendAdaptor strict_adaptor(strict_cfg, file_per_key_config);
    ASSERT_TRUE(strict_adaptor.Init());

    auto strict_scan_result = strict_adaptor.ScanMeta(
        [](const std::vector<std::string>&,
           std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; });
    ASSERT_TRUE(strict_scan_result);

    auto enable_strict = strict_adaptor.IsEnableOffloading();
    ASSERT_TRUE(enable_strict);
    EXPECT_FALSE(enable_strict.value());
}

TEST_F(StorageBackendTest, AdaptorScanMetaAndBatchLoadAcrossRestart) {
    FileStorageConfig cfg;
    cfg.storage_filepath = data_path;
    cfg.scanmeta_iterator_keys_limit = 16;
    cfg.total_keys_limit = 100;
    cfg.total_size_limit = 1 << 20;

    FilePerKeyConfig file_per_key_config;
    file_per_key_config.fsdir = "file_per_key_dir_batch_load_restart";
    file_per_key_config.enable_eviction = true;

    std::unordered_map<std::string, std::string> test_data = {
        {"simple-key", "hello world"},
        {"key/with:illegal*chars?", "value-2"},
        {"another_key", "third-value"},
    };

    {
        StorageBackendAdaptor adaptor(cfg, file_per_key_config);
        ASSERT_TRUE(adaptor.Init());

        std::unordered_map<std::string, std::vector<Slice>> batch_object;
        std::vector<std::unique_ptr<char[]>> write_buffers;
        write_buffers.reserve(test_data.size());

        for (auto& [key, value] : test_data) {
            auto buf = std::make_unique<char[]>(value.size());
            std::memcpy(buf.get(), value.data(), value.size());

            std::vector<Slice> slices;
            slices.emplace_back(
                Slice{buf.get(), static_cast<size_t>(value.size())});

            batch_object.emplace(key, std::move(slices));
            write_buffers.emplace_back(std::move(buf));
        }

        auto offload_res = adaptor.BatchOffload(
            batch_object,
            [](const std::vector<std::string>&,
               std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; });
        ASSERT_TRUE(offload_res);
    }

    {
        StorageBackendAdaptor adaptor(cfg, file_per_key_config);
        ASSERT_TRUE(adaptor.Init());

        std::vector<std::string> scan_keys;
        std::vector<StorageObjectMetadata> scan_metas;

        auto scan_res =
            adaptor.ScanMeta([&](const std::vector<std::string>& keys,
                                 std::vector<StorageObjectMetadata>& metas) {
                scan_keys.insert(scan_keys.end(), keys.begin(), keys.end());
                scan_metas.insert(scan_metas.end(), metas.begin(), metas.end());
                return ErrorCode::OK;
            });
        ASSERT_TRUE(scan_res);

        ASSERT_EQ(scan_keys.size(), test_data.size());
        ASSERT_EQ(scan_metas.size(), test_data.size());

        std::unordered_map<std::string, StorageObjectMetadata> meta_map;
        for (size_t i = 0; i < scan_keys.size(); ++i) {
            meta_map.emplace(scan_keys[i], scan_metas[i]);
        }

        for (auto& [key, value] : test_data) {
            auto it = meta_map.find(key);
            ASSERT_NE(it, meta_map.end())
                << "Meta for key " << key << " not found";
            EXPECT_EQ(it->second.data_size, static_cast<int64_t>(value.size()));
        }

        std::unordered_map<std::string, Slice> load_slices;
        std::vector<std::unique_ptr<char[]>> load_buffers;
        load_buffers.reserve(test_data.size());

        for (auto& [key, value] : test_data) {
            auto buf = std::make_unique<char[]>(value.size());
            load_slices.emplace(
                key, Slice{buf.get(), static_cast<size_t>(value.size())});
            load_buffers.emplace_back(std::move(buf));
        }

        auto load_res = adaptor.BatchLoad(load_slices);
        ASSERT_TRUE(load_res);

        for (auto& [key, value] : test_data) {
            auto it = load_slices.find(key);
            ASSERT_NE(it, load_slices.end());

            std::string loaded(static_cast<char*>(it->second.ptr),
                               it->second.size);
            EXPECT_EQ(loaded, value);
        }
    }
}

//-----------------------------------------------------------------------------

TEST_F(StorageBackendTest, OffsetAllocatorStorageBackend_BasicPutGet) {
    std::shared_ptr<SimpleAllocator> client_buffer_allocator =
        std::make_shared<SimpleAllocator>(128 * 1024 * 1024);
    FileStorageConfig config;
    config.storage_filepath = data_path;
    config.storage_backend_type = StorageBackendType::kOffsetAllocator;
    config.total_size_limit = 100 * 1024 * 1024;  // 100MB
    config.total_keys_limit = 10000;

    OffsetAllocatorStorageBackend storage_backend(config);
    ASSERT_TRUE(storage_backend.Init());

    // Test data
    std::unordered_map<std::string, std::string> test_data = {
        {"key1", "value1"},
        {"key2", "value2"},
        {"key3", "value3"},
    };

    // BatchOffload
    std::unordered_map<std::string, std::vector<Slice>> batch_object;
    std::vector<std::unique_ptr<char[]>> write_buffers;
    write_buffers.reserve(test_data.size());

    for (auto& [key, value] : test_data) {
        auto buf = std::make_unique<char[]>(value.size());
        std::memcpy(buf.get(), value.data(), value.size());
        std::vector<Slice> slices;
        slices.emplace_back(
            Slice{buf.get(), static_cast<size_t>(value.size())});
        batch_object.emplace(key, std::move(slices));
        write_buffers.emplace_back(std::move(buf));
    }

    auto offload_res = storage_backend.BatchOffload(
        batch_object,
        [](const std::vector<std::string>&,
           std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; });
    ASSERT_TRUE(offload_res);
    EXPECT_EQ(offload_res.value(), test_data.size());

    // BatchLoad
    std::unordered_map<std::string, Slice> load_slices;
    std::vector<std::unique_ptr<char[]>> load_buffers;
    load_buffers.reserve(test_data.size());

    for (auto& [key, value] : test_data) {
        auto buf = std::make_unique<char[]>(value.size());
        load_slices.emplace(
            key, Slice{buf.get(), static_cast<size_t>(value.size())});
        load_buffers.emplace_back(std::move(buf));
    }

    auto load_res = storage_backend.BatchLoad(load_slices);
    ASSERT_TRUE(load_res);

    // Verify data
    for (auto& [key, expected_value] : test_data) {
        auto is_exist_res = storage_backend.IsExist(key);
        ASSERT_TRUE(is_exist_res);
        EXPECT_TRUE(is_exist_res.value());

        auto it = load_slices.find(key);
        ASSERT_NE(it, load_slices.end());
        std::string loaded(static_cast<char*>(it->second.ptr), it->second.size);
        EXPECT_EQ(loaded, expected_value);
    }
}

//-----------------------------------------------------------------------------

TEST_F(StorageBackendTest, OffsetAllocatorStorageBackend_Overwrite) {
    std::shared_ptr<SimpleAllocator> client_buffer_allocator =
        std::make_shared<SimpleAllocator>(128 * 1024 * 1024);
    FileStorageConfig config;
    config.storage_filepath = data_path;
    config.storage_backend_type = StorageBackendType::kOffsetAllocator;
    config.total_size_limit = 100 * 1024 * 1024;
    config.total_keys_limit = 10000;

    OffsetAllocatorStorageBackend storage_backend(config);
    ASSERT_TRUE(storage_backend.Init());

    std::string key = "test_key";
    std::string value1 = "original_value";
    std::string value2 = "updated_value";

    // First write
    {
        auto buf = std::make_unique<char[]>(value1.size());
        std::memcpy(buf.get(), value1.data(), value1.size());
        std::unordered_map<std::string, std::vector<Slice>> batch_object;
        batch_object.emplace(
            key, std::vector<Slice>{Slice{buf.get(), value1.size()}});

        auto offload_res = storage_backend.BatchOffload(
            batch_object,
            [](const std::vector<std::string>&,
               std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; });
        ASSERT_TRUE(offload_res);
    }

    // Overwrite with different value
    {
        auto buf = std::make_unique<char[]>(value2.size());
        std::memcpy(buf.get(), value2.data(), value2.size());
        std::unordered_map<std::string, std::vector<Slice>> batch_object;
        batch_object.emplace(
            key, std::vector<Slice>{Slice{buf.get(), value2.size()}});

        auto offload_res = storage_backend.BatchOffload(
            batch_object,
            [](const std::vector<std::string>&,
               std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; });
        ASSERT_TRUE(offload_res);
    }

    // Verify new value
    {
        auto buf = std::make_unique<char[]>(value2.size());
        std::unordered_map<std::string, Slice> load_slices;
        load_slices.emplace(key, Slice{buf.get(), value2.size()});

        auto load_res = storage_backend.BatchLoad(load_slices);
        ASSERT_TRUE(load_res);

        std::string loaded(static_cast<char*>(buf.get()), value2.size());
        EXPECT_EQ(loaded, value2);
    }
}

//-----------------------------------------------------------------------------

TEST_F(StorageBackendTest, OffsetAllocatorStorageBackend_SizeMismatch) {
    FileStorageConfig config;
    config.storage_filepath = data_path;
    config.storage_backend_type = StorageBackendType::kOffsetAllocator;
    config.total_size_limit = 100 * 1024 * 1024;
    config.total_keys_limit = 10000;

    OffsetAllocatorStorageBackend storage_backend(config);
    ASSERT_TRUE(storage_backend.Init());

    // Write a valid record
    std::string key = "test_key";
    std::string value = "test_value";
    {
        auto buf = std::make_unique<char[]>(value.size());
        std::memcpy(buf.get(), value.data(), value.size());
        std::unordered_map<std::string, std::vector<Slice>> batch_object;
        batch_object.emplace(
            key, std::vector<Slice>{Slice{buf.get(), value.size()}});

        auto offload_res = storage_backend.BatchOffload(
            batch_object,
            [](const std::vector<std::string>&,
               std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; });
        ASSERT_TRUE(offload_res);
    }

    // Try to load with wrong size - should fail
    {
        size_t wrong_size = value.size() + 10;  // Wrong size
        auto buf = std::make_unique<char[]>(wrong_size);
        std::unordered_map<std::string, Slice> load_slices;
        load_slices.emplace(key, Slice{buf.get(), wrong_size});

        auto load_res = storage_backend.BatchLoad(load_slices);
        // Should fail due to size mismatch
        EXPECT_FALSE(load_res.has_value());
        EXPECT_EQ(load_res.error(), ErrorCode::INVALID_PARAMS);
    }
}

//-----------------------------------------------------------------------------

TEST_F(StorageBackendTest, OffsetAllocatorStorageBackend_Concurrency) {
    FileStorageConfig config;
    config.storage_filepath = data_path;
    config.storage_backend_type = StorageBackendType::kOffsetAllocator;
    config.total_size_limit = 100 * 1024 * 1024;
    config.total_keys_limit = 10000;

    OffsetAllocatorStorageBackend storage_backend(config);
    ASSERT_TRUE(storage_backend.Init());

    const int num_threads = 4;
    const int keys_per_thread = 10;
    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};

    // Concurrent writes
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < keys_per_thread; ++i) {
                std::string key =
                    "thread_" + std::to_string(t) + "_key_" + std::to_string(i);
                std::string value =
                    "value_" + std::to_string(t) + "_" + std::to_string(i);

                auto buf = std::make_unique<char[]>(value.size());
                std::memcpy(buf.get(), value.data(), value.size());
                std::unordered_map<std::string, std::vector<Slice>>
                    batch_object;
                batch_object.emplace(
                    key, std::vector<Slice>{Slice{buf.get(), value.size()}});

                auto offload_res = storage_backend.BatchOffload(
                    batch_object, [](const std::vector<std::string>&,
                                     std::vector<StorageObjectMetadata>&) {
                        return ErrorCode::OK;
                    });
                if (offload_res.has_value()) {
                    success_count++;
                }
            }
        });
    }

    // Concurrent reads (while writes are happening)
    std::atomic<int> read_success_count{0};
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < keys_per_thread; ++i) {
                std::string key =
                    "thread_" + std::to_string(t) + "_key_" + std::to_string(i);
                auto is_exist_res = storage_backend.IsExist(key);
                if (is_exist_res.has_value() && is_exist_res.value()) {
                    read_success_count++;
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // Verify all writes succeeded
    const int expected_writes = num_threads * keys_per_thread;
    EXPECT_EQ(success_count.load(), expected_writes)
        << "Expected " << expected_writes << " successful writes, got "
        << success_count.load();
}

//-----------------------------------------------------------------------------

TEST_F(StorageBackendTest, OffsetAllocatorStorageBackend_ScanMeta) {
    FileStorageConfig config;
    config.storage_filepath = data_path;
    config.storage_backend_type = StorageBackendType::kOffsetAllocator;
    config.total_size_limit = 100 * 1024 * 1024;
    config.total_keys_limit = 10000;

    OffsetAllocatorStorageBackend storage_backend(config);
    ASSERT_TRUE(storage_backend.Init());

    // Write some data
    std::unordered_map<std::string, std::string> test_data = {
        {"key1", "value1"},
        {"key2", "value2"},
        {"key3", "value3"},
    };

    for (auto& [key, value] : test_data) {
        auto buf = std::make_unique<char[]>(value.size());
        std::memcpy(buf.get(), value.data(), value.size());
        std::unordered_map<std::string, std::vector<Slice>> batch_object;
        batch_object.emplace(
            key, std::vector<Slice>{Slice{buf.get(), value.size()}});

        auto offload_res = storage_backend.BatchOffload(
            batch_object,
            [](const std::vector<std::string>&,
               std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; });
        ASSERT_TRUE(offload_res);
    }

    // Scan metadata
    std::vector<std::string> scan_keys;
    std::vector<StorageObjectMetadata> scan_metas;

    auto scan_res = storage_backend.ScanMeta(
        [&](const std::vector<std::string>& keys,
            std::vector<StorageObjectMetadata>& metas) {
            scan_keys.insert(scan_keys.end(), keys.begin(), keys.end());
            scan_metas.insert(scan_metas.end(), metas.begin(), metas.end());
            return ErrorCode::OK;
        });
    ASSERT_TRUE(scan_res);

    EXPECT_EQ(scan_keys.size(), test_data.size());
    EXPECT_EQ(scan_metas.size(), test_data.size());

    // Verify all keys are present
    for (const auto& [key, value] : test_data) {
        EXPECT_NE(std::find(scan_keys.begin(), scan_keys.end(), key),
                  scan_keys.end());
    }
}

//-----------------------------------------------------------------------------

TEST_F(StorageBackendTest, OffsetAllocatorStorageBackend_DoubleInit) {
    FileStorageConfig config;
    config.storage_filepath = data_path;
    config.storage_backend_type = StorageBackendType::kOffsetAllocator;
    config.total_size_limit = 10 * 1024 * 1024;  // 10MB
    config.total_keys_limit = 1000;

    OffsetAllocatorStorageBackend storage_backend(config);

    // First init should succeed
    ASSERT_TRUE(storage_backend.Init());

    // Second init should fail
    auto second_init = storage_backend.Init();
    EXPECT_FALSE(second_init.has_value());
    EXPECT_EQ(second_init.error(), ErrorCode::INTERNAL_ERROR);
}

//-----------------------------------------------------------------------------

TEST_F(StorageBackendTest, OffsetAllocatorStorageBackend_BatchOffloadEmpty) {
    FileStorageConfig config;
    config.storage_filepath = data_path;
    config.storage_backend_type = StorageBackendType::kOffsetAllocator;
    config.total_size_limit = 10 * 1024 * 1024;
    config.total_keys_limit = 1000;

    OffsetAllocatorStorageBackend storage_backend(config);
    ASSERT_TRUE(storage_backend.Init());

    std::unordered_map<std::string, std::vector<Slice>> batch_object;  // Empty

    auto offload_res = storage_backend.BatchOffload(
        batch_object,
        [](const std::vector<std::string>&,
           std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; });

    EXPECT_FALSE(offload_res.has_value());
    EXPECT_EQ(offload_res.error(), ErrorCode::INVALID_KEY);
}

//-----------------------------------------------------------------------------

TEST_F(StorageBackendTest, OffsetAllocatorStorageBackend_OutOfSpace) {
    FileStorageConfig config;
    config.storage_filepath = data_path;
    config.storage_backend_type = StorageBackendType::kOffsetAllocator;
    config.total_size_limit = 50 * 1024;  // Small: 50KB
    config.total_keys_limit = 10000;

    OffsetAllocatorStorageBackend storage_backend(config);
    ASSERT_TRUE(storage_backend.Init());

    // Write data until out of space
    std::vector<std::unique_ptr<char[]>> buffers;
    bool allocation_failed = false;

    for (int i = 0; i < 100 && !allocation_failed; ++i) {
        std::string key = "key_" + std::to_string(i);
        std::string value(1024, 'x');  // 1KB each

        auto buf = std::make_unique<char[]>(value.size());
        std::memcpy(buf.get(), value.data(), value.size());
        std::unordered_map<std::string, std::vector<Slice>> batch_object;
        batch_object.emplace(
            key, std::vector<Slice>{Slice{buf.get(), value.size()}});

        auto offload_res = storage_backend.BatchOffload(
            batch_object,
            [](const std::vector<std::string>&,
               std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; });

        // With partial success: batch with 1 key that fails returns 0 (not
        // error)
        if (!offload_res.has_value()) {
            allocation_failed = true;
            EXPECT_TRUE(offload_res.error() == ErrorCode::FILE_WRITE_FAIL ||
                        offload_res.error() == ErrorCode::KEYS_ULTRA_LIMIT);
        } else if (offload_res.value() == 0) {
            allocation_failed = true;
        } else {
            buffers.push_back(std::move(buf));
        }
    }

    // Verify that at least some allocations succeeded before failure
    EXPECT_GT(buffers.size(), 0)
        << "Expected at least some allocations to succeed before failure";
    EXPECT_TRUE(allocation_failed)
        << "Expected allocation to fail due to capacity after "
        << buffers.size() << " successful allocations";
}

//-----------------------------------------------------------------------------

TEST_F(StorageBackendTest, OffsetAllocatorStorageBackend_KeyNotFound) {
    FileStorageConfig config;
    config.storage_filepath = data_path;
    config.storage_backend_type = StorageBackendType::kOffsetAllocator;
    config.total_size_limit = 10 * 1024 * 1024;
    config.total_keys_limit = 1000;

    OffsetAllocatorStorageBackend storage_backend(config);
    ASSERT_TRUE(storage_backend.Init());

    // Try to load non-existent key
    auto buf = std::make_unique<char[]>(10);
    std::unordered_map<std::string, Slice> load_slices;
    load_slices.emplace("non_existent_key", Slice{buf.get(), 10});

    auto load_res = storage_backend.BatchLoad(load_slices);

    EXPECT_FALSE(load_res.has_value());
    EXPECT_EQ(load_res.error(), ErrorCode::OBJECT_NOT_FOUND);
}

//-----------------------------------------------------------------------------

TEST_F(StorageBackendTest, OffsetAllocatorStorageBackend_CorruptedHeader) {
    FileStorageConfig config;
    config.storage_filepath = data_path;
    config.storage_backend_type = StorageBackendType::kOffsetAllocator;
    config.total_size_limit = 10 * 1024 * 1024;
    config.total_keys_limit = 1000;

    OffsetAllocatorStorageBackend storage_backend(config);
    ASSERT_TRUE(storage_backend.Init());

    // Write valid record
    std::string key = "test_key";
    std::string value = "test_value";
    {
        auto buf = std::make_unique<char[]>(value.size());
        std::memcpy(buf.get(), value.data(), value.size());
        std::unordered_map<std::string, std::vector<Slice>> batch_object;
        batch_object.emplace(
            key, std::vector<Slice>{Slice{buf.get(), value.size()}});

        auto offload_res = storage_backend.BatchOffload(
            batch_object,
            [](const std::vector<std::string>&,
               std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; });
        ASSERT_TRUE(offload_res);
    }

    // Corrupt the header by writing garbage to the value_len field
    std::string data_file = data_path + "/kv_cache.data";
    int fd = open(data_file.c_str(), O_WRONLY);
    ASSERT_GE(fd, 0);

    // Use RAII to ensure fd is closed even if assertions fail
    struct FdCloser {
        int fd_;
        explicit FdCloser(int fd) : fd_(fd) {}
        ~FdCloser() {
            if (fd_ >= 0) close(fd_);
        }
    };
    FdCloser fd_closer(fd);

    // Seek past key_len to the value_len field
    ASSERT_NE(lseek(fd, sizeof(uint32_t), SEEK_SET), -1);

    uint32_t corrupt_value = 0xFFFFFFFF;  // Invalid value_len
    ssize_t written = write(fd, &corrupt_value, sizeof(corrupt_value));
    ASSERT_EQ(written, static_cast<ssize_t>(sizeof(corrupt_value)));
    // fd_closer destructor will close fd automatically

    // Try to load - should detect corruption via ValidateAgainstMetadata
    auto buf = std::make_unique<char[]>(value.size());
    std::unordered_map<std::string, Slice> load_slices;
    load_slices.emplace(key, Slice{buf.get(), value.size()});

    auto load_res = storage_backend.BatchLoad(load_slices);

    // Should fail due to corrupted header
    EXPECT_FALSE(load_res.has_value());
    EXPECT_EQ(load_res.error(), ErrorCode::FILE_READ_FAIL);
}

//-----------------------------------------------------------------------------

TEST_F(StorageBackendTest,
       OffsetAllocatorStorageBackend_IsEnableOffloadingLimits) {
    FileStorageConfig config;
    config.storage_filepath = data_path;
    config.storage_backend_type = StorageBackendType::kOffsetAllocator;
    config.total_size_limit = 50 * 1024;  // 50KB
    config.total_keys_limit = 5;          // Small limit

    OffsetAllocatorStorageBackend storage_backend(config);
    ASSERT_TRUE(storage_backend.Init());

    // Initially should be enabled
    auto is_enabled = storage_backend.IsEnableOffloading();
    ASSERT_TRUE(is_enabled);
    EXPECT_TRUE(is_enabled.value());

    // Write keys up to limit
    std::vector<std::unique_ptr<char[]>> buffers;
    for (int i = 0; i < 5; ++i) {
        std::string key = "key_" + std::to_string(i);
        std::string value = "value_" + std::to_string(i);

        auto buf = std::make_unique<char[]>(value.size());
        std::memcpy(buf.get(), value.data(), value.size());
        std::unordered_map<std::string, std::vector<Slice>> batch_object;
        batch_object.emplace(
            key, std::vector<Slice>{Slice{buf.get(), value.size()}});

        auto offload_res = storage_backend.BatchOffload(
            batch_object,
            [](const std::vector<std::string>&,
               std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; });
        ASSERT_TRUE(offload_res);
        buffers.push_back(std::move(buf));
    }

    // After reaching keys limit (5 keys, limit=5), should be disabled
    // Uses < comparison: 5 < 5 = FALSE, so offloading disabled
    is_enabled = storage_backend.IsEnableOffloading();
    ASSERT_TRUE(is_enabled);
    EXPECT_FALSE(is_enabled.value());
}

//-----------------------------------------------------------------------------

TEST_F(StorageBackendTest, OffsetAllocatorStorageBackend_EmptyValue) {
    FileStorageConfig config;
    config.storage_filepath = data_path;
    config.storage_backend_type = StorageBackendType::kOffsetAllocator;
    config.total_size_limit = 10 * 1024 * 1024;
    config.total_keys_limit = 1000;

    OffsetAllocatorStorageBackend storage_backend(config);
    ASSERT_TRUE(storage_backend.Init());

    // Write empty value
    std::string key = "empty_key";
    std::unordered_map<std::string, std::vector<Slice>> batch_object;
    // Empty slices vector should be skipped
    batch_object.emplace(key, std::vector<Slice>{});

    auto offload_res = storage_backend.BatchOffload(
        batch_object,
        [](const std::vector<std::string>&,
           std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; });

    // Should return 0 keys offloaded (empty slices are skipped)
    ASSERT_TRUE(offload_res);
    EXPECT_EQ(offload_res.value(), 0);
}

//-----------------------------------------------------------------------------

TEST_F(StorageBackendTest, OffsetAllocatorStorageBackend_MultipleSlices) {
    FileStorageConfig config;
    config.storage_filepath = data_path;
    config.storage_backend_type = StorageBackendType::kOffsetAllocator;
    config.total_size_limit = 10 * 1024 * 1024;
    config.total_keys_limit = 1000;

    OffsetAllocatorStorageBackend storage_backend(config);
    ASSERT_TRUE(storage_backend.Init());

    // Write value split across multiple slices
    std::string key = "multi_slice_key";
    std::string part1 = "Hello, ";
    std::string part2 = "World";
    std::string part3 = "!";
    std::string expected = part1 + part2 + part3;

    auto buf1 = std::make_unique<char[]>(part1.size());
    auto buf2 = std::make_unique<char[]>(part2.size());
    auto buf3 = std::make_unique<char[]>(part3.size());
    std::memcpy(buf1.get(), part1.data(), part1.size());
    std::memcpy(buf2.get(), part2.data(), part2.size());
    std::memcpy(buf3.get(), part3.data(), part3.size());

    std::unordered_map<std::string, std::vector<Slice>> batch_object;
    std::vector<Slice> slices;
    slices.push_back(Slice{buf1.get(), part1.size()});
    slices.push_back(Slice{buf2.get(), part2.size()});
    slices.push_back(Slice{buf3.get(), part3.size()});
    batch_object.emplace(key, std::move(slices));

    auto offload_res = storage_backend.BatchOffload(
        batch_object,
        [](const std::vector<std::string>&,
           std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; });
    ASSERT_TRUE(offload_res);

    // Read back and verify concatenation
    auto load_buf = std::make_unique<char[]>(expected.size());
    std::unordered_map<std::string, Slice> load_slices;
    load_slices.emplace(key, Slice{load_buf.get(), expected.size()});

    auto load_res = storage_backend.BatchLoad(load_slices);
    ASSERT_TRUE(load_res);

    std::string loaded(load_buf.get(), expected.size());
    EXPECT_EQ(loaded, expected);
}

//-----------------------------------------------------------------------------

TEST_F(StorageBackendTest,
       OffsetAllocatorStorageBackend_CompleteHandlerFailure) {
    FileStorageConfig config;
    config.storage_filepath = data_path;
    config.storage_backend_type = StorageBackendType::kOffsetAllocator;
    config.total_size_limit = 10 * 1024 * 1024;
    config.total_keys_limit = 1000;

    OffsetAllocatorStorageBackend storage_backend(config);
    ASSERT_TRUE(storage_backend.Init());

    std::string key = "test_key";
    std::string value = "test_value";
    auto buf = std::make_unique<char[]>(value.size());
    std::memcpy(buf.get(), value.data(), value.size());
    std::unordered_map<std::string, std::vector<Slice>> batch_object;
    batch_object.emplace(key,
                         std::vector<Slice>{Slice{buf.get(), value.size()}});

    // Handler that returns error
    auto offload_res = storage_backend.BatchOffload(
        batch_object, [](const std::vector<std::string>&,
                         std::vector<StorageObjectMetadata>&) {
            return ErrorCode::INTERNAL_ERROR;  // Simulate handler failure
        });

    EXPECT_FALSE(offload_res.has_value());
    EXPECT_EQ(offload_res.error(), ErrorCode::INTERNAL_ERROR);
}

//-----------------------------------------------------------------------------

TEST_F(StorageBackendTest, OffsetAllocatorStorageBackend_PartialSuccess) {
    FileStorageConfig config;
    config.storage_filepath = data_path;
    config.storage_backend_type = StorageBackendType::kOffsetAllocator;
    config.total_size_limit = 50 * 1024;  // Very small: 50KB
    config.total_keys_limit = 1000;

    OffsetAllocatorStorageBackend storage_backend(config);
    ASSERT_TRUE(storage_backend.Init());

    StorageBackendTest::TestPartialSuccessBehavior(
        storage_backend, "OffsetAllocatorStorageBackend");
}

//-----------------------------------------------------------------------------

TEST_F(StorageBackendTest, OffsetAllocatorStorageBackend_ScanMetaEmpty) {
    FileStorageConfig config;
    config.storage_filepath = data_path;
    config.storage_backend_type = StorageBackendType::kOffsetAllocator;
    config.total_size_limit = 10 * 1024 * 1024;
    config.total_keys_limit = 1000;

    OffsetAllocatorStorageBackend storage_backend(config);
    ASSERT_TRUE(storage_backend.Init());

    // Scan empty backend
    bool handler_called = false;
    auto scan_res = storage_backend.ScanMeta(
        [&handler_called](const std::vector<std::string>& keys,
                          std::vector<StorageObjectMetadata>& metas) {
            handler_called = true;
            return ErrorCode::OK;
        });

    ASSERT_TRUE(scan_res);
    EXPECT_FALSE(handler_called)
        << "Handler should not be called for empty backend";
}

//-----------------------------------------------------------------------------
// Combined test for all operations that should fail when not initialized

TEST_F(StorageBackendTest,
       OffsetAllocatorStorageBackend_AllMethodsFailWhenNotInitialized) {
    FileStorageConfig config;
    config.storage_filepath = data_path;
    config.storage_backend_type = StorageBackendType::kOffsetAllocator;
    config.total_size_limit = 10 * 1024 * 1024;
    config.total_keys_limit = 1000;

    OffsetAllocatorStorageBackend storage_backend(config);
    // Don't call Init() - all methods should return INTERNAL_ERROR

    // Test IsExist
    {
        auto result = storage_backend.IsExist("test_key");
        EXPECT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), ErrorCode::INTERNAL_ERROR);
    }

    // Test IsEnableOffloading
    {
        auto result = storage_backend.IsEnableOffloading();
        EXPECT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), ErrorCode::INTERNAL_ERROR);
    }

    // Test ScanMeta
    {
        auto result = storage_backend.ScanMeta(
            [](const std::vector<std::string>&,
               std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; });
        EXPECT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), ErrorCode::INTERNAL_ERROR);
    }

    // Test BatchLoad
    {
        std::vector<char> buffer(100);
        std::unordered_map<std::string, Slice> batch;
        batch["key"] = Slice{buffer.data(), buffer.size()};
        auto result = storage_backend.BatchLoad(batch);
        EXPECT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), ErrorCode::INTERNAL_ERROR);
    }

    // Test BatchOffload
    {
        std::string value = "test_value";
        std::unordered_map<std::string, std::vector<Slice>> batch;
        batch["key"] = {Slice{value.data(), value.size()}};
        auto result = storage_backend.BatchOffload(
            batch,
            [](const std::vector<std::string>&,
               std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; });
        EXPECT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), ErrorCode::INTERNAL_ERROR);
    }
}

//-----------------------------------------------------------------------------

// Concurrency test: verify no reuse-after-free with lock striping + refcounted
// handles Thread R repeatedly reads keyA while Thread W overwrites keyA and
// keyB Small capacity + large records GUARANTEES allocator reuse happens
// immediately
TEST_F(StorageBackendTest,
       OffsetAllocatorStorageBackend_LockStripingNoReuseAfterFree) {
    FileStorageConfig config;
    config.storage_filepath = data_path;
    config.storage_backend_type = StorageBackendType::kOffsetAllocator;
    // Small capacity to force immediate reuse: 500KB capacity, ~20KB per record
    // Can fit ~20 records total, alternating overwrites will force reuse
    // quickly
    config.total_size_limit = 500 * 1024;  // 500KB
    config.total_keys_limit = 100;

    OffsetAllocatorStorageBackend storage_backend(config);
    auto init_result = storage_backend.Init();
    ASSERT_TRUE(init_result.has_value());

    // Magic patterns to identify which key's data we're reading
    const std::string keyA = "keyA";
    const std::string keyB = "keyB";

    // Pattern: 8-byte magic header + key name repeated
    auto make_pattern = [](const std::string& key,
                           size_t total_size) -> std::string {
        std::string pattern;
        pattern.reserve(total_size);

        // Magic header: key name as 8-byte prefix (padded/truncated)
        std::string magic = key;
        magic.resize(8, '_');
        pattern += magic;

        // Fill rest with repeated key name
        while (pattern.size() < total_size) {
            pattern += key;
        }
        pattern.resize(total_size);
        return pattern;
    };

    // 20KB values to force reuse without being too large
    // Each record ~20KB, so ~20 records fit in 450KB capacity (90% of 500KB)
    // Alternating overwrites of keyA/keyB will cause frequent reuse
    const size_t value_size = 20 * 1024;  // 20KB values
    std::string patternA = make_pattern(keyA, value_size);
    std::string patternB = make_pattern(keyB, value_size);

    // Initial write of keyA
    {
        std::unordered_map<std::string, std::vector<Slice>> batch;
        batch[keyA] = {Slice{patternA.data(), patternA.size()}};

        auto offload_result = storage_backend.BatchOffload(
            batch,
            [](const std::vector<std::string>&,
               std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; });
        ASSERT_TRUE(offload_result.has_value())
            << "Initial write failed with error: "
            << (offload_result.has_value()
                    ? 0
                    : static_cast<int>(offload_result.error()));
    }

    std::atomic<bool> stop{false};
    std::atomic<int64_t> read_count{0};
    std::atomic<int64_t> write_count{0};
    std::atomic<bool> corruption_detected{false};

    // Reader thread: repeatedly read keyA and validate pattern
    auto reader_thread = std::thread([&]() {
        std::vector<char> buffer(value_size);

        while (!stop.load(std::memory_order_acquire)) {
            std::unordered_map<std::string, Slice> batch;
            batch[keyA] = Slice{buffer.data(), buffer.size()};

            auto load_result = storage_backend.BatchLoad(batch);
            if (load_result.has_value()) {
                // Validate magic header
                std::string magic(buffer.data(),
                                  std::min(size_t(8), buffer.size()));
                std::string expected_magic = keyA;
                expected_magic.resize(8, '_');

                if (magic != expected_magic) {
                    LOG(ERROR)
                        << "CORRUPTION: Expected magic '" << expected_magic
                        << "' but got '" << magic << "'";
                    corruption_detected.store(true, std::memory_order_release);
                    break;
                }

                // Validate pattern consistency (sample check)
                for (size_t i = 8; i < std::min(size_t(100), buffer.size());
                     ++i) {
                    size_t pattern_idx = i % patternA.size();
                    if (buffer[i] != patternA[pattern_idx]) {
                        LOG(ERROR)
                            << "CORRUPTION: Pattern mismatch at offset " << i;
                        corruption_detected.store(true,
                                                  std::memory_order_release);
                        break;
                    }
                }

                read_count.fetch_add(1, std::memory_order_relaxed);
            }

            // Intentional sleep to widen race window
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    });

    // Writer thread: repeatedly overwrite keyA and keyB to trigger reuse
    auto writer_thread = std::thread([&]() {
        int iteration = 0;
        while (!stop.load(std::memory_order_acquire)) {
            // Alternate between overwriting keyA and keyB
            // HOW REUSE IS GUARANTEED:
            // 1. Capacity = 450KB (90% of 500KB), each record = ~20KB
            // 2. Can fit ~20 records, but we only use 2 keys (keyA, keyB)
            // 3. When we overwrite keyA, old allocation is freed (if refcount →
            // 0)
            // 4. Allocator reuses that freed range for next allocation
            // 5. If reader still holds old AllocationPtr, extent stays alive
            // (refcount > 0)
            // 6. This tests the critical scenario: concurrent read of old
            // extent during reuse
            std::string target_key = (iteration % 2 == 0) ? keyA : keyB;
            std::string pattern = (target_key == keyA) ? patternA : patternB;

            std::unordered_map<std::string, std::vector<Slice>> batch;
            batch[target_key] = {Slice{pattern.data(), pattern.size()}};

            auto offload_result = storage_backend.BatchOffload(
                batch, [](const std::vector<std::string>&,
                          std::vector<StorageObjectMetadata>&) {
                    return ErrorCode::OK;
                });

            if (offload_result.has_value()) {
                write_count.fetch_add(1, std::memory_order_relaxed);
            }

            ++iteration;

            // Small delay to allow reader to catch up
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        }
    });

    // Run test for 3 seconds
    std::this_thread::sleep_for(std::chrono::seconds(3));
    stop.store(true, std::memory_order_release);

    reader_thread.join();
    writer_thread.join();

    LOG(INFO) << "Concurrency test completed: " << read_count.load()
              << " reads, " << write_count.load() << " writes";

    EXPECT_FALSE(corruption_detected.load())
        << "Detected corruption: reader got wrong data (reuse-after-free bug)";
    EXPECT_GT(read_count.load(), 0)
        << "Reader should have completed some reads";
    EXPECT_GT(write_count.load(), 0)
        << "Writer should have completed some writes";
}

//-----------------------------------------------------------------------------
// BucketStorageBackend: Duplicate Key Detection Tests (Phase 0 - D0)
//-----------------------------------------------------------------------------

TEST_F(StorageBackendTest, BucketStorageBackend_DuplicateKeyRejected) {
    FileStorageConfig config;
    config.storage_filepath = data_path;
    BucketBackendConfig bucket_config;
    BucketStorageBackend storage_backend(config, bucket_config);
    ASSERT_TRUE(storage_backend.Init());

    // Write initial key
    std::string key = "duplicate_test_key";
    std::string value1 = "original_value_data";
    auto buf1 = std::make_unique<char[]>(value1.size());
    std::memcpy(buf1.get(), value1.data(), value1.size());

    std::unordered_map<std::string, std::vector<Slice>> batch1;
    batch1.emplace(key, std::vector<Slice>{Slice{buf1.get(), value1.size()}});

    auto result1 = storage_backend.BatchOffload(
        batch1,
        [](const std::vector<std::string>&,
           std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; });
    ASSERT_TRUE(result1.has_value()) << "First write should succeed";

    // Attempt to write the same key again
    std::string value2 = "duplicate_value_data";
    auto buf2 = std::make_unique<char[]>(value2.size());
    std::memcpy(buf2.get(), value2.data(), value2.size());

    std::unordered_map<std::string, std::vector<Slice>> batch2;
    batch2.emplace(key, std::vector<Slice>{Slice{buf2.get(), value2.size()}});

    auto result2 = storage_backend.BatchOffload(
        batch2,
        [](const std::vector<std::string>&,
           std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; });
    ASSERT_FALSE(result2.has_value()) << "Duplicate key should be rejected";
    EXPECT_EQ(result2.error(), ErrorCode::OBJECT_ALREADY_EXISTS);

    // Verify original data is still readable and not corrupted
    auto is_exist = storage_backend.IsExist(key);
    ASSERT_TRUE(is_exist.has_value());
    EXPECT_TRUE(is_exist.value());

    auto read_buf = std::make_unique<char[]>(value1.size());
    std::unordered_map<std::string, Slice> load_slices;
    load_slices.emplace(key, Slice{read_buf.get(), value1.size()});

    auto load_result = storage_backend.BatchLoad(load_slices);
    ASSERT_TRUE(load_result.has_value()) << "Load should succeed";

    std::string loaded(read_buf.get(), value1.size());
    EXPECT_EQ(loaded, value1) << "Original data should be intact";
}

//-----------------------------------------------------------------------------

TEST_F(StorageBackendTest,
       BucketStorageBackend_DuplicateKeyCleanupOrphanedFiles) {
    FileStorageConfig config;
    config.storage_filepath = data_path;
    BucketBackendConfig bucket_config;
    BucketStorageBackend storage_backend(config, bucket_config);
    ASSERT_TRUE(storage_backend.Init());

    // Write initial key
    std::string key = "keyA";
    std::string value = "test_value_for_keyA";
    auto buf = std::make_unique<char[]>(value.size());
    std::memcpy(buf.get(), value.data(), value.size());

    std::unordered_map<std::string, std::vector<Slice>> batch1;
    batch1.emplace(key, std::vector<Slice>{Slice{buf.get(), value.size()}});

    auto result1 = storage_backend.BatchOffload(
        batch1,
        [](const std::vector<std::string>&,
           std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; });
    ASSERT_TRUE(result1.has_value());
    int64_t bucket1_id = result1.value();

    // Count files before duplicate attempt
    int file_count_before = 0;
    for (const auto& entry : fs::directory_iterator(data_path)) {
        if (entry.is_regular_file()) {
            file_count_before++;
        }
    }
    // Should have 1 .bucket + 1 .meta = 2 files
    EXPECT_EQ(file_count_before, 2);

    // Attempt to write duplicate key (this creates bucket files before
    // detecting duplicate)
    std::string value2 = "duplicate_value";
    auto buf2 = std::make_unique<char[]>(value2.size());
    std::memcpy(buf2.get(), value2.data(), value2.size());

    std::unordered_map<std::string, std::vector<Slice>> batch2;
    batch2.emplace(key, std::vector<Slice>{Slice{buf2.get(), value2.size()}});

    auto result2 = storage_backend.BatchOffload(
        batch2,
        [](const std::vector<std::string>&,
           std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; });
    ASSERT_FALSE(result2.has_value());
    EXPECT_EQ(result2.error(), ErrorCode::OBJECT_ALREADY_EXISTS);

    // Count files after duplicate attempt - orphaned files should be cleaned up
    int file_count_after = 0;
    for (const auto& entry : fs::directory_iterator(data_path)) {
        if (entry.is_regular_file()) {
            file_count_after++;
        }
    }
    // Should still have only 2 files (orphaned bucket 2 files should be cleaned
    // up)
    EXPECT_EQ(file_count_after, 2) << "Orphaned bucket files should be cleaned "
                                      "up after duplicate detection";

    // Verify bucket 1 files still exist
    std::string bucket1_data_path =
        data_path + "/" + std::to_string(bucket1_id) + ".bucket";
    std::string bucket1_meta_path =
        data_path + "/" + std::to_string(bucket1_id) + ".meta";
    EXPECT_TRUE(fs::exists(bucket1_data_path))
        << "Original bucket data file should still exist";
    EXPECT_TRUE(fs::exists(bucket1_meta_path))
        << "Original bucket metadata file should still exist";
}

//-----------------------------------------------------------------------------

TEST_F(StorageBackendTest,
       BucketStorageBackend_DuplicateBatchPartialRejection) {
    FileStorageConfig config;
    config.storage_filepath = data_path;
    BucketBackendConfig bucket_config;
    BucketStorageBackend storage_backend(config, bucket_config);
    ASSERT_TRUE(storage_backend.Init());

    // Write initial key "keyA"
    std::string keyA = "keyA";
    std::string valueA = "value_for_keyA";
    auto bufA = std::make_unique<char[]>(valueA.size());
    std::memcpy(bufA.get(), valueA.data(), valueA.size());

    std::unordered_map<std::string, std::vector<Slice>> batch1;
    batch1.emplace(keyA, std::vector<Slice>{Slice{bufA.get(), valueA.size()}});

    auto result1 = storage_backend.BatchOffload(
        batch1,
        [](const std::vector<std::string>&,
           std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; });
    ASSERT_TRUE(result1.has_value());

    // Attempt BatchOffload with batch containing ["keyA", "keyB"]
    std::string keyB = "keyB";
    std::string valueA2 = "duplicate_value_A";
    std::string valueB = "value_for_keyB";

    auto bufA2 = std::make_unique<char[]>(valueA2.size());
    auto bufB = std::make_unique<char[]>(valueB.size());
    std::memcpy(bufA2.get(), valueA2.data(), valueA2.size());
    std::memcpy(bufB.get(), valueB.data(), valueB.size());

    std::unordered_map<std::string, std::vector<Slice>> batch2;
    batch2.emplace(keyA,
                   std::vector<Slice>{Slice{bufA2.get(), valueA2.size()}});
    batch2.emplace(keyB, std::vector<Slice>{Slice{bufB.get(), valueB.size()}});

    auto result2 = storage_backend.BatchOffload(
        batch2,
        [](const std::vector<std::string>&,
           std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; });

    // Entire batch should be rejected
    ASSERT_FALSE(result2.has_value());
    EXPECT_EQ(result2.error(), ErrorCode::OBJECT_ALREADY_EXISTS);

    // Verify "keyB" was NOT written (batch is atomic)
    auto is_exist_B = storage_backend.IsExist(keyB);
    ASSERT_TRUE(is_exist_B.has_value());
    EXPECT_FALSE(is_exist_B.value())
        << "keyB should not exist - batch should be atomic";

    // Verify "keyA" still has original value
    auto read_buf = std::make_unique<char[]>(valueA.size());
    std::unordered_map<std::string, Slice> load_slices;
    load_slices.emplace(keyA, Slice{read_buf.get(), valueA.size()});

    auto load_result = storage_backend.BatchLoad(load_slices);
    ASSERT_TRUE(load_result.has_value());

    std::string loaded(read_buf.get(), valueA.size());
    EXPECT_EQ(loaded, valueA) << "keyA should still have original value";
}

//-----------------------------------------------------------------------------
// BucketReadGuard RAII Behavior Tests (Phase 1 - D2)
//-----------------------------------------------------------------------------

TEST_F(StorageBackendTest,
       BucketReadGuard_IncrementsAndDecrementsInflightReads) {
    // Create a BucketMetadata with inflight_reads_ = 0
    auto bucket = std::make_shared<BucketMetadata>();
    EXPECT_EQ(bucket->inflight_reads_.load(), 0);

    // Create a BucketReadGuard wrapping it
    {
        BucketReadGuard guard(bucket);
        EXPECT_EQ(bucket->inflight_reads_.load(), 1)
            << "Guard should increment inflight_reads_";
    }
    // Guard goes out of scope

    EXPECT_EQ(bucket->inflight_reads_.load(), 0)
        << "Guard destructor should decrement inflight_reads_";
}

//-----------------------------------------------------------------------------

TEST_F(StorageBackendTest, BucketReadGuard_MoveSemantics) {
    auto bucket = std::make_shared<BucketMetadata>();
    EXPECT_EQ(bucket->inflight_reads_.load(), 0);

    {
        BucketReadGuard guard1(bucket);
        EXPECT_EQ(bucket->inflight_reads_.load(), 1);

        // Move guard1 to guard2
        BucketReadGuard guard2(std::move(guard1));
        EXPECT_EQ(bucket->inflight_reads_.load(), 1)
            << "Move should not double-increment";

        // guard1 is now empty, guard2 holds the reference
    }
    // Both guards out of scope

    EXPECT_EQ(bucket->inflight_reads_.load(), 0)
        << "After move and destruction, count should be 0";
}

//-----------------------------------------------------------------------------

TEST_F(StorageBackendTest, BucketReadGuard_MultipleGuardsSameBucket) {
    auto bucket = std::make_shared<BucketMetadata>();
    EXPECT_EQ(bucket->inflight_reads_.load(), 0);

    {
        BucketReadGuard guard1(bucket);
        EXPECT_EQ(bucket->inflight_reads_.load(), 1);

        {
            BucketReadGuard guard2(bucket);
            EXPECT_EQ(bucket->inflight_reads_.load(), 2)
                << "Two guards should increment to 2";
        }
        // guard2 destroyed

        EXPECT_EQ(bucket->inflight_reads_.load(), 1)
            << "After guard2 destroyed, count should be 1";
    }
    // guard1 destroyed

    EXPECT_EQ(bucket->inflight_reads_.load(), 0)
        << "After all guards destroyed, count should be 0";
}

//-----------------------------------------------------------------------------
// Concurrent BatchLoad Tests (Lock-Free IO)
//-----------------------------------------------------------------------------

TEST_F(StorageBackendTest, BucketStorageBackend_ConcurrentReadsNoBlocking) {
    FileStorageConfig config;
    config.storage_filepath = data_path;
    BucketBackendConfig bucket_config;
    BucketStorageBackend storage_backend(config, bucket_config);
    ASSERT_TRUE(storage_backend.Init());

    // Write several keys across multiple buckets
    const int num_buckets = 3;
    const int keys_per_bucket = 5;
    std::unordered_map<std::string, std::string> test_data;

    for (int b = 0; b < num_buckets; ++b) {
        std::unordered_map<std::string, std::vector<Slice>> batch;
        std::vector<std::unique_ptr<char[]>> buffers;

        for (int k = 0; k < keys_per_bucket; ++k) {
            std::string key =
                "bucket" + std::to_string(b) + "_key" + std::to_string(k);
            std::string value = "value_for_" + key + "_data";
            test_data[key] = value;

            auto buf = std::make_unique<char[]>(value.size());
            std::memcpy(buf.get(), value.data(), value.size());
            batch.emplace(key,
                          std::vector<Slice>{Slice{buf.get(), value.size()}});
            buffers.push_back(std::move(buf));
        }

        auto result = storage_backend.BatchOffload(
            batch,
            [](const std::vector<std::string>&,
               std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; });
        ASSERT_TRUE(result.has_value());
    }

    // Launch N reader threads
    const int num_threads = 4;
    const int reads_per_thread = 10;
    std::atomic<int> successful_reads{0};
    std::atomic<bool> any_failure{false};
    std::vector<std::thread> threads;

    auto start_time = std::chrono::steady_clock::now();

    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            for (int r = 0; r < reads_per_thread; ++r) {
                // Read a subset of keys
                std::unordered_map<std::string, Slice> load_slices;
                std::vector<std::unique_ptr<char[]>> read_buffers;

                for (int k = 0; k < keys_per_bucket; ++k) {
                    int bucket_idx = (t + r + k) % num_buckets;
                    std::string key = "bucket" + std::to_string(bucket_idx) +
                                      "_key" + std::to_string(k);
                    size_t size = test_data[key].size();

                    auto buf = std::make_unique<char[]>(size);
                    load_slices.emplace(key, Slice{buf.get(), size});
                    read_buffers.push_back(std::move(buf));
                }

                auto load_result = storage_backend.BatchLoad(load_slices);
                if (load_result.has_value()) {
                    successful_reads.fetch_add(1);
                } else {
                    any_failure.store(true);
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    auto end_time = std::chrono::steady_clock::now();
    auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                           end_time - start_time)
                           .count();

    LOG(INFO) << "Concurrent reads test: " << successful_reads.load()
              << " successful reads in " << duration_ms << "ms";

    EXPECT_FALSE(any_failure.load()) << "All reads should succeed";
    EXPECT_EQ(successful_reads.load(), num_threads * reads_per_thread)
        << "All reads should complete";
}

//-----------------------------------------------------------------------------

TEST_F(StorageBackendTest, BucketStorageBackend_BatchLoadWithMixedBuckets) {
    FileStorageConfig config;
    config.storage_filepath = data_path;
    BucketBackendConfig bucket_config;
    BucketStorageBackend storage_backend(config, bucket_config);
    ASSERT_TRUE(storage_backend.Init());

    // Write keys to bucket A
    std::string keyA1 = "bucketA_key1";
    std::string keyA2 = "bucketA_key2";
    std::string valueA1 = "value_A1_data";
    std::string valueA2 = "value_A2_data";

    {
        std::unordered_map<std::string, std::vector<Slice>> batchA;
        auto bufA1 = std::make_unique<char[]>(valueA1.size());
        auto bufA2 = std::make_unique<char[]>(valueA2.size());
        std::memcpy(bufA1.get(), valueA1.data(), valueA1.size());
        std::memcpy(bufA2.get(), valueA2.data(), valueA2.size());

        batchA.emplace(keyA1,
                       std::vector<Slice>{Slice{bufA1.get(), valueA1.size()}});
        batchA.emplace(keyA2,
                       std::vector<Slice>{Slice{bufA2.get(), valueA2.size()}});

        auto result = storage_backend.BatchOffload(
            batchA,
            [](const std::vector<std::string>&,
               std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; });
        ASSERT_TRUE(result.has_value());
    }

    // Write keys to bucket B
    std::string keyB1 = "bucketB_key1";
    std::string keyB2 = "bucketB_key2";
    std::string valueB1 = "value_B1_data";
    std::string valueB2 = "value_B2_data";

    {
        std::unordered_map<std::string, std::vector<Slice>> batchB;
        auto bufB1 = std::make_unique<char[]>(valueB1.size());
        auto bufB2 = std::make_unique<char[]>(valueB2.size());
        std::memcpy(bufB1.get(), valueB1.data(), valueB1.size());
        std::memcpy(bufB2.get(), valueB2.data(), valueB2.size());

        batchB.emplace(keyB1,
                       std::vector<Slice>{Slice{bufB1.get(), valueB1.size()}});
        batchB.emplace(keyB2,
                       std::vector<Slice>{Slice{bufB2.get(), valueB2.size()}});

        auto result = storage_backend.BatchOffload(
            batchB,
            [](const std::vector<std::string>&,
               std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; });
        ASSERT_TRUE(result.has_value());
    }

    // BatchLoad keys from both buckets in single call
    auto readA1 = std::make_unique<char[]>(valueA1.size());
    auto readA2 = std::make_unique<char[]>(valueA2.size());
    auto readB1 = std::make_unique<char[]>(valueB1.size());
    auto readB2 = std::make_unique<char[]>(valueB2.size());

    std::unordered_map<std::string, Slice> load_slices;
    load_slices.emplace(keyA1, Slice{readA1.get(), valueA1.size()});
    load_slices.emplace(keyA2, Slice{readA2.get(), valueA2.size()});
    load_slices.emplace(keyB1, Slice{readB1.get(), valueB1.size()});
    load_slices.emplace(keyB2, Slice{readB2.get(), valueB2.size()});

    auto load_result = storage_backend.BatchLoad(load_slices);
    ASSERT_TRUE(load_result.has_value()) << "Mixed bucket load should succeed";

    // Verify all data read correctly
    EXPECT_EQ(std::string(readA1.get(), valueA1.size()), valueA1);
    EXPECT_EQ(std::string(readA2.get(), valueA2.size()), valueA2);
    EXPECT_EQ(std::string(readB1.get(), valueB1.size()), valueB1);
    EXPECT_EQ(std::string(readB2.get(), valueB2.size()), valueB2);
}

//-----------------------------------------------------------------------------
// DeleteBucket Tests
//-----------------------------------------------------------------------------

TEST_F(StorageBackendTest,
       BucketStorageBackend_DeleteBucketRemovesKeysAndFiles) {
    FileStorageConfig config;
    config.storage_filepath = data_path;
    BucketBackendConfig bucket_config;
    BucketStorageBackend storage_backend(config, bucket_config);
    ASSERT_TRUE(storage_backend.Init());

    // Write keys to bucket
    std::string k1 = "delete_test_k1";
    std::string k2 = "delete_test_k2";
    std::string k3 = "delete_test_k3";
    std::string v1 = "value1";
    std::string v2 = "value2";
    std::string v3 = "value3";

    std::unordered_map<std::string, std::vector<Slice>> batch;
    auto buf1 = std::make_unique<char[]>(v1.size());
    auto buf2 = std::make_unique<char[]>(v2.size());
    auto buf3 = std::make_unique<char[]>(v3.size());
    std::memcpy(buf1.get(), v1.data(), v1.size());
    std::memcpy(buf2.get(), v2.data(), v2.size());
    std::memcpy(buf3.get(), v3.data(), v3.size());

    batch.emplace(k1, std::vector<Slice>{Slice{buf1.get(), v1.size()}});
    batch.emplace(k2, std::vector<Slice>{Slice{buf2.get(), v2.size()}});
    batch.emplace(k3, std::vector<Slice>{Slice{buf3.get(), v3.size()}});

    auto offload_result = storage_backend.BatchOffload(
        batch,
        [](const std::vector<std::string>&,
           std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; });
    ASSERT_TRUE(offload_result.has_value());
    int64_t bucket_id = offload_result.value();

    // Verify keys exist before deletion
    EXPECT_TRUE(storage_backend.IsExist(k1).value());
    EXPECT_TRUE(storage_backend.IsExist(k2).value());
    EXPECT_TRUE(storage_backend.IsExist(k3).value());

    // Verify files exist
    std::string bucket_data_path =
        data_path + "/" + std::to_string(bucket_id) + ".bucket";
    std::string bucket_meta_path =
        data_path + "/" + std::to_string(bucket_id) + ".meta";
    EXPECT_TRUE(fs::exists(bucket_data_path));
    EXPECT_TRUE(fs::exists(bucket_meta_path));

    // Delete the bucket
    auto delete_result = storage_backend.DeleteBucket(bucket_id);
    ASSERT_TRUE(delete_result.has_value()) << "DeleteBucket should succeed";

    // Verify keys no longer exist
    EXPECT_FALSE(storage_backend.IsExist(k1).value());
    EXPECT_FALSE(storage_backend.IsExist(k2).value());
    EXPECT_FALSE(storage_backend.IsExist(k3).value());

    // Verify files are deleted
    EXPECT_FALSE(fs::exists(bucket_data_path))
        << "Bucket data file should be deleted";
    EXPECT_FALSE(fs::exists(bucket_meta_path))
        << "Bucket metadata file should be deleted";
}

//-----------------------------------------------------------------------------

TEST_F(StorageBackendTest, BucketStorageBackend_DeleteBucketNotFound) {
    FileStorageConfig config;
    config.storage_filepath = data_path;
    BucketBackendConfig bucket_config;
    BucketStorageBackend storage_backend(config, bucket_config);
    ASSERT_TRUE(storage_backend.Init());

    // Call DeleteBucket on non-existent bucket
    auto delete_result = storage_backend.DeleteBucket(999999);
    ASSERT_FALSE(delete_result.has_value());
    EXPECT_EQ(delete_result.error(), ErrorCode::BUCKET_NOT_FOUND);
}

//-----------------------------------------------------------------------------

TEST_F(StorageBackendTest,
       BucketStorageBackend_DeleteBucketWaitsForInflightReads) {
    FileStorageConfig config;
    config.storage_filepath = data_path;
    BucketBackendConfig bucket_config;
    BucketStorageBackend storage_backend(config, bucket_config);
    ASSERT_TRUE(storage_backend.Init());

    // Write keys to bucket
    std::string key = "inflight_test_key";
    std::string value = "inflight_test_value_data";
    auto buf = std::make_unique<char[]>(value.size());
    std::memcpy(buf.get(), value.data(), value.size());

    std::unordered_map<std::string, std::vector<Slice>> batch;
    batch.emplace(key, std::vector<Slice>{Slice{buf.get(), value.size()}});

    auto offload_result = storage_backend.BatchOffload(
        batch,
        [](const std::vector<std::string>&,
           std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; });
    ASSERT_TRUE(offload_result.has_value());
    int64_t bucket_id = offload_result.value();

    std::atomic<bool> read_started{false};
    std::atomic<bool> read_completed{false};
    std::atomic<bool> delete_started{false};
    std::atomic<bool> delete_completed{false};
    std::atomic<bool> read_success{false};
    std::string read_data;
    std::mutex read_data_mutex;

    // Reader thread: begins BatchLoad, sleeps, then completes
    std::thread reader_thread([&]() {
        auto read_buf = std::make_unique<char[]>(value.size());
        std::unordered_map<std::string, Slice> load_slices;
        load_slices.emplace(key, Slice{read_buf.get(), value.size()});

        read_started.store(true);

        // Simulate slow IO by sleeping after acquiring guard
        // Note: The guard is acquired inside BatchLoad, so we can't directly
        // control timing. Instead, we verify behavior through ordering.
        auto load_result = storage_backend.BatchLoad(load_slices);

        if (load_result.has_value()) {
            read_success.store(true);
            std::lock_guard<std::mutex> lock(read_data_mutex);
            read_data = std::string(read_buf.get(), value.size());
        }

        read_completed.store(true);
    });

    // Wait for reader to start
    while (!read_started.load()) {
        std::this_thread::yield();
    }

    // Small delay to ensure reader is inside BatchLoad
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    // Deleter thread: calls DeleteBucket
    std::thread deleter_thread([&]() {
        delete_started.store(true);
        auto delete_result = storage_backend.DeleteBucket(bucket_id);
        delete_completed.store(true);
    });

    reader_thread.join();
    deleter_thread.join();

    // Verify reader got correct data
    EXPECT_TRUE(read_success.load()) << "Reader should have succeeded";
    {
        std::lock_guard<std::mutex> lock(read_data_mutex);
        EXPECT_EQ(read_data, value) << "Reader should have gotten correct data";
    }

    // Verify both completed
    EXPECT_TRUE(read_completed.load());
    EXPECT_TRUE(delete_completed.load());

    // Verify bucket files are deleted after both complete
    std::string bucket_data_path =
        data_path + "/" + std::to_string(bucket_id) + ".bucket";
    std::string bucket_meta_path =
        data_path + "/" + std::to_string(bucket_id) + ".meta";
    EXPECT_FALSE(fs::exists(bucket_data_path));
    EXPECT_FALSE(fs::exists(bucket_meta_path));
}

//-----------------------------------------------------------------------------

TEST_F(StorageBackendTest,
       BucketStorageBackend_DeleteBucketConcurrentReadersComplete) {
    FileStorageConfig config;
    config.storage_filepath = data_path;
    BucketBackendConfig bucket_config;
    BucketStorageBackend storage_backend(config, bucket_config);
    ASSERT_TRUE(storage_backend.Init());

    // Write keys to bucket
    std::string key = "concurrent_delete_key";
    std::string value = "concurrent_delete_value_data";
    auto buf = std::make_unique<char[]>(value.size());
    std::memcpy(buf.get(), value.data(), value.size());

    std::unordered_map<std::string, std::vector<Slice>> batch;
    batch.emplace(key, std::vector<Slice>{Slice{buf.get(), value.size()}});

    auto offload_result = storage_backend.BatchOffload(
        batch,
        [](const std::vector<std::string>&,
           std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; });
    ASSERT_TRUE(offload_result.has_value());
    int64_t bucket_id = offload_result.value();

    const int num_readers = 4;
    std::atomic<int> readers_started{0};
    std::atomic<int> readers_completed{0};
    std::atomic<int> successful_reads{0};
    std::vector<std::thread> reader_threads;

    // Start reader threads with staggered start times
    for (int i = 0; i < num_readers; ++i) {
        reader_threads.emplace_back([&, i]() {
            // Stagger start
            std::this_thread::sleep_for(std::chrono::milliseconds(i * 5));

            readers_started.fetch_add(1);

            auto read_buf = std::make_unique<char[]>(value.size());
            std::unordered_map<std::string, Slice> load_slices;
            load_slices.emplace(key, Slice{read_buf.get(), value.size()});

            auto load_result = storage_backend.BatchLoad(load_slices);
            if (load_result.has_value()) {
                std::string loaded(read_buf.get(), value.size());
                if (loaded == value) {
                    successful_reads.fetch_add(1);
                }
            }
            // If load fails (key not found after delete), that's also
            // acceptable

            readers_completed.fetch_add(1);
        });
    }

    // Wait for all readers to start
    while (readers_started.load() < num_readers) {
        std::this_thread::yield();
    }

    // Small delay to ensure readers are in progress
    std::this_thread::sleep_for(std::chrono::milliseconds(5));

    // Start deleter
    std::thread deleter_thread([&]() {
        auto delete_result = storage_backend.DeleteBucket(bucket_id);
        // Delete should succeed eventually
        EXPECT_TRUE(delete_result.has_value() ||
                    delete_result.error() == ErrorCode::BUCKET_NOT_FOUND);
    });

    // Join all threads
    for (auto& t : reader_threads) {
        t.join();
    }
    deleter_thread.join();

    LOG(INFO) << "Concurrent delete test: " << successful_reads.load() << "/"
              << num_readers << " readers got data before delete";

    // All readers should have completed
    EXPECT_EQ(readers_completed.load(), num_readers);

    // Bucket should be deleted
    std::string bucket_data_path =
        data_path + "/" + std::to_string(bucket_id) + ".bucket";
    EXPECT_FALSE(fs::exists(bucket_data_path));
}

//-----------------------------------------------------------------------------
// Integration / Stress Tests
//-----------------------------------------------------------------------------

TEST_F(StorageBackendTest, BucketStorageBackend_ConcurrentReadWriteDelete) {
    FileStorageConfig config;
    config.storage_filepath = data_path;
    BucketBackendConfig bucket_config;
    BucketStorageBackend storage_backend(config, bucket_config);
    ASSERT_TRUE(storage_backend.Init());

    std::atomic<bool> stop{false};
    std::atomic<int64_t> write_count{0};
    std::atomic<int64_t> read_count{0};
    std::atomic<int64_t> delete_count{0};
    std::atomic<bool> corruption_detected{false};

    // Track created buckets
    std::mutex buckets_mutex;
    std::vector<int64_t> created_buckets;
    std::unordered_map<std::string, std::string> key_values;

    // Writer thread: creates new buckets with unique keys
    std::thread writer_thread([&]() {
        int counter = 0;
        while (!stop.load()) {
            std::string key = "stress_key_" + std::to_string(counter++);
            std::string value =
                "stress_value_" + std::to_string(counter) + "_data";

            auto buf = std::make_unique<char[]>(value.size());
            std::memcpy(buf.get(), value.data(), value.size());

            std::unordered_map<std::string, std::vector<Slice>> batch;
            batch.emplace(key,
                          std::vector<Slice>{Slice{buf.get(), value.size()}});

            auto result = storage_backend.BatchOffload(
                batch, [](const std::vector<std::string>&,
                          std::vector<StorageObjectMetadata>&) {
                    return ErrorCode::OK;
                });

            if (result.has_value()) {
                write_count.fetch_add(1);
                std::lock_guard<std::mutex> lock(buckets_mutex);
                created_buckets.push_back(result.value());
                key_values[key] = value;
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    });

    // Reader thread: reads random existing keys
    std::thread reader_thread([&]() {
        while (!stop.load()) {
            std::string key_to_read;
            std::string expected_value;

            {
                std::lock_guard<std::mutex> lock(buckets_mutex);
                if (key_values.empty()) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(5));
                    continue;
                }
                // Pick a random key
                auto it = key_values.begin();
                std::advance(it, rand() % key_values.size());
                key_to_read = it->first;
                expected_value = it->second;
            }

            auto read_buf = std::make_unique<char[]>(expected_value.size());
            std::unordered_map<std::string, Slice> load_slices;
            load_slices.emplace(key_to_read,
                                Slice{read_buf.get(), expected_value.size()});

            auto load_result = storage_backend.BatchLoad(load_slices);
            if (load_result.has_value()) {
                std::string loaded(read_buf.get(), expected_value.size());
                if (loaded != expected_value) {
                    LOG(ERROR) << "Corruption detected: expected '"
                               << expected_value << "' got '" << loaded << "'";
                    corruption_detected.store(true);
                }
                read_count.fetch_add(1);
            }
            // INVALID_KEY is acceptable if key was deleted

            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    });

    // Deleter thread: deletes oldest buckets
    std::thread deleter_thread([&]() {
        while (!stop.load()) {
            int64_t bucket_to_delete = -1;

            {
                std::lock_guard<std::mutex> lock(buckets_mutex);
                if (created_buckets.size() > 5) {
                    bucket_to_delete = created_buckets.front();
                    created_buckets.erase(created_buckets.begin());
                }
            }

            if (bucket_to_delete >= 0) {
                // Get the keys in this bucket before deleting
                std::vector<std::string> bucket_keys;
                storage_backend.GetBucketKeys(bucket_to_delete, bucket_keys);

                auto delete_result =
                    storage_backend.DeleteBucket(bucket_to_delete);
                if (delete_result.has_value()) {
                    delete_count.fetch_add(1);
                    // Remove deleted keys from key_values so the reader
                    // doesn't try to read keys that no longer exist
                    std::lock_guard<std::mutex> lock(buckets_mutex);
                    for (const auto& k : bucket_keys) {
                        key_values.erase(k);
                    }
                }
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
    });

    // Run for 2 seconds
    std::this_thread::sleep_for(std::chrono::seconds(2));
    stop.store(true);

    writer_thread.join();
    reader_thread.join();
    deleter_thread.join();

    LOG(INFO) << "Stress test completed: writes=" << write_count.load()
              << ", reads=" << read_count.load()
              << ", deletes=" << delete_count.load();

    EXPECT_FALSE(corruption_detected.load())
        << "No data corruption should occur";
    EXPECT_GT(write_count.load(), 0) << "Should have some successful writes";
    EXPECT_GT(read_count.load(), 0) << "Should have some successful reads";
}

//-----------------------------------------------------------------------------

}  // namespace mooncake::test

#include "storage_backend.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <ylt/util/tl/expected.hpp>

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
        fs::remove_all(data_path);
        fs::create_directories(data_path);
    }

    void TearDown() override {
        google::ShutdownGoogleLogging();
        LOG(INFO) << "Clear test data...";
        fs::remove_all(data_path);
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
    ASSERT_TRUE(mooncake::BatchOffloadUtil(storage_backend, keys, sizes,
                                           test_data, buckets));

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
    ASSERT_TRUE(mooncake::BatchOffloadUtil(storage_backend, keys, sizes,
                                           batch_data, buckets));
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

}  // namespace mooncake::test

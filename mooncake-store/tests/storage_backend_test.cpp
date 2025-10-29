#include <glog/logging.h>
#include <gtest/gtest.h>
#include "storage_backend.h"
#include "allocator.h"
#include "utils.h"
#include <ylt/struct_pb.hpp>
#include <filesystem>
#include <iostream>
#include <ranges>

namespace fs = std::filesystem;
namespace mooncake::test {
class StorageBackendTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("StorageBackendTest");
        FLAGS_logtostderr = true;
    }

    void TearDown() override {
        google::ShutdownGoogleLogging();
        std::string data_path =
            std::filesystem::current_path().string() + "/data";
        LOG(INFO) << "Clear test data...";
        for (const auto& entry : fs::directory_iterator(data_path)) {
            if (entry.is_regular_file()) {
                fs::remove(entry.path());
            }
        }
    }
};

tl::expected<void, ErrorCode> BatchOffload(
    std::vector<std::string>& keys,
    std::unordered_map<std::string, std::string>& batch_data,
    std::shared_ptr<SimpleAllocator>& client_buffer_allocator,
    BucketStorageBackend& storage_backend, std::vector<int64_t>& buckets) {
    size_t bucket_sz = 10;
    size_t batch_sz = 10;
    size_t data_sz = 10;
    for (size_t i = 0; i < bucket_sz; i++) {
        std::unordered_map<std::string, std::vector<Slice>> batched_slices;
        for (size_t j = 0; j < batch_sz; j++) {
            std::string key =
                "test_key_i_" + std::to_string(i) + "_j_" + std::to_string(j);
            size_t data_size = 0;
            std::string all_data;
            std::vector<Slice> slices;
            for (size_t k = 0; k < data_sz; k++) {
                std::string data = "test_data_i_" + std::to_string(i) + "_j_" +
                                   std::to_string(j) + "_k_" +
                                   std::to_string(k);
                all_data += data;
                data_size += data.size();
                void* buffer = client_buffer_allocator->allocate(data.size());
                memcpy(buffer, data.data(), data.size());
                slices.emplace_back(Slice{buffer, data.size()});
            }
            batched_slices.emplace(key, slices);
            batch_data.emplace(key, all_data);
            keys.emplace_back(key);
        }
        auto batch_store_object_one_result = storage_backend.BatchOffload(
            batched_slices,
            [&](const std::unordered_map<std::string, BucketObjectMetadata>&
                    keys) {
                if (keys.size() != batched_slices.size()) {
                    return ErrorCode::INVALID_KEY;
                }
                for (const auto& key : keys) {
                    if (batched_slices.find(key.first) ==
                        batched_slices.end()) {
                        return ErrorCode::INVALID_KEY;
                    }
                }
                return ErrorCode::OK;
            });
        if (!batch_store_object_one_result) {
            return tl::make_unexpected(batch_store_object_one_result.error());
        }
        buckets.emplace_back(batch_store_object_one_result.value());
    }
    return {};
}

TEST_F(StorageBackendTest, StorageBackendAll) {
    std::string data_path = std::filesystem::current_path().string() + "/data";
    fs::create_directories(data_path);
    std::shared_ptr<SimpleAllocator> client_buffer_allocator =
        std::make_shared<SimpleAllocator>(128 * 1024 * 1024);
    BucketStorageBackend storage_backend(data_path);
    for (const auto& entry : fs::directory_iterator(data_path)) {
        if (entry.is_regular_file()) {
            fs::remove(entry.path());
        }
    }
    ASSERT_TRUE(storage_backend.Init());
    ASSERT_TRUE(fs::directory_iterator(data_path) == fs::directory_iterator{});
    ASSERT_TRUE(!storage_backend.Init());
    std::unordered_map<std::string, std::string> test_data;
    std::vector<std::string> keys;
    std::vector<int64_t> buckets;
    auto test_batch_store_object_result = BatchOffload(
        keys, test_data, client_buffer_allocator, storage_backend, buckets);
    ASSERT_TRUE(test_batch_store_object_result);

    std::unordered_map<std::string, BucketObjectMetadata>
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
    std::string data_path = std::filesystem::current_path().string() + "/data";
    fs::create_directories(data_path);
    std::shared_ptr<SimpleAllocator> client_buffer_allocator =
        std::make_shared<SimpleAllocator>(128 * 1024 * 1024);
    BucketStorageBackend storage_backend(data_path);
    for (const auto& entry : fs::directory_iterator(data_path)) {
        if (entry.is_regular_file()) {
            fs::remove(entry.path());
        }
    }
    ASSERT_TRUE(storage_backend.Init());
    ASSERT_TRUE(!storage_backend.Init());
    std::unordered_map<std::string, std::string> test_data;
    std::vector<std::string> keys;
    std::vector<int64_t> buckets;
    auto test_batch_store_object_result = BatchOffload(
        keys, test_data, client_buffer_allocator, storage_backend, buckets);
    ASSERT_TRUE(test_batch_store_object_result);
    std::unordered_map<std::string, BucketObjectMetadata> objects;
    std::vector<int64_t> scan_buckets;
    auto res = storage_backend.BucketScan(0, objects, scan_buckets, 10);
    ASSERT_TRUE(res);
    ASSERT_EQ(res.value(), buckets.at(1));
    for (const auto& object : objects) {
        ASSERT_EQ(object.second.data_size, test_data.at(object.first).size());
        ASSERT_EQ(object.second.key_size, object.first.size());
    }
    ASSERT_EQ(scan_buckets.size(), 1);
    ASSERT_EQ(scan_buckets.at(0), buckets.at(0));
    objects.clear();
    scan_buckets.clear();
    res = storage_backend.BucketScan(0, objects, scan_buckets, 45);
    ASSERT_TRUE(res);
    ASSERT_EQ(res.value(), buckets.at(4));
    ASSERT_EQ(scan_buckets.size(), 4);
    for (int i = 0; i < 4; i++) {
        ASSERT_EQ(scan_buckets.at(i), buckets.at(i));
    }

    objects.clear();
    scan_buckets.clear();
    res = storage_backend.BucketScan(buckets.at(4), objects, scan_buckets, 45);
    ASSERT_TRUE(res);
    ASSERT_EQ(res.value(), buckets.at(8));
    ASSERT_EQ(scan_buckets.size(), 4);
    for (int i = 0; i < 4; i++) {
        ASSERT_EQ(scan_buckets.at(i), buckets.at(i + 4));
    }

    objects.clear();
    scan_buckets.clear();
    res = storage_backend.BucketScan(buckets.at(9), objects, scan_buckets, 45);
    ASSERT_TRUE(res);
    ASSERT_EQ(res.value(), 0);
    ASSERT_EQ(scan_buckets.size(), 1);
    ASSERT_EQ(scan_buckets.at(0), buckets.at(9));

    objects.clear();
    scan_buckets.clear();
    res = storage_backend.BucketScan(buckets.at(9) + 10, objects, scan_buckets,
                                     45);
    ASSERT_TRUE(res);
    ASSERT_EQ(res.value(), 0);
    ASSERT_EQ(scan_buckets.size(), 0);

    objects.clear();
    scan_buckets.clear();
    res = storage_backend.BucketScan(0, objects, scan_buckets, 8);
    ASSERT_TRUE(!res);
    ASSERT_EQ(res.error(), ErrorCode::KEYS_ULTRA_BUCKET_LIMIT);
    ASSERT_EQ(scan_buckets.size(), 0);
    ASSERT_EQ(objects.size(), 0);
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

    EXPECT_EQ(gen.NextId(), 11);  // 返回 10 + 1 = 11
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
            int64_t id = gen.NextId();  // 返回新值
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

    // 检查唯一性
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
        EXPECT_EQ(id, last_id + 1);
        last_id = id;
    }

    EXPECT_GE(last_id, 101000);  // 至少增长了 10 万
}

}  // namespace mooncake::test
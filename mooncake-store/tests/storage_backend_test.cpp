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
        std::string data_path = std::filesystem::current_path().string()+"/data";
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
    SequentialStorageBackend& storage_backend, std::vector<int64_t>& buckets) {
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
            [&](const std::unordered_map<std::string, SequentialObjectMetadata>&
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
    std::string data_path = std::filesystem::current_path().string()+"/data";
    fs::create_directories(data_path);
    std::shared_ptr<SimpleAllocator> client_buffer_allocator =
        std::make_shared<SimpleAllocator>(128 * 1024 * 1024);
    SequentialStorageBackend storage_backend(data_path);
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

    std::unordered_map<std::string, SequentialObjectMetadata>
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
    }
}
TEST_F(StorageBackendTest, BucketScan) {
    std::string data_path = std::filesystem::current_path().string()+"/data";
    fs::create_directories(data_path);
    std::shared_ptr<SimpleAllocator> client_buffer_allocator =
        std::make_shared<SimpleAllocator>(128 * 1024 * 1024);
    SequentialStorageBackend storage_backend(data_path);
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
    std::unordered_map<std::string, SequentialObjectMetadata> objects;
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
}  // namespace mooncake::test
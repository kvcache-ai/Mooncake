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
    std::unordered_map<std::string, std::string> test_data;
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
                  test_data.at(scan_keys[i]).size());
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

// 追加到 storage_backend_test.cpp 末尾即可

// 1. 基础存取：Slice 写入 + string / Slice 读取
TEST_F(StorageBackendTest, StorageBackend_StoreAndLoadWithSlicesAndString) {
    // 为了不影响其它用例，在 data_path 下再建一个独立子目录
    fs::path test_root = fs::path(data_path) / "sb_unit_test_store_load";
    fs::remove_all(test_root);
    fs::create_directories(test_root);

    // 关闭 eviction，避免依赖 Init / 磁盘配额逻辑
    StorageBackend backend(test_root.string(), /*fsdir=*/"", /*enable_eviction=*/false);

    const std::string filename = "object1.bin";
    fs::path full_path = test_root / filename;
    const std::string payload = "hello StorageBackend, this is a test payload";

    // --- 使用 Slice 写入 ---
    size_t half = static_cast<int64_t>(payload.size() / 2);
    Slice write_s1{const_cast<char*>(payload.data()), half};
    Slice write_s2{const_cast<char*>(payload.data()) + half,
                   static_cast<int64_t>(payload.size()) - half};
    std::vector<Slice> write_slices{write_s1, write_s2};

    auto store_result = backend.StoreObject(full_path.string(), write_slices);
    ASSERT_TRUE(store_result) << "StoreObject with slices should succeed";
    ASSERT_TRUE(fs::exists(full_path));

    // --- 先用 string 读回 ---
    std::string loaded_str;
    auto load_result_str =
        backend.LoadObject(full_path.string(), loaded_str,
                           static_cast<int64_t>(payload.size()));
    ASSERT_TRUE(load_result_str) << "LoadObject(string) should succeed";
    EXPECT_EQ(loaded_str, payload);

    // --- 再用 Slice 读回到 buffer ---
    std::string buf(payload.size(), '\0');
    Slice read_s1{buf.data(), half};
    Slice read_s2{buf.data() + half,
                  static_cast<int64_t>(payload.size()) - half};
    std::vector<Slice> read_slices{read_s1, read_s2};

    auto load_result_slices =
        backend.LoadObject(full_path.string(), read_slices,
                           static_cast<int64_t>(payload.size()));
    ASSERT_TRUE(load_result_slices) << "LoadObject(slices) should succeed";
    EXPECT_EQ(buf, payload);

    // 清理，避免影响其他用例
    fs::remove_all(test_root);
}

// 2. RemoveByRegex：只删除匹配到的文件
TEST_F(StorageBackendTest, StorageBackend_RemoveByRegex_RemovesOnlyMatchedFiles) {
    fs::path test_root = fs::path(data_path) / "sb_unit_test_regex";
    fs::remove_all(test_root);
    fs::create_directories(test_root);

    // 同样关闭 eviction，只测纯文件操作
    StorageBackend backend(test_root.string(), /*fsdir=*/"", /*enable_eviction=*/false);

    const std::string payload = "dummy data";

    fs::path keep_file   = test_root / "keep.bin";
    fs::path remove_f1   = test_root / "tmp_1.bin";
    fs::path remove_f2   = test_root / "tmp_2.bin";

    ASSERT_TRUE(backend.StoreObject(keep_file.string(), payload));
    ASSERT_TRUE(backend.StoreObject(remove_f1.string(), payload));
    ASSERT_TRUE(backend.StoreObject(remove_f2.string(), payload));

    ASSERT_TRUE(fs::exists(keep_file));
    ASSERT_TRUE(fs::exists(remove_f1));
    ASSERT_TRUE(fs::exists(remove_f2));

    // 删除文件名形如 tmp_*.bin 的文件
    backend.RemoveByRegex("tmp_.*\\.bin");

    // 预期：keep.bin 保留，其它两个被删掉
    EXPECT_TRUE(fs::exists(keep_file));
    EXPECT_FALSE(fs::exists(remove_f1));
    EXPECT_FALSE(fs::exists(remove_f2));

    fs::remove_all(test_root);
}

#include "storage_backend.h"

#include <gtest/gtest.h>
#include <glog/logging.h>

#include <filesystem>
#include <unordered_map>
#include <vector>
#include <string>
#include <cstring>
#include <memory>

namespace fs = std::filesystem;

namespace mooncake::test {

class StorageBackendAdaptorTest : public ::testing::Test {
   protected:
    FileStorageConfig config_;
    std::string root_dir_;

    void SetUp() override {
        google::InitGoogleLogging("StorageBackendAdaptorTest");
        FLAGS_logtostderr = true;

        // 使用单独的测试目录，避免影响其他测试
        root_dir_ = (fs::current_path() / "adaptor_data").string();
        fs::remove_all(root_dir_);
        fs::create_directories(root_dir_);

        // 填充 FileStorageConfig
        config_.storage_filepath = root_dir_;
        config_.fsdir = "file_per_key_dir";  // 任意子目录名即可，与 Adaptor 内部逻辑一致
        config_.enable_eviction = false;
        // 其余字段使用默认值即可
    }

    void TearDown() override {
        google::ShutdownGoogleLogging();
        fs::remove_all(root_dir_);
    }
};

// 帮助函数：构造 BatchOffload 需要的结构
static void PrepareBatch(
    const std::unordered_map<std::string, std::string>& src,
    std::unordered_map<std::string, std::vector<Slice>>& batch,
    std::vector<std::vector<char>>& buffers) {
    buffers.clear();
    batch.clear();
    buffers.reserve(src.size());

    for (const auto& [k, v] : src) {
        buffers.emplace_back(v.begin(), v.end());
        Slice s{buffers.back().data(),
                static_cast<size_t>(buffers.back().size())};
        batch[k].push_back(s);
    }
}

// 1. 基本读写 + IsExist 测试
TEST_F(StorageBackendAdaptorTest, BatchOffloadAndLoadRoundTrip) {
    StorageBackendAdaptor adaptor(config_);

    auto init_res = adaptor.Init();
    ASSERT_TRUE(init_res);

    // 准备测试数据
    std::unordered_map<std::string, std::string> src{
        {"key1", "value_1"},
        {"key2", "another_value_2"},
        {"key3", "中文值_3"},
    };

    // ------------- BatchOffload -------------
    std::unordered_map<std::string, std::vector<Slice>> offload_batch;
    std::vector<std::vector<char>> offload_buffers;
    PrepareBatch(src, offload_batch, offload_buffers);

    std::vector<std::string> cb_keys;
    std::vector<StorageObjectMetadata> cb_metas;

    auto offload_res =
        adaptor.BatchOffload(offload_batch,
                             [&](const std::vector<std::string>& keys,
                                 std::vector<StorageObjectMetadata>& metas) {
                                 cb_keys.insert(cb_keys.end(), keys.begin(),
                                                keys.end());
                                 cb_metas.insert(cb_metas.end(), metas.begin(),
                                                 metas.end());
                                 return ErrorCode::OK;
                             });

    ASSERT_TRUE(offload_res);
    ASSERT_EQ(cb_keys.size(), src.size());
    ASSERT_EQ(cb_metas.size(), src.size());

    // 校验 meta 中的 key_size / data_size
    for (size_t i = 0; i < cb_keys.size(); ++i) {
        const auto& k = cb_keys[i];
        const auto& meta = cb_metas[i];
        ASSERT_EQ(meta.key_size, static_cast<int64_t>(k.size()));
        ASSERT_EQ(meta.data_size,
                  static_cast<int64_t>(src.at(k).size()));
    }

    // ------------- IsExist -------------
    for (const auto& [k, v] : src) {
        auto exists = adaptor.IsExist(k);
        ASSERT_TRUE(exists);
        EXPECT_TRUE(exists.value());
    }

    // ------------- BatchLoad -------------
    std::unordered_map<std::string, Slice> load_batch;
    std::unordered_map<std::string, std::unique_ptr<char[]>> load_buf_holder;

    for (const auto& [k, v] : src) {
        auto buf = std::make_unique<char[]>(v.size());
        Slice s{buf.get(), static_cast<size_t>(v.size())};
        load_batch.emplace(k, s);
        load_buf_holder.emplace(k, std::move(buf));
    }

    auto load_res = adaptor.BatchLoad(load_batch);
    ASSERT_TRUE(load_res);

    // 校验读回内容与写入一致
    for (const auto& [k, v] : src) {
        auto it = load_batch.find(k);
        ASSERT_NE(it, load_batch.end());
        const Slice& s = it->second;
        ASSERT_EQ(s.size, static_cast<int64_t>(v.size()));

        std::string loaded(static_cast<char*>(s.ptr),
                           static_cast<size_t>(s.size));
        EXPECT_EQ(loaded, v);
    }
}

// 2. 空 batch 的错误处理（防止误调用）
TEST_F(StorageBackendAdaptorTest, EmptyBatchOffloadShouldReturnInvalidKey) {
    StorageBackendAdaptor adaptor(config_);
    auto init_res = adaptor.Init();
    ASSERT_TRUE(init_res);

    std::unordered_map<std::string, std::vector<Slice>> empty_batch;

    auto res =
        adaptor.BatchOffload(empty_batch,
                             [](const std::vector<std::string>&,
                                std::vector<StorageObjectMetadata>&) {
                                 return ErrorCode::OK;
                             });

    ASSERT_FALSE(res);
    EXPECT_EQ(res.error(), ErrorCode::INVALID_KEY);
}

// 3. ScanMeta：扫描 meta 并验证 key / data_size
TEST_F(StorageBackendAdaptorTest, ScanMetaCollectsAllKeysAndMetas) {
    StorageBackendAdaptor adaptor(config_);
    auto init_res = adaptor.Init();
    ASSERT_TRUE(init_res);

    // 先写入一些数据
    std::unordered_map<std::string, std::string> src{
        {"scan_key_1", std::string(10, 'a')},
        {"scan_key_2", std::string(5, 'b')},
        {"scan_key_3", std::string(20, 'c')},
    };

    std::unordered_map<std::string, std::vector<Slice>> offload_batch;
    std::vector<std::vector<char>> offload_buffers;
    PrepareBatch(src, offload_batch, offload_buffers);

    auto offload_res =
        adaptor.BatchOffload(offload_batch,
                             [](const std::vector<std::string>&,
                                std::vector<StorageObjectMetadata>&) {
                                 return ErrorCode::OK;
                             });
    ASSERT_TRUE(offload_res);

    // 为了测试 flush 行为，故意设置较小的 bucket_iterator_keys_limit
    FileStorageConfig scan_cfg = config_;
    scan_cfg.bucket_iterator_keys_limit = 2;

    std::vector<std::string> scanned_keys;
    std::vector<StorageObjectMetadata> scanned_metas;

    auto scan_res =
        adaptor.ScanMeta(scan_cfg,
                         [&](const std::vector<std::string>& keys,
                             std::vector<StorageObjectMetadata>& metas) {
                             scanned_keys.insert(scanned_keys.end(),
                                                 keys.begin(), keys.end());
                             scanned_metas.insert(scanned_metas.end(),
                                                  metas.begin(), metas.end());
                             return ErrorCode::OK;
                         });

    ASSERT_TRUE(scan_res);

    // 按 key 做一个 map，方便校验
    ASSERT_EQ(scanned_keys.size(), scanned_metas.size());

    std::unordered_map<std::string, StorageObjectMetadata> meta_map;
    for (size_t i = 0; i < scanned_keys.size(); ++i) {
        meta_map.emplace(scanned_keys[i], scanned_metas[i]);
    }

    // 应当能扫描出我们写进去的所有 key，且 data_size 与写入值一致
    for (const auto& [k, v] : src) {
        auto it = meta_map.find(k);
        ASSERT_NE(it, meta_map.end()) << "meta not found for key: " << k;

        const auto& meta = it->second;
        EXPECT_EQ(meta.key_size, static_cast<int64_t>(k.size()));
        EXPECT_EQ(meta.data_size, static_cast<int64_t>(v.size()));
    }
}

}  // namespace mooncake::test

}  // namespace mooncake::test
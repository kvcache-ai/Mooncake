#include "p2p/client/tiered_cache/tiers/bucket_storage_backend.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <thread>
#include <unistd.h>

namespace mooncake::v1::test {
namespace fs = std::filesystem;

class BucketStorageBackendTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("V1BucketStorageBackendTest");
        FLAGS_logtostderr = true;
    }

    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

    void SetUp() override {
        data_path = (fs::temp_directory_path() /
                     ("mooncake_v1_bucket_test_" + std::to_string(getpid())))
                        .string();
        fs::create_directories(data_path);
    }

    void TearDown() override {
        std::error_code ec;
        fs::remove_all(data_path, ec);
        EXPECT_FALSE(ec) << "Failed to clean up " << data_path << ": "
                         << ec.message();
    }

    std::string data_path;

    bool WaitForPathRemoved(
        const fs::path& path,
        std::chrono::milliseconds timeout = std::chrono::seconds(2)) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (!fs::exists(path)) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return !fs::exists(path);
    }
};

TEST_F(BucketStorageBackendTest, TemporaryBucketFilesAreCleanedDuringInit) {
    BucketStorageConfig config;
    config.storage_filepath = data_path;

    const std::string stale_bucket_tmp = data_path + "/123.bucket.tmp";
    const std::string stale_meta_tmp = data_path + "/123.meta.tmp";

    {
        std::ofstream bucket_tmp(stale_bucket_tmp, std::ios::binary);
        ASSERT_TRUE(bucket_tmp.is_open());
        bucket_tmp << "stale bucket payload";
    }
    {
        std::ofstream meta_tmp(stale_meta_tmp, std::ios::binary);
        ASSERT_TRUE(meta_tmp.is_open());
        meta_tmp << "stale metadata payload";
    }

    ASSERT_TRUE(fs::exists(stale_bucket_tmp));
    ASSERT_TRUE(fs::exists(stale_meta_tmp));

    BucketStorageBackend storage_backend(config);
    ASSERT_TRUE(storage_backend.Init());

    EXPECT_FALSE(fs::exists(stale_bucket_tmp));
    EXPECT_FALSE(fs::exists(stale_meta_tmp));
}

TEST_F(BucketStorageBackendTest, BucketStoreMetadataTracksPhysicalBytes) {
    BucketStorageConfig config;
    config.storage_filepath = data_path;
    BucketStorageBackend backend(config);
    ASSERT_TRUE(backend.Init());

    std::string value1(1024, 'a');
    std::string value2(1024, 'b');
    std::string value3(1024, 'c');
    std::unordered_map<std::string, std::vector<Slice>> batch = {
        {"key1", {Slice{value1.data(), value1.size()}}},
        {"key2", {Slice{value2.data(), value2.size()}}},
        {"key3", {Slice{value3.data(), value3.size()}}},
    };

    auto offload_res = backend.BatchOffload(batch);
    ASSERT_TRUE(offload_res);
    const int64_t bucket_id = offload_res.value();

    auto metadata_before = backend.GetStoreMetadata();
    ASSERT_TRUE(metadata_before);
    EXPECT_EQ(metadata_before->total_keys, 3);
    EXPECT_GT(metadata_before->total_size, 0);
    const int64_t physical_bytes_before = metadata_before->total_size;

    ASSERT_TRUE(backend.MarkKeyDeleted("key1"));
    ASSERT_TRUE(backend.MarkKeyDeleted("key2"));

    auto metadata_after_mark = backend.GetStoreMetadata();
    ASSERT_TRUE(metadata_after_mark);
    EXPECT_EQ(metadata_after_mark->total_keys, 1);
    EXPECT_EQ(metadata_after_mark->total_size, physical_bytes_before);

    auto evict_res = backend.EvictBucket(bucket_id);
    ASSERT_TRUE(evict_res);
    EXPECT_EQ(evict_res.value(), value3.size());

    EXPECT_TRUE(WaitForPathRemoved(fs::path(data_path) /
                                   (std::to_string(bucket_id) + ".bucket")));
    EXPECT_TRUE(WaitForPathRemoved(fs::path(data_path) /
                                   (std::to_string(bucket_id) + ".meta")));

    auto metadata_after_delete = backend.GetStoreMetadata();
    ASSERT_TRUE(metadata_after_delete);
    EXPECT_EQ(metadata_after_delete->total_keys, 0);
    EXPECT_EQ(metadata_after_delete->total_size, 0);
}

}  // namespace mooncake::v1::test

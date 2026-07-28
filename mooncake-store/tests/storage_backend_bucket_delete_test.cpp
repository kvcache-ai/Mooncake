#include "storage_backend.h"

#include <gtest/gtest.h>
#include <ylt/struct_pb.hpp>

#include <filesystem>
#include <fstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "local_delete.h"
#include "utils.h"

namespace mooncake::test {

namespace {

struct LegacyBucketObjectMetadata {
    int64_t offset;
    int64_t key_size;
    int64_t data_size;
};
YLT_REFL(LegacyBucketObjectMetadata, offset, key_size, data_size);

struct LegacyBucketMetadata {
    int64_t data_size;
    std::vector<std::string> keys;
    std::vector<LegacyBucketObjectMetadata> metadatas;
};
YLT_REFL(LegacyBucketMetadata, data_size, keys, metadatas);

class StorageBackendBucketDeleteTest : public ::testing::Test {
   protected:
    void SetUp() override {
        data_path_ =
            std::filesystem::temp_directory_path() /
            ("mooncake_bucket_delete_" + UuidToString(generate_uuid()));
        ASSERT_TRUE(std::filesystem::create_directories(data_path_));
    }

    void TearDown() override {
        std::error_code error;
        std::filesystem::remove_all(data_path_, error);
    }

    std::filesystem::path data_path_;
};

}  // namespace

TEST_F(StorageBackendBucketDeleteTest,
       DeleteIsDurableAndCannotDeleteRecreatedKey) {
    FileStorageConfig config;
    config.storage_filepath = data_path_.string();
    BucketBackendConfig bucket_config;
    bucket_config.bucket_keys_limit = 1;

    const ObjectIncarnation old_incarnation{11, 12};
    const ObjectIncarnation new_incarnation{21, 22};
    StorageObjectMetadata old_location;
    LocalDeleteTask remove_old;
    int64_t physical_size_after_delete = 0;

    {
        BucketStorageBackend backend(config, bucket_config);
        ASSERT_TRUE(backend.Init());
        std::string value(1024, 'A');
        std::vector<std::vector<std::string>> grouped;
        const std::unordered_map<std::string, ObjectIncarnation>
            old_incarnations{{"same-key", old_incarnation}};
        ASSERT_TRUE(backend.AllocateOffloadingBuckets(
            {{"same-key", static_cast<int64_t>(value.size())}}, grouped,
            &old_incarnations));
        ASSERT_EQ(grouped.size(), 1);
        auto stored = backend.BatchOffload(
            {{"same-key", {Slice{value.data(), value.size()}}}},
            [&](const std::vector<std::string>&,
                std::vector<StorageObjectMetadata>& metadatas) {
                old_location = metadatas.front();
                return ErrorCode::OK;
            });
        ASSERT_TRUE(stored);
        const auto physical_size_before_delete = backend.GetStoreMetadata();
        ASSERT_TRUE(physical_size_before_delete);

        remove_old = LocalDeleteTask{
            .task_id = GenerateLocalDeleteTaskId(),
            .local_disk_segment_id = "disk-a",
            .tenant_id = "default",
            .key = "same-key",
            .object_incarnation = old_incarnation,
            .expected_bucket_id = old_location.bucket_id,
        };
        const auto removed = backend.BatchMarkDeleted({remove_old});
        ASSERT_EQ(removed.size(), 1);
        EXPECT_EQ(removed.front().result, LocalDeleteResult::kRemoved);
        EXPECT_FALSE(backend.IsExist("same-key").value_or(true));
        EXPECT_EQ(backend.GetReclaimableBytes(),
                  old_location.key_size + old_location.data_size);
        const auto physical_size = backend.GetStoreMetadata();
        ASSERT_TRUE(physical_size);
        EXPECT_GE(physical_size->total_size,
                  physical_size_before_delete->total_size);
        physical_size_after_delete = physical_size->total_size;

        const auto redelivered = backend.BatchMarkDeleted({remove_old});
        ASSERT_EQ(redelivered.size(), 1);
        EXPECT_EQ(redelivered.front().result,
                  LocalDeleteResult::kAlreadyRemoved);
    }

    BucketStorageBackend restarted(config, bucket_config);
    ASSERT_TRUE(restarted.Init());
    ASSERT_TRUE(restarted.GetStoreMetadata());
    EXPECT_EQ(restarted.GetStoreMetadata()->total_size,
              physical_size_after_delete);
    EXPECT_EQ(restarted.GetReclaimableBytes(),
              old_location.key_size + old_location.data_size);
    EXPECT_FALSE(restarted.IsExist("same-key").value_or(true));
    const auto after_restart = restarted.BatchMarkDeleted({remove_old});
    ASSERT_EQ(after_restart.size(), 1);
    EXPECT_EQ(after_restart.front().result, LocalDeleteResult::kAlreadyRemoved);

    std::vector<std::string> scanned_keys;
    ASSERT_TRUE(restarted.ScanMeta([&](const std::vector<std::string>& keys,
                                       std::vector<StorageObjectMetadata>&) {
        scanned_keys.insert(scanned_keys.end(), keys.begin(), keys.end());
        return ErrorCode::OK;
    }));
    EXPECT_TRUE(scanned_keys.empty());

    std::string new_value(1024, 'B');
    std::vector<std::vector<std::string>> grouped;
    const std::unordered_map<std::string, ObjectIncarnation> new_incarnations{
        {"same-key", new_incarnation}};
    ASSERT_TRUE(restarted.AllocateOffloadingBuckets(
        {{"same-key", static_cast<int64_t>(new_value.size())}}, grouped,
        &new_incarnations));
    ASSERT_EQ(grouped.size(), 1);
    ASSERT_TRUE(restarted.BatchOffload(
        {{"same-key", {Slice{new_value.data(), new_value.size()}}}},
        [](const std::vector<std::string>&,
           std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; }));

    LocalDeleteTask delayed_old_delete{
        .task_id = GenerateLocalDeleteTaskId(),
        .local_disk_segment_id = "disk-a",
        .tenant_id = "default",
        .key = "same-key",
        .object_incarnation = old_incarnation,
        .expected_bucket_id = old_location.bucket_id,
    };
    const auto stale = restarted.BatchMarkDeleted({delayed_old_delete});
    ASSERT_EQ(stale.size(), 1);
    EXPECT_EQ(stale.front().result, LocalDeleteResult::kStaleVersion);
    EXPECT_TRUE(restarted.IsExist("same-key").value_or(false));
}

TEST_F(StorageBackendBucketDeleteTest,
       OneInvalidTaskDoesNotSuppressAnotherBucket) {
    FileStorageConfig config;
    config.storage_filepath = data_path_.string();
    BucketBackendConfig bucket_config;
    bucket_config.bucket_keys_limit = 1;

    BucketStorageBackend backend(config, bucket_config);
    ASSERT_TRUE(backend.Init());
    std::string first_value(32, 'A');
    std::string second_value(32, 'B');
    const ObjectIncarnation first_incarnation{11, 12};
    const ObjectIncarnation second_incarnation{21, 22};
    std::vector<std::vector<std::string>> grouped;
    const std::unordered_map<std::string, ObjectIncarnation> incarnations{
        {"first", first_incarnation}, {"second", second_incarnation}};
    ASSERT_TRUE(backend.AllocateOffloadingBuckets(
        {{"first", static_cast<int64_t>(first_value.size())},
         {"second", static_cast<int64_t>(second_value.size())}},
        grouped, &incarnations));

    std::unordered_map<std::string, StorageObjectMetadata> locations;
    ASSERT_TRUE(backend.BatchOffload(
        {{"first", {Slice{first_value.data(), first_value.size()}}},
         {"second", {Slice{second_value.data(), second_value.size()}}}},
        [&](const std::vector<std::string>& keys,
            std::vector<StorageObjectMetadata>& metadatas) {
            for (size_t i = 0; i < keys.size(); ++i) {
                locations.emplace(keys[i], metadatas[i]);
            }
            return ErrorCode::OK;
        }));
    ASSERT_EQ(locations.size(), 2);

    const LocalDeleteTask invalid{
        .task_id = GenerateLocalDeleteTaskId(),
        .local_disk_segment_id = "disk-a",
        .tenant_id = "default",
        .key = "first",
        .object_incarnation = {31, 32},
        .expected_bucket_id = locations.at("first").bucket_id,
    };
    const LocalDeleteTask valid{
        .task_id = GenerateLocalDeleteTaskId(),
        .local_disk_segment_id = "disk-a",
        .tenant_id = "default",
        .key = "second",
        .object_incarnation = second_incarnation,
        .expected_bucket_id = locations.at("second").bucket_id,
    };
    const auto results = backend.BatchMarkDeleted({invalid, valid});
    ASSERT_EQ(results.size(), 2);
    EXPECT_EQ(results[0].result, LocalDeleteResult::kStaleVersion);
    EXPECT_EQ(results[1].result, LocalDeleteResult::kRemoved);
    EXPECT_TRUE(backend.IsExist("first").value_or(false));
    EXPECT_FALSE(backend.IsExist("second").value_or(true));
}

TEST_F(StorageBackendBucketDeleteTest,
       CorruptMetadataFailsClosedAndPreservesBucketFiles) {
    FileStorageConfig config;
    config.storage_filepath = data_path_.string();
    BucketBackendConfig bucket_config;
    bucket_config.bucket_keys_limit = 1;

    StorageObjectMetadata location;
    {
        BucketStorageBackend backend(config, bucket_config);
        ASSERT_TRUE(backend.Init());
        std::string value(32, 'A');
        std::vector<std::vector<std::string>> grouped;
        ASSERT_TRUE(backend.AllocateOffloadingBuckets(
            {{"key", static_cast<int64_t>(value.size())}}, grouped));
        ASSERT_TRUE(backend.BatchOffload(
            {{"key", {Slice{value.data(), value.size()}}}},
            [&](const std::vector<std::string>&,
                std::vector<StorageObjectMetadata>& metadatas) {
                location = metadatas.front();
                return ErrorCode::OK;
            }));
    }

    const auto metadata_path =
        data_path_ / (std::to_string(location.bucket_id) + ".meta");
    const auto data_file_path =
        data_path_ / (std::to_string(location.bucket_id) + ".bucket");
    ASSERT_TRUE(std::filesystem::exists(metadata_path));
    ASSERT_TRUE(std::filesystem::exists(data_file_path));
    {
        std::ofstream corrupt(metadata_path,
                              std::ios::binary | std::ios::trunc);
        ASSERT_TRUE(corrupt.good());
        corrupt << "not-a-valid-protobuf";
    }

    BucketStorageBackend restarted(config, bucket_config);
    EXPECT_FALSE(restarted.Init());
    EXPECT_TRUE(std::filesystem::exists(metadata_path));
    EXPECT_TRUE(std::filesystem::exists(data_file_path));
}

TEST_F(StorageBackendBucketDeleteTest, MetadataIsBackwardCompatible) {
    LegacyBucketMetadata legacy{
        .data_size = 10,
        .keys = {"legacy"},
        .metadatas = {{.offset = 1, .key_size = 2, .data_size = 7}},
    };
    std::string legacy_bytes;
    struct_pb::to_pb(legacy, legacy_bytes);

    BucketMetadata upgraded;
    ASSERT_NO_THROW(struct_pb::from_pb(upgraded, legacy_bytes));
    ASSERT_EQ(upgraded.metadatas.size(), 1);
    EXPECT_TRUE(upgraded.metadatas.front().object_incarnation.IsZero());
    EXPECT_FALSE(upgraded.metadatas.front().tombstoned);

    upgraded.metadatas.front().object_incarnation = {31, 32};
    upgraded.metadatas.front().tombstoned = true;
    std::string current_bytes;
    struct_pb::to_pb(upgraded, current_bytes);
    BucketMetadata round_tripped;
    ASSERT_NO_THROW(struct_pb::from_pb(round_tripped, current_bytes));
    ASSERT_EQ(round_tripped.metadatas.size(), 1);
    EXPECT_EQ(round_tripped.metadatas.front().object_incarnation,
              (ObjectIncarnation{31, 32}));
    EXPECT_TRUE(round_tripped.metadatas.front().tombstoned);
}

}  // namespace mooncake::test

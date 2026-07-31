#include "storage_backend.h"

#include <gtest/gtest.h>
#include <ylt/struct_pb.hpp>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <functional>
#include <string>
#include <string_view>
#include <thread>
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

bool WaitUntil(const std::function<bool()>& predicate,
               std::chrono::seconds timeout = std::chrono::seconds(5)) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return predicate();
}

size_t CountBucketFiles(const std::filesystem::path& path,
                        std::string_view extension) {
    size_t count = 0;
    for (const auto& entry : std::filesystem::directory_iterator(path)) {
        std::error_code error;
        if (entry.is_regular_file(error) && !error &&
            entry.path().extension() == extension) {
            ++count;
        }
    }
    return count;
}

void WriteGcIntent(const std::filesystem::path& path,
                   const BucketGcIntent& intent) {
    std::string bytes;
    struct_pb::to_pb(intent, bytes);
    std::ofstream output(path / ".bucket_gc_intent",
                         std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(output.good());
    output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    ASSERT_TRUE(output.good());
}

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

TEST_F(StorageBackendBucketDeleteTest,
       GarbageCollectionMergesPartiallyDeadBuckets) {
    FileStorageConfig config;
    config.storage_filepath = data_path_.string();
    BucketBackendConfig bucket_config;
    bucket_config.bucket_keys_limit = 8;
    bucket_config.bucket_size_limit = 8 * 1024;
    bucket_config.gc_interval_seconds = 3600;
    bucket_config.gc_deleted_ratio = 0.25;

    std::unordered_map<std::string, std::string> live_values;
    std::unordered_map<std::string, StorageObjectMetadata> locations;
    int64_t physical_size_before_gc = 0;
    {
        BucketStorageBackend backend(config, bucket_config);
        ASSERT_TRUE(backend.Init());

        for (int bucket_index = 0; bucket_index < 3; ++bucket_index) {
            const std::string deleted_key =
                "deleted-" + std::to_string(bucket_index);
            const std::string live_key = "live-" + std::to_string(bucket_index);
            std::string deleted_value(7 * 1024,
                                      static_cast<char>('a' + bucket_index));
            std::string live_value(1024, static_cast<char>('A' + bucket_index));
            live_values.emplace(live_key, live_value);

            const ObjectIncarnation deleted_incarnation{
                100, static_cast<uint64_t>(bucket_index + 1)};
            const ObjectIncarnation live_incarnation{
                200, static_cast<uint64_t>(bucket_index + 1)};
            std::vector<std::vector<std::string>> grouped;
            const std::unordered_map<std::string, ObjectIncarnation>
                incarnations{{deleted_key, deleted_incarnation},
                             {live_key, live_incarnation}};
            ASSERT_TRUE(backend.AllocateOffloadingBuckets(
                {{deleted_key, static_cast<int64_t>(deleted_value.size())},
                 {live_key, static_cast<int64_t>(live_value.size())}},
                grouped, &incarnations));
            ASSERT_EQ(grouped.size(), 1);

            std::unordered_map<std::string, std::vector<Slice>> batch;
            batch.emplace(deleted_key,
                          std::vector<Slice>{Slice{deleted_value.data(),
                                                   deleted_value.size()}});
            batch.emplace(live_key, std::vector<Slice>{Slice{
                                        live_value.data(), live_value.size()}});
            ASSERT_TRUE(backend.BatchOffload(
                batch, [&](const std::vector<std::string>& keys,
                           std::vector<StorageObjectMetadata>& metadatas) {
                    for (size_t i = 0; i < keys.size(); ++i) {
                        locations[keys[i]] = metadatas[i];
                    }
                    return ErrorCode::OK;
                }));

            const LocalDeleteTask task{
                .task_id = GenerateLocalDeleteTaskId(),
                .local_disk_segment_id = "disk-a",
                .tenant_id = "default",
                .key = deleted_key,
                .object_incarnation = deleted_incarnation,
                .expected_bucket_id = locations.at(deleted_key).bucket_id,
            };
            const auto result = backend.BatchMarkDeleted({task});
            ASSERT_EQ(result.size(), 1);
            ASSERT_EQ(result.front().result, LocalDeleteResult::kRemoved);
        }

        ASSERT_GT(backend.GetReclaimableBytes(), 0);
        const auto before_gc = backend.GetStoreMetadata();
        ASSERT_TRUE(before_gc);
        physical_size_before_gc = before_gc->total_size;

        backend.RequestGarbageCollection();
        ASSERT_TRUE(WaitUntil([&] {
            return backend.GetReclaimableBytes() == 0 &&
                   CountBucketFiles(data_path_, ".meta") == 1 &&
                   CountBucketFiles(data_path_, ".bucket") == 1;
        }));

        const auto after_gc = backend.GetStoreMetadata();
        ASSERT_TRUE(after_gc);
        EXPECT_LT(after_gc->total_size, physical_size_before_gc);
        EXPECT_EQ(CountBucketFiles(data_path_, ".meta"), 1);
        EXPECT_EQ(CountBucketFiles(data_path_, ".bucket"), 1);

        for (const auto& [key, value] : live_values) {
            std::vector<char> buffer(value.size());
            std::unordered_map<std::string, Slice> load;
            load.emplace(key, Slice{buffer.data(), buffer.size()});
            ASSERT_TRUE(backend.BatchLoad(load));
            EXPECT_EQ(std::string(buffer.begin(), buffer.end()), value);
        }
    }

    bucket_config.gc_enable = false;
    BucketStorageBackend restarted(config, bucket_config);
    ASSERT_TRUE(restarted.Init());
    EXPECT_EQ(restarted.GetReclaimableBytes(), 0);
    EXPECT_FALSE(restarted.RequestGarbageCollection());
    for (const auto& [key, value] : live_values) {
        std::vector<char> buffer(value.size());
        std::unordered_map<std::string, Slice> load;
        load.emplace(key, Slice{buffer.data(), buffer.size()});
        ASSERT_TRUE(restarted.BatchLoad(load));
        EXPECT_EQ(std::string(buffer.begin(), buffer.end()), value);
    }
}

TEST_F(StorageBackendBucketDeleteTest,
       DiskHighWatermarkOverridesDeletedRatioThreshold) {
    FileStorageConfig config;
    config.storage_filepath = data_path_.string();
    config.disk_eviction_high_watermark_ratio = 0.90;
    config.disk_eviction_low_watermark_ratio = 0.80;
    BucketBackendConfig bucket_config;
    bucket_config.bucket_keys_limit = 10;
    bucket_config.bucket_size_limit = 32 * 1024;
    bucket_config.max_total_size = 10 * 1024;
    bucket_config.gc_interval_seconds = 3600;
    bucket_config.gc_deleted_ratio = 0.90;

    BucketStorageBackend backend(config, bucket_config);
    ASSERT_TRUE(backend.Init());
    std::unordered_map<std::string, std::vector<Slice>> batch;
    std::unordered_map<std::string, std::string> values;
    std::unordered_map<std::string, ObjectIncarnation> incarnations;
    std::unordered_map<std::string, int64_t> sizes;
    for (int i = 0; i < 10; ++i) {
        const std::string key = "key-" + std::to_string(i);
        auto [value_it, _] = values.emplace(key, std::string(940, 'A' + i));
        batch.emplace(key, std::vector<Slice>{Slice{value_it->second.data(),
                                                    value_it->second.size()}});
        incarnations.emplace(key,
                             ObjectIncarnation{300, static_cast<uint64_t>(i)});
        sizes.emplace(key, static_cast<int64_t>(value_it->second.size()));
    }
    std::vector<std::vector<std::string>> grouped;
    ASSERT_TRUE(
        backend.AllocateOffloadingBuckets(sizes, grouped, &incarnations));
    ASSERT_EQ(grouped.size(), 1);

    std::unordered_map<std::string, StorageObjectMetadata> locations;
    ASSERT_TRUE(backend.BatchOffload(
        batch, [&](const std::vector<std::string>& keys,
                   std::vector<StorageObjectMetadata>& metadatas) {
            for (size_t i = 0; i < keys.size(); ++i) {
                locations[keys[i]] = metadatas[i];
            }
            return ErrorCode::OK;
        }));
    const auto before_delete = backend.GetStoreMetadata();
    ASSERT_TRUE(before_delete);
    ASSERT_GT(before_delete->total_size,
              static_cast<int64_t>(bucket_config.max_total_size *
                                   config.disk_eviction_high_watermark_ratio));

    std::vector<LocalDeleteTask> tasks;
    for (int i = 0; i < 3; ++i) {
        const std::string key = "key-" + std::to_string(i);
        tasks.push_back(LocalDeleteTask{
            .task_id = GenerateLocalDeleteTaskId(),
            .local_disk_segment_id = "disk-a",
            .tenant_id = "default",
            .key = key,
            .object_incarnation = incarnations.at(key),
            .expected_bucket_id = locations.at(key).bucket_id,
        });
    }
    const auto deleted = backend.BatchMarkDeleted(tasks);
    ASSERT_EQ(deleted.size(), tasks.size());
    for (const auto& result : deleted) {
        ASSERT_EQ(result.result, LocalDeleteResult::kRemoved);
    }

    // 30% is below the configured 90% ratio. Crossing the disk high
    // watermark must still admit the bucket and reclaim it toward low water.
    ASSERT_TRUE(backend.RequestGarbageCollection(
        /* require_disk_pressure = */ true));
    ASSERT_TRUE(WaitUntil([&] {
        const auto state = backend.GetStoreMetadata();
        return state && backend.GetReclaimableBytes() == 0 &&
               state->total_size < before_delete->total_size;
    }));
    const auto after_gc = backend.GetStoreMetadata();
    ASSERT_TRUE(after_gc);
    EXPECT_LT(after_gc->total_size, before_delete->total_size);
    EXPECT_LE(after_gc->total_size,
              static_cast<int64_t>(bucket_config.max_total_size *
                                   config.disk_eviction_low_watermark_ratio));
}

TEST_F(StorageBackendBucketDeleteTest,
       GarbageCollectionUnlinksFullyDeadBucketWithoutReplacement) {
    FileStorageConfig config;
    config.storage_filepath = data_path_.string();
    BucketBackendConfig bucket_config;
    bucket_config.bucket_keys_limit = 1;
    bucket_config.gc_interval_seconds = 3600;

    BucketStorageBackend backend(config, bucket_config);
    ASSERT_TRUE(backend.Init());
    const std::string key = "dead";
    std::string value(1024, 'D');
    const ObjectIncarnation incarnation{501, 1};
    std::vector<std::vector<std::string>> grouped;
    const std::unordered_map<std::string, ObjectIncarnation> incarnations{
        {key, incarnation}};
    ASSERT_TRUE(backend.AllocateOffloadingBuckets(
        {{key, static_cast<int64_t>(value.size())}}, grouped, &incarnations));
    StorageObjectMetadata location;
    ASSERT_TRUE(backend.BatchOffload(
        {{key, {Slice{value.data(), value.size()}}}},
        [&](const std::vector<std::string>&,
            std::vector<StorageObjectMetadata>& metadatas) {
            location = metadatas.front();
            return ErrorCode::OK;
        }));
    const auto deleted = backend.BatchMarkDeleted({LocalDeleteTask{
        .task_id = GenerateLocalDeleteTaskId(),
        .local_disk_segment_id = "disk-a",
        .tenant_id = "default",
        .key = key,
        .object_incarnation = incarnation,
        .expected_bucket_id = location.bucket_id,
    }});
    ASSERT_EQ(deleted.size(), 1);
    ASSERT_EQ(deleted.front().result, LocalDeleteResult::kRemoved);

    backend.RequestGarbageCollection();
    ASSERT_TRUE(WaitUntil([&] {
        return backend.GetReclaimableBytes() == 0 &&
               CountBucketFiles(data_path_, ".meta") == 0 &&
               CountBucketFiles(data_path_, ".bucket") == 0;
    }));
    const auto state = backend.GetStoreMetadata();
    ASSERT_TRUE(state);
    EXPECT_EQ(state->total_keys, 0);
    EXPECT_EQ(state->total_size, 0);
}

TEST_F(StorageBackendBucketDeleteTest,
       GarbageCollectionIntentRecoversPreparedAndCommittedStates) {
    const auto run_case = [&](const std::string& name, bool committed) {
        const auto case_path = data_path_ / name;
        ASSERT_TRUE(std::filesystem::create_directories(case_path));
        FileStorageConfig config;
        config.storage_filepath = case_path.string();
        BucketBackendConfig bucket_config;
        bucket_config.bucket_keys_limit = 1;
        bucket_config.gc_enable = false;

        int64_t source_id = -1;
        int64_t target_id = -1;
        {
            BucketStorageBackend backend(config, bucket_config);
            ASSERT_TRUE(backend.Init());
            for (const auto& [key, incarnation] :
                 std::vector<std::pair<std::string, ObjectIncarnation>>{
                     {"source", {401, 1}}, {"target", {402, 1}}}) {
                std::string value(128, key.front());
                std::vector<std::vector<std::string>> grouped;
                const std::unordered_map<std::string, ObjectIncarnation>
                    incarnation_by_key{{key, incarnation}};
                ASSERT_TRUE(backend.AllocateOffloadingBuckets(
                    {{key, static_cast<int64_t>(value.size())}}, grouped,
                    &incarnation_by_key));
                StorageObjectMetadata location;
                ASSERT_TRUE(backend.BatchOffload(
                    {{key,
                      {Slice{value.data(),
                             static_cast<size_t>(value.size())}}}},
                    [&](const std::vector<std::string>&,
                        std::vector<StorageObjectMetadata>& metadatas) {
                        location = metadatas.front();
                        return ErrorCode::OK;
                    }));
                if (key == "source") {
                    source_id = location.bucket_id;
                } else {
                    target_id = location.bucket_id;
                }
            }
        }
        ASSERT_GE(source_id, 0);
        ASSERT_GE(target_id, 0);
        WriteGcIntent(case_path,
                      BucketGcIntent{.version = 1,
                                     .committed = committed,
                                     .target_bucket_id = target_id,
                                     .source_bucket_ids = {source_id}});

        BucketStorageBackend recovered(config, bucket_config);
        ASSERT_TRUE(recovered.Init());
        EXPECT_EQ(recovered.IsExist("source").value_or(false), !committed);
        EXPECT_EQ(recovered.IsExist("target").value_or(false), committed);
        EXPECT_FALSE(std::filesystem::exists(case_path / ".bucket_gc_intent"));
        EXPECT_EQ(CountBucketFiles(case_path, ".meta"), 1);
        EXPECT_EQ(CountBucketFiles(case_path, ".bucket"), 1);
    };

    run_case("prepared", false);
    run_case("committed", true);
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

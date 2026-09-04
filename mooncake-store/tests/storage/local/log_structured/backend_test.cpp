#include "storage/local/log_structured/log_structured_backend.h"

#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <future>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>

#include "tenant_id.h"

namespace mooncake {
namespace {

class BackendTempDirectory {
   public:
    BackendTempDirectory() {
        const auto id = next_id_.fetch_add(1, std::memory_order_relaxed);
        const char* tmpdir = std::getenv("TMPDIR");
        const std::filesystem::path base =
            tmpdir == nullptr ? std::filesystem::temp_directory_path()
                              : std::filesystem::path(tmpdir);
        path_ = base / ("mooncake-log-structured-backend-test-" +
                        std::to_string(getpid()) + "-" + std::to_string(id));
        std::filesystem::create_directories(path_);
    }

    ~BackendTempDirectory() {
        std::error_code error;
        std::filesystem::remove_all(path_, error);
    }

    const std::filesystem::path& path() const { return path_; }

   private:
    inline static std::atomic<uint64_t> next_id_{0};
    std::filesystem::path path_;
};

FileStorageConfig BackendConfig(const BackendTempDirectory& temp) {
    FileStorageConfig config;
    config.storage_backend_type = StorageBackendType::kLogStructured;
    config.storage_filepath = temp.path().string();
    config.total_keys_limit = 100;
    config.total_size_limit = 1024 * 1024;
    config.scanmeta_iterator_keys_limit = 1;
    return config;
}

std::unordered_map<std::string, std::vector<Slice>> SingleValueBatch(
    const std::string& key, std::string& value) {
    return {{key, {Slice{value.data(), value.size()}}}};
}

TEST(LogStructuredStorageBackendTest, OffloadLoadAndScanPreserveTenantKey) {
    BackendTempDirectory temp;
    const auto config = BackendConfig(temp);
    const std::string storage_key = TenantId("tenant-a").MakeScopedKey("key");
    std::string value = "payload";

    {
        LogStructuredStorageBackend backend(config);
        ASSERT_TRUE(backend.Init().has_value());
        auto result = backend.BatchOffload(
            SingleValueBatch(storage_key, value),
            [&](const std::vector<std::string>& keys,
                std::vector<StorageObjectMetadata>& metadatas) -> ErrorCode {
                EXPECT_EQ(keys, std::vector<std::string>{storage_key});
                EXPECT_EQ(metadatas.size(), size_t{1});
                if (metadatas.size() == 1) {
                    EXPECT_EQ(metadatas[0].bucket_id, -1);
                    EXPECT_EQ(metadatas[0].offset, 0);
                    EXPECT_EQ(metadatas[0].data_size,
                              static_cast<int64_t>(value.size()));
                }
                return ErrorCode::OK;
            });
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(*result, 1);

        std::string loaded(value.size(), '\0');
        std::unordered_map<std::string, Slice> slices{
            {storage_key, Slice{loaded.data(), loaded.size()}}};
        ASSERT_TRUE(backend.BatchLoad(slices).has_value());
        EXPECT_EQ(loaded, value);
    }

    LogStructuredStorageBackend recovered(config);
    ASSERT_TRUE(recovered.Init().has_value());
    std::vector<std::string> scanned_keys;
    ASSERT_TRUE(
        recovered
            .ScanMeta([&](const std::vector<std::string>& keys,
                          std::vector<StorageObjectMetadata>& metadatas) {
                scanned_keys.insert(scanned_keys.end(), keys.begin(),
                                    keys.end());
                EXPECT_EQ(keys.size(), metadatas.size());
                return ErrorCode::OK;
            })
            .has_value());
    EXPECT_EQ(scanned_keys, std::vector<std::string>{storage_key});
}

TEST(LogStructuredStorageBackendTest, BatchLoadsRecordsAcrossSegments) {
    BackendTempDirectory temp;
    const auto config = BackendConfig(temp);
    LogStructuredBackendConfig backend_config;
    backend_config.segment_size_bytes = 256;
    backend_config.sync_policy = LogStructuredSyncPolicy::kNone;
    backend_config.compaction_policy = LogStructuredCompactionPolicy::kNone;

    LogStructuredStorageBackend backend(config, backend_config);
    ASSERT_TRUE(backend.Init().has_value());
    const auto commit = [](const std::vector<std::string>&,
                           std::vector<StorageObjectMetadata>&) {
        return ErrorCode::OK;
    };

    std::unordered_map<std::string, std::string> values;
    for (size_t index = 0; index < 4; ++index) {
        const std::string key =
            TenantId("tenant-batch")
                .MakeScopedKey("key-" + std::to_string(index));
        auto [it, inserted] = values.emplace(
            key, std::string(96, static_cast<char>('a' + index)));
        ASSERT_TRUE(inserted);
        ASSERT_TRUE(
            backend
                .BatchOffload(SingleValueBatch(it->first, it->second), commit)
                .has_value());
    }

    std::unordered_map<std::string, std::string> loaded;
    std::unordered_map<std::string, Slice> slices;
    for (const auto& [key, value] : values) {
        auto [it, inserted] =
            loaded.emplace(key, std::string(value.size(), '\0'));
        ASSERT_TRUE(inserted);
        slices.emplace(key, Slice{it->second.data(), it->second.size()});
    }
    ASSERT_TRUE(backend.BatchLoad(slices).has_value());
    EXPECT_EQ(loaded, values);
}

TEST(LogStructuredStorageBackendTest, SupportsConcurrentBatchLoads) {
    BackendTempDirectory temp;
    const auto config = BackendConfig(temp);
    const std::string storage_key = TenantId("tenant-a").MakeScopedKey("key");
    std::string value(128 * 1024, 'v');

    LogStructuredStorageBackend backend(config);
    ASSERT_TRUE(backend.Init().has_value());
    ASSERT_TRUE(backend
                    .BatchOffload(SingleValueBatch(storage_key, value),
                                  [](const std::vector<std::string>&,
                                     std::vector<StorageObjectMetadata>&) {
                                      return ErrorCode::OK;
                                  })
                    .has_value());

    std::atomic<uint64_t> errors{0};
    std::vector<std::thread> readers;
    for (size_t index = 0; index < 8; ++index) {
        readers.emplace_back([&]() {
            for (size_t iteration = 0; iteration < 64; ++iteration) {
                std::string loaded(value.size(), '\0');
                std::unordered_map<std::string, Slice> slices{
                    {storage_key, Slice{loaded.data(), loaded.size()}}};
                auto result = backend.BatchLoad(slices);
                if (!result || loaded != value) {
                    errors.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }
    for (auto& reader : readers) reader.join();

    EXPECT_EQ(errors.load(std::memory_order_relaxed), uint64_t{0});
}

TEST(LogStructuredStorageBackendTest,
     CallbackFailureAbortsNewVersionAndPreservesCommittedValue) {
    BackendTempDirectory temp;
    const auto config = BackendConfig(temp);
    const std::string storage_key = TenantId("tenant-b").MakeScopedKey("key");
    std::string old_value = "old";
    std::string rejected_value = "new-value";

    LogStructuredStorageBackend backend(config);
    ASSERT_TRUE(backend.Init().has_value());
    ASSERT_TRUE(backend
                    .BatchOffload(SingleValueBatch(storage_key, old_value),
                                  [](const std::vector<std::string>&,
                                     std::vector<StorageObjectMetadata>&) {
                                      return ErrorCode::OK;
                                  })
                    .has_value());

    auto rejected =
        backend.BatchOffload(SingleValueBatch(storage_key, rejected_value),
                             [](const std::vector<std::string>&,
                                std::vector<StorageObjectMetadata>&) {
                                 return ErrorCode::INTERNAL_ERROR;
                             });
    ASSERT_FALSE(rejected.has_value());
    EXPECT_TRUE(rejected.error() == ErrorCode::INTERNAL_ERROR);

    std::string loaded(old_value.size(), '\0');
    std::unordered_map<std::string, Slice> slices{
        {storage_key, Slice{loaded.data(), loaded.size()}}};
    ASSERT_TRUE(backend.BatchLoad(slices).has_value());
    EXPECT_EQ(loaded, old_value);
}

TEST(LogStructuredStorageBackendTest, PartialFailureReportsOnlyStoredKeys) {
    BackendTempDirectory temp;
    const auto config = BackendConfig(temp);
    std::string first = "first";
    std::string second = "second";
    std::unordered_map<std::string, std::vector<Slice>> batch{
        {"key-1", {Slice{first.data(), first.size()}}},
        {"key-2", {Slice{second.data(), second.size()}}}};

    LogStructuredStorageBackend backend(config);
    ASSERT_TRUE(backend.Init().has_value());
    backend.SetTestFailurePredicate(
        [](const std::string& key) { return key == "key-2"; });

    std::vector<std::string> completed;
    auto result =
        backend.BatchOffload(batch, [&](const std::vector<std::string>& keys,
                                        std::vector<StorageObjectMetadata>&) {
            completed = keys;
            return ErrorCode::OK;
        });
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, 1);
    EXPECT_EQ(completed, std::vector<std::string>{"key-1"});
    EXPECT_EQ(backend.IsExist("key-1").value(), true);
    EXPECT_EQ(backend.IsExist("key-2").value(), false);
}

TEST(LogStructuredStorageBackendTest, ReadsAndValidatesBackendConfiguration) {
    setenv("MOONCAKE_LOG_SEGMENT_SIZE_BYTES", "4096", 1);
    setenv("MOONCAKE_LOG_WRITE_PARALLELISM", "7", 1);
    setenv("MOONCAKE_LOG_SYNC_POLICY", "batch", 1);
    setenv("MOONCAKE_LOG_CHECKPOINT_INTERVAL", "17", 1);
    setenv("MOONCAKE_LOG_COMPACTION_POLICY", "tiered", 1);
    setenv("MOONCAKE_LOG_COMPACTION_INTERVAL_MS", "25", 1);
    setenv("MOONCAKE_LOG_COMPACTION_FANOUT", "3", 1);
    setenv("MOONCAKE_LOG_COMPACTION_MAX_LEVELS", "5", 1);
    setenv("MOONCAKE_LOG_COMPACTION_MAX_SOURCES", "6", 1);
    setenv("MOONCAKE_LOG_COMPACTION_MAX_INPUT_BYTES", "8192", 1);
    setenv("MOONCAKE_LOG_COMPACTION_MAX_TARGET_BYTES", "16384", 1);
    setenv("MOONCAKE_LOG_COMPACTION_MAX_BYTES_PER_SEC", "32768", 1);
    setenv("MOONCAKE_LOG_COMPACTION_RESERVE_BYTES", "2048", 1);
    setenv("MOONCAKE_LOG_COMPACTION_MIN_RECLAIM_RATIO", "0.35", 1);

    const auto config = LogStructuredBackendConfig::FromEnvironment();

    unsetenv("MOONCAKE_LOG_SEGMENT_SIZE_BYTES");
    unsetenv("MOONCAKE_LOG_WRITE_PARALLELISM");
    unsetenv("MOONCAKE_LOG_SYNC_POLICY");
    unsetenv("MOONCAKE_LOG_CHECKPOINT_INTERVAL");
    unsetenv("MOONCAKE_LOG_COMPACTION_POLICY");
    unsetenv("MOONCAKE_LOG_COMPACTION_INTERVAL_MS");
    unsetenv("MOONCAKE_LOG_COMPACTION_FANOUT");
    unsetenv("MOONCAKE_LOG_COMPACTION_MAX_LEVELS");
    unsetenv("MOONCAKE_LOG_COMPACTION_MAX_SOURCES");
    unsetenv("MOONCAKE_LOG_COMPACTION_MAX_INPUT_BYTES");
    unsetenv("MOONCAKE_LOG_COMPACTION_MAX_TARGET_BYTES");
    unsetenv("MOONCAKE_LOG_COMPACTION_MAX_BYTES_PER_SEC");
    unsetenv("MOONCAKE_LOG_COMPACTION_RESERVE_BYTES");
    unsetenv("MOONCAKE_LOG_COMPACTION_MIN_RECLAIM_RATIO");

    EXPECT_TRUE(config.Validate());
    EXPECT_EQ(config.segment_size_bytes, uint64_t{4096});
    EXPECT_EQ(config.payload_write_parallelism, size_t{7});
    EXPECT_EQ(config.sync_policy, LogStructuredSyncPolicy::kBatch);
    EXPECT_EQ(config.checkpoint_interval_records, uint64_t{17});
    EXPECT_EQ(config.compaction_policy, LogStructuredCompactionPolicy::kTiered);
    EXPECT_EQ(config.compaction_interval_ms, uint64_t{25});
    EXPECT_EQ(config.compaction_fanout, size_t{3});
    EXPECT_EQ(config.compaction_max_levels, uint32_t{5});
    EXPECT_EQ(config.compaction_max_sources, size_t{6});
    EXPECT_EQ(config.compaction_max_input_bytes, uint64_t{8192});
    EXPECT_EQ(config.compaction_max_target_bytes, uint64_t{16384});
    EXPECT_EQ(config.compaction_max_bytes_per_second, uint64_t{32768});
    EXPECT_EQ(config.compaction_reserve_bytes, uint64_t{2048});
    EXPECT_DOUBLE_EQ(config.compaction_min_reclaim_ratio, 0.35);
}

TEST(LogStructuredStorageBackendTest, BackgroundTieringKeepsObjectsReadable) {
    BackendTempDirectory temp;
    auto config = BackendConfig(temp);
    LogStructuredBackendConfig backend_config;
    backend_config.segment_size_bytes = 256;
    backend_config.sync_policy = LogStructuredSyncPolicy::kBatch;
    backend_config.checkpoint_interval_records = 2;
    backend_config.compaction_policy = LogStructuredCompactionPolicy::kTiered;
    backend_config.compaction_interval_ms = 5;
    backend_config.compaction_fanout = 4;
    backend_config.compaction_max_sources = 4;
    backend_config.compaction_max_target_bytes = 1024;
    backend_config.compaction_min_reclaim_ratio = 1.0;

    LogStructuredStorageBackend backend(config, backend_config);
    ASSERT_TRUE(backend.Init().has_value());
    std::vector<std::string> values;
    for (size_t i = 0; i < 5; ++i) {
        values.push_back(std::string(96, static_cast<char>('a' + i)));
        const std::string key = "key-" + std::to_string(i);
        ASSERT_TRUE(backend
                        .BatchOffload(SingleValueBatch(key, values.back()),
                                      [](const std::vector<std::string>&,
                                         std::vector<StorageObjectMetadata>&) {
                                          return ErrorCode::OK;
                                      })
                        .has_value());
    }

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    size_t segment_count = 0;
    do {
        segment_count = 0;
        for (const auto& entry : std::filesystem::directory_iterator(
                 temp.path() / "log_structured" / "segments")) {
            if (entry.is_regular_file()) ++segment_count;
        }
        if (segment_count <= 2) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    } while (std::chrono::steady_clock::now() < deadline);
    EXPECT_LE(segment_count, size_t{2});

    for (size_t i = 0; i < values.size(); ++i) {
        const std::string key = "key-" + std::to_string(i);
        std::string loaded(values[i].size(), '\0');
        std::unordered_map<std::string, Slice> slices{
            {key, Slice{loaded.data(), loaded.size()}}};
        ASSERT_TRUE(backend.BatchLoad(slices).has_value());
        EXPECT_EQ(loaded, values[i]);
    }
}

TEST(LogStructuredStorageBackendTest,
     BatchLoadCompletesWhileBackgroundCompactionCopies) {
    BackendTempDirectory temp;
    auto config = BackendConfig(temp);
    LogStructuredBackendConfig backend_config;
    backend_config.segment_size_bytes = 512;
    backend_config.sync_policy = LogStructuredSyncPolicy::kBatch;
    backend_config.checkpoint_interval_records = 1000;
    backend_config.compaction_policy =
        LogStructuredCompactionPolicy::kReclaimOnly;
    backend_config.compaction_interval_ms = 5;
    backend_config.compaction_min_reclaim_ratio = 0.0;

    std::promise<void> target_ready;
    std::promise<void> allow_publication;
    auto allow_publication_future = allow_publication.get_future().share();
    std::atomic<bool> blocked{false};
    logstructured::LogStructuredStore::SetCompactionCrashPredicateForTest(
        [&](logstructured::CompactionCrashPoint point) {
            if (point !=
                    logstructured::CompactionCrashPoint::kAfterTargetRename ||
                blocked.exchange(true)) {
                return false;
            }
            target_ready.set_value();
            allow_publication_future.wait();
            return false;
        });

    LogStructuredStorageBackend backend(config, backend_config);
    ASSERT_TRUE(backend.Init().has_value());
    const std::string stale_key =
        TenantId("tenant-peer").MakeScopedKey("stale");
    const std::string live_key = TenantId("tenant-peer").MakeScopedKey("live");
    std::string stale_value(96, 'a');
    std::string live_value(96, 'b');
    std::string replacement_value(96, 'c');
    const auto commit = [](const std::vector<std::string>&,
                           std::vector<StorageObjectMetadata>&) {
        return ErrorCode::OK;
    };
    ASSERT_TRUE(
        backend.BatchOffload(SingleValueBatch(stale_key, stale_value), commit)
            .has_value());
    ASSERT_TRUE(
        backend.BatchOffload(SingleValueBatch(live_key, live_value), commit)
            .has_value());
    ASSERT_TRUE(backend
                    .BatchOffload(
                        SingleValueBatch(stale_key, replacement_value), commit)
                    .has_value());

    ASSERT_EQ(target_ready.get_future().wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    std::string loaded(live_value.size(), '\0');
    std::unordered_map<std::string, Slice> slices{
        {live_key, Slice{loaded.data(), loaded.size()}}};
    auto read = std::async(std::launch::async,
                           [&]() { return backend.BatchLoad(slices); });
    ASSERT_EQ(read.wait_for(std::chrono::seconds(1)),
              std::future_status::ready);
    ASSERT_TRUE(read.get().has_value());
    EXPECT_EQ(loaded, live_value);

    allow_publication.set_value();
    logstructured::LogStructuredStore::SetCompactionCrashPredicateForTest({});
}

TEST(LogStructuredStorageBackendTest,
     CapacityLimitRejectsAppendAndPreservesCommittedValue) {
    BackendTempDirectory temp;
    auto config = BackendConfig(temp);
    config.total_size_limit = 256;
    LogStructuredBackendConfig backend_config;
    backend_config.segment_size_bytes = 1024;
    backend_config.compaction_policy = LogStructuredCompactionPolicy::kNone;

    LogStructuredStorageBackend backend(config, backend_config);
    ASSERT_TRUE(backend.Init().has_value());
    const std::string storage_key =
        TenantId("tenant-a").MakeScopedKey("capacity-key");
    std::string old_value(96, 'a');
    ASSERT_TRUE(backend
                    .BatchOffload(SingleValueBatch(storage_key, old_value),
                                  [](const std::vector<std::string>&,
                                     std::vector<StorageObjectMetadata>&) {
                                      return ErrorCode::OK;
                                  })
                    .has_value());

    std::string new_value(96, 'b');
    auto rejected = backend.BatchOffload(
        SingleValueBatch(storage_key, new_value),
        [](const std::vector<std::string>&,
           std::vector<StorageObjectMetadata>&) { return ErrorCode::OK; });
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error(), ErrorCode::KEYS_ULTRA_LIMIT);

    std::string loaded(old_value.size(), '\0');
    std::unordered_map<std::string, Slice> slices{
        {storage_key, Slice{loaded.data(), loaded.size()}}};
    ASSERT_TRUE(backend.BatchLoad(slices).has_value());
    EXPECT_EQ(loaded, old_value);
}

TEST(LogStructuredStorageBackendTest,
     DiskWatermarkCompactsGarbageWithoutEvictingLiveKeys) {
    BackendTempDirectory temp;
    auto config = BackendConfig(temp);
    config.total_size_limit = 1024;
    LogStructuredBackendConfig backend_config;
    backend_config.segment_size_bytes = 128;
    backend_config.compaction_policy = LogStructuredCompactionPolicy::kNone;
    backend_config.compaction_reserve_bytes = 256;
    backend_config.compaction_max_sources = 8;
    backend_config.compaction_max_input_bytes = 4096;
    backend_config.compaction_max_target_bytes = 4096;

    LogStructuredStorageBackend backend(config, backend_config);
    ASSERT_TRUE(backend.Init().has_value());
    const std::string storage_key =
        TenantId("tenant-a").MakeScopedKey("replaced-key");
    for (char fill : {'a', 'b', 'c'}) {
        std::string value(96, fill);
        ASSERT_TRUE(backend
                        .BatchOffload(SingleValueBatch(storage_key, value),
                                      [](const std::vector<std::string>&,
                                         std::vector<StorageObjectMetadata>&) {
                                          return ErrorCode::OK;
                                      })
                        .has_value());
    }
    EXPECT_TRUE(backend.IsEnableOffloading().value());

    const auto segments_path = temp.path() / "log_structured" / "segments";
    const auto disk_bytes = [&]() {
        uint64_t bytes = 0;
        for (const auto& entry :
             std::filesystem::directory_iterator(segments_path)) {
            if (entry.is_regular_file()) bytes += entry.file_size();
        }
        return bytes;
    };
    const uint64_t before = disk_bytes();

    auto evicted = backend.EvictAboveDiskWatermark(0.70, 0.30);
    ASSERT_TRUE(evicted.has_value());
    EXPECT_TRUE(evicted->empty());
    EXPECT_LT(disk_bytes(), before);
    EXPECT_TRUE(backend.IsEnableOffloading().value());
    const auto stats = backend.SnapshotStats();
    ASSERT_TRUE(stats.has_value());
    EXPECT_GE(stats->compaction_runs, uint64_t{1});
    EXPECT_GT(stats->compaction_input_bytes, uint64_t{0});
    EXPECT_GT(stats->compaction_reclaimed_bytes, uint64_t{0});
    EXPECT_EQ(stats->compaction_errors, uint64_t{0});

    std::string loaded(96, '\0');
    std::unordered_map<std::string, Slice> slices{
        {storage_key, Slice{loaded.data(), loaded.size()}}};
    ASSERT_TRUE(backend.BatchLoad(slices).has_value());
    EXPECT_EQ(loaded, std::string(96, 'c'));
}

TEST(LogStructuredStorageBackendTest, RemoveAllSurvivesRestart) {
    BackendTempDirectory temp;
    const auto config = BackendConfig(temp);
    std::string first = "first-value";
    std::string second = "second-value";
    {
        LogStructuredBackendConfig backend_config;
        backend_config.segment_size_bytes = 256;
        backend_config.compaction_policy = LogStructuredCompactionPolicy::kNone;
        LogStructuredStorageBackend backend(config, backend_config);
        ASSERT_TRUE(backend.Init().has_value());
        std::unordered_map<std::string, std::vector<Slice>> batch{
            {"first", {Slice{first.data(), first.size()}}},
            {"second", {Slice{second.data(), second.size()}}}};
        ASSERT_TRUE(backend
                        .BatchOffload(batch,
                                      [](const std::vector<std::string>&,
                                         std::vector<StorageObjectMetadata>&) {
                                          return ErrorCode::OK;
                                      })
                        .has_value());
        backend.RemoveAll();
        EXPECT_FALSE(backend.IsExist("first").value());
        EXPECT_FALSE(backend.IsExist("second").value());
    }

    LogStructuredStorageBackend recovered(config);
    ASSERT_TRUE(recovered.Init().has_value());
    EXPECT_FALSE(recovered.IsExist("first").value());
    EXPECT_FALSE(recovered.IsExist("second").value());
    size_t scanned = 0;
    ASSERT_TRUE(recovered
                    .ScanMeta([&](const std::vector<std::string>& keys,
                                  std::vector<StorageObjectMetadata>&) {
                        scanned += keys.size();
                        return ErrorCode::OK;
                    })
                    .has_value());
    EXPECT_EQ(scanned, size_t{0});
}

TEST(LogStructuredStorageBackendTest, ExposesPhysicalStorageStats) {
    BackendTempDirectory temp;
    const auto config = BackendConfig(temp);
    const std::string storage_key =
        TenantId("tenant-stats").MakeScopedKey("key");
    std::string old_value = "old-value";
    std::string new_value = "new-value-with-more-bytes";

    LogStructuredStorageBackend backend(config);
    EXPECT_FALSE(backend.SnapshotStats().has_value());
    ASSERT_TRUE(backend.Init().has_value());
    ASSERT_TRUE(backend
                    .BatchOffload(SingleValueBatch(storage_key, old_value),
                                  [](const std::vector<std::string>&,
                                     std::vector<StorageObjectMetadata>&) {
                                      return ErrorCode::OK;
                                  })
                    .has_value());
    ASSERT_TRUE(backend
                    .BatchOffload(SingleValueBatch(storage_key, new_value),
                                  [](const std::vector<std::string>&,
                                     std::vector<StorageObjectMetadata>&) {
                                      return ErrorCode::OK;
                                  })
                    .has_value());

    const auto stats = backend.SnapshotStats();
    ASSERT_TRUE(stats.has_value());
    EXPECT_EQ(stats->logical_value_bytes, new_value.size());
    EXPECT_GT(stats->live_record_bytes, stats->logical_value_bytes);
    EXPECT_GT(stats->physical_bytes, stats->live_record_bytes);
    EXPECT_EQ(stats->reclaimable_bytes,
              stats->physical_bytes - stats->live_record_bytes);
    EXPECT_EQ(stats->active_segments, 1);
    EXPECT_EQ(stats->sealed_segments, 0);
    EXPECT_EQ(stats->retired_segments, 0);
    EXPECT_GT(stats->wal_sequence, uint64_t{0});
    EXPECT_LE(stats->checkpoint_sequence, stats->wal_sequence);
}

}  // namespace
}  // namespace mooncake

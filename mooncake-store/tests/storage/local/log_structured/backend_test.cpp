#include "storage/local/log_structured/log_structured_backend.h"

#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
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
    setenv("MOONCAKE_LOG_SYNC_POLICY", "batch", 1);
    setenv("MOONCAKE_LOG_CHECKPOINT_INTERVAL", "17", 1);
    setenv("MOONCAKE_LOG_COMPACTION_POLICY", "tiered", 1);
    setenv("MOONCAKE_LOG_COMPACTION_INTERVAL_MS", "25", 1);
    setenv("MOONCAKE_LOG_COMPACTION_FANOUT", "3", 1);
    setenv("MOONCAKE_LOG_COMPACTION_MAX_LEVELS", "5", 1);
    setenv("MOONCAKE_LOG_COMPACTION_MAX_SOURCES", "6", 1);
    setenv("MOONCAKE_LOG_COMPACTION_MAX_BYTES_PER_ROUND", "8192", 1);
    setenv("MOONCAKE_LOG_COMPACTION_MAX_TARGET_BYTES", "16384", 1);
    setenv("MOONCAKE_LOG_COMPACTION_MAX_BYTES_PER_SEC", "32768", 1);
    setenv("MOONCAKE_LOG_COMPACTION_RESERVE_BYTES", "2048", 1);
    setenv("MOONCAKE_LOG_COMPACTION_MIN_RECLAIM_RATIO", "0.35", 1);

    const auto config = LogStructuredBackendConfig::FromEnvironment();

    unsetenv("MOONCAKE_LOG_SEGMENT_SIZE_BYTES");
    unsetenv("MOONCAKE_LOG_SYNC_POLICY");
    unsetenv("MOONCAKE_LOG_CHECKPOINT_INTERVAL");
    unsetenv("MOONCAKE_LOG_COMPACTION_POLICY");
    unsetenv("MOONCAKE_LOG_COMPACTION_INTERVAL_MS");
    unsetenv("MOONCAKE_LOG_COMPACTION_FANOUT");
    unsetenv("MOONCAKE_LOG_COMPACTION_MAX_LEVELS");
    unsetenv("MOONCAKE_LOG_COMPACTION_MAX_SOURCES");
    unsetenv("MOONCAKE_LOG_COMPACTION_MAX_BYTES_PER_ROUND");
    unsetenv("MOONCAKE_LOG_COMPACTION_MAX_TARGET_BYTES");
    unsetenv("MOONCAKE_LOG_COMPACTION_MAX_BYTES_PER_SEC");
    unsetenv("MOONCAKE_LOG_COMPACTION_RESERVE_BYTES");
    unsetenv("MOONCAKE_LOG_COMPACTION_MIN_RECLAIM_RATIO");

    EXPECT_TRUE(config.Validate());
    EXPECT_EQ(config.segment_size_bytes, uint64_t{4096});
    EXPECT_EQ(config.sync_policy, LogStructuredSyncPolicy::kBatch);
    EXPECT_EQ(config.checkpoint_interval_records, uint64_t{17});
    EXPECT_EQ(config.compaction_policy, LogStructuredCompactionPolicy::kTiered);
    EXPECT_EQ(config.compaction_interval_ms, uint64_t{25});
    EXPECT_EQ(config.compaction_fanout, size_t{3});
    EXPECT_EQ(config.compaction_max_levels, uint32_t{5});
    EXPECT_EQ(config.compaction_max_sources, size_t{6});
    EXPECT_EQ(config.compaction_max_bytes_per_round, uint64_t{8192});
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
     DiskWatermarkCompactsGarbageWithoutEvictingLiveKeys) {
    BackendTempDirectory temp;
    auto config = BackendConfig(temp);
    config.total_size_limit = 512;
    LogStructuredBackendConfig backend_config;
    backend_config.segment_size_bytes = 128;
    backend_config.compaction_policy = LogStructuredCompactionPolicy::kNone;
    backend_config.compaction_reserve_bytes = 128;
    backend_config.compaction_max_sources = 8;
    backend_config.compaction_max_bytes_per_round = 4096;
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
    EXPECT_FALSE(backend.IsEnableOffloading().value());

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

    auto evicted = backend.EvictAboveDiskWatermark(0.90, 0.75);
    ASSERT_TRUE(evicted.has_value());
    EXPECT_TRUE(evicted->empty());
    EXPECT_LT(disk_bytes(), before);
    EXPECT_TRUE(backend.IsEnableOffloading().value());

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

}  // namespace
}  // namespace mooncake

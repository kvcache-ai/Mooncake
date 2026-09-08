#include "storage/local/rocksdb/rocksdb_backend.h"

#include <unistd.h>

#include <atomic>
#include <cstdlib>
#include <filesystem>
#include <string>
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
        path_ = base / ("mooncake-rocksdb-backend-test-" +
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
    config.storage_backend_type = StorageBackendType::kRocksDb;
    config.storage_filepath = temp.path().string();
    config.total_keys_limit = 100;
    config.total_size_limit = 16 * 1024 * 1024;
    config.scanmeta_iterator_keys_limit = 1;
    return config;
}

std::unordered_map<std::string, std::vector<Slice>> MakeBatch(
    std::unordered_map<std::string, std::string>& values) {
    std::unordered_map<std::string, std::vector<Slice>> batch;
    for (auto& [key, value] : values) {
        const size_t midpoint = value.size() / 2;
        batch[key] = {Slice{value.data(), midpoint},
                      Slice{value.data() + midpoint, value.size() - midpoint}};
    }
    return batch;
}

TEST(RocksDBStorageBackendTest, BatchLifecycleSurvivesRestartAndRemoveAll) {
    BackendTempDirectory temp;
    const auto config = BackendConfig(temp);
    std::unordered_map<std::string, std::string> values{
        {TenantId("tenant-a").MakeScopedKey("key-1"), "first-payload"},
        {TenantId("tenant-b").MakeScopedKey("key-2"),
         std::string(128 * 1024, 'b')}};

    {
        RocksDBStorageBackend backend(config);
        ASSERT_TRUE(backend.Init().has_value());
        auto written = backend.BatchOffload(
            MakeBatch(values),
            [&](const std::vector<std::string>& keys,
                std::vector<StorageObjectMetadata>& metadatas) {
                EXPECT_EQ(keys.size(), values.size());
                EXPECT_EQ(metadatas.size(), values.size());
                return ErrorCode::OK;
            });
        ASSERT_TRUE(written.has_value());
        EXPECT_EQ(*written, static_cast<int64_t>(values.size()));

        std::unordered_map<std::string, std::string> loaded;
        std::unordered_map<std::string, Slice> slices;
        for (const auto& [key, value] : values) {
            auto [iterator, inserted] =
                loaded.emplace(key, std::string(value.size(), '\0'));
            ASSERT_TRUE(inserted);
            slices.emplace(
                key, Slice{iterator->second.data(), iterator->second.size()});
        }
        ASSERT_TRUE(backend.BatchLoad(slices).has_value());
        EXPECT_EQ(loaded, values);
    }

    RocksDBStorageBackend recovered(config);
    ASSERT_TRUE(recovered.Init().has_value());
    std::unordered_map<std::string, int64_t> scanned;
    ASSERT_TRUE(
        recovered
            .ScanMeta([&](const std::vector<std::string>& keys,
                          std::vector<StorageObjectMetadata>& metadata) {
                EXPECT_EQ(keys.size(), metadata.size());
                for (size_t index = 0; index < keys.size(); ++index) {
                    scanned[keys[index]] = metadata[index].data_size;
                }
                return ErrorCode::OK;
            })
            .has_value());
    ASSERT_EQ(scanned.size(), values.size());
    for (const auto& [key, value] : values) {
        EXPECT_EQ(scanned[key], static_cast<int64_t>(value.size()));
        ASSERT_TRUE(recovered.IsExist(key).has_value());
        EXPECT_TRUE(*recovered.IsExist(key));
    }

    recovered.RemoveAll();
    for (const auto& [key, value] : values) {
        static_cast<void>(value);
        ASSERT_TRUE(recovered.IsExist(key).has_value());
        EXPECT_FALSE(*recovered.IsExist(key));
    }
    size_t scanned_after_remove = 0;
    ASSERT_TRUE(recovered
                    .ScanMeta([&](const std::vector<std::string>& keys,
                                  std::vector<StorageObjectMetadata>&) {
                        scanned_after_remove += keys.size();
                        return ErrorCode::OK;
                    })
                    .has_value());
    EXPECT_EQ(scanned_after_remove, 0);
}

TEST(RocksDBStorageBackendTest, FactoryCreatesEnabledBackend) {
    BackendTempDirectory temp;
    auto backend = CreateStorageBackend(BackendConfig(temp));
    ASSERT_TRUE(backend.has_value());
    ASSERT_TRUE((*backend)->Init().has_value());
}

TEST(RocksDBStorageBackendTest,
     RejectedOverwritePreservesPreviouslyCommittedValue) {
    BackendTempDirectory temp;
    const auto config = BackendConfig(temp);
    const std::string key = TenantId("tenant-a").MakeScopedKey("key");
    std::unordered_map<std::string, std::string> original{{key, "old-value"}};
    std::unordered_map<std::string, std::string> replacement{
        {key, "new-value"}};

    RocksDBStorageBackend backend(config);
    ASSERT_TRUE(backend.Init().has_value());
    ASSERT_TRUE(backend
                    .BatchOffload(MakeBatch(original),
                                  [](const std::vector<std::string>&,
                                     std::vector<StorageObjectMetadata>&) {
                                      return ErrorCode::OK;
                                  })
                    .has_value());
    auto rejected = backend.BatchOffload(
        MakeBatch(replacement), [](const std::vector<std::string>&,
                                   std::vector<StorageObjectMetadata>&) {
            return ErrorCode::INTERNAL_ERROR;
        });
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error(), ErrorCode::INTERNAL_ERROR);

    std::string loaded(original.at(key).size(), '\0');
    std::unordered_map<std::string, Slice> slices{
        {key, Slice{loaded.data(), loaded.size()}}};
    ASSERT_TRUE(backend.BatchLoad(slices).has_value());
    EXPECT_EQ(loaded, original.at(key));
}

}  // namespace
}  // namespace mooncake

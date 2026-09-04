#include "storage/local/log_structured/log_structured_backend.h"

#include <unistd.h>

#include <atomic>
#include <cstdlib>
#include <cstring>
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

}  // namespace
}  // namespace mooncake

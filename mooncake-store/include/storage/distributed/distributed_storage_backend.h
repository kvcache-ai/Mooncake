#pragma once

#include <memory>

#include "fs_adapter.h"
#include "storage_backend.h"

namespace mooncake {

struct DistributedStorageConfig {
    std::string fsdir = "distributed_dir";
    std::string fs_adapter_type = "hf3fs";
    bool enable_health_check = false;
    int hash_bucket_count = 256;

    bool Validate() const;
    static DistributedStorageConfig FromEnvironment();
};

/**
 * @brief Distributed filesystem storage backend.
 *
 * Implements StorageBackendInterface, delegating I/O to a FileSystemAdapter.
 * Does not handle eviction (DFS manages its own space).
 */
class DistributedStorageBackend : public StorageBackendInterface {
   public:
    DistributedStorageBackend(
        const FileStorageConfig& file_storage_config,
        const DistributedStorageConfig& distributed_config,
        std::unique_ptr<FileSystemAdapter> fs_adapter);

    tl::expected<void, ErrorCode> Init() override;

    tl::expected<int64_t, ErrorCode> BatchOffload(
        const std::unordered_map<std::string, std::vector<Slice>>& batch_object,
        std::function<ErrorCode(const std::vector<std::string>& keys,
                                std::vector<StorageObjectMetadata>& metadatas)>
            complete_handler,
        std::function<void(const std::vector<std::string>& evicted_keys)>
            eviction_handler = nullptr) override;

    tl::expected<void, ErrorCode> BatchLoad(
        std::unordered_map<std::string, Slice>& batched_slices) override;

    tl::expected<bool, ErrorCode> IsExist(const std::string& key) override;

    tl::expected<bool, ErrorCode> IsEnableOffloading() override;

    tl::expected<void, ErrorCode> ScanMeta(
        const std::function<ErrorCode(
            const std::vector<std::string>& keys,
            std::vector<StorageObjectMetadata>& metadatas)>& handler) override;

   private:
    std::string GetObjectPath(const std::string& key) const;
    static std::string EscapeFilename(const std::string& key);
    static std::string UnescapeFilename(const std::string& name);

    std::unique_ptr<FileSystemAdapter> fs_adapter_;
    DistributedStorageConfig distributed_config_;
    std::string root_dir_;
    int hash_bucket_count_;
    bool initialized_ = false;
};

}  // namespace mooncake

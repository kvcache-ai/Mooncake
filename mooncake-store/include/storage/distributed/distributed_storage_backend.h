#pragma once

#include <memory>

#include "fs_adapter.h"
#include "storage/adapter/object_storage_adapter.h"
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
 * @brief Distributed filesystem and object storage backend.
 *
 * Implements StorageBackendInterface using either a FileSystemAdapter for
 * per-object files or an ObjectStorageAdapter for object storage services.
 * Does not handle eviction.
 */
class DistributedStorageBackend : public StorageBackendInterface {
   public:
    DistributedStorageBackend(
        const FileStorageConfig& file_storage_config,
        const DistributedStorageConfig& distributed_config,
        std::unique_ptr<FileSystemAdapter> fs_adapter);

    // At least one adapter must be non-null. When object_storage_adapter is
    // provided, the backend uses object storage mode.
    DistributedStorageBackend(
        const FileStorageConfig& file_storage_config,
        const DistributedStorageConfig& distributed_config,
        std::unique_ptr<FileSystemAdapter> fs_adapter,
        std::unique_ptr<ObjectStorageAdapter> object_storage_adapter);

    tl::expected<void, ErrorCode> Init() override;

    tl::expected<int64_t, ErrorCode> BatchOffload(
        const std::unordered_map<std::string, std::vector<Slice>>& batch_object,
        std::function<ErrorCode(const std::vector<std::string>& keys,
                                std::vector<StorageObjectMetadata>& metadatas)>
            complete_handler,
        EvictionHandler eviction_handler = nullptr) override;

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
    std::unique_ptr<ObjectStorageAdapter> object_storage_adapter_;
    DistributedStorageConfig distributed_config_;
    std::string root_dir_;
    int hash_bucket_count_;
    bool use_object_storage_ = false;
    bool initialized_ = false;
};

}  // namespace mooncake

#pragma once

#include <memory>
#include <mutex>
#include <optional>
#include <vector>

#include "dfs_descriptor_cache.h"
#include "fs_adapter.h"
#include "storage_backend.h"

namespace mooncake {

struct DistributedStorageConfig {
    std::string fsdir = "/mnt/3fs/mooncake";
    std::string fs_adapter_type = "hf3fs";
    bool enable_health_check = false;
    int shard_count = 64;
    uint64_t shard_capacity = 4ULL * 1024 * 1024 * 1024;
    uint64_t alignment = 4096;
    bool single_tenant = true;

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
    ~DistributedStorageBackend() override;

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

    void SetDescriptorCache(std::shared_ptr<DfsDescriptorCache> cache) {
        desc_cache_ = std::move(cache);
    }

    std::shared_ptr<DfsDescriptorCache> GetDescriptorCache() const {
        return desc_cache_;
    }

   private:
    struct ShardFile {
        std::string path;
        int fd = -1;
        std::mutex mutex;
    };

    std::optional<DistributedFSDescriptor> LookupDescriptor(
        const std::string& storage_key) const;

    std::unique_ptr<FileSystemAdapter> fs_adapter_;
    std::shared_ptr<DfsDescriptorCache> desc_cache_;
    DistributedStorageConfig distributed_config_;
    std::string root_dir_;
    std::vector<std::unique_ptr<ShardFile>> shard_files_;
    bool initialized_ = false;
};

}  // namespace mooncake

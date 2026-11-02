#pragma once

#include <atomic>
#include <memory>

#include "nvme_kv/object_store.h"
#include "storage_backend.h"

namespace mooncake {

class NvmeKvStorageBackend : public StorageBackendInterface {
   public:
    explicit NvmeKvStorageBackend(
        const FileStorageConfig &file_storage_config_);

    static std::unique_ptr<NvmeKvObjectStore> CreateObjectStore(
        std::unique_ptr<NvmeKvConnector> connector);

    tl::expected<void, ErrorCode> Init() override;

    tl::expected<int64_t, ErrorCode> BatchOffload(
        const std::unordered_map<std::string, std::vector<Slice>> &batch_object,
        std::function<ErrorCode(const std::vector<std::string> &keys,
                                std::vector<StorageObjectMetadata> &metadatas)>
            complete_handler,
        EvictionHandler eviction_handler = nullptr) override;

    tl::expected<void, ErrorCode> BatchLoad(
        std::unordered_map<std::string, Slice> &batched_slices) override;

    tl::expected<bool, ErrorCode> IsExist(const std::string &key) override;
    tl::expected<bool, ErrorCode> IsEnableOffloading() override;

    tl::expected<void, ErrorCode> ScanMeta(
        const std::function<ErrorCode(
            const std::vector<std::string> &keys,
            std::vector<StorageObjectMetadata> &metadatas)> &handler) override;

   private:
    tl::expected<void, ErrorCode> InitDevice();
    std::atomic<bool> initialized_{false};
    std::unique_ptr<NvmeKvObjectStore> object_store_;
    std::atomic<int64_t> total_size_{0};
    std::atomic<int64_t> total_keys_{0};
};

}  // namespace mooncake

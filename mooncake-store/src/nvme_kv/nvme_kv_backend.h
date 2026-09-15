#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "nvme_kv_connector.h"
#include "nvme_kv_key_codec.h"
#include "nvme_kv_key_conflict_policy.h"
#include "nvme_kv_object_layout.h"
#include "rpc_types.h"
#include "storage_backend.h"
#include "thread_pool.h"

namespace mooncake {

class NvmeKvStorageBackend : public StorageBackendInterface {
   public:
    explicit NvmeKvStorageBackend(
        const FileStorageConfig &file_storage_config_);

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
    using PhysicalKey = NvmeKvPhysicalKey;

    struct OffloadResult {
        bool stored = false;
        bool inserted = false;
    };

    struct CachedManifest {
        uint32_t resolved_slot = 0;
        uint32_t payload_size = 0;
        std::vector<NvmeKvManifestChunkRecord> chunk_records;
        std::vector<size_t> chunk_offsets;
    };

    tl::expected<void, ErrorCode> InitDevice();
    tl::expected<OffloadResult, ErrorCode> OffloadOne(
        const std::string &key, const std::vector<Slice> &slices,
        size_t payload_size);
    void StoreBatchParallel(
        std::vector<NvmeKvCommandExecutor::StoreRequest> &requests);
    void RunParallelIo(size_t item_count,
                       const std::function<void(size_t)> &task,
                       size_t max_inflight = 0);
    void RunPipelinedIo(size_t item_count,
                        const std::function<void(size_t)> &io_task,
                        const std::function<void(size_t)> &completion_task);
    std::shared_ptr<const CachedManifest> FindCachedManifest(
        const std::string &key, size_t payload_size) const;
    std::shared_ptr<const CachedManifest> CacheManifest(
        const std::string &key, size_t payload_size, uint32_t resolved_slot,
        const std::vector<NvmeKvManifestChunkRecord> &chunk_records);
    void CacheManifestAfterWrite(
        const std::string &key, size_t payload_size, uint32_t resolved_slot,
        const std::vector<NvmeKvManifestChunkRecord> &chunk_records) noexcept;
    void InitIoWorkers();

    std::atomic<bool> initialized_{false};
    mutable std::shared_mutex manifest_cache_mutex_;
    std::unordered_map<std::string, std::shared_ptr<const CachedManifest>>
        manifest_cache_;
    std::shared_ptr<NvmeKvConnector> connector_;
    std::atomic<int64_t> total_size_{0};
    std::atomic<int64_t> total_keys_{0};
    size_t io_parallelism_ = 1;
    size_t prepare_concurrency_ = 1;
    size_t batch_submit_concurrency_ = 1;
    size_t root_submit_concurrency_ = 1;
    std::unique_ptr<ThreadPool> io_workers_;
    std::unique_ptr<ThreadPool> submit_workers_;
    std::unique_ptr<ThreadPool> root_submit_workers_;
};

}  // namespace mooncake

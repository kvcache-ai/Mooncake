#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "config/distributed_storage_config.h"
#include "fs_adapter.h"
#include "replica.h"
#include "storage/distributed/object_storage_adapter.h"
#include "storage_backend.h"

namespace mooncake {

// Filesystem mode is the descriptor-based shard/offset DFS data path. Object
// storage remains a separate, logical-key-oriented I/O mode.
enum class DistributedStorageMode {
    kFileSystem,
    kObjectStorage,
};

struct DfsWriteRequest {
    std::string key;
    DistributedFSDescriptor descriptor;
    std::vector<Slice> slices;
};

struct DfsReadRequest {
    std::string key;
    DistributedFSDescriptor descriptor;
    std::vector<Slice> slices;
};

/**
 * @brief Distributed filesystem and object storage backend.
 *
 * Uses a FileSystemAdapter for descriptor-based DFS reads and writes, or an
 * ObjectStorageAdapter for whole-object offload operations. Does not handle
 * eviction.
 */
class DistributedStorageBackend : public StorageBackendInterface {
   public:
    DistributedStorageBackend(
        const FileStorageConfig& file_storage_config,
        const DistributedStorageConfig& distributed_config,
        std::unique_ptr<FileSystemAdapter> fs_adapter);
    ~DistributedStorageBackend() override;

    // Exactly one adapter must be non-null.
    DistributedStorageBackend(
        const FileStorageConfig& file_storage_config,
        const DistributedStorageConfig& distributed_config,
        std::unique_ptr<FileSystemAdapter> fs_adapter,
        std::unique_ptr<ObjectStorageAdapter> object_storage_adapter);

    DistributedStorageMode GetStorageMode() const { return storage_mode_; }

    bool UsesObjectStorage() const {
        return storage_mode_ == DistributedStorageMode::kObjectStorage;
    }

    tl::expected<void, ErrorCode> Init() override;

    tl::expected<int64_t, ErrorCode> BatchOffload(
        const std::unordered_map<std::string, std::vector<Slice>>& batch_object,
        std::function<ErrorCode(const std::vector<std::string>& keys,
                                std::vector<StorageObjectMetadata>& metadatas)>
            complete_handler,
        EvictionHandler eviction_handler = nullptr) override;

    std::vector<tl::expected<void, ErrorCode>> BatchWrite(
        const std::vector<DfsWriteRequest>& requests);

    std::vector<tl::expected<void, ErrorCode>> BatchRead(
        const std::vector<DfsReadRequest>& requests);

    // Key-only storage backend operations cannot safely address DFS objects;
    // callers must use BatchRead/BatchWrite with request-scoped descriptors.
    tl::expected<void, ErrorCode> BatchLoad(
        std::unordered_map<std::string, Slice>& batched_slices) override;

    tl::expected<bool, ErrorCode> IsExist(const std::string& key) override;

    tl::expected<bool, ErrorCode> IsEnableOffloading() override;

    tl::expected<void, ErrorCode> ScanMeta(
        const std::function<ErrorCode(
            const std::vector<std::string>& keys,
            std::vector<StorageObjectMetadata>& metadatas)>& handler) override;

    // Upper bound on bucket files one BatchWrite/BatchRead holds open at once.
    static constexpr size_t kMaxOpenBucketsPerBatch = 64;

   private:
    struct ShardFile {
        std::string path;
        int fd = -1;
        std::mutex mutex;
    };
    struct DfsIoOp;

    tl::expected<ShardFile*, ErrorCode> GetOrOpenShard(
        const DistributedFSDescriptor& descriptor);
    tl::expected<int, ErrorCode> OpenBucket(
        const DistributedFSDescriptor& descriptor);
    bool UsesBucketAllocator() const;
    // Preserve per-request bucket handles and shard I/O locking for adapters
    // that do not opt in to batching.
    tl::expected<void, ErrorCode> ExecuteSingleIo(DfsIoOp& op, bool write);
    // Resolve fds for validated ops and submit them through SubmitBatchIo.
    // Both write failures into results[op.index]. Shard fds come from the
    // shard cache; bucket fds are opened per batch, at most
    // kMaxOpenBucketsPerBatch at a time, and closed after their submission.
    void ExecuteShardBatchIo(
        std::vector<DfsIoOp>& ops,
        std::vector<tl::expected<void, ErrorCode>>& results, bool write);
    void ExecuteBucketBatchIo(
        std::vector<DfsIoOp>& ops,
        std::vector<tl::expected<void, ErrorCode>>& results, bool write);
    // Submits io_requests (io_requests[i] belongs to *io_ops[i]) to the
    // adapter as one batch. Short transfers are completed when
    // complete_short_io is set and reported as failures otherwise.
    void SubmitBatchIo(const std::vector<FdIoRequest>& io_requests,
                       const std::vector<DfsIoOp*>& io_ops,
                       std::vector<tl::expected<void, ErrorCode>>& results,
                       bool write, bool complete_short_io);

    std::unique_ptr<FileSystemAdapter> fs_adapter_;
    std::unique_ptr<ObjectStorageAdapter> object_storage_adapter_;
    DistributedStorageConfig distributed_config_;
    std::string root_dir_;
    // Create shard entries only from descriptors published by the master.
    // The cache lock protects lookup/insertion; each shard's mutex protects
    // initialization (fd stays -1 until opening succeeds) and, for adapters
    // without batching, positional I/O. Once set, fd does not change, so
    // adapters that opt in to batching can use it concurrently. Entries are
    // never erased while the backend is running, so callers can retain a
    // ShardFile pointer after releasing the cache lock.
    std::mutex shard_files_mutex_;
    std::unordered_map<int, std::unique_ptr<ShardFile>> shard_files_;
    DistributedStorageMode storage_mode_ = DistributedStorageMode::kFileSystem;
    bool initialized_ = false;
};

}  // namespace mooncake

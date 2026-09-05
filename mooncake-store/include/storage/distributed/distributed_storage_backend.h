#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

#include "config/distributed_storage_config.h"
#include "fs_adapter.h"
#include "replica.h"
#include "storage/distributed/global_allocator_interface.h"
#include "storage/distributed/object_storage_adapter.h"
#include "storage_backend.h"

namespace mooncake {

class ThreadPool;

// Descriptor-based DFS and logical-key object storage remain distinct modes.
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
 * Uses a FileSystemAdapter for descriptor-based DFS I/O or an
 * ObjectStorageAdapter for logical-key offload operations. Does not handle
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

    /**
     * @brief Which DFS space-management mode this backend was configured for.
     *
     * The client uses this to choose between the synchronous SHARD write path
     * and the asynchronous BUCKET one.
     */
    DfsAllocatorType GetAllocatorType() const {
        return distributed_config_.allocator_type;
    }

   private:
    struct ShardFile {
        std::string path;
        int fd = -1;
        // Optional direct (page-cache-bypassing) read handle for the same
        // file; -1 when the adapter does not support direct reads.
        int direct_fd = -1;
        std::mutex mutex;
    };

    // BUCKET mode opens bucket data files on demand and caches the handles.
    // Handles are shared_ptr so an in-flight read/write keeps the fd alive even
    // if the cache entry is dropped (e.g. after the bucket is evicted).
    struct OpenFileHandle {
        std::string path;
        int fd = -1;
        FileSystemAdapter* adapter = nullptr;
        std::mutex mutex;

        ~OpenFileHandle() {
            if (fd >= 0 && adapter != nullptr) {
                (void)adapter->CloseFile(fd);
                fd = -1;
            }
        }
    };

    bool IsBucketMode() const {
        return distributed_config_.allocator_type == DfsAllocatorType::BUCKET;
    }

    /**
     * @brief A validated, ready-to-use target for one DFS request.
     *
     * `fd` and `mutex` are borrowed: in SHARD mode they belong to the
     * long-lived ShardFile, in BUCKET mode to `keepalive`, whose shared_ptr
     * guarantees the fd stays open for the duration of the I/O even if the
     * cache entry is dropped concurrently.
     */
    struct ResolvedTarget {
        int fd = -1;
        std::mutex* mutex = nullptr;
        std::shared_ptr<OpenFileHandle> keepalive;
    };

    /**
     * @brief Resolve `descriptor` to a validated I/O target.
     *
     * SHARD mode validates against the fixed shard table. BUCKET mode
     * canonicalizes the descriptor path, verifies it names the expected bucket
     * data file under the configured DFS root, and then opens/caches it.
     * When `resolved_path` is non-null, BUCKET mode stores the canonical
     * bucket data file path there.
     */
    tl::expected<ResolvedTarget, ErrorCode> ResolveTarget(
        const DistributedFSDescriptor& descriptor, const std::string& key,
        std::string* resolved_path = nullptr);

    tl::expected<std::shared_ptr<OpenFileHandle>, ErrorCode> GetOrOpenBucket(
        const std::string& path);

    /**
     * @brief Open/cache the direct (page-cache-bypassing) read handle for a
     * bucket data file. Returns NOT_SUPPORTED when the adapter has no direct
     * read path; callers then fall back to the regular handle.
     */
    tl::expected<std::shared_ptr<OpenFileHandle>, ErrorCode>
    GetOrOpenBucketDirect(const std::string& path);

    /**
     * @brief One validated BUCKET-mode read, resolved but not yet issued.
     */
    struct PreparedRead {
        size_t request_index = 0;
        ResolvedTarget target;
        uint64_t entry_start = 0;
        uint64_t reserved_size = 0;
    };

    /**
     * @brief Every read of one batch that targets the same bucket data file.
     *
     * Keyed on the handle's mutex rather than its fd: an fd number can be
     * reused once a handle is closed, while the mutex address uniquely
     * identifies the open handle for as long as a PreparedRead in the group
     * keeps it alive through `ResolvedTarget::keepalive`.
     */
    struct BucketReadGroup {
        std::mutex* mutex = nullptr;
        std::vector<PreparedRead> reads;
    };

    /**
     * @brief A run of contiguous entries collapsed into a single read.
     */
    struct MergedIo {
        uint64_t entry_start = 0;
        uint64_t total_size = 0;
        std::vector<const PreparedRead*> reads;
    };

    static ErrorCode ReadFully(FileSystemAdapter* fs_adapter,
                               const ResolvedTarget& target, uint64_t offset,
                               std::span<char> output);

    /// Scatter `value` (object_size bytes) across the request's slices.
    static void CopyToSlices(const DfsReadRequest& request, const char* value);

    /// Consumes `prepared`, bucketing its entries by open file handle.
    static std::unordered_map<std::mutex*, BucketReadGroup> GroupReadsByBucket(
        std::vector<PreparedRead>&& prepared);

    static void SortGroupByOffset(BucketReadGroup& group);

    static std::vector<MergedIo> BuildMergedIos(const BucketReadGroup& group);

    static void ExecuteMergedRead(
        const MergedIo& io, const std::vector<DfsReadRequest>& requests,
        std::vector<tl::expected<void, ErrorCode>>& results,
        const ResolvedTarget& target, std::mutex* mutex,
        FileSystemAdapter* fs_adapter, std::vector<char>& staging);

    static void FailGroupReads(
        const BucketReadGroup& group,
        std::vector<tl::expected<void, ErrorCode>>& results, ErrorCode error);

    static void ProcessBucketGroup(
        BucketReadGroup& group, const std::vector<DfsReadRequest>& requests,
        std::vector<tl::expected<void, ErrorCode>>& results,
        FileSystemAdapter* fs_adapter);

    /**
     * @brief One validated read for the per-key direct flow.
     *
     * `target` already points at the direct read handle when one is
     * available; `mutex` is null for direct handles because read-only,
     * offset-explicit I/O needs no serialization against other reads.
     */
    struct PreparedKeyRead {
        size_t request_index = 0;
        ResolvedTarget target;
        uint64_t value_offset = 0;
    };

    std::vector<tl::expected<void, ErrorCode>> BatchReadDirect(
        const std::vector<DfsReadRequest>& requests);

    /**
     * @brief Read one key's value (object_size bytes at value_offset) straight
     * into the request's slices.
     */
    void ExecuteKeyRead(const PreparedKeyRead& read,
                        const std::vector<DfsReadRequest>& requests,
                        std::vector<tl::expected<void, ErrorCode>>& results);

    static void DispatchParallelReads(
        std::unordered_map<std::mutex*, BucketReadGroup>& groups,
        const std::vector<DfsReadRequest>& requests,
        std::vector<tl::expected<void, ErrorCode>>& results, ThreadPool& pool,
        FileSystemAdapter* fs_adapter);

    std::unique_ptr<FileSystemAdapter> fs_adapter_;
    std::unique_ptr<ObjectStorageAdapter> object_storage_adapter_;
    DistributedStorageConfig distributed_config_;
    std::string root_dir_;
    // Canonical form of root_dir_, used to reject descriptor paths that try to
    // escape the configured DFS root.
    std::string canonical_root_dir_;
    std::vector<std::unique_ptr<ShardFile>> shard_files_;

    mutable std::mutex bucket_cache_mutex_;
    std::unordered_map<std::string, std::shared_ptr<OpenFileHandle>>
        bucket_cache_;
    // Direct read handles for bucket data files, keyed by canonical path.
    // Guarded by bucket_cache_mutex_ alongside bucket_cache_.
    std::unordered_map<std::string, std::shared_ptr<OpenFileHandle>>
        bucket_direct_cache_;
    std::unique_ptr<ThreadPool> batch_read_pool_;

    DistributedStorageMode storage_mode_ = DistributedStorageMode::kFileSystem;
    bool initialized_ = false;
};

}  // namespace mooncake

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
     */
    tl::expected<ResolvedTarget, ErrorCode> ResolveTarget(
        const DistributedFSDescriptor& descriptor, const std::string& key,
        bool read_only = false);

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
     * @brief One contiguous read scheduled as a single task.
     *
     * `entries` references the requests covered by this task. For direct,
     * unmerged reads it contains one entry; for merged bucket reads it may
     * contain several entries that share one contiguous on-disk run.
     */
    struct ReadEntry {
        size_t request_index = 0;
        uint64_t value_offset = 0;
    };

    struct ReadTask {
        ResolvedTarget target;
        uint64_t io_offset = 0;
        uint64_t total_size = 0;
        bool merged = false;
        bool direct_read = false;
        std::vector<ReadEntry> entries;
    };

    static ErrorCode ReadFully(FileSystemAdapter* fs_adapter,
                               const ResolvedTarget& target, uint64_t offset,
                               std::span<char> output, bool direct_read);

    static void CopyToSlices(const DfsReadRequest& request, const char* value);

    std::vector<ReadTask> PrepareReadTasks(
        const std::vector<DfsReadRequest>& requests,
        std::vector<tl::expected<void, ErrorCode>>& results);

    void ExecuteReadTask(const ReadTask& task,
                         const std::vector<DfsReadRequest>& requests,
                         std::vector<tl::expected<void, ErrorCode>>& results);

    void ExecuteSingleReadTask(
        const ReadTask& task, const std::vector<DfsReadRequest>& requests,
        std::vector<tl::expected<void, ErrorCode>>& results);

    void ExecuteMergedReadTask(
        const ReadTask& task, const std::vector<DfsReadRequest>& requests,
        std::vector<tl::expected<void, ErrorCode>>& results);

    void ExecuteReadTasks(
        const std::vector<ReadTask>& tasks,
        const std::vector<DfsReadRequest>& requests,
        std::vector<tl::expected<void, ErrorCode>>& results);

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

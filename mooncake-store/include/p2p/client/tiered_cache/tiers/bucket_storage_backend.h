#pragma once

// Temporary P2P V1 storage implementation, retained for the V1 rollout.
// Remove this backend and its V1-only callers/tests after V2 is validated and
// replaces V1 in production. Its bucket format is not a V2 dependency.

#include <atomic>
#include <condition_variable>
#include <deque>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common_types.h"
#include "file_interface.h"
#include "mutex.h"

namespace mooncake::v1 {

// V1's bucket format and accounting are independent of centralized offload.
struct BucketObjectMetadata {
    int64_t offset;
    int64_t key_size;
    int64_t data_size;
};
YLT_REFL(BucketObjectMetadata, offset, key_size, data_size);

struct BucketMetadata {
    int64_t meta_size;
    int64_t data_size;
    std::vector<std::string> keys;
    std::vector<BucketObjectMetadata> metadatas;
};
YLT_REFL(BucketMetadata, data_size, keys, metadatas);

struct OffloadMetadata {
    int64_t total_keys;
    int64_t total_size;
    OffloadMetadata(std::size_t keys, int64_t size)
        : total_keys(keys), total_size(size) {}
};

struct BucketStorageConfig {
    std::string storage_filepath = "/data/file_storage";
    int64_t total_keys_limit = 10'000'000;
    int64_t total_size_limit = 2LL * 1024 * 1024 * 1024 * 1024;
    int64_t bucket_size_limit = 256 * 1024 * 1024;
    int64_t bucket_keys_limit = 500;
};

// A snapshot of storage facts; StorageTier owns the eviction policy.
struct BucketStats {
    int64_t bucket_id;
    size_t total_keys;
    size_t valid_keys;
};

class LocalStorageSpaceManager {
   public:
    explicit LocalStorageSpaceManager(std::filesystem::path storage_root);
    tl::expected<void, ErrorCode> Init(uint64_t used_space_bytes);
    tl::expected<bool, ErrorCode> HasPhysicalSpace(
        uint64_t required_size) const;
    bool IsInitialized() const;
    bool TryReserve(uint64_t required_size);
    void Release(uint64_t size_to_release);
    uint64_t AvailableSpace() const;

   private:
    void RecalculateAvailableSpaceLocked();

    std::filesystem::path storage_root_;
    mutable std::shared_mutex mutex_;
    uint64_t total_space_ = 0;
    uint64_t used_space_ = 0;
    uint64_t available_space_ = 0;
    std::atomic<bool> initialized_{false};
};

class BucketIdGenerator {
   public:
    explicit BucketIdGenerator(int64_t start);
    int64_t NextId();
    int64_t CurrentId();
    static constexpr int64_t INIT_NEW_START_ID = -1;

   private:
    static constexpr int TIMESTAMP_SHIFT = 12;
    static constexpr int SEQUENCE_ID_SHIFT = 0;
    std::atomic<int64_t> current_id_;
};

class BucketStorageBackend {
   public:
    explicit BucketStorageBackend(const BucketStorageConfig& config);
    ~BucketStorageBackend();

    tl::expected<void, ErrorCode> Init();
    tl::expected<int64_t, ErrorCode> BatchOffload(
        const std::unordered_map<std::string, std::vector<Slice>>&
            batch_object);
    tl::expected<void, ErrorCode> BatchLoad(
        const std::unordered_map<std::string, Slice>& batched_slices);
    tl::expected<bool, ErrorCode> IsExist(const std::string& key);
    tl::expected<bool, ErrorCode> IsEnableOffloading();
    tl::expected<void, ErrorCode> GetBucketKeys(
        int64_t bucket_id, std::vector<std::string>& bucket_keys);
    tl::expected<OffloadMetadata, ErrorCode> GetStoreMetadata();
    std::vector<BucketStats> GetBucketStats() const;
    tl::expected<void, ErrorCode> MarkKeyDeleted(const std::string& key);
    // Returns live value bytes removed; physical deletion remains asynchronous.
    tl::expected<size_t, ErrorCode> EvictBucket(int64_t bucket_id);

   private:
    enum class FileMode { Read, Write };

    tl::expected<std::shared_ptr<BucketMetadata>, ErrorCode> BuildBucket(
        int64_t bucket_id,
        const std::unordered_map<std::string, std::vector<Slice>>& batch_object,
        std::vector<iovec>& iovs,
        std::vector<StorageObjectMetadata>& metadatas);
    tl::expected<void, ErrorCode> WriteBucket(
        int64_t bucket_id, std::shared_ptr<BucketMetadata> bucket_metadata,
        std::vector<iovec>& iovs);
    tl::expected<void, ErrorCode> StoreBucketMetadata(
        const std::string& metadata_path,
        const std::string& serialized_metadata);
    tl::expected<std::string, ErrorCode> SerializeBucketMetadata(
        const std::shared_ptr<BucketMetadata>& bucket_metadata);
    tl::expected<void, ErrorCode> LoadBucketMetadata(
        int64_t bucket_id, std::shared_ptr<BucketMetadata> bucket_metadata);
    tl::expected<void, ErrorCode> BatchLoadBucket(
        int64_t bucket_id, const std::vector<std::string>& keys,
        const std::vector<StorageObjectMetadata>& metadatas,
        const std::unordered_map<std::string, Slice>& batched_slices);
    tl::expected<std::string, ErrorCode> GetBucketMetadataPath(
        int64_t bucket_id);
    tl::expected<std::string, ErrorCode> GetBucketDataPath(int64_t bucket_id);
    tl::expected<std::unique_ptr<StorageFile>, ErrorCode> OpenFile(
        const std::string& path, FileMode mode) const;

    struct PendingBucketDeletion {
        int64_t bucket_id;
        uint64_t data_bytes;
        uint64_t meta_bytes;
        uint64_t queued_bytes;
        std::string data_path;
        std::string meta_path;
    };

    tl::expected<bool, ErrorCode> CanAcceptAnotherBucket() const;
    uint64_t MaxPendingDeletionBytes() const;
    void StartDeletionWorker();
    void StopDeletionWorker();
    void EnqueueBucketDeletion(PendingBucketDeletion task);
    void BucketDeletionWorker();

    std::atomic<bool> initialized_{false};
    std::optional<BucketIdGenerator> bucket_id_generator_;
    static constexpr const char* BUCKET_DATA_FILE_SUFFIX = ".bucket";
    static constexpr const char* BUCKET_METADATA_FILE_SUFFIX = ".meta";
    mutable SharedMutex mutex_;
    std::string storage_path_;
    BucketStorageConfig config_;
    int64_t physical_used_bytes_ GUARDED_BY(mutex_) = 0;
    std::unordered_map<std::string, StorageObjectMetadata> GUARDED_BY(mutex_)
        object_bucket_map_;
    std::map<int64_t, std::shared_ptr<BucketMetadata>> GUARDED_BY(
        mutex_) buckets_;
    std::unordered_map<int64_t, int> GUARDED_BY(mutex_) bucket_valid_keys_;
    LocalStorageSpaceManager space_manager_;

    std::mutex deletion_mutex_;
    std::condition_variable deletion_cv_;
    std::deque<PendingBucketDeletion> pending_bucket_deletions_;
    std::thread deletion_thread_;
    bool stop_deletion_worker_ = false;
    std::atomic<uint64_t> pending_deletion_bytes_{0};
    std::atomic<uint64_t> pending_deletion_count_{0};
};

}  // namespace mooncake::v1

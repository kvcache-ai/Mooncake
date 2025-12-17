#pragma once

#include "client_service.h"
#include "client_buffer.hpp"
#include "storage_backend.h"

namespace mooncake {

struct FileStorageConfig {
    // Path where data files are stored on disk
    std::string storage_filepath = "/data/file_storage";

    // Size of the local client-side buffer (used for caching or batching)
    int64_t local_buffer_size = 1280 * 1024 * 1024;  // ~1.2 GB

    // Limits for scanning and iteration operations
    int64_t bucket_iterator_keys_limit =
        20000;  // Max number of keys returned per Scan call
    int64_t bucket_keys_limit =
        500;  // Max number of keys allowed in a single bucket
    int64_t bucket_size_limit =
        256 * 1024 * 1024;  // Max total size of a single bucket (256 MB)

    // Global limits across all buckets
    int64_t total_keys_limit = 10'000'000;  // Maximum total number of keys
    int64_t total_size_limit =
        2ULL * 1024 * 1024 * 1024 * 1024;  // Maximum total storage size (2 TB)

    // Interval between heartbeats sent to the control plane (in seconds)
    uint32_t heartbeat_interval_seconds = 10;

    // Validates the configuration for correctness and consistency
    bool Validate() const;

    /**
     * @brief Creates a config instance by reading values from environment
     * variables.
     *
     * Uses default values if environment variables are not set or invalid.
     * This is a static factory method for easy configuration loading.
     *
     * @return FileStorageConfig with values from env or defaults
     */
    static FileStorageConfig FromEnvironment();

    static std::string GetEnvStringOr(const char* name,
                                      const std::string& default_value);

    template <typename T>
    static T GetEnvOr(const char* name, T default_value);
};

class BucketIterator {
   public:
    BucketIterator(std::shared_ptr<BucketStorageBackend> storage_backend,
                   int64_t limit);

    tl::expected<void, ErrorCode> HandleNext(
        const std::function<
            ErrorCode(const std::vector<std::string>& keys,
                      std::vector<StorageObjectMetadata>& metadatas,
                      const std::vector<int64_t>& buckets)>& handler);

    tl::expected<bool, ErrorCode> HasNext();

   private:
    std::shared_ptr<BucketStorageBackend> storage_backend_;
    int64_t limit_;
    mutable Mutex mutex_;
    int64_t GUARDED_BY(mutex_) next_bucket_ = -1;
};

class FileStorage {
   public:
    FileStorage(std::shared_ptr<Client> client,
                const std::string& local_rpc_addr,
                const FileStorageConfig& config);
    ~FileStorage();

    tl::expected<void, ErrorCode> Init();

    /**
     * @brief Reads multiple key-value (KV) entries from local storage and
     * forwards them to a remote node.
     * @param transfer_engine_addr Address of the remote transfer engine
     * (format: "ip:port")
     * @param keys                 List of keys to read from the local KV store
     * @param pointers             Array of remote memory base addresses (on the
     * destination node) where each corresponding value will be written
     * @param sizes                Expected size in bytes for each value
     * @return tl::expected<void, ErrorCode> indicating operation status.
     */
    tl::expected<void, ErrorCode> BatchGet(
        const std::string& transfer_engine_addr,
        const std::vector<std::string>& keys,
        const std::vector<uintptr_t>& pointers,
        const std::vector<int64_t>& sizes);

   private:
    friend class FileStorageTest;
    struct AllocatedBatch {
        std::vector<BufferHandle> handles;
        std::unordered_map<std::string, Slice> slices;

        AllocatedBatch() = default;
        AllocatedBatch(AllocatedBatch&&) = default;
        AllocatedBatch& operator=(AllocatedBatch&&) = default;

        AllocatedBatch(const AllocatedBatch&) = delete;
        AllocatedBatch& operator=(const AllocatedBatch&) = delete;

        ~AllocatedBatch() = default;
    };

    /**
     * @brief Offload object data and metadata.
     * @return tl::expected<void, ErrorCode> indicating operation status.
     */
    tl::expected<void, ErrorCode> OffloadObjects(
        const std::unordered_map<std::string, int64_t>& offloading_objects);

    /**
     * @brief Groups offloading keys into buckets based on size and existence
     * checks.
     * @param offloading_objects Input map of object keys and their sizes
     * (e.g., byte size).
     * @param buckets_keys Output parameter: receives a 2D vector where:
     *                     - Each outer element represents a bucket.
     *                     - Each inner vector contains the newly allocated
     * object keys within that bucket.
     * @return tl::expected<void, ErrorCode> indicating operation status.
     */
    tl::expected<void, ErrorCode> GroupOffloadingKeysByBucket(
        const std::unordered_map<std::string, int64_t>& offloading_objects,
        std::vector<std::vector<std::string>>& buckets_keys);

    /**
     * @brief Performs a heartbeat operation for the FileStorage component.
     * 1. Sends object status (e.g., access frequency, size) to the master via
     * client.
     * 2. Receives feedback on which objects should be offloaded.
     * 3. Triggers asynchronous offloading of pending objects.
     * @return tl::expected<void, ErrorCode> indicating operation status.
     */
    tl::expected<void, ErrorCode> Heartbeat();

    tl::expected<bool, ErrorCode> IsEnableOffloading();

    tl::expected<void, ErrorCode> BatchOffload(
        const std::vector<std::string>& keys);

    tl::expected<void, ErrorCode> BatchLoad(
        const std::unordered_map<std::string, Slice>& batch_object);

    tl::expected<void, ErrorCode> BatchQuerySegmentSlices(
        const std::vector<std::string>& keys,
        std::unordered_map<std::string, std::vector<Slice>>& batched_slices);

    tl::expected<void, ErrorCode> RegisterLocalMemory();

    tl::expected<AllocatedBatch, ErrorCode> AllocateBatch(
        const std::vector<std::string>& keys,
        const std::vector<int64_t>& sizes);

    std::shared_ptr<Client> client_;
    std::string local_rpc_addr_;
    FileStorageConfig config_;
    std::shared_ptr<BucketStorageBackend> storage_backend_;
    std::shared_ptr<ClientBufferAllocator> client_buffer_allocator_;

    mutable Mutex offloading_mutex_;
    std::unordered_map<std::string, int64_t> GUARDED_BY(offloading_mutex_)
        ungrouped_offloading_objects_;
    bool GUARDED_BY(offloading_mutex_) enable_offloading_;
    std::atomic<bool> heartbeat_running_;
    std::thread heartbeat_thread_;
};

}  // namespace mooncake
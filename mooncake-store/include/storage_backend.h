#pragma once

#include <glog/logging.h>

#include <atomic>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "file_interface.h"
#include "mutex.h"
#include "offset_allocator/offset_allocator.hpp"
#include "types.h"

namespace mooncake {
struct FileRecord {
    std::string path;
    uint64_t size;
};

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

    // Runtime-only fields (not serialized) for safe deletion support
    // Tracks number of in-flight reads to enable safe bucket deletion
    mutable std::atomic<int32_t> inflight_reads_{0};

    // Default constructor
    BucketMetadata() = default;

    // Copy constructor (atomic not copyable, so reset to 0)
    BucketMetadata(const BucketMetadata& other)
        : meta_size(other.meta_size),
          data_size(other.data_size),
          keys(other.keys),
          metadatas(other.metadatas),
          inflight_reads_(0) {}

    // Move constructor
    BucketMetadata(BucketMetadata&& other) noexcept
        : meta_size(other.meta_size),
          data_size(other.data_size),
          keys(std::move(other.keys)),
          metadatas(std::move(other.metadatas)),
          inflight_reads_(0) {}

    // Copy assignment
    BucketMetadata& operator=(const BucketMetadata& other) {
        if (this != &other) {
            meta_size = other.meta_size;
            data_size = other.data_size;
            keys = other.keys;
            metadatas = other.metadatas;
            // Don't copy inflight_reads_ - it's runtime state
        }
        return *this;
    }

    // Move assignment
    BucketMetadata& operator=(BucketMetadata&& other) noexcept {
        if (this != &other) {
            meta_size = other.meta_size;
            data_size = other.data_size;
            keys = std::move(other.keys);
            metadatas = std::move(other.metadatas);
            // Don't move inflight_reads_ - it's runtime state
        }
        return *this;
    }
};
YLT_REFL(BucketMetadata, data_size, keys, metadatas);

/**
 * @brief RAII guard for tracking in-flight bucket reads.
 *
 * Increments inflight_reads_ on construction, decrements on destruction.
 * This enables safe bucket deletion by waiting for all in-flight reads
 * to complete before deleting bucket files.
 *
 * Usage:
 *   auto guard = BucketReadGuard(bucket_metadata_ptr);
 *   // ... perform IO ...
 *   // guard destructor decrements counter
 */
class BucketReadGuard {
   public:
    explicit BucketReadGuard(std::shared_ptr<BucketMetadata> bucket)
        : bucket_(std::move(bucket)) {
        if (bucket_) {
            bucket_->inflight_reads_.fetch_add(1, std::memory_order_relaxed);
        }
    }

    ~BucketReadGuard() {
        if (bucket_) {
            bucket_->inflight_reads_.fetch_sub(1, std::memory_order_release);
        }
    }

    // Non-copyable
    BucketReadGuard(const BucketReadGuard&) = delete;
    BucketReadGuard& operator=(const BucketReadGuard&) = delete;

    // Movable
    BucketReadGuard(BucketReadGuard&& other) noexcept
        : bucket_(std::move(other.bucket_)) {
        other.bucket_ = nullptr;
    }

    BucketReadGuard& operator=(BucketReadGuard&& other) noexcept {
        if (this != &other) {
            // Release current bucket if any
            if (bucket_) {
                bucket_->inflight_reads_.fetch_sub(1,
                                                   std::memory_order_release);
            }
            bucket_ = std::move(other.bucket_);
            other.bucket_ = nullptr;
        }
        return *this;
    }

    const std::shared_ptr<BucketMetadata>& get() const { return bucket_; }

   private:
    std::shared_ptr<BucketMetadata> bucket_;
};

struct OffloadMetadata {
    int64_t total_keys;
    int64_t total_size;
    OffloadMetadata(std::size_t keys, int64_t size)
        : total_keys(keys), total_size(size) {}
};

enum class FileMode { Read, Write };

enum class StorageBackendType { kFilePerKey, kBucket, kOffsetAllocator };

static constexpr size_t kKB = 1024;
static constexpr size_t kMB = kKB * 1024;
static constexpr size_t kGB = kMB * 1024;

struct FilePerKeyConfig {
    std::string fsdir = "file_per_key_dir";  // Subdirectory name

    bool enable_eviction = true;  // Enable eviction for storage

    bool Validate() const;

    static FilePerKeyConfig FromEnvironment();
};

struct BucketBackendConfig {
    int64_t bucket_size_limit =
        256 * kMB;  // Max total size of a single bucket (256 MB)

    int64_t bucket_keys_limit = 500;  // Max number of keys allowed in a single
                                      // bucket, required by bucket backend only
    bool Validate() const;

    static BucketBackendConfig FromEnvironment();
};

struct FileStorageConfig {
    // type of the storage backend
    StorageBackendType storage_backend_type = StorageBackendType::kBucket;

    // Path where data files are stored on disk
    std::string storage_filepath = "/data/file_storage";

    // Size of the local client-side buffer (used for caching or batching)
    int64_t local_buffer_size = 1280 * kMB;  // ~1.2 GB

    // Limits for scanning and iteration operations
    int64_t scanmeta_iterator_keys_limit =
        20000;  // Max number of keys returned per Scan call, required by bucket
                // backend only
    // Global limits across all buckets
    int64_t total_keys_limit = 10'000'000;  // Maximum total number of keys
    int64_t total_size_limit =
        2ULL * 1024 * 1024 * 1024 * 1024;  // Maximum total storage size (2 TB)

    // Interval between heartbeats sent to the control plane (in seconds)
    uint32_t heartbeat_interval_seconds = 10;

    // Interval between client_buffer_gc (in seconds)
    uint32_t client_buffer_gc_interval_seconds = 1;
    uint64_t client_buffer_gc_ttl_ms = 5000;

    // Validates the configuration for correctness and consistency
    bool Validate() const;

    bool ValidatePath(std::string path) const;

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
};

class StorageBackendInterface {
   public:
    StorageBackendInterface(const FileStorageConfig& file_storage_config);

    virtual tl::expected<void, ErrorCode> Init() = 0;

    virtual tl::expected<int64_t, ErrorCode> BatchOffload(
        const std::unordered_map<std::string, std::vector<Slice>>& batch_object,
        std::function<ErrorCode(const std::vector<std::string>& keys,
                                std::vector<StorageObjectMetadata>& metadatas)>
            complete_handler) = 0;

    virtual tl::expected<void, ErrorCode> BatchLoad(
        const std::unordered_map<std::string, Slice>& batched_slices) = 0;

    virtual tl::expected<bool, ErrorCode> IsExist(const std::string& key) = 0;

    virtual tl::expected<bool, ErrorCode> IsEnableOffloading() = 0;

    virtual tl::expected<void, ErrorCode> ScanMeta(
        const std::function<ErrorCode(
            const std::vector<std::string>& keys,
            std::vector<StorageObjectMetadata>& metadatas)>& handler) = 0;

    // Test-only: Set predicate to force failures for specific keys in
    // BatchOffload. Default implementation does nothing (no failures injected).
    // Concrete backends can override to provide test failure injection.
    // Returns true if the key should fail, false otherwise.
    virtual void SetTestFailurePredicate(
        std::function<bool(const std::string& key)> /* predicate */) {
        // Default: no-op (no test failures injected)
    }

    FileStorageConfig file_storage_config_;
};

/**
 * @class StorageBackend
 * @brief Implementation of StorageBackend interface using local filesystem
 * storage.
 *
 * Provides thread-safe operations for storing and retrieving objects in a
 * directory hierarchy.
 */
class StorageBackend {
   public:
/**
 * @brief Constructs a new StorageBackend instance
 * @param root_dir Root directory path for object storage
 * @param fsdir  subdirectory name
 * @note Directory existence is not checked in constructor
 */
#ifdef USE_3FS
    explicit StorageBackend(const std::string& root_dir,
                            const std::string& fsdir, bool is_3fs_dir,
                            bool enable_eviction = true)
        : root_dir_(root_dir),
          fsdir_(fsdir),
          is_3fs_dir_(is_3fs_dir),
          enable_eviction_(enable_eviction) {
        resource_manager_ = std::make_unique<USRBIOResourceManager>();
        Hf3fsConfig config;
        config.mount_root = root_dir;
        resource_manager_->setDefaultParams(config);
    }
#else
    explicit StorageBackend(const std::string& root_dir,
                            const std::string& fsdir,
                            bool enable_eviction = true)
        : root_dir_(root_dir),
          fsdir_(fsdir),
          enable_eviction_(enable_eviction) {}
#endif

    /**
     * @brief Factory method to create a StorageBackend instance
     * @param root_dir Root directory path for object storage
     * @param fsdir  subdirectory name
     * @param enable_eviction Whether to enable disk eviction feature (default:
     * true) Note: Eviction is automatically disabled for 3FS mode
     * @return shared_ptr to new instance or nullptr if directory is invalid
     *
     * Performs validation of the root directory before creating the instance:
     * - Verifies directory exists
     * - Verifies path is actually a directory
     */
    static std::shared_ptr<StorageBackend> Create(const std::string& root_dir,
                                                  const std::string& fsdir,
                                                  bool enable_eviction = true) {
        namespace fs = std::filesystem;
        if (!fs::exists(root_dir)) {
            LOG(INFO) << "Root directory does not exist: " << root_dir;
            return nullptr;
        } else if (!fs::is_directory(root_dir)) {
            LOG(INFO) << "Root path is not a directory: " << root_dir;
            return nullptr;
        } else if (fsdir.empty()) {
            LOG(INFO) << "FSDIR cannot be empty";
            return nullptr;
        }

        fs::path root_path(root_dir);

        std::string real_fsdir = "moon_" + fsdir;
#ifdef USE_3FS
        bool is_3fs_dir = fs::exists(root_path / "3fs-virt") &&
                          fs::is_directory(root_path / "3fs-virt");
        return std::make_shared<StorageBackend>(root_dir, real_fsdir,
                                                is_3fs_dir, enable_eviction);
#else
        return std::make_shared<StorageBackend>(root_dir, real_fsdir,
                                                enable_eviction);
#endif
    }

    /**
     * @brief Initializes the storage backend.
     *
     * This method scans the storage directory to build its internal state.
     *
     * Idempotency: This method is idempotent; calling it multiple times has the
     * same effect as calling it once.
     *
     * Existing files: If there are existing files in the storage directory,
     * they will be scanned and incorporated into the internal state. No files
     * are deleted or overwritten during initialization.
     *
     * Thread-safety: This method is not thread-safe and should not be called
     * concurrently from multiple threads. It is recommended to call Init() from
     * a single thread before performing any other operations.
     *
     * Initialization requirement: Init() must be called after construction and
     * before any other operations. Using other methods before successful
     * initialization may result in undefined behavior.
     * @param quota_bytes Quota for the storage backend
     * @return tl::expected<void, ErrorCode> indicating operation status.
     */
    tl::expected<void, ErrorCode> Init(uint64_t quota_bytes);

    /**
     * @brief Evict files for satisfying quota limitation
     * @return bool indicating whether quota is satisfied
     */
    bool InitQuotaEvict();

    /**
     * @brief Stores an object composed of multiple slices
     * @param path path for the object
     * @param slices Vector of data slices to store
     * @return tl::expected<void, ErrorCode> indicating operation status
     */
    tl::expected<void, ErrorCode> StoreObject(const std::string& path,
                                              const std::vector<Slice>& slices);

    /**
     * @brief Stores an object from a string
     * @param path path for the object
     * @param str String containing object data
     * @return tl::expected<void, ErrorCode> indicating operation status
     */
    tl::expected<void, ErrorCode> StoreObject(const std::string& path,
                                              const std::string& str);

    /**
     * @brief Stores an object from a span of data
     * @param path path for the object
     * @param data Span containing object data
     * @return tl::expected<void, ErrorCode> indicating operation status
     */
    tl::expected<void, ErrorCode> StoreObject(const std::string& path,
                                              std::span<const char> data);

    /**
     * @brief Loads an object into slices
     * @param path path for the object
     * @param slices Output vector for loaded data slices
     * @param length Expected length of data to read
     * @return tl::expected<void, ErrorCode> indicating operation status
     */
    tl::expected<void, ErrorCode> LoadObject(const std::string& path,
                                             std::vector<Slice>& slices,
                                             int64_t length);

    /**
     * @brief Loads an object as a string
     * @param path path for the object
     * @param str Output string for loaded data
     * @param length Expected length of data to read
     * @return tl::expected<void, ErrorCode> indicating operation status
     */
    tl::expected<void, ErrorCode> LoadObject(const std::string& path,
                                             std::string& str, int64_t length);

    /**
     * @brief Deletes the physical file associated with the given object key
     * @param path Path to the file to remove
     */
    void RemoveFile(const std::string& path);

    /**
     * @brief Removes objects from the storage backend whose keys match a regex
     * pattern.
     * @param regex The regular expression string to match against object keys.
     * @return An expected object containing the number of removed objects on
     * success, or an ErrorCode on failure.
     */
    void RemoveByRegex(const std::string& key);

    /**
     * @brief Deletes all objects from the storage backend
     *
     * Removes all files in the cluster subdirectory.
     */
    void RemoveAll();

    enum class FileMode { Read, Write };
    // Root directory path for storage and  subdirectory name
    std::string root_dir_;
    std::string fsdir_;
    bool enable_eviction_{
        true};  // User-configurable flag to enable/disable eviction

#ifdef USE_3FS
    bool is_3fs_dir_{false};  // Flag to indicate if the storage is using 3FS
                              // directory structure
    std::unique_ptr<USRBIOResourceManager> resource_manager_;
#endif

   private:
    // File write queue for disk eviction - tracks files in FIFO order
    std::list<FileRecord> file_write_queue_;
    std::unordered_map<std::string, std::list<FileRecord>::iterator>
        file_queue_map_;
    mutable std::shared_mutex
        file_queue_mutex_;  // Mutex to protect file queue operations

    // Storage space tracking variables
    mutable std::shared_mutex
        space_mutex_;               // Mutex to protect space tracking variables
    uint64_t total_space_ = 0;      // Total storage space in bytes
    uint64_t used_space_ = 0;       // Used storage space in bytes
    uint64_t available_space_ = 0;  // Available storage space in bytes

    std::atomic<bool> initialized_{false};

    /**
     * @brief Make sure the path is valid and create necessary directories
     */
    void ResolvePath(const std::string& path) const;

    /**
     * @brief Creates a file object for the specified path and mode
     * @param path Filesystem path for the file
     * @param mode File access mode (read/write)
     * @return Unique pointer to the created StorageFile, or nullptr on failure
     */
    std::unique_ptr<StorageFile> create_file(const std::string& path,
                                             FileMode mode) const;

    /**
     * @brief Evicts a file based on FIFO order (earliest written first out)
     * @return Path of the evicted file, or empty string if no file was evicted
     */
    std::string EvictFile();

    /**
     * @brief Add file to write queue for FIFO tracking
     * @param path Path of the file to add to queue
     * @param size Size of the file
     */
    void AddFileToWriteQueue(const std::string& path, uint64_t size);

    /**
     * @brief Remove file from write queue
     * @param path Path of the file to remove from queue
     */
    void RemoveFileFromWriteQueue(const std::string& path);

    /**
     * @brief Checks if there is enough disk space for a write operation
     * @param required_size Size required for the write operation
     * @return true if there is enough space, false otherwise
     */
    bool CheckDiskSpace(size_t required_size);

    /**
     * @brief Select a file to evict based on FIFO order (earliest written
     * first)
     * @return The file to evict, or empty structure if no file found
     */
    FileRecord SelectFileToEvictByFIFO();

    /**
     * @brief Ensures that a specified amount of disk space is available,
     * performing evictions if necessary.
     *
     * @param required_size The amount of disk space, in bytes, required for the
     *                      upcoming write operation.
     * @return tl::expected<void, ErrorCode> Returns void on success, or
     *         ErrorCode::FILE_WRITE_FAIL if insufficient space remains after
     *         attempting evictions up to the maximum attempt limit.
     */
    tl::expected<void, ErrorCode> EnsureDiskSpace(size_t required_size);

    /**
     * @brief Releases a specified amount of disk space and updates internal
     * accounting.
     * @param size_to_release The amount of space, in bytes, to be released.
     */
    void ReleaseSpace(uint64_t size_to_release);

    /**
     * @brief Recalculates available_space_ based on total_space_ and
     * used_space_. Must be called with space_mutex_ locked.
     */
    void RecalculateAvailableSpace();

    /**
     * @brief Gets the actual filesystem directory name by removing "moon_"
     * prefix if present.
     * @return The actual directory name without "moon_" prefix.
     */
    std::string GetActualFsdir() const;

    /**
     * @brief Checks if disk eviction is enabled for this storage backend.
     * @return true if eviction is enabled (local mode), false if disabled (3FS
     * mode).
     */
    bool IsEvictionEnabled() const;

    /**
     * @brief Helper: Creates a file for writing and handles errors
     * @param path File path
     * @param reserved_size Size already reserved (for eviction mode). If 0, no
     * reservation was made.
     * @return File handle or error
     */
    tl::expected<std::unique_ptr<StorageFile>, ErrorCode> CreateFileForWriting(
        const std::string& path, uint64_t reserved_size = 0);

    /**
     * @brief Helper: Writes data using vector_write and handles errors
     * @param file File handle
     * @param path File path (for error messages)
     * @param slices Data slices to write
     * @param reserved_size Size already reserved (for eviction mode)
     * @return Written size or error
     */
    tl::expected<size_t, ErrorCode> WriteSlicesToFile(
        std::unique_ptr<StorageFile>& file, const std::string& path,
        const std::vector<Slice>& slices, uint64_t reserved_size = 0);

    /**
     * @brief Helper: Writes data using write() and handles errors
     * @param file File handle
     * @param path File path (for error messages)
     * @param data Data to write
     * @param reserved_size Size already reserved (for eviction mode)
     * @return Written size or error
     */
    tl::expected<size_t, ErrorCode> WriteDataToFile(
        std::unique_ptr<StorageFile>& file, const std::string& path,
        std::span<const char> data, uint64_t reserved_size = 0);
};

class BucketIdGenerator {
   public:
    explicit BucketIdGenerator(int64_t start);

    int64_t NextId();

    int64_t CurrentId();
    static constexpr int64_t INIT_NEW_START_ID = -1;

   private:
    static constexpr int SEQUENCE_BITS = 12;
    static constexpr int SEQUENCE_ID_SHIFT = 0;
    static constexpr int TIMESTAMP_SHIFT = SEQUENCE_BITS;
    static constexpr int64_t SEQUENCE_MASK = (1 << SEQUENCE_BITS) - 1;
    std::atomic<int64_t> current_id_;
};

class StorageBackendAdaptor : public StorageBackendInterface {
   public:
    StorageBackendAdaptor(const FileStorageConfig& file_storage_config,
                          const FilePerKeyConfig& file_per_key_config);

    tl::expected<void, ErrorCode> Init() override;

    tl::expected<int64_t, ErrorCode> BatchOffload(
        const std::unordered_map<std::string, std::vector<Slice>>& batch_object,
        std::function<ErrorCode(const std::vector<std::string>& keys,
                                std::vector<StorageObjectMetadata>& metadatas)>
            complete_handler) override;

    tl::expected<void, ErrorCode> BatchLoad(
        const std::unordered_map<std::string, Slice>& batched_slices) override;

    tl::expected<bool, ErrorCode> IsExist(const std::string& key) override;

    tl::expected<bool, ErrorCode> IsEnableOffloading() override;

    tl::expected<void, ErrorCode> ScanMeta(
        const std::function<ErrorCode(
            const std::vector<std::string>& keys,
            std::vector<StorageObjectMetadata>& metadatas)>& handler) override;

    // Test-only: Set predicate to force failures for specific keys in
    // BatchOffload. Returns true if the key should fail, false otherwise. This
    // allows deterministic testing of partial success behavior.
    void SetTestFailurePredicate(
        std::function<bool(const std::string& key)> predicate) override {
        test_failure_predicate_ = std::move(predicate);
    }

   private:
    const FilePerKeyConfig file_per_key_config_;

    // Test-only: Predicate to determine which keys should fail in BatchOffload.
    // Used for deterministic testing of partial success behavior.
    std::function<bool(const std::string& key)> test_failure_predicate_;

    std::atomic<bool> meta_scanned_{false};

    std::unique_ptr<StorageBackend> storage_backend_;

    std::string SanitizeKey(const std::string& key) const;

    std::string ResolvePath(const std::string& key) const;

    static std::string ConcatSlicesToString(const std::vector<Slice>& slices);

    mutable Mutex mutex_;

    int64_t total_keys GUARDED_BY(mutex_);

    int64_t total_size GUARDED_BY(mutex_);

    struct KVEntry {
        std::string key;    // K tensor or its storage identifier
        std::string value;  // V tensor or its storage block

        KVEntry() = default;

        KVEntry(std::string k, std::string v)
            : key(std::move(k)), value(std::move(v)) {}

        YLT_REFL(KVEntry, key, value);
    };
};

class BucketStorageBackend : public StorageBackendInterface {
   public:
    BucketStorageBackend(const FileStorageConfig& file_storage_config_,
                         const BucketBackendConfig& bucket_backend_config_);

    /**
     * @brief Offload objects in batches
     * @param batch_object  A map from object key to a list of data slices to be
     * stored.
     * @param complete_handler A callback function that is invoked after all
     * data is stored successfully.
     * @return tl::expected<void, ErrorCode> indicating operation status
     */
    tl::expected<int64_t, ErrorCode> BatchOffload(
        const std::unordered_map<std::string, std::vector<Slice>>& batch_object,
        std::function<ErrorCode(const std::vector<std::string>& keys,
                                std::vector<StorageObjectMetadata>& metadatas)>
            complete_handler) override;

    /**
     * @brief Retrieves metadata for multiple objects in a single batch
     * operation.
     * @param keys A list of object keys to query metadata for.
     * @param batch_object_metadata Output parameter that receives the
     * retrieved metadata.
     * @return tl::expected<void, ErrorCode> indicating operation status.
     */
    tl::expected<void, ErrorCode> BatchQuery(
        const std::vector<std::string>& keys,
        std::unordered_map<std::string, StorageObjectMetadata>&
            batch_object_metadata);

    /**
     * @brief Loads data for multiple objects in a batch operation.
     * @param batched_slices A map from object key to a pre-allocated writable
     * buffer (Slice).
     * @return tl::expected<void, ErrorCode> indicating operation status.
     */
    tl::expected<void, ErrorCode> BatchLoad(
        const std::unordered_map<std::string, Slice>& batched_slices) override;

    /**
     * @brief Retrieves the list of object keys belonging to a specific bucket.
     * @param bucket_id The unique identifier of the bucket to query.
     * @param bucket_keys Output parameter that will be populated with the list
     * of object keys.
     * @return tl::expected<void, ErrorCode> indicating operation status.
     */
    tl::expected<void, ErrorCode> GetBucketKeys(
        int64_t bucket_id, std::vector<std::string>& bucket_keys);

    /**
     * @brief Initializes the bucket storage backend.
     * @return tl::expected<void, ErrorCode> indicating operation status.
     */
    tl::expected<void, ErrorCode> Init() override;

    /**
     * @brief Checks whether an object with the specified key exists in the
     * storage system.
     * @param key The unique identifier of the object to check for existence.
     * @return tl::expected<void, ErrorCode> indicating operation status.
     */
    tl::expected<bool, ErrorCode> IsExist(const std::string& key) override;

    /**
     * @brief Scan existing object metadata from storage and report via handler.
     * @param handler Callback invoked with a batch of keys and metadatas.
     * @return tl::expected<void, ErrorCode> indicating operation status.
     */
    tl::expected<void, ErrorCode> ScanMeta(
        const std::function<ErrorCode(
            const std::vector<std::string>& keys,
            std::vector<StorageObjectMetadata>& metadatas)>& handler) override;

    /**
     * @brief Checks whether the backend is allowed to continue offloading.
     * @return tl::expected<bool, ErrorCode>
     * - On success: true 表示可以继续 offload；false 表示达到上限/不允许继续。
     * - On failure: 返回错误码（例如 IO/内部错误）。
     */
    tl::expected<bool, ErrorCode> IsEnableOffloading() override;

    /**
     * @brief 根据后端 bucket 限制（keys/size）将 offloading_objects 分桶。
     * @param offloading_objects Input map of object keys and their sizes
     * (bytes).
     * @param buckets_keys Output: bucketized keys; each inner vector is a
     * bucket.
     * @return tl::expected<void, ErrorCode> indicating operation status.
     */
    tl::expected<void, ErrorCode> AllocateOffloadingBuckets(
        const std::unordered_map<std::string, int64_t>& offloading_objects,
        std::vector<std::vector<std::string>>& buckets_keys);

    void ClearUngroupedOffloadingObjects();

    size_t UngroupedOffloadingObjectsSize() const;

    /**
     * @brief Iterate over the metadata of stored objects starting from a
     * specified bucket.
     * @param bucket_id The ID of the bucket to start scanning from.
     * @param keys          Output vector to receive object keys
     * @param metadatas     Output vector to receive object metadata
     * @param buckets       Output vector to receive corresponding destination
     * bucket IDs
     * @param limit         Maximum number of entries to return in this call
     * @return tl::expected<int64_t, ErrorCode>
     * - On success: the bucket ID where the next iteration should start  (or 0
     * if all data has been scanned).
     * - On failure: returns an error code (e.g., BUCKET_NOT_FOUND, IO_ERROR).
     */
    tl::expected<int64_t, ErrorCode> BucketScan(
        int64_t bucket_id, std::vector<std::string>& keys,
        std::vector<StorageObjectMetadata>& metadatas,
        std::vector<int64_t>& buckets, int64_t limit);

    /**
     * @brief Retrieves the global metadata of the store.
     * @return On success: `tl::expected` containing a `StoreMetadata`
     * object. On failure: an error code.
     */
    tl::expected<OffloadMetadata, ErrorCode> GetStoreMetadata();

    /**
     * @brief Delete a bucket and all its associated keys.
     *
     * This method safely deletes a bucket by:
     * 1. Removing the bucket and its keys from metadata maps (under lock)
     * 2. Waiting for all in-flight reads to complete (via inflight_reads_)
     * 3. Deleting the bucket data and metadata files
     *
     * Thread-safe: Can be called concurrently with BatchLoad operations.
     * The method blocks until all in-flight reads complete.
     *
     * @param bucket_id The bucket ID to delete.
     * @return tl::expected<void, ErrorCode>
     *         - OK on success
     *         - BUCKET_NOT_FOUND if bucket doesn't exist
     */
    tl::expected<void, ErrorCode> DeleteBucket(int64_t bucket_id);

   private:
    tl::expected<std::shared_ptr<BucketMetadata>, ErrorCode> BuildBucket(
        int64_t bucket_id,
        const std::unordered_map<std::string, std::vector<Slice>>& batch_object,
        std::vector<iovec>& iovs,
        std::vector<StorageObjectMetadata>& metadatas);

    tl::expected<void, ErrorCode> WriteBucket(
        int64_t bucket_id, std::shared_ptr<BucketMetadata> bucket_metadata,
        std::vector<iovec>& iovs);

    tl::expected<void, ErrorCode> StoreBucketMetadata(
        int64_t bucket_id, std::shared_ptr<BucketMetadata> bucket_metadata);

    tl::expected<void, ErrorCode> LoadBucketMetadata(
        int64_t bucket_id, std::shared_ptr<BucketMetadata> bucket_metadata);

    tl::expected<int64_t, ErrorCode> CreateBucketId();

    tl::expected<std::string, ErrorCode> GetBucketMetadataPath(
        int64_t bucket_id);

    tl::expected<std::string, ErrorCode> GetBucketDataPath(int64_t bucket_id);

    tl::expected<std::unique_ptr<StorageFile>, ErrorCode> OpenFile(
        const std::string& path, FileMode mode) const;

    tl::expected<void, ErrorCode> GroupOffloadingKeysByBucket(
        const std::unordered_map<std::string, int64_t>& offloading_objects,
        std::vector<std::vector<std::string>>& buckets_keys);

    tl::expected<void, ErrorCode> HandleNext(
        const std::function<
            ErrorCode(const std::vector<std::string>& keys,
                      std::vector<StorageObjectMetadata>& metadatas)>& handler);

    tl::expected<bool, ErrorCode> HasNext();

    /**
     * @brief Cleanup orphaned bucket files (data + metadata) for a given bucket
     * ID. Called when BatchOffload fails due to duplicate keys after files were
     * written.
     * @param bucket_id The bucket ID whose files should be deleted.
     */
    void CleanupOrphanedBucket(int64_t bucket_id);

   private:
    std::atomic<bool> initialized_{false};
    std::optional<BucketIdGenerator> bucket_id_generator_;
    static constexpr const char* BUCKET_DATA_FILE_SUFFIX = ".bucket";
    static constexpr const char* BUCKET_METADATA_FILE_SUFFIX = ".meta";
    /**
     * @brief A shared mutex to protect concurrent access to metadata.
     *
     * This mutex is used to synchronize read/write operations on the following
     * metadata members:
     * - object_bucket_map_: maps object keys to bucket IDs
     * - buckets_: ordered map of bucket ID to bucket metadata
     * - total_size_: cumulative data size of all stored objects
     */
    mutable SharedMutex mutex_;
    mutable Mutex iterator_mutex_;
    std::string storage_path_;
    int64_t total_size_ GUARDED_BY(mutex_) = 0;
    std::unordered_map<std::string, StorageObjectMetadata> GUARDED_BY(mutex_)
        object_bucket_map_;
    std::map<int64_t, std::shared_ptr<BucketMetadata>> GUARDED_BY(
        mutex_) buckets_;
    int64_t GUARDED_BY(mutex_) next_bucket_ = -1;
    BucketBackendConfig bucket_backend_config_;

    mutable Mutex offloading_mutex_;
    std::unordered_map<std::string, int64_t> GUARDED_BY(offloading_mutex_)
        ungrouped_offloading_objects_;
};

class OffsetAllocatorStorageBackend : public StorageBackendInterface {
   public:
    OffsetAllocatorStorageBackend(
        const FileStorageConfig& file_storage_config_);

    /**
     * @brief Initializes the offset allocator storage backend.
     * Creates/truncates the data file and initializes the allocator.
     * @return tl::expected<void, ErrorCode> indicating operation status.
     */
    tl::expected<void, ErrorCode> Init() override;

    /**
     * @brief Offload objects in batches
     * @param batch_object  A map from object key to a list of data slices to be
     * stored.
     * @param complete_handler A callback function that is invoked after all
     * data is stored successfully.
     * @return tl::expected<int64_t, ErrorCode> indicating number of objects
     * offloaded or error
     */
    tl::expected<int64_t, ErrorCode> BatchOffload(
        const std::unordered_map<std::string, std::vector<Slice>>& batch_object,
        std::function<ErrorCode(const std::vector<std::string>& keys,
                                std::vector<StorageObjectMetadata>& metadatas)>
            complete_handler) override;

    /**
     * @brief Loads data for multiple objects in a batch operation.
     * @param batched_slices A map from object key to a pre-allocated writable
     * buffer (Slice).
     * @return tl::expected<void, ErrorCode> indicating operation status.
     */
    tl::expected<void, ErrorCode> BatchLoad(
        const std::unordered_map<std::string, Slice>& batched_slices) override;

    /**
     * @brief Checks whether an object with the specified key exists in the
     * storage system.
     * @param key The unique identifier of the object to check for existence.
     * @return tl::expected<bool, ErrorCode> indicating existence status.
     */
    tl::expected<bool, ErrorCode> IsExist(const std::string& key) override;

    /**
     * @brief Checks whether the backend is allowed to continue offloading.
     * @return tl::expected<bool, ErrorCode>
     * - On success: true if offloading is enabled; false if out of space.
     * - On failure: returns error code.
     */
    tl::expected<bool, ErrorCode> IsEnableOffloading() override;

    /**
     * @brief Scan existing object metadata from storage and report via handler.
     * For V1, scans only in-memory map (no disk scanning).
     * @param handler Callback invoked with a batch of keys and metadatas.
     * @return tl::expected<void, ErrorCode> indicating operation status.
     */
    tl::expected<void, ErrorCode> ScanMeta(
        const std::function<ErrorCode(
            const std::vector<std::string>& keys,
            std::vector<StorageObjectMetadata>& metadatas)>& handler) override;

    // Test-only: Set predicate to force failures for specific keys in
    // BatchOffload. Returns true if the key should fail, false otherwise. This
    // allows deterministic testing of partial success behavior.
    void SetTestFailurePredicate(
        std::function<bool(const std::string& key)> predicate) override {
        test_failure_predicate_ = std::move(predicate);
    }

   private:
    // On-disk record header: [u32 key_len][u32 value_len] (8 bytes total)
    struct RecordHeader {
        // Length of key in bytes
        uint32_t key_len;

        // Length of value in bytes
        uint32_t value_len;

        // Header size: 8 bytes (2 * uint32_t). Currently assumes max object
        // size is 4GB. If we need to support larger objects, change this to 16
        // bytes.
        static constexpr size_t SIZE = sizeof(uint32_t) * 2;

        // Validate header against expected metadata
        bool ValidateAgainstMetadata(uint32_t expected_value_len) const {
            return value_len == expected_value_len;
        }

        // Validate key matches expected key
        tl::expected<void, ErrorCode> ValidateKey(
            const std::string& expected_key,
            const std::string& stored_key) const {
            if (stored_key.size() != key_len) {
                LOG(ERROR) << "Key length mismatch: expected " << key_len
                           << ", got " << stored_key.size();
                return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
            }
            if (stored_key != expected_key) {
                LOG(ERROR) << "Key mismatch: expected " << expected_key
                           << ", got " << stored_key;
                return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
            }
            return {};
        }
    };

    // Refcounted wrapper for move-only OffsetAllocationHandle. Physical extent
    // freed when last shared_ptr reference drops.
    struct RefCountedAllocationHandle {
        // RAII handle that frees allocation on destruction
        offset_allocator::OffsetAllocationHandle handle;
        explicit RefCountedAllocationHandle(
            offset_allocator::OffsetAllocationHandle&& h)
            : handle(std::move(h)) {}
        RefCountedAllocationHandle(const RefCountedAllocationHandle&) = delete;
        RefCountedAllocationHandle& operator=(
            const RefCountedAllocationHandle&) = delete;
        RefCountedAllocationHandle(RefCountedAllocationHandle&&) = default;
        RefCountedAllocationHandle& operator=(RefCountedAllocationHandle&&) =
            default;
    };

    // Refcounted allocation handle: shared_ptr ensures physical extent remains
    // alive until all readers release their references
    using AllocationPtr = std::shared_ptr<RefCountedAllocationHandle>;

    // Metadata entry for a stored object. Protected by stripe lock for the key.
    struct ObjectEntry {
        // Byte offset in data file where record is stored
        uint64_t offset;

        // Total record size: header (8) + key + value
        uint32_t total_size;

        // Value size only (excluding header and key)
        uint32_t value_size;

        // Refcounted handle keeps physical extent alive during reads
        AllocationPtr allocation;
        ObjectEntry(uint64_t off, uint32_t total, uint32_t val,
                    AllocationPtr alloc_ptr)
            : offset(off),
              total_size(total),
              value_size(val),
              allocation(std::move(alloc_ptr)) {}
    };

    // Returns full path to data file: {storage_path_}/kv_cache.data
    std::string GetDataFilePath() const;

    static constexpr size_t kNumShards =
        1024;  // Number of shards (must be power of 2 for bitwise optimization)

    // Compile-time check: kNumShards must be a power of 2 for fast bitwise
    // modulo
    static_assert((kNumShards & (kNumShards - 1)) == 0,
                  "kNumShards must be a power of 2");

    // Sharded metadata: each shard has its own lock and map (prevents data
    // races)
    struct MetadataShard {
        // RW lock protecting this shard's map
        mutable SharedMutex mutex;

        // Per-shard map storing key -> ObjectEntry mappings
        std::unordered_map<std::string, ObjectEntry> map;
    };

    // Maps key to shard index [0, kNumShards) using hash. Same key always maps
    // to same shard. Uses bitwise AND instead of modulo (%) for speed: hash &
    // (kNumShards-1) ≡ hash % kNumShards This optimization only works when
    // kNumShards is a power of 2 (enforced by static_assert)
    inline size_t ShardForKey(const std::string& key) const {
        return std::hash<std::string>{}(key) & (kNumShards - 1);
    }

    // Initialization flag: true after successful Init(), prevents double
    // initialization
    std::atomic<bool> initialized_{false};

    // Base storage directory path from FileStorageConfig::storage_filepath
    std::string storage_path_;

    // Full path to kv_cache.data file, computed in Init() from storage_path_
    std::string data_file_path_;

    // Maximum capacity in bytes (90% of total_size_limit), used for file
    // preallocation
    uint64_t capacity_;

    // Thread-safe allocator managing free space within [0, capacity_) range
    std::shared_ptr<offset_allocator::OffsetAllocator> allocator_;

    // File handle wrapper for I/O operations using preadv/pwritev
    std::unique_ptr<StorageFile> data_file_;

    // Sharded metadata maps: one map per shard with its own lock (prevents data
    // races)
    std::array<MetadataShard, kNumShards> shards_;

    // Total bytes stored (headers + keys + values), updated atomically without
    // locks
    std::atomic<int64_t> total_size_{0};

    // Total number of keys, updated atomically (avoids locking all shards for
    // counting)
    std::atomic<int64_t> total_keys_{0};

    // Test-only: Predicate to determine which keys should fail in BatchOffload.
    // Used for deterministic testing of partial success behavior.
    std::function<bool(const std::string& key)> test_failure_predicate_;
};

tl::expected<std::shared_ptr<StorageBackendInterface>, ErrorCode>
CreateStorageBackend(const FileStorageConfig& config);

}  // namespace mooncake
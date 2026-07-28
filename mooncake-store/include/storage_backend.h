#pragma once

#include <glog/logging.h>

#include <array>
#include <atomic>
#include <cstring>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <unordered_set>
#include <vector>

#include "file_interface.h"
#include "mutex.h"
#include "offset_allocator/offset_allocator.h"
#include "types.h"

namespace mooncake {
struct FileRecord {
    std::string path;
    uint64_t size;
    std::string key;  // Associated object key for eviction tracking
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
    // Last access timestamp in nanoseconds; used by LRU eviction policy.
    // Updated on every read with relaxed ordering (approximate is sufficient).
    mutable std::atomic<int64_t> last_access_ns_{0};

    // Default constructor
    BucketMetadata() = default;

    // Copy constructor (atomics not copyable, so reset to 0)
    BucketMetadata(const BucketMetadata& other)
        : meta_size(other.meta_size),
          data_size(other.data_size),
          keys(other.keys),
          metadatas(other.metadatas),
          inflight_reads_(0),
          last_access_ns_(0) {}

    // Move constructor
    BucketMetadata(BucketMetadata&& other) noexcept
        : meta_size(other.meta_size),
          data_size(other.data_size),
          keys(std::move(other.keys)),
          metadatas(std::move(other.metadatas)),
          inflight_reads_(0),
          last_access_ns_(0) {}

    // Copy assignment
    BucketMetadata& operator=(const BucketMetadata& other) {
        if (this != &other) {
            meta_size = other.meta_size;
            data_size = other.data_size;
            keys = other.keys;
            metadatas = other.metadatas;
            // Don't copy runtime state
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
            // Don't move runtime state
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

enum class StorageBackendType {
    kFilePerKey,
    kBucket,
    kOffsetAllocator,
    kDistributed
};

static constexpr size_t kKB = 1024;
static constexpr size_t kMB = kKB * 1024;
static constexpr size_t kGB = kMB * 1024;

struct FilePerKeyConfig {
    std::string fsdir = "file_per_key_dir";  // Subdirectory name

    bool enable_eviction = true;  // Enable eviction for storage

    bool Validate() const;

    static FilePerKeyConfig FromEnvironment();
};

enum class BucketEvictionPolicy {
    NONE,  // No eviction (default)
    FIFO,  // Evict oldest bucket first (by creation order)
    LRU,   // Evict least recently read bucket first
};

struct BucketBackendConfig {
    int64_t bucket_size_limit =
        256 * kMB;  // Max total size of a single bucket (256 MB)

    int64_t bucket_keys_limit = 500;  // Max number of keys allowed in a single
                                      // bucket, required by bucket backend only

    BucketEvictionPolicy eviction_policy =
        BucketEvictionPolicy::NONE;  // Eviction strategy

    int64_t max_total_size = 0;  // 0 = unlimited; evict when total_size_
                                 // exceeds this threshold (bytes)

    bool Validate() const;

    static BucketBackendConfig FromEnvironment();
};

enum class OffsetEvictionPolicy {
    NONE,  // No eviction
    FIFO,  // Evict oldest key first (by insertion order)
    LRU,   // Approximate LRU via cross-shard sampling (phase 2)
};

enum class OffsetPersistMode {
    kDisabled,  // No persistence (default)
    kRelaxed,   // Periodic checkpoint
    kStrict,    // Every BatchOffload is durable
};

struct OffsetAllocatorBackendConfig {
    OffsetEvictionPolicy eviction_policy = OffsetEvictionPolicy::NONE;

    // Watermark thresholds: eviction triggers when total_size_ exceeds high,
    // drives down to low. 0 = auto-resolved in Init() from ratios.
    int64_t high_watermark_bytes = 0;
    int64_t low_watermark_bytes = 0;
    double high_ratio = 0.90;
    double low_ratio = 0.80;

    // Key-count watermarks (symmetric with byte watermarks).
    // high triggers eviction, drives down to low.
    int64_t high_watermark_keys = 0;
    int64_t low_watermark_keys = 0;
    double keys_high_ratio = 0.95;
    double keys_low_ratio = 0.90;

    // Eviction caps
    size_t max_evict_per_offload = 4096;
    size_t fallback_evict_batch = 16;

    // Allocator node capacity override.
    // 0 = auto-derived from capacity_ / kMinObjectSize (capped at RAM budget).
    // Must be <= UINT32_MAX (OffsetAllocator::create takes uint32
    // max_capacity).
    int64_t max_capacity_nodes = 0;

    bool Validate() const;

    static OffsetAllocatorBackendConfig FromEnvironment();

    // ---- Persistence settings ----
    OffsetPersistMode persist_mode = OffsetPersistMode::kDisabled;
    int64_t persist_interval_seconds = 60;

    // ---- Record integrity ----
    // When true (default), every written record carries a CRC-32C over
    // header-prefix + key + value (RecordHeader::kFlagHasCrc), verified
    // once on recovery.  Disable only when torn writes are otherwise
    // impossible (kStrict mode on storage that honors fsync ordering,
    // e.g. power-loss-protected NVMe) or when values never pass through
    // the CPU (future DMA/GDS writers): unchecksummed records are then
    // validated by checkpoint ordering (seq guard) alone.
    bool enable_record_crc = true;
};

// ===== Persistence metadata structures =====

struct PersistedFifoEntry {
    uint64_t seq;
    std::string key;
};
YLT_REFL(PersistedFifoEntry, seq, key);

struct OffsetAllocatorPersistedMetadata {
    uint32_t version = 1;
    std::string allocator_state;
    uint64_t insert_seq = 0;
    std::vector<PersistedFifoEntry> fifo_entries;
    std::vector<std::string> evicted_keys_this_batch;
};
YLT_REFL(OffsetAllocatorPersistedMetadata, version, allocator_state, insert_seq,
         fifo_entries, evicted_keys_this_batch);

// Current on-disk format version of OffsetAllocatorPersistedMetadata.
// v2: RecordHeader grew from 8 to 20 bytes (added per-record seq + CRC-32C).
// v3: RecordHeader is 24 bytes (added `flags`; CRC-32C is now optional per
//     record) and the value region is aligned to 4 KiB within the record
//     (zero padding derived from key_len), so that DMA writers (e.g. GDS)
//     can share the layout.
// Older metadata is rejected on load (fresh start) because its data-file
// records cannot be parsed with the current record layout.
inline constexpr uint32_t kOffsetAllocatorPersistVersion = 3;

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

    // Use io_uring for file I/O instead of POSIX pread/pwrite
    bool use_uring = false;

    // Proactively evict local disk objects from the heartbeat thread once
    // backend usage crosses the high watermark.
    bool enable_disk_watermark_eviction = true;
    double disk_eviction_high_watermark_ratio = 0.90;
    double disk_eviction_low_watermark_ratio = 0.80;

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

    using EvictionHandler = std::function<tl::expected<void, ErrorCode>(
        const std::vector<std::string>& evicted_keys)>;

    virtual tl::expected<void, ErrorCode> Init() = 0;

    virtual tl::expected<int64_t, ErrorCode> BatchOffload(
        const std::unordered_map<std::string, std::vector<Slice>>& batch_object,
        std::function<ErrorCode(const std::vector<std::string>& keys,
                                std::vector<StorageObjectMetadata>& metadatas)>
            complete_handler,
        EvictionHandler eviction_handler = nullptr) = 0;

    virtual tl::expected<void, ErrorCode> BatchLoad(
        std::unordered_map<std::string, Slice>& batched_slices) = 0;

    virtual tl::expected<bool, ErrorCode> IsExist(const std::string& key) = 0;

    virtual tl::expected<bool, ErrorCode> IsEnableOffloading() = 0;

    virtual tl::expected<void, ErrorCode> ScanMeta(
        const std::function<ErrorCode(
            const std::vector<std::string>& keys,
            std::vector<StorageObjectMetadata>& metadatas)>& handler) = 0;

    // Reset internal scan iterator so that the next ScanMeta() call
    // starts from the beginning.  Required for backends that use
    // cursor-based iteration (e.g. BucketStorageBackend).
    virtual void ResetScanIterator() {}

    // Test-only: Set predicate to force failures for specific keys in
    // BatchOffload. Default implementation does nothing (no failures injected).
    // Concrete backends can override to provide test failure injection.
    // Returns true if the key should fail, false otherwise.
    virtual void SetTestFailurePredicate(
        std::function<bool(const std::string& key)> /* predicate */) {
        // Default: no-op (no test failures injected)
    }

    // Remove all persisted objects from disk. Called during RemoveAll to
    // clean up physical SSD files alongside master metadata deletion.
    virtual void RemoveAll() {}

    virtual tl::expected<std::vector<std::string>, ErrorCode>
    EvictAboveDiskWatermark(double /* high_watermark_ratio */,
                            double /* low_watermark_ratio */,
                            EvictionHandler /* eviction_handler */ = nullptr) {
        return std::vector<std::string>{};
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
    explicit StorageBackend(const std::string& root_dir,
                            const std::string& fsdir,
                            bool enable_eviction = true)
        : root_dir_(root_dir),
          fsdir_(fsdir),
          enable_eviction_(enable_eviction) {}

    /**
     * @brief Factory method to create a StorageBackend instance
     * @param root_dir Root directory path for object storage
     * @param fsdir  subdirectory name
     * @param enable_eviction Whether to enable disk eviction feature (default:
     * true) Note: Eviction is controlled by the enable_eviction parameter
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
        return std::make_shared<StorageBackend>(root_dir, real_fsdir,
                                                enable_eviction);
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
    tl::expected<std::vector<std::string>, ErrorCode> StoreObject(
        const std::string& path, const std::vector<Slice>& slices,
        const std::string& key = "",
        StorageBackendInterface::EvictionHandler eviction_handler = nullptr);

    /**
     * @brief Stores an object from a string
     * @param path path for the object
     * @param str String containing object data
     * @param key Optional object key for eviction tracking
     * @return tl::expected with evicted keys on success, ErrorCode on failure
     */
    tl::expected<std::vector<std::string>, ErrorCode> StoreObject(
        const std::string& path, const std::string& str,
        const std::string& key = "",
        StorageBackendInterface::EvictionHandler eviction_handler = nullptr);

    /**
     * @brief Stores an object from a span of data
     * @param path path for the object
     * @param data Span containing object data
     * @param key Optional object key for eviction tracking
     * @return tl::expected with evicted keys on success, ErrorCode on failure
     */
    tl::expected<std::vector<std::string>, ErrorCode> StoreObject(
        const std::string& path, std::span<const char> data,
        const std::string& key = "",
        StorageBackendInterface::EvictionHandler eviction_handler = nullptr);

    tl::expected<std::vector<std::string>, ErrorCode> EvictAboveDiskWatermark(
        double high_watermark_ratio, double low_watermark_ratio,
        StorageBackendInterface::EvictionHandler eviction_handler = nullptr);

    void UpdateFileRecordKey(const std::string& path, const std::string& key);

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
    bool use_uring_{false};  // Use io_uring for file I/O

   private:
    // File write queue for disk eviction - tracks files in FIFO order
    std::list<FileRecord> file_write_queue_;
    std::unordered_map<std::string, std::list<FileRecord>::iterator>
        file_queue_map_;
    std::unordered_set<std::string> pending_eviction_paths_;
    mutable std::shared_mutex
        file_queue_mutex_;  // Mutex to protect file queue operations
    static constexpr size_t kFilePathLockCount = 64;
    std::array<Mutex, kFilePathLockCount> file_path_mutexes_;

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
     * @return FileRecord of the evicted file, or empty record if no file was
     * evicted
     */
    FileRecord EvictFile();

    FileRecord PopFileToEvictByFIFO();

    void RestoreFileToWriteQueueFront(const FileRecord& record);

    tl::expected<void, ErrorCode> DeleteEvictedFile(const FileRecord& record);

    /**
     * @brief Add file to write queue for FIFO tracking
     * @param path Path of the file to add to queue
     * @param size Size of the file
     */
    void AddFileToWriteQueue(const std::string& path, uint64_t size,
                             const std::string& key = "");

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
     * @brief Ensures that a specified amount of disk space is available,
     * performing evictions if necessary.
     *
     * @param required_size The amount of disk space, in bytes, required for the
     *                      upcoming write operation.
     * @return tl::expected<void, ErrorCode> Returns void on success, or
     *         ErrorCode::FILE_WRITE_FAIL if insufficient space remains after
     *         attempting evictions up to the maximum attempt limit.
     */
    tl::expected<std::vector<std::string>, ErrorCode> EnsureDiskSpace(
        size_t required_size,
        StorageBackendInterface::EvictionHandler eviction_handler = nullptr);

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
     * @return true if eviction is enabled, false otherwise.
     */
    bool IsEvictionEnabled() const;

    Mutex& GetFilePathMutex(const std::string& path);

    bool IsFilePendingEviction(const std::string& path) const;

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

    // Test-only: Set predicate to force failures for specific keys in
    // BatchOffload. Returns true if the key should fail, false otherwise. This
    // allows deterministic testing of partial success behavior.
    void SetTestFailurePredicate(
        std::function<bool(const std::string& key)> predicate) override {
        test_failure_predicate_ = std::move(predicate);
    }

    void RemoveAll() override;

    tl::expected<std::vector<std::string>, ErrorCode> EvictAboveDiskWatermark(
        double high_watermark_ratio, double low_watermark_ratio,
        EvictionHandler eviction_handler = nullptr) override;

   private:
    const FilePerKeyConfig file_per_key_config_;

    // Test-only: Predicate to determine which keys should fail in BatchOffload.
    // Used for deterministic testing of partial success behavior.
    std::function<bool(const std::string& key)> test_failure_predicate_;

    std::atomic<bool> meta_scanned_{false};

    std::unique_ptr<StorageBackend> storage_backend_;

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

    ~BucketStorageBackend();

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
            complete_handler,
        EvictionHandler eviction_handler = nullptr) override;

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
        std::unordered_map<std::string, Slice>& batched_slices) override;

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

    void ResetScanIterator() override {
        MutexLocker locker(&iterator_mutex_);
        next_bucket_ = -1;
    }

    /**
     * @brief Checks whether the backend is allowed to continue offloading.
     * @return tl::expected<bool, ErrorCode>
     * - On success: true 表示可以继续 offload；false 表示达到上限/不允许继续。
     * - On failure: 返回错误码（例如 IO/内部错误）。
     */
    tl::expected<bool, ErrorCode> IsEnableOffloading() override;

    void RemoveAll() override;

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

    tl::expected<std::vector<std::string>, ErrorCode> EvictAboveDiskWatermark(
        double high_watermark_ratio, double low_watermark_ratio,
        EvictionHandler eviction_handler = nullptr) override;

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
     * @brief Remove any remaining data and metadata files for a bucket.
     * Used by write rollback and startup recovery of incomplete buckets.
     * @param bucket_id The bucket ID whose files should be deleted.
     */
    void CleanupOrphanedBucket(int64_t bucket_id);

    /**
     * @brief Rollback a committed bucket from the local index when
     * NotifyOffloadSuccess fails after local commit. Removes keys from
     * object_bucket_map_, removes the bucket from buckets_ and lru_index_,
     * waits for inflight reads to drain, then cleans up on-disk files.
     *
     * Called from BatchOffload when complete_handler fails after the local
     * index has already been committed.
     *
     * @param bucket_id The bucket ID to roll back.
     * @param keys The keys that were committed.
     */
    void RollbackCommittedBucket(int64_t bucket_id,
                                 const std::vector<std::string>& keys);

    // Holds eviction state between PrepareEviction and FinalizeEviction.
    // PrepareEviction removes buckets from metadata maps and returns this.
    // FinalizeEviction removes persisted metadata, waits for in-flight reads,
    // and then deletes the data files.
    struct PendingEviction {
        std::vector<std::string> keys;  // All keys in evicted buckets
        std::vector<std::pair<int64_t, std::shared_ptr<BucketMetadata>>>
            buckets;  // (bucket_id, metadata) for file deletion
        std::vector<std::string> write_keys;
        int64_t evicted_size = 0;
        int64_t write_size = 0;
    };

    /**
     * @brief Phase 1 of eviction: under exclusive lock, select and remove
     * oldest buckets (FIFO) until total_size_ + required_size <=
     * max_total_size. The removed buckets are returned for later file deletion.
     * Does nothing if eviction_policy == NONE or max_total_size == 0.
     * @param required_size Size of the incoming bucket to be written.
     * @return PendingEviction with all keys and bucket metadata removed.
     */
    tl::expected<PendingEviction, ErrorCode> PrepareEviction(
        int64_t required_size, const std::vector<std::string>& write_keys = {});

    void RestorePreparedEviction(PendingEviction&& pending);

    void RestorePreparedEvictionLocked(PendingEviction&& pending);

    void CommitPreparedEviction(const PendingEviction& pending);

    void ReleasePreparedWrite(const PendingEviction& pending);

    void ReleasePreparedWriteLocked(const PendingEviction& pending);

    /**
     * @brief Select the next bucket to evict according to the configured
     * eviction policy. Must be called with mutex_ held (exclusive).
     * @return Iterator into buckets_ pointing at the candidate, or
     *         buckets_.end() if no candidate is available.
     */
    std::map<int64_t, std::shared_ptr<BucketMetadata>>::iterator
    SelectEvictionCandidate();

    /**
     * @brief Phase 2 of eviction: delete persisted metadata for each evicted
     * bucket, wait for in-flight reads to drain, then delete the data file.
     * When metadata removal succeeds, doing it first prevents a later read
     * timeout or data-file deletion failure from leaving a bucket that Init()
     * could recover.
     * Must be called AFTER master has been notified via eviction_handler.
     * @param pending The result of a prior PrepareEviction call.
     */
    tl::expected<void, ErrorCode> FinalizeEviction(
        const PendingEviction& pending);

   public:
    /**
     * @brief Get a file instance for external buffer registration
     * Opens a temporary file to get access to the UringFile instance
     * @return Shared pointer to StorageFile or error
     */
    tl::expected<std::shared_ptr<StorageFile>, ErrorCode> GetFileInstance()
        const;

   private:
    // Alignment helper functions for O_DIRECT I/O
    static constexpr size_t kDirectIOAlignment = 4096;

    static inline size_t align_up(size_t size, size_t alignment) {
        return (size + alignment - 1) & ~(alignment - 1);
    }

    static inline int64_t align_down(int64_t offset, int64_t alignment) {
        return offset & ~(alignment - 1);
    }

    std::atomic<bool> initialized_{false};
    std::optional<BucketIdGenerator> bucket_id_generator_;
    static constexpr const char* BUCKET_DATA_FILE_SUFFIX = ".bucket";
    static constexpr const char* BUCKET_METADATA_FILE_SUFFIX = ".meta";

    // Aligned buffer for O_DIRECT I/O operations
    // We use a fixed-size buffer to avoid frequent allocations
    static constexpr size_t kAlignedBufferSize = 32 * 1024 * 1024;  // 16MB
    std::unique_ptr<void, void (*)(void*)> aligned_io_buffer_{nullptr,
                                                              [](void*) {}};
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
    std::unordered_set<std::string> GUARDED_BY(mutex_) pending_eviction_keys_;
    std::unordered_set<std::string> GUARDED_BY(mutex_) pending_write_keys_;
    int64_t pending_eviction_size_ GUARDED_BY(mutex_) = 0;
    int64_t pending_write_size_ GUARDED_BY(mutex_) = 0;
    std::map<int64_t, std::shared_ptr<BucketMetadata>> GUARDED_BY(
        mutex_) buckets_;
    // LRU eviction index: ordered set of {last_access_ns_, bucket_id}.
    // Maintained lazily — reads update last_access_ns_ atomically without
    // touching this index; SelectEvictionCandidate() repairs stale entries.
    std::set<std::pair<int64_t, int64_t>> GUARDED_BY(mutex_) lru_index_;
    int64_t GUARDED_BY(mutex_) next_bucket_ = -1;
    BucketBackendConfig bucket_backend_config_;

    mutable Mutex offloading_mutex_;
    std::unordered_map<std::string, int64_t> GUARDED_BY(offloading_mutex_)
        ungrouped_offloading_objects_;

    // File handle cache for UringFile to avoid repeated open/close overhead
    mutable Mutex file_cache_mutex_;
    mutable std::unordered_map<std::string, std::shared_ptr<StorageFile>>
        file_cache_ GUARDED_BY(file_cache_mutex_);

    // Get or open a file with caching support
    tl::expected<std::shared_ptr<StorageFile>, ErrorCode> GetOrOpenFile(
        const std::string& path, FileMode mode) const;

    // Clear file cache (called on destruction or when needed)
    void ClearFileCache();
};

class OffsetAllocatorStorageBackend : public StorageBackendInterface {
   public:
    OffsetAllocatorStorageBackend(
        const FileStorageConfig& file_storage_config_,
        const OffsetAllocatorBackendConfig& offset_backend_config = {});

    ~OffsetAllocatorStorageBackend();
    OffsetAllocatorStorageBackend(OffsetAllocatorStorageBackend&&) = default;
    OffsetAllocatorStorageBackend& operator=(OffsetAllocatorStorageBackend&&) =
        default;
    OffsetAllocatorStorageBackend(const OffsetAllocatorStorageBackend&) =
        delete;
    OffsetAllocatorStorageBackend& operator=(
        const OffsetAllocatorStorageBackend&) = delete;

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
            complete_handler,
        EvictionHandler eviction_handler = nullptr) override;

    /**
     * @brief Loads data for multiple objects in a batch operation.
     * @param batched_slices A map from object key to a pre-allocated writable
     * buffer (Slice).
     * @return tl::expected<void, ErrorCode> indicating operation status.
     */
    tl::expected<void, ErrorCode> BatchLoad(
        std::unordered_map<std::string, Slice>& batched_slices) override;

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

    // Returns the number of keys skipped after fallback eviction
    // could not make enough room (fragmentation, extents pinned by
    // in-flight reads, or allocator node exhaustion).  Monotonically
    // increasing; useful for distinguishing "watermark working" from
    // "thrashing but unable to free space".
    int64_t GetEvictionSkips() const {
        return eviction_skips_.load(std::memory_order_relaxed);
    }

    void RemoveAll() override;

    // On-disk record layout v3 (single definition, shared by the write,
    // read and recovery paths of this backend, and by future DMA writers
    // such as GDS):
    //
    //   [u32 key_len][u32 value_len][u64 seq][u32 flags][u32 crc32]
    //   [key bytes][zero padding][value bytes]
    //
    // The value region always starts at a kValueAlignment boundary within
    // the record so that DMA engines (e.g. cuFile) operate on aligned file
    // offsets.  The padding is a pure function of key_len, so writer,
    // reader and recovery derive the same layout independently.
    //
    // `seq` is the write's insert_seq_ stamp; on recovery any record with
    // seq >= the checkpoint's insert_seq was written after that checkpoint
    // and is dropped (its extent may hold a torn write).
    //
    // `flags` bit kFlagHasCrc: when set, `crc32` is a CRC-32C over the
    // header prefix (everything before crc32), the key and the value,
    // verified once on recovery so torn/stale records are detected and
    // skipped instead of being served as valid data.  When clear (records
    // whose value never touched the CPU, or CRC disabled via config),
    // recovery skips the checksum and trusts checkpoint ordering alone.
    struct RecordHeader {
        // Length of key in bytes
        uint32_t key_len;

        // Length of value in bytes. Currently assumes max object size is
        // 4GB. If we need to support larger objects, change this to 8 bytes.
        uint32_t value_len;

        // insert_seq_ stamp of this write (monotonic per BatchOffload entry)
        uint64_t seq;

        // Record flags; see kFlag* constants below.
        uint32_t flags;

        // CRC-32C over [key_len|value_len|seq|flags] + key + value.
        // Valid only when (flags & kFlagHasCrc).
        uint32_t crc32;

        // flags: crc32 field carries a valid CRC-32C of this record.
        static constexpr uint32_t kFlagHasCrc = 1u << 0;

        // All currently defined flag bits; recovery drops records with
        // unknown bits set (written by a newer format).
        static constexpr uint32_t kKnownFlags = kFlagHasCrc;

        // File-offset alignment of the value region within a record.
        // 4 KiB covers the logical block size of currently supported NVMe
        // devices (cuFile requirement for DMA).
        static constexpr uint32_t kValueAlignment = 4096;

        // Header size: 24 bytes on disk (fields are (de)serialized
        // field-by-field; do NOT use sizeof(RecordHeader), which includes
        // padding).
        static constexpr size_t SIZE =
            sizeof(uint32_t) * 2 + sizeof(uint64_t) + sizeof(uint32_t) * 2;

        // Size of the crc-covered header prefix (everything before crc32).
        static constexpr size_t PREFIX_SIZE =
            sizeof(uint32_t) * 2 + sizeof(uint64_t) + sizeof(uint32_t);

        // Zero-padding between key and value for the given key length.
        static constexpr uint32_t ValuePadding(uint32_t key_len) {
            const uint64_t head = SIZE + key_len;
            return static_cast<uint32_t>(
                (kValueAlignment - head % kValueAlignment) % kValueAlignment);
        }

        // Offset of the value region relative to the record start.
        static constexpr uint64_t ValueOffsetInRecord(uint32_t key_len) {
            return SIZE + key_len + ValuePadding(key_len);
        }

        // Total on-disk record size including padding.
        static constexpr uint64_t RecordSize(uint32_t key_len,
                                             uint32_t value_len) {
            return ValueOffsetInRecord(key_len) + value_len;
        }

        bool HasCrc() const { return (flags & kFlagHasCrc) != 0; }

        void WritePrefixTo(char* out) const {
            std::memcpy(out, &key_len, sizeof(key_len));
            std::memcpy(out + sizeof(key_len), &value_len, sizeof(value_len));
            std::memcpy(out + sizeof(key_len) + sizeof(value_len), &seq,
                        sizeof(seq));
            std::memcpy(out + sizeof(key_len) + sizeof(value_len) + sizeof(seq),
                        &flags, sizeof(flags));
        }

        void WriteTo(char* out) const {
            WritePrefixTo(out);
            std::memcpy(out + PREFIX_SIZE, &crc32, sizeof(crc32));
        }

        static RecordHeader ReadFrom(const char* buf) {
            RecordHeader h{};
            size_t off = 0;
            std::memcpy(&h.key_len, buf + off, sizeof(h.key_len));
            off += sizeof(h.key_len);
            std::memcpy(&h.value_len, buf + off, sizeof(h.value_len));
            off += sizeof(h.value_len);
            std::memcpy(&h.seq, buf + off, sizeof(h.seq));
            off += sizeof(h.seq);
            std::memcpy(&h.flags, buf + off, sizeof(h.flags));
            off += sizeof(h.flags);
            std::memcpy(&h.crc32, buf + off, sizeof(h.crc32));
            return h;
        }

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

   private:
    // Maximum key length accepted by BatchOffload and trusted on recovery.
    // Write-side enforcement (BatchOffload) and recovery-side validation
    // (RebuildShardMapsFromAllocator) must agree on this bound.
    static constexpr uint32_t kMaxKeyLen = 1024 * 1024;

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

        // Total record size: header + key + padding + value
        // (see RecordHeader::RecordSize)
        uint32_t total_size;

        // Value size only (excluding header and key)
        uint32_t value_size;

        // Refcounted handle keeps physical extent alive during reads
        AllocationPtr allocation;

        // Monotonic insertion sequence number. Points back to the slot in
        // fifo_index_ (seq -> key). Used during eviction to detect stale
        // index entries (lazy-repair) and to remove old slots on overwrite.
        uint64_t fifo_seq = 0;

        ObjectEntry(uint64_t off, uint32_t total, uint32_t val,
                    AllocationPtr alloc_ptr, uint64_t seq = 0)
            : offset(off),
              total_size(total),
              value_size(val),
              allocation(std::move(alloc_ptr)),
              fifo_seq(seq) {}
    };

    // Keeps evicted metadata and allocation handles alive until the master
    // accepts the replica-removal notification.
    struct PendingEviction {
        std::vector<std::pair<std::string, ObjectEntry>> objects;
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

    // File handle wrapper for I/O operations using preadv/pwritev. Held as a
    // shared_ptr so that an in-flight BatchLoad (which copies it into its
    // ReadPlan under the shard lock) keeps the old file alive while RemoveAll
    // rebinds this member to a freshly rebuilt file. Avoids use-after-free
    // when RemoveAll runs concurrently with a reader on another thread.
    std::shared_ptr<StorageFile> data_file_;

    // Sharded metadata maps: one map per shard with its own lock (prevents data
    // races)
    std::array<MetadataShard, kNumShards> shards_;

    // Total bytes stored (headers + keys + values), updated atomically without
    // locks
    std::atomic<int64_t> total_size_{0};

    // Total number of keys, updated atomically (avoids locking all shards for
    // counting)
    std::atomic<int64_t> total_keys_{0};

    // ===== Eviction-related members =====
    OffsetAllocatorBackendConfig cfg_;

    // Counter for keys skipped due to fallback eviction exhaustion.
    // See GetEvictionSkips() for the public accessor.
    std::atomic<int64_t> eviction_skips_{0};

    // Mutex protecting fifo_index_ and insert_seq_. Must be acquired BEFORE
    // any shard mutex (shards_[i].mutex) when both are held.
    mutable Mutex eviction_mutex_;

    // Global FIFO index: insertion sequence number -> key.
    // begin() = oldest key, the default eviction victim.
    // Entries allowed to be stale; lazy-repair at eviction time.
    std::map<uint64_t, std::string> fifo_index_;

    // Monotonic sequence number source for fifo_index_.
    std::atomic<uint64_t> insert_seq_{0};

    // Resolved watermark thresholds (bytes), computed in Init().
    int64_t high_watermark_bytes_ = 0;
    int64_t low_watermark_bytes_ = 0;

    // Resolved watermark thresholds (key count), computed in Init().
    int64_t high_watermark_keys_ = 0;
    int64_t low_watermark_keys_ = 0;

    // Evict keys from the FIFO index until both byte and key-count watermarks
    // are satisfied (or until the eviction cap is reached). Allocations remain
    // pinned in out_pending until notification succeeds.
    void EvictToMakeRoom(int64_t required_bytes, size_t min_victims,
                         const std::unordered_set<std::string>& batch_keys,
                         PendingEviction& out_pending);

    // Restore prepared victims when the master rejects their removal.
    void RestorePreparedEviction(PendingEviction&& pending);

    // Notify the master, then release prepared allocations on success or
    // restore their metadata on failure.
    tl::expected<void, ErrorCode> NotifyAndCommitPreparedEviction(
        const EvictionHandler& eviction_handler, PendingEviction& pending);

    // Record restart-persistence tombstones for prepared victims whose
    // eviction has become final.  No-op when persistence is disabled.
    void RecordEvictionTombstones(const PendingEviction& pending);

    // ===== Persistence methods =====

    std::string GetMetaFilePath() const;

    bool ShouldPersistNow() const;

    tl::expected<void, ErrorCode> SaveMetadata(
        const std::unordered_set<std::string>& evicted_keys_this_batch);

    tl::expected<OffsetAllocatorPersistedMetadata, ErrorCode> LoadMetadata();

    // Outcome of a recovery attempt.  Distinguishes "safe to start fresh"
    // (kNoMeta / kCorrupt) from "must not touch the persisted data"
    // (kTransientError, e.g. fd exhaustion or OOM — retrying Init later may
    // succeed, while a fresh start would destroy a recoverable cache).
    enum class RecoveryResult {
        kRecovered,       // persisted state fully restored
        kNoMeta,          // no metadata file: genuine first boot
        kCorrupt,         // meta/data missing, incompatible or corrupt
        kTransientError,  // temporary resource error: do NOT wipe data
    };

    RecoveryResult TryRecoverFromMetadata();

    // Rebuilds shard maps by scanning extents marked used in the
    // deserialized allocator.  Records stamped with
    // seq >= checkpoint_insert_seq were written after the checkpoint and
    // are dropped (their extents may hold torn writes).
    void RebuildShardMapsFromAllocator(uint64_t checkpoint_insert_seq);

    void RestoreAndRepairFifoIndex(
        const OffsetAllocatorPersistedMetadata& meta);

    // Test-only: Predicate to determine which keys should fail in BatchOffload.
    // Used for deterministic testing of partial success behavior.
    std::function<bool(const std::string& key)> test_failure_predicate_;

   private:
    // ---- Persistence state ----
    std::atomic<int64_t> last_persist_time_us_{0};
    std::unordered_set<std::string> all_evicted_this_batch_;
    std::atomic<bool> metadata_dirty_{false};

    // ---- Persistence metrics ----
    std::atomic<int64_t> last_save_metadata_cost_us_{0};
    std::atomic<int64_t> metadata_save_failures_{0};
    std::atomic<int64_t> metadata_load_fallbacks_{0};
    std::atomic<int64_t> metadata_consecutive_failures_{0};

    // ---- Test-only hooks ----
    std::atomic<int> test_metadata_write_failure_step_{0};
    // Skip the destructor's final checkpoint (simulates an abrupt crash).
    std::atomic<bool> test_skip_final_checkpoint_{false};

   public:
    // ---- Test accessors ----
    void SetMetadataWriteFailure(int step) {
        test_metadata_write_failure_step_ = step;
    }
    void SetSkipFinalCheckpointForTest() {
        test_skip_final_checkpoint_.store(true, std::memory_order_relaxed);
    }
    size_t GetAllEvictedThisBatchSizeForTest() const {
        return all_evicted_this_batch_.size();
    }
    int64_t GetMetadataSaveFailures() const {
        return metadata_save_failures_.load(std::memory_order_relaxed);
    }
    int64_t GetMetadataLoadFallbacks() const {
        return metadata_load_fallbacks_.load(std::memory_order_relaxed);
    }
    int64_t GetMetadataConsecutiveFailures() const {
        return metadata_consecutive_failures_.load(std::memory_order_relaxed);
    }

   private:
};

tl::expected<std::shared_ptr<StorageBackendInterface>, ErrorCode>
CreateStorageBackend(const FileStorageConfig& config);

}  // namespace mooncake

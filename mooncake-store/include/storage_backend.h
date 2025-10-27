#pragma once

#include <glog/logging.h>
#include <mutex>
#include <string>
#include <vector>
#include <filesystem>

#include "types.h"
#include "file_interface.h"

namespace mooncake {

struct SequentialObjectMetadata {
    size_t offset;
    size_t key_size;
    size_t data_size;
};
YLT_REFL(SequentialObjectMetadata, offset, key_size, data_size);

struct SequentialBucketMetadata {
    mutable std::shared_mutex statistics_mutex;
    size_t meta_size;
    size_t data_size;
    std::unordered_map<std::string, SequentialObjectMetadata> object_metadata;
    std::vector<std::string> keys;
};
YLT_REFL(SequentialBucketMetadata, data_size, object_metadata, keys);

struct SequentialOffloadMetadata {
    size_t total_keys;
    size_t total_size;
};

enum class FileMode { Read, Write };

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
                            const std::string& fsdir, bool is_3fs_dir)
        : root_dir_(root_dir), fsdir_(fsdir), is_3fs_dir_(is_3fs_dir) {
        resource_manager_ = std::make_unique<USRBIOResourceManager>();
        Hf3fsConfig config;
        config.mount_root = root_dir;
        resource_manager_->setDefaultParams(config);
    }
#else
    explicit StorageBackend(const std::string& root_dir,
                            const std::string& fsdir)
        : root_dir_(root_dir), fsdir_(fsdir) {}
#endif

    /**
     * @brief Factory method to create a StorageBackend instance
     * @param root_dir Root directory path for object storage
     * @param fsdir  subdirectory name
     * @return shared_ptr to new instance or nullptr if directory is invalid
     *
     * Performs validation of the root directory before creating the instance:
     * - Verifies directory exists
     * - Verifies path is actually a directory
     */
    static std::shared_ptr<StorageBackend> Create(const std::string& root_dir,
                                                  const std::string& fsdir) {
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
                                                is_3fs_dir);
#else
        return std::make_shared<StorageBackend>(root_dir, real_fsdir);
#endif
    }

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
                                             size_t length);

    /**
     * @brief Loads an object as a string
     * @param path path for the object
     * @param str Output string for loaded data
     * @param length Expected length of data to read
     * @return tl::expected<void, ErrorCode> indicating operation status
     */
    tl::expected<void, ErrorCode> LoadObject(const std::string& path,
                                             std::string& str, size_t length);

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

#ifdef USE_3FS
    bool is_3fs_dir_{false};  // Flag to indicate if the storage is using 3FS
                              // directory structure
    std::unique_ptr<USRBIOResourceManager> resource_manager_;
#endif

   private:
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
};

class SequentialStorageBackend {
   public:
    SequentialStorageBackend(const std::string& storage_filepath);

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
        std::function<ErrorCode(
            const std::unordered_map<std::string, SequentialObjectMetadata>&)>
            complete_handler);

    /**
     * @brief Retrieves metadata for multiple objects in a single batch
     * operation.
     * @param keys A list of object keys to query metadata for.
     * @param batche_object_metadata Output parameter that receives the
     * retrieved metadata.
     * @return tl::expected<void, ErrorCode> indicating operation status.
     */
    tl::expected<void, ErrorCode> BatchQuery(
        const std::vector<std::string>& keys,
        std::unordered_map<std::string, SequentialObjectMetadata>&
            batche_object_metadata);

    /**
     * @brief Loads data for multiple objects in a batch operation.
     * @param batched_slices A map from object key to a pre-allocated writable
     * buffer (Slice).
     * @return tl::expected<void, ErrorCode> indicating operation status.
     */
    tl::expected<void, ErrorCode> BatchLoad(
        std::unordered_map<std::string, Slice>& batched_slices);

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
     * @brief Initializes the sequential storage backend.
     * @return tl::expected<void, ErrorCode> indicating operation status.
     */
    tl::expected<void, ErrorCode> Init();

    /**
     * @brief Checks whether an object with the specified key exists in the
     * storage system.
     * @param key The unique identifier of the object to check for existence.
     * @return tl::expected<void, ErrorCode> indicating operation status.
     */
    tl::expected<bool, ErrorCode> IsExist(const std::string& key);

    /**
     * @brief Iterate over the metadata of stored objects starting from a
     * specified bucket.
     * @param bucket_id The ID of the bucket to start scanning from.
     * @param objects Output parameter: a map from object key to its metadata.
     * @param buckets Output parameter: a list of bucket IDs encountered during
     * iteration.
     * @param limit Maximum number of objects to return in this iteration.
     * @return tl::expected<int64_t, ErrorCode>
     * - On success: the bucket ID where the next iteration should start  (or 0
     * if all data has been scanned).
     * - On failure: returns an error code (e.g., BUCKET_NOT_FOUND, IO_ERROR).
     */
    tl::expected<int64_t, ErrorCode> BucketScan(
        int64_t bucket_id,
        std::unordered_map<std::string, SequentialObjectMetadata>& objects,
        std::vector<int64_t>& buckets, size_t limit);

    /**
     * @brief Retrieves the global metadata of the sequential store.
     * @return On success: `tl::expected` containing a `SequentialStoreMetadata`
     * object. On failure: an error code.
     */
    tl::expected<SequentialOffloadMetadata, ErrorCode> GetStoreMetadata();

   private:
    tl::expected<std::shared_ptr<SequentialBucketMetadata>, ErrorCode>
    BuildBucket(
        int64_t bucket_id,
        const std::unordered_map<std::string, std::vector<Slice>>& batch_object,
        std::vector<iovec>& iovs);

    tl::expected<void, ErrorCode> WriteBucket(
        int64_t bucket_id,
        std::shared_ptr<SequentialBucketMetadata> bucket_metadata,
        std::vector<iovec>& iovs);

    tl::expected<void, ErrorCode> StoreBucketMetadata(
        int64_t bucket_id,
        std::shared_ptr<SequentialBucketMetadata> bucket_metadata);

    tl::expected<void, ErrorCode> LoadBucketMetadata(
        int64_t bucket_id,
        std::shared_ptr<SequentialBucketMetadata> bucket_metadata);

    tl::expected<void, ErrorCode> BatchLoadBucket(
        int64_t bucket_id, const std::vector<std::string>& keys,
        std::unordered_map<std::string, Slice>& batched_slices);

    tl::expected<int64_t, ErrorCode> CreateBucketId();

    tl::expected<std::string, ErrorCode> GetBucketMetadataPath(
        int64_t bucket_id);

    tl::expected<std::string, ErrorCode> GetBucketDataPath(int64_t bucket_id);

    tl::expected<std::unique_ptr<StorageFile>, ErrorCode> OpenFile(
        const std::string& path, FileMode mode) const;

   private:
    bool initialized_ = false;
    static constexpr const char* BUCKET_METADATA_FILE_SUFFIX = ".meta";
    static constexpr int SEQUENCE_BITS = 12;
    static constexpr int SEQUENCE_ID_SHIFT = 0;
    static constexpr int TIMESTAMP_SHIFT = SEQUENCE_BITS;
    static constexpr int64_t SEQUENCE_MASK = (1 << SEQUENCE_BITS) - 1;

    int64_t m_i64SequenceID = 0;
    int64_t m_i64LastTimeStamp = 0;

    /**
     * @brief A shared mutex to protect concurrent access to metadata.
     *
     * This mutex is used to synchronize read/write operations on the following
     * metadata members:
     * - object_bucket_map_: maps object keys to bucket IDs
     * - buckets_: ordered map of bucket ID to bucket metadata
     * - total_size_: cumulative data size of all stored objects
     */
    mutable std::shared_mutex mutex_;
    std::string storage_path_;
    size_t total_size_ = 0;
    std::unordered_map<std::string, int64_t> object_bucket_map_;
    std::map<int64_t, std::shared_ptr<SequentialBucketMetadata>> buckets_;
};
}  // namespace mooncake

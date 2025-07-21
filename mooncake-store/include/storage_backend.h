#pragma once

#include <string>
#include <vector>
#include <mutex>
#include <fstream>
#include <types.h>
#include <file_interface.h>
#include <filesystem>
#include <thread>
#include <chrono>

namespace mooncake {
/**
 * @class StorageBackend
 * @brief Implementation of StorageBackend interface using local filesystem storage.
 * 
 * Provides thread-safe operations for storing and retrieving objects in a directory hierarchy.
 */
class StorageBackend  {
   public:
    /**
     * @brief Constructs a new StorageBackend instance
     * @param root_dir Root directory path for object storage
     * @param fsdir  subdirectory name
     * @note Directory existence is not checked in constructor
     */
    #ifdef USE_3FS
    explicit StorageBackend(const std::string& root_dir, const std::string& fsdir, bool is_3fs_dir)
        : root_dir_(root_dir), fsdir_(fsdir), is_3fs_dir_(is_3fs_dir) {
            resource_manager_ = std::make_unique<USRBIOResourceManager>();
            Hf3fsConfig config;
            config.mount_root = root_dir;
            resource_manager_->setDefaultParams(config);
    }
    #else
    explicit StorageBackend(const std::string& root_dir, const std::string& fsdir)
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
    static std::shared_ptr<StorageBackend> Create(const std::string& root_dir, const std::string& fsdir) {
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
        return std::make_shared<StorageBackend>(root_dir, real_fsdir, is_3fs_dir);
    #else
        return std::make_shared<StorageBackend>(root_dir, real_fsdir);
    #endif
    }  
    
    /**
     * @brief Stores an object composed of multiple slices
     * @param key Object identifier
     * @param slices Vector of data slices to store
     * @return tl::expected<void, ErrorCode> indicating operation status
     */
    tl::expected<void, ErrorCode> StoreObject(const ObjectKey& key, const std::vector<Slice>& slices) ;

    /**
     * @brief Stores an object from a string
     * @param key Object identifier
     * @param str String containing object data
     * @return tl::expected<void, ErrorCode> indicating operation status
     */
    tl::expected<void, ErrorCode> StoreObject(const ObjectKey& key, const std::string& str) ;

    /**
     * @brief Stores an object from a span of data
     * @param key Object identifier
     * @param data Span containing object data
     * @return tl::expected<void, ErrorCode> indicating operation status
     */
    tl::expected<void, ErrorCode> StoreObject(const ObjectKey& key, std::span<const char> data);
    
    /**
     * @brief Loads an object into slices
     * @param path KVCache File path to load from
     * @param slices Output vector for loaded data slices
     * @param length Expected length of data to read
     * @return tl::expected<void, ErrorCode> indicating operation status
     */
    tl::expected<void, ErrorCode> LoadObject(std::string& path, std::vector<Slice>& slices, size_t length) ;
    
    /**
     * @brief Loads an object as a string
     * @param path KVCache File path to load from
     * @param str Output string for loaded data
     * @param length Expected length of data to read
     * @return tl::expected<void, ErrorCode> indicating operation status
     */
    tl::expected<void, ErrorCode> LoadObject(std::string& path, std::string& str, size_t length) ;

    /**
     * @brief Checks if an object with the given key exists
     * @param key Object identifier
     * @return bool indicating whether the object exists
     */
    bool Existkey(const ObjectKey& key) ;

    /**
     * @brief Queries metadata for an object by key
     * @param key Object identifier
     * @return Optional Replica::Descriptor containing object metadata, or empty if not found
     * 
     * This method retrieves the file path and size for the given object key.
     */
    std::optional<Replica::Descriptor> Querykey(const ObjectKey& key);

    /**
     * @brief Batch queries metadata for multiple object keys
     * @param keys Vector of object identifiers
     * @return unordered_map mapping ObjectKey to Replica::Descriptor
     */
    std::unordered_map<ObjectKey, Replica::Descriptor> BatchQueryKey(const std::vector<ObjectKey>& keys);

    /**
     * @brief Deletes the physical file associated with the given object key
     * @param key Object identifier
     */
    void RemoveFile(const ObjectKey& key) ;

    /**
     * @brief Deletes all objects from the storage backend
     * 
     * Removes all files in the cluster subdirectory.
     */
    void RemoveAll() ;

    enum class FileMode {
        Read,
        Write
    };
    // Root directory path for storage and  subdirectory name
    std::string root_dir_;
    std::string fsdir_;

    #ifdef USE_3FS
    bool is_3fs_dir_{false};  // Flag to indicate if the storage is using 3FS directory structure
    std::unique_ptr<USRBIOResourceManager> resource_manager_;
    #endif

   private:
    /**
     * @brief Sanitizes object key for filesystem safety
     */
    std::string SanitizeKey(const ObjectKey& key) const;
    
    /**
     * @brief Resolves full filesystem path for an object
     */
    std::string ResolvePath(const ObjectKey& key) const;

    /**
     * @brief Creates a file object for the specified path and mode
     * @param path Filesystem path for the file
     * @param mode File access mode (read/write)
     * @return Unique pointer to the created StorageFile, or nullptr on failure
     */
    std::unique_ptr<StorageFile> create_file(const std::string& path, 
                                           FileMode mode) const;

};

}  // namespace mooncake
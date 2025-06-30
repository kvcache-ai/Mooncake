#pragma once

#include <string>
#include <vector>
#include <mutex>
#include <fstream>
#include <types.h>
#include <local_file.h>
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
    explicit StorageBackend(const std::string& root_dir, const std::string& fsdir): root_dir_(root_dir), fsdir_(fsdir) {}

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
        std::string real_fsdir = "moon_" + fsdir;
        return std::make_shared<StorageBackend>(root_dir, real_fsdir);
    }  
    
    /**
     * @brief Stores an object composed of multiple slices
     * @param key Object identifier
     * @param slices Vector of data slices to store
     * @return ErrorCode indicating operation status
     */
    ErrorCode StoreObject(const ObjectKey& key, const std::vector<Slice>& slices) ;
    
    /**
     * @brief Stores an object from a string
     * @param key Object identifier
     * @param str String containing object data
     * @return ErrorCode indicating operation status
     */
    ErrorCode StoreObject(const ObjectKey& key, const std::string& str) ;
    
    /**
     * @brief Loads an object into slices
     * @param key Object identifier
     * @param slices Output vector for loaded data slices
     * @return ErrorCode indicating operation status
     */
    ErrorCode LoadObject(const ObjectKey& key, std::vector<Slice>& slices) ;

    /**
     * @brief Loads an object into slices
     * @param key Object identifier
     * @param slices Output vector for loaded data slices
     * @return ErrorCode indicating operation status
     */
    ErrorCode LoadObject(const ObjectKey& key, std::vector<Slice>& slices, std::string& path) ;
    
    /**
     * @brief Loads an object as a string
     * @param key Object identifier
     * @param str Output string for loaded data
     * @return ErrorCode indicating operation status
     */
    ErrorCode LoadObject(const ObjectKey& key, std::string& str) ;

    /**
     * @brief Queries metadata for an object by key
     * @param key Object identifier
     * @return Optional Replica::Descriptor containing object metadata, or empty if not found
     * 
     * This method retrieves the file path and size for the given object key.
     */
    std::optional<Replica::Descriptor> StorageBackend::Querykey(const ObjectKey& key);

    /**
     * @brief Batch queries metadata for multiple object keys
     * @param keys Vector of object identifiers
     * @return unordered_map mapping ObjectKey to Replica::Descriptor
     */
    std::unordered_map<ObjectKey, Replica::Descriptor> BatchQueryKey(const std::vector<ObjectKey>& keys);

    /**
     * @brief Checks if an object with the given key exists
     * @param key Object identifier
     * @return ErrorCode::OK if exists, ErrorCode::FILE_NOT_FOUND if not exists, other ErrorCode for errors
     */
    ErrorCode Existkey(const ObjectKey& key) ;

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

    // Root directory path for storage and  subdirectory name
    std::string root_dir_;
    std::string fsdir_;
    
   private:
    /**
     * @brief Sanitizes object key for filesystem safety
     */
    std::string SanitizeKey(const ObjectKey& key) const;
    
    /**
     * @brief Resolves full filesystem path for an object
     */
    std::string ResolvePath(const ObjectKey& key) const;

    ErrorCode LoadObjectInPath(const std::string& path,
                                    std::vector<Slice>& slices);

};

}  // namespace mooncake
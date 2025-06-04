#include <string>
#include <vector>
#include <mutex>
#include <fstream>
#include <types.h>
#include <storage_backend.h>
#include <local_file.h>
#include <filesystem>

namespace mooncake {
/**
 * @class FileStorageBackend
 * @brief Implementation of StorageBackend interface using local filesystem storage.
 * 
 * Provides thread-safe operations for storing and retrieving objects in a directory hierarchy.
 */
class FileStorageBackend : public StorageBackend {
   public:
    /**
     * @brief Constructs a new FileStorageBackend instance
     * @param root_dir Root directory path for object storage
     * @note Directory existence is not checked in constructor
     */
    explicit FileStorageBackend(const std::string& root_dir): root_dir_(root_dir) {}

    /**
     * @brief Factory method to create a FileStorageBackend instance
     * @param root_dir Root directory path for object storage
     * @return shared_ptr to new instance or nullptr if directory is invalid
     * 
     * Performs validation of the root directory before creating the instance:
     * - Verifies directory exists
     * - Verifies path is actually a directory
     */
    static std::shared_ptr<FileStorageBackend> Create(const std::string& root_dir) {
        namespace fs = std::filesystem;
        if (!fs::exists(root_dir)) {
            LOG(INFO) << "Root directory does not exist: " << root_dir;
            return nullptr;
        } else if (!fs::is_directory(root_dir)) {
            LOG(INFO) << "Root path is not a directory: " << root_dir;
            return nullptr;
        }
        return std::make_shared<FileStorageBackend>(root_dir);
    }  
    
    /**
     * @brief Stores an object composed of multiple slices
     * @param key Object identifier
     * @param slices Vector of data slices to store
     * @return ErrorCode indicating operation status
     */
    ErrorCode StoreObject(const ObjectKey& key, const std::vector<Slice>& slices) override;
    
    /**
     * @brief Stores an object from a string
     * @param key Object identifier
     * @param str String containing object data
     * @return ErrorCode indicating operation status
     */
    ErrorCode StoreObject(const ObjectKey& key, const std::string& str) override;
    
    /**
     * @brief Loads an object into slices
     * @param key Object identifier
     * @param slices Output vector for loaded data slices
     * @return ErrorCode indicating operation status
     */
    ErrorCode LoadObject(const ObjectKey& key, std::vector<Slice>& slices) override;
    
    /**
     * @brief Loads an object as a string
     * @param key Object identifier
     * @param str Output string for loaded data
     * @return ErrorCode indicating operation status
     */
    ErrorCode LoadObject(const ObjectKey& key, std::string& str) override;

    /// Root directory path for storage
    std::string root_dir_;
    
   private:
    /**
     * @brief Sanitizes object key for filesystem safety
     */
    std::string SanitizeKey(const ObjectKey& key) const;
    
    /**
     * @brief Resolves full filesystem path for an object
     */
    std::string ResolvePath(const ObjectKey& key) const;

};

}  // namespace mooncake
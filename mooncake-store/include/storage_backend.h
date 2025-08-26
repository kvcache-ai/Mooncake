#pragma once

#include <glog/logging.h>

#include <string>
#include <vector>
#include <filesystem>

#include "types.h"
#include "file_interface.h"

namespace mooncake {
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

}  // namespace mooncake

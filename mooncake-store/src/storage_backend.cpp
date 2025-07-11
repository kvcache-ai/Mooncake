
// #include "storage_backend.h"
#include "storage_backend.h"
#include <fcntl.h>
#include <unistd.h>
#include <sys/uio.h>
#include <string>
#include <vector>


namespace mooncake {
  
ErrorCode StorageBackend::StoreObject(const ObjectKey& key,
                                     const std::vector<Slice>& slices) {
    std::string path = ResolvePath(key);

    if(std::filesystem::exists(path) == true) {
        return ErrorCode::FILE_OPEN_FAIL;
    }

    auto file = create_file(path, "wb");
    if (!file) {
        LOG(INFO) << "Failed to open file for writing: " << path;
        return ErrorCode::FILE_OPEN_FAIL;
    }

    std::vector<iovec> iovs;
    size_t slices_total_size = 0;
    for (const auto& slice : slices) {
        iovec io{ slice.ptr, slice.size };
        iovs.push_back(io);
        slices_total_size += slice.size;
    }

    ssize_t ret = file->pwritev(iovs.data(), static_cast<int>(iovs.size()), 0);

    if (ret < 0) {
        LOG(INFO) << "pwritev failed for: " << path;
        return ErrorCode::FILE_WRITE_FAIL;
    }

    if (ret != static_cast<ssize_t>(slices_total_size)) {
        LOG(INFO) << "Write size mismatch for: " << path
                   << ", expected: " << slices_total_size
                   << ", got: " << ret;
        return ErrorCode::FILE_WRITE_FAIL;
    }
    // TODO: Determine whether the data has been completely and correctly written.
    // If the write operation fails, the corresponding file should be deleted 
    // to prevent incomplete data from being found in subsequent get operations. 
    // Alternatively, a marking method can be used to record that the file is not a valid file.

    // Note: fclose is not necessary here as LocalFile destructor will handle it

    return ErrorCode::OK;
}

ErrorCode StorageBackend::StoreObject(const ObjectKey& key,
                                    const std::string& str) {
    std::string path = ResolvePath(key);

    if(std::filesystem::exists(path) == true) {
        return ErrorCode::FILE_OPEN_FAIL;
    }
    
    auto file = create_file(path, "wb");
    if (!file) {
        LOG(INFO) << "Failed to open file for writing: " << path;
        return ErrorCode::FILE_OPEN_FAIL;
    }

    size_t file_total_size = str.size();
    ssize_t ret = file->write(str, file_total_size);

    if (ret < 0) {
        LOG(INFO) << "pwritev failed for: " << path;

        return ErrorCode::FILE_WRITE_FAIL;
    }
    if (ret != static_cast<ssize_t>(file_total_size)) {
        LOG(INFO) << "Write size mismatch for: " << path
                   << ", expected: " << file_total_size
                   << ", got: " << ret;

        return ErrorCode::FILE_WRITE_FAIL;
    }
    // Note: fclose is not necessary here as LocalFile destructor will handle it

    return ErrorCode::OK;
}

ErrorCode StorageBackend::LoadObject(std::string& path,
                                    std::vector<Slice>& slices, size_t length) {

    auto file = create_file(path, "rb");
    if (!file) {
        LOG(INFO) << "Failed to open file for reading: " << path;
        return ErrorCode::FILE_OPEN_FAIL;
    }

    std::vector<iovec> iovs;                                    

    for (const auto& slice : slices) {
        iovec io{ slice.ptr, slice.size };
        iovs.push_back(io);
    }

    ssize_t ret = file->preadv(iovs.data(), static_cast<int>(iovs.size()), 0);

    if (ret < 0) {
        LOG(INFO) << "preadv failed for: " << path;

        return ErrorCode::FILE_READ_FAIL;
    }
    if (ret != static_cast<ssize_t>(length)) {
        LOG(INFO) << "Read size mismatch for: " << path
                   << ", expected: " << length
                   << ", got: " << ret;

        return ErrorCode::FILE_READ_FAIL;
    }

    // Note: fclose is not necessary here as LocalFile destructor will handle it

    return ErrorCode::OK;
}

ErrorCode StorageBackend::LoadObject(std::string& path,
                                    std::string& str, size_t length) {

    auto file = create_file(path, "rb");
    if (!file) {
        LOG(INFO) << "Failed to open file for reading: " << path;
        return ErrorCode::FILE_OPEN_FAIL;
    }

    ssize_t ret = file->read(str, length);

    if (ret < 0) {
        LOG(INFO) << "read failed for: " << path;
        return ErrorCode::FILE_READ_FAIL;
    }
    if (ret != static_cast<ssize_t>(length)) {
        LOG(INFO) << "Read size mismatch for: " << path
                   << ", expected: " << length
                   << ", got: " << ret;

        return ErrorCode::FILE_READ_FAIL;
    }

    // Note: fclose is not necessary here as LocalFile destructor will handle it

    return ErrorCode::OK;
}

std::optional<Replica::Descriptor> StorageBackend::Querykey(const ObjectKey& key) {
    std::string path = ResolvePath(key);
    namespace fs = std::filesystem;

    // Check if the file exists
    if (!fs::exists(path)) {
        return std::nullopt;  // File does not exist
    }

    // Populate object_info with file metadata
    Replica::Descriptor desc;
    auto& disk_desc = desc.descriptor_variant.emplace<DiskDescriptor>();
    disk_desc.file_path = path;
    disk_desc.file_size = fs::file_size(path);
    desc.status = ReplicaStatus::COMPLETE;
    
    return desc;
}

std::unordered_map<ObjectKey, Replica::Descriptor>
StorageBackend::BatchQueryKey(const std::vector<ObjectKey>& keys) {
    namespace fs = std::filesystem;
    std::unordered_map<ObjectKey, Replica::Descriptor> result;

    for (const auto& key : keys) {
        std::string path = ResolvePath(key);

        if (!fs::exists(path)) {
            LOG(WARNING) << "Key not found: " << key << ", skipping...";
            return {}; 
        }

        Replica::Descriptor desc;
        auto& disk_desc = desc.descriptor_variant.emplace<DiskDescriptor>();
        disk_desc.file_path = path;
        disk_desc.file_size = fs::file_size(path);
        desc.status = ReplicaStatus::COMPLETE;

        result.emplace(key, std::move(desc));
    }

    return result;
}

ErrorCode StorageBackend::Existkey(const ObjectKey& key) {
    std::string path = ResolvePath(key);
    namespace fs = std::filesystem;

    // Check if the file exists
    if (fs::exists(path)) {
        return ErrorCode::OK;
    } else {
        return ErrorCode::FILE_NOT_FOUND;
    }
}

void StorageBackend::RemoveFile(const ObjectKey& key) {
    std::string path = ResolvePath(key);
    namespace fs = std::filesystem;
    // TODO: attention: this function is not thread-safe, need to add lock if used in multi-thread environment
    // Check if the file exists before attempting to remove it
    // TODO: add a sleep to ensure the write thread has time to create the corresponding file
    // it will be fixed in the next version
    std::this_thread::sleep_for(std::chrono::microseconds(50));  //sleep for 50 us
    if (fs::exists(path)) {
        std::error_code ec;
        fs::remove(path, ec);
        if (ec) {
            LOG(ERROR) << "Failed to delete file: " << path << ", error: " << ec.message();
        }
    } 
}

void StorageBackend::RemoveAll() {
    namespace fs = std::filesystem;
    // Iterate through the root directory and remove all files
    for (const auto& entry : fs::directory_iterator(root_dir_)) {
        if (fs::is_regular_file(entry.status())) {
            std::error_code ec;
            fs::remove(entry.path(),ec);
            if (ec) {
                LOG(ERROR) << "Failed to delete file: " << entry.path() << ", error: " << ec.message();
            }
        }
    }

}

std::string StorageBackend::SanitizeKey(const ObjectKey& key) const {
    // Set of invalid filesystem characters to be replaced
    constexpr std::string_view kInvalidChars = "/\\:*?\"<>|";
    std::string sanitized_key;
    sanitized_key.reserve(key.size());
    
    for (char c : key) {
        // Replace invalid characters with underscore
        sanitized_key.push_back(
            kInvalidChars.find(c) != std::string_view::npos ? '_' : c
        );
    }
    return sanitized_key;
}

std::string StorageBackend::ResolvePath(const ObjectKey& key) const {
    // Compute hash of the key 
    size_t hash = std::hash<std::string>{}(key);
    
    // Use low 8 bits to create 2-level directory structure (e.g. "a1/b2")
    char dir1 = static_cast<char>('a' + (hash & 0x0F));       // Lower 4 bits -> 16 dirs
    char dir2 = static_cast<char>('a' + ((hash >> 4) & 0x0F)); // Next 4 bits -> 16 subdirs
    
    // Safely construct path using std::filesystem
    namespace fs = std::filesystem;
    fs::path dir_path = fs::path(root_dir_) / fsdir_ / std::string(1, dir1) / std::string(1, dir2);

    // Create directory if not exists
    std::error_code ec;
    if (!fs::exists(dir_path)) {
        if (!fs::create_directories(dir_path, ec) && ec) {
            LOG(INFO) << "Failed to create directory: " << dir_path << ", error: " << ec.message();
            return ""; // Empty string indicates failure
        }
    }

    // Combine directory path with sanitized filename
    fs::path full_path = dir_path / SanitizeKey(key);
    
    return full_path.lexically_normal().string();
}

std::pair<int, int> StorageBackend::parse_mode_flags(const std::string& mode) const{
    int flags = O_CLOEXEC;
    int access_mode = 0;
    
    if (mode.find('w') != std::string::npos) {
        access_mode = O_WRONLY | O_CREAT | O_TRUNC;
    } else if (mode.find('r') != std::string::npos) {
        access_mode = O_RDONLY;
    } else if (mode.find('a') != std::string::npos) {
        access_mode = O_APPEND | O_CREAT;
    }
    
    if (mode.find('+') != std::string::npos) {
        access_mode = O_RDWR;
    }
    if (mode.find('x') != std::string::npos) {
        flags |= O_EXCL;
    }
    
    return {flags | access_mode, access_mode};
}

std::unique_ptr<StorageFile> StorageBackend::create_posix_file(
    const std::string& path, const std::string& mode) const 
{
    FILE* file = fopen(path.c_str(), mode.c_str());
    return file ? std::make_unique<PosixFile>(path, file) : nullptr;
}
#ifdef USE_3FS
std::unique_ptr<StorageFile> StorageBackend::create_3fs_file(
    const std::string& path, const std::string& mode) const {

    auto [flags, access_mode] = parse_mode_flags(mode);
    
    int fd = open(path.c_str(), flags, 0644);
    if (fd < 0) return nullptr;
    
    if (hf3fs_reg_fd(fd, 0) > 0) {
        close(fd);
        return nullptr;
    }

    return resource_manager_ ? 
                std::make_unique<ThreeFSFile>(path, fd, resource_manager_.get()) : nullptr;
}
#endif

std::unique_ptr<StorageFile> StorageBackend::create_file(
    const std::string& path, const std::string& mode) const {
#ifdef USE_3FS
    if (is_3fs_dir_) {
        return create_3fs_file(path, mode); 
    }
#endif
    return create_posix_file(path, mode);   
}

}  // namespace mooncake
#include "file_storage_backend.h"
#include "local_file.h"
#include <sys/uio.h>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <glog/logging.h>

namespace mooncake {

FileStorageBackend::FileStorageBackend(const std::string& root_dir)
    : root_dir_(root_dir) {
    std::filesystem::create_directories(root_dir_);
}

std::string FileStorageBackend::SanitizeKey(const ObjectKey& key) const {
    std::string safe_key = key;
    std::replace(safe_key.begin(), safe_key.end(), '/', '_');
    return safe_key;
}

std::string FileStorageBackend::ResolvePath(const ObjectKey& key) const {
    std::hash<std::string> hasher;
    size_t hash = hasher(key);

    std::stringstream ss;
    ss << std::hex << ((hash >> 16) & 0xFF) << '/' << ((hash >> 8) & 0xFF);
    std::string prefix = ss.str();

    std::string safe_key = SanitizeKey(key);
    std::filesystem::path full_dir = std::filesystem::path(root_dir_) / prefix;
    std::filesystem::create_directories(full_dir);
    return (full_dir / safe_key).string();
}

ErrorCode FileStorageBackend::Write(const ObjectKey& key, const std::vector<Slice>& slices) {
    std::string path = ResolvePath(key);
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) {
        LOG(ERROR) << "Failed to open file for write: " << path;
        return ErrorCode::BAD_FILE;
    }

    LocalFile local_file(path, f, ErrorCode::OK);
    std::vector<iovec> iovs;
    for (const auto& slice : slices) {
        iovec io{ slice.ptr, slice.size };
        iovs.push_back(io);
    }

    ssize_t ret = local_file.pwritev(iovs.data(), static_cast<int>(iovs.size()), 0);
    if (ret < 0) {
        LOG(ERROR) << "pwritev failed for: " << path;
        return ErrorCode::BAD_FILE;
    }

    return ErrorCode::OK;
}

ErrorCode FileStorageBackend::Read(const ObjectKey& key, std::vector<Slice>& slices) {
    std::string path = ResolvePath(key);
    FILE* f = fopen(path.c_str(), "rb");
    if (!f) {
        LOG(WARNING) << "File not found: " << path;
        return ErrorCode::NOT_FOUND;
    }

    LocalFile local_file(path, f, ErrorCode::OK);
    std::vector<iovec> iovs;
    for (auto& slice : slices) {
        iovec io{ slice.ptr, slice.size };
        iovs.push_back(io);
    }

    ssize_t ret = local_file.preadv(iovs.data(), static_cast<int>(iovs.size()), 0);
    if (ret < 0) {
        LOG(ERROR) << "preadv failed for: " << path;
        return ErrorCode::BAD_FILE;
    }

    return ErrorCode::OK;
}

}  // namespace mooncake

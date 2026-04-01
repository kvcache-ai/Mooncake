#include "ha/snapshot/object/backends/local/local_file_snapshot_object_store.h"

#include <algorithm>
#include <cstdlib>
#include <fstream>
#include <sstream>

#include <fmt/format.h>
#include <glog/logging.h>

namespace mooncake {

namespace {

constexpr const char* kEnvLocalPath = "MOONCAKE_SNAPSHOT_LOCAL_PATH";

}  // namespace

LocalFileSnapshotObjectStore::LocalFileSnapshotObjectStore() {
    const char* env_path = std::getenv(kEnvLocalPath);
    if (!env_path || !*env_path) {
        throw std::runtime_error(
            "MOONCAKE_SNAPSHOT_LOCAL_PATH environment variable is not set. "
            "Please set it to a persistent directory path for snapshot "
            "storage. Example: export "
            "MOONCAKE_SNAPSHOT_LOCAL_PATH=/data/mooncake_snapshots");
    }

    std::error_code ec;
    fs::create_directories(env_path, ec);
    if (ec) {
        throw std::runtime_error(
            fmt::format("Failed to create base_path directory '{}': {}",
                        env_path, ec.message()));
    }

    base_path_ = fs::canonical(env_path, ec);
    if (ec) {
        throw std::runtime_error(fmt::format(
            "Failed to resolve base_path '{}': {}", env_path, ec.message()));
    }
    if (!fs::is_directory(base_path_)) {
        throw std::runtime_error(fmt::format(
            "base_path '{}' is not a directory", base_path_.string()));
    }

    LOG(INFO) << "LocalFileSnapshotObjectStore initialized with path: "
              << base_path_;
}

LocalFileSnapshotObjectStore::LocalFileSnapshotObjectStore(
    const std::string& base_path) {
    if (base_path.empty()) {
        throw std::runtime_error(
            "LocalFileSnapshotObjectStore base_path is empty. "
            "Please provide a valid persistent directory path for snapshot "
            "storage.");
    }

    std::error_code ec;
    fs::create_directories(base_path, ec);
    if (ec) {
        throw std::runtime_error(
            fmt::format("Failed to create base_path directory '{}': {}",
                        base_path, ec.message()));
    }

    base_path_ = fs::canonical(base_path, ec);
    if (ec) {
        throw std::runtime_error(fmt::format(
            "Failed to resolve base_path '{}': {}", base_path, ec.message()));
    }
    if (!fs::is_directory(base_path_)) {
        throw std::runtime_error(fmt::format(
            "base_path '{}' is not a directory", base_path_.string()));
    }

    LOG(INFO) << "LocalFileSnapshotObjectStore initialized with path: "
              << base_path_;
}

fs::path LocalFileSnapshotObjectStore::KeyToPath(const std::string& key) const {
    return base_path_ / key;
}

tl::expected<void, std::string>
LocalFileSnapshotObjectStore::EnsureDirectoryExists(
    const fs::path& dir_path) const {
    try {
        if (!fs::exists(dir_path)) {
            if (!fs::create_directories(dir_path)) {
                return tl::make_unexpected(fmt::format(
                    "Failed to create directory: {}", dir_path.string()));
            }
        }
        return {};
    } catch (const fs::filesystem_error& e) {
        return tl::make_unexpected(
            fmt::format("Filesystem error creating directory {}: {}",
                        dir_path.string(), e.what()));
    }
}

bool LocalFileSnapshotObjectStore::IsPathWithinBase(
    const fs::path& path) const {
    std::error_code ec;
    fs::path canonical_path = fs::weakly_canonical(path, ec);
    if (ec) {
        return false;
    }

    auto [base_end, path_it] =
        std::mismatch(base_path_.begin(), base_path_.end(),
                      canonical_path.begin(), canonical_path.end());
    (void)path_it;
    return base_end == base_path_.end();
}

tl::expected<void, std::string> LocalFileSnapshotObjectStore::UploadBuffer(
    const std::string& key, const std::vector<uint8_t>& buffer) {
    if (buffer.empty()) {
        return tl::make_unexpected("Error: Buffer is empty");
    }

    fs::path full_path = KeyToPath(key);
    if (!IsPathWithinBase(full_path)) {
        return tl::make_unexpected(
            fmt::format("Security error: Path {} is outside base directory {}",
                        full_path.string(), base_path_.string()));
    }

    auto dir_result = EnsureDirectoryExists(full_path.parent_path());
    if (!dir_result) {
        return dir_result;
    }

    std::ofstream file(full_path, std::ios::binary | std::ios::trunc);
    if (!file) {
        return tl::make_unexpected(fmt::format(
            "Failed to open file for writing: {}", full_path.string()));
    }

    file.write(reinterpret_cast<const char*>(buffer.data()),
               static_cast<std::streamsize>(buffer.size()));
    if (!file) {
        return tl::make_unexpected(fmt::format(
            "Failed to write data to file: {}", full_path.string()));
    }

    file.close();
    VLOG(1) << "Successfully uploaded buffer to: " << full_path
            << ", size: " << buffer.size();
    return {};
}

tl::expected<void, std::string> LocalFileSnapshotObjectStore::DownloadBuffer(
    const std::string& key, std::vector<uint8_t>& buffer) {
    fs::path full_path = KeyToPath(key);
    if (!IsPathWithinBase(full_path)) {
        return tl::make_unexpected(
            fmt::format("Security error: Path {} is outside base directory {}",
                        full_path.string(), base_path_.string()));
    }
    if (!fs::exists(full_path)) {
        return tl::make_unexpected(
            fmt::format("File not found: {}", full_path.string()));
    }

    std::error_code ec;
    auto file_size = fs::file_size(full_path, ec);
    if (ec) {
        return tl::make_unexpected(
            fmt::format("Failed to get file size: {}, error: {}",
                        full_path.string(), ec.message()));
    }

    std::ifstream file(full_path, std::ios::binary);
    if (!file) {
        return tl::make_unexpected(fmt::format(
            "Failed to open file for reading: {}", full_path.string()));
    }

    buffer.resize(file_size);
    file.read(reinterpret_cast<char*>(buffer.data()),
              static_cast<std::streamsize>(file_size));
    if (!file) {
        return tl::make_unexpected(fmt::format(
            "Failed to read data from file: {}", full_path.string()));
    }

    VLOG(1) << "Successfully downloaded buffer from: " << full_path
            << ", size: " << buffer.size();
    return {};
}

tl::expected<void, std::string> LocalFileSnapshotObjectStore::UploadString(
    const std::string& key, const std::string& data) {
    fs::path full_path = KeyToPath(key);
    if (!IsPathWithinBase(full_path)) {
        return tl::make_unexpected(
            fmt::format("Security error: Path {} is outside base directory {}",
                        full_path.string(), base_path_.string()));
    }

    auto dir_result = EnsureDirectoryExists(full_path.parent_path());
    if (!dir_result) {
        return dir_result;
    }

    std::ofstream file(full_path, std::ios::trunc);
    if (!file) {
        return tl::make_unexpected(fmt::format(
            "Failed to open file for writing: {}", full_path.string()));
    }

    file << data;
    if (!file) {
        return tl::make_unexpected(fmt::format(
            "Failed to write string to file: {}", full_path.string()));
    }

    file.close();
    VLOG(1) << "Successfully uploaded string to: " << full_path;
    return {};
}

tl::expected<void, std::string> LocalFileSnapshotObjectStore::DownloadString(
    const std::string& key, std::string& data) {
    fs::path full_path = KeyToPath(key);
    if (!IsPathWithinBase(full_path)) {
        return tl::make_unexpected(
            fmt::format("Security error: Path {} is outside base directory {}",
                        full_path.string(), base_path_.string()));
    }
    if (!fs::exists(full_path)) {
        return tl::make_unexpected(
            fmt::format("File not found: {}", full_path.string()));
    }

    std::ifstream file(full_path);
    if (!file) {
        return tl::make_unexpected(fmt::format(
            "Failed to open file for reading: {}", full_path.string()));
    }

    std::stringstream buffer;
    buffer << file.rdbuf();
    data = buffer.str();

    VLOG(1) << "Successfully downloaded string from: " << full_path;
    return {};
}

tl::expected<void, std::string>
LocalFileSnapshotObjectStore::DeleteObjectsWithPrefix(
    const std::string& prefix) {
    fs::path target_dir = KeyToPath(prefix);
    if (!IsPathWithinBase(target_dir)) {
        LOG(ERROR) << "Security violation: Attempted to delete path outside "
                      "base directory. base_path="
                   << base_path_ << ", target_path=" << target_dir;
        return tl::make_unexpected(
            fmt::format("Security error: Path {} is outside base directory {}",
                        target_dir.string(), base_path_.string()));
    }

    std::error_code ec;
    fs::path canonical_target = fs::weakly_canonical(target_dir, ec);
    if (!ec && canonical_target == base_path_) {
        LOG(ERROR) << "Security violation: Attempted to delete base "
                      "directory itself. base_path="
                   << base_path_;
        return tl::make_unexpected(
            fmt::format("Security error: Cannot delete base directory {}",
                        base_path_.string()));
    }

    if (!fs::exists(target_dir)) {
        return {};
    }
    if (!fs::is_directory(target_dir)) {
        return tl::make_unexpected(fmt::format(
            "Target path '{}' is not a directory", target_dir.string()));
    }

    auto removed_count = fs::remove_all(target_dir, ec);
    if (ec) {
        return tl::make_unexpected(
            fmt::format("Failed to remove directory {}: {}",
                        target_dir.string(), ec.message()));
    }
    VLOG(1) << "Removed directory: " << target_dir
            << ", items removed: " << removed_count;
    return {};
}

tl::expected<void, std::string>
LocalFileSnapshotObjectStore::ListObjectsWithPrefix(
    const std::string& prefix, std::vector<std::string>& object_keys) {
    object_keys.clear();
    fs::path target_dir = KeyToPath(prefix);
    if (!IsPathWithinBase(target_dir)) {
        return tl::make_unexpected(
            fmt::format("Security error: Path {} is outside base directory {}",
                        target_dir.string(), base_path_.string()));
    }
    if (!fs::exists(target_dir)) {
        return {};
    }
    if (!fs::is_directory(target_dir)) {
        return tl::make_unexpected(fmt::format(
            "Target path '{}' is not a directory", target_dir.string()));
    }

    for (const auto& entry : fs::recursive_directory_iterator(target_dir)) {
        if (entry.is_regular_file()) {
            fs::path relative = entry.path().lexically_relative(base_path_);
            object_keys.push_back(relative.string());
        }
    }

    VLOG(1) << "Listed " << object_keys.size()
            << " objects with prefix: " << prefix;
    return {};
}

bool LocalFileSnapshotObjectStore::IsNotFoundError(
    const std::string& error) const {
    return error.starts_with("File not found:");
}

std::string LocalFileSnapshotObjectStore::GetConnectionInfo() const {
    return fmt::format("LocalFileSnapshotObjectStore: base_path={}",
                       base_path_.string());
}

}  // namespace mooncake

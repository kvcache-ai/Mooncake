#include "serialize/serializer_backend.h"

#include <cstdlib>
#include <filesystem>
#include <fstream>

#include <fmt/format.h>
#include <glog/logging.h>

#ifdef HAVE_AWS_SDK
#include "utils/s3_helper.h"
#endif

namespace fs = std::filesystem;

namespace mooncake {

// ============================================================================
// SerializerBackend factory method implementation
// ============================================================================

std::unique_ptr<SerializerBackend> SerializerBackend::Create(
    SnapshotBackendType type) {
    switch (type) {
#ifdef HAVE_AWS_SDK
        case SnapshotBackendType::S3:
            return std::make_unique<S3Backend>();
#else
        case SnapshotBackendType::S3:
            throw std::runtime_error(
                "S3 backend requested but AWS SDK not available. "
                "Please rebuild with HAVE_AWS_SDK or use 'local' backend.");
#endif
        case SnapshotBackendType::LOCAL_FILE:
            return std::make_unique<LocalFileBackend>();
        default:
            throw std::invalid_argument("Unknown snapshot backend type");
    }
}

// ============================================================================
// S3Backend implementation (compiled only when HAVE_AWS_SDK is defined)
// ============================================================================

#ifdef HAVE_AWS_SDK

class S3Backend::Impl {
   public:
    Impl() : initialized_(InitializeOnce()), s3_helper_("", "", "") {}

    ~Impl() { S3Helper::ShutdownAPI(); }

   private:
    static bool InitializeOnce() {
        S3Helper::InitAPI();
        return true;
    }
    bool initialized_;

   public:
    S3Helper s3_helper_;
};

S3Backend::S3Backend() : impl_(std::make_unique<Impl>()) {
    LOG(INFO) << "S3Backend initialized";
}

tl::expected<void, std::string> S3Backend::UploadBuffer(
    const std::string& key, const std::vector<uint8_t>& buffer) {
    return impl_->s3_helper_.UploadBufferMultipart(key, buffer);
}

tl::expected<void, std::string> S3Backend::DownloadBuffer(
    const std::string& key, std::vector<uint8_t>& buffer) {
    return impl_->s3_helper_.DownloadBufferMultipart(key, buffer);
}

tl::expected<void, std::string> S3Backend::UploadString(
    const std::string& key, const std::string& data) {
    return impl_->s3_helper_.UploadString(key, data);
}

tl::expected<void, std::string> S3Backend::DownloadString(
    const std::string& key, std::string& data) {
    return impl_->s3_helper_.DownloadString(key, data);
}

tl::expected<void, std::string> S3Backend::DeleteObjectsWithPrefix(
    const std::string& prefix) {
    return impl_->s3_helper_.DeleteObjectsWithPrefix(prefix);
}

tl::expected<void, std::string> S3Backend::ListObjectsWithPrefix(
    const std::string& prefix, std::vector<std::string>& object_keys) {
    return impl_->s3_helper_.ListObjectsWithPrefix(prefix, object_keys);
}

std::string S3Backend::GetConnectionInfo() const {
    return impl_->s3_helper_.GetConnectionInfo();
}

#endif  // HAVE_AWS_SDK

// ============================================================================
// LocalFileBackend implementation
// ============================================================================

namespace {
constexpr const char* kEnvLocalPath = "MOONCAKE_SNAPSHOT_LOCAL_PATH";
}  // namespace

LocalFileBackend::LocalFileBackend() {
    const char* env_path = std::getenv(kEnvLocalPath);
    if (!env_path || !*env_path) {
        throw std::runtime_error(
            "MOONCAKE_SNAPSHOT_LOCAL_PATH environment variable is not set. "
            "Please set it to a persistent directory path for snapshot "
            "storage. "
            "Example: export "
            "MOONCAKE_SNAPSHOT_LOCAL_PATH=/data/mooncake_snapshots");
    }

    // Create directory if not exists, then canonicalize
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

    // Verify base_path_ is a directory
    if (!fs::is_directory(base_path_)) {
        throw std::runtime_error(fmt::format(
            "base_path '{}' is not a directory", base_path_.string()));
    }

    LOG(INFO) << "LocalFileBackend initialized with path: " << base_path_;
}

LocalFileBackend::LocalFileBackend(const std::string& base_path) {
    if (base_path.empty()) {
        throw std::runtime_error(
            "LocalFileBackend base_path is empty. "
            "Please provide a valid persistent directory path for snapshot "
            "storage.");
    }

    // Create directory if not exists, then canonicalize
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

    // Verify base_path_ is a directory
    if (!fs::is_directory(base_path_)) {
        throw std::runtime_error(fmt::format(
            "base_path '{}' is not a directory", base_path_.string()));
    }

    LOG(INFO) << "LocalFileBackend initialized with path: " << base_path_;
}

fs::path LocalFileBackend::KeyToPath(const std::string& key) const {
    // key format: "mooncake_master_snapshot/20231201_123456_000/metadata"
    // converts to:
    // "/base_path/mooncake_master_snapshot/20231201_123456_000/metadata"
    return base_path_ / key;
}

tl::expected<void, std::string> LocalFileBackend::EnsureDirectoryExists(
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

bool LocalFileBackend::IsPathWithinBase(const fs::path& path) const {
    std::error_code ec;
    // Use weakly_canonical to handle paths that may not exist yet
    fs::path canonical_path = fs::weakly_canonical(path, ec);
    if (ec) return false;

    // Check if path starts with base_path_ using iterator comparison
    auto [base_end, path_it] =
        std::mismatch(base_path_.begin(), base_path_.end(),
                      canonical_path.begin(), canonical_path.end());
    return base_end == base_path_.end();
}

tl::expected<void, std::string> LocalFileBackend::UploadBuffer(
    const std::string& key, const std::vector<uint8_t>& buffer) {
    if (buffer.empty()) {
        return tl::make_unexpected("Error: Buffer is empty");
    }

    fs::path full_path = KeyToPath(key);

    // Security check: verify target path is within base_path_
    if (!IsPathWithinBase(full_path)) {
        return tl::make_unexpected(
            fmt::format("Security error: Path {} is outside base directory {}",
                        full_path.string(), base_path_.string()));
    }

    // Ensure parent directory exists
    auto dir_result = EnsureDirectoryExists(full_path.parent_path());
    if (!dir_result) {
        return dir_result;
    }

    // Write to file
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

tl::expected<void, std::string> LocalFileBackend::DownloadBuffer(
    const std::string& key, std::vector<uint8_t>& buffer) {
    fs::path full_path = KeyToPath(key);

    // Security check: verify target path is within base_path_
    if (!IsPathWithinBase(full_path)) {
        return tl::make_unexpected(
            fmt::format("Security error: Path {} is outside base directory {}",
                        full_path.string(), base_path_.string()));
    }

    // Check if file exists
    if (!fs::exists(full_path)) {
        return tl::make_unexpected(
            fmt::format("File not found: {}", full_path.string()));
    }

    // Get file size
    std::error_code ec;
    auto file_size = fs::file_size(full_path, ec);
    if (ec) {
        return tl::make_unexpected(
            fmt::format("Failed to get file size: {}, error: {}",
                        full_path.string(), ec.message()));
    }

    // Open file
    std::ifstream file(full_path, std::ios::binary);
    if (!file) {
        return tl::make_unexpected(fmt::format(
            "Failed to open file for reading: {}", full_path.string()));
    }

    // Read file content
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

tl::expected<void, std::string> LocalFileBackend::UploadString(
    const std::string& key, const std::string& data) {
    fs::path full_path = KeyToPath(key);

    // Security check: verify target path is within base_path_
    if (!IsPathWithinBase(full_path)) {
        return tl::make_unexpected(
            fmt::format("Security error: Path {} is outside base directory {}",
                        full_path.string(), base_path_.string()));
    }

    // Ensure parent directory exists
    auto dir_result = EnsureDirectoryExists(full_path.parent_path());
    if (!dir_result) {
        return dir_result;
    }

    // Write to file
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

tl::expected<void, std::string> LocalFileBackend::DownloadString(
    const std::string& key, std::string& data) {
    fs::path full_path = KeyToPath(key);

    // Security check: verify target path is within base_path_
    if (!IsPathWithinBase(full_path)) {
        return tl::make_unexpected(
            fmt::format("Security error: Path {} is outside base directory {}",
                        full_path.string(), base_path_.string()));
    }

    // Check if file exists
    if (!fs::exists(full_path)) {
        return tl::make_unexpected(
            fmt::format("File not found: {}", full_path.string()));
    }

    // Open file
    std::ifstream file(full_path);
    if (!file) {
        return tl::make_unexpected(fmt::format(
            "Failed to open file for reading: {}", full_path.string()));
    }

    // Read file content
    std::stringstream buffer;
    buffer << file.rdbuf();
    data = buffer.str();

    VLOG(1) << "Successfully downloaded string from: " << full_path;
    return {};
}

tl::expected<void, std::string> LocalFileBackend::DeleteObjectsWithPrefix(
    const std::string& prefix) {
    // In snapshot scenarios, the prefix is always a directory path
    // (e.g., "mooncake_master_snapshot/20240101_123456_000/").
    // This method deletes the entire directory corresponding to the prefix.
    fs::path target_dir = KeyToPath(prefix);

    // Security check: verify target path is within base_path_
    if (!IsPathWithinBase(target_dir)) {
        LOG(ERROR) << "Security violation: Attempted to delete path "
                      "outside base directory. "
                   << "base_path=" << base_path_
                   << ", target_path=" << target_dir;
        return tl::make_unexpected(
            fmt::format("Security error: Path {} is outside base directory {}",
                        target_dir.string(), base_path_.string()));
    }

    // Don't allow deleting base_path_ itself
    std::error_code ec;
    fs::path canonical_target = fs::weakly_canonical(target_dir, ec);
    if (!ec && canonical_target == base_path_) {
        LOG(ERROR) << "Security violation: Attempted to delete base "
                      "directory itself. "
                   << "base_path=" << base_path_;
        return tl::make_unexpected(
            fmt::format("Security error: Cannot delete base directory {}",
                        base_path_.string()));
    }

    // Directory doesn't exist, treat as successful deletion
    if (!fs::exists(target_dir)) {
        return {};
    }

    // Verify target is a directory (per design constraint)
    if (!fs::is_directory(target_dir)) {
        return tl::make_unexpected(fmt::format(
            "Target path '{}' is not a directory", target_dir.string()));
    }

    // Delete the entire directory
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

tl::expected<void, std::string> LocalFileBackend::ListObjectsWithPrefix(
    const std::string& prefix, std::vector<std::string>& object_keys) {
    // In snapshot scenarios, the prefix is always a directory path
    // (e.g., "mooncake_master_snapshot/" or
    // "mooncake_master_snapshot/20240101_123456_000/"). This method lists all
    // files recursively under that directory.
    object_keys.clear();
    fs::path target_dir = KeyToPath(prefix);

    // Security check: verify target path is within base_path_
    if (!IsPathWithinBase(target_dir)) {
        return tl::make_unexpected(
            fmt::format("Security error: Path {} is outside base directory {}",
                        target_dir.string(), base_path_.string()));
    }

    // Directory doesn't exist, return empty list
    if (!fs::exists(target_dir)) {
        return {};
    }

    // Verify target is a directory (per design constraint)
    if (!fs::is_directory(target_dir)) {
        return tl::make_unexpected(fmt::format(
            "Target path '{}' is not a directory", target_dir.string()));
    }

    // Recursively traverse all files in the directory
    for (const auto& entry : fs::recursive_directory_iterator(target_dir)) {
        if (entry.is_regular_file()) {
            // Use path API to compute relative path
            fs::path relative = entry.path().lexically_relative(base_path_);
            object_keys.push_back(relative.string());
        }
    }

    VLOG(1) << "Listed " << object_keys.size()
            << " objects with prefix: " << prefix;
    return {};
}

std::string LocalFileBackend::GetConnectionInfo() const {
    return fmt::format("LocalFileBackend: base_path={}", base_path_.string());
}

}  // namespace mooncake

#include "serialize/serializer_backend.h"

#include <cstdlib>
#include <filesystem>
#include <fstream>

#include <fmt/format.h>
#include <glog/logging.h>

#ifdef HAVE_AWS_SDK
#include "utils/s3_helper.h"
#endif

#ifdef STORE_USE_ETCD
#include "etcd_helper.h"
#include "libetcd_wrapper.h"
#include "types.h"
#endif

namespace fs = std::filesystem;

namespace mooncake {

// ============================================================================
// SerializerBackend factory method implementation
// ============================================================================

std::unique_ptr<SerializerBackend> SerializerBackend::Create(
    SnapshotBackendType type, const std::string& etcd_endpoints) {
    (void)etcd_endpoints;
    switch (type) {
#ifdef STORE_USE_ETCD
        case SnapshotBackendType::ETCD:
            return std::make_unique<EtcdBackend>(etcd_endpoints);
#else
        case SnapshotBackendType::ETCD:
            LOG(WARNING)
                << "ETCD backend requested but STORE_USE_ETCD not enabled, "
                << "falling back to LocalFileBackend";
            return std::make_unique<LocalFileBackend>();
#endif
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
// EtcdBackend implementation (compiled only when STORE_USE_ETCD is defined)
// ============================================================================

#ifdef STORE_USE_ETCD

namespace {
constexpr size_t kEtcdMaxValueSize = 2000UL * 1000UL * 1000UL;
}

EtcdBackend::EtcdBackend(const std::string& endpoints, bool force_reconnect)
    : endpoints_(endpoints) {
    if (endpoints_.empty()) {
        LOG(WARNING) << "EtcdBackend initialized with empty endpoints";
    } else {
        LOG(INFO) << "EtcdBackend initialized with endpoints: " << endpoints_;
        char* err_msg = nullptr;
        int ret = NewSnapshotEtcdClient((char*)endpoints_.c_str(), &err_msg);
        // ret == -2 means the etcd snapshot client has already been initialized
        if (ret != 0 && ret != -2) {
            LOG(ERROR)
                << "EtcdBackend failed to initialize etcd snapshot client: "
                << (err_msg ? err_msg : "unknown error");
            if (err_msg) free(err_msg);
        } else {
            LOG(INFO)
                << "EtcdBackend successfully initialized etcd snapshot client";
        }
    }
}

tl::expected<void, std::string> EtcdBackend::UploadBuffer(
    const std::string& key, const std::vector<uint8_t>& buffer) {
    LOG(INFO) << "DEBUG EtcdBackend::UploadBuffer: key=[" << key
              << "] buffer_size=" << buffer.size();
    if (buffer.empty()) {
        return tl::make_unexpected("Error: Buffer is empty");
    }
    if (buffer.size() > kEtcdMaxValueSize) {
        return tl::make_unexpected(
            fmt::format("Error: Buffer size {} exceeds max value size {}",
                        buffer.size(), kEtcdMaxValueSize));
    }
    LOG(INFO) << "DEBUG: Calling SnapshotStorePutWrapper...";
    char* err_msg = nullptr;
    int ret = SnapshotStorePutWrapper(
        const_cast<char*>(key.data()), static_cast<int>(key.size()),
        const_cast<char*>(reinterpret_cast<const char*>(buffer.data())),
        static_cast<int>(buffer.size()), &err_msg);
    LOG(INFO) << "DEBUG: SnapshotStorePutWrapper returned ret=" << ret;
    if (ret != 0) {
        std::string error =
            err_msg ? err_msg : "SnapshotStorePutWrapper failed";
        if (err_msg) {
            free(err_msg);
        }
        return tl::make_unexpected(error);
    }
    return {};
}

tl::expected<void, std::string> EtcdBackend::DownloadBuffer(
    const std::string& key, std::vector<uint8_t>& buffer) {
    char* err_msg = nullptr;
    char* value_ptr = nullptr;
    int value_size = 0;
    long long revision_id = 0;  // GoInt64 is long long
    int ret = SnapshotStoreGetWrapper(const_cast<char*>(key.data()),
                                      static_cast<int>(key.size()), &value_ptr,
                                      &value_size, &revision_id, &err_msg);
    if (ret != 0) {
        std::string error =
            err_msg ? err_msg : "SnapshotStoreGetWrapper failed";
        if (err_msg) {
            free(err_msg);
        }
        return tl::make_unexpected(error);
    }
    buffer.clear();
    if (value_size > 0 && value_ptr == nullptr) {
        return tl::make_unexpected(
            "SnapshotStoreGetWrapper returned null value");
    }
    if (value_size > 0) {
        buffer.assign(value_ptr, value_ptr + value_size);
    }
    if (value_ptr) {
        free(value_ptr);
    }
    return {};
}

tl::expected<void, std::string> EtcdBackend::UploadString(
    const std::string& key, const std::string& data) {
    if (data.empty()) {
        return tl::make_unexpected("Error: String is empty");
    }
    if (data.size() > kEtcdMaxValueSize) {
        return tl::make_unexpected(
            fmt::format("Error: String size {} exceeds max value size {}",
                        data.size(), kEtcdMaxValueSize));
    }
    char* err_msg = nullptr;
    int ret = SnapshotStorePutWrapper(const_cast<char*>(key.data()),
                                      static_cast<int>(key.size()),
                                      const_cast<char*>(data.data()),
                                      static_cast<int>(data.size()), &err_msg);
    if (ret != 0) {
        std::string error =
            err_msg ? err_msg : "SnapshotStorePutWrapper failed";
        if (err_msg) {
            free(err_msg);
        }
        return tl::make_unexpected(error);
    }
    return {};
}

tl::expected<void, std::string> EtcdBackend::DownloadString(
    const std::string& key, std::string& data) {
    std::vector<uint8_t> buffer;
    auto result = DownloadBuffer(key, buffer);
    if (!result) {
        return result;
    }
    data.assign(reinterpret_cast<const char*>(buffer.data()), buffer.size());
    return {};
}

tl::expected<void, std::string> EtcdBackend::DeleteObjectsWithPrefix(
    const std::string& prefix) {
    char* err_msg = nullptr;
    // Use SnapshotStoreDeleteWrapper to match the client used for Put/Get
    int ret =
        SnapshotStoreDeleteWrapper(const_cast<char*>(prefix.data()),
                                   static_cast<int>(prefix.size()), &err_msg);
    if (ret != 0) {
        std::string error =
            err_msg ? err_msg : "SnapshotStoreDeleteWrapper failed";
        if (err_msg) {
            free(err_msg);
        }
        return tl::make_unexpected(error);
    }
    return {};
}

tl::expected<void, std::string> EtcdBackend::ListObjectsWithPrefix(
    const std::string& prefix, std::vector<std::string>& object_keys) {
    (void)prefix;
    object_keys.clear();
    return tl::make_unexpected(
        "EtcdBackend ListObjectsWithPrefix is not supported");
}

std::string EtcdBackend::GetConnectionInfo() const {
    if (endpoints_.empty()) {
        return "EtcdBackend: endpoints=empty";
    }
    return fmt::format("EtcdBackend: endpoints={}", endpoints_);
}

#endif  // STORE_USE_ETCD

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

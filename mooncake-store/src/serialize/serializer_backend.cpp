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
        default:
            return std::make_unique<LocalFileBackend>();
    }
}

// ============================================================================
// S3Backend implementation (compiled only when HAVE_AWS_SDK is defined)
// ============================================================================

#ifdef HAVE_AWS_SDK

class S3Backend::Impl {
   public:
    Impl() : s3_helper_("", "", "") {}

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
    // usePrefix=1 enables etcd's WithPrefix() option in the Go wrapper
    int ret = SnapshotStoreDeleteWrapper(
        const_cast<char*>(prefix.data()), static_cast<int>(prefix.size()),
        /*usePrefix=*/1, &err_msg);
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
        "EtcdBackend ListObjectsWithPrefix is not implemented");
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
constexpr const char* kDefaultLocalPath = "/tmp/mooncake_snapshots";
constexpr const char* kEnvLocalPath = "SNAPSHOT_LOCAL_PATH";
}  // namespace

LocalFileBackend::LocalFileBackend() {
    const char* env_path = std::getenv(kEnvLocalPath);
    if (env_path && *env_path) {
        base_path_ = env_path;
    } else {
        base_path_ = kDefaultLocalPath;
    }
    LOG(INFO) << "LocalFileBackend initialized with path: " << base_path_;
}

LocalFileBackend::LocalFileBackend(const std::string& base_path)
    : base_path_(base_path) {
    if (base_path_.empty()) {
        base_path_ = kDefaultLocalPath;
    }
    LOG(INFO) << "LocalFileBackend initialized with path: " << base_path_;
}

std::string LocalFileBackend::KeyToPath(const std::string& key) const {
    // key format: "master_snapshot/20231201_123456_000/metadata"
    // converts to: "/base_path/master_snapshot/20231201_123456_000/metadata"
    return base_path_ + "/" + key;
}

tl::expected<void, std::string> LocalFileBackend::EnsureDirectoryExists(
    const std::string& dir_path) const {
    try {
        if (!fs::exists(dir_path)) {
            if (!fs::create_directories(dir_path)) {
                return tl::make_unexpected(
                    fmt::format("Failed to create directory: {}", dir_path));
            }
        }
        return {};
    } catch (const fs::filesystem_error& e) {
        return tl::make_unexpected(fmt::format(
            "Filesystem error creating directory {}: {}", dir_path, e.what()));
    }
}

tl::expected<void, std::string> LocalFileBackend::UploadBuffer(
    const std::string& key, const std::vector<uint8_t>& buffer) {
    if (buffer.empty()) {
        return tl::make_unexpected("Error: Buffer is empty");
    }

    std::string full_path = KeyToPath(key);
    fs::path file_path(full_path);

    // Ensure parent directory exists
    auto dir_result = EnsureDirectoryExists(file_path.parent_path().string());
    if (!dir_result) {
        return dir_result;
    }

    // Write to file
    std::ofstream file(full_path, std::ios::binary | std::ios::trunc);
    if (!file) {
        return tl::make_unexpected(
            fmt::format("Failed to open file for writing: {}", full_path));
    }

    file.write(reinterpret_cast<const char*>(buffer.data()),
               static_cast<std::streamsize>(buffer.size()));

    if (!file) {
        return tl::make_unexpected(
            fmt::format("Failed to write data to file: {}", full_path));
    }

    file.close();
    VLOG(1) << "Successfully uploaded buffer to: " << full_path
            << ", size: " << buffer.size();
    return {};
}

tl::expected<void, std::string> LocalFileBackend::DownloadBuffer(
    const std::string& key, std::vector<uint8_t>& buffer) {
    std::string full_path = KeyToPath(key);

    // Check if file exists
    if (!fs::exists(full_path)) {
        return tl::make_unexpected(
            fmt::format("File not found: {}", full_path));
    }

    // Get file size
    std::error_code ec;
    auto file_size = fs::file_size(full_path, ec);
    if (ec) {
        return tl::make_unexpected(fmt::format(
            "Failed to get file size: {}, error: {}", full_path, ec.message()));
    }

    // Open file
    std::ifstream file(full_path, std::ios::binary);
    if (!file) {
        return tl::make_unexpected(
            fmt::format("Failed to open file for reading: {}", full_path));
    }

    // Read file content
    buffer.resize(file_size);
    file.read(reinterpret_cast<char*>(buffer.data()),
              static_cast<std::streamsize>(file_size));

    if (!file) {
        return tl::make_unexpected(
            fmt::format("Failed to read data from file: {}", full_path));
    }

    VLOG(1) << "Successfully downloaded buffer from: " << full_path
            << ", size: " << buffer.size();
    return {};
}

tl::expected<void, std::string> LocalFileBackend::UploadString(
    const std::string& key, const std::string& data) {
    std::string full_path = KeyToPath(key);
    fs::path file_path(full_path);

    // Ensure parent directory exists
    auto dir_result = EnsureDirectoryExists(file_path.parent_path().string());
    if (!dir_result) {
        return dir_result;
    }

    // Write to file
    std::ofstream file(full_path, std::ios::trunc);
    if (!file) {
        return tl::make_unexpected(
            fmt::format("Failed to open file for writing: {}", full_path));
    }

    file << data;
    if (!file) {
        return tl::make_unexpected(
            fmt::format("Failed to write string to file: {}", full_path));
    }

    file.close();
    VLOG(1) << "Successfully uploaded string to: " << full_path;
    return {};
}

tl::expected<void, std::string> LocalFileBackend::DownloadString(
    const std::string& key, std::string& data) {
    std::string full_path = KeyToPath(key);

    // Check if file exists
    if (!fs::exists(full_path)) {
        return tl::make_unexpected(
            fmt::format("File not found: {}", full_path));
    }

    // Open file
    std::ifstream file(full_path);
    if (!file) {
        return tl::make_unexpected(
            fmt::format("Failed to open file for reading: {}", full_path));
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
    std::string full_path = KeyToPath(prefix);

    try {
        // Security check: ensure path to delete is within base_path_
        std::error_code ec;
        fs::path canonical_base = fs::canonical(base_path_, ec);
        if (ec) {
            // base_path_ doesn't exist, try to create it and get canonical path
            fs::create_directories(base_path_);
            canonical_base = fs::canonical(base_path_, ec);
            if (ec) {
                return tl::make_unexpected(
                    fmt::format("Failed to resolve base path {}: {}",
                                base_path_, ec.message()));
            }
        }

        // Check if target path exists
        if (!fs::exists(full_path)) {
            // Path doesn't exist, treat as successful deletion
            return {};
        }

        fs::path canonical_target = fs::canonical(full_path, ec);
        if (ec) {
            return tl::make_unexpected(
                fmt::format("Failed to resolve target path {}: {}", full_path,
                            ec.message()));
        }

        // Verify target path is within base_path_
        std::string base_str = canonical_base.string();
        std::string target_str = canonical_target.string();

        // Ensure target path starts with base_path_
        if (target_str.length() < base_str.length() ||
            target_str.substr(0, base_str.length()) != base_str) {
            LOG(ERROR) << "Security violation: Attempted to delete path "
                          "outside base directory. "
                       << "base_path=" << base_str
                       << ", target_path=" << target_str;
            return tl::make_unexpected(fmt::format(
                "Security error: Path {} is outside base directory {}",
                full_path, base_path_));
        }

        // Don't allow deleting base_path_ itself
        if (target_str == base_str) {
            LOG(ERROR) << "Security violation: Attempted to delete base "
                          "directory itself. "
                       << "base_path=" << base_str;
            return tl::make_unexpected(fmt::format(
                "Security error: Cannot delete base directory {}", base_path_));
        }

        // Security check passed, execute delete operation
        if (fs::is_directory(full_path)) {
            auto removed_count = fs::remove_all(full_path, ec);
            if (ec) {
                return tl::make_unexpected(
                    fmt::format("Failed to remove directory {}: {}", full_path,
                                ec.message()));
            }
            VLOG(1) << "Removed directory: " << full_path
                    << ", items removed: " << removed_count;
        } else {
            // If it's a file, delete all files matching the prefix
            fs::path parent_dir = fs::path(full_path).parent_path();
            std::string prefix_name = fs::path(full_path).filename().string();

            // Verify parent directory is also within base_path_
            fs::path canonical_parent = fs::canonical(parent_dir, ec);
            if (ec || canonical_parent.string().substr(0, base_str.length()) !=
                          base_str) {
                return tl::make_unexpected(fmt::format(
                    "Security error: Parent path {} is outside base directory",
                    parent_dir.string()));
            }

            if (fs::exists(parent_dir) && fs::is_directory(parent_dir)) {
                for (const auto& entry : fs::directory_iterator(parent_dir)) {
                    std::string filename = entry.path().filename().string();
                    if (filename.find(prefix_name) == 0) {
                        fs::remove_all(entry.path(), ec);
                        if (ec) {
                            LOG(WARNING) << "Failed to remove: " << entry.path()
                                         << ", error: " << ec.message();
                        }
                    }
                }
            }
        }

        return {};
    } catch (const fs::filesystem_error& e) {
        return tl::make_unexpected(
            fmt::format("Filesystem error during delete: {}", e.what()));
    }
}

tl::expected<void, std::string> LocalFileBackend::ListObjectsWithPrefix(
    const std::string& prefix, std::vector<std::string>& object_keys) {
    object_keys.clear();
    std::string full_path = KeyToPath(prefix);

    try {
        // Get the parent directory of the prefix
        fs::path prefix_path(full_path);
        fs::path parent_dir = prefix_path.parent_path();

        if (!fs::exists(parent_dir)) {
            // Directory doesn't exist, return empty list
            return {};
        }

        std::string prefix_name = prefix_path.filename().string();

        // Recursively traverse directory
        std::function<void(const fs::path&)> traverse =
            [&](const fs::path& dir) {
                if (!fs::exists(dir) || !fs::is_directory(dir)) {
                    return;
                }

                for (const auto& entry : fs::directory_iterator(dir)) {
                    if (entry.is_directory()) {
                        // Check if directory name matches prefix
                        std::string relative_path =
                            entry.path().string().substr(base_path_.length() +
                                                         1);
                        if (relative_path.find(prefix) == 0 ||
                            prefix.find(relative_path) == 0) {
                            traverse(entry.path());
                        }
                    } else if (entry.is_regular_file()) {
                        // Convert file path to key relative to base_path_
                        std::string relative_path =
                            entry.path().string().substr(base_path_.length() +
                                                         1);
                        if (relative_path.find(prefix) == 0) {
                            object_keys.push_back(relative_path);
                        }
                    }
                }
            };

        // If prefix itself is a directory, traverse directly
        if (fs::exists(full_path) && fs::is_directory(full_path)) {
            traverse(full_path);
        } else {
            // Otherwise traverse from parent directory
            traverse(parent_dir);
        }

        VLOG(1) << "Listed " << object_keys.size()
                << " objects with prefix: " << prefix;
        return {};
    } catch (const fs::filesystem_error& e) {
        return tl::make_unexpected(
            fmt::format("Filesystem error during list: {}", e.what()));
    }
}

std::string LocalFileBackend::GetConnectionInfo() const {
    return fmt::format("LocalFileBackend: base_path={}", base_path_);
}

}  // namespace mooncake

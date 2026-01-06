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
// SerializerBackend 工厂方法实现
// ============================================================================

std::unique_ptr<SerializerBackend> SerializerBackend::Create(SnapshotBackendType type) {
    switch (type) {
#ifdef HAVE_AWS_SDK
        case SnapshotBackendType::S3:
            return std::make_unique<S3Backend>();
#else
        case SnapshotBackendType::S3:
            LOG(WARNING) << "S3 backend requested but AWS SDK not available, "
                         << "falling back to LocalFileBackend";
            return std::make_unique<LocalFileBackend>();
#endif
        case SnapshotBackendType::LOCAL_FILE:
        default:
            return std::make_unique<LocalFileBackend>();
    }
}

// ============================================================================
// S3Backend 实现 (仅在 HAVE_AWS_SDK 定义时编译)
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
// LocalFileBackend 实现
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
    // key 格式: "master_snapshot/20231201_123456_000/metadata"
    // 转换为: "/base_path/master_snapshot/20231201_123456_000/metadata"
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
        return tl::make_unexpected(
            fmt::format("Filesystem error creating directory {}: {}", dir_path, e.what()));
    }
}

tl::expected<void, std::string> LocalFileBackend::UploadBuffer(
    const std::string& key, const std::vector<uint8_t>& buffer) {
    if (buffer.empty()) {
        return tl::make_unexpected("Error: Buffer is empty");
    }

    std::string full_path = KeyToPath(key);
    fs::path file_path(full_path);

    // 确保父目录存在
    auto dir_result = EnsureDirectoryExists(file_path.parent_path().string());
    if (!dir_result) {
        return dir_result;
    }

    // 写入文件
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

    // 检查文件是否存在
    if (!fs::exists(full_path)) {
        return tl::make_unexpected(
            fmt::format("File not found: {}", full_path));
    }

    // 获取文件大小
    std::error_code ec;
    auto file_size = fs::file_size(full_path, ec);
    if (ec) {
        return tl::make_unexpected(
            fmt::format("Failed to get file size: {}, error: {}", full_path, ec.message()));
    }

    // 打开文件
    std::ifstream file(full_path, std::ios::binary);
    if (!file) {
        return tl::make_unexpected(
            fmt::format("Failed to open file for reading: {}", full_path));
    }

    // 读取文件内容
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

    // 确保父目录存在
    auto dir_result = EnsureDirectoryExists(file_path.parent_path().string());
    if (!dir_result) {
        return dir_result;
    }

    // 写入文件
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

    // 检查文件是否存在
    if (!fs::exists(full_path)) {
        return tl::make_unexpected(
            fmt::format("File not found: {}", full_path));
    }

    // 打开文件
    std::ifstream file(full_path);
    if (!file) {
        return tl::make_unexpected(
            fmt::format("Failed to open file for reading: {}", full_path));
    }

    // 读取文件内容
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
        // 安全检查：确保要删除的路径在 base_path_ 内
        std::error_code ec;
        fs::path canonical_base = fs::canonical(base_path_, ec);
        if (ec) {
            // base_path_ 不存在，尝试创建并获取规范化路径
            fs::create_directories(base_path_);
            canonical_base = fs::canonical(base_path_, ec);
            if (ec) {
                return tl::make_unexpected(
                    fmt::format("Failed to resolve base path {}: {}", base_path_, ec.message()));
            }
        }

        // 检查目标路径是否存在
        if (!fs::exists(full_path)) {
            // 路径不存在，视为删除成功
            return {};
        }

        fs::path canonical_target = fs::canonical(full_path, ec);
        if (ec) {
            return tl::make_unexpected(
                fmt::format("Failed to resolve target path {}: {}", full_path, ec.message()));
        }

        // 验证目标路径是否在 base_path_ 内
        std::string base_str = canonical_base.string();
        std::string target_str = canonical_target.string();

        // 确保目标路径以 base_path_ 开头
        if (target_str.length() < base_str.length() ||
            target_str.substr(0, base_str.length()) != base_str) {
            LOG(ERROR) << "Security violation: Attempted to delete path outside base directory. "
                       << "base_path=" << base_str << ", target_path=" << target_str;
            return tl::make_unexpected(
                fmt::format("Security error: Path {} is outside base directory {}",
                            full_path, base_path_));
        }

        // 不允许删除 base_path_ 本身
        if (target_str == base_str) {
            LOG(ERROR) << "Security violation: Attempted to delete base directory itself. "
                       << "base_path=" << base_str;
            return tl::make_unexpected(
                fmt::format("Security error: Cannot delete base directory {}", base_path_));
        }

        // 安全检查通过，执行删除操作
        if (fs::is_directory(full_path)) {
            auto removed_count = fs::remove_all(full_path, ec);
            if (ec) {
                return tl::make_unexpected(
                    fmt::format("Failed to remove directory {}: {}", full_path, ec.message()));
            }
            VLOG(1) << "Removed directory: " << full_path
                    << ", items removed: " << removed_count;
        } else {
            // 如果是文件，删除匹配前缀的所有文件
            fs::path parent_dir = fs::path(full_path).parent_path();
            std::string prefix_name = fs::path(full_path).filename().string();

            // 验证父目录也在 base_path_ 内
            fs::path canonical_parent = fs::canonical(parent_dir, ec);
            if (ec || canonical_parent.string().substr(0, base_str.length()) != base_str) {
                return tl::make_unexpected(
                    fmt::format("Security error: Parent path {} is outside base directory",
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
        // 获取前缀的父目录
        fs::path prefix_path(full_path);
        fs::path parent_dir = prefix_path.parent_path();

        if (!fs::exists(parent_dir)) {
            // 目录不存在，返回空列表
            return {};
        }

        std::string prefix_name = prefix_path.filename().string();

        // 递归遍历目录
        std::function<void(const fs::path&)> traverse = [&](const fs::path& dir) {
            if (!fs::exists(dir) || !fs::is_directory(dir)) {
                return;
            }

            for (const auto& entry : fs::directory_iterator(dir)) {
                if (entry.is_directory()) {
                    // 检查目录名是否匹配前缀
                    std::string relative_path = entry.path().string().substr(base_path_.length() + 1);
                    if (relative_path.find(prefix) == 0 || prefix.find(relative_path) == 0) {
                        traverse(entry.path());
                    }
                } else if (entry.is_regular_file()) {
                    // 将文件路径转换为相对于 base_path_ 的键
                    std::string relative_path = entry.path().string().substr(base_path_.length() + 1);
                    if (relative_path.find(prefix) == 0) {
                        object_keys.push_back(relative_path);
                    }
                }
            }
        };

        // 如果前缀本身是一个目录，直接遍历
        if (fs::exists(full_path) && fs::is_directory(full_path)) {
            traverse(full_path);
        } else {
            // 否则从父目录开始遍历
            traverse(parent_dir);
        }

        VLOG(1) << "Listed " << object_keys.size() << " objects with prefix: " << prefix;
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

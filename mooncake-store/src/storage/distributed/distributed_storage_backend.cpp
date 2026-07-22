#include "storage/distributed/distributed_storage_backend.h"

#include <fmt/format.h>

#include <algorithm>
#include <cstdlib>
#include <xxhash.h>
#include <filesystem>

#include "utils.h"

namespace mooncake {

// === DistributedStorageConfig ===

bool DistributedStorageConfig::Validate() const {
    if (fsdir.empty()) {
        LOG(ERROR) << "DistributedStorageConfig: fsdir is empty";
        return false;
    }
    if (!std::filesystem::path(fsdir).is_absolute()) {
        LOG(ERROR)
            << "DistributedStorageConfig: fsdir must be an absolute path: "
            << fsdir;
        return false;
    }
    if (fs_adapter_type.empty()) {
        LOG(ERROR) << "DistributedStorageConfig: fs_adapter_type is empty";
        return false;
    }
    if (fs_adapter_type != "hf3fs") {
        LOG(ERROR) << "DistributedStorageConfig: unsupported fs_adapter_type: "
                   << fs_adapter_type;
        return false;
    }
    if (hash_bucket_count <= 0) {
        LOG(ERROR) << "DistributedStorageConfig: hash_bucket_count must > 0";
        return false;
    }
    return true;
}

DistributedStorageConfig DistributedStorageConfig::FromEnvironment() {
    DistributedStorageConfig config;
    config.fsdir =
        GetEnvStringOr("MOONCAKE_DISTRIBUTED_ROOT_DIR", config.fsdir);
    if (!std::filesystem::path(config.fsdir).is_absolute()) {
        config.fsdir = std::filesystem::absolute(config.fsdir).string();
    }
    config.fs_adapter_type =
        GetEnvStringOr("MOONCAKE_DISTRIBUTED_FS_TYPE", config.fs_adapter_type);
    config.enable_health_check =
        GetEnvOr<bool>("MOONCAKE_DISTRIBUTED_HEALTH_CHECK", false);
    config.hash_bucket_count =
        GetEnvOr<int>("MOONCAKE_DISTRIBUTED_HASH_BUCKET_COUNT", 256);
    return config;
}

// === DistributedStorageBackend ===

DistributedStorageBackend::DistributedStorageBackend(
    const FileStorageConfig& file_storage_config,
    const DistributedStorageConfig& distributed_config,
    std::unique_ptr<FileSystemAdapter> fs_adapter)
    : DistributedStorageBackend(file_storage_config, distributed_config,
                                std::move(fs_adapter), nullptr) {}

DistributedStorageBackend::DistributedStorageBackend(
    const FileStorageConfig& file_storage_config,
    const DistributedStorageConfig& distributed_config,
    std::unique_ptr<FileSystemAdapter> fs_adapter,
    std::unique_ptr<ObjectStorageAdapter> object_storage_adapter)
    : StorageBackendInterface(file_storage_config),
      fs_adapter_(std::move(fs_adapter)),
      object_storage_adapter_(std::move(object_storage_adapter)),
      distributed_config_(distributed_config),
      root_dir_(distributed_config.fsdir),
      hash_bucket_count_(distributed_config.hash_bucket_count) {
    CHECK(fs_adapter_ || object_storage_adapter_)
        << "DistributedStorageBackend: at least one I/O adapter is required";
    use_object_storage_ = object_storage_adapter_ != nullptr;
}

tl::expected<void, ErrorCode> DistributedStorageBackend::Init() {
    if (initialized_) {
        LOG(WARNING) << "DistributedStorageBackend is already initialized";
        return {};
    }

    if (use_object_storage_) {
        auto init_result = object_storage_adapter_->Init();
        if (!init_result) return init_result;
        initialized_ = true;
        LOG(INFO) << "DistributedStorageBackend initialized, object adapter="
                  << object_storage_adapter_->GetName();
        return {};
    }

    auto init_result = fs_adapter_->Init(root_dir_);
    if (!init_result) return init_result;

    // Ensure root directory exists before health check
    std::error_code ec;
    std::filesystem::create_directories(root_dir_, ec);
    if (ec) {
        LOG(ERROR) << "Failed to create root directory " << root_dir_ << ": "
                   << ec.message();
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }

    if (distributed_config_.enable_health_check) {
        std::string probe_path =
            fmt::format("{}/.mooncake_health_probe_{}", root_dir_,
                        UuidToString(generate_uuid()));
        std::string probe_data = "health_check";
        auto write_result = fs_adapter_->WriteFile(
            probe_path,
            std::span<const char>(probe_data.data(), probe_data.size()));
        if (!write_result) {
            LOG(ERROR) << "DFS health check failed (write): "
                       << static_cast<int>(write_result.error());
            return tl::make_unexpected(write_result.error());
        }

        std::vector<char> read_buf(probe_data.size());
        auto read_result =
            fs_adapter_->ReadFile(probe_path, read_buf.data(), read_buf.size());
        if (!read_result || *read_result != probe_data.size() ||
            std::string(read_buf.data(), read_buf.size()) != probe_data) {
            LOG(ERROR) << "DFS health check failed (read back mismatch)";
            auto del_err = fs_adapter_->DeleteFile(probe_path);
            if (!del_err) {
                LOG(WARNING) << "Failed to delete health-check probe: "
                             << static_cast<int>(del_err.error());
            }
            return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
        }

        fs_adapter_->DeleteFile(probe_path);
        LOG(INFO) << "DFS health check passed, adapter="
                  << fs_adapter_->GetName();
    }

    // Ensure hash bucket directories exist
    for (int i = 0; i < hash_bucket_count_; ++i) {
        std::string bucket_dir = fmt::format("{}/{:02x}", root_dir_, i);
        std::filesystem::create_directories(bucket_dir, ec);
        if (ec) {
            LOG(ERROR) << "Failed to create bucket directory " << bucket_dir
                       << ": " << ec.message();
            return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        }
    }

    initialized_ = true;
    return {};
}

tl::expected<int64_t, ErrorCode> DistributedStorageBackend::BatchOffload(
    const std::unordered_map<std::string, std::vector<Slice>>& batch_object,
    std::function<ErrorCode(const std::vector<std::string>& keys,
                            std::vector<StorageObjectMetadata>& metadatas)>
        complete_handler,
    EvictionHandler eviction_handler) {
    if (!initialized_) {
        LOG(ERROR) << "DistributedStorageBackend is not initialized";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    if (eviction_handler) {
        LOG_FIRST_N(WARNING, 1)
            << "DistributedStorageBackend does not support eviction, "
               "eviction_handler ignored";
    }

    std::vector<std::string> success_keys;
    std::vector<StorageObjectMetadata> success_metas;

    for (const auto& [key, slices] : batch_object) {
        std::vector<iovec> iovs;
        iovs.reserve(slices.size());
        size_t total_size = 0;
        for (const auto& slice : slices) {
            iovs.push_back({slice.ptr, slice.size});
            total_size += slice.size;
        }

        size_t written = 0;
        if (use_object_storage_) {
            auto result = object_storage_adapter_->PutV(
                key, iovs.data(), static_cast<int>(iovs.size()));
            if (!result) {
                LOG(WARNING) << "Failed to offload key " << key << ": "
                             << static_cast<int>(result.error());
                continue;
            }
            written = total_size;
        } else {
            auto result =
                fs_adapter_->VectorWriteFile(GetObjectPath(key), iovs.data(),
                                             static_cast<int>(iovs.size()), 0);
            if (!result) {
                LOG(WARNING) << "Failed to offload key " << key << ": "
                             << static_cast<int>(result.error());
                continue;
            }
            written = *result;
        }

        success_keys.push_back(key);
        StorageObjectMetadata meta{-1, 0, static_cast<int64_t>(key.size()),
                                   static_cast<int64_t>(written), ""};
        success_metas.push_back(meta);
    }

    if (!success_keys.empty()) {
        auto err = complete_handler(success_keys, success_metas);
        if (err != ErrorCode::OK) {
            return tl::make_unexpected(err);
        }
    }

    return static_cast<int64_t>(success_keys.size());
}

tl::expected<void, ErrorCode> DistributedStorageBackend::BatchLoad(
    std::unordered_map<std::string, Slice>& batched_slices) {
    if (!initialized_) {
        LOG(ERROR) << "DistributedStorageBackend is not initialized";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    for (auto& [key, slice] : batched_slices) {
        auto result =
            use_object_storage_
                ? object_storage_adapter_->Get(key, slice.ptr, slice.size)
                : fs_adapter_->ReadFile(GetObjectPath(key), slice.ptr,
                                        slice.size);
        if (!result) {
            return tl::make_unexpected(result.error());
        }
        if (*result != slice.size) {
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
    }
    return {};
}

tl::expected<bool, ErrorCode> DistributedStorageBackend::IsExist(
    const std::string& key) {
    if (!initialized_) {
        LOG(ERROR) << "DistributedStorageBackend is not initialized";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    if (use_object_storage_) {
        return object_storage_adapter_->Exists(key);
    }
    return fs_adapter_->FileExists(GetObjectPath(key));
}

tl::expected<bool, ErrorCode> DistributedStorageBackend::IsEnableOffloading() {
    return true;
}

tl::expected<void, ErrorCode> DistributedStorageBackend::ScanMeta(
    const std::function<
        ErrorCode(const std::vector<std::string>& keys,
                  std::vector<StorageObjectMetadata>& metadatas)>& handler) {
    if (!initialized_) {
        LOG(ERROR) << "DistributedStorageBackend is not initialized";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    std::vector<std::string> batch_keys;
    std::vector<StorageObjectMetadata> batch_metas;
    const size_t batch_limit = static_cast<size_t>(std::max<int64_t>(
        1, file_storage_config_.scanmeta_iterator_keys_limit));

    if (use_object_storage_) {
        auto key_infos = object_storage_adapter_->ListKeys();
        if (!key_infos) {
            LOG(ERROR) << "Failed to list keys from object storage adapter: "
                       << static_cast<int>(key_infos.error());
            return tl::make_unexpected(key_infos.error());
        }

        for (const auto& info : *key_infos) {
            batch_keys.push_back(info.key);
            StorageObjectMetadata meta{-1, 0,
                                       static_cast<int64_t>(info.key.size()),
                                       static_cast<int64_t>(info.size), ""};
            batch_metas.push_back(meta);

            if (batch_keys.size() >= batch_limit) {
                auto err = handler(batch_keys, batch_metas);
                if (err != ErrorCode::OK) return tl::make_unexpected(err);
                batch_keys.clear();
                batch_metas.clear();
            }
        }
        if (!batch_keys.empty()) {
            auto err = handler(batch_keys, batch_metas);
            if (err != ErrorCode::OK) return tl::make_unexpected(err);
        }
        return {};
    }

    for (int i = 0; i < hash_bucket_count_; ++i) {
        std::string bucket_dir = fmt::format("{}/{:02x}", root_dir_, i);
        auto file_infos = fs_adapter_->ListFilesWithInfo(bucket_dir);
        if (!file_infos) {
            if (file_infos.error() == ErrorCode::FILE_NOT_FOUND) {
                continue;
            }
            LOG(ERROR) << "Failed to list files in bucket " << bucket_dir
                       << ": " << static_cast<int>(file_infos.error());
            return tl::make_unexpected(file_infos.error());
        }

        for (const auto& info : *file_infos) {
            std::string key = UnescapeFilename(info.name);
            batch_keys.push_back(key);
            StorageObjectMetadata meta{-1, 0, static_cast<int64_t>(key.size()),
                                       static_cast<int64_t>(info.size), ""};
            batch_metas.push_back(meta);

            if (batch_keys.size() >= batch_limit) {
                auto err = handler(batch_keys, batch_metas);
                if (err != ErrorCode::OK) return tl::make_unexpected(err);
                batch_keys.clear();
                batch_metas.clear();
            }
        }
    }
    if (!batch_keys.empty()) {
        auto err = handler(batch_keys, batch_metas);
        if (err != ErrorCode::OK) return tl::make_unexpected(err);
    }
    return {};
}

// === Key -> Path mapping ===

std::string DistributedStorageBackend::GetObjectPath(
    const std::string& key) const {
    uint64_t hash = XXH64(key.data(), key.size(), 0);
    std::string bucket = fmt::format("{:02x}", hash % hash_bucket_count_);
    std::string safe_key = EscapeFilename(key);
    return (std::filesystem::path(root_dir_) / bucket / safe_key).string();
}

std::string DistributedStorageBackend::EscapeFilename(const std::string& key) {
    std::string result;
    result.reserve(key.size() + 16);
    for (unsigned char c : key) {
        if (c == '@' || c == ':' || c == '/' || c == '\\' || c == '%' ||
            c < 0x20 || c > 0x7e) {
            result += fmt::format("%{:02x}", static_cast<unsigned>(c));
        } else {
            result += static_cast<char>(c);
        }
    }
    return result;
}

std::string DistributedStorageBackend::UnescapeFilename(
    const std::string& name) {
    auto is_hex = [](char c) {
        return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') ||
               (c >= 'A' && c <= 'F');
    };
    std::string result;
    result.reserve(name.size());
    for (size_t i = 0; i < name.size(); ++i) {
        if (name[i] == '%' && i + 2 < name.size() && is_hex(name[i + 1]) &&
            is_hex(name[i + 2])) {
            char hex[3] = {name[i + 1], name[i + 2], 0};
            unsigned long val = strtoul(hex, nullptr, 16);
            result += static_cast<char>(val);
            i += 2;
        } else {
            result += name[i];
        }
    }
    return result;
}

}  // namespace mooncake

#include "storage/distributed/distributed_storage_backend.h"

#include <filesystem>

#include "storage/distributed/dfs_global_allocator.h"
#include "types.h"
#include "utils.h"

namespace mooncake {

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
    if (fs_adapter_type != "hf3fs" && fs_adapter_type != "posix") {
        LOG(ERROR) << "DistributedStorageConfig: unsupported fs_adapter_type: "
                   << fs_adapter_type;
        return false;
    }
    if (shard_count <= 0) {
        LOG(ERROR) << "DistributedStorageConfig: shard_count must > 0";
        return false;
    }
    if (shard_capacity == 0) {
        LOG(ERROR) << "DistributedStorageConfig: shard_capacity must > 0";
        return false;
    }
    if (alignment == 0 || (alignment & (alignment - 1)) != 0) {
        LOG(ERROR) << "DistributedStorageConfig: alignment must be power of 2";
        return false;
    }
    if (shard_capacity % alignment != 0) {
        LOG(ERROR) << "DistributedStorageConfig: shard_capacity must align";
        return false;
    }
    if (!single_tenant) {
        LOG(ERROR) << "DistributedStorageConfig: Currently, DFS requires "
                      "single_tenant=true";
        return false;
    }
    return true;
}

DistributedStorageConfig DistributedStorageConfig::FromEnvironment() {
    DistributedStorageConfig config;
    config.fsdir = GetEnvStringOr(
        "MOONCAKE_DFS_ROOT_DIR",
        GetEnvStringOr("MOONCAKE_DISTRIBUTED_ROOT_DIR", config.fsdir));
    if (!std::filesystem::path(config.fsdir).is_absolute()) {
        config.fsdir = std::filesystem::absolute(config.fsdir).string();
    }
    config.fs_adapter_type = GetEnvStringOr(
        "MOONCAKE_DFS_FS_ADAPTER",
        GetEnvStringOr("MOONCAKE_DISTRIBUTED_FS_TYPE", config.fs_adapter_type));
    config.enable_health_check =
        GetEnvOr<bool>("MOONCAKE_DISTRIBUTED_HEALTH_CHECK", false);
    config.shard_count =
        GetEnvOr<int>("MOONCAKE_DFS_SHARD_COUNT", config.shard_count);
    config.shard_capacity = GetEnvOr<uint64_t>("MOONCAKE_DFS_SHARD_CAPACITY",
                                               config.shard_capacity);
    config.alignment =
        GetEnvOr<uint64_t>("MOONCAKE_DFS_ALIGNMENT", config.alignment);
    config.single_tenant =
        GetEnvOr<bool>("MOONCAKE_DFS_SINGLE_TENANT", config.single_tenant);
    return config;
}

DistributedStorageBackend::DistributedStorageBackend(
    const FileStorageConfig& file_storage_config,
    const DistributedStorageConfig& distributed_config,
    std::unique_ptr<FileSystemAdapter> fs_adapter)
    : StorageBackendInterface(file_storage_config),
      fs_adapter_(std::move(fs_adapter)),
      desc_cache_(std::make_shared<DfsDescriptorCache>()),
      distributed_config_(distributed_config),
      root_dir_(distributed_config.fsdir),
      alignment_(distributed_config.alignment) {}

DistributedStorageBackend::~DistributedStorageBackend() {
    for (auto& shard : shard_files_) {
        if (shard && shard->fd >= 0 && fs_adapter_) {
            fs_adapter_->CloseFile(shard->fd);
            shard->fd = -1;
        }
    }
    if (fs_adapter_) fs_adapter_->Shutdown();
}

tl::expected<void, ErrorCode> DistributedStorageBackend::Init() {
    if (initialized_) {
        LOG(WARNING) << "DistributedStorageBackend is already initialized";
        return {};
    }
    if (!desc_cache_) {
        desc_cache_ = std::make_shared<DfsDescriptorCache>();
    }

    std::error_code ec;
    std::filesystem::create_directories(root_dir_, ec);
    if (ec) {
        LOG(ERROR) << "Failed to create DFS root directory " << root_dir_
                   << ": " << ec.message();
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }

    auto init_result = fs_adapter_->Init(root_dir_);
    if (!init_result) return init_result;

    shard_files_.reserve(distributed_config_.shard_count);
    for (int i = 0; i < distributed_config_.shard_count; ++i) {
        std::string path = root_dir_ + "/dfs_shard_" +
                           DfsGlobalAllocator::FormatShardIdx(
                               i, distributed_config_.shard_count) +
                           ".data";
        auto fd_result = fs_adapter_->OpenFile(path);
        if (!fd_result) {
            LOG(ERROR) << "Failed to open DFS shard " << path << ": "
                       << fd_result.error();
            return tl::make_unexpected(fd_result.error());
        }
        auto shard = std::make_unique<ShardFile>();
        shard->path = std::move(path);
        shard->fd = *fd_result;
        shard_files_.push_back(std::move(shard));
    }

    initialized_ = true;
    return {};
}

tl::expected<int64_t, ErrorCode> DistributedStorageBackend::BatchOffload(
    const std::unordered_map<std::string, std::vector<Slice>>& batch_object,
    std::function<ErrorCode(const std::vector<std::string>& keys,
                            std::vector<StorageObjectMetadata>& metadatas)>
        complete_handler,
    std::function<void(const std::vector<std::string>& evicted_keys)>
        eviction_handler) {
    if (!initialized_) {
        LOG(ERROR) << "DistributedStorageBackend is not initialized";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    if (eviction_handler) {
        LOG_FIRST_N(WARNING, 1)
            << "DistributedStorageBackend DFS mode ignores local eviction "
               "handler";
    }

    std::vector<std::string> success_keys;
    std::vector<StorageObjectMetadata> success_metas;

    for (const auto& [storage_key, slices] : batch_object) {
        auto desc = LookupDescriptor(storage_key);
        if (!desc) {
            LOG(WARNING) << "DFS descriptor cache miss for key " << storage_key;
            continue;
        }
        if (desc->shard_idx < 0 ||
            desc->shard_idx >= static_cast<int>(shard_files_.size())) {
            LOG(ERROR) << "Invalid DFS shard_idx " << desc->shard_idx
                       << " for key " << storage_key;
            continue;
        }

        std::vector<iovec> iovs;
        iovs.reserve(slices.size());
        uint64_t total_size = 0;
        bool invalid = false;
        for (const auto& slice : slices) {
            if (!slice.ptr && slice.size > 0) {
                invalid = true;
                break;
            }
            if (reinterpret_cast<uintptr_t>(slice.ptr) % alignment_ != 0) {
                LOG(WARNING) << "DFS write buffer is not aligned for key "
                             << storage_key;
                invalid = true;
                break;
            }
            total_size += slice.size;
            iovs.push_back({slice.ptr, slice.size});
        }
        if (invalid) continue;
        if (total_size != desc->object_size) {
            LOG(WARNING) << "DFS size mismatch for key " << storage_key
                         << ", expected=" << desc->object_size
                         << ", actual=" << total_size;
            continue;
        }

        auto& shard = *shard_files_[desc->shard_idx];
        std::lock_guard lock(shard.mutex);
        auto result = fs_adapter_->WriteAt(shard.fd, iovs.data(), iovs.size(),
                                           static_cast<int64_t>(desc->offset));
        if (!result || *result != total_size) {
            LOG(WARNING) << "DFS write failed for key " << storage_key
                         << ", expected=" << total_size
                         << ", actual=" << (result ? *result : 0);
            continue;
        }

        success_keys.push_back(storage_key);
        success_metas.push_back(
            StorageObjectMetadata{-1, static_cast<int64_t>(desc->offset),
                                  static_cast<int64_t>(storage_key.size()),
                                  static_cast<int64_t>(desc->object_size), ""});
    }

    if (!success_keys.empty() && complete_handler) {
        auto err = complete_handler(success_keys, success_metas);
        if (err != ErrorCode::OK) return tl::make_unexpected(err);
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
        auto desc = LookupDescriptor(key);
        if (!desc) return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
        if (desc->shard_idx < 0 ||
            desc->shard_idx >= static_cast<int>(shard_files_.size())) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (slice.size < desc->object_size || !slice.ptr) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (reinterpret_cast<uintptr_t>(slice.ptr) % alignment_ != 0) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }

        auto& shard = *shard_files_[desc->shard_idx];
        std::lock_guard lock(shard.mutex);
        iovec iov{slice.ptr, static_cast<size_t>(desc->object_size)};
        auto result = fs_adapter_->ReadAt(shard.fd, &iov, 1,
                                          static_cast<int64_t>(desc->offset));
        if (!result || *result != desc->object_size) {
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
    return LookupDescriptor(key).has_value();
}

tl::expected<bool, ErrorCode> DistributedStorageBackend::IsEnableOffloading() {
    return true;
}

tl::expected<void, ErrorCode> DistributedStorageBackend::ScanMeta(
    const std::function<ErrorCode(
        const std::vector<std::string>& keys,
        std::vector<StorageObjectMetadata>& metadatas)>& /*handler*/) {
    return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
}

std::optional<DistributedFSDescriptor>
DistributedStorageBackend::LookupDescriptor(
    const std::string& storage_key) const {
    if (!desc_cache_) return std::nullopt;
    if (auto desc = desc_cache_->Get(storage_key)) return desc;

    auto [tenant_id, user_key] = ParseTenantScopedStorageKey(storage_key);
    (void)tenant_id;
    if (user_key != storage_key) {
        return desc_cache_->Get(user_key);
    }
    return std::nullopt;
}

}  // namespace mooncake

#include "storage/distributed/distributed_storage_backend.h"

#include <algorithm>
#include <filesystem>
#include <limits>
#include <map>
#include <optional>
#include <string_view>

#include "types.h"

namespace mooncake {

namespace {

std::optional<int> ParseShardFileName(std::string_view name) {
    constexpr std::string_view prefix = "dfs_shard_";
    constexpr std::string_view suffix = ".data";
    if (!name.starts_with(prefix) || !name.ends_with(suffix)) {
        return std::nullopt;
    }
    name.remove_prefix(prefix.size());
    name.remove_suffix(suffix.size());
    if (name.size() < 2) return std::nullopt;

    int index = 0;
    for (char digit : name) {
        if (digit < '0' || digit > '9' ||
            index > (std::numeric_limits<int>::max() - (digit - '0')) / 10) {
            return std::nullopt;
        }
        index = index * 10 + (digit - '0');
    }
    return index;
}

std::optional<int> ParseBucketFileName(std::string_view name) {
    constexpr std::string_view prefix = "bucket_";
    constexpr std::string_view suffix = ".data";
    if (!name.starts_with(prefix) || !name.ends_with(suffix)) {
        return std::nullopt;
    }
    name.remove_prefix(prefix.size());
    name.remove_suffix(suffix.size());
    if (name.empty()) return std::nullopt;

    int index = 0;
    for (char digit : name) {
        if (digit < '0' || digit > '9' ||
            index > (std::numeric_limits<int>::max() - (digit - '0')) / 10) {
            return std::nullopt;
        }
        index = index * 10 + (digit - '0');
    }
    return index;
}

bool IsDfsDescriptorRangeValid(const DistributedFSDescriptor& desc,
                               const DistributedStorageConfig& config) {
    if (config.alignment == 0 || desc.object_size == 0 ||
        desc.aligned_size < desc.object_size ||
        desc.offset % config.alignment != 0 ||
        desc.aligned_size % config.alignment != 0) {
        return false;
    }
    if (desc.offset > config.shard_capacity ||
        desc.aligned_size > config.shard_capacity - desc.offset) {
        return false;
    }

    constexpr uint64_t kMaxFileOffset =
        static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
    return desc.offset <= kMaxFileOffset &&
           desc.aligned_size <= kMaxFileOffset - desc.offset;
}

bool IsBucketDescriptorRangeValid(const DistributedFSDescriptor& desc,
                                  const DistributedStorageConfig& config) {
    if (config.alignment == 0 || desc.shard_idx < 0 || desc.object_size == 0 ||
        desc.aligned_size < desc.object_size ||
        desc.offset % config.alignment != 0 ||
        desc.aligned_size % config.alignment != 0 ||
        desc.offset > config.bucket_capacity ||
        desc.aligned_size > config.bucket_capacity - desc.offset ||
        desc.object_size > config.bucket_capacity - desc.offset) {
        return false;
    }
    constexpr uint64_t kMaxOffset =
        static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
    return desc.offset <= kMaxOffset &&
           desc.object_size <= kMaxOffset - desc.offset &&
           desc.aligned_size <= kMaxOffset - desc.offset;
}

// Completes a positional transfer of `total` bytes at `offset` whose first
// `transferred` bytes are already done, retrying short transfers.
tl::expected<void, ErrorCode> TransferAll(FileSystemAdapter& adapter, int fd,
                                          std::vector<iovec> iovs,
                                          int64_t offset, uint64_t total,
                                          uint64_t transferred, bool write) {
    size_t index = 0;
    size_t consumed = transferred;
    while (true) {
        while (consumed > 0 && index < iovs.size()) {
            if (consumed >= iovs[index].iov_len) {
                consumed -= iovs[index].iov_len;
                ++index;
            } else {
                iovs[index].iov_base =
                    static_cast<char*>(iovs[index].iov_base) + consumed;
                iovs[index].iov_len -= consumed;
                consumed = 0;
            }
        }
        if (transferred >= total) return {};

        while (index < iovs.size() && iovs[index].iov_len == 0) ++index;
        if (index == iovs.size() ||
            iovs.size() - index >
                static_cast<size_t>(std::numeric_limits<int>::max())) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        const auto count = static_cast<int>(iovs.size() - index);
        const auto position = offset + static_cast<int64_t>(transferred);
        auto result =
            write ? adapter.WriteAt(fd, iovs.data() + index, count, position)
                  : adapter.ReadAt(fd, iovs.data() + index, count, position);
        if (!result) return tl::make_unexpected(result.error());
        if (*result == 0 || *result > total - transferred) {
            return tl::make_unexpected(write ? ErrorCode::FILE_WRITE_FAIL
                                             : ErrorCode::FILE_READ_FAIL);
        }
        consumed = *result;
        transferred += *result;
    }
}

bool IsObjectDescriptorValid(const DistributedFSDescriptor& desc,
                             const DistributedStorageConfig& config,
                             const ObjectStorageAdapter& adapter) {
    if (!desc.IsObjectStorage() || desc.object_size == 0 ||
        desc.ObjectStorageBackend() != adapter.GetName() ||
        desc.ObjectStorageBackend() != config.fs_adapter_type) {
        return false;
    }
    return true;
}

tl::expected<uint64_t, ErrorCode> CalculateSliceBytes(
    std::span<const Slice> slices) {
    uint64_t total = 0;
    for (const auto& slice : slices) {
        if ((slice.ptr == nullptr && slice.size != 0) ||
            slice.size > std::numeric_limits<uint64_t>::max() - total) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        total += slice.size;
    }
    return total;
}

}  // namespace

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
      root_dir_(distributed_config.fsdir) {
    CHECK((fs_adapter_ != nullptr) != (object_storage_adapter_ != nullptr))
        << "DistributedStorageBackend: exactly one I/O adapter is required";
    if (object_storage_adapter_) {
        storage_mode_ = DistributedStorageMode::kObjectStorage;
    }
}

DistributedStorageBackend::~DistributedStorageBackend() {
    for (auto& [_, shard] : shard_files_) {
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

    if (UsesObjectStorage()) {
        auto init_result = object_storage_adapter_->Init();
        if (!init_result) return init_result;
        if (distributed_config_.enable_health_check) {
            auto health_result = object_storage_adapter_->CheckHealth();
            if (!health_result) {
                LOG(ERROR) << "Object storage health check failed, adapter="
                           << object_storage_adapter_->GetName() << ", error="
                           << static_cast<int>(health_result.error());
                return health_result;
            }
        }
        initialized_ = true;
        LOG(INFO) << "DistributedStorageBackend initialized, object adapter="
                  << object_storage_adapter_->GetName();
        return {};
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

    // Only descriptors returned by the master identify published shards.
    // Opening files from the configured count or a directory scan can retain
    // handles to staged shards that a failed expansion subsequently unlinks.
    initialized_ = true;
    return {};
}

tl::expected<DistributedStorageBackend::ShardFile*, ErrorCode>
DistributedStorageBackend::GetOrOpenShard(
    const DistributedFSDescriptor& descriptor) {
    const auto path =
        std::filesystem::path(descriptor.file_path).lexically_normal();
    auto root = std::filesystem::path(root_dir_).lexically_normal();
    if (root.filename().empty()) root = root.parent_path();
    auto parent = path.parent_path();
    if (parent.empty()) parent = ".";
    const auto index = ParseShardFileName(path.filename().string());
    if (descriptor.shard_idx < 0 || !index || *index != descriptor.shard_idx ||
        parent != root) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    ShardFile* shard;
    {
        std::lock_guard cache_lock(shard_files_mutex_);
        auto existing = shard_files_.find(descriptor.shard_idx);
        if (existing == shard_files_.end()) {
            existing = shard_files_
                           .emplace(descriptor.shard_idx,
                                    std::make_unique<ShardFile>())
                           .first;
        }
        shard = existing->second.get();
    }

    // File validation and opening may block. Serialize them only with this
    // shard's initialization and non-batched I/O, leaving other cached shards
    // available.
    std::lock_guard shard_lock(shard->mutex);
    auto file_path = path.string();
    if (shard->fd >= 0) {
        if (shard->path != file_path) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        return shard;
    }

    // The master prepares new shards before publishing their descriptors.
    // OpenFile may create a file, so reject missing/unprepared shards first.
    auto file_size = fs_adapter_->GetFileSize(file_path);
    if (!file_size) return tl::make_unexpected(file_size.error());
    if (*file_size != distributed_config_.shard_capacity) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    std::error_code ec;
    const auto canonical_root = std::filesystem::canonical(root, ec);
    if (ec) return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    const auto canonical_path = std::filesystem::canonical(path, ec);
    if (ec) return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    if (canonical_path.parent_path() != canonical_root ||
        canonical_path.filename() != path.filename()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto fd = fs_adapter_->OpenFile(file_path);
    if (!fd) return tl::make_unexpected(fd.error());
    // Failed attempts keep fd == -1 so a later request can retry
    // initialization.
    shard->path = std::move(file_path);
    shard->fd = *fd;
    return shard;
}

bool DistributedStorageBackend::UsesBucketAllocator() const {
    return distributed_config_.allocator_type == "bucket";
}

tl::expected<int, ErrorCode> DistributedStorageBackend::OpenBucket(
    const DistributedFSDescriptor& descriptor) {
    const auto path =
        std::filesystem::path(descriptor.file_path).lexically_normal();
    auto root = std::filesystem::path(root_dir_).lexically_normal();
    if (root.filename().empty()) root = root.parent_path();
    auto parent = path.parent_path();
    if (parent.empty()) parent = ".";
    const auto bucket_id = ParseBucketFileName(path.filename().string());
    if (!bucket_id || *bucket_id != descriptor.shard_idx || parent != root) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    const std::string file_path = path.string();
    auto file_size = fs_adapter_->GetFileSize(file_path);
    if (!file_size) return tl::make_unexpected(file_size.error());
    if (*file_size != distributed_config_.bucket_capacity) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    std::error_code error;
    const auto canonical_root = std::filesystem::canonical(root, error);
    if (error) return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    const auto canonical_path = std::filesystem::canonical(path, error);
    if (error) return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    if (canonical_path.parent_path() != canonical_root ||
        canonical_path.filename() != path.filename()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return fs_adapter_->OpenExistingFile(file_path);
}

tl::expected<int64_t, ErrorCode> DistributedStorageBackend::BatchOffload(
    const std::unordered_map<std::string, std::vector<Slice>>& batch_object,
    std::function<ErrorCode(const std::vector<std::string>& keys,
                            std::vector<StorageObjectMetadata>& metadatas)>
        complete_handler,
    EvictionHandler eviction_handler) {
    if (!UsesObjectStorage()) {
        return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    }
    if (!initialized_) {
        LOG(ERROR) << "DistributedStorageBackend is not initialized";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    if (eviction_handler) {
        LOG_FIRST_N(WARNING, 1)
            << "DistributedStorageBackend does not support eviction, "
               "eviction_handler ignored";
    }

    std::vector<ObjectPutRequest> object_requests;
    std::vector<std::vector<iovec>> iov_storage;
    std::vector<size_t> object_sizes;
    object_requests.reserve(batch_object.size());
    iov_storage.reserve(batch_object.size());
    object_sizes.reserve(batch_object.size());
    for (const auto& [key, slices] : batch_object) {
        if (slices.empty() ||
            slices.size() >
                static_cast<size_t>(std::numeric_limits<int>::max()))
            continue;
        auto total_size = CalculateSliceBytes(slices);
        if (!total_size || *total_size == 0 ||
            *total_size > std::numeric_limits<size_t>::max()) {
            LOG(WARNING) << "Failed to offload key " << key
                         << ": invalid slices";
            continue;
        }
        auto& iovs = iov_storage.emplace_back();
        iovs.reserve(slices.size());
        for (const auto& slice : slices)
            iovs.push_back({slice.ptr, slice.size});
        object_requests.push_back(
            {key, iovs.data(), static_cast<int>(iovs.size())});
        object_sizes.push_back(static_cast<size_t>(*total_size));
    }

    auto object_results = object_storage_adapter_->PutBatch(object_requests);
    if (object_results.size() != object_requests.size()) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    std::vector<std::string> success_keys;
    std::vector<StorageObjectMetadata> success_metas;
    success_keys.reserve(object_requests.size());
    success_metas.reserve(object_requests.size());
    for (size_t i = 0; i < object_results.size(); ++i) {
        if (!object_results[i]) {
            LOG(WARNING) << "Failed to offload key "
                         << object_requests[i].logical_key << ": "
                         << static_cast<int>(object_results[i].error());
            continue;
        }
        success_keys.push_back(object_requests[i].logical_key);
        success_metas.emplace_back(
            -1, 0, static_cast<int64_t>(object_requests[i].logical_key.size()),
            static_cast<int64_t>(object_sizes[i]), "");
    }

    if (complete_handler && !success_keys.empty()) {
        auto err = complete_handler(success_keys, success_metas);
        if (err != ErrorCode::OK) {
            return tl::make_unexpected(err);
        }
    }
    return static_cast<int64_t>(success_keys.size());
}

struct DistributedStorageBackend::DfsIoOp {
    size_t index;  // Position in the caller's request batch.
    const std::string& key;
    const DistributedFSDescriptor& descriptor;
    std::vector<iovec> iovs;
    uint64_t size;
};

tl::expected<void, ErrorCode> DistributedStorageBackend::ExecuteSingleIo(
    DfsIoOp& op, bool write) {
    if (UsesBucketAllocator()) {
        auto fd = OpenBucket(op.descriptor);
        if (!fd) return tl::make_unexpected(fd.error());
        auto result = TransferAll(*fs_adapter_, *fd, std::move(op.iovs),
                                  static_cast<int64_t>(op.descriptor.offset),
                                  op.size, 0, write);
        auto close_result = fs_adapter_->CloseFile(*fd);
        if (!result) return result;
        return close_result;
    }

    auto shard = GetOrOpenShard(op.descriptor);
    if (!shard) {
        LOG(ERROR) << "Failed to open DFS shard for key " << op.key << ": "
                   << shard.error();
        return tl::make_unexpected(shard.error());
    }
    std::lock_guard lock((*shard)->mutex);
    const auto count = static_cast<int>(op.iovs.size());
    const auto offset = static_cast<int64_t>(op.descriptor.offset);
    auto result =
        write
            ? fs_adapter_->WriteAt((*shard)->fd, op.iovs.data(), count, offset)
            : fs_adapter_->ReadAt((*shard)->fd, op.iovs.data(), count, offset);
    const char* op_name = write ? "write" : "read";
    if (!result) {
        LOG(WARNING) << "DFS " << op_name << " failed for key " << op.key
                     << ", error=" << result.error();
        return tl::make_unexpected(result.error());
    }
    if (*result != op.size) {
        LOG(WARNING) << "DFS short " << op_name << " for key " << op.key
                     << ", expected=" << op.size << ", actual=" << *result;
        return tl::make_unexpected(write ? ErrorCode::FILE_WRITE_FAIL
                                         : ErrorCode::FILE_READ_FAIL);
    }
    return {};
}

void DistributedStorageBackend::ExecuteShardBatchIo(
    std::vector<DfsIoOp>& ops,
    std::vector<tl::expected<void, ErrorCode>>& results, bool write) {
    std::vector<FdIoRequest> io_requests;
    std::vector<DfsIoOp*> io_ops;
    io_requests.reserve(ops.size());
    io_ops.reserve(ops.size());
    for (auto& op : ops) {
        auto shard = GetOrOpenShard(op.descriptor);
        if (!shard) {
            LOG(ERROR) << "Failed to open DFS shard for key " << op.key << ": "
                       << shard.error();
            results[op.index] = tl::make_unexpected(shard.error());
            continue;
        }
        io_requests.push_back({(*shard)->fd, op.iovs.data(),
                               static_cast<int>(op.iovs.size()),
                               static_cast<int64_t>(op.descriptor.offset)});
        io_ops.push_back(&op);
    }
    // Shard I/O fails short transfers.
    SubmitBatchIo(io_requests, io_ops, results, write, false);
}

void DistributedStorageBackend::ExecuteBucketBatchIo(
    std::vector<DfsIoOp>& ops,
    std::vector<tl::expected<void, ErrorCode>>& results, bool write) {
    // Group ops by the full descriptor identity OpenBucket validates, so each
    // bucket is opened once per batch.
    std::map<std::pair<std::string, int>, std::vector<DfsIoOp*>> buckets;
    for (auto& op : ops) {
        buckets[{op.descriptor.file_path, op.descriptor.shard_idx}].push_back(
            &op);
    }

    auto next = buckets.begin();
    while (next != buckets.end()) {
        std::vector<std::pair<int, const std::vector<DfsIoOp*>*>> opened;
        std::vector<FdIoRequest> io_requests;
        std::vector<DfsIoOp*> io_ops;
        for (; next != buckets.end() && opened.size() < kMaxOpenBucketsPerBatch;
             ++next) {
            const auto& bucket_ops = next->second;
            auto fd = OpenBucket(bucket_ops.front()->descriptor);
            if (!fd) {
                LOG(ERROR) << "Failed to open DFS bucket " << next->first.first
                           << ": " << fd.error();
                for (auto* op : bucket_ops) {
                    results[op->index] = tl::make_unexpected(fd.error());
                }
                continue;
            }
            opened.emplace_back(*fd, &bucket_ops);
            for (auto* op : bucket_ops) {
                io_requests.push_back(
                    {*fd, op->iovs.data(), static_cast<int>(op->iovs.size()),
                     static_cast<int64_t>(op->descriptor.offset)});
                io_ops.push_back(op);
            }
        }

        // Bucket I/O completes short transfers.
        SubmitBatchIo(io_requests, io_ops, results, write, true);

        // A failed close fails the bucket's otherwise successful requests.
        for (const auto& [fd, bucket_ops] : opened) {
            auto close_result = fs_adapter_->CloseFile(fd);
            if (close_result) continue;
            for (auto* op : *bucket_ops) {
                auto& result = results[op->index];
                if (result) result = tl::make_unexpected(close_result.error());
            }
        }
    }
}

void DistributedStorageBackend::SubmitBatchIo(
    const std::vector<FdIoRequest>& io_requests,
    const std::vector<DfsIoOp*>& io_ops,
    std::vector<tl::expected<void, ErrorCode>>& results, bool write,
    bool complete_short_io) {
    if (io_requests.empty()) return;
    const char* op_name = write ? "write" : "read";
    auto io_results = write ? fs_adapter_->BatchWriteAt(io_requests)
                            : fs_adapter_->BatchReadAt(io_requests);
    if (io_results.size() != io_requests.size()) {
        LOG(ERROR) << "Filesystem adapter returned an invalid batch " << op_name
                   << " result count";
        for (auto* op : io_ops) {
            results[op->index] = tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        return;
    }
    for (size_t i = 0; i < io_results.size(); ++i) {
        auto& op = *io_ops[i];
        const auto& result = io_results[i];
        if (!result) {
            LOG(WARNING) << "DFS " << op_name << " failed for key " << op.key
                         << ", error=" << result.error();
            results[op.index] = tl::make_unexpected(result.error());
        } else if (*result == op.size) {
            continue;
        } else if (complete_short_io && *result != 0 && *result < op.size) {
            auto rest =
                TransferAll(*fs_adapter_, io_requests[i].fd, std::move(op.iovs),
                            io_requests[i].offset, op.size, *result, write);
            if (!rest) results[op.index] = tl::make_unexpected(rest.error());
        } else {
            LOG(WARNING) << "DFS short " << op_name << " for key " << op.key
                         << ", expected=" << op.size << ", actual=" << *result;
            results[op.index] = tl::make_unexpected(
                write ? ErrorCode::FILE_WRITE_FAIL : ErrorCode::FILE_READ_FAIL);
        }
    }
}

std::vector<tl::expected<void, ErrorCode>>
DistributedStorageBackend::BatchWrite(
    const std::vector<DfsWriteRequest>& requests) {
    std::vector<tl::expected<void, ErrorCode>> results(requests.size());

    if (!initialized_) {
        LOG(ERROR) << "DistributedStorageBackend is not initialized";
        results.assign(requests.size(),
                       tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE));
        return results;
    }

    if (UsesObjectStorage()) {
        results.resize(requests.size());
        std::vector<ObjectStoragePutRequest> object_requests;
        std::vector<size_t> request_indices;
        object_requests.reserve(requests.size());
        request_indices.reserve(requests.size());
        for (size_t index = 0; index < requests.size(); ++index) {
            const auto& request = requests[index];
            const auto& desc = request.descriptor;
            if (!desc.IsObjectStorage()) {
                results[index] = tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
                continue;
            }
            auto total_size = CalculateSliceBytes(request.slices);
            if (request.slices.empty() || !total_size ||
                !IsObjectDescriptorValid(desc, distributed_config_,
                                         *object_storage_adapter_) ||
                *total_size != desc.object_size) {
                results[index] = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                continue;
            }
            object_requests.push_back(
                {request.key, request.slices, request.replace_existing});
            request_indices.push_back(index);
        }

        auto object_results =
            object_storage_adapter_->BatchPutV(object_requests);
        if (object_results.size() != object_requests.size()) {
            for (size_t index : request_indices) {
                results[index] = tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            }
            return results;
        }
        for (size_t index = 0; index < object_results.size(); ++index) {
            results[request_indices[index]] = std::move(object_results[index]);
        }
        return results;
    }

    // Storage layout: bucket I/O uses temporary fds and completes short writes;
    // shard I/O reuses cached fds and fails short writes.
    const bool bucket_mode = UsesBucketAllocator();
    // Execution mode: adapters that opt in (currently 3FS) collect validated
    // writes for submission after the loop. Other adapters (including POSIX)
    // execute each write in the loop, preserving per-request bucket handles
    // and shard I/O locking.
    const bool batch_io = fs_adapter_->SupportsBatchIo();
    std::vector<DfsIoOp> ops;
    if (batch_io) ops.reserve(requests.size());
    for (size_t i = 0; i < requests.size(); ++i) {
        const auto& request = requests[i];
        const auto& desc = request.descriptor;
        const bool range_valid =
            bucket_mode
                ? IsBucketDescriptorRangeValid(desc, distributed_config_)
                : IsDfsDescriptorRangeValid(desc, distributed_config_);
        if (!range_valid) {
            LOG(ERROR) << "Invalid DFS descriptor range for key " << request.key
                       << ", offset=" << desc.offset
                       << ", object_size=" << desc.object_size
                       << ", aligned_size=" << desc.aligned_size
                       << ", capacity="
                       << (bucket_mode ? distributed_config_.bucket_capacity
                                       : distributed_config_.shard_capacity);
            results[i] = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }

        if (request.slices.size() >
            static_cast<size_t>(std::numeric_limits<int>::max())) {
            results[i] = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }
        std::vector<iovec> iovs;
        iovs.reserve(request.slices.size());
        auto total_size = CalculateSliceBytes(request.slices);
        if (!total_size || *total_size != desc.object_size ||
            request.slices.size() >
                static_cast<size_t>(std::numeric_limits<int>::max())) {
            LOG(WARNING) << "Invalid DFS write request for key " << request.key
                         << ", expected=" << desc.object_size
                         << ", actual=" << (total_size ? *total_size : 0);
            results[i] = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }
        for (const auto& slice : request.slices) {
            iovs.push_back({slice.ptr, slice.size});
        }
        DfsIoOp op{i, request.key, desc, std::move(iovs), *total_size};
        if (batch_io) {
            ops.push_back(std::move(op));
        } else {
            results[i] = ExecuteSingleIo(op, true);
        }
    }

    if (!batch_io) return results;
    if (bucket_mode) {
        ExecuteBucketBatchIo(ops, results, true);
    } else {
        ExecuteShardBatchIo(ops, results, true);
    }
    return results;
}

std::vector<tl::expected<void, ErrorCode>> DistributedStorageBackend::BatchRead(
    const std::vector<DfsReadRequest>& requests) {
    std::vector<tl::expected<void, ErrorCode>> results(requests.size());

    if (!initialized_) {
        LOG(ERROR) << "DistributedStorageBackend is not initialized";
        results.assign(requests.size(),
                       tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE));
        return results;
    }

    if (UsesObjectStorage()) {
        results.resize(requests.size());
        std::vector<ObjectStorageGetRequest> object_requests;
        std::vector<size_t> request_indices;
        object_requests.reserve(requests.size());
        request_indices.reserve(requests.size());
        for (size_t index = 0; index < requests.size(); ++index) {
            const auto& request = requests[index];
            const auto& desc = request.descriptor;
            if (!desc.IsObjectStorage()) {
                results[index] = tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
                continue;
            }
            auto capacity = CalculateSliceBytes(request.slices);
            if (request.slices.empty() || !capacity ||
                desc.object_size > std::numeric_limits<size_t>::max() ||
                !IsObjectDescriptorValid(desc, distributed_config_,
                                         *object_storage_adapter_) ||
                *capacity < desc.object_size) {
                results[index] = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                continue;
            }
            object_requests.push_back({request.key, request.slices,
                                       static_cast<size_t>(desc.object_size)});
            request_indices.push_back(index);
        }

        auto object_results =
            object_storage_adapter_->BatchGetInto(object_requests);
        if (object_results.size() != object_requests.size()) {
            for (size_t index : request_indices) {
                results[index] = tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            }
            return results;
        }
        for (size_t index = 0; index < object_results.size(); ++index) {
            results[request_indices[index]] = std::move(object_results[index]);
        }
        return results;
    }

    // Storage layout: bucket I/O uses temporary fds and completes short reads;
    // shard I/O reuses cached fds and fails short reads.
    const bool bucket_mode = UsesBucketAllocator();
    // Execution mode: adapters that opt in (currently 3FS) collect validated
    // reads for submission after the loop. Other adapters (including POSIX)
    // execute each read in the loop, preserving per-request bucket handles
    // and shard I/O locking.
    const bool batch_io = fs_adapter_->SupportsBatchIo();
    std::vector<DfsIoOp> ops;
    if (batch_io) ops.reserve(requests.size());
    for (size_t i = 0; i < requests.size(); ++i) {
        const auto& request = requests[i];
        const auto& desc = request.descriptor;
        const bool range_valid =
            bucket_mode
                ? IsBucketDescriptorRangeValid(desc, distributed_config_)
                : IsDfsDescriptorRangeValid(desc, distributed_config_);
        if (!range_valid) {
            LOG(ERROR) << "Invalid DFS descriptor range for key " << request.key
                       << ", offset=" << desc.offset
                       << ", object_size=" << desc.object_size
                       << ", aligned_size=" << desc.aligned_size
                       << ", capacity="
                       << (bucket_mode ? distributed_config_.bucket_capacity
                                       : distributed_config_.shard_capacity);
            results[i] = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }
        if (desc.object_size > std::numeric_limits<size_t>::max() ||
            request.slices.size() >
                static_cast<size_t>(std::numeric_limits<int>::max())) {
            LOG(ERROR) << "DFS read request exceeds platform limits for key "
                       << request.key << ", object_size=" << desc.object_size
                       << ", slice_count=" << request.slices.size();
            results[i] = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }

        std::vector<iovec> iovs;
        iovs.reserve(request.slices.size());
        size_t remaining = static_cast<size_t>(desc.object_size);
        bool invalid = false;
        for (const auto& slice : request.slices) {
            if (!slice.ptr && slice.size > 0) {
                invalid = true;
                break;
            }
            if (remaining == 0 || slice.size == 0) {
                continue;
            }
            const size_t read_size = std::min(slice.size, remaining);
            iovs.push_back({slice.ptr, read_size});
            remaining -= read_size;
        }
        if (invalid || remaining != 0) {
            LOG(WARNING) << "Invalid DFS read request for key " << request.key
                         << ", expected capacity at least=" << desc.object_size;
            results[i] = tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }
        DfsIoOp op{i, request.key, desc, std::move(iovs), desc.object_size};
        if (batch_io) {
            ops.push_back(std::move(op));
        } else {
            results[i] = ExecuteSingleIo(op, false);
        }
    }

    if (!batch_io) return results;
    if (bucket_mode) {
        ExecuteBucketBatchIo(ops, results, false);
    } else {
        ExecuteShardBatchIo(ops, results, false);
    }
    return results;
}

bool DistributedStorageBackend::SupportsProviderQuery() const {
    return UsesObjectStorage() && object_storage_adapter_ &&
           object_storage_adapter_->SupportsProviderQuery();
}

std::chrono::milliseconds DistributedStorageBackend::ProviderQueryTimeout()
    const {
    return std::chrono::milliseconds(
        distributed_config_.provider_query_timeout_ms);
}

bool DistributedStorageBackend::IsProviderReplica(
    const Replica::Descriptor& replica) const {
    return UsesObjectStorage() && object_storage_adapter_ &&
           replica.is_dfs_replica() &&
           IsObjectDescriptorValid(replica.get_dfs_descriptor(),
                                   distributed_config_,
                                   *object_storage_adapter_);
}

ObjectStorageQueryResults DistributedStorageBackend::BatchQueryProvider(
    std::span<const std::string> logical_keys,
    std::chrono::steady_clock::time_point deadline) {
    if (!initialized_ || !SupportsProviderQuery()) {
        return ObjectStorageQueryResults(
            logical_keys.size(),
            tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE));
    }
    return object_storage_adapter_->BatchQueryProviderUntil(logical_keys,
                                                              deadline);
}

ObjectStorageIoResults DistributedStorageBackend::BatchDeleteProvider(
    std::span<const std::string> logical_keys) {
    if (!initialized_ || !UsesObjectStorage() || !object_storage_adapter_) {
        return ObjectStorageIoResults(
            logical_keys.size(),
            tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE));
    }
    return object_storage_adapter_->BatchDelete(logical_keys);
}

tl::expected<void, ErrorCode> DistributedStorageBackend::BatchLoad(
    std::unordered_map<std::string, Slice>& batched_slices) {
    if (!UsesObjectStorage()) {
        return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    }
    if (!initialized_) {
        LOG(ERROR) << "DistributedStorageBackend is not initialized";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    std::vector<ObjectGetRequest> requests;
    requests.reserve(batched_slices.size());
    for (auto& [key, slice] : batched_slices) {
        requests.push_back({key, slice.ptr, slice.size});
    }
    auto results = object_storage_adapter_->GetBatch(requests);
    if (results.size() != requests.size()) {
        LOG(ERROR)
            << "Object storage returned an invalid GET batch result count";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i]) {
            return tl::make_unexpected(results[i].error());
        }
        if (*results[i] != requests[i].size) {
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
    }
    return {};
}

tl::expected<bool, ErrorCode> DistributedStorageBackend::IsExist(
    const std::string& key) {
    if (!UsesObjectStorage()) {
        return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    }
    if (!initialized_) {
        LOG(ERROR) << "DistributedStorageBackend is not initialized";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return object_storage_adapter_->Exists(key);
}

tl::expected<bool, ErrorCode> DistributedStorageBackend::IsEnableOffloading() {
    return UsesObjectStorage();
}

tl::expected<void, ErrorCode> DistributedStorageBackend::ScanMeta(
    const std::function<
        ErrorCode(const std::vector<std::string>& keys,
                  std::vector<StorageObjectMetadata>& metadatas)>& handler) {
    if (!UsesObjectStorage()) {
        return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    }
    if (!initialized_) {
        LOG(ERROR) << "DistributedStorageBackend is not initialized";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    std::vector<std::string> batch_keys;
    std::vector<StorageObjectMetadata> batch_metas;
    const size_t batch_limit = static_cast<size_t>(std::max<int64_t>(
        1, file_storage_config_.scanmeta_iterator_keys_limit));

    auto key_infos = object_storage_adapter_->ListKeys();
    if (!key_infos) {
        LOG(ERROR) << "Failed to list keys from object storage adapter: "
                   << static_cast<int>(key_infos.error());
        return tl::make_unexpected(key_infos.error());
    }

    for (const auto& info : *key_infos) {
        batch_keys.push_back(info.logical_key);
        batch_metas.emplace_back(-1, 0,
                                 static_cast<int64_t>(info.logical_key.size()),
                                 static_cast<int64_t>(info.size), "");
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

}  // namespace mooncake

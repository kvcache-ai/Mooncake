#include "file_storage.h"

#include <memory>
#include <unordered_set>
#include <vector>

#include "utils.h"
namespace mooncake {

// Helper: Get integer from environment variable, fallback to default
template <typename T>
T FileStorageConfig::GetEnvOr(const char* name, T default_value) {
    const char* env_val = std::getenv(name);
    if (!env_val || std::string(env_val).empty()) {
        return default_value;
    }
    try {
        long long value = std::stoll(env_val);
        // Check range for unsigned types
        if constexpr (std::is_same_v<T, uint32_t>) {
            if (value < 0 || value > UINT32_MAX) throw std::out_of_range("");
        }
        return static_cast<T>(value);
    } catch (...) {
        return default_value;
    }
}

// Helper: Get string from environment variable, fallback to default
std::string FileStorageConfig::GetEnvStringOr(
    const char* name, const std::string& default_value) {
    const char* env_val = std::getenv(name);
    return env_val ? std::string(env_val) : default_value;
}

FileStorageConfig FileStorageConfig::FromEnvironment() {
    FileStorageConfig config;

    config.storage_filepath =
        GetEnvStringOr("FILE_STORAGE_PATH", config.storage_filepath);

    config.local_buffer_size =
        GetEnvOr<int64_t>("LOCAL_BUFFER_SIZE_BYTES", config.local_buffer_size);

    config.bucket_iterator_keys_limit = GetEnvOr<int64_t>(
        "BUCKET_ITERATOR_KEYS_LIMIT", config.bucket_iterator_keys_limit);

    config.bucket_keys_limit =
        GetEnvOr<int64_t>("BUCKET_KEYS_LIMIT", config.bucket_keys_limit);

    config.bucket_size_limit =
        GetEnvOr<int64_t>("BUCKET_SIZE_LIMIT_BYTES", config.bucket_size_limit);

    config.total_keys_limit =
        GetEnvOr<int64_t>("TOTAL_KEYS_LIMIT", config.total_keys_limit);

    config.total_size_limit =
        GetEnvOr<int64_t>("TOTAL_SIZE_LIMIT_BYTES", config.total_size_limit);

    config.heartbeat_interval_seconds = GetEnvOr<uint32_t>(
        "HEARTBEAT_INTERVAL_SECONDS", config.heartbeat_interval_seconds);

    return config;
}

bool FileStorageConfig::Validate() const {
    if (storage_filepath.empty()) {
        LOG(ERROR) << "FileStorageConfig: storage_filepath is invalid";
        return false;
    }
    const std::string& path = storage_filepath;
    namespace fs = std::filesystem;
    // 1. Must be an absolute path
    if (!fs::path(path).is_absolute()) {
        LOG(ERROR)
            << "FileStorageConfig: storage_filepath must be an absolute path: "
            << path;
        return false;
    }

    // 2. Check if the path contains ".." components that could lead to path
    // traversal (static check)
    fs::path p(path);
    for (const auto& component : p) {
        if (component == "..") {
            LOG(ERROR) << "FileStorageConfig: path traversal is not allowed: "
                       << path;
            return false;
        }
    }

    struct stat stat_buf;

    // 3. Use stat() to check if the path exists
    if (::stat(path.c_str(), &stat_buf) != 0) {
        LOG(ERROR) << "FileStorageConfig: storage_filepath does not exist: "
                   << path;
        return false;
    }
    // Path exists — check if it is a directory
    if (!S_ISDIR(stat_buf.st_mode)) {
        LOG(ERROR) << "FileStorageConfig: storage_filepath is not a directory: "
                   << path;
        return false;
    }

    // (Optional) Check write permission
    if (::access(path.c_str(), W_OK) != 0) {
        LOG(ERROR) << "FileStorageConfig: no write permission on directory: "
                   << path;
        return false;
    }

    // 4. Additional security: prevent symlink bypass (optional)
    // Use lstat to avoid automatic dereferencing of symbolic links
    struct stat lstat_buf;
    if (::lstat(path.c_str(), &lstat_buf) == 0) {
        if (S_ISLNK(lstat_buf.st_mode)) {
            LOG(ERROR) << "FileStorageConfig: symbolic link is not allowed: "
                       << path;
            return false;
        }
    }
    if (bucket_keys_limit <= 0) {
        LOG(ERROR) << "FileStorageConfig: bucket_keys_limit must > 0";
        return false;
    }
    if (bucket_size_limit <= 0) {
        LOG(ERROR) << "FileStorageConfig: bucket_size_limit must > 0";
        return false;
    }
    if (total_keys_limit <= 0) {
        LOG(ERROR) << "FileStorageConfig: total_keys_limit must > 0";
        return false;
    }
    if (total_size_limit == 0) {
        LOG(ERROR) << "FileStorageConfig: total_size_limit should not be zero";
        return false;
    }
    if (heartbeat_interval_seconds <= 0) {
        LOG(ERROR) << "FileStorageConfig: heartbeat_interval_seconds must > 0";
        return false;
    }
    return true;
}

FileStorage::FileStorage(std::shared_ptr<Client> client,
                         const std::string& segment_name,
                         const std::string& local_rpc_addr,
                         const FileStorageConfig& config)
    : client_(client),
      segment_name_(segment_name),
      local_rpc_addr_(local_rpc_addr),
      config_(config),
      storage_backend_(
          std::make_shared<BucketStorageBackend>(config.storage_filepath)),
      client_buffer_allocator_(
          ClientBufferAllocator::create(config.local_buffer_size, "")),
      sync_stat_bucket_iterator_(storage_backend_,
                                 config.bucket_iterator_keys_limit) {
    if (!config.Validate()) {
        throw std::invalid_argument("Invalid FileStorage configuration");
    }
}

FileStorage::~FileStorage() {
    LOG(INFO) << "Shutdown FileStorage...";
    heartbeat_running_ = false;
    if (heartbeat_thread_.joinable()) {
        heartbeat_thread_.join();
    }
}

tl::expected<void, ErrorCode> FileStorage::Init() {
    auto register_memory_result = RegisterLocalMemory();
    if (!register_memory_result) {
        LOG(ERROR) << "Failed to register local memory: "
                   << register_memory_result.error();
        return register_memory_result;
    }
    auto init_storage_backend_result = storage_backend_->Init();
    if (!init_storage_backend_result) {
        LOG(ERROR) << "Failed to init storage backend: "
                   << init_storage_backend_result.error();
        return init_storage_backend_result;
    }
    auto enable_offloading_result = IsEnableOffloading();
    if (!enable_offloading_result) {
        LOG(ERROR) << "Failed to get enable persist result, error : "
                   << enable_offloading_result.error();
        return tl::make_unexpected(enable_offloading_result.error());
    }
    {
        MutexLocker locker(&offloading_mutex_);
        enable_offloading_ = enable_offloading_result.value();
        auto mount_file_storage_result = client_->MountFileStorage(
            segment_name_, local_rpc_addr_, enable_offloading_);
        if (!mount_file_storage_result) {
            LOG(ERROR) << "Failed to mount file storage: "
                       << mount_file_storage_result.error();
            return mount_file_storage_result;
        }
    }

    BucketIterator bucket_iterator(storage_backend_,
                                   config_.bucket_iterator_keys_limit);
    while (true) {
        auto has_next_res = bucket_iterator.HasNext();
        if (!has_next_res) {
            LOG(ERROR) << "Failed to check for next bucket: "
                       << has_next_res.error();
            return tl::make_unexpected(has_next_res.error());
        }
        if (!has_next_res.value()) {
            break;
        }
        auto add_all_object_res = bucket_iterator.HandleNext(
            [this](const std::vector<std::string>& keys,
                   const std::vector<StorageObjectMetadata>& metadatas,
                   const std::vector<int64_t>&) {
                auto add_object_result = client_->NotifyOffloadSuccess(
                    segment_name_, keys, metadatas);
                if (!add_object_result) {
                    LOG(ERROR) << "Failed to add object to master: "
                               << add_object_result.error();
                    return add_object_result.error();
                }
                return ErrorCode::OK;
            });
        if (!add_all_object_res) {
            LOG(ERROR) << "Failed to add all object to master: "
                       << add_all_object_res.error();
            return add_all_object_res;
        }
    }

    heartbeat_running_.store(true);
    heartbeat_thread_ = std::thread([this]() {
        LOG(INFO) << "Starting periodic task with interval: "
                  << config_.heartbeat_interval_seconds
                  << "s, running is: " << heartbeat_running_.load();
        while (heartbeat_running_.load()) {
            Heartbeat();
            std::this_thread::sleep_for(
                std::chrono::seconds(config_.heartbeat_interval_seconds));
        }
    });
    return {};
}

tl::expected<void, ErrorCode> FileStorage::BatchGet(
    const std::string& transfer_engine_addr,
    const std::vector<std::string>& keys,
    const std::vector<uintptr_t>& pointers, const std::vector<int64_t>& sizes) {
    auto start_time = std::chrono::steady_clock::now();
    auto allocate_res = AllocateBatch(keys, sizes);
    if (!allocate_res) {
        LOG(ERROR) << "Failed to allocate batch objects, target = "
                   << transfer_engine_addr;
        return tl::make_unexpected(allocate_res.error());
    }
    auto result = BatchLoad(allocate_res.value().slices);
    if (!result) {
        LOG(ERROR) << "Batch load object failed,err_code = " << result.error();
        return result;
    }
    auto batch_put_result = client_->BatchPutOffloadObject(
        transfer_engine_addr, keys, pointers, allocate_res.value().slices);

    auto end_time = std::chrono::steady_clock::now();
    auto elapsed_time = std::chrono::duration_cast<std::chrono::microseconds>(
                            end_time - start_time)
                            .count();
    VLOG(1) << "Time taken for FileStorage::BatchGet: " << elapsed_time
            << "us,with transfer_engine_addr: " << transfer_engine_addr
            << ", key size: " << keys.size();
    if (!batch_put_result) {
        LOG(ERROR) << "Batch write offload object failed,err_code = "
                   << batch_put_result.error();
        return batch_put_result;
    }
    return {};
}

tl::expected<void, ErrorCode> FileStorage::OffloadObjects(
    const std::unordered_map<std::string, int64_t>& offloading_objects) {
    std::vector<std::vector<std::string>> buckets_keys;
    auto allocate_objects_result =
        GroupOffloadingKeysByBucket(offloading_objects, buckets_keys);
    if (!allocate_objects_result) {
        LOG(ERROR) << "GroupKeysByBucket failed with error: "
                   << allocate_objects_result.error();
        return allocate_objects_result;
    }
    for (const auto& keys : buckets_keys) {
        auto enable_offloading_result = IsEnableOffloading();
        if (!enable_offloading_result) {
            LOG(ERROR) << "Get is enable offloading failed with error: "
                       << enable_offloading_result.error();
            return tl::make_unexpected(enable_offloading_result.error());
        }
        if (!enable_offloading_result.value()) {
            LOG(WARNING) << "Unable to be persisted";
            MutexLocker locker(&offloading_mutex_);
            ungrouped_offloading_objects_.clear();
            enable_offloading_ = false;
            return tl::make_unexpected(ErrorCode::KEYS_ULTRA_LIMIT);
        }
        auto result = BatchOffload(keys);
        if (!result) {
            LOG(ERROR) << "Failed to store objects with error: "
                       << result.error();
            if (result.error() != ErrorCode::INVALID_READ) {
                return result;
            }
        }
    }
    return {};
}

tl::expected<bool, ErrorCode> FileStorage::IsEnableOffloading() {
    auto store_metadata_result = storage_backend_->GetStoreMetadata();
    if (!store_metadata_result) {
        LOG(ERROR) << "Failed to get store metadata: "
                   << store_metadata_result.error();
        return tl::make_unexpected(store_metadata_result.error());
    }
    const auto& store_metadata = store_metadata_result.value();
    auto enable_offloading =
        store_metadata.total_keys + config_.bucket_keys_limit <=
            config_.total_keys_limit &&
        store_metadata.total_size + config_.bucket_size_limit <=
            config_.total_size_limit;

    VLOG(1) << (enable_offloading ? "Enable" : "Unable")
            << " offloading,total keys: " << store_metadata.total_keys
            << ", bucket keys limit: " << config_.bucket_keys_limit
            << ", total keys limit: " << config_.total_keys_limit
            << ", total size: " << store_metadata.total_size
            << ", bucket size limit: " << config_.bucket_size_limit
            << ", total size limit: " << config_.total_size_limit;

    return enable_offloading;
}

tl::expected<void, ErrorCode> FileStorage::Heartbeat() {
    if (client_ == nullptr) {
        LOG(ERROR) << "client is nullptr";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    std::unordered_map<std::string, int64_t>
        offloading_objects;  // Objects selected for offloading

    // === STEP 1: Send heartbeat and get offloading decisions ===
    {
        MutexLocker locker(&offloading_mutex_);
        auto heartbeat_result = client_->OffloadObjectHeartbeat(
            segment_name_, enable_offloading_, offloading_objects);
        if (!heartbeat_result) {
            LOG(ERROR) << "Failed to send heartbeat with error: "
                       << heartbeat_result.error();
            return heartbeat_result;
        }
    }

    // === STEP 2: Persist offloaded objects (trigger actual data migration) ===
    auto offload_result = OffloadObjects(offloading_objects);
    if (!offload_result) {
        LOG(ERROR) << "Failed to persist objects with error: "
                   << offload_result.error();
        return offload_result;
    }

    // TODO(eviction): Implement an LRU eviction mechanism to manage local
    // storage capacity.
    return {};
}

tl::expected<void, ErrorCode> FileStorage::BatchOffload(
    const std::vector<std::string>& keys) {
    auto start_time = std::chrono::steady_clock::now();
    std::unordered_map<std::string, std::vector<Slice>> batch_object;
    auto query_result = BatchQuerySegmentSlices(keys, batch_object);
    if (!query_result) {
        LOG(ERROR) << "BatchQuerySlices failed with error: "
                   << query_result.error();
        return tl::make_unexpected(ErrorCode::INVALID_READ);
    }
    auto result = storage_backend_->BatchOffload(
        batch_object,
        [this](const std::vector<std::string>& keys,
               const std::vector<StorageObjectMetadata>& metadatas) {
            VLOG(1) << "Success to store objects, keys count: " << keys.size();
            auto result =
                client_->NotifyOffloadSuccess(segment_name_, keys, metadatas);
            if (!result) {
                LOG(ERROR) << "NotifyOffloadSuccess failed with error: "
                           << result.error();
                return result.error();
            }
            return ErrorCode::OK;
        });
    auto end_time = std::chrono::steady_clock::now();
    auto elapsed_time = std::chrono::duration_cast<std::chrono::microseconds>(
                            end_time - start_time)
                            .count();
    VLOG(1) << "Time taken for BatchStore: " << elapsed_time
            << "us,with keys count: " << keys.size();
    if (!result) {
        LOG(ERROR) << "Batch store object failed, err_code = "
                   << result.error();
        return tl::make_unexpected(result.error());
    }
    return {};
}

tl::expected<void, ErrorCode> FileStorage::BatchLoad(
    const std::unordered_map<std::string, Slice>& batch_object) {
    auto start_time = std::chrono::steady_clock::now();
    auto result = storage_backend_->BatchLoad(batch_object);
    auto end_time = std::chrono::steady_clock::now();
    auto elapsed_time = std::chrono::duration_cast<std::chrono::microseconds>(
                            end_time - start_time)
                            .count();
    VLOG(1) << "Time taken for BatchStore: " << elapsed_time
            << "us,with keys count: " << batch_object.size();
    if (!result) {
        LOG(ERROR) << "Batch load object failed,err_code = " << result.error();
    }
    return result;
}

tl::expected<void, ErrorCode> FileStorage::BatchQuerySegmentSlices(
    const std::vector<std::string>& keys,
    std::unordered_map<std::string, std::vector<Slice>>& batched_slices) {
    auto batched_query_results = client_->BatchQuery(keys);
    if (batched_query_results.empty())
        return tl::make_unexpected(ErrorCode::INVALID_REPLICA);
    for (size_t i = 0; i < batched_query_results.size(); ++i) {
        if (batched_query_results[i]) {
            for (const auto& descriptor :
                 batched_query_results[i].value().replicas) {
                if (descriptor.is_memory_replica()) {
                    std::vector<Slice> slices;
                    const auto& memory_descriptor = descriptor.get_memory_descriptor();
                    if (memory_descriptor.buffer_descriptor.transport_endpoint_ != segment_name_) {
                        break;
                    }
                    void* slice_ptr =
                        reinterpret_cast<void*>(memory_descriptor.buffer_descriptor.buffer_address_);
                    slices.emplace_back(Slice{slice_ptr, memory_descriptor.buffer_descriptor.size_});
                    batched_slices.insert({keys[i], std::move(slices)});
                    break;
                }
            }
            if (batched_slices.find(keys[i]) == batched_slices.end()) {
                LOG(ERROR) << "Key not found: " << keys[i];
                return tl::make_unexpected(ErrorCode::INVALID_KEY);
            }
        } else {
            LOG(ERROR) << "Key not found: " << keys[i];
            return tl::make_unexpected(batched_query_results[i].error());
        }
    }
    return {};
}

tl::expected<void, ErrorCode> FileStorage::RegisterLocalMemory() {
    auto error_code = client_->RegisterLocalMemory(
        client_buffer_allocator_->getBase(), config_.local_buffer_size,
        kWildcardLocation, false, false);
    if (!error_code) {
        LOG(ERROR) << "Failed to register local memory: " << error_code.error();
        return error_code;
    }
    return {};
}

tl::expected<FileStorage::AllocatedBatch, ErrorCode> FileStorage::AllocateBatch(
    const std::vector<std::string>& keys, const std::vector<int64_t>& sizes) {
    AllocatedBatch result;
    for (size_t i = 0; i < keys.size(); ++i) {
        assert(sizes[i] <= kMaxSliceSize);
        auto alloc_result = client_buffer_allocator_->allocate(sizes[i]);
        if (!alloc_result) {
            LOG(ERROR) << "Failed to allocate slice buffer, size = " << sizes[i]
                       << ", key = " << keys[i];
            return tl::make_unexpected(ErrorCode::BUFFER_OVERFLOW);
        }
        result.slices.emplace(
            keys[i], Slice{alloc_result->ptr(), static_cast<size_t>(sizes[i])});
        result.handles.emplace_back(std::move(alloc_result.value()));
    }
    return result;
}

tl::expected<void, ErrorCode> FileStorage::GroupOffloadingKeysByBucket(
    const std::unordered_map<std::string, int64_t>& offloading_objects,
    std::vector<std::vector<std::string>>& buckets_keys) {
    MutexLocker locker(&offloading_mutex_);
    auto it = offloading_objects.cbegin();
    int64_t residue_count =
        offloading_objects.size() + ungrouped_offloading_objects_.size();
    int64_t total_count =
        offloading_objects.size() + ungrouped_offloading_objects_.size();
    while (it != offloading_objects.cend()) {
        std::vector<std::string> bucket_keys;
        std::unordered_map<std::string, int64_t> bucket_objects;
        int64_t bucket_data_size = 0;
        // Process previously ungrouped objects first
        if (!ungrouped_offloading_objects_.empty()) {
            for (const auto& ungrouped_objects_it :
                 ungrouped_offloading_objects_) {
                bucket_data_size += ungrouped_objects_it.second;
                bucket_keys.push_back(ungrouped_objects_it.first);
                bucket_objects.emplace(ungrouped_objects_it.first,
                                       ungrouped_objects_it.second);
            }
            VLOG(1) << "Ungrouped offloading objects have been processed and "
                       "cleared; count="
                    << ungrouped_offloading_objects_.size();
            ungrouped_offloading_objects_.clear();
        }

        // Fill the rest of the bucket with new offloading objects
        for (size_t i = bucket_keys.size(); i < config_.bucket_keys_limit;
             ++i) {
            if (it == offloading_objects.cend()) {
                // No more objects to add — move current batch to ungrouped pool
                for (const auto& bucket_object : bucket_objects) {
                    ungrouped_offloading_objects_.emplace(bucket_object.first,
                                                          bucket_object.second);
                }
                VLOG(1) << "Add offloading objects to ungrouped pool. "
                        << "Total ungrouped count: "
                        << ungrouped_offloading_objects_.size();
                return {};
            }
            if (it->second > config_.bucket_size_limit) {
                LOG(ERROR) << "Object size exceeds bucket size limit: "
                           << "key=" << it->first
                           << ", object_size=" << it->second
                           << ", limit=" << config_.bucket_size_limit;
                ++it;
                continue;
            }
            auto is_exist_result = storage_backend_->IsExist(it->first);
            if (!is_exist_result) {
                LOG(ERROR) << "Failed to check existence in storage backend: "
                           << "key=" << it->first
                           << ", error=" << is_exist_result.error();
            }
            if (is_exist_result.value()) {
                ++it;
                continue;
            }
            if (bucket_data_size + it->second > config_.bucket_size_limit) {
                break;
            }
            bucket_data_size += it->second;
            bucket_keys.push_back(it->first);
            bucket_objects.emplace(it->first, it->second);
            ++it;
            if (bucket_data_size == config_.bucket_size_limit) {
                break;
            }
        }
        auto bucket_keys_count = bucket_keys.size();
        // Finalize current bucket
        residue_count -= bucket_keys_count;
        buckets_keys.push_back(std::move(bucket_keys));
        VLOG(1) << "Group objects with total object count: " << total_count
                << ", current bucket object count: " << bucket_keys_count
                << ", current bucket data size: " << bucket_data_size
                << ", grouped bucket count: " << buckets_keys.size()
                << ", residue object count: " << residue_count;
    }
    return {};
}

BucketIterator::BucketIterator(
    std::shared_ptr<BucketStorageBackend> storage_backend, int64_t limit)
    : storage_backend_(storage_backend), limit_(limit) {};

tl::expected<void, ErrorCode> BucketIterator::HandleNext(
    const std::function<
        ErrorCode(const std::vector<std::string>& keys,
                  const std::vector<StorageObjectMetadata>& metadatas,
                  const std::vector<int64_t>& buckets)>& handler) {
    MutexLocker locker(&mutex_);
    std::vector<std::string> keys;
    std::vector<StorageObjectMetadata> metadatas;
    std::vector<int64_t> buckets;
    auto key_iterator_result = storage_backend_->BucketScan(
        next_bucket_, keys, metadatas, buckets, limit_);
    if (!key_iterator_result) {
        LOG(ERROR) << "Bucket scan failed, error : "
                   << key_iterator_result.error();
        return tl::make_unexpected(key_iterator_result.error());
    }
    auto handle_result = handler(keys, metadatas, buckets);
    if (handle_result != ErrorCode::OK) {
        LOG(ERROR) << "Key iterator failed, error : " << handle_result;
        return tl::make_unexpected(handle_result);
    }
    next_bucket_ = key_iterator_result.value();
    return {};
}

tl::expected<bool, ErrorCode> BucketIterator::HasNext() {
    MutexLocker locker(&mutex_);
    return next_bucket_ != 0;
}

}  // namespace mooncake
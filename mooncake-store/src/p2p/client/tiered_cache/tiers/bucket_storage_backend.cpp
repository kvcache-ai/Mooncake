#include "p2p/client/tiered_cache/tiers/bucket_storage_backend.h"

#include <fcntl.h>
#include <glog/logging.h>
#include <algorithm>
#include <exception>
#include <unordered_set>

#include <ylt/struct_pb.hpp>

#include "utils.h"

namespace mooncake::v1 {

namespace {

std::string MakeTemporaryBucketPath(const std::string& path) {
    return path + ".tmp";
}

bool IsBucketTemporaryFile(const std::filesystem::path& path) {
    if (path.extension() != ".tmp") {
        return false;
    }

    const auto base_extension = path.stem().extension().string();
    return base_extension == ".bucket" || base_extension == ".meta";
}

void RemoveFileIfExists(const std::filesystem::path& path,
                        const char* failure_context) {
    std::error_code ec;
    const bool exists = std::filesystem::exists(path, ec);
    if (ec) {
        LOG(ERROR) << failure_context << ": " << path
                   << ", stat error: " << ec.message();
        return;
    }
    if (!exists) {
        return;
    }

    std::filesystem::remove(path, ec);
    if (ec) {
        LOG(ERROR) << failure_context << ": " << path
                   << ", error: " << ec.message();
    }
}

tl::expected<void, ErrorCode> RenameBucketFile(const std::string& from,
                                               const std::string& to) {
    std::error_code ec;
    std::filesystem::rename(from, to, ec);
    if (ec) {
        LOG(ERROR) << "Failed to rename bucket file from " << from << " to "
                   << to << ": " << ec.message();
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }
    return {};
}

}  // namespace

LocalStorageSpaceManager::LocalStorageSpaceManager(
    std::filesystem::path storage_root)
    : storage_root_(std::move(storage_root)) {}

tl::expected<void, ErrorCode> LocalStorageSpaceManager::Init(
    uint64_t used_space_bytes) {
    namespace fs = std::filesystem;

    std::shared_lock<std::shared_mutex> root_lock(mutex_);
    if (storage_root_.empty()) {
        LOG(ERROR) << "Storage root is empty for LocalStorageSpaceManager";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::error_code ec;
    auto space_info = fs::space(storage_root_, ec);
    if (ec) {
        LOG(ERROR) << "Failed to query filesystem space for " << storage_root_
                   << ": " << ec.message();
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    root_lock.unlock();

    constexpr double kDefaultQuotaPercentage = 0.9;
    const uint64_t total_space =
        static_cast<uint64_t>(space_info.capacity * kDefaultQuotaPercentage);

    std::unique_lock<std::shared_mutex> lock(mutex_);
    total_space_ = total_space;
    used_space_ = used_space_bytes;
    RecalculateAvailableSpaceLocked();
    initialized_.store(true, std::memory_order_release);
    return {};
}

tl::expected<bool, ErrorCode> LocalStorageSpaceManager::HasPhysicalSpace(
    uint64_t required_size) const {
    namespace fs = std::filesystem;

    {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        if (!initialized_.load(std::memory_order_acquire)) {
            LOG(ERROR) << "LocalStorageSpaceManager used before initialization";
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        if (available_space_ < required_size) {
            return false;
        }
        if (storage_root_.empty()) {
            LOG(ERROR) << "Storage root is empty for LocalStorageSpaceManager";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
    }

    std::error_code ec;
    const auto space_info = fs::space(storage_root_, ec);
    if (ec) {
        LOG(ERROR) << "Failed to query filesystem space for " << storage_root_
                   << ": " << ec.message();
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    return space_info.available >= required_size;
}

bool LocalStorageSpaceManager::IsInitialized() const {
    return initialized_.load(std::memory_order_acquire);
}

bool LocalStorageSpaceManager::TryReserve(uint64_t required_size) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (!initialized_.load(std::memory_order_acquire)) {
        LOG(ERROR) << "LocalStorageSpaceManager used before initialization";
        return false;
    }

    if (available_space_ < required_size) {
        return false;
    }

    used_space_ += required_size;
    available_space_ -= required_size;
    return true;
}

void LocalStorageSpaceManager::Release(uint64_t size_to_release) {
    if (size_to_release == 0) {
        return;
    }

    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (!initialized_.load(std::memory_order_acquire)) {
        LOG(ERROR) << "LocalStorageSpaceManager used before initialization";
        return;
    }

    if (size_to_release <= used_space_) {
        used_space_ -= size_to_release;
    } else {
        used_space_ = 0;
    }
    RecalculateAvailableSpaceLocked();
}

uint64_t LocalStorageSpaceManager::AvailableSpace() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return available_space_;
}

void LocalStorageSpaceManager::RecalculateAvailableSpaceLocked() {
    if (total_space_ >= used_space_) {
        available_space_ = total_space_ - used_space_;
    } else {
        available_space_ = 0;
    }
}

BucketIdGenerator::BucketIdGenerator(int64_t start) {
    if (start <= 0) {
        auto cur_time_stamp = time_gen();
        current_id_ = (cur_time_stamp << TIMESTAMP_SHIFT) | SEQUENCE_ID_SHIFT;
    } else {
        current_id_ = start;
    }
}

int64_t BucketIdGenerator::NextId() {
    return current_id_.fetch_add(1, std::memory_order_relaxed) + 1;
}

int64_t BucketIdGenerator::CurrentId() {
    return current_id_.load(std::memory_order_relaxed);
}

BucketStorageBackend::BucketStorageBackend(const BucketStorageConfig& config)
    : storage_path_(config.storage_filepath),
      config_(config),
      space_manager_(storage_path_) {}

BucketStorageBackend::~BucketStorageBackend() { StopDeletionWorker(); }

tl::expected<int64_t, ErrorCode> BucketStorageBackend::BatchOffload(
    const std::unordered_map<std::string, std::vector<Slice>>& batch_object) {
    if (!initialized_.load(std::memory_order_acquire)) {
        LOG(ERROR)
            << "Storage backend is not initialized. Call Init() before use.";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    if (batch_object.empty()) {
        LOG(ERROR) << "batch object is empty";
        return tl::make_unexpected(ErrorCode::INVALID_KEY);
    }

    auto enable_offloading_res = IsEnableOffloading();
    if (!enable_offloading_res) {
        return tl::make_unexpected(enable_offloading_res.error());
    }
    if (!enable_offloading_res.value()) {
        return tl::make_unexpected(ErrorCode::KEYS_ULTRA_LIMIT);
    }
    auto bucket_id = bucket_id_generator_->NextId();
    std::vector<iovec> iovs;
    std::vector<StorageObjectMetadata> metadatas;
    auto build_bucket_result =
        BuildBucket(bucket_id, batch_object, iovs, metadatas);
    if (!build_bucket_result) {
        LOG(ERROR) << "Failed to build bucket with id: " << bucket_id;
        return tl::make_unexpected(build_bucket_result.error());
    }
    auto bucket = build_bucket_result.value();
    auto write_bucket_result = WriteBucket(bucket_id, bucket, iovs);
    if (!write_bucket_result) {
        LOG(ERROR) << "Failed to write bucket with id: " << bucket_id;
        return tl::make_unexpected(write_bucket_result.error());
    }
    SharedMutexLocker lock(&mutex_);
    physical_used_bytes_ += bucket->data_size + bucket->meta_size;
    object_bucket_map_.reserve(object_bucket_map_.size() + bucket->keys.size());
    for (size_t i = 0; i < bucket->keys.size(); ++i) {
        object_bucket_map_.emplace(bucket->keys[i], std::move(metadatas[i]));
    }
    // Initialize valid key count for this bucket (before move!)
    bucket_valid_keys_[bucket_id] = bucket->keys.size();
    buckets_.emplace(bucket_id, std::move(bucket));
    return bucket_id;
}

tl::expected<void, ErrorCode> BucketStorageBackend::BatchLoad(
    const std::unordered_map<std::string, Slice>& batch_object) {
    std::unordered_map<int64_t, std::vector<std::string>> bucket_keys_map;
    std::unordered_map<int64_t, std::vector<StorageObjectMetadata>>
        bucket_key_metas_map;
    {
        SharedMutexLocker lock(&mutex_, shared_lock);
        for (const auto& key_it : batch_object) {
            auto object_bucket_it = object_bucket_map_.find(key_it.first);
            if (object_bucket_it == object_bucket_map_.end()) {
                LOG(ERROR) << "key " << key_it.first << " does not exist";
                return tl::make_unexpected(ErrorCode::INVALID_KEY);
            }
            auto [bucket_keys_it, create_keys_it] =
                bucket_keys_map.try_emplace(object_bucket_it->second.bucket_id);
            bucket_keys_it->second.emplace_back(key_it.first);
            auto [bucket_key_metas_it, create_metas_it] =
                bucket_key_metas_map.try_emplace(
                    object_bucket_it->second.bucket_id);
            bucket_key_metas_it->second.emplace_back(object_bucket_it->second);
        }
    }
    for (const auto& bucket_keys_it : bucket_keys_map) {
        auto result = BatchLoadBucket(
            bucket_keys_it.first, bucket_keys_it.second,
            bucket_key_metas_map.at(bucket_keys_it.first), batch_object);
        if (!result) {
            LOG(ERROR) << "Failed to load bucket " << bucket_keys_it.first;
            return result;
        }
    }
    return {};
}

tl::expected<void, ErrorCode> BucketStorageBackend::GetBucketKeys(
    int64_t bucket_id, std::vector<std::string>& bucket_keys) {
    SharedMutexLocker locker(&mutex_, shared_lock);
    auto bucket_it = buckets_.find(bucket_id);
    if (bucket_it == buckets_.end()) {
        return tl::make_unexpected(ErrorCode::BUCKET_NOT_FOUND);
    }
    for (const auto& key : bucket_it->second->keys) {
        bucket_keys.emplace_back(key);
    }
    return {};
}

tl::expected<void, ErrorCode> BucketStorageBackend::Init() {
    namespace fs = std::filesystem;
    try {
        if (initialized_.load(std::memory_order_acquire)) {
            LOG(ERROR) << "Storage backend already initialized";
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        std::error_code ec;
        if (!fs::exists(storage_path_)) {
            fs::create_directories(storage_path_, ec);
            if (ec) {
                LOG(ERROR) << "Failed to create bucket storage directory: "
                           << storage_path_ << ", error: " << ec.message();
                return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            }
        }
        SharedMutexLocker lock(&mutex_);
        object_bucket_map_.clear();
        buckets_.clear();
        physical_used_bytes_ = 0;
        int64_t max_bucket_id = BucketIdGenerator::INIT_NEW_START_ID;
        for (const auto& entry :
             fs::recursive_directory_iterator(storage_path_)) {
            if (entry.is_regular_file() &&
                entry.path().extension() == BUCKET_METADATA_FILE_SUFFIX) {
                const auto& bucket_id_str = entry.path().stem();
                int64_t bucket_id = std::stoll(bucket_id_str);
                auto [metadata_it, success] = buckets_.try_emplace(
                    bucket_id, std::make_shared<BucketMetadata>());
                if (!success) {
                    LOG(ERROR) << "Failed to load bucket " << bucket_id_str;
                    return tl::make_unexpected(
                        ErrorCode::BUCKET_ALREADY_EXISTS);
                }
                auto load_bucket_metadata_result =
                    LoadBucketMetadata(bucket_id, metadata_it->second);
                if (!load_bucket_metadata_result) {
                    LOG(ERROR)
                        << "Failed to load metadata for bucket: "
                        << bucket_id_str
                        << ", will delete the bucket's data and metadata";

                    auto bucket_data_path_res = GetBucketDataPath(bucket_id);
                    if (bucket_data_path_res) {
                        fs::remove(bucket_data_path_res.value());
                    }

                    auto bucket_meta_path_res =
                        GetBucketMetadataPath(bucket_id);
                    if (bucket_meta_path_res) {
                        fs::remove(bucket_meta_path_res.value());
                    }

                    buckets_.erase(bucket_id);
                    continue;
                }
                auto& meta = *(metadata_it->second);
                if (meta.data_size == 0 || meta.meta_size == 0 ||
                    meta.metadatas.empty() || meta.keys.empty()) {
                    LOG(ERROR) << "Metadata validation failed for bucket: "
                               << bucket_id_str
                               << ", will delete the bucket's data and "
                                  "metadata. Detailed values:";
                    LOG(ERROR) << "  data_size: " << meta.data_size
                               << " (should not be 0)";
                    LOG(ERROR) << "  meta_size: " << meta.meta_size
                               << " (should not be 0)";
                    LOG(ERROR)
                        << "  object_metadata.size(): " << meta.metadatas.size()
                        << " (empty: "
                        << (meta.metadatas.empty() ? "true" : "false") << ")";

                    LOG(ERROR)
                        << "  keys.size(): " << meta.keys.size()
                        << " (empty: " << (meta.keys.empty() ? "true" : "false")
                        << ")";
                    auto bucket_data_path_res = GetBucketDataPath(bucket_id);
                    if (bucket_data_path_res) {
                        fs::remove(bucket_data_path_res.value());
                    }

                    auto bucket_meta_path_res =
                        GetBucketMetadataPath(bucket_id);
                    if (bucket_meta_path_res) {
                        fs::remove(bucket_meta_path_res.value());
                    }

                    buckets_.erase(bucket_id);
                    continue;
                }
                if (bucket_id > max_bucket_id) {
                    max_bucket_id = bucket_id;
                }
                physical_used_bytes_ += metadata_it->second->data_size +
                                        metadata_it->second->meta_size;
                for (size_t i = 0; i < metadata_it->second->keys.size(); i++) {
                    object_bucket_map_.emplace(
                        metadata_it->second->keys[i],
                        StorageObjectMetadata{
                            metadata_it->first,
                            metadata_it->second->metadatas[i].offset,
                            metadata_it->second->metadatas[i].key_size,
                            metadata_it->second->metadatas[i].data_size, ""});
                }
                // Initialize valid key count for recovered bucket
                bucket_valid_keys_[bucket_id] =
                    metadata_it->second->keys.size();
            }
        }

        uint64_t stale_tmp_files_count = 0;
        uint64_t stale_tmp_space_freed = 0;
        for (const auto& entry :
             fs::recursive_directory_iterator(storage_path_)) {
            if (!entry.is_regular_file() ||
                !IsBucketTemporaryFile(entry.path())) {
                continue;
            }

            std::error_code cleanup_ec;
            const uint64_t file_size = entry.file_size(cleanup_ec);
            if (!cleanup_ec && fs::remove(entry.path(), cleanup_ec)) {
                stale_tmp_files_count++;
                stale_tmp_space_freed += file_size;
                LOG(WARNING) << "Removed stale temporary bucket file: "
                             << entry.path().string() << " (size: " << file_size
                             << " bytes)";
            } else if (cleanup_ec) {
                LOG(ERROR) << "Failed to remove stale temporary bucket file: "
                           << entry.path().string()
                           << ", error: " << cleanup_ec.message();
            }
        }

        if (stale_tmp_files_count > 0) {
            LOG(INFO) << "Temporary bucket cleanup completed: removed "
                      << stale_tmp_files_count << " stale file(s), freed "
                      << stale_tmp_space_freed << " bytes";
        }

        // Clean up orphaned bucket files (.bucket files without corresponding
        // .meta files) This handles the crash consistency case where data write
        // succeeded but metadata write failed
        std::unordered_set<int64_t> valid_bucket_ids;
        for (const auto& [id, _] : buckets_) {
            valid_bucket_ids.insert(id);
        }

        uint64_t orphaned_files_count = 0;
        uint64_t orphaned_space_freed = 0;

        for (const auto& entry :
             fs::recursive_directory_iterator(storage_path_)) {
            if (!entry.is_regular_file()) {
                continue;
            }

            std::string extension = entry.path().extension().string();

            // Only process .bucket files
            if (extension != BUCKET_DATA_FILE_SUFFIX) {
                continue;
            }

            // Extract bucket ID from filename (e.g., "12345.bucket" ->
            // "12345")
            auto bucket_id_str = entry.path().stem();
            int64_t bucket_id = std::stoll(bucket_id_str);

            // Check if this bucket has valid metadata
            if (valid_bucket_ids.find(bucket_id) != valid_bucket_ids.end()) {
                // Valid bucket, skip it
                continue;
            }

            // This is an orphaned .bucket file without metadata
            std::error_code cleanup_ec;
            uint64_t file_size = entry.file_size(cleanup_ec);
            if (!cleanup_ec && fs::remove(entry.path(), cleanup_ec)) {
                orphaned_files_count++;
                orphaned_space_freed += file_size;
                LOG(WARNING) << "Removed orphaned bucket file (no metadata): "
                             << entry.path().string() << " (size: " << file_size
                             << " bytes, "
                             << "bucket_id: " << bucket_id << ")";
            } else if (cleanup_ec) {
                LOG(ERROR) << "Failed to remove orphaned bucket file: "
                           << entry.path().string()
                           << ", error: " << cleanup_ec.message();
            }
        }

        if (orphaned_files_count > 0) {
            LOG(INFO) << "Orphan cleanup completed: removed "
                      << orphaned_files_count
                      << " orphaned bucket file(s), freed "
                      << orphaned_space_freed << " bytes";
        }

        auto init_space_result =
            space_manager_.Init(static_cast<uint64_t>(physical_used_bytes_));
        if (!init_space_result) {
            return init_space_result;
        }

        bucket_id_generator_.emplace(max_bucket_id);
        if (max_bucket_id == BucketIdGenerator::INIT_NEW_START_ID) {
            LOG(INFO) << "Initialized BucketIdGenerator with fresh start. "
                         "No existing buckets found; starting from ID: "
                      << bucket_id_generator_->CurrentId();
        } else {
            LOG(INFO) << "Initialized BucketIdGenerator from existing state. "
                      << "Last used bucket ID was " << max_bucket_id;
        }
        StartDeletionWorker();
        initialized_.store(true, std::memory_order_release);
    } catch (const std::exception& e) {
        LOG(ERROR) << "Bucket storage backend initialize error: " << e.what()
                   << std::endl;
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    return {};
}

tl::expected<bool, ErrorCode> BucketStorageBackend::IsExist(
    const std::string& key) {
    SharedMutexLocker lock(&mutex_, shared_lock);
    auto bucket_id_it = object_bucket_map_.find(key);
    if (bucket_id_it != object_bucket_map_.end()) {
        return true;
    }
    return false;
}

tl::expected<bool, ErrorCode> BucketStorageBackend::IsEnableOffloading() {
    auto store_metadata_result = GetStoreMetadata();
    if (!store_metadata_result) {
        LOG(ERROR) << "Failed to get store metadata: "
                   << store_metadata_result.error();
        return tl::make_unexpected(store_metadata_result.error());
    }

    auto physical_capacity_result = CanAcceptAnotherBucket();
    if (!physical_capacity_result) {
        LOG(ERROR) << "Failed to check bucket backend physical capacity: "
                   << physical_capacity_result.error();
        return tl::make_unexpected(physical_capacity_result.error());
    }

    const auto& store_metadata = store_metadata_result.value();
    auto enable_offloading =
        store_metadata.total_keys + config_.bucket_keys_limit <=
            config_.total_keys_limit &&
        store_metadata.total_size + config_.bucket_size_limit <=
            config_.total_size_limit &&
        physical_capacity_result.value();
    return enable_offloading;
}

tl::expected<OffloadMetadata, ErrorCode>
BucketStorageBackend::GetStoreMetadata() {
    SharedMutexLocker lock(&mutex_, shared_lock);
    OffloadMetadata metadata(object_bucket_map_.size(), physical_used_bytes_);
    return metadata;
}

tl::expected<size_t, ErrorCode> BucketStorageBackend::EvictBucket(
    int64_t bucket_id) {
    size_t freed_size = 0;
    size_t key_count = 0;
    uint64_t data_bytes = 0;
    uint64_t meta_bytes = 0;
    uint64_t queued_delete_bytes = 0;

    {
        SharedMutexLocker lock(&mutex_);

        // Find bucket metadata
        auto bucket_it = buckets_.find(bucket_id);
        if (bucket_it == buckets_.end()) {
            LOG(ERROR) << "Bucket " << bucket_id << " not found";
            return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
        }

        auto bucket_meta = bucket_it->second;
        key_count = bucket_meta->keys.size();

        // Known V1 limitation: a deleted key can be rewritten into a newer
        // bucket, but this loop erases its current mapping without checking
        // the bucket ID. NotifyBucketEviction also identifies replicas only
        // by key and tier. This can remove the newer replica and its route.
        // Intentionally deferred: retire V1 after V2 is validated and deployed.
        // Remove all live keys from object_bucket_map_ and calculate freed size
        for (const auto& key : bucket_meta->keys) {
            auto obj_it = object_bucket_map_.find(key);
            if (obj_it == object_bucket_map_.end()) {
                continue;
            }
            freed_size += obj_it->second.data_size;
            object_bucket_map_.erase(obj_it);
        }

        data_bytes = static_cast<uint64_t>(bucket_meta->data_size);
        meta_bytes = static_cast<uint64_t>(bucket_meta->meta_size);
        queued_delete_bytes = data_bytes + meta_bytes;

        buckets_.erase(bucket_it);
        bucket_valid_keys_.erase(bucket_id);
    }

    auto data_path_res = GetBucketDataPath(bucket_id);
    if (!data_path_res) {
        LOG(ERROR) << "Failed to build data path for bucket " << bucket_id
                   << ": " << data_path_res.error();
        return tl::make_unexpected(data_path_res.error());
    }

    auto meta_path_res = GetBucketMetadataPath(bucket_id);
    if (!meta_path_res) {
        LOG(ERROR) << "Failed to build metadata path for bucket " << bucket_id
                   << ": " << meta_path_res.error();
        return tl::make_unexpected(meta_path_res.error());
    }

    EnqueueBucketDeletion(PendingBucketDeletion{
        bucket_id, data_bytes, meta_bytes, queued_delete_bytes,
        std::move(data_path_res.value()), std::move(meta_path_res.value())});

    LOG(INFO) << "Evicted bucket " << bucket_id << " with " << key_count
              << " keys, released " << freed_size
              << " live bytes and scheduled async file deletion";

    return freed_size;
}

tl::expected<void, ErrorCode> BucketStorageBackend::MarkKeyDeleted(
    const std::string& key) {
    SharedMutexLocker lock(&mutex_);

    // Known V1 limitation: a delayed Free of an old buffer can erase a newer
    // mapping for the same key because no expected bucket ID is supplied.
    // Intentionally deferred with V1 retirement after V2 validation/deployment.
    // Find which bucket this key belongs to
    auto obj_it = object_bucket_map_.find(key);
    if (obj_it == object_bucket_map_.end()) {
        // Key not found, nothing to do
        return {};
    }

    int64_t bucket_id = obj_it->second.bucket_id;

    // Decrement valid key count for this bucket
    auto bucket_valid_it = bucket_valid_keys_.find(bucket_id);
    if (bucket_valid_it != bucket_valid_keys_.end() &&
        bucket_valid_it->second > 0) {
        bucket_valid_it->second--;
    }

    // Remove from object_bucket_map_. Physical bytes remain accounted until
    // async bucket deletion unlinks the underlying files.
    object_bucket_map_.erase(obj_it);

    return {};
}

tl::expected<bool, ErrorCode> BucketStorageBackend::CanAcceptAnotherBucket()
    const {
    const uint64_t pending_bytes =
        pending_deletion_bytes_.load(std::memory_order_acquire);
    const uint64_t pending_count =
        pending_deletion_count_.load(std::memory_order_acquire);
    const uint64_t backlog_limit = MaxPendingDeletionBytes();
    if (pending_bytes > backlog_limit) {
        LOG(WARNING) << "Bucket deletion backlog is too large: pending_bytes="
                     << pending_bytes << ", pending_count=" << pending_count
                     << ", backlog_limit=" << backlog_limit;
        return false;
    }

    const uint64_t required_space =
        static_cast<uint64_t>(config_.bucket_size_limit);
    if (!space_manager_.IsInitialized()) {
        std::error_code ec;
        const auto space_info = std::filesystem::space(storage_path_, ec);
        if (ec) {
            LOG(ERROR) << "Failed to query bucket backend disk space for "
                       << storage_path_ << ": " << ec.message();
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        return space_info.available >= required_space;
    }

    auto physical_space_result =
        space_manager_.HasPhysicalSpace(required_space);
    if (!physical_space_result) {
        return tl::make_unexpected(physical_space_result.error());
    }

    if (!physical_space_result.value()) {
        LOG(WARNING) << "Insufficient physical disk space for next bucket: "
                     << "available_quota=" << space_manager_.AvailableSpace()
                     << ", required=" << required_space
                     << ", pending_delete_bytes=" << pending_bytes
                     << ", pending_delete_count=" << pending_count;
        return false;
    }

    return true;
}

uint64_t BucketStorageBackend::MaxPendingDeletionBytes() const {
    constexpr uint64_t kMaxPendingDeletionBuckets = 8;
    const uint64_t bucket_bytes =
        static_cast<uint64_t>(config_.bucket_size_limit);
    return bucket_bytes * kMaxPendingDeletionBuckets;
}

void BucketStorageBackend::StartDeletionWorker() {
    std::lock_guard<std::mutex> lock(deletion_mutex_);
    if (deletion_thread_.joinable()) {
        return;
    }

    stop_deletion_worker_ = false;
    deletion_thread_ =
        std::thread(&BucketStorageBackend::BucketDeletionWorker, this);
}

void BucketStorageBackend::StopDeletionWorker() {
    {
        std::lock_guard<std::mutex> lock(deletion_mutex_);
        stop_deletion_worker_ = true;
    }
    deletion_cv_.notify_all();
    if (deletion_thread_.joinable()) {
        deletion_thread_.join();
    }
}

void BucketStorageBackend::EnqueueBucketDeletion(PendingBucketDeletion task) {
    const uint64_t queued_bytes = task.queued_bytes;
    // Known V1 race: the worker can consume this task after the queue unlock
    // but before the counters are incremented. Its decrement can transiently
    // underflow the counters and incorrectly block offload as deletion backlog.
    // Intentionally deferred: retire V1 after V2 is validated and deployed.
    {
        std::lock_guard<std::mutex> lock(deletion_mutex_);
        pending_bucket_deletions_.push_back(std::move(task));
    }
    pending_deletion_count_.fetch_add(1, std::memory_order_acq_rel);
    pending_deletion_bytes_.fetch_add(queued_bytes, std::memory_order_acq_rel);
    deletion_cv_.notify_one();
}

void BucketStorageBackend::BucketDeletionWorker() {
    namespace fs = std::filesystem;

    for (;;) {
        PendingBucketDeletion task;
        {
            std::unique_lock<std::mutex> lock(deletion_mutex_);
            deletion_cv_.wait(lock, [this] {
                return stop_deletion_worker_ ||
                       !pending_bucket_deletions_.empty();
            });

            if (stop_deletion_worker_ && pending_bucket_deletions_.empty()) {
                return;
            }

            task = std::move(pending_bucket_deletions_.front());
            pending_bucket_deletions_.pop_front();
        }

        std::error_code ec;
        bool release_data_space = false;
        fs::remove(task.data_path, ec);
        if (ec) {
            if (ec == std::errc::no_such_file_or_directory) {
                release_data_space = true;
            } else {
                LOG(WARNING)
                    << "Failed to remove bucket data file: " << task.data_path
                    << ", bucket_id=" << task.bucket_id
                    << ", error: " << ec.message();
            }
        } else {
            release_data_space = true;
        }

        ec.clear();
        bool release_meta_space = false;
        fs::remove(task.meta_path, ec);
        if (ec) {
            if (ec == std::errc::no_such_file_or_directory) {
                release_meta_space = true;
            } else {
                LOG(WARNING)
                    << "Failed to remove bucket metadata file: "
                    << task.meta_path << ", bucket_id=" << task.bucket_id
                    << ", error: " << ec.message();
            }
        } else {
            release_meta_space = true;
        }

        uint64_t released_bytes = 0;
        if (release_data_space) {
            released_bytes += task.data_bytes;
        }
        if (release_meta_space) {
            released_bytes += task.meta_bytes;
        }
        space_manager_.Release(released_bytes);
        if (released_bytes > 0) {
            SharedMutexLocker lock(&mutex_);
            if (physical_used_bytes_ >= static_cast<int64_t>(released_bytes)) {
                physical_used_bytes_ -= static_cast<int64_t>(released_bytes);
            } else {
                LOG(WARNING)
                    << "Bucket physical accounting underflow on async delete: "
                    << "bucket_id=" << task.bucket_id
                    << ", tracked_bytes=" << physical_used_bytes_
                    << ", released_bytes=" << released_bytes;
                physical_used_bytes_ = 0;
            }
        }

        pending_deletion_count_.fetch_sub(1, std::memory_order_acq_rel);
        pending_deletion_bytes_.fetch_sub(task.queued_bytes,
                                          std::memory_order_acq_rel);
    }
}

tl::expected<std::shared_ptr<BucketMetadata>, ErrorCode>
BucketStorageBackend::BuildBucket(
    int64_t bucket_id,
    const std::unordered_map<std::string, std::vector<Slice>>& batch_object,
    std::vector<iovec>& iovs, std::vector<StorageObjectMetadata>& metadatas) {
    auto bucket = std::make_shared<BucketMetadata>();
    int64_t storage_offset = 0;
    for (const auto& object : batch_object) {
        if (object.second.empty()) {
            LOG(ERROR) << "Failed to create bucket, object is empty";
            return tl::make_unexpected(ErrorCode::INVALID_KEY);
        }
        int64_t object_total_size = 0;
        iovs.emplace_back(
            iovec{const_cast<char*>(object.first.data()), object.first.size()});
        for (const auto& slice : object.second) {
            object_total_size += slice.size;
            iovs.emplace_back(iovec{slice.ptr, slice.size});
        }
        bucket->data_size += object_total_size + object.first.size();
        bucket->metadatas.emplace_back(BucketObjectMetadata{
            storage_offset, static_cast<int64_t>(object.first.size()),
            object_total_size});
        metadatas.emplace_back(StorageObjectMetadata{
            bucket_id, storage_offset,
            static_cast<int64_t>(object.first.size()), object_total_size, ""});
        bucket->keys.push_back(object.first);
        storage_offset += object_total_size + object.first.size();
    }
    return bucket;
}

tl::expected<std::string, ErrorCode>
BucketStorageBackend::SerializeBucketMetadata(
    const std::shared_ptr<BucketMetadata>& bucket_metadata) {
    if (!bucket_metadata) {
        LOG(ERROR) << "Bucket metadata is null";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::string serialized_metadata;
    struct_pb::to_pb(*bucket_metadata, serialized_metadata);
    return serialized_metadata;
}

tl::expected<void, ErrorCode> BucketStorageBackend::WriteBucket(
    int64_t bucket_id, std::shared_ptr<BucketMetadata> bucket_metadata,
    std::vector<iovec>& iovs) {
    namespace fs = std::filesystem;

    auto serialized_metadata_result = SerializeBucketMetadata(bucket_metadata);
    if (!serialized_metadata_result) {
        return tl::make_unexpected(serialized_metadata_result.error());
    }
    std::string serialized_metadata =
        std::move(serialized_metadata_result.value());
    bucket_metadata->meta_size = serialized_metadata.size();

    const uint64_t reserved_size = static_cast<uint64_t>(
        bucket_metadata->data_size + bucket_metadata->meta_size);
    auto physical_space_result = space_manager_.HasPhysicalSpace(reserved_size);
    if (!physical_space_result) {
        return tl::make_unexpected(physical_space_result.error());
    }
    if (!physical_space_result.value()) {
        LOG(ERROR) << "Insufficient physical disk space for bucket write: "
                   << "bucket_id=" << bucket_id
                   << ", required=" << reserved_size
                   << ", available=" << space_manager_.AvailableSpace();
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }
    if (!space_manager_.TryReserve(reserved_size)) {
        LOG(ERROR) << "Failed to reserve bucket write space: bucket_id="
                   << bucket_id << ", required=" << reserved_size
                   << ", available=" << space_manager_.AvailableSpace();
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }

    auto bucket_data_path_res = GetBucketDataPath(bucket_id);
    if (!bucket_data_path_res) {
        LOG(ERROR) << "Failed to get bucket data path, bucket_id=" << bucket_id;
        space_manager_.Release(reserved_size);
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    const std::string bucket_data_path = bucket_data_path_res.value();
    const std::string bucket_data_tmp_path =
        MakeTemporaryBucketPath(bucket_data_path);

    auto bucket_meta_path_res = GetBucketMetadataPath(bucket_id);
    if (!bucket_meta_path_res) {
        LOG(ERROR) << "Failed to get bucket metadata path, bucket_id="
                   << bucket_id;
        space_manager_.Release(reserved_size);
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    const std::string bucket_meta_path = bucket_meta_path_res.value();
    const std::string bucket_meta_tmp_path =
        MakeTemporaryBucketPath(bucket_meta_path);

    auto cleanup_failed_write = [&]() {
        RemoveFileIfExists(bucket_data_tmp_path,
                           "Failed to clean up temporary bucket data file");
        RemoveFileIfExists(bucket_meta_tmp_path,
                           "Failed to clean up temporary bucket metadata file");
        RemoveFileIfExists(bucket_data_path,
                           "Failed to clean up finalized bucket data file");
        RemoveFileIfExists(bucket_meta_path,
                           "Failed to clean up finalized bucket metadata file");
    };

    {
        auto open_file_result = OpenFile(bucket_data_tmp_path, FileMode::Write);
        if (!open_file_result) {
            LOG(ERROR) << "Failed to open file for bucket writing: "
                       << bucket_data_tmp_path;
            cleanup_failed_write();
            space_manager_.Release(reserved_size);
            return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
        }
        auto file = std::move(open_file_result.value());

        auto write_result = file->vector_write(iovs.data(), iovs.size(), 0);
        if (!write_result) {
            LOG(ERROR) << "vector_write failed for: " << bucket_id
                       << ", error: " << write_result.error();
            cleanup_failed_write();
            space_manager_.Release(reserved_size);
            return tl::make_unexpected(write_result.error());
        }
        if (static_cast<int64_t>(write_result.value()) !=
            bucket_metadata->data_size) {
            LOG(ERROR) << "Write size mismatch for: " << bucket_data_tmp_path
                       << ", expected: " << bucket_metadata->data_size
                       << ", got: " << write_result.value();
            cleanup_failed_write();
            space_manager_.Release(reserved_size);
            return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        }
    }

    auto store_bucket_metadata_result =
        StoreBucketMetadata(bucket_meta_tmp_path, serialized_metadata);
    if (!store_bucket_metadata_result) {
        LOG(ERROR) << "Failed to store bucket metadata, error: "
                   << store_bucket_metadata_result.error();
        cleanup_failed_write();
        space_manager_.Release(reserved_size);
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }

    auto rename_data_result =
        RenameBucketFile(bucket_data_tmp_path, bucket_data_path);
    if (!rename_data_result) {
        cleanup_failed_write();
        space_manager_.Release(reserved_size);
        return rename_data_result;
    }

    auto rename_meta_result =
        RenameBucketFile(bucket_meta_tmp_path, bucket_meta_path);
    if (!rename_meta_result) {
        cleanup_failed_write();
        space_manager_.Release(reserved_size);
        return rename_meta_result;
    }
    return {};
}

tl::expected<void, ErrorCode> BucketStorageBackend::StoreBucketMetadata(
    const std::string& metadata_path, const std::string& serialized_metadata) {
    auto open_file_result = OpenFile(metadata_path, FileMode::Write);
    if (!open_file_result) {
        LOG(ERROR) << "Failed to open file for bucket writing: "
                   << metadata_path;
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }
    auto file = std::move(open_file_result.value());
    auto write_result =
        file->write(serialized_metadata, serialized_metadata.size());
    if (!write_result) {
        LOG(ERROR) << "Write failed for: " << metadata_path
                   << ", error: " << write_result.error();
        return tl::make_unexpected(write_result.error());
    }
    if (write_result.value() != serialized_metadata.size()) {
        LOG(ERROR) << "Write size mismatch for: " << metadata_path
                   << ", expected: " << serialized_metadata.size()
                   << ", got: " << write_result.value();
        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
    }
    return {};
}

tl::expected<void, ErrorCode> BucketStorageBackend::LoadBucketMetadata(
    int64_t id, std::shared_ptr<BucketMetadata> metadata) {
    auto meta_path_res = GetBucketMetadataPath(id);
    if (!meta_path_res) {
        LOG(ERROR) << "Failed to get bucket metadata path, bucket_id=" << id;
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    auto meta_path = meta_path_res.value();
    auto open_file_result = OpenFile(meta_path, FileMode::Read);
    if (!open_file_result) {
        LOG(ERROR) << "Failed to open file for reading: " << meta_path;
        return tl::make_unexpected(open_file_result.error());
    }
    auto file = std::move(open_file_result.value());
    std::string str;
    int64_t size = std::filesystem::file_size(meta_path);
    auto read_result = file->read(str, size);
    if (!read_result) {
        LOG(ERROR) << "read failed for: " << meta_path
                   << ", error: " << read_result.error();
        return tl::make_unexpected(read_result.error());
    }
    if (static_cast<int64_t>(read_result.value()) != size) {
        LOG(ERROR) << "Read size mismatch for: " << meta_path
                   << ", expected: " << size
                   << ", got: " << read_result.value();
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }
    try {
        struct_pb::from_pb(*metadata, str);
        metadata->meta_size = size;
    } catch (const std::exception& e) {
        LOG(ERROR) << "Metadata parsing failed with exception: " << e.what();
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    } catch (...) {
        LOG(ERROR) << "Metadata parsing failed with unknown exception";
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }
    return {};
}

tl::expected<void, ErrorCode> BucketStorageBackend::BatchLoadBucket(
    int64_t bucket_id, const std::vector<std::string>& keys,
    const std::vector<StorageObjectMetadata>& metadatas,
    const std::unordered_map<std::string, Slice>& batched_slices) {
    SharedMutexLocker locker(&mutex_, shared_lock);
    auto storage_filepath_res = GetBucketDataPath(bucket_id);
    if (!storage_filepath_res) {
        LOG(ERROR) << "Failed to get bucket data path, bucket_id=" << bucket_id;
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    const auto& storage_filepath = storage_filepath_res.value();
    auto open_file_result = OpenFile(storage_filepath, FileMode::Read);
    if (!open_file_result) {
        LOG(ERROR) << "Failed to open file for reading: " << storage_filepath;
        return tl::make_unexpected(open_file_result.error());
    }
    auto file = std::move(open_file_result.value());
    for (size_t i = 0; i < keys.size(); i++) {
        const auto& key = keys[i];
        int64_t offset;
        const auto& slice = batched_slices.at(key);
        const auto& object_metadata = metadatas[i];
        if (object_metadata.data_size != static_cast<int64_t>(slice.size)) {
            LOG(ERROR) << "Read size mismatch for: " << storage_filepath
                       << ", expected: " << object_metadata.data_size
                       << ", got: " << slice.size;
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
        offset = object_metadata.offset;
        std::vector<iovec> iovs;
        iovs.emplace_back(iovec{slice.ptr, slice.size});
        auto read_result = file->vector_read(
            iovs.data(), static_cast<int>(iovs.size()), offset + key.size());
        if (!read_result) {
            LOG(ERROR) << "vector_read failed for: " << storage_filepath
                       << ", error: " << read_result.error();
            return tl::make_unexpected(read_result.error());
        }
        if (read_result.value() != slice.size) {
            LOG(ERROR) << "Read size mismatch for: " << storage_filepath
                       << ", expected: " << slice.size
                       << ", got: " << read_result.value();
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
    }
    return {};
}

tl::expected<std::string, ErrorCode> BucketStorageBackend::GetBucketDataPath(
    int64_t bucket_id) {
    std::string sep =
        storage_path_.empty() || storage_path_.back() == '/' ? "" : "/";
    return storage_path_ + sep + std::to_string(bucket_id) +
           BUCKET_DATA_FILE_SUFFIX;
}

tl::expected<std::string, ErrorCode>
BucketStorageBackend::GetBucketMetadataPath(int64_t bucket_id) {
    std::string sep =
        storage_path_.empty() || storage_path_.back() == '/' ? "" : "/";
    return storage_path_ + sep + std::to_string(bucket_id) +
           BUCKET_METADATA_FILE_SUFFIX;
}

tl::expected<std::unique_ptr<StorageFile>, ErrorCode>
BucketStorageBackend::OpenFile(const std::string& path, FileMode mode) const {
    int flags = O_CLOEXEC;
    int access_mode = 0;
    switch (mode) {
        case FileMode::Read:
            access_mode = O_RDONLY;
            break;
        case FileMode::Write:
            access_mode = O_WRONLY | O_CREAT | O_TRUNC;
            break;
    }

    int fd = open(path.c_str(), flags | access_mode, 0644);
    if (fd < 0) {
        return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
    }
    return std::make_unique<PosixFile>(path, fd);
}

std::vector<BucketStats> BucketStorageBackend::GetBucketStats() const {
    SharedMutexLocker lock(&mutex_, shared_lock);
    std::vector<BucketStats> stats;
    stats.reserve(buckets_.size());
    for (const auto& [bucket_id, metadata] : buckets_) {
        const auto it = bucket_valid_keys_.find(bucket_id);
        const size_t valid_keys =
            it == bucket_valid_keys_.end() ? 0 : it->second;
        stats.push_back({bucket_id, metadata->keys.size(), valid_keys});
    }
    return stats;
}

}  // namespace mooncake::v1

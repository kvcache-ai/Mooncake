#include "centralized_master_service.h"

#include <cassert>
#include <cstdint>
#include <shared_mutex>
#include <regex>
#include <unordered_set>
#include <ylt/util/tl/expected.hpp>

#include "master_metric_manager.h"
#include "types.h"

namespace mooncake {

CentralizedMasterService::CentralizedMasterService()
    : CentralizedMasterService(MasterServiceConfig()) {}

CentralizedMasterService::CentralizedMasterService(
    const MasterServiceConfig& config)
    : MasterService(config),
      default_kv_lease_ttl_(config.default_kv_lease_ttl),
      default_kv_soft_pin_ttl_(config.default_kv_soft_pin_ttl),
      allow_evict_soft_pinned_objects_(config.allow_evict_soft_pinned_objects),
      eviction_ratio_(config.eviction_ratio),
      eviction_high_watermark_ratio_(config.eviction_high_watermark_ratio),
      enable_offload_(config.enable_offload),
      cluster_id_(config.cluster_id),
      root_fs_dir_(config.root_fs_dir),
      global_file_segment_size_(config.global_file_segment_size),
      enable_disk_eviction_(config.enable_disk_eviction),
      quota_bytes_(config.quota_bytes),
      client_manager_(config.client_live_ttl_sec, config.memory_allocator,
                      [this] { this->ClearInvalidHandles(); }),
      memory_allocator_type_(config.memory_allocator),
      put_start_discard_timeout_sec_(config.put_start_discard_timeout_sec),
      put_start_release_timeout_sec_(config.put_start_release_timeout_sec) {
    if (eviction_ratio_ < 0.0 || eviction_ratio_ > 1.0) {
        LOG(ERROR) << "Eviction ratio must be between 0.0 and 1.0, "
                   << "current value: " << eviction_ratio_;
        throw std::invalid_argument("Invalid eviction ratio");
    }
    if (eviction_high_watermark_ratio_ < 0.0 ||
        eviction_high_watermark_ratio_ > 1.0) {
        LOG(ERROR)
            << "Eviction high watermark ratio must be between 0.0 and 1.0, "
            << "current value: " << eviction_high_watermark_ratio_;
        throw std::invalid_argument("Invalid eviction high watermark ratio");
    }

    if (put_start_release_timeout_sec_ <= put_start_discard_timeout_sec_) {
        LOG(ERROR) << "put_start_release_timeout="
                   << put_start_release_timeout_sec_.count()
                   << " must be larger than put_start_discard_timeout_sec="
                   << put_start_discard_timeout_sec_.count();
        throw std::invalid_argument(
            "put_start_release_timeout must be larger than "
            "put_start_discard_timeout_sec");
    }

    eviction_running_ = true;
    eviction_thread_ =
        std::thread(&CentralizedMasterService::EvictionThreadFunc, this);
    VLOG(1) << "action=start_eviction_thread";

    if (!root_fs_dir_.empty()) {
        use_disk_replica_ = true;
        MasterMetricManager::instance().inc_total_file_capacity(
            global_file_segment_size_);
    }

    client_manager_.Start();
}

CentralizedMasterService::~CentralizedMasterService() {
    // Stop and join the threads
    eviction_running_ = false;
    if (eviction_thread_.joinable()) {
        eviction_thread_.join();
    }
}

auto CentralizedMasterService::MountSegment(const Segment& segment,
                                            const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    ErrorCode err = client_manager_.MountSegment(segment, client_id);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "fail to mount segment"
                   << ", segment_name=" << segment.name
                   << ", client_id=" << client_id << ", ret=" << err;
        return tl::make_unexpected(err);
    }
    return {};
}

auto CentralizedMasterService::ReMountSegment(
    const std::vector<Segment>& segments, const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    ErrorCode err = client_manager_.ReMountSegment(segments, client_id);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "fail to remount segment"
                   << ", client_id=" << client_id << ", ret=" << err;
        return tl::make_unexpected(err);
    }
    return {};
}

void CentralizedMasterService::ClearInvalidHandles() {
    for (auto& shard : metadata_shards_) {
        MutexLocker lock(&shard.mutex);
        auto it = shard.metadata.begin();
        while (it != shard.metadata.end()) {
            if (CleanupStaleHandles(it->second)) {
                // If the object is empty, we need to erase the iterator
                it = shard.metadata.erase(it);
            } else {
                ++it;
            }
        }
    }
}

bool CentralizedMasterService::CleanupStaleHandles(ObjectMetadata& metadata) {
    // Iterate through replicas and remove those with invalid allocators
    auto replica_it = metadata.replicas.begin();
    while (replica_it != metadata.replicas.end()) {
        // Use any_of algorithm to check if any handle has an invalid allocator
        bool has_invalid_mem_handle = replica_it->has_invalid_mem_handle();

        // Remove replicas with invalid handles using erase-remove idiom
        if (has_invalid_mem_handle) {
            replica_it = metadata.replicas.erase(replica_it);
        } else {
            ++replica_it;
        }
    }

    // Return true if no valid replicas remain after cleanup
    return metadata.replicas.empty();
}

auto CentralizedMasterService::PutStart(const UUID& client_id,
                                        const std::string& key,
                                        const uint64_t slice_length,
                                        const ReplicateConfig& config)
    -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode> {
    if (config.replica_num == 0 || key.empty() || slice_length == 0) {
        LOG(ERROR) << "key=" << key << ", replica_num=" << config.replica_num
                   << ", slice_length=" << slice_length
                   << ", key_size=" << key.size() << ", error=invalid_params";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Validate slice lengths
    uint64_t total_length = 0;
    if ((memory_allocator_type_ == BufferAllocatorType::CACHELIB) &&
        (slice_length > kMaxSliceSize)) {
        LOG(ERROR) << "key=" << key << ", slice_length=" << slice_length
                   << ", max_size=" << kMaxSliceSize
                   << ", error=invalid_slice_size";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    total_length += slice_length;

    VLOG(1) << "key=" << key << ", value_length=" << total_length
            << ", slice_length=" << slice_length << ", config=" << config
            << ", action=put_start_begin";

    // Lock the shard and check if object already exists
    size_t shard_idx = getShardIndex(key);
    MutexLocker lock(&metadata_shards_[shard_idx].mutex);

    const auto now = std::chrono::steady_clock::now();
    auto it = metadata_shards_[shard_idx].metadata.find(key);
    if (it != metadata_shards_[shard_idx].metadata.end() &&
        !CleanupStaleHandles(it->second)) {
        auto& metadata = it->second;
        // If the object's PutStart expired and has not completed any
        // replicas, we can discard it and allow the new PutStart to
        // go.
        if (!metadata.HasCompletedReplicas() &&
            metadata.put_start_time + put_start_discard_timeout_sec_ < now) {
            auto replicas = metadata.DiscardProcessingReplicas();
            if (!replicas.empty()) {
                std::lock_guard lock(discarded_replicas_mutex_);
                discarded_replicas_.emplace_back(
                    std::move(replicas),
                    metadata.put_start_time + put_start_release_timeout_sec_);
            }
            metadata_shards_[shard_idx].processing_keys.erase(key);
            metadata_shards_[shard_idx].metadata.erase(it);
        } else {
            LOG(INFO) << "key=" << key << ", info=object_already_exists";
            return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
        }
    }

    // Allocate replicas
    std::vector<Replica> replicas;
    {
        std::vector<std::string> preferred_segments;
        if (!config.preferred_segment.empty()) {
            preferred_segments.push_back(config.preferred_segment);
        }
        auto allocation_result = client_manager_.Allocate(
            slice_length, config.replica_num, preferred_segments);

        if (!allocation_result.has_value()) {
            VLOG(1) << "Failed to allocate all replicas for key=" << key
                    << ", error: " << allocation_result.error();
            if (allocation_result.error() == ErrorCode::INVALID_PARAMS) {
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            need_eviction_ = true;
            return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }

        replicas = std::move(allocation_result.value());
    }

    // If disk replica is enabled, allocate a disk replica
    if (use_disk_replica_) {
        // Allocate a file path for the disk replica
        std::string file_path = ResolvePath(key);
        replicas.emplace_back(file_path, total_length,
                              ReplicaStatus::PROCESSING);
    }

    std::vector<Replica::Descriptor> replica_list;
    replica_list.reserve(replicas.size());
    for (const auto& replica : replicas) {
        replica_list.emplace_back(replica.get_descriptor());
    }

    // No need to set lease here. The object will not be evicted until
    // PutEnd is called.
    metadata_shards_[shard_idx].metadata.emplace(
        std::piecewise_construct, std::forward_as_tuple(key),
        std::forward_as_tuple(client_id, now, total_length, std::move(replicas),
                              config.with_soft_pin));
    // Also insert the metadata into processing set for monitoring.
    metadata_shards_[shard_idx].processing_keys.insert(key);

    return replica_list;
}

auto CentralizedMasterService::PutEnd(const UUID& client_id,
                                      const std::string& key,
                                      ReplicaType replica_type)
    -> tl::expected<void, ErrorCode> {
    MetadataAccessor accessor(this, key);
    if (!accessor.Exists()) {
        LOG(ERROR) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto& metadata = accessor.Get();
    if (client_id != metadata.client_id) {
        LOG(ERROR) << "Illegal client " << client_id << " to PutEnd key " << key
                   << ", was PutStart-ed by " << metadata.client_id;
        return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
    }

    for (auto& replica : metadata.replicas) {
        if (replica.type() == replica_type) {
            replica.mark_complete();
        }
        if (enable_offload_) {
            PushOffloadingQueue(key, replica);
        }
    }

    // If the object is completed, remove it from the processing set.
    if (metadata.IsAllReplicasComplete() && accessor.InProcessing()) {
        accessor.EraseFromProcessing();
    }

    if (replica_type == ReplicaType::MEMORY) {
        MasterMetricManager::instance().inc_mem_cache_nums();
    } else if (replica_type == ReplicaType::DISK) {
        MasterMetricManager::instance().inc_file_cache_nums();
    }
    // 1. Set lease timeout to now, indicating that the object has no lease
    // at beginning. 2. If this object has soft pin enabled, set it to be soft
    // pinned.
    metadata.GrantLease(0, default_kv_soft_pin_ttl_);
    return {};
}

std::vector<tl::expected<void, ErrorCode>>
CentralizedMasterService::BatchPutEnd(const UUID& client_id,
                                      const std::vector<std::string>& keys) {
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(keys.size());
    for (const auto& key : keys) {
        results.emplace_back(PutEnd(client_id, key, ReplicaType::MEMORY));
    }
    return results;
}

auto CentralizedMasterService::PutRevoke(const UUID& client_id,
                                         const std::string& key,
                                         ReplicaType replica_type)
    -> tl::expected<void, ErrorCode> {
    MetadataAccessor accessor(this, key);
    if (!accessor.Exists()) {
        LOG(INFO) << "key=" << key << ", info=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto& metadata = accessor.Get();
    if (client_id != metadata.client_id) {
        LOG(ERROR) << "Illegal client " << client_id << " to PutRevoke key "
                   << key << ", was PutStart-ed by " << metadata.client_id;
        return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
    }

    if (auto status = metadata.HasDiffRepStatus(ReplicaStatus::PROCESSING,
                                                replica_type)) {
        LOG(ERROR) << "key=" << key << ", status=" << *status
                   << ", error=invalid_replica_status";
        return tl::make_unexpected(ErrorCode::INVALID_WRITE);
    }

    if (replica_type == ReplicaType::MEMORY) {
        MasterMetricManager::instance().dec_mem_cache_nums();
    } else if (replica_type == ReplicaType::DISK) {
        MasterMetricManager::instance().dec_file_cache_nums();
    }

    metadata.EraseReplica(replica_type);

    // If the object is completed, remove it from the processing set.
    if (metadata.IsAllReplicasComplete() && accessor.InProcessing()) {
        accessor.EraseFromProcessing();
    }

    if (metadata.IsValid() == false) {
        accessor.Erase();
    }
    return {};
}

std::vector<tl::expected<void, ErrorCode>>
CentralizedMasterService::BatchPutRevoke(const UUID& client_id,
                                         const std::vector<std::string>& keys) {
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(keys.size());
    for (const auto& key : keys) {
        results.emplace_back(PutRevoke(client_id, key, ReplicaType::MEMORY));
    }
    return results;
}

auto CentralizedMasterService::AddReplica(const UUID& client_id,
                                          const std::string& key,
                                          Replica& replica)
    -> tl::expected<void, ErrorCode> {
    MetadataAccessor accessor(this, key);
    if (!accessor.Exists()) {
        LOG(ERROR) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    auto& metadata = accessor.Get();
    if (replica.type() != ReplicaType::LOCAL_DISK) {
        LOG(ERROR) << "Invalid replica type: " << replica.type()
                   << ". Expected ReplicaType::LOCAL_DISK.";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    bool update = false;
    for (size_t i = 0; i < metadata.replicas.size(); ++i) {
        if (metadata.replicas[i].type() == ReplicaType::LOCAL_DISK) {
            auto& descriptor = metadata.replicas[i]
                                   .get_descriptor()
                                   .get_local_disk_descriptor();
            if (descriptor.client_id == client_id) {
                update = true;
                descriptor.transport_endpoint = replica.get_descriptor()
                                                    .get_local_disk_descriptor()
                                                    .transport_endpoint;
                descriptor.object_size = replica.get_descriptor()
                                             .get_local_disk_descriptor()
                                             .object_size;
                break;
            }
        }
    }
    if (!update) {
        metadata.replicas.emplace_back(std::move(replica));
    }
    return {};
}

tl::expected<std::string, ErrorCode>
CentralizedMasterService::GetFsdir() const {
    if (root_fs_dir_.empty() || cluster_id_.empty()) {
        LOG(INFO)
            << "Storage root directory or cluster ID is not set. persisting "
               "data is disabled.";
        return std::string();
    }
    return root_fs_dir_ + "/" + cluster_id_;
}

tl::expected<GetStorageConfigResponse, ErrorCode>
CentralizedMasterService::GetStorageConfig() const {
    if (root_fs_dir_.empty() || cluster_id_.empty()) {
        LOG(INFO)
            << "Storage root directory or cluster ID is not set. persisting "
               "data is disabled.";
        return GetStorageConfigResponse("", enable_disk_eviction_,
                                        quota_bytes_);
    }
    std::string fsdir = root_fs_dir_ + "/" + cluster_id_;
    return GetStorageConfigResponse(fsdir, enable_disk_eviction_, quota_bytes_);
}

auto CentralizedMasterService::MountLocalDiskSegment(const UUID& client_id,
                                                     bool enable_offloading)
    -> tl::expected<void, ErrorCode> {
    if (!enable_offload_) {
        LOG(ERROR) << "	The offload functionality is not enabled";
        return tl::make_unexpected(ErrorCode::UNABLE_OFFLOAD);
    }

    auto err =
        client_manager_.MountLocalDiskSegment(client_id, enable_offloading);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "failed to mount local disk segment"
                      ", client_id="
                   << client_id << ", ret=" << err;
        return tl::make_unexpected(err);
    }
    return {};
}

auto CentralizedMasterService::OffloadObjectHeartbeat(const UUID& client_id,
                                                      bool enable_offloading)
    -> tl::expected<std::unordered_map<std::string, int64_t>, ErrorCode> {
    auto res =
        client_manager_.OffloadObjectHeartbeat(client_id, enable_offloading);
    if (!res.has_value()) {
        LOG(ERROR) << "failed to offload object heartbeat"
                      ", client_id="
                   << client_id << ", ret=" << res.error();
    }
    return res;
}

auto CentralizedMasterService::NotifyOffloadSuccess(
    const UUID& client_id, const std::vector<std::string>& keys,
    const std::vector<StorageObjectMetadata>& metadatas)
    -> tl::expected<void, ErrorCode> {
    for (size_t i = 0; i < keys.size(); ++i) {
        const auto& key = keys[i];
        const auto& metadata = metadatas[i];
        Replica replica(client_id, metadata.data_size,
                        metadata.transport_endpoint, ReplicaStatus::COMPLETE);
        auto res = AddReplica(client_id, key, replica);
        if (!res && res.error() != ErrorCode::OBJECT_NOT_FOUND) {
            LOG(ERROR) << "Failed to add replica: error=" << res.error()
                       << ", client_id=" << client_id << ", key=" << key;
            return tl::make_unexpected(res.error());
        }
    }
    return {};
}

tl::expected<void, ErrorCode> CentralizedMasterService::PushOffloadingQueue(
    const std::string& key, const Replica& replica) {
    const auto& segment_names = replica.get_segment_names();
    if (segment_names.empty()) {
        return {};
    }
    for (const auto& segment_name_it : segment_names) {
        if (!segment_name_it.has_value()) {
            continue;
        }
        const int64_t size = replica.get_descriptor()
                                 .get_memory_descriptor()
                                 .buffer_descriptor.size_;
        auto err = client_manager_.PushOffloadingQueue(key, size,
                                                       segment_name_it.value());
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "failed to push offloading queue"
                       << ", key=" << key << ", size=" << size
                       << ", segment_name=" << segment_name_it.value()
                       << ", ret=" << err;
            return tl::make_unexpected(err);
        }
    }
    return {};
}

void CentralizedMasterService::EvictionThreadFunc() {
    VLOG(1) << "action=eviction_thread_started";

    while (eviction_running_) {
        double used_ratio =
            MasterMetricManager::instance().get_global_mem_used_ratio();
        if (used_ratio > eviction_high_watermark_ratio_ ||
            (need_eviction_ && eviction_ratio_ > 0.0)) {
            double evict_ratio_target = std::max(
                eviction_ratio_,
                used_ratio - eviction_high_watermark_ratio_ + eviction_ratio_);
            double evict_ratio_lowerbound =
                std::max(evict_ratio_target * 0.5,
                         used_ratio - eviction_high_watermark_ratio_);
            BatchEvict(evict_ratio_target, evict_ratio_lowerbound);
        }

        std::this_thread::sleep_for(
            std::chrono::milliseconds(kEvictionThreadSleepMs));
    }

    VLOG(1) << "action=eviction_thread_stopped";
}

void CentralizedMasterService::DiscardExpiredProcessingKeys(
    MetadataShard& shard, const std::chrono::steady_clock::time_point& now) {
    std::list<DiscardedReplicas> discarded_replicas;

    for (auto key_it = shard.processing_keys.begin();
         key_it != shard.processing_keys.end();) {
        auto it = shard.metadata.find(*key_it);
        if (it == shard.metadata.end()) {
            // The key has been removed from metadata. This should be
            // impossible.
            LOG(ERROR) << "Key " << *key_it
                       << " was removed while in processing";
            key_it = shard.processing_keys.erase(key_it);
            continue;
        }

        auto& metadata = it->second;
        // If the object is not valid or not in processing state, just
        // remove it from the processing set.
        if (!metadata.IsValid() || metadata.IsAllReplicasComplete()) {
            if (!metadata.IsValid()) {
                shard.metadata.erase(it);
            }
            key_it = shard.processing_keys.erase(key_it);
            continue;
        }

        // If the object's PutStart timedout, discard and release it's
        // space. Note that instead of releasing the space directly, we
        // insert the replicas into the discarded list so that the
        // discarding and releasing operations can be recorded in
        // statistics.
        const auto ttl =
            metadata.put_start_time + put_start_release_timeout_sec_;
        if (ttl < now) {
            auto replicas = metadata.DiscardProcessingReplicas();
            if (!replicas.empty()) {
                discarded_replicas.emplace_back(std::move(replicas), ttl);
            }

            if (!metadata.IsValid()) {
                // All replicas of this object are discarded, just
                // remove the whole object.
                shard.metadata.erase(it);
            }

            key_it = shard.processing_keys.erase(key_it);
            continue;
        }

        key_it++;
    }

    if (!discarded_replicas.empty()) {
        std::lock_guard lock(discarded_replicas_mutex_);
        discarded_replicas_.splice(discarded_replicas_.end(),
                                   std::move(discarded_replicas));
    }
}

uint64_t CentralizedMasterService::ReleaseExpiredDiscardedReplicas(
    const std::chrono::steady_clock::time_point& now) {
    uint64_t released_cnt = 0;
    std::lock_guard lock(discarded_replicas_mutex_);
    discarded_replicas_.remove_if(
        [&now, &released_cnt](const DiscardedReplicas& item) {
            const bool expired = item.isExpired(now);
            if (expired && item.memSize() > 0) {
                released_cnt++;
            }
            return expired;
        });
    return released_cnt;
}

void CentralizedMasterService::BatchEvict(double evict_ratio_target,
                                          double evict_ratio_lowerbound) {
    if (evict_ratio_target < evict_ratio_lowerbound) {
        LOG(ERROR) << "evict_ratio_target=" << evict_ratio_target
                   << ", evict_ratio_lowerbound=" << evict_ratio_lowerbound
                   << ", error=invalid_params";
        evict_ratio_lowerbound = evict_ratio_target;
    }

    auto now = std::chrono::steady_clock::now();
    long evicted_count = 0;
    long object_count = 0;
    uint64_t total_freed_size = 0;

    // Candidates for second pass eviction
    std::vector<std::chrono::steady_clock::time_point> no_pin_objects;
    std::vector<std::chrono::steady_clock::time_point> soft_pin_objects;

    // Randomly select a starting shard to avoid imbalance eviction between
    // shards. No need to use expensive random_device here.
    size_t start_idx = rand() % metadata_shards_.size();

    // First pass: evict objects without soft pin and lease expired
    for (size_t i = 0; i < metadata_shards_.size(); i++) {
        auto& shard =
            metadata_shards_[(start_idx + i) % metadata_shards_.size()];
        MutexLocker lock(&shard.mutex);

        // Discard expired processing keys first so that they won't be counted
        // in later evictions.
        DiscardExpiredProcessingKeys(shard, now);

        // object_count must be updated at beginning as it will be used later
        // to compute ideal_evict_num
        object_count += shard.metadata.size();

        // To achieve evicted_count / object_count = evict_ratio_target,
        // ideally how many object should be evicted in this shard
        const long ideal_evict_num =
            std::ceil(object_count * evict_ratio_target) - evicted_count;

        std::vector<std::chrono::steady_clock::time_point>
            candidates;  // can be removed
        for (auto it = shard.metadata.begin(); it != shard.metadata.end();
             it++) {
            // Skip objects that are not expired or have incomplete replicas
            if (!it->second.IsLeaseExpired(now) ||
                it->second.HasDiffRepStatus(ReplicaStatus::COMPLETE,
                                            ReplicaType::MEMORY)) {
                continue;
            }
            if (!it->second.IsSoftPinned(now)) {
                if (ideal_evict_num > 0) {
                    // first pass candidates
                    candidates.push_back(it->second.lease_timeout);
                } else {
                    // No need to evict any object in this shard, put to
                    // second pass candidates
                    no_pin_objects.push_back(it->second.lease_timeout);
                }
            } else if (allow_evict_soft_pinned_objects_) {
                // second pass candidates, only if
                // allow_evict_soft_pinned_objects_ is true
                soft_pin_objects.push_back(it->second.lease_timeout);
            }
        }

        if (ideal_evict_num > 0 && !candidates.empty()) {
            long evict_num = std::min(ideal_evict_num, (long)candidates.size());
            long shard_evicted_count =
                0;  // number of objects evicted from this shard
            std::nth_element(candidates.begin(),
                             candidates.begin() + (evict_num - 1),
                             candidates.end());
            auto target_timeout = candidates[evict_num - 1];
            // Evict objects with lease timeout less than or equal to target.
            auto it = shard.metadata.begin();
            while (it != shard.metadata.end()) {
                // Skip objects that are not allowed to be evicted in the first
                // pass
                if (!it->second.IsLeaseExpired(now) ||
                    it->second.IsSoftPinned(now) ||
                    it->second.HasDiffRepStatus(ReplicaStatus::COMPLETE,
                                                ReplicaType::MEMORY) ||
                    !it->second.HasMemReplica()) {
                    ++it;
                    continue;
                }
                if (it->second.lease_timeout <= target_timeout) {
                    // Evict this object
                    total_freed_size +=
                        it->second.size * it->second.GetMemReplicaCount();
                    it->second.EraseReplica(
                        ReplicaType::MEMORY);  // Erase memory replicas
                    if (it->second.IsValid() == false) {
                        it = shard.metadata.erase(it);
                    } else {
                        ++it;
                    }
                    shard_evicted_count++;
                } else {
                    // second pass candidates
                    no_pin_objects.push_back(it->second.lease_timeout);
                    ++it;
                }
            }
            evicted_count += shard_evicted_count;
        }
    }

    // Try releasing discarded replicas before we decide whether to do the
    // second pass.
    uint64_t released_discarded_cnt = ReleaseExpiredDiscardedReplicas(now);

    // The ideal number of objects to evict in the second pass
    long target_evict_num = std::ceil(object_count * evict_ratio_lowerbound) -
                            evicted_count - released_discarded_cnt;
    // The actual number of objects we can evict in the second pass
    target_evict_num =
        std::min(target_evict_num,
                 (long)no_pin_objects.size() + (long)soft_pin_objects.size());

    // Do second pass eviction only if 1). there are candidates that can be
    // evicted AND 2). The evicted number in the first pass is less than
    // evict_ratio_lowerbound.
    if (target_evict_num > 0) {
        // If 1). there are enough candidates without soft pin OR 2). soft pin
        // candidates are empty, then do second pass A. Otherwise, do second
        // pass B. Note that the second condition is ensured implicitly by the
        // calculation of target_evict_num.
        if (target_evict_num <= static_cast<long>(no_pin_objects.size())) {
            // Second pass A: only evict objects without soft pin. The following
            // code is error-prone if target_evict_num > no_pin_objects.size().

            std::nth_element(no_pin_objects.begin(),
                             no_pin_objects.begin() + (target_evict_num - 1),
                             no_pin_objects.end());
            auto target_timeout = no_pin_objects[target_evict_num - 1];

            // Evict objects with lease timeout less than or equal to target.
            // Stop when the target is reached.
            for (size_t i = 0;
                 i < metadata_shards_.size() && target_evict_num > 0; i++) {
                auto& shard =
                    metadata_shards_[(start_idx + i) % metadata_shards_.size()];
                MutexLocker lock(&shard.mutex);
                auto it = shard.metadata.begin();
                while (it != shard.metadata.end() && target_evict_num > 0) {
                    if (it->second.lease_timeout <= target_timeout &&
                        !it->second.IsSoftPinned(now) &&
                        !it->second.HasDiffRepStatus(ReplicaStatus::COMPLETE,
                                                     ReplicaType::MEMORY) &&
                        it->second.HasMemReplica()) {
                        // Evict this object
                        total_freed_size +=
                            it->second.size * it->second.GetMemReplicaCount();
                        it->second.EraseReplica(
                            ReplicaType::MEMORY);  // Erase memory replicas
                        if (it->second.IsValid() == false) {
                            it = shard.metadata.erase(it);
                        } else {
                            ++it;
                        }
                        evicted_count++;
                        target_evict_num--;
                    } else {
                        ++it;
                    }
                }
            }
        } else if (!soft_pin_objects.empty()) {
            // Second pass B: Prioritize evicting objects without soft pin, but
            // also allow to evict soft pinned objects. The following code is
            // error-prone if the soft pin objects are empty.

            const long soft_pin_evict_num =
                target_evict_num - static_cast<long>(no_pin_objects.size());
            // For soft pin objects, prioritize to evict the ones with smaller
            // lease timeout.
            std::nth_element(
                soft_pin_objects.begin(),
                soft_pin_objects.begin() + (soft_pin_evict_num - 1),
                soft_pin_objects.end());
            auto soft_target_timeout = soft_pin_objects[soft_pin_evict_num - 1];

            // Stop when the target is reached.
            for (size_t i = 0;
                 i < metadata_shards_.size() && target_evict_num > 0; i++) {
                auto& shard =
                    metadata_shards_[(start_idx + i) % metadata_shards_.size()];
                MutexLocker lock(&shard.mutex);

                auto it = shard.metadata.begin();
                while (it != shard.metadata.end() && target_evict_num > 0) {
                    // Skip objects that are not expired or have incomplete
                    // replicas
                    if (!it->second.IsLeaseExpired(now) ||
                        it->second.HasDiffRepStatus(ReplicaStatus::COMPLETE,
                                                    ReplicaType::MEMORY) ||
                        !it->second.HasMemReplica()) {
                        ++it;
                        continue;
                    }
                    // Evict objects with 1). no soft pin OR 2). with soft pin
                    // and lease timeout less than or equal to target.
                    if (!it->second.IsSoftPinned(now) ||
                        it->second.lease_timeout <= soft_target_timeout) {
                        total_freed_size +=
                            it->second.size * it->second.GetMemReplicaCount();
                        it->second.EraseReplica(
                            ReplicaType::MEMORY);  // Erase memory replicas
                        if (it->second.IsValid() == false) {
                            it = shard.metadata.erase(it);
                        } else {
                            ++it;
                        }
                        evicted_count++;
                        target_evict_num--;
                    } else {
                        ++it;
                    }
                }
            }
        } else {
            // This should not happen.
            LOG(ERROR) << "Error in second pass eviction: target_evict_num="
                       << target_evict_num
                       << ", no_pin_objects.size()=" << no_pin_objects.size()
                       << ", soft_pin_objects.size()="
                       << soft_pin_objects.size()
                       << ", evicted_count=" << evicted_count
                       << ", object_count=" << object_count
                       << ", evict_ratio_target=" << evict_ratio_target
                       << ", evict_ratio_lowerbound=" << evict_ratio_lowerbound;
        }
    }

    if (evicted_count > 0 || released_discarded_cnt > 0) {
        need_eviction_ = false;
        MasterMetricManager::instance().inc_eviction_success(evicted_count,
                                                             total_freed_size);
    } else {
        if (object_count == 0) {
            // No objects to evict, no need to check again
            need_eviction_ = false;
        }
        MasterMetricManager::instance().inc_eviction_fail();
    }
    VLOG(1) << "action=evict_objects" << ", evicted_count=" << evicted_count
            << ", total_freed_size=" << total_freed_size;
}

std::string CentralizedMasterService::SanitizeKey(
    const std::string& key) const {
    // Set of invalid filesystem characters to be replaced
    constexpr std::string_view kInvalidChars = "/\\:*?\"<>|";
    std::string sanitized_key;
    sanitized_key.reserve(key.size());

    for (char c : key) {
        // Replace invalid characters with underscore
        sanitized_key.push_back(
            kInvalidChars.find(c) != std::string_view::npos ? '_' : c);
    }
    return sanitized_key;
}

std::string CentralizedMasterService::ResolvePath(
    const std::string& key) const {
    // Compute hash of the key
    size_t hash = std::hash<std::string>{}(key);

    // Use low 8 bits to create 2-level directory structure (e.g. "a1/b2")
    char dir1 =
        static_cast<char>('a' + (hash & 0x0F));  // Lower 4 bits -> 16 dirs
    char dir2 = static_cast<char>(
        'a' + ((hash >> 4) & 0x0F));  // Next 4 bits -> 16 subdirs

    // Safely construct path using std::filesystem
    namespace fs = std::filesystem;
    fs::path dir_path = fs::path(std::string(1, dir1)) / std::string(1, dir2);

    // Combine directory path with sanitized filename
    fs::path full_path =
        fs::path(root_fs_dir_) / cluster_id_ / dir_path / SanitizeKey(key);

    return full_path.lexically_normal().string();
}

}  // namespace mooncake

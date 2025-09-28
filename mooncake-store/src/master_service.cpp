#include "master_service.h"

#include <cassert>
#include <cstdint>
#include <shared_mutex>
#include <regex>
#include <ylt/util/tl/expected.hpp>

#include "master_metric_manager.h"
#include "segment.h"
#include "types.h"

namespace mooncake {

MasterService::MasterService() : MasterService(MasterServiceConfig()) {}

MasterService::MasterService(const MasterServiceConfig& config)
    : default_kv_lease_ttl_(config.default_kv_lease_ttl),
      default_kv_soft_pin_ttl_(config.default_kv_soft_pin_ttl),
      allow_evict_soft_pinned_objects_(config.allow_evict_soft_pinned_objects),
      eviction_ratio_(config.eviction_ratio),
      eviction_high_watermark_ratio_(config.eviction_high_watermark_ratio),
      client_live_ttl_sec_(config.client_live_ttl_sec),
      enable_ha_(config.enable_ha),
      cluster_id_(config.cluster_id),
      root_fs_dir_(config.root_fs_dir),
      segment_manager_(config.memory_allocator),
      allocation_strategy_(std::make_shared<RandomAllocationStrategy>()) {
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

    eviction_running_ = true;
    eviction_thread_ = std::thread(&MasterService::EvictionThreadFunc, this);
    VLOG(1) << "action=start_eviction_thread";

    // Start client monitor thread in all modes so TTL/heartbeat works
    client_monitor_running_ = true;
    client_monitor_thread_ =
        std::thread(&MasterService::ClientMonitorFunc, this);
    VLOG(1) << "action=start_client_monitor_thread";

    if (!root_fs_dir_.empty()) {
        use_disk_replica_ = true;
    }
}

MasterService::~MasterService() {
    // Stop and join the threads
    eviction_running_ = false;
    client_monitor_running_ = false;
    if (eviction_thread_.joinable()) {
        eviction_thread_.join();
    }
    if (client_monitor_thread_.joinable()) {
        client_monitor_thread_.join();
    }
}

auto MasterService::MountSegment(const Segment& segment, const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();

    // Tell the client monitor thread to start timing for this client. To
    // avoid the following undesired situations, this message must be sent
    // after locking the segment mutex and before the mounting operation
    // completes:
    // 1. Sending the message before the lock: the client expires and
    // unmouting invokes before this mounting are completed, which prevents
    // this segment being able to be unmounted forever;
    // 2. Sending the message after mounting the segment: After mounting
    // this segment, when trying to push id to the queue, the queue is
    // already full. However, at this point, the message must be sent,
    // otherwise this client cannot be monitored and expired.
    {
        PodUUID pod_client_id;
        pod_client_id.first = client_id.first;
        pod_client_id.second = client_id.second;
        if (!client_ping_queue_.push(pod_client_id)) {
            LOG(ERROR) << "segment_name=" << segment.name
                       << ", error=client_ping_queue_full";
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
    }

    auto err = segment_access.MountSegment(segment, client_id);
    if (err == ErrorCode::SEGMENT_ALREADY_EXISTS) {
        // Return OK because this is an idempotent operation
        return {};
    } else if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return {};
}

auto MasterService::ReMountSegment(const std::vector<Segment>& segments,
                                   const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    std::unique_lock<std::shared_mutex> lock(client_mutex_);
    if (ok_client_.contains(client_id)) {
        LOG(WARNING) << "client_id=" << client_id
                     << ", warn=client_already_remounted";
        // Return OK because this is an idempotent operation
        return {};
    }

    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();

    // Tell the client monitor thread to start timing for this client. To
    // avoid the following undesired situations, this message must be sent
    // after locking the segment mutex or client mutex and before the remounting
    // operation completes:
    // 1. Sending the message before the lock: the client expires and
    // unmouting invokes before this remounting are completed, which prevents
    // this segment being able to be unmounted forever;
    // 2. Sending the message after remounting the segments: After remounting
    // these segments, when trying to push id to the queue, the queue is
    // already full. However, at this point, the message must be sent,
    // otherwise this client cannot be monitored and expired.
    PodUUID pod_client_id;
    pod_client_id.first = client_id.first;
    pod_client_id.second = client_id.second;
    if (!client_ping_queue_.push(pod_client_id)) {
        LOG(ERROR) << "client_id=" << client_id
                   << ", error=client_ping_queue_full";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    ErrorCode err = segment_access.ReMountSegment(segments, client_id);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }

    // Change the client status to OK
    ok_client_.insert(client_id);
    MasterMetricManager::instance().inc_active_clients();

    return {};
}

void MasterService::ClearInvalidHandles() {
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

auto MasterService::UnmountSegment(const UUID& segment_id,
                                   const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    size_t metrics_dec_capacity = 0;  // to update the metrics

    // 1. Prepare to unmount the segment by deleting its allocator
    {
        ScopedSegmentAccess segment_access =
            segment_manager_.getSegmentAccess();
        ErrorCode err = segment_access.PrepareUnmountSegment(
            segment_id, metrics_dec_capacity);
        if (err == ErrorCode::SEGMENT_NOT_FOUND) {
            // Return OK because this is an idempotent operation
            return {};
        }
        if (err != ErrorCode::OK) {
            return tl::make_unexpected(err);
        }
    }  // Release the segment mutex before long-running step 2 and avoid
       // deadlocks

    // 2. Remove the metadata of the related objects
    ClearInvalidHandles();

    // 3. Commit the unmount operation
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();
    auto err = segment_access.CommitUnmountSegment(segment_id, client_id,
                                                   metrics_dec_capacity);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return {};
}

auto MasterService::ExistKey(const std::string& key)
    -> tl::expected<bool, ErrorCode> {
    MetadataAccessor accessor(this, key);
    if (!accessor.Exists()) {
        VLOG(1) << "key=" << key << ", info=object_not_found";
        return false;
    }

    auto& metadata = accessor.Get();
    for (const auto& replica : metadata.replicas) {
        if (replica.status() == ReplicaStatus::COMPLETE) {
            // Grant a lease to the object as it may be further used by the
            // client.
            metadata.GrantLease(default_kv_lease_ttl_,
                                default_kv_soft_pin_ttl_);
            return true;
        }
    }

    return false;  // If no complete replica is found, return false
}

std::vector<tl::expected<bool, ErrorCode>> MasterService::BatchExistKey(
    const std::vector<std::string>& keys) {
    std::vector<tl::expected<bool, ErrorCode>> results;
    results.reserve(keys.size());
    for (const auto& key : keys) {
        results.emplace_back(ExistKey(key));
    }
    return results;
}

auto MasterService::GetAllKeys()
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    std::vector<std::string> all_keys;
    for (size_t i = 0; i < kNumShards; i++) {
        MutexLocker lock(&metadata_shards_[i].mutex);
        for (const auto& item : metadata_shards_[i].metadata) {
            all_keys.push_back(item.first);
        }
    }
    return all_keys;
}

auto MasterService::GetAllSegments()
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();
    std::vector<std::string> all_segments;
    auto err = segment_access.GetAllSegments(all_segments);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return all_segments;
}

auto MasterService::QuerySegments(const std::string& segment)
    -> tl::expected<std::pair<size_t, size_t>, ErrorCode> {
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();
    size_t used, capacity;
    auto err = segment_access.QuerySegments(segment, used, capacity);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return std::make_pair(used, capacity);
}

auto MasterService::GetReplicaListByRegex(const std::string& regex_pattern)
    -> tl::expected<
        std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
        ErrorCode> {
    std::unordered_map<std::string, std::vector<Replica::Descriptor>> results;
    std::regex pattern;

    try {
        pattern = std::regex(regex_pattern, std::regex::ECMAScript);
    } catch (const std::regex_error& e) {
        LOG(ERROR) << "Invalid regex pattern: " << regex_pattern
                   << ", error: " << e.what();
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    for (size_t i = 0; i < kNumShards; ++i) {
        MutexLocker lock(&metadata_shards_[i].mutex);

        for (auto& [key, metadata] : metadata_shards_[i].metadata) {
            if (std::regex_search(key, pattern)) {
                std::vector<Replica::Descriptor> replica_list;
                replica_list.reserve(metadata.replicas.size());
                for (const auto& replica : metadata.replicas) {
                    if (replica.status() == ReplicaStatus::COMPLETE) {
                        replica_list.emplace_back(replica.get_descriptor());
                    }
                }
                if (replica_list.empty()) {
                    LOG(WARNING)
                        << "key=" << key
                        << " matched by regex, but has no complete replicas.";
                    continue;
                }

                results.emplace(key, std::move(replica_list));
                metadata.GrantLease(default_kv_lease_ttl_,
                                    default_kv_soft_pin_ttl_);
            }
        }
    }

    return results;
}

auto MasterService::GetReplicaList(std::string_view key)
    -> tl::expected<GetReplicaListResponse, ErrorCode> {
    MetadataAccessor accessor(this, std::string(key));
    if (!accessor.Exists()) {
        VLOG(1) << "key=" << key << ", info=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    auto& metadata = accessor.Get();

    std::vector<Replica::Descriptor> replica_list;
    replica_list.reserve(metadata.replicas.size());
    for (const auto& replica : metadata.replicas) {
        if (replica.status() == ReplicaStatus::COMPLETE) {
            replica_list.emplace_back(replica.get_descriptor());
        }
    }

    if (replica_list.empty()) {
        LOG(WARNING) << "key=" << key << ", error=replica_not_ready";
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }

    // Grant a lease to the object so it will not be removed
    // when the client is reading it.
    metadata.GrantLease(default_kv_lease_ttl_, default_kv_soft_pin_ttl_);

    return GetReplicaListResponse(std::move(replica_list),
                                  default_kv_lease_ttl_);
}

auto MasterService::PutStart(const std::string& key,
                             const std::vector<uint64_t>& slice_lengths,
                             const ReplicateConfig& config)
    -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode> {
    if (config.replica_num == 0 || key.empty() || slice_lengths.empty()) {
        LOG(ERROR) << "key=" << key << ", replica_num=" << config.replica_num
                   << ", slice_count=" << slice_lengths.size()
                   << ", key_size=" << key.size() << ", error=invalid_params";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Validate slice lengths
    uint64_t total_length = 0;
    for (size_t i = 0; i < slice_lengths.size(); ++i) {
        if (slice_lengths[i] > kMaxSliceSize) {
            LOG(ERROR) << "key=" << key << ", slice_index=" << i
                       << ", slice_size=" << slice_lengths[i]
                       << ", max_size=" << kMaxSliceSize
                       << ", error=invalid_slice_size";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        total_length += slice_lengths[i];
    }

    VLOG(1) << "key=" << key << ", value_length=" << total_length
            << ", slice_count=" << slice_lengths.size() << ", config=" << config
            << ", action=put_start_begin";

    // Lock the shard and check if object already exists
    size_t shard_idx = getShardIndex(key);
    MutexLocker lock(&metadata_shards_[shard_idx].mutex);

    auto it = metadata_shards_[shard_idx].metadata.find(key);
    if (it != metadata_shards_[shard_idx].metadata.end() &&
        !CleanupStaleHandles(it->second)) {
        LOG(INFO) << "key=" << key << ", info=object_already_exists";
        return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
    }

    // Allocate replicas
    std::vector<Replica> replicas;
    {
        ScopedAllocatorAccess allocator_access =
            segment_manager_.getAllocatorAccess();
        auto& allocators = allocator_access.getAllocators();
        auto& allocators_by_name = allocator_access.getAllocatorsByName();

        auto allocation_result = allocation_strategy_->Allocate(
            allocators, allocators_by_name, slice_lengths, config);

        if (!allocation_result.has_value()) {
            LOG(ERROR) << "Failed to allocate all replicas for key=" << key
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
        std::forward_as_tuple(total_length, std::move(replicas),
                              config.with_soft_pin));
    return replica_list;
}

auto MasterService::PutEnd(const std::string& key, ReplicaType replica_type)
    -> tl::expected<void, ErrorCode> {
    MetadataAccessor accessor(this, key);
    if (!accessor.Exists()) {
        LOG(ERROR) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto& metadata = accessor.Get();
    for (auto& replica : metadata.replicas) {
        if (replica.type() == replica_type) {
            replica.mark_complete();
        }
    }
    // 1. Set lease timeout to now, indicating that the object has no lease
    // at beginning. 2. If this object has soft pin enabled, set it to be soft
    // pinned.
    metadata.GrantLease(0, default_kv_soft_pin_ttl_);
    return {};
}

auto MasterService::PutRevoke(const std::string& key, ReplicaType replica_type)
    -> tl::expected<void, ErrorCode> {
    MetadataAccessor accessor(this, key);
    if (!accessor.Exists()) {
        LOG(INFO) << "key=" << key << ", info=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto& metadata = accessor.Get();
    if (auto status = metadata.HasDiffRepStatus(ReplicaStatus::PROCESSING,
                                                replica_type)) {
        LOG(ERROR) << "key=" << key << ", status=" << *status
                   << ", error=invalid_replica_status";
        return tl::make_unexpected(ErrorCode::INVALID_WRITE);
    }
    metadata.EraseReplica(replica_type);
    if (metadata.IsValid() == false) {
        accessor.Erase();
    }
    return {};
}

std::vector<tl::expected<void, ErrorCode>> MasterService::BatchPutEnd(
    const std::vector<std::string>& keys) {
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(keys.size());
    for (const auto& key : keys) {
        results.emplace_back(PutEnd(key, ReplicaType::MEMORY));
    }
    return results;
}

std::vector<tl::expected<void, ErrorCode>> MasterService::BatchPutRevoke(
    const std::vector<std::string>& keys) {
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(keys.size());
    for (const auto& key : keys) {
        results.emplace_back(PutRevoke(key, ReplicaType::MEMORY));
    }
    return results;
}

auto MasterService::Remove(const std::string& key)
    -> tl::expected<void, ErrorCode> {
    MetadataAccessor accessor(this, key);
    if (!accessor.Exists()) {
        VLOG(1) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto& metadata = accessor.Get();

    if (!metadata.IsLeaseExpired()) {
        VLOG(1) << "key=" << key << ", error=object_has_lease";
        return tl::make_unexpected(ErrorCode::OBJECT_HAS_LEASE);
    }

    if (!metadata.IsAllReplicasComplete()) {
        LOG(ERROR) << "key=" << key << ", error=replica_not_ready";
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }

    // Remove object metadata
    accessor.Erase();
    return {};
}

auto MasterService::RemoveByRegex(const std::string& regex_pattern)
    -> tl::expected<long, ErrorCode> {
    long removed_count = 0;
    std::regex pattern;

    try {
        pattern = std::regex(regex_pattern, std::regex::ECMAScript);
    } catch (const std::regex_error& e) {
        LOG(ERROR) << "Invalid regex pattern: " << regex_pattern
                   << ", error: " << e.what();
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    for (size_t i = 0; i < kNumShards; ++i) {
        MutexLocker lock(&metadata_shards_[i].mutex);

        for (auto it = metadata_shards_[i].metadata.begin();
             it != metadata_shards_[i].metadata.end();) {
            if (std::regex_search(it->first, pattern)) {
                if (!it->second.IsLeaseExpired()) {
                    VLOG(1) << "key=" << it->first
                            << " matched by regex, but has lease. Skipping "
                            << "removal.";
                    ++it;
                    continue;
                }
                if (!it->second.IsAllReplicasComplete()) {
                    LOG(WARNING) << "key=" << it->first
                                 << " matched by regex, but not all replicas "
                                    "are complete. Skipping removal.";
                    ++it;
                    continue;
                }

                VLOG(1) << "key=" << it->first
                        << " matched by regex. Removing.";
                it = metadata_shards_[i].metadata.erase(it);
                removed_count++;
            } else {
                ++it;
            }
        }
    }

    VLOG(1) << "action=remove_by_regex, pattern=" << regex_pattern
            << ", removed_count=" << removed_count;
    return removed_count;
}

long MasterService::RemoveAll() {
    long removed_count = 0;
    uint64_t total_freed_size = 0;
    // Store the current time to avoid repeatedly
    // calling std::chrono::steady_clock::now()
    auto now = std::chrono::steady_clock::now();

    for (auto& shard : metadata_shards_) {
        MutexLocker lock(&shard.mutex);
        if (shard.metadata.empty()) {
            continue;
        }

        // Only remove objects with expired leases
        auto it = shard.metadata.begin();
        while (it != shard.metadata.end()) {
            if (it->second.IsLeaseExpired(now)) {
                total_freed_size +=
                    it->second.size * it->second.GetMemReplicaCount();
                it = shard.metadata.erase(it);
                removed_count++;
            } else {
                ++it;
            }
        }
    }

    VLOG(1) << "action=remove_all_objects"
            << ", removed_count=" << removed_count
            << ", total_freed_size=" << total_freed_size;
    return removed_count;
}

bool MasterService::CleanupStaleHandles(ObjectMetadata& metadata) {
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

size_t MasterService::GetKeyCount() const {
    size_t total = 0;
    for (const auto& shard : metadata_shards_) {
        MutexLocker lock(&shard.mutex);
        total += shard.metadata.size();
    }
    return total;
}

auto MasterService::Ping(const UUID& client_id)
    -> tl::expected<PingResponse, ErrorCode> {
    std::shared_lock<std::shared_mutex> lock(client_mutex_);
    ClientStatus client_status;
    auto it = ok_client_.find(client_id);
    if (it != ok_client_.end()) {
        client_status = ClientStatus::OK;
    } else {
        client_status = ClientStatus::NEED_REMOUNT;
    }
    PodUUID pod_client_id = {client_id.first, client_id.second};
    if (!client_ping_queue_.push(pod_client_id)) {
        // Queue is full
        LOG(ERROR) << "client_id=" << client_id
                   << ", error=client_ping_queue_full";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return PingResponse(view_version_, client_status);
}

tl::expected<std::string, ErrorCode> MasterService::GetFsdir() const {
    if (root_fs_dir_.empty() || cluster_id_.empty()) {
        LOG(INFO)
            << "Storage root directory or cluster ID is not set. persisting "
               "data is disabled.";
        return std::string();
    }
    return root_fs_dir_ + "/" + cluster_id_;
}

void MasterService::EvictionThreadFunc() {
    VLOG(1) << "action=eviction_thread_started";

    while (eviction_running_) {
        double used_ratio =
            MasterMetricManager::instance().get_global_used_ratio();
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

void MasterService::BatchEvict(double evict_ratio_target,
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

    // The ideal number of objects to evict in the second pass
    long target_evict_num =
        std::ceil(object_count * evict_ratio_lowerbound) - evicted_count;
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

    if (evicted_count > 0) {
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

void MasterService::ClientMonitorFunc() {
    std::unordered_map<UUID, std::chrono::steady_clock::time_point,
                       boost::hash<UUID>>
        client_ttl;
    while (client_monitor_running_) {
        auto now = std::chrono::steady_clock::now();

        // Update the client ttl
        PodUUID pod_client_id;
        while (client_ping_queue_.pop(pod_client_id)) {
            UUID client_id = {pod_client_id.first, pod_client_id.second};
            client_ttl[client_id] =
                now + std::chrono::seconds(client_live_ttl_sec_);
        }

        // Find out expired clients
        std::vector<UUID> expired_clients;
        for (auto it = client_ttl.begin(); it != client_ttl.end();) {
            if (it->second < now) {
                LOG(INFO) << "client_id=" << it->first
                          << ", action=client_expired";
                expired_clients.push_back(it->first);
                it = client_ttl.erase(it);
            } else {
                ++it;
            }
        }

        // Update the client status to NEED_REMOUNT
        if (!expired_clients.empty()) {
            // Record which segments are unmounted, will be used in the commit
            // phase.
            std::vector<UUID> unmount_segments;
            std::vector<size_t> dec_capacities;
            std::vector<UUID> client_ids;
            std::vector<std::string> segment_names;
            {
                // Lock client_mutex and segment_mutex
                std::unique_lock<std::shared_mutex> lock(client_mutex_);
                for (auto& client_id : expired_clients) {
                    auto it = ok_client_.find(client_id);
                    if (it != ok_client_.end()) {
                        ok_client_.erase(it);
                        MasterMetricManager::instance().dec_active_clients();
                    }
                }

                ScopedSegmentAccess segment_access =
                    segment_manager_.getSegmentAccess();
                for (auto& client_id : expired_clients) {
                    std::vector<Segment> segments;
                    segment_access.GetClientSegments(client_id, segments);
                    for (auto& seg : segments) {
                        size_t metrics_dec_capacity = 0;
                        if (segment_access.PrepareUnmountSegment(
                                seg.id, metrics_dec_capacity) ==
                            ErrorCode::OK) {
                            unmount_segments.push_back(seg.id);
                            dec_capacities.push_back(metrics_dec_capacity);
                            client_ids.push_back(client_id);
                            segment_names.push_back(seg.name);
                        } else {
                            LOG(ERROR) << "client_id=" << client_id
                                       << ", segment_name=" << seg.name
                                       << ", "
                                          "error=prepare_unmount_expired_"
                                          "segment_failed";
                        }
                    }
                }
            }  // Release the mutex before long-running ClearInvalidHandles and
               // avoid deadlocks

            if (!unmount_segments.empty()) {
                ClearInvalidHandles();

                ScopedSegmentAccess segment_access =
                    segment_manager_.getSegmentAccess();
                for (size_t i = 0; i < unmount_segments.size(); i++) {
                    segment_access.CommitUnmountSegment(
                        unmount_segments[i], client_ids[i], dec_capacities[i]);
                    LOG(INFO) << "client_id=" << client_ids[i]
                              << ", segment_name=" << segment_names[i]
                              << ", action=unmount_expired_segment";
                }
            }
        }

        std::this_thread::sleep_for(
            std::chrono::milliseconds(kClientMonitorSleepMs));
    }
}

std::string MasterService::SanitizeKey(const std::string& key) const {
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

std::string MasterService::ResolvePath(const std::string& key) const {
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

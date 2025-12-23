#include "master_service.h"

#include <cassert>
#include <cstdint>
#include <shared_mutex>
#include <regex>
#include <unordered_set>
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
      enable_offload_(config.enable_offload),
      cluster_id_(config.cluster_id),
      root_fs_dir_(config.root_fs_dir),
      global_file_segment_size_(config.global_file_segment_size),
      enable_disk_eviction_(config.enable_disk_eviction),
      quota_bytes_(config.quota_bytes),
      segment_manager_(config.memory_allocator, config.enable_cxl),
      memory_allocator_type_(config.memory_allocator),
      allocation_strategy_(std::make_shared<RandomAllocationStrategy>()),
      put_start_discard_timeout_sec_(config.put_start_discard_timeout_sec),
      put_start_release_timeout_sec_(config.put_start_release_timeout_sec),
      task_manager_(config.task_manager_config),
      cxl_path_(config.cxl_path),
      cxl_size_(config.cxl_size),
      enable_cxl_(config.enable_cxl) {
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
    eviction_thread_ = std::thread(&MasterService::EvictionThreadFunc, this);
    VLOG(1) << "action=start_eviction_thread";

    // Start client monitor thread in all modes so TTL/heartbeat works
    client_monitor_running_ = true;
    client_monitor_thread_ =
        std::thread(&MasterService::ClientMonitorFunc, this);
    VLOG(1) << "action=start_client_monitor_thread";

    // Start task cleanup thread
    task_cleanup_running_ = true;
    task_cleanup_thread_ =
        std::thread(&MasterService::TaskCleanupThreadFunc, this);
    VLOG(1) << "action=start_task_cleanup_thread";

    if (!root_fs_dir_.empty()) {
        use_disk_replica_ = true;
        MasterMetricManager::instance().inc_total_file_capacity(
            global_file_segment_size_);
    }
    if (enable_cxl_) {
        allocation_strategy_ = std::make_shared<CxlAllocationStrategy>();
        segment_manager_.initializeCxlAllocator(cxl_path_, cxl_size_);
        VLOG(1) << "action=start_cxl_global_allocator";
    } else {
        allocation_strategy_ = std::make_shared<RandomAllocationStrategy>();
    }
}

MasterService::~MasterService() {
    // Stop and join the threads
    eviction_running_ = false;
    client_monitor_running_ = false;
    task_cleanup_running_ = false;

    // Wake sleepers so join() doesn't block for long sleep intervals.
    task_cleanup_cv_.notify_all();

    if (eviction_thread_.joinable()) {
        eviction_thread_.join();
    }
    if (client_monitor_thread_.joinable()) {
        client_monitor_thread_.join();
    }
    if (task_cleanup_thread_.joinable()) {
        task_cleanup_thread_.join();
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

    LOG(INFO) << "client_id=" << client_id
              << ", action=mount_segment, segment_name=" << segment.name;

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
    for (size_t i = 0; i < kNumShards; i++) {
        MetadataShardAccessorRW shard(this, i);
        auto it = shard->metadata.begin();
        while (it != shard->metadata.end()) {
            if (CleanupStaleHandles(it->second)) {
                // If the object is empty, we need to erase the iterator and
                // also erase the key from processing_keys and
                // replication_tasks.
                shard->processing_keys.erase(it->first);
                shard->replication_tasks.erase(it->first);
                it = shard->metadata.erase(it);
            } else {
                ++it;
            }
        }
    }
}

void MasterService::TaskCleanupThreadFunc() {
    LOG(INFO) << "Task cleanup thread started";
    while (task_cleanup_running_) {
        // Wait for the next cleanup interval, but allow fast shutdown.
        {
            std::unique_lock<std::mutex> lk(task_cleanup_mutex_);
            task_cleanup_cv_.wait_for(
                lk, std::chrono::milliseconds(kTaskCleanupThreadSleepMs),
                [&] { return !task_cleanup_running_.load(); });
        }

        if (!task_cleanup_running_) {
            break;
        }

        auto write_access = task_manager_.get_write_access();
        write_access.prune_expired_tasks();
        write_access.prune_finished_tasks();
    }
    LOG(INFO) << "Task cleanup thread stopped";
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
    MetadataAccessorRO accessor(this, key);
    if (!accessor.Exists()) {
        VLOG(1) << "key=" << key << ", info=object_not_found";
        return false;
    }

    const auto& metadata = accessor.Get();
    if (metadata.HasReplica(&Replica::fn_is_completed)) {
        // Grant a lease to the object as it may be further used by the
        // client.
        metadata.GrantLease(default_kv_lease_ttl_, default_kv_soft_pin_ttl_);
        return true;
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
        MetadataShardAccessorRO shard(this, i);
        for (const auto& item : shard->metadata) {
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

auto MasterService::QueryIp(const UUID& client_id)
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();
    std::vector<Segment> segments;
    ErrorCode err = segment_access.GetClientSegments(client_id, segments);
    if (err != ErrorCode::OK) {
        if (err == ErrorCode::SEGMENT_NOT_FOUND) {
            VLOG(1) << "QueryIp: client_id=" << client_id
                    << " not found or has no segments";
            return tl::make_unexpected(ErrorCode::CLIENT_NOT_FOUND);
        }

        LOG(ERROR) << "QueryIp: failed to get segments for client_id="
                   << client_id << ", error=" << toString(err);

        return tl::make_unexpected(err);
    }

    std::unordered_set<std::string> unique_ips;
    unique_ips.reserve(segments.size());
    for (const auto& segment : segments) {
        if (!segment.te_endpoint.empty()) {
            size_t colon_pos = segment.te_endpoint.find(':');
            if (colon_pos != std::string::npos) {
                std::string ip = segment.te_endpoint.substr(0, colon_pos);
                unique_ips.emplace(ip);
            } else {
                unique_ips.emplace(segment.te_endpoint);
            }
        }
    }

    if (unique_ips.empty()) {
        LOG(WARNING) << "QueryIp: client_id=" << client_id
                     << " has no valid IP addresses";
        return {};
    }
    std::vector<std::string> result(unique_ips.begin(), unique_ips.end());
    return result;
}

auto MasterService::BatchQueryIp(const std::vector<UUID>& client_ids)
    -> tl::expected<
        std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
        ErrorCode> {
    std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>
        results;
    results.reserve(client_ids.size());
    for (const auto& client_id : client_ids) {
        auto ip_result = QueryIp(client_id);
        if (ip_result.has_value()) {
            results.emplace(client_id, std::move(ip_result.value()));
        }
    }
    return results;
}

auto MasterService::BatchReplicaClear(
    const std::vector<std::string>& object_keys, const UUID& client_id,
    const std::string& segment_name)
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    std::vector<std::string> cleared_keys;
    cleared_keys.reserve(object_keys.size());
    const bool clear_all_segments = segment_name.empty();

    for (const auto& key : object_keys) {
        if (key.empty()) {
            LOG(WARNING) << "BatchReplicaClear: empty key, skipping";
            continue;
        }
        MetadataAccessorRW accessor(this, key);
        if (!accessor.Exists()) {
            LOG(WARNING) << "BatchReplicaClear: key=" << key
                         << " not found, skipping";
            continue;
        }

        auto& metadata = accessor.Get();

        // Security check: Ensure the requesting client owns the object.
        if (metadata.client_id != client_id) {
            LOG(WARNING) << "BatchReplicaClear: key=" << key
                         << " belongs to different client_id="
                         << metadata.client_id << ", expected=" << client_id
                         << ", skipping";
            continue;
        }

        // Safety check: Do not clear an object that has an active lease.
        if (!metadata.IsLeaseExpired()) {
            LOG(WARNING) << "BatchReplicaClear: key=" << key
                         << " has active lease, skipping";
            continue;
        }

        if (clear_all_segments) {
            // Check if all replicas are complete. Incomplete replicas could
            // indicate an ongoing Put operation, and clearing during this time
            // could lead to an inconsistent state or interfere with the write.
            if (!metadata.AllReplicas(&Replica::fn_is_completed)) {
                LOG(WARNING) << "BatchReplicaClear: key=" << key
                             << " has incomplete replicas, skipping";
                continue;
            }

            metadata.VisitReplicas(
                &Replica::fn_is_completed, [](Replica& replica) {
                    if (replica.is_memory_replica()) {
                        MasterMetricManager::instance().dec_mem_cache_nums();
                    } else if (replica.is_disk_replica()) {
                        MasterMetricManager::instance().dec_file_cache_nums();
                    }
                });

            // Erase the entire metadata (all replicas will be deallocated)
            accessor.Erase();
            cleared_keys.emplace_back(key);
            VLOG(1) << "BatchReplicaClear: successfully cleared all replicas "
                       "for key="
                    << key << " for client_id=" << client_id;
        } else {
            // Clear only replicas on the specified segment_name
            bool has_replica_on_segment = false;
            const auto match_replica_on_segment =
                [&](const Replica& replica) -> bool {
                if (!replica.is_completed()) {
                    return false;
                }
                const auto segment_names = replica.get_segment_names();
                for (const auto& seg_name : segment_names) {
                    if (seg_name.has_value() &&
                        seg_name.value() == segment_name) {
                        return true;
                    }
                }
                return false;
            };

            metadata.VisitReplicas(
                match_replica_on_segment, [&](Replica& replica) {
                    has_replica_on_segment = true;
                    if (replica.is_memory_replica()) {
                        MasterMetricManager::instance().dec_mem_cache_nums();
                    } else if (replica.is_disk_replica()) {
                        MasterMetricManager::instance().dec_file_cache_nums();
                    }
                });

            if (!has_replica_on_segment) {
                LOG(WARNING)
                    << "BatchReplicaClear: key=" << key
                    << " has no replica on segment_name=" << segment_name
                    << ", skipping";
                continue;
            }

            metadata.EraseReplicas(match_replica_on_segment);

            // If no valid replicas remain, erase the entire metadata
            if (!metadata.IsValid()) {
                accessor.Erase();
            }

            cleared_keys.emplace_back(key);
            VLOG(1) << "BatchReplicaClear: successfully cleared replicas on "
                       "segment_name="
                    << segment_name << " for key=" << key
                    << " for client_id=" << client_id;
        }
    }

    return cleared_keys;
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
        MetadataShardAccessorRO shard(this, i);

        for (const auto& [key, metadata] : shard->metadata) {
            if (std::regex_search(key, pattern)) {
                std::vector<Replica::Descriptor> replica_list;
                metadata.VisitReplicas(
                    &Replica::fn_is_completed,
                    [&replica_list](const Replica& replica) {
                        replica_list.emplace_back(replica.get_descriptor());
                    });

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

auto MasterService::GetReplicaList(const std::string& key)
    -> tl::expected<GetReplicaListResponse, ErrorCode> {
    MetadataAccessorRO accessor(this, key);

    MasterMetricManager::instance().inc_total_get_nums();

    if (!accessor.Exists()) {
        VLOG(1) << "key=" << key << ", info=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    const auto& metadata = accessor.Get();

    std::vector<Replica::Descriptor> replica_list;
    metadata.VisitReplicas(
        &Replica::fn_is_completed, [&replica_list](const Replica& replica) {
            replica_list.emplace_back(replica.get_descriptor());
        });

    if (replica_list.empty()) {
        LOG(WARNING) << "key=" << key << ", error=replica_not_ready";
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }

    if (replica_list[0].is_memory_replica()) {
        MasterMetricManager::instance().inc_mem_cache_hit_nums();
    } else if (replica_list[0].is_disk_replica()) {
        MasterMetricManager::instance().inc_file_cache_hit_nums();
    }
    MasterMetricManager::instance().inc_valid_get_nums();
    // Grant a lease to the object so it will not be removed
    // when the client is reading it.
    metadata.GrantLease(default_kv_lease_ttl_, default_kv_soft_pin_ttl_);

    return GetReplicaListResponse(std::move(replica_list),
                                  default_kv_lease_ttl_);
}

auto MasterService::PutStart(const UUID& client_id, const std::string& key,
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
    MetadataShardAccessorRW shard(this, getShardIndex(key));

    const auto now = std::chrono::steady_clock::now();
    auto it = shard->metadata.find(key);
    if (it != shard->metadata.end() && !CleanupStaleHandles(it->second)) {
        auto& metadata = it->second;
        // If the object's PutStart expired and has not completed any
        // replicas, we can discard it and allow the new PutStart to
        // go.
        if (!metadata.HasReplica(&Replica::fn_is_completed) &&
            metadata.put_start_time + put_start_discard_timeout_sec_ < now) {
            auto replicas = metadata.PopReplicas(&Replica::fn_is_processing);
            if (!replicas.empty()) {
                std::lock_guard lock(discarded_replicas_mutex_);
                discarded_replicas_.emplace_back(
                    std::move(replicas),
                    metadata.put_start_time + put_start_release_timeout_sec_);
            }
            shard->processing_keys.erase(key);
            shard->metadata.erase(it);
        } else {
            LOG(INFO) << "key=" << key << ", info=object_already_exists";
            return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
        }
    }

    // Allocate replicas
    std::vector<Replica> replicas;
    {
        ScopedAllocatorAccess allocator_access =
            segment_manager_.getAllocatorAccess();
        const auto& allocator_manager = allocator_access.getAllocatorManager();

        std::vector<std::string> preferred_segments;
        if (!config.preferred_segment.empty()) {
            preferred_segments.push_back(config.preferred_segment);
        } else if (!config.preferred_segments.empty()) {
            preferred_segments = config.preferred_segments;
        }

        auto allocation_result = allocation_strategy_->Allocate(
            allocator_manager, slice_length, config.replica_num,
            preferred_segments);

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
    shard->metadata.emplace(
        std::piecewise_construct, std::forward_as_tuple(key),
        std::forward_as_tuple(client_id, now, total_length, std::move(replicas),
                              config.with_soft_pin));
    // Also insert the metadata into processing set for monitoring.
    shard->processing_keys.insert(key);

    return replica_list;
}

auto MasterService::PutEnd(const UUID& client_id, const std::string& key,
                           ReplicaType replica_type)
    -> tl::expected<void, ErrorCode> {
    MetadataAccessorRW accessor(this, key);
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

    metadata.VisitReplicas(
        [replica_type](const Replica& replica) {
            return replica.type() == replica_type;
        },
        [](Replica& replica) { replica.mark_complete(); });

    if (enable_offload_) {
        metadata.VisitReplicas(&Replica::fn_is_completed,
                               [this, &key](const Replica& replica) {
                                   PushOffloadingQueue(key, replica);
                               });
    }

    // If the object is completed, remove it from the processing set.
    if (metadata.AllReplicas(&Replica::fn_is_completed) &&
        accessor.InProcessing()) {
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

auto MasterService::AddReplica(const UUID& client_id, const std::string& key,
                               Replica& replica)
    -> tl::expected<void, ErrorCode> {
    MetadataAccessorRW accessor(this, key);
    if (!accessor.Exists()) {
        accessor.Create(
            client_id,
            replica.get_descriptor().get_local_disk_descriptor().object_size,
            std::vector<Replica>{}, false);
    }
    auto& metadata = accessor.Get();
    if (replica.type() != ReplicaType::LOCAL_DISK) {
        LOG(ERROR) << "Invalid replica type: " << replica.type()
                   << ". Expected ReplicaType::LOCAL_DISK.";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    if (!metadata.HasReplica(&Replica::fn_is_local_disk_replica)) {
        std::vector<Replica> replicas;
        replicas.emplace_back(std::move(replica));
        metadata.AddReplicas(std::move(replicas));
        return {};
    }

    metadata.VisitReplicas(
        [client_id](const Replica& rep) {
            return rep.type() == ReplicaType::LOCAL_DISK &&
                   rep.get_descriptor().get_local_disk_descriptor().client_id ==
                       client_id;
        },
        [&replica](Replica& rep) {
            rep.get_descriptor()
                .get_local_disk_descriptor()
                .transport_endpoint = replica.get_descriptor()
                                          .get_local_disk_descriptor()
                                          .transport_endpoint;
            rep.get_descriptor().get_local_disk_descriptor().object_size =
                replica.get_descriptor()
                    .get_local_disk_descriptor()
                    .object_size;
        });
    return {};
}

auto MasterService::PutRevoke(const UUID& client_id, const std::string& key,
                              ReplicaType replica_type)
    -> tl::expected<void, ErrorCode> {
    MetadataAccessorRW accessor(this, key);
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

    auto processing_rep =
        metadata.GetFirstReplica([replica_type](const Replica& replica) {
            return replica.type() == replica_type && !replica.is_processing();
        });
    if (processing_rep != nullptr) {
        LOG(ERROR) << "key=" << key << ", status=" << processing_rep->status()
                   << ", error=invalid_replica_status";
        return tl::make_unexpected(ErrorCode::INVALID_WRITE);
    }

    if (replica_type == ReplicaType::MEMORY) {
        MasterMetricManager::instance().dec_mem_cache_nums();
    } else if (replica_type == ReplicaType::DISK) {
        MasterMetricManager::instance().dec_file_cache_nums();
    }

    metadata.EraseReplicas([replica_type](const Replica& replica) {
        return replica.type() == replica_type;
    });

    // If the object is completed, remove it from the processing set.
    if (metadata.AllReplicas(&Replica::fn_is_completed) &&
        accessor.InProcessing()) {
        accessor.EraseFromProcessing();
    }

    if (metadata.IsValid() == false) {
        accessor.Erase();
    }
    return {};
}

std::vector<tl::expected<void, ErrorCode>> MasterService::BatchPutEnd(
    const UUID& client_id, const std::vector<std::string>& keys) {
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(keys.size());
    for (const auto& key : keys) {
        results.emplace_back(PutEnd(client_id, key, ReplicaType::MEMORY));
    }
    return results;
}

std::vector<tl::expected<void, ErrorCode>> MasterService::BatchPutRevoke(
    const UUID& client_id, const std::vector<std::string>& keys) {
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(keys.size());
    for (const auto& key : keys) {
        results.emplace_back(PutRevoke(client_id, key, ReplicaType::MEMORY));
    }
    return results;
}

tl::expected<CopyStartResponse, ErrorCode> MasterService::CopyStart(
    const UUID& client_id, const std::string& key,
    const std::string& src_segment,
    const std::vector<std::string>& tgt_segments) {
    MetadataAccessorRW accessor(this, key);
    if (!accessor.Exists()) {
        LOG(ERROR) << "key=" << key << ", object not found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    if (accessor.HasReplicationTask()) {
        LOG(ERROR) << "key=" << key
                   << " already has an ongoing replication task";
        return tl::make_unexpected(ErrorCode::OBJECT_HAS_REPLICATION_TASK);
    }

    auto& metadata = accessor.Get();
    auto source = metadata.GetReplicaBySegmentName(src_segment);
    if (source == nullptr || !source->is_completed() ||
        source->has_invalid_mem_handle()) {
        LOG(ERROR) << "key=" << key << ", src_segment=" << src_segment
                   << ", replica not found or not valid";
        return tl::make_unexpected(ErrorCode::REPLICA_NOT_FOUND);
    }

    std::vector<Replica> replicas;
    replicas.reserve(tgt_segments.size());
    {
        ScopedAllocatorAccess allocator_access =
            segment_manager_.getAllocatorAccess();
        const auto& allocator_manager = allocator_access.getAllocatorManager();

        for (auto& tgt_segment : tgt_segments) {
            if (metadata.GetReplicaBySegmentName(tgt_segment) != nullptr) {
                // Skip used segments.
                continue;
            }

            auto replica = allocation_strategy_->AllocateFrom(
                allocator_manager, metadata.size, tgt_segment);
            if (!replica.has_value()) {
                LOG(ERROR) << "key=" << key << ", tgt_segment=" << tgt_segment
                           << ", failed to allocate replica";
                return tl::make_unexpected(replica.error());
            }
            replicas.push_back(std::move(*replica));
        }
    }

    CopyStartResponse response;
    response.targets.reserve(replicas.size());
    std::vector<ReplicaID> replica_ids;
    replica_ids.reserve(replicas.size());

    response.source = source->get_descriptor();
    for (const auto& replica : replicas) {
        replica_ids.push_back(replica.id());
        response.targets.emplace_back(replica.get_descriptor());
    }

    // Create replication task for tracking.
    auto& shard = accessor.GetShard();
    shard->replication_tasks.emplace(
        std::piecewise_construct, std::forward_as_tuple(key),
        std::forward_as_tuple(client_id, std::chrono::steady_clock::now(),
                              ReplicationTask::Type::COPY, source->id(),
                              std::move(replica_ids)));

    // Increase source refcnt to protect it from eviction.
    source->inc_refcnt();

    // Add replicas to the object.
    // DO NOT ACCESS source AFTER THIS !!!
    metadata.AddReplicas(std::move(replicas));

    return response;
}

tl::expected<void, ErrorCode> MasterService::CopyEnd(const UUID& client_id,
                                                     const std::string& key) {
    MetadataAccessorRW accessor(this, key);
    if (!accessor.Exists()) {
        LOG(ERROR) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    if (!accessor.HasReplicationTask()) {
        LOG(ERROR) << "key=" << key
                   << ", error=object has no ongoing replication task";
        return tl::make_unexpected(ErrorCode::OBJECT_NO_REPLICATION_TASK);
    }

    auto& task = accessor.GetReplicationTask();
    if (task.client_id != client_id) {
        LOG(ERROR) << "Illegal client " << client_id << " to CopyEnd key "
                   << key << ", was CopyStart-ed by " << task.client_id;
        return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
    }

    if (task.type != ReplicationTask::Type::COPY) {
        LOG(ERROR) << "Ongoing replication task type is MOVE instead of COPY";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto& metadata = accessor.Get();
    auto source_id = task.source_id;
    auto source = metadata.GetReplicaByID(source_id);
    if (source == nullptr || !source->is_completed() ||
        source->has_invalid_mem_handle()) {
        LOG(ERROR) << "key=" << key << ", source_id=" << source_id
                   << ", status=" << (source == nullptr ? "nullptr" : "invalid")
                   << ", copy source becomes invalid during data transfer";
        // Discard target replicas and clear the replication task.
        metadata.EraseReplicas([&task](const Replica& replica) {
            return std::find(task.replica_ids.begin(), task.replica_ids.end(),
                             replica.id()) != task.replica_ids.end();
        });
        accessor.EraseReplicationTask();
        if (!metadata.IsValid()) {
            // Remove the object if it does not have any replicas.
            accessor.Erase();
        }
        return tl::make_unexpected(ErrorCode::REPLICA_IS_GONE);
    }

    // Decrement source reference count
    source->dec_refcnt();

    // Mark all replica_ids as complete
    bool all_complete = true;
    for (const auto& replica_id : task.replica_ids) {
        auto replica = metadata.GetReplicaByID(replica_id);
        if (replica == nullptr || replica->has_invalid_mem_handle()) {
            LOG(WARNING)
                << "key=" << key << ", replica_id=" << replica_id
                << ", copy target becomes invalid during data transfer";
            all_complete = false;
        } else {
            replica->mark_complete();
        }
    }

    accessor.EraseReplicationTask();

    return all_complete ? tl::expected<void, ErrorCode>()
                        : tl::make_unexpected(ErrorCode::REPLICA_IS_GONE);
}

tl::expected<void, ErrorCode> MasterService::CopyRevoke(
    const UUID& client_id, const std::string& key) {
    MetadataAccessorRW accessor(this, key);
    if (!accessor.Exists()) {
        LOG(ERROR) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    if (!accessor.HasReplicationTask()) {
        LOG(ERROR) << "key=" << key
                   << ", error=object has no ongoing replication task";
        return tl::make_unexpected(ErrorCode::OBJECT_NO_REPLICATION_TASK);
    }

    auto& task = accessor.GetReplicationTask();
    if (task.client_id != client_id) {
        LOG(ERROR) << "Illegal client " << client_id << " to CopyRevoke key "
                   << key << ", was CopyStart-ed by " << task.client_id;
        return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
    }

    if (task.type != ReplicationTask::Type::COPY) {
        LOG(ERROR) << "Ongoing replication task type is MOVE instead of COPY";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto& metadata = accessor.Get();
    auto source_id = task.source_id;
    auto source = metadata.GetReplicaByID(source_id);
    if (source == nullptr) {
        LOG(WARNING) << "key=" << key << ", source_id=" << source_id
                     << ", copy source not found during revoke";
    } else {
        // Decrement source reference count
        source->dec_refcnt();
    }

    // Erase all replica_ids
    for (const auto& replica_id : task.replica_ids) {
        metadata.EraseReplicaByID(replica_id);
    }

    accessor.EraseReplicationTask();

    if (!metadata.IsValid()) {
        // Remove the object if it does not have any replicas.
        accessor.Erase();
    }

    return {};
}

tl::expected<MoveStartResponse, ErrorCode> MasterService::MoveStart(
    const UUID& client_id, const std::string& key,
    const std::string& src_segment, const std::string& tgt_segment) {
    if (src_segment == tgt_segment) {
        LOG(ERROR) << "key=" << key << ", move_tgt=" << tgt_segment
                   << " cannot be the same as move_src=" << src_segment;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    MetadataAccessorRW accessor(this, key);
    if (!accessor.Exists()) {
        LOG(ERROR) << "key=" << key << ", object not found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    if (accessor.HasReplicationTask()) {
        LOG(ERROR) << "key=" << key
                   << " already has an ongoing replication task";
        return tl::make_unexpected(ErrorCode::OBJECT_HAS_REPLICATION_TASK);
    }

    auto& metadata = accessor.Get();
    auto source = metadata.GetReplicaBySegmentName(src_segment);
    if (source == nullptr || !source->is_completed() ||
        source->has_invalid_mem_handle()) {
        LOG(ERROR) << "key=" << key << ", src_segment=" << src_segment
                   << ", replica not found or not completed";
        return tl::make_unexpected(ErrorCode::REPLICA_NOT_FOUND);
    }

    std::vector<Replica> replicas;
    if (metadata.GetReplicaBySegmentName(tgt_segment) == nullptr) {
        ScopedAllocatorAccess allocator_access =
            segment_manager_.getAllocatorAccess();
        const auto& allocator_manager = allocator_access.getAllocatorManager();

        auto replica = allocation_strategy_->AllocateFrom(
            allocator_manager, metadata.size, tgt_segment);
        if (!replica.has_value()) {
            LOG(ERROR) << "key=" << key << ", tgt_segment=" << tgt_segment
                       << ", failed to allocate replica";
            return tl::make_unexpected(replica.error());
        }
        replicas.push_back(std::move(*replica));
    }

    MoveStartResponse response;
    std::vector<ReplicaID> replica_ids;

    response.source = source->get_descriptor();
    if (!replicas.empty()) {
        replica_ids.push_back(replicas[0].id());
        response.target = replicas[0].get_descriptor();
    } else {
        response.target = std::nullopt;
    }

    // Create replication task for tracking.
    auto& shard = accessor.GetShard();
    shard->replication_tasks.emplace(
        std::piecewise_construct, std::forward_as_tuple(key),
        std::forward_as_tuple(client_id, std::chrono::steady_clock::now(),
                              ReplicationTask::Type::MOVE, source->id(),
                              std::move(replica_ids)));

    // Increase source refcnt to protect it from eviction.
    source->inc_refcnt();

    // Add replicas to the object.
    // DO NOT ACCESS source AFTER THIS !!!
    metadata.AddReplicas(std::move(replicas));

    return response;
}

tl::expected<void, ErrorCode> MasterService::MoveEnd(const UUID& client_id,
                                                     const std::string& key) {
    MetadataAccessorRW accessor(this, key);
    if (!accessor.Exists()) {
        LOG(ERROR) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    if (!accessor.HasReplicationTask()) {
        LOG(ERROR) << "key=" << key
                   << ", error=object has no ongoing replication task";
        return tl::make_unexpected(ErrorCode::OBJECT_NO_REPLICATION_TASK);
    }

    auto& task = accessor.GetReplicationTask();
    if (task.client_id != client_id) {
        LOG(ERROR) << "Illegal client " << client_id << " to MoveEnd key "
                   << key << ", was MoveStart-ed by " << task.client_id;
        return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
    }

    if (task.type != ReplicationTask::Type::MOVE) {
        LOG(ERROR) << "Ongoing replication task type is COPY instead of MOVE";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto& metadata = accessor.Get();
    auto source_id = task.source_id;
    auto source = metadata.GetReplicaByID(source_id);
    if (source == nullptr || !source->is_completed() ||
        source->has_invalid_mem_handle()) {
        LOG(ERROR) << "key=" << key << ", source_id=" << source_id
                   << ", status=" << (source == nullptr ? "nullptr" : "invalid")
                   << ", move source becomes invalid during data transfer";
        // Discard target replica and clear the replication task.
        metadata.EraseReplicas([&task](const Replica& replica) {
            return std::find(task.replica_ids.begin(), task.replica_ids.end(),
                             replica.id()) != task.replica_ids.end();
        });
        accessor.EraseReplicationTask();
        if (!metadata.IsValid()) {
            // Remove the object if it does not have any replicas.
            accessor.Erase();
        }
        return tl::make_unexpected(ErrorCode::REPLICA_IS_GONE);
    }

    // Decrement source reference count
    source->dec_refcnt();

    // If the move target has already existed on MoveStart, task.replica_ids
    // will be empty. Thus we need to check whether we have replica_ids to
    // process.
    if (!task.replica_ids.empty()) {
        auto replica_id = task.replica_ids[0];
        auto replica = metadata.GetReplicaByID(replica_id);
        if (replica == nullptr || replica->has_invalid_mem_handle()) {
            LOG(WARNING)
                << "key=" << key << ", replica_id=" << replica_id
                << ", move target becomes invalid during data transfer";
            accessor.EraseReplicationTask();
            return tl::make_unexpected(ErrorCode::REPLICA_IS_GONE);
        }

        // Mark replica as complete
        replica->mark_complete();
    }

    // Remove the source replica and release its space later.
    auto source_replica =
        metadata.PopReplicas([&source_id](const Replica& replica) {
            return replica.id() == source_id;
        });
    if (!source_replica.empty()) {
        std::lock_guard lock(discarded_replicas_mutex_);
        discarded_replicas_.emplace_back(
            std::move(source_replica),
            std::chrono::steady_clock::now() + put_start_release_timeout_sec_);
    }

    accessor.EraseReplicationTask();

    return {};
}

tl::expected<void, ErrorCode> MasterService::MoveRevoke(
    const UUID& client_id, const std::string& key) {
    MetadataAccessorRW accessor(this, key);
    if (!accessor.Exists()) {
        LOG(ERROR) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    if (!accessor.HasReplicationTask()) {
        LOG(ERROR) << "key=" << key
                   << ", error=object has no ongoing replication task";
        return tl::make_unexpected(ErrorCode::OBJECT_NO_REPLICATION_TASK);
    }

    auto& task = accessor.GetReplicationTask();
    if (task.client_id != client_id) {
        LOG(ERROR) << "Illegal client " << client_id << " to MoveRevoke key "
                   << key << ", was MoveStart-ed by " << task.client_id;
        return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
    }

    if (task.type != ReplicationTask::Type::MOVE) {
        LOG(ERROR) << "Ongoing replication task type is COPY instead of MOVE";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto& metadata = accessor.Get();
    auto source_id = task.source_id;
    auto source = metadata.GetReplicaByID(source_id);
    if (source == nullptr) {
        LOG(WARNING) << "key=" << key << ", source_id=" << source_id
                     << ", move source not found during revoke";
    } else {
        // Decrement source reference count
        source->dec_refcnt();
    }

    // Erase all replica_ids (in MOVE operation, there should be at most one)
    for (const auto& replica_id : task.replica_ids) {
        metadata.EraseReplicaByID(replica_id);
    }

    accessor.EraseReplicationTask();

    if (!metadata.IsValid()) {
        // Remove the object if it does not have any replicas.
        accessor.Erase();
    }

    return {};
}

auto MasterService::Remove(const std::string& key, bool force)
    -> tl::expected<void, ErrorCode> {
    MetadataAccessorRW accessor(this, key);
    if (!accessor.Exists()) {
        VLOG(1) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto& metadata = accessor.Get();

    if (!force && !metadata.IsLeaseExpired()) {
        VLOG(1) << "key=" << key << ", error=object_has_lease";
        return tl::make_unexpected(ErrorCode::OBJECT_HAS_LEASE);
    }

    /**
     * The reason the force operation here does not bypass the replica
     * check is that put operations (which could also be copy or move)
     * and remove operations might be happening concurrently, making it
     * extremely dangerous to perform a direct removal at this point.
     */
    if (!metadata.AllReplicas(&Replica::fn_is_completed)) {
        LOG(ERROR) << "key=" << key << ", error=replica_not_ready";
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }

    if (accessor.HasReplicationTask()) {
        LOG(ERROR) << "key=" << key << ", error=object_has_replication_task";
        return tl::make_unexpected(ErrorCode::OBJECT_HAS_REPLICATION_TASK);
    }

    // Remove object metadata
    accessor.Erase();
    return {};
}

auto MasterService::RemoveByRegex(const std::string& regex_pattern, bool force)
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
        MetadataShardAccessorRW shard(this, i);

        for (auto it = shard->metadata.begin(); it != shard->metadata.end();) {
            if (std::regex_search(it->first, pattern)) {
                if (!force && !it->second.IsLeaseExpired()) {
                    VLOG(1) << "key=" << it->first
                            << " matched by regex, but has lease. Skipping "
                            << "removal.";
                    ++it;
                    continue;
                }
                /**
                 * The reason the force operation here does not bypass the
                 * replica check is that put operations (which could also be
                 * copy or move) and remove operations might be happening
                 * concurrently, making it extremely dangerous to perform a
                 * direct removal at this point.
                 */
                if (!it->second.AllReplicas(&Replica::fn_is_completed)) {
                    LOG(WARNING) << "key=" << it->first
                                 << " matched by regex, but not all replicas "
                                    "are complete. Skipping removal.";
                    ++it;
                    continue;
                }
                if (metadata_shards_[i].replication_tasks.contains(it->first)) {
                    LOG(WARNING) << "key=" << it->first
                                 << ", matched by regex, but has replication "
                                    "task. Skipping removal.";
                    ++it;
                    continue;
                }

                VLOG(1) << "key=" << it->first
                        << " matched by regex. Removing.";
                it = shard->metadata.erase(it);
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

long MasterService::RemoveAll(bool force) {
    long removed_count = 0;
    uint64_t total_freed_size = 0;
    // Store the current time to avoid repeatedly
    // calling std::chrono::steady_clock::now()
    auto now = std::chrono::steady_clock::now();

    for (size_t i = 0; i < kNumShards; i++) {
        MetadataShardAccessorRW shard(this, i);
        if (shard->metadata.empty()) {
            continue;
        }

        // Only remove completed objects with expired leases (unless force=true)
        auto it = shard->metadata.begin();
        while (it != shard->metadata.end()) {
            /**
             * The reason the force operation here does not bypass the replica
             * check is that put operations (which could also be copy or move)
             * and remove operations might be happening concurrently, making it
             * extremely dangerous to perform a direct removal at this point.
             */
            if ((force || it->second.IsLeaseExpired(now)) &&
                it->second.AllReplicas(&Replica::fn_is_completed) &&
                !shard->replication_tasks.contains(it->first)) {
                auto mem_rep_count =
                    it->second.CountReplicas(&Replica::fn_is_memory_replica);
                total_freed_size += it->second.size * mem_rep_count;
                it = shard->metadata.erase(it);
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
    // Remove those with invalid allocators
    metadata.EraseReplicas([](const Replica& replica) {
        return replica.has_invalid_mem_handle();
    });

    // Return true if no valid replicas remain after cleanup
    return !metadata.IsValid();
}

size_t MasterService::GetKeyCount() const {
    size_t total = 0;
    for (size_t i = 0; i < kNumShards; i++) {
        MetadataShardAccessorRO shard(this, i);
        total += shard->metadata.size();
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

tl::expected<GetStorageConfigResponse, ErrorCode>
MasterService::GetStorageConfig() const {
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

auto MasterService::MountLocalDiskSegment(const UUID& client_id,
                                          bool enable_offloading)
    -> tl::expected<void, ErrorCode> {
    if (!enable_offload_) {
        LOG(ERROR) << "	The offload functionality is not enabled";
        return tl::make_unexpected(ErrorCode::UNABLE_OFFLOAD);
    }
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();

    auto err =
        segment_access.MountLocalDiskSegment(client_id, enable_offloading);
    if (err == ErrorCode::SEGMENT_ALREADY_EXISTS) {
        // Return OK because this is an idempotent operation
        return {};
    } else if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return {};
}

auto MasterService::OffloadObjectHeartbeat(const UUID& client_id,
                                           bool enable_offloading)
    -> tl::expected<std::unordered_map<std::string, int64_t>, ErrorCode> {
    ScopedLocalDiskSegmentAccess local_disk_segment_access =
        segment_manager_.getLocalDiskSegmentAccess();
    auto& client_local_disk_segment =
        local_disk_segment_access.getClientLocalDiskSegment();
    auto local_disk_segment_it = client_local_disk_segment.find(client_id);
    if (local_disk_segment_it == client_local_disk_segment.end()) {
        LOG(ERROR) << "Local disk segment not found with client id = "
                   << client_id;
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    MutexLocker locker(&local_disk_segment_it->second->offloading_mutex_);
    local_disk_segment_it->second->enable_offloading = enable_offloading;
    if (enable_offloading) {
        return std::move(local_disk_segment_it->second->offloading_objects);
    }
    return {};
}

auto MasterService::NotifyOffloadSuccess(
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

tl::expected<void, ErrorCode> MasterService::PushOffloadingQueue(
    const std::string& key, const Replica& replica) {
    const auto& segment_names = replica.get_segment_names();
    if (segment_names.empty()) {
        return {};
    }
    for (const auto& segment_name_it : segment_names) {
        if (!segment_name_it.has_value()) {
            continue;
        }
        ScopedLocalDiskSegmentAccess local_disk_segment_access =
            segment_manager_.getLocalDiskSegmentAccess();
        const auto& client_by_name =
            local_disk_segment_access.getClientByName();
        auto client_id_it = client_by_name.find(segment_name_it.value());
        if (client_id_it == client_by_name.end()) {
            LOG(ERROR) << "Segment " << segment_name_it.value() << " not found";
            return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
        }
        auto& client_local_disk_segment =
            local_disk_segment_access.getClientLocalDiskSegment();
        auto local_disk_segment_it =
            client_local_disk_segment.find(client_id_it->second);
        if (local_disk_segment_it == client_local_disk_segment.end()) {
            return tl::make_unexpected(ErrorCode::UNABLE_OFFLOADING);
        }
        MutexLocker locker(&local_disk_segment_it->second->offloading_mutex_);
        if (!local_disk_segment_it->second->enable_offloading) {
            return tl::make_unexpected(ErrorCode::UNABLE_OFFLOADING);
        }
        if (local_disk_segment_it->second->offloading_objects.size() >=
            offloading_queue_limit_) {
            return tl::make_unexpected(ErrorCode::KEYS_ULTRA_LIMIT);
        }
        local_disk_segment_it->second->offloading_objects.emplace(
            key, replica.get_descriptor()
                     .get_memory_descriptor()
                     .buffer_descriptor.size_);
    }
    return {};
}

void MasterService::EvictionThreadFunc() {
    VLOG(1) << "action=eviction_thread_started";

    auto last_discard_time = std::chrono::steady_clock::now();
    while (eviction_running_) {
        const auto now = std::chrono::steady_clock::now();
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
            last_discard_time = now;
        } else if (now - last_discard_time > put_start_release_timeout_sec_) {
            // Try discarding expired processing keys and ongoing replication
            // tasks if we have not done this for a long time.
            for (size_t i = 0; i < kNumShards; i++) {
                MetadataShardAccessorRW shard(this, i);
                DiscardExpiredProcessingReplicas(shard, now);
            }
            ReleaseExpiredDiscardedReplicas(now);
            last_discard_time = now;
        }

        std::this_thread::sleep_for(
            std::chrono::milliseconds(kEvictionThreadSleepMs));
    }

    VLOG(1) << "action=eviction_thread_stopped";
}

void MasterService::DiscardExpiredProcessingReplicas(
    MetadataShardAccessorRW& shard,
    const std::chrono::steady_clock::time_point& now) {
    std::list<DiscardedReplicas> discarded_replicas;

    // Part 1: Discard expired PutStart operations.
    for (auto key_it = shard->processing_keys.begin();
         key_it != shard->processing_keys.end();) {
        auto it = shard->metadata.find(*key_it);
        if (it == shard->metadata.end()) {
            // The key has been removed from metadata. This should be
            // impossible.
            LOG(ERROR) << "Key " << *key_it
                       << " was removed while in processing";
            key_it = shard->processing_keys.erase(key_it);
            continue;
        }

        auto& metadata = it->second;
        // If the object is not valid or not in processing state, just
        // remove it from the processing set.
        if (!metadata.IsValid() ||
            metadata.AllReplicas(&Replica::fn_is_completed)) {
            if (!metadata.IsValid()) {
                shard->metadata.erase(it);
            }
            key_it = shard->processing_keys.erase(key_it);
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
            auto replicas = metadata.PopReplicas(&Replica::fn_is_processing);
            if (!replicas.empty()) {
                discarded_replicas.emplace_back(std::move(replicas), ttl);
            }

            if (!metadata.IsValid()) {
                // All replicas of this object are discarded, just
                // remove the whole object.
                shard->metadata.erase(it);
            }

            key_it = shard->processing_keys.erase(key_it);
            continue;
        }

        key_it++;
    }

    // Part 2: Discard expired CopyStart/MoveStart operations.
    for (auto task_it = shard->replication_tasks.begin();
         task_it != shard->replication_tasks.end();) {
        auto metadata_it = shard->metadata.find(task_it->first);
        if (metadata_it == shard->metadata.end()) {
            // The key has been removed from metadata. This should be
            // impossible.
            LOG(ERROR) << "Key " << task_it->first
                       << " was removed with ongoing replication task";
            task_it = shard->replication_tasks.erase(task_it);
            continue;
        }

        const auto ttl =
            task_it->second.start_time + put_start_release_timeout_sec_;
        if (ttl > now) {
            // The task is not expired, skip it.
            task_it++;
            continue;
        }

        auto& metadata = metadata_it->second;

        // Release source refcnt.
        auto source = metadata.GetReplicaByID(task_it->second.source_id);
        if (source != nullptr) {
            source->dec_refcnt();
        }

        // Discard allocated replicas.
        auto& replica_ids = task_it->second.replica_ids;
        auto replicas =
            metadata.PopReplicas([&replica_ids](const Replica& replica) {
                auto it = std::find(replica_ids.begin(), replica_ids.end(),
                                    replica.id());
                return it != replica_ids.end();
            });
        if (!replicas.empty()) {
            discarded_replicas.emplace_back(std::move(replicas), ttl);
        }

        // Check whether the object is still valid.
        if (!metadata.IsValid()) {
            shard->metadata.erase(metadata_it);
        }

        task_it = shard->replication_tasks.erase(task_it);
    }

    if (!discarded_replicas.empty()) {
        std::lock_guard lock(discarded_replicas_mutex_);
        discarded_replicas_.splice(discarded_replicas_.end(),
                                   std::move(discarded_replicas));
    }
}

uint64_t MasterService::ReleaseExpiredDiscardedReplicas(
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

    auto can_evict_replicas = [](const ObjectMetadata& metadata) {
        return metadata.HasReplica([](const Replica& replica) {
            return replica.is_memory_replica() && replica.is_completed() &&
                   replica.get_refcnt() == 0;
        });
    };

    auto evict_replicas = [](ObjectMetadata& metadata) {
        return metadata.EraseReplicas([](const Replica& replica) {
            return replica.is_memory_replica() && replica.is_completed() &&
                   replica.get_refcnt() == 0;
        });
    };

    // Randomly select a starting shard to avoid imbalance eviction between
    // shards. No need to use expensive random_device here.
    size_t start_idx = rand() % kNumShards;

    // First pass: evict objects without soft pin and lease expired
    for (size_t i = 0; i < kNumShards; i++) {
        MetadataShardAccessorRW shard(this, (start_idx + i) % kNumShards);

        // Discard expired processing keys first so that they won't be counted
        // in later evictions.
        DiscardExpiredProcessingReplicas(shard, now);

        // object_count must be updated at beginning as it will be used later
        // to compute ideal_evict_num
        object_count += shard->metadata.size();

        // To achieve evicted_count / object_count = evict_ratio_target,
        // ideally how many object should be evicted in this shard
        const long ideal_evict_num =
            std::ceil(object_count * evict_ratio_target) - evicted_count;

        std::vector<std::chrono::steady_clock::time_point>
            candidates;  // can be removed
        for (auto it = shard->metadata.begin(); it != shard->metadata.end();
             it++) {
            // Skip objects that are not expired or have incomplete replicas
            if (!it->second.IsLeaseExpired(now) ||
                !can_evict_replicas(it->second)) {
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
            auto it = shard->metadata.begin();
            while (it != shard->metadata.end()) {
                // Skip objects that are not allowed to be evicted in the first
                // pass
                if (!it->second.IsLeaseExpired(now) ||
                    it->second.IsSoftPinned(now) ||
                    !can_evict_replicas(it->second)) {
                    ++it;
                    continue;
                }
                if (it->second.lease_timeout <= target_timeout) {
                    // Evict this object
                    total_freed_size +=
                        it->second.size *
                        evict_replicas(it->second);  // Erase memory replicas
                    if (it->second.IsValid() == false) {
                        it = shard->metadata.erase(it);
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
            for (size_t i = 0; i < kNumShards && target_evict_num > 0; i++) {
                MetadataShardAccessorRW shard(this,
                                              (start_idx + i) % kNumShards);
                auto it = shard->metadata.begin();
                while (it != shard->metadata.end() && target_evict_num > 0) {
                    if (it->second.lease_timeout <= target_timeout &&
                        !it->second.IsSoftPinned(now) &&
                        can_evict_replicas(it->second)) {
                        // Evict this object
                        total_freed_size +=
                            it->second.size *
                            evict_replicas(
                                it->second);  // Erase memory replicas
                        if (it->second.IsValid() == false) {
                            it = shard->metadata.erase(it);
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
            for (size_t i = 0; i < kNumShards && target_evict_num > 0; i++) {
                MetadataShardAccessorRW shard(this,
                                              (start_idx + i) % kNumShards);

                auto it = shard->metadata.begin();
                while (it != shard->metadata.end() && target_evict_num > 0) {
                    // Skip objects that are not expired or have incomplete
                    // replicas
                    if (!it->second.IsLeaseExpired(now) ||
                        !can_evict_replicas(it->second)) {
                        ++it;
                        continue;
                    }
                    // Evict objects with 1). no soft pin OR 2). with soft pin
                    // and lease timeout less than or equal to target.
                    if (!it->second.IsSoftPinned(now) ||
                        it->second.lease_timeout <= soft_target_timeout) {
                        total_freed_size +=
                            it->second.size *
                            evict_replicas(
                                it->second);  // Erase memory replicas
                        if (it->second.IsValid() == false) {
                            it = shard->metadata.erase(it);
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

tl::expected<UUID, ErrorCode> MasterService::CreateCopyTask(
    const std::string& key, const std::vector<std::string>& targets) {
    if (targets.empty()) {
        LOG(ERROR) << "key=" << key << ", error=empty_targets";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    MetadataAccessorRO accessor(this, key);
    if (!accessor.Exists()) {
        VLOG(1) << "key=" << key << ", info=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    ScopedSegmentAccess segment_accessor = segment_manager_.getSegmentAccess();
    for (const auto& target : targets) {
        if (!segment_accessor.ExistsSegmentName(target)) {
            LOG(ERROR) << "key=" << key << ", target_segment=" << target
                       << ", error=target_segment_not_mounted";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
    }

    const auto& metadata = accessor.Get();
    const auto& segment_names = metadata.GetReplicaSegmentNames();
    if (segment_names.empty()) {
        LOG(ERROR) << "key=" << key << ", error=no_valid_source_replicas";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    // Randomly pick a segment from the source replicas
    static thread_local std::mt19937 gen(std::random_device{}());
    std::uniform_int_distribution<size_t> dis(0, segment_names.size() - 1);
    std::string selected_source_segment = segment_names[dis(gen)];
    UUID select_client;
    ErrorCode error = segment_accessor.GetClientIdBySegmentName(
        selected_source_segment, select_client);
    if (error != ErrorCode::OK) {
        LOG(ERROR) << "key=" << key
                   << ", segment_name=" << selected_source_segment
                   << ", error=client_id_not_found";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return task_manager_.get_write_access()
        .submit_task_typed<TaskType::REPLICA_COPY>(
            select_client, {.key = key,
                            .source = selected_source_segment,
                            .targets = targets});
}

tl::expected<UUID, ErrorCode> MasterService::CreateMoveTask(
    const std::string& key, const std::string& source,
    const std::string& target) {
    MetadataAccessorRO accessor(this, key);
    if (!accessor.Exists()) {
        VLOG(1) << "key=" << key << ", info=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    if (source == target) {
        LOG(ERROR) << "key=" << key << ", source_segment=" << source
                   << ", target_segment=" << target
                   << ", error=source_target_segments_are_same";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    ScopedSegmentAccess segment_accessor = segment_manager_.getSegmentAccess();
    if (!segment_accessor.ExistsSegmentName(target)) {
        LOG(ERROR) << "key=" << key << ", target_segment=" << target
                   << ", error=target_segment_not_mounted";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    const auto& metadata = accessor.Get();
    const auto& segment_names = metadata.GetReplicaSegmentNames();
    if (std::find(segment_names.begin(), segment_names.end(), source) ==
        segment_names.end()) {
        LOG(ERROR) << "key=" << key << ", source_segment=" << source
                   << ", error=source_segment_not_found";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    UUID select_client;
    ErrorCode error =
        segment_accessor.GetClientIdBySegmentName(source, select_client);

    if (error != ErrorCode::OK) {
        LOG(ERROR) << "key=" << key << ", segment_name=" << source
                   << ", error=client_id_not_found";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    return task_manager_.get_write_access()
        .submit_task_typed<TaskType::REPLICA_MOVE>(
            select_client, {.key = key, .source = source, .target = target});
}

tl::expected<QueryTaskResponse, ErrorCode> MasterService::QueryTask(
    const UUID& task_id) {
    const auto& task_option =
        task_manager_.get_read_access().find_task_by_id(task_id);
    if (!task_option.has_value()) {
        LOG(ERROR) << "task_id=" << task_id << ", error=task_not_found";
        return tl::make_unexpected(ErrorCode::TASK_NOT_FOUND);
    }
    return QueryTaskResponse(task_option.value());
}

tl::expected<std::vector<TaskAssignment>, ErrorCode> MasterService::FetchTasks(
    const UUID& client_id, size_t batch_size) {
    const auto& tasks =
        task_manager_.get_write_access().pop_tasks(client_id, batch_size);
    std::vector<TaskAssignment> assignments;
    for (const auto& task : tasks) {
        assignments.emplace_back(task);
    }
    return assignments;
}

tl::expected<void, ErrorCode> MasterService::MarkTaskToComplete(
    const UUID& client_id, const TaskCompleteRequest& request) {
    auto write_access = task_manager_.get_write_access();
    ErrorCode err = write_access.complete_task(client_id, request.id,
                                               request.status, request.message);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "task_id=" << request.id
                   << ", error=complete_task_failed";
        return tl::make_unexpected(err);
    }
    return {};
}

}  // namespace mooncake

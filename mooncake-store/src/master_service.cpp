#include "master_service.h"

#include <cassert>
#include <cstdint>
#include <shared_mutex>
#include <regex>
#include <unordered_set>
#include <shared_mutex>
#include <unistd.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <ylt/util/tl/expected.hpp>
#include <boost/algorithm/string.hpp>

#include "master_metric_manager.h"
#include "segment.h"
#include "types.h"
#include "serialize/serializer.hpp"
#include "utils/zstd_util.h"
#include "utils/file_util.h"
#include "utils/snapshot_logger.h"

namespace mooncake {

// Snapshot file names
static const std::string SNAPSHOT_METADATA_FILE = "metadata";
static const std::string SNAPSHOT_SEGMENTS_FILE = "segments";
static const std::string SNAPSHOT_TASK_MANAGER_FILE = "task_manager";
static const std::string SNAPSHOT_MANIFEST_FILE = "manifest.txt";
static const std::string SNAPSHOT_LATEST_FILE = "latest.txt";
static const std::string SNAPSHOT_ROOT = "master_snapshot";
static const std::string SNAPSHOT_SERIALIZER_VERSION = "1.0.0";
static const std::string SNAPSHOT_SERIALIZER_TYPE = "messagepack";

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
      enable_snapshot_restore_(config.enable_snapshot_restore),
      enable_snapshot_(config.enable_snapshot),
      snapshot_backup_dir_(config.snapshot_backup_dir),
      snapshot_interval_seconds_(config.snapshot_interval_seconds),
      snapshot_child_timeout_seconds_(config.snapshot_child_timeout_seconds),
      snapshot_retention_count_(config.snapshot_retention_count),
      put_start_discard_timeout_sec_(config.put_start_discard_timeout_sec),
      put_start_release_timeout_sec_(config.put_start_release_timeout_sec),
      task_manager_(config.task_manager_config),
      cxl_path_(config.cxl_path),
      cxl_size_(config.cxl_size),
      enable_cxl_(config.enable_cxl) {
    if (enable_snapshot_ || enable_snapshot_restore_) {
        try {
            snapshot_backend_ =
                SerializerBackend::Create(config.snapshot_backend_type);
        } catch (const std::exception& e) {
            LOG(ERROR) << "Failed to create snapshot backend: " << e.what();
            throw std::runtime_error(
                fmt::format("Failed to create snapshot backend: {}", e.what()));
        }
        if (!snapshot_backup_dir_.empty()) {
            use_snapshot_backup_dir_ = true;
        }
    }

    if (enable_snapshot_restore_) {
        RestoreState();
    }
    if (enable_snapshot_ && snapshot_retention_count_ == 0) {
        LOG(ERROR) << "snapshot_retention_count must be greater than 0";
        throw std::invalid_argument("snapshot_retention_count must be > 0");
    }
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

    if (enable_snapshot_) {
        if (memory_allocator_type_ == BufferAllocatorType::OFFSET) {
            snapshot_running_ = true;
            snapshot_thread_ =
                std::thread(&MasterService::SnapshotThreadFunc, this);
        }
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
    snapshot_running_ = false;
    task_cleanup_running_ = false;

    // Wake sleepers so join() doesn't block for long sleep intervals.
    task_cleanup_cv_.notify_all();

    if (eviction_thread_.joinable()) {
        eviction_thread_.join();
    }
    if (client_monitor_thread_.joinable()) {
        client_monitor_thread_.join();
    }
    if (snapshot_thread_.joinable()) {
        snapshot_thread_.join();
    }
    if (task_cleanup_thread_.joinable()) {
        task_cleanup_thread_.join();
    }
}

auto MasterService::MountSegment(const Segment& segment, const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
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
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
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
        std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
        write_access.prune_expired_tasks();
        write_access.prune_finished_tasks();
    }
    LOG(INFO) << "Task cleanup thread stopped";
}

auto MasterService::UnmountSegment(const UUID& segment_id,
                                   const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    size_t metrics_dec_capacity = 0;  // to update the metrics

    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
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
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
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
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
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

    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
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
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
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

    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    // Lock the shard and check if object already exists
    MetadataShardAccessorRW shard(this, getShardIndex(key));

    const auto now = std::chrono::system_clock::now();
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
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
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
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
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
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
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
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
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
        std::forward_as_tuple(client_id, std::chrono::system_clock::now(),
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
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
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
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
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
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
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
        std::forward_as_tuple(client_id, std::chrono::system_clock::now(),
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
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
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
            std::chrono::system_clock::now() + put_start_release_timeout_sec_);
    }

    accessor.EraseReplicationTask();

    return {};
}

tl::expected<void, ErrorCode> MasterService::MoveRevoke(
    const UUID& client_id, const std::string& key) {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
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
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
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

    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
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
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    auto now = std::chrono::system_clock::now();

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
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
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
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
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

    auto last_discard_time = std::chrono::system_clock::now();
    while (eviction_running_) {
        const auto now = std::chrono::system_clock::now();
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
            {
                std::shared_lock<std::shared_mutex> shared_lock(
                    snapshot_mutex_);
                for (size_t i = 0; i < kNumShards; i++) {
                    MetadataShardAccessorRW shard(this, i);
                    DiscardExpiredProcessingReplicas(shard, now);
                }
                ReleaseExpiredDiscardedReplicas(now);
            }
            last_discard_time = now;
        }

        std::this_thread::sleep_for(
            std::chrono::milliseconds(kEvictionThreadSleepMs));
    }

    VLOG(1) << "action=eviction_thread_stopped";
}

void MasterService::DiscardExpiredProcessingReplicas(
    MetadataShardAccessorRW& shard,
    const std::chrono::system_clock::time_point& now) {
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
    const std::chrono::system_clock::time_point& now) {
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

void MasterService::SnapshotThreadFunc() {
    LOG(INFO) << "[Snapshot] snapshot_thread started";
    while (snapshot_running_) {
        std::this_thread::sleep_for(
            std::chrono::seconds(snapshot_interval_seconds_));
        if (!enable_snapshot_) {
            // Snapshot is disabled
            LOG(INFO)
                << "[Snapshot] Snapshot is disabled, waiting for next cycle";
            continue;
        }
        // Fork a child process to save current state

        std::string snapshot_id =
            FormatTimestamp(std::chrono::system_clock::now());
        LOG(INFO) << "[Snapshot] Preparing to fork child process, snapshot_id="
                  << snapshot_id;

        // Create pipe for child process logging
        int log_pipe[2];
        if (pipe(log_pipe) == -1) {
            LOG(ERROR) << "[Snapshot] Failed to create log pipe: "
                       << strerror(errno) << ", snapshot_id=" << snapshot_id;
            continue;
        }

        pid_t pid;
        {
            std::unique_lock<std::shared_mutex> lock(snapshot_mutex_);
            LOG(INFO) << "[Snapshot] Locking snapshot mutex, snapshot_id="
                      << snapshot_id;
            pid = fork();
        }
        if (pid == -1) {
            // Fork failed
            LOG(ERROR) << "[Snapshot] Failed to fork child process for state "
                          "persistence: "
                       << strerror(errno) << ", snapshot_id=" << snapshot_id;
            close(log_pipe[0]);
            close(log_pipe[1]);
        } else if (pid == 0) {
            // Child process
            // Close read end, set write end for logging
            close(log_pipe[0]);
            g_snapshot_log_pipe_fd = log_pipe[1];

            // Save current state using the configured persistence mechanism
            SNAP_LOG_INFO("[Snapshot] Child process started, snapshot_id={}",
                          snapshot_id);
            auto result = PersistState(snapshot_id);
            if (!result) {
                SNAP_LOG_ERROR(
                    "[Snapshot] Child process failed to persist state, "
                    "snapshot_id={},code={},msg={}",
                    snapshot_id, static_cast<int32_t>(result.error().code),
                    result.error().message);
                close(log_pipe[1]);
                _exit(1);  // Exit child process with error
            }
            SNAP_LOG_INFO(
                "[Snapshot] Child process successfully persisted state, "
                "snapshot_id={}",
                snapshot_id);

            close(log_pipe[1]);
            _exit(0);  // Exit child process successfully
        } else {
            // Parent process
            // Close write end, pass read end to wait function
            close(log_pipe[1]);
            WaitForSnapshotChild(pid, snapshot_id, log_pipe[0]);
            close(log_pipe[0]);
        }
    }
    LOG(INFO) << "[Snapshot] snapshot_thread stopped";
}

void MasterService::WaitForSnapshotChild(pid_t pid,
                                         const std::string& snapshot_id,
                                         int log_pipe_fd) {
    // Default 5 minute timeout
    const int64_t timeout_seconds = snapshot_child_timeout_seconds_;

    LOG(INFO)
        << "[Snapshot] waiting for child process to complete, snapshot_id="
        << snapshot_id << ", child_pid=" << pid
        << ", timeout=" << timeout_seconds << "s";

    // Set pipe to non-blocking mode
    int flags = fcntl(log_pipe_fd, F_GETFL, 0);
    if (flags == -1 || fcntl(log_pipe_fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        LOG(WARNING) << "[Snapshot] Failed to set pipe non-blocking: "
                     << strerror(errno);
    }

    // Buffer for reading child logs
    char buf[4096];
    std::string log_buffer;

    // Helper lambda to read and output child logs
    auto flush_child_logs = [&]() {
        while (true) {
            ssize_t n = read(log_pipe_fd, buf, sizeof(buf) - 1);
            if (n > 0) {
                buf[n] = '\0';
                log_buffer += buf;
                // Output complete lines
                size_t pos;
                while ((pos = log_buffer.find('\n')) != std::string::npos) {
                    std::string line = log_buffer.substr(0, pos);
                    log_buffer.erase(0, pos + 1);
                    if (!line.empty()) {
                        LOG(INFO) << "[Snapshot:Child] " << line;
                    }
                }
            } else {
                break;
            }
        }
    };

    // Record start time
    auto start_time = std::chrono::steady_clock::now();

    // Use non-blocking polling to wait
    while (true) {
        // Read child logs first
        flush_child_logs();

        int status;
        pid_t result = waitpid(pid, &status, WNOHANG);

        if (result == -1) {
            LOG(ERROR) << "[Snapshot] Failed to wait for child process: "
                       << strerror(errno) << ", snapshot_id=" << snapshot_id
                       << ", child_pid=" << pid;
            MasterMetricManager::instance().inc_snapshot_fail();
            return;
        } else if (result == 0) {
            // Child process is still running
            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                               std::chrono::steady_clock::now() - start_time)
                               .count();

            if (elapsed >= timeout_seconds) {
                // Timeout handling - flush remaining logs before killing
                flush_child_logs();
                if (!log_buffer.empty()) {
                    LOG(INFO) << "[Snapshot:Child] " << log_buffer;
                }
                HandleChildTimeout(pid, snapshot_id);
                MasterMetricManager::instance().inc_snapshot_fail();
                return;
            }

            // Brief sleep before checking again
            std::this_thread::sleep_for(std::chrono::seconds(2));
        } else {
            // Child process has exited
            // Flush remaining logs from child
            flush_child_logs();
            // Output any remaining incomplete line
            if (!log_buffer.empty()) {
                LOG(INFO) << "[Snapshot:Child] " << log_buffer;
            }

            HandleChildExit(pid, status, snapshot_id);
            auto elapsed =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - start_time)
                    .count();
            MasterMetricManager::instance().set_snapshot_duration_ms(elapsed);
            return;
        }
    }
}

void MasterService::HandleChildTimeout(pid_t pid,
                                       const std::string& snapshot_id) {
    LOG(WARNING) << "[Snapshot] Child process timeout, snapshot_id="
                 << snapshot_id << ", child_pid=" << pid
                 << ", killing child process";

    // Try to gracefully terminate the child process
    if (kill(pid, SIGTERM) == 0) {
        // Wait a few seconds to see if it exits gracefully
        std::this_thread::sleep_for(std::chrono::seconds(5));

        // Check if it has exited
        int status;
        if (waitpid(pid, &status, WNOHANG) == 0) {
            // Child process still not exited, force kill
            LOG(WARNING) << "[Snapshot] Child process still running, force "
                            "killing, snapshot_id="
                         << snapshot_id << ", child_pid=" << pid;
            kill(pid, SIGKILL);

            // Wait for force termination to complete
            waitpid(pid, &status, 0);
            LOG(WARNING)
                << "[Snapshot] Child process force killed, snapshot_id="
                << snapshot_id << ", child_pid=" << pid;
        } else {
            LOG(INFO) << "[Snapshot] Child process terminated gracefully after "
                         "SIGTERM, snapshot_id="
                      << snapshot_id << ", child_pid=" << pid;
        }
    } else {
        LOG(ERROR) << "[Snapshot] Failed to send SIGTERM to child process, "
                      "snapshot_id="
                   << snapshot_id << ", child_pid=" << pid
                   << ", error=" << strerror(errno);
    }
}

void MasterService::HandleChildExit(pid_t pid, int status,
                                    const std::string& snapshot_id) {
    if (WIFEXITED(status)) {
        int exit_code = WEXITSTATUS(status);
        if (exit_code != 0) {
            LOG(ERROR) << "[Snapshot] Child process exited with error code: "
                       << exit_code << ", snapshot_id=" << snapshot_id
                       << ", child_pid=" << pid;
            MasterMetricManager::instance().inc_snapshot_fail();
        } else {
            LOG(INFO) << "[Snapshot] Child process successfully persisted "
                         "state, snapshot_id="
                      << snapshot_id << ", child_pid=" << pid;
            MasterMetricManager::instance().inc_snapshot_success();
        }
    } else if (WIFSIGNALED(status)) {
        int signal = WTERMSIG(status);
        LOG(ERROR) << "[Snapshot] Child process terminated by signal: "
                   << signal << ", snapshot_id=" << snapshot_id
                   << ", child_pid=" << pid;
        MasterMetricManager::instance().inc_snapshot_fail();
    }
}

tl::expected<void, SerializationError> MasterService::PersistState(
    const std::string& snapshot_id) {
    try {
        SNAP_LOG_INFO(
            "[Snapshot] action=persisting_state start, snapshot_id={}, "
            "serializer_type={}, version={}",
            snapshot_id, SNAPSHOT_SERIALIZER_TYPE, SNAPSHOT_SERIALIZER_VERSION);
        MetadataSerializer metadata_serializer(this);
        SegmentSerializer segment_serializer(&segment_manager_);
        TaskManagerSerializer task_manager_serializer(&task_manager_);

        auto metadata_result = metadata_serializer.Serialize();
        if (!metadata_result) {
            SNAP_LOG_ERROR(
                "[Snapshot] metadata serialization failed, snapshot_id={}, "
                "code={}, msg={}",
                snapshot_id, static_cast<int>(metadata_result.error().code),
                metadata_result.error().message);

            return tl::make_unexpected(metadata_result.error());
        }
        SNAP_LOG_INFO(
            "[Snapshot] metadata serialization_successful, snapshot_id={}",
            snapshot_id);

        auto segment_result = segment_serializer.Serialize();
        if (!segment_result) {
            SNAP_LOG_ERROR(
                "[Snapshot] segment serialization failed, snapshot_id={}, "
                "code={}, msg={}",
                snapshot_id, static_cast<int>(segment_result.error().code),
                segment_result.error().message);
            return tl::make_unexpected(segment_result.error());
        }
        SNAP_LOG_INFO(
            "[Snapshot] segment serialization_successful, snapshot_id={}",
            snapshot_id);

        auto task_manager_result = task_manager_serializer.Serialize();
        if (!task_manager_result) {
            SNAP_LOG_ERROR(
                "[Snapshot] task manager serialization failed, snapshot_id={}, "
                "code={}, msg={}",
                snapshot_id, static_cast<int>(task_manager_result.error().code),
                task_manager_result.error().message);
            return tl::make_unexpected(task_manager_result.error());
        }
        SNAP_LOG_INFO(
            "[Snapshot] task manager serialization_successful, snapshot_id={}",
            snapshot_id);

        // Create storage path prefix
        std::string path_prefix = SNAPSHOT_ROOT + "/" + snapshot_id + "/";
        const auto& serialized_metadata = metadata_result.value();
        const auto& serialized_segment = segment_result.value();
        const auto& serialized_task_manager = task_manager_result.value();

        bool upload_success = true;
        std::string error_msg;
        SNAP_LOG_INFO("[Snapshot] Backend info: {}",
                      snapshot_backend_->GetConnectionInfo());

        // upload metadata
        std::string metadata_path = path_prefix + SNAPSHOT_METADATA_FILE;
        auto upload_result =
            UploadSnapshotFile(serialized_metadata, metadata_path,
                               SNAPSHOT_METADATA_FILE, snapshot_id);
        if (!upload_result) {
            error_msg.append(upload_result.error().message + "\n");
            upload_success = false;
        }

        // upload segment
        std::string segment_path = path_prefix + SNAPSHOT_SEGMENTS_FILE;
        upload_result = UploadSnapshotFile(serialized_segment, segment_path,
                                           SNAPSHOT_SEGMENTS_FILE, snapshot_id);
        if (!upload_result) {
            error_msg.append(upload_result.error().message + "\n");
            upload_success = false;
        }
        // upload task manager
        std::string task_manager_path =
            path_prefix + SNAPSHOT_TASK_MANAGER_FILE;
        upload_result =
            UploadSnapshotFile(serialized_task_manager, task_manager_path,
                               SNAPSHOT_TASK_MANAGER_FILE, snapshot_id);
        if (!upload_result) {
            error_msg.append(upload_result.error().message + "\n");
            upload_success = false;
        }

        std::string manifest_path = path_prefix + SNAPSHOT_MANIFEST_FILE;
        std::string manifest_content =
            fmt::format("{}|{}|{}", SNAPSHOT_SERIALIZER_TYPE,
                        SNAPSHOT_SERIALIZER_VERSION, snapshot_id);
        std::vector<uint8_t> manifest_bytes(manifest_content.begin(),
                                            manifest_content.end());
        upload_result = UploadSnapshotFile(manifest_bytes, manifest_path,
                                           SNAPSHOT_MANIFEST_FILE, snapshot_id);
        if (!upload_result) {
            error_msg.append(upload_result.error().message + "\n");
            upload_success = false;
        }

        if (!upload_success) {
            return tl::make_unexpected(
                SerializationError(ErrorCode::PERSISTENT_FAIL, error_msg));
        }

        // Update latest marker (atomic operation)
        // Format: protocol_type|version|20230801_123456_000
        std::string latest_path = SNAPSHOT_ROOT + "/" + SNAPSHOT_LATEST_FILE;
        std::string latest_content = snapshot_id;

        auto latest_update_result =
            snapshot_backend_->UploadString(latest_path, latest_content);
        if (!latest_update_result) {
            SNAP_LOG_ERROR(
                "[Snapshot] latest update failed, snapshot_id={}, file={}",
                snapshot_id, latest_path);
            if (use_snapshot_backup_dir_) {
                auto save_path = fs::path(snapshot_backup_dir_) / "save" /
                                 SNAPSHOT_LATEST_FILE;
                auto save_result =
                    FileUtil::SaveStringToFile(latest_content, save_path);
                if (!save_result) {
                    SNAP_LOG_ERROR(
                        "[Snapshot] save latest to disk failed, "
                        "snapshot_id={}, "
                        "content={}, file={}",
                        snapshot_id, latest_content, save_path.string());
                }
            }

            return tl::make_unexpected(SerializationError(
                ErrorCode::PERSISTENT_FAIL,
                fmt::format("latest update {} failed", latest_path)));
        }
        SNAP_LOG_INFO(
            "[Snapshot] Upload latest success: {}, snapshot_id={}, "
            "content={}",
            latest_path, snapshot_id, latest_content);

        CleanupOldSnapshot(snapshot_retention_count_, snapshot_id);
        SNAP_LOG_INFO("[Snapshot] action=persisting_state end, snapshot_id={}",
                      snapshot_id);
    } catch (const std::exception& e) {
        SNAP_LOG_ERROR(
            "[Snapshot] Exception during state persistent, snapshot_id={}, "
            "error={}",
            snapshot_id, e.what());
        return tl::make_unexpected(SerializationError(
            ErrorCode::PERSISTENT_FAIL,
            fmt::format("Exception during state persistent: {}", e.what())));
    } catch (...) {
        SNAP_LOG_ERROR(
            "[Snapshot] Unknown exception during state persistent, "
            "snapshot_id={}",
            snapshot_id);
        return tl::make_unexpected(
            SerializationError(ErrorCode::PERSISTENT_FAIL,
                               "Unknown exception during state persistent"));
    }
    return {};
}

tl::expected<void, SerializationError> MasterService::UploadSnapshotFile(
    const std::vector<uint8_t>& data, const std::string& path,
    const std::string& local_filename, const std::string& snapshot_id) {
    SNAP_LOG_INFO("[Snapshot] Uploading {} to: {}, snapshot_id={}",
                  local_filename, path, snapshot_id);

    std::string error_msg;
    auto upload_result = snapshot_backend_->UploadBuffer(path, data);
    if (!upload_result) {
        SNAP_LOG_ERROR(
            "[Snapshot] {} upload failed, snapshot_id={}, file={}, error={}",
            local_filename, snapshot_id, path, upload_result.error());

        // Upload failed, save locally for manual recovery in exception
        // scenarios
        if (use_snapshot_backup_dir_) {
            auto save_path =
                fs::path(snapshot_backup_dir_) / "save" / local_filename;
            auto save_result = FileUtil::SaveBinaryToFile(data, save_path);
            if (!save_result) {
                SNAP_LOG_ERROR(
                    "[Snapshot] save {} to disk failed, snapshot_id={}, "
                    "file={}",
                    local_filename, snapshot_id, save_path.string());
            }
        }

        error_msg.append(local_filename)
            .append(" upload ")
            .append(path)
            .append(" failed; ");
        return tl::make_unexpected(
            SerializationError(ErrorCode::PERSISTENT_FAIL, error_msg));
    } else {
        SNAP_LOG_INFO("[Snapshot] Upload {} success: {}, snapshot_id={}",
                      local_filename, path, snapshot_id);
    }

    return {};
}

void MasterService::CleanupOldSnapshot(int keep_count,
                                       const std::string& snapshot_id) {
    // 1. List all state directories
    std::string prefix = SNAPSHOT_ROOT + "/";
    std::vector<std::string> all_objects;
    auto list_result =
        snapshot_backend_->ListObjectsWithPrefix(prefix, all_objects);
    if (!list_result) {
        SNAP_LOG_ERROR(
            "[Snapshot] error=list failed, prefix={}, snapshot_id={}", prefix,
            snapshot_id);
        return;
    }

    // 2. Extract all timestamp directories (by finding directory structure)
    std::set<std::string> snapshot_dirs;
    std::regex state_dir_regex(
        "^" + prefix + R"((\d{8}_\d{6}_\d{3})/)");  // Match directory structure

    // Extract directory names by finding paths of all objects
    for (const auto& object_key : all_objects) {
        std::smatch match;
        if (std::regex_search(object_key, match, state_dir_regex)) {
            snapshot_dirs.insert(match[1].str());  // Extract timestamp part
        }
    }

    // 3. Convert to vector and sort by timestamp (descending, newest first)
    std::vector<std::string> sorted_snapshot_dirs(snapshot_dirs.begin(),
                                                  snapshot_dirs.end());
    std::sort(sorted_snapshot_dirs.begin(), sorted_snapshot_dirs.end(),
              std::greater<>());

    // 4. Delete old states exceeding retention count
    if (static_cast<int>(sorted_snapshot_dirs.size()) > keep_count) {
        for (int i = keep_count;
             i < static_cast<int>(sorted_snapshot_dirs.size()); i++) {
            std::string old_state_dir = sorted_snapshot_dirs[i];

            // Fault tolerance: skip if same as current snapshot_id to avoid
            // accidental deletion
            if (old_state_dir == snapshot_id) {
                SNAP_LOG_WARN(
                    "[Snapshot] Skipping deletion of current snapshot "
                    "directory {}, "
                    "snapshot_id={}",
                    old_state_dir, snapshot_id);
                continue;
            }

            std::string old_state_prefix = prefix + old_state_dir + "/";

            // Delete the entire old state directory (regardless of
            // manifest.json existence)
            auto delete_result =
                snapshot_backend_->DeleteObjectsWithPrefix(old_state_prefix);
            if (!delete_result) {
                SNAP_LOG_ERROR(
                    "[Snapshot] Failed to delete old state directory {}, "
                    "snapshot_id={}",
                    old_state_dir, snapshot_id);
            } else {
                SNAP_LOG_INFO(
                    "[Snapshot] Successfully deleted old state directory {}, "
                    "snapshot_id={}",
                    old_state_dir, snapshot_id);
            }
        }
    }
}

void MasterService::RestoreState() {
    try {
        auto now = std::chrono::system_clock::now();

        LOG(INFO) << "[Restore] Backend info: "
                  << snapshot_backend_->GetConnectionInfo();
        // 1. Read latest.txt file to get the latest state ID
        std::string latest_path = SNAPSHOT_ROOT + "/" + SNAPSHOT_LATEST_FILE;
        std::string latest_content;
        if (!snapshot_backend_->DownloadString(latest_path, latest_content)) {
            LOG(ERROR)
                << "[Restore] No previous snapshot found, starting fresh";
            return;
        }

        // Trim leading and trailing whitespace
        latest_content.erase(0, latest_content.find_first_not_of(" \t\r\n"));
        latest_content.erase(latest_content.find_last_not_of(" \t\r\n") + 1);

        if (latest_content.empty()) {
            LOG(ERROR)
                << "[Restore] Latest snapshot file is empty, starting fresh";
            return;
        }

        // Validate snapshot ID format
        static const std::regex snapshot_id_regex(R"(^\d{8}_\d{6}_\d{3}$)");
        if (!std::regex_match(latest_content, snapshot_id_regex)) {
            LOG(ERROR) << "[Restore] Invalid snapshot ID format: "
                       << latest_content << ", starting fresh";
            return;
        }

        std::string state_id = latest_content;
        std::string path_prefix = SNAPSHOT_ROOT + "/" + state_id + "/";

        // 2. Download manifest.txt to parse protocol version info
        std::string manifest_path = path_prefix + SNAPSHOT_MANIFEST_FILE;
        std::string manifest_content;
        if (!snapshot_backend_->DownloadString(manifest_path,
                                               manifest_content)) {
            LOG(ERROR) << "[Restore] Failed to download manifest file: "
                       << manifest_path << " , starting fresh";
            return;
        }

        if (use_snapshot_backup_dir_) {
            auto save_result = FileUtil::SaveStringToFile(
                manifest_content, fs::path(snapshot_backup_dir_) / "restore" /
                                      SNAPSHOT_MANIFEST_FILE);
            if (!save_result) {
                LOG(ERROR) << "[Restore] Failed to save manifest to file: "
                           << save_result.error();
            }
        }

        // Format: protocol_type|version|20230801_123456_000
        std::vector<std::string> parts;
        boost::split(parts, manifest_content, boost::is_any_of("|"));

        std::string protocol_type;  // Protocol type
        std::string version;        // Version

        if (parts.size() >= 3) {
            protocol_type = parts[0];
            version = parts[1];
        } else {
            // Invalid format
            LOG(ERROR) << "[Restore] Invalid latest snapshot format: "
                       << latest_content;
            return;
        }

        LOG(INFO) << "[Restore] Restoring state from snapshot: " << state_id
                  << " version: " << version << " protocol: " << protocol_type;

        // Strict compatibility check: fail fast on version/protocol mismatch
        if (protocol_type != SNAPSHOT_SERIALIZER_TYPE) {
            LOG(ERROR) << "[Restore] Unsupported protocol type: "
                       << protocol_type
                       << ", expected: " << SNAPSHOT_SERIALIZER_TYPE
                       << ", starting fresh";
            return;
        }
        if (version != SNAPSHOT_SERIALIZER_VERSION) {
            LOG(ERROR) << "[Restore] Incompatible snapshot version: " << version
                       << ", expected: " << SNAPSHOT_SERIALIZER_VERSION
                       << ", starting fresh";
            return;
        }

        // 3. Download metadata
        std::string metadata_path = path_prefix + SNAPSHOT_METADATA_FILE;
        std::vector<uint8_t> metadata_content;
        auto download_result =
            snapshot_backend_->DownloadBuffer(metadata_path, metadata_content);
        if (!download_result) {
            LOG(ERROR) << "[Restore] Failed to download metadata file: "
                       << metadata_path << "error=" << download_result.error();
            return;
        }

        if (use_snapshot_backup_dir_) {
            auto save_result = FileUtil::SaveBinaryToFile(
                metadata_content, fs::path(snapshot_backup_dir_) / "restore" /
                                      SNAPSHOT_METADATA_FILE);
            if (!save_result) {
                LOG(ERROR) << "[Restore] Failed to save metadata to file: "
                           << save_result.error();
            }
        }
        LOG(INFO) << "[Restore] Download metadata file success";

        // 4. Download segments
        std::string segments_path = path_prefix + SNAPSHOT_SEGMENTS_FILE;
        std::vector<uint8_t> segments_content;
        download_result =
            snapshot_backend_->DownloadBuffer(segments_path, segments_content);
        if (!download_result) {
            LOG(ERROR) << "Failed to download segments file: " << segments_path
                       << " error=" << download_result.error();
            return;
        }
        if (use_snapshot_backup_dir_) {
            auto save_result = FileUtil::SaveBinaryToFile(
                segments_content, fs::path(snapshot_backup_dir_) / "restore" /
                                      SNAPSHOT_SEGMENTS_FILE);
            if (!save_result) {
                LOG(ERROR) << "[Restore] Failed to save segments to file: "
                           << save_result.error();
            }
        }
        LOG(INFO) << "[Restore] Download segments file success";

        // 5. Download task manager state
        std::string task_manager_path =
            path_prefix + SNAPSHOT_TASK_MANAGER_FILE;
        std::vector<uint8_t> task_manager_content;
        download_result = snapshot_backend_->DownloadBuffer(
            task_manager_path, task_manager_content);
        if (!download_result) {
            LOG(ERROR) << "Failed to download task manager file: "
                       << task_manager_path
                       << " error=" << download_result.error();
            return;
        }
        if (use_snapshot_backup_dir_) {
            auto save_result = FileUtil::SaveBinaryToFile(
                task_manager_content, fs::path(snapshot_backup_dir_) /
                                          "restore" /
                                          SNAPSHOT_TASK_MANAGER_FILE);
            if (!save_result) {
                LOG(ERROR) << "[Restore] Failed to save task manager to file: "
                           << save_result.error();
            }
        }
        LOG(INFO) << "[Restore] Download task manager file success";

        // 6. Deserialize state
        SegmentSerializer segment_serializer(&segment_manager_);
        MetadataSerializer metadata_serializer(this);
        TaskManagerSerializer task_manager_serializer(&task_manager_);

        auto segments_result = segment_serializer.Deserialize(segments_content);
        if (!segments_result) {
            LOG(ERROR) << "[Restore] Failed to deserialize segments: "
                       << segments_result.error().code << " - "
                       << segments_result.error().message;
            segment_serializer.Reset();
            return;
        }
        LOG(INFO) << "[Restore] Deserialize segments success";

        auto metadata_result =
            metadata_serializer.Deserialize(metadata_content);
        if (!metadata_result) {
            LOG(ERROR) << "[Restore] Failed to deserialize metadata: "
                       << metadata_result.error().code;
            metadata_serializer.Reset();
            segment_serializer.Reset();
            return;
        }

        LOG(INFO) << "[Restore] Deserialize metadata success";

        auto task_manager_result =
            task_manager_serializer.Deserialize(task_manager_content);
        if (!task_manager_result) {
            LOG(ERROR) << "[Restore] Failed to deserialize task manager: "
                       << task_manager_result.error().code << " - "
                       << task_manager_result.error().message;
            task_manager_serializer.Reset();
            metadata_serializer.Reset();
            segment_serializer.Reset();
            return;
        }

        LOG(INFO) << "[Restore] Deserialize task manager success";

        std::vector<std::string> segment_names;
        {
            ScopedSegmentAccess segment_access =
                segment_manager_.getSegmentAccess();
            segment_access.GetAllSegmentNames(segment_names);
        }

        {
            // After deserialization, iterate through metadata_shards_ to clean
            // up non-ready metadata
            const bool skip_cleanup = std::getenv(
                "MOONCAKE_MASTER_SERVICE_SNAPSHOT_TEST_SKIP_CLEANUP");
            if (!skip_cleanup) {
                for (auto& shard : metadata_shards_) {
                    for (auto it = shard.metadata.begin();
                         it != shard.metadata.end();) {
                        if (it->second.HasDiffRepStatus(
                                ReplicaStatus::COMPLETE) ||
                            (it->second.IsLeaseExpired() &&
                             !it->second.IsSoftPinned(now))) {
                            VLOG(1)
                                << "clear metadata key=" << it->first
                                << " ,lease_timeout="
                                << std::chrono::duration_cast<
                                       std::chrono::milliseconds>(
                                       it->second.lease_timeout
                                           .time_since_epoch())
                                       .count()
                                << " ,soft_pin_timeout="
                                << (it->second.soft_pin_timeout.has_value()
                                        ? std::to_string(
                                              std::chrono::duration_cast<
                                                  std::chrono::milliseconds>(
                                                  it->second.soft_pin_timeout
                                                      .value()
                                                      .time_since_epoch())
                                                  .count())
                                        : "null");
                            it = shard.metadata.erase(it);
                        } else {
                            ++it;
                        }
                    }
                }
            }

            // Restore memory usage
            // Step 1: Reset memory usage
            MasterMetricManager::instance().reset_allocated_mem_size();
            for (auto& segment_name : segment_names) {
                MasterMetricManager::instance()
                    .reset_segment_allocated_mem_size(segment_name);
            }

            for (auto& shard : metadata_shards_) {
                for (auto it = shard.metadata.begin();
                     it != shard.metadata.end();) {
                    for (auto& replica : it->second.GetAllReplicas()) {
                        if (!replica.get_descriptor().is_memory_replica()) {
                            continue;
                        }
                        auto temp_segment_names = replica.get_segment_names();
                        if (temp_segment_names.empty()) {
                            continue;
                        }

                        std::string temp_segment_name;
                        if (temp_segment_names[0].has_value()) {
                            temp_segment_name = temp_segment_names[0].value();
                        }

                        auto buffer_descriptor = replica.get_descriptor()
                                                     .get_memory_descriptor()
                                                     .buffer_descriptor;
                        MasterMetricManager::instance().inc_allocated_mem_size(
                            temp_segment_name,
                            static_cast<int64_t>(buffer_descriptor.size_));
                    }
                    ++it;
                }
            }

            LOG(INFO)
                << "[Restore] Total allocated size after restore: "
                << MasterMetricManager::instance().get_allocated_mem_size();
        }

        {
            // Reset total capacity
            MasterMetricManager::instance().reset_total_mem_capacity();
            for (auto& segment_name : segment_names) {
                MasterMetricManager::instance()
                    .reset_segment_total_mem_capacity(segment_name);
            }

            ScopedSegmentAccess segment_access =
                segment_manager_.getSegmentAccess();
            std::vector<std::pair<Segment, UUID>> unready_segments;

            // Get all unready segments and their corresponding client_ids
            if (segment_access.GetUnreadySegments(unready_segments) ==
                ErrorCode::OK) {
                // Remove all unready segments
                for (const auto& [segment, client_id] : unready_segments) {
                    UnmountSegment(segment.id, client_id);
                }
            }

            std::vector<std::pair<Segment, UUID>> all_segments;
            auto err = segment_access.GetAllSegments(all_segments);

            if (err == ErrorCode::OK) {
                int64_t total_size = 0;
                for (const auto& [segment, client_id] : all_segments) {
                    Ping(client_id);  // Add to heartbeat monitoring
                    total_size += static_cast<int64_t>(segment.size);
                    // Restore segment usage
                    MasterMetricManager::instance().inc_total_mem_capacity(
                        segment.name, segment.size);
                }
                LOG(INFO) << "[Restore] Total capacity size after restore: "
                          << total_size;
            } else {
                LOG(ERROR) << "[Restore] Failed to get all segments, error: "
                           << err;
            }
        }

        LOG(INFO) << "[Restore] Successfully restored state from snapshot: "
                  << state_id;

    } catch (const std::exception& e) {
        LOG(ERROR) << "[Restore] Exception during state restoration: "
                   << e.what();
    } catch (...) {
        LOG(ERROR) << "[Restore] Unknown exception during state restoration";
    }
}

void MasterService::BatchEvict(double evict_ratio_target,
                               double evict_ratio_lowerbound) {
    if (evict_ratio_target < evict_ratio_lowerbound) {
        LOG(ERROR) << "evict_ratio_target=" << evict_ratio_target
                   << ", evict_ratio_lowerbound=" << evict_ratio_lowerbound
                   << ", error=invalid_params";
        evict_ratio_lowerbound = evict_ratio_target;
    }

    auto now = std::chrono::system_clock::now();
    long evicted_count = 0;
    long object_count = 0;
    uint64_t total_freed_size = 0;

    // Candidates for second pass eviction
    std::vector<std::chrono::system_clock::time_point> no_pin_objects;
    std::vector<std::chrono::system_clock::time_point> soft_pin_objects;

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
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);

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

        std::vector<std::chrono::system_clock::time_point>
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
    VLOG(1) << "action=evict_objects"
            << ", evicted_count=" << evicted_count
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
            std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
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

tl::expected<std::vector<uint8_t>, SerializationError>
MasterService::MetadataSerializer::Serialize() {
    msgpack::sbuffer sbuf;
    msgpack::packer<msgpack::sbuffer> packer(&sbuf);

    // Create top-level map with 3 fields: "shards", "discarded_replicas",
    // "replica_next_id"
    packer.pack_map(3);

    // 1. Serialize metadata shards
    packer.pack("shards");

    // First count non-empty shards
    size_t valid_shards = 0;
    for (size_t i = 0; i < kNumShards; ++i) {
        if (!service_->metadata_shards_[i].metadata.empty()) {
            valid_shards++;
        }
    }

    // Create shards map
    packer.pack_map(valid_shards);

    // Iterate through all shards, serialize each shard independently
    for (size_t shard_idx = 0; shard_idx < kNumShards; ++shard_idx) {
        const auto& shard = service_->metadata_shards_[shard_idx];

        // Skip if shard is empty
        if (shard.metadata.empty()) {
            continue;
        }

        // Use shard index as key
        packer.pack(shard_idx);

        // Create independent serialization buffer for current shard
        msgpack::sbuffer shard_buffer;
        msgpack::packer<msgpack::sbuffer> shard_packer(&shard_buffer);

        // Serialize shard using SerializeShard
        auto result = SerializeShard(shard, shard_packer);
        if (!result) {
            return tl::make_unexpected(SerializationError(
                result.error().code,
                fmt::format("Failed to serialize shard {}: {}", shard_idx,
                            result.error().message)));
        }

        // Compress data
        std::vector<uint8_t> compressed_data =
            zstd_compress(reinterpret_cast<const uint8_t*>(shard_buffer.data()),
                          shard_buffer.size(), 3);
        // Write entire shard serialized data as binary to main buffer
        packer.pack_bin(compressed_data.size());
        packer.pack_bin_body(
            reinterpret_cast<const char*>(compressed_data.data()),
            compressed_data.size());
    }

    // 2. Serialize discarded_replicas
    packer.pack("discarded_replicas");
    auto dr_result = SerializeDiscardedReplicas(packer);
    if (!dr_result) {
        return tl::make_unexpected(SerializationError(
            dr_result.error().code, "Failed to serialize discarded_replicas: " +
                                        dr_result.error().message));
    }

    // 3. Serialize replica_next_id (static variable for generating unique
    // replica IDs)
    packer.pack("replica_next_id");
    packer.pack(static_cast<uint64_t>(Replica::next_id_.load()));

    return std::vector<uint8_t>(
        reinterpret_cast<const uint8_t*>(sbuf.data()),
        reinterpret_cast<const uint8_t*>(sbuf.data()) + sbuf.size());
}

tl::expected<void, SerializationError>
MasterService::MetadataSerializer::Deserialize(
    const std::vector<uint8_t>& data) {
    // Parse MessagePack data directly
    msgpack::object_handle oh;
    try {
        oh = msgpack::unpack(reinterpret_cast<const char*>(data.data()),
                             data.size());
    } catch (const std::exception& e) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            "Failed to unpack MessagePack data: " + std::string(e.what())));
    }

    const msgpack::object& obj = oh.get();

    // Check if it's a map
    if (obj.type != msgpack::type::MAP) {
        return tl::make_unexpected(
            SerializationError(ErrorCode::DESERIALIZE_FAIL,
                               "Invalid MessagePack format: expected map"));
    }

    // Expected format: top-level map with "shards", "discarded_replicas",
    // and "replica_next_id"
    const msgpack::object* shards_obj = nullptr;
    const msgpack::object* discarded_replicas_obj = nullptr;
    const msgpack::object* replica_next_id_obj = nullptr;

    // Extract fields from top-level map
    for (uint32_t i = 0; i < obj.via.map.size; ++i) {
        const auto& key_obj = obj.via.map.ptr[i].key;
        if (key_obj.type == msgpack::type::STR) {
            std::string key = key_obj.as<std::string>();
            if (key == "shards") {
                shards_obj = &obj.via.map.ptr[i].val;
            } else if (key == "discarded_replicas") {
                discarded_replicas_obj = &obj.via.map.ptr[i].val;
            } else if (key == "replica_next_id") {
                replica_next_id_obj = &obj.via.map.ptr[i].val;
            }
        }
    }

    // Check required "shards" field
    if (shards_obj == nullptr) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL, "Missing 'shards' field"));
    }

    // Iterate and deserialize each shard
    for (uint32_t i = 0; i < shards_obj->via.map.size; ++i) {
        // Get shard index
        uint32_t shard_idx = shards_obj->via.map.ptr[i].key.as<uint32_t>();

        // Check shard index validity
        if (shard_idx >= kNumShards) {
            return tl::make_unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                fmt::format("Invalid shard index: {}", shard_idx)));
        }

        // Get shard binary data
        const msgpack::object& shard_data_obj = shards_obj->via.map.ptr[i].val;
        if (shard_data_obj.type != msgpack::type::BIN) {
            return tl::make_unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                "Invalid MessagePack format: expected binary data for shard"));
        }

        // Parse shard binary data directly, avoiding copy
        msgpack::object_handle shard_oh;
        try {
            auto decompressed_data = zstd_decompress(
                reinterpret_cast<const uint8_t*>(shard_data_obj.via.bin.ptr),
                shard_data_obj.via.bin.size);
            shard_oh = msgpack::unpack(
                reinterpret_cast<const char*>(decompressed_data.data()),
                decompressed_data.size());
        } catch (const std::exception& e) {
            return tl::make_unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                "Failed to unpack shard data: " + std::string(e.what())));
        }

        const msgpack::object& shard_obj = shard_oh.get();

        // Get shard reference and deserialize
        auto& shard = service_->metadata_shards_[shard_idx];
        auto result = DeserializeShard(shard_obj, shard);
        if (!result) {
            return tl::make_unexpected(SerializationError(
                result.error().code,
                fmt::format("Failed to deserialize shard {}: {}", shard_idx,
                            result.error().message)));
        }
    }

    // Deserialize discarded_replicas
    if (discarded_replicas_obj == nullptr) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            "Missing required field 'discarded_replicas' in snapshot data"));
    }
    auto dr_result = DeserializeDiscardedReplicas(*discarded_replicas_obj);
    if (!dr_result) {
        return tl::make_unexpected(
            SerializationError(dr_result.error().code,
                               "Failed to deserialize discarded_replicas: " +
                                   dr_result.error().message));
    }

    // Restore replica_next_id
    if (replica_next_id_obj == nullptr) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            "Missing required field 'replica_next_id' in snapshot data"));
    }
    auto next_id = replica_next_id_obj->as<uint64_t>();
    Replica::next_id_.store(next_id);
    LOG(INFO) << "Restored Replica::next_id_ to " << next_id;

    return {};
}

void MasterService::MetadataSerializer::Reset() {
    for (auto& shard : service_->metadata_shards_) {
        shard.metadata.clear();
    }
    {
        std::lock_guard lock(service_->discarded_replicas_mutex_);
        service_->discarded_replicas_.clear();
    }
    Replica::next_id_.store(1);
}

tl::expected<void, SerializationError>
MasterService::MetadataSerializer::SerializeShard(const MetadataShard& shard,
                                                  MsgpackPacker& packer) const {
    // MetadataShard format: map with "metadata" field
    packer.pack_map(1);

    // Serialize metadata
    packer.pack("metadata");
    packer.pack_array(shard.metadata.size());

    // Sort keys to ensure consistent serialization order.
    // NOTE: sort may be slow for large shards.
    std::vector<std::string> sorted_keys;
    sorted_keys.reserve(shard.metadata.size());
    for (const auto& [key, metadata] : shard.metadata) {
        sorted_keys.push_back(key);
    }
    std::sort(sorted_keys.begin(), sorted_keys.end());

    for (const auto& key : sorted_keys) {
        const auto& metadata = shard.metadata.at(key);
        // Each metadata item format: [key, metadata_object]
        packer.pack_array(2);
        packer.pack(key);

        auto result = SerializeMetadata(metadata, packer);
        if (!result) {
            return tl::make_unexpected(SerializationError(
                result.error().code,
                fmt::format("Failed to serialize metadata for key '{}': {}",
                            key, result.error().message)));
        }
    }

    return {};
}

tl::expected<void, SerializationError>
MasterService::MetadataSerializer::DeserializeShard(const msgpack::object& obj,
                                                    MetadataShard& shard) {
    if (obj.type != msgpack::type::MAP) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL, "Invalid shard format: expected map"));
    }

    const msgpack::object* metadata_array = nullptr;

    // Extract fields from shard map
    for (uint32_t i = 0; i < obj.via.map.size; ++i) {
        const auto& key_obj = obj.via.map.ptr[i].key;
        if (key_obj.type == msgpack::type::STR) {
            std::string field_key(key_obj.via.str.ptr, key_obj.via.str.size);
            if (field_key == "metadata") {
                metadata_array = &obj.via.map.ptr[i].val;
            }
        }
    }

    // Clear existing data
    shard.metadata.clear();

    // Deserialize metadata
    if (metadata_array == nullptr ||
        metadata_array->type != msgpack::type::ARRAY) {
        return tl::make_unexpected(
            SerializationError(ErrorCode::DESERIALIZE_FAIL,
                               "Missing or invalid 'metadata' field in shard"));
    }

    shard.metadata.reserve(metadata_array->via.array.size);

    for (uint32_t j = 0; j < metadata_array->via.array.size; ++j) {
        const msgpack::object& item = metadata_array->via.array.ptr[j];

        if (item.type != msgpack::type::ARRAY || item.via.array.size != 2) {
            return tl::make_unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                "Invalid metadata item format: expected [key, metadata]"));
        }

        std::string key = item.via.array.ptr[0].as<std::string>();
        const msgpack::object& value_obj = item.via.array.ptr[1];

        auto metadata_result = DeserializeMetadata(value_obj);
        if (!metadata_result) {
            LOG(ERROR) << "Failed to deserialize metadata for key: " << key
                       << ": " << metadata_result.error().message;
            continue;
        }

        auto metadata_ptr = std::move(metadata_result.value());
        auto [it, inserted] = shard.metadata.emplace(
            std::piecewise_construct, std::forward_as_tuple(std::move(key)),
            std::forward_as_tuple(
                metadata_ptr->client_id, metadata_ptr->put_start_time,
                metadata_ptr->size, metadata_ptr->PopReplicas(),
                metadata_ptr->soft_pin_timeout.has_value()));

        it->second.lease_timeout = metadata_ptr->lease_timeout;
        it->second.soft_pin_timeout = metadata_ptr->soft_pin_timeout;
    }

    return {};
}

tl::expected<void, SerializationError>
MasterService::MetadataSerializer::SerializeMetadata(
    const MasterService::ObjectMetadata& metadata,
    MsgpackPacker& packer) const {
    // Pack ObjectMetadata using array structure for efficiency
    // Format: [client_id, put_start_time, size, lease_timeout,
    // has_soft_pin_timeout, soft_pin_timeout, replicas_count, replicas...]

    size_t array_size = 7;  // size, lease_timeout, has_soft_pin_timeout,
                            // soft_pin_timeout, replicas_count
    array_size += metadata.CountReplicas();  // One element per replica
    packer.pack_array(array_size);

    // Serialize client_id
    std::string client_id = UuidToString(metadata.client_id);
    packer.pack(client_id);

    // Serialize put_start_time (convert to timestamp)
    auto put_start_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                              metadata.put_start_time.time_since_epoch())
                              .count();
    packer.pack(put_start_time);

    // Serialize size
    packer.pack(static_cast<uint64_t>(metadata.size));

    // Serialize lease_timeout (convert to timestamp)
    auto lease_timestamp =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            metadata.lease_timeout.time_since_epoch())
            .count();
    packer.pack(lease_timestamp);

    // Serialize soft_pin_timeout (if exists)
    if (metadata.soft_pin_timeout.has_value()) {
        packer.pack(true);  // Mark soft_pin_timeout exists
        auto soft_pin_timestamp =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                metadata.soft_pin_timeout.value().time_since_epoch())
                .count();
        packer.pack(soft_pin_timestamp);
    } else {
        packer.pack(false);        // Mark soft_pin_timeout does not exist
        packer.pack(uint64_t(0));  // Placeholder
    }

    // Serialize replicas count
    packer.pack(static_cast<uint32_t>(metadata.CountReplicas()));

    // Serialize replicas
    for (const auto& replica : metadata.GetAllReplicas()) {
        auto result = Serializer<Replica>::serialize(
            replica, service_->segment_manager_.getView(), packer);
        if (!result) {
            return tl::unexpected(result.error());
        }
    }

    return {};
}

tl::expected<std::unique_ptr<MasterService::ObjectMetadata>, SerializationError>
MasterService::MetadataSerializer::DeserializeMetadata(
    const msgpack::object& obj) const {
    // Check if input is a valid array
    if (obj.type != msgpack::type::ARRAY) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            "deserialize ObjectMetadata state is not an array"));
    }

    // Need at least 7 elements: client_id, put_start_time, size, lease_timeout,
    // has_soft_pin_timeout, soft_pin_timeout, replicas_count
    if (obj.via.array.size < 7) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            "deserialize ObjectMetadata array size is too small"));
    }

    msgpack::object* array = obj.via.array.ptr;
    uint32_t index = 0;

    // Deserialize client_id string
    std::string client_id_str = array[index++].as<std::string>();
    UUID client_id;
    StringToUuid(client_id_str, client_id);

    // Deserialize put_start_time
    uint64_t put_start_time_timestamp = array[index++].as<uint64_t>();

    // Deserialize size
    auto size = static_cast<size_t>(array[index++].as<uint64_t>());

    // Deserialize lease_timeout
    uint64_t lease_timestamp = array[index++].as<uint64_t>();

    // Deserialize soft_pin_timeout flag
    bool has_soft_pin_timeout = array[index++].as<bool>();

    // Deserialize soft_pin_timeout value
    uint64_t soft_pin_timestamp = array[index++].as<uint64_t>();

    // Deserialize replicas count
    uint32_t replicas_count = array[index++].as<uint32_t>();

    // Check if array size matches replicas_count
    if (obj.via.array.size != 7 + replicas_count) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            "deserialize ObjectMetadata array size mismatch"));
    }

    // Deserialize replicas
    std::vector<Replica> replicas;
    replicas.reserve(replicas_count);

    for (uint32_t i = 0; i < replicas_count; i++) {
        auto result = Serializer<Replica>::deserialize(
            array[index++], service_->segment_manager_.getView());
        if (!result) {
            return tl::unexpected(result.error());
        }
        replicas.emplace_back(std::move(*result.value()));
    }

    // Create ObjectMetadata instance
    bool enable_soft_pin = has_soft_pin_timeout;
    auto metadata = std::make_unique<ObjectMetadata>(
        client_id,
        std::chrono::system_clock::time_point(
            std::chrono::milliseconds(put_start_time_timestamp)),
        size, std::move(replicas), enable_soft_pin);
    metadata->lease_timeout = std::chrono::system_clock::time_point(
        std::chrono::milliseconds(lease_timestamp));

    // Set soft_pin_timeout (if exists)
    if (has_soft_pin_timeout) {
        metadata->soft_pin_timeout.emplace(
            std::chrono::system_clock::time_point(
                std::chrono::milliseconds(soft_pin_timestamp)));
    }

    return metadata;
}

std::string MasterService::FormatTimestamp(
    const std::chrono::system_clock::time_point& tp) {
    auto now = std::chrono::system_clock::now();
    auto time_t = std::chrono::system_clock::to_time_t(now);

    std::stringstream ss;
    ss << std::put_time(std::localtime(&time_t), "%Y%m%d_%H%M%S");

    // Add milliseconds to ensure uniqueness
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  now.time_since_epoch()) %
              1000;

    ss << "_" << std::setfill('0') << std::setw(3) << ms.count();

    return ss.str();
}

tl::expected<UUID, ErrorCode> MasterService::CreateCopyTask(
    const std::string& key, const std::vector<std::string>& targets) {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
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
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
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
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
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
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
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

tl::expected<void, SerializationError>
MasterService::MetadataSerializer::SerializeDiscardedReplicas(
    MsgpackPacker& packer) const {
    std::lock_guard lock(service_->discarded_replicas_mutex_);

    // Serialize as array: [count, item1, item2, ...]
    packer.pack_array(service_->discarded_replicas_.size());

    for (const auto& item : service_->discarded_replicas_) {
        // Each item: [ttl_timestamp, mem_size, replica_count, replica1,
        // replica2, ...]
        auto ttl_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          item.ttl_.time_since_epoch())
                          .count();

        packer.pack_array(3 + item.replicas_.size());
        packer.pack(ttl_ms);          // ttl timestamp
        packer.pack(item.mem_size_);  // mem_size
        packer.pack(
            static_cast<uint32_t>(item.replicas_.size()));  // replica count

        // Serialize each replica
        for (const auto& replica : item.replicas_) {
            auto result = Serializer<Replica>::serialize(
                replica, service_->segment_manager_.getView(), packer);
            if (!result) {
                return tl::unexpected(result.error());
            }
        }
    }

    return {};
}

tl::expected<void, SerializationError>
MasterService::MetadataSerializer::DeserializeDiscardedReplicas(
    const msgpack::object& obj) {
    if (obj.type != msgpack::type::ARRAY) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL, "discarded_replicas: expected array"));
    }

    std::list<DiscardedReplicas> temp_list;

    for (uint32_t i = 0; i < obj.via.array.size; ++i) {
        const msgpack::object& item_obj = obj.via.array.ptr[i];

        if (item_obj.type != msgpack::type::ARRAY ||
            item_obj.via.array.size < 3) {
            return tl::make_unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                fmt::format("Invalid discarded_replicas item at index {}: "
                            "expected array with at least 3 elements",
                            i)));
        }

        const msgpack::object* item_array = item_obj.via.array.ptr;

        // Deserialize ttl
        uint64_t ttl_ms = item_array[0].as<uint64_t>();
        auto ttl = std::chrono::system_clock::time_point(
            std::chrono::milliseconds(ttl_ms));

        // Deserialize mem_size
        uint64_t mem_size = item_array[1].as<uint64_t>();

        // Deserialize replica count
        uint32_t replica_count = item_array[2].as<uint32_t>();

        if (item_obj.via.array.size != 3 + replica_count) {
            return tl::make_unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                fmt::format(
                    "Discarded replicas item size mismatch at index {}: "
                    "expected {} elements, got {}",
                    i, 3 + replica_count, item_obj.via.array.size)));
        }

        // Deserialize replicas
        std::vector<Replica> replicas;
        replicas.reserve(replica_count);

        for (uint32_t j = 0; j < replica_count; ++j) {
            auto replica_result = Serializer<Replica>::deserialize(
                item_array[3 + j], service_->segment_manager_.getView());
            if (!replica_result) {
                return tl::make_unexpected(SerializationError(
                    ErrorCode::DESERIALIZE_FAIL,
                    fmt::format("Failed to deserialize replica {} in "
                                "discarded_replicas item {}: {}",
                                j, i, replica_result.error().message)));
            }
            replicas.emplace_back(std::move(*replica_result.value()));
        }

        // Create DiscardedReplicas and manually set mem_size_
        temp_list.emplace_back(std::move(replicas), ttl);
        // Set the deserialized mem_size
        temp_list.back().mem_size_ = mem_size;
    }

    // Move deserialized items to service's discarded_replicas_
    if (!temp_list.empty()) {
        std::lock_guard lock(service_->discarded_replicas_mutex_);
        service_->discarded_replicas_ = std::move(temp_list);
    }

    return {};
}

}  // namespace mooncake

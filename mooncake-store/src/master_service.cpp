#include "master_service.h"

#include <cassert>
#include <cstdint>
#include <regex>
#include <unordered_set>
#include <shared_mutex>
#include <ylt/util/tl/expected.hpp>
#include <ylt/struct_json/json_writer.h>

#include "allocator.h"
#include "etcd_helper.h"
#include "etcd_oplog_store.h"
#include "master_metric_manager.h"
#include "metadata_store.h"  // For MetadataPayload
#include "segment.h"
#include "types.h"
// replication_service.h removed - using etcd-based OpLog sync instead

namespace mooncake {

namespace {

// A minimal allocator implementation used only to keep AllocatedBuffer handles
// "valid" after standby promotion. It does NOT own memory.
class DummyBufferAllocator final : public BufferAllocatorBase {
   public:
    DummyBufferAllocator(std::string segment_name, std::string transport_endpoint)
        : segment_name_(std::move(segment_name)),
          transport_endpoint_(std::move(transport_endpoint)) {}

    std::unique_ptr<AllocatedBuffer> allocate(size_t /*size*/) override {
        return nullptr;
    }
    void deallocate(AllocatedBuffer* /*handle*/) override {
        // no-op: we don't own memory
    }
    size_t capacity() const override { return kAllocatorUnknownFreeSpace; }
    size_t size() const override { return 0; }
    std::string getSegmentName() const override { return segment_name_; }
    std::string getTransportEndpoint() const override { return transport_endpoint_; }
    size_t getLargestFreeRegion() const override { return kAllocatorUnknownFreeSpace; }

   private:
    std::string segment_name_;
    std::string transport_endpoint_;
};

static Replica ReplicaFromDescriptor(
    const Replica::Descriptor& desc,
    const std::shared_ptr<BufferAllocatorBase>& allocator_keepalive) {
    if (desc.is_memory_replica()) {
        const auto& mem = desc.get_memory_descriptor();
        const auto& bd = mem.buffer_descriptor;
        if (!allocator_keepalive) {
            // This would make the buffer handle invalid immediately (allocator stored
            // as weak_ptr in AllocatedBuffer). Callers restoring from standby should
            // always provide a keepalive allocator.
            LOG(ERROR) << "ReplicaFromDescriptor(memory) missing keepalive allocator, "
                       << "transport_endpoint=" << bd.transport_endpoint_;
        }

        auto buf = std::make_unique<AllocatedBuffer>(
            allocator_keepalive, reinterpret_cast<void*>(bd.buffer_address_),
            static_cast<size_t>(bd.size_));
        return Replica(std::move(buf), desc.status);
    }
    if (desc.is_disk_replica()) {
        const auto& disk = desc.get_disk_descriptor();
        return Replica(disk.file_path, disk.object_size, desc.status);
    }
    const auto& ld = desc.get_local_disk_descriptor();
    UUID client_id{ld.client_id_first, ld.client_id_second};
    return Replica(client_id, ld.object_size, ld.transport_endpoint, desc.status);
}

}  // namespace

MasterService::MasterService() : MasterService(MasterServiceConfig()) {}

std::string MasterService::SerializeMetadataForOpLog(const ObjectMetadata& metadata) const {
    MetadataPayload payload;
    payload.client_id_first = metadata.client_id.first;
    payload.client_id_second = metadata.client_id.second;
    payload.size = metadata.size;
    
    // Extract replica descriptors
    payload.replicas.reserve(metadata.replicas.size());
    for (const auto& replica : metadata.replicas) {
        payload.replicas.push_back(replica.get_descriptor());
    }
    
    // NOTE: Lease information is NOT serialized because:
    // 1. Standby does not perform eviction, so lease info is not used
    // 2. After promotion, new Primary should grant fresh leases, not restore old ones
    
    // Serialize to JSON
    std::string json_str;
    struct_json::to_json(payload, json_str);
    return json_str;
}

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
      segment_manager_(config.memory_allocator),
      memory_allocator_type_(config.memory_allocator),
      allocation_strategy_(std::make_shared<RandomAllocationStrategy>()),
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
    eviction_thread_ = std::thread(&MasterService::EvictionThreadFunc, this);
    VLOG(1) << "action=start_eviction_thread";

    // Start client monitor thread in all modes so TTL/heartbeat works
    client_monitor_running_ = true;
    client_monitor_thread_ =
        std::thread(&MasterService::ClientMonitorFunc, this);
    VLOG(1) << "action=start_client_monitor_thread";

    if (!root_fs_dir_.empty()) {
        use_disk_replica_ = true;
        MasterMetricManager::instance().inc_total_file_capacity(
            global_file_segment_size_);
    }

    // Initialize EtcdOpLogStore if HA is enabled
    // Note: This requires STORE_USE_ETCD to be enabled at compile time
    // Note: etcd connection should be established before MasterService construction
    // (e.g., in MasterServiceSupervisor), so we can use the existing connection
#ifdef STORE_USE_ETCD
    if (enable_ha_ && !cluster_id_.empty()) {
        // Try to create EtcdOpLogStore - if etcd is not connected, operations will fail
        // but we can still use memory buffer as fallback
        // Writer: enable batch update for `/latest` to reduce etcd write pressure.
        auto etcd_oplog_store =
            std::make_shared<EtcdOpLogStore>(cluster_id_, /*enable_latest_seq_batch_update=*/true);
        oplog_manager_.SetEtcdOpLogStore(etcd_oplog_store);
        LOG(INFO) << "EtcdOpLogStore initialized for cluster_id="
                  << cluster_id_ << " (etcd connection should be established "
                  << "before MasterService construction)";
    } else if (enable_ha_) {
        LOG(WARNING) << "HA mode enabled but cluster_id is empty, "
                        "OpLog will only be stored in memory buffer";
    }
#else
    if (enable_ha_) {
        LOG(WARNING) << "HA mode enabled but STORE_USE_ETCD is not enabled at "
                        "compile time, OpLog will only be stored in memory buffer. "
                        "Recompile with -DSTORE_USE_ETCD=ON to enable etcd support.";
    }
#endif
}

// Helper function to append OpLog entry
// Note: In the new etcd-based design, OpLog will be written to etcd by EtcdOpLogStore
// This method only appends to OpLogManager's buffer for now
void MasterService::AppendOpLogAndNotify(OpType type, const std::string& key,
                                         const std::string& payload) {
    oplog_manager_.Append(type, key, payload);
    // TODO: In Phase 1, integrate with EtcdOpLogStore to write to etcd
}

void MasterService::RestoreFromStandbySnapshot(
    const std::vector<std::pair<std::string, StandbyObjectMetadata>>& snapshot,
    uint64_t initial_oplog_sequence_id) {
    // 1) Ensure OpLog sequence continues without regression after failover.
    oplog_manager_.SetInitialSequenceId(initial_oplog_sequence_id);

    // 2) Restore metadata entries.
    // Keep dummy allocators alive for restored memory replicas. AllocatedBuffer
    // only holds a weak_ptr to allocator, so without this keepalive map the
    // allocator would expire immediately and transport_endpoint_ would be lost.
    standby_allocator_keepalive_.clear();
    auto get_keepalive_allocator =
        [this](const std::string& transport_endpoint)
        -> std::shared_ptr<BufferAllocatorBase> {
        auto it = standby_allocator_keepalive_.find(transport_endpoint);
        if (it != standby_allocator_keepalive_.end()) {
            return it->second;
        }
        auto alloc = std::make_shared<DummyBufferAllocator>(
            /*segment_name=*/std::string(), transport_endpoint);
        standby_allocator_keepalive_.emplace(transport_endpoint, alloc);
        return alloc;
    };

    const auto now = std::chrono::steady_clock::now();
    size_t restored = 0;
    for (const auto& kv : snapshot) {
        const std::string& key = kv.first;
        const StandbyObjectMetadata& sm = kv.second;

        std::vector<Replica> replicas;
        replicas.reserve(sm.replicas.size());
        for (const auto& rd : sm.replicas) {
            if (rd.is_memory_replica()) {
                const auto& bd = rd.get_memory_descriptor().buffer_descriptor;
                replicas.emplace_back(
                    ReplicaFromDescriptor(rd, get_keepalive_allocator(bd.transport_endpoint_)));
            } else {
                replicas.emplace_back(ReplicaFromDescriptor(rd, nullptr));
            }
        }

        // NOTE: Lease information is NOT restored because:
        // 1. Standby does not use lease info (no eviction)
        // 2. New Primary should grant fresh leases after promotion
        // 3. Restoring old lease TTLs could cause immediate eviction if they're expired
        const bool enable_soft_pin = false;  // Will be set by new Primary if needed

        const size_t shard_idx = getShardIndex(key);
        MutexLocker lock(&metadata_shards_[shard_idx].mutex);

        // Overwrite existing key if any.
        metadata_shards_[shard_idx].metadata.erase(key);
        auto [it, inserted] = metadata_shards_[shard_idx].metadata.emplace(
            std::piecewise_construct, std::forward_as_tuple(key),
            std::forward_as_tuple(sm.client_id, now, static_cast<size_t>(sm.size),
                                  std::move(replicas), enable_soft_pin));
        (void)inserted;

        // Lease will be granted by new Primary when objects are accessed
        // (via GetReplicaList, ExistKey, etc.)

        // Objects restored from PUT_END are expected to be completed.
        metadata_shards_[shard_idx].processing_keys.erase(key);

        restored++;
    }

    LOG(INFO) << "Restored metadata from standby snapshot: restored_keys="
              << restored << ", initial_oplog_sequence_id=" << initial_oplog_sequence_id;
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
            // Note: LEASE_RENEW is not recorded in OpLog since Standby does not
            // perform eviction. Standby will receive DELETE events from Primary
            // when objects are evicted.
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
        MetadataAccessor accessor(this, key);
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
            if (!metadata.IsAllReplicasComplete()) {
                LOG(WARNING) << "BatchReplicaClear: key=" << key
                             << " has incomplete replicas, skipping";
                continue;
            }

            // Before erasing, decrement cache metrics for each COMPLETE replica
            for (const auto& replica : metadata.replicas) {
                if (replica.status() == ReplicaStatus::COMPLETE) {
                    if (replica.is_memory_replica()) {
                        MasterMetricManager::instance().dec_mem_cache_nums();
                    } else if (replica.is_disk_replica()) {
                        MasterMetricManager::instance().dec_file_cache_nums();
                    }
                }
            }

            // Erase the entire metadata (all replicas will be deallocated)
            accessor.Erase();
            cleared_keys.emplace_back(key);
            VLOG(1) << "BatchReplicaClear: successfully cleared all replicas "
                       "for key="
                    << key << " for client_id=" << client_id;
        } else {
            // Clear only replicas on the specified segment_name
            bool has_replica_on_segment = false;
            std::vector<size_t> replicas_to_remove;

            for (size_t i = 0; i < metadata.replicas.size(); ++i) {
                const auto& replica = metadata.replicas[i];
                if (replica.status() != ReplicaStatus::COMPLETE) {
                    continue;
                }
                auto segment_names = replica.get_segment_names();
                for (const auto& seg_name : segment_names) {
                    if (seg_name.has_value() &&
                        seg_name.value() == segment_name) {
                        has_replica_on_segment = true;
                        replicas_to_remove.emplace_back(i);
                        break;
                    }
                }
            }

            if (!has_replica_on_segment) {
                LOG(WARNING)
                    << "BatchReplicaClear: key=" << key
                    << " has no replica on segment_name=" << segment_name
                    << ", skipping";
                continue;
            }

            // Remove replicas on the specified segment (in reverse order to
            // maintain indices)
            for (auto it = replicas_to_remove.rbegin();
                 it != replicas_to_remove.rend(); ++it) {
                size_t idx = *it;
                const auto& replica = metadata.replicas[idx];

                if (replica.is_memory_replica()) {
                    MasterMetricManager::instance().dec_mem_cache_nums();
                } else if (replica.is_disk_replica()) {
                    MasterMetricManager::instance().dec_file_cache_nums();
                }
                metadata.replicas.erase(metadata.replicas.begin() + idx);
            }

            // If no valid replicas remain, erase the entire metadata
            if (metadata.replicas.empty() || !metadata.IsValid()) {
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
                // Note: LEASE_RENEW is not recorded in OpLog since Standby does not
                // perform eviction. Standby will receive DELETE events from Primary
                // when objects are evicted.
            }
        }
    }

    return results;
}

auto MasterService::GetReplicaList(std::string_view key)
    -> tl::expected<GetReplicaListResponse, ErrorCode> {
    MetadataAccessor accessor(this, std::string(key));

    MasterMetricManager::instance().inc_total_get_nums();

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

    if (replica_list[0].is_memory_replica()) {
        MasterMetricManager::instance().inc_mem_cache_hit_nums();
    } else if (replica_list[0].is_disk_replica()) {
        MasterMetricManager::instance().inc_file_cache_hit_nums();
    }
    MasterMetricManager::instance().inc_valid_get_nums();
    // Grant a lease to the object so it will not be removed
    // when the client is reading it.
    metadata.GrantLease(default_kv_lease_ttl_, default_kv_soft_pin_ttl_);
    // Note: LEASE_RENEW is not recorded in OpLog since Standby does not
    // perform eviction. Standby will receive DELETE events from Primary
    // when objects are evicted.

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
        ScopedAllocatorAccess allocator_access =
            segment_manager_.getAllocatorAccess();
        const auto& allocator_manager = allocator_access.getAllocatorManager();

        std::vector<std::string> preferred_segments;
        if (!config.preferred_segment.empty()) {
            preferred_segments.push_back(config.preferred_segment);
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
    metadata_shards_[shard_idx].metadata.emplace(
        std::piecewise_construct, std::forward_as_tuple(key),
        std::forward_as_tuple(client_id, now, total_length, std::move(replicas),
                              config.with_soft_pin));
    // Also insert the metadata into processing set for monitoring.
    metadata_shards_[shard_idx].processing_keys.insert(key);

    return replica_list;
}

auto MasterService::PutEnd(const UUID& client_id, const std::string& key,
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

    // Record OpLog entry for PUT_END so that standbys can replay this change.
    // Serialize metadata (replicas, size, lease) to payload so Standby can restore
    // complete metadata when promoted to Primary.
    std::string metadata_payload = SerializeMetadataForOpLog(metadata);
    AppendOpLogAndNotify(OpType::PUT_END, key, metadata_payload);

    return {};
}

auto MasterService::AddReplica(const UUID& client_id, const std::string& key,
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
            if (descriptor.GetClientId() == client_id) {
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

auto MasterService::PutRevoke(const UUID& client_id, const std::string& key,
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

    // Log the revoke operation so that standbys can roll back their metadata.
    AppendOpLogAndNotify(OpType::PUT_REVOKE, key);

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

    // Log explicit remove so that standbys can delete the same key.
    AppendOpLogAndNotify(OpType::REMOVE, key);

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

        // Only remove completed objects with expired leases
        auto it = shard.metadata.begin();
        while (it != shard.metadata.end()) {
            if (it->second.IsLeaseExpired(now) &&
                it->second.IsAllReplicasComplete()) {
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
        LOG(ERROR) << "Local disk segment not fount with client id = "
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

void MasterService::DiscardExpiredProcessingKeys(
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

OpLogManager& MasterService::GetOpLogManager() {
    return oplog_manager_;
}

// SetReplicationService removed - using etcd-based OpLog sync instead

}  // namespace mooncake
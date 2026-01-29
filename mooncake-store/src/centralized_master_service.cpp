#include "centralized_master_service.h"

#include <cassert>
#include <cstdint>
#include <random>
#include <ylt/util/tl/expected.hpp>

#include "master_metric_manager.h"
#include "mutex.h"
#include "types.h"

namespace mooncake {

// =========== CentralizedObjectMetadata implementation ===========
CentralizedMasterService::CentralizedObjectMetadata::
    ~CentralizedObjectMetadata() {
    if (soft_pin_timeout_) {
        MasterMetricManager::instance().dec_soft_pin_key_count(1);
    }
}

CentralizedMasterService::CentralizedObjectMetadata::CentralizedObjectMetadata(
    const UUID& client_id,
    const std::chrono::steady_clock::time_point put_start_time,
    size_t value_length, std::vector<Replica>&& reps, bool enable_soft_pin)
    : ObjectMetadata(value_length, std::move(reps)),
      owner_client_id_(client_id),
      put_start_time_(put_start_time),
      lease_timeout_(),
      soft_pin_timeout_(std::nullopt),
      replication_task_cnt_(0) {
    if (enable_soft_pin) {
        soft_pin_timeout_.emplace();
        MasterMetricManager::instance().inc_soft_pin_key_count(1);
    }
}

std::optional<ReplicaStatus>
CentralizedMasterService::CentralizedObjectMetadata::HasDiffRepStatus(
    ReplicaStatus status, ReplicaType replica_type) const {
    for (const auto& replica : replicas_) {
        if (replica.status() != status && replica.type() == replica_type) {
            return replica.status();
        }
    }
    return std::nullopt;
}

void CentralizedMasterService::CentralizedObjectMetadata::GrantLease(
    const uint64_t ttl, const uint64_t soft_ttl) const {
    SpinLocker locker(&lock_);
    std::chrono::steady_clock::time_point now =
        std::chrono::steady_clock::now();
    lease_timeout_ =
        std::max(lease_timeout_, now + std::chrono::milliseconds(ttl));
    if (soft_pin_timeout_) {
        soft_pin_timeout_ = std::max(*soft_pin_timeout_,
                                     now + std::chrono::milliseconds(soft_ttl));
    }
}

bool CentralizedMasterService::CentralizedObjectMetadata::IsLeaseExpired()
    const {
    SpinLocker locker(&lock_);
    return std::chrono::steady_clock::now() >= lease_timeout_;
}

bool CentralizedMasterService::CentralizedObjectMetadata::IsLeaseExpired(
    const std::chrono::steady_clock::time_point& now) const {
    SpinLocker locker(&lock_);
    return now >= lease_timeout_;
}

bool CentralizedMasterService::CentralizedObjectMetadata::IsSoftPinned() const {
    SpinLocker locker(&lock_);
    return soft_pin_timeout_ &&
           std::chrono::steady_clock::now() < *soft_pin_timeout_;
}

bool CentralizedMasterService::CentralizedObjectMetadata::IsSoftPinned(
    const std::chrono::steady_clock::time_point& now) const {
    SpinLocker locker(&lock_);
    return soft_pin_timeout_ && now < *soft_pin_timeout_;
}

// Hook Implementations
tl::expected<void, ErrorCode>
CentralizedMasterService::CentralizedObjectMetadata::IsObjectRemovable(
    bool force) const {
    if (!force && !IsLeaseExpired()) {
        return tl::make_unexpected(ErrorCode::OBJECT_HAS_LEASE);
    }

    if (!AllReplicas(&Replica::fn_is_completed)) {
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }

    if (replication_task_cnt_ > 0) {
        return tl::make_unexpected(ErrorCode::OBJECT_HAS_REPLICATION_TASK);
    }
    return {};
}

bool CentralizedMasterService::CentralizedObjectMetadata::IsReplicaAccessible(
    const Replica& replica) const {
    return replica.status() == ReplicaStatus::COMPLETE;
}

tl::expected<void, ErrorCode>
CentralizedMasterService::CentralizedObjectMetadata::IsReplicaRemovable(
    const Replica& replica) const {
    if (!IsLeaseExpired()) {
        // TODO: wanyue-wy
        // Discuss this condition with community:
        // Following old logic of main branch, in BatchReplicaClear(),
        // it clears a replica only when the object has no lease.
        // Thus, we just simply follow it here.
        // However, we think this is unreasonable.
        // It might happen that we want to evict a replica, but the object has
        // lease, because the lease is key level rather than replica level in
        // current implementation.
        return tl::make_unexpected(ErrorCode::OBJECT_HAS_LEASE);
    }
    if (replica.status() != ReplicaStatus::COMPLETE) {
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }
    return {};
}

// ================= CentralizedMasterService implementation =================
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
      client_manager_(config.client_live_ttl_sec, config.client_crashed_ttl_sec,
                      config.memory_allocator, view_version_,
                      config.enable_cxl, config.cxl_path, config.cxl_size),
      memory_allocator_type_(config.memory_allocator),
      put_start_discard_timeout_sec_(config.put_start_discard_timeout_sec),
      put_start_release_timeout_sec_(config.put_start_release_timeout_sec),
      task_manager_(config.task_manager_config) {
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

    task_cleanup_running_ = true;
    task_cleanup_thread_ =
        std::thread(&CentralizedMasterService::TaskCleanupThreadFunc, this);
    VLOG(1) << "action=start_task_cleanup_thread";

    if (!root_fs_dir_.empty()) {
        use_disk_replica_ = true;
        MasterMetricManager::instance().inc_total_file_capacity(
            global_file_segment_size_);
    }

    InitializeClientManager();

    client_manager_.Start();
}

CentralizedMasterService::~CentralizedMasterService() {
    eviction_running_ = false;
    task_cleanup_running_ = false;

    // Wake sleepers so join() doesn't block for long sleep intervals.
    task_cleanup_cv_.notify_all();

    if (eviction_thread_.joinable()) {
        eviction_thread_.join();
    }
    if (task_cleanup_thread_.joinable()) {
        task_cleanup_thread_.join();
    }
}

auto CentralizedMasterService::BatchReplicaClear(
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
        CentralizedMetadataAccessor accessor(this, key);
        if (!accessor.Exists()) {
            LOG(WARNING) << "BatchReplicaClear: key=" << key
                         << " not found, skipping";
            continue;
        }

        auto& metadata = accessor.Get();

        // Security check: Ensure the requesting client owns the object.
        if (metadata.owner_client_id_ != client_id) {
            LOG(WARNING) << "BatchReplicaClear: key=" << key
                         << " belongs to different owner_client_id="
                         << metadata.owner_client_id_
                         << ", expected=" << client_id << ", skipping";
            continue;
        }

        if (clear_all_segments) {
            if (auto res = metadata.IsObjectRemovable(); !res) {
                LOG(WARNING) << "BatchReplicaClear: key=" << key
                             << " cannot be removed, reason=" << res.error();
                continue;
            }
            OnObjectRemoved(metadata);

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
                if (auto res = metadata.IsReplicaRemovable(replica); !res) {
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

            auto& shard = accessor.GetShard().GetRef();
            metadata.VisitReplicas(
                match_replica_on_segment, [&](Replica& replica) {
                    has_replica_on_segment = true;
                    RemoveReplicaFromSegmentIndex(shard, key, replica);
                    OnReplicaRemoved(replica);
                });

            if (!has_replica_on_segment) {
                LOG(WARNING)
                    << "BatchReplicaClear: key=" << key
                    << " has no replica on segment_name=" << segment_name
                    << ", skipping";
                continue;
            }

            metadata.EraseReplicas(match_replica_on_segment);

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

void CentralizedMasterService::OnObjectAccessed(
    const ObjectMetadata& metadata) {
    const auto& c_metadata =
        static_cast<const CentralizedObjectMetadata&>(metadata);
    c_metadata.GrantLease(default_kv_lease_ttl_, default_kv_soft_pin_ttl_);
}

void CentralizedMasterService::OnObjectHit(const ObjectMetadata& metadata) {
    auto& replica = metadata.replicas_[0];
    if (replica.is_memory_replica()) {
        MasterMetricManager::instance().inc_mem_cache_hit_nums();
    } else if (replica.is_disk_replica()) {
        MasterMetricManager::instance().inc_file_cache_hit_nums();
    }
    MasterMetricManager::instance().inc_valid_get_nums();
}

void CentralizedMasterService::OnReplicaRemoved(const Replica& replica) {
    if (replica.status() == ReplicaStatus::COMPLETE) {
        if (replica.is_memory_replica()) {
            MasterMetricManager::instance().dec_mem_cache_nums();
        } else if (replica.is_disk_replica()) {
            MasterMetricManager::instance().dec_file_cache_nums();
        }
    }
}

void CentralizedMasterService::OnReplicaAdded(const Replica& replica) {
    if (replica.status() == ReplicaStatus::COMPLETE) {
        if (replica.is_memory_replica()) {
            MasterMetricManager::instance().inc_mem_cache_nums();
        } else if (replica.is_disk_replica()) {
            MasterMetricManager::instance().inc_file_cache_nums();
        }
    }
}

auto CentralizedMasterService::GetReplicaList(
    std::string_view key, const GetReplicaListRequestConfig& config)
    -> tl::expected<GetReplicaListResponse, ErrorCode> {
    auto res = MasterService::GetReplicaList(key, config);
    if (!res) {
        LOG(ERROR) << "Failed to get replica list for key: " << key;
        return res;
    }

    if (!res->centralized_extra) {
        res->centralized_extra.emplace(default_kv_lease_ttl_);
    } else {
        res->centralized_extra->lease_ttl_ms = default_kv_lease_ttl_;
    }
    return res;
}

std::vector<Replica::Descriptor> CentralizedMasterService::FilterReplicas(
    const GetReplicaListRequestConfig& config, const ObjectMetadata& metadata) {
    // 1. fillter qualified replicas
    std::vector<Replica::Descriptor> candidates;
    candidates.reserve(metadata.replicas_.size());
    for (const auto& replica : metadata.replicas_) {
        if (metadata.IsReplicaAccessible(replica)) {
            candidates.emplace_back(replica.get_descriptor());
        }
    }

    if (config.max_candidates ==
            GetReplicaListRequestConfig::RETURN_ALL_CANDIDATES ||
        config.max_candidates >= candidates.size() || candidates.empty()) {
        return candidates;
    }

    // 2. the number of qualified replicas is larger than limit,
    // choose the best ones.
    // Priority: Memory > LocalDisk > others
    std::vector<Replica::Descriptor> sorted_candidates = std::move(candidates);
    auto get_priority = [](ReplicaType type) {
        if (type == ReplicaType::MEMORY) return 2;
        if (type == ReplicaType::LOCAL_DISK) return 1;
        return 0;
    };

    std::sort(sorted_candidates.begin(), sorted_candidates.end(),
              [&](const auto& a, const auto& b) {
                  return get_priority(a.type()) > get_priority(b.type());
              });

    sorted_candidates.resize(config.max_candidates);
    return sorted_candidates;
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
    MetadataShardAccessorRW shard_rw(this, GetShardIndex(key));
    auto& shard = static_cast<CentralizedMetadataShard&>(shard_rw.GetRef());

    const auto now = std::chrono::steady_clock::now();
    auto it = shard.metadata.find(key);
    if (it != shard.metadata.end()) {
        auto* metadata =
            static_cast<CentralizedObjectMetadata*>(it->second.get());
        // If the object's PutStart expired and has not completed any
        // replicas, we can discard it and allow the new PutStart to
        // go.
        if (!metadata->HasReplica(&Replica::fn_is_completed) &&
            metadata->put_start_time_ + put_start_discard_timeout_sec_ < now) {
            auto replicas = metadata->PopReplicas(&Replica::fn_is_processing);
            if (!replicas.empty()) {
                MutexLocker lock(&discarded_replicas_mutex_);
                discarded_replicas_.emplace_back(
                    std::move(replicas),
                    metadata->put_start_time_ + put_start_release_timeout_sec_);
            }
            shard.processing_keys.erase(key);
            shard.metadata.erase(it);
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
        } else if (!config.preferred_segments.empty()) {
            preferred_segments = config.preferred_segments;
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

    // Create Metadata using unique_ptr
    auto metadata = std::make_unique<CentralizedObjectMetadata>(
        client_id, now, total_length, std::move(replicas),
        config.with_soft_pin);

    // No need to set lease here. The object will not be evicted until
    // PutEnd is called.
    shard.metadata.emplace(std::piecewise_construct, std::forward_as_tuple(key),
                           std::forward_as_tuple(std::move(metadata)));
    // Also insert the metadata into processing set for monitoring.
    shard.processing_keys.insert(key);

    return replica_list;
}

auto CentralizedMasterService::PutEnd(const UUID& client_id,
                                      const std::string& key,
                                      ReplicaType replica_type)
    -> tl::expected<void, ErrorCode> {
    CentralizedMetadataAccessor accessor(this, key);
    if (!accessor.Exists()) {
        LOG(ERROR) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto& metadata = accessor.Get();
    if (client_id != metadata.owner_client_id_) {
        LOG(ERROR) << "Illegal client " << client_id << " to PutEnd key " << key
                   << ", was PutStart-ed by " << metadata.owner_client_id_;
        return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
    }

    metadata.VisitReplicas(
        [replica_type](const Replica& replica) {
            return replica.type() == replica_type;
        },
        [&](Replica& replica) {
            replica.mark_complete();
            AddReplicaToSegmentIndex(accessor.GetShard().GetRef(),
                                     accessor.GetKey(), replica);
            OnReplicaAdded(replica);
        });

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
    CentralizedMetadataAccessor accessor(this, key);
    if (!accessor.Exists()) {
        LOG(INFO) << "key=" << key << ", info=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto& metadata = accessor.Get();
    if (client_id != metadata.owner_client_id_) {
        LOG(ERROR) << "Illegal client " << client_id << " to PutRevoke key "
                   << key << ", was PutStart-ed by "
                   << metadata.owner_client_id_;
        return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
    }

    auto non_processing =
        metadata.GetFirstReplica([replica_type](const Replica& replica) {
            return replica.type() == replica_type && !replica.is_processing();
        });
    if (non_processing != nullptr) {
        LOG(ERROR) << "key=" << key << ", status=" << non_processing->status()
                   << ", error=invalid_replica_status";
        return tl::make_unexpected(ErrorCode::INVALID_WRITE);
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
    CentralizedMetadataAccessor accessor(this, key);
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
        [&client_id](const Replica& rep) {
            return rep.type() == ReplicaType::LOCAL_DISK &&
                   rep.get_descriptor().get_local_disk_descriptor().client_id ==
                       client_id;
        },
        [&replica](Replica& rep) {
            // TODO: wanyue-wy
            // The modification of `descriptor` could not take effect in the
            // `metadata.replicas_`.
            // The bug also exists in the main branch of Mooncake.
            // When merge the P2P development branch to the main branch,
            // we should check whether this bug is fixed.
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

tl::expected<std::string, ErrorCode> CentralizedMasterService::GetFsdir()
    const {
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
        LOG(ERROR) << "The offload functionality is not enabled";
        return tl::make_unexpected(ErrorCode::UNABLE_OFFLOAD);
    }

    auto ret =
        client_manager_.MountLocalDiskSegment(client_id, enable_offloading);
    if (!ret.has_value()) {
        LOG(ERROR) << "failed to mount local disk segment"
                      ", client_id="
                   << client_id << ", ret=" << ret.error();
        return ret;
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
        auto ret = client_manager_.PushOffloadingQueue(key, size,
                                                       segment_name_it.value());
        if (!ret.has_value()) {
            LOG(ERROR) << "failed to push offloading queue"
                       << ", key=" << key << ", size=" << size
                       << ", segment_name=" << segment_name_it.value()
                       << ", ret=" << ret.error();
            return ret;
        }
    }
    return {};
}

void CentralizedMasterService::EvictionThreadFunc() {
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
            for (size_t i = 0; i < kNumShards; ++i) {
                MetadataShardAccessorRW shard_rw(this, i);
                auto& shard =
                    static_cast<CentralizedMetadataShard&>(shard_rw.GetRef());
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

void CentralizedMasterService::DiscardExpiredProcessingReplicas(
    CentralizedMetadataShard& shard,
    const std::chrono::steady_clock::time_point& now) {
    std::list<DiscardedReplicas> discarded_replicas;

    // Part 1: Discard expired PutStart operations.
    auto& processing_set = shard.processing_keys;
    for (auto key_it = processing_set.begin();
         key_it != processing_set.end();) {
        auto it = shard.metadata.find(*key_it);
        if (it == shard.metadata.end()) {
            // The key has been removed from metadata. This should be
            // impossible.
            LOG(ERROR) << "Key " << *key_it
                       << " was removed while in processing";
            key_it = processing_set.erase(key_it);
            continue;
        }

        auto* metadata =
            static_cast<CentralizedObjectMetadata*>(it->second.get());
        // If the object is not valid or not in processing state, just
        // remove it from the processing set.
        if (!metadata->IsValid() ||
            metadata->AllReplicas(&Replica::fn_is_completed)) {
            if (!metadata->IsValid()) {
                shard.metadata.erase(it);
            }
            key_it = processing_set.erase(key_it);
            continue;
        }

        // If the object's PutStart timedout, discard and release it's
        // space. Note that instead of releasing the space directly, we
        // insert the replicas into the discarded list so that the
        // discarding and releasing operations can be recorded in
        // statistics.
        const auto ttl =
            metadata->put_start_time_ + put_start_release_timeout_sec_;
        if (ttl < now) {
            auto replicas = metadata->PopReplicas(&Replica::fn_is_processing);
            if (!replicas.empty()) {
                discarded_replicas.emplace_back(std::move(replicas), ttl);
            }

            if (!metadata->IsValid()) {
                // All replicas of this object are discarded, just
                // remove the whole object.
                shard.metadata.erase(it);
            }

            key_it = processing_set.erase(key_it);
            continue;
        }

        key_it++;
    }

    // Part 2: Discard expired CopyStart/MoveStart operations.
    for (auto task_it = shard.replication_tasks.begin();
         task_it != shard.replication_tasks.end();) {
        auto meta_it = shard.metadata.find(task_it->first);
        if (meta_it == shard.metadata.end()) {
            // The key has been removed from metadata. This should be
            // impossible.
            LOG(ERROR) << "Key " << task_it->first
                       << " was removed with ongoing replication task";
            task_it = shard.replication_tasks.erase(task_it);
            continue;
        }

        const auto ttl =
            task_it->second.start_time + put_start_release_timeout_sec_;
        if (ttl > now) {
            ++task_it;
            continue;
        }

        auto* metadata =
            static_cast<CentralizedObjectMetadata*>(meta_it->second.get());

        // Release source refcnt.
        auto source = metadata->GetReplicaByID(task_it->second.source_id);
        if (source != nullptr) {
            source->dec_refcnt();
        }

        // Discard allocated target replicas.
        const auto& replica_ids = task_it->second.replica_ids;
        auto replicas =
            metadata->PopReplicas([&replica_ids](const Replica& replica) {
                auto it = std::find(replica_ids.begin(), replica_ids.end(),
                                    replica.id());
                return it != replica_ids.end();
            });
        if (!replicas.empty()) {
            discarded_replicas.emplace_back(std::move(replicas), ttl);
        }

        // Decrement counter before erasing task.
        metadata->replication_task_cnt_--;

        if (!metadata->IsValid()) {
            shard.metadata.erase(meta_it);
        }

        task_it = shard.replication_tasks.erase(task_it);
    }

    if (!discarded_replicas.empty()) {
        MutexLocker lock(&discarded_replicas_mutex_);
        discarded_replicas_.splice(discarded_replicas_.end(),
                                   std::move(discarded_replicas));
    }
}

uint64_t CentralizedMasterService::ReleaseExpiredDiscardedReplicas(
    const std::chrono::steady_clock::time_point& now) {
    uint64_t released_cnt = 0;
    MutexLocker lock(&discarded_replicas_mutex_);
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

    auto can_evict_replicas = [](const CentralizedObjectMetadata& metadata) {
        return metadata.HasReplica([](const Replica& replica) {
            return replica.is_memory_replica() && replica.is_completed() &&
                   replica.get_refcnt() == 0;
        });
    };

    // Evict qualifying replicas, removing from segment index first.
    // Returns count of evicted replicas.
    auto evict_replicas = [this](CentralizedObjectMetadata& metadata,
                                 CentralizedMetadataShard& shard,
                                 const std::string& key) -> size_t {
        auto could_remove = [](const Replica& replica) {
            return replica.is_memory_replica() && replica.is_completed() &&
                   replica.get_refcnt() == 0;
        };
        for (const auto& replica : metadata.replicas_) {
            if (could_remove(replica)) {
                RemoveReplicaFromSegmentIndex(shard, key, replica);
                OnReplicaRemoved(replica);
            }
        }
        return metadata.EraseReplicas([&could_remove](const Replica& replica) {
            return could_remove(replica);
        });
    };

    // Randomly select a starting shard to avoid imbalance eviction between
    // shards. No need to use expensive random_device here.
    size_t start_idx = rand() % GetShardCount();

    // First pass: evict objects without soft pin and lease expired
    for (size_t i = 0; i < GetShardCount(); i++) {
        size_t current_idx = (start_idx + i) % GetShardCount();
        MetadataShardAccessorRW shard_rw(this, current_idx);
        auto& shard = static_cast<CentralizedMetadataShard&>(shard_rw.GetRef());

        // Discard expired processing keys first so that they won't be counted
        // in later evictions.
        DiscardExpiredProcessingReplicas(shard, now);

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
            auto* metadata =
                static_cast<CentralizedObjectMetadata*>(it->second.get());
            // Skip objects that are not expired or have no evictable replicas
            if (!metadata->IsLeaseExpired(now) ||
                !can_evict_replicas(*metadata)) {
                continue;
            }
            if (!metadata->IsSoftPinned(now)) {
                if (ideal_evict_num > 0) {
                    // first pass candidates
                    candidates.push_back(metadata->lease_timeout_);
                } else {
                    // No need to evict any object in this shard, put to
                    // second pass candidates
                    no_pin_objects.push_back(metadata->lease_timeout_);
                }
            } else if (allow_evict_soft_pinned_objects_) {
                // second pass candidates, only if
                // allow_evict_soft_pinned_objects_ is true
                soft_pin_objects.push_back(metadata->lease_timeout_);
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
                auto* metadata =
                    static_cast<CentralizedObjectMetadata*>(it->second.get());
                // Skip objects that are not allowed to be evicted in the first
                // pass
                if (!metadata->IsLeaseExpired(now) ||
                    metadata->IsSoftPinned(now) ||
                    !can_evict_replicas(*metadata)) {
                    ++it;
                    continue;
                }
                if (metadata->lease_timeout_ <= target_timeout) {
                    // Evict this object
                    total_freed_size +=
                        metadata->size_ *
                        evict_replicas(*metadata, shard, it->first);
                    if (metadata->IsValid() == false) {
                        it = shard.metadata.erase(it);
                    } else {
                        ++it;
                    }
                    shard_evicted_count++;
                } else {
                    // second pass candidates
                    no_pin_objects.push_back(metadata->lease_timeout_);
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
            for (size_t i = 0; i < GetShardCount() && target_evict_num > 0;
                 i++) {
                size_t current_idx = (start_idx + i) % GetShardCount();
                MetadataShardAccessorRW shard_rw(this, current_idx);
                auto& shard =
                    static_cast<CentralizedMetadataShard&>(shard_rw.GetRef());
                auto it = shard.metadata.begin();
                while (it != shard.metadata.end() && target_evict_num > 0) {
                    auto* metadata = static_cast<CentralizedObjectMetadata*>(
                        it->second.get());
                    if (metadata->lease_timeout_ <= target_timeout &&
                        !metadata->IsSoftPinned(now) &&
                        can_evict_replicas(*metadata)) {
                        // Evict this object
                        total_freed_size +=
                            metadata->size_ *
                            evict_replicas(*metadata, shard,
                                           it->first);  // Erase memory replicas
                        if (metadata->IsValid() == false) {
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
            for (size_t i = 0; i < GetShardCount() && target_evict_num > 0;
                 i++) {
                size_t current_idx = (start_idx + i) % GetShardCount();
                MetadataShardAccessorRW shard_rw(this, current_idx);
                auto& shard =
                    static_cast<CentralizedMetadataShard&>(shard_rw.GetRef());

                auto it = shard.metadata.begin();
                while (it != shard.metadata.end() && target_evict_num > 0) {
                    auto* metadata = static_cast<CentralizedObjectMetadata*>(
                        it->second.get());
                    // Skip objects that are not expired or have no evictable
                    // replicas
                    if (!metadata->IsLeaseExpired(now) ||
                        !can_evict_replicas(*metadata)) {
                        ++it;
                        continue;
                    }
                    // Evict objects with 1). no soft pin OR 2). with soft pin
                    // and lease timeout less than or equal to target.
                    if (!metadata->IsSoftPinned(now) ||
                        metadata->lease_timeout_ <= soft_target_timeout) {
                        total_freed_size +=
                            metadata->size_ *
                            evict_replicas(*metadata, shard,
                                           it->first);  // Erase memory replicas
                        if (metadata->IsValid() == false) {
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
    VLOG(1) << "action=evict_objects"
            << ", evicted_count=" << evicted_count
            << ", total_freed_size=" << total_freed_size;
}

void CentralizedMasterService::OnSegmentRemoved(const UUID& segment_id) {
    // 1. Call base implementation to clean up metadata references
    MasterService::OnSegmentRemoved(segment_id);

    for (size_t i = 0; i < kNumShards; ++i) {
        MetadataShardAccessorRW shard_rw(this, i);
        auto& c_shard =
            static_cast<CentralizedMetadataShard&>(shard_rw.GetRef());

        // 2. Clean up dangling replication tasks
        for (auto task_it = c_shard.replication_tasks.begin();
             task_it != c_shard.replication_tasks.end();) {
            if (c_shard.metadata.find(task_it->first) ==
                c_shard.metadata.end()) {
                task_it = c_shard.replication_tasks.erase(task_it);
            } else {
                ++task_it;
            }
        }

        // 3. Clean up dangling processing keys
        auto it = c_shard.processing_keys.begin();
        while (it != c_shard.processing_keys.end()) {
            auto meta_it = c_shard.metadata.find(*it);
            if (meta_it == c_shard.metadata.end()) {
                // Key in processing set but not in metadata -> dangling
                it = c_shard.processing_keys.erase(it);
            } else {
                // Check if this object has replicas on the removed segment
                // Since this object is in processing_keys, it likely has
                // PROCESSING replicas (which are not indexed)
                auto& metadata = *meta_it->second;
                auto& replicas = metadata.replicas_;
                for (int k = replicas.size() - 1; k >= 0; --k) {
                    auto id = replicas[k].get_segment_id();
                    if (id.has_value() && id.value() == segment_id) {
                        OnReplicaRemoved(replicas[k]);
                        replicas.erase(replicas.begin() + k);
                    }
                }

                if (replicas.empty()) {
                    OnObjectRemoved(metadata);
                    c_shard.replication_tasks.erase(meta_it->first);
                    c_shard.metadata.erase(meta_it);
                    it = c_shard.processing_keys.erase(it);
                } else {
                    ++it;
                }
            }
        }
    }
}

void CentralizedMasterService::TaskCleanupThreadFunc() {
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

tl::expected<UUID, ErrorCode> CentralizedMasterService::CreateCopyTask(
    const std::string& key, const std::vector<std::string>& targets) {
    if (targets.empty()) {
        LOG(ERROR) << "key=" << key << ", error=empty_targets";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    CentralizedMetadataAccessor accessor(this, key);
    if (!accessor.Exists()) {
        VLOG(1) << "key=" << key << ", info=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    for (const auto& target : targets) {
        if (!client_manager_.QuerySegments(target).has_value()) {
            LOG(ERROR) << "key=" << key << ", target_segment=" << target
                       << ", error=target_segment_not_mounted";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
    }

    auto& metadata = accessor.Get();
    const auto segment_names = metadata.GetReplicaSegmentNames();
    if (segment_names.empty()) {
        LOG(ERROR) << "key=" << key << ", error=no_valid_source_replicas";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    static thread_local std::mt19937 gen(std::random_device{}());
    std::uniform_int_distribution<size_t> dis(0, segment_names.size() - 1);
    const auto& source_name = segment_names[dis(gen)];

    auto client_id_res = client_manager_.GetClientIdBySegmentName(source_name);
    if (!client_id_res.has_value()) {
        LOG(ERROR) << "key=" << key << ", segment_name=" << source_name
                   << ", error=client_id_not_found";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    return task_manager_.get_write_access()
        .submit_task_typed<TaskType::REPLICA_COPY>(
            client_id_res.value(), {.key = key, .targets = targets});
}

tl::expected<UUID, ErrorCode> CentralizedMasterService::CreateMoveTask(
    const std::string& key, const std::string& source,
    const std::string& target) {
    CentralizedMetadataAccessor accessor(this, key);
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

    if (!client_manager_.QuerySegments(target).has_value()) {
        LOG(ERROR) << "key=" << key << ", target_segment=" << target
                   << ", error=target_segment_not_mounted";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto& metadata = accessor.Get();
    const auto segment_names = metadata.GetReplicaSegmentNames();
    if (std::find(segment_names.begin(), segment_names.end(), source) ==
        segment_names.end()) {
        LOG(ERROR) << "key=" << key << ", source_segment=" << source
                   << ", error=source_segment_not_found";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto client_id_res = client_manager_.GetClientIdBySegmentName(source);
    if (!client_id_res.has_value()) {
        LOG(ERROR) << "key=" << key << ", segment_name=" << source
                   << ", error=client_id_not_found";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    return task_manager_.get_write_access()
        .submit_task_typed<TaskType::REPLICA_MOVE>(
            client_id_res.value(),
            {.key = key, .source = source, .target = target});
}

tl::expected<QueryTaskResponse, ErrorCode> CentralizedMasterService::QueryTask(
    const UUID& task_id) {
    const auto& task_option =
        task_manager_.get_read_access().find_task_by_id(task_id);
    if (!task_option.has_value()) {
        LOG(ERROR) << "task_id=" << task_id << ", error=task_not_found";
        return tl::make_unexpected(ErrorCode::TASK_NOT_FOUND);
    }
    return QueryTaskResponse(task_option.value());
}

tl::expected<std::vector<TaskAssignment>, ErrorCode>
CentralizedMasterService::FetchTasks(const UUID& client_id, size_t batch_size) {
    const auto& tasks =
        task_manager_.get_write_access().pop_tasks(client_id, batch_size);
    std::vector<TaskAssignment> assignments;
    for (const auto& task : tasks) {
        assignments.emplace_back(task);
    }
    return assignments;
}

tl::expected<void, ErrorCode> CentralizedMasterService::MarkTaskToComplete(
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

tl::expected<CopyStartResponse, ErrorCode> CentralizedMasterService::CopyStart(
    const UUID& client_id, const std::string& key,
    const std::string& src_segment,
    const std::vector<std::string>& tgt_segments) {
    CentralizedMetadataAccessor accessor(this, key);
    if (!accessor.Exists()) {
        LOG(ERROR) << "key=" << key << ", error=object_not_found";
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
    for (const auto& tgt_segment : tgt_segments) {
        if (metadata.GetReplicaBySegmentName(tgt_segment) != nullptr) {
            // Skip used segments.
            continue;
        }
        auto result = client_manager_.AllocateFrom(metadata.size_, tgt_segment);
        if (!result.has_value()) {
            LOG(ERROR) << "key=" << key << ", tgt_segment=" << tgt_segment
                       << ", failed to allocate replica";
            return tl::make_unexpected(result.error());
        }
        replicas.push_back(std::move(*result));
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
    accessor.AddReplicationTask(client_id, ReplicationTask::Type::COPY,
                                source->id(), std::move(replica_ids));

    // Increase source refcnt to protect it from eviction.
    source->inc_refcnt();

    // Add replicas to the object.
    // DO NOT ACCESS source AFTER THIS - replicas_ reallocation may invalidate.
    metadata.AddReplicas(std::move(replicas));

    return response;
}

tl::expected<void, ErrorCode> CentralizedMasterService::CopyEnd(
    const UUID& client_id, const std::string& key) {
    CentralizedMetadataAccessor accessor(this, key);
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

    // Decrement source reference count.
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
            AddReplicaToSegmentIndex(accessor.GetShard().GetRef(),
                                     accessor.GetKey(), *replica);
            OnReplicaAdded(*replica);
        }
    }

    accessor.EraseReplicationTask();

    return all_complete ? tl::expected<void, ErrorCode>{}
                        : tl::make_unexpected(ErrorCode::REPLICA_IS_GONE);
}

tl::expected<void, ErrorCode> CentralizedMasterService::CopyRevoke(
    const UUID& client_id, const std::string& key) {
    CentralizedMetadataAccessor accessor(this, key);
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
    for (const auto& rid : task.replica_ids) {
        metadata.EraseReplicaByID(rid);
    }

    accessor.EraseReplicationTask();

    if (!metadata.IsValid()) {
        // Remove the object if it does not have any replicas.
        accessor.Erase();
    }
    return {};
}

tl::expected<MoveStartResponse, ErrorCode> CentralizedMasterService::MoveStart(
    const UUID& client_id, const std::string& key,
    const std::string& src_segment, const std::string& tgt_segment) {
    if (src_segment == tgt_segment) {
        LOG(ERROR) << "key=" << key << ", move_tgt=" << tgt_segment
                   << " cannot be the same as move_src=" << src_segment;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    CentralizedMetadataAccessor accessor(this, key);
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
        auto result = client_manager_.AllocateFrom(metadata.size_, tgt_segment);
        if (!result.has_value()) {
            LOG(ERROR) << "key=" << key << ", tgt_segment=" << tgt_segment
                       << ", failed to allocate replica";
            return tl::make_unexpected(result.error());
        }
        replicas.push_back(std::move(*result));
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
    accessor.AddReplicationTask(client_id, ReplicationTask::Type::MOVE,
                                source->id(), std::move(replica_ids));

    // Increase source refcnt to protect it from eviction.
    source->inc_refcnt();

    // Add replicas to the object.
    // DO NOT ACCESS source AFTER THIS - replicas_ reallocation may invalidate.
    metadata.AddReplicas(std::move(replicas));

    return response;
}

tl::expected<void, ErrorCode> CentralizedMasterService::MoveEnd(
    const UUID& client_id, const std::string& key) {
    CentralizedMetadataAccessor accessor(this, key);
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

    // Decrement source reference count.
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
        replica->mark_complete();
        AddReplicaToSegmentIndex(accessor.GetShard().GetRef(),
                                 accessor.GetKey(), *replica);
        OnReplicaAdded(*replica);
    }

    // Remove source replica: remove from segment index first, then erase.
    RemoveReplicaFromSegmentIndex(accessor.GetShard().GetRef(),
                                  accessor.GetKey(), *source);
    OnReplicaRemoved(*source);
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

tl::expected<void, ErrorCode> CentralizedMasterService::MoveRevoke(
    const UUID& client_id, const std::string& key) {
    CentralizedMetadataAccessor accessor(this, key);
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
    for (const auto& rid : task.replica_ids) {
        metadata.EraseReplicaByID(rid);
    }

    accessor.EraseReplicationTask();

    if (!metadata.IsValid()) {
        // Remove the object if it does not have any replicas.
        accessor.Erase();
    }
    return {};
}

}  // namespace mooncake

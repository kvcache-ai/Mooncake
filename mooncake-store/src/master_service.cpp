#include "master_service.h"

#include <thread>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <limits>
#include <random>
#include <shared_mutex>
#include <sstream>
#include <regex>
#include <unordered_set>
#include <unistd.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <ylt/util/tl/expected.hpp>
#include <boost/algorithm/string.hpp>

#include "master_metric_manager.h"
#include "common.h"
#include "segment.h"
#ifdef USE_NOF
#include "spdk/spdk_wrapper.h"
#endif
#ifdef STORE_USE_ETCD
#include "etcd_helper.h"
#include "ha/oplog/etcd_oplog_store.h"
#endif
#include "ha/snapshot/catalog/backends/embedded/embedded_snapshot_catalog_store.h"
#include "ha/snapshot/catalog/backends/redis/redis_snapshot_catalog_store.h"
#include "ha/snapshot/object/snapshot_object_store.h"
#include "types.h"
#include "serialize/serializer.hpp"
#include "ha/snapshot/snapshot_logger.h"
#include "utils/zstd_util.h"
#include "utils/file_util.h"
#include "utils.h"

namespace mooncake {

// Snapshot file names
static const std::string SNAPSHOT_METADATA_FILE = "metadata";
static const std::string SNAPSHOT_SEGMENTS_FILE = "segments";
static const std::string SNAPSHOT_TASK_MANAGER_FILE = "task_manager";
static const std::string SNAPSHOT_MANIFEST_FILE = "manifest.txt";
static const std::string SNAPSHOT_LATEST_FILE = "latest.txt";
static const std::string SNAPSHOT_BACKUP_SAVE_DIR =
    "mooncake_snapshot_save_backup";
static const std::string SNAPSHOT_BACKUP_RESTORE_DIR =
    "mooncake_snapshot_restore_backup";
static const std::string SNAPSHOT_SERIALIZER_VERSION = "1.0.0";
static const std::string SNAPSHOT_SERIALIZER_TYPE = "messagepack";

namespace {

constexpr size_t kUnlimitedSnapshotList = 0;

// Per-cycle offload cap as a fraction of `offloading_queue_limit_`. Used only
// when offload-on-evict mode is active. Defers memory eviction for at most
// this fraction of the queue limit per BatchEvict cycle; beyond that, eviction
// falls back according to `offload_force_evict_`. A future change may expose
// this as a configurable parameter if workloads demand tuning.
constexpr double kOffloadCapRatio = 0.5;

enum class SnapshotCatalogBackendKind {
    kEmbedded,
    kRedis,
};

tl::expected<SnapshotCatalogBackendKind, std::string> ParseSnapshotCatalogKind(
    std::string_view store_type) {
    if (store_type.empty() || store_type == "embedded" ||
        store_type == "payload") {
        return SnapshotCatalogBackendKind::kEmbedded;
    }
    if (store_type == "redis") {
        return SnapshotCatalogBackendKind::kRedis;
    }
    return tl::make_unexpected("unknown snapshot catalog store type: " +
                               std::string(store_type));
}

int64_t CurrentTimeMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

size_t RandomIndex(size_t upper_bound) {
    static thread_local std::mt19937 generator(std::random_device{}());
    std::uniform_int_distribution<size_t> dist(0, upper_bound - 1);
    return dist(generator);
}

bool HasExpectedReplicaAllocation(const ReplicateConfig& config,
                                  size_t allocated_memory_replicas,
                                  size_t allocated_nof_replicas) {
    if (config.nof_replica_num == 0) {
        return allocated_memory_replicas > 0;
    }
    if (DetermineReplicaWriteMode(config) ==
        ReplicaWriteMode::FLEXIBLE_DUAL_REPLICA) {
        return allocated_memory_replicas + allocated_nof_replicas > 0;
    }
    return allocated_memory_replicas == config.replica_num &&
           allocated_nof_replicas == config.nof_replica_num;
}

tl::expected<std::string, ErrorCode> GetGroupIdForKey(
    const ReplicateConfig& config, size_t key_count, size_t key_index) {
    if (!config.group_ids.has_value()) {
        return "";
    }
    if (config.group_ids->size() != key_count || key_index >= key_count) {
        LOG(ERROR) << "group_ids.size()=" << config.group_ids->size()
                   << ", key_count=" << key_count
                   << ", error=invalid_group_ids";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return config.group_ids->at(key_index);
}

}  // namespace

MasterService::MasterService() : MasterService(MasterServiceConfig()) {}

MasterService::MasterService(const MasterServiceConfig& config)
    : graceful_unmount_scheduler_(
          [this](const GracefulUnmountDeadlineRecord& record) {
              auto result =
                  this->UnmountSegment(record.segment_id, record.client_id);
              if (!result.has_value()) {
                  LOG(WARNING)
                      << "Failed to complete graceful unmount, segment_id="
                      << record.segment_id << ", client_id=" << record.client_id
                      << ", error=" << toString(result.error());
              }
          }),
      default_kv_lease_ttl_(config.default_kv_lease_ttl),
      default_kv_soft_pin_ttl_(config.default_kv_soft_pin_ttl),
      allow_evict_soft_pinned_objects_(config.allow_evict_soft_pinned_objects),
      eviction_ratio_(config.eviction_ratio),
      eviction_high_watermark_ratio_(config.eviction_high_watermark_ratio),
      nof_eviction_ratio_(config.nof_eviction_ratio),
      nof_eviction_high_watermark_ratio_(
          config.nof_eviction_high_watermark_ratio),
      view_version_(config.view_version),
      client_live_ttl_sec_(config.client_live_ttl_sec),
      nof_heartbeat_interval_sec_(
          std::chrono::seconds(config.nof_heartbeat_interval_sec)),
      nof_heartbeat_probe_timeout_ms_(
          std::chrono::milliseconds(config.nof_heartbeat_probe_timeout_ms)),
      nof_heartbeat_failures_threshold_(
          config.nof_heartbeat_failures_threshold),
      enable_ha_(config.enable_ha),
      enable_offload_(config.enable_offload),
      ha_backend_type_(config.ha_backend_type),
      ha_backend_connstring_(config.ha_backend_connstring),
      cluster_id_(config.cluster_id),
      root_fs_dir_(config.root_fs_dir),
      global_file_segment_size_(config.global_file_segment_size),
      enable_disk_eviction_(config.enable_disk_eviction),
      quota_bytes_(config.quota_bytes),
      enable_tenant_quota_(config.enable_tenant_quota),
      default_tenant_quota_bytes_(config.default_tenant_quota_bytes),
      tenant_quota_pool_capacity_bytes_(
          config.tenant_quota_pool_capacity_bytes),
      segment_manager_(config.memory_allocator, config.enable_cxl),
      nof_segment_manager_(config.memory_allocator),
      memory_allocator_type_(config.memory_allocator),
      allocation_strategy_(
          CreateAllocationStrategy(config.allocation_strategy_type)),
      enable_snapshot_restore_(config.enable_snapshot_restore),
      enable_snapshot_(config.enable_snapshot),
      snapshot_backup_dir_(config.snapshot_backup_dir),
      snapshot_interval_seconds_(config.snapshot_interval_seconds),
      snapshot_child_timeout_seconds_(config.snapshot_child_timeout_seconds),
      snapshot_retention_count_(config.snapshot_retention_count),
      snapshot_catalog_store_type_(config.snapshot_catalog_store_type),
      snapshot_catalog_store_connstring_(
          config.snapshot_catalog_store_connstring),
      put_start_discard_timeout_sec_(config.put_start_discard_timeout_sec),
      put_start_release_timeout_sec_(config.put_start_release_timeout_sec),
      task_manager_(config.task_manager_config),
      cxl_path_(config.cxl_path),
      cxl_size_(config.cxl_size),
      enable_cxl_(config.enable_cxl) {
    if (enable_snapshot_ || enable_snapshot_restore_) {
        try {
            auto object_store_type =
                ParseSnapshotObjectStoreType(config.snapshot_object_store_type);
            snapshot_object_store_ =
                SnapshotObjectStore::Create(object_store_type);
            snapshot_catalog_store_ = CreateSnapshotCatalogStore();
        } catch (const std::exception& e) {
            LOG(ERROR) << "Failed to create snapshot stores: " << e.what();
            throw std::runtime_error(
                fmt::format("Failed to create snapshot stores: {}", e.what()));
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

#ifdef USE_NOF
    if (nof_heartbeat_interval_sec_.count() <= 0) {
        LOG(ERROR) << "nof_heartbeat_interval_sec must be positive, current "
                   << nof_heartbeat_interval_sec_.count();
        throw std::invalid_argument("Invalid nof heartbeat interval");
    }
    if (nof_heartbeat_probe_timeout_ms_.count() <= 0) {
        LOG(ERROR) << "nof_heartbeat_probe_timeout_ms must be positive, "
                   << "current " << nof_heartbeat_probe_timeout_ms_.count();
        throw std::invalid_argument("Invalid nof heartbeat probe timeout");
    }
    if (nof_heartbeat_failures_threshold_ == 0) {
        LOG(ERROR) << "nof_heartbeat_failures_threshold must be positive";
        throw std::invalid_argument("Invalid nof heartbeat failure threshold");
    }

    nof_probe_fn_ = [](const std::string& te_endpoint, uint32_t timeout_ms,
                       std::string* error_reason) {
        return SpdkWrapper::GetInstance().ProbeNofSegment(
            te_endpoint, timeout_ms, error_reason);
    };
#endif

    // Offload-on-evict: defer LOCAL_DISK offload to eviction time
    offload_on_evict_ = enable_offload_ && config.offload_on_evict;
    if (offload_on_evict_) {
        LOG(INFO) << "Offload-on-evict mode enabled: DRAM offload to "
                     "LOCAL_DISK will occur at eviction time instead of "
                     "PutEnd";
        offload_force_evict_ = config.offload_force_evict;
        if (offload_force_evict_) {
            LOG(INFO) << "Force-evict enabled: objects exceeding offload "
                         "cap will be evicted without disk offload";
        }
    }

    // Promotion-on-hit: when Get observes a LOCAL_DISK-only key, queue an
    // async copy back to MEMORY. Only meaningful when offload is enabled
    // (otherwise no LOCAL_DISK replicas exist in the first place).
    promotion_on_hit_ = enable_offload_ && config.promotion_on_hit;
    promotion_admission_threshold_ = config.promotion_admission_threshold;
    promotion_queue_limit_ = config.promotion_queue_limit;
    promotion_max_per_heartbeat_ = config.promotion_max_per_heartbeat;
    // Clamp to >=1: 0 would make PromotionObjectHeartbeat return an empty
    // batch every call, silently disabling promotion delivery.
    if (promotion_max_per_heartbeat_ == 0) {
        promotion_max_per_heartbeat_ = 1;
    }
    // Defense-in-depth clamp: master.cpp clamps threshold into [1, 255]
    // at flag-parse time, but direct MasterServiceConfig construction
    // (tests, embedded users) bypasses that. Without the clamp here,
    // threshold=0 would silently bypass the frequency gate entirely
    // (freq < 0 is never true for uint8_t).
    if (promotion_admission_threshold_ == 0) {
        promotion_admission_threshold_ = 1;
    } else if (promotion_admission_threshold_ > 255) {
        promotion_admission_threshold_ = 255;
    }
    if (config.promotion_on_hit && !enable_offload_) {
        LOG(WARNING) << "promotion_on_hit=true was requested but "
                     << "enable_offload=false; promotion is silently "
                     << "disabled because it requires offload to produce "
                     << "LOCAL_DISK replicas. Set enable_offload=true to "
                     << "use this feature.";
    }
    if (promotion_on_hit_) {
        promotion_sketch_ = std::make_unique<CountMinSketch>();
        LOG(INFO) << "Promotion-on-hit mode enabled: LOCAL_DISK-only Gets "
                     "will queue async promotion to MEMORY (threshold="
                  << promotion_admission_threshold_
                  << ", queue_limit=" << promotion_queue_limit_
                  << ", max_per_heartbeat=" << promotion_max_per_heartbeat_
                  << ")";
    }

    eviction_running_ = true;
    eviction_thread_ = std::thread(&MasterService::EvictionThreadFunc, this);
    VLOG(1) << "action=start_eviction_thread";

    // Start client monitor thread in all modes so TTL/heartbeat works
    client_monitor_running_ = true;
    client_monitor_thread_ =
        std::thread(&MasterService::ClientMonitorFunc, this);
    VLOG(1) << "action=start_client_monitor_thread";

#ifdef USE_NOF
    nof_heartbeat_running_ = true;
    nof_heartbeat_thread_ =
        std::thread(&MasterService::NofHeartbeatThreadFunc, this);
    VLOG(1) << "action=start_nof_heartbeat_thread";
#endif

    // Start task cleanup thread
    task_cleanup_running_ = true;
    task_cleanup_thread_ =
        std::thread(&MasterService::TaskCleanupThreadFunc, this);
    VLOG(1) << "action=start_task_cleanup_thread";

    job_dispatch_running_ = true;
    job_dispatch_thread_ =
        std::thread(&MasterService::JobDispatchThreadFunc, this);
    VLOG(1) << "action=start_job_dispatch_thread";

    if (!root_fs_dir_.empty()) {
        use_disk_replica_ = true;
        if (global_file_segment_size_ == std::numeric_limits<int64_t>::max()) {
            MasterMetricManager::instance().set_dfs_capacity_unlimited(true);
        } else {
            MasterMetricManager::instance().inc_total_file_capacity(
                global_file_segment_size_);
        }
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
    }
}

std::unique_ptr<ha::SnapshotCatalogStore>
MasterService::CreateSnapshotCatalogStore() {
    auto catalog_kind = ParseSnapshotCatalogKind(snapshot_catalog_store_type_);
    if (!catalog_kind) {
        throw std::invalid_argument(catalog_kind.error());
    }

    switch (catalog_kind.value()) {
        case SnapshotCatalogBackendKind::kEmbedded:
            return std::make_unique<
                ha::backends::embedded::EmbeddedSnapshotCatalogStore>(
                snapshot_object_store_.get(), cluster_id_);
        case SnapshotCatalogBackendKind::kRedis: {
#ifndef STORE_USE_REDIS
            throw std::invalid_argument(
                "redis snapshot catalog store is unavailable in the current "
                "build");
#else
            const auto connstring = !snapshot_catalog_store_connstring_.empty()
                                        ? snapshot_catalog_store_connstring_
                                        : ha_backend_connstring_;
            if (connstring.empty()) {
                throw std::invalid_argument(
                    "redis snapshot catalog store requires a connection "
                    "string");
            }
            return std::make_unique<
                ha::backends::redis::RedisSnapshotCatalogStore>(
                snapshot_object_store_.get(), connstring, cluster_id_);
#endif
        }
    }

    throw std::invalid_argument("unknown snapshot catalog store type");
}

MasterService::~MasterService() {
    // Stop and join the threads
    eviction_running_ = false;
    client_monitor_running_ = false;
    snapshot_running_ = false;
    task_cleanup_running_ = false;
    job_dispatch_running_ = false;
    graceful_unmount_scheduler_.Stop();
#ifdef USE_NOF
    nof_heartbeat_running_ = false;
#endif

    // Wake sleepers so join() doesn't block for long sleep intervals.
    task_cleanup_cv_.notify_all();

    if (eviction_thread_.joinable()) {
        eviction_thread_.join();
    }
    if (client_monitor_thread_.joinable()) {
        client_monitor_thread_.join();
    }
#ifdef USE_NOF
    if (nof_heartbeat_thread_.joinable()) {
        nof_heartbeat_thread_.join();
    }
#endif
    if (snapshot_thread_.joinable()) {
        snapshot_thread_.join();
    }
    if (task_cleanup_thread_.joinable()) {
        task_cleanup_thread_.join();
    }
    if (job_dispatch_thread_.joinable()) {
        job_dispatch_thread_.join();
    }
}

void MasterService::SetNoFProbeFnForTesting(NoFProbeFn fn) {
#ifdef USE_NOF
    std::lock_guard<std::mutex> lock(nof_probe_fn_mutex_);
    if (fn) {
        nof_probe_fn_ = std::move(fn);
        return;
    }
    nof_probe_fn_ = [](const std::string& te_endpoint, uint32_t timeout_ms,
                       std::string* error_reason) {
        return SpdkWrapper::GetInstance().ProbeNofSegment(
            te_endpoint, timeout_ms, error_reason);
    };
#else
    (void)fn;
#endif
}

size_t MasterService::GetMountedNoFSegmentCountForTesting() {
    std::vector<MountedNoFSegmentSnapshot> mounted_segments;
    nof_segment_manager_.GetMountedSegmentsSnapshot(mounted_segments);
    return mounted_segments.size();
}

bool MasterService::IsNoFSegmentMountedForTesting(const UUID& segment_id) {
    std::vector<MountedNoFSegmentSnapshot> mounted_segments;
    nof_segment_manager_.GetMountedSegmentsSnapshot(mounted_segments);
    return std::any_of(
        mounted_segments.begin(), mounted_segments.end(),
        [&segment_id](const MountedNoFSegmentSnapshot& snapshot) {
            return snapshot.segment_id == segment_id &&
                   snapshot.status == SegmentStatus::OK;
        });
}

std::optional<uint32_t> MasterService::GetNoFHeartbeatFailureCountForTesting(
    const UUID& segment_id) {
    std::lock_guard<std::mutex> lock(nof_heartbeat_mutex_);
    auto it = nof_heartbeat_states_.find(segment_id);
    if (it == nof_heartbeat_states_.end()) {
        return std::nullopt;
    }
    return it->second.consecutive_failures;
}

std::optional<TenantQuotaSnapshot>
MasterService::GetTenantQuotaSnapshotForTesting(
    const std::string& tenant_id) const {
    const auto normalized_tenant = NormalizeTenantId(tenant_id);
    const auto shard_idx = getTenantQuotaShardIndex(normalized_tenant);
    const auto& shard = tenant_quota_shards_[shard_idx];
    std::lock_guard<std::mutex> lock(shard.mutex);
    auto it = shard.tenants.find(normalized_tenant);
    if (it == shard.tenants.end()) {
        return std::nullopt;
    }
    const auto& state = it->second;
    return TenantQuotaSnapshot{
        .tenant_id = normalized_tenant,
        .requested_quota_bytes = state.requested_quota_bytes,
        .effective_quota_bytes = state.effective_quota_bytes,
        .used_bytes = state.used_bytes,
        .reserved_bytes = state.reserved_bytes,
        .committed_count = state.committed_count,
        .has_explicit_policy = state.has_explicit_policy,
        .over_quota = state.over_quota};
}

auto MasterService::MountSegment(const Segment& segment, const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    {
        ScopedSegmentAccess segment_access =
            segment_manager_.getSegmentAccess();

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
    }
    RecomputeTenantEffectiveQuotas();
    return {};
}

auto MasterService::MountNoFSegment(const NoFSegment& segment,
                                    const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
#ifndef USE_NOF
    LOG(ERROR) << "client_id=" << client_id << ", segment_name=" << segment.name
               << ", error=nof_pool_disabled";
    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
#else
    ScopedNoFSegmentAccess nof_segment_access =
        nof_segment_manager_.getNoFSegmentAccess();

    LOG(INFO) << "NoF segment mount: "
              << "client_id=" << client_id
              << ", action=mount_segment, segment_name=" << segment.name;

    auto err = nof_segment_access.MountSegment(segment, client_id);
    if (err == ErrorCode::SEGMENT_ALREADY_EXISTS) {
        // Return OK because this is an idempotent operation
        return {};
    } else if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return {};
#endif
}

auto MasterService::ReMountSegment(const std::vector<Segment>& segments,
                                   const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    {
        std::unique_lock<std::shared_mutex> lock(client_mutex_);
        if (ok_client_.contains(client_id)) {
            LOG(WARNING) << "client_id=" << client_id
                         << ", warn=client_already_remounted";
            // Return OK because this is an idempotent operation
            return {};
        }

        {
            ScopedSegmentAccess segment_access =
                segment_manager_.getSegmentAccess();

            // Tell the client monitor thread to start timing for this client.
            // To avoid the following undesired situations, this message must be
            // sent after locking the segment mutex or client mutex and before
            // the remounting operation completes:
            // 1. Sending the message before the lock: the client expires and
            // unmouting invokes before this remounting are completed, which
            // prevents this segment being able to be unmounted forever;
            // 2. Sending the message after remounting the segments: After
            // remounting these segments, when trying to push id to the queue,
            // the queue is already full. However, at this point, the message
            // must be sent, otherwise this client cannot be monitored and
            // expired.
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
        }

        // Change the client status to OK
        ok_client_.insert(client_id);
        MasterMetricManager::instance().inc_active_clients();
    }
    RecomputeTenantEffectiveQuotas();

    return {};
}

auto MasterService::ReMountNoFSegment(const std::vector<NoFSegment>& segments,
                                      const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
#ifndef USE_NOF
    LOG(ERROR) << "client_id=" << client_id
               << ", segments_count=" << segments.size()
               << ", error=nof_pool_disabled";
    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
#else
    ScopedNoFSegmentAccess nof_segment_access =
        nof_segment_manager_.getNoFSegmentAccess();
    ErrorCode err = nof_segment_access.ReMountSegment(segments, client_id);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return {};
#endif
}

std::unordered_set<UUID, boost::hash<UUID>>
MasterService::getAliveClientsSnapshot() const {
    std::shared_lock<std::shared_mutex> lock(client_mutex_);
    return ok_client_;
}

size_t MasterService::getMetadataShardIndex(const std::string& tenant_id,
                                            const std::string& key) const {
    const auto normalized_tenant = NormalizeTenantId(tenant_id);
    std::shared_lock<std::shared_mutex> lock(group_routing_mutex_);
    auto it =
        object_group_ids_.find(MakeTenantScopedKey(normalized_tenant, key));
    if (it == object_group_ids_.end()) {
        return getShardIndex(normalized_tenant, key);
    }
    return getShardIndex(it->second);
}

size_t MasterService::getTenantQuotaShardIndex(
    const std::string& tenant_id) const {
    return std::hash<std::string>{}(NormalizeTenantId(tenant_id)) %
           kNumTenantQuotaShards;
}

uint64_t MasterService::CompletedMemoryQuotaCharge(
    const ObjectMetadata& metadata) const {
    return static_cast<uint64_t>(metadata.size) *
           metadata.CountReplicas([](const Replica& replica) {
               return replica.is_memory_replica() && replica.is_completed();
           });
}

uint64_t MasterService::RequestedMemoryQuotaCharge(
    uint64_t value_length, const ReplicateConfig& config) const {
    const unsigned __int128 charge =
        static_cast<unsigned __int128>(value_length) * config.replica_num;
    if (charge > std::numeric_limits<uint64_t>::max()) {
        return std::numeric_limits<uint64_t>::max();
    }
    return static_cast<uint64_t>(charge);
}

uint64_t MasterService::GetTenantQuotaCapacityBytes() {
    if (tenant_quota_pool_capacity_bytes_ != 0) {
        return tenant_quota_pool_capacity_bytes_;
    }
    uint64_t capacity = 0;
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();
    std::vector<std::pair<Segment, UUID>> segments;
    if (segment_access.GetAllSegments(segments) != ErrorCode::OK) {
        return 0;
    }
    for (const auto& [segment, _] : segments) {
        if (capacity > std::numeric_limits<uint64_t>::max() - segment.size) {
            return std::numeric_limits<uint64_t>::max();
        }
        capacity += segment.size;
    }
    return capacity;
}

void MasterService::RecomputeTenantEffectiveQuotas() {
    if (!enable_tenant_quota_) {
        return;
    }
    const uint64_t capacity = GetTenantQuotaCapacityBytes();

    std::vector<std::pair<size_t, std::string>> active_tenants;
    for (size_t i = 0; i < kNumTenantQuotaShards; ++i) {
        auto& shard = tenant_quota_shards_[i];
        std::lock_guard<std::mutex> lock(shard.mutex);
        for (auto it = shard.tenants.begin(); it != shard.tenants.end();) {
            const auto& tenant_id = it->first;
            auto& state = it->second;
            if (!state.has_explicit_policy) {
                state.requested_quota_bytes = default_tenant_quota_bytes_;
            }
            if (!state.has_explicit_policy && state.used_bytes == 0 &&
                state.reserved_bytes == 0 && state.committed_count == 0) {
                it = shard.tenants.erase(it);
                continue;
            }
            active_tenants.emplace_back(i, tenant_id);
            ++it;
        }
    }

    if (default_tenant_quota_bytes_ == 0) {
        for (const auto& [shard_idx, tenant_id] : active_tenants) {
            auto& shard = tenant_quota_shards_[shard_idx];
            std::lock_guard<std::mutex> lock(shard.mutex);
            auto it = shard.tenants.find(tenant_id);
            if (it == shard.tenants.end()) {
                continue;
            }
            auto& state = it->second;
            state.effective_quota_bytes = std::numeric_limits<uint64_t>::max();
            state.over_quota = false;
        }
        return;
    }

    const uint64_t active_count = active_tenants.size();
    for (size_t ordinal = 0; ordinal < active_tenants.size(); ++ordinal) {
        const auto& [shard_idx, tenant_id] = active_tenants[ordinal];
        auto& shard = tenant_quota_shards_[shard_idx];
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto it = shard.tenants.find(tenant_id);
        if (it == shard.tenants.end()) {
            continue;
        }
        auto& state = it->second;
        uint64_t effective = 0;
        if (active_count > 0 && capacity > 0) {
            effective = capacity / active_count;
            if (ordinal < capacity % active_count) {
                ++effective;
            }
        }
        state.effective_quota_bytes = effective;
        state.over_quota = static_cast<unsigned __int128>(state.used_bytes) +
                               state.reserved_bytes >
                           state.effective_quota_bytes;
    }
}

tl::expected<void, ErrorCode> MasterService::ReserveTenantQuota(
    const std::string& tenant_id, uint64_t bytes) {
    if (!enable_tenant_quota_ || bytes == 0) {
        return {};
    }
    const auto normalized_tenant = NormalizeTenantId(tenant_id);
    auto exceeds_effective_quota = [](const TenantQuotaState& state,
                                      uint64_t additional_bytes) {
        return static_cast<unsigned __int128>(state.used_bytes) +
                   state.reserved_bytes + additional_bytes >
               state.effective_quota_bytes;
    };
    auto refresh_over_quota = [](TenantQuotaState& state) {
        state.over_quota = state.effective_quota_bytes !=
                               std::numeric_limits<uint64_t>::max() &&
                           static_cast<unsigned __int128>(state.used_bytes) +
                                   state.reserved_bytes >
                               state.effective_quota_bytes;
    };

    auto& shard =
        tenant_quota_shards_[getTenantQuotaShardIndex(normalized_tenant)];
    {
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto it = shard.tenants.find(normalized_tenant);
        if (it != shard.tenants.end()) {
            auto& state = it->second;
            if (exceeds_effective_quota(state, bytes)) {
                return tl::make_unexpected(ErrorCode::TENANT_QUOTA_EXCEEDED);
            }

            state.reserved_bytes += bytes;
            refresh_over_quota(state);
            return {};
        }

        auto [insert_it, _] = shard.tenants.try_emplace(normalized_tenant);
        auto& state = insert_it->second;
        state.requested_quota_bytes = default_tenant_quota_bytes_;
        state.effective_quota_bytes = default_tenant_quota_bytes_ == 0
                                          ? std::numeric_limits<uint64_t>::max()
                                          : 0;
        state.over_quota = false;

        if (default_tenant_quota_bytes_ == 0) {
            if (exceeds_effective_quota(state, bytes)) {
                shard.tenants.erase(insert_it);
                return tl::make_unexpected(ErrorCode::TENANT_QUOTA_EXCEEDED);
            }
            state.reserved_bytes += bytes;
            return {};
        }

        state.reserved_bytes = bytes;
    }

    RecomputeTenantEffectiveQuotas();

    bool rollback_needs_recompute = false;
    {
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto it = shard.tenants.find(normalized_tenant);
        if (it == shard.tenants.end()) {
            return tl::make_unexpected(ErrorCode::TENANT_QUOTA_EXCEEDED);
        }

        auto& state = it->second;
        if (!exceeds_effective_quota(state, 0)) {
            refresh_over_quota(state);
            return {};
        }

        if (state.reserved_bytes < bytes) {
            LOG(ERROR) << "tenant quota reserve rollback mismatch tenant="
                       << normalized_tenant << ", bytes=" << bytes
                       << ", reserved=" << state.reserved_bytes;
            return tl::make_unexpected(ErrorCode::TENANT_QUOTA_EXCEEDED);
        }
        state.reserved_bytes -= bytes;
        if (!state.has_explicit_policy && state.used_bytes == 0 &&
            state.reserved_bytes == 0 && state.committed_count == 0) {
            shard.tenants.erase(it);
            rollback_needs_recompute = true;
        } else {
            refresh_over_quota(state);
        }
    }

    if (rollback_needs_recompute) {
        RecomputeTenantEffectiveQuotas();
    }
    return tl::make_unexpected(ErrorCode::TENANT_QUOTA_EXCEEDED);
}

void MasterService::CommitTenantQuota(const std::string& tenant_id,
                                      uint64_t bytes) {
    if (!enable_tenant_quota_ || bytes == 0) {
        return;
    }
    const auto normalized_tenant = NormalizeTenantId(tenant_id);
    auto& shard =
        tenant_quota_shards_[getTenantQuotaShardIndex(normalized_tenant)];
    std::lock_guard<std::mutex> lock(shard.mutex);
    auto it = shard.tenants.find(normalized_tenant);
    if (it == shard.tenants.end()) {
        LOG(ERROR) << "tenant quota commit mismatch tenant="
                   << normalized_tenant << ", bytes=" << bytes
                   << ", reserved=0";
        return;
    }
    auto& state = it->second;
    if (state.reserved_bytes < bytes) {
        LOG(ERROR) << "tenant quota commit mismatch tenant="
                   << normalized_tenant << ", bytes=" << bytes
                   << ", reserved=" << state.reserved_bytes;
        return;
    }
    state.reserved_bytes -= bytes;
    if (state.used_bytes > std::numeric_limits<uint64_t>::max() - bytes) {
        state.used_bytes = std::numeric_limits<uint64_t>::max();
    } else {
        state.used_bytes += bytes;
    }
    ++state.committed_count;
    state.over_quota =
        state.effective_quota_bytes != std::numeric_limits<uint64_t>::max() &&
        static_cast<unsigned __int128>(state.used_bytes) +
                state.reserved_bytes >
            state.effective_quota_bytes;
}

void MasterService::AbortTenantQuota(const std::string& tenant_id,
                                     uint64_t bytes) {
    if (!enable_tenant_quota_ || bytes == 0) {
        return;
    }
    const auto normalized_tenant = NormalizeTenantId(tenant_id);
    auto& shard =
        tenant_quota_shards_[getTenantQuotaShardIndex(normalized_tenant)];
    bool recompute_needed = false;
    {
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto it = shard.tenants.find(normalized_tenant);
        if (it == shard.tenants.end()) {
            LOG(ERROR) << "tenant quota abort mismatch tenant="
                       << normalized_tenant << ", bytes=" << bytes
                       << ", reserved=0";
            return;
        }
        auto& state = it->second;
        if (state.reserved_bytes < bytes) {
            LOG(ERROR) << "tenant quota abort mismatch tenant="
                       << normalized_tenant << ", bytes=" << bytes
                       << ", reserved=" << state.reserved_bytes;
            return;
        }
        state.reserved_bytes -= bytes;
        if (!state.has_explicit_policy && state.used_bytes == 0 &&
            state.reserved_bytes == 0 && state.committed_count == 0) {
            shard.tenants.erase(it);
            recompute_needed = true;
        } else {
            state.over_quota =
                state.effective_quota_bytes !=
                    std::numeric_limits<uint64_t>::max() &&
                static_cast<unsigned __int128>(state.used_bytes) +
                        state.reserved_bytes >
                    state.effective_quota_bytes;
        }
    }
    if (recompute_needed) {
        RecomputeTenantEffectiveQuotas();
    }
}

void MasterService::ReleaseTenantQuota(const std::string& tenant_id,
                                       uint64_t bytes) {
    if (!enable_tenant_quota_ || bytes == 0) {
        return;
    }
    const auto normalized_tenant = NormalizeTenantId(tenant_id);
    auto& shard =
        tenant_quota_shards_[getTenantQuotaShardIndex(normalized_tenant)];
    bool recompute_needed = false;
    {
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto it = shard.tenants.find(normalized_tenant);
        if (it == shard.tenants.end()) {
            LOG(ERROR) << "tenant quota release mismatch tenant="
                       << normalized_tenant << ", bytes=" << bytes
                       << ", used=0";
            return;
        }
        auto& state = it->second;
        if (state.used_bytes < bytes) {
            LOG(ERROR) << "tenant quota release mismatch tenant="
                       << normalized_tenant << ", bytes=" << bytes
                       << ", used=" << state.used_bytes;
            return;
        }
        state.used_bytes -= bytes;
        if (state.committed_count > 0) {
            --state.committed_count;
        }
        if (!state.has_explicit_policy && state.used_bytes == 0 &&
            state.reserved_bytes == 0 && state.committed_count == 0) {
            shard.tenants.erase(it);
            recompute_needed = true;
        } else {
            state.over_quota =
                state.effective_quota_bytes !=
                    std::numeric_limits<uint64_t>::max() &&
                static_cast<unsigned __int128>(state.used_bytes) +
                        state.reserved_bytes >
                    state.effective_quota_bytes;
        }
    }
    if (recompute_needed) {
        RecomputeTenantEffectiveQuotas();
    }
}

void MasterService::ReleaseTenantQuotaPartial(const std::string& tenant_id,
                                              uint64_t bytes) {
    if (!enable_tenant_quota_ || bytes == 0) {
        return;
    }
    const auto normalized_tenant = NormalizeTenantId(tenant_id);
    auto& shard =
        tenant_quota_shards_[getTenantQuotaShardIndex(normalized_tenant)];
    std::lock_guard<std::mutex> lock(shard.mutex);
    auto it = shard.tenants.find(normalized_tenant);
    if (it == shard.tenants.end()) {
        LOG(ERROR) << "tenant quota partial release mismatch tenant="
                   << normalized_tenant << ", bytes=" << bytes << ", used=0";
        return;
    }
    auto& state = it->second;
    if (state.used_bytes < bytes) {
        LOG(ERROR) << "tenant quota partial release mismatch tenant="
                   << normalized_tenant << ", bytes=" << bytes
                   << ", used=" << state.used_bytes;
        return;
    }
    state.used_bytes -= bytes;
    state.over_quota =
        state.effective_quota_bytes != std::numeric_limits<uint64_t>::max() &&
        static_cast<unsigned __int128>(state.used_bytes) +
                state.reserved_bytes >
            state.effective_quota_bytes;
}

void MasterService::ReleaseCommittedQuotaCharge(ObjectMetadata& metadata,
                                                uint64_t bytes) {
    if (!enable_tenant_quota_ || bytes == 0) {
        return;
    }
    const uint64_t release_bytes =
        std::min(bytes, metadata.committed_quota_charge_bytes);
    if (release_bytes == metadata.committed_quota_charge_bytes) {
        ReleaseTenantQuota(metadata.tenant_id, release_bytes);
    } else {
        ReleaseTenantQuotaPartial(metadata.tenant_id, release_bytes);
    }
    metadata.committed_quota_charge_bytes -= release_bytes;
}

void MasterService::RebuildTenantQuotaUsageFromMetadata() {
    if (!enable_tenant_quota_) {
        return;
    }

    std::unordered_map<std::string, uint64_t> used_by_tenant;
    std::unordered_map<std::string, uint64_t> committed_count_by_tenant;
    for (size_t i = 0; i < kNumShards; ++i) {
        MetadataShardAccessorRW shard(this, i);
        for (auto& [tenant_id, tenant_state] : shard->tenants) {
            for (auto& [_, metadata] : tenant_state.metadata) {
                const uint64_t charge = CompletedMemoryQuotaCharge(metadata);
                metadata.reserved_quota_charge_bytes = 0;
                metadata.committed_quota_charge_bytes = charge;
                metadata.pending_replaced_quota_charge_bytes = 0;
                if (charge == 0) {
                    continue;
                }
                used_by_tenant[tenant_id] += charge;
                committed_count_by_tenant[tenant_id]++;
            }
        }
    }

    for (size_t i = 0; i < kNumTenantQuotaShards; ++i) {
        auto& shard = tenant_quota_shards_[i];
        std::lock_guard<std::mutex> lock(shard.mutex);
        for (auto& [_, state] : shard.tenants) {
            state.used_bytes = 0;
            state.reserved_bytes = 0;
            state.committed_count = 0;
        }
    }
    for (const auto& [tenant_id, used_bytes] : used_by_tenant) {
        auto& shard = tenant_quota_shards_[getTenantQuotaShardIndex(tenant_id)];
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto& state = shard.tenants[tenant_id];
        state.requested_quota_bytes = default_tenant_quota_bytes_;
        state.used_bytes = used_bytes;
        state.committed_count = committed_count_by_tenant[tenant_id];
    }
    RecomputeTenantEffectiveQuotas();
}

std::optional<std::string> MasterService::GetGroupRoute(
    const std::string& tenant_id, const std::string& key) const {
    const auto normalized_tenant = NormalizeTenantId(tenant_id);
    std::shared_lock<std::shared_mutex> lock(group_routing_mutex_);
    auto it =
        object_group_ids_.find(MakeTenantScopedKey(normalized_tenant, key));
    if (it == object_group_ids_.end()) {
        return std::nullopt;
    }
    return it->second;
}

MasterService::ObjectOperationLock MasterService::AcquireObjectOperationLock(
    const std::string& tenant_id, const std::string& key) {
    const auto scoped_key = MakeTenantScopedKey(tenant_id, key);
    const auto stripe_idx =
        std::hash<std::string>{}(scoped_key) % kObjectOperationLockStripes;
    return {std::unique_lock<std::mutex>(object_operation_locks_[stripe_idx])};
}

void MasterService::RegisterGroupMember(TenantState& tenant_state,
                                        const std::string& tenant_id,
                                        const std::string& key,
                                        const std::string& group_id) {
    if (group_id.empty()) {
        return;
    }
    const auto normalized_tenant = NormalizeTenantId(tenant_id);
    std::unique_lock<std::shared_mutex> lock(group_routing_mutex_);
    object_group_ids_[MakeTenantScopedKey(normalized_tenant, key)] = group_id;
    groups_needing_lease_refresh_.insert(
        MakeTenantScopedKey(normalized_tenant, group_id));
    tenant_state.group_members[group_id].insert(key);
}

void MasterService::UnregisterGroupMember(TenantState& tenant_state,
                                          const std::string& tenant_id,
                                          const std::string& key,
                                          const std::string& group_id) {
    if (group_id.empty()) {
        return;
    }
    const auto normalized_tenant = NormalizeTenantId(tenant_id);
    bool group_empty = false;
    auto group_it = tenant_state.group_members.find(group_id);
    if (group_it != tenant_state.group_members.end()) {
        group_it->second.erase(key);
        if (group_it->second.empty()) {
            tenant_state.group_members.erase(group_it);
            group_empty = true;
        }
    }
    std::unique_lock<std::shared_mutex> lock(group_routing_mutex_);
    auto route_it =
        object_group_ids_.find(MakeTenantScopedKey(normalized_tenant, key));
    if (route_it != object_group_ids_.end() && route_it->second == group_id) {
        object_group_ids_.erase(route_it);
    }
    if (group_empty) {
        groups_needing_lease_refresh_.erase(
            MakeTenantScopedKey(normalized_tenant, group_id));
    }
}

bool MasterService::HasCompletedMemoryCacheReplica(
    const ObjectMetadata& metadata) {
    return metadata.HasReplica([](const Replica& replica) {
        return replica.is_memory_replica() && replica.is_completed();
    });
}

bool MasterService::HasCompletedDiskCacheReplica(
    const ObjectMetadata& metadata) {
    return metadata.HasReplica([](const Replica& replica) {
        return replica.is_disk_replica() && replica.is_completed();
    });
}

void MasterService::SyncCacheTotalAccounting(ObjectMetadata& metadata) {
    const bool has_memory_cache_replica =
        HasCompletedMemoryCacheReplica(metadata);
    const bool has_disk_cache_replica = HasCompletedDiskCacheReplica(metadata);

    if (!metadata.memory_cache_total_accounted && has_memory_cache_replica) {
        MasterMetricManager::instance().inc_mem_cache_nums();
        metadata.memory_cache_total_accounted = true;
    } else if (metadata.memory_cache_total_accounted &&
               !has_memory_cache_replica) {
        MasterMetricManager::instance().dec_mem_cache_nums();
        metadata.memory_cache_total_accounted = false;
    }

    if (!metadata.disk_cache_total_accounted && has_disk_cache_replica) {
        MasterMetricManager::instance().inc_file_cache_nums();
        metadata.disk_cache_total_accounted = true;
    } else if (metadata.disk_cache_total_accounted && !has_disk_cache_replica) {
        MasterMetricManager::instance().dec_file_cache_nums();
        metadata.disk_cache_total_accounted = false;
    }
}

void MasterService::AccountCacheTotalRemoval(ObjectMetadata& metadata) {
    if (metadata.memory_cache_total_accounted) {
        MasterMetricManager::instance().dec_mem_cache_nums();
        metadata.memory_cache_total_accounted = false;
    }
    if (metadata.disk_cache_total_accounted) {
        MasterMetricManager::instance().dec_file_cache_nums();
        metadata.disk_cache_total_accounted = false;
    }
}

void MasterService::RebuildCacheTotalAccounting() {
    MasterMetricManager::instance().reset_cache_total_nums();
    for (auto& shard : metadata_shards_) {
        for (auto& tenant_entry : shard.tenants) {
            for (auto& metadata_entry : tenant_entry.second.metadata) {
                SyncCacheTotalAccounting(metadata_entry.second);
            }
        }
    }
}

std::vector<Replica> MasterService::PopReplicasWithCacheTotalAccounting(
    ObjectMetadata& metadata,
    const std::function<bool(const Replica&)>& pred_fn) {
    auto replicas = metadata.PopReplicas(pred_fn);
    SyncCacheTotalAccounting(metadata);
    return replicas;
}

std::vector<Replica> MasterService::PopReplicasWithCacheTotalAccounting(
    ObjectMetadata& metadata) {
    auto replicas = metadata.PopReplicas();
    SyncCacheTotalAccounting(metadata);
    return replicas;
}

size_t MasterService::EraseReplicasWithCacheTotalAccounting(
    ObjectMetadata& metadata,
    const std::function<bool(const Replica&)>& pred_fn) {
    auto erased_replicas =
        PopReplicasWithCacheTotalAccounting(metadata, pred_fn);
    return erased_replicas.size();
}

std::unordered_map<std::string, MasterService::ObjectMetadata>::iterator
MasterService::EraseMetadata(
    TenantState& tenant_state,
    std::unordered_map<std::string, ObjectMetadata>::iterator it,
    const std::string& tenant_id) {
    return EraseMetadata(tenant_state, it, tenant_id, QuotaEraseMode::kFull);
}

std::unordered_map<std::string, MasterService::ObjectMetadata>::iterator
MasterService::EraseMetadata(
    TenantState& tenant_state,
    std::unordered_map<std::string, ObjectMetadata>::iterator it,
    const std::string& tenant_id, QuotaEraseMode quota_mode) {
    return EraseMetadata(tenant_state, it, tenant_id, quota_mode, nullptr);
}

std::unordered_map<std::string, MasterService::ObjectMetadata>::iterator
MasterService::EraseMetadata(
    TenantState& tenant_state,
    std::unordered_map<std::string, ObjectMetadata>::iterator it,
    const std::string& tenant_id, QuotaEraseMode quota_mode,
    MetadataShardAccessorRW* shard) {
    bool had_completed_disk = it->second.HasReplica([](const Replica& r) {
        return r.is_local_disk_replica() && r.is_completed();
    });
    const std::string key = it->first;
    const std::string group_id = it->second.group_id;
    auto& metadata = it->second;
    AccountCacheTotalRemoval(metadata);
    switch (quota_mode) {
        case QuotaEraseMode::kFull:
            AbortTenantQuota(tenant_id, metadata.reserved_quota_charge_bytes);
            ReleaseTenantQuota(tenant_id,
                               metadata.committed_quota_charge_bytes);
            ReleaseTenantQuota(tenant_id,
                               metadata.pending_replaced_quota_charge_bytes);
            break;
        case QuotaEraseMode::kPreserveOld:
            AbortTenantQuota(tenant_id, metadata.reserved_quota_charge_bytes);
            break;
        case QuotaEraseMode::kAbortOnly:
            AbortTenantQuota(tenant_id, metadata.reserved_quota_charge_bytes);
            break;
    }
    auto next = tenant_state.metadata.erase(it);
    if (had_completed_disk && shard) {
        shard->OnDiskReplicaRemoved(had_completed_disk);
    }
    UnregisterGroupMember(tenant_state, tenant_id, key, group_id);
    return next;
}

void MasterService::RebuildGroupRoutingIndex() {
    std::unordered_map<std::string, std::string> rebuilt_group_ids;
    std::unordered_set<std::string> groups_needing_refresh;
    for (size_t shard_idx = 0; shard_idx < kNumShards; ++shard_idx) {
        MetadataShardAccessorRW shard(this, shard_idx);
        for (auto& [tenant_id, tenant_state] : shard->tenants) {
            tenant_state.group_members.clear();
            for (const auto& [key, metadata] : tenant_state.metadata) {
                if (!metadata.IsGrouped()) {
                    continue;
                }
                tenant_state.group_members[metadata.group_id].insert(key);
                rebuilt_group_ids[MakeTenantScopedKey(tenant_id, key)] =
                    metadata.group_id;
                groups_needing_refresh.insert(
                    MakeTenantScopedKey(tenant_id, metadata.group_id));
            }
        }
    }
    {
        std::unique_lock<std::shared_mutex> lock(group_routing_mutex_);
        object_group_ids_ = std::move(rebuilt_group_ids);
        groups_needing_lease_refresh_ = std::move(groups_needing_refresh);
    }
}

void MasterService::GrantLeaseForGroup(const TenantState& tenant_state,
                                       const std::string& key,
                                       const ObjectMetadata& metadata) const {
    if (!metadata.IsGrouped()) {
        metadata.GrantLease(default_kv_lease_ttl_, default_kv_soft_pin_ttl_);
        return;
    }

    bool needs_refresh = metadata.NeedsLeaseRefresh(default_kv_lease_ttl_,
                                                    default_kv_soft_pin_ttl_);
    if (!needs_refresh) {
        std::shared_lock<std::shared_mutex> lock(group_routing_mutex_);
        needs_refresh = groups_needing_lease_refresh_.find(MakeTenantScopedKey(
                            metadata.tenant_id, metadata.group_id)) !=
                        groups_needing_lease_refresh_.end();
    }
    if (!needs_refresh) {
        return;
    }

    auto group_it = tenant_state.group_members.find(metadata.group_id);
    if (group_it == tenant_state.group_members.end()) {
        metadata.GrantLease(default_kv_lease_ttl_, default_kv_soft_pin_ttl_);
        return;
    }

    for (const auto& member_key : group_it->second) {
        auto mit = tenant_state.metadata.find(member_key);
        if (mit != tenant_state.metadata.end()) {
            mit->second.GrantLease(default_kv_lease_ttl_,
                                   default_kv_soft_pin_ttl_);
        }
    }
    if (group_it->second.find(key) == group_it->second.end()) {
        metadata.GrantLease(default_kv_lease_ttl_, default_kv_soft_pin_ttl_);
    }
    {
        std::unique_lock<std::shared_mutex> lock(group_routing_mutex_);
        groups_needing_lease_refresh_.erase(
            MakeTenantScopedKey(metadata.tenant_id, metadata.group_id));
    }
}

void MasterService::ClearInvalidHandles() {
    ClearInvalidHandles(getAliveClientsSnapshot());
}

void MasterService::ClearInvalidHandles(
    const std::unordered_set<UUID, boost::hash<UUID>>& alive_clients) {
    for (size_t i = 0; i < kNumShards; i++) {
        MetadataShardAccessorRW shard(this, i);
        for (auto tenant_it = shard->tenants.begin();
             tenant_it != shard->tenants.end();) {
            auto& tenant_state = tenant_it->second;
            auto it = tenant_state.metadata.begin();
            while (it != tenant_state.metadata.end()) {
                if (CleanupStaleHandles(it->second, alive_clients, &shard)) {
                    tenant_state.processing_keys.erase(it->first);
                    tenant_state.replication_tasks.erase(it->first);
                    tenant_state.offloading_tasks.erase(it->first);
                    ErasePromotionTaskIfPresent(tenant_state, it->first,
                                                tenant_it->first);
                    it = EraseMetadata(tenant_state, it, tenant_it->first,
                                       QuotaEraseMode::kFull, &shard);
                } else {
                    ++it;
                }
            }
            if (tenant_state.Empty()) {
                tenant_it = shard->tenants.erase(tenant_it);
            } else {
                ++tenant_it;
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

        std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
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
    {
        ScopedSegmentAccess segment_access =
            segment_manager_.getSegmentAccess();
        auto err = segment_access.CommitUnmountSegment(segment_id, client_id,
                                                       metrics_dec_capacity);
        if (err != ErrorCode::OK) {
            return tl::make_unexpected(err);
        }
    }
    RecomputeTenantEffectiveQuotas();
    return {};
}

auto MasterService::GracefulUnmountSegment(const UUID& segment_id,
                                           const UUID& client_id,
                                           uint64_t grace_period_ms)
    -> tl::expected<void, ErrorCode> {
    std::unique_lock<std::shared_mutex> lock(snapshot_mutex_);
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();

    // Verify ownership: the segment must belong to the calling client
    std::vector<Segment> client_segments;
    auto err = segment_access.GetClientSegments(client_id, client_segments);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    bool owned = false;
    for (auto& seg : client_segments) {
        if (seg.id == segment_id) {
            owned = true;
            break;
        }
    }
    if (!owned) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    err = segment_access.PrepareGracefulUnmountSegment(segment_id);
    if (err == ErrorCode::SEGMENT_NOT_FOUND) {
        return {};
    }
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }

    auto expire_time = std::chrono::steady_clock::now() +
                       std::chrono::milliseconds(grace_period_ms);
    graceful_unmount_scheduler_.Schedule({segment_id, client_id}, expire_time);
    return {};
}

auto MasterService::UnmountNoFSegment(const UUID& segment_id,
                                      const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
#ifndef USE_NOF
    LOG(ERROR) << "client_id=" << client_id << ", segment_id=" << segment_id
               << ", error=nof_pool_disabled";
    return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
#else
    size_t metrics_dec_capacity = 0;  // to update the metrics

    // 1. Prepare to unmount the segment by deleting its allocator
    {
        ScopedNoFSegmentAccess segment_access =
            nof_segment_manager_.getNoFSegmentAccess();
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
    ScopedNoFSegmentAccess segment_access =
        nof_segment_manager_.getNoFSegmentAccess();
    auto err = segment_access.CommitUnmountSegment(segment_id, client_id,
                                                   metrics_dec_capacity);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    {
        std::lock_guard<std::mutex> lock(nof_heartbeat_mutex_);
        nof_heartbeat_states_.erase(segment_id);
    }
    return {};
#endif
}

auto MasterService::ExistKey(const std::string& key,
                             const std::string& tenant_id)
    -> tl::expected<bool, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    MetadataAccessorRO accessor(this, MakeObjectIdentity(key, tenant_id));
    if (!accessor.Exists()) {
        VLOG(1) << "key=" << key << ", info=object_not_found";
        return false;
    }

    const auto& metadata = accessor.Get();
    if (metadata.HasReplica(&Replica::fn_is_completed)) {
        // Grant a lease to the object as it may be further used by the
        // client.
        auto* ts = accessor.GetTenantState();
        if (ts) {
            GrantLeaseForGroup(*ts, key, metadata);
        } else {
            metadata.GrantLease(default_kv_lease_ttl_,
                                default_kv_soft_pin_ttl_);
        }
        return true;
    }

    return false;  // If no complete replica is found, return false
}

std::vector<tl::expected<bool, ErrorCode>> MasterService::BatchExistKey(
    const std::vector<std::string>& keys, const std::string& tenant_id) {
    const std::string normalized_tenant = NormalizeTenantId(tenant_id);
    std::vector<tl::expected<bool, ErrorCode>> results(keys.size());
    if (keys.empty()) {
        return results;
    }

    std::vector<std::vector<size_t>> indices_by_shard(kNumShards);
    {
        std::shared_lock<std::shared_mutex> group_routing_lock(
            group_routing_mutex_);
        for (size_t i = 0; i < keys.size(); ++i) {
            auto route_it = object_group_ids_.find(
                MakeTenantScopedKey(normalized_tenant, keys[i]));
            const size_t shard_idx =
                route_it == object_group_ids_.end()
                    ? getShardIndex(normalized_tenant, keys[i])
                    : getShardIndex(route_it->second);
            indices_by_shard[shard_idx].push_back(i);
        }
    }

    const size_t start_shard = RandomIndex(kNumShards);
    for (size_t scanned = 0; scanned < kNumShards; ++scanned) {
        const size_t shard_idx =
            (start_shard + kNumShards - scanned) % kNumShards;
        const auto& key_indices = indices_by_shard[shard_idx];
        if (key_indices.empty()) {
            continue;
        }

        std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
        MetadataShardAccessorRO shard(this, shard_idx);
        auto tenant_it = shard->tenants.find(normalized_tenant);
        if (tenant_it == shard->tenants.end()) {
            for (const size_t i : key_indices) {
                VLOG(1) << "key=" << keys[i]
                        << ", tenant_id=" << normalized_tenant
                        << ", info=object_not_found";
                results[i] = false;
            }
            continue;
        }

        const auto& tenant_state = tenant_it->second;
        for (const size_t i : key_indices) {
            const auto& key = keys[i];
            auto it = tenant_state.metadata.find(key);
            if (it == tenant_state.metadata.end() || !it->second.IsValid()) {
                VLOG(1) << "key=" << key << ", tenant_id=" << normalized_tenant
                        << ", info=object_not_found";
                results[i] = false;
                continue;
            }

            const auto& metadata = it->second;
            if (metadata.HasReplica(&Replica::fn_is_completed)) {
                GrantLeaseForGroup(tenant_state, key, metadata);
                results[i] = true;
            } else {
                results[i] = false;
            }
        }
    }
    return results;
}

auto MasterService::GetAllKeys(const std::string& tenant_id)
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    std::vector<std::string> all_keys;
    const auto normalized_tenant = NormalizeTenantId(tenant_id);
    for (size_t i = 0; i < kNumShards; i++) {
        MetadataShardAccessorRO shard(this, i);
        auto tenant_it = shard->tenants.find(normalized_tenant);
        if (tenant_it == shard->tenants.end()) {
            continue;
        }
        for (const auto& item : tenant_it->second.metadata) {
            all_keys.push_back(item.second.user_key.empty()
                                   ? item.first
                                   : item.second.user_key);
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

auto MasterService::GetAllNoFSegments()
    -> tl::expected<std::vector<NoFSegment>, ErrorCode> {
    std::vector<MountedNoFSegmentSnapshot> mounted_segments;
    nof_segment_manager_.GetMountedSegmentsSnapshot(mounted_segments);

    std::vector<NoFSegment> result;
    for (const auto& segment : mounted_segments) {
        result.push_back(segment.segment);
    }

    return result;
}

auto MasterService::GetNoFSegmentsByName(const std::string& segment_name)
    -> tl::expected<std::vector<NoFSegmentOwnerInfo>, ErrorCode> {
    return nof_segment_manager_.GetSegmentsByName(segment_name);
}

auto MasterService::GetSegmentsDetail()
    -> tl::expected<std::vector<SegmentDetailInfo>, ErrorCode> {
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();

    // Get full info of all segments (including Segment and client_id)
    std::vector<std::pair<Segment, UUID>> all_segments;
    auto err = segment_access.GetAllSegments(all_segments);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }

    std::vector<SegmentDetailInfo> result;
    result.reserve(all_segments.size());

    for (const auto& [segment, client_id] : all_segments) {
        SegmentDetailInfo info;
        info.segment_name = segment.name;
        info.segment_id = segment.id;
        info.client_id = client_id;
        info.base_address = segment.base;
        info.size_bytes = segment.size;
        info.te_endpoint = segment.te_endpoint;
        info.protocol = segment.protocol;

        // Query segment status
        segment_access.GetSegmentStatusByName(segment.name, info.status);

        // Query allocator used/capacity
        size_t used = 0, capacity = 0;
        segment_access.QuerySegments(segment.name, used, capacity);
        info.allocator_used_bytes = used;
        info.allocator_capacity_bytes = capacity;

        result.push_back(std::move(info));
    }

    return result;
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

auto MasterService::QuerySegmentStatus(const std::string& segment_name)
    -> tl::expected<SegmentStatus, ErrorCode> {
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();
    SegmentStatus status = SegmentStatus::UNDEFINED;
    auto err = segment_access.GetSegmentStatusByName(segment_name, status);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return status;
}

auto MasterService::QuerySegmentStatusById(const UUID& segment_id)
    -> tl::expected<SegmentStatus, ErrorCode> {
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();
    SegmentStatus status = SegmentStatus::UNDEFINED;
    auto err = segment_access.GetSegmentStatusById(segment_id, status);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return status;
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
            unique_ips.emplace(getHostNameWithoutPort(segment.te_endpoint));
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
        // BatchReplicaClear is a default-tenant compatibility/admin helper.
        MetadataAccessorRW accessor(this, MakeObjectIdentity(key, "default"));
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

            // Erase the entire metadata (all replicas will be deallocated)
            // accessor.Erase() internally calls EraseMetadata which already
            // decrements disk_object_count via OnDiskReplicaRemoved.
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

            has_replica_on_segment =
                metadata.HasReplica(match_replica_on_segment);

            if (!has_replica_on_segment) {
                LOG(WARNING)
                    << "BatchReplicaClear: key=" << key
                    << " has no replica on segment_name=" << segment_name
                    << ", skipping";
                continue;
            }

            bool had_completed_disk_on_segment =
                metadata.HasReplica([&segment_name](const Replica& r) {
                    if (!r.is_local_disk_replica() || !r.is_completed())
                        return false;
                    for (const auto& name : r.get_segment_names()) {
                        if (name.has_value() && name.value() == segment_name)
                            return true;
                    }
                    return false;
                });

            EraseReplicasWithCacheTotalAccounting(metadata,
                                                  match_replica_on_segment);

            if (had_completed_disk_on_segment &&
                !metadata.HasReplica([](const Replica& r) {
                    return r.is_local_disk_replica() && r.is_completed();
                })) {
                auto& shard = accessor.GetShard();
                shard.OnDiskReplicaRemoved(had_completed_disk_on_segment,
                                           metadata);
            }

            // If no valid replicas remain, erase the entire metadata
            // accessor.Erase() internally calls EraseMetadata which already
            // decrements disk_object_count via OnDiskReplicaRemoved.
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

auto MasterService::GetReplicaListByRegex(const std::string& regex_pattern,
                                          const std::string& tenant_id)
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
    const auto normalized_tenant = NormalizeTenantId(tenant_id);
    for (size_t i = 0; i < kNumShards; ++i) {
        MetadataShardAccessorRO shard(this, i);
        auto tenant_it = shard->tenants.find(normalized_tenant);
        if (tenant_it == shard->tenants.end()) {
            continue;
        }
        for (const auto& [key, metadata] : tenant_it->second.metadata) {
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
                GrantLeaseForGroup(tenant_it->second, key, metadata);
            }
        }
    }

    return results;
}

auto MasterService::GetReplicaList(const std::string& key,
                                   const std::string& tenant_id)
    -> tl::expected<GetReplicaListResponse, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    const auto object_id = MakeObjectIdentity(key, tenant_id);

    GetReplicaListResponse resp({}, default_kv_lease_ttl_);
    bool promotion_eligible = false;
    {
        MetadataAccessorRO accessor(this, object_id);

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

        // TODO: NoF SSD support (ranhaojia)
        if (replica_list[0].is_memory_replica()) {
            MasterMetricManager::instance().inc_mem_cache_hit_nums();
        } else if (replica_list[0].is_disk_replica()) {
            MasterMetricManager::instance().inc_file_cache_hit_nums();
        }
        MasterMetricManager::instance().inc_valid_get_nums();
        // Grant a lease to the object so it will not be removed
        // when the client is reading it.
        auto* ts = accessor.GetTenantState();
        if (ts) {
            GrantLeaseForGroup(*ts, key, metadata);
        } else {
            metadata.GrantLease(default_kv_lease_ttl_,
                                default_kv_soft_pin_ttl_);
        }

        // Promotion-on-hit eligibility: only when no MEMORY replica is
        // present but at least one LOCAL_DISK replica is. Decided here while
        // we hold the RO accessor; the actual enqueue happens after we
        // release the accessor below to avoid lock-upgrade complexity.
        if (promotion_on_hit_) {
            const bool any_memory =
                metadata.HasReplica(&Replica::fn_is_memory_replica);
            const bool any_local_disk =
                metadata.HasReplica(&Replica::fn_is_local_disk_replica);
            promotion_eligible = !any_memory && any_local_disk;
        }

        resp = GetReplicaListResponse(std::move(replica_list),
                                      default_kv_lease_ttl_);
    }
    // RO accessor released. Safe to take a fresh RW accessor now.
    if (promotion_eligible) {
        TryPushPromotionQueue(object_id);
    }
    return resp;
}

std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
MasterService::BatchGetReplicaList(const std::vector<std::string>& keys,
                                   const std::string& tenant_id) {
    using GetResult = tl::expected<GetReplicaListResponse, ErrorCode>;

    std::vector<GetResult> results(
        keys.size(), tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND));
    if (keys.empty()) {
        return results;
    }

    const auto normalized_tenant = NormalizeTenantId(tenant_id);
    constexpr size_t kInvalidKeyIndex = std::numeric_limits<size_t>::max();
    std::array<size_t, kNumShards> key_list_heads;
    key_list_heads.fill(kInvalidKeyIndex);
    std::vector<size_t> next_key_indexes(keys.size(), kInvalidKeyIndex);
    {
        std::shared_lock<std::shared_mutex> lock(group_routing_mutex_);
        for (size_t i = keys.size(); i > 0; --i) {
            const size_t original_idx = i - 1;
            const auto scoped_key =
                MakeTenantScopedKey(normalized_tenant, keys[original_idx]);
            const auto route_it = object_group_ids_.find(scoped_key);
            const size_t shard_idx =
                route_it == object_group_ids_.end()
                    ? getShardIndex(normalized_tenant, keys[original_idx])
                    : getShardIndex(route_it->second);
            next_key_indexes[original_idx] = key_list_heads[shard_idx];
            key_list_heads[shard_idx] = original_idx;
        }
    }

    const size_t start_shard = RandomIndex(kNumShards);
    for (size_t scanned = 0; scanned < kNumShards; ++scanned) {
        const size_t shard_idx =
            (start_shard + kNumShards - scanned) % kNumShards;
        if (key_list_heads[shard_idx] == kInvalidKeyIndex) {
            continue;
        }

        std::vector<ObjectIdentity> promotion_candidates;
        std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
        {
            MetadataShardAccessorRO shard(this, shard_idx);
            const auto tenant_it = shard->tenants.find(normalized_tenant);
            for (size_t original_idx = key_list_heads[shard_idx];
                 original_idx != kInvalidKeyIndex;
                 original_idx = next_key_indexes[original_idx]) {
                const std::string& key = keys[original_idx];
                MasterMetricManager::instance().inc_total_get_nums();

                if (tenant_it == shard->tenants.end()) {
                    VLOG(1) << "key=" << key << ", info=object_not_found";
                    results[original_idx] =
                        tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
                    continue;
                }

                const auto& tenant_state = tenant_it->second;
                const auto metadata_it = tenant_state.metadata.find(key);
                if (metadata_it == tenant_state.metadata.end() ||
                    !metadata_it->second.IsValid()) {
                    VLOG(1) << "key=" << key << ", info=object_not_found";
                    results[original_idx] =
                        tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
                    continue;
                }

                const auto& metadata = metadata_it->second;
                std::vector<Replica::Descriptor> replica_list;
                metadata.VisitReplicas(
                    &Replica::fn_is_completed,
                    [&replica_list](const Replica& replica) {
                        replica_list.emplace_back(replica.get_descriptor());
                    });

                if (replica_list.empty()) {
                    LOG(WARNING)
                        << "key=" << key << ", error=replica_not_ready";
                    results[original_idx] =
                        tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
                    continue;
                }

                if (replica_list[0].is_memory_replica()) {
                    MasterMetricManager::instance().inc_mem_cache_hit_nums();
                } else if (replica_list[0].is_disk_replica()) {
                    MasterMetricManager::instance().inc_file_cache_hit_nums();
                }
                MasterMetricManager::instance().inc_valid_get_nums();
                GrantLeaseForGroup(tenant_state, key, metadata);

                if (promotion_on_hit_) {
                    const bool any_memory =
                        metadata.HasReplica(&Replica::fn_is_memory_replica);
                    const bool any_local_disk =
                        metadata.HasReplica(&Replica::fn_is_local_disk_replica);
                    if (!any_memory && any_local_disk) {
                        promotion_candidates.push_back(
                            MakeObjectIdentity(key, normalized_tenant));
                    }
                }

                results[original_idx] = GetReplicaListResponse(
                    std::move(replica_list), default_kv_lease_ttl_);
            }
        }

        for (const auto& object_id : promotion_candidates) {
            TryPushPromotionQueue(object_id);
        }
    }

    return results;
}

auto MasterService::AllocateAndInsertMetadata(
    MetadataShardAccessorRW& shard, const UUID& client_id,
    const std::string& key, uint64_t value_length,
    const ReplicateConfig& config, const std::string& group_id,
    const std::string& tenant_id,
    const std::chrono::system_clock::time_point& now)
    -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode> {
    auto& tenant_state = shard->tenants[tenant_id];
    if (tenant_state.metadata.contains(key)) {
        LOG(INFO) << "key=" << key << ", info=object_already_exists";
        return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
    }
    if (GetGroupRoute(tenant_id, key).has_value()) {
        LOG(INFO) << "key=" << key << ", info=object_already_exists";
        return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
    }

    const uint64_t reserved_quota_charge =
        RequestedMemoryQuotaCharge(value_length, config);
    auto quota_result = ReserveTenantQuota(tenant_id, reserved_quota_charge);
    if (!quota_result) {
        return tl::make_unexpected(quota_result.error());
    }
    auto abort_reserved_quota = [&] {
        AbortTenantQuota(tenant_id, reserved_quota_charge);
    };

    std::vector<Replica> replicas;
    const auto write_mode = DetermineReplicaWriteMode(config);
    size_t allocated_memory_replicas = 0;
    size_t allocated_nof_replicas = 0;
    if (config.replica_num > 0) {
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
            allocator_manager, value_length, config.replica_num,
            preferred_segments);

        if (!allocation_result.has_value()) {
            VLOG(1) << "Failed to allocate replicas for key=" << key
                    << ", error: " << allocation_result.error();
            if (allocation_result.error() == ErrorCode::INVALID_PARAMS) {
                abort_reserved_quota();
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            if (write_mode != ReplicaWriteMode::FLEXIBLE_DUAL_REPLICA) {
                MasterMetricManager::instance().inc_put_start_alloc_failures();
                need_mem_eviction_ = true;
                abort_reserved_quota();
                return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
            }
        } else {
            allocated_memory_replicas = allocation_result->size();
            replicas = std::move(allocation_result.value());
        }
    }

#ifdef USE_NOF
    if (config.nof_replica_num > 0 &&
        nof_segment_manager_.getMountedSegmentCount() > 0) {
        ScopedAllocatorAccess allocator_access =
            nof_segment_manager_.getAllocatorAccess();
        const auto& allocator_manager = allocator_access.getAllocatorManager();

        std::vector<std::string> preferred_segments =
            config.preferred_nof_segments;

        auto allocation_result = allocation_strategy_->Allocate(
            allocator_manager, value_length, config.nof_replica_num,
            preferred_segments, std::set<std::string>(), ReplicaType::NOF_SSD);

        if (!allocation_result.has_value()) {
            VLOG(1) << "Failed to allocate nof replicas for key=" << key
                    << ", error: " << allocation_result.error();
            if (allocation_result.error() == ErrorCode::INVALID_PARAMS) {
                abort_reserved_quota();
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            if (write_mode != ReplicaWriteMode::FLEXIBLE_DUAL_REPLICA) {
                MasterMetricManager::instance().inc_put_start_alloc_failures();
                need_nof_eviction_ = true;
                abort_reserved_quota();
                return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
            }
        } else {
            allocated_nof_replicas = allocation_result->size();
            for (auto& replica : allocation_result.value()) {
                replicas.push_back(std::move(replica));
            }
        }
    }
#endif

    if (!HasExpectedReplicaAllocation(config, allocated_memory_replicas,
                                      allocated_nof_replicas)) {
        if ((config.replica_num > 0 &&
             allocated_memory_replicas != config.replica_num) ||
            (config.nof_replica_num > 0 &&
             allocated_nof_replicas != config.nof_replica_num)) {
            MasterMetricManager::instance().inc_put_start_alloc_failures();
            if (config.replica_num > 0 &&
                allocated_memory_replicas != config.replica_num) {
                need_mem_eviction_ = true;
            }
            if (config.nof_replica_num > 0 &&
                allocated_nof_replicas != config.nof_replica_num) {
                need_nof_eviction_ = true;
            }
        }
        VLOG(1) << "Failed to satisfy replica allocation requirement for key="
                << key << ", requested_memory_replicas=" << config.replica_num
                << ", allocated_memory_replicas=" << allocated_memory_replicas
                << ", requested_nof_replicas=" << config.nof_replica_num
                << ", allocated_nof_replicas=" << allocated_nof_replicas;
        abort_reserved_quota();
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }

    if (use_disk_replica_) {
        std::string file_path =
            ResolvePathFromKey(key, root_fs_dir_, cluster_id_);
        replicas.emplace_back(file_path, value_length,
                              ReplicaStatus::PROCESSING);
    }

    std::vector<Replica::Descriptor> replica_list;
    replica_list.reserve(replicas.size());
    int i = 0;
    VLOG(1) << "PutStart, create replicas: client_id=" << client_id
            << ", key=" << key << ", value_length=" << value_length;
    for (const auto& replica : replicas) {
        const auto desc = replica.get_descriptor();
        replica_list.emplace_back(desc);

        if (replica.is_memory_replica()) {
            const auto& mem_desc = desc.get_memory_descriptor();
            VLOG(1) << "Replica #" << ++i << ": buffer_address="
                    << mem_desc.buffer_descriptor.buffer_address_
                    << ", transport_endpoint="
                    << mem_desc.buffer_descriptor.transport_endpoint_;
        } else if (replica.is_nof_replica()) {
            const auto& nof_desc = desc.get_nof_descriptor();
            VLOG(1) << "Replica #" << ++i << ": buffer_address="
                    << nof_desc.buffer_descriptor.buffer_address_
                    << ", transport_endpoint="
                    << nof_desc.buffer_descriptor.transport_endpoint_;
        }
    }

    auto [it, inserted] = tenant_state.metadata.emplace(
        std::piecewise_construct, std::forward_as_tuple(key),
        std::forward_as_tuple(client_id, now, value_length, std::move(replicas),
                              config.with_soft_pin, config.with_hard_pin,
                              config.data_type, group_id, tenant_id, key));
    if (!inserted) {
        LOG(INFO) << "key=" << key << ", info=object_already_exists";
        abort_reserved_quota();
        return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
    }
    it->second.reserved_quota_charge_bytes = reserved_quota_charge;
    RegisterGroupMember(tenant_state, tenant_id, key, group_id);
    tenant_state.processing_keys.insert(key);

    return replica_list;
}

auto MasterService::PutStart(const UUID& client_id, const std::string& key,
                             const std::string& tenant_id,
                             const uint64_t slice_length,
                             const ReplicateConfig& config)
    -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode> {
    const auto object_id = MakeObjectIdentity(key, tenant_id);
    if ((config.replica_num == 0 && config.nof_replica_num == 0) ||
        key.empty() || slice_length == 0) {
        LOG(ERROR) << "key=" << key << ", replica_num=" << config.replica_num
                   << ", nof_replica_num=" << config.nof_replica_num
                   << ", slice_length=" << slice_length
                   << ", key_size=" << key.size() << ", error=invalid_params";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (config.prefer_alloc_in_same_node && config.nof_replica_num > 0) {
        LOG(ERROR) << "key=" << key
                   << ", nof_replica_num=" << config.nof_replica_num
                   << ", prefer_alloc_in_same_node="
                   << config.prefer_alloc_in_same_node
                   << ", error=nof_not_supported_with_prefer_same_node";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
#ifndef USE_NOF
    if (config.nof_replica_num > 0) {
        LOG(ERROR) << "key=" << key
                   << ", nof_replica_num=" << config.nof_replica_num
                   << ", error=nof_pool_disabled";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
#endif

    if ((memory_allocator_type_ == BufferAllocatorType::CACHELIB) &&
        (slice_length > kMaxSliceSize)) {
        LOG(ERROR) << "key=" << key << ", slice_length=" << slice_length
                   << ", max_size=" << kMaxSliceSize
                   << ", error=invalid_slice_size";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    VLOG(1) << "key=" << key << ", value_length=" << slice_length
            << ", config=" << config << ", action=put_start_begin";

    auto group_id_result = GetGroupIdForKey(config, 1, 0);
    if (!group_id_result) {
        return tl::make_unexpected(group_id_result.error());
    }
    const std::string group_id = group_id_result.value();

    [[maybe_unused]] auto object_operation_lock =
        AcquireObjectOperationLock(object_id.tenant_id, object_id.user_key);
    const auto now = std::chrono::system_clock::now();
    std::optional<size_t> retry_shard_idx;
    {
        auto alive_clients = getAliveClientsSnapshot();
        std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
        const size_t lookup_shard_idx =
            getMetadataShardIndex(object_id.tenant_id, object_id.user_key);
        MetadataShardAccessorRW shard(this, lookup_shard_idx);
        auto& tenant_state = shard->tenants[object_id.tenant_id];

        auto it = tenant_state.metadata.find(key);
        if (it != tenant_state.metadata.end()) {
            if (CleanupStaleHandles(it->second, alive_clients, &shard)) {
                tenant_state.processing_keys.erase(key);
                tenant_state.replication_tasks.erase(key);
                tenant_state.offloading_tasks.erase(key);
                ErasePromotionTaskIfPresent(tenant_state, key,
                                            object_id.tenant_id);
                EraseMetadata(tenant_state, it, object_id.tenant_id,
                              QuotaEraseMode::kFull, &shard);
                it = tenant_state.metadata.end();
            } else {
                auto& metadata = it->second;
                if (metadata.HasReplica(&Replica::fn_is_completed) ||
                    metadata.put_start_time + put_start_discard_timeout_sec_ >=
                        now) {
                    LOG(INFO)
                        << "key=" << key << ", info=object_already_exists";
                    return tl::make_unexpected(
                        ErrorCode::OBJECT_ALREADY_EXISTS);
                }
                auto replicas =
                    metadata.PopReplicas(&Replica::fn_is_processing);
                if (!replicas.empty()) {
                    std::lock_guard lock(discarded_replicas_mutex_);
                    discarded_replicas_.emplace_back(
                        std::move(replicas),
                        metadata.put_start_time +
                            put_start_release_timeout_sec_);
                }
                tenant_state.processing_keys.erase(key);
                EraseMetadata(tenant_state, it, object_id.tenant_id,
                              QuotaEraseMode::kFull, &shard);
                it = tenant_state.metadata.end();
            }
        }

        if (it == tenant_state.metadata.end()) {
            const size_t target_shard_idx =
                group_id.empty()
                    ? getShardIndex(object_id.tenant_id, object_id.user_key)
                    : getShardIndex(group_id);
            if (target_shard_idx != lookup_shard_idx) {
                retry_shard_idx = target_shard_idx;
                if (tenant_state.Empty()) {
                    shard->tenants.erase(object_id.tenant_id);
                }
            } else {
                return AllocateAndInsertMetadata(shard, client_id, key,
                                                 slice_length, config, group_id,
                                                 object_id.tenant_id, now);
            }
        }
    }
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    MetadataShardAccessorRW shard(this, retry_shard_idx.value());
    auto& retry_tenant_state = shard->tenants[object_id.tenant_id];
    if (GetGroupRoute(object_id.tenant_id, object_id.user_key).has_value() ||
        retry_tenant_state.metadata.contains(key)) {
        LOG(INFO) << "key=" << key << ", info=object_already_exists";
        return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
    }
    return AllocateAndInsertMetadata(shard, client_id, key, slice_length,
                                     config, group_id, object_id.tenant_id,
                                     now);
}

auto MasterService::PutEnd(const UUID& client_id, const std::string& key,
                           const std::string& tenant_id,
                           ReplicaType replica_type)
    -> tl::expected<void, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    const auto object_id = MakeObjectIdentity(key, tenant_id);
    MetadataAccessorRW accessor(this, object_id);
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
            if (replica_type == ReplicaType::ALL) {
                return (replica.is_memory_replica() &&
                        !replica.has_invalid_mem_handle()) ||
                       (replica.is_nof_replica() &&
                        !replica.has_invalid_nof_handle());
            }
            if (replica_type == ReplicaType::MEMORY) {
                return replica.is_memory_replica() &&
                       !replica.has_invalid_mem_handle();
            }
            if (replica_type == ReplicaType::NOF_SSD) {
                return replica.is_nof_replica() &&
                       !replica.has_invalid_nof_handle();
            }
            return replica.type() == replica_type;
        },
        [](Replica& replica) { replica.mark_complete(); });

    const bool has_memory_replica = metadata.HasMemReplica();
    const bool should_settle_quota =
        replica_type == ReplicaType::MEMORY ||
        (replica_type == ReplicaType::ALL && has_memory_replica) ||
        !has_memory_replica;
    if (metadata.reserved_quota_charge_bytes > 0 && should_settle_quota) {
        const uint64_t actual_charge = CompletedMemoryQuotaCharge(metadata);
        const uint64_t commit_charge =
            actual_charge > metadata.committed_quota_charge_bytes
                ? actual_charge - metadata.committed_quota_charge_bytes
                : 0;
        const uint64_t abort_charge =
            metadata.reserved_quota_charge_bytes > commit_charge
                ? metadata.reserved_quota_charge_bytes - commit_charge
                : 0;
        CommitTenantQuota(object_id.tenant_id, commit_charge);
        AbortTenantQuota(object_id.tenant_id, abort_charge);
        metadata.reserved_quota_charge_bytes = 0;
        metadata.committed_quota_charge_bytes = actual_charge;
        ReleaseTenantQuota(object_id.tenant_id,
                           metadata.pending_replaced_quota_charge_bytes);
        metadata.pending_replaced_quota_charge_bytes = 0;
    }

    if (enable_offload_ && !offload_on_evict_) {
        auto& tenant_state = accessor.GetTenantState();
        metadata.VisitReplicas(
            [](const Replica& replica) {
                return replica.is_completed() && replica.is_memory_replica();
            },
            [this, &object_id, &tenant_state](Replica& replica) {
                auto result = PushOffloadingQueue(object_id, replica);
                if (result) {
                    replica.inc_refcnt();
                    tenant_state.offloading_tasks.emplace(
                        object_id.user_key,
                        OffloadingTask{replica.id(),
                                       std::chrono::system_clock::now()});
                }
            });
    }

    // If the object is completed, remove it from the processing set.
    if (metadata.AllReplicas(&Replica::fn_is_completed) &&
        accessor.InProcessing()) {
        accessor.EraseFromProcessing();
    }

    SyncCacheTotalAccounting(metadata);
    // TODO: add inc_nof_cache_nums() (ranhaojia)
    // 1. Set lease timeout to now, indicating that the object has no lease
    // at beginning. 2. If this object has soft pin enabled, set it to be soft
    // pinned.
    metadata.GrantLease(0, default_kv_soft_pin_ttl_);
    return {};
}

auto MasterService::AddReplica(const UUID& client_id, const std::string& key,
                               const std::string& tenant_id, Replica& replica)
    -> tl::expected<void, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    const auto object_id = MakeObjectIdentity(key, tenant_id);
    MetadataAccessorRW accessor(this, object_id);
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
        auto& shard = accessor.GetShard();
        shard.OnDiskReplicaAdded(metadata);
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
                              const std::string& tenant_id,
                              ReplicaType replica_type)
    -> tl::expected<void, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    const auto object_id = MakeObjectIdentity(key, tenant_id);
    MetadataAccessorRW accessor(this, object_id);
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

    auto processing_rep = metadata.GetFirstReplica([replica_type](
                                                       const Replica& replica) {
        if (replica_type == ReplicaType::ALL) {
            return (replica.is_memory_replica() || replica.is_nof_replica()) &&
                   !replica.is_processing();
        }
        return replica.type() == replica_type && !replica.is_processing();
    });
    if (processing_rep != nullptr) {
        LOG(ERROR) << "key=" << key << ", status=" << processing_rep->status()
                   << ", error=invalid_replica_status";
        return tl::make_unexpected(ErrorCode::INVALID_WRITE);
    }

    const uint64_t before_charge = CompletedMemoryQuotaCharge(metadata);
    EraseReplicasWithCacheTotalAccounting(
        metadata, [replica_type](const Replica& replica) {
            if (replica_type == ReplicaType::ALL) {
                return replica.is_memory_replica() || replica.is_nof_replica();
            }
            return replica.type() == replica_type;
        });
    const uint64_t after_charge = CompletedMemoryQuotaCharge(metadata);
    if (before_charge > after_charge) {
        ReleaseCommittedQuotaCharge(metadata, before_charge - after_charge);
    }
    if (!metadata.HasReplica(&Replica::fn_is_memory_replica)) {
        AbortTenantQuota(object_id.tenant_id,
                         metadata.reserved_quota_charge_bytes);
        metadata.reserved_quota_charge_bytes = 0;
    }

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
    const UUID& client_id, const std::vector<std::string>& keys,
    const std::string& tenant_id, ReplicaType replica_type) {
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(keys.size());
    for (const auto& key : keys) {
        results.emplace_back(PutEnd(client_id, key, tenant_id, replica_type));
    }
    return results;
}

std::vector<tl::expected<void, ErrorCode>> MasterService::BatchPutRevoke(
    const UUID& client_id, const std::vector<std::string>& keys,
    const std::string& tenant_id, ReplicaType replica_type) {
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(keys.size());
    for (const auto& key : keys) {
        results.emplace_back(
            PutRevoke(client_id, key, tenant_id, replica_type));
    }
    return results;
}

// UpsertStart — insert-or-update entry point.
//
// Three-way dispatch depending on key state:
//   Case A: key does not exist  → allocate new buffers (same as PutStart)
//   Case B: key exists, same size → in-place update (reuse existing buffers)
//   Case C: key exists, different size → discard old + allocate new
//
// Before reaching Case B/C the function runs safety checks and may preempt
// an in-progress Put/Upsert on the same key.  Preempted PROCESSING replicas
// are moved to discarded_replicas_ for delayed release (the previous writer
// may still be performing RDMA writes to those buffers).
//
// Note: during Case B the key is temporarily unreadable (all replicas are
// PROCESSING).  Readers will get REPLICA_IS_NOT_READY until UpsertEnd.
auto MasterService::UpsertStart(const UUID& client_id, const std::string& key,
                                const std::string& tenant_id,
                                const uint64_t slice_length,
                                const ReplicateConfig& config)
    -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode> {
    const auto object_id = MakeObjectIdentity(key, tenant_id);
    // --- Parameter validation (same as PutStart) ---
    if ((config.replica_num == 0 && config.nof_replica_num == 0) ||
        key.empty() || slice_length == 0) {
        LOG(ERROR) << "key=" << key << ", replica_num=" << config.replica_num
                   << ", nof_replica_num=" << config.nof_replica_num
                   << ", slice_length=" << slice_length
                   << ", key_size=" << key.size() << ", error=invalid_params";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (config.prefer_alloc_in_same_node && config.nof_replica_num > 0) {
        LOG(ERROR) << "key=" << key
                   << ", nof_replica_num=" << config.nof_replica_num
                   << ", prefer_alloc_in_same_node="
                   << config.prefer_alloc_in_same_node
                   << ", error=nof_not_supported_with_prefer_same_node";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
#ifndef USE_NOF
    if (config.nof_replica_num > 0) {
        LOG(ERROR) << "key=" << key
                   << ", nof_replica_num=" << config.nof_replica_num
                   << ", error=nof_pool_disabled";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
#endif

    if ((memory_allocator_type_ == BufferAllocatorType::CACHELIB) &&
        (slice_length > kMaxSliceSize)) {
        LOG(ERROR) << "key=" << key << ", slice_length=" << slice_length
                   << ", max_size=" << kMaxSliceSize
                   << ", error=invalid_slice_size";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    VLOG(1) << "key=" << key << ", value_length=" << slice_length
            << ", config=" << config << ", action=upsert_start_begin";

    auto group_id_result = GetGroupIdForKey(config, 1, 0);
    if (!group_id_result) {
        return tl::make_unexpected(group_id_result.error());
    }
    const std::string group_id = group_id_result.value();

    [[maybe_unused]] auto object_operation_lock =
        AcquireObjectOperationLock(object_id.tenant_id, object_id.user_key);
    const auto now = std::chrono::system_clock::now();
    std::optional<size_t> case_a_retry_shard_idx;
    {
        // --- Lock acquisition ---
        auto alive_clients = getAliveClientsSnapshot();
        std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
        // Use getMetadataShardIndex to find the object at its current shard
        // (handles both grouped and ungrouped routing).
        const size_t lookup_shard_idx =
            getMetadataShardIndex(object_id.tenant_id, object_id.user_key);
        MetadataShardAccessorRW shard(this, lookup_shard_idx);
        auto& tenant_state = shard->tenants[object_id.tenant_id];

        auto it = tenant_state.metadata.find(key);

        // --- Step 0: stale handle cleanup ---
        if (it != tenant_state.metadata.end() &&
            CleanupStaleHandles(it->second, alive_clients, &shard)) {
            tenant_state.processing_keys.erase(key);
            ErasePromotionTaskIfPresent(tenant_state, key, object_id.tenant_id);
            EraseMetadata(tenant_state, it, object_id.tenant_id,
                          QuotaEraseMode::kFull, &shard);
            it = tenant_state.metadata.end();
        }

        // --- Step 1: safety checks and preemption (only if key exists) ---
        if (it != tenant_state.metadata.end()) {
            auto& metadata = it->second;

            // Reject if the caller tries to change group membership.
            // Group membership is immutable while an object exists.
            if (config.group_ids.has_value() && metadata.group_id != group_id) {
                LOG(ERROR) << "key=" << key
                           << ", error=group_membership_is_immutable";
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }

            // Reject if a Copy/Move task is actively reading this key's
            // replicas.
            if (tenant_state.replication_tasks.count(key) > 0) {
                LOG(INFO) << "key=" << key
                          << ", error=object_has_replication_task";
                return tl::make_unexpected(
                    ErrorCode::OBJECT_HAS_REPLICATION_TASK);
            }

            // Reject if an offload-to-disk task is in progress (same reason).
            if (tenant_state.offloading_tasks.count(key) > 0) {
                LOG(INFO) << "key=" << key
                          << ", error=object_has_offloading_task";
                return tl::make_unexpected(
                    ErrorCode::OBJECT_HAS_REPLICATION_TASK);
            }

            // Preempt an in-progress Put/Upsert on the same key.  The previous
            // writer's PROCESSING replicas are moved to discarded_replicas_
            // with a TTL so they are not freed while the old writer may still
            // be doing RDMA writes.  Unlike PutStart (which only preempts after
            // a timeout), UpsertStart preempts immediately.
            if (tenant_state.processing_keys.count(key) > 0) {
                auto processing_replicas =
                    metadata.PopReplicas(&Replica::fn_is_processing);
                if (!processing_replicas.empty()) {
                    std::lock_guard lock(discarded_replicas_mutex_);
                    discarded_replicas_.emplace_back(
                        std::move(processing_replicas),
                        now + put_start_release_timeout_sec_);
                }
                tenant_state.processing_keys.erase(key);

                // If no COMPLETE replicas survive the preemption, this key
                // effectively does not exist — fall through to Case A.
                if (!metadata.HasReplica(&Replica::fn_is_completed)) {
                    ErasePromotionTaskIfPresent(tenant_state, key,
                                                object_id.tenant_id);
                    EraseMetadata(tenant_state, it, object_id.tenant_id,
                                  QuotaEraseMode::kFull, &shard);
                    it = tenant_state.metadata.end();
                }
            }
        }

        // --- Case A: key does not exist (or was erased above) ---
        // Allocate fresh buffers, identical to PutStart.
        if (it == tenant_state.metadata.end()) {
            VLOG(1) << "key=" << key << ", action=upsert_start_case_a";
            const size_t case_a_shard_idx =
                group_id.empty()
                    ? getShardIndex(object_id.tenant_id, object_id.user_key)
                    : getShardIndex(group_id);
            if (case_a_shard_idx != lookup_shard_idx) {
                case_a_retry_shard_idx = case_a_shard_idx;
                if (tenant_state.Empty()) {
                    shard->tenants.erase(object_id.tenant_id);
                }
            } else {
                return AllocateAndInsertMetadata(shard, client_id, key,
                                                 slice_length, config, group_id,
                                                 object_id.tenant_id, now);
            }
        } else {
            // --- Step 2: key exists with COMPLETE replicas → Case B or C ---
            auto& metadata = it->second;

            // Reject if any reader holds a reference (refcnt > 0).  Overwriting
            // a buffer that an RDMA read is streaming from would cause data
            // corruption. The client should retry after readers finish.
            if (metadata.HasReplica(&Replica::fn_is_busy)) {
                LOG(INFO) << "key=" << key << ", error=object_replica_busy";
                return tl::make_unexpected(ErrorCode::OBJECT_REPLICA_BUSY);
            }

            if (metadata.size == slice_length) {
                // --- Case B: same size — in-place update ---
                // Reuse existing buffer addresses.  No allocation or
                // deallocation. The client will RDMA-write new data to the same
                // addresses.
                //
                // hard_pinned is const and preserved automatically — upsert
                // does not change the eviction protection level of an existing
                // object.
                metadata.client_id = client_id;
                metadata.put_start_time = now;

                // Reconcile soft_pin state with the incoming config.
                {
                    SpinLocker locker(&metadata.lock);
                    if (config.with_soft_pin && !metadata.soft_pin_timeout) {
                        metadata.soft_pin_timeout.emplace();
                        MasterMetricManager::instance().inc_soft_pin_key_count(
                            1);
                    } else if (!config.with_soft_pin &&
                               metadata.soft_pin_timeout) {
                        metadata.soft_pin_timeout.reset();
                        MasterMetricManager::instance().dec_soft_pin_key_count(
                            1);
                    }
                }

                // Mark COMPLETE → PROCESSING so readers won't see stale data
                // mid-transfer.  The key becomes unreadable until UpsertEnd.
                metadata.VisitReplicas(
                    &Replica::fn_is_completed,
                    [](Replica& replica) { replica.mark_processing(); });
                SyncCacheTotalAccounting(metadata);

                tenant_state.processing_keys.insert(key);

                // Return the existing descriptors — same buffer addresses as
                // before.
                std::vector<Replica::Descriptor> replica_list;
                const auto& all_replicas = metadata.GetAllReplicas();
                replica_list.reserve(all_replicas.size());
                for (const auto& replica : all_replicas) {
                    replica_list.emplace_back(replica.get_descriptor());
                }

                VLOG(1) << "key=" << key
                        << ", action=upsert_start_case_b_inplace";
                return replica_list;
            }

            // --- Case C: different size — discard old replicas and reallocate
            // --- Old buffers cannot be reused.  Move them to
            // discarded_replicas_ for delayed release (readers may still hold
            // descriptors without refcnt), then allocate fresh buffers at the
            // new size.
            //
            // Preserve hard_pin and soft_pin from the old metadata so that
            // eviction protection survives a size-changing upsert (RFC §2.2.2).
            ReplicateConfig merged_config = config;
            merged_config.with_hard_pin =
                merged_config.with_hard_pin || metadata.IsHardPinned();
            merged_config.with_soft_pin =
                merged_config.with_soft_pin || metadata.IsSoftPinned();

            const std::string existing_group_id = metadata.group_id;
            const uint64_t old_quota_charge =
                metadata.committed_quota_charge_bytes != 0
                    ? metadata.committed_quota_charge_bytes
                    : CompletedMemoryQuotaCharge(metadata);
            auto old_replicas = PopReplicasWithCacheTotalAccounting(metadata);
            if (!old_replicas.empty()) {
                std::lock_guard lock(discarded_replicas_mutex_);
                discarded_replicas_.emplace_back(
                    std::move(old_replicas),
                    now + put_start_release_timeout_sec_);
            }
            EraseMetadata(tenant_state, it, object_id.tenant_id,
                          QuotaEraseMode::kPreserveOld, &shard);

            VLOG(1) << "key=" << key
                    << ", action=upsert_start_case_c_reallocate";
            auto allocate_result = AllocateAndInsertMetadata(
                shard, client_id, key, slice_length, merged_config,
                existing_group_id, object_id.tenant_id, now);
            if (!allocate_result) {
                ReleaseTenantQuota(object_id.tenant_id, old_quota_charge);
                return allocate_result;
            }
            auto new_it = tenant_state.metadata.find(key);
            if (new_it != tenant_state.metadata.end()) {
                new_it->second.pending_replaced_quota_charge_bytes =
                    old_quota_charge;
            }
            return allocate_result;
        }
    }
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    MetadataShardAccessorRW shard(this, case_a_retry_shard_idx.value());
    auto& retry_tenant_state = shard->tenants[object_id.tenant_id];
    const auto current_route =
        GetGroupRoute(object_id.tenant_id, object_id.user_key);
    if (current_route.has_value() ||
        retry_tenant_state.metadata.contains(key)) {
        LOG(INFO) << "key=" << key << ", info=object_already_exists";
        return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
    }
    return AllocateAndInsertMetadata(shard, client_id, key, slice_length,
                                     config, group_id, object_id.tenant_id,
                                     now);
}

auto MasterService::UpsertEnd(const UUID& client_id, const std::string& key,
                              const std::string& tenant_id,
                              ReplicaType replica_type)
    -> tl::expected<void, ErrorCode> {
    return PutEnd(client_id, key, tenant_id, replica_type);
}

auto MasterService::UpsertRevoke(const UUID& client_id, const std::string& key,
                                 const std::string& tenant_id,
                                 ReplicaType replica_type)
    -> tl::expected<void, ErrorCode> {
    return PutRevoke(client_id, key, tenant_id, replica_type);
}

std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
MasterService::BatchUpsertStart(const UUID& client_id,
                                const std::vector<std::string>& keys,
                                const std::string& tenant_id,
                                const std::vector<uint64_t>& slice_lengths,
                                const ReplicateConfig& config) {
    if (keys.size() != slice_lengths.size()) {
        LOG(ERROR) << "BatchUpsertStart: keys.size()=" << keys.size()
                   << " != slice_lengths.size()=" << slice_lengths.size();
        return std::vector<
            tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>(
            keys.size(), tl::make_unexpected(ErrorCode::INVALID_PARAMS));
    }
    if (config.group_ids.has_value() &&
        config.group_ids->size() != keys.size()) {
        LOG(ERROR) << "BatchUpsertStart: group_ids.size()="
                   << config.group_ids->size()
                   << " != keys.size()=" << keys.size();
        return std::vector<
            tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>(
            keys.size(), tl::make_unexpected(ErrorCode::INVALID_PARAMS));
    }
    std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
        results;
    results.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        auto key_config = config.ForSingleKey(i);
        results.emplace_back(UpsertStart(client_id, keys[i], tenant_id,
                                         slice_lengths[i], key_config));
    }
    return results;
}

std::vector<tl::expected<void, ErrorCode>> MasterService::BatchUpsertEnd(
    const UUID& client_id, const std::vector<std::string>& keys,
    const std::string& tenant_id) {
    return BatchPutEnd(client_id, keys, tenant_id);
}

std::vector<tl::expected<void, ErrorCode>> MasterService::BatchUpsertRevoke(
    const UUID& client_id, const std::vector<std::string>& keys,
    const std::string& tenant_id) {
    return BatchPutRevoke(client_id, keys, tenant_id);
}

auto MasterService::EvictDiskReplica(const UUID& client_id,
                                     const std::string& key,
                                     const std::string& tenant_id,
                                     ReplicaType replica_type)
    -> tl::expected<void, ErrorCode> {
    const auto object_id = MakeObjectIdentity(key, tenant_id);
    MetadataAccessorRW accessor(this, object_id);
    if (!accessor.Exists()) {
        LOG(INFO) << "key=" << key << ", tenant_id=" << object_id.tenant_id
                  << ", info=object_not_found_for_eviction";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto& metadata = accessor.Get();

    if (replica_type == ReplicaType::DISK) {
        EraseReplicasWithCacheTotalAccounting(
            metadata,
            [](const Replica& replica) { return replica.is_disk_replica(); });
    } else if (replica_type == ReplicaType::LOCAL_DISK) {
        bool had_completed_disk = metadata.HasReplica([](const Replica& r) {
            return r.is_local_disk_replica() && r.is_completed();
        });
        metadata.EraseReplicas([&client_id](const Replica& replica) {
            return replica.is_local_disk_replica() &&
                   replica.get_descriptor()
                           .get_local_disk_descriptor()
                           .client_id == client_id;
        });
        if (had_completed_disk) {
            auto& shard = accessor.GetShard();
            shard.OnDiskReplicaRemoved(had_completed_disk, metadata);
        }
    } else {
        LOG(ERROR) << "key=" << key
                   << ", error=invalid_replica_type_for_eviction";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    if (!metadata.IsValid()) {
        accessor.Erase();
    }
    return {};
}

std::vector<tl::expected<void, ErrorCode>> MasterService::BatchEvictDiskReplica(
    const UUID& client_id, const std::vector<std::string>& keys,
    const std::string& tenant_id, ReplicaType replica_type) {
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(keys.size());
    for (const auto& key : keys) {
        results.push_back(
            EvictDiskReplica(client_id, key, tenant_id, replica_type));
    }
    return results;
}

tl::expected<CopyStartResponse, ErrorCode> MasterService::CopyStart(
    const UUID& client_id, const std::string& key, const std::string& tenant_id,
    const std::string& src_segment,
    const std::vector<std::string>& tgt_segments) {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    const auto object_id = MakeObjectIdentity(key, tenant_id);
    {
        ScopedSegmentAccess segment_access =
            segment_manager_.getSegmentAccess();
        for (const auto& tgt_segment : tgt_segments) {
            if (!segment_access.ExistsSegmentName(tgt_segment)) {
                LOG(ERROR) << "key=" << key << ", tgt_segment=" << tgt_segment
                           << ", error=target_segment_not_found";
                return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
            }
            if (!segment_access.IsSegmentAllocatable(tgt_segment)) {
                LOG(ERROR) << "key=" << key << ", tgt_segment=" << tgt_segment
                           << ", error=target_segment_not_allocatable";
                return tl::make_unexpected(
                    ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
            }
        }
    }
    MetadataAccessorRW accessor(this, object_id);
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
        // PR2 limitation: Copy can allocate extra physical MEMORY replicas
        // without tenant quota admission. It does not change the logical
        // object set; full quota-aware Copy admission is deferred.
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
    auto& tenant_state = accessor.GetTenantState();
    tenant_state.replication_tasks.emplace(
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

tl::expected<void, ErrorCode> MasterService::CopyEnd(
    const UUID& client_id, const std::string& key,
    const std::string& tenant_id) {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    MetadataAccessorRW accessor(this, MakeObjectIdentity(key, tenant_id));
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
        EraseReplicasWithCacheTotalAccounting(
            metadata, [&task](const Replica& replica) {
                return std::find(task.replica_ids.begin(),
                                 task.replica_ids.end(),
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
    SyncCacheTotalAccounting(metadata);

    accessor.EraseReplicationTask();

    return all_complete ? tl::expected<void, ErrorCode>()
                        : tl::make_unexpected(ErrorCode::REPLICA_IS_GONE);
}

tl::expected<void, ErrorCode> MasterService::CopyRevoke(
    const UUID& client_id, const std::string& key,
    const std::string& tenant_id) {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    MetadataAccessorRW accessor(this, MakeObjectIdentity(key, tenant_id));
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
        EraseReplicasWithCacheTotalAccounting(
            metadata, [&replica_id](const Replica& replica) {
                return replica.id() == replica_id;
            });
    }

    accessor.EraseReplicationTask();

    if (!metadata.IsValid()) {
        // Remove the object if it does not have any replicas.
        accessor.Erase();
    }

    return {};
}

tl::expected<MoveStartResponse, ErrorCode> MasterService::MoveStart(
    const UUID& client_id, const std::string& key, const std::string& tenant_id,
    const std::string& src_segment, const std::string& tgt_segment) {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    const auto object_id = MakeObjectIdentity(key, tenant_id);
    if (src_segment == tgt_segment) {
        LOG(ERROR) << "key=" << key << ", move_tgt=" << tgt_segment
                   << " cannot be the same as move_src=" << src_segment;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    {
        ScopedSegmentAccess segment_access =
            segment_manager_.getSegmentAccess();
        if (!segment_access.ExistsSegmentName(tgt_segment)) {
            LOG(ERROR) << "key=" << key << ", tgt_segment=" << tgt_segment
                       << ", error=target_segment_not_found";
            return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
        }
        if (!segment_access.IsSegmentAllocatable(tgt_segment)) {
            LOG(ERROR) << "key=" << key << ", tgt_segment=" << tgt_segment
                       << ", error=target_segment_not_allocatable";
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }
    }

    MetadataAccessorRW accessor(this, object_id);
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
        // PR2 limitation: Move can allocate a replacement physical MEMORY
        // replica without tenant quota admission. Logical object accounting is
        // unchanged; full quota-aware Move admission is deferred.
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
    auto& tenant_state = accessor.GetTenantState();
    tenant_state.replication_tasks.emplace(
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

tl::expected<void, ErrorCode> MasterService::MoveEnd(
    const UUID& client_id, const std::string& key,
    const std::string& tenant_id) {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    MetadataAccessorRW accessor(this, MakeObjectIdentity(key, tenant_id));
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
        EraseReplicasWithCacheTotalAccounting(
            metadata, [&task](const Replica& replica) {
                return std::find(task.replica_ids.begin(),
                                 task.replica_ids.end(),
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
        SyncCacheTotalAccounting(metadata);
    }

    // Remove the source replica and release its space later.
    auto source_replica = PopReplicasWithCacheTotalAccounting(
        metadata, [&source_id](const Replica& replica) {
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
    const UUID& client_id, const std::string& key,
    const std::string& tenant_id) {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    MetadataAccessorRW accessor(this, MakeObjectIdentity(key, tenant_id));
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
        EraseReplicasWithCacheTotalAccounting(
            metadata, [&replica_id](const Replica& replica) {
                return replica.id() == replica_id;
            });
    }

    accessor.EraseReplicationTask();

    if (!metadata.IsValid()) {
        // Remove the object if it does not have any replicas.
        accessor.Erase();
    }

    return {};
}

auto MasterService::Remove(const std::string& key, const std::string& tenant_id,
                           bool force) -> tl::expected<void, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    const auto object_id = MakeObjectIdentity(key, tenant_id);
    MetadataAccessorRW accessor(this, object_id);
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

    auto& tenant_state = accessor.GetTenantState();
    ErasePromotionTaskIfPresent(tenant_state, key, object_id.tenant_id);
    accessor.Erase();
    return {};
}

auto MasterService::RemoveByRegex(const std::string& regex_pattern,
                                  const std::string& tenant_id, bool force)
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
    const auto normalized_tenant = NormalizeTenantId(tenant_id);
    for (size_t i = 0; i < kNumShards; ++i) {
        MetadataShardAccessorRW shard(this, i);
        auto tenant_it = shard->tenants.find(normalized_tenant);
        if (tenant_it == shard->tenants.end()) {
            continue;
        }
        auto& tenant_state = tenant_it->second;

        for (auto it = tenant_state.metadata.begin();
             it != tenant_state.metadata.end();) {
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
                if (tenant_state.replication_tasks.contains(it->first)) {
                    LOG(WARNING) << "key=" << it->first
                                 << ", matched by regex, but has replication "
                                    "task. Skipping removal.";
                    ++it;
                    continue;
                }

                VLOG(1) << "key=" << it->first
                        << " matched by regex. Removing.";
                ErasePromotionTaskIfPresent(tenant_state, it->first,
                                            normalized_tenant);
                it = EraseMetadata(tenant_state, it, normalized_tenant,
                                   QuotaEraseMode::kFull, &shard);
                removed_count++;
            } else {
                ++it;
            }
        }
        if (tenant_state.Empty()) {
            shard->tenants.erase(tenant_it);
        }
    }

    VLOG(1) << "action=remove_by_regex, pattern=" << regex_pattern
            << ", removed_count=" << removed_count;
    return removed_count;
}

long MasterService::RemoveAll(bool force) {
    long removed_count = 0;
    uint64_t total_freed_size = 0;
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    auto now = std::chrono::system_clock::now();

    for (size_t i = 0; i < kNumShards; i++) {
        MetadataShardAccessorRW shard(this, i);
        for (auto tenant_it = shard->tenants.begin();
             tenant_it != shard->tenants.end();) {
            auto& tenant_state = tenant_it->second;
            auto it = tenant_state.metadata.begin();
            while (it != tenant_state.metadata.end()) {
                if ((force || it->second.IsLeaseExpired(now)) &&
                    it->second.AllReplicas(&Replica::fn_is_completed) &&
                    !tenant_state.replication_tasks.contains(it->first)) {
                    auto mem_rep_count = it->second.CountReplicas(
                        &Replica::fn_is_memory_replica);
                    total_freed_size += it->second.size * mem_rep_count;
                    ErasePromotionTaskIfPresent(tenant_state, it->first,
                                                tenant_it->first);
                    it = EraseMetadata(tenant_state, it, tenant_it->first,
                                       QuotaEraseMode::kFull, &shard);
                    removed_count++;
                } else {
                    ++it;
                }
            }
            if (tenant_state.Empty()) {
                tenant_it = shard->tenants.erase(tenant_it);
            } else {
                ++tenant_it;
            }
        }
    }

    VLOG(1) << "action=remove_all_objects"
            << ", removed_count=" << removed_count
            << ", total_freed_size=" << total_freed_size;
    return removed_count;
}

long MasterService::RemoveAll(const std::string& tenant_id, bool force) {
    long removed_count = 0;
    uint64_t total_freed_size = 0;
    // Store the current time to avoid repeatedly
    // calling std::chrono::steady_clock::now()
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    auto now = std::chrono::system_clock::now();
    const auto normalized_tenant = NormalizeTenantId(tenant_id);

    for (size_t i = 0; i < kNumShards; i++) {
        MetadataShardAccessorRW shard(this, i);
        auto tenant_it = shard->tenants.find(normalized_tenant);
        if (tenant_it == shard->tenants.end()) {
            continue;
        }
        auto& tenant_state = tenant_it->second;
        auto it = tenant_state.metadata.begin();
        while (it != tenant_state.metadata.end()) {
            if ((force || it->second.IsLeaseExpired(now)) &&
                it->second.AllReplicas(&Replica::fn_is_completed) &&
                !tenant_state.replication_tasks.contains(it->first)) {
                auto mem_rep_count =
                    it->second.CountReplicas(&Replica::fn_is_memory_replica);
                total_freed_size += it->second.size * mem_rep_count;
                ErasePromotionTaskIfPresent(tenant_state, it->first,
                                            normalized_tenant);
                it = EraseMetadata(tenant_state, it, normalized_tenant,
                                   QuotaEraseMode::kFull, &shard);
                removed_count++;
            } else {
                ++it;
            }
        }
        if (tenant_state.Empty()) {
            shard->tenants.erase(tenant_it);
        }
    }

    VLOG(1) << "action=remove_all_objects"
            << ", tenant_id=" << normalized_tenant
            << ", removed_count=" << removed_count
            << ", total_freed_size=" << total_freed_size;
    return removed_count;
}

auto MasterService::BatchRemove(const std::vector<std::string>& keys,
                                const std::string& tenant_id, bool force)
    -> std::vector<tl::expected<void, ErrorCode>> {
    std::vector<tl::expected<void, ErrorCode>> results(keys.size());
    const auto normalized_tenant = NormalizeTenantId(tenant_id);

    // Group keys by shard to reduce lock contention
    std::unordered_map<size_t,
                       std::vector<std::pair<size_t, const std::string*>>>
        keys_by_shard;
    keys_by_shard.reserve(
        std::min(keys.size(), static_cast<size_t>(kNumShards)));

    for (size_t i = 0; i < keys.size(); ++i) {
        size_t shard_idx = getMetadataShardIndex(normalized_tenant, keys[i]);
        keys_by_shard[shard_idx].emplace_back(i, &keys[i]);
    }

    std::shared_lock<std::shared_mutex> snapshot_lock(snapshot_mutex_);

    auto alive_clients = getAliveClientsSnapshot();

    // Process each shard once, acquiring lock per shard
    for (auto& [shard_idx, key_group] : keys_by_shard) {
        MetadataShardAccessorRW shard(this, shard_idx);
        auto now = std::chrono::system_clock::now();

        for (const auto& [original_idx, key_ptr] : key_group) {
            const std::string& key = *key_ptr;
            auto tenant_it = shard->tenants.find(normalized_tenant);
            if (tenant_it == shard->tenants.end()) {
                VLOG(1) << "key=" << key << ", error=object_not_found";
                results[original_idx] =
                    tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
                continue;
            }
            auto& tenant_state = tenant_it->second;
            auto it = tenant_state.metadata.find(key);

            if (it == tenant_state.metadata.end()) {
                VLOG(1) << "key=" << key << ", error=object_not_found";
                results[original_idx] =
                    tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
                continue;
            }

            // Clean up stale replica handles (consistent with single Remove)
            if (CleanupStaleHandles(it->second, alive_clients, &shard)) {
                tenant_state.processing_keys.erase(key);
                tenant_state.replication_tasks.erase(key);
                tenant_state.offloading_tasks.erase(key);
                ErasePromotionTaskIfPresent(tenant_state, key,
                                            normalized_tenant);
                EraseMetadata(tenant_state, it, normalized_tenant,
                              QuotaEraseMode::kFull, &shard);
                if (tenant_state.Empty()) {
                    shard->tenants.erase(tenant_it);
                }
                results[original_idx] =
                    tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
                continue;
            }
            if (!it->second.IsValid()) {
                results[original_idx] =
                    tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
                continue;
            }

            auto& metadata = it->second;

            if (!force && !metadata.IsLeaseExpired(now)) {
                VLOG(1) << "key=" << key << ", error=object_has_lease";
                results[original_idx] =
                    tl::make_unexpected(ErrorCode::OBJECT_HAS_LEASE);
                continue;
            }

            if (!metadata.AllReplicas(&Replica::fn_is_completed)) {
                LOG(ERROR) << "key=" << key << ", error=replica_not_ready";
                results[original_idx] =
                    tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
                continue;
            }

            if (tenant_state.replication_tasks.contains(key)) {
                LOG(ERROR) << "key=" << key
                           << ", error=object_has_replication_task";
                results[original_idx] =
                    tl::make_unexpected(ErrorCode::OBJECT_HAS_REPLICATION_TASK);
                continue;
            }

            // Remove object metadata
            ErasePromotionTaskIfPresent(tenant_state, key, normalized_tenant);
            EraseMetadata(tenant_state, it, normalized_tenant,
                          QuotaEraseMode::kFull, &shard);
            if (tenant_state.Empty()) {
                shard->tenants.erase(tenant_it);
            }
            results[original_idx] = {};  // Success
        }
    }

    return results;
}

bool MasterService::CleanupStaleHandles(
    ObjectMetadata& metadata,
    const std::unordered_set<UUID, boost::hash<UUID>>& alive_clients,
    MetadataShardAccessorRW* shard) {
    bool had_completed_disk = metadata.HasReplica([](const Replica& r) {
        return r.is_local_disk_replica() && r.is_completed();
    });
    // Remove those with invalid allocators (memory replicas on unmounted
    // segments) and local_disk replicas whose owner client is no longer alive.
    const uint64_t before_charge = CompletedMemoryQuotaCharge(metadata);
    EraseReplicasWithCacheTotalAccounting(
        metadata, [&alive_clients](const Replica& replica) {
            return (replica.has_invalid_mem_handle() ||
                    replica.has_invalid_nof_handle() ||
                    replica.has_stale_local_disk_client(alive_clients)) &&
                   replica.is_completed();
        });
    const uint64_t after_charge = CompletedMemoryQuotaCharge(metadata);
    if (before_charge > after_charge) {
        ReleaseCommittedQuotaCharge(metadata, before_charge - after_charge);
    }
    if (had_completed_disk && shard &&
        !metadata.HasReplica([](const Replica& r) {
            return r.is_local_disk_replica() && r.is_completed();
        })) {
        shard->OnDiskReplicaRemoved(had_completed_disk, metadata);
    }

    // Return true if no valid replicas remain after cleanup
    return !metadata.IsValid();
}

size_t MasterService::GetKeyCount() const {
    size_t total = 0;
    for (size_t i = 0; i < kNumShards; i++) {
        MetadataShardAccessorRO shard(this, i);
        for (const auto& [tenant_id, tenant_state] : shard->tenants) {
            total += tenant_state.metadata.size();
        }
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

    // Notify the client monitor thread to start tracking this client's TTL.
    // Without this, a client that only mounts a LOCAL_DISK segment (and
    // doesn't ping) would be considered expired by ClientMonitorFunc, which
    // would then clear all its LOCAL_DISK replicas.
    PodUUID pod_client_id;
    pod_client_id.first = client_id.first;
    pod_client_id.second = client_id.second;
    if (!client_ping_queue_.push(pod_client_id)) {
        LOG(ERROR) << "client_id=" << client_id
                   << ", error=client_ping_queue_full";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    return {};
}

auto MasterService::OffloadObjectHeartbeat(const UUID& client_id,
                                           bool enable_offloading)
    -> tl::expected<std::vector<OffloadTaskItem>, ErrorCode> {
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
    std::unordered_map<std::string, OffloadTaskItem> offloading_objects_copy;
    {
        MutexLocker locker(&local_disk_segment_it->second->offloading_mutex_);
        local_disk_segment_it->second->enable_offloading = enable_offloading;
        if (enable_offloading) {
            std::vector<OffloadTaskItem> result;
            result.reserve(
                local_disk_segment_it->second->offloading_objects.size());
            for (const auto& [_, task] :
                 local_disk_segment_it->second->offloading_objects) {
                result.push_back(task);
            }
            local_disk_segment_it->second->offloading_objects.clear();
            return result;
        }
        // Offloading is disabled: clear the pending queue to prevent
        // unbounded growth that would trigger KEYS_ULTRA_LIMIT in
        // PushOffloadingQueue. We must also clean up corresponding
        // offloading_tasks and decrement source replica refcounts to avoid
        // resource leaks and blocked writes (OBJECT_HAS_REPLICATION_TASK).
        // Copy keys out before releasing the mutex to avoid lock order
        // violation: the lock order is Shard Lock -> offloading_mutex_, so we
        // must release offloading_mutex_ before taking shard locks via
        // MetadataAccessorRW.
        offloading_objects_copy =
            std::move(local_disk_segment_it->second->offloading_objects);
    }

    for (auto& [_, task] : offloading_objects_copy) {
        const auto object_id = MakeObjectIdentity(task.key, task.tenant_id);
        MetadataAccessorRW accessor(this, object_id);
        if (accessor.Exists()) {
            auto& tenant_state = accessor.GetTenantState();
            auto task_it =
                tenant_state.offloading_tasks.find(object_id.user_key);
            if (task_it != tenant_state.offloading_tasks.end()) {
                auto source =
                    accessor.Get().GetReplicaByID(task_it->second.source_id);
                if (source) {
                    source->dec_refcnt();
                }
                tenant_state.offloading_tasks.erase(task_it);
            }
        }
    }
    return {};
}

auto MasterService::ReportSsdCapacity(const UUID& client_id,
                                      int64_t ssd_total_capacity_bytes)
    -> tl::expected<void, ErrorCode> {
    if (ssd_total_capacity_bytes < 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
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
    int64_t old_capacity =
        local_disk_segment_it->second->ssd_total_capacity_bytes;
    if (ssd_total_capacity_bytes != old_capacity) {
        local_disk_segment_it->second->ssd_total_capacity_bytes =
            ssd_total_capacity_bytes;
        if (old_capacity > 0) {
            MasterMetricManager::instance().dec_total_file_capacity(
                old_capacity);
        }
        if (ssd_total_capacity_bytes > 0) {
            MasterMetricManager::instance().inc_total_file_capacity(
                ssd_total_capacity_bytes);
        }
    }
    return {};
}

auto MasterService::NotifyOffloadSuccess(
    const UUID& client_id, const std::vector<OffloadTaskItem>& tasks,
    const std::vector<StorageObjectMetadata>& metadatas)
    -> tl::expected<void, ErrorCode> {
    if (tasks.size() != metadatas.size()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    for (size_t i = 0; i < tasks.size(); ++i) {
        const auto& task = tasks[i];
        const auto& metadata = metadatas[i];
        const auto object_id = MakeObjectIdentity(task.key, task.tenant_id);

        // Release refcnt and clear offloading task.
        {
            MetadataAccessorRW accessor(this, object_id);
            if (accessor.Exists()) {
                auto& obj_metadata = accessor.Get();
                auto& tenant_state = accessor.GetTenantState();
                auto task_it =
                    tenant_state.offloading_tasks.find(object_id.user_key);
                if (task_it != tenant_state.offloading_tasks.end()) {
                    auto source =
                        obj_metadata.GetReplicaByID(task_it->second.source_id);
                    if (source != nullptr) {
                        source->dec_refcnt();
                    }
                    tenant_state.offloading_tasks.erase(task_it);
                }
            }
        }

        // Add LOCAL_DISK replica.
        Replica replica(client_id, metadata.data_size,
                        metadata.transport_endpoint, ReplicaStatus::COMPLETE);
        auto res = AddReplica(client_id, object_id.user_key,
                              object_id.tenant_id, replica);
        if (!res && res.error() != ErrorCode::OBJECT_NOT_FOUND) {
            LOG(ERROR) << "Failed to add replica: error=" << res.error()
                       << ", client_id=" << client_id
                       << ", tenant_id=" << object_id.tenant_id
                       << ", key=" << object_id.user_key;
            return tl::make_unexpected(res.error());
        }
    }
    return {};
}

tl::expected<void, ErrorCode> MasterService::PushOffloadingQueue(
    const ObjectIdentity& object_id, Replica& replica) {
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
        const int64_t size = replica.get_descriptor()
                                 .get_memory_descriptor()
                                 .buffer_descriptor.size_;
        auto res = local_disk_segment_it->second->offloading_objects.emplace(
            MakeTenantScopedStorageKey(object_id.tenant_id, object_id.user_key),
            OffloadTaskItem{.tenant_id = object_id.tenant_id,
                            .key = object_id.user_key,
                            .size = size});
        if (!res.second) {
            return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
        }
    }
    return {};
}

// Promotion-on-hit

// Push a key onto the holder client's promotion_objects map. Resolves the
// holder via the LOCAL_DISK replica's embedded client_id rather than via
// the segment-name reverse lookup.
tl::expected<void, ErrorCode> MasterService::PushPromotionQueue(
    const ObjectIdentity& object_id, Replica& source_replica) {
    auto holder_id = source_replica.get_local_disk_client_id();
    if (!holder_id.has_value()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    ScopedLocalDiskSegmentAccess local_disk_segment_access =
        segment_manager_.getLocalDiskSegmentAccess();
    auto& client_local_disk_segment =
        local_disk_segment_access.getClientLocalDiskSegment();
    auto local_disk_segment_it =
        client_local_disk_segment.find(holder_id.value());
    if (local_disk_segment_it == client_local_disk_segment.end()) {
        // Holder client expired or never had a LocalDiskSegment registered;
        // the LOCAL_DISK replica will be cleaned up by ClientMonitorFunc on
        // its own schedule.
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    MutexLocker locker(&local_disk_segment_it->second->offloading_mutex_);
    auto res = local_disk_segment_it->second->promotion_objects.emplace(
        MakeTenantScopedStorageKey(object_id.tenant_id, object_id.user_key),
        PromotionTaskItem{
            .tenant_id = object_id.tenant_id,
            .key = object_id.user_key,
            .size = static_cast<int64_t>(source_replica.get_descriptor()
                                             .get_local_disk_descriptor()
                                             .object_size)});
    if (!res.second) {
        return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
    }
    return {};
}

void MasterService::TryPushPromotionQueue(const ObjectIdentity& object_id) {
    if (!promotion_on_hit_ || !promotion_sketch_) {
        return;
    }
    const auto& key = object_id.user_key;
    const auto admission_key =
        MakeTenantScopedStorageKey(object_id.tenant_id, key);

    // Frequency gate: bump and compare against the threshold. The sketch
    // returns uint8_t (saturating at 255); promotion_admission_threshold_
    // is clamped into [1, 255] at config parse time (see master.cpp), so
    // direct comparison is well-defined and threshold=0 (which would
    // bypass the gate entirely since freq is uint8_t) cannot reach here.
    const uint8_t freq = promotion_sketch_->increment(admission_key);
    if (freq < promotion_admission_threshold_) {
        MasterMetricManager::instance().inc_promotion_rejected_frequency();
        return;
    }

    // Watermark gate: don't promote if DRAM is already under eviction
    // pressure. The check is best-effort (state can change between this
    // sample and the actual allocation in PromotionAllocStart).
    const double used_ratio =
        MasterMetricManager::instance().get_global_mem_used_ratio();
    if (used_ratio >= eviction_high_watermark_ratio_) {
        MasterMetricManager::instance().inc_promotion_rejected_watermark();
        return;
    }

    // Acquire a fresh RW shard accessor for dedup, refcnt-pin, and task
    // record. Safe to call here because GetReplicaList has already released
    // its RO accessor.
    MetadataAccessorRW accessor(this, object_id);
    if (!accessor.Exists()) {
        return;
    }
    auto& metadata = accessor.Get();
    auto& tenant_state = accessor.GetTenantState();

    // Dedup: don't queue twice if a promotion is already in flight or if a
    // MEMORY replica has appeared since GetReplicaList observed only-disk.
    if (tenant_state.promotion_tasks.count(key) > 0) {
        return;
    }
    if (metadata.HasReplica(&Replica::fn_is_memory_replica)) {
        return;
    }

    // Cap gate: read the cluster-wide in-flight count. Soft cap — a
    // benign TOCTOU race between this load and the emplace below can let
    // a few extra tasks slip in, but the per-shard mutex already
    // serializes inserts within a shard and the dedup gate above prevents
    // duplicate work, so the worst case is N concurrent inserters across
    // distinct shards each admitting one extra task. Atomic load is
    // relaxed because the value is purely advisory.
    if (promotion_in_flight_.load(std::memory_order_relaxed) >=
        promotion_queue_limit_) {
        MasterMetricManager::instance().inc_promotion_rejected_cap();
        return;
    }

    // Find the LOCAL_DISK source replica.
    Replica* source = nullptr;
    metadata.VisitReplicas(&Replica::fn_is_local_disk_replica,
                           [&source](Replica& r) {
                               if (source == nullptr) source = &r;
                           });
    if (source == nullptr) {
        return;
    }

    // Pin the source replica.
    source->inc_refcnt();
    const uint64_t object_size =
        source->get_descriptor().get_local_disk_descriptor().object_size;

    // Try to enqueue on the holder client. On failure, drop the refcnt back.
    auto push_result = PushPromotionQueue(object_id, *source);
    if (!push_result) {
        source->dec_refcnt();
        VLOG(1) << "promotion_push_failed key=" << key
                << " error=" << push_result.error();
        return;
    }

    // Capture the holder client_id so NotifyPromotionSuccess can reject
    // calls from other clients. PushPromotionQueue already validated
    // get_local_disk_client_id() returns a value, so .value() is safe.
    const UUID holder_id = source->get_local_disk_client_id().value();

    // Record the in-flight task. alloc_id is filled in by
    // PromotionAllocStart once the new MEMORY replica is staged.
    tenant_state.promotion_tasks.emplace(
        key, PromotionTask{.source_id = source->id(),
                           .alloc_id = 0,
                           .object_size = object_size,
                           .start_time = std::chrono::system_clock::now(),
                           .holder_id = holder_id});
    promotion_in_flight_.fetch_add(1, std::memory_order_relaxed);
    MasterMetricManager::instance().inc_promotion_in_flight();
    MasterMetricManager::instance().inc_promotion_admitted();
    VLOG(1) << "promotion_queued key=" << key << " size=" << object_size;
}

auto MasterService::PromotionObjectHeartbeat(const UUID& client_id)
    -> tl::expected<std::vector<PromotionTaskItem>, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    ScopedLocalDiskSegmentAccess local_disk_segment_access =
        segment_manager_.getLocalDiskSegmentAccess();
    auto& client_local_disk_segment =
        local_disk_segment_access.getClientLocalDiskSegment();
    auto local_disk_segment_it = client_local_disk_segment.find(client_id);
    if (local_disk_segment_it == client_local_disk_segment.end()) {
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    MutexLocker locker(&local_disk_segment_it->second->offloading_mutex_);
    // Return at most promotion_max_per_heartbeat_ tasks. Each task does
    // a synchronous SSD read + RDMA write on the client side; allowing
    // more than one per heartbeat risks blocking past the client-
    // liveness window and the master marking the client dead. The rest
    // stay queued in promotion_objects for subsequent heartbeats. The
    // cap must live here (server side) rather than on the client so
    // leftover work isn't silently dropped.
    auto& src = local_disk_segment_it->second->promotion_objects;
    std::vector<PromotionTaskItem> result;
    while (result.size() < promotion_max_per_heartbeat_ && !src.empty()) {
        auto node = src.extract(src.begin());
        result.push_back(std::move(node.mapped()));
    }
    return result;
}

auto MasterService::PromotionAllocStart(
    const UUID& client_id, const std::string& key, const std::string& tenant_id,
    uint64_t size, const std::vector<std::string>& preferred_segments)
    -> tl::expected<PromotionAllocStartResponse, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    const auto object_id = MakeObjectIdentity(key, tenant_id);
    MetadataAccessorRW accessor(this, object_id);
    if (!accessor.Exists()) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    auto& metadata = accessor.Get();

    // Verify the in-flight task still exists before allocating. The
    // reaper can sweep it between the holder's heartbeat and this
    // AllocStart call (a hung client, GC pause, or HA failover can
    // stall AllocStart past put_start_release_timeout_sec_). If we
    // allocated and AddReplicas'd anyway, the staged PROCESSING MEMORY
    // replica would have no PromotionTask pointing at it: the generic
    // PROCESSING reaper iterates tenant_state.processing_keys (never
    // populated by promotion) and the promotion-task reaper would have
    // nothing left to iterate, leaking the buffer until the object is
    // removed or evicted. The shard mutex is held for the rest of this
    // function, so the iterator stays valid across the allocation step.
    auto& tenant_state = accessor.GetTenantState();
    auto task_it = tenant_state.promotion_tasks.find(object_id.user_key);
    if (task_it == tenant_state.promotion_tasks.end()) {
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }

    // Holder-only gate (see PromotionTask::holder_id doc).
    if (task_it->second.holder_id != client_id) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Defensive size check: must match the source LOCAL_DISK
    // descriptor's object_size captured at admission. A mismatch would
    // let a buggy caller request a wrong-sized allocation — smaller
    // risks RDMA overflow, larger wastes DRAM pinned until reaper TTL.
    if (task_it->second.object_size != size) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (metadata.HasReplica(&Replica::fn_is_memory_replica)) {
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }
    if (task_it->second.alloc_id != 0 ||
        task_it->second.reserved_quota_charge_bytes != 0) {
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }

    const uint64_t reserved_quota_charge = size;
    auto quota_result =
        ReserveTenantQuota(object_id.tenant_id, reserved_quota_charge);
    if (!quota_result) {
        return tl::make_unexpected(quota_result.error());
    }
    auto abort_reserved_quota = [&] {
        AbortTenantQuota(object_id.tenant_id, reserved_quota_charge);
    };

    // Allocate a single MEMORY replica via the existing strategy, biased to
    // the holder's mem segment when possible.
    ReplicateConfig config;
    config.replica_num = 1;
    if (!preferred_segments.empty()) {
        config.preferred_segments = preferred_segments;
    }

    std::vector<Replica> staged_replicas;
    {
        ScopedAllocatorAccess allocator_access =
            segment_manager_.getAllocatorAccess();
        const auto& allocator_manager = allocator_access.getAllocatorManager();
        auto allocation_result = allocation_strategy_->Allocate(
            allocator_manager, size, config.replica_num, preferred_segments);
        if (!allocation_result) {
            abort_reserved_quota();
            return tl::make_unexpected(allocation_result.error());
        }
        staged_replicas = std::move(allocation_result.value());
    }
    if (staged_replicas.empty()) {
        abort_reserved_quota();
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }

    // Append the new PROCESSING MEMORY replica to the existing object's
    // metadata. Visible only after NotifyPromotionSuccess flips it COMPLETE.
    Replica::Descriptor desc = staged_replicas[0].get_descriptor();
    const ReplicaID new_id = staged_replicas[0].id();
    std::vector<Replica> to_add;
    to_add.push_back(std::move(staged_replicas[0]));
    metadata.AddReplicas(std::move(to_add));

    // Record the new replica's ID on the in-flight PromotionTask so
    // NotifyPromotionSuccess knows exactly which replica to commit. A
    // concurrent Put on this key may stage other PROCESSING MEMORY
    // replicas; using alloc_id avoids the "first PROCESSING memory"
    // ambiguity.
    //
    // Also reset start_time so the reaper TTL covers the active-
    // transfer phase (AllocStart -> SSD read -> RDMA write -> Notify)
    // measured from when a master-allocated buffer becomes vulnerable,
    // rather than being consumed by queue-waiting. Without the reset,
    // a backlogged task could enter active transfer with little TTL
    // remaining and the reaper could free the staged replica via
    // EraseReplicaByID mid-RDMA-write. The queue-waiting phase
    // (alloc_id == 0) is bounded by its own original start_time window
    // during which the reaper's EraseReplicaByID branch is a no-op.
    task_it->second.alloc_id = new_id;
    task_it->second.reserved_quota_charge_bytes = reserved_quota_charge;
    task_it->second.start_time = std::chrono::system_clock::now();
    return PromotionAllocStartResponse{std::move(desc)};
}

auto MasterService::NotifyPromotionSuccess(const UUID& client_id,
                                           const std::string& key,
                                           const std::string& tenant_id)
    -> tl::expected<void, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    const auto object_id = MakeObjectIdentity(key, tenant_id);
    MetadataAccessorRW accessor(this, object_id);
    if (!accessor.Exists()) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    auto& metadata = accessor.Get();
    auto& tenant_state = accessor.GetTenantState();

    // Look up the in-flight task to find the exact replica we staged. A
    // concurrent Put on this key may have created other PROCESSING MEMORY
    // replicas, so we must not just "mark first PROCESSING memory
    // complete" — that would risk committing someone else's half-written
    // replica.
    auto task_it = tenant_state.promotion_tasks.find(object_id.user_key);
    if (task_it == tenant_state.promotion_tasks.end() ||
        task_it->second.alloc_id == 0) {
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }

    // Holder-only gate (see PromotionTask::holder_id doc).
    if (task_it->second.holder_id != client_id) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    bool committed = false;
    Replica* staged = metadata.GetReplicaByID(task_it->second.alloc_id);
    if (staged != nullptr && staged->is_memory_replica() &&
        staged->is_processing()) {
        staged->mark_complete();
        committed = true;
    }

    // Drop the source LOCAL_DISK replica's refcnt and erase the task.
    auto* source = metadata.GetReplicaByID(task_it->second.source_id);
    if (source != nullptr) {
        source->dec_refcnt();
    }
    const uint64_t completed_bytes = task_it->second.object_size;
    const uint64_t reserved_quota_charge =
        task_it->second.reserved_quota_charge_bytes;
    if (committed) {
        const uint64_t actual_charge = CompletedMemoryQuotaCharge(metadata);
        const uint64_t commit_charge =
            actual_charge > metadata.committed_quota_charge_bytes
                ? actual_charge - metadata.committed_quota_charge_bytes
                : 0;
        const uint64_t abort_charge =
            reserved_quota_charge > commit_charge
                ? reserved_quota_charge - commit_charge
                : 0;
        CommitTenantQuota(object_id.tenant_id, commit_charge);
        AbortTenantQuota(object_id.tenant_id, abort_charge);
        metadata.committed_quota_charge_bytes = actual_charge;
    } else {
        AbortTenantQuota(object_id.tenant_id, reserved_quota_charge);
    }
    task_it->second.reserved_quota_charge_bytes = 0;
    tenant_state.promotion_tasks.erase(task_it);
    promotion_in_flight_.fetch_sub(1, std::memory_order_relaxed);
    MasterMetricManager::instance().dec_promotion_in_flight();
    if (committed) {
        SyncCacheTotalAccounting(metadata);
        MasterMetricManager::instance().inc_promotion_completed();
        MasterMetricManager::instance().inc_promotion_completed_bytes(
            static_cast<int64_t>(completed_bytes));
    } else {
        MasterMetricManager::instance().inc_promotion_cancelled();
    }

    // Erase the per-client promotion_objects entry (best-effort; the
    // heartbeat may have already drained it).
    {
        ScopedLocalDiskSegmentAccess local_disk_segment_access =
            segment_manager_.getLocalDiskSegmentAccess();
        auto& client_local_disk_segment =
            local_disk_segment_access.getClientLocalDiskSegment();
        auto it = client_local_disk_segment.find(client_id);
        if (it != client_local_disk_segment.end()) {
            MutexLocker locker(&it->second->offloading_mutex_);
            it->second->promotion_objects.erase(MakeTenantScopedStorageKey(
                object_id.tenant_id, object_id.user_key));
        }
    }

    if (!committed) {
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }
    return {};
}

auto MasterService::NotifyPromotionFailure(const UUID& client_id,
                                           const std::string& key,
                                           const std::string& tenant_id)
    -> tl::expected<void, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    const auto object_id = MakeObjectIdentity(key, tenant_id);
    MetadataAccessorRW accessor(this, object_id);
    if (!accessor.Exists()) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    auto& metadata = accessor.Get();
    auto& tenant_state = accessor.GetTenantState();

    auto task_it = tenant_state.promotion_tasks.find(object_id.user_key);
    if (task_it == tenant_state.promotion_tasks.end()) {
        // No task to release. Either the reaper already swept it, or the
        // client never had a task here. Return OK to keep this RPC
        // idempotent — repeated failure notifications on the same key
        // should be safe.
        return {};
    }

    // Holder-only gate (see PromotionTask::holder_id doc).
    if (task_it->second.holder_id != client_id) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Mirror the reaper's expiry path; see DiscardExpiredProcessingReplicas
    // Part 4 for the full rationale on each step.
    auto* source = metadata.GetReplicaByID(task_it->second.source_id);
    if (source != nullptr) {
        source->dec_refcnt();
    }
    if (task_it->second.alloc_id != 0) {
        const ReplicaID alloc_id = task_it->second.alloc_id;
        EraseReplicasWithCacheTotalAccounting(
            metadata, [alloc_id](const Replica& replica) {
                return replica.id() == alloc_id;
            });
    }
    AbortTenantQuota(object_id.tenant_id,
                     task_it->second.reserved_quota_charge_bytes);
    task_it->second.reserved_quota_charge_bytes = 0;
    tenant_state.promotion_tasks.erase(task_it);
    promotion_in_flight_.fetch_sub(1, std::memory_order_relaxed);
    MasterMetricManager::instance().dec_promotion_in_flight();
    MasterMetricManager::instance().inc_promotion_failed();

    // Clear the holder's per-client promotion_objects entry. Same
    // best-effort cleanup pattern as NotifyPromotionSuccess — the
    // heartbeat may have already drained it.
    {
        ScopedLocalDiskSegmentAccess local_disk_segment_access =
            segment_manager_.getLocalDiskSegmentAccess();
        auto& client_local_disk_segment =
            local_disk_segment_access.getClientLocalDiskSegment();
        auto it = client_local_disk_segment.find(client_id);
        if (it != client_local_disk_segment.end()) {
            MutexLocker locker(&it->second->offloading_mutex_);
            it->second->promotion_objects.erase(MakeTenantScopedStorageKey(
                object_id.tenant_id, object_id.user_key));
        }
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
            (need_mem_eviction_ && eviction_ratio_ > 0.0)) {
            LOG(INFO) << "[EVICT-TRIGGER] memory_ratio=" << used_ratio
                      << " high_watermark=" << eviction_high_watermark_ratio_
                      << " need_mem_eviction=" << need_mem_eviction_
                      << " eviction_ratio=" << eviction_ratio_;
            double evict_ratio_target = std::max(
                eviction_ratio_,
                used_ratio - eviction_high_watermark_ratio_ + eviction_ratio_);
            double evict_ratio_lowerbound =
                std::max(evict_ratio_target * 0.5,
                         used_ratio - eviction_high_watermark_ratio_);
            BatchEvict(evict_ratio_target, evict_ratio_lowerbound);
            LOG(INFO) << "[EVICT-DONE] BatchEvict execution completed.";
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

#ifdef USE_NOF
        double nof_used_ratio =
            MasterMetricManager::instance().get_global_nof_used_ratio();
        if (nof_used_ratio > nof_eviction_high_watermark_ratio_ ||
            (need_nof_eviction_ && nof_eviction_ratio_ > 0.0)) {
            double nof_evict_ratio_target =
                std::max(nof_eviction_ratio_,
                         nof_used_ratio - nof_eviction_high_watermark_ratio_ +
                             nof_eviction_ratio_);
            double nof_evict_ratio_lowerbound =
                std::max(nof_evict_ratio_target * 0.5,
                         nof_used_ratio - nof_eviction_high_watermark_ratio_);
            NoFBatchEvict(nof_evict_ratio_target, nof_evict_ratio_lowerbound);
        }
#endif

        std::this_thread::sleep_for(
            std::chrono::milliseconds(kEvictionThreadSleepMs));
    }

    VLOG(1) << "action=eviction_thread_stopped";
}

void MasterService::DiscardExpiredProcessingReplicas(
    MetadataShardAccessorRW& shard,
    const std::chrono::system_clock::time_point& now) {
    std::list<DiscardedReplicas> discarded_replicas;

    for (auto tenant_it = shard->tenants.begin();
         tenant_it != shard->tenants.end();) {
        auto& tenant_state = tenant_it->second;

        for (auto key_it = tenant_state.processing_keys.begin();
             key_it != tenant_state.processing_keys.end();) {
            auto it = tenant_state.metadata.find(*key_it);
            if (it == tenant_state.metadata.end()) {
                LOG(ERROR) << "Key " << *key_it
                           << " was removed while in processing";
                key_it = tenant_state.processing_keys.erase(key_it);
                continue;
            }

            auto& metadata = it->second;
            if (!metadata.IsValid() ||
                metadata.AllReplicas(&Replica::fn_is_completed)) {
                if (!metadata.IsValid()) {
                    EraseMetadata(tenant_state, it, tenant_it->first,
                                  QuotaEraseMode::kFull, &shard);
                }
                key_it = tenant_state.processing_keys.erase(key_it);
                continue;
            }

            const auto ttl =
                metadata.put_start_time + put_start_release_timeout_sec_;
            if (ttl < now) {
                auto replicas =
                    metadata.PopReplicas(&Replica::fn_is_processing);
                if (!replicas.empty()) {
                    discarded_replicas.emplace_back(std::move(replicas), ttl);
                }
                if (!metadata.IsValid()) {
                    EraseMetadata(tenant_state, it, tenant_it->first,
                                  QuotaEraseMode::kFull, &shard);
                }
                key_it = tenant_state.processing_keys.erase(key_it);
                continue;
            }
            key_it++;
        }

        for (auto task_it = tenant_state.replication_tasks.begin();
             task_it != tenant_state.replication_tasks.end();) {
            auto metadata_it = tenant_state.metadata.find(task_it->first);
            if (metadata_it == tenant_state.metadata.end()) {
                LOG(ERROR) << "Key " << task_it->first
                           << " was removed with ongoing replication task";
                task_it = tenant_state.replication_tasks.erase(task_it);
                continue;
            }

            const auto ttl =
                task_it->second.start_time + put_start_release_timeout_sec_;
            if (ttl > now) {
                task_it++;
                continue;
            }

            auto& metadata = metadata_it->second;
            auto source = metadata.GetReplicaByID(task_it->second.source_id);
            if (source != nullptr) {
                source->dec_refcnt();
            }

            auto& replica_ids = task_it->second.replica_ids;
            auto replicas = PopReplicasWithCacheTotalAccounting(
                metadata, [&replica_ids](const Replica& replica) {
                    auto it = std::find(replica_ids.begin(), replica_ids.end(),
                                        replica.id());
                    return it != replica_ids.end();
                });
            if (!replicas.empty()) {
                discarded_replicas.emplace_back(std::move(replicas), ttl);
            }
            if (!metadata.IsValid()) {
                EraseMetadata(tenant_state, metadata_it, tenant_it->first,
                              QuotaEraseMode::kFull, &shard);
            }
            task_it = tenant_state.replication_tasks.erase(task_it);
        }

        for (auto task_it = tenant_state.offloading_tasks.begin();
             task_it != tenant_state.offloading_tasks.end();) {
            const auto ttl =
                task_it->second.start_time + put_start_release_timeout_sec_;
            if (ttl > now) {
                task_it++;
                continue;
            }
            auto metadata_it = tenant_state.metadata.find(task_it->first);
            if (metadata_it != tenant_state.metadata.end()) {
                auto source = metadata_it->second.GetReplicaByID(
                    task_it->second.source_id);
                if (source != nullptr) {
                    source->dec_refcnt();
                }
            }
            LOG(WARNING) << "Offloading task expired for key: "
                         << task_it->first;
            task_it = tenant_state.offloading_tasks.erase(task_it);
        }

        for (auto task_it = tenant_state.promotion_tasks.begin();
             task_it != tenant_state.promotion_tasks.end();) {
            const auto ttl =
                task_it->second.start_time + put_start_release_timeout_sec_;
            if (ttl > now) {
                task_it++;
                continue;
            }
            auto metadata_it = tenant_state.metadata.find(task_it->first);
            if (metadata_it != tenant_state.metadata.end()) {
                auto source = metadata_it->second.GetReplicaByID(
                    task_it->second.source_id);
                if (source != nullptr) {
                    source->dec_refcnt();
                }
                if (task_it->second.alloc_id != 0) {
                    const ReplicaID alloc_id = task_it->second.alloc_id;
                    EraseReplicasWithCacheTotalAccounting(
                        metadata_it->second,
                        [alloc_id](const Replica& replica) {
                            return replica.id() == alloc_id;
                        });
                }
            }
            AbortTenantQuota(tenant_it->first,
                             task_it->second.reserved_quota_charge_bytes);
            task_it->second.reserved_quota_charge_bytes = 0;
            LOG(WARNING) << "Promotion task expired for key: "
                         << task_it->first;
            task_it = tenant_state.promotion_tasks.erase(task_it);
            promotion_in_flight_.fetch_sub(1, std::memory_order_relaxed);
            MasterMetricManager::instance().dec_promotion_in_flight();
            MasterMetricManager::instance().inc_promotion_expired();
        }

        if (tenant_state.Empty()) {
            tenant_it = shard->tenants.erase(tenant_it);
        } else {
            ++tenant_it;
        }
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

        const std::string& snapshot_root =
            snapshot_catalog_store_->GetSnapshotRoot();
        const std::string path_prefix = snapshot_root + snapshot_id + "/";
        const std::string manifest_path = path_prefix + SNAPSHOT_MANIFEST_FILE;
        auto descriptor =
            BuildSnapshotDescriptor(snapshot_id, manifest_path, path_prefix);
        if (!descriptor) {
            LOG(ERROR) << "[Snapshot] Failed to build descriptor before fork, "
                          "snapshot_id="
                       << snapshot_id
                       << ", code=" << toString(descriptor.error().code)
                       << ", msg=" << descriptor.error().message;
            close(log_pipe[0]);
            close(log_pipe[1]);
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
            auto result = PersistState(descriptor.value());
            if (!result) {
                SNAP_LOG_ERROR(
                    "[Snapshot] Child process failed to persist state, "
                    "snapshot_id={},code={},msg={}",
                    snapshot_id, toString(result.error().code),
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

tl::expected<ha::OpLogSequenceId, SerializationError>
MasterService::ResolveSnapshotSequenceId() const {
    if (!enable_ha_ || ha_backend_type_ != "etcd") {
        // OpLog sequence ids start at 1. Returning 0 here is a sentinel that
        // means "no persisted OpLog boundary", so a standby that later calls
        // Recover(0) will replay from the first entry when oplog following is
        // enabled.
        return ha::OpLogSequenceId{0};
    }

#ifndef STORE_USE_ETCD
    return tl::make_unexpected(SerializationError(
        ErrorCode::UNAVAILABLE_IN_CURRENT_MODE,
        "etcd snapshot sequence resolution is unavailable in this build"));
#else
    auto oplog_store = GetSnapshotBoundaryOpLogStore();
    if (!oplog_store) {
        return tl::make_unexpected(oplog_store.error());
    }

    uint64_t sequence_id = 0;
    auto err = oplog_store.value()->GetLatestSequenceId(sequence_id);
    if (err == ErrorCode::OPLOG_ENTRY_NOT_FOUND) {
        return ha::OpLogSequenceId{0};
    }
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(SerializationError(
            err, fmt::format("failed to resolve snapshot sequence boundary: {}",
                             toString(err))));
    }

    return static_cast<ha::OpLogSequenceId>(sequence_id);
#endif
}

#ifdef STORE_USE_ETCD
tl::expected<EtcdOpLogStore*, SerializationError>
MasterService::GetSnapshotBoundaryOpLogStore() const {
    if (ha_backend_connstring_.empty()) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::INVALID_PARAMS,
            "etcd snapshot sequence resolution requires a backend connstring"));
    }

    std::lock_guard<std::mutex> lock(snapshot_boundary_oplog_store_mutex_);
    if (snapshot_boundary_oplog_store_ != nullptr) {
        return snapshot_boundary_oplog_store_.get();
    }

    auto err =
        EtcdHelper::ConnectToEtcdStoreClient(ha_backend_connstring_.c_str());
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(SerializationError(
            err, fmt::format("failed to connect to etcd for snapshot boundary: "
                             "{}",
                             toString(err))));
    }

    auto oplog_store = std::make_unique<EtcdOpLogStore>(cluster_id_);
    err = oplog_store->Init();
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(SerializationError(
            err, fmt::format("failed to initialize etcd oplog store: {}",
                             toString(err))));
    }

    snapshot_boundary_oplog_store_ = std::move(oplog_store);
    return snapshot_boundary_oplog_store_.get();
}
#endif

tl::expected<ha::SnapshotDescriptor, SerializationError>
MasterService::BuildSnapshotDescriptor(const std::string& snapshot_id,
                                       const std::string& manifest_path,
                                       const std::string& object_prefix) const {
    auto sequence_id = ResolveSnapshotSequenceId();
    if (!sequence_id) {
        return tl::make_unexpected(sequence_id.error());
    }

    const std::string& snapshot_root =
        snapshot_catalog_store_->GetSnapshotRoot();
    auto descriptor = ha::snapshot_catalog_store_detail::MakeSnapshotDescriptor(
        snapshot_root, snapshot_id);
    descriptor.last_included_seq = sequence_id.value();
    descriptor.producer_view_version = view_version_;
    descriptor.manifest_key = manifest_path;
    descriptor.object_prefix = object_prefix;
    descriptor.created_at_ms = CurrentTimeMs();
    return descriptor;
}

tl::expected<void, SerializationError> MasterService::PersistState(
    const std::string& snapshot_id) {
    const std::string& snapshot_root =
        snapshot_catalog_store_->GetSnapshotRoot();
    const std::string path_prefix = snapshot_root + snapshot_id + "/";
    const std::string manifest_path = path_prefix + SNAPSHOT_MANIFEST_FILE;
    auto descriptor =
        BuildSnapshotDescriptor(snapshot_id, manifest_path, path_prefix);
    if (!descriptor) {
        return tl::make_unexpected(descriptor.error());
    }
    return PersistState(descriptor.value());
}

tl::expected<void, SerializationError> MasterService::PersistState(
    const ha::SnapshotDescriptor& descriptor) {
    const std::string& snapshot_id = descriptor.snapshot_id;
    const std::string& path_prefix = descriptor.object_prefix;
    const std::string& manifest_path = descriptor.manifest_key;

    try {
        auto* snapshot_catalog_store = GetSnapshotCatalogStore();
        if (!snapshot_catalog_store) {
            return tl::make_unexpected(SerializationError(
                ErrorCode::PERSISTENT_FAIL,
                "snapshot catalog store is not initialized"));
        }

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
                snapshot_id, toString(metadata_result.error().code),
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
                snapshot_id, toString(segment_result.error().code),
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
                snapshot_id, toString(task_manager_result.error().code),
                task_manager_result.error().message);
            return tl::make_unexpected(task_manager_result.error());
        }
        SNAP_LOG_INFO(
            "[Snapshot] task manager serialization_successful, snapshot_id={}",
            snapshot_id);

        const auto& serialized_metadata = metadata_result.value();
        const auto& serialized_segment = segment_result.value();
        const auto& serialized_task_manager = task_manager_result.value();

        // When backup_dir is enabled, try all uploads to ensure complete backup
        // When backup_dir is disabled, use fail-fast mode
        bool upload_success = true;
        std::string error_msg;
        SNAP_LOG_INFO("[Snapshot] Backend info: {}",
                      snapshot_object_store_->GetConnectionInfo());

        // Upload metadata
        std::string metadata_path = path_prefix + SNAPSHOT_METADATA_FILE;
        auto upload_result =
            UploadSnapshotPayloadFile(serialized_metadata, metadata_path,
                                      SNAPSHOT_METADATA_FILE, snapshot_id);
        if (!upload_result) {
            SNAP_LOG_ERROR(
                "[Snapshot] metadata upload failed, snapshot_id={}, "
                "path={}, code={}, msg={}",
                snapshot_id, metadata_path,
                toString(upload_result.error().code),
                upload_result.error().message);
            if (!use_snapshot_backup_dir_) {
                return tl::make_unexpected(upload_result.error());
            }
            error_msg.append(upload_result.error().message + "\n");
            upload_success = false;
        }

        // Upload segment
        std::string segment_path = path_prefix + SNAPSHOT_SEGMENTS_FILE;
        upload_result =
            UploadSnapshotPayloadFile(serialized_segment, segment_path,
                                      SNAPSHOT_SEGMENTS_FILE, snapshot_id);
        if (!upload_result) {
            SNAP_LOG_ERROR(
                "[Snapshot] segment upload failed, snapshot_id={}, "
                "path={}, code={}, msg={}",
                snapshot_id, segment_path, toString(upload_result.error().code),
                upload_result.error().message);
            if (!use_snapshot_backup_dir_) {
                return tl::make_unexpected(upload_result.error());
            }
            error_msg.append(upload_result.error().message + "\n");
            upload_success = false;
        }

        // Upload task manager
        std::string task_manager_path =
            path_prefix + SNAPSHOT_TASK_MANAGER_FILE;
        upload_result = UploadSnapshotPayloadFile(
            serialized_task_manager, task_manager_path,
            SNAPSHOT_TASK_MANAGER_FILE, snapshot_id);
        if (!upload_result) {
            SNAP_LOG_ERROR(
                "[Snapshot] task_manager upload failed, snapshot_id={}, "
                "path={}, code={}, msg={}",
                snapshot_id, task_manager_path,
                toString(upload_result.error().code),
                upload_result.error().message);
            if (!use_snapshot_backup_dir_) {
                return tl::make_unexpected(upload_result.error());
            }
            error_msg.append(upload_result.error().message + "\n");
            upload_success = false;
        }

        // Upload manifest
        std::string manifest_content =
            fmt::format("{}|{}|{}", SNAPSHOT_SERIALIZER_TYPE,
                        SNAPSHOT_SERIALIZER_VERSION, snapshot_id);
        std::vector<uint8_t> manifest_bytes(manifest_content.begin(),
                                            manifest_content.end());
        upload_result = UploadSnapshotPayloadFile(
            manifest_bytes, manifest_path, SNAPSHOT_MANIFEST_FILE, snapshot_id);
        if (!upload_result) {
            SNAP_LOG_ERROR(
                "[Snapshot] manifest upload failed, snapshot_id={}, "
                "path={}, code={}, msg={}",
                snapshot_id, manifest_path,
                toString(upload_result.error().code),
                upload_result.error().message);
            if (!use_snapshot_backup_dir_) {
                return tl::make_unexpected(upload_result.error());
            }
            error_msg.append(upload_result.error().message + "\n");
            upload_success = false;
        }

        if (!upload_success) {
            return tl::make_unexpected(
                SerializationError(ErrorCode::PERSISTENT_FAIL, error_msg));
        }

        // Publish snapshot catalog entry and advance the latest marker.
        std::string latest_path =
            snapshot_catalog_store->GetSnapshotRoot() + SNAPSHOT_LATEST_FILE;
        std::string latest_content = snapshot_id;

        auto publish_result = snapshot_catalog_store->Publish(descriptor);
        if (publish_result != ErrorCode::OK) {
            SNAP_LOG_ERROR(
                "[Snapshot] latest update failed, snapshot_id={}, file={}, "
                "code={}",
                snapshot_id, latest_path, toString(publish_result));
            if (use_snapshot_backup_dir_) {
                auto save_path = fs::path(snapshot_backup_dir_) /
                                 SNAPSHOT_BACKUP_SAVE_DIR /
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

tl::expected<void, SerializationError> MasterService::UploadSnapshotPayloadFile(
    const std::vector<uint8_t>& data, const std::string& path,
    const std::string& local_filename, const std::string& snapshot_id) {
    SNAP_LOG_INFO("[Snapshot] Uploading {} to: {}, snapshot_id={}",
                  local_filename, path, snapshot_id);

    std::string error_msg;
    auto upload_result = snapshot_object_store_->UploadBuffer(path, data);
    if (!upload_result) {
        SNAP_LOG_ERROR(
            "[Snapshot] {} upload failed, snapshot_id={}, file={}, error={}",
            local_filename, snapshot_id, path, upload_result.error());

        // Upload failed, save locally for manual recovery in exception
        // scenarios
        if (use_snapshot_backup_dir_) {
            auto save_path = fs::path(snapshot_backup_dir_) /
                             SNAPSHOT_BACKUP_SAVE_DIR / local_filename;
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
    auto* snapshot_catalog_store = GetSnapshotCatalogStore();
    if (!snapshot_catalog_store) {
        SNAP_LOG_ERROR(
            "[Snapshot] snapshot catalog store is not initialized, "
            "snapshot_id={}",
            snapshot_id);
        return;
    }

    // List() loads one descriptor per published snapshot. This remains cheap
    // because CleanupOldSnapshot() itself enforces snapshot_retention_count_
    // and keeps the catalog single-digit in normal deployments.
    auto list_result = snapshot_catalog_store->List(kUnlimitedSnapshotList);
    if (!list_result) {
        SNAP_LOG_ERROR("[Snapshot] error=list failed, snapshot_id={}, code={}",
                       snapshot_id, toString(list_result.error()));
        return;
    }

    const auto& snapshots = list_result.value();

    if (static_cast<int>(snapshots.size()) > keep_count) {
        for (int i = keep_count; i < static_cast<int>(snapshots.size()); i++) {
            const std::string& old_state_dir = snapshots[i].snapshot_id;

            if (old_state_dir == snapshot_id) {
                SNAP_LOG_WARN(
                    "[Snapshot] Skipping deletion of current snapshot "
                    "directory {}, "
                    "snapshot_id={}",
                    old_state_dir, snapshot_id);
                continue;
            }

            auto delete_result = snapshot_catalog_store->Delete(old_state_dir);
            if (delete_result != ErrorCode::OK) {
                SNAP_LOG_ERROR(
                    "[Snapshot] Failed to delete old snapshot {}, "
                    "snapshot_id={}, code={}",
                    old_state_dir, snapshot_id, toString(delete_result));
            } else {
                SNAP_LOG_INFO(
                    "[Snapshot] Successfully deleted old snapshot {}, "
                    "snapshot_id={}",
                    old_state_dir, snapshot_id);
            }
        }
    }
}

void MasterService::RestoreState() {
    auto* snapshot_catalog_store = GetSnapshotCatalogStore();
    if (!snapshot_catalog_store) {
        LOG(ERROR) << "[Restore] Snapshot catalog store is not initialized, "
                      "starting fresh";
        return;
    }

    LOG(INFO) << "[Restore] Backend info: "
              << snapshot_object_store_->GetConnectionInfo();

    std::vector<ha::SnapshotDescriptor> restore_candidates;
    std::unordered_set<std::string> candidate_ids;
    std::optional<std::string> latest_snapshot_id;

    auto latest_result = snapshot_catalog_store->GetLatest();
    if (!latest_result) {
        LOG(WARNING) << "[Restore] Failed to load latest snapshot marker: "
                     << toString(latest_result.error())
                     << ", falling back to published snapshot listing";
    } else if (latest_result->has_value()) {
        const auto& latest_snapshot = latest_result->value();
        latest_snapshot_id = latest_snapshot.snapshot_id;
        restore_candidates.push_back(latest_snapshot);
        candidate_ids.emplace(latest_snapshot.snapshot_id);
    }

    // Snapshot ids use YYYYMMDD_HHMMSS_mmm, so lexicographic order matches
    // creation order. List() may perform one descriptor read per published
    // snapshot; retention cleanup keeps that set bounded in practice.
    auto snapshots_result =
        snapshot_catalog_store->List(kUnlimitedSnapshotList);
    if (!snapshots_result) {
        if (restore_candidates.empty()) {
            LOG(ERROR) << "[Restore] Failed to list restorable snapshots: "
                       << toString(snapshots_result.error())
                       << ", starting fresh";
            return;
        }
        LOG(WARNING) << "[Restore] Failed to list fallback snapshots: "
                     << toString(snapshots_result.error())
                     << ", attempting latest marker only";
    } else {
        for (const auto& snapshot : snapshots_result.value()) {
            // Snapshot ids are timestamp-derived, so string comparison keeps
            // only candidates at or before the latest marker chronologically.
            if (latest_snapshot_id.has_value() &&
                snapshot.snapshot_id > latest_snapshot_id.value()) {
                continue;
            }
            if (!candidate_ids.emplace(snapshot.snapshot_id).second) {
                continue;
            }
            restore_candidates.push_back(snapshot);
        }
    }

    if (restore_candidates.empty()) {
        LOG(ERROR) << "[Restore] No previous snapshot found, starting fresh";
        return;
    }

    const auto now = std::chrono::system_clock::now();
    for (const auto& snapshot : restore_candidates) {
        ResetStateAfterFailedRestoreAttempt();
        if (TryRestoreStateFromSnapshot(snapshot, now)) {
            return;
        }
    }

    ResetStateAfterFailedRestoreAttempt();
    LOG(ERROR) << "[Restore] Failed to restore from all candidate snapshots "
               << "(count=" << restore_candidates.size() << "), starting fresh";
}

bool MasterService::TryRestoreStateFromSnapshot(
    const ha::SnapshotDescriptor& snapshot,
    const std::chrono::system_clock::time_point& now) {
    const std::string& state_id = snapshot.snapshot_id;
    std::string path_prefix = snapshot.object_prefix;
    if (path_prefix.empty()) {
        path_prefix =
            snapshot_catalog_store_->GetSnapshotRoot() + state_id + "/";
    }

    std::string manifest_path = snapshot.manifest_key;
    if (manifest_path.empty()) {
        manifest_path = path_prefix + SNAPSHOT_MANIFEST_FILE;
    }

    auto fail_restore = [&](const std::string& message) {
        LOG(WARNING) << "[Restore] Snapshot candidate " << state_id
                     << " is unusable: " << message;
        ResetStateAfterFailedRestoreAttempt();
        return false;
    };

    try {
        std::string manifest_content;
        auto manifest_result = snapshot_object_store_->DownloadString(
            manifest_path, manifest_content);
        if (!manifest_result) {
            return fail_restore("failed to download manifest '" +
                                manifest_path +
                                "': " + manifest_result.error());
        }

        if (use_snapshot_backup_dir_) {
            auto save_result = FileUtil::SaveStringToFile(
                manifest_content, fs::path(snapshot_backup_dir_) /
                                      SNAPSHOT_BACKUP_RESTORE_DIR /
                                      SNAPSHOT_MANIFEST_FILE);
            if (!save_result) {
                LOG(ERROR) << "[Restore] Failed to save manifest to file: "
                           << save_result.error();
            }
        }

        std::vector<std::string> parts;
        boost::split(parts, manifest_content, boost::is_any_of("|"));
        if (parts.size() < 3) {
            return fail_restore("invalid snapshot manifest format");
        }

        const std::string& protocol_type = parts[0];
        const std::string& version = parts[1];

        LOG(INFO) << "[Restore] Trying snapshot: " << state_id
                  << " version: " << version << " protocol: " << protocol_type;

        if (protocol_type != SNAPSHOT_SERIALIZER_TYPE) {
            return fail_restore("unsupported protocol type '" + protocol_type +
                                "', expected '" + SNAPSHOT_SERIALIZER_TYPE +
                                "'");
        }
        if (version != SNAPSHOT_SERIALIZER_VERSION) {
            return fail_restore("incompatible snapshot version '" + version +
                                "', expected '" + SNAPSHOT_SERIALIZER_VERSION +
                                "'");
        }

        std::string metadata_path = path_prefix + SNAPSHOT_METADATA_FILE;
        std::vector<uint8_t> metadata_content;
        auto download_result = snapshot_object_store_->DownloadBuffer(
            metadata_path, metadata_content);
        if (!download_result) {
            return fail_restore("failed to download metadata '" +
                                metadata_path +
                                "': " + download_result.error());
        }

        if (use_snapshot_backup_dir_) {
            auto save_result = FileUtil::SaveBinaryToFile(
                metadata_content, fs::path(snapshot_backup_dir_) /
                                      SNAPSHOT_BACKUP_RESTORE_DIR /
                                      SNAPSHOT_METADATA_FILE);
            if (!save_result) {
                LOG(ERROR) << "[Restore] Failed to save metadata to file: "
                           << save_result.error();
            }
        }
        LOG(INFO) << "[Restore] Download metadata file success";

        std::string segments_path = path_prefix + SNAPSHOT_SEGMENTS_FILE;
        std::vector<uint8_t> segments_content;
        download_result = snapshot_object_store_->DownloadBuffer(
            segments_path, segments_content);
        if (!download_result) {
            return fail_restore("failed to download segments '" +
                                segments_path +
                                "': " + download_result.error());
        }
        if (use_snapshot_backup_dir_) {
            auto save_result = FileUtil::SaveBinaryToFile(
                segments_content, fs::path(snapshot_backup_dir_) /
                                      SNAPSHOT_BACKUP_RESTORE_DIR /
                                      SNAPSHOT_SEGMENTS_FILE);
            if (!save_result) {
                LOG(ERROR) << "[Restore] Failed to save segments to file: "
                           << save_result.error();
            }
        }
        LOG(INFO) << "[Restore] Download segments file success";

        std::string task_manager_path =
            path_prefix + SNAPSHOT_TASK_MANAGER_FILE;
        std::vector<uint8_t> task_manager_content;
        download_result = snapshot_object_store_->DownloadBuffer(
            task_manager_path, task_manager_content);
        if (!download_result) {
            return fail_restore("failed to download task_manager '" +
                                task_manager_path +
                                "': " + download_result.error());
        }
        if (use_snapshot_backup_dir_) {
            auto save_result = FileUtil::SaveBinaryToFile(
                task_manager_content, fs::path(snapshot_backup_dir_) /
                                          SNAPSHOT_BACKUP_RESTORE_DIR /
                                          SNAPSHOT_TASK_MANAGER_FILE);
            if (!save_result) {
                LOG(ERROR) << "[Restore] Failed to save task manager to file: "
                           << save_result.error();
            }
        }
        LOG(INFO) << "[Restore] Download task manager file success";

        SegmentSerializer segment_serializer(&segment_manager_);
        MetadataSerializer metadata_serializer(this);
        TaskManagerSerializer task_manager_serializer(&task_manager_);

        auto segments_result = segment_serializer.Deserialize(segments_content);
        if (!segments_result) {
            return fail_restore(
                fmt::format("failed to deserialize segments: {} - {}",
                            static_cast<int>(segments_result.error().code),
                            segments_result.error().message));
        }
        LOG(INFO) << "[Restore] Deserialize segments success";

        auto metadata_result =
            metadata_serializer.Deserialize(metadata_content);
        if (!metadata_result) {
            return fail_restore(
                fmt::format("failed to deserialize metadata: {} - {}",
                            static_cast<int>(metadata_result.error().code),
                            metadata_result.error().message));
        }
        LOG(INFO) << "[Restore] Deserialize metadata success";

        auto task_manager_result =
            task_manager_serializer.Deserialize(task_manager_content);
        if (!task_manager_result) {
            return fail_restore(
                fmt::format("failed to deserialize task manager: {} - {}",
                            static_cast<int>(task_manager_result.error().code),
                            task_manager_result.error().message));
        }
        LOG(INFO) << "[Restore] Deserialize task manager success";

        std::vector<std::string> segment_names;
        {
            ScopedSegmentAccess segment_access =
                segment_manager_.getSegmentAccess();
            segment_access.GetAllSegmentNames(segment_names);
        }

        {
            const bool skip_cleanup = std::getenv(
                "MOONCAKE_MASTER_SERVICE_SNAPSHOT_TEST_SKIP_CLEANUP");
            if (!skip_cleanup) {
                auto cleanup_now = now;
                for (auto& shard : metadata_shards_) {
                    for (auto tenant_it = shard.tenants.begin();
                         tenant_it != shard.tenants.end();) {
                        auto& tenant_state = tenant_it->second;
                        for (auto it = tenant_state.metadata.begin();
                             it != tenant_state.metadata.end();) {
                            if (it->second.HasDiffRepStatus(
                                    ReplicaStatus::COMPLETE) ||
                                (it->second.IsLeaseExpired(cleanup_now) &&
                                 !it->second.IsSoftPinned(cleanup_now))) {
                                VLOG(1) << "clear metadata key=" << it->first;
                                it = EraseMetadata(tenant_state, it,
                                                   tenant_it->first);
                            } else {
                                ++it;
                            }
                        }
                        if (tenant_state.Empty()) {
                            tenant_it = shard.tenants.erase(tenant_it);
                        } else {
                            ++tenant_it;
                        }
                    }
                }
            }

            MasterMetricManager::instance().reset_allocated_mem_size();
            RebuildCacheTotalAccounting();
            for (auto& segment_name : segment_names) {
                MasterMetricManager::instance()
                    .reset_segment_allocated_mem_size(segment_name);
            }

            for (auto& shard : metadata_shards_) {
                for (auto& [tenant_id, tenant_state] : shard.tenants) {
                    for (auto it = tenant_state.metadata.begin();
                         it != tenant_state.metadata.end();) {
                        for (auto& replica : it->second.GetAllReplicas()) {
                            if (!replica.get_descriptor().is_memory_replica()) {
                                continue;
                            }
                            auto temp_segment_names =
                                replica.get_segment_names();
                            if (temp_segment_names.empty()) {
                                continue;
                            }
                            if (!temp_segment_names[0].has_value()) {
                                continue;
                            }
                            auto buffer_descriptor =
                                replica.get_descriptor()
                                    .get_memory_descriptor()
                                    .buffer_descriptor;
                            MasterMetricManager::instance()
                                .inc_allocated_mem_size(
                                    temp_segment_names[0].value(),
                                    static_cast<int64_t>(
                                        buffer_descriptor.size_));
                        }
                        ++it;
                    }
                }
            }

            LOG(INFO)
                << "[Restore] Total allocated size after restore: "
                << MasterMetricManager::instance().get_allocated_mem_size();
        }

        {
            MasterMetricManager::instance().reset_total_mem_capacity();
            for (auto& segment_name : segment_names) {
                MasterMetricManager::instance()
                    .reset_segment_total_mem_capacity(segment_name);
            }

            ScopedSegmentAccess segment_access =
                segment_manager_.getSegmentAccess();
            std::vector<std::pair<Segment, UUID>> unready_segments;
            if (segment_access.GetUnreadySegments(unready_segments) ==
                ErrorCode::OK) {
                for (const auto& [segment, client_id] : unready_segments) {
                    UnmountSegment(segment.id, client_id);
                }
            }

            std::vector<std::pair<Segment, UUID>> all_segments;
            auto err = segment_access.GetAllSegments(all_segments);

            if (err == ErrorCode::OK) {
                int64_t total_size = 0;
                for (const auto& [segment, client_id] : all_segments) {
                    Ping(client_id);
                    total_size += static_cast<int64_t>(segment.size);
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
        RebuildTenantQuotaUsageFromMetadata();
        return true;
    } catch (const std::exception& e) {
        return fail_restore("exception during state restoration: " +
                            std::string(e.what()));
    } catch (...) {
        return fail_restore("unknown exception during state restoration");
    }
}

void MasterService::ResetStateAfterFailedRestoreAttempt() {
    SegmentSerializer segment_serializer(&segment_manager_);
    MetadataSerializer metadata_serializer(this);
    TaskManagerSerializer task_manager_serializer(&task_manager_);

    task_manager_serializer.Reset();
    metadata_serializer.Reset();
    segment_serializer.Reset();

    {
        std::unique_lock<std::shared_mutex> lock(client_mutex_);
        ok_client_.clear();
    }
    PodUUID pod_uuid;
    while (client_ping_queue_.pop(pod_uuid)) {
    }

    MasterMetricManager::instance().reset_allocated_mem_size();
    MasterMetricManager::instance().reset_total_mem_capacity();
    MasterMetricManager::instance().reset_cache_total_nums();
}

ha::SnapshotCatalogStore* MasterService::GetSnapshotCatalogStore() {
    return snapshot_catalog_store_.get();
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

    auto is_evictable_memory_replica = [](const Replica& replica) {
        return replica.is_memory_replica() && replica.is_completed() &&
               replica.get_refcnt() == 0;
    };

    auto can_evict_replicas = [&](const ObjectMetadata& metadata) {
        return metadata.HasReplica(is_evictable_memory_replica);
    };

    auto evict_replicas =
        [&, this](ObjectMetadata& metadata,
                  std::vector<std::vector<Replica>>& deferred_replicas) {
            const uint64_t before_charge = CompletedMemoryQuotaCharge(metadata);
            auto replicas = PopReplicasWithCacheTotalAccounting(
                metadata, is_evictable_memory_replica);
            const size_t replica_count = replicas.size();
            if (!replicas.empty()) {
                deferred_replicas.emplace_back(std::move(replicas));
            }
            const uint64_t after_charge = CompletedMemoryQuotaCharge(metadata);
            if (before_charge > after_charge) {
                ReleaseCommittedQuotaCharge(metadata,
                                            before_charge - after_charge);
            }
            return metadata.size * replica_count;
        };

    // --- Offload-on-evict support ---
    long offload_queued_this_cycle = 0;
    long offload_deferred_count = 0;
    long offload_cap_forced_count = 0;    // #keys force-evicted due to cap
    long offload_push_failed_forced = 0;  // #keys force-evicted on push fail
    const long offload_cap =
        offload_on_evict_
            ? static_cast<long>(offloading_queue_limit_ * kOffloadCapRatio)
            : 0;

    auto has_local_disk_replica = [](const ObjectMetadata& metadata) {
        return metadata.HasReplica(&Replica::fn_is_local_disk_replica);
    };

    // Returns freed bytes. Returns 0 if offload-queued and no additional
    // replicas were evicted (all MEMORY replicas of the key are now pinned).
    auto try_evict_or_offload =
        [&, this](
            const std::string& tenant_id, const std::string& key,
            ObjectMetadata& metadata, TenantState& tenant_state,
            std::vector<std::vector<Replica>>& deferred_replicas) -> uint64_t {
        if (!offload_on_evict_) {
            // Original behavior
            return evict_replicas(metadata, deferred_replicas);
        }

        // LOCAL_DISK replica already exists — safe to delete MEMORY immediately
        if (has_local_disk_replica(metadata)) {
            return evict_replicas(metadata, deferred_replicas);
        }

        // Force-evict cap: if force_evict enabled and cap reached, force
        // delete. Warning is aggregated at the end of the cycle to avoid log
        // flooding.
        if (offload_force_evict_ && offload_queued_this_cycle >= offload_cap) {
            offload_cap_forced_count++;
            return evict_replicas(metadata, deferred_replicas);
        }

        // Queue one MEMORY replica for offload; others will be evicted below.
        bool queued = false;
        metadata.VisitReplicas(
            [](const Replica& r) {
                return r.is_memory_replica() && r.is_completed() &&
                       r.get_refcnt() == 0;
            },
            [this, &tenant_id, &key, &tenant_state, &queued,
             &now](Replica& replica) {
                if (queued) return;  // only need to pin one replica for offload
                auto result = PushOffloadingQueue(
                    MakeObjectIdentity(key, tenant_id), replica);
                if (result) {
                    replica.inc_refcnt();
                    tenant_state.offloading_tasks.emplace(
                        key, OffloadingTask{replica.id(), now});
                    queued = true;
                }
            });

        if (queued) {
            offload_queued_this_cycle++;
            offload_deferred_count++;
            // Any remaining MEMORY replicas with refcnt==0 are redundant copies
            // (data survives via the pinned replica → disk). Evict them now to
            // reclaim memory immediately rather than waiting another cycle.
            return evict_replicas(metadata, deferred_replicas);
        }

        // PushOffloadingQueue failed. Default (data-preserving) behavior is to
        // skip this cycle — the outer eviction loop will retry after the
        // offload queue drains. Only force-evict when explicitly opted in, to
        // prevent silent data loss when the queue is unavailable.
        if (offload_force_evict_) {
            offload_push_failed_forced++;
            return evict_replicas(metadata, deferred_replicas);
        }
        return 0;
    };

    struct EvictionResult {
        uint64_t freed_bytes{0};
        long evicted_objects{0};
    };

    auto try_evict_group_or_object =
        [&, this](const std::string& tenant_id, const std::string& key,
                  ObjectMetadata& metadata, MetadataShardAccessorRW& shard,
                  TenantState& tenant_state,
                  std::vector<std::vector<Replica>>& deferred_replicas,
                  bool allow_soft_pinned) -> EvictionResult {
        if (!metadata.IsGrouped()) {
            uint64_t freed = try_evict_or_offload(
                tenant_id, key, metadata, tenant_state, deferred_replicas);
            return {.freed_bytes = freed, .evicted_objects = freed > 0 ? 1 : 0};
        }

        auto group_it = tenant_state.group_members.find(metadata.group_id);
        if (group_it == tenant_state.group_members.end()) {
            uint64_t freed = try_evict_or_offload(
                tenant_id, key, metadata, tenant_state, deferred_replicas);
            return {.freed_bytes = freed, .evicted_objects = freed > 0 ? 1 : 0};
        }

        for (const auto& member_key : group_it->second) {
            auto member_it = tenant_state.metadata.find(member_key);
            if (member_it != tenant_state.metadata.end() &&
                !member_it->second.IsLeaseExpired(now)) {
                return {};
            }
        }

        EvictionResult result;
        std::vector<std::string> member_keys(group_it->second.begin(),
                                             group_it->second.end());
        for (const auto& member_key : member_keys) {
            auto member_it = tenant_state.metadata.find(member_key);
            if (member_it == tenant_state.metadata.end()) {
                continue;
            }
            auto& member_metadata = member_it->second;
            if (member_metadata.IsHardPinned() ||
                !member_metadata.IsLeaseExpired(now) ||
                (!allow_soft_pinned && member_metadata.IsSoftPinned(now)) ||
                !can_evict_replicas(member_metadata)) {
                continue;
            }

            uint64_t freed =
                try_evict_or_offload(tenant_id, member_key, member_metadata,
                                     tenant_state, deferred_replicas);
            result.freed_bytes += freed;
            if (freed > 0) {
                result.evicted_objects++;
            }
            if (member_key != key && !member_metadata.IsValid()) {
                EraseMetadata(tenant_state, member_it, tenant_id,
                              QuotaEraseMode::kFull, &shard);
            }
        }
        return result;
    };

    // Candidate carries key for safe lookup after releasing shard lock.
    // Iterators would be invalid if the shard is modified between phases.
    struct Candidate {
        size_t shard_idx;
        std::string tenant_id;
        std::string key;
        std::chrono::system_clock::time_point lease_timeout;
    };

    // Randomly select a starting shard to avoid imbalance eviction between
    // shards.
    size_t start_idx = RandomIndex(kNumShards);
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);

    // ===== Phase 1: Parallel candidate collection =====
    // N threads each scan a batch of shards, collecting Candidates with
    // shard_idx + tenant_id + key for safe re-lookup in Phase 2.
    int num_threads = std::min((int)kNumShards, 16);
    size_t shards_per_thread = (kNumShards + num_threads - 1) / num_threads;

    std::vector<std::vector<Candidate>> local_candidates(num_threads);
    std::vector<long> local_eviction_base(num_threads, 0);
    std::vector<long> local_object_count(num_threads, 0);
    std::vector<std::vector<std::chrono::system_clock::time_point>>
        local_soft_pin(num_threads);

    std::vector<std::thread> threads;
    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t] {
            size_t s_start = t * shards_per_thread;
            size_t s_end = std::min(s_start + shards_per_thread, kNumShards);
            for (size_t s = s_start; s < s_end; s++) {
                MetadataShardAccessorRW shard(this, s);
                DiscardExpiredProcessingReplicas(shard, now);

                size_t shard_metadata_count = 0;
                size_t shard_evictable_count = 0;
                for (const auto& [tenant_id, tenant_state] : shard->tenants) {
                    shard_metadata_count += tenant_state.metadata.size();
                    for (auto it = tenant_state.metadata.begin();
                         it != tenant_state.metadata.end(); ++it) {
                        if (it->second.IsHardPinned()) continue;
                        bool has_evictable = can_evict_replicas(it->second);
                        if (has_evictable) shard_evictable_count++;
                        if (!it->second.IsLeaseExpired(now) || !has_evictable)
                            continue;
                        if (!it->second.IsSoftPinned(now)) {
                            local_candidates[t].push_back(
                                {s, tenant_id, it->first,
                                 it->second.lease_timeout});
                        } else if (allow_evict_soft_pinned_objects_) {
                            local_soft_pin[t].push_back(
                                it->second.lease_timeout);
                        }
                    }
                }
                local_object_count[t] += shard_metadata_count;
                local_eviction_base[t] += shard_evictable_count;
            }
        });
    }
    for (auto& t : threads) t.join();

    // Merge per-thread results
    long total_eviction_base = 0;
    for (auto v : local_eviction_base) total_eviction_base += v;

    long object_count = 0;
    for (auto v : local_object_count) object_count += v;

    std::vector<Candidate> candidates;
    {
        size_t total = 0;
        for (auto& v : local_candidates) total += v.size();
        candidates.reserve(total);
    }
    for (auto& v : local_candidates) {
        candidates.insert(candidates.end(), std::make_move_iterator(v.begin()),
                          std::make_move_iterator(v.end()));
    }

    std::vector<std::chrono::system_clock::time_point> soft_pin_objects;
    {
        size_t total = 0;
        for (auto& v : local_soft_pin) total += v.size();
        soft_pin_objects.reserve(total);
    }
    for (auto& v : local_soft_pin) {
        soft_pin_objects.insert(soft_pin_objects.end(),
                                std::make_move_iterator(v.begin()),
                                std::make_move_iterator(v.end()));
    }

    if (total_eviction_base == 0) {
        need_mem_eviction_ = false;
        VLOG(1) << "[EVICT-DIAG] object_count=" << object_count
                << " eviction_base=0 (no evictable memory objects)";
        return;
    }

    // ===== Phase 2: Serial eviction via key lookup =====
    long evicted_count = 0;
    uint64_t total_freed_size = 0;
    std::vector<std::chrono::system_clock::time_point> no_pin_objects;
    std::vector<std::vector<Replica>> deferred_replicas;

    // First pass: evict candidates with no soft pin
    if (!candidates.empty()) {
        long ideal_evict_num =
            std::ceil(total_eviction_base * evict_ratio_target);
        long evict_num = std::min(ideal_evict_num, (long)candidates.size());

        std::nth_element(candidates.begin(),
                         candidates.begin() + (evict_num - 1), candidates.end(),
                         [](const Candidate& a, const Candidate& b) {
                             return a.lease_timeout < b.lease_timeout;
                         });
        auto target_timeout = candidates[evict_num - 1].lease_timeout;

        // Treat evict_num as a minimum: if re-validation skips a candidate,
        // continue trying the next one so actual evicted count reaches
        // evict_num. This matches the old per-shard over-eviction behavior.
        long evicted_this_pass = 0;
        for (auto& c : candidates) {
            if (evicted_this_pass >= evict_num &&
                c.lease_timeout > target_timeout) {
                no_pin_objects.push_back(c.lease_timeout);
                continue;
            }
            {
                MetadataShardAccessorRW shard(this, c.shard_idx);
                auto tenant_it = shard->tenants.find(c.tenant_id);
                if (tenant_it == shard->tenants.end()) continue;
                auto& tenant_state = tenant_it->second;
                auto it = tenant_state.metadata.find(c.key);
                if (it == tenant_state.metadata.end()) continue;
                // Re-validate: state may have changed since Phase 1
                if (!it->second.IsLeaseExpired(now) ||
                    it->second.IsSoftPinned(now) ||
                    !can_evict_replicas(it->second)) {
                    no_pin_objects.push_back(c.lease_timeout);
                    continue;
                }
                auto evict_result = try_evict_group_or_object(
                    c.tenant_id, c.key, it->second, shard, tenant_state,
                    deferred_replicas,
                    /*allow_soft_pinned=*/false);
                total_freed_size += evict_result.freed_bytes;
                if (!it->second.IsValid()) {
                    EraseMetadata(tenant_state, it, c.tenant_id,
                                  QuotaEraseMode::kFull, &shard);
                }
                if (tenant_state.Empty()) {
                    shard->tenants.erase(tenant_it);
                }
                evicted_count += evict_result.evicted_objects;
                evicted_this_pass += evict_result.evicted_objects;
            }
            deferred_replicas.clear();
        }
    }

    // Try releasing discarded replicas before we decide whether to do the
    // second pass.
    uint64_t released_discarded_cnt = ReleaseExpiredDiscardedReplicas(now);

    // The ideal number of objects to evict in the second pass
    long target_evict_num =
        std::ceil(total_eviction_base * evict_ratio_lowerbound) -
        evicted_count - released_discarded_cnt;
    // The actual number of objects we can evict in the second pass
    target_evict_num =
        std::min(target_evict_num,
                 (long)no_pin_objects.size() + (long)soft_pin_objects.size());

    // Do second pass eviction only if 1). there are candidates that can be
    // evicted AND 2). The evicted number in the first pass is less than
    // evict_ratio_lowerbound.
    if (target_evict_num > 0) {
        if (target_evict_num <= static_cast<long>(no_pin_objects.size())) {
            // Second pass A: only evict objects without soft pin.
            std::nth_element(no_pin_objects.begin(),
                             no_pin_objects.begin() + (target_evict_num - 1),
                             no_pin_objects.end());
            auto target_timeout = no_pin_objects[target_evict_num - 1];

            // Evict via key lookup — avoid full metadata traversal
            for (size_t i = 0; i < kNumShards && target_evict_num > 0; i++) {
                {
                    MetadataShardAccessorRW shard(this,
                                                  (start_idx + i) % kNumShards);
                    for (auto tenant_it = shard->tenants.begin();
                         tenant_it != shard->tenants.end() &&
                         target_evict_num > 0;) {
                        auto& tenant_state = tenant_it->second;
                        auto it = tenant_state.metadata.begin();
                        while (it != tenant_state.metadata.end() &&
                               target_evict_num > 0) {
                            if (!it->second.IsHardPinned() &&
                                it->second.IsLeaseExpired(now) &&
                                it->second.lease_timeout <= target_timeout &&
                                !it->second.IsSoftPinned(now) &&
                                can_evict_replicas(it->second)) {
                                auto evict_result = try_evict_group_or_object(
                                    tenant_it->first, it->first, it->second,
                                    shard, tenant_state, deferred_replicas,
                                    /*allow_soft_pinned=*/false);
                                total_freed_size += evict_result.freed_bytes;
                                if (!it->second.IsValid()) {
                                    it = EraseMetadata(
                                        tenant_state, it, tenant_it->first,
                                        QuotaEraseMode::kFull, &shard);
                                } else {
                                    ++it;
                                }
                                evicted_count += evict_result.evicted_objects;
                                target_evict_num -=
                                    evict_result.evicted_objects;
                            } else {
                                ++it;
                            }
                        }
                        if (tenant_state.Empty()) {
                            tenant_it = shard->tenants.erase(tenant_it);
                        } else {
                            ++tenant_it;
                        }
                    }
                }
                deferred_replicas.clear();
            }
        } else if (!soft_pin_objects.empty()) {
            // Second pass B: Prioritize evicting objects without soft pin,
            // but also allow evicting soft pinned objects.
            const long soft_pin_evict_num =
                target_evict_num - static_cast<long>(no_pin_objects.size());
            std::nth_element(
                soft_pin_objects.begin(),
                soft_pin_objects.begin() + (soft_pin_evict_num - 1),
                soft_pin_objects.end());
            auto soft_target_timeout = soft_pin_objects[soft_pin_evict_num - 1];

            for (size_t i = 0; i < kNumShards && target_evict_num > 0; i++) {
                {
                    MetadataShardAccessorRW shard(this,
                                                  (start_idx + i) % kNumShards);

                    for (auto tenant_it = shard->tenants.begin();
                         tenant_it != shard->tenants.end() &&
                         target_evict_num > 0;) {
                        auto& tenant_state = tenant_it->second;
                        auto it = tenant_state.metadata.begin();
                        while (it != tenant_state.metadata.end() &&
                               target_evict_num > 0) {
                            if (it->second.IsHardPinned() ||
                                !it->second.IsLeaseExpired(now) ||
                                !can_evict_replicas(it->second)) {
                                ++it;
                                continue;
                            }
                            if (!it->second.IsSoftPinned(now) ||
                                it->second.lease_timeout <=
                                    soft_target_timeout) {
                                auto evict_result = try_evict_group_or_object(
                                    tenant_it->first, it->first, it->second,
                                    shard, tenant_state, deferred_replicas,
                                    /*allow_soft_pinned=*/true);
                                total_freed_size += evict_result.freed_bytes;
                                if (!it->second.IsValid()) {
                                    it = EraseMetadata(
                                        tenant_state, it, tenant_it->first,
                                        QuotaEraseMode::kFull, &shard);
                                } else {
                                    ++it;
                                }
                                evicted_count += evict_result.evicted_objects;
                                target_evict_num -=
                                    evict_result.evicted_objects;
                            } else {
                                ++it;
                            }
                        }
                        if (tenant_state.Empty()) {
                            tenant_it = shard->tenants.erase(tenant_it);
                        } else {
                            ++tenant_it;
                        }
                    }
                }
                deferred_replicas.clear();
            }
        } else {
            LOG(ERROR) << "Error in second pass eviction: target_evict_num="
                       << target_evict_num
                       << ", no_pin_objects.size()=" << no_pin_objects.size()
                       << ", soft_pin_objects.size()="
                       << soft_pin_objects.size()
                       << ", evicted_count=" << evicted_count
                       << ", eviction_base=" << total_eviction_base
                       << ", evict_ratio_target=" << evict_ratio_target
                       << ", evict_ratio_lowerbound=" << evict_ratio_lowerbound;
        }
    }

    if (evicted_count > 0 || released_discarded_cnt > 0) {
        need_mem_eviction_ = false;
        MasterMetricManager::instance().inc_eviction_success(evicted_count,
                                                             total_freed_size);
        MasterMetricManager::instance().inc_mem_eviction_success(
            evicted_count, total_freed_size);
    } else if (offload_deferred_count > 0) {
        need_mem_eviction_ = false;
        MasterMetricManager::instance().inc_eviction_success(0, 0);
        MasterMetricManager::instance().inc_mem_eviction_success(0, 0);
    } else {
        if (total_eviction_base == 0) {
            need_mem_eviction_ = false;
        }
        MasterMetricManager::instance().inc_eviction_fail();
        MasterMetricManager::instance().inc_mem_eviction_fail();
    }
    VLOG(1) << "action=evict_objects"
            << ", evicted_count=" << evicted_count
            << ", offload_deferred=" << offload_deferred_count
            << ", offload_cap_forced=" << offload_cap_forced_count
            << ", offload_push_failed_forced=" << offload_push_failed_forced
            << ", total_freed_size=" << total_freed_size
            << ", eviction_base=" << total_eviction_base
            << ", actual_evict_ratio="
            << (total_eviction_base > 0
                    ? (double)evicted_count / total_eviction_base
                    : 0.0)
            << ", target_evict_ratio=" << evict_ratio_target;
    VLOG(1) << "[EVICT-DIAG] object_count=" << object_count
            << " disk_object_count=" << (object_count - total_eviction_base)
            << " eviction_base=" << total_eviction_base << " disk_ratio="
            << (object_count > 0
                    ? (double)(object_count - total_eviction_base) /
                          object_count
                    : 0.0)
            << " ideal_evict_num_inflated="
            << (long)std::ceil(object_count * evict_ratio_target)
            << " ideal_evict_num_correct="
            << (long)std::ceil(total_eviction_base * evict_ratio_target);
    LOG(INFO) << "[EVICT-RESULT] evicted_count=" << evicted_count
              << ", eviction_base=" << total_eviction_base
              << ", actual_evict_ratio="
              << (total_eviction_base > 0
                      ? (double)evicted_count / total_eviction_base
                      : 0.0)
              << ", target_evict_ratio=" << evict_ratio_target;
    if (offload_on_evict_ && evicted_count == 0 && offload_deferred_count > 0) {
        LOG(WARNING) << "[EVICT] No memory freed this cycle; "
                     << offload_deferred_count
                     << " objects deferred for disk offload. "
                        "Consider lowering eviction_high_watermark_ratio.";
    }
    if (offload_cap_forced_count > 0) {
        LOG(WARNING) << "[EVICT] Offload cap (" << offload_cap
                     << ") reached; force-evicted " << offload_cap_forced_count
                     << " object(s) without disk offload this cycle.";
    }
    if (offload_push_failed_forced > 0) {
        LOG(WARNING) << "[EVICT] PushOffloadingQueue failed for "
                     << offload_push_failed_forced
                     << " object(s); force-evicted without disk offload "
                        "(offload_force_evict=true).";
    }
}

void MasterService::NoFBatchEvict(double evict_ratio_target,
                                  double evict_ratio_lowerbound) {
    if (evict_ratio_target < evict_ratio_lowerbound) {
        LOG(ERROR) << "nof_evict_ratio_target=" << evict_ratio_target
                   << ", nof_evict_ratio_lowerbound=" << evict_ratio_lowerbound
                   << ", error=invalid_params";
        evict_ratio_lowerbound = evict_ratio_target;
    }

    auto now = std::chrono::system_clock::now();
    long evicted_count = 0;
    long object_count = 0;
    uint64_t total_freed_size = 0;

    size_t start_idx = RandomIndex(metadata_shards_.size());
    for (size_t i = 0; i < metadata_shards_.size(); i++) {
        MetadataShardAccessorRW shard(
            this, (start_idx + i) % metadata_shards_.size());
        DiscardExpiredProcessingReplicas(shard, now);
        for (const auto& [tenant_id, tenant_state] : shard->tenants) {
            object_count += tenant_state.metadata.size();
        }

        const long ideal_evict_num =
            std::ceil(object_count * evict_ratio_target) - evicted_count;
        if (ideal_evict_num <= 0) {
            continue;
        }

        long shard_evicted_count = 0;
        for (auto tenant_it = shard->tenants.begin();
             tenant_it != shard->tenants.end() &&
             shard_evicted_count < ideal_evict_num;) {
            auto& tenant_state = tenant_it->second;
            for (auto it = tenant_state.metadata.begin();
                 it != tenant_state.metadata.end() &&
                 shard_evicted_count < ideal_evict_num;) {
                auto& metadata = it->second;
                if (metadata.IsHardPinned() || !metadata.IsLeaseExpired(now) ||
                    metadata.IsSoftPinned(now)) {
                    ++it;
                    continue;
                }

                const size_t erased =
                    metadata.EraseReplicas([](const Replica& replica) {
                        return replica.is_nof_replica() &&
                               replica.is_completed() &&
                               replica.get_refcnt() == 0;
                    });
                if (erased == 0) {
                    ++it;
                    continue;
                }

                total_freed_size += metadata.size * erased;
                shard_evicted_count++;
                if (!metadata.IsValid()) {
                    it = EraseMetadata(tenant_state, it, tenant_it->first,
                                       QuotaEraseMode::kFull, &shard);
                } else {
                    ++it;
                }
            }
            if (tenant_state.Empty()) {
                tenant_it = shard->tenants.erase(tenant_it);
            } else {
                ++tenant_it;
            }
        }
        evicted_count += shard_evicted_count;
    }

    if (evicted_count > 0) {
        need_nof_eviction_ = false;
        MasterMetricManager::instance().inc_eviction_success(evicted_count,
                                                             total_freed_size);
        MasterMetricManager::instance().inc_nof_eviction_success(
            evicted_count, total_freed_size);
    } else {
        if (object_count == 0) {
            need_nof_eviction_ = false;
        }
        MasterMetricManager::instance().inc_eviction_fail();
        MasterMetricManager::instance().inc_nof_eviction_fail();
    }

    VLOG(1) << "action=evict_nof_replicas"
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
            // Notify graceful unmount scheduler to drop pending records
            // for expired clients. The actual unmount is handled below.
            for (auto& cid : expired_clients) {
                graceful_unmount_scheduler_.RemoveIf(
                    [&cid](const GracefulUnmountDeadlineRecord& record) {
                        return record.client_id == cid;
                    });
            }

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
                    // mounted mem segemtns of this expired client
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
                                          "mem_segment_failed";
                        }
                    }
                }
            }  // Release the mutex before long-running ClearInvalidHandles and
               // avoid deadlocks

            // Always clean up invalid handles when there are expired clients,
            // even if no memory segments were unmounted. This is necessary
            // to clean up local_disk replicas whose owner client has expired.
            ClearInvalidHandles();

            // Commit unmount of memory segments and clean up local_disk
            // segments for expired clients. Both require the exclusive
            // segment lock.
            {
                ScopedSegmentAccess segment_access =
                    segment_manager_.getSegmentAccess();
                for (size_t i = 0; i < unmount_segments.size(); i++) {
                    segment_access.CommitUnmountSegment(
                        unmount_segments[i], client_ids[i], dec_capacities[i]);
                    LOG(INFO) << "client_id=" << client_ids[i]
                              << ", segment_name=" << segment_names[i]
                              << ", action=unmount_expired_mem_segment";
                }
                for (auto& client_id : expired_clients) {
                    segment_access.UnmountLocalDiskSegment(client_id);
                }
            }
            RecomputeTenantEffectiveQuotas();
        }

        std::this_thread::sleep_for(
            std::chrono::milliseconds(kClientMonitorSleepMs));
    }
}

bool MasterService::ProbeNoFSegment(const std::string& te_endpoint,
                                    std::string* error_reason) {
#ifndef USE_NOF
    if (error_reason) {
        *error_reason = "nof_pool_disabled";
    }
    return false;
#else
    NoFProbeFn probe_fn;
    {
        std::lock_guard<std::mutex> lock(nof_probe_fn_mutex_);
        probe_fn = nof_probe_fn_;
    }
    if (!probe_fn) {
        if (error_reason) {
            *error_reason = "probe_not_configured";
        }
        return false;
    }
    return probe_fn(
        te_endpoint,
        static_cast<uint32_t>(nof_heartbeat_probe_timeout_ms_.count()),
        error_reason);
#endif
}

bool MasterService::TryUnmountNoFSegmentByHeartbeat(
    const MountedNoFSegmentSnapshot& snapshot,
    const std::string& error_reason) {
    size_t metrics_dec_capacity = 0;
    {
        auto nof_segment_access = nof_segment_manager_.getNoFSegmentAccess();
        ErrorCode err = nof_segment_access.PrepareUnmountSegment(
            snapshot.segment_id, metrics_dec_capacity);
        if (err == ErrorCode::SEGMENT_NOT_FOUND ||
            err == ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS) {
            std::lock_guard<std::mutex> lock(nof_heartbeat_mutex_);
            nof_heartbeat_states_.erase(snapshot.segment_id);
            VLOG(1) << "segment_id=" << snapshot.segment_id
                    << ", action=skip_nof_heartbeat_unmount"
                    << ", reason=" << toString(err);
            return false;
        }
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "segment_id=" << snapshot.segment_id
                       << ", segment_name=" << snapshot.segment.name
                       << ", error=prepare_unmount_nof_segment_by_"
                          "heartbeat_failed"
                       << ", reason=" << err;
            return false;
        }
    }

    ClearInvalidHandles();

    {
        auto nof_segment_access = nof_segment_manager_.getNoFSegmentAccess();
        ErrorCode err = nof_segment_access.CommitUnmountSegment(
            snapshot.segment_id, snapshot.client_id, metrics_dec_capacity);
        if (err != ErrorCode::OK && err != ErrorCode::SEGMENT_NOT_FOUND) {
            LOG(ERROR) << "segment_id=" << snapshot.segment_id
                       << ", segment_name=" << snapshot.segment.name
                       << ", error=commit_unmount_nof_segment_by_"
                          "heartbeat_failed"
                       << ", reason=" << err;
            return false;
        }
    }

    {
        std::lock_guard<std::mutex> lock(nof_heartbeat_mutex_);
        nof_heartbeat_states_.erase(snapshot.segment_id);
    }
    MasterMetricManager::instance()
        .inc_nof_segments_unmounted_by_heartbeat_total();
    LOG(INFO) << "segment_id=" << snapshot.segment_id
              << ", client_id=" << snapshot.client_id
              << ", segment_name=" << snapshot.segment.name
              << ", endpoint=" << snapshot.segment.te_endpoint
              << ", action=unmount_nof_segment_by_heartbeat"
              << ", last_error_reason=" << error_reason;
    return true;
}

void MasterService::NofHeartbeatThreadFunc() {
    size_t next_probe_index = 0;
    while (nof_heartbeat_running_) {
        auto now = std::chrono::steady_clock::now();
        std::vector<MountedNoFSegmentSnapshot> mounted_segments;
        nof_segment_manager_.GetMountedSegmentsSnapshot(mounted_segments);

        std::vector<MountedNoFSegmentSnapshot> ok_segments;
        ok_segments.reserve(mounted_segments.size());
        for (const auto& snapshot : mounted_segments) {
            if (snapshot.status == SegmentStatus::OK) {
                ok_segments.push_back(snapshot);
            }
        }

        std::optional<MountedNoFSegmentSnapshot> probe_target;
        {
            std::lock_guard<std::mutex> lock(nof_heartbeat_mutex_);
            std::unordered_set<UUID, boost::hash<UUID>> live_segment_ids;
            live_segment_ids.reserve(ok_segments.size());

            const auto interval_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    nof_heartbeat_interval_sec_);
            for (size_t i = 0; i < ok_segments.size(); ++i) {
                const auto& snapshot = ok_segments[i];
                live_segment_ids.insert(snapshot.segment_id);
                auto [it, inserted] =
                    nof_heartbeat_states_.try_emplace(snapshot.segment_id);
                auto& state = it->second;
                state.owner_client_id = snapshot.client_id;
                state.segment_name = snapshot.segment.name;
                state.te_endpoint = snapshot.segment.te_endpoint;
                if (inserted) {
                    int64_t spread_ms = 0;
                    if (!ok_segments.empty()) {
                        spread_ms = static_cast<int64_t>(
                            (interval_ms.count() * i) / ok_segments.size());
                    }
                    state.last_success_at = now;
                    state.next_probe_at = now + nof_heartbeat_interval_sec_ +
                                          std::chrono::milliseconds(spread_ms);
                }
            }

            for (auto it = nof_heartbeat_states_.begin();
                 it != nof_heartbeat_states_.end();) {
                if (!live_segment_ids.contains(it->first)) {
                    it = nof_heartbeat_states_.erase(it);
                } else {
                    ++it;
                }
            }

            if (!ok_segments.empty()) {
                next_probe_index %= ok_segments.size();
                for (size_t offset = 0; offset < ok_segments.size(); ++offset) {
                    const auto& candidate =
                        ok_segments[(next_probe_index + offset) %
                                    ok_segments.size()];
                    auto state_it =
                        nof_heartbeat_states_.find(candidate.segment_id);
                    if (state_it == nof_heartbeat_states_.end()) {
                        continue;
                    }
                    if (state_it->second.next_probe_at <= now) {
                        probe_target = candidate;
                        next_probe_index = (next_probe_index + offset + 1) %
                                           ok_segments.size();
                        break;
                    }
                }
            }
        }

        if (!probe_target.has_value()) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(kNoFHeartbeatThreadSleepMs));
            continue;
        }

        auto probe_start = std::chrono::steady_clock::now();
        std::string error_reason;
        bool probe_success =
            ProbeNoFSegment(probe_target->segment.te_endpoint, &error_reason);
        auto latency_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now() - probe_start)
                              .count();
        MasterMetricManager::instance().observe_nof_heartbeat_probe_latency_ms(
            latency_ms);

        if (probe_success) {
            MasterMetricManager::instance().inc_nof_heartbeat_success_total();
            auto success_time = std::chrono::steady_clock::now();
            {
                std::lock_guard<std::mutex> lock(nof_heartbeat_mutex_);
                auto it = nof_heartbeat_states_.find(probe_target->segment_id);
                if (it != nof_heartbeat_states_.end()) {
                    it->second.consecutive_failures = 0;
                    it->second.last_success_at = success_time;
                    it->second.last_error_reason.clear();
                    it->second.next_probe_at =
                        success_time + nof_heartbeat_interval_sec_;
                }
            }
            VLOG(1) << "segment_id=" << probe_target->segment_id
                    << ", segment_name=" << probe_target->segment.name
                    << ", endpoint=" << probe_target->segment.te_endpoint
                    << ", action=nof_heartbeat_success"
                    << ", latency_ms=" << latency_ms;
            continue;
        }

        MasterMetricManager::instance().inc_nof_heartbeat_failure_total();
        if (error_reason == "completion_timeout") {
            MasterMetricManager::instance().inc_nof_heartbeat_timeout_total();
        }

        bool should_unmount = false;
        uint32_t failure_count = 0;
        auto failure_time = std::chrono::steady_clock::now();
        auto alive_timeout =
            nof_heartbeat_interval_sec_ *
            static_cast<int64_t>(nof_heartbeat_failures_threshold_);
        {
            std::lock_guard<std::mutex> lock(nof_heartbeat_mutex_);
            auto it = nof_heartbeat_states_.find(probe_target->segment_id);
            if (it != nof_heartbeat_states_.end()) {
                it->second.consecutive_failures++;
                failure_count = it->second.consecutive_failures;
                it->second.last_error_reason = error_reason;
                it->second.next_probe_at =
                    failure_time + nof_heartbeat_interval_sec_;
                should_unmount =
                    failure_time - it->second.last_success_at >= alive_timeout;
            }
        }

        LOG(WARNING) << "segment_id=" << probe_target->segment_id
                     << ", segment_name=" << probe_target->segment.name
                     << ", endpoint=" << probe_target->segment.te_endpoint
                     << ", action=nof_heartbeat_failure"
                     << ", failure_count=" << failure_count
                     << ", latency_ms=" << latency_ms
                     << ", reason=" << error_reason;

        if (should_unmount) {
            TryUnmountNoFSegmentByHeartbeat(*probe_target, error_reason);
        }
    }
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

    // First count shards that have actual metadata entries.
    // A shard may have empty tenants left after eviction erased all
    // metadata but didn't clean up the tenant map; using metadata_count
    // (not tenants.empty()) ensures the count matches the skip logic below.
    size_t valid_shards = 0;
    for (size_t i = 0; i < kNumShards; ++i) {
        size_t metadata_count = 0;
        for (const auto& [tid, ts] : service_->metadata_shards_[i].tenants) {
            metadata_count += ts.metadata.size();
        }
        if (metadata_count > 0) {
            valid_shards++;
        }
    }

    // Create shards map
    packer.pack_map(valid_shards);

    // Iterate through all shards, serialize each shard independently
    for (size_t shard_idx = 0; shard_idx < kNumShards; ++shard_idx) {
        const auto& shard = service_->metadata_shards_[shard_idx];

        // Skip shards with no actual metadata entries.
        // A shard may have empty tenants left after eviction erased all
        // metadata but didn't clean up the tenant map; serializing those
        // would produce an entry that deserialization never recreates,
        // breaking the snapshot round-trip comparison.
        size_t metadata_count = 0;
        for (const auto& [tid, ts] : shard.tenants) {
            metadata_count += ts.metadata.size();
        }
        if (metadata_count == 0) {
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
    service_->RebuildGroupRoutingIndex();
    return {};
}

void MasterService::MetadataSerializer::Reset() {
    for (auto& shard : service_->metadata_shards_) {
        shard.tenants.clear();
    }
    {
        std::unique_lock<std::shared_mutex> lock(
            service_->group_routing_mutex_);
        service_->object_group_ids_.clear();
        service_->groups_needing_lease_refresh_.clear();
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
    size_t metadata_count = 0;
    for (const auto& [tenant_id, tenant_state] : shard.tenants) {
        metadata_count += tenant_state.metadata.size();
    }
    packer.pack_array(metadata_count);

    // Sort tenant/key pairs to ensure consistent serialization order.
    // NOTE: sort may be slow for large shards.
    struct SortedEntry {
        std::string tenant_id;
        std::string key;
        const ObjectMetadata* metadata;
    };
    std::vector<SortedEntry> sorted_entries;
    sorted_entries.reserve(metadata_count);
    for (const auto& [tenant_id, tenant_state] : shard.tenants) {
        for (const auto& [key, metadata] : tenant_state.metadata) {
            sorted_entries.push_back({tenant_id, key, &metadata});
        }
    }
    std::sort(sorted_entries.begin(), sorted_entries.end(),
              [](const SortedEntry& lhs, const SortedEntry& rhs) {
                  if (lhs.tenant_id != rhs.tenant_id) {
                      return lhs.tenant_id < rhs.tenant_id;
                  }
                  return lhs.key < rhs.key;
              });

    for (const auto& entry : sorted_entries) {
        // Each metadata item format: [tenant_id, key, metadata_object].
        packer.pack_array(3);
        packer.pack(entry.tenant_id);
        packer.pack(entry.key);

        auto result = SerializeMetadata(*entry.metadata, packer);
        if (!result) {
            return tl::make_unexpected(SerializationError(
                result.error().code,
                fmt::format("Failed to serialize metadata for key '{}': {}",
                            entry.key, result.error().message)));
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
    shard.tenants.clear();

    // Deserialize metadata
    if (metadata_array == nullptr ||
        metadata_array->type != msgpack::type::ARRAY) {
        return tl::make_unexpected(
            SerializationError(ErrorCode::DESERIALIZE_FAIL,
                               "Missing or invalid 'metadata' field in shard"));
    }

    shard.tenants.reserve(metadata_array->via.array.size);

    for (uint32_t j = 0; j < metadata_array->via.array.size; ++j) {
        const msgpack::object& item = metadata_array->via.array.ptr[j];

        if (item.type != msgpack::type::ARRAY ||
            (item.via.array.size != 2 && item.via.array.size != 3)) {
            return tl::make_unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                "Invalid metadata item format: expected [key, metadata] or "
                "[tenant_id, key, metadata]"));
        }

        std::string tenant_id = "default";
        std::string key;
        const msgpack::object* value_obj = nullptr;
        if (item.via.array.size == 2) {
            key = item.via.array.ptr[0].as<std::string>();
            value_obj = &item.via.array.ptr[1];
        } else {
            tenant_id =
                NormalizeTenantId(item.via.array.ptr[0].as<std::string>());
            key = item.via.array.ptr[1].as<std::string>();
            value_obj = &item.via.array.ptr[2];
        }

        auto metadata_result = DeserializeMetadata(*value_obj);
        if (!metadata_result) {
            LOG(ERROR) << "Failed to deserialize metadata for key: " << key
                       << ": " << metadata_result.error().message;
            continue;
        }

        auto metadata_ptr = std::move(metadata_result.value());
        auto& tenant_state = shard.tenants[tenant_id];
        const std::string user_key = key;
        auto [it, inserted] = tenant_state.metadata.emplace(
            std::piecewise_construct, std::forward_as_tuple(std::move(key)),
            std::forward_as_tuple(
                metadata_ptr->client_id, metadata_ptr->put_start_time,
                metadata_ptr->size, metadata_ptr->PopReplicas(),
                metadata_ptr->soft_pin_timeout.has_value(),
                metadata_ptr->IsHardPinned(), metadata_ptr->data_type,
                metadata_ptr->group_id, tenant_id, user_key));

        it->second.lease_timeout = metadata_ptr->lease_timeout;
        it->second.soft_pin_timeout = metadata_ptr->soft_pin_timeout;

        // Recompute disk_object_count for restored metadata
        if (it->second.HasReplica([](const Replica& r) {
                return r.is_local_disk_replica() && r.is_completed();
            })) {
            shard.disk_object_count++;
        }
    }

    return {};
}

tl::expected<void, SerializationError>
MasterService::MetadataSerializer::SerializeMetadata(
    const MasterService::ObjectMetadata& metadata,
    MsgpackPacker& packer) const {
    // Pack ObjectMetadata using array structure for efficiency
    // Format: [client_id, put_start_time, size, lease_timeout,
    // has_soft_pin_timeout, soft_pin_timeout, replicas_count, data_type,
    // replicas..., hard_pinned, group_id]

    size_t array_size = 10;  // client_id, put_start_time, size, lease_timeout,
                             // has_soft_pin_timeout, soft_pin_timeout,
                             // replicas_count, data_type, hard_pinned, group_id
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

    // Serialize data_type
    packer.pack(static_cast<uint8_t>(metadata.data_type));

    // Serialize replicas
    for (const auto& replica : metadata.GetAllReplicas()) {
        auto result = Serializer<Replica>::serialize(
            replica, service_->segment_manager_.getView(), packer);
        if (!result) {
            return tl::unexpected(result.error());
        }
    }

    packer.pack(metadata.IsHardPinned());
    packer.pack(metadata.group_id);

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

    // Format detection (decode optional fields by type for back-compat):
    //   v1: 7 + replicas_count, no optional fields
    //   v2: 8 + replicas_count, either data_type or hard_pinned
    //   v3: 9 + replicas_count, data_type + hard_pinned or hard_pinned +
    //   group_id v4: 10 + replicas_count, data_type + hard_pinned + group_id
    // 64-bit arithmetic keeps an attacker-controlled near-UINT32_MAX
    // replicas_count from wrapping the bounds and slipping an out-of-bounds
    // index past the size check.
    constexpr uint64_t kBaseFieldCount = 7;
    constexpr uint64_t kMaxOptionalFieldCount = 3;
    const uint64_t total_elements = obj.via.array.size;
    const uint64_t min_elements = kBaseFieldCount + replicas_count;
    if (total_elements < min_elements ||
        total_elements > min_elements + kMaxOptionalFieldCount) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            "deserialize ObjectMetadata array size mismatch"));
    }

    ObjectDataType data_type = ObjectDataType::UNKNOWN;
    if (index < total_elements &&
        array[index].type == msgpack::type::POSITIVE_INTEGER) {
        data_type = static_cast<ObjectDataType>(array[index++].as<uint8_t>());
    }

    // Deserialize replicas
    std::vector<Replica> replicas;
    replicas.reserve(replicas_count);

    for (uint32_t i = 0; i < replicas_count; i++) {
        // Defensive bound: the data_type skip above can consume a slot the
        // size check counted on, so a crafted entry whose first post-count
        // field looks like a data_type could otherwise read past the array.
        // Mirrors the standby reader in catalog_backed_snapshot_provider.cpp.
        if (index >= total_elements) {
            return tl::unexpected(
                SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                   "deserialize ObjectMetadata truncated"));
        }
        auto result = Serializer<Replica>::deserialize(
            array[index++], service_->segment_manager_.getView());
        if (!result) {
            return tl::unexpected(result.error());
        }
        replicas.emplace_back(std::move(*result.value()));
    }

    // Deserialize hard_pinned (if present, otherwise default to false)
    bool is_hard_pinned = false;
    if (index < obj.via.array.size &&
        array[index].type == msgpack::type::BOOLEAN) {
        is_hard_pinned = array[index++].as<bool>();
    }

    std::string group_id;
    if (index < obj.via.array.size && array[index].type == msgpack::type::STR) {
        group_id = array[index++].as<std::string>();
    }

    // Create ObjectMetadata instance
    bool enable_soft_pin = has_soft_pin_timeout;
    auto metadata = std::make_unique<ObjectMetadata>(
        client_id,
        std::chrono::system_clock::time_point(
            std::chrono::milliseconds(put_start_time_timestamp)),
        size, std::move(replicas), enable_soft_pin, is_hard_pinned, data_type,
        group_id);
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
    auto time_t = std::chrono::system_clock::to_time_t(tp);

    std::stringstream ss;
    ss << std::put_time(std::localtime(&time_t), "%Y%m%d_%H%M%S");

    // Add milliseconds to ensure uniqueness
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  tp.time_since_epoch()) %
              1000;

    ss << "_" << std::setfill('0') << std::setw(3) << ms.count();

    return ss.str();
}

tl::expected<UUID, ErrorCode> MasterService::CreateCopyTask(
    const std::string& key, const std::string& tenant_id,
    const std::vector<std::string>& targets) {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    const auto object_id = MakeObjectIdentity(key, tenant_id);
    if (targets.empty()) {
        LOG(ERROR) << "key=" << key << ", error=empty_targets";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    MetadataAccessorRO accessor(this, object_id);
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
        if (!segment_accessor.IsSegmentAllocatable(target)) {
            LOG(ERROR) << "key=" << key << ", target_segment=" << target
                       << ", error=target_segment_not_allocatable";
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
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
            select_client, {.tenant_id = object_id.tenant_id,
                            .key = object_id.user_key,
                            .source = selected_source_segment,
                            .targets = targets});
}

tl::expected<UUID, ErrorCode> MasterService::CreateMoveTask(
    const std::string& key, const std::string& tenant_id,
    const std::string& source, const std::string& target) {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    const auto object_id = MakeObjectIdentity(key, tenant_id);
    MetadataAccessorRO accessor(this, object_id);
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
    if (!segment_accessor.IsSegmentAllocatable(target)) {
        LOG(ERROR) << "key=" << key << ", target_segment=" << target
                   << ", error=target_segment_not_allocatable";
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
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
            select_client, {.tenant_id = object_id.tenant_id,
                            .key = object_id.user_key,
                            .source = source,
                            .target = target});
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

tl::expected<void, ErrorCode> MasterService::ValidateDrainRequest(
    const CreateDrainJobRequest& request) {
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();
    return ValidateDrainRequestLocked(segment_access, request);
}

tl::expected<void, ErrorCode> MasterService::ValidateDrainRequestLocked(
    ScopedSegmentAccess& segment_access, const CreateDrainJobRequest& request) {
    if (request.segments.empty() || request.max_concurrency == 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::unordered_set<std::string> unique_segments(request.segments.begin(),
                                                    request.segments.end());
    if (unique_segments.size() != request.segments.size()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    for (const auto& segment_name : request.segments) {
        if (!segment_access.ExistsSegmentName(segment_name)) {
            return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
        }
        SegmentStatus status = SegmentStatus::UNDEFINED;
        auto err = segment_access.GetSegmentStatusByName(segment_name, status);
        if (err != ErrorCode::OK) {
            return tl::make_unexpected(err);
        }
        if (status != SegmentStatus::OK) {
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }
    }

    for (const auto& target_segment : request.target_segments) {
        if (unique_segments.contains(target_segment)) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (!segment_access.ExistsSegmentName(target_segment)) {
            return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
        }
        if (!segment_access.IsSegmentAllocatable(target_segment)) {
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }
    }
    return {};
}

tl::expected<UUID, ErrorCode> MasterService::CreateDrainJob(
    const CreateDrainJobRequest& request) {
    std::vector<std::string> draining_segments;
    {
        ScopedSegmentAccess segment_access =
            segment_manager_.getSegmentAccess();
        auto valid = ValidateDrainRequestLocked(segment_access, request);
        if (!valid.has_value()) {
            return tl::make_unexpected(valid.error());
        }

        draining_segments.reserve(request.segments.size());
        for (const auto& segment_name : request.segments) {
            auto err = segment_access.SetSegmentStatusByName(
                segment_name, SegmentStatus::DRAINING);
            if (err != ErrorCode::OK) {
                for (const auto& updated_segment : draining_segments) {
                    (void)segment_access.SetSegmentStatusByName(
                        updated_segment, SegmentStatus::OK);
                }
                return tl::make_unexpected(err);
            }
            draining_segments.push_back(segment_name);
        }
    }

    auto job = std::make_shared<DrainJob>();
    job->id = generate_uuid();
    job->request = request;
    job->created_at = std::chrono::system_clock::now();
    job->last_updated_at = job->created_at;
    job->status = JobStatus::CREATED;
    job->message = "Drain job created";

    {
        std::lock_guard<std::mutex> lock(job_mutex_);
        drain_jobs_.emplace(job->id, job);
    }

    return job->id;
}

tl::expected<QueryJobResponse, ErrorCode> MasterService::QueryDrainJob(
    const UUID& job_id) {
    std::shared_ptr<DrainJob> job;
    {
        std::lock_guard<std::mutex> lock(job_mutex_);
        auto it = drain_jobs_.find(job_id);
        if (it == drain_jobs_.end()) {
            return tl::make_unexpected(ErrorCode::JOB_NOT_FOUND);
        }
        job = it->second;
    }

    std::lock_guard<std::mutex> job_lock(job->mutex);
    QueryJobResponse response;
    response.id = job->id;
    response.type = job->type;
    response.status = job->status;
    response.created_at_ms_epoch = static_cast<int64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            job->created_at.time_since_epoch())
            .count());
    response.last_updated_at_ms_epoch = static_cast<int64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            job->last_updated_at.time_since_epoch())
            .count());
    response.segments = job->request.segments;
    response.succeeded_units = job->succeeded_units;
    response.failed_units = job->failed_units;
    response.blocked_units = job->blocked_units;
    response.active_units = static_cast<uint64_t>(job->active_tasks.size());
    response.migrated_bytes = job->migrated_bytes;
    response.message = job->message;
    return response;
}

tl::expected<void, ErrorCode> MasterService::CancelDrainJob(
    const UUID& job_id) {
    std::shared_ptr<DrainJob> job;
    {
        std::lock_guard<std::mutex> lock(job_mutex_);
        auto it = drain_jobs_.find(job_id);
        if (it == drain_jobs_.end()) {
            return tl::make_unexpected(ErrorCode::JOB_NOT_FOUND);
        }
        job = it->second;
    }

    std::vector<std::string> segments_to_restore;
    {
        std::lock_guard<std::mutex> job_lock(job->mutex);
        if (job->status == JobStatus::SUCCEEDED ||
            job->status == JobStatus::FAILED ||
            job->status == JobStatus::CANCELED || !job->active_tasks.empty()) {
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }

        job->status = JobStatus::CANCELED;
        job->last_updated_at = std::chrono::system_clock::now();
        job->message = "Drain job canceled";
        segments_to_restore = job->request.segments;
    }

    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();
    for (const auto& segment_name : segments_to_restore) {
        SegmentStatus status = SegmentStatus::UNDEFINED;
        if (segment_access.GetSegmentStatusByName(segment_name, status) ==
                ErrorCode::OK &&
            status != SegmentStatus::UNMOUNTING) {
            (void)segment_access.SetSegmentStatusByName(segment_name,
                                                        SegmentStatus::OK);
        }
    }
    return {};
}

std::string MasterService::MakeDrainUnitKey(
    const std::string& tenant_id, const std::string& key,
    const std::string& source_segment) const {
    const auto normalized_tenant = NormalizeTenantId(tenant_id);
    return std::to_string(normalized_tenant.size()) + ":" + normalized_tenant +
           ":" + std::to_string(key.size()) + ":" + key + ":" + source_segment;
}

std::optional<std::string> MasterService::SelectDrainTargetForKey(
    const ObjectMetadata& metadata, const std::string& source_segment,
    const std::vector<std::string>& requested_targets) {
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();
    std::vector<std::string> candidate_segments = requested_targets;
    if (candidate_segments.empty()) {
        auto err = segment_access.GetAllSegments(candidate_segments);
        if (err != ErrorCode::OK) {
            return std::nullopt;
        }
    }

    const auto existing_segments = metadata.GetReplicaSegmentNames();
    double best_util = std::numeric_limits<double>::max();
    std::optional<std::string> best_target;
    for (const auto& candidate : candidate_segments) {
        if (candidate == source_segment) {
            continue;
        }
        if (std::find(existing_segments.begin(), existing_segments.end(),
                      candidate) != existing_segments.end()) {
            continue;
        }
        if (!segment_access.IsSegmentAllocatable(candidate)) {
            continue;
        }
        size_t used = 0, capacity = 0;
        if (segment_access.QuerySegments(candidate, used, capacity) !=
                ErrorCode::OK ||
            capacity == 0) {
            continue;
        }
        const double util =
            static_cast<double>(used) / static_cast<double>(capacity);
        if (util < best_util) {
            best_util = util;
            best_target = candidate;
        }
    }
    return best_target;
}

void MasterService::RefreshDrainJobTasks(DrainJob& job) {
    auto read_access = task_manager_.get_read_access();
    std::vector<UUID> finished_task_ids;
    finished_task_ids.reserve(job.active_tasks.size());

    for (const auto& [task_id, active_task] : job.active_tasks) {
        auto task_opt = read_access.find_task_by_id(task_id);
        if (!task_opt.has_value()) {
            finished_task_ids.push_back(task_id);
            job.failed_units++;
            job.terminal_failed_unit_keys.insert(active_task.unit_key);
            continue;
        }
        if (!task_opt->is_finished()) {
            continue;
        }

        finished_task_ids.push_back(task_id);
        if (task_opt->status == TaskStatus::SUCCESS) {
            job.succeeded_units++;
            job.migrated_bytes += active_task.bytes;
            job.completed_unit_keys.insert(active_task.unit_key);
        } else {
            job.failed_units++;
            auto& retry_count = job.retry_counts[active_task.unit_key];
            retry_count++;
            if (retry_count >= kMaxDrainUnitRetries) {
                job.terminal_failed_unit_keys.insert(active_task.unit_key);
            }
        }
    }

    for (const auto& task_id : finished_task_ids) {
        job.active_tasks.erase(task_id);
    }
}

void MasterService::ScheduleDrainJobTasks(DrainJob& job) {
    if (job.status == JobStatus::CREATED) {
        job.status = JobStatus::PLANNING;
    }

    const uint32_t max_concurrency =
        std::max<uint32_t>(1, job.request.max_concurrency);
    if (job.active_tasks.size() >= max_concurrency) {
        job.status = JobStatus::RUNNING;
        return;
    }

    struct DrainPlan {
        std::string tenant_id;
        std::string key;
        std::string source_segment;
        std::string target_segment;
        size_t bytes;
        std::string unit_key;
    };

    const size_t slots = max_concurrency - job.active_tasks.size();
    std::vector<DrainPlan> plans;
    plans.reserve(slots);
    std::unordered_set<std::string> active_unit_keys;
    for (const auto& [_, task] : job.active_tasks) {
        active_unit_keys.insert(task.unit_key);
    }

    std::unordered_set<std::string> blocked_unit_keys;
    {
        std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
        for (size_t i = 0; i < kNumShards; ++i) {
            MetadataShardAccessorRO shard(this, i);
            for (const auto& [tenant_id, tenant_state] : shard->tenants) {
                for (const auto& [key, metadata] : tenant_state.metadata) {
                    for (const auto& source_segment : job.request.segments) {
                        const auto unit_key =
                            MakeDrainUnitKey(tenant_id, key, source_segment);
                        if (job.completed_unit_keys.contains(unit_key) ||
                            active_unit_keys.contains(unit_key) ||
                            job.terminal_failed_unit_keys.contains(unit_key)) {
                            continue;
                        }

                        const auto replica_segments =
                            metadata.GetReplicaSegmentNames();
                        if (std::find(replica_segments.begin(),
                                      replica_segments.end(), source_segment) ==
                            replica_segments.end()) {
                            continue;
                        }

                        if (metadata.IsHardPinned() ||
                            !metadata.IsLeaseExpired() ||
                            !metadata.AllReplicas(&Replica::fn_is_completed) ||
                            tenant_state.replication_tasks.contains(key)) {
                            blocked_unit_keys.insert(unit_key);
                            continue;
                        }

                        auto target = SelectDrainTargetForKey(
                            metadata, source_segment,
                            job.request.target_segments);
                        if (!target.has_value()) {
                            blocked_unit_keys.insert(unit_key);
                            continue;
                        }

                        if (plans.size() < slots) {
                            plans.push_back({tenant_id, key, source_segment,
                                             *target, metadata.size, unit_key});
                        }
                    }
                }
            }
        }
    }

    job.blocked_units = blocked_unit_keys.size();

    for (const auto& plan : plans) {
        auto task_id = CreateMoveTask(plan.key, plan.tenant_id,
                                      plan.source_segment, plan.target_segment);
        if (task_id.has_value()) {
            ActiveDrainTask active_task;
            active_task.task_id = task_id.value();
            active_task.tenant_id = plan.tenant_id;
            active_task.key = plan.key;
            active_task.source_segment = plan.source_segment;
            active_task.target_segment = plan.target_segment;
            active_task.bytes = plan.bytes;
            active_task.unit_key = plan.unit_key;
            job.active_tasks.emplace(task_id.value(), std::move(active_task));
        } else if (task_id.error() == ErrorCode::NO_AVAILABLE_HANDLE ||
                   task_id.error() ==
                       ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS ||
                   task_id.error() == ErrorCode::OBJECT_HAS_REPLICATION_TASK) {
            job.blocked_units++;
        } else {
            job.failed_units++;
            auto& retry_count = job.retry_counts[plan.unit_key];
            retry_count++;
            if (retry_count >= kMaxDrainUnitRetries) {
                job.terminal_failed_unit_keys.insert(plan.unit_key);
            }
        }
    }

    job.status = JobStatus::RUNNING;
    job.last_updated_at = std::chrono::system_clock::now();
    job.message = "Drain job running";
}

bool MasterService::MaybeCompleteDrainJob(DrainJob& job) {
    if (!job.active_tasks.empty()) {
        return false;
    }

    std::unordered_set<std::string> remaining_segments;
    std::unordered_set<std::string> remaining_unit_keys;
    {
        std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
        for (size_t i = 0; i < kNumShards; ++i) {
            MetadataShardAccessorRO shard(this, i);
            for (const auto& [tenant_id, tenant_state] : shard->tenants) {
                for (const auto& [key, metadata] : tenant_state.metadata) {
                    const auto replica_segments =
                        metadata.GetReplicaSegmentNames();
                    for (const auto& source_segment : job.request.segments) {
                        if (std::find(replica_segments.begin(),
                                      replica_segments.end(), source_segment) !=
                            replica_segments.end()) {
                            remaining_segments.insert(source_segment);
                            remaining_unit_keys.insert(MakeDrainUnitKey(
                                tenant_id, key, source_segment));
                        }
                    }
                }
            }
        }
    }

    {
        ScopedSegmentAccess segment_access =
            segment_manager_.getSegmentAccess();
        for (const auto& segment_name : job.request.segments) {
            if (!remaining_segments.contains(segment_name)) {
                (void)segment_access.SetSegmentStatusByName(
                    segment_name, SegmentStatus::DRAINED);
            }
        }
    }

    if (remaining_segments.empty()) {
        job.status = JobStatus::SUCCEEDED;
        job.last_updated_at = std::chrono::system_clock::now();
        job.message = "Drain job finished successfully";
        return true;
    }

    bool all_remaining_terminal_failed = !remaining_unit_keys.empty();
    for (const auto& unit_key : remaining_unit_keys) {
        if (!job.terminal_failed_unit_keys.contains(unit_key)) {
            all_remaining_terminal_failed = false;
            break;
        }
    }
    if (!all_remaining_terminal_failed) {
        return false;
    }

    {
        ScopedSegmentAccess segment_access =
            segment_manager_.getSegmentAccess();
        for (const auto& segment_name : job.request.segments) {
            SegmentStatus status = SegmentStatus::UNDEFINED;
            if (segment_access.GetSegmentStatusByName(segment_name, status) ==
                    ErrorCode::OK &&
                status != SegmentStatus::UNMOUNTING) {
                (void)segment_access.SetSegmentStatusByName(segment_name,
                                                            SegmentStatus::OK);
            }
        }
    }

    job.status = JobStatus::FAILED;
    job.last_updated_at = std::chrono::system_clock::now();
    job.message = "Drain job failed: unrecoverable units remain";
    return true;
}

void MasterService::ProcessDrainJobs() {
    std::vector<std::shared_ptr<DrainJob>> jobs;
    {
        std::lock_guard<std::mutex> lock(job_mutex_);
        jobs.reserve(drain_jobs_.size());
        for (const auto& [_, job] : drain_jobs_) {
            jobs.push_back(job);
        }
    }

    for (const auto& job : jobs) {
        if (!job) {
            continue;
        }
        std::lock_guard<std::mutex> job_lock(job->mutex);
        if (job->status == JobStatus::SUCCEEDED ||
            job->status == JobStatus::FAILED ||
            job->status == JobStatus::CANCELED) {
            continue;
        }
        RefreshDrainJobTasks(*job);
        if (MaybeCompleteDrainJob(*job)) {
            continue;
        }
        ScheduleDrainJobTasks(*job);
    }
}

void MasterService::JobDispatchThreadFunc() {
    while (job_dispatch_running_) {
        ProcessDrainJobs();
        std::this_thread::sleep_for(
            std::chrono::milliseconds(kJobDispatchThreadSleepMs));
    }
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

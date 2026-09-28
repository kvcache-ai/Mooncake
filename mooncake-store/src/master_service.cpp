#include "master_service.h"

#include <algorithm>
#include <array>
#include <cassert>
#include <cctype>
#include <cmath>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <future>
#include <iterator>
#include <limits>
#include <map>
#include <set>
#include <shared_mutex>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <tuple>
#include <regex>
#include <unordered_set>
#include <unistd.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <ylt/util/tl/expected.hpp>
#include <ylt/standalone/cinatra/url_encode_decode.hpp>
#include <boost/algorithm/string.hpp>

#include "http_metadata_server.h"
#include "master_metric_manager.h"
#include "common.h"
#include "common/network.h"
#include "environ.h"
#include "segment.h"
#include "segment/region_driver.h"
#ifdef USE_NOF
#include "spdk/spdk_wrapper.h"
#endif
#ifdef STORE_USE_ETCD
#include "etcd_helper.h"
#include "ha/kv/etcd_ha_kv_backend.h"
#endif
#include "ha/oplog/oplog_batch_storage.h"
#include "ha/oplog/ordered_oplog_writer.h"
#include "ha/snapshot/catalog/backends/embedded/embedded_snapshot_catalog_store.h"
#include "ha/snapshot/catalog/backends/redis/redis_snapshot_catalog_store.h"
#include "ha/snapshot/object/snapshot_object_store.h"
#include "ha/snapshot/snapshot_constants.h"
#include "types.h"
#include "serialize/serializer.h"
#include "ha/snapshot/snapshot_logger.h"
#include "common/zstd_util.h"
#include "common/file_util.h"
#include "storage/distributed/shard_allocator.h"
#include "storage/distributed/distributed_storage_backend.h"
#include "storage/distributed/immutable_bucket_allocator.h"
#include "random.h"
#include "kv_event/kv_event_config.h"
#include "master_snapshot_manager.h"
#include "master_snapshot_repository.h"
#include "ha_metric_manager.h"
#include "metadata_store.h"

namespace mooncake {

namespace {

// Per-cycle offload cap as a fraction of `offloading_queue_limit_`. Used only
// when offload-on-evict mode is active. Defers memory eviction for at most
// this fraction of the queue limit per BatchEvict cycle; beyond that, eviction
// falls back according to `offload_force_evict_`.
// NOTE: Both offloading_queue_limit_ and offload_cap_ratio_ are now
// configurable via --offloading_queue_limit and --offload_cap_ratio flags.

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

uint64_t SaturatingAdd(uint64_t lhs, uint64_t rhs) {
    if (lhs > std::numeric_limits<uint64_t>::max() - rhs) {
        return std::numeric_limits<uint64_t>::max();
    }
    return lhs + rhs;
}

uint64_t SaturatingMultiply(uint64_t lhs, uint64_t rhs) {
    if (lhs != 0 && rhs > std::numeric_limits<uint64_t>::max() / lhs) {
        return std::numeric_limits<uint64_t>::max();
    }
    return lhs * rhs;
}

// Decides whether PutStart may proceed with the replicas that were
// actually allocated. Three deliberately different policies apply:
//
//  - Memory-only (nof_replica_num == 0): best-effort. Fewer than
//    config.replica_num replicas (but at least one) still succeed, even
//    though DetermineReplicaWriteMode() classifies such configs as
//    RELIABLE_MULTI_REPLICA. The shortfall is surfaced via a WARNING log
//    (action=put_start_partial_allocation) and the
//    master_put_start_partial_allocations_total metric.
//  - FLEXIBLE_DUAL_REPLICA (1 memory + 1 NoF): allocating either side
//    alone is sufficient.
//  - Any other config with nof_replica_num > 0: strict. Both replica
//    types must match the requested counts exactly, otherwise PutStart
//    fails with NO_AVAILABLE_HANDLE.
//
// The "reliable" guarantee of RELIABLE_MULTI_REPLICA is enforced at the
// transfer stage (all allocated replicas must complete or the put is
// revoked), not at the allocation stage for memory-only configs.
bool HasExpectedReplicaAllocation(const ReplicateConfig& config,
                                  size_t allocated_memory_replicas,
                                  size_t allocated_nof_replicas) {
    if (config.nof_replica_num == 0 && config.dfs_replica_num == 0) {
        return allocated_memory_replicas > 0;
    }
    if (DetermineReplicaWriteMode(config) ==
        ReplicaWriteMode::FLEXIBLE_DUAL_REPLICA) {
        return allocated_memory_replicas + allocated_nof_replicas > 0;
    }
    return allocated_memory_replicas == config.replica_num &&
           allocated_nof_replicas == config.nof_replica_num;
}

void LogTenantQuotaLedgerError(const TenantQuotaResult& result,
                               std::string_view operation,
                               const TenantId& tenant_id,
                               std::string_view key) {
    if (result) {
        return;
    }
    LOG(ERROR) << "tenant quota ledger error operation=" << operation
               << ", tenant=" << tenant_id.value() << ", key=" << key
               << ", error=" << static_cast<int>(result.error());
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
    : tenants_([this](const TenantId& tenant_id) {
          // The factory binds every tenant to its quota account; the table
          // keeps one stable account per tenant id, so a tenant rebuilt after
          // a snapshot reset is bound to the account its charges went to.
          return std::make_shared<metadata::Tenant>(
              enable_multi_tenants_
                  ? tenant_quota_table_.GetOrCreateTenantHandle(tenant_id)
                  : nullptr);
      }),
      graceful_unmount_scheduler_(
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
      replica_cleanup_worker_([this] { ClearInvalidHandles(); }),
      enable_async_segment_cleanup_(
          !config.enable_ha && !config.enable_snapshot && !config.enable_cxl),
      default_kv_lease_ttl_(config.default_kv_lease_ttl),
      default_kv_soft_pin_ttl_(config.default_kv_soft_pin_ttl),
      max_kv_soft_pin_ttl_(config.max_kv_soft_pin_ttl),
      allow_evict_soft_pinned_objects_(config.allow_evict_soft_pinned_objects),
      eviction_ratio_(config.eviction_ratio),
      eviction_high_watermark_ratio_(config.eviction_high_watermark_ratio),
      tenant_eviction_high_watermark_ratio_(
          config.tenant_eviction_high_watermark_ratio),
      nof_eviction_ratio_(config.nof_eviction_ratio),
      nof_eviction_high_watermark_ratio_(
          config.nof_eviction_high_watermark_ratio),
      view_version_(config.view_version),
      client_active_ttl_sec_(config.client_active_ttl_sec),
      client_suspicion_ttl_sec_(config.client_suspicion_ttl_sec),
      nof_heartbeat_interval_sec_(
          std::chrono::seconds(config.nof_heartbeat_interval_sec)),
      nof_heartbeat_probe_timeout_ms_(
          std::chrono::milliseconds(config.nof_heartbeat_probe_timeout_ms)),
      nof_heartbeat_failures_threshold_(
          config.nof_heartbeat_failures_threshold),
      enable_ha_(config.enable_ha),
      enable_offload_(config.enable_offload),
      enable_oplog_(config.enable_ha && config.enable_oplog &&
                    config.ha_backend_type == "etcd"),
      weight_management_mutations_enabled_(
          !enable_oplog_ ||
          config.weight_management_oplog_capability_confirmed),
      oplog_batch_max_entries_(config.oplog_batch_max_entries),
      cluster_id_(config.cluster_id),
      root_fs_dir_(config.root_fs_dir),
      enable_disk_eviction_(config.enable_disk_eviction),
      quota_bytes_(config.quota_bytes),
      enable_multi_tenants_(config.enable_multi_tenants),
      segment_manager_(config.memory_allocator, config.enable_cxl),
      nof_segment_manager_(config.memory_allocator),
      memory_allocator_type_(config.memory_allocator),
      allocation_strategy_type_(config.enable_cxl
                                    ? AllocationStrategyType::CXL
                                    : config.allocation_strategy_type),
      allocation_strategy_(CreateAllocationStrategy(allocation_strategy_type_,
                                                    local_ssd_manager_)),
      put_start_discard_timeout_sec_(config.put_start_discard_timeout_sec),
      put_start_release_timeout_sec_(config.put_start_release_timeout_sec),
      offloading_queue_limit_(config.offloading_queue_limit),
      offload_cap_ratio_(config.offload_cap_ratio),
      task_manager_(config.task_manager_config),
      batch_oplog_writer_factory_(
          [](OrderedOpLogWriterConfig writer_config,
             OrderedOpLogWriter::WriteBatchFn write_batch) {
              return std::make_unique<OrderedOpLogWriter>(
                  std::move(writer_config), std::move(write_batch));
          }) {
    if (default_kv_soft_pin_ttl_ > max_kv_soft_pin_ttl_) {
        LOG(ERROR) << "Invalid soft-pin TTL configuration: default="
                   << default_kv_soft_pin_ttl_
                   << ", max=" << max_kv_soft_pin_ttl_;
        throw std::invalid_argument("Invalid soft-pin TTL configuration");
    }

    // Initialize HTTP metadata key prefix (read env var once at startup)
    const char* custom_prefix = std::getenv("MC_METADATA_CLUSTER_ID");
    if (custom_prefix && std::strlen(custom_prefix) > 0) {
        http_metadata_prefix_ = "mooncake/" + std::string(custom_prefix);
        if (http_metadata_prefix_.back() != '/') {
            http_metadata_prefix_ += '/';
        }
    } else {
        http_metadata_prefix_ = "mooncake/";
    }
    if (allocation_strategy_type_ == AllocationStrategyType::LOCAL_FIRST) {
        LOG(INFO) << "Local-first allocation strategy enabled";
    }

    const bool use_snapshot_backup_dir = !config.snapshot_backup_dir.empty();
    if (!config.enable_oplog_snapshot &&
        (config.enable_snapshot || config.enable_snapshot_restore)) {
        try {
            auto object_store_type =
                ParseSnapshotObjectStoreType(config.snapshot_object_store_type);
            snapshot_object_store_ =
                SnapshotObjectStore::Create(object_store_type);
            snapshot_catalog_store_ = CreateSnapshotCatalogStore(config);
        } catch (const std::exception& e) {
            LOG(ERROR) << "Failed to create snapshot stores: " << e.what();
            throw std::runtime_error(
                fmt::format("Failed to create snapshot stores: {}", e.what()));
        }
        // Initialize repository and codec for both save and restore
        snapshot_repository_ = std::make_unique<MasterSnapshotRepository>(
            snapshot_object_store_.get(), snapshot_catalog_store_.get(),
            config.snapshot_backup_dir, use_snapshot_backup_dir);
        snapshot_codec_ = std::make_unique<ha::MasterSnapshotCodec>();
    }

    if (enable_multi_tenants_) {
        auto store = CreateTenantQuotaPolicyStore(
            config.tenant_quota_connector_type,
            config.tenant_quota_connector_uri, cluster_id_);
        if (!store) {
            throw std::invalid_argument(store.error());
        }
        tenant_quota_policy_store_ = std::move(store.value());
    }

    if (config.enable_snapshot_restore && !config.enable_oplog_snapshot) {
        RestoreState();
    }
    if (enable_multi_tenants_) {
        LoadTenantQuotaPoliciesFromStoreOrThrow();
        RebuildTenantQuotaUsageFromMetadata();
    }
    if (config.enable_snapshot && config.snapshot_retention_count == 0) {
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
    if (tenant_eviction_high_watermark_ratio_ < 0.0 ||
        tenant_eviction_high_watermark_ratio_ > 1.0) {
        LOG(ERROR) << "Tenant eviction high watermark ratio must be between "
                      "0.0 and 1.0, current value: "
                   << tenant_eviction_high_watermark_ratio_;
        throw std::invalid_argument(
            "Invalid tenant eviction high watermark ratio");
    }

    // Validate offload tuning knobs here (not only via gflags validator),
    // because values loaded from a configuration file bypass the gflags
    // validator chain.
    if (offload_cap_ratio_ < 0.0 || offload_cap_ratio_ > 1.0) {
        LOG(ERROR) << "offload_cap_ratio must be between 0.0 and 1.0, "
                   << "current value: " << offload_cap_ratio_;
        throw std::invalid_argument("Invalid offload_cap_ratio");
    }
    if (offloading_queue_limit_ == 0) {
        LOG(ERROR) << "offloading_queue_limit must be greater than 0";
        throw std::invalid_argument("Invalid offloading_queue_limit");
    }
    if (offloading_queue_limit_ > 100'000'000ULL) {
        LOG(ERROR) << "offloading_queue_limit must be <= 100000000 to avoid "
                   << "overflow when computing offload_cap, current value: "
                   << offloading_queue_limit_;
        throw std::invalid_argument("Invalid offloading_queue_limit");
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

    if (config.dynamic_replication_mode == "observe") {
        dynamic_replication_mode_ = DynamicReplicationMode::kObserve;
    } else if (config.dynamic_replication_mode == "enforce") {
        dynamic_replication_mode_ = DynamicReplicationMode::kEnforce;
    } else {
        dynamic_replication_mode_ = DynamicReplicationMode::kOff;
    }
    dynamic_replication_heat_window_seconds_ =
        std::max<uint32_t>(1, config.dynamic_replication_heat_window_seconds);
    dynamic_replication_admission_qps_threshold_ =
        config.dynamic_replication_admission_qps_threshold > 0.0
            ? config.dynamic_replication_admission_qps_threshold
            : 0.8;
    dynamic_replication_max_memory_replicas_ =
        std::max<size_t>(1, config.dynamic_replication_max_memory_replicas);
    if (DynamicReplicationEnabled()) {
        LOG(INFO) << "Dynamic MEMORY replication mode enabled: mode="
                  << config.dynamic_replication_mode << ", heat_window_seconds="
                  << dynamic_replication_heat_window_seconds_
                  << ", admission_qps_threshold="
                  << dynamic_replication_admission_qps_threshold_
                  << ", max_memory_replicas="
                  << dynamic_replication_max_memory_replicas_;
    }

    InitDfsAllocatorFromEnvironment(config);
    kv_event_publisher_ =
        std::make_unique<KvEventPublisher>(BuildKvEventConfig(config));
    kv_track_tenant_epochs_ = KvEventsEnabled();

    if (enable_oplog_ && !cluster_id_.empty()) {
#ifdef STORE_USE_ETCD
        if (config.ha_backend_connstring.empty()) {
            LOG(INFO) << "Skipping automatic batch-record OpLog writer "
                         "initialization; no HA backend connstring configured";
        } else {
            ErrorCode connect_err = EtcdHelper::ConnectToEtcdStoreClient(
                config.ha_backend_connstring.c_str());
            if (connect_err != ErrorCode::OK) {
                throw std::runtime_error(fmt::format(
                    "failed to connect HA batch-record OpLog writer to etcd: "
                    "{}",
                    toString(connect_err)));
            }
            auto backend = std::make_shared<EtcdHaKvBackend>();
            // Direct MasterService construction has no leadership session;
            // supervisor-created services carry a non-zero acquired view.
            ErrorCode err = InitializeBatchOpLogWriter(
                std::move(backend),
                /*require_fenced_writer=*/view_version_ > 0);
            if (err != ErrorCode::OK) {
                throw std::runtime_error(fmt::format(
                    "failed to create HA batch-record OpLog writer: {}",
                    toString(err)));
            }
        }
#else
        if (config.ha_backend_connstring.empty()) {
            LOG(INFO) << "Skipping automatic batch-record OpLog writer "
                         "initialization; no HA backend connstring configured";
        } else {
            throw std::runtime_error(
                "failed to create HA batch-record OpLog writer: ETCD support "
                "not compiled in");
        }
#endif
    }

    // This worker is part of the Client lifecycle protocol. Start it before
    // any raw std::thread so a startup failure can unwind the constructor
    // normally instead of encountering joinable thread destructors.
    client_offboarding_worker_.Start();
    VLOG(1) << "action=start_client_offboarding_worker";

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

    replica_cleanup_worker_.Start();

    // NOTE: The async HTTP metadata cleanup worker is started lazily in
    // setHttpMetadataRemoteUrl() once http_metadata_remote_url_ is set,
    // since that happens after this constructor returns (in
    // WrappedMasterService).

    job_dispatch_running_ = true;
    job_dispatch_thread_ =
        std::thread(&MasterService::JobDispatchThreadFunc, this);
    VLOG(1) << "action=start_job_dispatch_thread";

    dynamic_replication_admission_running_ = true;
    dynamic_replication_admission_thread_ = std::thread(
        &MasterService::DynamicReplicationAdmissionThreadFunc, this);
    VLOG(1) << "action=start_dynamic_replication_admission_thread";

    if (!root_fs_dir_.empty()) {
        use_disk_replica_ = true;
        if (config.global_file_segment_size ==
            std::numeric_limits<int64_t>::max()) {
            MasterMetricManager::instance().set_dfs_capacity_unlimited(true);
        } else {
            MasterMetricManager::instance().inc_total_file_capacity(
                config.global_file_segment_size);
        }
    }

    if (config.enable_snapshot && !enable_oplog_) {
        if (memory_allocator_type_ == BufferAllocatorType::OFFSET) {
            // Initialize and start snapshot manager
            MasterSnapshotManagerOptions snapshot_options;
            snapshot_options.snapshot_interval_seconds =
                config.snapshot_interval_seconds;
            snapshot_options.snapshot_child_timeout_seconds =
                config.snapshot_child_timeout_seconds;
            snapshot_options.snapshot_retention_count =
                config.snapshot_retention_count;
            snapshot_options.snapshot_backup_dir = config.snapshot_backup_dir;
            snapshot_options.use_snapshot_backup_dir = use_snapshot_backup_dir;

            snapshot_manager_ = std::make_unique<MasterSnapshotManager>(
                this, snapshot_options, snapshot_mutex_,
                snapshot_object_store_.get(), snapshot_catalog_store_.get());
            snapshot_manager_->Start();
        }
    } else if (config.enable_snapshot && enable_oplog_) {
        LOG(INFO) << "Skipping primary snapshot generation in batch-record "
                     "OpLog mode; snapshots are owned by standby";
    }

    if (config.enable_cxl) {
        allocation_strategy_ = std::make_shared<CxlAllocationStrategy>();
        const auto result = segment_manager_.initializeCxlAllocator(
            config.cxl_path, config.cxl_size);
        LOG_IF(FATAL, result != ErrorCode::OK)
            << "Failed to initialize CXL allocator: "
            << static_cast<int>(result);
        VLOG(1) << "action=start_cxl_global_allocator";
    }
}

tl::expected<int, ErrorCode> MasterService::GetDfsShardCount() const {
    if (!shard_allocator_ || !shard_allocator_->IsInitialized()) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    }
    return shard_allocator_->GetShardCount();
}

tl::expected<int, ErrorCode> MasterService::ExpandDfsShards(int shard_count) {
    if (!shard_allocator_ || !shard_allocator_->IsInitialized()) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    }
    return shard_allocator_->ExpandShards(shard_count);
}

void MasterService::InitDfsAllocatorFromEnvironment(
    const MasterServiceConfig& config) {
    enable_dfs_ = Environ::GetBool(
        "MOONCAKE_ENABLE_DFS", Environ::GetBool("MOONCAKE_DFS_ENABLED", false));
    if (!enable_dfs_) return;

    if (config.enable_snapshot || config.enable_snapshot_restore ||
        enable_oplog_) {
        LOG(ERROR) << "DFS cannot be enabled with snapshot or oplog recovery "
                      "until DFS allocator state restoration is supported";
        throw std::invalid_argument(
            "DFS is incompatible with snapshot/oplog recovery");
    }

    const auto dfs_config = DistributedStorageConfig::FromEnvironment();
    if (!dfs_config.single_tenant) {
        LOG(ERROR) << "Currently, DFS backend is not supported in "
                      "multi-tenant mode";
        enable_dfs_ = false;
        return;
    }

    shard_allocator_ = nullptr;
    bucket_allocator_ = nullptr;
    if (dfs_config.allocator_type == "bucket") {
        auto allocator = std::make_unique<ImmutableBucketAllocator>();
        bucket_allocator_ = allocator.get();
        dfs_allocator_ = std::move(allocator);
    } else {
        auto allocator = std::make_unique<ShardAllocator>();
        shard_allocator_ = allocator.get();
        dfs_allocator_ = std::move(allocator);
    }
    auto init_result = dfs_allocator_->Init(dfs_config);
    if (!init_result) {
        LOG(ERROR) << "Failed to initialize DFS allocator, error="
                   << init_result.error() << ", config={"
                   << dfs_config.FormatStr() << "}";
        dfs_allocator_.reset();
        shard_allocator_ = nullptr;
        bucket_allocator_ = nullptr;
        enable_dfs_ = false;
        return;
    }

    LOG(INFO) << "DFS allocator initialized, type="
              << ToString(dfs_allocator_->Type()) << ", config={"
              << dfs_config.FormatStr() << "}"
              << (shard_allocator_ != nullptr
                      ? ", ready_shard_count=" +
                            std::to_string(shard_allocator_->GetShardCount())
                      : "");
}

std::unique_ptr<ha::SnapshotCatalogStore>
MasterService::CreateSnapshotCatalogStore(const MasterServiceConfig& config) {
    auto catalog_kind =
        ParseSnapshotCatalogKind(config.snapshot_catalog_store_type);
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
            const auto connstring =
                !config.snapshot_catalog_store_connstring.empty()
                    ? config.snapshot_catalog_store_connstring
                    : config.ha_backend_connstring;
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

    // Stop snapshot manager (non-blocking)
    if (snapshot_manager_) {
        snapshot_manager_->Stop();
    }

    task_cleanup_running_ = false;
    job_dispatch_running_ = false;
    dynamic_replication_admission_running_ = false;
    http_metadata_cleanup_running_ = false;
    graceful_unmount_scheduler_.Stop();
    replica_cleanup_worker_.Stop();
#ifdef USE_NOF
    nof_heartbeat_running_ = false;
#endif

    // Wake sleepers so join() doesn't block for long sleep intervals.
    task_cleanup_cv_.notify_all();
    dynamic_replication_admission_cv_.notify_all();
    http_metadata_cleanup_cv_.notify_all();

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
    if (task_cleanup_thread_.joinable()) {
        task_cleanup_thread_.join();
    }
    if (http_metadata_cleanup_thread_.joinable()) {
        http_metadata_cleanup_thread_.join();
    }
    if (job_dispatch_thread_.joinable()) {
        job_dispatch_thread_.join();
    }
    if (dynamic_replication_admission_thread_.joinable()) {
        dynamic_replication_admission_thread_.join();
    }

    // Join the snapshot producer before dropping queued/backoff offboarding
    // work. No snapshot can observe those residual jobs after this point.
    if (snapshot_manager_) {
        snapshot_manager_.reset();
    }
    client_offboarding_worker_.Stop();
    if (ordered_oplog_writer_) {
        ordered_oplog_writer_->Stop();
    }
    for (const auto& [segment, bytes] : standby_accounted_memory_bytes_) {
        MasterMetricManager::instance().dec_allocated_mem_size(
            segment, static_cast<int64_t>(bytes));
        MasterMetricManager::instance().remove_segment_metrics(segment);
    }

    // Segments still mounted here never went through CommitUnmountSegment;
    // release their capacity contribution so the process-lifetime
    // MasterMetricManager stays consistent when the next leadership term
    // constructs a fresh MasterService and the clients remount.
    segment_manager_.releaseCapacityMetrics();

    std::unique_lock<std::shared_mutex> client_lock(client_mutex_);
    auto& metrics = MasterMetricManager::instance();
    for (const auto& [_, record] : client_liveness_records_) {
        metrics.on_client_liveness_record_removed(record->state());
    }
    client_liveness_records_.clear();
}

void MasterService::SetBatchOpLogTerminalCallback(
    OrderedOpLogWriter::TerminalCallback callback) {
    if (ordered_oplog_writer_) {
        ordered_oplog_writer_->SetTerminalCallback(std::move(callback));
    }
}

void MasterService::StopBatchOpLogWriter() {
    if (ordered_oplog_writer_) ordered_oplog_writer_->Stop();
}

TieredStorageUsageSnapshot MasterService::GetStorageUsageSnapshot() const {
    return {
        .memory = segment_manager_.GetMemoryUsageSnapshot(),
        .nof = nof_segment_manager_.GetUsageSnapshot(),
    };
}

bool MasterService::IsTenantQuotaEnabled() const {
    return enable_multi_tenants_;
}

std::vector<TenantQuotaSnapshot> MasterService::ListTenantQuotaSnapshots()
    const {
    return tenant_quota_table_.ListTenantSnapshots();
}

std::optional<TenantQuotaSnapshot> MasterService::GetTenantQuotaSnapshot(
    const TenantId& tenant_id) const {
    assert(tenant_id.IsValid());
    return tenant_quota_table_.GetTenantSnapshot(tenant_id);
}

tl::expected<TenantQuotaSnapshot, ErrorCode>
MasterService::UpsertTenantQuotaPolicy(const TenantId& tenant_id,
                                       uint64_t requested_quota_bytes) {
    assert(tenant_id.IsValid());
    if (!enable_multi_tenants_) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    }
    if (requested_quota_bytes == 0 ||
        requested_quota_bytes > TenantQuotaAccount::kMaxChargedBytes) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::lock_guard<std::mutex> policy_lock(tenant_quota_policy_mutex_);
    auto policy = BuildTenantQuotaPolicySnapshot();
    policy.tenant_quotas[tenant_id.value()] = requested_quota_bytes;
    auto save_result = tenant_quota_policy_store_->Save(policy);
    if (!save_result) {
        LOG(ERROR) << "failed to save tenant quota policy: "
                   << save_result.error();
        return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
    }
    ApplyTenantQuotaPolicies(policy);
    auto result_snapshot = GetTenantQuotaSnapshot(tenant_id);
    if (!result_snapshot.has_value()) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return result_snapshot.value();
}

tl::expected<std::optional<TenantQuotaSnapshot>, ErrorCode>
MasterService::DeleteTenantQuotaPolicy(const TenantId& tenant_id) {
    assert(tenant_id.IsValid());
    if (!enable_multi_tenants_) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    }

    std::lock_guard<std::mutex> policy_lock(tenant_quota_policy_mutex_);
    auto policy = BuildTenantQuotaPolicySnapshot();
    auto policy_it = policy.tenant_quotas.find(tenant_id.value());
    if (policy_it == policy.tenant_quotas.end()) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    const uint64_t requested_quota_bytes = policy_it->second;

    auto restore_policy = [&] {
        std::lock_guard<std::mutex> recompute_lock(
            tenant_quota_recompute_mutex_);
        const uint64_t capacity = GetTenantQuotaAllocatableCapacityBytes();
        auto result = tenant_quota_table_.UpsertTenantPolicy(
            tenant_id, requested_quota_bytes, capacity);
        if (!result) {
            LOG(ERROR) << "failed to restore tenant quota policy tenant="
                       << tenant_id.value();
        }
    };

    auto disable_result =
        tenant_quota_table_.DisableTenantPolicyIfEmpty(tenant_id);
    if (!disable_result) {
        return tl::make_unexpected(disable_result.error() ==
                                           TenantQuotaError::kTenantNotEmpty
                                       ? ErrorCode::TENANT_NOT_EMPTY
                                       : ErrorCode::OBJECT_NOT_FOUND);
    }

    if (TenantHasObjects(tenant_id)) {
        restore_policy();
        return tl::make_unexpected(ErrorCode::TENANT_NOT_EMPTY);
    }

    policy.tenant_quotas.erase(policy_it);
    auto save_result = tenant_quota_policy_store_->Save(policy);
    if (!save_result) {
        restore_policy();
        LOG(ERROR) << "failed to save tenant quota policy: "
                   << save_result.error();
        return tl::make_unexpected(ErrorCode::PERSISTENT_FAIL);
    }
    ApplyTenantQuotaPolicies(policy);
    return GetTenantQuotaSnapshot(tenant_id);
}

auto MasterService::MountSegment(const Segment& segment, const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    ErrorCode mount_result = ErrorCode::INTERNAL_ERROR;
    {
        std::unique_lock<std::shared_mutex> client_lock(client_mutex_);
        auto [record_it, inserted] = client_liveness_records_.try_emplace(
            client_id, std::make_shared<ClientLivenessRecord>(
                           ClientLivenessRecord::Clock::now()));
        const auto record = record_it->second;
        if (inserted) {
            MasterMetricManager::instance().client_liveness_record_created();
        }
        const auto observation =
            record->ObserveAndRun(ClientLivenessRecord::Clock::now(), [&] {
                std::shared_lock<std::shared_mutex> snapshot_lock(
                    snapshot_mutex_);
                ScopedSegmentAccess segment_access =
                    segment_manager_.getSegmentAccess();
                LOG(INFO) << "client_id=" << client_id
                          << ", action=mount_segment, segment_name="
                          << segment.name;
                mount_result =
                    segment_access.MountSegment(segment, client_id, record);
                return mount_result == ErrorCode::OK ||
                       mount_result == ErrorCode::SEGMENT_ALREADY_EXISTS;
            });
        if (observation == ClientLivenessObservation::REJECTED_OFFLINE) {
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }
        if (mount_result != ErrorCode::OK &&
            mount_result != ErrorCode::SEGMENT_ALREADY_EXISTS) {
            if (inserted) {
                client_liveness_records_.erase(client_id);
                MasterMetricManager::instance()
                    .on_client_liveness_record_removed(
                        ClientLivenessState::ACTIVE);
            }
            return tl::make_unexpected(mount_result);
        }
        if (observation == ClientLivenessObservation::RECOVERED_ACTIVE) {
            MasterMetricManager::instance().client_liveness_recovered();
            LOG(INFO) << "client_id=" << client_id
                      << ", action=client_liveness_recovered, "
                         "signal=memory_mount";
        }
    }

    if (mount_result == ErrorCode::OK && enable_oplog_ &&
        ordered_oplog_writer_) {
        SegmentMountOp op;
        op.segment_name = segment.name;
        op.transport_endpoint = segment.te_endpoint;
        op.capacity = segment.size;
        op.is_memory_segment = true;
        op.file_path.clear();
        auto bytes = struct_pack::serialize(op);
        PersistSegmentOpForHAOrEnqueue("MountSegment", OpType::SEGMENT_MOUNT,
                                       segment.te_endpoint,
                                       std::string(bytes.begin(), bytes.end()));
    }
    if (mount_result == ErrorCode::OK) {
        UpdateClientHostId(client_id, segment.host_id);
        RecomputeTenantEffectiveQuotas();
    }
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

ErrorCode MasterService::ValidateStandbyRemountSegment(
    const Segment& segment) const {
    const StandbySegmentInfo* match = nullptr;
    for (const auto& standby : standby_memory_segments_) {
        if (standby.transport_endpoint == segment.te_endpoint ||
            standby.segment_name == segment.name) {
            if (match != nullptr && match != &standby) {
                return ErrorCode::INVALID_PARAMS;
            }
            match = &standby;
        }
    }
    if (match != nullptr && segment.protocol == "cxl") {
        return ErrorCode::UNAVAILABLE_IN_CURRENT_MODE;
    }
    if (match != nullptr && (match->segment_name != segment.name ||
                             match->transport_endpoint != segment.te_endpoint ||
                             match->capacity != segment.size)) {
        return ErrorCode::INVALID_PARAMS;
    }
    return ErrorCode::OK;
}

auto MasterService::ReMountSegment(const std::vector<Segment>& segments,
                                   const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    {
        std::unique_lock<std::shared_mutex> client_lock(client_mutex_);
        auto [record_it, record_inserted] =
            client_liveness_records_.try_emplace(
                client_id, std::make_shared<ClientLivenessRecord>(
                               ClientLivenessRecord::Clock::now()));
        const auto record = record_it->second;
        if (record_inserted) {
            MasterMetricManager::instance().client_liveness_record_created();
        }
        auto remount_guard = record->TryAcquireRetainingGuard();
        if (!remount_guard) {
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }
        const auto discard_provisional_record = [&] {
            if (!record_inserted) {
                return;
            }
            client_liveness_records_.erase(client_id);
            MasterMetricManager::instance().on_client_liveness_record_removed(
                ClientLivenessState::ACTIVE);
        };
        std::unique_lock<std::shared_mutex> snapshot_lock(snapshot_mutex_);
        for (const auto& segment : segments) {
            if (!segment.host_id.empty()) {
                client_host_id_[client_id] = segment.host_id;
                break;
            }
        }
        {
            auto segment_access = segment_manager_.getSegmentAccess();
            for (const auto& segment : segments) {
                auto standby_validation =
                    ValidateStandbyRemountSegment(segment);
                if (standby_validation != ErrorCode::OK) {
                    discard_provisional_record();
                    return tl::make_unexpected(standby_validation);
                }
                auto validation =
                    segment_access.ValidateRemountSegment(segment, client_id);
                if (validation != ErrorCode::OK) {
                    discard_provisional_record();
                    return tl::make_unexpected(validation);
                }
            }
        }
        if (ok_client_.contains(client_id)) {
            LOG(WARNING) << "client_id=" << client_id
                         << ", warn=client_already_remounted";
            // Return OK because this is an idempotent operation
            return {};
        }

        struct SegmentRestore {
            Segment segment;
            std::shared_ptr<BufferAllocatorBase> old_allocator;
            std::shared_ptr<BufferAllocatorBase> restored_allocator;
            std::vector<Replica*> replicas;
            std::vector<AllocatedBuffer::Descriptor> descriptors;
            std::vector<std::unique_ptr<AllocatedBuffer>> buffers;
            uint64_t imported_size{0};
        };
        std::vector<SegmentRestore> restores;
        restores.reserve(segments.size());
        std::vector<bool> segment_existed(segments.size());
        auto rollback_new_segments = [&] {
            ScopedSegmentAccess segment_access =
                segment_manager_.getSegmentAccess();
            for (size_t i = 0; i < segments.size(); ++i) {
                if (segment_existed[i] ||
                    !segment_access.GetAllocator(segments[i].id)) {
                    continue;
                }
                size_t capacity = 0;
                if (segment_access.PrepareUnmountSegment(
                        segments[i].id, capacity) != ErrorCode::OK) {
                    LOG(ERROR) << "segment_name=" << segments[i].name
                               << ", error=remount_rollback_prepare_failed";
                    continue;
                }
                if (segment_access.CommitUnmountSegment(
                        segments[i].id, client_id, capacity) != ErrorCode::OK) {
                    LOG(ERROR) << "segment_name=" << segments[i].name
                               << ", error=remount_rollback_commit_failed";
                }
            }
        };
        auto fail_remount =
            [&](ErrorCode error) -> tl::expected<void, ErrorCode> {
            rollback_new_segments();
            discard_provisional_record();
            return tl::make_unexpected(error);
        };

        ErrorCode remount_error = ErrorCode::OK;
        {
            ScopedSegmentAccess segment_access =
                segment_manager_.getSegmentAccess();
            for (size_t i = 0; i < segments.size(); ++i) {
                segment_existed[i] =
                    segment_access.GetAllocator(segments[i].id) != nullptr;
            }

            remount_error =
                segment_access.ReMountSegment(segments, client_id, record);
            if (remount_error == ErrorCode::OK) {
                segment_access.BindClientLiveness(client_id, record);
                for (const auto& segment : segments) {
                    auto allocator = segment_access.GetAllocator(segment.id);
                    Segment authoritative;
                    if (!allocator ||
                        !segment_access.GetSegment(segment.id, authoritative)) {
                        remount_error = ErrorCode::INTERNAL_ERROR;
                        break;
                    }
                    restores.push_back({std::move(authoritative),
                                        std::move(allocator),
                                        nullptr,
                                        {},
                                        {},
                                        {},
                                        0});
                }
            }
        }
        if (remount_error != ErrorCode::OK) {
            return fail_remount(remount_error);
        }

        bool ambiguous_endpoint = false;
        bool unsupported_cxl = false;
        // Strong handles, so every object this walk touched stays alive until
        // the repair below reaches it; there the entry is re-locked and
        // re-checked, and an entry torn down in the meantime is skipped.
        std::vector<std::shared_ptr<ObjectEntry>> affected_objects;
        std::unordered_set<const ObjectEntry*> affected_object_set;
        const bool any_standby_kept_alive = std::any_of(
            segments.begin(), segments.end(), [this](const Segment& segment) {
                return standby_accounted_memory_bytes_.contains(segment.name);
            });
        if (any_standby_kept_alive) {
            tenants_.Visit([&](const TenantId&,
                               const std::shared_ptr<metadata::Tenant>&
                                   tenant) {
                for (const auto& entry : tenant->SnapshotObjects()) {
                    bool matched_this_object = false;
                    entry->WithExclusiveAccess([&](ObjectMetadata& metadata,
                                                   ObjectEntry::State& state) {
                        if (state.is_torn_down) {
                            return;
                        }
                        metadata.VisitReplicas(
                            [](const Replica& replica) {
                                return replica.is_memory_replica() &&
                                       replica.status() !=
                                           ReplicaStatus::REMOVED &&
                                       replica.status() !=
                                           ReplicaStatus::FAILED;
                            },
                            [&](Replica& replica) {
                                auto descriptor = replica.get_descriptor()
                                                      .get_memory_descriptor()
                                                      .buffer_descriptor;
                                SegmentRestore* match = nullptr;
                                for (auto& restore : restores) {
                                    if (descriptor.transport_endpoint_ ==
                                            restore.segment.te_endpoint ||
                                        descriptor.transport_endpoint_ ==
                                            restore.segment.name) {
                                        if (match != nullptr) {
                                            ambiguous_endpoint = true;
                                            return;
                                        }
                                        match = &restore;
                                    }
                                }
                                if (match != nullptr) {
                                    if (descriptor.protocol_ == "cxl") {
                                        unsupported_cxl = true;
                                        return;
                                    }
                                    descriptor.transport_endpoint_ =
                                        match->segment.te_endpoint;
                                    match->replicas.push_back(&replica);
                                    match->descriptors.push_back(descriptor);
                                    matched_this_object = true;
                                }
                            });
                    });
                    if (matched_this_object &&
                        affected_object_set.insert(entry.get()).second) {
                        affected_objects.push_back(entry);
                    }
                }
            });
        }
        if (ambiguous_endpoint) {
            return fail_remount(ErrorCode::INVALID_PARAMS);
        }
        if (unsupported_cxl) {
            return fail_remount(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
        }

        for (auto& restore : restores) {
            if (restore.descriptors.empty()) {
                continue;
            }
            const RegionResourceSpec spec{
                restore.segment.id, restore.segment.name, restore.segment.base,
                restore.segment.size, restore.segment.te_endpoint};
            auto allocations =
                BuildRegionLiveAllocations(spec, restore.descriptors);
            if (!allocations) {
                return fail_remount(allocations.error());
            }
            if (std::dynamic_pointer_cast<OffsetBufferAllocator>(
                    restore.old_allocator)) {
                auto imported = ImportOffsetBufferAllocator(
                    restore.segment.name, restore.segment.base,
                    restore.segment.size, restore.segment.te_endpoint,
                    *allocations);
                if (!imported) {
                    return fail_remount(ErrorCode::INVALID_PARAMS);
                }
                restore.restored_allocator = std::move(imported->allocator);
                restore.buffers = std::move(imported->buffers);
            } else if (std::dynamic_pointer_cast<CachelibBufferAllocator>(
                           restore.old_allocator)) {
                auto imported = ImportCachelibBufferAllocator(
                    restore.segment.name, restore.segment.base,
                    restore.segment.size, restore.segment.te_endpoint,
                    *allocations);
                if (!imported) {
                    return fail_remount(ErrorCode::INVALID_PARAMS);
                }
                restore.restored_allocator = std::move(imported->allocator);
                restore.buffers = std::move(imported->buffers);
            } else {
                return fail_remount(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
            }
        }

        std::vector<ScopedSegmentAccess::AllocatorReplacement>
            allocator_replacements;
        for (auto& restore : restores) {
            if (restore.restored_allocator) {
                if (restore.buffers.size() != restore.replicas.size() ||
                    std::any_of(restore.buffers.begin(), restore.buffers.end(),
                                [](const auto& buffer) { return !buffer; })) {
                    return fail_remount(ErrorCode::INTERNAL_ERROR);
                }
                restore.imported_size = std::accumulate(
                    restore.descriptors.begin(), restore.descriptors.end(),
                    uint64_t{0}, [](uint64_t sum, const auto& descriptor) {
                        return sum + descriptor.size_;
                    });
                auto accounted =
                    standby_accounted_memory_bytes_.find(restore.segment.name);
                if (accounted == standby_accounted_memory_bytes_.end() ||
                    accounted->second < restore.imported_size) {
                    return fail_remount(ErrorCode::INTERNAL_ERROR);
                }
                allocator_replacements.push_back({restore.segment.id,
                                                  restore.old_allocator,
                                                  restore.restored_allocator});
            }
        }
        bool allocators_replaced = false;
        {
            ScopedSegmentAccess segment_access =
                segment_manager_.getSegmentAccess();
            allocators_replaced =
                segment_access.ReplaceAllocators(allocator_replacements);
        }
        if (!allocators_replaced) {
            return fail_remount(ErrorCode::INTERNAL_ERROR);
        }
        {
            ScopedSegmentAccess segment_access =
                segment_manager_.getSegmentAccess();
            for (auto& restore : restores) {
                for (auto& buffer : restore.buffers) {
                    segment_access.BindBufferToSegment(restore.segment.id,
                                                       *buffer);
                }
            }
        }
        for (auto& restore : restores) {
            if (restore.imported_size != 0) {
                MasterMetricManager::instance().dec_allocated_mem_size(
                    restore.segment.name,
                    static_cast<int64_t>(restore.imported_size));
                auto accounted =
                    standby_accounted_memory_bytes_.find(restore.segment.name);
                accounted->second -= restore.imported_size;
                if (accounted->second == 0) {
                    standby_accounted_memory_bytes_.erase(accounted);
                }
            }
            for (size_t i = 0; i < restore.replicas.size(); ++i) {
                (void)restore.replicas[i]->replace_memory_buffer(
                    std::move(restore.buffers[i]));
            }
            invalid_replica_endpoints_.erase(restore.segment.te_endpoint);
            invalid_replica_endpoints_.erase(restore.segment.name);
            standby_allocator_keepalive_.erase(restore.segment.te_endpoint);
            standby_allocator_keepalive_.erase(restore.segment.name);
        }
        for (const auto& entry : affected_objects) {
            entry->WithExclusiveAccess(
                [&](ObjectMetadata& metadata, ObjectEntry::State& state) {
                    if (state.is_torn_down) {
                        return;
                    }
                    metadata.GrantReadLease(
                        std::chrono::milliseconds(default_kv_lease_ttl_));
                });
        }

        // Change the client status to OK
        ok_client_.insert(client_id);
        MasterMetricManager::instance().inc_active_clients();
    }

    if (enable_oplog_ && ordered_oplog_writer_) {
        for (const auto& seg : segments) {
            SegmentMountOp op;
            op.segment_name = seg.name;
            op.transport_endpoint = seg.te_endpoint;
            op.capacity = seg.size;
            op.is_memory_segment = true;
            op.file_path.clear();
            auto bytes = struct_pack::serialize(op);
            PersistSegmentOpForHAOrEnqueue(
                "ReMountSegment", OpType::SEGMENT_MOUNT, seg.name,
                std::string(bytes.begin(), bytes.end()));
        }
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

std::shared_ptr<ClientLivenessRecord> MasterService::FindClientRecord(
    const UUID& client_id) const {
    std::shared_lock<std::shared_mutex> lock(client_mutex_);
    const auto it = client_liveness_records_.find(client_id);
    return it == client_liveness_records_.end() ? nullptr : it->second;
}

std::unordered_set<UUID, boost::hash<UUID>>
MasterService::GetRetainingClientIdsLocked() const {
    std::unordered_set<UUID, boost::hash<UUID>> clients;
    clients.reserve(client_liveness_records_.size());
    for (const auto& [client_id, record] : client_liveness_records_) {
        if (record->ShouldRetainResources()) {
            clients.insert(client_id);
        }
    }
    return clients;
}

void MasterService::UpdateClientHostId(const UUID& client_id,
                                       const std::string& host_id) {
    if (host_id.empty()) {
        return;
    }
    {
        std::shared_lock<std::shared_mutex> lock(client_mutex_);
        auto it = client_host_id_.find(client_id);
        if (it != client_host_id_.end() && it->second == host_id) {
            return;
        }
    }

    std::unique_lock<std::shared_mutex> lock(client_mutex_);
    auto it = client_host_id_.find(client_id);
    if (it == client_host_id_.end() || it->second != host_id) {
        client_host_id_[client_id] = host_id;
    }
}

std::string MasterService::GetClientHostId(const UUID& client_id) const {
    std::shared_lock<std::shared_mutex> lock(client_mutex_);
    auto it = client_host_id_.find(client_id);
    return it == client_host_id_.end() ? std::string() : it->second;
}

const TenantId& MasterService::ResolveRequestTenantId(
    const TenantId& tenant_id) const {
    assert(tenant_id.IsValid());
    if (!enable_multi_tenants_) {
        return TenantId::Default();
    }
    return tenant_id;
}

MasterService::ObjectIdentity MasterService::MakeObjectIdentityForRequest(
    const std::string& user_key, const TenantId& tenant_id) const {
    return {ResolveRequestTenantId(tenant_id), user_key};
}

bool MasterService::IsTenantRegistered(const TenantId& tenant_id) const {
    if (!enable_multi_tenants_) {
        return true;
    }
    return tenant_quota_table_.IsTenantRegistered(tenant_id);
}

tl::expected<TenantId, ErrorCode> MasterService::ResolveTenantIdForWrite(
    const TenantId& tenant_id) const {
    assert(tenant_id.IsValid());
    if (!enable_multi_tenants_) {
        return TenantId::Default();
    }
    std::lock_guard<std::mutex> policy_lock(tenant_quota_policy_mutex_);
    return ResolveTenantIdForWriteLocked(tenant_id);
}

tl::expected<TenantId, ErrorCode> MasterService::ResolveTenantIdForWriteLocked(
    const TenantId& tenant_id) const {
    assert(tenant_id.IsValid());
    if (!enable_multi_tenants_) {
        return TenantId::Default();
    }
    if (!IsTenantRegistered(tenant_id)) {
        return tl::make_unexpected(ErrorCode::TENANT_NOT_REGISTERED);
    }
    return tenant_id;
}

bool MasterService::TenantHasObjects(const TenantId& tenant_id) const {
    const auto tenant = tenants_.Lookup(tenant_id);
    return tenant != nullptr && tenant->ObjectCount() > 0;
}

TenantQuotaPolicySnapshot MasterService::BuildTenantQuotaPolicySnapshot()
    const {
    TenantQuotaPolicySnapshot snapshot;
    for (const auto& [tenant_id, requested_quota_bytes] :
         tenant_quota_table_.GetTenantPolicies()) {
        snapshot.tenant_quotas.emplace(tenant_id.value(),
                                       requested_quota_bytes);
    }
    return snapshot;
}

void MasterService::ApplyTenantQuotaPolicies(
    const TenantQuotaPolicySnapshot& snapshot) {
    TenantQuotaPolicyMap policies;
    for (const auto& [tenant_id, requested_quota_bytes] :
         snapshot.tenant_quotas) {
        policies.emplace(TenantId(tenant_id), requested_quota_bytes);
    }
    std::lock_guard<std::mutex> recompute_lock(tenant_quota_recompute_mutex_);
    const uint64_t capacity = GetTenantQuotaAllocatableCapacityBytes();
    auto result = tenant_quota_table_.ApplyTenantPolicies(policies, capacity);
    if (!result) {
        throw std::invalid_argument(
            "tenant quota policy exceeds atomic accounting range");
    }
}

void MasterService::LoadTenantQuotaPoliciesFromStoreOrThrow() {
    if (!enable_multi_tenants_) {
        return;
    }
    if (!tenant_quota_policy_store_) {
        throw std::runtime_error(
            "tenant quota policy store is not initialized");
    }
    std::lock_guard<std::mutex> policy_lock(tenant_quota_policy_mutex_);
    auto snapshot = tenant_quota_policy_store_->Load();
    if (!snapshot) {
        throw std::runtime_error("failed to load tenant quota policy: " +
                                 snapshot.error());
    }
    ApplyTenantQuotaPolicies(snapshot.value());
}

uint64_t MasterService::CompletedMemoryQuotaCharge(
    const ObjectMetadata& metadata) const {
    const auto completed_replicas =
        metadata.CountReplicas([](const Replica& replica) {
            return replica.is_memory_replica() && replica.is_completed();
        });
    const unsigned __int128 charge =
        static_cast<unsigned __int128>(metadata.size) * completed_replicas;
    return charge > std::numeric_limits<uint64_t>::max()
               ? std::numeric_limits<uint64_t>::max()
               : static_cast<uint64_t>(charge);
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

uint64_t MasterService::GetTenantQuotaAllocatableCapacityBytes() {
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
    if (!enable_multi_tenants_) {
        return;
    }
    std::lock_guard<std::mutex> recompute_lock(tenant_quota_recompute_mutex_);
    const uint64_t capacity = GetTenantQuotaAllocatableCapacityBytes();
    tenant_quota_table_.RecomputeEffectiveQuotas(capacity);
}

std::shared_ptr<metadata::Tenant> MasterService::GetOrCreateTenantHandle(
    const TenantId& tenant_id) {
    return tenants_.GetOrCreateTenant(tenant_id);
}

// --- Per-tenant replica-action records --------------------------------------
//
// Replica-action leases and the promotion-candidate key index, both
// re-validated against the entry their tenant's own route publishes.

void MasterService::EraseReplicaActionLeasesForObject(const TenantId& tenant_id,
                                                      std::string_view key) {
    std::lock_guard<std::mutex> lock(replica_action_mutex_);
    const auto it = replica_action_state_.find(tenant_id);
    if (it == replica_action_state_.end()) {
        return;
    }
    it->second.leases.EraseForObject(key);
}

void MasterService::ResetReplicaActionStateForReload() {
    // The per-entry pass first: it unindexes candidates through the same mutex
    // the table clear below takes. Both halves of the reset live here, so a
    // reload path cannot retire one without the other.
    ReconcilePromotionBookkeeping();
    std::lock_guard<std::mutex> lock(replica_action_mutex_);
    replica_action_state_.clear();
}

std::optional<ReplicaActionLease> MasterService::FindDynamicReplicationLease(
    const TenantId& tenant_id, const UUID& proposal_id) {
    std::lock_guard<std::mutex> lock(replica_action_mutex_);
    const auto it = replica_action_state_.find(tenant_id);
    if (it == replica_action_state_.end()) {
        return std::nullopt;
    }
    return it->second.leases.Find(proposal_id);
}

void MasterService::PutDynamicReplicationLease(
    const TenantId& tenant_id, const std::shared_ptr<ObjectEntry>& entry,
    const UUID& proposal_id, ReplicaActionLease lease) {
    assert(entry != nullptr);
    // A lease is filed under the key of the entry that names its publication.
    assert(lease.key == entry->key());
    std::lock_guard<std::mutex> lock(replica_action_mutex_);
    replica_action_state_[tenant_id].leases.Put(proposal_id, std::move(lease));
}

bool MasterService::RemoveDynamicReplicationLease(const TenantId& tenant_id,
                                                  const UUID& proposal_id) {
    std::lock_guard<std::mutex> lock(replica_action_mutex_);
    const auto it = replica_action_state_.find(tenant_id);
    if (it == replica_action_state_.end()) {
        return false;
    }
    return it->second.leases.Remove(proposal_id);
}

void MasterService::EraseExpiredDynamicReplicationLeases(
    const TenantId& tenant_id,
    const std::chrono::system_clock::time_point& now) {
    std::lock_guard<std::mutex> lock(replica_action_mutex_);
    const auto it = replica_action_state_.find(tenant_id);
    if (it == replica_action_state_.end()) {
        return;
    }
    it->second.leases.EraseExpired(now);
}

void MasterService::IndexPromotionCandidate(const TenantId& tenant_id,
                                            const std::string& key) {
    std::lock_guard<std::mutex> lock(replica_action_mutex_);
    replica_action_state_[tenant_id].promotion_candidate_keys.insert(key);
}

void MasterService::UnindexPromotionCandidate(const TenantId& tenant_id,
                                              const std::string& key) {
    std::lock_guard<std::mutex> lock(replica_action_mutex_);
    const auto it = replica_action_state_.find(tenant_id);
    if (it == replica_action_state_.end()) {
        return;
    }
    it->second.promotion_candidate_keys.erase(key);
}

std::vector<std::string> MasterService::PromotionCandidateKeys(
    const TenantId& tenant_id) const {
    std::lock_guard<std::mutex> lock(replica_action_mutex_);
    const auto it = replica_action_state_.find(tenant_id);
    if (it == replica_action_state_.end()) {
        return {};
    }
    return {it->second.promotion_candidate_keys.begin(),
            it->second.promotion_candidate_keys.end()};
}

std::vector<std::string> MasterService::TakePromotionCandidateSlice(
    const TenantId& tenant_id, size_t limit) {
    std::vector<std::string> slice;
    if (limit == 0) {
        return slice;
    }
    std::lock_guard<std::mutex> lock(replica_action_mutex_);
    const auto state_it = replica_action_state_.find(tenant_id);
    if (state_it == replica_action_state_.end()) {
        return slice;
    }
    auto& state = state_it->second;
    const auto& keys = state.promotion_candidate_keys;
    if (keys.empty()) {
        state.promotion_retry_resume_key.clear();
        return slice;
    }

    // The index is ordered, so resuming after the last key of the previous
    // slice costs a lookup rather than a walk over the keys already examined.
    auto it = keys.upper_bound(state.promotion_retry_resume_key);
    if (it == keys.end()) {
        // The keys after the resume point are gone: start over at the beginning
        // rather than skipping this tenant for the rest of the process.
        it = keys.begin();
    }
    const auto start = it;
    while (it != keys.end() && slice.size() < limit) {
        slice.push_back(*it);
        ++it;
    }
    if (it == keys.end()) {
        auto wrapped = keys.begin();
        while (wrapped != start && slice.size() < limit) {
            slice.push_back(*wrapped);
            ++wrapped;
        }
        // The whole index was examined: the next round starts at the beginning.
        state.promotion_retry_resume_key.clear();
    } else {
        state.promotion_retry_resume_key = slice.back();
    }
    return slice;
}

// Erase any in-flight PromotionTask of the guarded entry, refund its pending
// charge, and decrement the cluster-wide in-flight counter. The caller holds
// the entry's own lock.
void MasterService::ErasePromotionTaskLocked(metadata::Tenant& tenant,
                                             ObjectEntry::State& state) {
    if (!state.promotion_task.has_value()) {
        return;
    }
    ReleaseTenantQuota(
        GetBoundTenantQuotaHandle(tenant),
        std::exchange(state.promotion_task->pending_quota_charge_bytes, 0));
    state.promotion_task.reset();
    promotion_in_flight_.fetch_sub(1, std::memory_order_relaxed);
    MasterMetricManager::instance().dec_promotion_in_flight();
    MasterMetricManager::instance().inc_promotion_cancelled();
}

// Drops the pending dynamic-replication state of the guarded entry and the
// leases of its key. The caller holds the entry's own lock.
void MasterService::ClearDynamicReplicationStateLocked(
    const TenantId& tenant_id, const std::shared_ptr<ObjectEntry>& entry,
    ObjectEntry::State& state) {
    state.dynamic_replication_pending.reset();
    state.dynamic_replication_cooldown =
        std::chrono::steady_clock::time_point{};
    EraseReplicaActionLeasesForObject(tenant_id, entry->key());
}

TenantQuotaHandle MasterService::GetBoundTenantQuotaHandle(
    const metadata::Tenant& tenant) const {
    if (!enable_multi_tenants_) {
        return nullptr;
    }
    const auto account = tenant.QuotaAccount();
    assert(account != nullptr);
    return account;
}

tl::expected<void, ErrorCode> MasterService::ChargeTenantQuota(
    TenantQuotaHandle account, uint64_t bytes) {
    if (!enable_multi_tenants_) {
        return {};
    }
    if (account == nullptr) {
        LOG(ERROR) << "tenant quota charge attempted without a bound handle";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    auto result = account->TryCharge(bytes);
    if (result) {
        return {};
    }
    return tl::make_unexpected(
        result.error().error == TenantQuotaError::kTenantNotRegistered
            ? ErrorCode::TENANT_NOT_REGISTERED
        : result.error().error == TenantQuotaError::kQuotaExceeded
            ? ErrorCode::TENANT_QUOTA_EXCEEDED
        : result.error().error == TenantQuotaError::kInvalidArgument
            ? ErrorCode::INVALID_PARAMS
            : ErrorCode::INTERNAL_ERROR);
}

void MasterService::ReleaseTenantQuota(TenantQuotaHandle account,
                                       uint64_t bytes) {
    if (!enable_multi_tenants_ || bytes == 0) {
        return;
    }
    if (account == nullptr) {
        LOG(ERROR) << "tenant quota release attempted without a bound handle"
                   << ", bytes=" << bytes;
        return;
    }
    if (!account->Release(bytes)) {
        LOG(ERROR) << "tenant quota release mismatch bytes=" << bytes;
    }
}

void MasterService::RebuildTenantQuotaUsageFromMetadata() {
    if (!enable_multi_tenants_) {
        return;
    }

    TenantQuotaUsageMap usage;
    tenants_.Visit([&](const TenantId& tenant_id,
                       const std::shared_ptr<metadata::Tenant>& tenant) {
        const auto objects = tenant->SnapshotObjects();
        if (objects.empty()) {
            return;
        }
        uint64_t charged_bytes = 0;
        for (const auto& entry : objects) {
            entry->WithSharedAccess([&](const ObjectMetadata& metadata,
                                        const ObjectEntry::State&) {
                const uint64_t charge = CompletedMemoryQuotaCharge(metadata);
                if (charge > TenantQuotaAccount::kMaxChargedBytes ||
                    charged_bytes >
                        TenantQuotaAccount::kMaxChargedBytes - charge) {
                    throw std::overflow_error(
                        "rebuilt tenant quota exceeds 2^63 - 1 bytes");
                }
                charged_bytes += charge;
            });
        }
        usage.emplace(tenant_id, charged_bytes);
    });

    tenants_.Visit([&](const TenantId& tenant_id,
                       const std::shared_ptr<metadata::Tenant>& tenant) {
        const auto account = tenant->QuotaAccount();
        for (const auto& entry : tenant->SnapshotObjects()) {
            entry->WithExclusiveAccess([&](ObjectMetadata& metadata,
                                           ObjectEntry::State&) {
                auto rebuild_result = metadata.quota_ledger.Rebuild(
                    account, CompletedMemoryQuotaCharge(metadata));
                if (!rebuild_result) {
                    throw std::runtime_error(
                        "failed to rebuild object tenant quota ledger for " +
                        tenant_id.value() + "/" + entry->key());
                }
            });
        }
    });

    for (const auto& [tenant_id, _] : usage) {
        if (!tenant_quota_table_.IsTenantRegistered(tenant_id)) {
            LOG(WARNING)
                << "tenant " << tenant_id.value()
                << " exists in metadata but has no connector quota policy; "
                   "creating orphan quota state";
        }
    }
    std::lock_guard<std::mutex> recompute_lock(tenant_quota_recompute_mutex_);
    const uint64_t capacity = GetTenantQuotaAllocatableCapacityBytes();
    auto rebuild_result = tenant_quota_table_.RebuildUsage(usage, capacity);
    if (!rebuild_result) {
        throw std::runtime_error("failed to rebuild tenant quota usage");
    }
}

MasterService::ObjectOperationLock MasterService::AcquireObjectOperationLock(
    const TenantId& tenant_id, const std::string& key) {
    const auto scoped_key = tenant_id.MakeScopedKey(key);
    const auto stripe_idx =
        std::hash<std::string>{}(scoped_key) % kObjectOperationLockStripes;
    return {std::unique_lock<std::mutex>(object_operation_locks_[stripe_idx])};
}

WeightMetadataStore::Result<WeightRevisionMetadata>
MasterService::BeginWeightImport(const BeginWeightImportRequest& request) {
    return weight_manager_.BeginWeightImport(request);
}

WeightMetadataStore::Result<WeightRevisionMetadata>
MasterService::CommitWeightImport(const CommitWeightImportRequest& request) {
    return weight_manager_.CommitWeightImport(request);
}

WeightMetadataStore::Result<WeightRevisionMetadata>
MasterService::AbortWeightImport(const AbortWeightImportRequest& request) {
    return weight_manager_.AbortWeightImport(request);
}

WeightMetadataStore::Result<WeightRevisionView>
MasterService::GetWeightRevision(
    const GetWeightRevisionRequest& request) const {
    return weight_manager_.GetWeightRevision(request);
}

WeightMetadataStore::Result<ListWeightRevisionsResponse>
MasterService::ListWeightRevisions(
    const ListWeightRevisionsRequest& request) const {
    return weight_manager_.ListWeightRevisions(request);
}

WeightMetadataStore::Result<WeightRevisionLease>
MasterService::AcquireWeightRevisionLease(
    const AcquireWeightRevisionLeaseRequest& request) {
    return weight_manager_.AcquireWeightRevisionLease(request);
}

WeightMetadataStore::Result<WeightRevisionLease>
MasterService::RenewWeightRevisionLease(
    const RenewWeightRevisionLeaseRequest& request) {
    return weight_manager_.RenewWeightRevisionLease(request);
}

WeightMetadataStore::Result<void> MasterService::ReleaseWeightRevisionLease(
    const ReleaseWeightRevisionLeaseRequest& request) {
    return weight_manager_.ReleaseWeightRevisionLease(request);
}

std::vector<std::string> MasterService::GetGroupMemberKeys(
    const TenantId& tenant_id, const std::string& group_id) const {
    const auto tenant = tenants_.Lookup(ResolveRequestTenantId(tenant_id));
    if (tenant == nullptr) {
        return {};
    }
    return tenant->GroupMembers(group_id);
}

MasterService::GroupEvictionResult MasterService::EvictGroupOrObject(
    const TenantId& tenant_id, const std::string& key,
    const std::string& group_id, bool allow_soft_pinned,
    std::chrono::system_clock::time_point now,
    const std::function<MasterService::EvictMemberOutcome(
        const std::string&, ObjectMetadata&, ObjectEntry::State&,
        metadata::Tenant&)>& evict_one_member) {
    GroupEvictionResult result;

    // The membership is read from the tenant's group table without an object
    // lock, so each key below is re-resolved: a group can be rebuilt while
    // this walk runs and a key may name a different object, or none.
    const auto tenant_handle = tenants_.Lookup(tenant_id);
    if (tenant_handle == nullptr) {
        return result;
    }
    metadata::Tenant& tenant = *tenant_handle;

    std::vector<std::string> member_keys = tenant.GroupMembers(group_id);
    if (member_keys.empty()) {
        member_keys.push_back(key);
    }

    auto is_evictable_memory_replica = [this](const Replica& replica) {
        return IsEvictableMemoryReplica(replica);
    };

    std::vector<std::shared_ptr<ObjectEntry>> members_to_erase;
    for (const auto& member_key : member_keys) {
        // One member at a time, re-validated under its own entry lock: a live
        // shared group lease leaves no member's lease expired, so the group is
        // skipped here. The plain published-object access, so the read-write
        // accessor's invalid-replica cleanup runs only after the callback.
        (void)tenant.WithPublishedObject(
            member_key,
            [&](ObjectMetadata& member_metadata, ObjectEntry::State& state) {
                if (member_metadata.IsHardPinned() ||
                    !member_metadata.IsLeaseExpired(now) ||
                    (!allow_soft_pinned &&
                     IsSoftPinActive(member_metadata, now)) ||
                    !member_metadata.HasReplica(is_evictable_memory_replica)) {
                    return;
                }
                // The callback performs only the path-specific eviction (oplog
                // persist, offload, publish). Member teardown happens below,
                // after this lock is released, so no member is erased while
                // another one's lock is held. `key` is the caller's to erase.
                EvictMemberOutcome member_outcome = evict_one_member(
                    member_key, member_metadata, state, tenant);
                result.freed_bytes += member_outcome.freed_bytes;
                result.evicted_objects += member_outcome.evicted_objects;
                if (member_outcome.stop_scan) {
                    result.stop_scan = true;
                    result.error = member_outcome.error;
                    return;
                }
                // A member the eviction left with nothing valid is torn down
                // below, after this lock is released: the callback held that
                // entry's lock, so `member_entry` is what the route publishes.
                if (member_key != key && !member_metadata.IsValid()) {
                    const auto member_entry = tenant.Get(member_key);
                    if (member_entry != nullptr) {
                        members_to_erase.push_back(member_entry);
                    }
                }
            });
        if (result.stop_scan) {
            break;
        }
    }
    // Erasing takes each member's lock again and drops its route slot only
    // while that slot still publishes it, so a same-key replacement survives.
    for (const auto& member_entry : members_to_erase) {
        member_entry->WithExclusiveAccess(
            [&](ObjectMetadata& member_metadata, ObjectEntry::State& state) {
                (void)EraseMetadata(tenant, member_entry, member_metadata,
                                    state, tenant_id);
            });
    }
    return result;
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
        return (replica.is_disk_replica() || replica.is_local_disk_replica()) &&
               replica.is_completed();
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
    tenants_.Visit(
        [](const TenantId&, const std::shared_ptr<metadata::Tenant>& tenant) {
            // The objects are re-resolved one at a time: a route slot can be
            // replaced while this walk runs, and the accounting follows the
            // entry the slot publishes now.
            for (const auto& entry : tenant->SnapshotObjects()) {
                entry->WithExclusiveAccess(
                    [](ObjectMetadata& metadata, ObjectEntry::State&) {
                        SyncCacheTotalAccounting(metadata);
                    });
            }
        });
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

size_t MasterService::RecordDynamicReplicaRemoval(
    ObjectMetadata& metadata, const std::vector<ReplicaID>& replica_ids) {
    if (replica_ids.empty()) {
        return 0;
    }
    const size_t removed = metadata.ForgetDynamicReplicas(replica_ids);
    if (removed > 0) {
        metadata.SetDynamicReplicationRecreateAfter(
            std::chrono::steady_clock::now() +
            kDynamicReplicationRecreateCooldown);
    }
    return removed;
}

size_t MasterService::EraseReplicasWithCacheTotalAccounting(
    ObjectMetadata& metadata,
    const std::function<bool(const Replica&)>& pred_fn,
    std::vector<ReplicaID>* erased_replica_ids) {
    auto erased_replicas =
        PopReplicasWithCacheTotalAccounting(metadata, pred_fn);
    if (erased_replica_ids != nullptr) {
        erased_replica_ids->reserve(erased_replica_ids->size() +
                                    erased_replicas.size());
        for (const auto& replica : erased_replicas) {
            erased_replica_ids->push_back(replica.id());
        }
    }
    std::vector<ReplicaID> erased_ids;
    erased_ids.reserve(erased_replicas.size());
    for (const auto& replica : erased_replicas) {
        erased_ids.push_back(replica.id());
    }
    RecordDynamicReplicaRemoval(metadata, erased_ids);
    // Release SSD/local-disk usage for any local-disk replicas being removed.
    // No-op for memory/noF replicas, so it is safe to call unconditionally.
    ReleaseLocalDiskUsage(erased_replicas);
    FreeDfsReplicas(metadata.user_key, erased_replicas);
    return erased_replicas.size();
}

tl::expected<void, ErrorCode> MasterService::SettlePrimaryWriteQuotaIfReady(
    metadata::Tenant& tenant, ObjectMetadata& metadata) {
    if (!enable_multi_tenants_) {
        return {};
    }
    if (!metadata.IsValid()) {
        LOG(ERROR) << "tenant quota surviving-object settlement attempted for "
                      "invalid metadata, tenant="
                   << metadata.tenant_id.value()
                   << ", key=" << metadata.user_key;
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    if (metadata.HasReplica([](const Replica& replica) {
            return replica.is_memory_replica() && replica.is_processing();
        })) {
        return {};
    }

    auto account = GetBoundTenantQuotaHandle(tenant);
    auto settle_result = metadata.quota_ledger.SettlePrimaryWrite(
        account, CompletedMemoryQuotaCharge(metadata));
    if (!settle_result) {
        LogTenantQuotaLedgerError(settle_result, "settle_primary_write",
                                  metadata.tenant_id, metadata.user_key);
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return {};
}

void MasterService::FinalizeRemovedReplicasAfterDurable(
    const std::shared_ptr<ObjectEntry>& entry, const OpLogEntry& durable_entry,
    const std::vector<ReplicaID>& replica_ids, QuotaEraseMode quota_mode,
    const std::vector<std::string>& previous_media_hint) {
    if (entry == nullptr || replica_ids.empty()) {
        return;
    }

    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    const TenantId tenant_id(durable_entry.tenant_id);
    const auto tenant = tenants_.Lookup(tenant_id);
    if (tenant == nullptr) {
        return;
    }
    // The plain published-object access, not the read-write one: the
    // invalid-replica cleanup that accessor runs first would mutate whatever
    // the route publishes now, including a replacement this cleanup does not
    // own.
    (void)tenant->WithPublishedObject(
        durable_entry.object_key,
        [&](ObjectMetadata& metadata, ObjectEntry::State& state) {
            // Identity gate: act only while the route still publishes the
            // entry this cleanup was armed for. A same-key recreate that landed
            // while it was in flight keeps its own replicas, quota, KV state
            // and task records untouched.
            const auto published = tenant->Get(durable_entry.object_key);
            if (published != entry) {
                return;
            }
            std::unordered_set<ReplicaID> ids(replica_ids.begin(),
                                              replica_ids.end());
            // Finalized replicas are already marked REMOVED, so their media is
            // missing from the completed-replica view: snapshot the pre-removal
            // set for the event delta.
            auto previous_media = KvRemovalSnapshot(metadata);
            previous_media.insert(previous_media.end(),
                                  previous_media_hint.begin(),
                                  previous_media_hint.end());
            auto erased_replicas = PopReplicasWithCacheTotalAccounting(
                metadata, [&ids](const Replica& replica) {
                    return replica.status() == ReplicaStatus::REMOVED &&
                           ids.contains(replica.id());
                });
            if (erased_replicas.empty()) {
                return;
            }
            std::vector<ReplicaID> erased_replica_ids;
            erased_replica_ids.reserve(erased_replicas.size());
            for (const auto& replica : erased_replicas) {
                erased_replica_ids.push_back(replica.id());
            }
            RecordDynamicReplicaRemoval(metadata, erased_replica_ids);
            const uint64_t erased_memory_replicas = static_cast<uint64_t>(
                std::count_if(erased_replicas.begin(), erased_replicas.end(),
                              [](const Replica& replica) {
                                  return replica.is_memory_replica();
                              }));
            const bool has_processing_memory =
                metadata.HasReplica([](const Replica& replica) {
                    return replica.is_memory_replica() &&
                           replica.is_processing();
                });
            if (enable_multi_tenants_ && erased_memory_replicas > 0 &&
                has_processing_memory) {
                const uint64_t committed_charge =
                    metadata.quota_ledger.CommittedBytes();
                if (metadata.size > committed_charge / erased_memory_replicas) {
                    LOG(ERROR)
                        << "tenant quota removed-replica release exceeds "
                           "committed bytes, tenant="
                        << tenant_id.value()
                        << ", key=" << durable_entry.object_key
                        << ", object_size=" << metadata.size
                        << ", erased_memory_replicas=" << erased_memory_replicas
                        << ", committed_bytes=" << committed_charge;
                } else {
                    const uint64_t release_bytes =
                        metadata.size * erased_memory_replicas;
                    auto release_result =
                        metadata.quota_ledger.ReleaseCommitted(
                            GetBoundTenantQuotaHandle(*tenant), release_bytes);
                    LogTenantQuotaLedgerError(release_result,
                                              "release_committed", tenant_id,
                                              durable_entry.object_key);
                }
            }
            ReleaseLocalDiskUsage(erased_replicas);
            FreeDfsReplicas(metadata.user_key, erased_replicas);
            CancelPromotionTaskForRemovedReplicas(*tenant, metadata, state,
                                                  erased_replica_ids);
            if (!metadata.IsValid()) {
                (void)EraseMetadata(*tenant, published, metadata, state,
                                    tenant_id, quota_mode, previous_media);
            } else {
                SyncKvObjectState(durable_entry.object_key, metadata, tenant_id,
                                  previous_media);
                auto settle_result =
                    SettlePrimaryWriteQuotaIfReady(*tenant, metadata);
                if (settle_result &&
                    metadata.AllReplicas(&Replica::fn_is_completed)) {
                    state.is_processing = false;
                }
            }
        });
}

void MasterService::FinalizeMetadataEraseAfterDurable(
    std::shared_ptr<ObjectEntry> entry, const TenantId& tenant_id,
    QuotaEraseMode quota_mode) {
    // Act on the pinned publication only: a same-key recreate that landed while
    // this entry was in flight must not be erased.
    if (entry == nullptr) {
        return;
    }
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    const auto tenant = tenants_.Lookup(tenant_id);
    if (tenant == nullptr) {
        return;
    }
    // One exclusive access makes `is_torn_down` both the liveness decision and
    // the teardown claim, so an already-dismantled entry is not released a
    // second time (refcounts, quota charges, KV removal events).
    entry->WithExclusiveAccess(
        [&](ObjectMetadata& metadata, ObjectEntry::State& state) -> void {
            // Identity gate before any side effect: once the route publishes a
            // replacement, this erase is not that publication's business.
            if (state.is_torn_down || tenant->Get(entry->key()) != entry) {
                return;
            }
            (void)EraseMetadata(*tenant, entry, metadata, state, tenant_id,
                                quota_mode);
        });
}

void MasterService::FinalizeExpiredProcessingReplicasAfterDurable(
    std::shared_ptr<ObjectEntry> entry, const OpLogEntry& durable_entry,
    const std::vector<ReplicaID>& processing_replica_ids,
    const std::chrono::system_clock::time_point& ttl) {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    const TenantId tenant_id(durable_entry.tenant_id);
    const auto tenant = tenants_.Lookup(tenant_id);
    if (tenant == nullptr || entry == nullptr) {
        return;
    }
    // The plain published-object access, so a replacement that landed while
    // this cleanup was in flight is not even swept for invalid replicas.
    (void)tenant->WithPublishedObject(
        durable_entry.object_key,
        [&](ObjectMetadata& metadata, ObjectEntry::State& state) {
            // Identity gate: the durable cleanup was requested for the
            // publication that is still routed, so a same-key recreate keeps
            // its own replicas, quota and task state.
            const auto published = tenant->Get(durable_entry.object_key);
            if (published != entry) {
                return;
            }
            // Only the replicas this cleanup was armed for, and only while they
            // are still processing: UpsertStart preempts one round on this same
            // entry and starts the next one, whose replicas carry other ids and
            // own their data.
            const std::unordered_set<ReplicaID> armed_ids(
                processing_replica_ids.begin(), processing_replica_ids.end());
            // Snapshot before popping: the pop is what makes a medium
            // disappear, and the publisher needs the pre-pop set for the
            // removal delta, as in FinalizeExpiredReplicationTaskAfterDurable.
            const auto previous_media = KvRemovalSnapshot(metadata);
            auto replicas = PopReplicasWithCacheTotalAccounting(
                metadata, [&armed_ids](const Replica& replica) {
                    return replica.is_processing() &&
                           armed_ids.contains(replica.id());
                });
            if (!replicas.empty()) {
                FreeDfsReplicas(metadata.user_key, replicas);
                std::lock_guard lock(discarded_replicas_mutex_);
                discarded_replicas_.emplace_back(std::move(replicas), ttl);
            }
            // A round still in flight owns the object now: its own end settles
            // its quota, publishes its state and clears the flag, and this
            // cleanup is not that round's business.
            if (metadata.HasReplica(&Replica::fn_is_processing)) {
                return;
            }
            if (!metadata.IsValid()) {
                (void)EraseMetadata(*tenant, published, metadata, state,
                                    tenant_id, QuotaEraseMode::kFull,
                                    previous_media);
            } else {
                SyncKvObjectState(durable_entry.object_key, metadata, tenant_id,
                                  previous_media);
                auto settle_result =
                    SettlePrimaryWriteQuotaIfReady(*tenant, metadata);
                if (settle_result && state.is_processing) {
                    state.is_processing = false;
                }
            }
        });
}

void MasterService::FinalizeExpiredReplicationTaskAfterDurable(
    std::shared_ptr<ObjectEntry> entry, const OpLogEntry& durable_entry,
    ReplicaID source_id, const std::vector<ReplicaID>& target_ids,
    const UUID& dynamic_replication_lease_id,
    uint64_t dynamic_replication_version_epoch,
    const std::chrono::system_clock::time_point& ttl) {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    const TenantId tenant_id(durable_entry.tenant_id);
    const auto tenant = tenants_.Lookup(tenant_id);
    if (tenant == nullptr || entry == nullptr) {
        return;
    }
    // The plain published-object access, so a replacement that landed while
    // this cleanup was in flight is not even swept for invalid replicas.
    (void)tenant->WithPublishedObject(
        durable_entry.object_key,
        [&](ObjectMetadata& metadata, ObjectEntry::State& state) {
            // Identity gate before any side effect: the cleanup belongs to the
            // publication that armed it.
            const auto published = tenant->Get(durable_entry.object_key);
            if (published != entry) {
                return;
            }
            // The task records the ids and the lease it was created with, so
            // the routed publication proves this cleanup belongs to it.
            if (!state.replication_task.has_value()) {
                return;
            }
            const auto& task = *state.replication_task;
            if (!task.durable_cleanup_pending || task.source_id != source_id ||
                task.replica_ids != target_ids ||
                task.dynamic_replication_lease_id !=
                    dynamic_replication_lease_id ||
                task.dynamic_replication_version_epoch !=
                    dynamic_replication_version_epoch) {
                return;
            }

            if (auto source = metadata.GetReplicaByID(source_id);
                source != nullptr) {
                source->dec_refcnt();
            }

            // Snapshot the media before popping: targets match by id regardless
            // of status, so a COMPLETE replica dropped here would otherwise be
            // unrecoverable for the remove delta.
            const auto previous_media = KvRemovalSnapshot(metadata);
            std::unordered_set<ReplicaID> ids(target_ids.begin(),
                                              target_ids.end());
            auto replicas = PopReplicasWithCacheTotalAccounting(
                metadata, [&ids](const Replica& replica) {
                    return ids.contains(replica.id());
                });
            std::vector<ReplicaID> erased_replica_ids;
            erased_replica_ids.reserve(replicas.size());
            for (const auto& replica : replicas) {
                erased_replica_ids.push_back(replica.id());
            }
            const bool dynamic_task = dynamic_replication_lease_id != UUID{} ||
                                      dynamic_replication_version_epoch != 0;
            RecordDynamicReplicaRemoval(metadata, erased_replica_ids);
            if (!replicas.empty()) {
                FreeDfsReplicas(metadata.user_key, replicas);
                std::lock_guard lock(discarded_replicas_mutex_);
                discarded_replicas_.emplace_back(std::move(replicas), ttl);
            }
            if (dynamic_task) {
                ClearDynamicReplicationStateLocked(tenant_id, published, state);
            }
            if (!metadata.IsValid()) {
                (void)EraseMetadata(*tenant, published, metadata, state,
                                    tenant_id, QuotaEraseMode::kFull,
                                    previous_media);
            } else {
                // Publish before the task is dropped: dropping it ends this
                // cleanup.
                SyncKvObjectState(durable_entry.object_key, metadata, tenant_id,
                                  previous_media);
                if (state.replication_task.has_value()) {
                    ReleaseTenantQuota(
                        GetBoundTenantQuotaHandle(*tenant),
                        state.replication_task->pending_quota_charge_bytes);
                    state.replication_task.reset();
                }
            }
        });
}

MasterService::StaleHandleCleanupPlan
MasterService::BuildStaleHandleCleanupPlan(
    const ObjectMetadata& metadata,
    const std::unordered_set<UUID, boost::hash<UUID>>& retaining_clients)
    const {
    return BuildStaleHandleCleanupPlan(
        metadata, [&retaining_clients](const Replica& replica) {
            return (replica.has_invalid_mem_handle() ||
                    replica.has_invalid_nof_handle() ||
                    replica.has_stale_local_disk_client(retaining_clients)) &&
                   replica.is_completed();
        });
}

MasterService::StaleHandleCleanupPlan
MasterService::BuildStaleHandleCleanupPlan(
    const ObjectMetadata& metadata,
    const std::function<bool(const Replica&)>& is_stale) const {
    StaleHandleCleanupPlan plan;
    bool has_valid_after_cleanup = false;
    for (const auto& replica : metadata.GetAllReplicas()) {
        if (is_stale(replica)) {
            plan.removed_ids.push_back(replica.id());
            continue;
        }
        if (replica.status() == ReplicaStatus::COMPLETE) {
            plan.remaining.push_back(replica.get_descriptor());
        }
        if (!replica.is_memory_replica() || !replica.has_invalid_mem_handle()) {
            has_valid_after_cleanup = true;
        }
    }
    plan.would_invalidate = metadata.size == 0 || !has_valid_after_cleanup;
    return plan;
}

tl::expected<void, ErrorCode> MasterService::PersistStaleHandleCleanupForHA(
    const std::string& why, const TenantId& tenant_id,
    const std::shared_ptr<ObjectEntry>& entry, const std::string& key,
    ObjectMetadata& metadata, const StaleHandleCleanupPlan& plan) {
    if (plan.removed_ids.empty() || !enable_oplog_) {
        return {};
    }

    const auto op_type =
        plan.would_invalidate ? OpType::REMOVE : OpType::PUT_END;
    const std::string payload =
        plan.would_invalidate ? std::string{}
                              : SerializeMetadataForOpLogFromReplicaDescriptors(
                                    metadata, plan.remaining);

    auto reservation = ReserveBatchOpLogSlot();
    if (!reservation) {
        return tl::make_unexpected(reservation.error());
    }
    const std::unordered_set<ReplicaID> ids(plan.removed_ids.begin(),
                                            plan.removed_ids.end());
    metadata.VisitReplicas(
        [&ids](const Replica& replica) { return ids.contains(replica.id()); },
        [](Replica& replica) { replica.mark_removed(); });
    auto result = AppendReservedOpLogWithDurableFinalize(
        std::move(reservation.value()), op_type, tenant_id.value(), key,
        payload,
        [this, entry,
         removed_ids = plan.removed_ids](const OpLogEntry& durable_entry) {
            FinalizeRemovedReplicasAfterDurable(
                entry, durable_entry, removed_ids, QuotaEraseMode::kFull);
        });
    if (!result) {
        metadata.VisitReplicas(
            [&ids](const Replica& replica) {
                return ids.contains(replica.id());
            },
            [](Replica& replica) { replica.cancel_remove(); });
        LOG(WARNING) << why
                     << ": stale cleanup OpLog queue failed for key=" << key
                     << ", err=" << static_cast<int>(result.error());
        return tl::make_unexpected(result.error());
    }
    return {};
}

// Drops the object and every record keyed to it, so callers must not release
// any of them first. The caller holds the entry's own lock. The teardown runs
// at most once: `state.is_torn_down` claims it, and a second eraser of the same
// entry returns false instead of releasing everything twice.
bool MasterService::EraseMetadata(
    metadata::Tenant& tenant, const std::shared_ptr<ObjectEntry>& entry,
    ObjectMetadata& metadata, ObjectEntry::State& state,
    const TenantId& tenant_id, QuotaEraseMode quota_mode,
    const std::vector<std::string>& previous_media_hint) {
    if (state.is_torn_down) {
        return false;
    }
    state.is_torn_down = true;

    const std::string key = entry->key();
    const auto account = GetBoundTenantQuotaHandle(tenant);

    // Preserve the publisher's state while a size-changing upsert temporarily
    // removes the old metadata. A complete erase emits one remove event per
    // media that was previously visible.
    if (quota_mode == QuotaEraseMode::kPreserveOld) {
        // Union rather than replace: the caller hint may only describe one
        // tier, so a medium still visible in metadata must not be dropped
        // from the remove delta.
        auto previous_media = KvRemovalSnapshot(metadata);
        previous_media.insert(previous_media.end(), previous_media_hint.begin(),
                              previous_media_hint.end());
        SyncKvObjectState(key, metadata, tenant_id, previous_media);
    } else {
        PublishKvRemoved(key, metadata, tenant_id, previous_media_hint);
    }

    // Drop the offload task and its source refcount before the object goes: a
    // Store Worker may still have an offload in flight for this key, and the
    // task would otherwise sit as an orphan until it expires.
    if (state.offloading_task.has_value()) {
        auto source = metadata.GetReplicaByID(state.offloading_task->source_id);
        if (source != nullptr) {
            source->dec_refcnt();
        }
        state.offloading_task.reset();

        // The mailbox entry must be dropped too, otherwise the next
        // OffloadObjectHeartbeat drains a task-less key back to the client and
        // produces an orphan bucket.
        local_ssd_manager_.RemoveOffloadFromAll(tenant_id, key);
    }
    state.is_processing = false;
    if (state.replication_task.has_value()) {
        auto source =
            metadata.GetReplicaByID(state.replication_task->source_id);
        if (source != nullptr) {
            source->dec_refcnt();
        }
        ReleaseTenantQuota(account,
                           state.replication_task->pending_quota_charge_bytes);
        state.replication_task.reset();
    }
    ErasePromotionTaskLocked(tenant, state);
    // The global candidate count has to be settled here too: an erased entry
    // can no longer reach the retry loop's expiry path, so its candidate would
    // hold a slot toward the admission limit forever.
    EraseCandidateLocked(tenant_id, entry, state);
    ClearDynamicReplicationStateLocked(tenant_id, entry, state);

    ReleaseLocalDiskUsage(metadata.GetAllReplicas());
    FreeDfsReplicas(key, metadata.GetAllReplicas());
    AccountCacheTotalRemoval(metadata);
    if (metadata.GetCommittedSoftPinTimeout()) {
        soft_pin_deadline_index_.Remove(tenant_id.MakeScopedKey(key));
    }
    switch (quota_mode) {
        case QuotaEraseMode::kFull:
            if (enable_multi_tenants_ &&
                metadata.quota_ledger.TotalChargedBytes() != 0) {
                auto release_result = metadata.quota_ledger.ReleaseAll(account);
                LogTenantQuotaLedgerError(release_result, "release_all",
                                          tenant_id, key);
            }
            break;
        case QuotaEraseMode::kPreserveOld:
        case QuotaEraseMode::kAbortOnly:
            if (enable_multi_tenants_ &&
                metadata.quota_ledger.PendingBytes() != 0) {
                auto refund_result =
                    metadata.quota_ledger.RefundPending(account);
                LogTenantQuotaLedgerError(refund_result, "refund_pending",
                                          tenant_id, key);
            }
            if (enable_multi_tenants_ &&
                metadata.quota_ledger.TotalChargedBytes() != 0) {
                LOG(ERROR)
                    << "tenant quota ledger still owns bytes during preserved "
                       "metadata erase tenant="
                    << tenant_id.value() << ", key=" << key
                    << ", bytes=" << metadata.quota_ledger.TotalChargedBytes();
            }
            break;
    }
    // The route slot and the group membership go together, and only while the
    // slot still publishes this entry: once it is released, a newer publication
    // of the same key may register the membership this teardown would drop.
    (void)tenant.RemoveObject(entry);
    return true;
}

void MasterService::CleanupInvalidMemoryReplicas(
    const TenantId& tenant_id, metadata::Tenant& tenant,
    const std::shared_ptr<ObjectEntry>& entry, ObjectMetadata& metadata,
    ObjectEntry::State& state) {
    // With HA and the oplog on, an invalid replica is dropped through the
    // oplog, so it is not swept here.
    if (enable_ha_ && enable_oplog_) {
        return;
    }
    // Gate the snapshot on the publisher being live: this runs on every
    // read-write access, and the VisitReplicas walk plus vector allocation is
    // pure overhead when KV events are off (the default).
    const auto previous_kv_media = KvMediaSnapshot(metadata);
    // Erase invalid memory replicas (those with unmounted segments). No
    // client_mutex_ needed, since only memory replicas are checked.
    const uint64_t before_charge = CompletedMemoryQuotaCharge(metadata);
    std::vector<ReplicaID> removed_replica_ids;
    EraseReplicasWithCacheTotalAccounting(
        metadata,
        [](const Replica& replica) { return replica.has_invalid_mem_handle(); },
        &removed_replica_ids);
    CancelPromotionTaskForRemovedReplicas(tenant, metadata, state,
                                          removed_replica_ids);
    const uint64_t after_charge = CompletedMemoryQuotaCharge(metadata);
    if (enable_multi_tenants_ && before_charge > after_charge) {
        auto release_result = metadata.quota_ledger.ReleaseCommitted(
            GetBoundTenantQuotaHandle(tenant), before_charge - after_charge);
        if (!release_result) {
            LOG(ERROR) << "tenant quota committed release mismatch tenant="
                       << tenant_id.value() << ", key=" << metadata.user_key
                       << ", bytes=" << before_charge - after_charge;
        }
    }
    SyncKvObjectState(metadata.user_key, metadata, tenant_id,
                      previous_kv_media);
    // If no valid replica remains, the whole object goes.
    if (!metadata.IsValid()) {
        (void)EraseMetadata(tenant, entry, metadata, state, tenant_id);
    }
}

void MasterService::ReleaseLocalDiskUsage(
    const std::vector<Replica>& replicas) {
    std::unordered_map<UUID, int64_t, boost::hash<UUID>> bytes_by_client;
    for (const auto& replica : replicas) {
        if (!replica.is_local_disk_replica()) {
            continue;
        }
        const auto descriptor =
            replica.get_descriptor().get_local_disk_descriptor();
        if (descriptor.object_size > 0) {
            bytes_by_client[descriptor.client_id] += descriptor.object_size;
        }
    }
    if (bytes_by_client.empty()) {
        return;
    }

    for (const auto& [client_id, bytes] : bytes_by_client) {
        local_ssd_manager_.AdjustUsedBytes(client_id, -bytes);
    }
}

void MasterService::RebuildGroupState() {
    // Group membership and the shared group leases are derived from the
    // grouped objects each tenant publishes, so the rebuild is per tenant. The
    // maximum restored deadline per group keeps a grouped object off a
    // zero-deadline lease that post-restore cleanup would drop.
    tenants_.Visit(
        [](const TenantId&, const std::shared_ptr<metadata::Tenant>& tenant) {
            tenant->RebuildGroupState();
        });
}

void MasterService::SoftPinDeadlineIndex::MaybeCompactLocked() {
    const size_t live_count = registrations_.size();
    const size_t ratio_limit =
        live_count > std::numeric_limits<size_t>::max() / kCompactionRatio
            ? std::numeric_limits<size_t>::max()
            : live_count * kCompactionRatio;
    const size_t threshold = std::max(kMinCompactionThreshold, ratio_limit);
    if (heap_.size() <= threshold) {
        return;
    }

    decltype(heap_) rebuilt;
    for (const auto& [scoped_key, registration] : registrations_) {
        rebuilt.push(Entry{registration.deadline, scoped_key});
    }
    heap_.swap(rebuilt);
}

void MasterService::SoftPinDeadlineIndex::Upsert(std::string scoped_key,
                                                 const TimePoint& deadline) {
    std::lock_guard lock(mutex_);
    const auto it = registrations_.find(scoped_key);
    if (it != registrations_.end() && it->second.deadline == deadline) {
        return;
    }

    auto [registration_it, inserted] =
        registrations_.insert_or_assign(scoped_key, Registration{deadline});
    (void)inserted;
    heap_.push(Entry{deadline, registration_it->first});
    MaybeCompactLocked();
}

void MasterService::SoftPinDeadlineIndex::Remove(
    const std::string& scoped_key) {
    std::lock_guard lock(mutex_);
    if (registrations_.erase(scoped_key) > 0) {
        MaybeCompactLocked();
    }
}

void MasterService::SoftPinDeadlineIndex::RemoveIfMatches(
    const std::string& scoped_key, const TimePoint& deadline) {
    std::lock_guard lock(mutex_);
    const auto it = registrations_.find(scoped_key);
    if (it != registrations_.end() && it->second.deadline == deadline) {
        registrations_.erase(it);
        MaybeCompactLocked();
    }
}

std::vector<MasterService::SoftPinDeadlineIndex::Entry>
MasterService::SoftPinDeadlineIndex::PopExpired(const TimePoint& now) {
    std::vector<Entry> expired;
    std::lock_guard lock(mutex_);
    while (!heap_.empty() && heap_.top().deadline <= now) {
        Entry entry = heap_.top();
        heap_.pop();

        const auto it = registrations_.find(entry.scoped_key);
        if (it == registrations_.end() ||
            it->second.deadline != entry.deadline) {
            continue;
        }
        registrations_.erase(it);
        expired.push_back(std::move(entry));
    }
    MaybeCompactLocked();
    return expired;
}

void MasterService::SoftPinDeadlineIndex::Clear() {
    std::lock_guard lock(mutex_);
    registrations_.clear();
    decltype(heap_) empty;
    heap_.swap(empty);
}

auto MasterService::ResolveSoftPinRequest(const ReplicateConfig& config) const
    -> tl::expected<ResolvedSoftPinRequest, ErrorCode> {
    switch (config.soft_pin_action) {
        case SoftPinAction::PRESERVE:
        case SoftPinAction::DISABLE:
            if (config.soft_pin_ttl_ms.has_value()) {
                LOG(ERROR) << "soft_pin_action=" << config.soft_pin_action
                           << ", soft_pin_ttl_ms=" << *config.soft_pin_ttl_ms
                           << ", error=ttl_requires_enable";
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            return ResolvedSoftPinRequest{config.soft_pin_action,
                                          std::chrono::milliseconds::zero()};
        case SoftPinAction::ENABLE: {
            const uint64_t ttl_ms =
                config.soft_pin_ttl_ms.value_or(default_kv_soft_pin_ttl_);
            if (ttl_ms > max_kv_soft_pin_ttl_) {
                LOG(ERROR) << "soft_pin_ttl_ms=" << ttl_ms
                           << ", max_kv_soft_pin_ttl=" << max_kv_soft_pin_ttl_
                           << ", error=soft_pin_ttl_exceeds_limit";
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            return ResolvedSoftPinRequest{config.soft_pin_action,
                                          std::chrono::milliseconds(ttl_ms)};
        }
    }
    LOG(ERROR) << "soft_pin_action="
               << static_cast<uint32_t>(config.soft_pin_action)
               << ", error=invalid_soft_pin_action";
    return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
}

void MasterService::ApplySoftPinMetricDelta(int metric_delta) {
    if (metric_delta > 0) {
        MasterMetricManager::instance().inc_soft_pin_key_count(metric_delta);
    } else if (metric_delta < 0) {
        MasterMetricManager::instance().dec_soft_pin_key_count(-metric_delta);
    }
}

void MasterService::ApplySoftPinEvaluation(
    const ObjectMetadata& metadata,
    const ObjectMetadata::SoftPinEvaluation& result) const {
    if (!result.deadline_to_index && !result.removed_deadline) {
        ApplySoftPinMetricDelta(result.metric_delta);
        return;
    }

    const auto scoped_key = metadata.tenant_id.MakeScopedKey(metadata.user_key);
    if (result.deadline_to_index) {
        soft_pin_deadline_index_.Upsert(scoped_key, *result.deadline_to_index);
    } else if (result.removed_deadline) {
        soft_pin_deadline_index_.RemoveIfMatches(scoped_key,
                                                 *result.removed_deadline);
    }
    ApplySoftPinMetricDelta(result.metric_delta);
}

bool MasterService::IsSoftPinActive(
    const ObjectMetadata& metadata,
    const std::chrono::system_clock::time_point& now) const {
    const auto evaluation = metadata.EvaluateSoftPin(now);
    ApplySoftPinEvaluation(metadata, evaluation);
    return evaluation.active;
}

void MasterService::CleanupExpiredSoftPins(
    const std::chrono::system_clock::time_point& now) {
    auto expired_entries = soft_pin_deadline_index_.PopExpired(now);

    // Each deadline is resolved on its own through the route of the tenant the
    // scoped key names: the key may have been republished since, and only the
    // deadline the index still holds may expire a soft pin.
    int expired_count = 0;
    for (const auto& expired : expired_entries) {
        const auto [tenant_id, key] =
            TenantId::ParseScopedKey(expired.scoped_key);
        const ObjectIdentity object_id{tenant_id, key};
        const auto expired_now = WithObjectMetadataForWriteAndCleanup(
            object_id,
            [&expired, &now](
                metadata::Tenant&, const std::shared_ptr<ObjectEntry>&,
                ObjectMetadata& metadata, ObjectEntry::State&) -> bool {
                return metadata.ExpireSoftPinIfDeadlineMatches(expired.deadline,
                                                               now);
            });
        if (expired_now.value_or(false)) {
            ++expired_count;
        }
    }
    if (expired_count > 0) {
        MasterMetricManager::instance().dec_soft_pin_key_count(expired_count);
    }
}

void MasterService::ClearInvalidHandles() {
    std::shared_lock<std::shared_mutex> client_lock(client_mutex_);
    std::shared_lock<std::shared_mutex> snapshot_lock(snapshot_mutex_);
    auto retaining_clients = GetRetainingClientIdsLocked();
    client_lock.unlock();
    ClearInvalidHandles(retaining_clients);
}

void MasterService::ClearInvalidHandles(
    const std::unordered_set<UUID, boost::hash<UUID>>& retaining_clients) {
    (void)ClearStaleHandles([&retaining_clients](const Replica& replica) {
        return (replica.has_invalid_mem_handle() ||
                replica.has_invalid_nof_handle() ||
                replica.has_stale_local_disk_client(retaining_clients)) &&
               replica.is_completed();
    });
}

void MasterService::ClearLocalDiskHandlesOwnedBy(const UUID& owner) {
    (void)ClearStaleHandles([&owner](const Replica& replica) {
        return replica.is_local_disk_replica() && replica.is_completed() &&
               replica.get_local_disk_client_id() == owner;
    });
}

tl::expected<void, ErrorCode> MasterService::ClearStaleHandles(
    const std::function<bool(const Replica&)>& is_stale) {
    std::optional<ErrorCode> first_persist_error;
    tenants_.Visit([&](const TenantId& tenant_id,
                       const std::shared_ptr<metadata::Tenant>& tenant) {
        // The snapshot hands out handles to what the route published, so every
        // key below is resolved through the route again: an entry can be
        // replaced under the same key between the snapshot and the access.
        for (const auto& snapshot_entry : tenant->SnapshotObjects()) {
            const std::string key = snapshot_entry->key();
            const ObjectIdentity object_id{tenant_id, key};
            // Reader pre-check: an object with nothing to sweep skips the write
            // path and the invalid-replica cleanup it starts with. The object
            // is re-classified under the write access below, since it may have
            // changed.
            const auto needs_work = WithObjectMetadataForRead(
                object_id,
                [&is_stale](const metadata::Tenant&,
                            const std::shared_ptr<ObjectEntry>&,
                            const ObjectMetadata& metadata,
                            const ObjectEntry::State&) -> bool {
                    return metadata.HasReplica(is_stale) || !metadata.IsValid();
                });
            if (!needs_work.value_or(false)) {
                continue;
            }
            // The plain published-object access, not the read-write one: this
            // walk classifies and tears down itself, and the read-write
            // accessor would drop the stale replicas before the callback could
            // see what it erased.
            (void)tenant->WithPublishedObject(
                key, [&](ObjectMetadata& metadata, ObjectEntry::State& state) {
                    const std::shared_ptr<ObjectEntry> entry = tenant->Get(key);
                    if (entry == nullptr) {
                        return;
                    }
                    const auto cleanup_plan =
                        BuildStaleHandleCleanupPlan(metadata, is_stale);
                    if (!cleanup_plan.removed_ids.empty()) {
                        if (enable_ha_ && enable_oplog_) {
                            // The erase runs from the durable-finalize
                            // callback.
                            auto persist_result =
                                PersistStaleHandleCleanupForHA(
                                    "ClearStaleHandles", tenant_id, entry, key,
                                    metadata, cleanup_plan);
                            if (!persist_result && !first_persist_error) {
                                first_persist_error = persist_result.error();
                            }
                            return;
                        }
                        if (CleanupStaleHandles(key, tenant_id, *tenant,
                                                metadata, state, is_stale)) {
                            (void)EraseMetadata(*tenant, entry, metadata, state,
                                                tenant_id,
                                                QuotaEraseMode::kFull);
                        }
                        return;
                    }
                    if (metadata.IsValid()) {
                        return;
                    }
                    if (enable_ha_) {
                        if (enable_oplog_) {
                            auto persist_result =
                                AppendOpLogWithDurableFinalize(
                                    OpType::REMOVE, tenant_id.value(), key, {},
                                    [this, entry, tenant_id](
                                        const OpLogEntry& durable_entry) {
                                        FinalizeMetadataEraseAfterDurable(
                                            entry, tenant_id,
                                            QuotaEraseMode::kFull);
                                    });
                            if (!persist_result) {
                                LOG(WARNING)
                                    << "ClearStaleHandles(last replica)"
                                    << ": REMOVE persist failed for key=" << key
                                    << ", err="
                                    << static_cast<int>(persist_result.error());
                                if (!first_persist_error) {
                                    first_persist_error =
                                        persist_result.error();
                                }
                            }
                        }
                        return;
                    }
                    (void)EraseMetadata(*tenant, entry, metadata, state,
                                        tenant_id, QuotaEraseMode::kFull);
                });
        }
    });

    if (first_persist_error) {
        return tl::make_unexpected(*first_persist_error);
    }
    return {};
}

bool MasterService::ProcessClientOffboardingJob(ClientOffboardingJob& job) {
    bool quota_recompute_needed = false;
    {
        std::shared_lock<std::shared_mutex> snapshot_lock(snapshot_mutex_);

        if (!job.pending_prepare_segments.empty()) {
            ScopedSegmentAccess segment_access =
                segment_manager_.getSegmentAccess();
            for (auto it = job.pending_prepare_segments.begin();
                 it != job.pending_prepare_segments.end();) {
                SegmentStatus status = SegmentStatus::UNDEFINED;
                const auto status_err =
                    segment_access.GetSegmentStatusById(it->segment_id, status);
                if (status_err == ErrorCode::SEGMENT_NOT_FOUND) {
                    it = job.pending_prepare_segments.erase(it);
                    continue;
                }
                if (status_err == ErrorCode::OK &&
                    status == SegmentStatus::UNMOUNTING) {
                    ++it;
                    continue;
                }

                size_t metrics_dec_capacity = 0;
                const auto err = segment_access.PrepareUnmountSegment(
                    it->segment_id, metrics_dec_capacity);
                if (err == ErrorCode::OK) {
                    job.prepared_segments.push_back(
                        {.segment_id = it->segment_id,
                         .segment_name = it->segment_name,
                         .transport_endpoint = it->transport_endpoint,
                         .metrics_dec_capacity = metrics_dec_capacity});
                    it = job.pending_prepare_segments.erase(it);
                    continue;
                }
                if (err == ErrorCode::SEGMENT_NOT_FOUND) {
                    it = job.pending_prepare_segments.erase(it);
                    continue;
                }
                LOG(ERROR) << "client_id=" << job.client_id
                           << ", segment_name=" << it->segment_name
                           << ", action=prepare_client_offboarding"
                           << ", error=" << toString(err);
                ++it;
            }
            if (!job.pending_prepare_segments.empty()) {
                return false;
            }
        }

        if (!job.metadata_cleanup_accepted) {
            const auto cleanup_result =
                ClearStaleHandles([&job](const Replica& replica) {
                    if (!replica.is_completed()) {
                        return false;
                    }
                    if (replica.is_local_disk_replica()) {
                        return replica.get_local_disk_client_id() ==
                               job.client_id;
                    }
                    return replica.is_memory_replica() &&
                           replica.isAffiliatedWith(job.liveness);
                });
            if (!cleanup_result) {
                LOG(ERROR) << "client_id=" << job.client_id
                           << ", action=queue_client_offboarding_metadata"
                           << ", error=" << toString(cleanup_result.error());
                return false;
            }
            bool unfinished_affiliated_replica = false;
            tenants_.Visit(
                [&](const TenantId& tenant_id,
                    const std::shared_ptr<metadata::Tenant>& tenant) {
                    if (unfinished_affiliated_replica) {
                        return;
                    }
                    // Resolved through the route again: the snapshot holds
                    // handles, and the key may have been republished since.
                    for (const auto& snapshot_entry :
                         tenant->SnapshotObjects()) {
                        const ObjectIdentity object_id{tenant_id,
                                                       snapshot_entry->key()};
                        const auto affiliated = WithObjectMetadataForRead(
                            object_id,
                            [&job](const metadata::Tenant&,
                                   const std::shared_ptr<ObjectEntry>&,
                                   const ObjectMetadata& metadata,
                                   const ObjectEntry::State&) -> bool {
                                return metadata.HasReplica(
                                    [&job](const Replica& replica) {
                                        return replica.is_processing() &&
                                               replica.isAffiliatedWith(
                                                   job.liveness);
                                    });
                            });
                        if (affiliated.value_or(false)) {
                            unfinished_affiliated_replica = true;
                            break;
                        }
                    }
                });
            if (unfinished_affiliated_replica) {
                LOG(ERROR)
                    << "client_id=" << job.client_id
                    << ", action=wait_client_offboarding_processing_replica";
                return false;
            }
            job.metadata_cleanup_accepted = true;
        }

        if (!job.local_ssd_unregistered) {
            auto capacity = local_ssd_manager_.UnregisterClient(job.client_id);
            if (capacity && *capacity > 0) {
                MasterMetricManager::instance().dec_total_file_capacity(
                    *capacity);
                quota_recompute_needed = true;
            }
            job.local_ssd_unregistered = true;
        }

        for (auto it = job.prepared_segments.begin();
             it != job.prepared_segments.end();) {
            ScopedSegmentAccess segment_access =
                segment_manager_.getSegmentAccess();
            const auto commit_result = segment_access.CommitUnmountSegment(
                it->segment_id, job.client_id, it->metrics_dec_capacity,
                /*retain_name_registration=*/true);
            if (commit_result != ErrorCode::OK &&
                commit_result != ErrorCode::SEGMENT_NOT_FOUND) {
                LOG(ERROR) << "client_id=" << job.client_id
                           << ", segment_name=" << it->segment_name
                           << ", action=commit_client_offboarding"
                           << ", error=" << toString(commit_result);
                ++it;
                continue;
            }

            if (enable_oplog_ && ordered_oplog_writer_ &&
                !it->transport_endpoint.empty()) {
                SegmentUnmountOp op{it->transport_endpoint};
                auto bytes = struct_pack::serialize(op);
                auto persist_result = AppendOpLogVisibleBeforeDurable(
                    OpType::SEGMENT_UNMOUNT, TenantId::Default().value(),
                    it->transport_endpoint,
                    std::string(bytes.begin(), bytes.end()));
                if (!persist_result) {
                    LOG(ERROR)
                        << "client_id=" << job.client_id
                        << ", segment_name=" << it->segment_name
                        << ", action=queue_client_offboarding_unmount"
                        << ", error=" << toString(persist_result.error());
                    ++it;
                    continue;
                }
            }

            cleanupHttpMetadata(it->segment_name);
            segment_access.ReleaseUnmountedSegmentName(it->segment_id,
                                                       it->segment_name);
            LOG(INFO) << "client_id=" << job.client_id
                      << ", segment_name=" << it->segment_name
                      << ", action=unmount_offline_mem_segment";
            quota_recompute_needed = true;
            it = job.prepared_segments.erase(it);
        }
    }

    if (quota_recompute_needed) {
        RecomputeTenantEffectiveQuotas();
    }
    if (!job.prepared_segments.empty()) {
        return false;
    }

    std::unique_lock<std::shared_mutex> client_lock(client_mutex_);
    const auto current = client_liveness_records_.find(job.client_id);
    if (current != client_liveness_records_.end() &&
        current->second == job.liveness) {
        const auto state = current->second->state();
        client_liveness_records_.erase(current);
        MasterMetricManager::instance().on_client_liveness_record_removed(
            state);
    }
    return true;
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
        {
            auto write_access = task_manager_.get_write_access();
            write_access.prune_expired_tasks();
            write_access.prune_finished_tasks();
        }
        CleanupExpiredSoftPins(std::chrono::system_clock::now());
        CleanupExpiredDynamicReplicationState();
    }
    LOG(INFO) << "Task cleanup thread stopped";
}

auto MasterService::UnmountSegment(const UUID& segment_id,
                                   const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    size_t metrics_dec_capacity = 0;  // to update the metrics

    std::shared_lock<std::shared_mutex> client_lock(client_mutex_);
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    auto retaining_clients = GetRetainingClientIdsLocked();
    client_lock.unlock();
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
    }

    // Keep HA, snapshot, and CXL behavior unchanged. Regular memory segments
    // become unreadable as soon as PrepareUnmountSegment releases their
    // allocator; only the physical metadata sweep is deferred.
    if (enable_async_segment_cleanup_) {
        replica_cleanup_worker_.Schedule();
    } else {
        ClearInvalidHandles(retaining_clients);
    }

    // Cache endpoint before commit removes segment from registry.
    std::string segment_name;
    std::string te_endpoint;
    if (!segment_manager_.GetSegmentBasicInfo(segment_id, segment_name,
                                              te_endpoint)) {
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }

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

    if (enable_oplog_ && ordered_oplog_writer_ && !te_endpoint.empty()) {
        SegmentUnmountOp op{te_endpoint};
        auto bytes = struct_pack::serialize(op);
        PersistSegmentOpForHAOrEnqueue("UnmountSegment",
                                       OpType::SEGMENT_UNMOUNT, te_endpoint,
                                       std::string(bytes.begin(), bytes.end()));
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

    std::shared_lock<std::shared_mutex> client_lock(client_mutex_);
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    auto retaining_clients = GetRetainingClientIdsLocked();
    client_lock.unlock();

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
    ClearInvalidHandles(retaining_clients);

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

auto MasterService::ExistKey(const std::string& key, const TenantId& tenant_id)
    -> tl::expected<bool, ErrorCode> {
    return ExistKeyImpl(key, tenant_id, /*grant_lease=*/true);
}

auto MasterService::ProbeKey(const std::string& key, const TenantId& tenant_id)
    -> tl::expected<bool, ErrorCode> {
    return ExistKeyImpl(key, tenant_id, /*grant_lease=*/false);
}

auto MasterService::ExistKeyImpl(const std::string& key,
                                 const TenantId& tenant_id, bool grant_lease)
    -> tl::expected<bool, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    // A publication that carries no valid metadata answers as an absent key,
    // since the read accessor only reports a live publication.
    bool metadata_invalid = false;
    const auto published = WithObjectMetadataForRead(
        MakeObjectIdentityForRequest(key, tenant_id),
        [&](const metadata::Tenant&, const std::shared_ptr<ObjectEntry>&,
            const ObjectMetadata& metadata, const ObjectEntry::State&) -> bool {
            if (!metadata.IsValid()) {
                metadata_invalid = true;
                return false;
            }
            if (!HasReadableReplica(metadata)) {
                return false;
            }
            if (grant_lease) {
                // Grant a lease to the object as it may be further used by the
                // client. Read path is group-agnostic: only the object's own
                // lease is refreshed.
                metadata.GrantReadLease(
                    std::chrono::milliseconds(default_kv_lease_ttl_));
            }
            return true;
        });
    if (!published.has_value() || metadata_invalid) {
        VLOG(1) << "key=" << key << ", info=object_not_found";
        return false;
    }
    return *published;
}

std::vector<tl::expected<bool, ErrorCode>> MasterService::BatchExistKey(
    const std::vector<std::string>& keys, const TenantId& tenant_id) {
    return BatchExistKeyImpl(keys, tenant_id, /*grant_lease=*/true);
}

std::vector<tl::expected<bool, ErrorCode>> MasterService::BatchProbeKey(
    const std::vector<std::string>& keys, const TenantId& tenant_id) {
    return BatchExistKeyImpl(keys, tenant_id, /*grant_lease=*/false);
}

std::vector<tl::expected<bool, ErrorCode>> MasterService::BatchExistKeyImpl(
    const std::vector<std::string>& keys, const TenantId& tenant_id,
    bool grant_lease) {
    const TenantId& normalized_tenant = ResolveRequestTenantId(tenant_id);
    std::vector<tl::expected<bool, ErrorCode>> results(keys.size());
    if (keys.empty()) {
        return results;
    }

    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    for (size_t i = 0; i < keys.size(); ++i) {
        const std::string& key = keys[i];
        bool metadata_invalid = false;
        const auto exists = WithObjectMetadataForRead(
            normalized_tenant, key,
            [&](const metadata::Tenant&, const std::shared_ptr<ObjectEntry>&,
                const ObjectMetadata& metadata,
                const ObjectEntry::State&) -> bool {
                if (!metadata.IsValid()) {
                    metadata_invalid = true;
                    return false;
                }
                if (!HasReadableReplica(metadata)) {
                    return false;
                }
                if (grant_lease) {
                    // Grant a lease to the object as it may be further used by
                    // the client. Read path is group-agnostic: only the
                    // object's own lease is refreshed.
                    metadata.GrantReadLease(
                        std::chrono::milliseconds(default_kv_lease_ttl_));
                }
                return true;
            });
        if (!exists.has_value() || metadata_invalid) {
            VLOG(1) << "key=" << key << ", tenant_id=" << normalized_tenant
                    << ", info=object_not_found";
        }
        results[i] = exists.value_or(false);
    }
    return results;
}

auto MasterService::GetAllKeys(const TenantId& tenant_id)
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    std::vector<std::string> all_keys;
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    const TenantId& normalized_tenant = ResolveRequestTenantId(tenant_id);
    const auto tenant = tenants_.Lookup(normalized_tenant);
    if (tenant == nullptr) {
        return all_keys;
    }
    // The snapshot hands out handles to what the route published, so each key
    // is resolved through the route again while its object is read.
    for (const auto& snapshot_entry : tenant->SnapshotObjects()) {
        const std::string& key = snapshot_entry->key();
        const ObjectIdentity object_id{normalized_tenant, key};
        (void)WithObjectMetadataForRead(
            object_id,
            [this, &all_keys, &key](const metadata::Tenant&,
                                    const std::shared_ptr<ObjectEntry>&,
                                    const ObjectMetadata& metadata,
                                    const ObjectEntry::State&) -> void {
                if (!HasReadableReplica(metadata)) {
                    return;
                }
                all_keys.push_back(
                    metadata.user_key.empty() ? key : metadata.user_key);
            });
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

tl::expected<void, ErrorCode> MasterService::RestoreFromStandbySnapshot(
    const std::vector<StandbyObjectEntry>& objects,
    uint64_t initial_oplog_sequence_id,
    const std::vector<StandbySegmentInfo>& segments,
    const WeightMetadataSnapshot& weight_metadata) {
    return RestoreFromStandbyState(&objects, nullptr, initial_oplog_sequence_id,
                                   segments, objects.size(), std::nullopt,
                                   &weight_metadata);
}

tl::expected<void, ErrorCode> MasterService::RestoreFromBatchOpLogPromotion(
    BatchOpLogPromotionHandoff handoff, size_t chunk_object_count) {
    if (!handoff.metadata_store || chunk_object_count == 0 ||
        ((handoff.applied_cursor.batch_id == 0) !=
         (handoff.applied_cursor.last_seq == 0))) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return RestoreFromStandbyState(nullptr, std::move(handoff.metadata_store),
                                   handoff.applied_cursor.last_seq,
                                   handoff.segments, chunk_object_count,
                                   handoff.max_replica_id, nullptr);
}

tl::expected<void, ErrorCode> MasterService::RestoreFromStandbyState(
    const std::vector<StandbyObjectEntry>* legacy_objects,
    std::unique_ptr<StandbyMetadataStore> metadata_store,
    uint64_t initial_oplog_sequence_id,
    const std::vector<StandbySegmentInfo>& segments, size_t chunk_object_count,
    std::optional<ReplicaID> expected_max_replica_id,
    const WeightMetadataSnapshot* legacy_weight_metadata) {
    if (enable_dfs_) {
        LOG(ERROR) << "RestoreFromStandbySnapshot: DFS allocator state "
                      "restoration is not supported";
        return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
    }
    if ((legacy_objects == nullptr) == (metadata_store == nullptr) ||
        (metadata_store && chunk_object_count == 0) ||
        (legacy_objects && legacy_weight_metadata == nullptr)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const WeightMetadataSnapshot weight_metadata =
        metadata_store ? metadata_store->SnapshotWeightMetadata()
                       : *legacy_weight_metadata;
    if (!ValidateWeightMetadataSnapshot(weight_metadata)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    // The ordered writer initializes its sequence from durable_prefix.
    std::unique_lock<std::shared_mutex> client_lock(client_mutex_);
    std::unique_lock<std::shared_mutex> snapshot_lock(snapshot_mutex_);
    std::unordered_map<UUID, std::shared_ptr<ClientLivenessRecord>,
                       boost::hash<UUID>>
        new_known_owner_records;
    const auto record_for_known_owner = [&](const UUID& owner) {
        const auto existing = client_liveness_records_.find(owner);
        if (existing != client_liveness_records_.end()) {
            return existing->second;
        }
        auto [record, inserted] = new_known_owner_records.try_emplace(
            owner, std::make_shared<ClientLivenessRecord>(
                       ClientLivenessRecord::Clock::now()));
        (void)inserted;
        return record->second;
    };

    const auto resolve_standby_object = [](const StandbyObjectEntry& entry) {
        auto [scoped_tenant_id, user_key] = TenantId::ParseScopedKey(entry.key);
        TenantId tenant_id(entry.tenant_id);
        if (tenant_id.IsDefault() && !scoped_tenant_id.IsDefault()) {
            tenant_id = std::move(scoped_tenant_id);
        }
        return std::make_pair(std::move(tenant_id), std::move(user_key));
    };

    ReplicaID max_live_id = 0;
    if (metadata_store) {
        if (!expected_max_replica_id ||
            !metadata_store->ValidateReplicaIds(max_live_id) ||
            max_live_id != *expected_max_replica_id) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
    } else {
        for (const auto& entry : *legacy_objects) {
            auto [tenant_id, user_key] = resolve_standby_object(entry);
            std::unordered_set<ReplicaID> object_replica_ids;
            for (const auto& desc : entry.metadata.replicas) {
                if (desc.id == 0 ||
                    desc.id == std::numeric_limits<ReplicaID>::max() ||
                    !object_replica_ids.insert(desc.id).second) {
                    LOG(ERROR)
                        << "RestoreFromStandbySnapshot: invalid or duplicate "
                        << "replica id=" << desc.id
                        << ", tenant=" << tenant_id.value()
                        << ", key=" << user_key;
                    return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                }
                max_live_id = std::max(max_live_id, desc.id);
            }
        }
    }

    std::vector<StandbySegmentInfo> restored_memory_segments;
    std::unordered_map<std::string, const StandbySegmentInfo*>
        memory_segments_by_alias;
    std::unordered_map<std::string, std::shared_ptr<BufferAllocatorBase>>
        restored_allocators;
    std::unordered_set<std::string> restored_invalid_endpoints;
    for (const auto& seg : segments) {
        if (!seg.is_memory_segment) {
            continue;
        }
        if (seg.segment_name.empty() || seg.transport_endpoint.empty() ||
            seg.capacity == 0) {
            LOG(ERROR) << "RestoreFromStandbySnapshot: invalid memory segment "
                       << "name=" << seg.segment_name
                       << ", endpoint=" << seg.transport_endpoint
                       << ", capacity=" << seg.capacity;
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        const auto add_alias = [&](const std::string& alias) {
            return memory_segments_by_alias.emplace(alias, &seg).second;
        };
        if (!add_alias(seg.transport_endpoint) ||
            (seg.segment_name != seg.transport_endpoint &&
             !add_alias(seg.segment_name))) {
            LOG(ERROR)
                << "RestoreFromStandbySnapshot: ambiguous memory segment "
                << "name=" << seg.segment_name
                << ", endpoint=" << seg.transport_endpoint;
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }

        restored_memory_segments.push_back(seg);
        auto allocator = std::make_shared<DummyBufferAllocator>(
            seg.segment_name, seg.transport_endpoint);
        restored_allocators[seg.transport_endpoint] = allocator;
        if (seg.segment_name != seg.transport_endpoint) {
            restored_allocators[seg.segment_name] = allocator;
        }
        if (!segment_manager_.HasSegmentByEndpoint(seg.transport_endpoint)) {
            restored_invalid_endpoints.insert(seg.transport_endpoint);
            if (seg.segment_name != seg.transport_endpoint) {
                restored_invalid_endpoints.insert(seg.segment_name);
            }
        }
    }

    struct PreparedObject {
        const StandbyObjectEntry* entry;
        TenantId tenant_id;
        std::string user_key;
        std::vector<Replica> replicas;
    };
    const bool bounded_restore = metadata_store != nullptr;
    // Batch restore spans chunks, so it needs an ordered cross-chunk index;
    // legacy restore keeps its contiguous vector-and-sort validation path.
    std::unordered_map<const StandbySegmentInfo*,
                       std::vector<std::pair<uintptr_t, uint64_t>>>
        legacy_memory_ranges;
    std::unordered_map<const StandbySegmentInfo*, std::map<uintptr_t, uint64_t>>
        bounded_memory_ranges;
    std::unordered_map<std::string, uint64_t> restored_accounted_memory_bytes;
    size_t restored_object_count = 0;

    const auto restore_chunk =
        [&](const std::vector<StandbyObjectEntry>& objects)
        -> tl::expected<void, ErrorCode> {
        std::vector<PreparedObject> prepared_objects;
        std::unordered_set<std::string> chunk_object_ids;
        for (const auto& entry : objects) {
            auto [tenant_id, user_key] = resolve_standby_object(entry);
            if (!tenant_id.IsValid()) {
                LOG(ERROR) << "RestoreFromStandbySnapshot: invalid tenant_id="
                           << entry.tenant_id << ", key=" << entry.key;
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            if (!chunk_object_ids.insert(tenant_id.MakeScopedKey(user_key))
                     .second) {
                LOG(ERROR)
                    << "RestoreFromStandbySnapshot: duplicate object, tenant="
                    << tenant_id.value() << ", key=" << user_key;
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            const auto existing_tenant = tenants_.Lookup(tenant_id);
            if (existing_tenant != nullptr &&
                existing_tenant->ContainsObject(user_key)) {
                LOG(ERROR)
                    << "RestoreFromStandbySnapshot: object already exists, "
                    << "tenant=" << tenant_id.value() << ", key=" << user_key;
                return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
            }
            const auto& standby_meta = entry.metadata;
            std::vector<Replica> replicas;
            replicas.reserve(standby_meta.replicas.size());

            for (const auto& desc : standby_meta.replicas) {
                if (desc.is_memory_replica()) {
                    const auto& buffer =
                        desc.get_memory_descriptor().buffer_descriptor;
                    auto segment_it = memory_segments_by_alias.find(
                        buffer.transport_endpoint_);
                    if (segment_it == memory_segments_by_alias.end()) {
                        LOG(ERROR)
                            << "RestoreFromStandbySnapshot: unknown memory "
                            << "endpoint=" << buffer.transport_endpoint_
                            << ", tenant=" << tenant_id.value()
                            << ", key=" << user_key;
                        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                    }
                    if (buffer.size_ != standby_meta.size ||
                        buffer.size_ == 0 ||
                        buffer.buffer_address_ >
                            std::numeric_limits<uintptr_t>::max() -
                                buffer.size_) {
                        LOG(ERROR)
                            << "RestoreFromStandbySnapshot: invalid memory "
                            << "descriptor, tenant=" << tenant_id.value()
                            << ", key=" << user_key;
                        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                    }

                    const auto* segment = segment_it->second;
                    if (desc.status != ReplicaStatus::REMOVED &&
                        desc.status != ReplicaStatus::FAILED) {
                        auto& bytes =
                            restored_accounted_memory_bytes[segment
                                                                ->segment_name];
                        if (bytes > segment->capacity ||
                            buffer.size_ > segment->capacity - bytes) {
                            LOG(ERROR)
                                << "RestoreFromStandbySnapshot: memory "
                                   "descriptors exceed segment capacity, "
                                   "segment="
                                << segment->segment_name;
                            return tl::make_unexpected(
                                ErrorCode::INVALID_PARAMS);
                        }
                        if (bounded_restore) {
                            auto& ranges = bounded_memory_ranges[segment];
                            auto next =
                                ranges.lower_bound(buffer.buffer_address_);
                            const uintptr_t end =
                                buffer.buffer_address_ + buffer.size_;
                            if ((next != ranges.end() && end > next->first) ||
                                (next != ranges.begin() &&
                                 std::prev(next)->first +
                                         std::prev(next)->second >
                                     buffer.buffer_address_)) {
                                LOG(ERROR) << "RestoreFromStandbySnapshot: "
                                              "overlapping "
                                              "memory descriptors";
                                return tl::make_unexpected(
                                    ErrorCode::INVALID_PARAMS);
                            }
                            ranges.emplace(buffer.buffer_address_,
                                           buffer.size_);
                        } else {
                            legacy_memory_ranges[segment].emplace_back(
                                buffer.buffer_address_, buffer.size_);
                        }
                        bytes += buffer.size_;
                    }

                    auto alloc =
                        restored_allocators.at(buffer.transport_endpoint_);
                    auto restored_buffer =
                        std::make_unique<AllocatedBuffer>(alloc, buffer);
                    replicas.push_back(Replica(
                        desc.id, std::move(restored_buffer), desc.status));
                } else if (desc.is_nof_replica()) {
                    const auto& buffer =
                        desc.get_nof_descriptor().buffer_descriptor;
                    if (buffer.size_ != standby_meta.size ||
                        buffer.size_ == 0) {
                        LOG(ERROR) << "RestoreFromStandbySnapshot: invalid NoF "
                                   << "descriptor, tenant=" << tenant_id.value()
                                   << ", key=" << user_key;
                        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                    }
                    auto& alloc =
                        restored_allocators[buffer.transport_endpoint_];
                    if (!alloc) {
                        alloc = std::make_shared<DummyBufferAllocator>(
                            buffer.transport_endpoint_,
                            buffer.transport_endpoint_);
                    }
                    auto restored_buffer =
                        std::make_unique<AllocatedBuffer>(alloc, buffer);
                    replicas.push_back(
                        Replica(desc.id, std::move(restored_buffer),
                                desc.status, ReplicaType::NOF_SSD));
                } else if (desc.is_disk_replica()) {
                    const auto& disk_desc = desc.get_disk_descriptor();
                    if (disk_desc.object_size != standby_meta.size) {
                        LOG(ERROR)
                            << "RestoreFromStandbySnapshot: invalid disk "
                            << "descriptor, tenant=" << tenant_id.value()
                            << ", key=" << user_key;
                        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                    }
                    replicas.push_back(Replica(desc.id, disk_desc.file_path,
                                               disk_desc.object_size,
                                               desc.status));
                } else if (desc.is_local_disk_replica()) {
                    const auto& local_disk_desc =
                        desc.get_local_disk_descriptor();
                    if (local_disk_desc.object_size != standby_meta.size) {
                        LOG(ERROR)
                            << "RestoreFromStandbySnapshot: invalid local disk "
                            << "descriptor, tenant=" << tenant_id.value()
                            << ", key=" << user_key;
                        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                    }
                    replicas.push_back(Replica(
                        desc.id, local_disk_desc.client_id,
                        local_disk_desc.object_size,
                        local_disk_desc.transport_endpoint, desc.status,
                        record_for_known_owner(local_disk_desc.client_id)));
                } else {
                    LOG(ERROR)
                        << "RestoreFromStandbySnapshot: unsupported replica "
                        << "descriptor, tenant=" << tenant_id.value()
                        << ", key=" << user_key;
                    return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                }
            }
            prepared_objects.push_back({&entry, std::move(tenant_id),
                                        std::move(user_key),
                                        std::move(replicas)});
        }

        for (const auto& object : prepared_objects) {
            const auto existing_tenant = tenants_.Lookup(object.tenant_id);
            if (existing_tenant != nullptr &&
                existing_tenant->ContainsObject(object.user_key)) {
                return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
            }
        }

        if (!bounded_restore) {
            for (auto& [segment, ranges] : legacy_memory_ranges) {
                (void)segment;
                std::sort(ranges.begin(), ranges.end());
                for (size_t i = 1; i < ranges.size(); ++i) {
                    if (ranges[i].first <
                        ranges[i - 1].first + ranges[i - 1].second) {
                        LOG(ERROR)
                            << "RestoreFromStandbySnapshot: overlapping memory "
                               "descriptors";
                        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                    }
                }
            }
        }

        const auto now = std::chrono::system_clock::now();
        for (auto& object : prepared_objects) {
            const auto& standby_meta = object.entry->metadata;
            const auto tenant = GetOrCreateTenantHandle(object.tenant_id);
            auto entry =
                std::make_shared<ObjectEntry>(std::make_unique<ObjectMetadata>(
                    standby_meta.client_id, now, standby_meta.size,
                    std::move(object.replicas), std::nullopt,
                    standby_meta.hard_pinned.value_or(false),
                    standby_meta.data_type, standby_meta.group_id,
                    object.tenant_id, object.user_key));
            if (!tenant->InsertObject(entry)) {
                // The pre-validation pass above rejected duplicates, so the
                // key can only be routed here if the payload disagrees with
                // itself.
                return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
            }
            // InsertObject wires the group member and the group's shared lease,
            // and a restored object carries no in-flight task, which is the
            // state a freshly built entry already has.
        }
        restored_object_count += objects.size();
        return {};
    };

    if (legacy_objects) {
        auto restored = restore_chunk(*legacy_objects);
        if (!restored) {
            return restored;
        }
    } else {
        std::vector<StandbyObjectEntry> objects;
        for (;;) {
            if (!metadata_store->DrainChunk(chunk_object_count, objects)) {
                return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            }
            if (objects.empty()) {
                break;
            }
            auto restored = restore_chunk(objects);
            if (!restored) {
                return restored;
            }
        }
    }

    for (const auto& [segment, bytes] : standby_accounted_memory_bytes_) {
        MasterMetricManager::instance().dec_allocated_mem_size(
            segment, static_cast<int64_t>(bytes));
    }
    for (const auto& [segment, bytes] : restored_accounted_memory_bytes) {
        MasterMetricManager::instance().inc_allocated_mem_size(
            segment, static_cast<int64_t>(bytes));
    }
    standby_accounted_memory_bytes_ =
        std::move(restored_accounted_memory_bytes);
    standby_memory_segments_ = std::move(restored_memory_segments);
    standby_allocator_keepalive_ = std::move(restored_allocators);
    invalid_replica_endpoints_ = std::move(restored_invalid_endpoints);
    for (auto& [client_id, record] : new_known_owner_records) {
        client_liveness_records_.emplace(client_id, std::move(record));
        MasterMetricManager::instance().client_liveness_record_created();
    }

    if (max_live_id != 0) {
        const ReplicaID desired_next_id = max_live_id + 1;
        ReplicaID current_next_id = Replica::next_id_.load();
        while (current_next_id < desired_next_id &&
               !Replica::next_id_.compare_exchange_weak(current_next_id,
                                                        desired_next_id)) {
        }
    }

    if (enable_multi_tenants_) {
        RebuildTenantQuotaUsageFromMetadata();
    }

    auto restored_weight_metadata =
        weight_manager_.RestoreSnapshot(weight_metadata);
    if (!restored_weight_metadata) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    LOG(INFO) << "Restored from standby: " << restored_object_count
              << " objects, " << segments.size()
              << " segments, initial_seq_id=" << initial_oplog_sequence_id
              << ", invalid_endpoints=" << invalid_replica_endpoints_.size();
    return {};
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
    return BatchReplicaClear(object_keys, client_id, segment_name, "default");
}

auto MasterService::BatchReplicaClear(
    const std::vector<std::string>& object_keys, const UUID& client_id,
    const std::string& segment_name, const std::string& tenant_id)
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    std::vector<std::string> cleared_keys;
    cleared_keys.reserve(object_keys.size());
    const bool clear_all_segments = segment_name.empty();
    const TenantId requested_tenant(tenant_id);
    if (!requested_tenant.IsValid()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const TenantId& normalized_tenant =
        ResolveRequestTenantId(requested_tenant);

    for (const auto& key : object_keys) {
        if (key.empty()) {
            LOG(WARNING) << "BatchReplicaClear: tenant=" << normalized_tenant
                         << " empty key, skipping";
            continue;
        }
        const bool ran = WithObjectMetadataForWriteAndCleanup(
            MakeObjectIdentity(key, normalized_tenant),
            [&](metadata::Tenant& tenant,
                const std::shared_ptr<ObjectEntry>& entry,
                ObjectMetadata& metadata, ObjectEntry::State& state) -> void {
                if (!metadata.IsValid()) {
                    LOG(WARNING)
                        << "BatchReplicaClear: tenant=" << normalized_tenant
                        << " key=" << key << " not found, skipping";
                    return;
                }

                const auto previous_kv_media = KvMediaSnapshot(metadata);

                // Security check: Ensure the requesting client owns the object.
                if (metadata.client_id != client_id) {
                    LOG(WARNING)
                        << "BatchReplicaClear: tenant=" << normalized_tenant
                        << " key=" << key << " belongs to different client_id="
                        << metadata.client_id << ", expected=" << client_id
                        << ", skipping";
                    return;
                }

                // Safety check: Do not clear an object that has an active
                // lease.
                if (!metadata.IsLeaseExpired()) {
                    LOG(WARNING)
                        << "BatchReplicaClear: tenant=" << normalized_tenant
                        << " key=" << key << " has active lease, skipping";
                    return;
                }

                if (clear_all_segments) {
                    // Clearing with a Put still in flight could leave an
                    // inconsistent state or interfere with the write, so every
                    // replica must be complete.
                    if (!metadata.AllReplicas(&Replica::fn_is_completed)) {
                        LOG(WARNING)
                            << "BatchReplicaClear: tenant=" << normalized_tenant
                            << " key=" << key
                            << " has incomplete replicas, skipping";
                        return;
                    }

                    if (enable_ha_) {
                        if (enable_oplog_) {
                            auto reservation = ReserveBatchOpLogSlot();
                            if (!reservation) {
                                return;
                            }
                            std::vector<ReplicaID> removed_ids;
                            metadata.VisitReplicas(
                                &Replica::fn_is_completed,
                                [&removed_ids](Replica& replica) {
                                    removed_ids.push_back(replica.id());
                                    replica.mark_removed();
                                });
                            auto persist_result =
                                AppendReservedOpLogWithDurableFinalize(
                                    std::move(reservation.value()),
                                    OpType::REMOVE, normalized_tenant.value(),
                                    key, {},
                                    [this, entry,
                                     removed_ids = std::move(removed_ids)](
                                        const OpLogEntry& durable_entry) {
                                        FinalizeRemovedReplicasAfterDurable(
                                            entry, durable_entry, removed_ids,
                                            QuotaEraseMode::kFull);
                                    });
                            if (!persist_result) {
                                return;
                            }
                            cleared_keys.emplace_back(key);
                            VLOG(1) << "BatchReplicaClear: tenant="
                                    << normalized_tenant
                                    << " successfully cleared all replicas "
                                       "for key="
                                    << key << " for client_id=" << client_id;
                            return;
                        }
                    }

                    // Erase the entire metadata: every replica it holds is
                    // deallocated with it.
                    (void)EraseMetadata(tenant, entry, metadata, state,
                                        normalized_tenant);
                    cleared_keys.emplace_back(key);
                    VLOG(1)
                        << "BatchReplicaClear: tenant=" << normalized_tenant
                        << " successfully cleared all replicas for key=" << key
                        << " for client_id=" << client_id;
                } else {
                    // Clear only replicas on the specified segment_name
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

                    if (!metadata.HasReplica(match_replica_on_segment)) {
                        LOG(WARNING)
                            << "BatchReplicaClear: tenant=" << normalized_tenant
                            << " key=" << key
                            << " has no replica on segment_name="
                            << segment_name << ", skipping";
                        return;
                    }

                    if (enable_ha_) {
                        if (enable_oplog_) {
                            auto reservation = ReserveBatchOpLogSlot();
                            if (!reservation) {
                                return;
                            }
                            auto remaining = BuildRemainingReplicaDescriptors(
                                metadata,
                                [&match_replica_on_segment](const Replica& r) {
                                    return match_replica_on_segment(r);
                                });
                            std::vector<ReplicaID> removed_ids;
                            metadata.VisitReplicas(
                                match_replica_on_segment,
                                [&removed_ids](Replica& replica) {
                                    removed_ids.push_back(replica.id());
                                    replica.mark_removed();
                                });

                            tl::expected<OpLogEntry, ErrorCode> persist_result;
                            if (remaining.empty()) {
                                persist_result =
                                    AppendReservedOpLogWithDurableFinalize(
                                        std::move(reservation.value()),
                                        OpType::REMOVE,
                                        normalized_tenant.value(), key, {},
                                        [this, entry,
                                         removed_ids = std::move(removed_ids)](
                                            const OpLogEntry& durable_entry) {
                                            FinalizeRemovedReplicasAfterDurable(
                                                entry, durable_entry,
                                                removed_ids,
                                                QuotaEraseMode::kFull);
                                        });
                            } else {
                                persist_result =
                                    AppendReservedOpLogWithDurableFinalize(
                                        std::move(reservation.value()),
                                        OpType::PUT_END,
                                        normalized_tenant.value(), key,
                                        SerializeMetadataForOpLogFromReplicaDescriptors(
                                            metadata, remaining),
                                        [this, entry,
                                         removed_ids = std::move(removed_ids)](
                                            const OpLogEntry& durable_entry) {
                                            FinalizeRemovedReplicasAfterDurable(
                                                entry, durable_entry,
                                                removed_ids,
                                                QuotaEraseMode::kFull);
                                        });
                            }
                            if (!persist_result) {
                                return;
                            }
                            cleared_keys.emplace_back(key);
                            VLOG(1) << "BatchReplicaClear: tenant="
                                    << normalized_tenant
                                    << " successfully cleared replicas on "
                                       "segment_name="
                                    << segment_name << " for key=" << key
                                    << " for client_id=" << client_id;
                            return;
                        }
                    }

                    EraseReplicasWithCacheTotalAccounting(
                        metadata, match_replica_on_segment);

                    // If no valid replicas remain, the object holds nothing
                    // readable and is erased entirely.
                    if (!metadata.IsValid()) {
                        (void)EraseMetadata(tenant, entry, metadata, state,
                                            normalized_tenant);
                    } else {
                        SyncKvObjectState(key, metadata, normalized_tenant,
                                          previous_kv_media);
                    }

                    cleared_keys.emplace_back(key);
                    VLOG(1) << "BatchReplicaClear: tenant=" << normalized_tenant
                            << " successfully cleared replicas on segment_name="
                            << segment_name << " for key=" << key
                            << " for client_id=" << client_id;
                }
            });
        if (!ran) {
            LOG(WARNING) << "BatchReplicaClear: tenant=" << normalized_tenant
                         << " key=" << key << " not found, skipping";
        }
    }

    return cleared_keys;
}

bool MasterService::TryGetReadableReplicaDescriptor(
    const Replica& replica, Replica::Descriptor& descriptor) const {
    if (!replica.is_completed() || replica.has_invalid_mem_handle() ||
        replica.has_invalid_nof_handle()) {
        return false;
    }
    if (!replica.getDescriptorIfAvailable(descriptor)) {
        return false;
    }
    std::optional<std::string> endpoint;
    if (descriptor.is_memory_replica()) {
        endpoint = descriptor.get_memory_descriptor()
                       .buffer_descriptor.transport_endpoint_;
    } else if (descriptor.is_nof_replica()) {
        endpoint = descriptor.get_nof_descriptor()
                       .buffer_descriptor.transport_endpoint_;
    } else if (descriptor.is_local_disk_replica()) {
        endpoint = descriptor.get_local_disk_descriptor().transport_endpoint;
    }
    if (endpoint && invalid_replica_endpoints_.contains(*endpoint)) {
        return false;
    }
    return true;
}

std::vector<Replica::Descriptor> MasterService::GetReadableReplicaDescriptors(
    const ObjectMetadata& metadata) const {
    std::vector<Replica::Descriptor> descriptors;
    descriptors.reserve(metadata.CountReplicas());
    metadata.VisitReplicas(
        [](const Replica&) { return true; },
        [this, &descriptors](const Replica& replica) {
            Replica::Descriptor descriptor;
            if (TryGetReadableReplicaDescriptor(replica, descriptor)) {
                descriptors.push_back(std::move(descriptor));
            }
        });
    return descriptors;
}

bool MasterService::IsReplicaReadable(const Replica& replica) const {
    Replica::Descriptor descriptor;
    return TryGetReadableReplicaDescriptor(replica, descriptor);
}

bool MasterService::HasReadableReplica(const ObjectMetadata& metadata) const {
    return metadata.HasReplica(
        [this](const Replica& replica) { return IsReplicaReadable(replica); });
}

bool MasterService::IsEvictableMemoryReplica(const Replica& replica) const {
    return replica.is_memory_replica() && IsReplicaReadable(replica) &&
           replica.get_refcnt() == 0;
}

auto MasterService::GetReplicaListByRegex(const std::string& regex_pattern,
                                          const TenantId& tenant_id)
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
    const TenantId& normalized_tenant = ResolveRequestTenantId(tenant_id);
    const auto tenant = tenants_.Lookup(normalized_tenant);
    if (tenant == nullptr) {
        return results;
    }
    // The snapshot holds handles to what the route published, so each key is
    // resolved through the route again while its object is read.
    for (const auto& snapshot_entry : tenant->SnapshotObjects()) {
        const std::string& key = snapshot_entry->key();
        if (!std::regex_search(key, pattern)) {
            continue;
        }
        const ObjectIdentity object_id{normalized_tenant, key};
        (void)WithObjectMetadataForRead(
            object_id,
            [this, &results, &key](const metadata::Tenant&,
                                   const std::shared_ptr<ObjectEntry>&,
                                   const ObjectMetadata& metadata,
                                   const ObjectEntry::State&) -> void {
                auto replica_list = GetReadableReplicaDescriptors(metadata);
                if (replica_list.empty()) {
                    LOG(WARNING)
                        << "key=" << key
                        << " matched by regex, but has no complete replicas.";
                    return;
                }
                results.emplace(key, std::move(replica_list));
                metadata.GrantReadLease(
                    std::chrono::milliseconds(default_kv_lease_ttl_));
            });
    }

    return results;
}

auto MasterService::GetReplicaList(const std::string& key,
                                   const TenantId& tenant_id)
    -> tl::expected<GetReplicaListResponse, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    const auto object_id = MakeObjectIdentityForRequest(key, tenant_id);

    bool promotion_eligible = false;
    bool dynamic_replication_observed = false;
    MasterMetricManager::instance().inc_total_get_nums();
    auto outcome = WithObjectMetadataForRead(
        object_id,
        [&](const metadata::Tenant&, const std::shared_ptr<ObjectEntry>&,
            const ObjectMetadata& metadata, const ObjectEntry::State&)
            -> tl::expected<GetReplicaListResponse, ErrorCode> {
            if (!metadata.IsValid()) {
                VLOG(1) << "key=" << key << ", info=object_not_found";
                return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
            }

            auto replica_list = GetReadableReplicaDescriptors(metadata);
            if (dfs_allocator_) {
                for (const auto& replica : replica_list) {
                    if (replica.is_dfs_replica()) {
                        const auto& desc = replica.get_dfs_descriptor();
                        dfs_allocator_->UpdateAccess(key, desc);
                    }
                }
            }

            if (replica_list.empty()) {
                if (metadata.AllReplicas([](const Replica& replica) {
                        return replica.status() == ReplicaStatus::REMOVED;
                    })) {
                    return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
                }
                LOG(WARNING) << "key=" << key << ", error=replica_not_ready";
                return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
            }

            // TODO: NoF SSD support (ranhaojia)
            if (replica_list[0].is_memory_replica()) {
                MasterMetricManager::instance().inc_mem_cache_hit_nums();
                MasterMetricManager::instance().inc_mem_cache_hit_bytes(
                    static_cast<int64_t>(metadata.size));
            } else if (replica_list[0].is_local_disk_replica() ||
                       replica_list[0].is_disk_replica()) {
                MasterMetricManager::instance().inc_file_cache_hit_nums();
                MasterMetricManager::instance().inc_file_cache_hit_bytes(
                    static_cast<int64_t>(metadata.size));
            }
            MasterMetricManager::instance().inc_valid_get_nums();
            // Grant a lease to the object so it will not be removed
            // when the client is reading it. Read path is group-agnostic: only
            // the object's own lease is refreshed.
            metadata.GrantReadLease(
                std::chrono::milliseconds(default_kv_lease_ttl_));

            // Promotion-on-hit: no MEMORY replica and at least one LOCAL_DISK
            // one. Decided here while the object is read; the enqueue happens
            // after the read is released below.
            if (promotion_on_hit_) {
                const bool any_memory =
                    std::any_of(replica_list.begin(), replica_list.end(),
                                [](const auto& descriptor) {
                                    return descriptor.is_memory_replica();
                                });
                const bool any_local_disk =
                    std::any_of(replica_list.begin(), replica_list.end(),
                                [](const auto& descriptor) {
                                    return descriptor.is_local_disk_replica();
                                });
                promotion_eligible = !any_memory && any_local_disk;
            }
            if (DynamicReplicationEnabled()) {
                const size_t memory_replicas =
                    std::count_if(replica_list.begin(), replica_list.end(),
                                  [](const auto& descriptor) {
                                      return descriptor.is_memory_replica();
                                  });
                dynamic_replication_observed =
                    memory_replicas > 0 &&
                    memory_replicas < dynamic_replication_max_memory_replicas_;
            }

            return GetReplicaListResponse(std::move(replica_list),
                                          default_kv_lease_ttl_,
                                          metadata.object_checksum);
        });
    if (!outcome.has_value()) {
        VLOG(1) << "key=" << key << ", info=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    if (!outcome->has_value()) {
        return tl::make_unexpected(outcome->error());
    }
    // The read is released. Safe to take a fresh write access now.
    if (promotion_eligible) {
        TryPushPromotionQueue(object_id);
    }
    if (dynamic_replication_observed) {
        MaybeQueueDynamicReplicaProposal(object_id);
    }
    return std::move(*outcome);
}

auto MasterService::GetReplicaListForAdmin(const std::string& key,
                                           const TenantId& tenant_id)
    -> tl::expected<GetReplicaListResponse, ErrorCode> {
    assert(tenant_id.IsValid());
    const auto object_id = MakeObjectIdentity(key, tenant_id);

    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    auto outcome = WithObjectMetadataForRead(
        object_id,
        [&](const metadata::Tenant&, const std::shared_ptr<ObjectEntry>&,
            const ObjectMetadata& metadata, const ObjectEntry::State&)
            -> tl::expected<GetReplicaListResponse, ErrorCode> {
            if (!metadata.IsValid()) {
                VLOG(1) << "key=" << key << ", info=object_not_found";
                return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
            }

            auto replica_list = GetReadableReplicaDescriptors(metadata);

            if (replica_list.empty()) {
                LOG(WARNING) << "key=" << key << ", error=replica_not_ready";
                return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
            }

            return GetReplicaListResponse(std::move(replica_list),
                                          default_kv_lease_ttl_,
                                          metadata.object_checksum);
        });
    if (!outcome.has_value()) {
        VLOG(1) << "key=" << key << ", info=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    if (!outcome->has_value()) {
        return tl::make_unexpected(outcome->error());
    }
    return std::move(*outcome);
}

std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
MasterService::BatchGetReplicaList(const std::vector<std::string>& keys,
                                   const TenantId& tenant_id) {
    using GetResult = tl::expected<GetReplicaListResponse, ErrorCode>;

    assert(tenant_id.IsValid());

    std::vector<GetResult> results(
        keys.size(), tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND));
    if (keys.empty()) {
        return results;
    }

    const TenantId& normalized_tenant = ResolveRequestTenantId(tenant_id);
    std::vector<ObjectIdentity> promotion_candidates;
    std::vector<ObjectIdentity> dynamic_replication_candidates;
    std::unordered_set<std::string> dynamic_replication_seen;
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    for (size_t original_idx = 0; original_idx < keys.size(); ++original_idx) {
        const std::string& key = keys[original_idx];
        MasterMetricManager::instance().inc_total_get_nums();

        const ObjectIdentity object_id{normalized_tenant, key};
        auto outcome = WithObjectMetadataForRead(
            object_id,
            [&](const metadata::Tenant&, const std::shared_ptr<ObjectEntry>&,
                const ObjectMetadata& metadata,
                const ObjectEntry::State&) -> GetResult {
                if (!metadata.IsValid()) {
                    VLOG(1) << "key=" << key << ", info=object_not_found";
                    return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
                }
                auto replica_list = GetReadableReplicaDescriptors(metadata);

                if (replica_list.empty()) {
                    if (metadata.AllReplicas([](const Replica& replica) {
                            return replica.status() == ReplicaStatus::REMOVED;
                        })) {
                        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
                    }
                    LOG(WARNING)
                        << "key=" << key << ", error=replica_not_ready";
                    return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
                }

                if (replica_list[0].is_memory_replica()) {
                    MasterMetricManager::instance().inc_mem_cache_hit_nums();
                    MasterMetricManager::instance().inc_mem_cache_hit_bytes(
                        static_cast<int64_t>(metadata.size));
                } else if (replica_list[0].is_local_disk_replica() ||
                           replica_list[0].is_disk_replica()) {
                    MasterMetricManager::instance().inc_file_cache_hit_nums();
                    MasterMetricManager::instance().inc_file_cache_hit_bytes(
                        static_cast<int64_t>(metadata.size));
                }
                MasterMetricManager::instance().inc_valid_get_nums();
                metadata.GrantReadLease(
                    std::chrono::milliseconds(default_kv_lease_ttl_));

                if (promotion_on_hit_) {
                    const bool any_memory =
                        std::any_of(replica_list.begin(), replica_list.end(),
                                    [](const auto& descriptor) {
                                        return descriptor.is_memory_replica();
                                    });
                    const bool any_local_disk = std::any_of(
                        replica_list.begin(), replica_list.end(),
                        [](const auto& descriptor) {
                            return descriptor.is_local_disk_replica();
                        });
                    if (!any_memory && any_local_disk) {
                        promotion_candidates.push_back(object_id);
                    }
                }
                if (DynamicReplicationEnabled()) {
                    const size_t memory_replicas =
                        std::count_if(replica_list.begin(), replica_list.end(),
                                      [](const auto& descriptor) {
                                          return descriptor.is_memory_replica();
                                      });
                    if (memory_replicas > 0 &&
                        memory_replicas <
                            dynamic_replication_max_memory_replicas_) {
                        if (dynamic_replication_seen
                                .insert(object_id.tenant_id.MakeScopedKey(
                                    object_id.user_key))
                                .second) {
                            dynamic_replication_candidates.push_back(object_id);
                        }
                    }
                }

                return GetReplicaListResponse(std::move(replica_list),
                                              default_kv_lease_ttl_,
                                              metadata.object_checksum);
            });
        if (outcome.has_value()) {
            results[original_idx] = std::move(*outcome);
        } else {
            VLOG(1) << "key=" << key << ", info=object_not_found";
        }
    }

    // The reads are released. Safe to take fresh write accesses now.
    for (const auto& object_id : promotion_candidates) {
        TryPushPromotionQueue(object_id);
    }
    for (const auto& object_id : dynamic_replication_candidates) {
        MaybeQueueDynamicReplicaProposal(object_id);
    }

    return results;
}

std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
MasterService::BatchGetReplicaListForAdmin(const std::vector<std::string>& keys,
                                           const TenantId& tenant_id) {
    using GetResult = tl::expected<GetReplicaListResponse, ErrorCode>;

    assert(tenant_id.IsValid());

    std::vector<GetResult> results(
        keys.size(), tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND));
    if (keys.empty()) {
        return results;
    }

    const TenantId& normalized_tenant = tenant_id;
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    for (size_t original_idx = 0; original_idx < keys.size(); ++original_idx) {
        const std::string& key = keys[original_idx];
        const ObjectIdentity object_id{normalized_tenant, key};
        auto outcome = WithObjectMetadataForRead(
            object_id,
            [&](const metadata::Tenant&, const std::shared_ptr<ObjectEntry>&,
                const ObjectMetadata& metadata,
                const ObjectEntry::State&) -> GetResult {
                if (!metadata.IsValid()) {
                    return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
                }
                auto replica_list = GetReadableReplicaDescriptors(metadata);
                if (replica_list.empty()) {
                    return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
                }
                return GetReplicaListResponse(std::move(replica_list),
                                              default_kv_lease_ttl_,
                                              metadata.object_checksum);
            });
        if (outcome.has_value()) {
            results[original_idx] = std::move(*outcome);
        }
    }

    return results;
}

auto MasterService::AllocateReplicas(const std::string& key,
                                     uint64_t value_length,
                                     const ReplicateConfig& config,
                                     const std::string& writer_host_id,
                                     bool* dfs_allocation_failed)
    -> tl::expected<std::vector<Replica>, ErrorCode> {
    if (dfs_allocation_failed != nullptr) {
        *dfs_allocation_failed = false;
    }
    std::vector<Replica> replicas;
    const auto write_mode = DetermineReplicaWriteMode(config);
    size_t allocated_memory_replicas = 0;
    size_t allocated_nof_replicas = 0;
    bool has_enough_memory_segments = false;

    if (config.replica_num > 0) {
        std::vector<std::string> preferred_segments;
        auto append_preferred_segment = [&preferred_segments](
                                            const std::string& segment_name) {
            if (!segment_name.empty() &&
                std::find(preferred_segments.begin(), preferred_segments.end(),
                          segment_name) == preferred_segments.end()) {
                preferred_segments.push_back(segment_name);
            }
        };
        if (!config.preferred_segment.empty()) {
            append_preferred_segment(config.preferred_segment);
        } else {
            for (const auto& preferred_segment : config.preferred_segments) {
                append_preferred_segment(preferred_segment);
            }
        }
        AllocatorManager allocator_snapshot;
        {
            ScopedAllocatorAccess allocator_access =
                segment_manager_.getAllocatorAccess();
            has_enough_memory_segments = allocator_access.getAllocatorManager()
                                             .getServingNames()
                                             .size() >= config.replica_num;
            if (!writer_host_id.empty()) {
                auto host_ordered_segments =
                    allocator_access.GetHostOrderedSegments(writer_host_id,
                                                            key);
                for (const auto& segment_name : host_ordered_segments) {
                    append_preferred_segment(segment_name);
                }
                if (!host_ordered_segments.empty()) {
                    VLOG(1) << "key=" << key
                            << ", writer_host_id=" << writer_host_id
                            << ", local_first_preferred_segments="
                            << host_ordered_segments.size();
                }
            }
            allocator_snapshot = allocator_access.SnapshotAllocatorManager();
        }

        auto allocation_result = allocation_strategy_->Allocate(
            allocator_snapshot, value_length, config.replica_num,
            preferred_segments, std::set<std::string>(), ReplicaType::MEMORY);

        if (!allocation_result.has_value()) {
            VLOG(1) << "Failed to allocate replicas for key=" << key
                    << ", error: " << allocation_result.error();
            if (allocation_result.error() == ErrorCode::INVALID_PARAMS) {
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            if (write_mode != ReplicaWriteMode::FLEXIBLE_DUAL_REPLICA) {
                MasterMetricManager::instance().inc_put_start_alloc_failures();
                if (has_enough_memory_segments) {
                    need_mem_eviction_ = true;
                }
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
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            if (write_mode != ReplicaWriteMode::FLEXIBLE_DUAL_REPLICA) {
                MasterMetricManager::instance().inc_put_start_alloc_failures();
                need_nof_eviction_ = true;
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
                allocated_memory_replicas != config.replica_num &&
                has_enough_memory_segments) {
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
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }

    // Best-effort / flexible modes may pass the check above with fewer
    // replicas than requested (see HasExpectedReplicaAllocation). Surface
    // the degradation so callers and operators can detect the reduced
    // redundancy instead of failing silently.
    if (allocated_memory_replicas < config.replica_num ||
        allocated_nof_replicas < config.nof_replica_num) {
        MasterMetricManager::instance().inc_put_start_partial_allocations();
        LOG(WARNING) << "key=" << key << ", action=put_start_partial_allocation"
                     << ", requested_memory_replicas=" << config.replica_num
                     << ", allocated_memory_replicas="
                     << allocated_memory_replicas
                     << ", requested_nof_replicas=" << config.nof_replica_num
                     << ", allocated_nof_replicas=" << allocated_nof_replicas;
    }

    if (use_disk_replica_) {
        std::string file_path =
            FileUtil::ResolvePathFromKey(key, root_fs_dir_, cluster_id_);
        replicas.emplace_back(file_path, value_length,
                              ReplicaStatus::PROCESSING);
    }

    if (config.dfs_replica_num > 0) {
        if (!dfs_allocator_ || !dfs_allocator_->IsInitialized()) {
            LOG(ERROR) << "Failed to allocate DFS replica for key=" << key
                       << ", error=dfs_allocator_not_initialized";
            return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
        }
        auto alloc = dfs_allocator_->Allocate(key, value_length);
        if (!alloc) {
            LOG(ERROR) << "Failed to allocate DFS replica for key=" << key
                       << ", error=" << alloc.error();
            if (dfs_allocation_failed != nullptr) {
                *dfs_allocation_failed =
                    alloc.error() == ErrorCode::NO_AVAILABLE_HANDLE;
            }
            return tl::make_unexpected(alloc.error());
        }
        replicas.emplace_back(std::move(*alloc), ReplicaStatus::PROCESSING);
    }

    return replicas;
}

auto MasterService::InsertMetadata(
    metadata::Tenant& tenant, const UUID& client_id, const std::string& key,
    uint64_t value_length, const ReplicateConfig& config,
    const std::string& group_id, const TenantId& tenant_id,
    const std::chrono::system_clock::time_point& now,
    const ResolvedSoftPinRequest& soft_pin_request,
    std::vector<Replica>&& replicas, uint64_t pending_quota_charge,
    std::optional<std::chrono::system_clock::time_point>
        committed_soft_pin_timeout)
    -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode> {
    const auto deadline_to_index = committed_soft_pin_timeout;
    if (tenant.Get(key) != nullptr) {
        FreeDfsReplicas(key, replicas);
        return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
    }

    std::vector<Replica::Descriptor> replica_list;
    std::unordered_set<ReplicaID> eligible_replica_ids;
    replica_list.reserve(replicas.size());
    eligible_replica_ids.reserve(replicas.size());
    int i = 0;
    VLOG(1) << "PutStart, create replicas: client_id=" << client_id
            << ", key=" << key << ", value_length=" << value_length;
    for (const auto& replica : replicas) {
        const auto desc = replica.get_descriptor();
        replica_list.emplace_back(desc);
        eligible_replica_ids.insert(replica.id());

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
        } else if (replica.is_dfs_replica()) {
            const auto& dfs_desc = desc.get_dfs_descriptor();
            VLOG(1) << "Replica #" << ++i << ": dfs_file=" << dfs_desc.file_path
                    << ", offset=" << dfs_desc.offset
                    << ", shard_idx=" << dfs_desc.shard_idx;
        }
    }

    // Everything a reader of the object relies on is written through the entry
    // before it is published: InsertObject takes that same lock, and the route
    // reaches the entry the moment it publishes, so the tenant charge, the
    // in-flight mark and the pending soft-pin action are all in place first.
    auto envelope = std::make_unique<ObjectMetadata>(
        client_id, now, value_length, std::move(replicas),
        std::move(committed_soft_pin_timeout), config.with_hard_pin,
        config.data_type, group_id, tenant_id, key);
    auto entry = std::make_shared<ObjectEntry>(std::move(envelope));
    const bool ready = entry->WithExclusiveAccess(
        [&](ObjectMetadata& metadata, ObjectEntry::State& state) {
            if (enable_multi_tenants_) {
                auto adopt_result = metadata.quota_ledger.AdoptPendingCharge(
                    GetBoundTenantQuotaHandle(tenant), pending_quota_charge);
                if (!adopt_result) {
                    LogTenantQuotaLedgerError(adopt_result, "adopt_pending",
                                              tenant_id, key);
                    return false;
                }
            }
            metadata.BeginSoftPinAction(soft_pin_request,
                                        std::move(eligible_replica_ids));
            state.is_processing = true;
            return true;
        });
    if (!ready) {
        // Nothing was published, so the caller's release of the pending charge
        // is the whole rollback; only the replicas this attempt allocated have
        // to be given back.
        const auto orphaned = entry->WithExclusiveAccess(
            [](ObjectMetadata& metadata, ObjectEntry::State&) {
                return metadata.PopReplicas();
            });
        FreeDfsReplicas(key, orphaned);
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    // InsertObject publishes the entry and wires it to its group and that
    // group's shared lease. On a false return the key is already routed, and
    // the unpublished envelope owns the replicas this call allocated and
    // releases their DFS extents itself.
    if (!tenant.InsertObject(entry)) {
        entry->WithExclusiveAccess(
            [&](ObjectMetadata& metadata, ObjectEntry::State&) {
                FreeDfsReplicas(key, metadata.GetAllReplicas());
            });
        return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
    }
    if (deadline_to_index) {
        soft_pin_deadline_index_.Upsert(tenant_id.MakeScopedKey(key),
                                        *deadline_to_index);
    }

    return replica_list;
}

auto MasterService::AllocateAndInsertMetadata(
    metadata::Tenant& tenant, const UUID& client_id, const std::string& key,
    uint64_t value_length, const ReplicateConfig& config,
    const std::string& writer_host_id, const std::string& group_id,
    const TenantId& tenant_id, const std::chrono::system_clock::time_point& now,
    const ResolvedSoftPinRequest& soft_pin_request,
    std::optional<std::chrono::system_clock::time_point>
        committed_soft_pin_timeout,
    bool* dfs_allocation_failed)
    -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode> {
    if (tenant.Get(key) != nullptr) {
        return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
    }

    const uint64_t pending_quota_charge =
        RequestedMemoryQuotaCharge(value_length, config);
    auto quota_result = ChargeTenantQuota(GetBoundTenantQuotaHandle(tenant),
                                          pending_quota_charge);
    if (!quota_result) {
        return tl::make_unexpected(quota_result.error());
    }

    auto allocation_result = AllocateReplicas(
        key, value_length, config, writer_host_id, dfs_allocation_failed);
    if (!allocation_result) {
        ReleaseTenantQuota(GetBoundTenantQuotaHandle(tenant),
                           pending_quota_charge);
        return tl::make_unexpected(allocation_result.error());
    }

    auto insert_result = InsertMetadata(
        tenant, client_id, key, value_length, config, group_id, tenant_id, now,
        soft_pin_request, std::move(allocation_result.value()),
        pending_quota_charge, std::move(committed_soft_pin_timeout));
    if (!insert_result) {
        ReleaseTenantQuota(GetBoundTenantQuotaHandle(tenant),
                           pending_quota_charge);
    }
    return insert_result;
}

auto MasterService::PutStart(const UUID& client_id, const std::string& key,
                             const TenantId& tenant_id,
                             const uint64_t slice_length,
                             const ReplicateConfig& config)
    -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode> {
    const auto object_id = MakeObjectIdentityForRequest(key, tenant_id);
    if ((config.replica_num == 0 && config.nof_replica_num == 0 &&
         config.dfs_replica_num == 0) ||
        key.empty() || slice_length == 0) {
        LOG(ERROR) << "key=" << key << ", replica_num=" << config.replica_num
                   << ", nof_replica_num=" << config.nof_replica_num
                   << ", dfs_replica_num=" << config.dfs_replica_num
                   << ", slice_length=" << slice_length
                   << ", key_size=" << key.size() << ", error=invalid_params";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (config.dfs_replica_num > 1 ||
        (config.dfs_replica_num > 0 && config.replica_num == 0)) {
        LOG(ERROR) << "key=" << key << ", replica_num=" << config.replica_num
                   << ", dfs_replica_num=" << config.dfs_replica_num
                   << ", error=invalid_dfs_replica_config";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (config.dfs_replica_num > 0 && !object_id.tenant_id.IsDefault()) {
        LOG(ERROR) << "key=" << key << ", tenant_id=" << tenant_id
                   << ", error=dfs_currently_requires_default_tenant";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (config.dfs_replica_num > 0 &&
        (!enable_dfs_ || !dfs_allocator_ || !dfs_allocator_->IsInitialized())) {
        LOG(ERROR) << "key=" << key << ", error=dfs_allocator_not_initialized";
        return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
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

    auto soft_pin_request = ResolveSoftPinRequest(config);
    if (!soft_pin_request) {
        return tl::make_unexpected(soft_pin_request.error());
    }

    UpdateClientHostId(client_id, config.host_id);
    std::string writer_host_id;
    if ((allocation_strategy_type_ == AllocationStrategyType::LOCAL_FIRST ||
         config.prefer_alloc_in_same_node) &&
        config.replica_num == 1) {
        writer_host_id = config.host_id.empty() ? GetClientHostId(client_id)
                                                : config.host_id;
    }

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
    // Set by AllocateReplicas only when DFS allocation itself failed with
    // capacity exhaustion; gates the forced bucket eviction retry below so
    // memory/NoF exhaustion cannot delete unrelated DFS data.
    bool dfs_allocation_failed = false;
    auto admit =
        [&]() -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode> {
        auto now = std::chrono::system_clock::now();
        {
            std::shared_lock<std::shared_mutex> client_lock(client_mutex_);
            std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
            auto retaining_clients = GetRetainingClientIdsLocked();
            client_lock.unlock();
            const auto tenant = GetOrCreateTenantHandle(object_id.tenant_id);
            auto admission_result =
                ChargeTenantQuota(GetBoundTenantQuotaHandle(*tenant), 0);
            if (!admission_result) {
                return tl::make_unexpected(admission_result.error());
            }

            // Set when the entry the route publishes for this key refused the
            // write; left empty when that entry was erased or is no longer
            // routed, which leaves the key free for the create below.
            std::optional<ErrorCode> refused;
            // The read-modify region runs under the entry's own lock, so a
            // concurrent Get/PutEnd/Remove on this key serializes behind it.
            (void)WithObjectMetadataForWriteAndCleanup(
                object_id,
                [&](metadata::Tenant& resolved_tenant,
                    const std::shared_ptr<ObjectEntry>& entry,
                    ObjectMetadata& metadata,
                    ObjectEntry::State& state) -> void {
                    auto cleanup_plan = BuildStaleHandleCleanupPlan(
                        metadata, retaining_clients);
                    if (!cleanup_plan.removed_ids.empty()) {
                        auto persist_result = PersistStaleHandleCleanupForHA(
                            "PutStart(stale cleanup)", object_id.tenant_id,
                            entry, key, metadata, cleanup_plan);
                        if (!persist_result) {
                            refused = persist_result.error();
                            return;
                        }
                        if (enable_oplog_) {
                            refused = ErrorCode::OBJECT_ALREADY_EXISTS;
                            return;
                        }
                        if (CleanupStaleHandles(key, object_id.tenant_id,
                                                resolved_tenant, metadata,
                                                state, retaining_clients)) {
                            (void)EraseMetadata(
                                resolved_tenant, entry, metadata, state,
                                object_id.tenant_id, QuotaEraseMode::kFull);
                            return;
                        }
                    }
                    if (metadata.HasReplica(&Replica::fn_is_completed) ||
                        metadata.put_start_time +
                                put_start_discard_timeout_sec_ >=
                            now) {
                        refused = ErrorCode::OBJECT_ALREADY_EXISTS;
                        return;
                    }
                    if (enable_oplog_ && ordered_oplog_writer_) {
                        auto err =
                            PersistRemoveForHA("PutStart(stale cleanup REMOVE)",
                                               object_id.tenant_id, key);
                        if (!err) {
                            refused = err.error();
                            return;
                        }
                    }
                    auto replicas = PopReplicasWithCacheTotalAccounting(
                        metadata, &Replica::fn_is_processing);
                    if (!replicas.empty()) {
                        FreeDfsReplicas(key, replicas);
                        std::lock_guard lock(discarded_replicas_mutex_);
                        discarded_replicas_.emplace_back(
                            std::move(replicas),
                            metadata.put_start_time +
                                put_start_release_timeout_sec_);
                    }
                    (void)EraseMetadata(resolved_tenant, entry, metadata, state,
                                        object_id.tenant_id,
                                        QuotaEraseMode::kFull);
                });
            if (refused.has_value()) {
                return tl::make_unexpected(*refused);
            }
            return AllocateAndInsertMetadata(
                *tenant, client_id, key, slice_length, config, writer_host_id,
                group_id, object_id.tenant_id, now, *soft_pin_request,
                std::nullopt, &dfs_allocation_failed);
        }
    };

    // Over-quota is rejected outright. Making room is the background pass's
    // job (EvictTenantsOverWatermark), not the write path's: the same contract
    // the pool has when enable_multi_tenants is false -- this write fails, the
    // evictor builds headroom, the next one succeeds. Evicting inline instead
    // put a full metadata scan on admission, which every concurrent writer at
    // the ceiling repeated for the same freed extent.
    //
    // Bucket-count exhaustion is different: serialize recovery, retry admission
    // after any recovery ahead of us, then allow one forced, fully validated
    // bucket eviction and one final retry. The path is gated on
    // dfs_allocation_failed so memory/NoF exhaustion keeps its
    // background-eviction behavior.
    auto result = admit();
    if (!result && dfs_allocation_failed && config.dfs_replica_num > 0 &&
        bucket_allocator_ != nullptr) {
        std::lock_guard recovery_lock(dfs_bucket_recovery_mutex_);
        dfs_allocation_failed = false;
        result = admit();
        if (!result && dfs_allocation_failed &&
            TryRecoverDfsSpaceAfterAllocationFailure()) {
            dfs_allocation_failed = false;
            result = admit();
        }
    }
    if (!result && result.error() == ErrorCode::TENANT_QUOTA_EXCEEDED) {
        MasterMetricManager::instance().inc_tenant_quota_reject(
            object_id.tenant_id.value(), "quota_exceeded");
    }
    return result;
}

auto MasterService::PutEnd(const UUID& client_id, const ObjectMeta& object_meta,
                           const TenantId& tenant_id, ReplicaType replica_type)
    -> tl::expected<void, ErrorCode> {
    const auto& key = object_meta.key;
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    const auto object_id = MakeObjectIdentityForRequest(key, tenant_id);
    const auto outcome = WithObjectMetadataForWriteAndCleanup(
        object_id,
        [&](metadata::Tenant& tenant, const std::shared_ptr<ObjectEntry>&,
            ObjectMetadata& metadata,
            ObjectEntry::State& state) -> tl::expected<void, ErrorCode> {
            // No usable replica: answered as absent, like the read APIs.
            if (!metadata.IsValid()) {
                LOG(ERROR) << "key=" << key << ", error=object_not_found";
                return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
            }

            if (client_id != metadata.client_id) {
                LOG(ERROR) << "Illegal client " << client_id
                           << " to PutEnd key " << key
                           << ", was PutStart-ed by " << metadata.client_id;
                return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
            }

            auto is_target_replica = [replica_type](const Replica& replica) {
                if (replica_type == ReplicaType::ALL) {
                    return (replica.is_memory_replica() &&
                            !replica.has_invalid_mem_handle()) ||
                           (replica.is_nof_replica() &&
                            !replica.has_invalid_nof_handle()) ||
                           replica.is_dfs_replica();
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
            };

            // A successful End removes the processing marker. A retry is a
            // no-op only when every replica that End targeted is already
            // COMPLETE: a promotion-owned PROCESSING replica keeps this check
            // from accepting a MEMORY/ALL End and is never modified here.
            if (!state.is_processing) {
                bool has_target_replica = false;
                bool all_target_replicas_complete = true;
                for (const auto& replica : metadata.GetAllReplicas()) {
                    if (!is_target_replica(replica)) {
                        continue;
                    }
                    has_target_replica = true;
                    if (!replica.is_completed()) {
                        all_target_replicas_complete = false;
                        break;
                    }
                }
                if (has_target_replica && all_target_replicas_complete) {
                    return {};
                }
                LOG(ERROR) << "key=" << key
                           << ", error=no_primary_write_in_progress";
                return tl::make_unexpected(ErrorCode::INVALID_WRITE);
            }

            // A DFS write that went through the immutable bucket allocator
            // commits its buckets here: the write is visible from now on, so a
            // later recovery pass must not hand those extents back.
            if (bucket_allocator_ != nullptr &&
                (replica_type == ReplicaType::ALL ||
                 replica_type == ReplicaType::DFS)) {
                std::vector<DistributedFSDescriptor> pending_descriptors;
                metadata.VisitReplicas(
                    [](const Replica& replica) {
                        return replica.is_dfs_replica() &&
                               replica.is_processing();
                    },
                    [&pending_descriptors](const Replica& replica) {
                        pending_descriptors.push_back(
                            replica.get_dfs_descriptor());
                    });
                for (const auto& descriptor : pending_descriptors) {
                    if (!bucket_allocator_->MarkCommitted(key, descriptor)) {
                        LOG(ERROR)
                            << "key=" << key
                            << ", error=dfs_runtime_commit_failed, bucket_id="
                            << descriptor.shard_idx;
                        return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
                    }
                }
            }

            const bool had_completed_replica =
                metadata.HasReplica(&Replica::fn_is_completed);
            bool completed_pending_replica = false;
            metadata.VisitReplicas(
                [&is_target_replica](const Replica& replica) {
                    return replica.is_processing() &&
                           is_target_replica(replica);
                },
                [this, &key, &metadata,
                 &completed_pending_replica](Replica& replica) {
                    if (replica.is_processing() &&
                        metadata.PendingSoftPinOwnsReplica(replica.id())) {
                        completed_pending_replica = true;
                    }
                    replica.mark_complete();
                    if (replica.is_dfs_replica() && dfs_allocator_) {
                        const auto& desc = replica.get_dfs_descriptor();
                        dfs_allocator_->UpdateAccess(key, desc);
                    }
                });

            if (!had_completed_replica && completed_pending_replica &&
                metadata.HasReplica(&Replica::fn_is_completed)) {
                const auto soft_pin_result = metadata.CommitPendingSoftPin(
                    std::chrono::system_clock::now());
                ApplySoftPinEvaluation(metadata, soft_pin_result);
            }

            if (object_meta.object_checksum.has_value() ||
                replica_type == ReplicaType::ALL ||
                replica_type == ReplicaType::MEMORY ||
                replica_type == ReplicaType::NOF_SSD) {
                metadata.object_checksum = object_meta.object_checksum;
            }

            auto settle_result =
                SettlePrimaryWriteQuotaIfReady(tenant, metadata);
            if (!settle_result) {
                return tl::make_unexpected(settle_result.error());
            }

            if (replica_type != ReplicaType::DFS && enable_offload_ &&
                !offload_on_evict_ &&
                !metadata.HasReplica([](const Replica& replica) {
                    return replica.is_dfs_replica() && replica.is_processing();
                })) {
                // One marker covers every mirror pushed below, so the mirrors
                // are collected before the marker is recorded.
                std::optional<ReplicaID> source_id;
                std::vector<UUID> mirror_clients;
                metadata.VisitReplicas(
                    [](const Replica& replica) {
                        return replica.is_completed() &&
                               replica.is_memory_replica();
                    },
                    [this, &object_id, &source_id,
                     &mirror_clients](Replica& replica) {
                        auto result = PushOffloadingQueue(object_id, replica,
                                                          &mirror_clients);
                        if (result && !source_id.has_value()) {
                            replica.inc_refcnt();
                            source_id = replica.id();
                        }
                    });
                if (source_id.has_value()) {
                    state.offloading_task = OffloadingTask{
                        *source_id, std::chrono::system_clock::now(),
                        std::move(mirror_clients)};
                }
            }

            // If the object is completed, remove it from the processing set.
            if (metadata.AllReplicas(&Replica::fn_is_completed) &&
                state.is_processing) {
                state.is_processing = false;
            }

            SyncCacheTotalAccounting(metadata);
            // TODO: add inc_nof_cache_nums() (ranhaojia)
            metadata.GrantReadLease(std::chrono::milliseconds::zero());
            PublishKvStored(key, metadata, object_id.tenant_id);

            if (enable_oplog_ && ordered_oplog_writer_) {
                std::string payload = SerializeMetadataForOpLog(metadata);
                auto result = AppendOpLogVisibleBeforeDurable(
                    OpType::PUT_END, object_id.tenant_id.value(), key, payload);
                if (!result) {
                    LOG(WARNING)
                        << "PutEnd: OpLog queue failed for key=" << key
                        << ", err=" << static_cast<int>(result.error());
                }
            }
            return {};
        });
    if (!outcome.has_value()) {
        LOG(ERROR) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    return *outcome;
}

auto MasterService::AddReplica(const UUID& client_id, const std::string& key,
                               const TenantId& tenant_id, Replica& replica)
    -> tl::expected<bool, ErrorCode> {
    const auto client_liveness = FindClientRecord(client_id);
    auto retaining_guard = client_liveness
                               ? client_liveness->TryAcquireRetainingGuard()
                               : std::nullopt;
    if (!retaining_guard) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    replica.bindClientLiveness(client_liveness);
    return AddReplicaForRetainedClient(client_id, key, tenant_id, replica);
}

auto MasterService::AddReplicaForRetainedClient(const UUID& client_id,
                                                const std::string& key,
                                                const TenantId& tenant_id,
                                                Replica& replica)
    -> tl::expected<bool, ErrorCode> {
    assert(tenant_id.IsValid());
    TenantId normalized_tenant;
    std::unique_lock<std::mutex> policy_lock(tenant_quota_policy_mutex_,
                                             std::defer_lock);
    if (enable_multi_tenants_) {
        policy_lock.lock();
        auto normalized_tenant_result =
            ResolveTenantIdForWriteLocked(tenant_id);
        if (!normalized_tenant_result) {
            return tl::make_unexpected(normalized_tenant_result.error());
        }
        normalized_tenant = std::move(normalized_tenant_result.value());
    }
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    // Same admission rule as NotifyOffloadSuccess's existing-object path,
    // checked inside the same shared-lock section as the write below: a disk
    // replica may only be registered for a client whose LOCAL_DISK segment
    // entry still exists, so a registration cannot land after a concurrent
    // UnmountLocalDiskSegment's sweep and survive as a stale owner. Scoped
    // to enable_offload_, where the segment registry exists and a
    // deregistration can race; with the subsystem off both the mount and
    // unmount RPCs refuse, so there is nothing to check against and no race
    // to close.
    if (enable_offload_ && !HasMountedLocalDiskSegment(client_id)) {
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    const ObjectIdentity object_id{std::move(normalized_tenant), key};
    const auto outcome = WithObjectMetadataForWriteAndCleanup(
        object_id,
        [&](metadata::Tenant&, const std::shared_ptr<ObjectEntry>&,
            ObjectMetadata& metadata,
            ObjectEntry::State&) -> tl::expected<bool, ErrorCode> {
            const auto previous_kv_media = KvMediaSnapshot(metadata);
            if (replica.type() != ReplicaType::LOCAL_DISK) {
                LOG(ERROR) << "Invalid replica type: " << replica.type()
                           << ". Expected ReplicaType::LOCAL_DISK.";
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }

            const bool replacing_existing =
                metadata.HasReplica(&Replica::fn_is_local_disk_replica);

            if (enable_oplog_ && ordered_oplog_writer_) {
                std::vector<Replica::Descriptor> post;
                for (const auto& existing : metadata.GetAllReplicas()) {
                    if (existing.status() != ReplicaStatus::COMPLETE) continue;
                    if (replacing_existing &&
                        existing.type() == ReplicaType::LOCAL_DISK &&
                        existing.get_descriptor()
                                .get_local_disk_descriptor()
                                .client_id == client_id) {
                        // Substitute with the updated descriptor.
                        Replica::Descriptor updated = existing.get_descriptor();
                        updated.get_local_disk_descriptor().transport_endpoint =
                            replica.get_descriptor()
                                .get_local_disk_descriptor()
                                .transport_endpoint;
                        updated.get_local_disk_descriptor().object_size =
                            replica.get_descriptor()
                                .get_local_disk_descriptor()
                                .object_size;
                        post.push_back(std::move(updated));
                    } else {
                        post.push_back(existing.get_descriptor());
                    }
                }
                if (!replacing_existing) {
                    // The new LOCAL_DISK replica is COMPLETE upon AddReplica.
                    post.push_back(replica.get_descriptor());
                }

                auto persist_result = AppendOpLogVisibleBeforeDurable(
                    OpType::PUT_END, object_id.tenant_id.value(), key,
                    SerializeMetadataForOpLogFromReplicaDescriptors(metadata,
                                                                    post));
                if (!persist_result) {
                    return tl::make_unexpected(persist_result.error());
                }
            }

            if (!replacing_existing) {
                std::vector<Replica> replicas;
                replicas.emplace_back(std::move(replica));
                metadata.AddReplicas(std::move(replicas));
                SyncCacheTotalAccounting(metadata);
                SyncKvObjectState(key, metadata, object_id.tenant_id,
                                  previous_kv_media);
                return true;
            }

            metadata.VisitReplicas(
                [client_id](const Replica& rep) {
                    return rep.type() == ReplicaType::LOCAL_DISK &&
                           rep.get_descriptor()
                                   .get_local_disk_descriptor()
                                   .client_id == client_id;
                },
                [&replica](Replica& rep) {
                    rep.get_descriptor()
                        .get_local_disk_descriptor()
                        .transport_endpoint = replica.get_descriptor()
                                                  .get_local_disk_descriptor()
                                                  .transport_endpoint;
                    rep.get_descriptor()
                        .get_local_disk_descriptor()
                        .object_size = replica.get_descriptor()
                                           .get_local_disk_descriptor()
                                           .object_size;
                });
            return false;
        });
    if (outcome.has_value()) {
        return *outcome;
    }
    // Nothing is published for this key, so this offload completion is what
    // brings the object back. The envelope is built with its replica already
    // attached and published once, fully set up, so a reader that reaches the
    // object finds the disk replica that names it.
    if (replica.type() != ReplicaType::LOCAL_DISK) {
        LOG(ERROR) << "Invalid replica type: " << replica.type()
                   << ". Expected ReplicaType::LOCAL_DISK.";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const uint64_t object_size =
        replica.get_descriptor().get_local_disk_descriptor().object_size;
    std::vector<Replica> replicas;
    replicas.emplace_back(std::move(replica));
    auto envelope = std::make_unique<ObjectMetadata>(
        client_id, std::chrono::system_clock::now(), object_size,
        std::move(replicas), std::nullopt, false, ObjectDataType::UNKNOWN,
        std::string(), object_id.tenant_id, object_id.user_key);
    auto entry = std::make_shared<ObjectEntry>(std::move(envelope));
    entry->WithExclusiveAccess(
        [&](ObjectMetadata& metadata, ObjectEntry::State&) {
            // The one replica is already in the envelope, so the medium and
            // the cache total are accounted for under the entry's own lock
            // before the publish makes the object reachable.
            SyncCacheTotalAccounting(metadata);
            SyncKvObjectState(key, metadata, object_id.tenant_id);
        });
    const auto tenant = GetOrCreateTenantHandle(object_id.tenant_id);
    if (!tenant->InsertObject(entry)) {
        // A concurrent publish took the key: its own registration stands and
        // this replica was registered nowhere, so the accounting added here is
        // given back and the caller learns the object is not there.
        entry->WithExclusiveAccess(
            [](ObjectMetadata& metadata, ObjectEntry::State&) {
                AccountCacheTotalRemoval(metadata);
            });
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    if (enable_oplog_ && ordered_oplog_writer_) {
        // The object this completion created carries exactly this replica, so
        // the record lists the object's own replicas.
        const auto persist_result = entry->WithExclusiveAccess(
            [&](ObjectMetadata& metadata, ObjectEntry::State&) {
                std::vector<Replica::Descriptor> post;
                const auto& stored_replicas = metadata.GetAllReplicas();
                post.reserve(stored_replicas.size());
                for (const auto& stored : stored_replicas) {
                    post.push_back(stored.get_descriptor());
                }
                return AppendOpLogVisibleBeforeDurable(
                    OpType::PUT_END, object_id.tenant_id.value(), key,
                    SerializeMetadataForOpLogFromReplicaDescriptors(metadata,
                                                                    post));
            });
        if (!persist_result) {
            return tl::make_unexpected(persist_result.error());
        }
    }
    return true;
}

auto MasterService::PutRevoke(const UUID& client_id, const std::string& key,
                              const TenantId& tenant_id,
                              ReplicaType replica_type)
    -> tl::expected<void, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    const auto object_id = MakeObjectIdentityForRequest(key, tenant_id);
    const auto outcome = WithObjectMetadataForWriteAndCleanup(
        object_id,
        [&](metadata::Tenant& tenant, const std::shared_ptr<ObjectEntry>& entry,
            ObjectMetadata& metadata,
            ObjectEntry::State& state) -> tl::expected<void, ErrorCode> {
            // No usable replica: answered as absent, like the read APIs.
            if (!metadata.IsValid()) {
                LOG(INFO) << "key=" << key << ", info=object_not_found";
                return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
            }

            const auto previous_kv_media = KvMediaSnapshot(metadata);
            if (client_id != metadata.client_id) {
                LOG(ERROR) << "Illegal client " << client_id
                           << " to PutRevoke key " << key
                           << ", was PutStart-ed by " << metadata.client_id;
                return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
            }

            if (!state.is_processing) {
                LOG(ERROR) << "key=" << key
                           << ", error=no_primary_write_in_progress";
                return tl::make_unexpected(ErrorCode::INVALID_WRITE);
            }

            auto processing_rep = metadata.GetFirstReplica(
                [replica_type](const Replica& replica) {
                    if (replica_type == ReplicaType::ALL) {
                        return (replica.is_memory_replica() ||
                                replica.is_nof_replica() ||
                                replica.is_dfs_replica()) &&
                               !replica.is_processing();
                    }
                    return replica.type() == replica_type &&
                           !replica.is_processing();
                });
            if (processing_rep != nullptr) {
                LOG(ERROR) << "key=" << key
                           << ", status=" << processing_rep->status()
                           << ", error=invalid_replica_status";
                return tl::make_unexpected(ErrorCode::INVALID_WRITE);
            }

            auto target_pred = [replica_type](const Replica& r) {
                if (!r.is_processing()) {
                    return false;
                }
                if (replica_type == ReplicaType::ALL) {
                    return r.is_memory_replica() || r.is_nof_replica() ||
                           r.is_dfs_replica();
                }
                return r.type() == replica_type;
            };

            if (enable_oplog_ && ordered_oplog_writer_) {
                auto remaining =
                    BuildRemainingReplicaDescriptors(metadata, target_pred);
                std::vector<ReplicaID> removed_ids;
                auto reservation = ReserveBatchOpLogSlot();
                if (!reservation) {
                    return tl::make_unexpected(reservation.error());
                }
                metadata.VisitReplicas(target_pred, [&removed_ids](Replica& r) {
                    removed_ids.push_back(r.id());
                    r.mark_removed();
                });
                metadata.ClearPendingSoftPinIfNoViableReplica();

                tl::expected<OpLogEntry, ErrorCode> persist_result;
                if (remaining.empty()) {
                    persist_result = AppendReservedOpLogWithDurableFinalize(
                        std::move(reservation.value()), OpType::REMOVE,
                        tenant_id.value(), key, {},
                        [this, entry, removed_ids = std::move(removed_ids)](
                            const OpLogEntry& durable_entry) {
                            FinalizeRemovedReplicasAfterDurable(
                                entry, durable_entry, removed_ids,
                                QuotaEraseMode::kFull);
                        });
                } else {
                    persist_result = AppendReservedOpLogWithDurableFinalize(
                        std::move(reservation.value()), OpType::PUT_END,
                        tenant_id.value(), key,
                        SerializeMetadataForOpLogFromReplicaDescriptors(
                            metadata, remaining),
                        [this, entry, removed_ids = std::move(removed_ids)](
                            const OpLogEntry& durable_entry) {
                            FinalizeRemovedReplicasAfterDurable(
                                entry, durable_entry, removed_ids,
                                QuotaEraseMode::kFull);
                        });
                }
                if (!persist_result) {
                    return tl::make_unexpected(persist_result.error());
                }
                return {};
            }

            const uint64_t before_charge = CompletedMemoryQuotaCharge(metadata);
            EraseReplicasWithCacheTotalAccounting(metadata, target_pred);
            metadata.ClearPendingSoftPinIfNoViableReplica();
            const uint64_t after_charge = CompletedMemoryQuotaCharge(metadata);
            if (enable_multi_tenants_ && before_charge > after_charge) {
                auto release_result = metadata.quota_ledger.ReleaseCommitted(
                    GetBoundTenantQuotaHandle(tenant),
                    before_charge - after_charge);
                if (!release_result) {
                    LogTenantQuotaLedgerError(release_result,
                                              "release_committed",
                                              object_id.tenant_id, key);
                    return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
                }
            }
            if (!metadata.IsValid()) {
                // The revoke dropped the write's last replica, so the object
                // goes with it: one teardown releases the refcounts, quota
                // charges and KV removal events this publication still owns.
                (void)EraseMetadata(tenant, entry, metadata, state,
                                    object_id.tenant_id, QuotaEraseMode::kFull);
                return {};
            }

            SyncKvObjectState(key, metadata, object_id.tenant_id,
                              previous_kv_media);

            auto settle_result =
                SettlePrimaryWriteQuotaIfReady(tenant, metadata);
            if (!settle_result) {
                return tl::make_unexpected(settle_result.error());
            }

            // If the object is completed, remove it from the processing set.
            if (metadata.AllReplicas(&Replica::fn_is_completed) &&
                state.is_processing) {
                state.is_processing = false;
            }

            return {};
        });
    if (!outcome.has_value()) {
        LOG(INFO) << "key=" << key << ", info=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    return *outcome;
}

auto MasterService::PutEnd(const UUID& client_id, const std::string& key,
                           const TenantId& tenant_id, ReplicaType replica_type)
    -> tl::expected<void, ErrorCode> {
    return PutEnd(client_id, ObjectMeta{key, std::nullopt}, tenant_id,
                  replica_type);
}

std::vector<tl::expected<void, ErrorCode>> MasterService::BatchPutEnd(
    const UUID& client_id, const std::vector<ObjectMeta>& object_metas,
    const TenantId& tenant_id, ReplicaType replica_type) {
    assert(tenant_id.IsValid());
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(object_metas.size());
    for (const auto& object_meta : object_metas) {
        results.emplace_back(
            PutEnd(client_id, object_meta, tenant_id, replica_type));
    }
    return results;
}

std::vector<tl::expected<void, ErrorCode>> MasterService::BatchPutRevoke(
    const UUID& client_id, const std::vector<std::string>& keys,
    const TenantId& tenant_id, ReplicaType replica_type) {
    assert(tenant_id.IsValid());
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
//   Case B: key exists, same size, no readers → in-place update
//   Case C: key exists, different size, bucket DFS, or leased memory → allocate
//           new buffers
//
// Before reaching Case B/C the function runs safety checks and may preempt
// an in-progress Put/Upsert on the same key.  Preempted PROCESSING replicas
// are moved to discarded_replicas_ for delayed release (the previous writer
// may still be performing RDMA writes to those buffers).
//
// Note: during Case B the key is temporarily unreadable (all replicas are
// PROCESSING).  Readers will get REPLICA_IS_NOT_READY until UpsertEnd.
auto MasterService::UpsertStart(const UUID& client_id, const std::string& key,
                                const TenantId& tenant_id,
                                const uint64_t slice_length,
                                const ReplicateConfig& config)
    -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode> {
    const auto object_id = MakeObjectIdentityForRequest(key, tenant_id);
    // --- Parameter validation (same as PutStart) ---
    if ((config.replica_num == 0 && config.nof_replica_num == 0 &&
         config.dfs_replica_num == 0) ||
        key.empty() || slice_length == 0) {
        LOG(ERROR) << "key=" << key << ", replica_num=" << config.replica_num
                   << ", nof_replica_num=" << config.nof_replica_num
                   << ", dfs_replica_num=" << config.dfs_replica_num
                   << ", slice_length=" << slice_length
                   << ", key_size=" << key.size() << ", error=invalid_params";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (config.dfs_replica_num > 1 ||
        (config.dfs_replica_num > 0 && config.replica_num == 0)) {
        LOG(ERROR) << "key=" << key << ", replica_num=" << config.replica_num
                   << ", dfs_replica_num=" << config.dfs_replica_num
                   << ", error=invalid_dfs_replica_config";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (config.dfs_replica_num > 0 && !object_id.tenant_id.IsDefault()) {
        LOG(ERROR) << "key=" << key << ", tenant_id=" << tenant_id
                   << ", error=dfs_currently_requires_default_tenant";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (config.dfs_replica_num > 0 &&
        (!enable_dfs_ || dfs_allocator_ == nullptr ||
         !dfs_allocator_->IsInitialized())) {
        LOG(ERROR) << "key=" << key
                   << ", dfs_replica_num=" << config.dfs_replica_num
                   << ", error=dfs_service_unavailable";
        return tl::make_unexpected(ErrorCode::DFS_SERVICE_UNAVAILABLE);
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

    auto soft_pin_request = ResolveSoftPinRequest(config);
    if (!soft_pin_request) {
        return tl::make_unexpected(soft_pin_request.error());
    }

    UpdateClientHostId(client_id, config.host_id);
    std::string writer_host_id;
    if ((allocation_strategy_type_ == AllocationStrategyType::LOCAL_FIRST ||
         config.prefer_alloc_in_same_node) &&
        config.replica_num == 1) {
        writer_host_id = config.host_id.empty() ? GetClientHostId(client_id)
                                                : config.host_id;
    }

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
    bool dfs_allocation_failed = false;
    // What the read-modify-write region on the routed entry answered; empty
    // when that entry was erased or is no longer routed (Case A below).
    using PutAllocation =
        tl::expected<std::vector<Replica::Descriptor>, ErrorCode>;
    std::optional<PutAllocation> settled;
    // Case C's handoff. Its erase ends the entry's lock, so publishing the
    // replacement cannot run inside the region that erased the old object.
    bool reallocating = false;
    ReplicateConfig merged_config;
    std::string existing_group_id;
    std::optional<std::chrono::system_clock::time_point>
        committed_soft_pin_timeout;
    std::optional<std::vector<Replica>> replacement_replicas;
    uint64_t replacement_pending_quota_charge = 0;
    bool has_replacement_charge = false;
    TenantQuotaLedger replacement_charge;
    std::optional<std::chrono::system_clock::time_point>
        case_a_committed_soft_pin_timeout;
    // Read under client_mutex_ and held for the rest of the operation.
    std::unordered_set<UUID, boost::hash<UUID>> retaining_clients;
    std::chrono::system_clock::time_point now;

    // The whole read-modify-write runs under the entry's own lock, so a
    // concurrent Get/PutEnd/Remove on this key serializes behind it.
    auto rewrite = [&](metadata::Tenant& tenant,
                       const std::shared_ptr<ObjectEntry>& entry,
                       ObjectMetadata& metadata, ObjectEntry::State& state) {
        // --- Step 0: stale handle cleanup ---
        auto cleanup_plan =
            BuildStaleHandleCleanupPlan(metadata, retaining_clients);
        if (!cleanup_plan.removed_ids.empty()) {
            auto persist_result = PersistStaleHandleCleanupForHA(
                "UpsertStart(stale cleanup)", object_id.tenant_id, entry, key,
                metadata, cleanup_plan);
            if (!persist_result) {
                settled.emplace(tl::make_unexpected(persist_result.error()));
                return;
            }
            if (enable_oplog_) {
                settled.emplace(
                    tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS));
                return;
            }
            if (CleanupStaleHandles(key, object_id.tenant_id, tenant, metadata,
                                    state, retaining_clients)) {
                // EraseMetadata handles is_processing, replication_task,
                // offloading_task (with dec_refcnt), and promotion task
                // cleanup.
                (void)EraseMetadata(tenant, entry, metadata, state,
                                    object_id.tenant_id, QuotaEraseMode::kFull);
                return;
            }
        }

        // --- Step 1: safety checks and preemption ---
        // Reject if the caller tries to change group membership. Group
        // membership is immutable while an object exists.
        if (config.group_ids.has_value() && metadata.group_id != group_id) {
            LOG(ERROR) << "key=" << key
                       << ", error=group_membership_is_immutable";
            settled.emplace(tl::make_unexpected(ErrorCode::INVALID_PARAMS));
            return;
        }

        // Reject if a Copy/Move task is actively reading this key's replicas.
        if (state.replication_task.has_value()) {
            LOG(INFO) << "key=" << key << ", error=object_has_replication_task";
            settled.emplace(
                tl::make_unexpected(ErrorCode::OBJECT_HAS_REPLICATION_TASK));
            return;
        }

        if (state.promotion_task.has_value()) {
            LOG(INFO) << "key=" << key << ", error=object_has_promotion_task";
            settled.emplace(
                tl::make_unexpected(ErrorCode::OBJECT_HAS_REPLICATION_TASK));
            return;
        }

        // An in-place Upsert returns and overwrites the existing buffers, so
        // every completed Client-owned target must still be serving before any
        // queue or metadata state is changed.
        if (metadata.size == slice_length &&
            metadata.HasReplica([](const Replica& replica) {
                if (!replica.is_completed() ||
                    (!replica.is_memory_replica() &&
                     !replica.is_local_disk_replica())) {
                    return false;
                }
                Replica::Descriptor descriptor;
                return !replica.getDescriptorIfAvailable(descriptor);
            })) {
            settled.emplace(
                tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS));
            return;
        }

        // Cancel a still-queued offload so the upsert can take the key over.
        // Once a store worker owns the task it is reading the source buffer, so
        // the upsert waits for NotifyOffloadSuccess to clear the marker.
        if (!CancelQueuedOffloadTask(metadata, state, object_id)) {
            LOG(INFO) << "key=" << key << ", error=object_has_offloading_task";
            settled.emplace(
                tl::make_unexpected(ErrorCode::OBJECT_HAS_REPLICATION_TASK));
            return;
        }

        // Preempt an in-progress Put/Upsert on the same key: the previous
        // writer's PROCESSING replicas move to discarded_replicas_ with a TTL,
        // so they are not freed while that writer may still write over RDMA.
        // UpsertStart preempts immediately; PutStart waits for a timeout.
        if (state.is_processing) {
            auto processing_replicas =
                metadata.PopReplicas(&Replica::fn_is_processing);
            metadata.ClearPendingSoftPinAction();
            if (!processing_replicas.empty()) {
                FreeDfsReplicas(key, processing_replicas);
                std::lock_guard lock(discarded_replicas_mutex_);
                discarded_replicas_.emplace_back(
                    std::move(processing_replicas),
                    now + put_start_release_timeout_sec_);
            }
            state.is_processing = false;

            // If no COMPLETE replicas survive the preemption, this key
            // effectively does not exist — fall through to Case A.
            if (!metadata.HasReplica(&Replica::fn_is_completed)) {
                case_a_committed_soft_pin_timeout =
                    metadata.GetCommittedSoftPinTimeout();
                if (case_a_committed_soft_pin_timeout &&
                    *case_a_committed_soft_pin_timeout <= now) {
                    case_a_committed_soft_pin_timeout.reset();
                }
                (void)EraseMetadata(tenant, entry, metadata, state,
                                    object_id.tenant_id, QuotaEraseMode::kFull);
                return;
            }
            auto settle_result =
                SettlePrimaryWriteQuotaIfReady(tenant, metadata);
            if (!settle_result) {
                settled.emplace(tl::make_unexpected(settle_result.error()));
                return;
            }
        }

        // --- Step 2: key exists with COMPLETE replicas → Case B or C ---
        // Reject if any reader holds a reference (refcnt > 0).
        // Overwriting a buffer that an RDMA read is streaming from
        // would cause data corruption. The client should retry after
        // readers finish.
        if (metadata.HasReplica(&Replica::fn_is_busy)) {
            LOG(INFO) << "key=" << key << ", error=object_replica_busy";
            settled.emplace(
                tl::make_unexpected(ErrorCode::OBJECT_REPLICA_BUSY));
            return;
        }

        // A bucket allocator hands out extents that cannot be rewritten in
        // place, so a key whose DFS replica lives in one of its buckets takes
        // the reallocating case below even when the size is unchanged. The same
        // holds for a replica that bucket eviction already took: the requested
        // topology is restored by allocating it again.
        bool restore_missing_bucket_dfs = false;
        if (metadata.size == slice_length) {
            // Validate same-size DFS topology before changing storage.
            const size_t existing_dfs_replicas =
                metadata.CountReplicas(&Replica::fn_is_dfs_replica);
            if (config.dfs_replica_num > 0 || existing_dfs_replicas > 0) {
                const size_t existing_memory_replicas =
                    metadata.CountReplicas(&Replica::fn_is_memory_replica);
                const size_t existing_nof_replicas =
                    metadata.CountReplicas(&Replica::fn_is_nof_replica);
                restore_missing_bucket_dfs =
                    bucket_allocator_ != nullptr &&
                    existing_memory_replicas == config.replica_num &&
                    existing_nof_replicas == config.nof_replica_num &&
                    existing_dfs_replicas == 0 && config.dfs_replica_num > 0;
                if (existing_memory_replicas != config.replica_num ||
                    existing_nof_replicas != config.nof_replica_num ||
                    (existing_dfs_replicas != config.dfs_replica_num &&
                     !restore_missing_bucket_dfs)) {
                    LOG(ERROR)
                        << "key=" << key
                        << ", error=dfs_upsert_topology_mismatch"
                        << ", existing_memory=" << existing_memory_replicas
                        << ", requested_memory=" << config.replica_num
                        << ", existing_nof=" << existing_nof_replicas
                        << ", requested_nof=" << config.nof_replica_num
                        << ", existing_dfs=" << existing_dfs_replicas
                        << ", requested_dfs=" << config.dfs_replica_num;
                    settled.emplace(
                        tl::make_unexpected(ErrorCode::INVALID_PARAMS));
                    return;
                }
            }
        }

        const bool has_read_lease = !metadata.IsLeaseExpired(now);
        if (has_read_lease && metadata.HasReplica([](const Replica& replica) {
                return !replica.is_memory_replica();
            })) {
            // File-backed reads can resolve storage by key, so they
            // cannot retain an old version across replacement.
            settled.emplace(tl::make_unexpected(ErrorCode::OBJECT_HAS_LEASE));
            return;
        }

        const bool has_bucket_dfs_replica =
            bucket_allocator_ != nullptr &&
            metadata.HasReplica(&Replica::fn_is_dfs_replica);
        if (metadata.size == slice_length && !has_read_lease &&
            !has_bucket_dfs_replica && !restore_missing_bucket_dfs) {
            metadata.client_id = client_id;
            metadata.put_start_time = now;

            const auto previous_kv_media = KvMediaSnapshot(metadata);

            // Mark COMPLETE → PROCESSING so readers won't see stale
            // data mid-transfer.  The key becomes unreadable until
            // UpsertEnd.
            std::unordered_set<ReplicaID> eligible_replica_ids;
            metadata.VisitReplicas(
                &Replica::fn_is_completed,
                [&eligible_replica_ids](Replica& replica) {
                    eligible_replica_ids.insert(replica.id());
                    replica.mark_processing();
                });
            metadata.BeginSoftPinAction(*soft_pin_request,
                                        std::move(eligible_replica_ids));
            SyncCacheTotalAccounting(metadata);
            SyncKvObjectState(key, metadata, object_id.tenant_id,
                              previous_kv_media);

            state.is_processing = true;

            // Return the existing descriptors — same buffer addresses
            // as before.
            std::vector<Replica::Descriptor> replica_list;
            const auto& all_replicas = metadata.GetAllReplicas();
            replica_list.reserve(all_replicas.size());
            for (const auto& replica : all_replicas) {
                replica_list.emplace_back(replica.get_descriptor());
            }

            VLOG(1) << "key=" << key << ", action=upsert_start_case_b_inplace";
            settled.emplace(std::move(replica_list));
            return;
        }

        // --- Case C: different size or active readers — reallocate
        // --- Old buffers cannot be reused.  Move them to
        // discarded_replicas_ for delayed release (readers may still
        // hold descriptors without refcnt), then allocate fresh buffers
        // at the new size.
        //
        // Preserve hard_pin and soft_pin from the old metadata so that
        // eviction protection survives a size-changing upsert (RFC
        // §2.2.2).
        merged_config = config;
        merged_config.with_hard_pin =
            merged_config.with_hard_pin || metadata.IsHardPinned();
        committed_soft_pin_timeout = metadata.GetCommittedSoftPinTimeout();
        if (committed_soft_pin_timeout && *committed_soft_pin_timeout <= now) {
            committed_soft_pin_timeout.reset();
        }

        existing_group_id = metadata.group_id;
        const auto previous_kv_media = KvMediaSnapshot(metadata);
        auto* quota_account = GetBoundTenantQuotaHandle(tenant);
        has_replacement_charge = enable_multi_tenants_ &&
                                 metadata.quota_ledger.TotalChargedBytes() != 0;

        // A leased memory snapshot still references the old replica
        // descriptors. Allocate the replacement before removing the
        // old metadata so an allocation failure leaves the old object
        // readable instead of turning it into OBJECT_NOT_FOUND.
        if (has_read_lease) {
            replacement_pending_quota_charge =
                RequestedMemoryQuotaCharge(slice_length, merged_config);
            auto quota_result = ChargeTenantQuota(
                quota_account, replacement_pending_quota_charge);
            if (!quota_result) {
                settled.emplace(tl::make_unexpected(quota_result.error()));
                return;
            }
            auto allocation_result = AllocateReplicas(
                key, slice_length, merged_config, writer_host_id);
            if (!allocation_result) {
                ReleaseTenantQuota(quota_account,
                                   replacement_pending_quota_charge);
                settled.emplace(tl::make_unexpected(allocation_result.error()));
                return;
            }
            replacement_replicas.emplace(std::move(allocation_result.value()));
        }

        if (has_replacement_charge) {
            auto transfer_result =
                metadata.quota_ledger.TransferReplacementCharge(
                    quota_account, replacement_charge);
            if (!transfer_result) {
                LogTenantQuotaLedgerError(transfer_result,
                                          "transfer_replacement_out",
                                          object_id.tenant_id, key);
                if (replacement_replicas.has_value()) {
                    FreeDfsReplicas(key, *replacement_replicas);
                    ReleaseTenantQuota(quota_account,
                                       replacement_pending_quota_charge);
                }
                settled.emplace(tl::make_unexpected(ErrorCode::INTERNAL_ERROR));
                return;
            }
        }
        // A query granted before this write lock can retain its
        // descriptor for a full read TTL. Keep the old allocation for
        // at least that long as well as the writer grace period.
        const auto release_at =
            std::chrono::system_clock::now() +
            std::max(std::chrono::milliseconds(default_kv_lease_ttl_),
                     std::chrono::duration_cast<std::chrono::milliseconds>(
                         put_start_release_timeout_sec_));
        auto old_replicas = PopReplicasWithCacheTotalAccounting(metadata);
        if (!old_replicas.empty()) {
            FreeDfsReplicas(key, old_replicas);
            std::lock_guard lock(discarded_replicas_mutex_);
            discarded_replicas_.emplace_back(std::move(old_replicas),
                                             release_at);
        }
        // The erase ends this callback: the replacement is published after it,
        // once the entry's lock is gone, so a reader in between finds the key
        // absent. Only this call's own per-key operation lock keeps another
        // PutStart or UpsertStart out of that window.
        (void)EraseMetadata(tenant, entry, metadata, state, object_id.tenant_id,
                            QuotaEraseMode::kPreserveOld, previous_kv_media);
        reallocating = true;
    };

    auto admit =
        [&]() -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode> {
        now = std::chrono::system_clock::now();
        {
            // --- Lock acquisition ---
            std::shared_lock<std::shared_mutex> client_lock(client_mutex_);
            std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
            retaining_clients = GetRetainingClientIdsLocked();
            client_lock.unlock();
            // group_id does not affect routing: the key is the only thing the
            // tenant's route is indexed by.
            const auto tenant = GetOrCreateTenantHandle(object_id.tenant_id);
            auto admission_result =
                ChargeTenantQuota(GetBoundTenantQuotaHandle(*tenant), 0);
            if (!admission_result) {
                return tl::make_unexpected(admission_result.error());
            }

            // The read-modify-write region runs under the entry's own lock; a
            // key this tenant does not publish is left to Case A below.
            (void)WithObjectMetadataForWriteAndCleanup(
                object_id,
                [&](metadata::Tenant& published_tenant,
                    const std::shared_ptr<ObjectEntry>& entry,
                    ObjectMetadata& metadata,
                    ObjectEntry::State& state) -> void {
                    rewrite(published_tenant, entry, metadata, state);
                });

            if (settled.has_value()) {
                return std::move(*settled);
            }

            if (reallocating) {
                VLOG(1) << "key=" << key
                        << ", action=upsert_start_case_c_reallocate";
                auto* quota_account = GetBoundTenantQuotaHandle(*tenant);
                tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
                    allocate_result =
                        replacement_replicas.has_value()
                            ? InsertMetadata(
                                  *tenant, client_id, key, slice_length,
                                  merged_config, existing_group_id,
                                  object_id.tenant_id, now, *soft_pin_request,
                                  std::move(*replacement_replicas),
                                  replacement_pending_quota_charge,
                                  committed_soft_pin_timeout)
                            : AllocateAndInsertMetadata(
                                  *tenant, client_id, key, slice_length,
                                  merged_config, writer_host_id,
                                  existing_group_id, object_id.tenant_id, now,
                                  *soft_pin_request, committed_soft_pin_timeout,
                                  &dfs_allocation_failed);
                if (!allocate_result) {
                    if (replacement_replicas.has_value()) {
                        ReleaseTenantQuota(quota_account,
                                           replacement_pending_quota_charge);
                    }
                    if (has_replacement_charge) {
                        auto rollback_result =
                            replacement_charge.ReleaseReplacement(
                                quota_account);
                        LogTenantQuotaLedgerError(rollback_result,
                                                  "rollback_replacement",
                                                  object_id.tenant_id, key);
                    }
                    return allocate_result;
                }
                auto new_entry = tenant->Get(key);
                if (has_replacement_charge) {
                    if (new_entry == nullptr) {
                        auto rollback_result =
                            replacement_charge.ReleaseReplacement(
                                quota_account);
                        LogTenantQuotaLedgerError(rollback_result,
                                                  "rollback_replacement",
                                                  object_id.tenant_id, key);
                        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
                    }
                    auto transfer_result = new_entry->WithExclusiveAccess(
                        [&](ObjectMetadata& new_metadata, ObjectEntry::State&) {
                            return replacement_charge.TransferReplacementCharge(
                                quota_account, new_metadata.quota_ledger);
                        });
                    if (!transfer_result) {
                        LogTenantQuotaLedgerError(transfer_result,
                                                  "transfer_replacement_in",
                                                  object_id.tenant_id, key);
                        // Erase the publication this call just made, not
                        // whatever the key resolves to now: a concurrent writer
                        // may have replaced it in between.
                        (void)WithObjectMetadataForWriteAndCleanup(
                            object_id,
                            [&](metadata::Tenant& resolved_tenant,
                                const std::shared_ptr<ObjectEntry>& resolved,
                                ObjectMetadata& resolved_metadata,
                                ObjectEntry::State& resolved_state) -> void {
                                if (resolved != new_entry) {
                                    return;
                                }
                                (void)EraseMetadata(
                                    resolved_tenant, resolved,
                                    resolved_metadata, resolved_state,
                                    object_id.tenant_id, QuotaEraseMode::kFull);
                            });
                        if (replacement_charge.ReplacedBytes() != 0) {
                            auto rollback_result =
                                replacement_charge.ReleaseReplacement(
                                    quota_account);
                            LogTenantQuotaLedgerError(rollback_result,
                                                      "rollback_replacement",
                                                      object_id.tenant_id, key);
                        }
                        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
                    }
                }
                return allocate_result;
            }

            // --- Case A: key does not exist (or was erased above) ---
            // Allocate fresh buffers, identical to PutStart. group_id does not
            // affect routing: the key is the only thing the tenant's route is
            // indexed by.
            VLOG(1) << "key=" << key << ", action=upsert_start_case_a";
            return AllocateAndInsertMetadata(
                *tenant, client_id, key, slice_length, config, writer_host_id,
                group_id, object_id.tenant_id, now, *soft_pin_request,
                std::move(case_a_committed_soft_pin_timeout),
                &dfs_allocation_failed);
        }
    };

    // Over-quota is rejected outright. Making room is the background pass's
    // job (EvictTenantsOverWatermark), not the write path's: the same contract
    // the pool has when enable_multi_tenants is false -- this write fails, the
    // evictor builds headroom, the next one succeeds. Evicting inline instead
    // put a full metadata scan on admission, which every concurrent writer at
    // the ceiling repeated for the same freed extent.
    auto result = admit();
    // Eviction resolves every object through its own tenant, so recover only
    // once admit has released the entry it held. Serialize recovery and
    // re-admit before deciding whether this request still needs to evict one
    // bucket.
    if (!result && dfs_allocation_failed && config.dfs_replica_num > 0 &&
        bucket_allocator_ != nullptr) {
        std::lock_guard recovery_lock(dfs_bucket_recovery_mutex_);
        dfs_allocation_failed = false;
        result = admit();
        if (!result && dfs_allocation_failed &&
            TryRecoverDfsSpaceAfterAllocationFailure()) {
            dfs_allocation_failed = false;
            result = admit();
        }
    }
    if (!result && result.error() == ErrorCode::TENANT_QUOTA_EXCEEDED) {
        MasterMetricManager::instance().inc_tenant_quota_reject(
            object_id.tenant_id.value(), "quota_exceeded");
    }
    return result;
}

auto MasterService::UpsertEnd(const UUID& client_id,
                              const ObjectMeta& object_meta,
                              const TenantId& tenant_id,
                              ReplicaType replica_type)
    -> tl::expected<void, ErrorCode> {
    return PutEnd(client_id, object_meta, tenant_id, replica_type);
}

auto MasterService::UpsertEnd(const UUID& client_id, const std::string& key,
                              const TenantId& tenant_id,
                              ReplicaType replica_type)
    -> tl::expected<void, ErrorCode> {
    return UpsertEnd(client_id, ObjectMeta{key, std::nullopt}, tenant_id,
                     replica_type);
}

auto MasterService::UpsertRevoke(const UUID& client_id, const std::string& key,
                                 const TenantId& tenant_id,
                                 ReplicaType replica_type)
    -> tl::expected<void, ErrorCode> {
    return PutRevoke(client_id, key, tenant_id, replica_type);
}

std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
MasterService::BatchUpsertStart(const UUID& client_id,
                                const std::vector<std::string>& keys,
                                const TenantId& tenant_id,
                                const std::vector<uint64_t>& slice_lengths,
                                const ReplicateConfig& config) {
    assert(tenant_id.IsValid());
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
    const UUID& client_id, const std::vector<ObjectMeta>& object_metas,
    const TenantId& tenant_id) {
    return BatchPutEnd(client_id, object_metas, tenant_id, ReplicaType::ALL);
}

std::vector<tl::expected<void, ErrorCode>> MasterService::BatchUpsertRevoke(
    const UUID& client_id, const std::vector<std::string>& keys,
    const TenantId& tenant_id) {
    return BatchPutRevoke(client_id, keys, tenant_id);
}

auto MasterService::EvictDiskReplica(const UUID& client_id,
                                     const std::string& key,
                                     const TenantId& tenant_id,
                                     ReplicaType replica_type)
    -> tl::expected<void, ErrorCode> {
    const auto object_id = MakeObjectIdentityForRequest(key, tenant_id);
    // One read-modify-write under the entry's own lock: the replicas it drops,
    // the accounting it releases and the removal it publishes cannot interleave
    // with another point operation on the key.
    const auto outcome = WithObjectMetadataForWriteAndCleanup(
        object_id,
        [&](metadata::Tenant& tenant, const std::shared_ptr<ObjectEntry>& entry,
            ObjectMetadata& metadata,
            ObjectEntry::State& state) -> tl::expected<void, ErrorCode> {
            const auto previous_kv_media = KvMediaSnapshot(metadata);

            if (replica_type != ReplicaType::DISK &&
                replica_type != ReplicaType::LOCAL_DISK) {
                LOG(ERROR) << "key=" << key
                           << ", error=invalid_replica_type_for_eviction";
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }

            auto target_pred = [replica_type, &client_id](const Replica& r) {
                if (replica_type == ReplicaType::DISK) {
                    return r.is_disk_replica();
                } else if (replica_type == ReplicaType::LOCAL_DISK) {
                    return r.is_local_disk_replica() &&
                           r.get_descriptor()
                                   .get_local_disk_descriptor()
                                   .client_id == client_id;
                }
                return false;
            };

            if (enable_oplog_ && ordered_oplog_writer_) {
                auto remaining =
                    BuildRemainingReplicaDescriptors(metadata, target_pred);
                if (enable_oplog_) {
                    auto reservation = ReserveBatchOpLogSlot();
                    if (!reservation) {
                        return tl::make_unexpected(reservation.error());
                    }
                    std::vector<ReplicaID> removed_ids;
                    metadata.VisitReplicas(
                        target_pred, [&removed_ids](Replica& replica) {
                            removed_ids.push_back(replica.id());
                            replica.mark_removed();
                        });

                    tl::expected<OpLogEntry, ErrorCode> persist_result;
                    if (remaining.empty()) {
                        persist_result = AppendReservedOpLogWithDurableFinalize(
                            std::move(reservation.value()), OpType::REMOVE,
                            metadata.tenant_id.value(), key, {},
                            [this, entry, removed_ids = std::move(removed_ids)](
                                const OpLogEntry& durable_entry) {
                                FinalizeRemovedReplicasAfterDurable(
                                    entry, durable_entry, removed_ids,
                                    QuotaEraseMode::kFull);
                            });
                    } else {
                        persist_result = AppendReservedOpLogWithDurableFinalize(
                            std::move(reservation.value()), OpType::PUT_END,
                            metadata.tenant_id.value(), key,
                            SerializeMetadataForOpLogFromReplicaDescriptors(
                                metadata, remaining),
                            [this, entry, removed_ids = std::move(removed_ids)](
                                const OpLogEntry& durable_entry) {
                                FinalizeRemovedReplicasAfterDurable(
                                    entry, durable_entry, removed_ids,
                                    QuotaEraseMode::kFull);
                            });
                    }
                    if (!persist_result) {
                        return tl::make_unexpected(persist_result.error());
                    }
                    return {};
                }

                tl::expected<OpLogEntry, ErrorCode> persist_result;
                if (remaining.empty()) {
                    persist_result = AppendOpLogWithDurableFinalize(
                        OpType::REMOVE, metadata.tenant_id.value(), key, {},
                        nullptr);
                } else {
                    persist_result = AppendOpLogWithDurableFinalize(
                        OpType::PUT_END, metadata.tenant_id.value(), key,
                        SerializeMetadataForOpLogFromReplicaDescriptors(
                            metadata, remaining),
                        nullptr);
                }
                if (!persist_result) {
                    return tl::make_unexpected(persist_result.error());
                }
            }

            EraseReplicasWithCacheTotalAccounting(metadata, target_pred);

            SyncKvObjectState(key, metadata, object_id.tenant_id,
                              previous_kv_media);
            if (!metadata.IsValid()) {
                (void)EraseMetadata(tenant, entry, metadata, state,
                                    object_id.tenant_id);
            }
            return {};
        });
    if (!outcome.has_value()) {
        LOG(INFO) << "key=" << key
                  << ", tenant_id=" << object_id.tenant_id.value()
                  << ", info=object_not_found_for_eviction";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    return *outcome;
}

std::vector<tl::expected<void, ErrorCode>> MasterService::BatchEvictDiskReplica(
    const UUID& client_id, const std::vector<std::string>& keys,
    const TenantId& tenant_id, ReplicaType replica_type) {
    assert(tenant_id.IsValid());
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(keys.size());
    for (const auto& key : keys) {
        results.push_back(
            EvictDiskReplica(client_id, key, tenant_id, replica_type));
    }
    return results;
}

tl::expected<CopyStartResponse, ErrorCode> MasterService::CopyStart(
    const UUID& client_id, const std::string& key, const TenantId& tenant_id,
    const std::string& src_segment,
    const std::vector<std::string>& tgt_segments,
    const UUID& dynamic_replication_lease_id,
    uint64_t dynamic_replication_version_epoch) {
    auto normalized_tenant_result = ResolveTenantIdForWrite(tenant_id);
    if (!normalized_tenant_result) {
        return tl::make_unexpected(normalized_tenant_result.error());
    }
    const ObjectIdentity object_id{std::move(normalized_tenant_result.value()),
                                   key};
    const bool dynamic_copy = dynamic_replication_lease_id != UUID{};
    const auto record = FindClientRecord(client_id);
    auto serving_guard =
        record ? record->TryAcquireServingGuard() : std::nullopt;
    if (!serving_guard) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    if (!dynamic_copy) {
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
    // One read-modify-write under the entry's own lock: the source replica it
    // pins, the task it registers and the replicas it attaches cannot
    // interleave with another point operation on the key.
    const auto outcome = WithObjectMetadataForWriteAndCleanup(
        object_id,
        [&](metadata::Tenant& tenant, const std::shared_ptr<ObjectEntry>& entry,
            ObjectMetadata& metadata, ObjectEntry::State& state)
            -> tl::expected<CopyStartResponse, ErrorCode> {
            // No usable replica, so nothing here can be a copy source.
            if (!metadata.IsValid()) {
                LOG(ERROR) << "key=" << key << ", object not found";
                return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
            }

            if (state.replication_task.has_value()) {
                LOG(ERROR) << "key=" << key
                           << " already has an ongoing replication task";
                return tl::make_unexpected(
                    ErrorCode::OBJECT_HAS_REPLICATION_TASK);
            }

            size_t new_replica_count = 0;
            for (const auto& tgt_segment : tgt_segments) {
                if (metadata.GetReplicaBySegmentName(tgt_segment) == nullptr) {
                    ++new_replica_count;
                }
            }

            auto pending_validation = ValidateDynamicReplicaPendingForCopyStart(
                state, dynamic_replication_lease_id, client_id, src_segment,
                DynamicReplicationVersionEpoch(metadata),
                dynamic_replication_version_epoch, tgt_segments);
            if (new_replica_count == 0 && dynamic_copy) {
                if (!pending_validation) {
                    return tl::make_unexpected(pending_validation.error());
                }
                ClearDynamicReplicationStateLocked(object_id.tenant_id, entry,
                                                   state);
                return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
            }
            if (!pending_validation) {
                return tl::make_unexpected(pending_validation.error());
            }
            if (dynamic_copy) {
                ScopedSegmentAccess segment_access =
                    segment_manager_.getSegmentAccess();
                for (const auto& tgt_segment : tgt_segments) {
                    if (!segment_access.ExistsSegmentName(tgt_segment)) {
                        ClearDynamicReplicationStateLocked(object_id.tenant_id,
                                                           entry, state);
                        LOG(ERROR)
                            << "key=" << key << ", tgt_segment=" << tgt_segment
                            << ", error=target_segment_not_found";
                        return tl::make_unexpected(
                            ErrorCode::SEGMENT_NOT_FOUND);
                    }
                    if (!segment_access.IsSegmentAllocatable(tgt_segment)) {
                        ClearDynamicReplicationStateLocked(object_id.tenant_id,
                                                           entry, state);
                        LOG(ERROR)
                            << "key=" << key << ", tgt_segment=" << tgt_segment
                            << ", error=target_segment_not_allocatable";
                        return tl::make_unexpected(
                            ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
                    }
                }
            }

            auto source = metadata.GetReplicaBySegmentName(src_segment);
            if (source == nullptr || !source->is_completed() ||
                source->has_invalid_mem_handle()) {
                LOG(ERROR) << "key=" << key << ", src_segment=" << src_segment
                           << ", replica not found or not valid";
                if (dynamic_copy) {
                    ClearDynamicReplicationStateLocked(object_id.tenant_id,
                                                       entry, state);
                }
                return tl::make_unexpected(ErrorCode::REPLICA_NOT_FOUND);
            }
            Replica::Descriptor source_descriptor;
            if (!TryGetReadableReplicaDescriptor(*source, source_descriptor)) {
                if (dynamic_copy) {
                    ClearDynamicReplicationStateLocked(object_id.tenant_id,
                                                       entry, state);
                }
                return tl::make_unexpected(
                    ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
            }

            const uint64_t pending_quota_charge =
                SaturatingMultiply(static_cast<uint64_t>(metadata.size),
                                   static_cast<uint64_t>(new_replica_count));
            auto quota_result = ChargeTenantQuota(
                GetBoundTenantQuotaHandle(tenant), pending_quota_charge);
            if (!quota_result) {
                if (quota_result.error() == ErrorCode::TENANT_QUOTA_EXCEEDED) {
                    MasterMetricManager::instance().inc_tenant_quota_reject(
                        object_id.tenant_id.value(), "quota_exceeded");
                }
                if (dynamic_copy) {
                    ClearDynamicReplicationStateLocked(object_id.tenant_id,
                                                       entry, state);
                }
                return tl::make_unexpected(quota_result.error());
            }
            auto refund_pending_quota = [&] {
                ReleaseTenantQuota(GetBoundTenantQuotaHandle(tenant),
                                   pending_quota_charge);
            };

            std::vector<Replica> replicas;
            replicas.reserve(new_replica_count);
            std::vector<std::string> new_target_segments;
            new_target_segments.reserve(new_replica_count);
            // Target allocation takes the target owner's guard, so do not keep
            // the executor's guard across it: source and target may be the same
            // Client, and two opposite copies would deadlock on each other's
            // guards. The entry lock keeps terminal cleanup behind this call.
            serving_guard.reset();
            AllocatorManager allocator_snapshot;
            {
                ScopedAllocatorAccess allocator_access =
                    segment_manager_.getAllocatorAccess();
                allocator_snapshot =
                    allocator_access.SnapshotAllocatorManager();
            }

            for (auto& tgt_segment : tgt_segments) {
                if (metadata.GetReplicaBySegmentName(tgt_segment) != nullptr) {
                    // Skip used segments.
                    continue;
                }

                auto replica = allocation_strategy_->AllocateFrom(
                    allocator_snapshot, metadata.size, tgt_segment);
                if (!replica.has_value()) {
                    LOG(ERROR)
                        << "key=" << key << ", tgt_segment=" << tgt_segment
                        << ", failed to allocate replica";
                    refund_pending_quota();
                    if (dynamic_copy) {
                        ClearDynamicReplicationStateLocked(object_id.tenant_id,
                                                           entry, state);
                    }
                    return tl::make_unexpected(replica.error());
                }
                replicas.push_back(std::move(*replica));
                new_target_segments.push_back(tgt_segment);
            }

            CopyStartResponse response;
            response.targets.reserve(replicas.size());
            std::vector<ReplicaID> replica_ids;
            replica_ids.reserve(replicas.size());

            response.source = std::move(source_descriptor);
            for (const auto& replica : replicas) {
                replica_ids.push_back(replica.id());
                Replica::Descriptor descriptor;
                if (!replica.getDescriptorIfAvailable(descriptor)) {
                    refund_pending_quota();
                    if (dynamic_copy) {
                        ClearDynamicReplicationStateLocked(object_id.tenant_id,
                                                           entry, state);
                    }
                    return tl::make_unexpected(
                        ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
                }
                response.targets.push_back(std::move(descriptor));
            }

            // Create replication task for tracking. The task slot was checked
            // above, under this same entry lock.
            state.replication_task.emplace(
                client_id, std::chrono::system_clock::now(),
                ReplicationTask::Type::COPY, source->id(),
                std::move(replica_ids), pending_quota_charge,
                dynamic_replication_lease_id,
                dynamic_replication_version_epoch);

            RegisterDynamicReplicaStart(
                metadata, state, src_segment,
                DynamicReplicationVersionEpoch(metadata), new_target_segments,
                state.replication_task->replica_ids);

            // Increase source refcnt to protect it from eviction.
            source->inc_refcnt();

            // Add replicas to the object.
            // DO NOT ACCESS source AFTER THIS !!!
            metadata.AddReplicas(std::move(replicas));

            return response;
        });
    if (!outcome.has_value()) {
        LOG(ERROR) << "key=" << key << ", object not found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    return *outcome;
}

tl::expected<void, ErrorCode> MasterService::CopyEnd(
    const UUID& client_id, const std::string& key, const TenantId& tenant_id,
    const UUID& dynamic_replication_lease_id,
    uint64_t dynamic_replication_version_epoch) {
    const auto record = FindClientRecord(client_id);
    auto retaining_guard =
        record ? record->TryAcquireRetainingGuard() : std::nullopt;
    if (!retaining_guard) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    const auto object_id = MakeObjectIdentityForRequest(key, tenant_id);
    // One read-modify-write under the entry's own lock: the task it takes
    // ownership of, the quota it settles and the replicas it completes or
    // discards cannot interleave with another point operation on the key.
    const auto outcome = WithObjectMetadataForWriteAndCleanup(
        object_id,
        [&](metadata::Tenant& tenant, const std::shared_ptr<ObjectEntry>& entry,
            ObjectMetadata& metadata,
            ObjectEntry::State& state) -> tl::expected<void, ErrorCode> {
            // No usable replica, so there is no replication task to complete.
            if (!metadata.IsValid()) {
                LOG(ERROR) << "key=" << key << ", error=object_not_found";
                return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
            }

            if (!state.replication_task.has_value()) {
                LOG(ERROR) << "key=" << key
                           << ", error=object has no ongoing replication task";
                return tl::make_unexpected(
                    ErrorCode::OBJECT_NO_REPLICATION_TASK);
            }

            // Taken by value: the task is erased while this frame still runs.
            const ReplicationTask task = *state.replication_task;
            if (task.client_id != client_id) {
                LOG(ERROR) << "Illegal client " << client_id
                           << " to CopyEnd key " << key
                           << ", was CopyStart-ed by " << task.client_id;
                return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
            }

            if (task.type != ReplicationTask::Type::COPY) {
                LOG(ERROR)
                    << "Ongoing replication task type is MOVE instead of COPY";
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            if (task.durable_cleanup_pending) {
                return tl::make_unexpected(
                    ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
            }
            if (task.dynamic_replication_lease_id !=
                    dynamic_replication_lease_id ||
                task.dynamic_replication_version_epoch !=
                    dynamic_replication_version_epoch) {
                LOG(ERROR) << "key=" << key
                           << ", error=dynamic_replication_token_mismatch";
                return tl::make_unexpected(ErrorCode::INVALID_VERSION);
            }

            const auto previous_kv_media = KvMediaSnapshot(metadata);
            auto source_id = task.source_id;
            auto source = metadata.GetReplicaByID(source_id);
            if (source == nullptr || !source->is_completed() ||
                source->has_invalid_mem_handle()) {
                LOG(ERROR)
                    << "key=" << key << ", source_id=" << source_id
                    << ", status="
                    << (source == nullptr ? "nullptr" : "invalid")
                    << ", copy source becomes invalid during data transfer";
                // Release the refcnt taken in CopyStart; this error path must
                // do it too, or the source replica stays pinned and can never
                // be evicted.
                if (source != nullptr) {
                    source->dec_refcnt();
                }
                // Discard target replicas and clear the replication task.
                EraseReplicasWithCacheTotalAccounting(
                    metadata, [&task](const Replica& replica) {
                        return std::find(task.replica_ids.begin(),
                                         task.replica_ids.end(),
                                         replica.id()) !=
                               task.replica_ids.end();
                    });
                ReleaseTenantQuota(GetBoundTenantQuotaHandle(tenant),
                                   task.pending_quota_charge_bytes);
                state.replication_task.reset();
                ClearDynamicReplicationStateLocked(object_id.tenant_id, entry,
                                                   state);
                if (!metadata.IsValid()) {
                    // Remove the object if it does not have any replicas.
                    (void)EraseMetadata(tenant, entry, metadata, state,
                                        object_id.tenant_id);
                }
                return tl::make_unexpected(ErrorCode::REPLICA_IS_GONE);
            }

            // First validate that all target replicas are still healthy. If any
            // replica is invalid we won't be able to mark it complete; this
            // affects the post-mutation descriptor list.
            bool all_complete = true;
            uint64_t completed_quota_charge = 0;
            std::vector<ReplicaID> commit_target_ids;
            std::vector<ReplicaID> failed_target_ids;
            commit_target_ids.reserve(task.replica_ids.size());
            for (const auto& replica_id : task.replica_ids) {
                auto replica = metadata.GetReplicaByID(replica_id);
                if (replica == nullptr || replica->has_invalid_mem_handle()) {
                    LOG(WARNING)
                        << "key=" << key << ", replica_id=" << replica_id
                        << ", copy target becomes invalid during data transfer";
                    all_complete = false;
                    failed_target_ids.push_back(replica_id);
                } else {
                    commit_target_ids.push_back(replica_id);
                }
            }

            std::optional<OrderedOpLogWriter::Reservation> batch_reservation;
            if (enable_ha_ && enable_oplog_) {
                auto reservation = ReserveBatchOpLogSlot();
                if (!reservation) {
                    return tl::make_unexpected(reservation.error());
                }
                batch_reservation = std::move(reservation.value());
            }

            source->dec_refcnt();
            for (const auto& replica_id : commit_target_ids) {
                auto replica = metadata.GetReplicaByID(replica_id);
                if (replica != nullptr) {
                    replica->mark_complete();
                    completed_quota_charge =
                        SaturatingAdd(completed_quota_charge,
                                      static_cast<uint64_t>(metadata.size));
                }
            }

            metadata.MarkDynamicReplicasComplete(commit_target_ids);
            metadata.ForgetDynamicReplicas(failed_target_ids);
            if (!failed_target_ids.empty()) {
                std::unordered_set<ReplicaID> failed_ids(
                    failed_target_ids.begin(), failed_target_ids.end());
                EraseReplicasWithCacheTotalAccounting(
                    metadata, [&failed_ids](const Replica& replica) {
                        return failed_ids.contains(replica.id());
                    });
            }

            ClearDynamicReplicationStateLocked(object_id.tenant_id, entry,
                                               state);

            if (enable_oplog_ && ordered_oplog_writer_) {
                std::vector<Replica::Descriptor> post;
                metadata.VisitReplicas(
                    &Replica::fn_is_completed, [&post](const Replica& replica) {
                        post.push_back(replica.get_descriptor());
                    });
                auto payload = SerializeMetadataForOpLogFromReplicaDescriptors(
                    metadata, post);
                if (batch_reservation) {
                    auto persist_result =
                        AppendReservedOpLogWithDurableFinalize(
                            std::move(*batch_reservation), OpType::PUT_END,
                            tenant_id.value(), key, payload, nullptr);
                    if (!persist_result) {
                        LOG(WARNING)
                            << "CopyEnd: PUT_END persist failed for key=" << key
                            << ", err="
                            << static_cast<int>(persist_result.error());
                    }
                } else {
                    auto persist_result = AppendOpLogVisibleBeforeDurable(
                        OpType::PUT_END, tenant_id.value(), key, payload);
                    if (!persist_result) {
                        LOG(WARNING)
                            << "CopyEnd: PUT_END persist failed for key=" << key
                            << ", err="
                            << static_cast<int>(persist_result.error());
                    }
                }
            }

            SyncCacheTotalAccounting(metadata);

            if (enable_multi_tenants_) {
                auto settle_result = metadata.quota_ledger.SettleAdditional(
                    GetBoundTenantQuotaHandle(tenant),
                    task.pending_quota_charge_bytes, completed_quota_charge);
                if (!settle_result) {
                    LogTenantQuotaLedgerError(settle_result,
                                              "settle_additional",
                                              metadata.tenant_id, key);
                    return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
                }
            }

            state.replication_task.reset();

            SyncKvObjectState(key, metadata, tenant_id, previous_kv_media);

            return all_complete
                       ? tl::expected<void, ErrorCode>()
                       : tl::make_unexpected(ErrorCode::REPLICA_IS_GONE);
        });
    if (!outcome.has_value()) {
        LOG(ERROR) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    return *outcome;
}

tl::expected<void, ErrorCode> MasterService::CopyRevoke(
    const UUID& client_id, const std::string& key, const TenantId& tenant_id,
    const UUID& dynamic_replication_lease_id,
    uint64_t dynamic_replication_version_epoch) {
    const auto record = FindClientRecord(client_id);
    auto retaining_guard =
        record ? record->TryAcquireRetainingGuard() : std::nullopt;
    if (!retaining_guard) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    const auto object_id = MakeObjectIdentityForRequest(key, tenant_id);
    // The revoke runs under the entry's own lock so the task it tears down and
    // the targets it discards stay one read-modify-write.
    const auto outcome = WithObjectMetadataForWriteAndCleanup(
        object_id,
        [&](metadata::Tenant& tenant, const std::shared_ptr<ObjectEntry>& entry,
            ObjectMetadata& metadata,
            ObjectEntry::State& state) -> tl::expected<void, ErrorCode> {
            // No usable replica, so there is no replication task to revoke.
            if (!metadata.IsValid()) {
                LOG(ERROR) << "key=" << key << ", error=object_not_found";
                return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
            }

            if (!state.replication_task.has_value()) {
                LOG(ERROR) << "key=" << key
                           << ", error=object has no ongoing replication task";
                return tl::make_unexpected(
                    ErrorCode::OBJECT_NO_REPLICATION_TASK);
            }

            // Taken by value: the task is erased while this frame still runs.
            const ReplicationTask task = *state.replication_task;
            if (task.client_id != client_id) {
                LOG(ERROR) << "Illegal client " << client_id
                           << " to CopyRevoke key " << key
                           << ", was CopyStart-ed by " << task.client_id;
                return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
            }

            if (task.type != ReplicationTask::Type::COPY) {
                LOG(ERROR)
                    << "Ongoing replication task type is MOVE instead of COPY";
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            if (task.durable_cleanup_pending) {
                return tl::make_unexpected(
                    ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
            }
            if (task.dynamic_replication_lease_id !=
                    dynamic_replication_lease_id ||
                task.dynamic_replication_version_epoch !=
                    dynamic_replication_version_epoch) {
                LOG(ERROR) << "key=" << key
                           << ", error=dynamic_replication_token_mismatch";
                return tl::make_unexpected(ErrorCode::INVALID_VERSION);
            }

            const auto source_id = task.source_id;
            const auto replica_ids = task.replica_ids;
            auto source = metadata.GetReplicaByID(source_id);
            if (source == nullptr) {
                LOG(WARNING) << "key=" << key << ", source_id=" << source_id
                             << ", copy source not found during revoke";
            } else {
                // Decrement source reference count
                source->dec_refcnt();
            }

            // Erase all replica_ids
            for (const auto& replica_id : replica_ids) {
                EraseReplicasWithCacheTotalAccounting(
                    metadata, [&replica_id](const Replica& replica) {
                        return replica.id() == replica_id;
                    });
            }

            ReleaseTenantQuota(GetBoundTenantQuotaHandle(tenant),
                               task.pending_quota_charge_bytes);
            state.replication_task.reset();
            ClearDynamicReplicationStateLocked(object_id.tenant_id, entry,
                                               state);

            if (!metadata.IsValid()) {
                // Remove the object if it does not have any replicas.
                (void)EraseMetadata(tenant, entry, metadata, state,
                                    object_id.tenant_id);
            }

            return {};
        });
    if (!outcome.has_value()) {
        LOG(ERROR) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    return *outcome;
}

tl::expected<MoveStartResponse, ErrorCode> MasterService::MoveStart(
    const UUID& client_id, const std::string& key, const TenantId& tenant_id,
    const std::string& src_segment, const std::string& tgt_segment) {
    const auto object_id = MakeObjectIdentityForRequest(key, tenant_id);
    const auto record = FindClientRecord(client_id);
    auto serving_guard =
        record ? record->TryAcquireServingGuard() : std::nullopt;
    if (!serving_guard) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
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

    // One read-modify-write under the entry's own lock: the source replica it
    // pins, the quota it charges and the task it registers land together.
    const auto outcome = WithObjectMetadataForWriteAndCleanup(
        object_id,
        [&](metadata::Tenant& tenant, const std::shared_ptr<ObjectEntry>& entry,
            ObjectMetadata& metadata, ObjectEntry::State& state)
            -> tl::expected<MoveStartResponse, ErrorCode> {
            // No usable replica, so nothing here can be a move source.
            if (!metadata.IsValid()) {
                LOG(ERROR) << "key=" << key << ", object not found";
                return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
            }

            if (state.replication_task.has_value()) {
                LOG(ERROR) << "key=" << key
                           << " already has an ongoing replication task";
                return tl::make_unexpected(
                    ErrorCode::OBJECT_HAS_REPLICATION_TASK);
            }

            auto source = metadata.GetReplicaBySegmentName(src_segment);
            if (source == nullptr || !source->is_completed() ||
                source->has_invalid_mem_handle()) {
                LOG(ERROR) << "key=" << key << ", src_segment=" << src_segment
                           << ", replica not found or not completed";
                return tl::make_unexpected(ErrorCode::REPLICA_NOT_FOUND);
            }
            Replica::Descriptor source_descriptor;
            if (!TryGetReadableReplicaDescriptor(*source, source_descriptor)) {
                return tl::make_unexpected(
                    ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
            }

            std::vector<Replica> replicas;
            // See CopyStart: target allocation owns its own Client guard, so
            // the entry lock is what keeps terminal cleanup ordered after this
            // admitted MoveStart.
            serving_guard.reset();
            if (metadata.GetReplicaBySegmentName(tgt_segment) == nullptr) {
                const uint64_t pending_quota_charge =
                    SaturatingMultiply(static_cast<uint64_t>(metadata.size), 1);
                auto quota_result = ChargeTenantQuota(
                    GetBoundTenantQuotaHandle(tenant), pending_quota_charge);
                if (!quota_result) {
                    if (quota_result.error() ==
                        ErrorCode::TENANT_QUOTA_EXCEEDED) {
                        MasterMetricManager::instance().inc_tenant_quota_reject(
                            object_id.tenant_id.value(), "quota_exceeded");
                    }
                    return tl::make_unexpected(quota_result.error());
                }
                auto refund_pending_quota = [&] {
                    ReleaseTenantQuota(GetBoundTenantQuotaHandle(tenant),
                                       pending_quota_charge);
                };

                AllocatorManager allocator_snapshot;
                {
                    ScopedAllocatorAccess allocator_access =
                        segment_manager_.getAllocatorAccess();
                    allocator_snapshot =
                        allocator_access.SnapshotAllocatorManager();
                }
                auto replica = allocation_strategy_->AllocateFrom(
                    allocator_snapshot, metadata.size, tgt_segment);
                if (!replica.has_value()) {
                    LOG(ERROR)
                        << "key=" << key << ", tgt_segment=" << tgt_segment
                        << ", failed to allocate replica";
                    refund_pending_quota();
                    return tl::make_unexpected(replica.error());
                }
                replicas.push_back(std::move(*replica));
            } else {
                auto quota_result =
                    ChargeTenantQuota(GetBoundTenantQuotaHandle(tenant), 0);
                if (!quota_result) {
                    return tl::make_unexpected(quota_result.error());
                }
            }

            const uint64_t pending_quota_charge =
                replicas.empty() ? 0
                                 : SaturatingMultiply(
                                       static_cast<uint64_t>(metadata.size), 1);

            MoveStartResponse response;
            std::vector<ReplicaID> replica_ids;

            response.source = std::move(source_descriptor);
            if (!replicas.empty()) {
                replica_ids.push_back(replicas[0].id());
                Replica::Descriptor descriptor;
                if (!replicas[0].getDescriptorIfAvailable(descriptor)) {
                    ReleaseTenantQuota(GetBoundTenantQuotaHandle(tenant),
                                       pending_quota_charge);
                    return tl::make_unexpected(
                        ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
                }
                response.target = std::move(descriptor);
            } else {
                response.target = std::nullopt;
            }

            // Create replication task for tracking. The task slot was checked
            // above, under this same entry lock.
            state.replication_task.emplace(
                client_id, std::chrono::system_clock::now(),
                ReplicationTask::Type::MOVE, source->id(),
                std::move(replica_ids), pending_quota_charge, UUID{}, 0);

            // Increase source refcnt to protect it from eviction.
            source->inc_refcnt();

            // Add replicas to the object.
            // DO NOT ACCESS source AFTER THIS !!!
            metadata.AddReplicas(std::move(replicas));

            return response;
        });
    if (!outcome.has_value()) {
        LOG(ERROR) << "key=" << key << ", object not found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    return *outcome;
}

tl::expected<void, ErrorCode> MasterService::MoveEnd(
    const UUID& client_id, const std::string& key, const TenantId& tenant_id) {
    const auto record = FindClientRecord(client_id);
    auto retaining_guard =
        record ? record->TryAcquireRetainingGuard() : std::nullopt;
    if (!retaining_guard) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    const auto object_id = MakeObjectIdentityForRequest(key, tenant_id);
    // One read-modify-write under the entry's own lock: the task it takes
    // ownership of, the persist it appends and the quota it settles cannot
    // interleave with another point operation on the key.
    const auto outcome = WithObjectMetadataForWriteAndCleanup(
        object_id,
        [&](metadata::Tenant& tenant, const std::shared_ptr<ObjectEntry>& entry,
            ObjectMetadata& metadata,
            ObjectEntry::State& state) -> tl::expected<void, ErrorCode> {
            // No usable replica, so there is no replication task to complete.
            if (!metadata.IsValid()) {
                LOG(ERROR) << "key=" << key << ", error=object_not_found";
                return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
            }

            if (!state.replication_task.has_value()) {
                LOG(ERROR) << "key=" << key
                           << ", error=object has no ongoing replication task";
                return tl::make_unexpected(
                    ErrorCode::OBJECT_NO_REPLICATION_TASK);
            }

            // Taken by value: the task is erased while this frame still runs.
            const ReplicationTask task = *state.replication_task;
            if (task.client_id != client_id) {
                LOG(ERROR) << "Illegal client " << client_id
                           << " to MoveEnd key " << key
                           << ", was MoveStart-ed by " << task.client_id;
                return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
            }

            if (task.type != ReplicationTask::Type::MOVE) {
                LOG(ERROR)
                    << "Ongoing replication task type is COPY instead of MOVE";
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }

            const auto previous_kv_media = KvMediaSnapshot(metadata);
            auto source_id = task.source_id;
            auto source = metadata.GetReplicaByID(source_id);
            if (source == nullptr || !source->is_completed() ||
                source->has_invalid_mem_handle()) {
                LOG(ERROR)
                    << "key=" << key << ", source_id=" << source_id
                    << ", status="
                    << (source == nullptr ? "nullptr" : "invalid")
                    << ", move source becomes invalid during data transfer";
                // Release the refcnt taken in MoveStart. The success path below
                // does this once the move completes; this error path must do it
                // too, or the source replica stays pinned and can never be
                // evicted.
                if (source != nullptr) {
                    source->dec_refcnt();
                }
                // Discard target replica and clear the replication task.
                EraseReplicasWithCacheTotalAccounting(
                    metadata, [&task](const Replica& replica) {
                        return std::find(task.replica_ids.begin(),
                                         task.replica_ids.end(),
                                         replica.id()) !=
                               task.replica_ids.end();
                    });
                ReleaseTenantQuota(GetBoundTenantQuotaHandle(tenant),
                                   task.pending_quota_charge_bytes);
                state.replication_task.reset();
                if (!metadata.IsValid()) {
                    // Remove the object if it does not have any replicas.
                    (void)EraseMetadata(tenant, entry, metadata, state,
                                        object_id.tenant_id);
                }
                return tl::make_unexpected(ErrorCode::REPLICA_IS_GONE);
            }

            // Validate the target replica before any mutation. Source
            // dec_refcnt and target mark_complete are deferred until after
            // persist.
            bool has_target = !task.replica_ids.empty();
            ReplicaID target_id =
                has_target ? task.replica_ids[0] : ReplicaID{};
            if (has_target) {
                auto replica = metadata.GetReplicaByID(target_id);
                if (replica == nullptr || replica->has_invalid_mem_handle()) {
                    LOG(WARNING)
                        << "key=" << key << ", replica_id=" << target_id
                        << ", move target becomes invalid during data transfer";
                    // Release the refcnt taken in MoveStart. The success path
                    // below does this once the move completes; this error path
                    // must do it too, or the source replica stays pinned.
                    source->dec_refcnt();
                    // Discard target replica and clear the replication task.
                    EraseReplicasWithCacheTotalAccounting(
                        metadata, [&task](const Replica& replica) {
                            return std::find(task.replica_ids.begin(),
                                             task.replica_ids.end(),
                                             replica.id()) !=
                                   task.replica_ids.end();
                        });
                    ReleaseTenantQuota(GetBoundTenantQuotaHandle(tenant),
                                       task.pending_quota_charge_bytes);
                    state.replication_task.reset();
                    return tl::make_unexpected(ErrorCode::REPLICA_IS_GONE);
                }
            }

            if (enable_oplog_ && ordered_oplog_writer_) {
                // Build post-mutation descriptors:
                //   - existing COMPLETE replicas, except the source (about to
                //   be popped)
                //   - target (if any) flipped to COMPLETE
                std::vector<Replica::Descriptor> post;
                for (const auto& rep : metadata.GetAllReplicas()) {
                    if (rep.id() == source_id) continue;
                    if (rep.status() == ReplicaStatus::COMPLETE) {
                        post.push_back(rep.get_descriptor());
                        continue;
                    }
                    if (has_target && rep.id() == target_id) {
                        Replica::Descriptor desc = rep.get_descriptor();
                        desc.status = ReplicaStatus::COMPLETE;
                        post.push_back(std::move(desc));
                    }
                }

                tl::expected<OpLogEntry, ErrorCode> persist_result;
                if (enable_oplog_) {
                    auto reservation = ReserveBatchOpLogSlot();
                    if (!reservation) {
                        return tl::make_unexpected(reservation.error());
                    }
                    source->mark_removed();
                    persist_result = AppendReservedOpLogWithDurableFinalize(
                        std::move(reservation.value()), OpType::PUT_END,
                        metadata.tenant_id.value(), key,
                        SerializeMetadataForOpLogFromReplicaDescriptors(
                            metadata, post),
                        [this, entry,
                         removed_ids = std::vector<ReplicaID>{source_id}](
                            const OpLogEntry& durable_entry) {
                            FinalizeRemovedReplicasAfterDurable(
                                entry, durable_entry, removed_ids,
                                QuotaEraseMode::kFull);
                        });
                } else {
                    persist_result = AppendOpLogWithDurableFinalize(
                        OpType::PUT_END, metadata.tenant_id.value(), key,
                        SerializeMetadataForOpLogFromReplicaDescriptors(
                            metadata, post),
                        nullptr);
                }
                if (!persist_result) {
                    return tl::make_unexpected(persist_result.error());
                }
            }

            // Persist OK — apply local commit.
            source->dec_refcnt();
            if (has_target) {
                auto replica = metadata.GetReplicaByID(target_id);
                if (replica != nullptr) {
                    replica->mark_complete();
                }
            }
            if (enable_multi_tenants_) {
                auto settle_result = metadata.quota_ledger.SettleAdditional(
                    GetBoundTenantQuotaHandle(tenant),
                    task.pending_quota_charge_bytes,
                    has_target ? static_cast<uint64_t>(metadata.size) : 0);
                if (!settle_result) {
                    LogTenantQuotaLedgerError(settle_result,
                                              "settle_additional",
                                              metadata.tenant_id, key);
                    return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
                }
            }

            if (!(enable_ha_ && enable_oplog_)) {
                // Remove the source replica and release its space later.
                auto source_replica = PopReplicasWithCacheTotalAccounting(
                    metadata, [&source_id](const Replica& replica) {
                        return replica.id() == source_id;
                    });
                if (!source_replica.empty()) {
                    FreeDfsReplicas(key, source_replica);
                    if (enable_multi_tenants_) {
                        auto release_result =
                            metadata.quota_ledger.ReleaseCommitted(
                                GetBoundTenantQuotaHandle(tenant),
                                static_cast<uint64_t>(metadata.size));
                        if (!release_result) {
                            LogTenantQuotaLedgerError(release_result,
                                                      "release_committed",
                                                      metadata.tenant_id, key);
                            return tl::make_unexpected(
                                ErrorCode::INTERNAL_ERROR);
                        }
                    }
                    std::lock_guard lock(discarded_replicas_mutex_);
                    discarded_replicas_.emplace_back(
                        std::move(source_replica),
                        std::chrono::system_clock::now() +
                            put_start_release_timeout_sec_);
                }
            }

            state.replication_task.reset();

            SyncKvObjectState(key, metadata, tenant_id, previous_kv_media);

            return {};
        });
    if (!outcome.has_value()) {
        LOG(ERROR) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    return *outcome;
}

tl::expected<void, ErrorCode> MasterService::MoveRevoke(
    const UUID& client_id, const std::string& key, const TenantId& tenant_id) {
    const auto record = FindClientRecord(client_id);
    auto retaining_guard =
        record ? record->TryAcquireRetainingGuard() : std::nullopt;
    if (!retaining_guard) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    const auto object_id = MakeObjectIdentityForRequest(key, tenant_id);
    // The revoke runs under the entry's own lock so the task it tears down and
    // the target it discards stay one read-modify-write.
    const auto outcome = WithObjectMetadataForWriteAndCleanup(
        object_id,
        [&](metadata::Tenant& tenant, const std::shared_ptr<ObjectEntry>& entry,
            ObjectMetadata& metadata,
            ObjectEntry::State& state) -> tl::expected<void, ErrorCode> {
            // No usable replica, so there is no replication task to revoke.
            if (!metadata.IsValid()) {
                LOG(ERROR) << "key=" << key << ", error=object_not_found";
                return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
            }

            if (!state.replication_task.has_value()) {
                LOG(ERROR) << "key=" << key
                           << ", error=object has no ongoing replication task";
                return tl::make_unexpected(
                    ErrorCode::OBJECT_NO_REPLICATION_TASK);
            }

            // Taken by value: the task is erased while this frame still runs.
            const ReplicationTask task = *state.replication_task;
            if (task.client_id != client_id) {
                LOG(ERROR) << "Illegal client " << client_id
                           << " to MoveRevoke key " << key
                           << ", was MoveStart-ed by " << task.client_id;
                return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
            }

            if (task.type != ReplicationTask::Type::MOVE) {
                LOG(ERROR)
                    << "Ongoing replication task type is COPY instead of MOVE";
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }

            auto source_id = task.source_id;
            auto source = metadata.GetReplicaByID(source_id);
            if (source == nullptr) {
                LOG(WARNING) << "key=" << key << ", source_id=" << source_id
                             << ", move source not found during revoke";
            } else {
                // Decrement source reference count
                source->dec_refcnt();
            }

            // Erase all replica_ids (in MOVE operation, there should be at most
            // one)
            for (const auto& replica_id : task.replica_ids) {
                EraseReplicasWithCacheTotalAccounting(
                    metadata, [&replica_id](const Replica& replica) {
                        return replica.id() == replica_id;
                    });
            }

            ReleaseTenantQuota(GetBoundTenantQuotaHandle(tenant),
                               task.pending_quota_charge_bytes);
            state.replication_task.reset();

            if (!metadata.IsValid()) {
                // Remove the object if it does not have any replicas.
                (void)EraseMetadata(tenant, entry, metadata, state,
                                    object_id.tenant_id);
            }

            return {};
        });
    if (!outcome.has_value()) {
        LOG(ERROR) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    return *outcome;
}

auto MasterService::Remove(const std::string& key, const TenantId& tenant_id,
                           bool force) -> tl::expected<void, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    const auto object_id = MakeObjectIdentityForRequest(key, tenant_id);
    // The existence check and the teardown run in one section under the
    // entry's own lock, so a concurrent write on this key cannot slip between
    // them.
    const auto outcome = WithObjectMetadataForWriteAndCleanup(
        object_id,
        [&](metadata::Tenant& tenant, const std::shared_ptr<ObjectEntry>& entry,
            ObjectMetadata& metadata,
            ObjectEntry::State& state) -> tl::expected<void, ErrorCode> {
            // No usable replica, so there is nothing to remove.
            if (!metadata.IsValid()) {
                VLOG(1) << "key=" << key << ", error=object_not_found";
                return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
            }

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

            if (state.replication_task.has_value()) {
                LOG(ERROR) << "key=" << key
                           << ", error=object_has_replication_task";
                return tl::make_unexpected(
                    ErrorCode::OBJECT_HAS_REPLICATION_TASK);
            }

            if (enable_ha_) {
                if (enable_oplog_) {
                    auto reservation = ReserveBatchOpLogSlot();
                    if (!reservation) {
                        return tl::make_unexpected(reservation.error());
                    }
                    std::vector<ReplicaID> removed_ids;
                    metadata.VisitReplicas(
                        &Replica::fn_is_completed,
                        [&removed_ids](Replica& replica) {
                            removed_ids.push_back(replica.id());
                            replica.mark_removed();
                        });
                    auto persist_result =
                        AppendReservedOpLogWithDurableFinalize(
                            std::move(reservation.value()), OpType::REMOVE,
                            object_id.tenant_id.value(), key, {},
                            [this, entry, removed_ids = std::move(removed_ids)](
                                const OpLogEntry& durable_entry) {
                                FinalizeRemovedReplicasAfterDurable(
                                    entry, durable_entry, removed_ids,
                                    QuotaEraseMode::kFull);
                            });
                    if (!persist_result) {
                        return tl::make_unexpected(persist_result.error());
                    }
                    return {};
                }
            }
            (void)EraseMetadata(tenant, entry, metadata, state,
                                object_id.tenant_id);
            return {};
        });
    if (!outcome.has_value()) {
        VLOG(1) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    return *outcome;
}

auto MasterService::RemoveByRegex(const std::string& regex_pattern,
                                  const TenantId& tenant_id, bool force)
    -> tl::expected<long, ErrorCode> {
    assert(tenant_id.IsValid());
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
    const TenantId& normalized_tenant = ResolveRequestTenantId(tenant_id);
    const auto tenant_handle = tenants_.Lookup(normalized_tenant);
    if (tenant_handle != nullptr) {
        // The snapshot only names the keys to consider: each key is resolved
        // again below, so the checks and the erase stay atomic with it.
        for (const auto& snapshot_entry : tenant_handle->SnapshotObjects()) {
            const std::string& key = snapshot_entry->key();
            if (!std::regex_search(key, pattern)) {
                continue;
            }
            (void)WithObjectMetadataForWriteAndCleanup(
                MakeObjectIdentity(key, normalized_tenant),
                [&](metadata::Tenant& tenant,
                    const std::shared_ptr<ObjectEntry>& entry,
                    ObjectMetadata& metadata,
                    ObjectEntry::State& state) -> void {
                    if (!force && !metadata.IsLeaseExpired()) {
                        VLOG(1) << "key=" << key
                                << " matched by regex, but has lease. Skipping "
                                   "removal.";
                        return;
                    }
                    /**
                     * The reason the force operation here does not bypass the
                     * replica check is that put operations (which could also be
                     * copy or move) and remove operations might be happening
                     * concurrently, making it extremely dangerous to perform a
                     * direct removal at this point.
                     */
                    if (!metadata.AllReplicas(&Replica::fn_is_completed)) {
                        LOG(WARNING)
                            << "key=" << key
                            << " matched by regex, but not all replicas "
                               "are complete. Skipping removal.";
                        return;
                    }
                    if (state.replication_task.has_value()) {
                        LOG(WARNING)
                            << "key=" << key
                            << ", matched by regex, but has replication "
                               "task. Skipping removal.";
                        return;
                    }

                    VLOG(1) << "key=" << key << " matched by regex. Removing.";
                    if (enable_ha_) {
                        if (enable_oplog_) {
                            auto reservation = ReserveBatchOpLogSlot();
                            if (!reservation) {
                                return;
                            }
                            std::vector<ReplicaID> removed_ids;
                            metadata.VisitReplicas(
                                &Replica::fn_is_completed,
                                [&removed_ids](Replica& replica) {
                                    removed_ids.push_back(replica.id());
                                    replica.mark_removed();
                                });
                            auto persist_result =
                                AppendReservedOpLogWithDurableFinalize(
                                    std::move(reservation.value()),
                                    OpType::REMOVE, normalized_tenant.value(),
                                    key, {},
                                    [this, entry,
                                     removed_ids = std::move(removed_ids)](
                                        const OpLogEntry& durable_entry) {
                                        FinalizeRemovedReplicasAfterDurable(
                                            entry, durable_entry, removed_ids,
                                            QuotaEraseMode::kFull);
                                    });
                            if (!persist_result) {
                                return;
                            }
                            removed_count++;
                            return;
                        }
                    }
                    (void)EraseMetadata(tenant, entry, metadata, state,
                                        normalized_tenant);
                    removed_count++;
                });
        }
    }

    VLOG(1) << "action=remove_by_regex, pattern=" << regex_pattern
            << ", removed_count=" << removed_count;
    return removed_count;
}

long MasterService::RemoveAll(bool force) {
    long removed_count = 0;
    int64_t total_freed_size = 0;
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    auto now = std::chrono::system_clock::now();
    // Tracking which tenants ended up empty costs a hash insert per visited
    // object, so it is skipped entirely when nobody is listening for `cleared`.
    const bool track_cleared_tenants =
        KvEventsEnabled() || kv_track_tenant_epochs_;
    // Baselines must be read before the scan starts, not at first sight of each
    // tenant: a commit can land in a tenant the walk already passed while that
    // tenant is still unseen, and a per-tenant read would then fold that commit
    // into its own baseline and wrongly conclude nothing raced.
    const auto epoch_baseline = SnapshotKvTenantEpochs(track_cleared_tenants);
    std::unordered_set<std::string> tenants_seen;
    std::unordered_set<std::string> tenants_with_remaining_objects;

    // Since RemoveAll clears everything, signal ALL clients with a
    // LocalSSD clients to physically clear their SSD immediately.
    // This lets client cleanup overlap with master metadata deletion.
    local_ssd_manager_.RequestRemoveAll();

    // Delete metadata — runs concurrently with client SSD cleanup.
    // The walk collects the tenants present when it starts and releases the
    // registry lock before the callback runs, so the hook below may commit a
    // new object (or a new tenant) without deadlocking.
    size_t tenants_scanned = 0;
    tenants_.Visit([&](const TenantId& tenant_id,
                       const std::shared_ptr<metadata::Tenant>& tenant) {
        // The snapshot only names the keys to consider: each key is resolved
        // again below, so the checks and the erase stay atomic with it.
        for (const auto& snapshot_entry : tenant->SnapshotObjects()) {
            const std::string& key = snapshot_entry->key();
            // The plain published-object access: the read-write accessor syncs
            // the object's KV state, which bumps the tenant epoch this scan's
            // `cleared` decision is compared against.
            (void)tenant->WithPublishedObject(
                key, [&](ObjectMetadata& metadata, ObjectEntry::State& state) {
                    const std::shared_ptr<ObjectEntry> entry = tenant->Get(key);
                    if (entry == nullptr) {
                        return;
                    }
                    // Record the tenant only once the loop actually reaches an
                    // object. A tenant row can outlive its objects, and
                    // recording it unconditionally made every empty-but-present
                    // tenant look like a fresh wipe and emit a spurious
                    // `cleared`.
                    if (track_cleared_tenants) {
                        tenants_seen.insert(tenant_id.value());
                    }
                    if ((force || metadata.IsLeaseExpired(now)) &&
                        metadata.AllReplicas(&Replica::fn_is_completed) &&
                        !state.replication_task.has_value()) {
                        auto mem_rep_count = metadata.CountReplicas(
                            &Replica::fn_is_memory_replica);

                        if (enable_ha_) {
                            if (enable_oplog_) {
                                auto reservation = ReserveBatchOpLogSlot();
                                if (!reservation) {
                                    // Skipped, so the tenant is not empty.
                                    if (track_cleared_tenants) {
                                        tenants_with_remaining_objects.insert(
                                            tenant_id.value());
                                    }
                                    return;
                                }
                                std::vector<ReplicaID> removed_ids;
                                metadata.VisitReplicas(
                                    &Replica::fn_is_completed,
                                    [&removed_ids](Replica& replica) {
                                        removed_ids.push_back(replica.id());
                                        replica.mark_removed();
                                    });
                                auto persist_result =
                                    AppendReservedOpLogWithDurableFinalize(
                                        std::move(reservation.value()),
                                        OpType::REMOVE, tenant_id.value(), key,
                                        {},
                                        [this, entry,
                                         removed_ids = std::move(removed_ids)](
                                            const OpLogEntry& durable_entry) {
                                            FinalizeRemovedReplicasAfterDurable(
                                                entry, durable_entry,
                                                removed_ids,
                                                QuotaEraseMode::kFull);
                                        });
                                if (!persist_result) {
                                    if (track_cleared_tenants) {
                                        tenants_with_remaining_objects.insert(
                                            tenant_id.value());
                                    }
                                    return;
                                }
                                total_freed_size +=
                                    metadata.size * mem_rep_count;
                                removed_count++;
                                return;
                            }
                        }

                        total_freed_size += metadata.size * mem_rep_count;
                        ErasePromotionTaskLocked(*tenant, state);
                        (void)EraseMetadata(*tenant, entry, metadata, state,
                                            tenant_id);
                        removed_count++;
                    } else {
                        // Only a skipped object means the tenant is not empty.
                        // Under HA+oplog the erase is deferred to the durable
                        // callback, so the object is still routed and the
                        // tenant does not look emptied.
                        if (track_cleared_tenants) {
                            tenants_with_remaining_objects.insert(
                                tenant_id.value());
                        }
                    }
                });
        }
        // The hook fires once per tenant, after that tenant's walk, so a test
        // can commit into the tenant the walk just finished.
        if (kv_remove_all_tenant_hook_) {
            kv_remove_all_tenant_hook_(tenants_scanned);
        }
        ++tenants_scanned;
    });

    // tenants_seen holds only tenants that actually held an object, so this
    // difference is "held objects and lost all of them" — a real wipe. Both
    // sets stay empty unless track_cleared_tenants was set.
    if (track_cleared_tenants) {
        for (const auto& tenant : tenants_seen) {
            if (!tenants_with_remaining_objects.contains(tenant)) {
                PublishKvClearedIfEpochUnchanged(
                    TenantId(tenant),
                    epoch_baseline[KvTenantEpochSlot(tenant)]);
            }
        }
    }

    VLOG(1) << "action=remove_all_objects"
            << ", removed_count=" << removed_count
            << ", total_freed_size=" << total_freed_size;
    return removed_count;
}

long MasterService::RemoveAll(const TenantId& tenant_id, bool force) {
    long removed_count = 0;
    int64_t total_freed_size = 0;
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    auto now = std::chrono::system_clock::now();
    const TenantId& normalized_tenant = ResolveRequestTenantId(tenant_id);
    // A `cleared` event means "this tenant is now empty". Counting the tenant's
    // objects gets that wrong both ways: a tenant that never existed looks
    // empty, and under HA+oplog a full wipe looks non-empty because the erase
    // is deferred to the durable callback. Count what the scan did instead.
    bool saw_any_object = false;
    bool skipped_any_object = false;
    // This overload knows its tenant up front, so the epoch can be read before
    // the scan starts rather than at first sight of an object.
    const uint64_t entry_epoch = ReadKvTenantEpoch(normalized_tenant.value());

    // For the tenant-scoped overload, only signal clients that own LOCAL_DISK
    // replicas of THIS tenant — clearing all clients would cross-delete other
    // tenants' SSD data.
    std::unordered_set<UUID, boost::hash<UUID>> clients_with_disk_replicas;

    const auto tenant_handle = tenants_.Lookup(normalized_tenant);
    if (tenant_handle != nullptr) {
        // The snapshot only names the keys to consider: each key is resolved
        // again below, so the checks and the erase stay atomic with it.
        for (const auto& snapshot_entry : tenant_handle->SnapshotObjects()) {
            const std::string& key = snapshot_entry->key();
            // The plain published-object access: the read-write accessor syncs
            // the object's KV state, which bumps the tenant epoch this scan
            // compares against.
            (void)tenant_handle->WithPublishedObject(
                key, [&](ObjectMetadata& metadata, ObjectEntry::State& state) {
                    const std::shared_ptr<ObjectEntry> entry =
                        tenant_handle->Get(key);
                    if (entry == nullptr) {
                        return;
                    }
                    saw_any_object = true;
                    if ((force || metadata.IsLeaseExpired(now)) &&
                        metadata.AllReplicas(&Replica::fn_is_completed) &&
                        !state.replication_task.has_value()) {
                        metadata.VisitReplicas(
                            &Replica::fn_is_local_disk_replica,
                            [&clients_with_disk_replicas](
                                const Replica& replica) {
                                auto cid = replica.get_local_disk_client_id();
                                if (cid) {
                                    clients_with_disk_replicas.insert(*cid);
                                }
                            });
                        auto mem_rep_count = metadata.CountReplicas(
                            &Replica::fn_is_memory_replica);
                        if (enable_ha_) {
                            if (enable_oplog_) {
                                auto reservation = ReserveBatchOpLogSlot();
                                if (!reservation) {
                                    // Skipped, so this is not a wipe.
                                    skipped_any_object = true;
                                    return;
                                }
                                std::vector<ReplicaID> removed_ids;
                                metadata.VisitReplicas(
                                    &Replica::fn_is_completed,
                                    [&removed_ids](Replica& replica) {
                                        removed_ids.push_back(replica.id());
                                        replica.mark_removed();
                                    });
                                auto persist_result =
                                    AppendReservedOpLogWithDurableFinalize(
                                        std::move(reservation.value()),
                                        OpType::REMOVE,
                                        normalized_tenant.value(), key, {},
                                        [this, entry,
                                         removed_ids = std::move(removed_ids)](
                                            const OpLogEntry& durable_entry) {
                                            FinalizeRemovedReplicasAfterDurable(
                                                entry, durable_entry,
                                                removed_ids,
                                                QuotaEraseMode::kFull);
                                        });
                                if (!persist_result) {
                                    skipped_any_object = true;
                                    return;
                                }
                                total_freed_size +=
                                    metadata.size * mem_rep_count;
                                removed_count++;
                                return;
                            }
                        }
                        total_freed_size += metadata.size * mem_rep_count;
                        ErasePromotionTaskLocked(*tenant_handle, state);
                        (void)EraseMetadata(*tenant_handle, entry, metadata,
                                            state, normalized_tenant);
                        removed_count++;
                    } else {
                        skipped_any_object = true;
                    }
                });
        }
    }

    // The hook fires once for this tenant, after the scan, so a test can commit
    // into the tenant the walk just finished.
    if (kv_remove_all_tenant_hook_) {
        kv_remove_all_tenant_hook_(0);
    }

    local_ssd_manager_.RequestRemoveAll(std::vector<UUID>(
        clients_with_disk_replicas.begin(), clients_with_disk_replicas.end()));

    if (saw_any_object && !skipped_any_object) {
        PublishKvClearedIfEpochUnchanged(normalized_tenant, entry_epoch);
    }

    VLOG(1) << "action=remove_all_objects"
            << ", tenant_id=" << normalized_tenant.value()
            << ", removed_count=" << removed_count
            << ", total_freed_size=" << total_freed_size
            << ", signaled_clients=" << clients_with_disk_replicas.size();
    return removed_count;
}

auto MasterService::BatchRemove(const std::vector<std::string>& keys,
                                const TenantId& tenant_id, bool force)
    -> std::vector<tl::expected<void, ErrorCode>> {
    std::vector<tl::expected<void, ErrorCode>> results(keys.size());
    const TenantId& normalized_tenant = ResolveRequestTenantId(tenant_id);

    std::shared_lock<std::shared_mutex> client_lock(client_mutex_);
    std::shared_lock<std::shared_mutex> snapshot_lock(snapshot_mutex_);
    auto retaining_clients = GetRetainingClientIdsLocked();
    client_lock.unlock();

    // A tenant that was never registered owns none of these keys, so each one
    // is reported missing, as the single-key Remove does.
    if (tenants_.Lookup(normalized_tenant) == nullptr) {
        for (auto& result : results) {
            result = tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
        }
        return results;
    }

    auto now = std::chrono::system_clock::now();

    for (size_t original_idx = 0; original_idx < keys.size(); ++original_idx) {
        const std::string& key = keys[original_idx];
        // Every check and the erase run under the entry's own lock, so a
        // concurrent write on this key cannot slip between them.
        const auto outcome = WithObjectMetadataForWriteAndCleanup(
            MakeObjectIdentity(key, normalized_tenant),
            [&](metadata::Tenant& tenant,
                const std::shared_ptr<ObjectEntry>& entry,
                ObjectMetadata& metadata,
                ObjectEntry::State& state) -> tl::expected<void, ErrorCode> {
                // Clean up stale replica handles (consistent with single
                // Remove).
                auto cleanup_plan =
                    BuildStaleHandleCleanupPlan(metadata, retaining_clients);
                if (!cleanup_plan.removed_ids.empty()) {
                    auto persist_result = PersistStaleHandleCleanupForHA(
                        "BatchRemove(stale cleanup)", normalized_tenant, entry,
                        key, metadata, cleanup_plan);
                    if (!persist_result) {
                        return tl::make_unexpected(persist_result.error());
                    }
                    if (enable_oplog_) {
                        return tl::make_unexpected(
                            cleanup_plan.would_invalidate
                                ? ErrorCode::OBJECT_NOT_FOUND
                                : ErrorCode::OBJECT_ALREADY_EXISTS);
                    }
                    if (CleanupStaleHandles(key, normalized_tenant, tenant,
                                            metadata, state,
                                            retaining_clients)) {
                        (void)EraseMetadata(tenant, entry, metadata, state,
                                            normalized_tenant,
                                            QuotaEraseMode::kFull);
                        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
                    }
                }
                if (!metadata.IsValid()) {
                    return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
                }

                if (!force && !metadata.IsLeaseExpired(now)) {
                    VLOG(1) << "key=" << key << ", error=object_has_lease";
                    return tl::make_unexpected(ErrorCode::OBJECT_HAS_LEASE);
                }

                if (!metadata.AllReplicas(&Replica::fn_is_completed)) {
                    LOG(ERROR) << "key=" << key << ", error=replica_not_ready";
                    return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
                }

                if (state.replication_task.has_value()) {
                    LOG(ERROR) << "key=" << key
                               << ", error=object_has_replication_task";
                    return tl::make_unexpected(
                        ErrorCode::OBJECT_HAS_REPLICATION_TASK);
                }

                // Remove object metadata
                if (enable_ha_) {
                    if (enable_oplog_) {
                        auto reservation = ReserveBatchOpLogSlot();
                        if (!reservation) {
                            return tl::make_unexpected(reservation.error());
                        }
                        std::vector<ReplicaID> removed_ids;
                        metadata.VisitReplicas(
                            &Replica::fn_is_completed,
                            [&removed_ids](Replica& replica) {
                                removed_ids.push_back(replica.id());
                                replica.mark_removed();
                            });
                        auto persist_result =
                            AppendReservedOpLogWithDurableFinalize(
                                std::move(reservation.value()), OpType::REMOVE,
                                normalized_tenant.value(), key, {},
                                [this, entry,
                                 removed_ids = std::move(removed_ids)](
                                    const OpLogEntry& durable_entry) {
                                    FinalizeRemovedReplicasAfterDurable(
                                        entry, durable_entry, removed_ids,
                                        QuotaEraseMode::kFull);
                                });
                        if (!persist_result) {
                            return tl::make_unexpected(persist_result.error());
                        }
                        return {};
                    }
                }
                (void)EraseMetadata(tenant, entry, metadata, state,
                                    normalized_tenant, QuotaEraseMode::kFull);
                return {};
            });
        if (!outcome.has_value()) {
            VLOG(1) << "key=" << key << ", error=object_not_found";
            results[original_idx] =
                tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
            continue;
        }
        results[original_idx] = *outcome;
    }

    return results;
}

void MasterService::CancelPromotionTaskForRemovedReplicas(
    metadata::Tenant& tenant, ObjectMetadata& metadata,
    ObjectEntry::State& state,
    const std::vector<ReplicaID>& removed_replica_ids) {
    if (removed_replica_ids.empty() || !state.promotion_task.has_value() ||
        state.promotion_task->alloc_id == 0 ||
        std::find(removed_replica_ids.begin(), removed_replica_ids.end(),
                  state.promotion_task->alloc_id) ==
            removed_replica_ids.end()) {
        return;
    }

    if (auto* source = metadata.GetReplicaByID(state.promotion_task->source_id);
        source != nullptr) {
        source->dec_refcnt();
    }
    const UUID holder_id = state.promotion_task->holder_id;
    ErasePromotionTaskLocked(tenant, state);

    // Best-effort cleanup of a task that may still be queued on the holder.
    local_ssd_manager_.RemovePromotion(holder_id, metadata.tenant_id,
                                       metadata.user_key);
}

bool MasterService::CleanupStaleHandles(
    const std::string& key, const TenantId& tenant_id, metadata::Tenant& tenant,
    ObjectMetadata& metadata, ObjectEntry::State& state,
    const std::unordered_set<UUID, boost::hash<UUID>>& retaining_clients) {
    // Removes replicas with invalid allocators (memory replicas on unmounted
    // segments) and local_disk replicas whose owner no longer retains
    // resources.
    // Kept as a thin wrapper over the predicate form so the owner-targeted
    // LOCAL_DISK sweep (ClearLocalDiskHandlesOwnedBy) shares this accounting
    // rather than duplicating it.
    return CleanupStaleHandles(
        key, tenant_id, tenant, metadata, state,
        [&retaining_clients](const Replica& replica) {
            return (replica.has_invalid_mem_handle() ||
                    replica.has_invalid_nof_handle() ||
                    replica.has_stale_local_disk_client(retaining_clients)) &&
                   replica.is_completed();
        });
}

bool MasterService::CleanupStaleHandles(
    const std::string& key, const TenantId& tenant_id, metadata::Tenant& tenant,
    ObjectMetadata& metadata, ObjectEntry::State& state,
    const std::function<bool(const Replica&)>& is_stale) {
    const auto previous_kv_media = KvMediaSnapshot(metadata);
    const uint64_t before_charge = CompletedMemoryQuotaCharge(metadata);
    std::vector<ReplicaID> removed_replica_ids;
    EraseReplicasWithCacheTotalAccounting(metadata, is_stale,
                                          &removed_replica_ids);
    CancelPromotionTaskForRemovedReplicas(tenant, metadata, state,
                                          removed_replica_ids);
    const uint64_t after_charge = CompletedMemoryQuotaCharge(metadata);
    if (enable_multi_tenants_ && before_charge > after_charge) {
        auto release_result = metadata.quota_ledger.ReleaseCommitted(
            GetBoundTenantQuotaHandle(tenant), before_charge - after_charge);
        LogTenantQuotaLedgerError(release_result, "release_committed",
                                  metadata.tenant_id, metadata.user_key);
    }

    SyncKvObjectState(key, metadata, tenant_id, previous_kv_media);

    // Return true if no valid replicas remain after cleanup
    return !metadata.IsValid();
}

void MasterService::FreeDfsReplicas(const std::string& key,
                                    const std::vector<Replica>& replicas) {
    if (!dfs_allocator_) return;
    for (const auto& replica : replicas) {
        if (!replica.is_dfs_replica()) continue;
        const auto& desc = replica.get_dfs_descriptor();
        dfs_allocator_->Free(key, desc);
    }
}

void MasterService::RunBucketDfsEviction() {
    (void)RunBucketDfsEvictionInternal(false);
}

bool MasterService::TryRecoverDfsSpaceAfterAllocationFailure() {
    if (bucket_allocator_ == nullptr ||
        !bucket_allocator_->IsEvictionEnabled()) {
        return false;
    }
    LOG(WARNING) << "DFS bucket allocation exhausted; forcing one validated "
                    "whole-bucket eviction";
    return RunBucketDfsEvictionInternal(true);
}

bool MasterService::RunBucketDfsEvictionInternal(bool force_one) {
    if (bucket_allocator_ == nullptr) return false;
    const size_t retried = bucket_allocator_->RetryFailedEvictions();
    bool evicted = retried > 0;
    if (force_one && retried > 0) return evicted;

    const TenantId tenant_id = TenantId::Default();
    std::set<int64_t> attempted;
    while (true) {
        auto pending =
            force_one ? bucket_allocator_->PrepareEvictionForAllocationFailure()
                      : bucket_allocator_->PrepareEviction();
        if (pending.Empty()) return evicted;
        if (!attempted.insert(pending.bucket_id()).second) {
            bucket_allocator_->AbortEviction(std::move(pending));
            return evicted;
        }

        const auto& candidates = pending.Candidates();

        auto matches_candidate =
            [](const Replica& replica,
               const DfsAllocatorInterface::EvictionCandidate& candidate) {
                if (!replica.is_dfs_replica()) return false;
                const auto& descriptor = replica.get_dfs_descriptor();
                return descriptor.shard_idx == candidate.descriptor.shard_idx &&
                       descriptor.offset == candidate.descriptor.offset &&
                       descriptor.object_size ==
                           candidate.descriptor.object_size &&
                       descriptor.aligned_size ==
                           candidate.descriptor.aligned_size &&
                       descriptor.file_path == candidate.descriptor.file_path;
            };

        // The bucket goes as a whole: every member is validated before the
        // allocator commits the bucket, and one member that may not be evicted
        // aborts the attempt. A prepared eviction keeps the bucket's extents
        // out of allocation until it is committed or aborted, so each member is
        // resolved through its own tenant and read under its own entry lock,
        // the way the shard allocator's cycle does it.
        bool all_accepted = true;
        ImmutableBucketAllocator::EvictedBucket evicted_bucket;
        bool logical_committed = false;
        {
            std::shared_lock<std::shared_mutex> snapshot_lock(snapshot_mutex_);
            auto now = std::chrono::system_clock::now();
            for (const auto& candidate : candidates) {
                const auto accepted = WithObjectMetadataForRead(
                    MakeObjectIdentityForRequest(candidate.key, tenant_id),
                    [&](const metadata::Tenant&,
                        const std::shared_ptr<ObjectEntry>&,
                        const ObjectMetadata& metadata,
                        const ObjectEntry::State& state) -> bool {
                        if (!metadata.HasReplica([&](const Replica& replica) {
                                return matches_candidate(replica, candidate);
                            })) {
                            // A key without the candidate replica has already
                            // released the extent from the master's side.
                            return true;
                        }
                        const bool processing =
                            metadata.HasReplica([&](const Replica& replica) {
                                return matches_candidate(replica, candidate) &&
                                       replica.is_processing();
                            });
                        return !processing && !state.is_processing &&
                               !metadata.IsHardPinned() &&
                               metadata.IsLeaseExpired(now) &&
                               (!IsSoftPinActive(metadata, now) ||
                                allow_evict_soft_pinned_objects_);
                    });
                if (accepted.has_value() && !*accepted) {
                    all_accepted = false;
                    break;
                }
            }

            if (all_accepted) {
                auto logical = bucket_allocator_->CommitEvictionLogical(
                    std::move(pending));
                if (logical) {
                    evicted_bucket = std::move(*logical);
                    logical_committed = true;
                } else {
                    LOG(ERROR) << "DFS bucket eviction logical commit failed, "
                                  "error="
                               << logical.error();
                }
            }
        }

        if (all_accepted && logical_committed) {
            // The bucket is committed, so the members that still carry one of
            // its replicas drop it now. Each key is resolved again: a member
            // may have been republished since it was validated, and that
            // publication owns whatever it holds.
            std::shared_lock<std::shared_mutex> snapshot_lock(snapshot_mutex_);
            for (const auto& candidate : candidates) {
                (void)WithObjectMetadataForWriteAndCleanup(
                    MakeObjectIdentityForRequest(candidate.key, tenant_id),
                    [&](metadata::Tenant& tenant,
                        const std::shared_ptr<ObjectEntry>& entry,
                        ObjectMetadata& metadata,
                        ObjectEntry::State& state) -> void {
                        const size_t erased =
                            metadata.EraseReplicas([&](const Replica& replica) {
                                return matches_candidate(replica, candidate);
                            });
                        if (erased == 0) {
                            return;
                        }
                        PublishKvRemovedAfterEvict(candidate.key, erased,
                                                   "disk", metadata, tenant_id);
                        if (!metadata.IsValid()) {
                            EraseMetadata(tenant, entry, metadata, state,
                                          tenant_id, QuotaEraseMode::kFull);
                        }
                    });
            }
        }

        if (!all_accepted) {
            bucket_allocator_->AbortEviction(std::move(pending));
            continue;
        }
        if (!logical_committed) {
            bucket_allocator_->AbortEviction(std::move(pending));
            continue;
        }

        auto deleted =
            bucket_allocator_->DeleteEvictedBucket(std::move(evicted_bucket));
        if (!deleted) {
            LOG(ERROR) << "DFS bucket data deletion failed; capacity remains "
                          "reserved for runtime retry, error="
                       << deleted.error();
            return evicted;
        } else {
            evicted = true;
            if (force_one) return true;
        }
    }
}

void MasterService::RunDfsEviction() {
    if (!dfs_allocator_) return;
    if (bucket_allocator_ != nullptr) {
        RunBucketDfsEviction();
        return;
    }
    RunShardDfsEviction();
}

void MasterService::RunShardDfsEviction() {
    if (!shard_allocator_) return;

    const TenantId tenant_id = TenantId::Default();
    using CandidateIdentity = std::tuple<std::string, int, uint64_t>;
    std::set<CandidateIdentity> attempted;

    while (true) {
        auto pending = shard_allocator_->PrepareEviction();
        if (pending.Empty()) return;

        const auto candidates = pending.Candidates();
        std::vector<bool> accepted(candidates.size(), false);
        std::vector<bool> considered(candidates.size(), false);
        bool saw_repeated_candidate = false;

        // Prepared allocator extents stay unavailable until
        // ResolvePreparedEviction(), so each candidate is validated and removed
        // under its own entry lock rather than in one spanning transaction.
        std::vector<size_t> candidate_indices;
        candidate_indices.reserve(candidates.size());
        for (size_t i = 0; i < candidates.size(); ++i) {
            const auto& candidate = candidates[i];
            if (!attempted
                     .emplace(candidate.key, candidate.shard_idx,
                              candidate.offset)
                     .second) {
                saw_repeated_candidate = true;
                continue;
            }
            considered[i] = true;
            candidate_indices.push_back(i);
        }

        auto matches_candidate = [](const Replica& replica,
                                    const auto& candidate) {
            return replica.is_dfs_replica() &&
                   replica.get_dfs_descriptor().shard_idx ==
                       candidate.shard_idx &&
                   replica.get_dfs_descriptor().offset == candidate.offset;
        };

        auto now = std::chrono::system_clock::now();
        // Once a candidate has been seen in this cycle it is excluded above;
        // encountering it again means the LRU scan has wrapped.
        {
            std::shared_lock<std::shared_mutex> snapshot_lock(snapshot_mutex_);
            if (tenants_.Lookup(tenant_id) == nullptr) {
                // No such tenant: every candidate is already evicted from the
                // master's point of view.
                for (const size_t i : candidate_indices) {
                    accepted[i] = true;
                }
            } else {
                for (const size_t i : candidate_indices) {
                    const auto& candidate = candidates[i];
                    const bool ran = WithObjectMetadataForWriteAndCleanup(
                        MakeObjectIdentityForRequest(candidate.key, tenant_id),
                        [&](metadata::Tenant& tenant,
                            const std::shared_ptr<ObjectEntry>& entry,
                            ObjectMetadata& metadata,
                            ObjectEntry::State& state) -> void {
                            const bool has_candidate = metadata.HasReplica(
                                [&](const Replica& replica) {
                                    return matches_candidate(replica,
                                                             candidate);
                                });
                            if (!has_candidate) {
                                accepted[i] = true;
                                return;
                            }

                            const bool candidate_is_processing =
                                metadata.HasReplica([&](const Replica& r) {
                                    return matches_candidate(r, candidate) &&
                                           r.is_processing();
                                });
                            accepted[i] = !candidate_is_processing &&
                                          !state.is_processing &&
                                          !metadata.IsHardPinned() &&
                                          metadata.IsLeaseExpired(now) &&
                                          (!IsSoftPinActive(metadata, now) ||
                                           allow_evict_soft_pinned_objects_);
                            if (!accepted[i]) return;

                            // A missing descriptor was accepted above and is
                            // already evicted; otherwise the descriptor is
                            // removed before its extent can be committed again.
                            const size_t erased = metadata.EraseReplicas(
                                [&](const Replica& replica) {
                                    return matches_candidate(replica,
                                                             candidate) &&
                                           !replica.is_processing();
                                });
                            if (erased > 0) {
                                PublishKvRemovedAfterEvict(candidate.key,
                                                           erased, "disk",
                                                           metadata, tenant_id);
                                if (!metadata.IsValid()) {
                                    (void)EraseMetadata(tenant, entry, metadata,
                                                        state, tenant_id,
                                                        QuotaEraseMode::kFull);
                                }
                            }
                        });
                    if (!ran) {
                        // The key publishes nothing, so its extent is already
                        // evicted.
                        accepted[i] = true;
                    }
                }
            }
        }

        shard_allocator_->ResolvePreparedEviction(std::move(pending), accepted);

        // A protected allocation stays live, but moving it to the MRU side
        // lets this cycle inspect colder candidates behind it. The attempted
        // set bounds the scan when every remaining allocation is protected.
        for (size_t i = 0; i < candidates.size(); ++i) {
            if (considered[i] && !accepted[i]) {
                shard_allocator_->UpdateAccess(candidates[i].key,
                                               candidates[i].shard_idx,
                                               candidates[i].offset);
            }
        }

        if (saw_repeated_candidate) {
            return;
        }
    }
}

size_t MasterService::GetKeyCount() const {
    size_t total = 0;
    tenants_.Visit([&total](const TenantId&,
                            const std::shared_ptr<metadata::Tenant>& tenant) {
        total += tenant->ObjectCount();
    });
    return total;
}

auto MasterService::Ping(const UUID& client_id)
    -> tl::expected<PingResponse, ErrorCode> {
    std::shared_lock<std::shared_mutex> lock(client_mutex_);
    const auto record_it = client_liveness_records_.find(client_id);
    bool observation_accepted = false;
    if (record_it != client_liveness_records_.end()) {
        const auto observation =
            record_it->second->Observe(ClientLivenessRecord::Clock::now());
        observation_accepted =
            observation != ClientLivenessObservation::REJECTED_OFFLINE;
        if (observation == ClientLivenessObservation::RECOVERED_ACTIVE) {
            MasterMetricManager::instance().client_liveness_recovered();
            LOG(INFO) << "client_id=" << client_id
                      << ", action=client_liveness_recovered, signal=ping";
        }
    }
    const ClientStatus client_status =
        observation_accepted && ok_client_.contains(client_id)
            ? ClientStatus::OK
            : ClientStatus::NEED_REMOUNT;
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
    std::unique_lock<std::shared_mutex> client_lock(client_mutex_);
    auto [record_it, inserted] = client_liveness_records_.try_emplace(
        client_id, std::make_shared<ClientLivenessRecord>(
                       ClientLivenessRecord::Clock::now()));
    const auto record = record_it->second;
    if (inserted) {
        MasterMetricManager::instance().client_liveness_record_created();
    }
    ErrorCode err = ErrorCode::INTERNAL_ERROR;
    const auto observation =
        record->ObserveAndRun(ClientLivenessRecord::Clock::now(), [&] {
            std::shared_lock<std::shared_mutex> snapshot_lock(snapshot_mutex_);
            err =
                local_ssd_manager_.RegisterClient(client_id, enable_offloading);
            return err == ErrorCode::OK ||
                   err == ErrorCode::SEGMENT_ALREADY_EXISTS;
        });
    if (observation == ClientLivenessObservation::REJECTED_OFFLINE) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    if (err != ErrorCode::OK && err != ErrorCode::SEGMENT_ALREADY_EXISTS) {
        if (inserted) {
            client_liveness_records_.erase(client_id);
            MasterMetricManager::instance().on_client_liveness_record_removed(
                ClientLivenessState::ACTIVE);
        }
        return tl::make_unexpected(err);
    }
    if (observation == ClientLivenessObservation::RECOVERED_ACTIVE) {
        MasterMetricManager::instance().client_liveness_recovered();
        LOG(INFO) << "client_id=" << client_id
                  << ", action=client_liveness_recovered, "
                     "signal=local_disk_mount";
    }

    return {};
}

auto MasterService::UnmountLocalDiskSegment(const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    if (!enable_offload_) {
        LOG(ERROR) << "The offload functionality is not enabled";
        return tl::make_unexpected(ErrorCode::UNABLE_OFFLOAD);
    }

    // Deregister before sweeping this client's replicas, and take
    // snapshot_mutex_ exclusively: a concurrent NotifyOffloadSuccess
    // registration then resolves entirely before or entirely after the sweep.
    // The sweep runs with the registry lock released, as in ClientMonitorFunc.
    std::optional<int64_t> reported_capacity;
    {
        std::unique_lock<std::shared_mutex> snapshot_lock(snapshot_mutex_);
        reported_capacity = local_ssd_manager_.UnregisterClient(client_id);
    }
    if (!reported_capacity) {
        // Idempotent, the same way MountLocalDiskSegment treats an
        // already-mounted segment as success.
        return {};
    }
    if (*reported_capacity > 0) {
        MasterMetricManager::instance().dec_total_file_capacity(
            *reported_capacity);
    }

    // Sweep exactly this owner's replicas. Deliberately not a
    // liveness-complement sweep (ClearInvalidHandles with a staying set):
    // that classifies by absence from a point-in-time snapshot, so an owner
    // that mounts and registers after the snapshot but before the sweep
    // reaches its object would have its replicas classified stale and
    // erased -- and when that disk replica was the key's only one, the key
    // itself. An owner-id predicate cannot misclassify a concurrent mount,
    // whatever the interleaving.
    ClearLocalDiskHandlesOwnedBy(client_id);

    LOG(INFO) << "client_id=" << client_id
              << ", action=unmount_local_disk_segment_by_request";
    return {};
}

bool MasterService::HasMountedLocalDiskSegment(const UUID& client_id) {
    return local_ssd_manager_.GetUsage(client_id).has_value();
}

auto MasterService::OffloadObjectHeartbeat(const UUID& client_id,
                                           bool enable_offloading)
    -> tl::expected<std::vector<OffloadTaskItem>, ErrorCode> {
    const auto record = FindClientRecord(client_id);
    auto serving_guard =
        record ? record->TryAcquireServingGuard() : std::nullopt;
    if (!serving_guard) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    auto pending = local_ssd_manager_.SetOffloadingAndTakePending(
        client_id, enable_offloading);
    if (!pending) {
        LOG(ERROR) << "Local disk segment not found with client id = "
                   << client_id;
        return tl::make_unexpected(pending.error());
    }
    if (enable_offloading) {
        return std::move(*pending);
    }
    // Offloading is disabled: clear the pending queue to prevent unbounded
    // growth that would trigger KEYS_ULTRA_LIMIT in PushOffloadingQueue. We
    // must also clean up each entry's offloading task and decrement source
    // replica refcounts to avoid resource leaks and blocked writes
    // (OBJECT_HAS_REPLICATION_TASK).
    for (auto& task : *pending) {
        const auto object_id =
            MakeObjectIdentity(task.key, TenantId(task.tenant_id));
        (void)WithObjectMetadataForWriteAndCleanup(
            object_id,
            [&](metadata::Tenant&, const std::shared_ptr<ObjectEntry>&,
                ObjectMetadata& metadata, ObjectEntry::State& state) -> void {
                if (!state.offloading_task.has_value()) {
                    return;
                }
                if (auto source = metadata.GetReplicaByID(
                        state.offloading_task->source_id);
                    source != nullptr) {
                    source->dec_refcnt();
                }
                state.offloading_task.reset();
            });
    }
    return std::vector<OffloadTaskItem>{};
}

auto MasterService::PollRemoveAll(const UUID& client_id)
    -> tl::expected<bool, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    return local_ssd_manager_.ConsumeRemoveAll(client_id);
}

auto MasterService::ReportSsdCapacity(const UUID& client_id,
                                      int64_t ssd_total_capacity_bytes)
    -> tl::expected<void, ErrorCode> {
    if (ssd_total_capacity_bytes < 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const auto record = FindClientRecord(client_id);
    auto retaining_guard =
        record ? record->TryAcquireRetainingGuard() : std::nullopt;
    if (!retaining_guard) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    auto capacity =
        local_ssd_manager_.ReportCapacity(client_id, ssd_total_capacity_bytes);
    if (!capacity) {
        if (capacity.error() == ErrorCode::SEGMENT_NOT_FOUND) {
            LOG(ERROR) << "Local disk segment not found with client id = "
                       << client_id;
        }
        return tl::make_unexpected(capacity.error());
    }
    int64_t old_capacity = capacity->previous_bytes;
    if (ssd_total_capacity_bytes != old_capacity) {
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
    const auto record = FindClientRecord(client_id);
    auto retaining_guard =
        record ? record->TryAcquireRetainingGuard() : std::nullopt;
    if (!retaining_guard) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    // Set when an entry's replica registration is refused because the client
    // no longer has a LOCAL_DISK segment entry (see the per-entry checks
    // below). NACK cleanups still run for the rest of the batch; the caller
    // gets SEGMENT_NOT_FOUND so a rescan stops re-registering.
    bool refused_unmounted = false;

    for (size_t i = 0; i < tasks.size(); ++i) {
        const auto& task = tasks[i];
        const auto& metadata = metadatas[i];
        const TenantId task_tenant = enable_multi_tenants_
                                         ? TenantId(task.tenant_id)
                                         : TenantId::Default();
        const auto request_object_id =
            MakeObjectIdentityForRequest(task.key, task_tenant);

        // NACK sentinel: offload failed on worker. Clean up the
        // offloading_task + dec_refcnt but skip AddReplica.
        if (metadata.data_size < 0) {
            std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
            (void)WithObjectMetadataForWriteAndCleanup(
                request_object_id,
                [&](metadata::Tenant&, const std::shared_ptr<ObjectEntry>&,
                    ObjectMetadata& obj_metadata,
                    ObjectEntry::State& state) -> void {
                    if (!state.offloading_task.has_value()) {
                        return;
                    }
                    if (auto source = obj_metadata.GetReplicaByID(
                            state.offloading_task->source_id);
                        source != nullptr) {
                        source->dec_refcnt();
                    }
                    state.offloading_task.reset();
                    MasterMetricManager::instance().inc_offload_failed(
                        UuidToString(client_id));
                });
            continue;
        }

        Replica replica(client_id, metadata.data_size,
                        metadata.transport_endpoint, ReplicaStatus::COMPLETE,
                        record);
        bool handled_existing_object = false;
        bool added_new_local_disk_replica = false;
        {
            std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
            // A disk replica may only be registered for a client whose
            // LOCAL_DISK segment entry still exists. Checked inside this
            // shared-lock section, before the per-object lock, so the check
            // and the write below cannot straddle UnmountLocalDiskSegment's
            // removal (which holds snapshot_mutex_ exclusively): either the
            // replica lands first and its sweep erases it, or the check here
            // sees the segment gone and refuses. Without this, a
            // registration racing a deregistration -- an in-flight rescan
            // batch, or an offload completion from a heartbeat that was past
            // its own drain check -- could land after the sweep and leave
            // the master advertising a departed owner. Scoped to
            // enable_offload_, where the segment registry exists and a
            // deregistration can race; with the subsystem off both the mount
            // and unmount RPCs refuse, so there is nothing to check against
            // and no race to close.
            const bool segment_mounted =
                !enable_offload_ || HasMountedLocalDiskSegment(client_id);
            const auto outcome = WithObjectMetadataForWriteAndCleanup(
                request_object_id,
                [&](metadata::Tenant&, const std::shared_ptr<ObjectEntry>&,
                    ObjectMetadata& obj_metadata, ObjectEntry::State& state)
                    -> tl::expected<void, ErrorCode> {
                    const auto previous_kv_media =
                        KvMediaSnapshot(obj_metadata);
                    if (state.offloading_task.has_value() &&
                        replica.type() != ReplicaType::LOCAL_DISK) {
                        LOG(ERROR) << "Invalid replica type: " << replica.type()
                                   << ". Expected ReplicaType::LOCAL_DISK.";
                        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                    }

                    // Only a master-admitted offload completion may bypass
                    // tenant registration for an existing orphan object;
                    // without this task marker, the regular check applies.
                    if (state.offloading_task.has_value()) {
                        auto source = obj_metadata.GetReplicaByID(
                            state.offloading_task->source_id);
                        if (source != nullptr) {
                            source->dec_refcnt();
                        }
                        state.offloading_task.reset();

                        if (!segment_mounted) {
                            // The offload bookkeeping above still ran; only the
                            // registration is refused.
                            refused_unmounted = true;
                        } else if (!obj_metadata.HasReplica(
                                       &Replica::fn_is_local_disk_replica)) {
                            std::vector<Replica> replicas;
                            replicas.emplace_back(std::move(replica));
                            obj_metadata.AddReplicas(std::move(replicas));
                            SyncCacheTotalAccounting(obj_metadata);
                            MasterMetricManager::instance()
                                .inc_offload_completed(UuidToString(client_id));
                            added_new_local_disk_replica = true;
                        } else {
                            obj_metadata.VisitReplicas(
                                [client_id](const Replica& rep) {
                                    return rep.type() ==
                                               ReplicaType::LOCAL_DISK &&
                                           rep.get_descriptor()
                                                   .get_local_disk_descriptor()
                                                   .client_id == client_id;
                                },
                                [&replica](Replica& rep) {
                                    rep.get_descriptor()
                                        .get_local_disk_descriptor()
                                        .transport_endpoint =
                                        replica.get_descriptor()
                                            .get_local_disk_descriptor()
                                            .transport_endpoint;
                                    rep.get_descriptor()
                                        .get_local_disk_descriptor()
                                        .object_size =
                                        replica.get_descriptor()
                                            .get_local_disk_descriptor()
                                            .object_size;
                                });
                        }
                        handled_existing_object = true;
                        SyncKvObjectState(
                            request_object_id.user_key, obj_metadata,
                            request_object_id.tenant_id, previous_kv_media);
                    }
                    return {};
                });
            if (outcome.has_value() && !outcome->has_value()) {
                return tl::make_unexpected(outcome->error());
            }
        }

        if (!handled_existing_object) {
            auto normalized_tenant_result =
                ResolveTenantIdForWrite(request_object_id.tenant_id);
            if (!normalized_tenant_result) {
                return tl::make_unexpected(normalized_tenant_result.error());
            }
            const ObjectIdentity object_id{
                std::move(normalized_tenant_result.value()),
                request_object_id.user_key};

            auto res = AddReplicaForRetainedClient(
                client_id, object_id.user_key, object_id.tenant_id, replica);
            if (!res) {
                if (res.error() == ErrorCode::OBJECT_NOT_FOUND) {
                    continue;
                }
                if (res.error() == ErrorCode::SEGMENT_NOT_FOUND) {
                    // AddReplica's own mounted-segment check refused it (the
                    // segment entry vanished under a deregistration). Finish
                    // the batch so remaining NACK cleanups still run.
                    refused_unmounted = true;
                    continue;
                }
                LOG(ERROR) << "Failed to add replica: error=" << res.error()
                           << ", client_id=" << client_id
                           << ", tenant_id=" << object_id.tenant_id.value()
                           << ", key=" << object_id.user_key;
                return tl::make_unexpected(res.error());
            }
            added_new_local_disk_replica = res.value();
        }
        if (metadata.data_size > 0 && added_new_local_disk_replica) {
            local_ssd_manager_.AdjustUsedBytes(client_id, metadata.data_size);
        }
    }

    if (refused_unmounted) {
        LOG(WARNING) << "client_id=" << client_id
                     << ", action=notify_offload_success_refused"
                     << ", error=no_local_disk_segment";
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    return {};
}

tl::expected<void, ErrorCode> MasterService::PushOffloadingQueue(
    const ObjectIdentity& object_id, Replica& replica,
    std::vector<UUID>* mirror_clients) {
    const auto& segment_names = replica.get_segment_names();
    // No source segment names means there is no usable source segment to
    // offload from. Returning a silent {} here caused the caller to record an
    // OffloadingTask + inc_refcnt for work that was never enqueued, leaking the
    // refcount until TTL expiry (issue #2997). Return an explicit
    // UNABLE_OFFLOADING so callers treat this as a no-op rather than a success.
    if (segment_names.empty() ||
        std::none_of(segment_names.begin(), segment_names.end(),
                     [](const auto& name) { return name.has_value(); })) {
        return tl::make_unexpected(ErrorCode::UNABLE_OFFLOADING);
    }
    Replica::Descriptor source_descriptor;
    if (!TryGetReadableReplicaDescriptor(replica, source_descriptor) ||
        !source_descriptor.is_memory_replica()) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    bool any_enqueued = false;
    std::string client_id_str;
    const auto liveness = replica.getClientLiveness();
    if (!liveness) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    for (const auto& segment_name_it : segment_names) {
        if (!segment_name_it.has_value()) {
            continue;
        }
        std::optional<UUID> client_id;
        {
            auto allocator_access = segment_manager_.getAllocatorAccess();
            client_id =
                allocator_access.GetOwnerClientId(segment_name_it.value());
        }
        if (!client_id) {
            return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
        }
        // Callers hold the source object's entry lock. This serving observation
        // is the admission point; terminal cleanup must take that same entry
        // lock before it can remove the source or unregister the destination.
        if (!liveness->IsServing()) {
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }
        auto allocator_access = segment_manager_.getAllocatorAccess();
        const auto current_owner =
            allocator_access.GetOwnerClientId(segment_name_it.value());
        if (!current_owner || *current_owner != *client_id ||
            !TryGetReadableReplicaDescriptor(replica, source_descriptor)) {
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }
        const int64_t size =
            source_descriptor.get_memory_descriptor().buffer_descriptor.size_;
        auto err = local_ssd_manager_.EnqueueOffload(
            *client_id,
            OffloadTaskItem{.tenant_id = object_id.tenant_id.value(),
                            .key = object_id.user_key,
                            .size = size},
            offloading_queue_limit_);
        if (err == ErrorCode::SEGMENT_NOT_FOUND) {
            return tl::make_unexpected(ErrorCode::UNABLE_OFFLOADING);
        }
        if (err != ErrorCode::OK) {
            if (err == ErrorCode::KEYS_ULTRA_LIMIT) {
                MasterMetricManager::instance().inc_offload_enqueue_rejected(
                    UuidToString(*client_id));
            }
            return tl::make_unexpected(err);
        }
        if (mirror_clients != nullptr) {
            mirror_clients->push_back(*client_id);
        }
        client_id_str = UuidToString(*client_id);
        any_enqueued = true;
    }
    // Every segment name was nullopt (or EnqueueOffload found no usable
    // segment), so nothing was enqueued. Same invariant as the empty case:
    // never report success when no task was actually submitted.
    if (!any_enqueued) {
        return tl::make_unexpected(ErrorCode::UNABLE_OFFLOADING);
    }
    MasterMetricManager::instance().inc_offload_enqueued(client_id_str);
    return {};
}

bool MasterService::CancelQueuedOffloadTask(ObjectMetadata& metadata,
                                            ObjectEntry::State& state,
                                            const ObjectIdentity& object_id) {
    if (!state.offloading_task.has_value()) {
        return true;
    }
    const auto& mirror_clients = state.offloading_task->mirror_clients;
    if (mirror_clients.empty()) {
        return false;
    }

    if (!local_ssd_manager_.CancelOffloadsIfAllPending(
            mirror_clients, object_id.tenant_id, object_id.user_key)) {
        return false;
    }

    auto source = metadata.GetReplicaByID(state.offloading_task->source_id);
    if (source != nullptr) {
        source->dec_refcnt();
    }
    MasterMetricManager::instance().inc_offload_cancelled(
        UuidToString(mirror_clients[0]));
    state.offloading_task.reset();
    return true;
}

// Promotion-on-hit

// Push a key into the holder client's promotion mailbox. Resolve the holder
// via the LOCAL_DISK replica's embedded client_id rather than via the
// segment-name reverse lookup.
tl::expected<void, ErrorCode> MasterService::PushPromotionQueue(
    const ObjectIdentity& object_id, Replica& source_replica) {
    auto holder_id = source_replica.get_local_disk_client_id();
    if (!holder_id.has_value()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const auto liveness = source_replica.getClientLiveness();
    if (!liveness) {
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    // TryPushPromotionQueue holds the source object's entry lock, which orders
    // a concurrently accepted enqueue before terminal cleanup.
    if (!liveness->IsServing()) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    auto err = local_ssd_manager_.EnqueuePromotion(
        *holder_id, PromotionTaskItem{.tenant_id = object_id.tenant_id.value(),
                                      .key = object_id.user_key,
                                      .size = static_cast<int64_t>(
                                          source_replica.get_descriptor()
                                              .get_local_disk_descriptor()
                                              .object_size)});
    if (err == ErrorCode::SEGMENT_NOT_FOUND) {
        // Holder client expired or never registered LocalSSD;
        // the LOCAL_DISK replica will be cleaned up by ClientMonitorFunc on
        // its own schedule.
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return {};
}

// --- Promotion retry candidate helpers ---

void MasterService::DecrementCandidateCount() {
    uint64_t count = promotion_candidate_count_.load(std::memory_order_relaxed);
    while (count > 0) {
        if (promotion_candidate_count_.compare_exchange_weak(
                count, count - 1, std::memory_order_relaxed)) {
            return;
        }
    }
}

void MasterService::EraseCandidateLocked(
    const TenantId& tenant_id, const std::shared_ptr<ObjectEntry>& entry,
    ObjectEntry::State& state) {
    if (!state.promotion_candidate.has_value()) {
        return;
    }
    state.promotion_candidate.reset();
    UnindexPromotionCandidate(tenant_id, entry->key());
    DecrementCandidateCount();
}

void MasterService::EraseCandidate(const ObjectIdentity& object_id) {
    const auto tenant = tenants_.Lookup(object_id.tenant_id);
    if (tenant == nullptr) {
        return;
    }
    const auto entry = tenant->Get(object_id.user_key);
    if (entry == nullptr) {
        return;
    }
    entry->WithExclusiveAccess([&](ObjectMetadata&, ObjectEntry::State& state) {
        EraseCandidateLocked(object_id.tenant_id, entry, state);
    });
}

void MasterService::RecordOrUpdateCandidateLocked(
    const TenantId& tenant_id, const std::shared_ptr<ObjectEntry>& entry,
    ObjectEntry::State& state, uint8_t sketch_score,
    PromotionCandidateReason reason, ErrorCode last_error,
    uint32_t execution_failures) {
    const auto now = std::chrono::steady_clock::now();
    const std::string& key = entry->key();
    if (state.promotion_candidate.has_value()) {
        // Update existing entry: refresh last_seen, reset
        // retry_after/retry_count. execution_failures is intentionally NOT
        // updated here: a read refresh is new demand signal and may extend
        // the budget, but it must not erase the failure history of this
        // admission chain.
        PromotionCandidate& candidate = *state.promotion_candidate;
        candidate.last_seen = now;
        candidate.last_reason = reason;
        candidate.last_error = last_error;
        if (sketch_score > candidate.sketch_score) {
            candidate.sketch_score = sketch_score;
        }
        candidate.retry_after = now;
        candidate.retry_count = 0;
        return;
    }

    // Reserve a slot in the global candidate limit.
    uint64_t count = promotion_candidate_count_.load(std::memory_order_relaxed);
    while (count < kPromotionCandidateLimit) {
        if (promotion_candidate_count_.compare_exchange_weak(
                count, count + 1, std::memory_order_relaxed)) {
            break;
        }
    }
    if (count >= kPromotionCandidateLimit) {
        VLOG(1) << "promotion_candidate_dropped key=" << key
                << " reason=global_limit";
        MasterMetricManager::instance().inc_promotion_candidate_dropped_limit();
        return;
    }

    state.promotion_candidate =
        PromotionCandidate{.sketch_score = sketch_score,
                           .first_seen = now,
                           .last_seen = now,
                           .retry_after = now,
                           .last_reason = reason,
                           .last_error = last_error,
                           .retry_count = 0,
                           .execution_failures = execution_failures};
    IndexPromotionCandidate(tenant_id, key);
    MasterMetricManager::instance().inc_promotion_candidate_recorded();
    VLOG(1) << "promotion_candidate_recorded key=" << key;
}

std::chrono::milliseconds MasterService::CandidateBackoff(
    uint32_t retry_count) const {
    uint64_t backoff_ms =
        static_cast<uint64_t>(kPromotionCandidateInitialBackoff.count());
    for (uint32_t i = 1; i < retry_count; ++i) {
        backoff_ms = std::min<uint64_t>(
            backoff_ms * 2,
            static_cast<uint64_t>(kPromotionCandidateMaxBackoff.count()));
    }
    return std::chrono::milliseconds(backoff_ms);
}

bool MasterService::IsTransientResult(PromotionQueueResult result) const {
    return result == PromotionQueueResult::kWatermarkRejected ||
           result == PromotionQueueResult::kQueueCapRejected ||
           result == PromotionQueueResult::kPushFailed;
}

void MasterService::BackoffCandidate(const ObjectIdentity& object_id,
                                     PromotionQueueResult result) {
    const auto now = std::chrono::steady_clock::now();
    const auto tenant = tenants_.Lookup(object_id.tenant_id);
    if (tenant == nullptr) {
        return;
    }
    const auto entry = tenant->Get(object_id.user_key);
    if (entry == nullptr) {
        return;
    }
    entry->WithExclusiveAccess([&](ObjectMetadata&, ObjectEntry::State& state) {
        if (!state.promotion_candidate.has_value()) {
            return;
        }

        PromotionCandidate& candidate = *state.promotion_candidate;
        candidate.retry_count++;
        if (result == PromotionQueueResult::kWatermarkRejected) {
            candidate.last_reason = PromotionCandidateReason::kWatermark;
            candidate.last_error = ErrorCode::OK;
        } else if (result == PromotionQueueResult::kQueueCapRejected) {
            candidate.last_reason = PromotionCandidateReason::kQueueCap;
            candidate.last_error = ErrorCode::OK;
        } else {
            candidate.last_reason = PromotionCandidateReason::kPushFailed;
        }

        const bool ttl_expired =
            now - candidate.last_seen >= kPromotionCandidateTtl;
        if (ttl_expired ||
            candidate.retry_count >= kPromotionCandidateMaxRetries) {
            VLOG(1) << "promotion_candidate_gave_up key=" << object_id.user_key
                    << " retries=" << candidate.retry_count;
            EraseCandidateLocked(object_id.tenant_id, entry, state);
            MasterMetricManager::instance()
                .inc_promotion_candidate_expired_evaluated();
        } else {
            candidate.retry_after =
                now + CandidateBackoff(candidate.retry_count);
        }
    });
}

void MasterService::ReconcilePromotionBookkeeping() {
    tenants_.Visit([this](const TenantId& tenant_id,
                          const std::shared_ptr<metadata::Tenant>& tenant) {
        for (const auto& entry : tenant->SnapshotObjects()) {
            entry->WithExclusiveAccess(
                [&](ObjectMetadata&, ObjectEntry::State& state) {
                    EraseCandidateLocked(tenant_id, entry, state);
                });
        }
    });
    promotion_candidate_count_.store(0, std::memory_order_relaxed);
    promotion_retry_cursor_.store(0, std::memory_order_relaxed);
    promotion_in_flight_.store(0, std::memory_order_relaxed);
}

size_t MasterService::RunPromotionCandidateRetry() {
    if (!promotion_on_hit_ ||
        promotion_candidate_count_.load(std::memory_order_relaxed) == 0) {
        return 0;
    }

    const auto now = std::chrono::steady_clock::now();
    std::vector<ObjectIdentity> due_candidates;
    due_candidates.reserve(kPromotionRetryBatchSize);
    size_t scanned = 0;

    // One round examines at most kPromotionRetryScanBudget candidates, and no
    // more than an equal share of them from one tenant, so a tenant whose index
    // is mostly candidates that are not due yet cannot spend the round on
    // itself and leave the later tenants unserved. The next round starts at the
    // tenant after the last one this round reached.
    {
        std::shared_lock<std::shared_mutex> snap_lock(snapshot_mutex_);
        std::vector<TenantId> tenant_ids;
        tenants_.Visit([&](const TenantId& tenant_id,
                           const std::shared_ptr<metadata::Tenant>&) {
            tenant_ids.push_back(tenant_id);
        });
        if (tenant_ids.empty()) {
            promotion_retry_cursor_.store(0, std::memory_order_relaxed);
            promotion_retry_last_scanned_.store(0, std::memory_order_relaxed);
            return 0;
        }

        const size_t start =
            promotion_retry_cursor_.load(std::memory_order_relaxed) %
            tenant_ids.size();
        const size_t per_tenant_share =
            std::max<size_t>(1, kPromotionRetryScanBudget / tenant_ids.size());
        for (size_t i = 0;
             i < tenant_ids.size() && scanned < kPromotionRetryScanBudget &&
             due_candidates.size() < kPromotionRetryBatchSize;
             ++i) {
            const TenantId& tenant_id =
                tenant_ids[(start + i) % tenant_ids.size()];
            const size_t slice_limit =
                std::min(per_tenant_share, kPromotionRetryScanBudget - scanned);
            for (const auto& key :
                 TakePromotionCandidateSlice(tenant_id, slice_limit)) {
                ++scanned;
                (void)WithObjectMetadataForWriteAndCleanup(
                    ObjectIdentity{.tenant_id = tenant_id, .user_key = key},
                    [&](metadata::Tenant&,
                        const std::shared_ptr<ObjectEntry>& entry,
                        ObjectMetadata& metadata,
                        ObjectEntry::State& state) -> void {
                        // A candidate is unindexed under the entry lock that
                        // drops it, and an erased entry is unindexed by its
                        // teardown, so a key in the index carries a candidate
                        // unless it was republished since.
                        if (!state.promotion_candidate.has_value()) {
                            return;
                        }
                        PromotionCandidate& candidate =
                            *state.promotion_candidate;

                        const bool ttl_expired =
                            now - candidate.last_seen >= kPromotionCandidateTtl;
                        if (ttl_expired || candidate.retry_count >=
                                               kPromotionCandidateMaxRetries) {
                            VLOG(1) << "promotion_candidate_expired key=" << key
                                    << " retry_count=" << candidate.retry_count;
                            // retry_count == 0: the TTL elapsed before the
                            // scheduler reached it, so the scan budget was too
                            // small. retry_count > 0: it gave up after retries.
                            if (candidate.retry_count == 0) {
                                MasterMetricManager::instance()
                                    .inc_promotion_candidate_expired_unevaluated();
                            } else {
                                MasterMetricManager::instance()
                                    .inc_promotion_candidate_expired_evaluated();
                            }
                            EraseCandidateLocked(tenant_id, entry, state);
                            return;
                        }
                        if (candidate.retry_after > now) {
                            return;
                        }

                        // Cheap ineligibility filter, so the retry loop
                        // does not carry a candidate it will only reject
                        // once it re-resolves it.
                        if (!metadata.IsValid() || state.is_processing ||
                            state.promotion_task.has_value() ||
                            metadata.HasReplica(
                                &Replica::fn_is_memory_replica) ||
                            !metadata.HasReplica(
                                &Replica::fn_is_local_disk_replica)) {
                            EraseCandidateLocked(tenant_id, entry, state);
                            return;
                        }

                        due_candidates.push_back(ObjectIdentity{
                            .tenant_id = tenant_id, .user_key = key});
                    });
                if (scanned >= kPromotionRetryScanBudget ||
                    due_candidates.size() >= kPromotionRetryBatchSize) {
                    break;
                }
            }
            promotion_retry_cursor_.store((start + i + 1) % tenant_ids.size(),
                                          std::memory_order_relaxed);
        }
    }

    size_t queued = 0;
    {
        std::shared_lock<std::shared_mutex> snap_lock(snapshot_mutex_);
        for (const auto& object_id : due_candidates) {
            const auto result =
                TryPushPromotionQueue(object_id, /*record_candidate=*/false);
            if (result == PromotionQueueResult::kQueued) {
                queued++;
                MasterMetricManager::instance()
                    .inc_promotion_candidate_admitted();
            } else if (IsTransientResult(result)) {
                MasterMetricManager::instance()
                    .inc_promotion_candidate_admission_rejected();
                BackoffCandidate(object_id, result);
            } else {
                EraseCandidate(object_id);
            }
        }
    }

    promotion_retry_last_scanned_.store(scanned, std::memory_order_relaxed);
    return queued;
}

bool MasterService::DynamicReplicationEnabled() const {
    return dynamic_replication_mode_ != DynamicReplicationMode::kOff;
}

uint64_t MasterService::DynamicReplicationStableScore(
    const std::string& key, const std::string& segment) {
    uint64_t hash = 1469598103934665603ULL;
    auto mix = [&hash](std::string_view value) {
        for (const unsigned char c : value) {
            hash ^= c;
            hash *= 1099511628211ULL;
        }
        hash ^= 0xff;
        hash *= 1099511628211ULL;
    };
    mix(key);
    mix(segment);
    return hash;
}

bool MasterService::DynamicReplicationEnforce() const {
    return dynamic_replication_mode_ == DynamicReplicationMode::kEnforce;
}

uint32_t MasterService::DynamicReplicationAdmissionMinHits() const {
    const double hits = std::ceil(dynamic_replication_admission_qps_threshold_ *
                                  dynamic_replication_heat_window_seconds_);
    return std::max<uint32_t>(1, static_cast<uint32_t>(hits));
}

void MasterService::CleanupDynamicReplicationWindowsLocked(
    std::chrono::steady_clock::time_point now, std::chrono::seconds window) {
    size_t scanned = 0;
    while (scanned < kDynamicReplicationWindowCleanupBudget &&
           !dynamic_replication_window_order_.empty()) {
        auto key = std::move(dynamic_replication_window_order_.front());
        dynamic_replication_window_order_.pop_front();
        auto it = dynamic_replication_windows_.find(key);
        if (it != dynamic_replication_windows_.end()) {
            if (now - it->second.window_start > window * 2) {
                dynamic_replication_windows_.erase(it);
            } else {
                dynamic_replication_window_order_.push_back(std::move(key));
            }
        }
        scanned++;
    }
}

bool MasterService::ObserveDynamicReplicationAccess(
    const ObjectIdentity& object_id) {
    if (!DynamicReplicationEnabled()) {
        return false;
    }
    const auto now = std::chrono::steady_clock::now();
    const auto window =
        std::chrono::seconds(dynamic_replication_heat_window_seconds_);
    const auto admission_key =
        object_id.tenant_id.MakeScopedKey(object_id.user_key);

    std::lock_guard<std::mutex> lock(dynamic_replication_mutex_);
    if (dynamic_replication_windows_.size() >=
            kDynamicReplicationWindowEntryLimit &&
        now >= dynamic_replication_next_window_cleanup_) {
        dynamic_replication_next_window_cleanup_ =
            now + kDynamicReplicationWindowCleanupInterval;
        CleanupDynamicReplicationWindowsLocked(now, window);
    }

    auto counter_it = dynamic_replication_windows_.find(admission_key);
    if (counter_it == dynamic_replication_windows_.end()) {
        if (dynamic_replication_windows_.size() >=
            kDynamicReplicationWindowEntryLimit) {
            return false;
        }
        counter_it = dynamic_replication_windows_
                         .emplace(admission_key, DynamicReplicationWindow{})
                         .first;
        dynamic_replication_window_order_.push_back(admission_key);
    }

    auto& counter = counter_it->second;
    if (counter.window_start.time_since_epoch().count() == 0 ||
        now - counter.window_start >= window) {
        counter.window_start = now;
        counter.hits = 0;
    }
    counter.hits++;
    return counter.hits == DynamicReplicationAdmissionMinHits();
}

bool MasterService::DynamicReplicationHeatAdmitted(
    const ObjectIdentity& object_id) {
    if (!DynamicReplicationEnabled()) {
        return false;
    }
    const auto now = std::chrono::steady_clock::now();
    const auto window =
        std::chrono::seconds(dynamic_replication_heat_window_seconds_);
    const auto admission_key =
        object_id.tenant_id.MakeScopedKey(object_id.user_key);

    std::lock_guard<std::mutex> lock(dynamic_replication_mutex_);
    auto counter_it = dynamic_replication_windows_.find(admission_key);
    if (counter_it == dynamic_replication_windows_.end()) {
        return false;
    }
    const auto& counter = counter_it->second;
    if (counter.window_start.time_since_epoch().count() == 0 ||
        now - counter.window_start >= window) {
        return false;
    }
    return counter.hits >= DynamicReplicationAdmissionMinHits();
}

void MasterService::MaybeQueueDynamicReplicaProposal(
    const ObjectIdentity& object_id) {
    if (!ObserveDynamicReplicationAccess(object_id)) {
        return;
    }
    if (dynamic_replication_mode_ == DynamicReplicationMode::kObserve) {
        VLOG(1) << "dynamic_replication_observe_would_propose key="
                << object_id.user_key;
        return;
    }
    if (!DynamicReplicationEnforce()) {
        return;
    }
    EnqueueDynamicReplicaProposal(object_id);
}

void MasterService::EnqueueDynamicReplicaProposal(
    const ObjectIdentity& object_id) {
    const auto scoped_key =
        object_id.tenant_id.MakeScopedKey(object_id.user_key);
    std::lock_guard<std::mutex> lock(dynamic_replication_admission_mutex_);
    if (dynamic_replication_admission_queued_.contains(scoped_key) ||
        dynamic_replication_admission_queue_.size() >=
            kDynamicReplicationAdmissionQueueLimit) {
        return;
    }
    dynamic_replication_admission_queue_.push(object_id);
    dynamic_replication_admission_queued_.insert(scoped_key);
    dynamic_replication_admission_cv_.notify_one();
}

void MasterService::TrySubmitDynamicReplicaProposal(
    const ObjectIdentity& object_id) {
    if (!DynamicReplicationEnforce() ||
        !DynamicReplicationHeatAdmitted(object_id)) {
        return;
    }

    ReplicaActionProposal proposal;
    proposal.action = ReplicaActionType::ADD;
    proposal.proposal_id = generate_uuid();
    proposal.tenant_id = object_id.tenant_id.value();
    proposal.key = object_id.user_key;
    proposal.expire_at_ms_epoch =
        DynamicReplicationNowMs() + kDynamicReplicationLeaseTtl.count();

    auto lease = SubmitReplicaActionProposalLocked(proposal);
    if (!lease.has_value()) {
        VLOG(1) << "dynamic_replication_auto_proposal_rejected key="
                << object_id.user_key << ", error_code=" << lease.error();
    }
}

void MasterService::DynamicReplicationAdmissionThreadFunc() {
    VLOG(1) << "action=dynamic_replication_admission_thread_started";
    while (dynamic_replication_admission_running_) {
        std::vector<ObjectIdentity> batch;
        {
            std::unique_lock<std::mutex> lock(
                dynamic_replication_admission_mutex_);
            dynamic_replication_admission_cv_.wait_for(
                lock,
                std::chrono::milliseconds(
                    kDynamicReplicationAdmissionThreadSleepMs),
                [&] {
                    return !dynamic_replication_admission_running_.load() ||
                           !dynamic_replication_admission_queue_.empty();
                });
            if (!dynamic_replication_admission_running_) {
                break;
            }
            while (!dynamic_replication_admission_queue_.empty() &&
                   batch.size() < kDynamicReplicationAdmissionBatchSize) {
                auto object_id =
                    std::move(dynamic_replication_admission_queue_.front());
                dynamic_replication_admission_queue_.pop();
                dynamic_replication_admission_queued_.erase(
                    object_id.tenant_id.MakeScopedKey(object_id.user_key));
                batch.push_back(std::move(object_id));
            }
        }
        for (const auto& object_id : batch) {
            std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
            TrySubmitDynamicReplicaProposal(object_id);
        }
    }
    VLOG(1) << "action=dynamic_replication_admission_thread_stopped";
}

uint64_t MasterService::DynamicReplicationVersionEpoch(
    const ObjectMetadata& metadata) const {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            metadata.put_start_time.time_since_epoch())
            .count());
}

int64_t MasterService::DynamicReplicationNowMs() {
    return static_cast<int64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
}

void MasterService::CleanupExpiredDynamicReplicationState() {
    if (!DynamicReplicationEnabled()) {
        return;
    }
    const int64_t now_ms = DynamicReplicationNowMs();
    // A tenant that ends up with no pending record stays registered: the
    // registry keeps every tenant it published reachable, so a handle an
    // in-flight operation already loaded is never dropped under it. Only
    // MetadataSerializer::Reset retires tenants.
    tenants_.Visit([&](const TenantId& tenant_id,
                       const std::shared_ptr<metadata::Tenant>& tenant) {
        // (key, entry at detection): the re-resolution below has to confirm
        // that the entry was not torn down and replaced, or the expired task of
        // the old publication would be failed against the replacement object.
        std::vector<std::pair<std::string, std::shared_ptr<ObjectEntry>>>
            expired_pending_keys;
        for (const auto& entry : tenant->SnapshotObjects()) {
            entry->WithSharedAccess(
                [&](const ObjectMetadata&, const ObjectEntry::State& state) {
                    if (state.dynamic_replication_pending.has_value() &&
                        state.dynamic_replication_pending->expire_at_ms_epoch <
                            now_ms) {
                        expired_pending_keys.emplace_back(entry->key(), entry);
                    }
                });
        }
        for (const auto& expired : expired_pending_keys) {
            const std::string& key = expired.first;
            const std::shared_ptr<ObjectEntry>& detected_entry = expired.second;
            const auto entry = tenant->Get(key);
            if (entry != detected_entry) {
                // The key was dropped or published again after detection: the
                // expired task died with the entry it belonged to, and the
                // replacement owns pending state of its own that must not be
                // failed here.
                continue;
            }
            entry->WithExclusiveAccess(
                [&](ObjectMetadata&, ObjectEntry::State& state) {
                    if (state.is_torn_down) {
                        return;
                    }
                    if (state.dynamic_replication_pending.has_value()) {
                        task_manager_.get_write_access().fail_task_if_pending(
                            state.dynamic_replication_pending->task_id,
                            "dynamic replica lease expired");
                    }
                    ClearDynamicReplicationStateLocked(tenant_id, entry, state);
                });
        }
        EraseExpiredDynamicReplicationLeases(tenant_id,
                                             std::chrono::system_clock::now());
    });
}

bool MasterService::HasDynamicReplicationPending(
    const TenantId& tenant_id, const std::shared_ptr<ObjectEntry>& entry,
    ObjectEntry::State& state) {
    if (!state.dynamic_replication_pending.has_value()) {
        return false;
    }
    if (state.dynamic_replication_pending->expire_at_ms_epoch >=
        DynamicReplicationNowMs()) {
        return true;
    }
    ClearDynamicReplicationStateLocked(tenant_id, entry, state);
    return false;
}

std::optional<MasterService::DynamicReplicaPlan>
MasterService::SelectDynamicReplicaPlan(
    const ObjectMetadata& metadata,
    const std::optional<std::string>& preferred_target_segment,
    std::string target_domain) {
    const std::string& object_key = metadata.user_key;
    std::unordered_set<std::string> existing_segments;
    std::unordered_set<std::string> existing_hosts;
    std::vector<std::string> source_segments;
    std::unordered_map<std::string, std::shared_ptr<ClientLivenessRecord>>
        source_liveness_by_segment;
    size_t memory_replicas = 0;

    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();
    std::vector<std::pair<Segment, UUID>> segments;
    if (segment_access.GetAllSegments(segments) != ErrorCode::OK) {
        return std::nullopt;
    }
    std::unordered_map<std::string, Segment> segments_by_name;
    for (const auto& [segment, client_id] : segments) {
        (void)client_id;
        segments_by_name.emplace(segment.name, segment);
    }

    metadata.VisitReplicas(
        [this](const Replica& replica) {
            return IsReplicaReadable(replica) && replica.is_memory_replica();
        },
        [&](const Replica& replica) {
            memory_replicas++;
            const auto liveness = replica.getClientLiveness();
            for (const auto& segment_name : replica.get_segment_names()) {
                if (!segment_name.has_value()) {
                    continue;
                }
                existing_segments.insert(*segment_name);
                source_segments.push_back(*segment_name);
                source_liveness_by_segment[*segment_name] = liveness;
                auto segment_it = segments_by_name.find(*segment_name);
                if (segment_it != segments_by_name.end() &&
                    !segment_it->second.host_id.empty()) {
                    existing_hosts.insert(segment_it->second.host_id);
                }
            }
        });

    if (source_segments.empty() || memory_replicas == 0 ||
        memory_replicas >= dynamic_replication_max_memory_replicas_) {
        return std::nullopt;
    }

    std::sort(source_segments.begin(), source_segments.end());
    source_segments.erase(
        std::unique(source_segments.begin(), source_segments.end()),
        source_segments.end());
    const std::string source_segment = *std::min_element(
        source_segments.begin(), source_segments.end(),
        [&](const auto& lhs, const auto& rhs) {
            return DynamicReplicationStableScore(object_key, lhs) <
                   DynamicReplicationStableScore(object_key, rhs);
        });

    auto is_valid_target = [&](const Segment& segment) {
        if (existing_segments.contains(segment.name) ||
            !segment_access.IsSegmentAllocatable(segment.name)) {
            return false;
        }
        size_t used = 0;
        size_t capacity = 0;
        if (segment_access.QuerySegments(segment.name, used, capacity) !=
                ErrorCode::OK ||
            capacity == 0 || used >= capacity ||
            capacity - used < metadata.size) {
            return false;
        }
        const double util_after = static_cast<double>(used + metadata.size) /
                                  static_cast<double>(capacity);
        return util_after < kDynamicReplicationTargetHighWatermark;
    };

    auto target_score = [&](const Segment& segment) {
        size_t used = 0;
        size_t capacity = 0;
        if (segment_access.QuerySegments(segment.name, used, capacity) !=
                ErrorCode::OK ||
            capacity == 0) {
            return std::tuple<bool, double, uint64_t>(
                false, std::numeric_limits<double>::max(),
                std::numeric_limits<uint64_t>::max());
        }
        const bool different_host = segment.host_id.empty() ||
                                    existing_hosts.empty() ||
                                    !existing_hosts.contains(segment.host_id);
        const double util =
            static_cast<double>(used) / static_cast<double>(capacity);
        return std::tuple<bool, double, uint64_t>(
            different_host, util,
            DynamicReplicationStableScore(object_key, segment.name));
    };

    std::optional<std::string> target_segment;
    if (preferred_target_segment.has_value()) {
        const auto preferred = std::find_if(
            segments.begin(), segments.end(), [&](const auto& entry) {
                return entry.first.name == *preferred_target_segment;
            });
        if (preferred != segments.end() && is_valid_target(preferred->first)) {
            target_segment = preferred->first.name;
        }
    }
    if (!target_segment.has_value()) {
        std::optional<std::tuple<bool, double, uint64_t>> best_score;
        for (const auto& [segment, client_id] : segments) {
            (void)client_id;
            if (!is_valid_target(segment)) {
                continue;
            }
            const auto score = target_score(segment);
            if (!best_score.has_value() ||
                std::get<0>(score) > std::get<0>(*best_score) ||
                (std::get<0>(score) == std::get<0>(*best_score) &&
                 std::get<1>(score) < std::get<1>(*best_score)) ||
                (std::get<0>(score) == std::get<0>(*best_score) &&
                 std::get<1>(score) == std::get<1>(*best_score) &&
                 std::get<2>(score) < std::get<2>(*best_score))) {
                best_score = score;
                target_segment = segment.name;
            }
        }
    }

    if (!target_segment.has_value()) {
        return std::nullopt;
    }
    const auto source_liveness =
        source_liveness_by_segment.find(source_segment);
    if (source_liveness == source_liveness_by_segment.end() ||
        !source_liveness->second) {
        return std::nullopt;
    }
    return DynamicReplicaPlan{
        .source_segment = source_segment,
        .target_segment = *target_segment,
        .target_domain = std::move(target_domain),
        .source_liveness = source_liveness->second,
    };
}

tl::expected<ReplicaActionLease, ErrorCode>
MasterService::SubmitReplicaActionProposal(
    const ReplicaActionProposal& proposal) {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    return SubmitReplicaActionProposalLocked(proposal);
}

tl::expected<ReplicaActionLease, ErrorCode>
MasterService::SubmitReplicaActionProposalLocked(
    const ReplicaActionProposal& proposal) {
    if (!DynamicReplicationEnforce()) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    if (proposal.action != ReplicaActionType::ADD || proposal.key.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (proposal.proposal_id == UUID{0, 0}) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!proposal.requester_domain.empty() || !proposal.target_domain.empty()) {
        // Domain-aware admission and placement are reserved for the next stage.
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const int64_t now_ms = DynamicReplicationNowMs();
    if (proposal.expire_at_ms_epoch > 0 &&
        proposal.expire_at_ms_epoch < now_ms) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto normalized_tenant_result =
        ResolveTenantIdForWrite(TenantId(proposal.tenant_id));
    if (!normalized_tenant_result) {
        return tl::make_unexpected(normalized_tenant_result.error());
    }
    ObjectIdentity object_id{std::move(normalized_tenant_result.value()),
                             proposal.key};
    const UUID proposal_id = proposal.proposal_id;

    // The entry's own lock is held for the whole admission, so the task state
    // the proposal is validated against is the one this call commits.
    const auto outcome = WithObjectMetadataForWriteAndCleanup(
        object_id,
        [&](metadata::Tenant&, const std::shared_ptr<ObjectEntry>& entry,
            ObjectMetadata& metadata, ObjectEntry::State& state)
            -> tl::expected<ReplicaActionLease, ErrorCode> {
            // No usable replica: no proposal is admitted, no lease is served.
            if (!metadata.IsValid()) {
                return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
            }

            auto existing_lease =
                FindDynamicReplicationLease(object_id.tenant_id, proposal_id);
            if (existing_lease.has_value()) {
                if (existing_lease->expire_at_ms_epoch >= now_ms) {
                    const bool same_request =
                        existing_lease->action == proposal.action &&
                        existing_lease->tenant_id ==
                            object_id.tenant_id.value() &&
                        existing_lease->key == object_id.user_key &&
                        (proposal.observed_version_epoch == 0 ||
                         existing_lease->version_epoch ==
                             proposal.observed_version_epoch) &&
                        (!proposal.preferred_target_segment.has_value() ||
                         existing_lease->target_segment ==
                             *proposal.preferred_target_segment) &&
                        (proposal.target_domain.empty() ||
                         existing_lease->target_domain ==
                             proposal.target_domain);
                    if (!same_request) {
                        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                    }
                    return *existing_lease;
                }
                // The expired lease is dropped before the new proposal is
                // recorded; whether this call removed it is not interesting
                // here.
                (void)RemoveDynamicReplicationLease(object_id.tenant_id,
                                                    proposal_id);
            }

            if (!DynamicReplicationHeatAdmitted(object_id)) {
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }

            if (state.dynamic_replication_cooldown !=
                std::chrono::steady_clock::time_point{}) {
                const auto now = std::chrono::steady_clock::now();
                if (state.dynamic_replication_cooldown > now) {
                    return tl::make_unexpected(
                        ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
                }
                state.dynamic_replication_cooldown =
                    std::chrono::steady_clock::time_point{};
            }

            if (state.is_processing || state.replication_task.has_value() ||
                HasDynamicReplicationPending(object_id.tenant_id, entry,
                                             state)) {
                return tl::make_unexpected(
                    ErrorCode::OBJECT_HAS_REPLICATION_TASK);
            }

            if (proposal.observed_version_epoch != 0 &&
                proposal.observed_version_epoch !=
                    DynamicReplicationVersionEpoch(metadata)) {
                return tl::make_unexpected(ErrorCode::INVALID_VERSION);
            }
            if (proposal.object_size_bytes != 0 &&
                proposal.object_size_bytes != metadata.size) {
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            if (metadata.DynamicReplicationRecreateBlocked(
                    std::chrono::steady_clock::now())) {
                return tl::make_unexpected(
                    ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
            }

            auto plan = SelectDynamicReplicaPlan(
                metadata, proposal.preferred_target_segment,
                proposal.target_domain);
            if (!plan.has_value()) {
                return tl::make_unexpected(
                    ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
            }

            const UUID lease_id = generate_uuid();
            const uint64_t version_epoch =
                DynamicReplicationVersionEpoch(metadata);
            const int64_t server_deadline_ms =
                now_ms + kDynamicReplicationLeaseTtl.count();
            const int64_t lease_expire_at_ms_epoch =
                proposal.expire_at_ms_epoch > 0
                    ? std::min(proposal.expire_at_ms_epoch, server_deadline_ms)
                    : server_deadline_ms;

            ReplicaActionLease lease;
            lease.proposal_id = proposal_id;
            lease.lease_id = lease_id;
            lease.action = ReplicaActionType::ADD;
            lease.tenant_id = object_id.tenant_id.value();
            lease.key = object_id.user_key;
            lease.source_segment = plan->source_segment;
            lease.target_segment = plan->target_segment;
            lease.target_domain = plan->target_domain;
            lease.version_epoch = version_epoch;
            lease.expire_at_ms_epoch = lease_expire_at_ms_epoch;

            state.dynamic_replication_pending = DynamicReplicaPending{
                .proposal_id = proposal_id,
                .lease_id = lease.lease_id,
                .source_segment = lease.source_segment,
                .target_segment = lease.target_segment,
                .target_domain = lease.target_domain,
                .version_epoch = lease.version_epoch,
                .expire_at_ms_epoch = lease.expire_at_ms_epoch,
                .task_id = UUID{}};

            auto task = SubmitDynamicReplicaCopyTask(object_id, *plan, lease_id,
                                                     version_epoch);
            if (!task.has_value()) {
                ClearDynamicReplicationStateLocked(object_id.tenant_id, entry,
                                                   state);
                return tl::make_unexpected(task.error());
            }

            lease.task_id = task.value();
            state.dynamic_replication_pending->task_id = lease.task_id;
            state.dynamic_replication_cooldown =
                std::chrono::steady_clock::now() +
                kDynamicReplicationActionCooldown;
            PutDynamicReplicationLease(object_id.tenant_id, entry, proposal_id,
                                       lease);
            return lease;
        });

    if (!outcome.has_value()) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    return *outcome;
}

tl::expected<UUID, ErrorCode> MasterService::SubmitDynamicReplicaCopyTask(
    const ObjectIdentity& object_id, const DynamicReplicaPlan& plan,
    const UUID& lease_id, uint64_t version_epoch) {
    UUID source_client;
    {
        ScopedSegmentAccess segment_access =
            segment_manager_.getSegmentAccess();
        ErrorCode error = segment_access.GetClientIdBySegmentName(
            plan.source_segment, source_client);
        if (error != ErrorCode::OK) {
            return tl::make_unexpected(error);
        }
    }
    const auto& liveness = plan.source_liveness;
    if (!liveness) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    // The proposal caller holds the object's entry lock; serving eligibility is
    // observed without blocking on the Client transition mutex, and offboarding
    // cleanup is ordered after this section.
    if (!liveness->IsServing()) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();
    UUID current_source_client;
    if (segment_access.GetClientIdBySegmentName(
            plan.source_segment, current_source_client) != ErrorCode::OK ||
        current_source_client != source_client) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    return task_manager_.get_write_access()
        .submit_task_typed<TaskType::REPLICA_COPY>(
            source_client,
            {.tenant_id = object_id.tenant_id.value(),
             .key = object_id.user_key,
             .source = plan.source_segment,
             .targets = {plan.target_segment},
             .dynamic_replication_lease_id_high = lease_id.first,
             .dynamic_replication_lease_id_low = lease_id.second,
             .dynamic_replication_version_epoch = version_epoch});
}

PromotionQueueResult MasterService::TryPushPromotionQueue(
    const ObjectIdentity& object_id, bool record_candidate) {
    if (!promotion_on_hit_ || !promotion_sketch_) {
        return PromotionQueueResult::kDisabled;
    }
    const auto& key = object_id.user_key;
    const auto admission_key = object_id.tenant_id.MakeScopedKey(key);

    // Frequency gate: bump and compare against the threshold. The sketch
    // returns uint8_t (saturating at 255); promotion_admission_threshold_
    // is clamped into [1, 255] at config parse time (see master.cpp), so
    // direct comparison is well-defined and threshold=0 (which would
    // bypass the gate entirely since freq is uint8_t) cannot reach here.
    const uint8_t freq = promotion_sketch_->increment(admission_key);
    if (freq < promotion_admission_threshold_) {
        MasterMetricManager::instance().inc_promotion_rejected_frequency();
        return PromotionQueueResult::kFrequencyRejected;
    }

    // Watermark gate: don't promote if DRAM is already under eviction
    // pressure. The check is best-effort (state can change between this
    // sample and the actual allocation in PromotionAllocStart).
    const double used_ratio = segment_manager_.GetMemoryUsage().used_ratio();
    if (used_ratio >= eviction_high_watermark_ratio_) {
        MasterMetricManager::instance().inc_promotion_rejected_watermark();
        if (record_candidate) {
            // A key that is not published has nothing to retry: the callback
            // does not run for it. One with no usable replica has nothing to
            // promote either.
            (void)WithObjectMetadataForWriteAndCleanup(
                object_id,
                [&](metadata::Tenant&,
                    const std::shared_ptr<ObjectEntry>& entry,
                    ObjectMetadata& metadata,
                    ObjectEntry::State& state) -> void {
                    if (!metadata.IsValid()) {
                        return;
                    }
                    RecordOrUpdateCandidateLocked(
                        object_id.tenant_id, entry, state, freq,
                        PromotionCandidateReason::kWatermark, ErrorCode::OK);
                });
        }
        return PromotionQueueResult::kWatermarkRejected;
    }

    // Dedup, refcnt-pin and the task record all need the object's own lock.
    // Safe to take it here because GetReplicaList has already released this
    // object.
    const auto outcome = WithObjectMetadataForWriteAndCleanup(
        object_id,
        [&](metadata::Tenant&, const std::shared_ptr<ObjectEntry>& entry,
            ObjectMetadata& metadata,
            ObjectEntry::State& state) -> PromotionQueueResult {
            // No usable replica: the retry path drops the candidate.
            if (!metadata.IsValid()) {
                return PromotionQueueResult::kNotFound;
            }
            // A primary Put/Upsert owns all PROCESSING replicas while the entry
            // is processing. Promotion must not establish a second owner.
            if (state.is_processing) {
                EraseCandidateLocked(object_id.tenant_id, entry, state);
                return PromotionQueueResult::kAlreadyInFlight;
            }

            // Dedup: don't queue twice if a promotion is already in flight or
            // if a MEMORY replica has appeared since GetReplicaList observed
            // only-disk.
            if (state.promotion_task.has_value()) {
                // A read hit an already in-flight promotion: re-mark the queued
                // entry's recency so the next heartbeat delivers it ahead of
                // stale admissions. No-op once the heartbeat took the entry or
                // the holder's mailbox is gone.
                local_ssd_manager_.TouchPromotion(
                    state.promotion_task->holder_id, object_id.tenant_id, key);
                EraseCandidateLocked(object_id.tenant_id, entry, state);
                return PromotionQueueResult::kAlreadyInFlight;
            }
            if (metadata.HasReplica(&Replica::fn_is_memory_replica)) {
                EraseCandidateLocked(object_id.tenant_id, entry, state);
                return PromotionQueueResult::kMemoryReplicaPresent;
            }

            // Find the LOCAL_DISK source replica.
            Replica* source = nullptr;
            metadata.VisitReplicas(&Replica::fn_is_local_disk_replica,
                                   [&source](Replica& r) {
                                       if (source == nullptr) source = &r;
                                   });
            if (source == nullptr) {
                EraseCandidateLocked(object_id.tenant_id, entry, state);
                return PromotionQueueResult::kNoLocalDiskSource;
            }

            // Soft cap: this load can race the record below and let a few extra
            // tasks in, but a key's insert is serialized by the entry lock and
            // the dedup gate prevents duplicate work, so the worst case is a
            // small overshoot. Relaxed load, since the value is advisory.
            if (promotion_in_flight_.load(std::memory_order_relaxed) >=
                promotion_queue_limit_) {
                MasterMetricManager::instance().inc_promotion_rejected_cap();
                if (record_candidate) {
                    RecordOrUpdateCandidateLocked(
                        object_id.tenant_id, entry, state, freq,
                        PromotionCandidateReason::kQueueCap, ErrorCode::OK);
                }
                return PromotionQueueResult::kQueueCapRejected;
            }

            // Pin the source replica.
            source->inc_refcnt();
            const uint64_t object_size = source->get_descriptor()
                                             .get_local_disk_descriptor()
                                             .object_size;

            // Try to enqueue on the holder client. On failure, drop the refcnt
            // back.
            auto push_result = PushPromotionQueue(object_id, *source);
            if (!push_result) {
                source->dec_refcnt();
                VLOG(1) << "promotion_push_failed key=" << key
                        << " error=" << push_result.error();
                if (push_result.error() == ErrorCode::OBJECT_ALREADY_EXISTS) {
                    EraseCandidateLocked(object_id.tenant_id, entry, state);
                    return PromotionQueueResult::kAlreadyInFlight;
                }
                if (push_result.error() == ErrorCode::SEGMENT_NOT_FOUND ||
                    push_result.error() == ErrorCode::INVALID_PARAMS) {
                    EraseCandidateLocked(object_id.tenant_id, entry, state);
                    return PromotionQueueResult::kNoLocalDiskSource;
                }
                if (record_candidate) {
                    RecordOrUpdateCandidateLocked(
                        object_id.tenant_id, entry, state, freq,
                        PromotionCandidateReason::kPushFailed,
                        push_result.error());
                }
                return PromotionQueueResult::kPushFailed;
            }

            // Capture the holder client_id so NotifyPromotionSuccess can reject
            // calls from other clients; PushPromotionQueue already validated
            // get_local_disk_client_id(), so .value() is safe.
            const UUID holder_id = source->get_local_disk_client_id().value();

            // Record the in-flight task. alloc_id is filled in by
            // PromotionAllocStart once the new MEMORY replica is staged.
            // Propagate the execution-failure count across the candidate's
            // consumption so NotifyPromotionFailure can bound self-sustaining
            // execution-failure cycles; an absent candidate means a fresh
            // chain (0).
            uint32_t execution_failures = 0;
            if (state.promotion_candidate.has_value()) {
                execution_failures =
                    state.promotion_candidate->execution_failures;
            }
            EraseCandidateLocked(object_id.tenant_id, entry, state);
            state.promotion_task =
                PromotionTask{.source_id = source->id(),
                              .alloc_id = 0,
                              .object_size = object_size,
                              .start_time = std::chrono::system_clock::now(),
                              .holder_id = holder_id,
                              .execution_failures = execution_failures};
            promotion_in_flight_.fetch_add(1, std::memory_order_relaxed);
            MasterMetricManager::instance().inc_promotion_in_flight();
            MasterMetricManager::instance().inc_promotion_admitted();
            VLOG(1) << "promotion_queued key=" << key
                    << " size=" << object_size;
            return PromotionQueueResult::kQueued;
        });

    if (!outcome.has_value()) {
        return PromotionQueueResult::kNotFound;
    }
    return *outcome;
}

auto MasterService::PromotionObjectHeartbeat(const UUID& client_id)
    -> tl::expected<std::vector<PromotionTaskItem>, ErrorCode> {
    const auto record = FindClientRecord(client_id);
    auto serving_guard =
        record ? record->TryAcquireServingGuard() : std::nullopt;
    if (!serving_guard) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    // Return at most promotion_max_per_heartbeat_ tasks. Each task does
    // a synchronous SSD read + RDMA write on the client side; allowing
    // more than one per heartbeat risks blocking past the client-
    // liveness window and the master marking the client dead. The rest
    // stay queued in the mailbox for subsequent heartbeats. The
    // cap must live here (server side) rather than on the client so
    // leftover work isn't silently dropped.
    return local_ssd_manager_.TakePromotions(client_id,
                                             promotion_max_per_heartbeat_);
}

auto MasterService::PromotionAllocStart(
    const UUID& client_id, const std::string& key, const TenantId& tenant_id,
    uint64_t size, const std::vector<std::string>& preferred_segments)
    -> tl::expected<PromotionAllocStartResponse, ErrorCode> {
    const auto object_id = MakeObjectIdentityForRequest(key, tenant_id);
    const auto record = FindClientRecord(client_id);
    auto serving_guard =
        record ? record->TryAcquireServingGuard() : std::nullopt;
    if (!serving_guard) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    // The entry lock is held for the rest of this function, so the task read
    // below cannot be swept between that read and the commit of the staged
    // replica.
    auto outcome = WithObjectMetadataForWriteAndCleanup(
        object_id,
        [&](metadata::Tenant& tenant, const std::shared_ptr<ObjectEntry>&,
            ObjectMetadata& metadata, ObjectEntry::State& state)
            -> tl::expected<PromotionAllocStartResponse, ErrorCode> {
            // No usable replica: nothing is staged, no DRAM is charged.
            if (!metadata.IsValid()) {
                return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
            }
            if (state.is_processing) {
                return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
            }

            // Verify the in-flight task still exists before allocating. The
            // reaper can sweep it between the holder's heartbeat and this
            // AllocStart call (a hung client, GC pause, or HA failover can
            // stall AllocStart past put_start_release_timeout_sec_). If we
            // allocated and AddReplicas'd anyway, the staged PROCESSING MEMORY
            // replica would have no PromotionTask pointing at it: the generic
            // PROCESSING reaper walks state.is_processing (never set by
            // promotion) and the promotion-task reaper would have nothing left
            // to walk, leaking the buffer until the object is removed or
            // evicted.
            if (!state.promotion_task.has_value()) {
                return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
            }
            auto& promotion_task = *state.promotion_task;

            // Holder-only gate (see PromotionTask::holder_id doc).
            if (promotion_task.holder_id != client_id) {
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }

            // Defensive size check: must match the source LOCAL_DISK
            // descriptor's object_size captured at admission. A mismatch would
            // let a buggy caller request a wrong-sized allocation — smaller
            // risks RDMA overflow, larger wastes DRAM pinned until reaper TTL.
            if (promotion_task.object_size != size) {
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            if (metadata.HasReplica(&Replica::fn_is_memory_replica)) {
                return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
            }
            if (promotion_task.alloc_id != 0 ||
                promotion_task.pending_quota_charge_bytes != 0) {
                return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
            }

            const uint64_t pending_quota_charge = size;
            auto quota_result = ChargeTenantQuota(
                GetBoundTenantQuotaHandle(tenant), pending_quota_charge);
            if (!quota_result) {
                return tl::make_unexpected(quota_result.error());
            }
            auto refund_pending_quota = [&] {
                ReleaseTenantQuota(GetBoundTenantQuotaHandle(tenant),
                                   pending_quota_charge);
            };

            // Allocate a single MEMORY replica via the existing strategy,
            // biased to the holder's mem segment when possible.
            ReplicateConfig config;
            config.replica_num = 1;
            if (!preferred_segments.empty()) {
                config.preferred_segments = preferred_segments;
            }

            std::vector<Replica> staged_replicas;
            // Allocation takes the selected memory Segment owner's guard, so
            // release the holder guard first: a co-located holder would
            // self-deadlock, and two Clients could form a source/target lock
            // cycle. The entry lock stays held across the commit.
            serving_guard.reset();
            AllocatorManager allocator_snapshot;
            {
                ScopedAllocatorAccess allocator_access =
                    segment_manager_.getAllocatorAccess();
                allocator_snapshot =
                    allocator_access.SnapshotAllocatorManager();
            }
            auto allocation_result = allocation_strategy_->Allocate(
                allocator_snapshot, size, config.replica_num,
                preferred_segments);
            if (!allocation_result) {
                refund_pending_quota();
                return tl::make_unexpected(allocation_result.error());
            }
            staged_replicas = std::move(allocation_result.value());
            if (staged_replicas.empty()) {
                refund_pending_quota();
                return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
            }

            // Append the new PROCESSING MEMORY replica to the existing
            // object's metadata. Visible only after NotifyPromotionSuccess
            // flips it COMPLETE.
            Replica::Descriptor desc;
            if (!staged_replicas[0].getDescriptorIfAvailable(desc)) {
                refund_pending_quota();
                return tl::make_unexpected(
                    ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
            }
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
            promotion_task.alloc_id = new_id;
            promotion_task.pending_quota_charge_bytes = pending_quota_charge;
            promotion_task.start_time = std::chrono::system_clock::now();
            return PromotionAllocStartResponse{std::move(desc)};
        });
    if (!outcome.has_value()) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    return *outcome;
}

auto MasterService::NotifyPromotionSuccess(const UUID& client_id,
                                           const std::string& key,
                                           const TenantId& tenant_id)
    -> tl::expected<void, ErrorCode> {
    const auto record = FindClientRecord(client_id);
    auto retaining_guard =
        record ? record->TryAcquireRetainingGuard() : std::nullopt;
    if (!retaining_guard) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    const auto object_id = MakeObjectIdentityForRequest(key, tenant_id);
    // The entry lock is held for the whole read-modify-write, so the replica
    // this call commits cannot be swapped under it and the task it consumes
    // cannot be swept in between.
    const auto outcome = WithObjectMetadataForWriteAndCleanup(
        object_id,
        [&](metadata::Tenant& tenant, const std::shared_ptr<ObjectEntry>&,
            ObjectMetadata& metadata,
            ObjectEntry::State& state) -> tl::expected<void, ErrorCode> {
            // No usable replica, so no promotion of it can complete.
            if (!metadata.IsValid()) {
                return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
            }
            const auto previous_kv_media = KvMediaSnapshot(metadata);

            // Look up the in-flight task for the exact replica staged: a
            // concurrent Put on this key may have created other PROCESSING
            // MEMORY replicas, so committing "the first PROCESSING memory"
            // could commit someone else's half-written replica.
            if (!state.promotion_task.has_value() ||
                state.promotion_task->alloc_id == 0) {
                return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
            }
            auto& promotion_task = *state.promotion_task;

            // Holder-only gate (see PromotionTask::holder_id doc).
            if (promotion_task.holder_id != client_id) {
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }

            bool committed = false;
            Replica* staged = metadata.GetReplicaByID(promotion_task.alloc_id);
            if (staged != nullptr && staged->is_memory_replica() &&
                staged->is_processing()) {
                std::optional<OrderedOpLogWriter::Reservation>
                    batch_reservation;
                if (enable_ha_ && enable_oplog_) {
                    auto reservation = ReserveBatchOpLogSlot();
                    if (!reservation) {
                        return tl::make_unexpected(reservation.error());
                    }
                    batch_reservation = std::move(reservation.value());
                }
                staged->mark_complete();
                committed = true;
                if (enable_oplog_ && ordered_oplog_writer_) {
                    std::vector<Replica::Descriptor> post;
                    metadata.VisitReplicas(
                        &Replica::fn_is_completed,
                        [&post](const Replica& replica) {
                            post.push_back(replica.get_descriptor());
                        });

                    const auto payload =
                        SerializeMetadataForOpLogFromReplicaDescriptors(
                            metadata, post);
                    if (batch_reservation) {
                        auto persist_result =
                            AppendReservedOpLogWithDurableFinalize(
                                std::move(*batch_reservation), OpType::PUT_END,
                                tenant_id.value(), key, payload, nullptr);
                        if (!persist_result) {
                            LOG(WARNING)
                                << "NotifyPromotionSuccess: PUT_END persist "
                                   "failed for key="
                                << key << ", err="
                                << static_cast<int>(persist_result.error());
                        }
                    } else {
                        auto persist_result = AppendOpLogVisibleBeforeDurable(
                            OpType::PUT_END, tenant_id.value(), key, payload);
                        if (!persist_result) {
                            LOG(WARNING)
                                << "NotifyPromotionSuccess: PUT_END persist "
                                   "failed for key="
                                << key << ", err="
                                << static_cast<int>(persist_result.error());
                        }
                    }
                }
            }

            // Drop the source LOCAL_DISK replica's refcnt and erase the task.
            auto* source = metadata.GetReplicaByID(promotion_task.source_id);
            if (source != nullptr) {
                source->dec_refcnt();
            }
            const uint64_t completed_bytes = promotion_task.object_size;
            if (committed) {
                if (enable_multi_tenants_) {
                    auto settle_result = metadata.quota_ledger.SettleAdditional(
                        GetBoundTenantQuotaHandle(tenant),
                        promotion_task.pending_quota_charge_bytes,
                        completed_bytes);
                    if (!settle_result) {
                        LogTenantQuotaLedgerError(
                            settle_result, "settle_additional",
                            object_id.tenant_id, object_id.user_key);
                        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
                    }
                }
            } else {
                ReleaseTenantQuota(
                    GetBoundTenantQuotaHandle(tenant),
                    std::exchange(promotion_task.pending_quota_charge_bytes,
                                  0));
            }
            state.promotion_task.reset();
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

            // Erase the per-client promotion mailbox entry (best-effort; the
            // heartbeat may have already drained it).
            local_ssd_manager_.RemovePromotion(client_id, object_id.tenant_id,
                                               object_id.user_key);

            if (!committed) {
                return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
            }
            SyncKvObjectState(key, metadata, object_id.tenant_id,
                              previous_kv_media);
            return {};
        });
    if (!outcome.has_value()) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    return *outcome;
}

auto MasterService::NotifyPromotionFailure(const UUID& client_id,
                                           const std::string& key,
                                           const TenantId& tenant_id)
    -> tl::expected<void, ErrorCode> {
    const auto record = FindClientRecord(client_id);
    auto retaining_guard =
        record ? record->TryAcquireRetainingGuard() : std::nullopt;
    if (!retaining_guard) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    const auto object_id = MakeObjectIdentityForRequest(key, tenant_id);
    // The entry lock is held for the whole release, so the task cannot be
    // swept by the reaper between the task lookup and the teardown below.
    const auto outcome = WithObjectMetadataForWriteAndCleanup(
        object_id,
        [&](metadata::Tenant& tenant, const std::shared_ptr<ObjectEntry>& entry,
            ObjectMetadata& metadata,
            ObjectEntry::State& state) -> tl::expected<void, ErrorCode> {
            // No usable replica: the not-found answer of a never-published key.
            if (!metadata.IsValid()) {
                return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
            }
            if (!state.promotion_task.has_value()) {
                // No task to release. Either the reaper already swept it, or
                // the client never had a task here. Return OK to keep this RPC
                // idempotent — repeated failure notifications on the same key
                // should be safe.
                return {};
            }
            auto& promotion_task = *state.promotion_task;

            // Holder-only gate (see PromotionTask::holder_id doc).
            if (promotion_task.holder_id != client_id) {
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }

            // Mirror the reaper's expiry path; see
            // DiscardExpiredProcessingReplicas Part 4 for the full rationale on
            // each step.
            auto* source = metadata.GetReplicaByID(promotion_task.source_id);
            if (source != nullptr) {
                source->dec_refcnt();
            }
            if (promotion_task.alloc_id != 0) {
                const ReplicaID alloc_id = promotion_task.alloc_id;
                EraseReplicasWithCacheTotalAccounting(
                    metadata, [alloc_id](const Replica& replica) {
                        return replica.id() == alloc_id;
                    });
            }
            ReleaseTenantQuota(
                GetBoundTenantQuotaHandle(tenant),
                std::exchange(promotion_task.pending_quota_charge_bytes, 0));
            // Capture the chain's execution-failure count BEFORE resetting the
            // task.
            const uint32_t prior_failures = promotion_task.execution_failures;
            state.promotion_task.reset();
            promotion_in_flight_.fetch_sub(1, std::memory_order_relaxed);
            MasterMetricManager::instance().dec_promotion_in_flight();
            MasterMetricManager::instance().inc_promotion_failed();

            // A transient execution failure must not kill the promotion:
            // re-record a retry candidate for the retry loop, bounded by
            // kPromotionCandidateMaxRetries/kPromotionCandidateTtl and by
            // kMaxPromotionExecutionFailures. count(), never increment().
            if (!metadata.HasReplica(&Replica::fn_is_memory_replica) &&
                metadata.HasReplica(&Replica::fn_is_local_disk_replica)) {
                if (prior_failures >= kMaxPromotionExecutionFailures) {
                    LOG(WARNING)
                        << "promotion_execution_gave_up key="
                        << object_id.user_key << " failures=" << prior_failures;
                    MasterMetricManager::instance()
                        .inc_promotion_execution_gave_up();
                } else {
                    const auto admission_key =
                        object_id.tenant_id.MakeScopedKey(object_id.user_key);
                    const uint8_t freq =
                        promotion_sketch_
                            ? promotion_sketch_->count(admission_key)
                            : 0;
                    RecordOrUpdateCandidateLocked(
                        object_id.tenant_id, entry, state, freq,
                        PromotionCandidateReason::kExecutionFailed,
                        ErrorCode::OK, prior_failures + 1);
                }
            }

            // Clear the holder's per-client promotion mailbox entry. Same
            // best-effort cleanup pattern as NotifyPromotionSuccess — the
            // heartbeat may have already drained it.
            local_ssd_manager_.RemovePromotion(client_id, object_id.tenant_id,
                                               object_id.user_key);

            return {};
        });
    if (!outcome.has_value()) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    return *outcome;
}

void MasterService::EvictTenantsOverWatermark() {
    if (!enable_multi_tenants_ ||
        tenant_eviction_high_watermark_ratio_ <= 0.0) {
        return;
    }

    // Evict down to (watermark - eviction_ratio) of the tenant's quota,
    // mirroring what BatchEvict does for the pool: free a slab so subsequent
    // admissions find room already available, rather than freeing exactly the
    // deficit of the object currently being admitted.
    const double target_ratio =
        std::max(0.0, tenant_eviction_high_watermark_ratio_ - eviction_ratio_);

    for (const auto& snapshot : tenant_quota_table_.ListTenantSnapshots()) {
        if (snapshot.effective_quota_bytes == 0) {
            continue;
        }
        const double used_ratio =
            static_cast<double>(snapshot.charged_bytes) /
            static_cast<double>(snapshot.effective_quota_bytes);
        if (used_ratio <= tenant_eviction_high_watermark_ratio_) {
            continue;
        }
        const double excess_ratio = used_ratio - target_ratio;
        const auto target_bytes = static_cast<uint64_t>(
            excess_ratio * static_cast<double>(snapshot.effective_quota_bytes));
        if (target_bytes == 0) {
            continue;
        }

        // VLOG rather than INFO: this pass runs once per second and emits a
        // line per over-watermark tenant, so sustained quota pressure in a
        // large multi-tenant deployment would flood the log. The pool-level
        // [EVICT-TRIGGER] above stays at INFO because it is a single line
        // gated on a pool-wide threshold.
        VLOG(1) << "[TENANT-EVICT-TRIGGER] tenant=" << snapshot.tenant_id
                << " used_ratio=" << used_ratio
                << " high_watermark=" << tenant_eviction_high_watermark_ratio_
                << " target_ratio=" << target_ratio
                << " target_bytes=" << target_bytes;
        const TenantQuotaEvictionResult result =
            EvictTenantMemoryForQuota(snapshot.tenant_id, target_bytes);
        VLOG(1) << "[TENANT-EVICT-DONE] tenant=" << snapshot.tenant_id
                << " freed_bytes=" << result.freed_bytes
                << " evicted_objects=" << result.evicted_objects;
    }
}

void MasterService::EvictionThreadFunc() {
    VLOG(1) << "action=eviction_thread_started";

    auto last_discard_time = std::chrono::system_clock::now();
    auto next_dfs_eviction_time = std::chrono::steady_clock::now();
    auto next_tenant_eviction_time = std::chrono::steady_clock::now();
    while (eviction_running_) {
        const auto now = std::chrono::system_clock::now();
        double used_ratio = segment_manager_.GetMemoryUsage().used_ratio();
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
                tenants_.Visit(
                    [&](const TenantId&,
                        const std::shared_ptr<metadata::Tenant>& tenant) {
                        DiscardExpiredProcessingReplicas(*tenant, now);
                    });
                ReleaseExpiredDiscardedReplicas(now);
            }
            last_discard_time = now;
        }

        // Tenant-scoped watermark. Intentionally outside the branch above:
        // the case this exists for is a tenant over its OWN watermark while
        // the pool is still under the pool-wide one, so it must not be gated
        // on the pool-level condition having fired.
        if (enable_multi_tenants_ &&
            tenant_eviction_high_watermark_ratio_ > 0.0) {
            const auto steady_now = std::chrono::steady_clock::now();
            if (steady_now >= next_tenant_eviction_time) {
                EvictTenantsOverWatermark();
                next_tenant_eviction_time =
                    std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(kTenantEvictionCheckIntervalMs);
            }
        }

#ifdef USE_NOF
        double nof_used_ratio = nof_segment_manager_.GetUsage().used_ratio();
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

        if (dfs_allocator_ && dfs_allocator_->IsEvictionEnabled()) {
            const auto steady_now = std::chrono::steady_clock::now();
            if (steady_now >= next_dfs_eviction_time) {
                RunDfsEviction();
                next_dfs_eviction_time =
                    std::chrono::steady_clock::now() +
                    dfs_allocator_->GetEvictionCheckInterval();
            }
        }

        if (promotion_candidate_count_.load(std::memory_order_relaxed) > 0) {
            RunPromotionCandidateRetry();
        }

        std::this_thread::sleep_for(
            std::chrono::milliseconds(kEvictionThreadSleepMs));
    }

    VLOG(1) << "action=eviction_thread_stopped";
}

void MasterService::DiscardExpiredProcessingReplicas(
    metadata::Tenant& tenant,
    const std::chrono::system_clock::time_point& now) {
    std::list<DiscardedReplicas> discarded_replicas;

    // Each task of this tenant lives on its own entry, so one snapshot walk
    // reaches them all; each key is then resolved again under its own entry
    // lock, so the sweep acts on the publication the route holds now. Two
    // entries of one tenant are never held at once.
    const auto entries = tenant.SnapshotObjects();

    // The handle does not carry the tenant id, and the bookkeeping a removal
    // feeds is keyed by it, so the id is read from the first object.
    TenantId tenant_id = TenantId::Default();
    if (!entries.empty()) {
        entries.front()->WithSharedAccess(
            [&](const ObjectMetadata& metadata, const ObjectEntry::State&) {
                tenant_id = metadata.tenant_id;
            });
    }

    for (const auto& snapshot_entry : entries) {
        const std::string& key = snapshot_entry->key();
        (void)tenant.WithPublishedObject(key, [&](ObjectMetadata& metadata,
                                                  ObjectEntry::State& state) {
            if (!state.is_processing) {
                return;
            }
            // The callback runs under this entry's own lock, so the route still
            // publishes exactly this entry and the teardown below acts on this
            // publication.
            const std::shared_ptr<ObjectEntry> entry = tenant.Get(key);
            assert(entry != nullptr);

            if (!metadata.IsValid() ||
                metadata.AllReplicas(&Replica::fn_is_completed)) {
                metadata.ClearPendingSoftPinIfNoViableReplica();
                if (!metadata.IsValid()) {
                    (void)EraseMetadata(tenant, entry, metadata, state,
                                        tenant_id);
                } else {
                    auto settle_result =
                        SettlePrimaryWriteQuotaIfReady(tenant, metadata);
                    if (settle_result) {
                        state.is_processing = false;
                    }
                }
                return;
            }

            const auto ttl =
                metadata.put_start_time + put_start_release_timeout_sec_;
            if (ttl < now) {
                const bool had_complete_replica =
                    metadata.HasReplica(&Replica::fn_is_completed);
                // Predict post-discard descriptors WITHOUT mutating: drop
                // PROCESSING replicas; keep COMPLETE replicas.
                auto post_descriptors = BuildRemainingReplicaDescriptors(
                    metadata, &Replica::fn_is_processing);
                const bool would_invalidate = post_descriptors.empty();

                if (had_complete_replica && enable_oplog_ &&
                    ordered_oplog_writer_) {
                    // The replicas of this write round, recorded here: the
                    // callback runs later, after UpsertStart may have preempted
                    // this round on the same entry and started the next one,
                    // whose replicas carry other ids.
                    std::vector<ReplicaID> processing_replica_ids;
                    metadata.VisitReplicas(
                        &Replica::fn_is_processing,
                        [&processing_replica_ids](const Replica& replica) {
                            processing_replica_ids.push_back(replica.id());
                        });
                    tl::expected<OpLogEntry, ErrorCode> persist_result;
                    if (would_invalidate) {
                        persist_result = AppendOpLogWithDurableFinalize(
                                OpType::REMOVE, tenant_id.value(), key, {},
                                enable_oplog_
                                    ? [this, entry, ttl,
                                       processing_replica_ids](
                                          const OpLogEntry& durable_entry) {
                                          FinalizeExpiredProcessingReplicasAfterDurable(
                                              entry, durable_entry,
                                              processing_replica_ids, ttl);
                                      }
                                    : DurableFinalizeCallback{});
                    } else {
                        persist_result = AppendOpLogWithDurableFinalize(
                                OpType::PUT_END, tenant_id.value(), key,
                                SerializeMetadataForOpLogFromReplicaDescriptors(
                                    metadata, post_descriptors),
                                enable_oplog_
                                    ? [this, entry, ttl,
                                       processing_replica_ids](
                                          const OpLogEntry& durable_entry) {
                                          FinalizeExpiredProcessingReplicasAfterDurable(
                                              entry, durable_entry,
                                              processing_replica_ids, ttl);
                                      }
                                    : DurableFinalizeCallback{});
                    }
                    if (!persist_result) {
                        LOG(WARNING) << "DiscardExpiredProcessingReplicas: "
                                        "OpLog persist failed for key="
                                     << key << ", err="
                                     << static_cast<int>(persist_result.error())
                                     << ", deferring discard";
                        return;
                    }
                    if (enable_oplog_) {
                        return;
                    }
                }

                // Persist OK (or HA disabled / never published) — apply.
                auto replicas =
                    metadata.PopReplicas(&Replica::fn_is_processing);
                metadata.ClearPendingSoftPinIfNoViableReplica();
                if (!replicas.empty()) {
                    FreeDfsReplicas(key, replicas);
                    discarded_replicas.emplace_back(std::move(replicas), ttl);
                }
                if (!metadata.IsValid()) {
                    (void)EraseMetadata(tenant, entry, metadata, state,
                                        tenant_id);
                } else {
                    auto settle_result =
                        SettlePrimaryWriteQuotaIfReady(tenant, metadata);
                    if (settle_result) {
                        state.is_processing = false;
                    }
                }
            }
        });
    }

    for (const auto& snapshot_entry : entries) {
        const std::string& key = snapshot_entry->key();
        (void)tenant.WithPublishedObject(key, [&](ObjectMetadata& metadata,
                                                  ObjectEntry::State& state) {
            if (!state.replication_task.has_value()) {
                return;
            }
            auto& task = *state.replication_task;

            const auto ttl = task.start_time + put_start_release_timeout_sec_;
            if (ttl > now || task.durable_cleanup_pending) {
                return;
            }

            // The handle the route publishes now, resolved under this
            // entry's own lock: the dynamic-replication state and the
            // teardown below both name this publication.
            const std::shared_ptr<ObjectEntry> entry = tenant.Get(key);
            assert(entry != nullptr);

            const bool had_complete_replica =
                metadata.HasReplica(&Replica::fn_is_completed);
            const std::vector<ReplicaID> replica_ids = task.replica_ids;

            const auto target_pred = [&replica_ids](const Replica& r) {
                return std::find(replica_ids.begin(), replica_ids.end(),
                                 r.id()) != replica_ids.end();
            };
            // Predict post-discard descriptor list WITHOUT mutating: drop
            // task target replicas; keep the rest of the COMPLETE replicas.
            auto post_descriptors =
                BuildRemainingReplicaDescriptors(metadata, target_pred);
            const bool would_invalidate = post_descriptors.empty();

            if (had_complete_replica && enable_oplog_ &&
                ordered_oplog_writer_) {
                task.durable_cleanup_pending = true;
                tl::expected<OpLogEntry, ErrorCode> persist_result;
                auto source_id = task.source_id;
                auto target_ids = replica_ids;
                auto dynamic_lease_id = task.dynamic_replication_lease_id;
                auto dynamic_version_epoch =
                    task.dynamic_replication_version_epoch;
                if (would_invalidate) {
                    persist_result = AppendOpLogWithDurableFinalize(
                            OpType::REMOVE, tenant_id.value(), key, {},
                            enable_oplog_
                                ? [this, entry, source_id,
                                   target_ids = std::move(target_ids),
                                   dynamic_lease_id, dynamic_version_epoch,
                                   ttl](const OpLogEntry& durable_entry) {
                                      FinalizeExpiredReplicationTaskAfterDurable(
                                          entry, durable_entry, source_id,
                                          target_ids, dynamic_lease_id,
                                          dynamic_version_epoch, ttl);
                                  }
                                : DurableFinalizeCallback{});
                } else {
                    persist_result = AppendOpLogWithDurableFinalize(
                            OpType::PUT_END, tenant_id.value(), key,
                            SerializeMetadataForOpLogFromReplicaDescriptors(
                                metadata, post_descriptors),
                            enable_oplog_
                                ? [this, entry, source_id,
                                   target_ids = std::move(target_ids),
                                   dynamic_lease_id, dynamic_version_epoch,
                                   ttl](const OpLogEntry& durable_entry) {
                                      FinalizeExpiredReplicationTaskAfterDurable(
                                          entry, durable_entry, source_id,
                                          target_ids, dynamic_lease_id,
                                          dynamic_version_epoch, ttl);
                                  }
                                : DurableFinalizeCallback{});
                }
                if (!persist_result) {
                    LOG(WARNING)
                        << "DiscardExpiredProcessingReplicas: OpLog "
                           "persist failed for replication task key="
                        << key
                        << ", err=" << static_cast<int>(persist_result.error())
                        << ", deferring discard";
                    task.durable_cleanup_pending = false;
                    return;
                }
                if (enable_oplog_) {
                    return;
                }
            }

            auto source = metadata.GetReplicaByID(task.source_id);
            if (source != nullptr) {
                source->dec_refcnt();
            }

            auto replicas =
                PopReplicasWithCacheTotalAccounting(metadata, target_pred);
            std::vector<ReplicaID> erased_replica_ids;
            erased_replica_ids.reserve(replicas.size());
            for (const auto& replica : replicas) {
                erased_replica_ids.push_back(replica.id());
            }
            const bool dynamic_task =
                task.dynamic_replication_lease_id != UUID{} ||
                task.dynamic_replication_version_epoch != 0;
            RecordDynamicReplicaRemoval(metadata, erased_replica_ids);
            if (!replicas.empty()) {
                FreeDfsReplicas(key, replicas);
                discarded_replicas.emplace_back(std::move(replicas), ttl);
            }
            if (dynamic_task) {
                ClearDynamicReplicationStateLocked(tenant_id, entry, state);
            }
            if (!metadata.IsValid()) {
                (void)EraseMetadata(tenant, entry, metadata, state, tenant_id);
            } else {
                ReleaseTenantQuota(GetBoundTenantQuotaHandle(tenant),
                                   task.pending_quota_charge_bytes);
                state.replication_task.reset();
            }
        });
    }

    for (const auto& snapshot_entry : entries) {
        const std::string& key = snapshot_entry->key();
        (void)tenant.WithPublishedObject(
            key, [&](ObjectMetadata& metadata, ObjectEntry::State& state) {
                if (!state.offloading_task.has_value()) {
                    return;
                }
                const auto ttl = state.offloading_task->start_time +
                                 put_start_release_timeout_sec_;
                if (ttl > now) {
                    return;
                }
                auto source =
                    metadata.GetReplicaByID(state.offloading_task->source_id);
                if (source != nullptr) {
                    source->dec_refcnt();
                }
                LOG(WARNING) << "Offloading task expired for key: " << key
                             << " tenant=" << tenant_id;
                state.offloading_task.reset();
            });
    }

    for (const auto& snapshot_entry : entries) {
        const std::string& key = snapshot_entry->key();
        (void)tenant.WithPublishedObject(
            key, [&](ObjectMetadata& metadata, ObjectEntry::State& state) {
                if (!state.promotion_task.has_value()) {
                    return;
                }
                const auto ttl = state.promotion_task->start_time +
                                 put_start_release_timeout_sec_;
                if (ttl > now) {
                    return;
                }
                auto source =
                    metadata.GetReplicaByID(state.promotion_task->source_id);
                if (source != nullptr) {
                    source->dec_refcnt();
                }
                if (state.promotion_task->alloc_id != 0) {
                    const ReplicaID alloc_id = state.promotion_task->alloc_id;
                    EraseReplicasWithCacheTotalAccounting(
                        metadata, [alloc_id](const Replica& replica) {
                            return replica.id() == alloc_id;
                        });
                }
                ReleaseTenantQuota(
                    GetBoundTenantQuotaHandle(tenant),
                    std::exchange(
                        state.promotion_task->pending_quota_charge_bytes, 0));
                LOG(WARNING) << "Promotion task expired for key: " << key;
                state.promotion_task.reset();
                promotion_in_flight_.fetch_sub(1, std::memory_order_relaxed);
                MasterMetricManager::instance().dec_promotion_in_flight();
                MasterMetricManager::instance().inc_promotion_expired();
            });
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

/**
 * @brief Restore master state from snapshot using three-phase architecture.
 *
 * Phase 1 (Repository): Load candidate snapshots from catalog
 * Phase 2 (Repository + Codec): Download payloads and decode to memory
 * Phase 3 (Service): Apply decoded state and rebuild metrics
 *
 * Attempts restore from candidates in chronological order until one succeeds.
 * If all candidates fail, starts with a fresh state.
 */
void MasterService::RestoreState() {
    auto* snapshot_catalog_store = snapshot_catalog_store_.get();
    if (!snapshot_catalog_store) {
        LOG(ERROR) << "[Restore] Snapshot catalog store is not initialized, "
                      "starting fresh";
        return;
    }

    LOG(INFO) << "[Restore] Backend info: "
              << snapshot_object_store_->GetConnectionInfo();

    // Phase 1: Find snapshot candidates (repository responsibility)
    auto latest_result = snapshot_repository_->LoadLatestSnapshot();
    std::optional<ha::SnapshotDescriptor> latest_snapshot;
    if (!latest_result) {
        LOG(WARNING) << "[Restore] Failed to load latest snapshot marker: "
                     << toString(latest_result.error())
                     << ", falling back to published snapshot listing";
    } else {
        latest_snapshot = latest_result.value();
    }

    auto candidates_result =
        snapshot_repository_->LoadRestoreCandidates(latest_snapshot);
    if (!candidates_result || candidates_result->empty()) {
        LOG(ERROR) << "[Restore] No previous snapshot found, starting fresh";
        return;
    }

    // Phase 2 & 3: Try each candidate
    const auto now = std::chrono::system_clock::now();
    for (const auto& snapshot : candidates_result.value()) {
        ResetStateAfterFailedRestoreAttempt();

        try {
            // Phase 2a: Download payloads (repository responsibility)
            auto payloads_result =
                snapshot_repository_->DownloadSnapshotPayloads(snapshot);
            if (!payloads_result) {
                LOG(WARNING)
                    << "[Restore] Snapshot candidate " << snapshot.snapshot_id
                    << " is unusable: failed to download payloads: "
                    << payloads_result.error().message;
                continue;
            }

            // Phase 2b: Decode payloads (codec responsibility)
            auto decode_result =
                snapshot_codec_->Decode(this, payloads_result.value());
            if (!decode_result) {
                LOG(WARNING)
                    << "[Restore] Snapshot candidate " << snapshot.snapshot_id
                    << " is unusable: " << decode_result.error().message;
                continue;
            }

            // Phase 3: Apply state (master service responsibility)
            auto apply_result = ApplySnapshotState(now);
            if (!apply_result) {
                LOG(WARNING)
                    << "[Restore] Snapshot candidate " << snapshot.snapshot_id
                    << " is unusable: failed to apply state: "
                    << apply_result.error().message;
                continue;
            }

            LOG(INFO) << "[Restore] Successfully restored state from snapshot: "
                      << snapshot.snapshot_id;
            return;
        } catch (const std::exception& e) {
            LOG(WARNING) << "[Restore] Snapshot candidate "
                         << snapshot.snapshot_id
                         << " is unusable: exception during restore: "
                         << e.what();
            // State reset already happened at loop start; continue to next
            continue;
        } catch (...) {
            LOG(WARNING) << "[Restore] Snapshot candidate "
                         << snapshot.snapshot_id
                         << " is unusable: unknown exception during restore";
            continue;
        }
    }

    ResetStateAfterFailedRestoreAttempt();
    LOG(ERROR) << "[Restore] Failed to restore from all candidate snapshots "
               << "(count=" << candidates_result->size() << "), starting fresh";
}

void MasterService::ResetStateAfterFailedRestoreAttempt() {
    SegmentSerializer segment_serializer(&segment_manager_);
    MetadataSerializer metadata_serializer(this);
    TaskManagerSerializer task_manager_serializer(&task_manager_);

    task_manager_serializer.Reset();
    metadata_serializer.Reset();
    segment_serializer.Reset();
    local_ssd_manager_.Clear();

    {
        std::unique_lock<std::shared_mutex> lock(client_mutex_);
        ok_client_.clear();
        client_host_id_.clear();
        client_liveness_records_.clear();
    }

    MasterMetricManager::instance().reset_allocated_mem_size();
    MasterMetricManager::instance().reset_total_mem_capacity();
    MasterMetricManager::instance().reset_cache_total_nums();
    MasterMetricManager::instance().reset_client_liveness_metrics();
}

tl::expected<void, SerializationError>
MasterService::RebuildClientLivenessAfterSnapshotRestore() {
    std::vector<std::pair<Segment, UUID>> segments;
    {
        ScopedSegmentAccess segment_access =
            segment_manager_.getSegmentAccess();
        const auto err = segment_access.GetAllSegments(segments);
        if (err != ErrorCode::OK && err != ErrorCode::SEGMENT_NOT_FOUND) {
            return tl::make_unexpected(SerializationError(
                err, "failed to enumerate restored memory segments"));
        }
    }

    std::unordered_set<UUID, boost::hash<UUID>> client_ids;
    for (const auto& [segment, owner] : segments) {
        (void)segment;
        client_ids.insert(owner);
    }
    for (const auto& client_id : local_ssd_manager_.GetClientIds()) {
        client_ids.insert(client_id);
    }

    tenants_.Visit([&](const TenantId&,
                       const std::shared_ptr<metadata::Tenant>& tenant) {
        for (const auto& entry : tenant->SnapshotObjects()) {
            entry->WithSharedAccess([&](const ObjectMetadata& metadata,
                                        const ObjectEntry::State&) {
                for (const auto& replica : metadata.GetAllReplicas()) {
                    if (replica.is_local_disk_replica()) {
                        const auto owner = replica.get_local_disk_client_id();
                        if (owner) {
                            client_ids.insert(*owner);
                        }
                    }
                }
            });
        }
    });

    std::unordered_map<UUID, std::shared_ptr<ClientLivenessRecord>,
                       boost::hash<UUID>>
        records;
    records.reserve(client_ids.size());
    const auto now = ClientLivenessRecord::Clock::now();
    for (const auto& client_id : client_ids) {
        records.emplace(client_id, std::make_shared<ClientLivenessRecord>(now));
    }

    bool missing_memory_registration = false;
    {
        ScopedSegmentAccess segment_access =
            segment_manager_.getSegmentAccess();
        for (const auto& [client_id, record] : records) {
            segment_access.BindClientLiveness(client_id, record);
        }
        tenants_.Visit([&](const TenantId&,
                           const std::shared_ptr<metadata::Tenant>& tenant) {
            for (const auto& entry : tenant->SnapshotObjects()) {
                // Exclusive because the replicas below are rebound in place.
                entry->WithExclusiveAccess([&](ObjectMetadata& metadata,
                                               ObjectEntry::State&) {
                    metadata.VisitReplicas(
                        [](const Replica&) { return true; },
                        [&](Replica& replica) {
                            if (replica.is_memory_replica()) {
                                auto& buffer =
                                    *std::get<MemoryReplicaData>(replica.data_)
                                         .buffer;
                                if (!segment_access.RebindBufferToOwningSegment(
                                        buffer)) {
                                    missing_memory_registration = true;
                                }
                            } else if (replica.is_local_disk_replica()) {
                                const auto owner =
                                    replica.get_local_disk_client_id();
                                if (owner) {
                                    replica.bindClientLiveness(
                                        records.at(*owner));
                                }
                            }
                        });
                });
            }
        });
    }
    if (missing_memory_registration) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            "restored memory replica has no Segment registration"));
    }

    {
        std::unique_lock<std::shared_mutex> lock(client_mutex_);
        client_liveness_records_ = std::move(records);
    }
    MasterMetricManager::instance().reset_client_liveness_metrics(
        static_cast<int64_t>(client_ids.size()));
    return {};
}

tl::expected<void, SerializationError> MasterService::ApplySnapshotState(
    const std::chrono::system_clock::time_point& now) {
    // Note: Codec has already called Deserialize() on all payloads,
    // so the internal state is already restored. This method handles
    // post-restore cleanup and metrics rebuilding.

    auto liveness_result = RebuildClientLivenessAfterSnapshotRestore();
    if (!liveness_result) {
        return tl::make_unexpected(liveness_result.error());
    }

    std::vector<std::string> segment_names;
    {
        ScopedSegmentAccess segment_access =
            segment_manager_.getSegmentAccess();
        segment_access.GetAllSegmentNames(segment_names);
    }

    // Cleanup expired metadata (unless test environment disables it)
    {
        const bool skip_cleanup =
            std::getenv("MOONCAKE_MASTER_SERVICE_SNAPSHOT_TEST_SKIP_CLEANUP");
        if (!skip_cleanup) {
            auto cleanup_now = now;
            tenants_.Visit(
                [&](const TenantId& tenant_id,
                    const std::shared_ptr<metadata::Tenant>& tenant) {
                    // The stale entries are collected first because the erase
                    // below mutates the route this walk reads. Each pinned
                    // handle keeps the teardown on the entry that was
                    // inspected, never on a same-key replacement.
                    std::vector<std::shared_ptr<ObjectEntry>> stale_entries;
                    for (const auto& entry : tenant->SnapshotObjects()) {
                        entry->WithSharedAccess(
                            [&](const ObjectMetadata& metadata,
                                const ObjectEntry::State&) {
                                if (metadata.HasDiffRepStatus(
                                        ReplicaStatus::COMPLETE) ||
                                    metadata.IsLeaseExpired(cleanup_now)) {
                                    VLOG(1) << "clear metadata key="
                                            << entry->key();
                                    stale_entries.push_back(entry);
                                }
                            });
                    }
                    for (const auto& entry : stale_entries) {
                        // The route slot goes only while it still publishes
                        // this entry, so a same-key republish survives.
                        entry->WithExclusiveAccess(
                            [&](ObjectMetadata& metadata,
                                ObjectEntry::State& state) {
                                (void)EraseMetadata(*tenant, entry, metadata,
                                                    state, tenant_id);
                            });
                    }
                });
        }

        // Rebuild allocated memory metrics
        MasterMetricManager::instance().reset_allocated_mem_size();
        RebuildCacheTotalAccounting();
        for (auto& segment_name : segment_names) {
            MasterMetricManager::instance().reset_segment_allocated_mem_size(
                segment_name);
        }

        tenants_.Visit([&](const TenantId&,
                           const std::shared_ptr<metadata::Tenant>& tenant) {
            for (const auto& entry : tenant->SnapshotObjects()) {
                entry->WithSharedAccess([&](const ObjectMetadata& metadata,
                                            const ObjectEntry::State&) {
                    for (const auto& replica : metadata.GetAllReplicas()) {
                        if (!replica.get_descriptor().is_memory_replica()) {
                            continue;
                        }
                        auto temp_segment_names = replica.get_segment_names();
                        if (temp_segment_names.empty()) {
                            continue;
                        }
                        if (!temp_segment_names[0].has_value()) {
                            continue;
                        }
                        auto buffer_descriptor = replica.get_descriptor()
                                                     .get_memory_descriptor()
                                                     .buffer_descriptor;
                        MasterMetricManager::instance().inc_allocated_mem_size(
                            temp_segment_names[0].value(),
                            static_cast<int64_t>(buffer_descriptor.size_));
                    }
                });
            }
        });

        LOG(INFO) << "[Restore] Total allocated size after restore: "
                  << segment_manager_.GetMemoryUsage().used_bytes;
    }

    // Soft pin is runtime-only and is never restored from a snapshot.
    soft_pin_deadline_index_.Clear();

    // Rebuild total capacity metrics
    {
        MasterMetricManager::instance().reset_total_mem_capacity();
        for (auto& segment_name : segment_names) {
            MasterMetricManager::instance().reset_segment_total_mem_capacity(
                segment_name);
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

    return {};
}

MasterService::TenantQuotaEvictionResult
MasterService::EvictTenantMemoryForQuota(const TenantId& tenant_id,
                                         uint64_t target_bytes) {
    TenantQuotaEvictionResult total;
    if (!enable_multi_tenants_ || target_bytes == 0) {
        return total;
    }

    const TenantId normalized_tenant(tenant_id);
    auto now = std::chrono::system_clock::now();
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);

    // A tenant that is not registered owns no object, so there is nothing to
    // free for it.
    const auto tenant_handle = tenants_.Lookup(normalized_tenant);
    if (tenant_handle == nullptr) {
        return total;
    }
    metadata::Tenant& tenant = *tenant_handle;

    auto is_evictable_memory_replica = [this](const Replica& replica) {
        return IsEvictableMemoryReplica(replica);
    };
    auto can_evict_replicas = [&](const ObjectMetadata& metadata) {
        return metadata.HasReplica(is_evictable_memory_replica);
    };
    auto has_local_disk_replica = [](const ObjectMetadata& metadata) {
        return metadata.HasReplica(&Replica::fn_is_local_disk_replica);
    };
    auto evict_replicas =
        [&, this](metadata::Tenant& member_tenant, ObjectMetadata& metadata,
                  std::vector<std::vector<Replica>>& deferred_replicas) {
            const uint64_t before_charge = CompletedMemoryQuotaCharge(metadata);
            auto replicas = PopReplicasWithCacheTotalAccounting(
                metadata, is_evictable_memory_replica);
            std::vector<ReplicaID> erased_ids;
            erased_ids.reserve(replicas.size());
            for (const auto& replica : replicas) {
                erased_ids.push_back(replica.id());
            }
            RecordDynamicReplicaRemoval(metadata, erased_ids);
            const uint64_t replica_count = replicas.size();
            if (!replicas.empty()) {
                deferred_replicas.emplace_back(std::move(replicas));
            }
            const uint64_t after_charge = CompletedMemoryQuotaCharge(metadata);
            if (before_charge > after_charge) {
                auto release_result = metadata.quota_ledger.ReleaseCommitted(
                    GetBoundTenantQuotaHandle(member_tenant),
                    before_charge - after_charge);
                LogTenantQuotaLedgerError(release_result, "release_committed",
                                          metadata.tenant_id,
                                          metadata.user_key);
            }
            return metadata.size * replica_count;
        };
    long offload_queued_this_call = 0;
    long offload_deferred_count = 0;
    long offload_cap_forced_count = 0;
    long offload_push_failed_forced = 0;
    const long offload_cap =
        offload_on_evict_
            ? static_cast<long>(offloading_queue_limit_ * offload_cap_ratio_)
            : 0;

    auto try_evict_or_offload = [&, this](const std::string& key,
                                          ObjectMetadata& metadata,
                                          metadata::Tenant& member_tenant,
                                          ObjectEntry::State& state,
                                          std::vector<std::vector<Replica>>&
                                              deferred_replicas) {
        if (!offload_on_evict_) {
            return evict_replicas(member_tenant, metadata, deferred_replicas);
        }

        if (has_local_disk_replica(metadata)) {
            return evict_replicas(member_tenant, metadata, deferred_replicas);
        }

        if (offload_force_evict_ && offload_queued_this_call >= offload_cap) {
            ++offload_cap_forced_count;
            return evict_replicas(member_tenant, metadata, deferred_replicas);
        }

        bool queued = false;
        metadata.VisitReplicas(
            is_evictable_memory_replica,
            [this, &key, &normalized_tenant, &state, &queued,
             &now](Replica& replica) {
                if (queued) {
                    return;
                }
                std::vector<UUID> mirror_clients;
                auto result = PushOffloadingQueue(
                    MakeObjectIdentity(key, normalized_tenant), replica,
                    &mirror_clients);
                if (result) {
                    replica.inc_refcnt();
                    // The caller holds this member's own lock, so the task
                    // lands on the entry being evicted.
                    state.offloading_task = OffloadingTask{
                        replica.id(), now, std::move(mirror_clients)};
                    queued = true;
                }
            });

        if (queued) {
            ++offload_queued_this_call;
            ++offload_deferred_count;
            return evict_replicas(member_tenant, metadata, deferred_replicas);
        }

        if (offload_force_evict_) {
            ++offload_push_failed_forced;
            return evict_replicas(member_tenant, metadata, deferred_replicas);
        }
        return uint64_t{0};
    };

    // Evicts a single object or a whole group. MUST be called WITHOUT holding
    // any per-object lock: the trigger is resolved and re-validated under its
    // own lock, and for a grouped object EvictGroupOrObject resolves and locks
    // one member at a time.
    auto try_evict_group_or_object =
        [&, this](const std::string& key, bool allow_soft_pinned,
                  std::vector<std::vector<Replica>>& deferred_replicas)
        -> TenantQuotaEvictionResult {
        // Snapshot grouped-ness under the trigger's own lock; that lock is
        // released before EvictGroupOrObject.
        std::string group_id;
        TenantQuotaEvictionResult direct_result;
        const bool ran = tenant.WithPublishedObject(
            key, [&](ObjectMetadata& metadata, ObjectEntry::State& state) {
                // Re-validate: state may have changed since the collection
                // phase. Soft pins are only a blocker when this pass is not
                // allowed to evict them.
                if (now < metadata.EvictionDeadline() ||
                    (!allow_soft_pinned && IsSoftPinActive(metadata, now)) ||
                    !can_evict_replicas(metadata)) {
                    return;
                }
                if (metadata.IsGrouped()) {
                    group_id = metadata.group_id;
                    return;
                }
                const uint64_t freed = try_evict_or_offload(
                    key, metadata, tenant, state, deferred_replicas);
                direct_result = {.freed_bytes = freed,
                                 .evicted_objects = freed > 0 ? 1U : 0U};
                if (freed > 0) {
                    PublishKvRemovedAfterEvict(key, 1, "cpu", metadata,
                                               normalized_tenant);
                }
                if (!metadata.IsValid()) {
                    // The callback runs under this entry's own lock, so the
                    // route still publishes exactly this entry.
                    const auto entry = tenant.Get(key);
                    assert(entry != nullptr);
                    (void)EraseMetadata(tenant, entry, metadata, state,
                                        normalized_tenant);
                }
            });
        if (!ran) {
            return {};
        }
        if (group_id.empty()) {
            return direct_result;
        }
        TenantQuotaEvictionResult result = direct_result;

        // Grouped object: the group is evicted all-or-none at the group level
        // (either the whole group is protected or it is a candidate), based on
        // the shared group TTL. Within an evicted group, member-level
        // best-effort safety still applies (hard pins, soft pins, active
        // writes, busy replicas are skipped). Re-validation of each member
        // (lease/pin/evictable replica) happens inside EvictGroupOrObject under
        // the member's own lock, so this callback only performs the
        // path-specific eviction. Object routing is decoupled from groups, so
        // membership is read from the tenant's own object route.
        auto evict_one_member =
            [&, this](const std::string& member_key,
                      ObjectMetadata& member_metadata,
                      ObjectEntry::State& state,
                      metadata::Tenant& member_tenant) -> EvictMemberOutcome {
            const uint64_t freed =
                try_evict_or_offload(member_key, member_metadata, member_tenant,
                                     state, deferred_replicas);
            EvictMemberOutcome outcome{.freed_bytes = freed,
                                       .evicted_objects = freed > 0 ? 1 : 0};
            if (freed > 0) {
                PublishKvRemovedAfterEvict(member_key, 1, "cpu",
                                           member_metadata, normalized_tenant);
            }
            // A non-trigger member the eviction left invalid is torn down by
            // EvictGroupOrObject once this lock has been released, so this
            // callback does not erase it.
            return outcome;
        };

        GroupEvictionResult group_result =
            EvictGroupOrObject(normalized_tenant, key, group_id,
                               allow_soft_pinned, now, evict_one_member);
        result.freed_bytes = group_result.freed_bytes;
        result.evicted_objects =
            static_cast<uint64_t>(group_result.evicted_objects);

        // EvictGroupOrObject erased every member except the trigger, which is
        // this caller's to erase. Resolve it once more and drop it if the
        // eviction left it invalid; the route slot goes only while it still
        // publishes this object, so a same-key republish survives.
        (void)tenant.WithPublishedObject(
            key, [&](ObjectMetadata& metadata, ObjectEntry::State& state) {
                if (!metadata.IsValid()) {
                    const auto entry = tenant.Get(key);
                    assert(entry != nullptr);
                    (void)EraseMetadata(tenant, entry, metadata, state,
                                        normalized_tenant);
                }
            });
        return result;
    };

    auto pass = [&](bool allow_soft_pinned) {
        // One snapshot covers the tenant's whole population. The snapshot only
        // names the keys to consider: each is resolved and re-validated under
        // its own lock inside try_evict_group_or_object, and the pass stops as
        // soon as the target is met.
        std::vector<std::string> candidate_keys;
        for (const auto& entry : tenant.SnapshotObjects()) {
            entry->WithSharedAccess([&](const ObjectMetadata& metadata,
                                        const ObjectEntry::State&) {
                if (metadata.IsHardPinned() || !metadata.IsLeaseExpired(now) ||
                    (!allow_soft_pinned && IsSoftPinActive(metadata, now)) ||
                    !can_evict_replicas(metadata)) {
                    return;
                }
                candidate_keys.push_back(entry->key());
            });
        }

        // The popped replicas are held only until the key that owned them has
        // been torn down, so a quota-driven pass returns the freed space to the
        // allocator as each key completes.
        std::vector<std::vector<Replica>> deferred_replicas;
        for (const auto& key : candidate_keys) {
            if (total.freed_bytes >= target_bytes) {
                break;
            }
            auto evict_result = try_evict_group_or_object(
                key, allow_soft_pinned, deferred_replicas);
            total.freed_bytes += evict_result.freed_bytes;
            total.evicted_objects += evict_result.evicted_objects;
            deferred_replicas.clear();
        }
    };

    pass(/*allow_soft_pinned=*/false);
    if (allow_evict_soft_pinned_objects_ && total.freed_bytes < target_bytes) {
        pass(/*allow_soft_pinned=*/true);
    }

    if (total.freed_bytes > 0) {
        MasterMetricManager::instance().inc_tenant_evict_bytes(
            normalized_tenant.value(),
            static_cast<int64_t>(std::min<uint64_t>(
                total.freed_bytes,
                static_cast<uint64_t>(std::numeric_limits<int64_t>::max()))));
    }
    if (offload_on_evict_ && total.freed_bytes == 0 &&
        offload_deferred_count > 0) {
        LOG(WARNING) << "[TENANT-EVICT] No memory freed for tenant "
                     << normalized_tenant << "; " << offload_deferred_count
                     << " object(s) deferred for disk offload.";
    }
    if (offload_cap_forced_count > 0) {
        LOG(WARNING) << "[TENANT-EVICT] Offload cap (" << offload_cap
                     << ") reached for tenant " << normalized_tenant
                     << "; force-evicted " << offload_cap_forced_count
                     << " object(s) without disk offload.";
    }
    if (offload_push_failed_forced > 0) {
        LOG(WARNING) << "[TENANT-EVICT] PushOffloadingQueue failed for tenant "
                     << normalized_tenant << " on "
                     << offload_push_failed_forced
                     << " object(s); force-evicted without disk offload "
                        "(offload_force_evict=true).";
    }
    return total;
}

void MasterService::BatchEvict(double evict_ratio_target,
                               double evict_ratio_lowerbound) {
    // Consume the allocation-pressure signal before this pass. A new failure
    // during the pass remains visible to the next iteration.
    const bool allocation_pressure =
        need_mem_eviction_.exchange(false, std::memory_order_relaxed);

    if (evict_ratio_target < evict_ratio_lowerbound) {
        LOG(ERROR) << "evict_ratio_target=" << evict_ratio_target
                   << ", evict_ratio_lowerbound=" << evict_ratio_lowerbound
                   << ", error=invalid_params";
        evict_ratio_lowerbound = evict_ratio_target;
    }

    auto now = std::chrono::system_clock::now();

    auto is_evictable_memory_replica = [this](const Replica& replica) {
        return IsEvictableMemoryReplica(replica);
    };

    auto can_evict_replicas = [&](const ObjectMetadata& metadata) {
        return metadata.HasReplica(is_evictable_memory_replica);
    };

    auto evict_replicas =
        [&, this](metadata::Tenant& tenant, ObjectMetadata& metadata,
                  std::vector<std::vector<Replica>>& deferred_replicas) {
            if (enable_oplog_) {
                return metadata.size *
                       metadata.CountReplicas([](const Replica& replica) {
                           return replica.is_memory_replica() &&
                                  replica.status() == ReplicaStatus::REMOVED;
                       });
            }
            const uint64_t before_charge = CompletedMemoryQuotaCharge(metadata);
            auto replicas = PopReplicasWithCacheTotalAccounting(
                metadata, is_evictable_memory_replica);
            std::vector<ReplicaID> erased_ids;
            erased_ids.reserve(replicas.size());
            for (const auto& replica : replicas) {
                erased_ids.push_back(replica.id());
            }
            RecordDynamicReplicaRemoval(metadata, erased_ids);
            const size_t replica_count = replicas.size();
            if (!replicas.empty()) {
                deferred_replicas.emplace_back(std::move(replicas));
            }
            const uint64_t after_charge = CompletedMemoryQuotaCharge(metadata);
            if (enable_multi_tenants_ && before_charge > after_charge) {
                auto release_result = metadata.quota_ledger.ReleaseCommitted(
                    GetBoundTenantQuotaHandle(tenant),
                    before_charge - after_charge);
                LogTenantQuotaLedgerError(release_result, "release_committed",
                                          metadata.tenant_id,
                                          metadata.user_key);
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
            ? static_cast<long>(offloading_queue_limit_ * offload_cap_ratio_)
            : 0;

    auto has_local_disk_replica = [](const ObjectMetadata& metadata) {
        return metadata.HasReplica(&Replica::fn_is_local_disk_replica);
    };

    // Returns freed bytes. Returns 0 if offload-queued and no additional
    // replicas were evicted (all MEMORY replicas of the key are now pinned).
    auto try_evict_or_offload =
        [&, this](
            const TenantId& tenant_id, const std::string& key,
            ObjectMetadata& metadata, metadata::Tenant& tenant,
            ObjectEntry::State& state,
            std::vector<std::vector<Replica>>& deferred_replicas) -> uint64_t {
        if (enable_oplog_) {
            return evict_replicas(tenant, metadata, deferred_replicas);
        }
        if (!offload_on_evict_) {
            // Original behavior
            return evict_replicas(tenant, metadata, deferred_replicas);
        }

        // LOCAL_DISK replica already exists — safe to delete MEMORY immediately
        if (has_local_disk_replica(metadata)) {
            return evict_replicas(tenant, metadata, deferred_replicas);
        }

        // Force-evict cap: if force_evict enabled and cap reached, force
        // delete. Warning is aggregated at the end of the cycle to avoid log
        // flooding.
        if (offload_force_evict_ && offload_queued_this_cycle >= offload_cap) {
            offload_cap_forced_count++;
            return evict_replicas(tenant, metadata, deferred_replicas);
        }

        // Queue one MEMORY replica for offload; others will be evicted below.
        bool queued = false;
        metadata.VisitReplicas(
            is_evictable_memory_replica,
            [this, &tenant_id, &key, &state, &queued, &now](Replica& replica) {
                if (queued) {
                    return;  // only one replica has to be pinned for offload
                }
                std::vector<UUID> mirror_clients;
                auto result =
                    PushOffloadingQueue(MakeObjectIdentity(key, tenant_id),
                                        replica, &mirror_clients);
                if (result) {
                    replica.inc_refcnt();
                    // The caller holds this entry's own lock, so the task
                    // lands on the entry being evicted.
                    state.offloading_task = OffloadingTask{
                        replica.id(), now, std::move(mirror_clients)};
                    queued = true;
                }
            });

        if (queued) {
            offload_queued_this_cycle++;
            offload_deferred_count++;
            // Any remaining MEMORY replicas with refcnt==0 are redundant copies
            // (data survives via the pinned replica → disk). Evict them now to
            // reclaim memory immediately rather than waiting another cycle.
            return evict_replicas(tenant, metadata, deferred_replicas);
        }

        // PushOffloadingQueue failed. Default (data-preserving) behavior is to
        // skip this cycle — the outer eviction loop will retry after the
        // offload queue drains. Only force-evict when explicitly opted in, to
        // prevent silent data loss when the queue is unavailable.
        if (offload_force_evict_) {
            offload_push_failed_forced++;
            return evict_replicas(tenant, metadata, deferred_replicas);
        }
        return 0;
    };

    // HA strong-consistency: persist the post-eviction state before the
    // helper mutates metadata. Keep the removed IDs local until submission
    // succeeds so a consumed reservation cannot strand REMOVED replicas.
    auto persist_evict_oplog_or_skip =
        [&, this](const TenantId& tenant_id, const std::string& key,
                  const std::shared_ptr<ObjectEntry>& entry,
                  ObjectMetadata& metadata) -> tl::expected<void, ErrorCode> {
        if (!enable_oplog_) {
            return {};
        }

        // Predict the descriptor list after evict_replicas() runs:
        // drop COMPLETE memory replicas with refcnt==0; keep everything else
        // that is COMPLETE.
        auto remaining = BuildRemainingReplicaDescriptors(
            metadata, is_evictable_memory_replica);

        auto reservation = ReserveBatchOpLogSlot();
        if (!reservation) {
            LOG(WARNING) << "BatchEvict: OpLog reservation failed for key="
                         << key
                         << ", err=" << static_cast<int>(reservation.error())
                         << ", stopping eviction scan";
            return tl::make_unexpected(reservation.error());
        }
        std::vector<ReplicaID> removed_ids;
        metadata.VisitReplicas(is_evictable_memory_replica,
                               [&removed_ids](Replica& replica) {
                                   removed_ids.push_back(replica.id());
                                   replica.mark_removed();
                               });
        tl::expected<OpLogEntry, ErrorCode> persist_result;
        if (remaining.empty()) {
            persist_result = AppendReservedOpLogWithDurableFinalize(
                std::move(reservation.value()), OpType::REMOVE,
                tenant_id.value(), key, {},
                [this, entry, removed_ids](const OpLogEntry& durable_entry) {
                    FinalizeRemovedReplicasAfterDurable(entry, durable_entry,
                                                        removed_ids,
                                                        QuotaEraseMode::kFull);
                });
        } else {
            persist_result = AppendReservedOpLogWithDurableFinalize(
                std::move(reservation.value()), OpType::PUT_END,
                tenant_id.value(), key,
                SerializeMetadataForOpLogFromReplicaDescriptors(metadata,
                                                                remaining),
                [this, entry, removed_ids](const OpLogEntry& durable_entry) {
                    FinalizeRemovedReplicasAfterDurable(entry, durable_entry,
                                                        removed_ids,
                                                        QuotaEraseMode::kFull);
                });
        }
        if (!persist_result) {
            const ErrorCode error = persist_result.error();
            for (const auto& id : removed_ids) {
                if (auto* replica = metadata.GetReplicaByID(id);
                    replica != nullptr) {
                    replica->cancel_remove();
                }
            }
            LOG(WARNING) << "BatchEvict: OpLog persist failed for key=" << key
                         << ", err=" << static_cast<int>(error)
                         << ", stopping eviction scan";
            return tl::make_unexpected(error);
        }
        return {};
    };

    struct EvictionResult {
        uint64_t freed_bytes{0};
        long evicted_objects{0};
        bool stop_scan{false};
        // Set when the candidate was present but its re-validation failed
        // (lease still live, soft-pinned, or no evictable replica), so the
        // first pass can keep its lease timeout for the second-pass census.
        bool revalidation_skipped{false};
    };

    // Evicts a single object or a whole group. MUST be called WITHOUT any
    // per-object lock: the trigger is resolved and re-validated under its own
    // lock, and that lock is not held across the member walk, so each member is
    // re-resolved when its turn comes.
    auto try_evict_group_or_object =
        [&, this](const TenantId& tenant_id, const std::string& key,
                  bool allow_soft_pinned,
                  std::vector<std::vector<Replica>>& deferred_replicas)
        -> EvictionResult {
        const auto tenant_handle = tenants_.Lookup(tenant_id);
        if (tenant_handle == nullptr) {
            return {};
        }
        metadata::Tenant& tenant = *tenant_handle;

        // Snapshot grouped-ness under the trigger's own lock; that lock is
        // released before EvictGroupOrObject.
        std::string group_id;
        EvictionResult direct_result;
        const bool ran = tenant.WithPublishedObject(
            key, [&](ObjectMetadata& metadata, ObjectEntry::State& state) {
                // Re-validation: state may have changed since the census. Soft
                // pins are only a blocker when this pass is not allowed to
                // evict them.
                if (now < metadata.EvictionDeadline() ||
                    (!allow_soft_pinned && IsSoftPinActive(metadata, now)) ||
                    !can_evict_replicas(metadata)) {
                    direct_result.revalidation_skipped = true;
                    return;
                }

                if (metadata.IsGrouped()) {
                    group_id = metadata.group_id;
                    return;
                }
                // The callback runs under this entry's own lock, so this is the
                // publication the durable cleanup below belongs to.
                const auto entry = tenant.Get(key);
                assert(entry != nullptr);
                auto submission = persist_evict_oplog_or_skip(tenant_id, key,
                                                              entry, metadata);
                if (!submission) {
                    direct_result.stop_scan = true;
                    return;
                }
                const uint64_t freed = try_evict_or_offload(
                    tenant_id, key, metadata, tenant, state, deferred_replicas);
                direct_result.freed_bytes = freed;
                direct_result.evicted_objects = freed > 0 ? 1 : 0;
                if (!enable_oplog_ && freed > 0) {
                    PublishKvRemovedAfterEvict(key, 1, "cpu", metadata,
                                               tenant_id);
                }
                if (!enable_oplog_ && !metadata.IsValid()) {
                    // The callback runs under this entry's own lock, so the
                    // route still publishes exactly this entry.
                    const auto entry = tenant.Get(key);
                    assert(entry != nullptr);
                    (void)EraseMetadata(tenant, entry, metadata, state,
                                        tenant_id);
                }
            });
        if (!ran) {
            return {};
        }
        if (group_id.empty()) {
            return direct_result;
        }
        EvictionResult result = direct_result;

        // Grouped object: the group is evicted all-or-none at the group level
        // (either the whole group is protected or it is a candidate), based on
        // the shared group TTL. Within an evicted group, member-level
        // best-effort safety still applies below (hard pins, soft pins, active
        // writes, busy replicas are skipped). Re-validation of each member
        // (lease/pin/evictable replica) happens inside EvictGroupOrObject under
        // the member's own lock, so this callback only performs the
        // path-specific eviction. Object routing is decoupled from groups, so
        // membership is read from the tenant's own object route.
        auto evict_one_member =
            [&, this](const std::string& member_key,
                      ObjectMetadata& member_metadata,
                      ObjectEntry::State& state,
                      metadata::Tenant& member_tenant) -> EvictMemberOutcome {
            // The callback runs under this member's own lock, so this is the
            // publication the durable cleanup below belongs to.
            const auto member_entry = member_tenant.Get(member_key);
            assert(member_entry != nullptr);
            auto submission = persist_evict_oplog_or_skip(
                tenant_id, member_key, member_entry, member_metadata);
            if (!submission) {
                return {.stop_scan = true};
            }
            const uint64_t freed =
                try_evict_or_offload(tenant_id, member_key, member_metadata,
                                     member_tenant, state, deferred_replicas);
            EvictMemberOutcome outcome{.freed_bytes = freed,
                                       .evicted_objects = freed > 0 ? 1 : 0};
            if (freed > 0 && !enable_oplog_) {
                PublishKvRemovedAfterEvict(member_key, 1, "cpu",
                                           member_metadata, tenant_id);
            }
            // A non-trigger member the eviction left invalid is torn down by
            // EvictGroupOrObject once this lock has been released, so this
            // callback does not erase it.
            return outcome;
        };

        GroupEvictionResult group_result = EvictGroupOrObject(
            tenant_id, key, group_id, allow_soft_pinned, now, evict_one_member);
        result.freed_bytes = group_result.freed_bytes;
        result.evicted_objects = group_result.evicted_objects;
        result.stop_scan = group_result.stop_scan;

        // EvictGroupOrObject erased every member except the trigger, which is
        // the caller's to erase. Resolve it once more and drop it if the
        // eviction left it invalid; the route slot goes only while it still
        // publishes this object, so a same-key republish survives.
        if (!enable_oplog_) {
            (void)tenant.WithPublishedObject(
                key, [&](ObjectMetadata& metadata, ObjectEntry::State& state) {
                    if (!metadata.IsValid()) {
                        const auto entry = tenant.Get(key);
                        assert(entry != nullptr);
                        (void)EraseMetadata(tenant, entry, metadata, state,
                                            tenant_id);
                    }
                });
        }
        return result;
    };

    // Candidate carries the key for a safe lookup after the walk that found it
    // ends. Handles would be stale if a tenant's route is modified between
    // phases.
    struct Candidate {
        TenantId tenant_id;
        std::string key;
        std::chrono::system_clock::time_point lease_timeout;
    };

    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);

    // The census partitions over the tenants the registry holds right now, and
    // the handles are strong, so a tenant created or removed while the census
    // runs leaves no dangling tenant.
    std::vector<std::pair<TenantId, std::shared_ptr<metadata::Tenant>>> tenants;
    tenants_.Visit([&](const TenantId& tenant_id,
                       const std::shared_ptr<metadata::Tenant>& tenant) {
        tenants.emplace_back(tenant_id, tenant);
    });

    // ===== Phase 1: Parallel candidate census =====
    // N threads each scan a batch of tenants. For selective ratios only the
    // lease timestamps are collected here; full tenant/key identities are
    // materialized afterwards for a bounded frontier around the eviction
    // cutoff. High ratios collect full Candidates directly, because a census
    // followed by a second scan would cost more than the identities it saves.
    constexpr size_t kMaxCensusThreads = 16;
    const int num_threads =
        static_cast<int>(std::min(tenants.size(), kMaxCensusThreads));
    const size_t tenants_per_thread =
        num_threads == 0 ? 0 : (tenants.size() + num_threads - 1) / num_threads;

    constexpr size_t kMinReserveSlack = 1024;
    constexpr size_t kMinFrontierLimit = 64 * 1024;
    constexpr size_t kReserveSlackDivisor = 10;
    constexpr size_t kFrontierDivisor = 4;
    // Above this target ratio the reserve frontier would already cover a
    // large share of the population, so selective materialization stops
    // paying for the extra scan it costs.
    constexpr double kCompactPrebypassTargetRatio =
        static_cast<double>(kReserveSlackDivisor) /
        static_cast<double>(kFrontierDivisor * (kReserveSlackDivisor + 1));
    const bool compact_frontier_prebypass =
        evict_ratio_target >= kCompactPrebypassTargetRatio;

    std::vector<std::vector<Candidate>> local_candidates(num_threads);
    std::vector<std::vector<std::chrono::system_clock::time_point>>
        local_no_pin(num_threads);
    std::vector<long> local_eviction_base(num_threads, 0);
    std::vector<long> local_object_count(num_threads, 0);
    std::vector<std::vector<std::chrono::system_clock::time_point>>
        local_soft_pin(num_threads);

    std::vector<std::thread> threads;
    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back([&, t] {
            const size_t s_start = t * tenants_per_thread;
            const size_t s_end =
                std::min(s_start + tenants_per_thread, tenants.size());
            for (size_t s = s_start; s < s_end; s++) {
                const TenantId& tenant_id = tenants[s].first;
                const auto& tenant = tenants[s].second;
                DiscardExpiredProcessingReplicas(*tenant, now);

                local_object_count[t] +=
                    static_cast<long>(tenant->ObjectCount());
                // The snapshot only names the keys to consider: every eviction
                // re-resolves its candidate under that object's own lock.
                for (const auto& entry : tenant->SnapshotObjects()) {
                    entry->WithSharedAccess([&](const ObjectMetadata& metadata,
                                                const ObjectEntry::State&) {
                        if (metadata.IsHardPinned()) {
                            return;
                        }
                        const bool has_evictable = can_evict_replicas(metadata);
                        if (has_evictable) {
                            local_eviction_base[t]++;
                        }
                        // Grouped objects are evicted all-or-none, so rank them
                        // by the shared group TTL instead of each member's own
                        // lease: this keeps the candidate cutoff aligned with
                        // group boundaries and prevents over-eviction.
                        const auto deadline = metadata.EvictionDeadline();
                        if (now < deadline || !has_evictable) {
                            return;
                        }
                        if (!IsSoftPinActive(metadata, now)) {
                            if (compact_frontier_prebypass) {
                                local_candidates[t].push_back(
                                    {tenant_id, entry->key(), deadline});
                            } else {
                                local_no_pin[t].push_back(deadline);
                            }
                        } else if (allow_evict_soft_pinned_objects_) {
                            local_soft_pin[t].push_back(deadline);
                        }
                    });
                }
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
    if (compact_frontier_prebypass) {
        size_t total = 0;
        for (auto& v : local_candidates) total += v.size();
        candidates.reserve(total);
        for (auto& v : local_candidates) {
            candidates.insert(candidates.end(),
                              std::make_move_iterator(v.begin()),
                              std::make_move_iterator(v.end()));
        }
    }

    std::vector<std::chrono::system_clock::time_point> no_pin_timeouts;
    {
        size_t total = 0;
        for (auto& v : local_no_pin) total += v.size();
        no_pin_timeouts.reserve(total);
    }
    for (auto& v : local_no_pin) {
        no_pin_timeouts.insert(no_pin_timeouts.end(),
                               std::make_move_iterator(v.begin()),
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
        VLOG(1) << "[EVICT-DIAG] object_count=" << object_count
                << " eviction_base=0 (no evictable memory objects)";
        return;
    }

    const long ideal_evict_num =
        std::ceil(total_eviction_base * evict_ratio_target);
    const size_t no_pin_count =
        compact_frontier_prebypass ? candidates.size() : no_pin_timeouts.size();
    const long primary_no_pin_num =
        std::min(ideal_evict_num, static_cast<long>(no_pin_count));

    // Re-scan metadata and copy full identities only for objects inside the
    // requested timestamp range. The eligibility conditions are identical to
    // the census above, so the selected set matches what the census counted.
    auto collect_candidates = [&](bool use_cutoff,
                                  std::chrono::system_clock::time_point cutoff,
                                  bool collect_older_or_equal) {
        std::vector<std::vector<Candidate>> local_frontier(num_threads);
        std::vector<std::thread> collectors;
        collectors.reserve(num_threads);

        for (int t = 0; t < num_threads; t++) {
            collectors.emplace_back([&, t] {
                const size_t s_start = t * tenants_per_thread;
                const size_t s_end =
                    std::min(s_start + tenants_per_thread, tenants.size());
                for (size_t s = s_start; s < s_end; s++) {
                    const TenantId& tenant_id = tenants[s].first;
                    const auto& tenant = tenants[s].second;
                    for (const auto& entry : tenant->SnapshotObjects()) {
                        entry->WithSharedAccess(
                            [&](const ObjectMetadata& metadata,
                                const ObjectEntry::State&) {
                                if (metadata.IsHardPinned() ||
                                    IsSoftPinActive(metadata, now) ||
                                    !can_evict_replicas(metadata)) {
                                    return;
                                }
                                // Group-aware eviction deadline (shared group
                                // TTL for grouped objects) so the cutoff
                                // matches eviction.
                                const auto deadline =
                                    metadata.EvictionDeadline();
                                if (now < deadline) {
                                    return;
                                }
                                if (use_cutoff) {
                                    const bool in_range =
                                        collect_older_or_equal
                                            ? deadline <= cutoff
                                            : deadline > cutoff;
                                    if (!in_range) {
                                        return;
                                    }
                                }
                                local_frontier[t].push_back(
                                    {tenant_id, entry->key(), deadline});
                            });
                    }
                }
            });
        }
        for (auto& collector : collectors) collector.join();

        size_t total = 0;
        for (const auto& v : local_frontier) total += v.size();
        std::vector<Candidate> merged;
        merged.reserve(total);
        for (auto& v : local_frontier) {
            merged.insert(merged.end(), std::make_move_iterator(v.begin()),
                          std::make_move_iterator(v.end()));
        }
        return merged;
    };

    bool compact_frontier_used = false;
    std::chrono::system_clock::time_point reserve_cutoff{};

    if (primary_no_pin_num > 0 && !compact_frontier_prebypass) {
        const size_t primary_count = static_cast<size_t>(primary_no_pin_num);
        // The reserve absorbs objects that stop being evictable between the
        // census and the eviction pass, so ordinary churn does not require a
        // second materialization pass.
        const size_t reserve_slack = std::max(
            kMinReserveSlack,
            (primary_count + kReserveSlackDivisor - 1) / kReserveSlackDivisor);
        const size_t reserve_count =
            std::min(no_pin_count, primary_count + reserve_slack);
        const size_t frontier_limit =
            std::max(kMinFrontierLimit,
                     (no_pin_count + kFrontierDivisor - 1) / kFrontierDivisor);

        if (reserve_count <= frontier_limit) {
            std::nth_element(no_pin_timeouts.begin(),
                             no_pin_timeouts.begin() + (reserve_count - 1),
                             no_pin_timeouts.end());
            reserve_cutoff = no_pin_timeouts[reserve_count - 1];
            candidates = collect_candidates(/*use_cutoff=*/true, reserve_cutoff,
                                            /*collect_older_or_equal=*/true);

            // Shortfall guard: if churn left the frontier holding fewer
            // objects than the target needs, fall back to the full candidate
            // set so evict_num below still derives from the requested target
            // rather than from a shrunken frontier.
            if (candidates.size() >= primary_count) {
                compact_frontier_used = true;
            } else {
                candidates =
                    collect_candidates(/*use_cutoff=*/false, {},
                                       /*collect_older_or_equal=*/true);
            }
        } else {
            candidates = collect_candidates(/*use_cutoff=*/false, {},
                                            /*collect_older_or_equal=*/true);
        }
    }
    // ===== Phase 2: Serial eviction via key lookup =====
    long evicted_count = 0;
    uint64_t total_freed_size = 0;
    bool stop_eviction_scan = false;
    std::vector<std::chrono::system_clock::time_point> no_pin_objects;
    std::vector<std::vector<Replica>> deferred_replicas;

    // First pass: evict candidates with no soft pin
    if (!candidates.empty()) {
        long evict_num = std::min(ideal_evict_num, (long)candidates.size());

        std::nth_element(candidates.begin(),
                         candidates.begin() + (evict_num - 1), candidates.end(),
                         [](const Candidate& a, const Candidate& b) {
                             return a.lease_timeout < b.lease_timeout;
                         });

        // Treat evict_num as a minimum: if re-validation skips a candidate,
        // continue trying the next one so actual evicted count reaches
        // evict_num.
        long evicted_this_pass = 0;
        auto evict_candidate_batch = [&](std::vector<Candidate>& batch) {
            for (auto& c : batch) {
                if (stop_eviction_scan) break;
                // Stop once the target object count is reached. Grouped
                // eviction is all-or-none, so evicting a group can overshoot
                // evict_num by at most one group; using evict_num (rather than
                // the per-object lease cutoff) bounds that overshoot and keeps
                // the eviction ratio close to the target.
                if (evicted_this_pass >= evict_num) {
                    no_pin_objects.push_back(c.lease_timeout);
                    continue;
                }
                auto evict_result = try_evict_group_or_object(
                    c.tenant_id, c.key,
                    /*allow_soft_pinned=*/false, deferred_replicas);

                total_freed_size += evict_result.freed_bytes;

                if (evict_result.revalidation_skipped) {
                    no_pin_objects.push_back(c.lease_timeout);
                }

                evicted_count += evict_result.evicted_objects;
                evicted_this_pass += evict_result.evicted_objects;
                if (evict_result.stop_scan) {
                    stop_eviction_scan = true;
                }
                deferred_replicas.clear();
            }
        };

        evict_candidate_batch(candidates);

        // Metadata may change after the frontier is materialized. If the
        // reserve is exhausted before the target is met, refill from the
        // remainder of the current no-soft-pin population. This recovery
        // scan is paid only on churn and preserves the behavior of
        // continuing past the cutoff until evict_num is reached.
        if (!stop_eviction_scan && compact_frontier_used &&
            evicted_this_pass < evict_num) {
            auto refill_candidates = collect_candidates(
                /*use_cutoff=*/true, reserve_cutoff,
                /*collect_older_or_equal=*/false);
            evict_candidate_batch(refill_candidates);
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
    if (!stop_eviction_scan && target_evict_num > 0) {
        if (target_evict_num <= static_cast<long>(no_pin_objects.size())) {
            // Second pass A: only evict objects without soft pin.
            std::nth_element(no_pin_objects.begin(),
                             no_pin_objects.begin() + (target_evict_num - 1),
                             no_pin_objects.end());
            auto target_timeout = no_pin_objects[target_evict_num - 1];

            // Evict via key lookup, avoiding a second route traversal per
            // candidate: one walk collects the keys inside the target timeout,
            // and eviction re-resolves each under its own lock.
            {
                std::vector<std::pair<TenantId, std::string>> to_evict;
                tenants_.Visit(
                    [&](const TenantId& tenant_id,
                        const std::shared_ptr<metadata::Tenant>& tenant) {
                        for (const auto& entry : tenant->SnapshotObjects()) {
                            entry->WithSharedAccess(
                                [&](const ObjectMetadata& metadata,
                                    const ObjectEntry::State&) {
                                    if (!metadata.IsHardPinned() &&
                                        now >= metadata.EvictionDeadline() &&
                                        metadata.EvictionDeadline() <=
                                            target_timeout &&
                                        !IsSoftPinActive(metadata, now) &&
                                        can_evict_replicas(metadata)) {
                                        to_evict.emplace_back(tenant_id,
                                                              entry->key());
                                    }
                                });
                        }
                    });
                for (auto& c : to_evict) {
                    if (target_evict_num <= 0 || stop_eviction_scan) break;
                    auto evict_result = try_evict_group_or_object(
                        c.first, c.second,
                        /*allow_soft_pinned=*/false, deferred_replicas);
                    total_freed_size += evict_result.freed_bytes;
                    evicted_count += evict_result.evicted_objects;
                    target_evict_num -= evict_result.evicted_objects;
                    if (evict_result.stop_scan) {
                        stop_eviction_scan = true;
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

            {
                std::vector<std::pair<TenantId, std::string>> to_evict;
                tenants_.Visit(
                    [&](const TenantId& tenant_id,
                        const std::shared_ptr<metadata::Tenant>& tenant) {
                        for (const auto& entry : tenant->SnapshotObjects()) {
                            entry->WithSharedAccess(
                                [&](const ObjectMetadata& metadata,
                                    const ObjectEntry::State&) {
                                    if (metadata.IsHardPinned() ||
                                        now < metadata.EvictionDeadline() ||
                                        !can_evict_replicas(metadata)) {
                                        return;
                                    }
                                    if (!IsSoftPinActive(metadata, now) ||
                                        metadata.EvictionDeadline() <=
                                            soft_target_timeout) {
                                        to_evict.emplace_back(tenant_id,
                                                              entry->key());
                                    }
                                });
                        }
                    });
                for (auto& c : to_evict) {
                    if (target_evict_num <= 0 || stop_eviction_scan) break;
                    auto evict_result = try_evict_group_or_object(
                        c.first, c.second,
                        /*allow_soft_pinned=*/true, deferred_replicas);
                    total_freed_size += evict_result.freed_bytes;
                    evicted_count += evict_result.evicted_objects;
                    target_evict_num -= evict_result.evicted_objects;
                    if (evict_result.stop_scan) {
                        stop_eviction_scan = true;
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

    const bool made_progress = evicted_count > 0 || released_discarded_cnt > 0;
    const bool success = made_progress || offload_deferred_count > 0;
    if (allocation_pressure && !stop_eviction_scan && !success &&
        total_eviction_base > 0) {
        need_mem_eviction_.store(true, std::memory_order_relaxed);
    }

    if (success) {
        MasterMetricManager::instance().inc_eviction_success(evicted_count,
                                                             total_freed_size);
        MasterMetricManager::instance().inc_mem_eviction_success(
            evicted_count, total_freed_size);
    } else {
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
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);

    auto is_evictable_nof_replica = [](const Replica& replica) {
        return replica.is_nof_replica() && replica.is_completed() &&
               replica.get_refcnt() == 0;
    };

    // Count every object first: the target is a share of the whole population.
    // Discarding expired processing replicas before the count keeps the count
    // and the eviction pass on the same population.
    tenants_.Visit(
        [&](const TenantId&, const std::shared_ptr<metadata::Tenant>& tenant) {
            DiscardExpiredProcessingReplicas(*tenant, now);
            object_count += static_cast<long>(tenant->ObjectCount());
        });
    const long ideal_evict_num =
        static_cast<long>(std::ceil(object_count * evict_ratio_target));

    if (ideal_evict_num > 0) {
        tenants_.Visit([&](const TenantId& tenant_id,
                           const std::shared_ptr<metadata::Tenant>& tenant) {
            if (evicted_count >= ideal_evict_num) {
                return;
            }
            // The snapshot only names the keys to consider: each one is
            // resolved again under its own lock below.
            for (const auto& snapshot_entry : tenant->SnapshotObjects()) {
                if (evicted_count >= ideal_evict_num) {
                    break;
                }
                const std::string& key = snapshot_entry->key();
                (void)tenant->WithPublishedObject(key, [&](ObjectMetadata&
                                                               metadata,
                                                           ObjectEntry::State&
                                                               state) {
                    // Runs under this entry's own lock, so this is the
                    // publication the durable cleanup below belongs to.
                    const auto entry = tenant->Get(key);
                    assert(entry != nullptr);
                    if (metadata.IsHardPinned() ||
                        !metadata.IsLeaseExpired(now) ||
                        IsSoftPinActive(metadata, now)) {
                        return;
                    }

                    // Probe: any NoF replicas eligible for eviction?
                    const bool has_evictable_nof =
                        metadata.HasReplica(is_evictable_nof_replica);
                    if (!has_evictable_nof) {
                        return;
                    }

                    // HA strong consistency: persist BEFORE erasing NoF
                    // replicas. Skip the key on persist failure.
                    if (enable_oplog_ && ordered_oplog_writer_) {
                        auto remaining = BuildRemainingReplicaDescriptors(
                            metadata, is_evictable_nof_replica);
                        if (enable_oplog_) {
                            auto reservation = ReserveBatchOpLogSlot();
                            if (!reservation) {
                                LOG(WARNING)
                                    << "NoFBatchEvict: OpLog reservation "
                                       "failed for key="
                                    << key << ", err="
                                    << static_cast<int>(reservation.error())
                                    << ", skipping eviction";
                                return;
                            }
                            std::vector<ReplicaID> removed_ids;
                            metadata.VisitReplicas(
                                is_evictable_nof_replica,
                                [&removed_ids](Replica& replica) {
                                    removed_ids.push_back(replica.id());
                                    replica.mark_removed();
                                });
                            const size_t removed_count = removed_ids.size();
                            tl::expected<OpLogEntry, ErrorCode> persist_result;
                            if (remaining.empty()) {
                                persist_result =
                                    AppendReservedOpLogWithDurableFinalize(
                                        std::move(reservation.value()),
                                        OpType::REMOVE, tenant_id.value(), key,
                                        {},
                                        [this, entry,
                                         removed_ids = std::move(removed_ids)](
                                            const OpLogEntry& durable_entry) {
                                            FinalizeRemovedReplicasAfterDurable(
                                                entry, durable_entry,
                                                removed_ids,
                                                QuotaEraseMode::kFull);
                                        });
                            } else {
                                persist_result =
                                    AppendReservedOpLogWithDurableFinalize(
                                        std::move(reservation.value()),
                                        OpType::PUT_END, tenant_id.value(), key,
                                        SerializeMetadataForOpLogFromReplicaDescriptors(
                                            metadata, remaining),
                                        [this, entry,
                                         removed_ids = std::move(removed_ids)](
                                            const OpLogEntry& durable_entry) {
                                            FinalizeRemovedReplicasAfterDurable(
                                                entry, durable_entry,
                                                removed_ids,
                                                QuotaEraseMode::kFull);
                                        });
                            }
                            if (!persist_result) {
                                LOG(WARNING)
                                    << "NoFBatchEvict: OpLog persist "
                                       "failed for key="
                                    << key << ", err="
                                    << static_cast<int>(persist_result.error())
                                    << ", skipping eviction";
                                return;
                            }
                            total_freed_size += metadata.size * removed_count;
                            evicted_count++;
                            return;
                        }

                        tl::expected<OpLogEntry, ErrorCode> persist_result;
                        if (remaining.empty()) {
                            persist_result = AppendOpLogWithDurableFinalize(
                                OpType::REMOVE, tenant_id.value(), key, {},
                                nullptr);
                        } else {
                            persist_result = AppendOpLogWithDurableFinalize(
                                OpType::PUT_END, tenant_id.value(), key,
                                SerializeMetadataForOpLogFromReplicaDescriptors(
                                    metadata, remaining),
                                nullptr);
                        }
                        if (!persist_result) {
                            LOG(WARNING)
                                << "NoFBatchEvict: OpLog persist failed "
                                   "for key="
                                << key << ", err="
                                << static_cast<int>(persist_result.error())
                                << ", skipping eviction";
                            return;
                        }
                    }

                    const size_t erased =
                        metadata.EraseReplicas(is_evictable_nof_replica);
                    if (erased == 0) {
                        return;
                    }

                    total_freed_size += metadata.size * erased;
                    evicted_count++;
                    PublishKvRemovedAfterEvict(key, erased, "disk", metadata,
                                               tenant_id);
                    if (!metadata.IsValid()) {
                        // The callback runs under this entry's own lock, so
                        // the route still publishes exactly this entry.
                        const auto entry = tenant->Get(key);
                        assert(entry != nullptr);
                        (void)EraseMetadata(*tenant, entry, metadata, state,
                                            tenant_id);
                    }
                });
            }
        });
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
    while (client_monitor_running_) {
        const auto now = ClientLivenessRecord::Clock::now();
        std::vector<std::pair<UUID, std::shared_ptr<ClientLivenessRecord>>>
            clients;
        {
            std::shared_lock<std::shared_mutex> lock(client_mutex_);
            clients.reserve(client_liveness_records_.size());
            for (const auto& entry : client_liveness_records_) {
                clients.push_back(entry);
            }
        }

        for (const auto& [client_id, record] : clients) {
            const auto transition = record->EvaluateAndRetire(
                now, std::chrono::seconds(client_active_ttl_sec_),
                std::chrono::seconds(client_suspicion_ttl_sec_),
                [&] { client_offboarding_worker_.ReserveJob(); },
                [&] {
                    {
                        std::unique_lock<std::shared_mutex> client_lock(
                            client_mutex_);
                        const auto current =
                            client_liveness_records_.find(client_id);
                        CHECK(current != client_liveness_records_.end());
                        CHECK(current->second == record);
                        if (ok_client_.erase(client_id) != 0) {
                            MasterMetricManager::instance()
                                .dec_active_clients();
                        }
                        client_host_id_.erase(client_id);
                    }
                    graceful_unmount_scheduler_.RemoveIf(
                        [&client_id](
                            const GracefulUnmountDeadlineRecord& pending) {
                            return pending.client_id == client_id;
                        });

                    ClientOffboardingJob job;
                    job.client_id = client_id;
                    job.liveness = record;
                    std::shared_lock<std::shared_mutex> snapshot_lock(
                        snapshot_mutex_);
                    {
                        ScopedSegmentAccess segment_access =
                            segment_manager_.getSegmentAccess();
                        std::vector<Segment> segments;
                        const auto get_result =
                            segment_access.GetClientSegments(client_id,
                                                             segments);
                        if (get_result == ErrorCode::OK) {
                            for (const auto& segment : segments) {
                                SegmentStatus status = SegmentStatus::UNDEFINED;
                                const auto status_result =
                                    segment_access.GetSegmentStatusById(
                                        segment.id, status);
                                if (status_result ==
                                    ErrorCode::SEGMENT_NOT_FOUND) {
                                    continue;
                                }
                                if (status_result == ErrorCode::OK &&
                                    status == SegmentStatus::UNMOUNTING) {
                                    job.pending_prepare_segments.push_back(
                                        {.segment_id = segment.id,
                                         .segment_name = segment.name,
                                         .transport_endpoint =
                                             segment.te_endpoint});
                                    continue;
                                }

                                size_t metrics_dec_capacity = 0;
                                const auto prepare_result =
                                    segment_access.PrepareUnmountSegment(
                                        segment.id, metrics_dec_capacity);
                                if (prepare_result == ErrorCode::OK) {
                                    job.prepared_segments.push_back(
                                        {.segment_id = segment.id,
                                         .segment_name = segment.name,
                                         .transport_endpoint =
                                             segment.te_endpoint,
                                         .metrics_dec_capacity =
                                             metrics_dec_capacity});
                                } else if (prepare_result !=
                                           ErrorCode::SEGMENT_NOT_FOUND) {
                                    job.pending_prepare_segments.push_back(
                                        {.segment_id = segment.id,
                                         .segment_name = segment.name,
                                         .transport_endpoint =
                                             segment.te_endpoint});
                                    LOG(ERROR)
                                        << "client_id=" << client_id
                                        << ", segment_name=" << segment.name
                                        << ", action=prepare_client_offboarding"
                                        << ", error="
                                        << toString(prepare_result);
                                }
                            }
                        }
                    }

                    MasterMetricManager::instance()
                        .client_liveness_became_offline();
                    client_offboarding_worker_.ScheduleReserved(std::move(job));
                });

            if (transition == ClientLivenessTransition::BECAME_SUSPECTED) {
                MasterMetricManager::instance()
                    .client_liveness_became_suspected();
                LOG(INFO) << "client_id=" << client_id
                          << ", action=client_liveness_suspected";
            } else if (transition == ClientLivenessTransition::BECAME_OFFLINE) {
                LOG(INFO) << "client_id=" << client_id
                          << ", action=client_liveness_offline";
            }
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
    std::shared_lock<std::shared_mutex> client_lock(client_mutex_);
    std::shared_lock<std::shared_mutex> snapshot_lock(snapshot_mutex_);
    auto retaining_clients = GetRetainingClientIdsLocked();
    client_lock.unlock();
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

    ClearInvalidHandles(retaining_clients);

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
MasterService::MetadataSerializer::Serialize(
    const WeightMetadataSnapshot* frozen_weight_metadata) {
    msgpack::sbuffer sbuf;
    msgpack::packer<msgpack::sbuffer> packer(&sbuf);

    // Weight metadata is optional on decode so snapshots produced before weight
    // management remain valid.
    packer.pack_map(4);

    // 1. Serialize the metadata, one entry per tenant that holds objects. The
    // "shards" key and numeric entry keys stay so a snapshot written by an
    // older master still loads; each object is routed by the tenant id in its
    // own payload, not by the entry key.
    packer.pack("shards");

    std::vector<std::pair<TenantId, std::shared_ptr<metadata::Tenant>>>
        tenants_with_metadata;
    service_->tenants_.Visit(
        [&](const TenantId& tenant_id,
            const std::shared_ptr<metadata::Tenant>& tenant) {
            if (tenant->ObjectCount() > 0) {
                tenants_with_metadata.emplace_back(tenant_id, tenant);
            }
        });
    // Deterministic order, so the same state serializes to the same bytes.
    std::sort(tenants_with_metadata.begin(), tenants_with_metadata.end(),
              [](const auto& lhs, const auto& rhs) {
                  return lhs.first.value() < rhs.first.value();
              });
    packer.pack_map(tenants_with_metadata.size());

    uint32_t tenant_index = 0;
    for (const auto& [tenant_id, tenant] : tenants_with_metadata) {
        packer.pack(tenant_index++);

        // Independent buffer per tenant, compressed on its own.
        msgpack::sbuffer tenant_buffer;
        msgpack::packer<msgpack::sbuffer> tenant_packer(&tenant_buffer);

        auto result = SerializeTenant(tenant_id, *tenant, tenant_packer);
        if (!result) {
            return tl::make_unexpected(SerializationError(
                result.error().code,
                fmt::format("Failed to serialize tenant entry #{}: {}",
                            tenant_index, result.error().message)));
        }

        std::vector<uint8_t> compressed_data = zstd_compress(
            reinterpret_cast<const uint8_t*>(tenant_buffer.data()),
            tenant_buffer.size(), 3);
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

    packer.pack("weight_metadata");
    WeightMetadataSnapshot live_weight_metadata;
    if (frozen_weight_metadata == nullptr) {
        live_weight_metadata = service_->weight_manager_.ExportSnapshot();
        frozen_weight_metadata = &live_weight_metadata;
    }
    const auto encoded_weight_metadata =
        struct_pack::serialize(*frozen_weight_metadata);
    packer.pack_bin(encoded_weight_metadata.size());
    packer.pack_bin_body(encoded_weight_metadata.data(),
                         encoded_weight_metadata.size());

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

    const msgpack::object* shards_obj = nullptr;
    const msgpack::object* discarded_replicas_obj = nullptr;
    const msgpack::object* replica_next_id_obj = nullptr;
    const msgpack::object* weight_metadata_obj = nullptr;

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
            } else if (key == "weight_metadata") {
                weight_metadata_obj = &obj.via.map.ptr[i].val;
            }
        }
    }

    // Check required "shards" field
    if (shards_obj == nullptr) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL, "Missing 'shards' field"));
    }

    WeightMetadataSnapshot weight_metadata;
    if (weight_metadata_obj != nullptr) {
        if (weight_metadata_obj->type != msgpack::type::BIN) {
            return tl::make_unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                "Invalid MessagePack format: weight_metadata must be binary"));
        }
        const std::string encoded(weight_metadata_obj->via.bin.ptr,
                                  weight_metadata_obj->via.bin.ptr +
                                      weight_metadata_obj->via.bin.size);
        if (struct_pack::deserialize_to(weight_metadata, encoded) !=
            struct_pack::errc::ok) {
            return tl::make_unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                "Failed to deserialize weight_metadata snapshot"));
        }
    }
    if (!ValidateWeightMetadataSnapshot(weight_metadata)) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL, "Invalid weight_metadata snapshot"));
    }

    // The payload is the whole metadata this service publishes, so decoding
    // replaces what the routes held: a tenant the payload does not carry would
    // otherwise keep publishing the keys of the state this call started from.
    {
        std::vector<TenantId> replaced_tenants;
        service_->tenants_.Visit(
            [&replaced_tenants](const TenantId& tenant_id,
                                const std::shared_ptr<metadata::Tenant>&) {
                replaced_tenants.push_back(tenant_id);
            });
        for (const auto& tenant_id : replaced_tenants) {
            service_->tenants_.Remove(tenant_id);
        }
    }
    service_->ResetReplicaActionStateForReload();

    // Iterate and deserialize each tenant entry
    for (uint32_t i = 0; i < shards_obj->via.map.size; ++i) {
        // Entry keys are ignored: each object is routed into its own tenant's
        // container by the tenant id in its own payload.
        // Get tenant binary data
        const msgpack::object& tenant_data_obj = shards_obj->via.map.ptr[i].val;
        if (tenant_data_obj.type != msgpack::type::BIN) {
            return tl::make_unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                "Invalid MessagePack format: expected binary data for shard"));
        }

        // Parse tenant binary data directly, avoiding copy
        msgpack::object_handle tenant_oh;
        try {
            auto decompressed_data = zstd_decompress(
                reinterpret_cast<const uint8_t*>(tenant_data_obj.via.bin.ptr),
                tenant_data_obj.via.bin.size);
            tenant_oh = msgpack::unpack(
                reinterpret_cast<const char*>(decompressed_data.data()),
                decompressed_data.size());
        } catch (const std::exception& e) {
            return tl::make_unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                "Failed to unpack shard data: " + std::string(e.what())));
        }

        const msgpack::object& tenant_obj = tenant_oh.get();

        // Each object lands in the tenant its own payload names, so no
        // re-routing pass is needed after restore.
        auto result = DeserializeTenant(tenant_obj);
        if (!result) {
            return tl::make_unexpected(SerializationError(
                result.error().code,
                fmt::format("Failed to deserialize tenant entry #{}: {}", i,
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
    // Old snapshots restore an empty weight domain only after decoding
    // succeeds.
    auto restored = service_->weight_manager_.RestoreSnapshot(weight_metadata);
    if (!restored) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL, "Invalid weight_metadata snapshot"));
    }

    // Deserialization already routed every object, so only the group membership
    // and the shared leases are rebuilt here.
    service_->RebuildGroupState();
    service_->ReconcilePromotionBookkeeping();
    return {};
}

void MasterService::MetadataSerializer::Reset() {
    service_->weight_manager_.Clear();
    service_->soft_pin_deadline_index_.Clear();
    {
        // The ids are collected before any removal: Visit walks one immutable
        // frame, so a removal from inside the callback would not apply to the
        // frame being walked.
        std::vector<TenantId> tenant_ids;
        service_->tenants_.Visit([&](const TenantId& tenant_id,
                                     const std::shared_ptr<metadata::Tenant>&) {
            tenant_ids.push_back(tenant_id);
        });
        for (const auto& tenant_id : tenant_ids) {
            service_->tenants_.Remove(tenant_id);
        }
    }
    // The tenants are gone, so their replica-action records name keys no
    // publication owns: leases a client could still act on, and candidate index
    // entries the retry loop would keep retrying.
    service_->ResetReplicaActionStateForReload();
    // Group membership and the shared leases live inside each tenant, so
    // dropping the tenants above cleared them with it.
    {
        std::lock_guard lock(service_->discarded_replicas_mutex_);
        service_->discarded_replicas_.clear();
    }
    Replica::next_id_.store(1);
}

tl::expected<void, SerializationError>
MasterService::MetadataSerializer::SerializeTenant(
    const TenantId& tenant_id, const metadata::Tenant& tenant,
    MsgpackPacker& packer) const {
    // Tenant payload: a map with one "metadata" field, whose items keep the
    // per-object layout [tenant_id, key, metadata_object].
    packer.pack_map(1);

    packer.pack("metadata");
    // Sorted by key, so the same tenant state serializes to the same bytes.
    // NOTE: the sort may be slow for a tenant that holds many objects.
    auto entries = tenant.SnapshotObjects();
    std::sort(entries.begin(), entries.end(),
              [](const auto& lhs, const auto& rhs) {
                  return lhs->key() < rhs->key();
              });
    packer.pack_array(entries.size());

    for (const auto& entry : entries) {
        auto result = entry->WithSharedAccess(
            [&](const ObjectMetadata& metadata, const ObjectEntry::State&)
                -> tl::expected<void, SerializationError> {
                // Each metadata item format: [tenant_id, key, metadata_object].
                packer.pack_array(3);
                packer.pack(tenant_id.value());
                packer.pack(entry->key());

                auto serialized = SerializeMetadata(metadata, packer);
                if (!serialized) {
                    return tl::make_unexpected(SerializationError(
                        serialized.error().code,
                        fmt::format("Failed to serialize metadata for key "
                                    "'{}': {}",
                                    entry->key(), serialized.error().message)));
                }
                return {};
            });
        if (!result) {
            return result;
        }
    }

    return {};
}

tl::expected<void, SerializationError>
MasterService::MetadataSerializer::DeserializeTenant(
    const msgpack::object& obj) {
    if (obj.type != msgpack::type::MAP) {
        return tl::make_unexpected(
            SerializationError(ErrorCode::DESERIALIZE_FAIL,
                               "Invalid tenant format: expected map"));
    }

    const msgpack::object* metadata_array = nullptr;

    for (uint32_t i = 0; i < obj.via.map.size; ++i) {
        const auto& key_obj = obj.via.map.ptr[i].key;
        if (key_obj.type == msgpack::type::STR) {
            std::string field_key(key_obj.via.str.ptr, key_obj.via.str.size);
            if (field_key == "metadata") {
                metadata_array = &obj.via.map.ptr[i].val;
            }
        }
    }

    if (metadata_array == nullptr ||
        metadata_array->type != msgpack::type::ARRAY) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            "Missing or invalid 'metadata' field in tenant payload"));
    }

    for (uint32_t j = 0; j < metadata_array->via.array.size; ++j) {
        const msgpack::object& item = metadata_array->via.array.ptr[j];

        if (item.type != msgpack::type::ARRAY ||
            (item.via.array.size != 2 && item.via.array.size != 3)) {
            return tl::make_unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                "Invalid metadata item format: expected [key, metadata] or "
                "[tenant_id, key, metadata]"));
        }

        TenantId tenant_id;
        std::string key;
        const msgpack::object* value_obj = nullptr;
        if (item.via.array.size == 2) {
            key = item.via.array.ptr[0].as<std::string>();
            value_obj = &item.via.array.ptr[1];
        } else {
            tenant_id = TenantId(item.via.array.ptr[0].as<std::string>());
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
        const auto tenant = service_->GetOrCreateTenantHandle(tenant_id);
        auto entry =
            std::make_shared<ObjectEntry>(std::make_unique<ObjectMetadata>(
                metadata_ptr->client_id, metadata_ptr->put_start_time,
                metadata_ptr->size, metadata_ptr->PopReplicas(), std::nullopt,
                metadata_ptr->IsHardPinned(), metadata_ptr->data_type,
                metadata_ptr->group_id, tenant_id, key));
        if (!tenant->InsertObject(entry)) {
            return tl::make_unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                "Duplicate key '" + key + "' in tenant payload"));
        }

        // InsertObject wires the identity and the group lease, so the fields it
        // does not carry are filled in under the entry's own lock.
        entry->WithExclusiveAccess([&](ObjectMetadata& restored_metadata,
                                       ObjectEntry::State&) {
            restored_metadata.lease_->ExtendTo(
                metadata_ptr->EvictionDeadline());
            restored_metadata.object_checksum = metadata_ptr->object_checksum;
        });
    }

    return {};
}

tl::expected<void, SerializationError>
MasterService::MetadataSerializer::SerializeMetadata(
    const ObjectMetadata& metadata, MsgpackPacker& packer) const {
    // Pack ObjectMetadata using array structure for efficiency
    // Format: [client_id, put_start_time, size, lease_timeout,
    // has_soft_pin_timeout, soft_pin_timeout, replicas_count, data_type,
    // replicas..., hard_pinned, group_id, object_checksum?]

    size_t array_size = 10;  // client_id, put_start_time, size, lease_timeout,
                             // has_soft_pin_timeout, soft_pin_timeout,
                             // replicas_count, data_type, hard_pinned, group_id
    array_size += metadata.CountReplicas();  // One element per replica
    if (metadata.object_checksum.has_value()) {
        ++array_size;
    }
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

    // Serialize the authoritative lease deadline (converted to timestamp).
    auto lease_timestamp =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            metadata.lease_->ExpiresAt().time_since_epoch())
            .count();
    packer.pack(lease_timestamp);

    // Keep the legacy snapshot slots for format compatibility, but soft pin is
    // runtime-only state and is intentionally not persisted.
    packer.pack(false);
    packer.pack(uint64_t(0));

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
    if (metadata.object_checksum.has_value()) {
        packer.pack(*metadata.object_checksum);
    }

    return {};
}

tl::expected<std::unique_ptr<ObjectMetadata>, SerializationError>
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
    if (!StringToUuid(client_id_str, client_id)) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            fmt::format("deserialize ObjectMetadata invalid client_id UUID: {}",
                        client_id_str)));
    }

    // Deserialize put_start_time
    uint64_t put_start_time_timestamp = array[index++].as<uint64_t>();

    // Deserialize size
    auto size = static_cast<size_t>(array[index++].as<uint64_t>());

    // Deserialize lease_timeout
    uint64_t lease_timestamp = array[index++].as<uint64_t>();

    // Parse and discard the legacy soft-pin fields. Recovered objects always
    // become ordinary cache.
    (void)array[index++].as<bool>();
    (void)array[index++].as<uint64_t>();

    const auto max_timestamp =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::time_point::max().time_since_epoch())
            .count();
    if (max_timestamp < 0 ||
        put_start_time_timestamp > static_cast<uint64_t>(max_timestamp) ||
        lease_timestamp > static_cast<uint64_t>(max_timestamp)) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            "ObjectMetadata timestamp exceeds system_clock range"));
    }

    // Deserialize replicas count
    uint32_t replicas_count = array[index++].as<uint32_t>();

    // Format detection (decode optional fields by type for back-compat):
    //   v1: 7 + replicas_count, no optional fields
    //   v2: 8 + replicas_count, either data_type or hard_pinned
    //   v3: 9 + replicas_count, data_type + hard_pinned or hard_pinned +
    //   group_id v4: 10 + replicas_count, data_type + hard_pinned + group_id
    //   v5: 11 + replicas_count, v4 + object_checksum
    // 64-bit arithmetic keeps an attacker-controlled near-UINT32_MAX
    // replicas_count from wrapping the bounds and slipping an out-of-bounds
    // index past the size check.
    constexpr uint64_t kBaseFieldCount = 7;
    constexpr uint64_t kMaxOptionalFieldCount = 4;
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

    std::optional<uint64_t> object_checksum;
    if (index < total_elements &&
        array[index].type == msgpack::type::POSITIVE_INTEGER) {
        object_checksum = array[index++].as<uint64_t>();
    }
    if (index != total_elements) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            "deserialize ObjectMetadata optional field type mismatch"));
    }

    // Create ObjectMetadata instance. Soft pin is not restored.
    auto metadata = std::make_unique<ObjectMetadata>(
        client_id,
        std::chrono::system_clock::time_point(
            std::chrono::milliseconds(put_start_time_timestamp)),
        size, std::move(replicas), std::nullopt, is_hard_pinned, data_type,
        group_id);
    metadata->object_checksum = object_checksum;
    metadata->lease_->ExtendTo(std::chrono::system_clock::time_point(
        std::chrono::milliseconds(lease_timestamp)));

    return metadata;
}

tl::expected<void, ErrorCode>
MasterService::ValidateDynamicReplicaPendingForCopyStart(
    ObjectEntry::State& state, const UUID& dynamic_replication_lease_id,
    const UUID& client_id, const std::string& source_segment,
    uint64_t current_version_epoch, uint64_t dynamic_replication_version_epoch,
    const std::vector<std::string>& target_segments) {
    const bool dynamic_copy = dynamic_replication_lease_id != UUID{};
    // Dropping a pending task clears the cooldown with it, so the next proposal
    // is not held back by the abandoned one. The lease records of this key
    // expire on their own deadline and are left to that sweep.
    auto drop_pending = [&state] {
        state.dynamic_replication_pending.reset();
        state.dynamic_replication_cooldown = {};
    };
    if (!state.dynamic_replication_pending.has_value()) {
        if (dynamic_copy) {
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }
        return {};
    }
    // Taken by value: the branches below reset the pending state this reference
    // would otherwise point into.
    const auto pending = *state.dynamic_replication_pending;
    if (pending.expire_at_ms_epoch < DynamicReplicationNowMs()) {
        drop_pending();
        if (dynamic_copy) {
            return tl::make_unexpected(
                ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
        }
        return {};
    }
    if (!dynamic_copy) {
        return tl::make_unexpected(ErrorCode::OBJECT_HAS_REPLICATION_TASK);
    }
    if (pending.version_epoch != current_version_epoch) {
        drop_pending();
        return tl::make_unexpected(ErrorCode::INVALID_VERSION);
    }
    if (pending.lease_id != dynamic_replication_lease_id ||
        pending.version_epoch != dynamic_replication_version_epoch) {
        return tl::make_unexpected(ErrorCode::INVALID_VERSION);
    }
    if (pending.source_segment != source_segment ||
        target_segments.size() != 1 ||
        target_segments.front() != pending.target_segment) {
        return tl::make_unexpected(ErrorCode::OBJECT_HAS_REPLICATION_TASK);
    }
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();
    UUID source_client;
    auto err = segment_access.GetClientIdBySegmentName(pending.source_segment,
                                                       source_client);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    if (source_client != client_id) {
        return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
    }
    return {};
}

void MasterService::RegisterDynamicReplicaStart(
    ObjectMetadata& metadata, ObjectEntry::State& state,
    const std::string& source_segment, uint64_t version_epoch,
    const std::vector<std::string>& target_segments,
    const std::vector<ReplicaID>& replica_ids) {
    if (!state.dynamic_replication_pending.has_value()) {
        return;
    }
    const auto pending = *state.dynamic_replication_pending;
    state.dynamic_replication_pending.reset();
    if (pending.source_segment != source_segment ||
        pending.expire_at_ms_epoch < DynamicReplicationNowMs() ||
        pending.version_epoch != version_epoch) {
        return;
    }
    for (size_t i = 0; i < target_segments.size() && i < replica_ids.size();
         ++i) {
        if (target_segments[i] != pending.target_segment) {
            continue;
        }
        metadata.MarkDynamicReplica(
            replica_ids[i], ObjectMetadata::DynamicReplicaRecord{
                                .created_at = std::chrono::system_clock::now(),
                                .source_segment = pending.source_segment,
                                .target_segment = pending.target_segment,
                                .target_domain = pending.target_domain,
                                .complete = false});
        return;
    }
}

tl::expected<UUID, ErrorCode> MasterService::CreateCopyTask(
    const std::string& key, const TenantId& tenant_id,
    const std::vector<std::string>& targets) {
    auto normalized_tenant_result = ResolveTenantIdForWrite(tenant_id);
    if (!normalized_tenant_result) {
        return tl::make_unexpected(normalized_tenant_result.error());
    }
    const ObjectIdentity object_id{std::move(normalized_tenant_result.value()),
                                   key};
    if (targets.empty()) {
        LOG(ERROR) << "key=" << key << ", error=empty_targets";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::vector<
        std::tuple<std::string, UUID, std::shared_ptr<ClientLivenessRecord>>>
        serving_sources;
    {
        std::shared_lock<std::shared_mutex> snapshot_lock(snapshot_mutex_);
        const auto discovery = WithObjectMetadataForRead(
            object_id,
            [&](const metadata::Tenant&, const std::shared_ptr<ObjectEntry>&,
                const ObjectMetadata& metadata,
                const ObjectEntry::State&) -> tl::expected<void, ErrorCode> {
                // A key whose envelope holds no usable replica is reported as
                // absent here, the same way the read APIs report it.
                if (!metadata.IsValid()) {
                    return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
                }
                ScopedSegmentAccess segment_accessor =
                    segment_manager_.getSegmentAccess();
                for (const auto& target : targets) {
                    if (!segment_accessor.ExistsSegmentName(target)) {
                        LOG(ERROR)
                            << "key=" << key << ", target_segment=" << target
                            << ", error=target_segment_not_mounted";
                        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                    }
                    if (!segment_accessor.IsSegmentAllocatable(target)) {
                        LOG(ERROR)
                            << "key=" << key << ", target_segment=" << target
                            << ", error=target_segment_not_allocatable";
                        return tl::make_unexpected(
                            ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
                    }
                }

                metadata.VisitReplicas(
                    [this](const Replica& replica) {
                        return IsReplicaReadable(replica);
                    },
                    [&](const Replica& replica) {
                        for (const auto& segment_name :
                             replica.get_segment_names()) {
                            if (!segment_name) {
                                continue;
                            }
                            UUID owner;
                            if (segment_accessor.GetClientIdBySegmentName(
                                    *segment_name, owner) == ErrorCode::OK) {
                                const auto liveness =
                                    replica.getClientLiveness();
                                if (liveness) {
                                    serving_sources.emplace_back(
                                        *segment_name, owner,
                                        std::move(liveness));
                                }
                            }
                        }
                    });
                return {};
            });
        if (!discovery.has_value()) {
            VLOG(1) << "key=" << key << ", info=object_not_found";
            return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
        }
        if (!*discovery) {
            return tl::make_unexpected(discovery->error());
        }
    }
    if (serving_sources.empty()) {
        LOG(ERROR) << "key=" << key << ", error=no_valid_source_replicas";
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }

    const auto& [selected_source_segment, select_client, liveness] =
        serving_sources[randomIndex(serving_sources.size())];
    auto serving_guard = liveness->TryAcquireServingGuard();
    if (!serving_guard) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }

    // Completion paths acquire the Client guard before the entry lock.
    // Reacquire and validate in that same order after discovery.
    std::shared_lock<std::shared_mutex> snapshot_lock(snapshot_mutex_);
    const auto submission = WithObjectMetadataForRead(
        object_id,
        [&](const metadata::Tenant&, const std::shared_ptr<ObjectEntry>&,
            const ObjectMetadata& metadata,
            const ObjectEntry::State&) -> tl::expected<void, ErrorCode> {
            ScopedSegmentAccess submission_access =
                segment_manager_.getSegmentAccess();
            UUID current_source_client;
            if (!metadata.IsValid() ||
                submission_access.GetClientIdBySegmentName(
                    selected_source_segment, current_source_client) !=
                    ErrorCode::OK ||
                current_source_client != select_client ||
                !metadata.HasReplica(
                    [this, &selected_source_segment](const Replica& replica) {
                        if (!IsReplicaReadable(replica)) {
                            return false;
                        }
                        const auto names = replica.get_segment_names();
                        return std::any_of(
                            names.begin(), names.end(),
                            [&selected_source_segment](const auto& name) {
                                return name && *name == selected_source_segment;
                            });
                    })) {
                return tl::make_unexpected(
                    ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
            }
            for (const auto& target : targets) {
                if (!submission_access.IsSegmentAllocatable(target)) {
                    return tl::make_unexpected(
                        ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
                }
            }
            return {};
        });
    if (!submission.has_value() || !*submission) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    return task_manager_.get_write_access()
        .submit_task_typed<TaskType::REPLICA_COPY>(
            select_client, {.tenant_id = object_id.tenant_id.value(),
                            .key = object_id.user_key,
                            .source = selected_source_segment,
                            .targets = targets});
}

tl::expected<UUID, ErrorCode> MasterService::CreateMoveTask(
    const std::string& key, const TenantId& tenant_id,
    const std::string& source, const std::string& target) {
    auto normalized_tenant_result = ResolveTenantIdForWrite(tenant_id);
    if (!normalized_tenant_result) {
        return tl::make_unexpected(normalized_tenant_result.error());
    }
    const ObjectIdentity object_id{std::move(normalized_tenant_result.value()),
                                   key};
    if (source == target) {
        LOG(ERROR) << "key=" << key << ", source_segment=" << source
                   << ", target_segment=" << target
                   << ", error=source_target_segments_are_same";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    bool source_exists = false;
    bool source_is_serving = false;
    UUID select_client;
    std::shared_ptr<ClientLivenessRecord> source_liveness;
    {
        std::shared_lock<std::shared_mutex> snapshot_lock(snapshot_mutex_);
        const auto discovery = WithObjectMetadataForRead(
            object_id,
            [&](const metadata::Tenant&, const std::shared_ptr<ObjectEntry>&,
                const ObjectMetadata& metadata,
                const ObjectEntry::State&) -> tl::expected<void, ErrorCode> {
                // A key whose envelope holds no usable replica is reported as
                // absent here, the same way the read APIs report it.
                if (!metadata.IsValid()) {
                    return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
                }
                ScopedSegmentAccess segment_accessor =
                    segment_manager_.getSegmentAccess();
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

                metadata.VisitReplicas(
                    [](const Replica&) { return true; },
                    [this, &source, &source_exists, &source_is_serving,
                     &source_liveness](const Replica& replica) {
                        const auto names = replica.get_segment_names();
                        const bool matches =
                            std::any_of(names.begin(), names.end(),
                                        [&source](const auto& name) {
                                            return name && *name == source;
                                        });
                        if (matches) {
                            source_exists = true;
                            if (IsReplicaReadable(replica)) {
                                source_is_serving = true;
                                source_liveness = replica.getClientLiveness();
                            }
                        }
                    });
                if (!source_exists) {
                    LOG(ERROR) << "key=" << key << ", source_segment=" << source
                               << ", error=source_replica_not_found";
                    return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                }
                if (!source_is_serving) {
                    LOG(ERROR) << "key=" << key << ", source_segment=" << source
                               << ", error=source_segment_not_serving";
                    return tl::make_unexpected(
                        ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
                }
                ErrorCode error = segment_accessor.GetClientIdBySegmentName(
                    source, select_client);
                if (error != ErrorCode::OK) {
                    LOG(ERROR) << "key=" << key << ", segment_name=" << source
                               << ", error=client_id_not_found";
                    return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
                }
                return {};
            });
        if (!discovery.has_value()) {
            VLOG(1) << "key=" << key << ", info=object_not_found";
            return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
        }
        if (!*discovery) {
            return tl::make_unexpected(discovery->error());
        }
    }

    if (!source_liveness) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    auto serving_guard = source_liveness->TryAcquireServingGuard();
    if (!serving_guard) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }

    // Do not hold an entry lock while acquiring the Client guard: MoveEnd takes
    // those locks in the opposite order.
    std::shared_lock<std::shared_mutex> snapshot_lock(snapshot_mutex_);
    const auto submission = WithObjectMetadataForRead(
        object_id,
        [&](const metadata::Tenant&, const std::shared_ptr<ObjectEntry>&,
            const ObjectMetadata& metadata,
            const ObjectEntry::State&) -> tl::expected<void, ErrorCode> {
            ScopedSegmentAccess submission_access =
                segment_manager_.getSegmentAccess();
            UUID current_source_client;
            if (!metadata.IsValid() ||
                submission_access.GetClientIdBySegmentName(
                    source, current_source_client) != ErrorCode::OK ||
                current_source_client != select_client ||
                !submission_access.IsSegmentAllocatable(target) ||
                !metadata.HasReplica([this, &source](const Replica& replica) {
                    if (!IsReplicaReadable(replica)) {
                        return false;
                    }
                    const auto names = replica.get_segment_names();
                    return std::any_of(names.begin(), names.end(),
                                       [&source](const auto& name) {
                                           return name && *name == source;
                                       });
                })) {
                return tl::make_unexpected(
                    ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
            }
            return {};
        });
    if (!submission.has_value() || !*submission) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }

    return task_manager_.get_write_access()
        .submit_task_typed<TaskType::REPLICA_MOVE>(
            select_client, {.tenant_id = object_id.tenant_id.value(),
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
    const auto liveness = FindClientRecord(client_id);
    if (!liveness) {
        return std::vector<TaskAssignment>{};
    }
    auto serving_guard = liveness->TryAcquireServingGuard();
    if (!serving_guard) {
        return std::vector<TaskAssignment>{};
    }
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
    const auto liveness = FindClientRecord(client_id);
    if (!liveness) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
    auto retaining_guard = liveness->TryAcquireRetainingGuard();
    if (!retaining_guard) {
        return tl::make_unexpected(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    }
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
    const TenantId& tenant_id, const std::string& key,
    const std::string& source_segment) const {
    return std::to_string(tenant_id.value().size()) + ":" + tenant_id.value() +
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
        TenantId tenant_id;
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
        tenants_.Visit([&](const TenantId& tenant_id,
                           const std::shared_ptr<metadata::Tenant>& tenant) {
            for (const auto& snapshot_entry : tenant->SnapshotObjects()) {
                // The snapshot hands out handles, so each key is resolved again
                // here: a key republished meanwhile is judged on what it holds
                // now, not on what the snapshot captured.
                (void)WithObjectMetadataForRead(
                    MakeObjectIdentity(snapshot_entry->key(), tenant_id),
                    [&](const metadata::Tenant&,
                        const std::shared_ptr<ObjectEntry>& entry,
                        const ObjectMetadata& metadata,
                        const ObjectEntry::State& state) -> void {
                        const std::string& key = entry->key();
                        for (const auto& source_segment :
                             job.request.segments) {
                            const auto unit_key = MakeDrainUnitKey(
                                tenant_id, key, source_segment);
                            if (job.completed_unit_keys.contains(unit_key) ||
                                active_unit_keys.contains(unit_key) ||
                                job.terminal_failed_unit_keys.contains(
                                    unit_key)) {
                                continue;
                            }

                            const auto replica_segments =
                                metadata.GetReplicaSegmentNames();
                            if (std::find(replica_segments.begin(),
                                          replica_segments.end(),
                                          source_segment) ==
                                replica_segments.end()) {
                                continue;
                            }

                            bool source_is_serving = false;
                            metadata.VisitReplicas(
                                [this,
                                 &source_segment](const Replica& replica) {
                                    if (!IsReplicaReadable(replica)) {
                                        return false;
                                    }
                                    const auto names =
                                        replica.get_segment_names();
                                    return std::any_of(
                                        names.begin(), names.end(),
                                        [&source_segment](const auto& name) {
                                            return name &&
                                                   *name == source_segment;
                                        });
                                },
                                [&source_is_serving](const Replica&) {
                                    source_is_serving = true;
                                });
                            if (!source_is_serving) {
                                blocked_unit_keys.insert(unit_key);
                                continue;
                            }

                            if (metadata.IsHardPinned() ||
                                !metadata.IsLeaseExpired() ||
                                !metadata.AllReplicas(
                                    &Replica::fn_is_completed) ||
                                state.replication_task.has_value()) {
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
                                                 *target, metadata.size,
                                                 unit_key});
                            }
                        }
                    });
            }
        });
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
        tenants_.Visit([&](const TenantId& tenant_id,
                           const std::shared_ptr<metadata::Tenant>& tenant) {
            for (const auto& snapshot_entry : tenant->SnapshotObjects()) {
                // The snapshot hands out handles, so each key is resolved again
                // here: counting a replaced object's replicas would report a
                // segment as drained while the object holding the key now still
                // lives on it.
                (void)WithObjectMetadataForRead(
                    MakeObjectIdentity(snapshot_entry->key(), tenant_id),
                    [&](const metadata::Tenant&,
                        const std::shared_ptr<ObjectEntry>& entry,
                        const ObjectMetadata& metadata,
                        const ObjectEntry::State&) -> void {
                        const std::string& key = entry->key();
                        const auto replica_segments =
                            metadata.GetReplicaSegmentNames();
                        for (const auto& source_segment :
                             job.request.segments) {
                            if (std::find(replica_segments.begin(),
                                          replica_segments.end(),
                                          source_segment) !=
                                replica_segments.end()) {
                                remaining_segments.insert(source_segment);
                                remaining_unit_keys.insert(MakeDrainUnitKey(
                                    tenant_id, key, source_segment));
                            }
                        }
                    });
            }
        });
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

KvEventConfig MasterService::BuildKvEventConfig(
    const MasterServiceConfig& config) {
    KvEventConfig kv_config;
    kv_config.enabled = config.enable_kv_events;
    kv_config.bind_endpoint = config.kv_events_bind_endpoint;
    kv_config.model_name = config.kv_events_model_name;
    kv_config.backend_id = config.kv_events_backend_id;
    kv_config.tenant_id = config.kv_events_tenant_id;
    kv_config.additional_salt = config.kv_events_additional_salt;
    kv_config.lora_name = config.kv_events_lora_name;
    kv_config.block_size = config.kv_events_block_size;
    kv_config.dp_rank = config.kv_events_dp_rank;
    kv_config.emit_legacy_compat_fields = config.kv_events_emit_legacy_compat;
    kv_config.emit_object_key = config.kv_events_emit_object_key;
    kv_config.queue_capacity = config.kv_events_queue_capacity;
    return kv_config;
}

// Every non-memory replica type, including DFS, is normalized to the "disk"
// medium so subscribers see one logical tier per storage class.
std::vector<std::string> MasterService::KvMediaForMetadata(
    const ObjectMetadata& metadata) {
    bool has_cpu = false;
    bool has_disk = false;
    metadata.VisitReplicas(
        [](const Replica& replica) { return replica.is_completed(); },
        [&](const Replica& replica) {
            if (replica.is_memory_replica() &&
                !replica.has_invalid_mem_handle()) {
                has_cpu = true;
            } else if (replica.is_nof_replica() &&
                       !replica.has_invalid_nof_handle()) {
                has_disk = true;
            } else if (replica.is_disk_replica() ||
                       replica.is_local_disk_replica() ||
                       replica.is_dfs_replica()) {
                has_disk = true;
            }
        });
    std::vector<std::string> media;
    if (has_cpu) {
        media.emplace_back("cpu");
    }
    if (has_disk) {
        media.emplace_back("disk");
    }
    return media;
}

std::vector<std::string> MasterService::KvMediaForRemoval(
    const ObjectMetadata& metadata) {
    auto media = KvMediaForMetadata(metadata);
    bool has_cpu = std::find(media.begin(), media.end(), "cpu") != media.end();
    bool has_disk =
        std::find(media.begin(), media.end(), "disk") != media.end();
    // KvMediaForMetadata above only counts completed replicas. A removal path
    // can run after a replica has moved to PROCESSING or REMOVED, and both
    // states still had their medium announced to subscribers, so both must
    // appear in the removal delta or the tier is never retracted.
    metadata.VisitReplicas(
        [](const Replica& replica) {
            return replica.status() == ReplicaStatus::REMOVED ||
                   replica.status() == ReplicaStatus::PROCESSING;
        },
        [&](const Replica& replica) {
            if (replica.is_memory_replica()) {
                has_cpu = true;
            } else if (replica.is_nof_replica() || replica.is_disk_replica() ||
                       replica.is_local_disk_replica() ||
                       replica.is_dfs_replica()) {
                has_disk = true;
            }
        });
    if (has_cpu &&
        std::find(media.begin(), media.end(), "cpu") == media.end()) {
        media.emplace_back("cpu");
    }
    if (has_disk &&
        std::find(media.begin(), media.end(), "disk") == media.end()) {
        media.emplace_back("disk");
    }
    return media;
}

void MasterService::PublishKvStored(const std::string& key,
                                    const ObjectMetadata& metadata,
                                    const TenantId& tenant_id) {
    // The epoch is bumped even when no publisher is listening so that the
    // ordering rule is testable, and always before the enqueue below: a
    // concurrent RemoveAll must either see the new epoch and withhold its
    // `cleared`, or have already published it ahead of this `stored`.
    BumpKvTenantEpoch(tenant_id.value());
    if (!kv_event_publisher_ || !kv_event_publisher_->enabled()) {
        return;
    }
    kv_event_publisher_->PublishCommitted(key, KvMediaForMetadata(metadata),
                                          tenant_id.value(), metadata.group_id);
}

void MasterService::SyncKvObjectState(
    const std::string& key, const ObjectMetadata& metadata,
    const TenantId& tenant_id,
    const std::vector<std::string>& previous_media_hint) {
    const bool publishing =
        kv_event_publisher_ && kv_event_publisher_->enabled();
    if (!publishing && !kv_track_tenant_epochs_) {
        return;
    }
    auto current_media = KvMediaForMetadata(metadata);
    // Only an object that still has a medium can contradict a `cleared`. Sync
    // calls that end with no medium left are retractions, which a clear may
    // safely follow.
    if (!current_media.empty()) {
        BumpKvTenantEpoch(tenant_id.value());
    }
    if (!publishing) {
        return;
    }
    kv_event_publisher_->SyncObjectState(key, current_media, tenant_id.value(),
                                         metadata.group_id,
                                         previous_media_hint);
}

void MasterService::PublishKvRemoved(
    const std::string& key, const ObjectMetadata& metadata,
    const TenantId& tenant_id,
    const std::vector<std::string>& previous_media_hint) {
    if (!kv_event_publisher_ || !kv_event_publisher_->enabled()) {
        return;
    }
    auto previous_media = KvRemovalSnapshot(metadata);
    previous_media.insert(previous_media.end(), previous_media_hint.begin(),
                          previous_media_hint.end());
    kv_event_publisher_->PublishObjectRemoved(
        key, tenant_id.value(), metadata.group_id, previous_media);
}

void MasterService::PublishKvRemovedAfterEvict(const std::string& key,
                                               size_t evicted_replica_count,
                                               const std::string& medium,
                                               const ObjectMetadata& metadata,
                                               const TenantId& tenant_id) {
    if (evicted_replica_count == 0) {
        return;
    }
    if (!kv_event_publisher_ || !kv_event_publisher_->enabled()) {
        return;
    }
    // The object lost its last replica, so the caller is about to erase it and
    // EraseMetadata publishes the full removal itself. Emitting here as well
    // would repeat every medium. This mirrors the oplog eviction path, which
    // already chooses between EraseMetadata and SyncKvObjectState.
    if (!metadata.IsValid()) {
        return;
    }
    // The object survives. Feeding the evicted medium in as part of the
    // previous set lets the delta drop it, unless another replica still lives
    // on it.
    auto previous_media = KvMediaForMetadata(metadata);
    if (std::find(previous_media.begin(), previous_media.end(), medium) ==
        previous_media.end()) {
        previous_media.push_back(medium);
    }
    SyncKvObjectState(key, metadata, tenant_id, previous_media);
}

void MasterService::PublishKvCleared(const TenantId& tenant_id) {
    if (!kv_event_publisher_ || !kv_event_publisher_->enabled()) {
        return;
    }
    kv_event_publisher_->PublishCleared(tenant_id.value());
}

void MasterService::PublishKvClearedIfEpochUnchanged(const TenantId& tenant_id,
                                                     uint64_t expected_epoch) {
    if (!kv_track_tenant_epochs_) {
        PublishKvCleared(tenant_id);
        return;
    }
    // Holding the epoch lock across the publish is what makes this a barrier
    // rather than a racy re-check: a commit that wants to announce this tenant
    // has to wait for the lock, and will then find the clear already enqueued.
    std::lock_guard<std::mutex> lock(kv_tenant_epoch_mutex_);
    if (kv_tenant_epochs_[KvTenantEpochSlot(tenant_id.value())] !=
        expected_epoch) {
        // A commit landed in an already-scanned shard. Its `stored` is on the
        // wire, so a clear here would retract a live object.
        kv_cleared_suppressed_by_epoch_.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    kv_cleared_published_.fetch_add(1, std::memory_order_relaxed);
    PublishKvCleared(tenant_id);
}

bool MasterService::KvEventsEnabled() const {
    return kv_event_publisher_ && kv_event_publisher_->enabled();
}

KvEventPublisher::Stats MasterService::GetKvEventStats() const {
    if (!kv_event_publisher_) {
        return {};
    }
    return kv_event_publisher_->GetStats();
}

void MasterService::setHttpMetadataServer(HttpMetadataServer* server) {
    http_metadata_server_ = server;
    if (server) {
        LOG(INFO) << "HTTP metadata cleanup on client timeout: enabled "
                     "(co-located metadata server)";
    }
}

void MasterService::setHttpMetadataRemoteUrl(
    const std::string& metadata_connstring) {
    // The HTTP client is built without TLS, so only plain http:// metadata
    // servers support remote cleanup.
    if (metadata_connstring.rfind("http://", 0) != 0) {
        LOG(WARNING) << "enable_metadata_cleanup_on_timeout is set but the "
                        "configured metadata server '"
                     << metadata_connstring
                     << "' is not an http:// endpoint; remote cleanup "
                        "currently supports only plain HTTP. Metadata cleanup "
                        "on timeout disabled.";
        return;
    }
    http_metadata_remote_url_ = metadata_connstring;
    LOG(INFO) << "HTTP metadata cleanup on client timeout: enabled "
                 "(remote metadata server "
              << metadata_connstring << ")";
    // Start async cleanup worker now that http_metadata_remote_url_ is set.
    http_metadata_cleanup_running_ = true;
    http_metadata_cleanup_thread_ =
        std::thread(&MasterService::HttpMetadataCleanupThreadFunc, this);
    LOG(INFO) << "HTTP metadata cleanup worker thread started";
}

void MasterService::cleanupHttpMetadata(const std::string& segment_name) {
    // Co-located: remove in-process, safe to run inline (no network I/O).
    if (http_metadata_server_) {
        const std::string ram_key =
            http_metadata_prefix_ + "ram/" + segment_name;
        const std::string rpc_key =
            http_metadata_prefix_ + "rpc_meta/" + segment_name;
        bool ram_removed = http_metadata_server_->removeKey(ram_key);
        bool rpc_removed = http_metadata_server_->removeKey(rpc_key);
        LOG(INFO) << "Cleaned up HTTP metadata for segment: " << segment_name
                  << ", ram_key_removed=" << ram_removed
                  << ", rpc_key_removed=" << rpc_removed;
        return;
    }

    // Separately-deployed: enqueue for async cleanup so a slow/unreachable
    // server never blocks the client monitor thread.
    if (!http_metadata_remote_url_.empty()) {
        {
            std::lock_guard<std::mutex> lk(http_metadata_cleanup_mutex_);
            http_metadata_cleanup_queue_.push_back(segment_name);
        }
        http_metadata_cleanup_cv_.notify_one();
        return;
    }

    // Neither configured: cleanup is disabled, nothing to do.
}

bool MasterService::removeRemoteHttpMetadataKey(const std::string& key) const {
    // Matches the timeout the Transfer Engine HTTP metadata plugin used.
    constexpr std::chrono::milliseconds kTimeout{3000};
    auto result = httpDelete(
        http_metadata_remote_url_ + "?key=" + code_utils::url_encode(key),
        kTimeout);
    if (!result) {
        LOG(WARNING) << "Remote HTTP metadata cleanup failed for key: " << key
                     << ": " << result.error();
        return false;
    }
    return true;
}

void MasterService::HttpMetadataCleanupThreadFunc() {
    LOG(INFO) << "HTTP metadata cleanup worker started";
    while (http_metadata_cleanup_running_) {
        std::vector<std::string> batch;
        {
            std::unique_lock<std::mutex> lk(http_metadata_cleanup_mutex_);
            http_metadata_cleanup_cv_.wait(lk, [&] {
                return !http_metadata_cleanup_queue_.empty() ||
                       !http_metadata_cleanup_running_.load();
            });
            if (!http_metadata_cleanup_running_ &&
                http_metadata_cleanup_queue_.empty()) {
                break;
            }
            batch.swap(http_metadata_cleanup_queue_);
        }

        for (const auto& segment_name : batch) {
            const std::string ram_key =
                http_metadata_prefix_ + "ram/" + segment_name;
            const std::string rpc_key =
                http_metadata_prefix_ + "rpc_meta/" + segment_name;

            // Each key attempted independently so one failure does not
            // prevent cleanup of the other.
            const bool ram_removed = removeRemoteHttpMetadataKey(ram_key);
            const bool rpc_removed = removeRemoteHttpMetadataKey(rpc_key);
            LOG(INFO) << "Cleaned up remote HTTP metadata for segment: "
                      << segment_name << ", ram_key_removed=" << ram_removed
                      << ", rpc_key_removed=" << rpc_removed;
        }
    }
    LOG(INFO) << "HTTP metadata cleanup worker stopped";
}

std::string MasterService::SerializeMetadataForOpLog(
    const ObjectMetadata& metadata) const {
    MetadataPayload payload;
    payload.client_id = metadata.client_id;
    payload.size = metadata.size;
    payload.group_id = metadata.group_id;
    payload.data_type = metadata.data_type;
    payload.hard_pinned = metadata.IsHardPinned();

    // Extract replica descriptors - get them all at once
    const auto& replicas = metadata.GetAllReplicas();
    payload.replicas.reserve(replicas.size());
    for (const auto& replica : replicas) {
        payload.replicas.push_back(replica.get_descriptor());
    }

    // NOTE: Lease information is NOT serialized because:
    // 1. Standby does not perform eviction, so lease info is not used
    // 2. After promotion, new Primary should grant fresh leases, not restore
    // old ones

    // Serialize using struct_pack (msgpack binary format)
    auto result = struct_pack::serialize(payload);
    return std::string(result.begin(), result.end());
}

std::string MasterService::SerializeMetadataForOpLogWithoutMemReplicas(
    const ObjectMetadata& metadata) const {
    MetadataPayload payload;
    payload.client_id = metadata.client_id;
    payload.size = metadata.size;
    payload.group_id = metadata.group_id;
    payload.data_type = metadata.data_type;
    payload.hard_pinned = metadata.IsHardPinned();

    const auto& replicas = metadata.GetAllReplicas();
    payload.replicas.reserve(replicas.size());
    for (const auto& replica : replicas) {
        if (replica.type() == ReplicaType::MEMORY) {
            continue;
        }
        payload.replicas.push_back(replica.get_descriptor());
    }

    auto result = struct_pack::serialize(payload);
    return std::string(result.begin(), result.end());
}

std::string MasterService::SerializeMetadataForOpLogFromReplicaDescriptors(
    const ObjectMetadata& metadata,
    const std::vector<Replica::Descriptor>& replicas) const {
    MetadataPayload payload;
    payload.client_id = metadata.client_id;
    payload.size = metadata.size;
    payload.replicas = replicas;
    payload.group_id = metadata.group_id;
    payload.data_type = metadata.data_type;
    payload.hard_pinned = metadata.IsHardPinned();
    auto result = struct_pack::serialize(payload);
    return std::string(result.begin(), result.end());
}

ErrorCode MasterService::InitializeBatchOpLogWriter(
    std::shared_ptr<HaKvBackend> backend, bool require_fenced_writer) {
    if (!backend || !backend->SupportsTxn()) {
        return ErrorCode::INVALID_PARAMS;
    }
    if (require_fenced_writer && view_version_ == 0) {
        LOG(ERROR) << "Fenced batch OpLog writer requires a non-zero acquired "
                      "producer view";
        return ErrorCode::INVALID_PARAMS;
    }

    auto storage = std::make_unique<OpLogBatchStorage>(cluster_id_, *backend);
    DurablePrefix durable_prefix;
    ErrorCode err = storage->InitDurablePrefix(durable_prefix);
    if (err != ErrorCode::OK) {
        return err;
    }
    if (require_fenced_writer) {
        err = storage->ClaimProducerView(view_version_);
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to claim producer view for batch OpLog "
                       << "writer: " << toString(err);
            return err;
        }
    }

    OrderedOpLogWriterConfig writer_config;
    writer_config.max_entries_per_batch = oplog_batch_max_entries_;
    writer_config.initial_durable_prefix = durable_prefix;
    OpLogBatchStorage* storage_ptr = storage.get();
    const ViewVersionId producer_view_version = view_version_;
    OrderedOpLogWriter::WriteBatchFn write_batch =
        [storage_ptr, require_fenced_writer, producer_view_version](
            const OpLogBatchRecord& batch,
            const DurablePrefix& expected_prefix) {
            if (require_fenced_writer) {
                return storage_ptr->WriteBatchAndAdvancePrefix(
                    batch, expected_prefix, producer_view_version);
            }
            return storage_ptr->WriteBatchAndAdvancePrefix(batch,
                                                           expected_prefix);
        };
    auto writer =
        batch_oplog_writer_factory_(writer_config, std::move(write_batch));
    if (!writer) {
        return ErrorCode::INVALID_PARAMS;
    }
    if (!writer->IsAccepting()) {
        return writer->LastError();
    }
    writer->Start();

    if (ordered_oplog_writer_) {
        ordered_oplog_writer_->Stop();
    }
    batch_oplog_kv_backend_ = std::move(backend);
    batch_oplog_storage_ = std::move(storage);
    ordered_oplog_writer_ = std::move(writer);
    ordered_oplog_writer_->ActivateRuntimeMetrics();
    return ErrorCode::OK;
}

tl::expected<uint64_t, ErrorCode>
MasterService::AppendOpLogVisibleBeforeDurable(OpType type,
                                               const std::string& tenant_id,
                                               const std::string& key,
                                               const std::string& payload) {
    if (!enable_oplog_) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!ordered_oplog_writer_) {
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }

    const TenantId resolved_tenant(enable_multi_tenants_
                                       ? tenant_id
                                       : std::string(TenantId::kDefaultValue));
    if (!resolved_tenant.IsValid()) {
        return tl::unexpected(ErrorCode::TENANT_NOT_REGISTERED);
    }

    auto reservation = ordered_oplog_writer_->Reserve();
    if (!reservation) {
        return tl::unexpected(reservation.error());
    }
    OpLogEntry entry;
    entry.op_type = type;
    entry.tenant_id = resolved_tenant.value();
    entry.object_key = key;
    entry.payload = payload;
    auto pending = ordered_oplog_writer_->Commit(std::move(reservation.value()),
                                                 std::move(entry), nullptr);
    if (!pending) {
        return tl::unexpected(pending.error());
    }
    return pending.value().sequence_id();
}

tl::expected<OpLogEntry, ErrorCode>
MasterService::AppendOpLogWithDurableFinalize(
    OpType type, const std::string& tenant_id, const std::string& key,
    const std::string& payload, DurableFinalizeCallback callback) {
    if (!enable_oplog_) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto reservation = ReserveBatchOpLogSlot();
    if (!reservation) {
        return tl::unexpected(reservation.error());
    }
    return AppendReservedOpLogWithDurableFinalize(
        std::move(reservation.value()), type, tenant_id, key, payload,
        std::move(callback));
}

tl::expected<OrderedOpLogWriter::Reservation, ErrorCode>
MasterService::ReserveBatchOpLogSlot() {
    if (!enable_oplog_) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!ordered_oplog_writer_) {
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return ordered_oplog_writer_->Reserve();
}

tl::expected<OpLogEntry, ErrorCode>
MasterService::AppendReservedOpLogWithDurableFinalize(
    OrderedOpLogWriter::Reservation&& reservation, OpType type,
    const std::string& tenant_id, const std::string& key,
    const std::string& payload, DurableFinalizeCallback callback) {
    if (!enable_oplog_) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    const TenantId resolved_tenant(enable_multi_tenants_
                                       ? tenant_id
                                       : std::string(TenantId::kDefaultValue));
    if (!resolved_tenant.IsValid()) {
        return tl::unexpected(ErrorCode::TENANT_NOT_REGISTERED);
    }
    OpLogEntry entry;
    entry.op_type = type;
    entry.tenant_id = resolved_tenant.value();
    entry.object_key = key;
    entry.payload = payload;
    auto pending = ordered_oplog_writer_->Commit(std::move(reservation), entry,
                                                 std::move(callback));
    if (!pending) {
        return tl::unexpected(pending.error());
    }
    entry.sequence_id = pending.value().sequence_id();
    return entry;
}

tl::expected<void, ErrorCode> MasterService::PersistRemoveForHA(
    const char* why, const std::string& key) {
    return PersistRemoveForHA(why, TenantId::Default(), key);
}

tl::expected<void, ErrorCode> MasterService::PersistRemoveForHA(
    const char* why, const TenantId& tenant_id, const std::string& key) {
    auto result = AppendOpLogWithDurableFinalize(
        OpType::REMOVE, tenant_id.value(), key, {}, nullptr);
    if (!result) {
        LOG(WARNING) << why << ": REMOVE persist failed for key=" << key
                     << ", err=" << static_cast<int>(result.error());
        return tl::unexpected(result.error());
    }
    return {};
}

void MasterService::PersistSegmentOpForHAOrEnqueue(const char* why, OpType type,
                                                   const std::string& key,
                                                   const std::string& payload) {
    PersistSegmentOpForHAOrEnqueue(why, type, TenantId::Default(), key,
                                   payload);
}

void MasterService::PersistSegmentOpForHAOrEnqueue(const char* why, OpType type,
                                                   const TenantId& tenant_id,
                                                   const std::string& key,
                                                   const std::string& payload) {
    auto result =
        AppendOpLogVisibleBeforeDurable(type, tenant_id.value(), key, payload);
    if (!result) {
        LOG(WARNING) << why << ": segment OpLog queue failed for key=" << key
                     << ", type=" << static_cast<int>(type)
                     << ", err=" << static_cast<int>(result.error());
    }
}

std::vector<Replica::Descriptor>
MasterService::BuildRemainingReplicaDescriptors(
    const ObjectMetadata& metadata,
    const std::function<bool(const Replica&)>& should_remove) const {
    std::vector<Replica::Descriptor> remaining;
    for (const auto& replica : metadata.GetAllReplicas()) {
        if (!should_remove(replica) &&
            replica.status() == ReplicaStatus::COMPLETE) {
            remaining.push_back(replica.get_descriptor());
        }
    }
    return remaining;
}

}  // namespace mooncake

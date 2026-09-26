#pragma once

#include <utility>

#include "master_service.h"

namespace mooncake::test {

// The single test access boundary for MasterService. Keep test-only inspection,
// mutation and synchronous drivers here, not in the production service API.
// Raw state accessors do not acquire locks. Callers must preserve the service's
// lock order; synchronous drivers must not race their background workers.
class MasterServiceTestPeer {
   public:
    explicit MasterServiceTestPeer(MasterService& service)
        : service_(service) {}

    using GroupDomainAccessorRO = MasterService::GroupDomainAccessorRO;
    using GroupDomainAccessorRW = MasterService::GroupDomainAccessorRW;
    using MetadataAccessorRO = MasterService::MetadataAccessorRO;
    using MetadataAccessorRW = MasterService::MetadataAccessorRW;
    using MetadataSerializer = MasterService::MetadataSerializer;
    using MetadataShard = MasterService::MetadataShard;
    using MetadataShardAccessorRO = MasterService::MetadataShardAccessorRO;
    using MetadataShardAccessorRW = MasterService::MetadataShardAccessorRW;
    using ObjectIdentity = MasterService::ObjectIdentity;
    using ObjectMetadata = mooncake::ObjectMetadata;
    using PromotionQueueResult = mooncake::PromotionQueueResult;
    using PromotionTask = mooncake::PromotionTask;
    using QuotaEraseMode = MasterService::QuotaEraseMode;
    using TenantQuotaEvictionResult = MasterService::TenantQuotaEvictionResult;
    using TenantState = MasterService::TenantState;

    static constexpr auto kDynamicReplicationWindowEntryLimit =
        MasterService::kDynamicReplicationWindowEntryLimit;
    static constexpr auto kMaxPromotionExecutionFailures =
        MasterService::kMaxPromotionExecutionFailures;
    static constexpr auto kNumShards = MasterService::kNumShards;
    static constexpr auto kObjectOperationLockStripes =
        MasterService::kObjectOperationLockStripes;
    static constexpr auto kPromotionCandidateMaxRetries =
        MasterService::kPromotionCandidateMaxRetries;

    ErrorCode SetBatchOpLogBackendForTesting(
        std::shared_ptr<HaKvBackend> backend);

    void SetBatchOpLogWriterFactoryForTesting(
        MasterService::BatchOpLogWriterFactory factory);

    // Drive one eviction cycle synchronously, without the periodic worker.
    void RunBatchEvictForTesting(double evict_ratio_target,
                                 double evict_ratio_lowerbound);

    void RunNoFBatchEvictForTesting(double evict_ratio_target,
                                    double evict_ratio_lowerbound);

    void RunDfsEvictionForTesting();

    // Drive one tenant watermark eviction pass without the periodic worker.
    void RunTenantEvictForTesting();

    // Enable epoch bookkeeping without starting a live KV event publisher.
    void SetKvTenantEpochTrackingForTesting(bool enabled);

    // Called after each shard lock is released during RemoveAll. Tests can
    // commit into an already-scanned shard to pin the interleaving.
    void SetRemoveAllShardHookForTesting(std::function<void(size_t)> hook);

    // Counts of published clears and clears suppressed by a concurrent commit.
    uint64_t GetKvClearedPublishedForTesting() const;

    uint64_t GetKvClearedSuppressedForTesting() const;

    void SetNoFProbeFnForTesting(MasterService::NoFProbeFn fn);
    void SetNoFProbeReleaseFnForTesting(MasterService::NoFProbeReleaseFn fn);

    size_t GetMountedNoFSegmentCountForTesting();

    bool IsNoFSegmentMountedForTesting(const UUID& segment_id);

    std::optional<uint32_t> GetNoFHeartbeatFailureCountForTesting(
        const UUID& segment_id);

    size_t RunPromotionCandidateRetryForTesting();

    size_t CountCandidatesForTesting(const TenantId& tenant_id);

    void ResetCandidateBackoffsForTesting();

    size_t SoftPinHeapSize() const;

    size_t SoftPinRegistrationCount() const;

    static auto& AllocationStrategy(MasterService& service) {
        return service.allocation_strategy_;
    }
    static const auto& AllocationStrategy(const MasterService& service) {
        return service.allocation_strategy_;
    }

    static auto& BatchOplogStorage(MasterService& service) {
        return service.batch_oplog_storage_;
    }
    static const auto& BatchOplogStorage(const MasterService& service) {
        return service.batch_oplog_storage_;
    }

    static auto& ClientLivenessRecords(MasterService& service) {
        return service.client_liveness_records_;
    }
    static const auto& ClientLivenessRecords(const MasterService& service) {
        return service.client_liveness_records_;
    }

    static auto& ClientMutex(MasterService& service) {
        return service.client_mutex_;
    }
    static const auto& ClientMutex(const MasterService& service) {
        return service.client_mutex_;
    }

    static auto& DynamicReplicationWindows(MasterService& service) {
        return service.dynamic_replication_windows_;
    }
    static const auto& DynamicReplicationWindows(const MasterService& service) {
        return service.dynamic_replication_windows_;
    }

    static auto& EnableDfs(MasterService& service) {
        return service.enable_dfs_;
    }
    static const auto& EnableDfs(const MasterService& service) {
        return service.enable_dfs_;
    }

    static auto& EnableOplog(MasterService& service) {
        return service.enable_oplog_;
    }
    static const auto& EnableOplog(const MasterService& service) {
        return service.enable_oplog_;
    }

    static auto& EvictionRunning(MasterService& service) {
        return service.eviction_running_;
    }
    static const auto& EvictionRunning(const MasterService& service) {
        return service.eviction_running_;
    }

    static auto& EvictionThread(MasterService& service) {
        return service.eviction_thread_;
    }
    static const auto& EvictionThread(const MasterService& service) {
        return service.eviction_thread_;
    }

    static auto& LocalSsdManager(MasterService& service) {
        return service.local_ssd_manager_;
    }
    static const auto& LocalSsdManager(const MasterService& service) {
        return service.local_ssd_manager_;
    }

    static auto& MetadataShards(MasterService& service) {
        return service.metadata_shards_;
    }
    static const auto& MetadataShards(const MasterService& service) {
        return service.metadata_shards_;
    }

    static auto& NeedMemEviction(MasterService& service) {
        return service.need_mem_eviction_;
    }
    static const auto& NeedMemEviction(const MasterService& service) {
        return service.need_mem_eviction_;
    }

    static auto& NofSegmentManager(MasterService& service) {
        return service.nof_segment_manager_;
    }
    static const auto& NofSegmentManager(const MasterService& service) {
        return service.nof_segment_manager_;
    }

    static auto& ObjectOperationLocks(MasterService& service) {
        return service.object_operation_locks_;
    }
    static const auto& ObjectOperationLocks(const MasterService& service) {
        return service.object_operation_locks_;
    }

    static auto& OrderedOplogWriter(MasterService& service) {
        return service.ordered_oplog_writer_;
    }
    static const auto& OrderedOplogWriter(const MasterService& service) {
        return service.ordered_oplog_writer_;
    }

    static auto& PromotionAdmissionThreshold(MasterService& service) {
        return service.promotion_admission_threshold_;
    }
    static const auto& PromotionAdmissionThreshold(
        const MasterService& service) {
        return service.promotion_admission_threshold_;
    }

    static auto& PromotionCandidateCount(MasterService& service) {
        return service.promotion_candidate_count_;
    }
    static const auto& PromotionCandidateCount(const MasterService& service) {
        return service.promotion_candidate_count_;
    }

    static auto& PromotionInFlight(MasterService& service) {
        return service.promotion_in_flight_;
    }
    static const auto& PromotionInFlight(const MasterService& service) {
        return service.promotion_in_flight_;
    }

    static auto& ReplicaCleanupWorker(MasterService& service) {
        return service.replica_cleanup_worker_;
    }
    static const auto& ReplicaCleanupWorker(const MasterService& service) {
        return service.replica_cleanup_worker_;
    }

    static auto& RootFsDir(MasterService& service) {
        return service.root_fs_dir_;
    }
    static const auto& RootFsDir(const MasterService& service) {
        return service.root_fs_dir_;
    }

    static auto& SegmentManager(MasterService& service) {
        return service.segment_manager_;
    }
    static const auto& SegmentManager(const MasterService& service) {
        return service.segment_manager_;
    }

    static auto& SnapshotCatalogStore(MasterService& service) {
        return service.snapshot_catalog_store_;
    }
    static const auto& SnapshotCatalogStore(const MasterService& service) {
        return service.snapshot_catalog_store_;
    }

    static auto& SnapshotManager(MasterService& service) {
        return service.snapshot_manager_;
    }
    static const auto& SnapshotManager(const MasterService& service) {
        return service.snapshot_manager_;
    }

    static auto& SnapshotMutex(MasterService& service) {
        return service.snapshot_mutex_;
    }
    static const auto& SnapshotMutex(const MasterService& service) {
        return service.snapshot_mutex_;
    }

    static auto& SnapshotObjectStore(MasterService& service) {
        return service.snapshot_object_store_;
    }
    static const auto& SnapshotObjectStore(const MasterService& service) {
        return service.snapshot_object_store_;
    }

    static auto& SoftPinDeadlineIndex(MasterService& service) {
        return service.soft_pin_deadline_index_;
    }
    static const auto& SoftPinDeadlineIndex(const MasterService& service) {
        return service.soft_pin_deadline_index_;
    }

    static auto& TaskManager(MasterService& service) {
        return service.task_manager_;
    }
    static const auto& TaskManager(const MasterService& service) {
        return service.task_manager_;
    }

    static auto& TenantQuotaPolicyMutex(MasterService& service) {
        return service.tenant_quota_policy_mutex_;
    }
    static const auto& TenantQuotaPolicyMutex(const MasterService& service) {
        return service.tenant_quota_policy_mutex_;
    }

    static auto& TenantQuotaPolicyStore(MasterService& service) {
        return service.tenant_quota_policy_store_;
    }
    static const auto& TenantQuotaPolicyStore(const MasterService& service) {
        return service.tenant_quota_policy_store_;
    }

    static auto& TenantQuotaRecomputeMutex(MasterService& service) {
        return service.tenant_quota_recompute_mutex_;
    }
    static const auto& TenantQuotaRecomputeMutex(const MasterService& service) {
        return service.tenant_quota_recompute_mutex_;
    }

    static auto& TenantQuotaTable(MasterService& service) {
        return service.tenant_quota_table_;
    }
    static const auto& TenantQuotaTable(const MasterService& service) {
        return service.tenant_quota_table_;
    }

    static auto& WeightMetadata(MasterService& service) {
        return service.weight_manager_.weight_metadata_;
    }
    static const auto& WeightMetadata(const MasterService& service) {
        return service.weight_manager_.weight_metadata_;
    }

    auto AddReplicaForRetainedClient(const UUID& client_id,
                                     const std::string& key,
                                     const TenantId& tenant_id,
                                     Replica& replica)
        -> tl::expected<bool, ErrorCode> {
        return service_.AddReplicaForRetainedClient(client_id, key, tenant_id,
                                                    replica);
    }

    tl::expected<uint64_t, ErrorCode> AppendOpLogVisibleBeforeDurable(
        OpType type, const std::string& tenant_id, const std::string& key,
        const std::string& payload) {
        return service_.AppendOpLogVisibleBeforeDurable(type, tenant_id, key,
                                                        payload);
    }

    tl::expected<OpLogEntry, ErrorCode> AppendOpLogWithDurableFinalize(
        OpType type, const std::string& tenant_id, const std::string& key,
        const std::string& payload,
        MasterService::DurableFinalizeCallback callback) {
        return service_.AppendOpLogWithDurableFinalize(
            type, tenant_id, key, payload, std::move(callback));
    }

    tl::expected<void, ErrorCode> ChargeTenantQuota(TenantQuotaHandle account,
                                                    uint64_t bytes) {
        return service_.ChargeTenantQuota(std::move(account), bytes);
    }

    void CleanupExpiredSoftPins(
        const std::chrono::system_clock::time_point& now) {
        service_.CleanupExpiredSoftPins(now);
    }

    void ClearCandidatesForReload() { service_.ClearCandidatesForReload(); }

    void ClearDynamicReplicationStateForKey(TenantState& tenant_state,
                                            const std::string& key) {
        service_.ClearDynamicReplicationStateForKey(tenant_state, key);
    }

    void ClearLocalDiskHandlesOwnedBy(const UUID& owner) {
        service_.ClearLocalDiskHandlesOwnedBy(owner);
    }

    void ClearInvalidHandles() { service_.ClearInvalidHandles(); }

    void ClearInvalidHandles(
        const std::unordered_set<UUID, boost::hash<UUID>>& retaining_clients) {
        service_.ClearInvalidHandles(retaining_clients);
    }

    std::unique_ptr<ha::SnapshotCatalogStore> CreateSnapshotCatalogStore(
        const MasterServiceConfig& config);

    void DiscardExpiredProcessingReplicas(
        MetadataShardAccessorRW& shard,
        const std::chrono::system_clock::time_point& now) {
        service_.DiscardExpiredProcessingReplicas(shard, now);
    }

    uint32_t DynamicReplicationAdmissionMinHits() const {
        return service_.DynamicReplicationAdmissionMinHits();
    }

    static int64_t DynamicReplicationNowMs() {
        return MasterService::DynamicReplicationNowMs();
    }

    uint64_t DynamicReplicationVersionEpoch(
        const ObjectMetadata& metadata) const {
        return service_.DynamicReplicationVersionEpoch(metadata);
    }

    size_t EraseReplicasWithCacheTotalAccounting(
        ObjectMetadata& metadata,
        const std::function<bool(const Replica&)>& pred_fn,
        std::vector<ReplicaID>* erased_replica_ids = nullptr) {
        return service_.EraseReplicasWithCacheTotalAccounting(
            metadata, pred_fn, erased_replica_ids);
    }

    TenantQuotaEvictionResult EvictTenantMemoryForQuota(
        const TenantId& tenant_id, uint64_t target_bytes) {
        return service_.EvictTenantMemoryForQuota(tenant_id, target_bytes);
    }

    void FinalizeExpiredProcessingReplicasAfterDurable(
        const OpLogEntry& durable_entry,
        const std::chrono::system_clock::time_point& ttl) {
        service_.FinalizeExpiredProcessingReplicasAfterDurable(durable_entry,
                                                               ttl);
    }

    void FinalizeRemovedReplicasAfterDurable(
        const OpLogEntry& durable_entry,
        const std::vector<ReplicaID>& replica_ids, QuotaEraseMode quota_mode,
        const std::vector<std::string>& previous_media_hint = {}) {
        service_.FinalizeRemovedReplicasAfterDurable(
            durable_entry, replica_ids, quota_mode, previous_media_hint);
    }

    std::shared_ptr<ClientLivenessRecord> FindClientRecord(
        const UUID& client_id) const {
        return service_.FindClientRecord(client_id);
    }

    TenantQuotaHandle GetBoundTenantQuotaHandle(
        const TenantState& tenant_state) const {
        return service_.GetBoundTenantQuotaHandle(tenant_state);
    }

    TenantState& GetOrCreateTenantState(MetadataShard& shard,
                                        const TenantId& tenant_id) {
        return service_.GetOrCreateTenantState(shard, tenant_id);
    }

    bool IsReplicaReadable(const Replica& replica) const {
        return service_.IsReplicaReadable(replica);
    }

    static std::vector<std::string> KvMediaForMetadata(
        const ObjectMetadata& metadata) {
        return MasterService::KvMediaForMetadata(metadata);
    }

    static std::vector<std::string> KvMediaForRemoval(
        const ObjectMetadata& metadata) {
        return MasterService::KvMediaForRemoval(metadata);
    }

    void LoadTenantQuotaPoliciesFromStoreOrThrow() {
        service_.LoadTenantQuotaPoliciesFromStoreOrThrow();
    }

    ObjectIdentity MakeObjectIdentityForRequest(
        const std::string& user_key, const TenantId& tenant_id) const {
        return service_.MakeObjectIdentityForRequest(user_key, tenant_id);
    }

    bool ObserveDynamicReplicationAccess(const ObjectIdentity& object_id) {
        return service_.ObserveDynamicReplicationAccess(object_id);
    }

    bool ProcessClientOffboardingJob(ClientOffboardingJob& job) {
        return service_.ProcessClientOffboardingJob(job);
    }

    tl::expected<void, ErrorCode> PushOffloadingQueue(
        const ObjectIdentity& object_id, Replica& replica,
        std::vector<UUID>* mirror_clients = nullptr) {
        return service_.PushOffloadingQueue(object_id, replica, mirror_clients);
    }

    void ReRouteRestoredObjectsByKey() {
        service_.ReRouteRestoredObjectsByKey();
    }

    void RebuildGroupState() { service_.RebuildGroupState(); }

    void RebuildTenantQuotaUsageFromMetadata() {
        service_.RebuildTenantQuotaUsageFromMetadata();
    }

    void RecomputeTenantEffectiveQuotas() {
        service_.RecomputeTenantEffectiveQuotas();
    }

    void ReleaseTenantQuota(TenantQuotaHandle account, uint64_t bytes) {
        service_.ReleaseTenantQuota(std::move(account), bytes);
    }

    const TenantId& ResolveRequestTenantId(const TenantId& tenant_id) const {
        return service_.ResolveRequestTenantId(tenant_id);
    }

    size_t RunPromotionCandidateRetry(size_t max_shards_to_scan) {
        return service_.RunPromotionCandidateRetry(max_shards_to_scan);
    }

    size_t RunPromotionCandidateRetry() {
        return service_.RunPromotionCandidateRetry();
    }

    PromotionQueueResult TryPushPromotionQueue(const ObjectIdentity& object_id,
                                               bool record_candidate = true) {
        return service_.TryPushPromotionQueue(object_id, record_candidate);
    }

    size_t getShardIndex(const TenantId& tenant_id,
                         const std::string& user_key) const {
        return service_.getShardIndex(tenant_id, user_key);
    }

    size_t getShardIndex(const std::string& key) const {
        return service_.getShardIndex(key);
    }

   private:
    MasterService& service_;
};

}  // namespace mooncake::test

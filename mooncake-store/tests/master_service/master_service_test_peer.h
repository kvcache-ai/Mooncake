#pragma once

#include <utility>

#include "master_service.h"
#include "object_test_helpers.h"

namespace mooncake::test {

// The single test access boundary for MasterService. Keep test-only inspection,
// mutation and synchronous drivers here, not in the production service API.
// Raw state accessors do not acquire locks. Callers must preserve the service's
// lock order; synchronous drivers must not race their background workers.
class MasterServiceTestPeer {
   public:
    explicit MasterServiceTestPeer(MasterService& service)
        : service_(service) {}

    using MetadataAccessorRO = MasterService::MetadataAccessorRO;
    using MetadataAccessorRW = MasterService::MetadataAccessorRW;
    using MetadataSerializer = MasterService::MetadataSerializer;
    using ObjectIdentity = MasterService::ObjectIdentity;
    using ObjectMetadata = mooncake::ObjectMetadata;
    using PromotionQueueResult = mooncake::PromotionQueueResult;
    using PromotionTask = mooncake::PromotionTask;
    using QuotaEraseMode = MasterService::QuotaEraseMode;
    using TenantQuotaEvictionResult = MasterService::TenantQuotaEvictionResult;

    static constexpr auto kDynamicReplicationWindowEntryLimit =
        DynamicReplicationController::kWindowEntryLimit;
    static constexpr auto kMaxPromotionExecutionFailures =
        MasterService::kMaxPromotionExecutionFailures;
    static constexpr auto kObjectOperationLockStripes =
        MasterService::kObjectOperationLockStripes;
    static constexpr auto kPromotionCandidateMaxRetries =
        PromotionCandidateTracker::kMaxRetries;

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

    // Called after each tenant's route is released during RemoveAll, which is
    // the only point where a test can commit into an already-scanned tenant.
    // The argument reports the scan's own progress, not a container index.
    void SetRemoveAllTenantHookForTesting(std::function<void(size_t)> hook);

    // Counts of published clears and clears suppressed by a concurrent commit.
    uint64_t GetKvClearedPublishedForTesting() const;

    uint64_t GetKvClearedSuppressedForTesting() const;

    void SetNoFProbeFnForTesting(MasterService::NoFProbeFn fn);

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
        return service.dynamic_replication_.windows_;
    }
    static const auto& DynamicReplicationWindows(const MasterService& service) {
        return service.dynamic_replication_.windows_;
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

    // --- The tenant metadata model ------------------------------------------
    // One tenant per registered tenant id, owning that tenant's object route,
    // its group table and its bound quota account. A pass over all objects is
    // `Visit` over the registry followed by a `ReadCursor` per tenant.

    static auto& Tenants(MasterService& service) { return service.tenants_; }
    static const auto& Tenants(const MasterService& service) {
        return service.tenants_;
    }

    // The entry the tenant of `object_id` routes for its key, or nullptr when
    // that tenant is absent or the key is not routed. The tenant id is resolved
    // as for a request. The handle is strong, so the entry outlives the
    // lookup; its metadata is read under the entry's own lock.
    static std::shared_ptr<ObjectEntry> FindObject(
        MasterService& service, const ObjectIdentity& object_id) {
        auto tenant = service.tenants_.Lookup(object_id.tenant_id);
        return tenant == nullptr ? nullptr : tenant->Get(object_id.user_key);
    }

    // A replica-action lease is recorded per tenant and keyed by proposal id,
    // so a caller names both.
    static std::optional<ReplicaActionLease> FindDynamicReplicationLease(
        MasterService& service, const TenantId& tenant_id,
        const UUID& proposal_id) {
        return service.dynamic_replication_.FindLease(tenant_id, proposal_id);
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
        return service.promotion_candidates_.count_;
    }
    static const auto& PromotionCandidateCount(const MasterService& service) {
        return service.promotion_candidates_.count_;
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
        return service.tenant_quota_.policy_mutex_;
    }
    static const auto& TenantQuotaPolicyMutex(const MasterService& service) {
        return service.tenant_quota_.policy_mutex_;
    }

    static auto& TenantQuotaPolicyStore(MasterService& service) {
        return service.tenant_quota_.policy_store_;
    }
    static const auto& TenantQuotaPolicyStore(const MasterService& service) {
        return service.tenant_quota_.policy_store_;
    }

    static auto& TenantQuotaRecomputeMutex(MasterService& service) {
        return service.tenant_quota_.recompute_mutex_;
    }
    static const auto& TenantQuotaRecomputeMutex(const MasterService& service) {
        return service.tenant_quota_.recompute_mutex_;
    }

    static auto& TenantQuotaTable(MasterService& service) {
        return service.tenant_quota_.table_;
    }
    static const auto& TenantQuotaTable(const MasterService& service) {
        return service.tenant_quota_.table_;
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

    void CleanupExpiredSoftPins(
        const std::chrono::system_clock::time_point& now) {
        service_.CleanupExpiredSoftPins(now);
    }

    void ClearCandidatesForReload() { service_.ClearCandidatesForReload(); }

    // The caller holds the entry exclusively, as through MetadataAccessorRW.
    void ClearDynamicReplicationStateForKey(const TenantId& tenant_id,
                                            const ObjectEntry& entry,
                                            ObjectEntry::State& state) {
        service_.dynamic_replication_.ClearPendingLocked(tenant_id, entry,
                                                         state);
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
        metadata::Tenant& tenant, const TenantId& tenant_id,
        const std::chrono::system_clock::time_point& now) {
        service_.DiscardExpiredProcessingReplicas(tenant, tenant_id, now);
    }

    uint32_t DynamicReplicationAdmissionMinHits() const {
        return service_.dynamic_replication_.AdmissionMinHits();
    }

    static int64_t DynamicReplicationNowMs() {
        return DynamicReplicationController::NowMs();
    }

    uint64_t DynamicReplicationVersionEpoch(
        const ObjectMetadata& metadata) const {
        return DynamicReplicationController::VersionEpoch(metadata);
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
        std::shared_ptr<ObjectEntry> entry, const OpLogEntry& durable_entry,
        const std::chrono::system_clock::time_point& ttl) {
        service_.FinalizeExpiredProcessingReplicasAfterDurable(
            std::move(entry), durable_entry, ttl);
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

    // Resolves the tenant, creating it through the registry's factory on first
    // use; the factory binds the tenant's quota account, so a caller that holds
    // a tenant always has one to charge against. The id is taken as given.
    std::shared_ptr<metadata::Tenant> GetOrCreateTenantHandle(
        const TenantId& tenant_id) {
        return service_.tenants_.GetOrCreateTenant(tenant_id);
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
        service_.tenant_quota_.LoadPoliciesOrThrow();
    }

    bool ObserveDynamicReplicationAccess(const ObjectIdentity& object_id) {
        return service_.dynamic_replication_.ObserveAccess(object_id.tenant_id,
                                                           object_id.user_key);
    }

    bool ProcessClientOffboardingJob(ClientOffboardingJob& job) {
        return service_.ProcessClientOffboardingJob(job);
    }

    tl::expected<void, ErrorCode> PushOffloadingQueue(
        const ObjectIdentity& object_id, Replica& replica,
        std::vector<UUID>* mirror_clients = nullptr) {
        return service_.PushOffloadingQueue(object_id, replica, mirror_clients);
    }

    void RebuildGroupState() { service_.RebuildGroupState(); }

    void RebuildTenantQuotaUsageFromMetadata() {
        service_.RebuildTenantQuotaUsageFromMetadata();
    }

    void RecomputeTenantEffectiveQuotas() {
        service_.tenant_quota_.Recompute();
    }

    size_t RunPromotionCandidateRetry() {
        return service_.RunPromotionCandidateRetry();
    }

    // Seeds an in-flight PromotionTask on (tenant, key), publishing the entry
    // when the key is not routed yet, so a test can drive
    // NotifyPromotionSuccess without the on-hit admission gate.
    void SeedPromotionTaskForTesting(const TenantId& tenant_id,
                                     const std::string& key,
                                     const UUID& holder_id, ReplicaID alloc_id,
                                     uint64_t object_size);

    PromotionQueueResult TryPushPromotionQueue(const ObjectIdentity& object_id,
                                               bool record_candidate = true) {
        return service_.TryPushPromotionQueue(object_id, record_candidate);
    }

    // Runs `fn` while the entry's own lock is held, the way a mutating path
    // holds it. A test parks a path that must take this entry by blocking
    // inside `fn`.
    template <typename Fn>
    void WithEntryLockedForTesting(const TenantId& tenant_id,
                                   const std::string& key, Fn&& fn) {
        auto entry = FindObject(service_, ObjectIdentity{tenant_id, key});
        assert(entry != nullptr);
        test::ObjectEntryTestPeer::WithExclusiveAccess(
            *entry, [&](ObjectMetadata&, ObjectEntry::State&) { fn(); });
    }

   private:
    MasterService& service_;
};

}  // namespace mooncake::test

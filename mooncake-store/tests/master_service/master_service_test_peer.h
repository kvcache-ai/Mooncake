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

    size_t RunWeightReconciliationForTesting(uint64_t now_ms,
                                             size_t limit = 32) {
        return service_.weight_manager_.ReconcileWeightMetadataStoreOnce(now_ms,
                                                                         limit);
    }

    bool DropWeightGroupMemberForTesting(const WeightRevisionIdentity& identity,
                                         const std::string& key) {
        return service_
            .RemoveObject(key, TenantId(identity.tenant_id), true, true)
            .has_value();
    }

    using MetadataAccessorRO = MasterService::MetadataAccessorRO;
    using MetadataAccessorRW = MasterService::MetadataAccessorRW;
    using ObjectIdentity = MasterService::ObjectIdentity;
    using ObjectMetadata = mooncake::ObjectMetadata;
    using PromotionQueueResult = mooncake::PromotionQueueResult;
    using PromotionTask = mooncake::PromotionTask;
    using NamespaceEvictionResult = MasterService::NamespaceEvictionResult;

    static constexpr auto kDynamicReplicationWindowEntryLimit =
        DynamicReplicationController::kWindowEntryLimit;
    static constexpr auto kMaxPromotionExecutionFailures =
        MasterService::kMaxPromotionExecutionFailures;
    static constexpr auto kPromotionCandidateMaxRetries =
        PromotionCandidateTracker::kMaxRetries;

    ErrorCode SetBatchOpLogBackendForTesting(
        std::shared_ptr<HaKvBackend> backend);

    void SetBatchOpLogWriterFactoryForTesting(
        MasterService::BatchOpLogWriterFactory factory);

    // The OpLog payload built from one object's metadata, so a suite can name
    // the fields that payload carries.
    static std::string SerializeMetadataForOpLogForTesting(
        MasterService& service, const ObjectMetadata& metadata);
    static std::string
    SerializeMetadataForOpLogFromReplicaDescriptorsForTesting(
        MasterService& service, const ObjectMetadata& metadata,
        const std::vector<Replica::Descriptor>& replicas);

    // Drive one eviction cycle synchronously, without the periodic worker.
    void RunBatchEvictForTesting(double evict_ratio_target,
                                 double evict_ratio_lowerbound);

    void RunNoFBatchEvictForTesting(double evict_ratio_target,
                                    double evict_ratio_lowerbound);

    void RunDfsEvictionForTesting();

    // Queue one segment through the private HTTP metadata cleanup path, as the
    // client monitor does when a client times out.
    void CleanupHttpMetadataForTesting(const std::string& segment_name);

    // Segments waiting in the remote HTTP metadata cleanup queue.
    size_t HttpMetadataCleanupQueueSizeForTesting() const;

    // Drive one tenant watermark eviction pass without the periodic worker.
    void RunTenantEvictForTesting();

    // The service's walk over the namespaces that hold objects.
    void VisitNamespaces(
        const std::function<void(const TenantId&, metadata::Namespace&)>& visit)
        const {
        service_.VisitNamespaces(visit);
    }

    // Enable epoch bookkeeping without starting a live KV event publisher.
    void SetKvTenantEpochTrackingForTesting(bool enabled);

    // Called once per tenant RemoveAll has finished removing. Tests can commit
    // into a tenant the walk already passed to pin the interleaving.
    void SetRemoveAllTenantHookForTesting(
        std::function<void(const TenantId&)> hook);

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

    // A replica-action lease is recorded per tenant and keyed by proposal id,
    // so a caller names both.
    static std::optional<ReplicaActionLease> FindDynamicReplicationLease(
        MasterService& service, const TenantId& tenant_id,
        const UUID& proposal_id) {
        return service.dynamic_replication_.FindLease(tenant_id, proposal_id);
    }

    // A tenant keeps a lease table only while one of its leases is in flight.
    static bool HasDynamicReplicationLeaseTable(
        const DynamicReplicationController& controller,
        const TenantId& tenant_id) {
        std::lock_guard<std::mutex> lock(controller.lease_mutex_);
        return controller.leases_.contains(tenant_id);
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

    // One namespace per tenant id written to, owning that tenant's object
    // route and its group index.
    static auto& Namespaces(MasterService& service) {
        return service.namespaces_;
    }
    static const auto& Namespaces(const MasterService& service) {
        return service.namespaces_;
    }

    // The object under `object_id`, held shared or exclusively for the
    // caller's scope the way a request path holds it; nullopt when that
    // tenant is absent or the key holds no object. A test parks a path that
    // must take the key by keeping the write guard.
    static std::optional<route::ReadGuard> ReadObject(
        const MasterService& service, const ObjectIdentity& object_id) {
        const metadata::Namespace* ns =
            service.namespaces_.Lookup(object_id.tenant_id);
        return ns == nullptr ? std::nullopt
                             : ns->objects.Read(object_id.user_key);
    }
    static std::optional<route::WriteGuard> WriteObject(
        MasterService& service, const ObjectIdentity& object_id) {
        metadata::Namespace* ns =
            service.namespaces_.Lookup(object_id.tenant_id);
        return ns == nullptr ? std::nullopt
                             : ns->objects.Write(object_id.user_key);
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

    static auto& MasterStateMutex(MasterService& service) {
        return service.master_state_mutex_;
    }
    static const auto& MasterStateMutex(const MasterService& service) {
        return service.master_state_mutex_;
    }

    // Restore lifecycle (RFC #4700, slice 1).
    static MasterService::RestoreLifecycleState GetRestoreLifecycleState(
        MasterService& service) {
        return service.lifecycle_state_.load(std::memory_order_acquire);
    }

    // Force a lifecycle state so worker-admission can be tested without
    // running a real promotion restore.
    static void SetRestoreLifecycleStateForTesting(
        MasterService& service, MasterService::RestoreLifecycleState state) {
        service.lifecycle_state_.store(state, std::memory_order_release);
    }

    bool AdmitsBusinessWorkersForTesting() const {
        return service_.AdmitsBusinessWorkers();
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
        return service.tenant_quota_policy_->manager_.policy_mutex_;
    }
    static const auto& TenantQuotaPolicyMutex(const MasterService& service) {
        return service.tenant_quota_policy_->manager_.policy_mutex_;
    }

    static auto& TenantQuotaPolicyStore(MasterService& service) {
        return service.tenant_quota_policy_->manager_.policy_store_;
    }
    static const auto& TenantQuotaPolicyStore(const MasterService& service) {
        return service.tenant_quota_policy_->manager_.policy_store_;
    }

    static auto& TenantQuotaRecomputeMutex(MasterService& service) {
        return service.tenant_quota_policy_->manager_.recompute_mutex_;
    }
    static const auto& TenantQuotaRecomputeMutex(const MasterService& service) {
        return service.tenant_quota_policy_->manager_.recompute_mutex_;
    }

    static auto& TenantQuotaTable(MasterService& service) {
        return service.tenant_quota_policy_->manager_.table_;
    }
    static const auto& TenantQuotaTable(const MasterService& service) {
        return service.tenant_quota_policy_->manager_.table_;
    }

    static auto& WeightMetadata(MasterService& service) {
        return service.weight_manager_.weight_metadata_;
    }
    static auto& WeightManager(MasterService& service) {
        return service.weight_manager_;
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

    void CleanupExpiredDynamicReplicationState() {
        service_.CleanupExpiredDynamicReplicationState();
    }

    void CleanupExpiredSoftPins(
        const std::chrono::system_clock::time_point& now) {
        service_.CleanupExpiredSoftPins(now);
    }

    // The caller holds the key exclusively, as through MetadataAccessorRW.
    void ClearDynamicReplicationStateForKey(const TenantId& tenant_id,
                                            const std::string& key,
                                            route::ObjectState& state) {
        service_.dynamic_replication_.ClearPendingLocked(tenant_id, key, state);
    }

    void ClearLocalDiskHandlesOwnedBy(const UUID& owner) {
        service_.ClearLocalDiskHandlesOwnedBy(owner);
    }

    void ClearInvalidHandles() { service_.ClearInvalidHandles(); }

    void ClearInvalidHandles(
        const std::unordered_set<UUID, boost::hash<UUID>>& retaining_clients) {
        service_.ClearInvalidHandles(retaining_clients);
    }

    void DiscardExpiredProcessingReplicas(
        const TenantId& tenant_id,
        const std::chrono::system_clock::time_point& now) {
        if (const metadata::Namespace* ns =
                service_.namespaces_.Lookup(tenant_id)) {
            service_.DiscardExpiredProcessingReplicas(*ns, now);
        }
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

    // Submits a dynamic replica proposal under the shared state lock that
    // SubmitReplicaActionProposalLocked expects.
    tl::expected<ReplicaActionLease, ErrorCode> SubmitReplicaActionProposal(
        const ReplicaActionProposal& proposal) {
        std::shared_lock<std::shared_mutex> lock(service_.master_state_mutex_);
        return service_.SubmitReplicaActionProposalLocked(proposal);
    }

    size_t EraseReplicasWithCacheTotalAccounting(
        ObjectMetadata& metadata,
        const std::function<bool(const Replica&)>& pred_fn,
        std::vector<ReplicaID>* erased_replica_ids = nullptr) {
        return service_.EraseReplicasWithCacheTotalAccounting(
            metadata, pred_fn, erased_replica_ids);
    }

    NamespaceEvictionResult EvictNamespaceMemory(const TenantId& tenant_id,
                                                 uint64_t target_bytes) {
        return service_.EvictNamespaceMemory(tenant_id, target_bytes);
    }

    void FinalizeExpiredProcessingReplicasAfterDurable(
        route::ObjectRef object, const OpLogEntry& durable_entry,
        const std::chrono::system_clock::time_point& ttl) {
        service_.FinalizeExpiredProcessingReplicasAfterDurable(
            std::move(object), durable_entry, ttl);
    }

    void FinalizeMetadataEraseAfterDurable(route::ObjectRef object,
                                           const TenantId& tenant_id) {
        service_.FinalizeMetadataEraseAfterDurable(std::move(object),
                                                   tenant_id);
    }

    void FinalizeRemovedReplicasAfterDurable(
        const OpLogEntry& durable_entry,
        const std::vector<ReplicaID>& replica_ids) {
        service_.FinalizeRemovedReplicasAfterDurable(durable_entry,
                                                     replica_ids);
    }

    std::shared_ptr<ClientLivenessRecord> FindClientRecord(
        const UUID& client_id) const {
        return service_.FindClientRecord(client_id);
    }

    // Resolves the namespace, creating it through the table's factory on first
    // use; the factory hangs the namespace policy's attachment (the tenant's
    // quota account) on it.
    metadata::Namespace& GetOrCreateNamespace(const TenantId& tenant_id) {
        return service_.namespaces_.GetOrCreate(tenant_id);
    }

    // The tenant's one stable quota account; multi-tenancy only.
    mooncake::TenantQuotaAccount& TenantQuotaAccount(
        const TenantId& tenant_id) {
        return service_.tenant_quota_policy_->manager_.AccountFor(tenant_id);
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
        service_.tenant_quota_policy_->manager_.LoadPoliciesOrThrow();
    }

    ObjectIdentity MakeObjectIdentityForRequest(
        const std::string& user_key, const TenantId& tenant_id) const {
        return service_.MakeObjectIdentityForRequest(user_key, tenant_id);
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

    const TenantId& ResolveRequestTenantId(const TenantId& tenant_id) const {
        return service_.ResolveRequestTenantId(tenant_id);
    }

    size_t RunPromotionCandidateRetry() {
        return service_.RunPromotionCandidateRetry();
    }

    PromotionQueueResult TryPushPromotionQueue(const ObjectIdentity& object_id,
                                               bool record_candidate = true) {
        return service_.TryPushPromotionQueue(object_id, record_candidate);
    }

   private:
    MasterService& service_;
};

}  // namespace mooncake::test

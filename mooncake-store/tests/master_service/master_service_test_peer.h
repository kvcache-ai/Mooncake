#pragma once

#include <cassert>
#include <utility>

#include "master_service.h"

namespace mooncake::test {
namespace detail {

// A result type the entry helpers below reject: their own empty optional
// already reports "the callback did not run", so a callback that returns an
// optional would be wrapped a second time.
template <typename T>
struct IsOptionalResult : std::false_type {};
template <typename T>
struct IsOptionalResult<std::optional<T>> : std::true_type {};

}  // namespace detail

// The single test access boundary for MasterService. Keep test-only inspection,
// mutation and synchronous drivers here, not in the production service API.
// Raw state accessors do not acquire locks. Callers must preserve the service's
// lock order; synchronous drivers must not race their background workers.
class MasterServiceTestPeer {
   public:
    explicit MasterServiceTestPeer(MasterService& service)
        : service_(service) {}

    using MetadataSerializer = MasterService::MetadataSerializer;
    using ObjectIdentity = MasterService::ObjectIdentity;
    using ObjectMetadata = mooncake::ObjectMetadata;
    using PromotionQueueResult = mooncake::PromotionQueueResult;
    using PromotionTask = mooncake::PromotionTask;
    using QuotaEraseMode = MasterService::QuotaEraseMode;
    using TenantQuotaEvictionResult = MasterService::TenantQuotaEvictionResult;
    using TenantRegistry = metadata::TenantRegistry;

    static constexpr auto kDynamicReplicationWindowEntryLimit =
        MasterService::kDynamicReplicationWindowEntryLimit;
    static constexpr auto kMaxPromotionExecutionFailures =
        MasterService::kMaxPromotionExecutionFailures;
    static constexpr auto kObjectOperationLockStripes =
        MasterService::kObjectOperationLockStripes;
    static constexpr auto kPromotionCandidateMaxRetries =
        MasterService::kPromotionCandidateMaxRetries;
    static constexpr auto kPromotionRetryScanBudget =
        MasterService::kPromotionRetryScanBudget;

    ErrorCode SetBatchOpLogBackendForTesting(
        std::shared_ptr<HaKvBackend> backend);

    void SetBatchOpLogWriterFactoryForTesting(
        MasterService::BatchOpLogWriterFactory factory);

    // Installs an OpLog writer without the etcd handshake: the batches such a
    // writer accepts are made durable by its own `write_batch`. Everything
    // above it -- slot reservation, entry construction and the commit that
    // registers the durable finalizer -- is the production path.
    static void InstallOpLogWriterForTesting(
        MasterService& service, std::unique_ptr<OrderedOpLogWriter> writer) {
        assert(!service.ordered_oplog_writer_);
        assert(writer != nullptr);
        service.ordered_oplog_writer_ = std::move(writer);
        service.ordered_oplog_writer_->Start();
        service.ordered_oplog_writer_->ActivateRuntimeMetrics();
    }

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

    // Ages every candidate past its TTL, so the next retry round that examines
    // one drops it.
    void AgeCandidatesPastTtlForTesting();

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

    // --- The tenant metadata model ------------------------------------------
    // One tenant per registered tenant id, owning that tenant's object route,
    // its group table and its bound quota account. A walk over all objects is
    // `Visit` over the registry followed by `SnapshotObjects` per tenant.

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
        auto tenant = TenantForRequest(service, object_id.tenant_id);
        return tenant == nullptr ? nullptr : tenant->Get(object_id.user_key);
    }

    // A replica-action lease is recorded per tenant and keyed by proposal id,
    // so a caller names both.
    static std::optional<ReplicaActionLease> FindDynamicReplicationLease(
        MasterService& service, const TenantId& tenant_id,
        const UUID& proposal_id) {
        return service.FindDynamicReplicationLease(tenant_id, proposal_id);
    }

    // The keys whose published object carries a promotion candidate, for one
    // tenant. The candidate itself lives in the entry's own state, so each key
    // is resolved again before it is read.
    static std::vector<std::string> PromotionCandidateKeys(
        MasterService& service, const TenantId& tenant_id) {
        return service.PromotionCandidateKeys(tenant_id);
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

    static auto& PromotionRetryCursor(MasterService& service) {
        return service.promotion_retry_cursor_;
    }
    static const auto& PromotionRetryCursor(const MasterService& service) {
        return service.promotion_retry_cursor_;
    }

    static auto& PromotionRetryLastScanned(MasterService& service) {
        return service.promotion_retry_last_scanned_;
    }
    static const auto& PromotionRetryLastScanned(const MasterService& service) {
        return service.promotion_retry_last_scanned_;
    }

    // The next slice of one tenant's candidate index, as the retry sweep takes
    // it.
    static std::vector<std::string> TakePromotionCandidateSlice(
        MasterService& service, const TenantId& tenant_id, size_t limit) {
        return service.TakePromotionCandidateSlice(tenant_id, limit);
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

    void ReconcilePromotionBookkeeping() {
        service_.ReconcilePromotionBookkeeping();
    }

    // Drops `key`'s pending dynamic-replication task state under the entry's
    // own lock, the way the sweep paths do.
    void ClearDynamicReplicationStateForKey(const TenantId& tenant_id,
                                            metadata::Tenant& tenant,
                                            const std::string& key) {
        auto entry = tenant.Get(key);
        if (entry == nullptr) {
            return;
        }
        entry->WithExclusiveAccess(
            [&](ObjectMetadata&, ObjectEntry::State& state) {
                service_.ClearDynamicReplicationStateLocked(tenant_id, entry,
                                                            state);
            });
    }

    void ClearLocalDiskHandlesOwnedBy(const UUID& owner) {
        service_.ClearLocalDiskHandlesOwnedBy(owner);
    }

    // The two phases of the expired-pending sweep, exposed separately so a test
    // can let a new proposal take the key between them: the sweep must end the
    // proposal it scanned and nothing else.
    std::vector<MasterService::ExpiredDynamicReplicationPending>
    CollectExpiredDynamicReplicationPending(const metadata::Tenant& tenant,
                                            int64_t now_ms) const {
        return service_.CollectExpiredDynamicReplicationPending(tenant, now_ms);
    }

    void CleanupExpiredDynamicReplicationPending(
        const TenantId& tenant_id,
        const std::vector<MasterService::ExpiredDynamicReplicationPending>&
            expired) {
        service_.CleanupExpiredDynamicReplicationPending(tenant_id, expired);
    }

    void ClearInvalidHandles() { service_.ClearInvalidHandles(); }

    void ClearInvalidHandles(
        const std::unordered_set<UUID, boost::hash<UUID>>& retaining_clients) {
        service_.ClearInvalidHandles(retaining_clients);
    }

    std::unique_ptr<ha::SnapshotCatalogStore> CreateSnapshotCatalogStore(
        const MasterServiceConfig& config);

    void DiscardExpiredProcessingReplicas(
        metadata::Tenant& tenant,
        const std::chrono::system_clock::time_point& now) {
        service_.DiscardExpiredProcessingReplicas(tenant, now);
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
        std::shared_ptr<ObjectEntry> entry, const OpLogEntry& durable_entry,
        const std::vector<ReplicaID>& processing_replica_ids,
        const std::chrono::system_clock::time_point& ttl) {
        service_.FinalizeExpiredProcessingReplicasAfterDurable(
            std::move(entry), durable_entry, processing_replica_ids, ttl);
    }

    void FinalizeExpiredReplicationTaskAfterDurable(
        std::shared_ptr<ObjectEntry> entry, const OpLogEntry& durable_entry,
        ReplicaID source_id, const std::vector<ReplicaID>& target_ids,
        const UUID& dynamic_replication_lease_id,
        uint64_t dynamic_replication_version_epoch,
        const std::chrono::system_clock::time_point& ttl) {
        service_.FinalizeExpiredReplicationTaskAfterDurable(
            std::move(entry), durable_entry, source_id, target_ids,
            dynamic_replication_lease_id, dynamic_replication_version_epoch,
            ttl);
    }

    void FinalizeMetadataEraseAfterDurable(std::shared_ptr<ObjectEntry> entry,
                                           const TenantId& tenant_id,
                                           QuotaEraseMode quota_mode) {
        service_.FinalizeMetadataEraseAfterDurable(std::move(entry), tenant_id,
                                                   quota_mode);
    }

    void FinalizeRemovedReplicasAfterDurable(
        const std::shared_ptr<ObjectEntry>& entry,
        const OpLogEntry& durable_entry,
        const std::vector<ReplicaID>& replica_ids, QuotaEraseMode quota_mode,
        const std::vector<std::string>& previous_media_hint = {}) {
        service_.FinalizeRemovedReplicasAfterDurable(
            entry, durable_entry, replica_ids, quota_mode, previous_media_hint);
    }

    std::shared_ptr<ClientLivenessRecord> FindClientRecord(
        const UUID& client_id) const {
        return service_.FindClientRecord(client_id);
    }

    // Test access to the service-owned lease table: the dynamic-replication
    // proposal path files a lease under the publication that owns the
    // proposal, and a test stages that record directly.
    void PutDynamicReplicationLeaseForTesting(
        const TenantId& tenant_id, const std::shared_ptr<ObjectEntry>& entry,
        const UUID& proposal_id, ReplicaActionLease lease) {
        service_.PutDynamicReplicationLease(tenant_id, entry, proposal_id,
                                            std::move(lease));
    }

    TenantQuotaHandle GetBoundTenantQuotaHandle(
        const metadata::Tenant& tenant) const {
        return service_.GetBoundTenantQuotaHandle(tenant);
    }

    // Resolves the tenant, creating it through the registry's factory on first
    // use; the factory binds the tenant's quota account, so a caller that holds
    // a tenant always has one to charge against. The id is taken as given.
    std::shared_ptr<metadata::Tenant> GetOrCreateTenantHandle(
        const TenantId& tenant_id) {
        return service_.GetOrCreateTenantHandle(tenant_id);
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

    // Tears one object down the way a remove path does, with the same
    // accounting, without going through the public remove entry points: the
    // entry's own lock is held while the teardown runs and the route slot is
    // dropped. False when the tenant is absent or the key is not routed.
    bool EraseObjectForTesting(const TenantId& tenant_id,
                               const std::string& key) {
        const TenantId normalized = service_.ResolveRequestTenantId(tenant_id);
        auto tenant_handle = service_.tenants_.Lookup(normalized);
        if (tenant_handle == nullptr) {
            return false;
        }
        auto entry = tenant_handle->Get(key);
        if (entry == nullptr) {
            return false;
        }
        return entry->WithExclusiveAccess(
            [&](ObjectMetadata& metadata, ObjectEntry::State& state) {
                return service_.EraseMetadata(*tenant_handle, entry, metadata,
                                              state, normalized);
            });
    }

    // Runs `fn` while the entry's own lock is held, the way a mutating path
    // holds it. A test parks a path that must take this entry by blocking
    // inside `fn`.
    template <typename Fn>
    void WithEntryLockedForTesting(const TenantId& tenant_id,
                                   const std::string& key, Fn&& fn) {
        auto entry = FindObject(
            service_, service_.MakeObjectIdentityForRequest(key, tenant_id));
        assert(entry != nullptr);
        entry->WithExclusiveAccess(
            [&](ObjectMetadata&, ObjectEntry::State&) { fn(); });
    }

    // Runs `fn(tenant, entry, metadata, state)` on the object `tenant_id`'s
    // route publishes for `key`, under the entry's own shared lock, with the
    // callback contract of MasterService::WithObjectMetadataForRead. Empty
    // when the tenant is absent, the key is unrouted or the object unreadable.
    template <typename Fn>
    [[nodiscard]] auto WithPublishedObjectForRead(const TenantId& tenant_id,
                                                  const std::string& key,
                                                  Fn&& fn) const
        -> std::optional<std::invoke_result_t<
            Fn, const metadata::Tenant&, const std::shared_ptr<ObjectEntry>&,
            const ObjectMetadata&, const ObjectEntry::State&>> {
        using Result = std::invoke_result_t<
            Fn, const metadata::Tenant&, const std::shared_ptr<ObjectEntry>&,
            const ObjectMetadata&, const ObjectEntry::State&>;
        static_assert(
            !std::is_void_v<Result>,
            "this helper reports absence through its own optional, so "
            "the callback must return a result");
        static_assert(!std::is_reference_v<Result>,
                      "the result is carried by value: nothing a callback "
                      "returns may outlive the lock it ran under");
        static_assert(
            !detail::IsOptionalResult<std::remove_cv_t<Result>>::value,
            "the callback must return its result itself; an optional here "
            "would be wrapped a second time");
        auto tenant = TenantForRequest(service_, tenant_id);
        if (tenant == nullptr) {
            return std::nullopt;
        }
        auto entry = tenant->Get(key);
        if (entry == nullptr) {
            return std::nullopt;
        }
        return entry->WithSharedAccess(
            [&](const ObjectMetadata& metadata,
                const ObjectEntry::State& state) -> std::optional<Result> {
                if (state.is_torn_down || !metadata.IsValid()) {
                    return std::nullopt;
                }
                return std::optional<Result>{
                    std::forward<Fn>(fn)(*tenant, entry, metadata, state)};
            });
    }

    // The same under the entry's own write lock, for a test that stages state a
    // production path would have produced. `fn` may mutate the object and the
    // state, and the same readability predicate gates it.
    template <typename Fn>
    [[nodiscard]] auto WithPublishedObjectForWrite(const TenantId& tenant_id,
                                                   const std::string& key,
                                                   Fn&& fn)
        -> std::optional<std::invoke_result_t<
            Fn, metadata::Tenant&, const std::shared_ptr<ObjectEntry>&,
            ObjectMetadata&, ObjectEntry::State&>> {
        using Result =
            std::invoke_result_t<Fn, metadata::Tenant&,
                                 const std::shared_ptr<ObjectEntry>&,
                                 ObjectMetadata&, ObjectEntry::State&>;
        static_assert(
            !std::is_void_v<Result>,
            "this helper reports absence through its own optional, so "
            "the callback must return a result");
        static_assert(!std::is_reference_v<Result>,
                      "the result is carried by value: nothing a callback "
                      "returns may outlive the lock it ran under");
        static_assert(
            !detail::IsOptionalResult<std::remove_cv_t<Result>>::value,
            "the callback must return its result itself; an optional here "
            "would be wrapped a second time");
        auto tenant = TenantForRequest(service_, tenant_id);
        if (tenant == nullptr) {
            return std::nullopt;
        }
        auto entry = tenant->Get(key);
        if (entry == nullptr) {
            return std::nullopt;
        }
        return entry->WithExclusiveAccess(
            [&](ObjectMetadata& metadata,
                ObjectEntry::State& state) -> std::optional<Result> {
                if (state.is_torn_down || !metadata.IsValid()) {
                    return std::nullopt;
                }
                return std::optional<Result>{
                    std::forward<Fn>(fn)(*tenant, entry, metadata, state)};
            });
    }

    // Runs `fn(entry, metadata, state)` on the object `tenant_id`'s route
    // publishes for `key`, under the entry's own shared lock, with the tenant
    // taken as given and the envelope handed over exactly as stored: no
    // readability predicate, so a test sees state a request path would refuse.
    template <typename Fn>
    [[nodiscard]] auto WithStoredObjectForRead(const TenantId& tenant_id,
                                               const std::string& key,
                                               Fn&& fn) const
        -> std::optional<std::invoke_result_t<
            Fn, const std::shared_ptr<ObjectEntry>&, const ObjectMetadata&,
            const ObjectEntry::State&>> {
        using Result =
            std::invoke_result_t<Fn, const std::shared_ptr<ObjectEntry>&,
                                 const ObjectMetadata&,
                                 const ObjectEntry::State&>;
        static_assert(
            !std::is_void_v<Result>,
            "this helper reports absence through its own optional, so "
            "the callback must return a result");
        static_assert(!std::is_reference_v<Result>,
                      "the result is carried by value: nothing a callback "
                      "returns may outlive the lock it ran under");
        static_assert(
            !detail::IsOptionalResult<std::remove_cv_t<Result>>::value,
            "the callback must return its result itself; an optional here "
            "would be wrapped a second time");
        auto tenant = service_.tenants_.Lookup(tenant_id);
        if (tenant == nullptr) {
            return std::nullopt;
        }
        auto entry = tenant->Get(key);
        if (entry == nullptr) {
            return std::nullopt;
        }
        return entry->WithSharedAccess(
            [&](const ObjectMetadata& metadata,
                const ObjectEntry::State& state) -> std::optional<Result> {
                return std::optional<Result>{
                    std::forward<Fn>(fn)(entry, metadata, state)};
            });
    }

   private:
    // The tenant `tenant_id` names, resolved the way the service resolves a
    // request tenant, or null when no tenant is registered for it.
    static std::shared_ptr<metadata::Tenant> TenantForRequest(
        MasterService& service, const TenantId& tenant_id) {
        return service.tenants_.Lookup(
            service.ResolveRequestTenantId(tenant_id));
    }

    MasterService& service_;
};

}  // namespace mooncake::test

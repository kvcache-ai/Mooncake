#include "master_service.h"
#include "master_service/master_service_test_peer.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <future>
#include <fstream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>
#include <unistd.h>

#include "allocation_strategy.h"
#include "tenant_quota_policy_store.h"
#include "types.h"

namespace mooncake::test {

class BlockingTenantQuotaPolicyStore final : public TenantQuotaPolicyStore {
   public:
    explicit BlockingTenantQuotaPolicyStore(TenantQuotaPolicySnapshot snapshot)
        : snapshot_(std::move(snapshot)),
          allow_save_(allow_save_promise_.get_future()) {}

    std::future<void> SaveStarted() {
        return save_started_promise_.get_future();
    }

    void AllowSave() { allow_save_promise_.set_value(); }

    tl::expected<TenantQuotaPolicySnapshot, std::string> Load() override {
        return snapshot_;
    }

    tl::expected<void, std::string> Save(
        const TenantQuotaPolicySnapshot& snapshot) override {
        snapshot_ = snapshot;
        save_started_promise_.set_value();
        allow_save_.wait();
        return {};
    }

   private:
    TenantQuotaPolicySnapshot snapshot_;
    std::promise<void> save_started_promise_;
    std::promise<void> allow_save_promise_;
    std::future<void> allow_save_;
};

// An OpLog writer whose batches are durable on `write_batch`, used to drive the
// sweeps that register a durable finalizer. While it is holding, the finalizer
// the service committed is kept for the test to run when it chooses; the
// reservation, entry construction and commit above the writer are the
// production path.
class HoldingOpLogWriter final : public OrderedOpLogWriter {
   public:
    HoldingOpLogWriter()
        : OrderedOpLogWriter(OrderedOpLogWriterConfig{},
                             [](const OpLogBatchRecord&, const DurablePrefix&) {
                                 return ErrorCode::OK;
                             }) {}

    ~HoldingOpLogWriter() override { Stop(); }

    void HoldFinalizers() {
        std::lock_guard<std::mutex> lock(mutex_);
        holding_ = true;
    }

    tl::expected<PendingHandle, ErrorCode> Commit(
        Reservation&& reservation, OpLogEntry entry,
        DurableCallback callback) override {
        const std::string key = entry.object_key;
        bool hold = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            hold = holding_;
        }
        if (!hold) {
            return OrderedOpLogWriter::Commit(
                std::move(reservation), std::move(entry), std::move(callback));
        }
        return OrderedOpLogWriter::Commit(
            std::move(reservation), std::move(entry),
            [this, key,
             callback = std::move(callback)](const OpLogEntry& durable) {
                std::lock_guard<std::mutex> lock(mutex_);
                held_[key] = Held{durable, callback};
                cv_.notify_all();
            });
    }

    // The OpLog entry the finalizer committed for `key` was armed with. The
    // callback runs on the writer's own thread, so this waits for it.
    OpLogEntry WaitForHeldEntry(const std::string& key) {
        std::unique_lock<std::mutex> lock(mutex_);
        EXPECT_TRUE(cv_.wait_for(lock, std::chrono::seconds(5),
                                 [&] { return held_.count(key) != 0; }))
            << "no durable finalizer was committed for " << key;
        const auto it = held_.find(key);
        return it == held_.end() ? OpLogEntry{} : it->second.entry;
    }

    // Runs the finalizer the service committed for `key`, on the calling
    // thread.
    void RunHeldFinalizerFor(const std::string& key) {
        DurableCallback finalizer;
        OpLogEntry durable;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            EXPECT_TRUE(cv_.wait_for(lock, std::chrono::seconds(5),
                                     [&] { return held_.count(key) != 0; }))
                << "no durable finalizer was committed for " << key;
            auto it = held_.find(key);
            if (it == held_.end()) {
                return;
            }
            durable = it->second.entry;
            finalizer = it->second.finalizer;
            held_.erase(it);
        }
        finalizer(durable);
    }

   private:
    struct Held {
        OpLogEntry entry;
        DurableCallback finalizer;
    };

    std::mutex mutex_;
    std::condition_variable cv_;
    bool holding_{false};
    std::map<std::string, Held> held_;
};

#ifdef USE_NOF
class BlockingAllocationStrategy final : public AllocationStrategy {
   public:
    BlockingAllocationStrategy()
        : allow_allocation_(allow_allocation_promise_.get_future()) {}

    std::future<void> AllocationStarted() {
        return allocation_started_promise_.get_future();
    }

    void AllowAllocation() { allow_allocation_promise_.set_value(); }

    tl::expected<std::vector<Replica>, ErrorCode> Allocate(
        const AllocatorManager& allocator_manager, const size_t slice_length,
        const size_t replica_num,
        const std::vector<std::string>& preferred_segments,
        const std::set<std::string>& excluded_segments,
        const ReplicaType replica_type) override {
        BlockOnce();
        return delegate_.Allocate(allocator_manager, slice_length, replica_num,
                                  preferred_segments, excluded_segments,
                                  replica_type);
    }

    tl::expected<Replica, ErrorCode> AllocateFrom(
        const AllocatorManager& allocator_manager, const size_t slice_length,
        const std::string& segment_name) override {
        return delegate_.AllocateFrom(allocator_manager, slice_length,
                                      segment_name);
    }

   private:
    void BlockOnce() {
        bool expected = true;
        if (block_next_allocation_.compare_exchange_strong(expected, false)) {
            allocation_started_promise_.set_value();
            allow_allocation_.wait();
        }
    }

    RandomAllocationStrategy delegate_;
    std::atomic<bool> block_next_allocation_{true};
    std::promise<void> allocation_started_promise_;
    std::promise<void> allow_allocation_promise_;
    std::future<void> allow_allocation_;
};
#endif

class MasterServiceTenantQuotaTest : public ::testing::Test {
   protected:
    static constexpr size_t kSegmentBase = 0x500000000;

    std::string WritePolicyFile(
        const std::map<TenantId, uint64_t>& tenant_quotas) {
        TenantQuotaPolicySnapshot snapshot;
        for (const auto& [tenant_id, quota] : tenant_quotas) {
            snapshot.tenant_quotas.emplace(tenant_id.value(), quota);
        }
        auto path =
            std::filesystem::temp_directory_path() /
            ("mooncake_tenant_quota_test_" + std::to_string(::getpid()) + "_" +
             std::to_string(next_policy_file_++) + ".yaml");
        std::ofstream out(path);
        out << FormatTenantQuotaPolicyYaml(snapshot);
        out.close();
        policy_files_.push_back(path.string());
        return path.string();
    }

    MasterServiceConfig MakeConfig(
        const std::map<TenantId, uint64_t>& tenant_quotas,
        bool enable_multi_tenants = true) {
        auto builder = MasterServiceConfig::builder().set_enable_multi_tenants(
            enable_multi_tenants);
        if (enable_multi_tenants) {
            builder.set_tenant_quota_connector_type("file")
                .set_tenant_quota_connector_uri(WritePolicyFile(tenant_quotas));
        }
        return builder.build();
    }

    // Config for the tenant-scoped eviction watermark. Two settings here are
    // load-bearing rather than incidental:
    //   default_kv_lease_ttl(0) -- EvictTenantMemoryForQuota skips any object
    //     whose lease is still live, so with the 10 s default the pass under
    //     test would be a no-op and the assertions would pass for the wrong
    //     reason.
    //   eviction_high_watermark_ratio(1.0) -- keeps the POOL-wide evictor out
    //     of the way. The whole point of the tenant watermark is that it fires
    //     while the pool is nowhere near its own, so the test must not let the
    //     pool-level path account for the bytes freed.
    // A nullopt ratio leaves the builder default in place, which is how the
    // "on out of the box" behaviour is exercised.
    MasterServiceConfig MakeTenantWatermarkConfig(
        const std::map<TenantId, uint64_t>& tenant_quotas,
        std::optional<double> tenant_high_watermark_ratio,
        double eviction_ratio = 0.05) {
        auto builder =
            MasterServiceConfig::builder()
                .set_enable_multi_tenants(true)
                .set_tenant_quota_connector_type("file")
                .set_tenant_quota_connector_uri(WritePolicyFile(tenant_quotas))
                .set_default_kv_lease_ttl(0)
                .set_eviction_ratio(eviction_ratio)
                .set_eviction_high_watermark_ratio(1.0);
        if (tenant_high_watermark_ratio.has_value()) {
            builder.set_tenant_eviction_high_watermark_ratio(
                *tenant_high_watermark_ratio);
        }
        return builder.build();
    }

    UUID MountSegment(MasterService& service, size_t size = 4096,
                      std::string name = "quota_segment") {
        Segment segment;
        segment.id = generate_uuid();
        segment.name = std::move(name);
        segment.base = kSegmentBase + next_segment_offset_;
        segment.size = size;
        segment.te_endpoint = segment.name;
        next_segment_offset_ += size + 4096;

        UUID client_id = generate_uuid();
        auto result = service.MountSegment(segment, client_id);
        EXPECT_TRUE(result.has_value()) << toString(result.error());
        return client_id;
    }

#ifdef USE_NOF
    UUID MountNoFSegment(MasterService& service, size_t size = 4096,
                         std::string name = "quota_nof_segment") {
        NoFSegment segment;
        segment.id = generate_uuid();
        segment.name = std::move(name);
        segment.base = kSegmentBase + next_segment_offset_;
        segment.size = size;
        segment.te_endpoint = segment.name;
        next_segment_offset_ += size + 4096;

        UUID client_id = generate_uuid();
        auto result = service.MountNoFSegment(segment, client_id);
        EXPECT_TRUE(result.has_value()) << toString(result.error());
        return client_id;
    }
#endif

    ReplicateConfig MemoryConfig() {
        ReplicateConfig config;
        config.replica_num = 1;
        return config;
    }

    void PutComplete(MasterService& service, const UUID& client_id,
                     const std::string& key, const TenantId& tenant_id,
                     uint64_t size) {
        auto start =
            service.PutStart(client_id, key, tenant_id, size, MemoryConfig());
        ASSERT_TRUE(start.has_value()) << toString(start.error());
        auto end =
            service.PutEnd(client_id, key, tenant_id, ReplicaType::MEMORY);
        ASSERT_TRUE(end.has_value()) << toString(end.error());
    }

    // MasterService befriends this fixture, but TEST_F bodies are a derived
    // class and friendship does not inherit -- so private access has to go
    // through a fixture method, as it does for the helpers around this one.
    void RunTenantEvictionPass(MasterService& service) {
        MasterServiceTestPeer(service).RunTenantEvictForTesting();
    }

    TenantQuotaSnapshot Snapshot(MasterService& service,
                                 const TenantId& tenant_id) {
        auto snapshot = service.GetTenantQuotaSnapshot(tenant_id);
        EXPECT_TRUE(snapshot.has_value());
        return *snapshot;
    }

    void ReloadTenantQuotaPolicyFromStore(MasterService& service) {
        MasterServiceTestPeer(service)
            .LoadTenantQuotaPoliciesFromStoreOrThrow();
        MasterServiceTestPeer(service).RebuildTenantQuotaUsageFromMetadata();
    }

    void ReplaceTenantQuotaPolicyStore(
        MasterService& service, std::unique_ptr<TenantQuotaPolicyStore> store) {
        MasterServiceTestPeer::TenantQuotaPolicyStore(service) =
            std::move(store);
    }

    int64_t LocalDiskUsedBytes(MasterService& service, const UUID& client_id) {
        auto usage =
            MasterServiceTestPeer::LocalSsdManager(service).GetUsage(client_id);
        EXPECT_TRUE(usage.has_value());
        if (!usage) {
            return -1;
        }
        return usage->used_bytes;
    }

#ifdef USE_NOF
    void ReplaceAllocationStrategy(
        MasterService& service, std::shared_ptr<AllocationStrategy> strategy) {
        MasterServiceTestPeer::AllocationStrategy(service) =
            std::move(strategy);
    }
#endif

    tl::expected<void, ErrorCode> ChargeTenantQuotaForTest(
        MasterService& service, const TenantId& tenant_id, uint64_t bytes) {
        return MasterServiceTestPeer(service).ChargeTenantQuota(
            MasterServiceTestPeer::TenantQuotaTable(service)
                .GetOrCreateTenantHandle(tenant_id),
            bytes);
    }

    // The tenant for one tenant id, created through the registry's factory on
    // first use, which binds its quota account.
    std::shared_ptr<metadata::Tenant> GetOrCreateTenantHandleForTest(
        MasterService& service, const TenantId& tenant_id) {
        return MasterServiceTestPeer(service).GetOrCreateTenantHandle(
            tenant_id);
    }

    // The one quota account bound to that tenant.
    TenantQuotaHandle GetBoundTenantQuotaHandleForTest(
        MasterService& service, const TenantId& tenant_id) {
        auto tenant =
            MasterServiceTestPeer(service).GetOrCreateTenantHandle(tenant_id);
        if (tenant == nullptr) {
            return nullptr;
        }
        return MasterServiceTestPeer(service).GetBoundTenantQuotaHandle(
            *tenant);
    }

    tl::expected<void, ErrorCode> ChargeBoundTenantQuotaForTest(
        MasterService& service, TenantQuotaHandle account, uint64_t bytes) {
        return MasterServiceTestPeer(service).ChargeTenantQuota(account, bytes);
    }

    void ReleaseBoundTenantQuotaForTest(MasterService& service,
                                        TenantQuotaHandle account,
                                        uint64_t bytes) {
        MasterServiceTestPeer(service).ReleaseTenantQuota(account, bytes);
    }

    // Sweeps one tenant: its objects are its whole route, so no key is named.
    void DiscardExpiredProcessingForTest(MasterService& service,
                                         const TenantId& tenant_id) {
        auto tenant =
            MasterServiceTestPeer(service).GetOrCreateTenantHandle(tenant_id);
        ASSERT_NE(tenant, nullptr);
        MasterServiceTestPeer(service).DiscardExpiredProcessingReplicas(
            *tenant, std::chrono::system_clock::time_point::max());
    }

    // Runs the durable processing cleanup the sweep arms for this publication's
    // current write round, with the replica ids that sweep records.
    void FinalizeExpiredProcessingForTest(MasterService& service,
                                          const TenantId& tenant_id,
                                          const std::string& key) {
        auto object_entry = MasterServiceTestPeer::FindObject(
            service, MasterServiceTestPeer::ObjectIdentity{tenant_id, key});
        ASSERT_NE(object_entry, nullptr);
        OpLogEntry durable;
        durable.tenant_id = tenant_id.value();
        durable.object_key = key;
        MasterServiceTestPeer(service)
            .FinalizeExpiredProcessingReplicasAfterDurable(
                object_entry, durable, ProcessingReplicaIdsOf(object_entry),
                std::chrono::system_clock::now());
    }

    void FinalizeRemovedMemoryReplicasForTest(MasterService& service,
                                              const TenantId& tenant_id,
                                              const std::string& key) {
        std::vector<ReplicaID> removed_ids;
        const auto visited =
            MasterServiceTestPeer(service).WithPublishedObjectForWrite(
                tenant_id, key,
                [&removed_ids](metadata::Tenant&,
                               const std::shared_ptr<ObjectEntry>&,
                               ObjectMetadata& metadata, ObjectEntry::State&) {
                    metadata.VisitReplicas(
                        &Replica::fn_is_memory_replica,
                        [&removed_ids](Replica& replica) {
                            removed_ids.push_back(replica.id());
                            replica.mark_removed();
                        });
                    return true;
                });
        ASSERT_TRUE(visited.has_value());
        ASSERT_FALSE(removed_ids.empty());

        OpLogEntry durable_entry;
        durable_entry.tenant_id = tenant_id.value();
        durable_entry.object_key = key;
        // The durable cleanup names the publication it was armed for, so the
        // test hands over the one the route publishes now.
        auto object_entry = MasterServiceTestPeer::FindObject(
            service, MasterServiceTestPeer::ObjectIdentity{tenant_id, key});
        ASSERT_NE(object_entry, nullptr);
        MasterServiceTestPeer(service).FinalizeRemovedReplicasAfterDurable(
            object_entry, durable_entry, removed_ids,
            MasterServiceTestPeer::QuotaEraseMode::kFull);
    }

    void AddCompletedDiskReplica(MasterService& service, const UUID& client_id,
                                 const std::string& key,
                                 const TenantId& tenant_id, uint64_t size) {
        Replica disk_replica(client_id, size, "disk-endpoint",
                             ReplicaStatus::COMPLETE);
        auto result =
            service.AddReplica(client_id, key, tenant_id, disk_replica);
        ASSERT_TRUE(result.has_value()) << toString(result.error());
    }

    // --- The publication a durable cleanup was armed for ---------------------
    //
    // A durable finalizer is armed for one publication and runs later, on the
    // OpLog writer's thread, after the key may have been removed and published
    // again. Each test in this section stages two publications of one key, one
    // on a segment each: `superseded_`, which the cleanup was armed for and
    // which has already left the route, and `replacement_`, which the route
    // publishes now. Each owns a soft pin, a promotion candidate, a
    // dynamic-replication lease and a replication task of its own, so a stale
    // cleanup that acts by key instead of by publication shows up in all of
    // them.

    struct StagedPublication {
        UUID client;
        std::string segment;
        UUID proposal;
        std::chrono::system_clock::time_point soft_pin_deadline;
        ReplicationTask task;
        std::shared_ptr<ObjectEntry> entry;
    };

    StagedPublication superseded_;
    StagedPublication replacement_;
    OpLogEntry durable_entry_;

    // The promotion machinery the staging needs: promotion_on_hit binds only
    // with offload enabled, and a zero pool watermark sends every admission
    // down the watermark-rejection branch that records the retry candidate.
    MasterServiceConfig MakeRecreateConfig(const TenantId& tenant,
                                           bool enable_oplog = false) {
        MasterServiceConfig config = MakeConfig({{tenant, 4096}});
        config.enable_offload = true;
        config.promotion_on_hit = true;
        config.promotion_admission_threshold = 1;
        config.eviction_high_watermark_ratio = 0.0;
        // The processing sweep commits a durable cleanup only with the OpLog
        // writer on, which the service enables for HA plus oplog.
        config.enable_ha = enable_oplog;
        config.enable_oplog = enable_oplog;
        return config;
    }

    // An expired write round on `key`: a PROCESSING memory target plus a
    // completed disk replica, which is what makes the processing sweep find
    // work to do and commit a durable cleanup for it.
    std::shared_ptr<ObjectEntry> StageExpiredProcessingRound(
        MasterService& service, const UUID& client, const TenantId& tenant,
        const std::string& key, uint64_t object_size) {
        auto start =
            service.PutStart(client, key, tenant, object_size, MemoryConfig());
        EXPECT_TRUE(start.has_value()) << toString(start.error());
        AddCompletedDiskReplica(service, client, key, tenant, object_size);
        return MasterServiceTestPeer::FindObject(
            service, MasterServiceTestPeer::ObjectIdentity{tenant, key});
    }

    // Leaves nothing running that would move the state under test: the
    // eviction pass retries promotion candidates and its retry loop unindexes a
    // candidate whose object still carries a memory replica, and the
    // invalid-handle sweep erases an object whose last replica has a dead
    // handle.
    void QuiesceRecreateBackgroundWorkers(MasterService& service) {
        MasterServiceTestPeer::EvictionRunning(service) = false;
        if (MasterServiceTestPeer::EvictionThread(service).joinable()) {
            MasterServiceTestPeer::EvictionThread(service).join();
        }
        MasterServiceTestPeer::ReplicaCleanupWorker(service).Stop();
    }

    // Publishes one complete MEMORY replica of `key` on `segment`, which is the
    // only segment its replicas can land on.
    void PublishOnSegment(MasterService& service, const UUID& client,
                          const std::string& segment, const TenantId& tenant,
                          const std::string& key, uint64_t size) {
        ReplicateConfig config = MemoryConfig();
        config.preferred_segment = segment;
        auto start = service.PutStart(client, key, tenant, size, config);
        ASSERT_TRUE(start.has_value()) << toString(start.error());
        auto end = service.PutEnd(client, key, tenant, ReplicaType::MEMORY);
        ASSERT_TRUE(end.has_value()) << toString(end.error());
    }

    // The soft pin a soft-pinning write leaves: committed on the publication
    // and registered in the deadline index together.
    void CommitSoftPin(MasterService& service, const TenantId& tenant,
                       const std::string& key,
                       const std::shared_ptr<ObjectEntry>& entry,
                       const std::chrono::system_clock::time_point& deadline) {
        entry->WithExclusiveAccess(
            [&](ObjectMetadata& metadata, ObjectEntry::State&) {
                SpinLocker locker(&metadata.lock);
                metadata.soft_pin_timeout = deadline;
            });
        MasterServiceTestPeer::SoftPinDeadlineIndex(service).Upsert(
            tenant.MakeScopedKey(key), deadline);
    }

    // The dynamic-replication lease one publication owns, keyed by its own
    // proposal id. An hour out: no sweep can retract it during the test.
    ReplicaActionLease MakeReplicaActionLease(const TenantId& tenant,
                                              const std::string& key,
                                              const UUID& proposal_id) {
        ReplicaActionLease lease;
        lease.proposal_id = proposal_id;
        lease.lease_id = proposal_id;
        lease.tenant_id = tenant.value();
        lease.key = key;
        lease.expire_at_ms_epoch =
            MasterServiceTestPeer::DynamicReplicationNowMs() + 3600000;
        return lease;
    }

    // Publishes one publication of `key` on its own segment and stages the
    // state it owns, so only the publication whose segment goes away is
    // invalidated. `cleanup_pending` and `version_epoch` are the replication
    // task records a durable cleanup of that publication is armed with, so the
    // two publications must differ in them.
    void StagePublication(MasterService& service, const TenantId& tenant,
                          const std::string& key, uint64_t size,
                          const std::string& segment, bool cleanup_pending,
                          uint64_t version_epoch, StagedPublication& staged) {
        MasterServiceTestPeer peer(service);
        staged.segment = segment;
        staged.client = MountSegment(service, /*size=*/4096, segment);
        PublishOnSegment(service, staged.client, segment, tenant, key, size);
        staged.entry = MasterServiceTestPeer::FindObject(
            service, MasterServiceTestPeer::ObjectIdentity{tenant, key});
        ASSERT_NE(staged.entry, nullptr);
        staged.proposal = generate_uuid();
        staged.soft_pin_deadline =
            std::chrono::system_clock::now() + std::chrono::seconds(600);

        // The records a replication in flight leaves behind: this publication's
        // own replica as the source, and the target id its task had just
        // allocated.
        const auto replica_ids = MemoryReplicaIdsOf(staged.entry);
        ASSERT_EQ(replica_ids.size(), 1u);
        staged.task =
            ReplicationTask{.client_id = staged.client,
                            .start_time = std::chrono::system_clock::now(),
                            .type = ReplicationTask::Type::COPY,
                            .source_id = replica_ids.front(),
                            .replica_ids = {replica_ids.front() + 1},
                            .pending_quota_charge_bytes = 0,
                            .dynamic_replication_lease_id = staged.proposal,
                            .dynamic_replication_version_epoch = version_epoch,
                            .durable_cleanup_pending = cleanup_pending};

        CommitSoftPin(service, tenant, key, staged.entry,
                      staged.soft_pin_deadline);
        using Result = MasterServiceTestPeer::PromotionQueueResult;
        const MasterServiceTestPeer::ObjectIdentity identity{tenant, key};
        const auto queued =
            peer.TryPushPromotionQueue(identity, /*record_candidate=*/true);
        ASSERT_EQ(queued, Result::kWatermarkRejected);
        peer.PutDynamicReplicationLeaseForTesting(
            tenant, staged.entry, staged.proposal,
            MakeReplicaActionLease(tenant, key, staged.proposal));
        SetReplicationTaskForTest(service, tenant, key, staged.task);

        durable_entry_.tenant_id = tenant.value();
        durable_entry_.object_key = key;
    }

    void StageSupersededPublication(MasterService& service,
                                    const TenantId& tenant,
                                    const std::string& key, uint64_t size) {
        StagePublication(service, tenant, key, size, "superseded_segment",
                         /*cleanup_pending=*/true, /*version_epoch=*/1,
                         superseded_);
    }

    void StageReplacementPublication(MasterService& service,
                                     const TenantId& tenant,
                                     const std::string& key, uint64_t size) {
        StagePublication(service, tenant, key, size, "replacement_segment",
                         /*cleanup_pending=*/false, /*version_epoch=*/2,
                         replacement_);
    }

    // The one mounted segment named `name`, or an empty UUID.
    UUID SegmentIdByName(MasterService& service, const std::string& name) {
        auto segment_access =
            MasterServiceTestPeer::SegmentManager(service).getSegmentAccess();
        std::vector<std::pair<Segment, UUID>> mounted_segments;
        EXPECT_EQ(segment_access.GetAllSegments(mounted_segments),
                  ErrorCode::OK);
        for (const auto& mounted : mounted_segments) {
            if (mounted.first.name == name) {
                return mounted.first.id;
            }
        }
        return UUID{};
    }

    // Invalidates the memory handles of `segment_name`'s replicas the way a
    // segment going away does. The public unmount path would sweep the objects
    // instead.
    void PrepareSegmentUnmount(MasterService& service,
                               const std::string& segment_name) {
        const UUID segment_id = SegmentIdByName(service, segment_name);
        ASSERT_NE(segment_id, UUID{});
        auto segment_access =
            MasterServiceTestPeer::SegmentManager(service).getSegmentAccess();
        size_t metrics_dec_capacity = 0;
        ASSERT_EQ(ErrorCode::OK, segment_access.PrepareUnmountSegment(
                                     segment_id, metrics_dec_capacity));
    }

    // Stages the replication task the publication now routed for `key` owns.
    void SetReplicationTaskForTest(MasterService& service,
                                   const TenantId& tenant,
                                   const std::string& key,
                                   const ReplicationTask& task) {
        MasterServiceTestPeer peer(service);
        const auto staged = peer.WithPublishedObjectForWrite(
            tenant, key,
            [&task](metadata::Tenant&, const std::shared_ptr<ObjectEntry>&,
                    ObjectMetadata&, ObjectEntry::State& state) {
                state.replication_task = task;
                return true;
            });
        ASSERT_TRUE(staged.has_value());
    }

    // What one publication owns, as a stale cleanup must leave it: the state on
    // its own entry, the service records keyed by its key (the candidate index
    // and its count, the dynamic-replication lease) and the tenant charge.
    struct PublicationState {
        std::vector<ReplicaID> replica_ids;
        std::vector<std::string> kv_media;
        std::optional<std::chrono::system_clock::time_point> soft_pin;
        bool is_processing{false};
        uint64_t committed_quota{0};
        std::vector<uint64_t> candidate_fields;
        std::vector<uint64_t> task_fields;
        std::vector<std::string> candidate_keys;
        size_t candidate_count{0};
        uint64_t charged_bytes{0};
        UUID lease_proposal;
    };

    // The soft-pin deadline the index still holds for `key`, read through the
    // index's only reader: PopExpired consumes what it reports, so a test reads
    // this last. `due` is the deadline the registration is expected to hold.
    std::optional<std::chrono::system_clock::time_point>
    RegisteredSoftPinDeadline(
        MasterService& service, const TenantId& tenant, const std::string& key,
        const std::chrono::system_clock::time_point& due) {
        auto& index = MasterServiceTestPeer::SoftPinDeadlineIndex(service);
        const std::string scoped_key = tenant.MakeScopedKey(key);
        for (const auto& registration : index.PopExpired(due)) {
            if (registration.scoped_key == scoped_key) {
                return registration.deadline;
            }
        }
        return std::nullopt;
    }

    // The replica ids of one publication's memory replicas, read through its
    // own handle: a replica whose memory handle is dead is exactly the state
    // these tests stage, and a readability predicate would hide it.
    std::vector<ReplicaID> MemoryReplicaIdsOf(
        const std::shared_ptr<ObjectEntry>& entry) {
        return entry->WithSharedAccess(
            [](const ObjectMetadata& metadata, const ObjectEntry::State&) {
                std::vector<ReplicaID> ids;
                metadata.VisitReplicas(&Replica::fn_is_memory_replica,
                                       [&ids](const Replica& replica) {
                                           ids.push_back(replica.id());
                                       });
                std::sort(ids.begin(), ids.end());
                return ids;
            });
    }

    // The replica ids of one publication's processing replicas, in id order:
    // the write round in flight on that publication.
    std::vector<ReplicaID> ProcessingReplicaIdsOf(
        const std::shared_ptr<ObjectEntry>& entry) {
        return entry->WithSharedAccess(
            [](const ObjectMetadata& metadata, const ObjectEntry::State&) {
                std::vector<ReplicaID> ids;
                metadata.VisitReplicas(&Replica::fn_is_processing,
                                       [&ids](const Replica& replica) {
                                           ids.push_back(replica.id());
                                       });
                std::sort(ids.begin(), ids.end());
                return ids;
            });
    }

    bool IsProcessingOf(const std::shared_ptr<ObjectEntry>& entry) {
        return entry->WithSharedAccess(
            [](const ObjectMetadata&, const ObjectEntry::State& state) {
                return state.is_processing;
            });
    }

    // The media one publication announces, the bytes its quota ledger holds,
    // the soft-pin deadline committed on it, and how many of its replicas carry
    // a dead memory handle.
    std::vector<std::string> KvMediaOf(
        const std::shared_ptr<ObjectEntry>& entry) {
        return entry->WithSharedAccess(
            [](const ObjectMetadata& metadata, const ObjectEntry::State&) {
                return MasterServiceTestPeer::KvMediaForMetadata(metadata);
            });
    }

    std::optional<std::chrono::system_clock::time_point> CommittedSoftPinOf(
        const std::shared_ptr<ObjectEntry>& entry) {
        return entry->WithSharedAccess(
            [](const ObjectMetadata& metadata, const ObjectEntry::State&) {
                return metadata.GetCommittedSoftPinTimeout();
            });
    }

    uint64_t CommittedQuotaOf(const std::shared_ptr<ObjectEntry>& entry) {
        return entry->WithSharedAccess(
            [](const ObjectMetadata& metadata, const ObjectEntry::State&) {
                return metadata.quota_ledger.CommittedBytes();
            });
    }

    size_t InvalidHandleCountOf(const std::shared_ptr<ObjectEntry>& entry) {
        return entry->WithSharedAccess(
            [](const ObjectMetadata& metadata, const ObjectEntry::State&) {
                return metadata.CountReplicas([](const Replica& replica) {
                    return replica.has_invalid_mem_handle();
                });
            });
    }

    // The scalar contents of one publication's promotion candidate, in field
    // order. The candidate record has no equality and its timestamps move, so a
    // test compares this projection instead.
    std::vector<uint64_t> PromotionCandidateFieldsOf(
        const std::shared_ptr<ObjectEntry>& entry) {
        return entry->WithSharedAccess(
            [](const ObjectMetadata&,
               const ObjectEntry::State& state) -> std::vector<uint64_t> {
                if (!state.promotion_candidate.has_value()) {
                    return {};
                }
                const auto& candidate = *state.promotion_candidate;
                return {candidate.sketch_score,
                        static_cast<uint64_t>(candidate.last_reason),
                        static_cast<uint64_t>(candidate.last_error),
                        candidate.retry_count, candidate.execution_failures};
            });
    }

    // The contents of one replication task, in field order, with its target
    // replica ids last. The task record has no equality either.
    static std::vector<uint64_t> ReplicationTaskFields(
        const ReplicationTask& task) {
        std::vector<uint64_t> fields{
            static_cast<uint64_t>(task.type),
            task.source_id,
            task.pending_quota_charge_bytes,
            task.dynamic_replication_lease_id.first,
            task.dynamic_replication_lease_id.second,
            task.dynamic_replication_version_epoch,
            task.durable_cleanup_pending ? 1u : 0u,
            task.client_id.first,
            task.client_id.second,
            static_cast<uint64_t>(task.start_time.time_since_epoch().count())};
        fields.insert(fields.end(), task.replica_ids.begin(),
                      task.replica_ids.end());
        return fields;
    }

    std::vector<uint64_t> ReplicationTaskFieldsOf(
        const std::shared_ptr<ObjectEntry>& entry) {
        return entry->WithSharedAccess(
            [](const ObjectMetadata&, const ObjectEntry::State& state) {
                return state.replication_task.has_value()
                           ? ReplicationTaskFields(*state.replication_task)
                           : std::vector<uint64_t>{};
            });
    }

    // Reads everything a stale cleanup must leave alone. `staged_task` is the
    // task the staging recorded and the lease is the one it filed, both checked
    // here, so no test asserts against a state its own staging never reached.
    PublicationState CapturePublicationState(
        MasterService& service, MasterServiceTestPeer& peer,
        const TenantId& tenant, const std::shared_ptr<ObjectEntry>& entry,
        const ReplicationTask& staged_task, const UUID& lease_proposal) {
        PublicationState state;
        state.replica_ids = MemoryReplicaIdsOf(entry);
        state.kv_media = KvMediaOf(entry);
        state.soft_pin = CommittedSoftPinOf(entry);
        state.is_processing = IsProcessingOf(entry);
        state.committed_quota = CommittedQuotaOf(entry);
        state.candidate_fields = PromotionCandidateFieldsOf(entry);
        state.task_fields = ReplicationTaskFieldsOf(entry);
        state.candidate_keys =
            MasterServiceTestPeer::PromotionCandidateKeys(service, tenant);
        state.candidate_count = peer.CountCandidatesForTesting(tenant);
        state.charged_bytes = Snapshot(service, tenant).charged_bytes;
        state.lease_proposal = lease_proposal;
        EXPECT_EQ(state.task_fields, ReplicationTaskFields(staged_task));
        EXPECT_TRUE(state.soft_pin.has_value());
        EXPECT_FALSE(state.candidate_fields.empty());
        EXPECT_EQ(state.candidate_keys,
                  (std::vector<std::string>{entry->key()}));
        EXPECT_EQ(state.candidate_count, 1u);
        EXPECT_TRUE(MasterServiceTestPeer::FindDynamicReplicationLease(
                        service, tenant, lease_proposal)
                        .has_value());
        return state;
    }

    // The publication the cleanup did not own keeps every record it holds.
    void ExpectPublicationUnchanged(MasterService& service,
                                    MasterServiceTestPeer& peer,
                                    const TenantId& tenant,
                                    const std::shared_ptr<ObjectEntry>& entry,
                                    const PublicationState& before) {
        EXPECT_EQ(
            MasterServiceTestPeer::FindObject(
                service,
                MasterServiceTestPeer::ObjectIdentity{tenant, entry->key()}),
            entry);
        EXPECT_EQ(MemoryReplicaIdsOf(entry), before.replica_ids);
        EXPECT_EQ(KvMediaOf(entry), before.kv_media);
        EXPECT_EQ(CommittedSoftPinOf(entry), before.soft_pin);
        EXPECT_EQ(IsProcessingOf(entry), before.is_processing);
        EXPECT_EQ(CommittedQuotaOf(entry), before.committed_quota);
        EXPECT_EQ(PromotionCandidateFieldsOf(entry), before.candidate_fields);
        EXPECT_EQ(ReplicationTaskFieldsOf(entry), before.task_fields);
        EXPECT_EQ(
            MasterServiceTestPeer::PromotionCandidateKeys(service, tenant),
            before.candidate_keys);
        EXPECT_EQ(peer.CountCandidatesForTesting(tenant),
                  before.candidate_count);
        EXPECT_TRUE(MasterServiceTestPeer::FindDynamicReplicationLease(
                        service, tenant, before.lease_proposal)
                        .has_value());
        EXPECT_EQ(Snapshot(service, tenant).charged_bytes,
                  before.charged_bytes);
    }

    // How the superseded publication leaves the route before the replacement is
    // published. `kEraseRemoved` marks its replicas REMOVED first, the state
    // FinalizeRemovedReplicasAfterDurable acts on.
    enum class SupersededExit { kErase, kEraseRemoved, kReleaseRouteSlot };

    // Publishes one key twice, one segment per publication: `superseded_`
    // leaves the route through `exit`, and the replacement is published on its
    // own segment with `invalidate_replacement_handle` deciding whether its
    // memory handle is dead. `removed_ids` receives the ids kEraseRemoved
    // marked. The state the replacement owns is checked here; a test that reads
    // it again captures what it needs.
    void StageRecreate(MasterService& service, MasterServiceTestPeer& peer,
                       const TenantId& tenant, const std::string& key,
                       uint64_t object_size, SupersededExit exit,
                       bool invalidate_replacement_handle,
                       std::vector<ReplicaID>* removed_ids = nullptr) {
        QuiesceRecreateBackgroundWorkers(service);
        StageSupersededPublication(service, tenant, key, object_size);

        if (exit == SupersededExit::kEraseRemoved) {
            ASSERT_NE(removed_ids, nullptr);
            // The replicas the superseded publication's own cleanup pops.
            const auto marked = peer.WithPublishedObjectForWrite(
                tenant, key,
                [removed_ids](metadata::Tenant&,
                              const std::shared_ptr<ObjectEntry>&,
                              ObjectMetadata& metadata, ObjectEntry::State&) {
                    metadata.VisitReplicas(
                        &Replica::fn_is_memory_replica,
                        [removed_ids](Replica& replica) {
                            removed_ids->push_back(replica.id());
                            replica.mark_removed();
                        });
                    return true;
                });
            ASSERT_TRUE(marked.has_value());
            ASSERT_EQ(removed_ids->size(), 1u);
        }
        if (exit == SupersededExit::kReleaseRouteSlot) {
            // The erase cleanup's own gate is the route identity and its
            // teardown claim would make a stale call a no-op, so the superseded
            // publication leaves the route without one: the state a reload
            // rebuilding the tenant's route leaves behind.
            ASSERT_TRUE(GetOrCreateTenantHandleForTest(service, tenant)
                            ->RemoveObject(superseded_.entry));
        } else {
            ASSERT_TRUE(peer.EraseObjectForTesting(tenant, key));
        }

        StageReplacementPublication(service, tenant, key, object_size);
        ASSERT_NE(replacement_.entry, superseded_.entry);
        if (invalidate_replacement_handle) {
            // The replacement's handle is dead, the state the read-write
            // accessor the older cleanups resolved through drops before its
            // callback.
            PrepareSegmentUnmount(service, replacement_.segment);
            ASSERT_EQ(InvalidHandleCountOf(replacement_.entry), 1u);
        }
        // Checks that the replacement really owns the soft pin, the promotion
        // candidate, the candidate index and its lease.
        (void)CapturePublicationState(service, peer, tenant, replacement_.entry,
                                      replacement_.task, replacement_.proposal);
    }

    void ExpectDiskOnlyObjectAndChargedBytes(MasterService& service,
                                             const TenantId& tenant_id,
                                             const std::string& key,
                                             uint64_t charged_bytes) {
        EXPECT_EQ(Snapshot(service, tenant_id).charged_bytes, charged_bytes);
        auto replicas = service.GetReplicaList(key, tenant_id);
        ASSERT_TRUE(replicas.has_value()) << toString(replicas.error());
        ASSERT_EQ(replicas->replicas.size(), 1);
        EXPECT_TRUE(replicas->replicas.front().is_local_disk_replica());
    }

    std::unique_lock<std::shared_mutex> LockSnapshotForTest(
        MasterService& service) {
        return std::unique_lock<std::shared_mutex>(
            MasterServiceTestPeer::SnapshotMutex(service));
    }

    std::unique_lock<std::mutex> LockTenantQuotaRecomputeForTest(
        MasterService& service) {
        return std::unique_lock<std::mutex>(
            MasterServiceTestPeer::TenantQuotaRecomputeMutex(service));
    }

    std::unique_lock<std::mutex> LockTenantQuotaPolicyForTest(
        MasterService& service) {
        return std::unique_lock<std::mutex>(
            MasterServiceTestPeer::TenantQuotaPolicyMutex(service));
    }

    ErrorCode MountSegmentWithoutQuotaRecomputeForTest(MasterService& service,
                                                       size_t size,
                                                       std::string name) {
        Segment segment;
        segment.id = generate_uuid();
        segment.name = std::move(name);
        segment.base = kSegmentBase + next_segment_offset_;
        segment.size = size;
        segment.te_endpoint = segment.name;
        next_segment_offset_ += size + 4096;

        auto segment_access =
            MasterServiceTestPeer::SegmentManager(service).getSegmentAccess();
        return segment_access.MountSegment(
            segment, generate_uuid(),
            std::make_shared<ClientLivenessRecord>(
                ClientLivenessRecord::Clock::now()));
    }

    void RecomputeTenantEffectiveQuotasForTest(MasterService& service) {
        MasterServiceTestPeer(service).RecomputeTenantEffectiveQuotas();
    }

    bool WaitForTenantQuotaPolicyMutexContention(MasterService& service) {
        for (int i = 0; i < 500; ++i) {
            if (!MasterServiceTestPeer::TenantQuotaPolicyMutex(service)
                     .try_lock()) {
                return true;
            }
            MasterServiceTestPeer::TenantQuotaPolicyMutex(service).unlock();
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return false;
    }

    void TearDown() override {
        for (const auto& path : policy_files_) {
            std::error_code ec;
            std::filesystem::remove(path, ec);
        }
    }

    size_t next_segment_offset_ = 0;
    size_t next_policy_file_ = 0;
    std::vector<std::string> policy_files_;
};

TEST_F(MasterServiceTenantQuotaTest,
       SingleTenantModeCollapsesTenantsAndDisablesQuota) {
    MasterService service(MakeConfig({}, /*enable_multi_tenants=*/false));
    UUID client_id = MountSegment(service, /*size=*/1024);

    PutComplete(service, client_id, "shared-key", TenantId("tenant-a"), 800);

    EXPECT_TRUE(service.ExistKey("shared-key", TenantId("tenant-b")).value());
    auto duplicate = service.PutStart(client_id, "shared-key",
                                      TenantId("tenant-b"), 1, MemoryConfig());
    ASSERT_FALSE(duplicate.has_value());
    EXPECT_EQ(duplicate.error(), ErrorCode::OBJECT_ALREADY_EXISTS);
    EXPECT_TRUE(service
                    .Remove("shared-key", TenantId("tenant-b"),
                            /*force=*/true)
                    .has_value());
    EXPECT_FALSE(
        service.GetTenantQuotaSnapshot(TenantId("tenant-a")).has_value());
}

TEST_F(MasterServiceTenantQuotaTest,
       MultiTenantModeRejectsUnregisteredAndImplicitDefaultWrites) {
    MasterService service(MakeConfig({{TenantId("tenant-a"), 1000}}));
    UUID client_id = MountSegment(service);

    auto missing = service.PutStart(client_id, "missing", TenantId("tenant-b"),
                                    10, MemoryConfig());
    ASSERT_FALSE(missing.has_value());
    EXPECT_EQ(missing.error(), ErrorCode::TENANT_NOT_REGISTERED);

    auto implicit_default = service.PutStart(
        client_id, "default-key", TenantId::Default(), 10, MemoryConfig());
    ASSERT_FALSE(implicit_default.has_value());
    EXPECT_EQ(implicit_default.error(), ErrorCode::TENANT_NOT_REGISTERED);

    const std::string control_tenant("tenant\0bad", 10);
    EXPECT_FALSE(TenantId(control_tenant).IsValid());

    auto register_default =
        service.UpsertTenantQuotaPolicy(TenantId::Default(), 100);
    ASSERT_TRUE(register_default.has_value())
        << toString(register_default.error());
    PutComplete(service, client_id, "registered-default", TenantId::Default(),
                10);

    PutComplete(service, client_id, "ok", TenantId("tenant-a"), 10);
}

TEST_F(MasterServiceTenantQuotaTest,
       GetOrCreateTenantHandleIsIdempotentForOneTenantId) {
    const TenantId tenant_id("tenant-a");
    MasterService service(MakeConfig({{tenant_id, 1000}}));
    MountSegment(service);

    auto first_tenant = GetOrCreateTenantHandleForTest(service, tenant_id);
    auto second_tenant = GetOrCreateTenantHandleForTest(service, tenant_id);

    ASSERT_NE(first_tenant, nullptr);
    // One tenant id names one tenant, so a second lookup yields the same one.
    EXPECT_EQ(first_tenant, second_tenant);

    auto* first_handle = GetBoundTenantQuotaHandleForTest(service, tenant_id);
    auto* second_handle = GetBoundTenantQuotaHandleForTest(service, tenant_id);

    ASSERT_NE(first_handle, nullptr);
    // ...and that tenant owns exactly one bound account.
    EXPECT_EQ(first_handle, second_handle);

    auto charge = ChargeBoundTenantQuotaForTest(service, first_handle, 128);
    ASSERT_TRUE(charge.has_value()) << toString(charge.error());
    EXPECT_EQ(Snapshot(service, tenant_id).charged_bytes, 128);

    ReleaseBoundTenantQuotaForTest(service, second_handle, 128);
    EXPECT_EQ(Snapshot(service, tenant_id).charged_bytes, 0);
}

TEST_F(MasterServiceTenantQuotaTest,
       ChargeRejectsMissingHandleWhenQuotaIsEnabled) {
    const TenantId tenant_id("tenant-a");
    MasterService service(MakeConfig({{tenant_id, 1000}}));
    MountSegment(service);

    auto charge = ChargeBoundTenantQuotaForTest(service, nullptr, 1);
    ASSERT_FALSE(charge.has_value());
    EXPECT_EQ(charge.error(), ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(Snapshot(service, tenant_id).charged_bytes, 0);
}

TEST_F(MasterServiceTenantQuotaTest,
       MultiTenantModeRejectsUnregisteredOffloadSuccess) {
    MasterService service(MakeConfig({{TenantId("tenant-a"), 1000}}));
    UUID client_id = MountSegment(service);

    StorageObjectMetadata metadata;
    metadata.data_size = 128;
    metadata.transport_endpoint = "disk-endpoint";
    std::vector<OffloadTaskItem> tasks{
        OffloadTaskItem{.tenant_id = "tenant-b", .key = "ghost", .size = 128}};

    auto result = service.NotifyOffloadSuccess(client_id, tasks, {metadata});

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::TENANT_NOT_REGISTERED);
    auto missing = service.ExistKey("ghost", TenantId("tenant-b"));
    ASSERT_TRUE(missing.has_value()) << toString(missing.error());
    EXPECT_FALSE(missing.value());
}

TEST_F(MasterServiceTenantQuotaTest,
       MultiTenantModeAllowsRegisteredOffloadSuccess) {
    MasterService service(MakeConfig({{TenantId("tenant-a"), 1000}}));
    UUID client_id = MountSegment(service);

    StorageObjectMetadata metadata;
    metadata.data_size = 128;
    metadata.transport_endpoint = "disk-endpoint";
    std::vector<OffloadTaskItem> tasks{
        OffloadTaskItem{.tenant_id = "tenant-a", .key = "cold", .size = 128}};

    auto result = service.NotifyOffloadSuccess(client_id, tasks, {metadata});

    ASSERT_TRUE(result.has_value()) << toString(result.error());
    auto exists = service.ExistKey("cold", TenantId("tenant-a"));
    ASSERT_TRUE(exists.has_value()) << toString(exists.error());
    EXPECT_TRUE(exists.value());
    EXPECT_EQ(Snapshot(service, TenantId("tenant-a")).charged_bytes, 0);
}

TEST_F(MasterServiceTenantQuotaTest,
       ConnectorPolicyReloadKeepsLocalDiskOnlyOrphanAccessible) {
    const std::string initial_policy = WritePolicyFile(
        {{TenantId("tenant-a"), 1000}, {TenantId("tenant-b"), 1000}});
    auto config = MasterServiceConfig::builder()
                      .set_enable_multi_tenants(true)
                      .set_tenant_quota_connector_type("file")
                      .set_tenant_quota_connector_uri(initial_policy)
                      .build();
    MasterService service(config);
    UUID client_id = MountSegment(service);

    StorageObjectMetadata metadata;
    metadata.data_size = 128;
    metadata.transport_endpoint = "disk-endpoint";
    std::vector<OffloadTaskItem> tasks{
        OffloadTaskItem{.tenant_id = "tenant-b", .key = "cold", .size = 128}};
    ASSERT_TRUE(
        service.NotifyOffloadSuccess(client_id, tasks, {metadata}).has_value());

    {
        std::ofstream out(initial_policy);
        TenantQuotaPolicySnapshot replacement;
        replacement.tenant_quotas = {{"tenant-a", 1000}};
        out << FormatTenantQuotaPolicyYaml(replacement);
    }
    ReloadTenantQuotaPolicyFromStore(service);

    EXPECT_FALSE(
        service.GetTenantQuotaSnapshot(TenantId("tenant-b")).has_value());

    EXPECT_TRUE(service.Remove("cold", TenantId("tenant-b"), /*force=*/true)
                    .has_value());
    EXPECT_FALSE(
        service.GetTenantQuotaSnapshot(TenantId("tenant-b")).has_value());
}

TEST_F(MasterServiceTenantQuotaTest,
       NotifyOffloadSuccessCompletesExistingOrphanObject) {
    const std::string initial_policy = WritePolicyFile(
        {{TenantId("tenant-a"), 1000}, {TenantId("tenant-b"), 1000}});
    auto config = MasterServiceConfig::builder()
                      .set_enable_multi_tenants(true)
                      .set_enable_offload(true)
                      .set_tenant_quota_connector_type("file")
                      .set_tenant_quota_connector_uri(initial_policy)
                      .build();
    MasterService service(config);
    UUID client_id = MountSegment(service);
    ASSERT_TRUE(service.MountLocalDiskSegment(client_id, true).has_value());
    PutComplete(service, client_id, "warming", TenantId("tenant-b"), 128);

    {
        std::ofstream out(initial_policy);
        TenantQuotaPolicySnapshot replacement;
        replacement.tenant_quotas = {{"tenant-a", 1000}};
        out << FormatTenantQuotaPolicyYaml(replacement);
    }
    ReloadTenantQuotaPolicyFromStore(service);
    auto orphan = Snapshot(service, TenantId("tenant-b"));
    EXPECT_FALSE(orphan.has_explicit_policy);
    EXPECT_TRUE(orphan.admission_closed);
    EXPECT_EQ(orphan.charged_bytes, 128);

    StorageObjectMetadata metadata;
    metadata.data_size = 128;
    metadata.transport_endpoint = "disk-endpoint";
    std::vector<OffloadTaskItem> tasks{OffloadTaskItem{
        .tenant_id = "tenant-b", .key = "warming", .size = 128}};

    auto result = service.NotifyOffloadSuccess(client_id, tasks, {metadata});

    ASSERT_TRUE(result.has_value()) << toString(result.error());
    auto replicas = service.GetReplicaList("warming", TenantId("tenant-b"));
    ASSERT_TRUE(replicas.has_value()) << toString(replicas.error());
    EXPECT_TRUE(std::any_of(replicas->replicas.begin(),
                            replicas->replicas.end(),
                            [](const Replica::Descriptor& replica) {
                                return replica.is_local_disk_replica();
                            }));
}

TEST_F(MasterServiceTenantQuotaTest,
       NotifyOffloadSuccessRejectsOrphanObjectWithoutOffloadTask) {
    const std::string initial_policy = WritePolicyFile(
        {{TenantId("tenant-a"), 1000}, {TenantId("tenant-b"), 1000}});
    auto config = MasterServiceConfig::builder()
                      .set_enable_multi_tenants(true)
                      .set_tenant_quota_connector_type("file")
                      .set_tenant_quota_connector_uri(initial_policy)
                      .build();
    MasterService service(config);
    UUID client_id = MountSegment(service);
    PutComplete(service, client_id, "warming", TenantId("tenant-b"), 128);

    {
        std::ofstream out(initial_policy);
        TenantQuotaPolicySnapshot replacement;
        replacement.tenant_quotas = {{"tenant-a", 1000}};
        out << FormatTenantQuotaPolicyYaml(replacement);
    }
    ReloadTenantQuotaPolicyFromStore(service);
    EXPECT_FALSE(Snapshot(service, TenantId("tenant-b")).has_explicit_policy);

    StorageObjectMetadata metadata;
    metadata.data_size = 128;
    metadata.transport_endpoint = "disk-endpoint";
    std::vector<OffloadTaskItem> tasks{OffloadTaskItem{
        .tenant_id = "tenant-b", .key = "warming", .size = 128}};

    auto result = service.NotifyOffloadSuccess(client_id, tasks, {metadata});

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::TENANT_NOT_REGISTERED);
}

TEST_F(MasterServiceTenantQuotaTest,
       NotifyOffloadSuccessDoesNotCountAddReplicaUpdateAsNewDiskUsage) {
    const std::string policy = WritePolicyFile({{TenantId("tenant-a"), 1000}});
    auto config = MasterServiceConfig::builder()
                      .set_enable_multi_tenants(true)
                      .set_enable_offload(true)
                      .set_tenant_quota_connector_type("file")
                      .set_tenant_quota_connector_uri(policy)
                      .build();
    MasterService service(config);
    UUID client_a = MountSegment(service, 4096, "quota_segment_a");
    UUID client_b = MountSegment(service, 4096, "quota_segment_b");
    ASSERT_TRUE(service.MountLocalDiskSegment(client_a, true).has_value());
    ASSERT_TRUE(service.MountLocalDiskSegment(client_b, true).has_value());

    StorageObjectMetadata first_metadata;
    first_metadata.data_size = 128;
    first_metadata.transport_endpoint = "disk-endpoint-a";
    std::vector<OffloadTaskItem> tasks{
        OffloadTaskItem{.tenant_id = "tenant-a", .key = "cold", .size = 128}};
    ASSERT_TRUE(service.NotifyOffloadSuccess(client_a, tasks, {first_metadata})
                    .has_value());
    EXPECT_EQ(LocalDiskUsedBytes(service, client_a), 128);
    EXPECT_EQ(LocalDiskUsedBytes(service, client_b), 0);

    StorageObjectMetadata second_metadata;
    second_metadata.data_size = 128;
    second_metadata.transport_endpoint = "disk-endpoint-b";
    auto result =
        service.NotifyOffloadSuccess(client_b, tasks, {second_metadata});

    ASSERT_TRUE(result.has_value()) << toString(result.error());
    EXPECT_EQ(LocalDiskUsedBytes(service, client_a), 128);
    EXPECT_EQ(LocalDiskUsedBytes(service, client_b), 0);
}

TEST_F(MasterServiceTenantQuotaTest,
       RegisteredTenantQuotaAdmissionDoesNotCreateImplicitTenants) {
    MasterService service(MakeConfig({{TenantId("tenant-a"), 100}}));
    UUID client_id = MountSegment(service);

    auto hard_pinned = MemoryConfig();
    hard_pinned.with_hard_pin = true;
    auto first = service.PutStart(client_id, "key-a", TenantId("tenant-a"), 80,
                                  hard_pinned);
    ASSERT_TRUE(first.has_value()) << toString(first.error());
    EXPECT_EQ(Snapshot(service, TenantId("tenant-a")).charged_bytes, 80);
    ASSERT_TRUE(service
                    .PutEnd(client_id, "key-a", TenantId("tenant-a"),
                            ReplicaType::MEMORY)
                    .has_value());

    auto over = service.PutStart(client_id, "key-b", TenantId("tenant-a"), 30,
                                 MemoryConfig());

    ASSERT_FALSE(over.has_value());
    EXPECT_EQ(over.error(), ErrorCode::TENANT_QUOTA_EXCEEDED);
    EXPECT_EQ(Snapshot(service, TenantId("tenant-a")).charged_bytes, 80);
    EXPECT_FALSE(
        service.GetTenantQuotaSnapshot(TenantId("tenant-b")).has_value());
}

TEST_F(MasterServiceTenantQuotaTest, PutRevokeRefundsStartCharge) {
    MasterService service(MakeConfig({{TenantId("tenant-a"), 100}}));
    UUID client_id = MountSegment(service);

    auto start = service.PutStart(client_id, "key", TenantId("tenant-a"), 100,
                                  MemoryConfig());
    ASSERT_TRUE(start.has_value()) << toString(start.error());
    EXPECT_EQ(Snapshot(service, TenantId("tenant-a")).charged_bytes, 100);

    auto over = service.PutStart(client_id, "other", TenantId("tenant-a"), 1,
                                 MemoryConfig());
    ASSERT_FALSE(over.has_value());
    EXPECT_EQ(over.error(), ErrorCode::TENANT_QUOTA_EXCEEDED);

    ASSERT_TRUE(service
                    .PutRevoke(client_id, "key", TenantId("tenant-a"),
                               ReplicaType::MEMORY)
                    .has_value());
    EXPECT_EQ(Snapshot(service, TenantId("tenant-a")).charged_bytes, 0);
}

TEST_F(MasterServiceTenantQuotaTest,
       SizeChangingUpsertTransfersAndReleasesReplacementCharge) {
    const TenantId tenant_id("tenant-a");
    MasterService service(MakeConfig({{tenant_id, 1000}}));
    UUID client_id = MountSegment(service);
    PutComplete(service, client_id, "key", tenant_id, 100);

    auto upsert =
        service.UpsertStart(client_id, "key", tenant_id, 200, MemoryConfig());
    ASSERT_TRUE(upsert.has_value()) << toString(upsert.error());
    EXPECT_EQ(Snapshot(service, tenant_id).charged_bytes, 300);

    auto end =
        service.UpsertEnd(client_id, "key", tenant_id, ReplicaType::MEMORY);
    ASSERT_TRUE(end.has_value()) << toString(end.error());
    EXPECT_EQ(Snapshot(service, tenant_id).charged_bytes, 200);
}

TEST_F(MasterServiceTenantQuotaTest,
       SizeChangingUpsertFromDiskOnlyObjectChargesNewReplica) {
    const TenantId tenant_id("tenant-a");
    MasterService service(MakeConfig({{tenant_id, 1000}}));
    UUID client_id = MountSegment(service);

    StorageObjectMetadata metadata;
    metadata.data_size = 100;
    metadata.transport_endpoint = "disk-endpoint";
    std::vector<OffloadTaskItem> tasks{OffloadTaskItem{
        .tenant_id = tenant_id.value(), .key = "key", .size = 100}};
    ASSERT_TRUE(
        service.NotifyOffloadSuccess(client_id, tasks, {metadata}).has_value());
    ASSERT_EQ(Snapshot(service, tenant_id).charged_bytes, 0);

    auto upsert =
        service.UpsertStart(client_id, "key", tenant_id, 200, MemoryConfig());
    ASSERT_TRUE(upsert.has_value()) << toString(upsert.error());
    EXPECT_EQ(Snapshot(service, tenant_id).charged_bytes, 200);

    auto end =
        service.UpsertEnd(client_id, "key", tenant_id, ReplicaType::MEMORY);
    ASSERT_TRUE(end.has_value()) << toString(end.error());
    EXPECT_EQ(Snapshot(service, tenant_id).charged_bytes, 200);
}

TEST_F(MasterServiceTenantQuotaTest,
       SizeChangingUpsertRevokeReleasesNewAndReplacementCharge) {
    const TenantId tenant_id("tenant-a");
    MasterService service(MakeConfig({{tenant_id, 1000}}));
    UUID client_id = MountSegment(service);
    PutComplete(service, client_id, "key", tenant_id, 100);

    auto upsert =
        service.UpsertStart(client_id, "key", tenant_id, 200, MemoryConfig());
    ASSERT_TRUE(upsert.has_value()) << toString(upsert.error());
    EXPECT_EQ(Snapshot(service, tenant_id).charged_bytes, 300);

    auto revoke =
        service.UpsertRevoke(client_id, "key", tenant_id, ReplicaType::MEMORY);
    ASSERT_TRUE(revoke.has_value()) << toString(revoke.error());
    EXPECT_EQ(Snapshot(service, tenant_id).charged_bytes, 0);
}

TEST_F(MasterServiceTenantQuotaTest,
       PartialProcessingExpirySettlesPendingCharge) {
    const TenantId tenant_id("tenant-a");
    MasterService service(MakeConfig({{tenant_id, 1000}}));
    UUID client_id = MountSegment(service);

    auto start =
        service.PutStart(client_id, "key", tenant_id, 100, MemoryConfig());
    ASSERT_TRUE(start.has_value()) << toString(start.error());
    AddCompletedDiskReplica(service, client_id, "key", tenant_id, 100);
    EXPECT_EQ(Snapshot(service, tenant_id).charged_bytes, 100);

    DiscardExpiredProcessingForTest(service, tenant_id);

    ExpectDiskOnlyObjectAndChargedBytes(service, tenant_id, "key", 0);
}

TEST_F(MasterServiceTenantQuotaTest,
       DurablePartialProcessingExpirySettlesPendingCharge) {
    const TenantId tenant_id("tenant-a");
    MasterService service(MakeConfig({{tenant_id, 1000}}));
    UUID client_id = MountSegment(service);
    const MasterServiceTestPeer::ObjectIdentity identity{tenant_id, "key"};

    auto start =
        service.PutStart(client_id, "key", tenant_id, 100, MemoryConfig());
    ASSERT_TRUE(start.has_value()) << toString(start.error());
    AddCompletedDiskReplica(service, client_id, "key", tenant_id, 100);
    EXPECT_EQ(Snapshot(service, tenant_id).charged_bytes, 100);

    // No next round intervenes, so the ids the cleanup was armed for are the
    // ones it removes.
    const auto entry = MasterServiceTestPeer::FindObject(service, identity);
    ASSERT_NE(entry, nullptr);
    const std::vector<ReplicaID> armed_ids = ProcessingReplicaIdsOf(entry);
    ASSERT_EQ(armed_ids.size(), 1u);

    FinalizeExpiredProcessingForTest(service, tenant_id, "key");

    ExpectDiskOnlyObjectAndChargedBytes(service, tenant_id, "key", 0);
    EXPECT_TRUE(ProcessingReplicaIdsOf(entry).empty());
    EXPECT_FALSE(IsProcessingOf(entry));
}

TEST_F(MasterServiceTenantQuotaTest,
       PartialSizeChangingUpsertRevokeReleasesReplacementCharge) {
    const TenantId tenant_id("tenant-a");
    MasterService service(MakeConfig({{tenant_id, 1000}}));
    UUID client_id = MountSegment(service);
    PutComplete(service, client_id, "key", tenant_id, 100);

    auto upsert =
        service.UpsertStart(client_id, "key", tenant_id, 200, MemoryConfig());
    ASSERT_TRUE(upsert.has_value()) << toString(upsert.error());
    AddCompletedDiskReplica(service, client_id, "key", tenant_id, 200);
    EXPECT_EQ(Snapshot(service, tenant_id).charged_bytes, 300);

    auto revoke =
        service.UpsertRevoke(client_id, "key", tenant_id, ReplicaType::MEMORY);

    ASSERT_TRUE(revoke.has_value()) << toString(revoke.error());
    ExpectDiskOnlyObjectAndChargedBytes(service, tenant_id, "key", 0);
}

TEST_F(MasterServiceTenantQuotaTest,
       DurablePartialUpsertRevokeReleasesReplacementCharge) {
    const TenantId tenant_id("tenant-a");
    MasterService service(MakeConfig({{tenant_id, 1000}}));
    UUID client_id = MountSegment(service);
    PutComplete(service, client_id, "key", tenant_id, 100);

    auto upsert =
        service.UpsertStart(client_id, "key", tenant_id, 200, MemoryConfig());
    ASSERT_TRUE(upsert.has_value()) << toString(upsert.error());
    AddCompletedDiskReplica(service, client_id, "key", tenant_id, 200);
    EXPECT_EQ(Snapshot(service, tenant_id).charged_bytes, 300);

    FinalizeRemovedMemoryReplicasForTest(service, tenant_id, "key");

    ExpectDiskOnlyObjectAndChargedBytes(service, tenant_id, "key", 0);
}

// A durable cleanup is armed for one publication, so a same-key recreate that
// lands while it is still in flight keeps the state it owns. The replacement is
// staged with a dead memory handle, the state the read-write accessor the
// cleanup resolves through drops before its callback.
TEST_F(MasterServiceTenantQuotaTest,
       DurableRemovalOfSupersededPublicationSparesTheRecreate) {
    const TenantId tenant("tenant-a");
    const std::string key = "recreated-key";
    const uint64_t object_size = 128;

    MasterService service(MakeRecreateConfig(tenant));
    MasterServiceTestPeer peer(service);
    std::vector<ReplicaID> removed_ids;
    StageRecreate(service, peer, tenant, key, object_size,
                  SupersededExit::kEraseRemoved,
                  /*invalidate_replacement_handle=*/true, &removed_ids);
    const auto before_replicas = MemoryReplicaIdsOf(replacement_.entry);
    const uint64_t before_charged = Snapshot(service, tenant).charged_bytes;

    peer.FinalizeRemovedReplicasAfterDurable(
        superseded_.entry, durable_entry_, removed_ids,
        MasterServiceTestPeer::QuotaEraseMode::kFull);

    // Its own discriminator: the route still publishes the replacement, with
    // the dead-handle replica the accessor this cleanup resolved through would
    // have dropped, and the charge it carries is untouched.
    EXPECT_EQ(MasterServiceTestPeer::FindObject(
                  service, MasterServiceTestPeer::ObjectIdentity{tenant, key}),
              replacement_.entry);
    EXPECT_EQ(MemoryReplicaIdsOf(replacement_.entry), before_replicas);
    EXPECT_EQ(InvalidHandleCountOf(replacement_.entry), 1u);
    EXPECT_EQ(Snapshot(service, tenant).charged_bytes, before_charged);
}

// The comprehensive superseded-publication test: the metadata erase did its
// damage under the object key rather than under the publication, so it
// published a KV removal for the key, retracted every dynamic-replication lease
// of that key, dropped the key's soft-pin deadline registration, unindexed its
// promotion candidate and released the superseded publication's charge. Every
// record the replacement owns is read again after it.
TEST_F(MasterServiceTenantQuotaTest, DurableMetadataEraseSparesTheRecreate) {
    const TenantId tenant("tenant-a");
    const std::string key = "recreated-key";
    const uint64_t object_size = 128;

    MasterService service(MakeRecreateConfig(tenant));
    MasterServiceTestPeer peer(service);
    StageRecreate(service, peer, tenant, key, object_size,
                  SupersededExit::kReleaseRouteSlot,
                  /*invalidate_replacement_handle=*/false);
    const auto before =
        CapturePublicationState(service, peer, tenant, replacement_.entry,
                                replacement_.task, replacement_.proposal);
    // The replacement owns the soft-pin registration this erase used to take by
    // key, and the superseded publication's charge is still on the tenant:
    // dropping its route slot released nothing.
    ASSERT_EQ(peer.SoftPinRegistrationCount(), 1u);
    ASSERT_EQ(before.charged_bytes, 2 * object_size);

    peer.FinalizeMetadataEraseAfterDurable(
        superseded_.entry, tenant,
        MasterServiceTestPeer::QuotaEraseMode::kFull);

    ExpectPublicationUnchanged(service, peer, tenant, replacement_.entry,
                               before);
    // The index's only reader consumes what it reports, so the registration is
    // read last: it still holds the deadline the replacement registered.
    EXPECT_EQ(RegisteredSoftPinDeadline(service, tenant, key,
                                        replacement_.soft_pin_deadline),
              replacement_.soft_pin_deadline);
}

// The expired-processing cleanup resolves through the tenant's plain published
// object. It used to resolve through the read-write accessor, whose
// invalid-replica cleanup ran on whatever the route published, so a same-key
// recreate whose own replica has a dead handle was dropped and torn down before
// the cleanup compared the publication it was armed for.
TEST_F(MasterServiceTenantQuotaTest,
       DurableExpiredProcessingCleanupSparesTheRecreate) {
    const TenantId tenant("tenant-a");
    const std::string key = "recreated-key";
    const uint64_t object_size = 128;

    MasterService service(MakeRecreateConfig(tenant));
    MasterServiceTestPeer peer(service);
    StageRecreate(service, peer, tenant, key, object_size,
                  SupersededExit::kErase,
                  /*invalidate_replacement_handle=*/true);
    const auto before_replicas = MemoryReplicaIdsOf(replacement_.entry);

    peer.FinalizeExpiredProcessingReplicasAfterDurable(
        superseded_.entry, durable_entry_,
        MemoryReplicaIdsOf(superseded_.entry),
        std::chrono::system_clock::now());

    // Its own discriminator: the accessor would have dropped the replacement's
    // dead-handle replica, and the route would have published nothing.
    EXPECT_EQ(MasterServiceTestPeer::FindObject(
                  service, MasterServiceTestPeer::ObjectIdentity{tenant, key}),
              replacement_.entry);
    EXPECT_EQ(InvalidHandleCountOf(replacement_.entry), 1u);
    EXPECT_EQ(MemoryReplicaIdsOf(replacement_.entry), before_replicas);
}

// The expired-replication cleanup carried the same accessor damage as the
// processing one, and it adds its own task-token check: the task records the
// ids and the lease it was created with, so the cleanup of one publication must
// not consume the task another one owns.
TEST_F(MasterServiceTenantQuotaTest,
       DurableExpiredReplicationCleanupSparesTheRecreate) {
    const TenantId tenant("tenant-a");
    const std::string key = "recreated-key";
    const uint64_t object_size = 128;

    MasterService service(MakeRecreateConfig(tenant));
    MasterServiceTestPeer peer(service);
    StageRecreate(service, peer, tenant, key, object_size,
                  SupersededExit::kErase,
                  /*invalidate_replacement_handle=*/true);
    // The records of the task the superseded publication armed this cleanup
    // for: the callback is handed them verbatim.
    const ReplicationTask superseded_task = superseded_.task;
    // The replacement owns a different task in every field the callback
    // compares.
    ASSERT_TRUE(superseded_task.durable_cleanup_pending);
    ASSERT_FALSE(replacement_.task.durable_cleanup_pending);
    ASSERT_NE(replacement_.task.source_id, superseded_task.source_id);
    ASSERT_NE(replacement_.task.replica_ids, superseded_task.replica_ids);
    ASSERT_NE(replacement_.task.dynamic_replication_lease_id,
              superseded_task.dynamic_replication_lease_id);

    peer.FinalizeExpiredReplicationTaskAfterDurable(
        superseded_.entry, durable_entry_, superseded_task.source_id,
        superseded_task.replica_ids,
        superseded_task.dynamic_replication_lease_id,
        superseded_task.dynamic_replication_version_epoch,
        std::chrono::system_clock::now());

    // Its own discriminator: the task-token check matched the superseded
    // publication's own records, so the replacement keeps the task it was
    // staged with and the route it was published on.
    EXPECT_EQ(MasterServiceTestPeer::FindObject(
                  service, MasterServiceTestPeer::ObjectIdentity{tenant, key}),
              replacement_.entry);
    EXPECT_EQ(ReplicationTaskFieldsOf(replacement_.entry),
              ReplicationTaskFields(replacement_.task));
}

// The comprehensive stale-cleanup test, driven through the real scheduling
// path: DiscardExpiredProcessingReplicas finds each expired write round,
// records its replica ids and commits them into a durable finalizer, and the
// writer this test installs holds those finalizers until the test runs them.
TEST_F(MasterServiceTenantQuotaTest,
       DurableProcessingCleanupSparesTheNextRound) {
    const TenantId tenant("tenant-a");
    const std::string key = "processing-round-key";
    const std::string untouched_key = "processing-round-key-without-successor";
    const uint64_t object_size = 128;
    const MasterServiceTestPeer::ObjectIdentity identity{tenant, key};

    MasterService service(MakeRecreateConfig(tenant, /*enable_oplog=*/true));
    MasterServiceTestPeer peer(service);
    QuiesceRecreateBackgroundWorkers(service);
    const UUID client = MountSegment(service, /*size=*/4096, "round-segment");
    // A completed disk replica may only be registered for a client whose local
    // disk segment is mounted.
    ASSERT_TRUE(
        service.MountLocalDiskSegment(client, /*enable_offloading=*/true)
            .has_value());

    const auto entry =
        StageExpiredProcessingRound(service, client, tenant, key, object_size);
    ASSERT_NE(entry, nullptr);
    const std::vector<ReplicaID> armed_ids = ProcessingReplicaIdsOf(entry);
    ASSERT_EQ(armed_ids.size(), 1u);
    const auto untouched_entry = StageExpiredProcessingRound(
        service, client, tenant, untouched_key, object_size);
    ASSERT_NE(untouched_entry, nullptr);
    const std::vector<ReplicaID> untouched_armed_ids =
        ProcessingReplicaIdsOf(untouched_entry);
    ASSERT_EQ(untouched_armed_ids.size(), 1u);

    auto writer = std::make_unique<HoldingOpLogWriter>();
    auto* holding_writer = writer.get();
    MasterServiceTestPeer::InstallOpLogWriterForTesting(service,
                                                        std::move(writer));
    holding_writer->HoldFinalizers();
    DiscardExpiredProcessingForTest(service, tenant);
    // The sweep committed a durable entry per key instead of discarding inline:
    // with the OpLog on, the replicas go only when that finalizer runs.
    const OpLogEntry armed_entry = holding_writer->WaitForHeldEntry(key);
    ASSERT_EQ(armed_entry.object_key, key);
    ASSERT_EQ(armed_entry.op_type, OpType::PUT_END);
    ASSERT_EQ(ProcessingReplicaIdsOf(entry), armed_ids);

    // The next round on the same entry: UpsertStart preempts `armed_ids` and
    // moves the complete replica into PROCESSING.
    auto upsert =
        service.UpsertStart(client, key, tenant, object_size, MemoryConfig());
    ASSERT_TRUE(upsert.has_value()) << toString(upsert.error());
    ASSERT_EQ(MasterServiceTestPeer::FindObject(service, identity), entry);
    const std::vector<ReplicaID> next_round_ids = ProcessingReplicaIdsOf(entry);
    ASSERT_EQ(next_round_ids.size(), 1u);
    ASSERT_NE(next_round_ids, armed_ids);

    // The next round owns its own soft pin, promotion candidate, lease and
    // task.
    const UUID proposal = generate_uuid();
    CommitSoftPin(service, tenant, key, entry,
                  std::chrono::system_clock::now() + std::chrono::seconds(600));
    ASSERT_EQ(peer.TryPushPromotionQueue(identity, /*record_candidate=*/true),
              MasterServiceTestPeer::PromotionQueueResult::kWatermarkRejected);
    peer.PutDynamicReplicationLeaseForTesting(
        tenant, entry, proposal, MakeReplicaActionLease(tenant, key, proposal));
    const ReplicationTask next_round_task{
        .client_id = client,
        .start_time = std::chrono::system_clock::now(),
        .type = ReplicationTask::Type::COPY,
        .source_id = next_round_ids.front(),
        .replica_ids = {next_round_ids.front() + 1},
        .pending_quota_charge_bytes = 0,
        .dynamic_replication_lease_id = proposal,
        .dynamic_replication_version_epoch = 3,
        .durable_cleanup_pending = false};
    SetReplicationTaskForTest(service, tenant, key, next_round_task);

    const auto before = CapturePublicationState(service, peer, tenant, entry,
                                                next_round_task, proposal);
    ASSERT_TRUE(before.is_processing);

    // The finalizer the sweep committed for `key`, running against the next
    // round: the preempted ids stay gone, the next round's replica stays, and
    // every record the stale callback could have taken by key is still there.
    holding_writer->RunHeldFinalizerFor(key);
    EXPECT_EQ(ProcessingReplicaIdsOf(entry), next_round_ids);
    ExpectPublicationUnchanged(service, peer, tenant, entry, before);

    // The finalizer the sweep committed for the key nothing took over: it
    // removes the ids it recorded and ends that round.
    holding_writer->RunHeldFinalizerFor(untouched_key);
    EXPECT_TRUE(ProcessingReplicaIdsOf(untouched_entry).empty());
    EXPECT_FALSE(IsProcessingOf(untouched_entry));
    ExpectDiskOnlyObjectAndChargedBytes(service, tenant, untouched_key, 0);
}

TEST_F(MasterServiceTenantQuotaTest, CopyStartRequiresQuotaForNewReplica) {
    MasterService service(MakeConfig({{TenantId("tenant-a"), 150}}));
    UUID client_id = MountSegment(service, /*size=*/1024, "segment-a");
    MountSegment(service, /*size=*/1024, "segment-b");

    ReplicateConfig config = MemoryConfig();
    config.preferred_segment = "segment-a";
    auto put_start =
        service.PutStart(client_id, "key", TenantId("tenant-a"), 100, config);
    ASSERT_TRUE(put_start.has_value()) << toString(put_start.error());
    ASSERT_TRUE(
        service
            .PutEnd(client_id, "key", TenantId("tenant-a"), ReplicaType::MEMORY)
            .has_value());

    auto copy = service.CopyStart(client_id, "key", TenantId("tenant-a"),
                                  "segment-a", {"segment-b"});

    ASSERT_FALSE(copy.has_value());
    EXPECT_EQ(copy.error(), ErrorCode::TENANT_QUOTA_EXCEEDED);
    auto snapshot = Snapshot(service, TenantId("tenant-a"));
    EXPECT_EQ(snapshot.charged_bytes, 100);
}

TEST_F(MasterServiceTenantQuotaTest, CopyEndRetainsAdditionalReplicaCharge) {
    MasterService service(MakeConfig({{TenantId("tenant-a"), 300}}));
    UUID client_id = MountSegment(service, /*size=*/1024, "segment-a");
    MountSegment(service, /*size=*/1024, "segment-b");

    ReplicateConfig config = MemoryConfig();
    config.preferred_segment = "segment-a";
    auto put_start =
        service.PutStart(client_id, "key", TenantId("tenant-a"), 100, config);
    ASSERT_TRUE(put_start.has_value()) << toString(put_start.error());
    ASSERT_TRUE(
        service
            .PutEnd(client_id, "key", TenantId("tenant-a"), ReplicaType::MEMORY)
            .has_value());

    auto copy = service.CopyStart(client_id, "key", TenantId("tenant-a"),
                                  "segment-a", {"segment-b"});
    ASSERT_TRUE(copy.has_value()) << toString(copy.error());
    auto in_flight = Snapshot(service, TenantId("tenant-a"));
    EXPECT_EQ(in_flight.charged_bytes, 200);

    ASSERT_TRUE(
        service.CopyEnd(client_id, "key", TenantId("tenant-a")).has_value());
    auto completed = Snapshot(service, TenantId("tenant-a"));
    EXPECT_EQ(completed.charged_bytes, 200);
}

TEST_F(MasterServiceTenantQuotaTest, CopyRevokeRefundsStartCharge) {
    MasterService service(MakeConfig({{TenantId("tenant-a"), 300}}));
    UUID client_id = MountSegment(service, /*size=*/1024, "segment-a");
    MountSegment(service, /*size=*/1024, "segment-b");

    ReplicateConfig config = MemoryConfig();
    config.preferred_segment = "segment-a";
    auto put_start =
        service.PutStart(client_id, "key", TenantId("tenant-a"), 100, config);
    ASSERT_TRUE(put_start.has_value()) << toString(put_start.error());
    ASSERT_TRUE(
        service
            .PutEnd(client_id, "key", TenantId("tenant-a"), ReplicaType::MEMORY)
            .has_value());

    ASSERT_TRUE(service
                    .CopyStart(client_id, "key", TenantId("tenant-a"),
                               "segment-a", {"segment-b"})
                    .has_value());
    EXPECT_EQ(Snapshot(service, TenantId("tenant-a")).charged_bytes, 200);

    ASSERT_TRUE(
        service.CopyRevoke(client_id, "key", TenantId("tenant-a")).has_value());
    EXPECT_EQ(Snapshot(service, TenantId("tenant-a")).charged_bytes, 100);
}

TEST_F(MasterServiceTenantQuotaTest,
       MoveStartRequiresQuotaForTemporaryReplica) {
    MasterService service(MakeConfig({{TenantId("tenant-a"), 150}}));
    UUID client_id = MountSegment(service, /*size=*/1024, "segment-a");
    MountSegment(service, /*size=*/1024, "segment-b");

    ReplicateConfig config = MemoryConfig();
    config.preferred_segment = "segment-a";
    auto put_start =
        service.PutStart(client_id, "key", TenantId("tenant-a"), 100, config);
    ASSERT_TRUE(put_start.has_value()) << toString(put_start.error());
    ASSERT_TRUE(
        service
            .PutEnd(client_id, "key", TenantId("tenant-a"), ReplicaType::MEMORY)
            .has_value());

    auto move = service.MoveStart(client_id, "key", TenantId("tenant-a"),
                                  "segment-a", "segment-b");

    ASSERT_FALSE(move.has_value());
    EXPECT_EQ(move.error(), ErrorCode::TENANT_QUOTA_EXCEEDED);
    auto snapshot = Snapshot(service, TenantId("tenant-a"));
    EXPECT_EQ(snapshot.charged_bytes, 100);
}

TEST_F(MasterServiceTenantQuotaTest, MoveEndSettlesToFinalReplicaCharge) {
    MasterService service(MakeConfig({{TenantId("tenant-a"), 300}}));
    UUID client_id = MountSegment(service, /*size=*/1024, "segment-a");
    MountSegment(service, /*size=*/1024, "segment-b");

    ReplicateConfig config = MemoryConfig();
    config.preferred_segment = "segment-a";
    ASSERT_TRUE(
        service.PutStart(client_id, "key", TenantId("tenant-a"), 100, config)
            .has_value());
    ASSERT_TRUE(
        service
            .PutEnd(client_id, "key", TenantId("tenant-a"), ReplicaType::MEMORY)
            .has_value());

    ASSERT_TRUE(service
                    .MoveStart(client_id, "key", TenantId("tenant-a"),
                               "segment-a", "segment-b")
                    .has_value());
    EXPECT_EQ(Snapshot(service, TenantId("tenant-a")).charged_bytes, 200);

    ASSERT_TRUE(
        service.MoveEnd(client_id, "key", TenantId("tenant-a")).has_value());
    EXPECT_EQ(Snapshot(service, TenantId("tenant-a")).charged_bytes, 100);
}

TEST_F(MasterServiceTenantQuotaTest, DeletePolicyRequiresTenantWithoutObjects) {
    MasterService service(MakeConfig({{TenantId("tenant-a"), 1000}}));
    UUID client_id = MountSegment(service);
    PutComplete(service, client_id, "key", TenantId("tenant-a"), 100);

    auto delete_non_empty =
        service.DeleteTenantQuotaPolicy(TenantId("tenant-a"));
    ASSERT_FALSE(delete_non_empty.has_value());
    EXPECT_EQ(delete_non_empty.error(), ErrorCode::TENANT_NOT_EMPTY);

    auto upsert = service.UpsertTenantQuotaPolicy(TenantId("tenant-b"), 100);
    ASSERT_TRUE(upsert.has_value()) << toString(upsert.error());
    auto delete_empty = service.DeleteTenantQuotaPolicy(TenantId("tenant-b"));
    ASSERT_TRUE(delete_empty.has_value()) << toString(delete_empty.error());
    EXPECT_FALSE(delete_empty.value().has_value());
}

TEST_F(MasterServiceTenantQuotaTest,
       DeletePolicyBlocksValidatedChargesBeforeConnectorSave) {
    MasterService service(MakeConfig({{TenantId("tenant-a"), 1000}}));
    MountSegment(service);

    TenantQuotaPolicySnapshot current_policy;
    current_policy.tenant_quotas = {{"tenant-a", 1000}};
    auto blocking_store =
        std::make_unique<BlockingTenantQuotaPolicyStore>(current_policy);
    auto* blocking_store_ptr = blocking_store.get();
    auto save_started = blocking_store_ptr->SaveStarted();
    ReplaceTenantQuotaPolicyStore(service, std::move(blocking_store));

    using DeleteResult =
        tl::expected<std::optional<TenantQuotaSnapshot>, ErrorCode>;
    std::optional<DeleteResult> delete_result;
    std::thread delete_thread([&] {
        delete_result.emplace(
            service.DeleteTenantQuotaPolicy(TenantId("tenant-a")));
    });

    if (save_started.wait_for(std::chrono::seconds(5)) !=
        std::future_status::ready) {
        blocking_store_ptr->AllowSave();
        delete_thread.join();
        FAIL() << "timed out waiting for connector save";
    }

    auto charge = ChargeTenantQuotaForTest(service, TenantId("tenant-a"), 1);
    EXPECT_FALSE(charge.has_value());
    EXPECT_EQ(charge.error(), ErrorCode::TENANT_NOT_REGISTERED);

    auto zero_byte_charge =
        ChargeTenantQuotaForTest(service, TenantId("tenant-a"), 0);
    EXPECT_FALSE(zero_byte_charge.has_value());
    EXPECT_EQ(zero_byte_charge.error(), ErrorCode::TENANT_NOT_REGISTERED);

    blocking_store_ptr->AllowSave();
    delete_thread.join();

    ASSERT_TRUE(delete_result.has_value());
    ASSERT_TRUE(delete_result->has_value()) << toString(delete_result->error());
    EXPECT_FALSE(delete_result->value().has_value());
    EXPECT_FALSE(
        service.GetTenantQuotaSnapshot(TenantId("tenant-a")).has_value());
}

TEST_F(MasterServiceTenantQuotaTest,
       DeletePolicyWaitsForInFlightAddReplicaBeforeEmptyCheck) {
    MasterService service(MakeConfig({{TenantId("tenant-a"), 1000}}));
    UUID client_id = MountSegment(service);

    TenantQuotaPolicySnapshot current_policy;
    current_policy.tenant_quotas = {{"tenant-a", 1000}};
    auto blocking_store =
        std::make_unique<BlockingTenantQuotaPolicyStore>(current_policy);
    auto* blocking_store_ptr = blocking_store.get();
    auto save_started = blocking_store_ptr->SaveStarted();
    ReplaceTenantQuotaPolicyStore(service, std::move(blocking_store));

    auto snapshot_lock = LockSnapshotForTest(service);
    std::optional<tl::expected<bool, ErrorCode>> add_result;
    std::thread add_thread([&] {
        Replica replica(client_id, 128, "disk-endpoint",
                        ReplicaStatus::COMPLETE);
        add_result.emplace(service.AddReplica(client_id, "cold",
                                              TenantId("tenant-a"), replica));
    });

    if (!WaitForTenantQuotaPolicyMutexContention(service)) {
        snapshot_lock.unlock();
        add_thread.join();
        FAIL() << "timed out waiting for AddReplica to enter tenant policy "
                  "critical section";
    }

    using DeleteResult =
        tl::expected<std::optional<TenantQuotaSnapshot>, ErrorCode>;
    std::optional<DeleteResult> delete_result;
    std::thread delete_thread([&] {
        delete_result.emplace(
            service.DeleteTenantQuotaPolicy(TenantId("tenant-a")));
    });

    const auto premature_save =
        save_started.wait_for(std::chrono::milliseconds(200));
    if (premature_save == std::future_status::ready) {
        blocking_store_ptr->AllowSave();
    }
    snapshot_lock.unlock();
    add_thread.join();
    delete_thread.join();

    ASSERT_EQ(premature_save, std::future_status::timeout)
        << "tenant deletion reached connector save before in-flight "
           "AddReplica completed";
    ASSERT_TRUE(add_result.has_value());
    ASSERT_TRUE(add_result->has_value()) << toString(add_result->error());
    ASSERT_TRUE(delete_result.has_value());
    ASSERT_FALSE(delete_result->has_value());
    EXPECT_EQ(delete_result->error(), ErrorCode::TENANT_NOT_EMPTY);
    auto exists = service.ExistKey("cold", TenantId("tenant-a"));
    ASSERT_TRUE(exists.has_value()) << toString(exists.error());
    EXPECT_TRUE(exists.value());
}

#ifdef USE_NOF
TEST_F(MasterServiceTenantQuotaTest,
       DeletePolicySeesZeroChargePutStartMetadataCreateWithoutPolicyLock) {
    MasterService service(MakeConfig({{TenantId("tenant-a"), 1000}}));
    UUID client_id = MountNoFSegment(service);

    auto blocking_strategy = std::make_shared<BlockingAllocationStrategy>();
    auto* blocking_strategy_ptr = blocking_strategy.get();
    auto allocation_started = blocking_strategy_ptr->AllocationStarted();
    ReplaceAllocationStrategy(service, std::move(blocking_strategy));

    ReplicateConfig config;
    config.replica_num = 0;
    config.nof_replica_num = 1;

    std::optional<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
        put_result;
    auto policy_lock = LockTenantQuotaPolicyForTest(service);
    std::thread put_thread([&] {
        put_result.emplace(service.PutStart(client_id, "nof-key",
                                            TenantId("tenant-a"), 128, config));
    });

    if (allocation_started.wait_for(std::chrono::seconds(5)) !=
        std::future_status::ready) {
        policy_lock.unlock();
        blocking_strategy_ptr->AllowAllocation();
        put_thread.join();
        FAIL() << "zero-charge PutStart waited for tenant quota policy mutex";
    }
    policy_lock.unlock();

    using DeleteResult =
        tl::expected<std::optional<TenantQuotaSnapshot>, ErrorCode>;
    std::promise<DeleteResult> delete_promise;
    auto delete_future = delete_promise.get_future();
    std::thread delete_thread([&] {
        delete_promise.set_value(
            service.DeleteTenantQuotaPolicy(TenantId("tenant-a")));
    });

    EXPECT_EQ(delete_future.wait_for(std::chrono::milliseconds(200)),
              std::future_status::timeout)
        << "tenant deletion passed the metadata scan while zero-charge "
           "PutStart was still in flight for that tenant";

    blocking_strategy_ptr->AllowAllocation();
    put_thread.join();
    delete_thread.join();

    ASSERT_TRUE(put_result.has_value());
    ASSERT_TRUE(put_result->has_value()) << toString(put_result->error());
    auto delete_result = delete_future.get();
    ASSERT_FALSE(delete_result.has_value());
    EXPECT_EQ(delete_result.error(), ErrorCode::TENANT_NOT_EMPTY);

    auto snapshot = Snapshot(service, TenantId("tenant-a"));
    EXPECT_EQ(snapshot.charged_bytes, 0);
}
#endif

TEST_F(MasterServiceTenantQuotaTest,
       EffectiveQuotaUsesOnlyExplicitPolicyAndScalesProportionally) {
    MasterService service(
        MakeConfig({{TenantId("tenant-a"), 200}, {TenantId("tenant-b"), 400}}));
    MountSegment(service, /*size=*/300);

    EXPECT_EQ(Snapshot(service, TenantId("tenant-a")).effective_quota_bytes,
              100);
    EXPECT_EQ(Snapshot(service, TenantId("tenant-b")).effective_quota_bytes,
              200);
}

TEST_F(MasterServiceTenantQuotaTest,
       CapacityIsSampledInsideQuotaRecomputeCoordination) {
    MasterService service(MakeConfig({{TenantId("tenant-a"), 1000}}));
    auto recompute_lock = LockTenantQuotaRecomputeForTest(service);
    ASSERT_EQ(MountSegmentWithoutQuotaRecomputeForTest(service, /*size=*/100,
                                                       "capacity-a"),
              ErrorCode::OK);

    std::promise<void> recompute_started;
    auto recompute_started_future = recompute_started.get_future();
    auto recompute = std::async(std::launch::async, [&] {
        recompute_started.set_value();
        RecomputeTenantEffectiveQuotasForTest(service);
    });
    recompute_started_future.wait();

    EXPECT_EQ(recompute.wait_for(std::chrono::milliseconds(100)),
              std::future_status::timeout);
    const auto second_mount_result = MountSegmentWithoutQuotaRecomputeForTest(
        service, /*size=*/50, "capacity-b");
    recompute_lock.unlock();

    ASSERT_EQ(second_mount_result, ErrorCode::OK);
    ASSERT_EQ(recompute.wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    recompute.get();
    EXPECT_EQ(Snapshot(service, TenantId("tenant-a")).effective_quota_bytes,
              150);
}

TEST_F(MasterServiceTenantQuotaTest,
       ConnectorPolicyReloadCreatesOrphanStateAndAllowsCleanup) {
    const std::string initial_policy = WritePolicyFile(
        {{TenantId("tenant-a"), 1000}, {TenantId("tenant-b"), 1000}});
    auto config = MasterServiceConfig::builder()
                      .set_enable_multi_tenants(true)
                      .set_tenant_quota_connector_type("file")
                      .set_tenant_quota_connector_uri(initial_policy)
                      .build();
    MasterService service(config);
    UUID client_id = MountSegment(service);
    PutComplete(service, client_id, "orphan-key", TenantId("tenant-b"), 100);

    {
        std::ofstream out(initial_policy);
        TenantQuotaPolicySnapshot replacement;
        replacement.tenant_quotas = {{"tenant-a", 1000}};
        out << FormatTenantQuotaPolicyYaml(replacement);
    }
    ReloadTenantQuotaPolicyFromStore(service);

    auto orphan = Snapshot(service, TenantId("tenant-b"));
    EXPECT_FALSE(orphan.has_explicit_policy);
    EXPECT_EQ(orphan.requested_quota_bytes, 0);
    EXPECT_EQ(orphan.effective_quota_bytes, 0);
    EXPECT_TRUE(orphan.over_quota);

    EXPECT_TRUE(
        service.GetReplicaList("orphan-key", TenantId("tenant-b")).has_value());
    auto write = service.PutStart(client_id, "new-key", TenantId("tenant-b"), 1,
                                  MemoryConfig());
    ASSERT_FALSE(write.has_value());
    EXPECT_EQ(write.error(), ErrorCode::TENANT_NOT_REGISTERED);

    EXPECT_TRUE(service
                    .Remove("orphan-key", TenantId("tenant-b"),
                            /*force=*/true)
                    .has_value());
}

// --- Tenant-scoped eviction watermark -------------------------------------
//
// This pass is the only thing that makes room for a tenant. A tenant whose
// effective quota sits at or below the pool-wide eviction_high_watermark_ratio
// reaches its own ceiling before the pool crosses the pool-wide watermark:
// EvictionThreadFunc gates on a pool-global used_ratio (strictly greater), and
// the quota path cannot arm it either -- need_mem_eviction_ is only set next to
// inc_put_start_alloc_failures(), and a quota rejection returns before
// allocation is attempted. Admission itself does not evict; it rejects with
// TENANT_QUOTA_EXCEEDED and leaves the headroom to this pass.

TEST_F(MasterServiceTenantQuotaTest, TenantEvictionWatermarkDefaultsToOn) {
    const TenantId tenant("tenant-a");
    // No explicit ratio: the tenant watermark must default to the same 0.90 as
    // the pool-wide one, so a multi-tenant master gets the evict-to-make-room
    // contract without having to opt in.
    ASSERT_DOUBLE_EQ(DEFAULT_TENANT_EVICTION_HIGH_WATERMARK_RATIO,
                     DEFAULT_EVICTION_HIGH_WATERMARK_RATIO);
    MasterService service(MakeTenantWatermarkConfig({{tenant, 1000}},
                                                    /*watermark=*/std::nullopt,
                                                    /*eviction_ratio=*/0.05));
    UUID client_id = MountSegment(service, /*size=*/4096);

    for (int i = 0; i < 4; ++i) {
        PutComplete(service, client_id, "key-" + std::to_string(i), tenant,
                    240);
    }
    ASSERT_EQ(Snapshot(service, tenant).charged_bytes, 960u);  // 0.96 > 0.90

    RunTenantEvictionPass(service);

    EXPECT_LE(Snapshot(service, tenant).charged_bytes, 850u)
        << "the pass must run without an explicit watermark setting";
}

TEST_F(MasterServiceTenantQuotaTest,
       TenantEvictionWatermarkOfZeroDisablesThePass) {
    const TenantId tenant("tenant-a");
    // Watermark 0.0 == the pre-existing behaviour, and remains available as an
    // opt-out.
    MasterService service(MakeTenantWatermarkConfig({{tenant, 1000}},
                                                    /*watermark=*/0.0));
    UUID client_id = MountSegment(service, /*size=*/4096);

    for (int i = 0; i < 4; ++i) {
        PutComplete(service, client_id, "key-" + std::to_string(i), tenant,
                    240);
    }
    ASSERT_EQ(Snapshot(service, tenant).charged_bytes, 960u);

    RunTenantEvictionPass(service);

    EXPECT_EQ(Snapshot(service, tenant).charged_bytes, 960u)
        << "the pass must not run when the watermark is 0";
}

TEST_F(MasterServiceTenantQuotaTest,
       TenantEvictionWatermarkLeavesTenantsBelowItAlone) {
    const TenantId tenant("tenant-a");
    MasterService service(MakeTenantWatermarkConfig({{tenant, 1000}},
                                                    /*watermark=*/0.9));
    UUID client_id = MountSegment(service, /*size=*/4096);

    for (int i = 0; i < 4; ++i) {
        PutComplete(service, client_id, "key-" + std::to_string(i), tenant,
                    200);
    }
    ASSERT_EQ(Snapshot(service, tenant).charged_bytes, 800u);  // 0.80

    RunTenantEvictionPass(service);

    EXPECT_EQ(Snapshot(service, tenant).charged_bytes, 800u);
}

TEST_F(MasterServiceTenantQuotaTest,
       TenantEvictionWatermarkEvictsDownToTargetRatio) {
    const TenantId tenant("tenant-a");
    // watermark 0.9, eviction_ratio 0.05 -> evict down to 0.85 of the quota.
    MasterService service(MakeTenantWatermarkConfig({{tenant, 1000}},
                                                    /*watermark=*/0.9,
                                                    /*eviction_ratio=*/0.05));
    UUID client_id = MountSegment(service, /*size=*/4096);

    for (int i = 0; i < 4; ++i) {
        PutComplete(service, client_id, "key-" + std::to_string(i), tenant,
                    240);
    }
    ASSERT_EQ(Snapshot(service, tenant).charged_bytes, 960u);  // 0.96 > 0.90

    RunTenantEvictionPass(service);

    const uint64_t charged = Snapshot(service, tenant).charged_bytes;
    EXPECT_LT(charged, 960u) << "tenant was over its watermark and not evicted";
    EXPECT_LE(charged, 850u) << "must reach (watermark - eviction_ratio)";
}

TEST_F(MasterServiceTenantQuotaTest,
       TenantEvictionWatermarkOnlyTouchesTenantsOverIt) {
    const TenantId hot("tenant-hot");
    const TenantId cold("tenant-cold");
    // Sum of requested (2000) is below capacity, so each tenant gets its
    // literal quota rather than a proportional share.
    MasterService service(MakeTenantWatermarkConfig({{hot, 1000}, {cold, 1000}},
                                                    /*watermark=*/0.9));
    UUID client_id = MountSegment(service, /*size=*/8192);

    for (int i = 0; i < 4; ++i) {
        PutComplete(service, client_id, "hot-" + std::to_string(i), hot, 240);
        PutComplete(service, client_id, "cold-" + std::to_string(i), cold, 100);
    }
    ASSERT_EQ(Snapshot(service, hot).charged_bytes, 960u);   // 0.96
    ASSERT_EQ(Snapshot(service, cold).charged_bytes, 400u);  // 0.40

    RunTenantEvictionPass(service);

    EXPECT_LE(Snapshot(service, hot).charged_bytes, 850u);
    EXPECT_EQ(Snapshot(service, cold).charged_bytes, 400u)
        << "a tenant under its own watermark must not pay for a noisy one";
}

}  // namespace mooncake::test

// Unit tests for dynamic MEMORY replica fanout on hot reads.

#include "master_service.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace mooncake::test {

class DynamicReplicationTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("DynamicReplicationTest");
        FLAGS_logtostderr = true;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }

    static constexpr size_t kDefaultSegmentSize = 16 * 1024 * 1024;

    struct MountedSegmentContext {
        UUID client_id;
        UUID segment_id;
        std::string segment_name;
    };

    MountedSegmentContext PrepareSegment(MasterService& service,
                                         std::string name, size_t base) const {
        Segment segment;
        segment.id = generate_uuid();
        segment.name = std::move(name);
        segment.base = base;
        segment.size = kDefaultSegmentSize;
        segment.te_endpoint = segment.name;
        UUID client_id = generate_uuid();
        auto mount_result = service.MountSegment(segment, client_id);
        EXPECT_TRUE(mount_result.has_value());
        return {.client_id = client_id,
                .segment_id = segment.id,
                .segment_name = segment.name};
    }

    void PutObject(MasterService& service, const UUID& client_id,
                   const std::string& key,
                   const std::string& preferred_segment) const {
        ReplicateConfig config;
        config.replica_num = 1;
        config.preferred_segment = preferred_segment;
        auto put_start =
            service.PutStart(client_id, key, TenantId::Default(), 1024, config);
        ASSERT_TRUE(put_start.has_value());
        auto put_end = service.PutEnd(client_id, key, TenantId::Default(),
                                      ReplicaType::MEMORY);
        ASSERT_TRUE(put_end.has_value());
    }

    void AdmitDynamicReplication(MasterService& service,
                                 const std::string& key) const {
        for (uint32_t i = 0; i < service.DynamicReplicationAdmissionMinHits();
             ++i) {
            ObserveDynamicReplicationAccess(service, key);
        }
    }

    std::vector<TaskAssignment> WaitForTasks(MasterService& service,
                                             const UUID& client_id,
                                             size_t expected) const {
        std::vector<TaskAssignment> last;
        for (int i = 0; i < 100; ++i) {
            auto tasks = service.FetchTasks(client_id, 16);
            EXPECT_TRUE(tasks.has_value());
            if (!tasks.has_value()) {
                return last;
            }
            if (tasks->size() >= expected) {
                return *tasks;
            }
            last = *tasks;
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return last;
    }

    ReplicaActionProposal BuildProposal(
        MasterService& service, const std::string& key,
        const std::optional<std::string>& preferred_target_segment =
            std::nullopt,
        bool admit = true) const {
        ReplicaActionProposal proposal;
        proposal.action = ReplicaActionType::ADD;
        proposal.proposal_id = generate_uuid();
        proposal.tenant_id = TenantId::Default().value();
        proposal.key = key;
        proposal.expire_at_ms_epoch = MasterService::DynamicReplicationNowMs() +
                                      std::chrono::seconds(30).count() * 1000;
        if (preferred_target_segment.has_value()) {
            proposal.preferred_target_segment = *preferred_target_segment;
        }

        MasterService::MetadataAccessorRO accessor(
            &service, MasterService::ObjectIdentity{TenantId::Default(), key});
        EXPECT_TRUE(accessor.Exists());
        if (accessor.Exists()) {
            const auto& metadata = accessor.Get();
            proposal.observed_version_epoch =
                service.DynamicReplicationVersionEpoch(metadata);
            proposal.object_size_bytes = static_cast<uint64_t>(metadata.size);
        }
        if (admit) {
            AdmitDynamicReplication(service, key);
        }
        return proposal;
    }

    size_t DynamicReplicaCount(MasterService& service,
                               const std::string& key) const {
        MasterService::MetadataAccessorRO accessor(
            &service, MasterService::ObjectIdentity{TenantId::Default(), key});
        EXPECT_TRUE(accessor.Exists());
        return accessor.Exists() ? accessor.Get().DynamicReplicaCount() : 0;
    }

    bool HasCompleteDynamicReplica(MasterService& service,
                                   const std::string& key,
                                   const std::string& target_segment) const {
        MasterService::MetadataAccessorRO accessor(
            &service, MasterService::ObjectIdentity{TenantId::Default(), key});
        EXPECT_TRUE(accessor.Exists());
        if (!accessor.Exists()) {
            return false;
        }
        for (const auto& [replica_id, record] :
             accessor.Get().dynamic_replicas) {
            (void)replica_id;
            if (record.target_segment == target_segment && record.complete) {
                return true;
            }
        }
        return false;
    }

    bool HasIncompleteDynamicReplica(MasterService& service,
                                     const std::string& key,
                                     const std::string& target_segment) const {
        MasterService::MetadataAccessorRO accessor(
            &service, MasterService::ObjectIdentity{TenantId::Default(), key});
        EXPECT_TRUE(accessor.Exists());
        if (!accessor.Exists()) {
            return false;
        }
        return std::any_of(accessor.Get().dynamic_replicas.begin(),
                           accessor.Get().dynamic_replicas.end(),
                           [&target_segment](const auto& entry) {
                               return entry.second.target_segment ==
                                          target_segment &&
                                      !entry.second.complete;
                           });
    }

    void BumpVersionEpoch(MasterService& service,
                          const std::string& key) const {
        MasterService::MetadataAccessorRW accessor(
            &service, MasterService::ObjectIdentity{TenantId::Default(), key});
        ASSERT_TRUE(accessor.Exists());
        accessor.Get().put_start_time += std::chrono::milliseconds(1);
    }

    bool HasDynamicState(MasterService& service, const std::string& key) const {
        MasterService::MetadataAccessorRW accessor(
            &service, MasterService::ObjectIdentity{TenantId::Default(), key});
        auto& tenant_state = accessor.GetTenantState();
        const bool has_lease = std::any_of(
            tenant_state.dynamic_replication_leases.begin(),
            tenant_state.dynamic_replication_leases.end(),
            [&key](const auto& entry) { return entry.second.key == key; });
        return tenant_state.dynamic_replication_pending.contains(key) ||
               tenant_state.dynamic_replication_cooldowns.contains(key) ||
               has_lease;
    }

    size_t DynamicReplicationWindowEntryLimit() const {
        return MasterService::kDynamicReplicationWindowEntryLimit;
    }

    size_t DynamicReplicationWindowCount(MasterService& service) const {
        return service.dynamic_replication_windows_.size();
    }

    void ExpireDynamicReplicationWindows(MasterService& service) const {
        const auto stale_start =
            std::chrono::steady_clock::now() - std::chrono::seconds(3);
        for (auto& [_, window] : service.dynamic_replication_windows_) {
            window.window_start = stale_start;
        }
    }

    void ClearDynamicReplicationState(MasterService& service,
                                      const std::string& key) const {
        MasterService::MetadataAccessorRW accessor(
            &service, MasterService::ObjectIdentity{TenantId::Default(), key});
        ASSERT_TRUE(accessor.Exists());
        service.ClearDynamicReplicationStateForKey(accessor.GetTenantState(),
                                                   key);
    }

    void DiscardExpiredProcessingReplicas(MasterService& service,
                                          const std::string& key) const {
        const size_t shard_idx =
            service.getMetadataShardIndex(TenantId::Default(), key);
        MasterService::MetadataShardAccessorRW shard(&service, shard_idx);
        service.DiscardExpiredProcessingReplicas(
            shard, std::chrono::system_clock::now() + std::chrono::seconds(1));
    }

    bool ObserveDynamicReplicationAccess(MasterService& service,
                                         const std::string& key) const {
        return service.ObserveDynamicReplicationAccess(
            MasterService::ObjectIdentity{TenantId::Default(), key});
    }

    size_t EvictReplicaOnSegment(MasterService& service, const std::string& key,
                                 const std::string& target_segment) const {
        MasterService::MetadataAccessorRW accessor(
            &service, MasterService::ObjectIdentity{TenantId::Default(), key});
        EXPECT_TRUE(accessor.Exists());
        if (!accessor.Exists()) {
            return 0;
        }
        std::vector<ReplicaID> erased_replica_ids;
        return service.EraseReplicasWithCacheTotalAccounting(
            accessor.Get(),
            [&target_segment](const Replica& replica) {
                if (!replica.is_memory_replica()) {
                    return false;
                }
                const auto& segment_names = replica.get_segment_names();
                return std::any_of(segment_names.begin(), segment_names.end(),
                                   [&target_segment](const auto& name) {
                                       return name && *name == target_segment;
                                   });
            },
            &erased_replica_ids);
    }

    void ExpireDynamicPending(MasterService& service,
                              const std::string& key) const {
        MasterService::MetadataAccessorRW accessor(
            &service, MasterService::ObjectIdentity{TenantId::Default(), key});
        auto& tenant_state = accessor.GetTenantState();
        auto pending_it = tenant_state.dynamic_replication_pending.find(key);
        ASSERT_NE(pending_it, tenant_state.dynamic_replication_pending.end());
        pending_it->second.expire_at_ms_epoch = 1;
    }
};

TEST_F(DynamicReplicationTest,
       EnforceQueuesCopyFromProposalAboveFrequencyThreshold) {
    MasterServiceConfig config;
    config.dynamic_replication_mode = "enforce";
    config.dynamic_replication_heat_window_seconds = 10;
    config.dynamic_replication_admission_qps_threshold = 0.2;
    config.dynamic_replication_max_memory_replicas = 2;
    config.default_kv_lease_ttl = 2000;
    MasterService service(config);

    auto source = PrepareSegment(service, "segment_0", 0x300000000);
    auto target = PrepareSegment(service, "segment_1", 0x400000000);
    PutObject(service, source.client_id, "hot-key", source.segment_name);

    auto no_task = service.FetchTasks(source.client_id, 16);
    ASSERT_TRUE(no_task.has_value());
    EXPECT_TRUE(no_task->empty());

    auto lease =
        service.SubmitReplicaActionProposal(BuildProposal(service, "hot-key"));
    ASSERT_TRUE(lease.has_value());
    auto tasks = WaitForTasks(service, source.client_id, 1);
    ASSERT_EQ(tasks.size(), 1u);
    EXPECT_EQ(tasks.front().type, TaskType::REPLICA_COPY);

    ReplicaCopyPayload payload;
    struct_json::from_json(payload, tasks.front().payload);
    EXPECT_EQ(payload.key, "hot-key");
    EXPECT_EQ(payload.source, source.segment_name);
    ASSERT_EQ(payload.targets.size(), 1u);
    EXPECT_EQ(payload.targets.front(), target.segment_name);
}

TEST_F(DynamicReplicationTest, EnforceGetPathQueuesCopyAfterHeatThreshold) {
    MasterServiceConfig config;
    config.dynamic_replication_mode = "enforce";
    config.dynamic_replication_heat_window_seconds = 10;
    config.dynamic_replication_admission_qps_threshold = 0.2;
    config.dynamic_replication_max_memory_replicas = 2;
    MasterService service(config);

    auto source = PrepareSegment(service, "segment_0", 0x410000000);
    auto target = PrepareSegment(service, "segment_1", 0x420000000);
    PutObject(service, source.client_id, "auto-hot-key", source.segment_name);

    ASSERT_TRUE(service.GetReplicaList("auto-hot-key", TenantId::Default())
                    .has_value());
    ASSERT_TRUE(service.GetReplicaList("auto-hot-key", TenantId::Default())
                    .has_value());

    auto tasks = WaitForTasks(service, source.client_id, 1);
    ASSERT_EQ(tasks.size(), 1u);
    EXPECT_EQ(tasks.front().type, TaskType::REPLICA_COPY);

    ReplicaCopyPayload payload;
    struct_json::from_json(payload, tasks.front().payload);
    EXPECT_EQ(payload.key, "auto-hot-key");
    EXPECT_EQ(payload.source, source.segment_name);
    ASSERT_EQ(payload.targets.size(), 1u);
    EXPECT_EQ(payload.targets.front(), target.segment_name);
}

TEST_F(DynamicReplicationTest,
       EnforceBatchGetPathQueuesCopyAfterHeatThreshold) {
    MasterServiceConfig config;
    config.dynamic_replication_mode = "enforce";
    config.dynamic_replication_heat_window_seconds = 10;
    config.dynamic_replication_admission_qps_threshold = 0.2;
    config.dynamic_replication_max_memory_replicas = 2;
    MasterService service(config);

    auto source = PrepareSegment(service, "segment_0", 0xea0000000);
    auto target = PrepareSegment(service, "segment_1", 0xeb0000000);
    PutObject(service, source.client_id, "batch-auto-hot-key",
              source.segment_name);

    auto first_batch = service.BatchGetReplicaList({"batch-auto-hot-key"},
                                                   TenantId::Default());
    ASSERT_EQ(first_batch.size(), 1u);
    ASSERT_TRUE(first_batch.front().has_value());
    auto second_batch = service.BatchGetReplicaList({"batch-auto-hot-key"},
                                                    TenantId::Default());
    ASSERT_EQ(second_batch.size(), 1u);
    ASSERT_TRUE(second_batch.front().has_value());

    auto tasks = WaitForTasks(service, source.client_id, 1);
    ASSERT_EQ(tasks.size(), 1u);
    ReplicaCopyPayload payload;
    struct_json::from_json(payload, tasks.front().payload);
    EXPECT_EQ(payload.key, "batch-auto-hot-key");
    EXPECT_EQ(payload.source, source.segment_name);
    ASSERT_EQ(payload.targets.size(), 1u);
    EXPECT_EQ(payload.targets.front(), target.segment_name);
}

TEST_F(DynamicReplicationTest, ObserveModeDoesNotQueueCopy) {
    MasterServiceConfig config;
    config.dynamic_replication_mode = "observe";
    config.dynamic_replication_heat_window_seconds = 10;
    config.dynamic_replication_admission_qps_threshold = 0.1;
    config.dynamic_replication_max_memory_replicas = 2;
    MasterService service(config);

    auto source = PrepareSegment(service, "segment_0", 0x500000000);
    (void)PrepareSegment(service, "segment_1", 0x600000000);
    PutObject(service, source.client_id, "observe-key", source.segment_name);

    for (int i = 0; i < 3; ++i) {
        auto resp = service.GetReplicaList("observe-key", TenantId::Default());
        ASSERT_TRUE(resp.has_value());
    }

    auto tasks = service.FetchTasks(source.client_id, 16);
    ASSERT_TRUE(tasks.has_value());
    EXPECT_TRUE(tasks->empty());
}

TEST_F(DynamicReplicationTest, MaxReplicaLimitSuppressesFanout) {
    MasterServiceConfig config;
    config.dynamic_replication_mode = "enforce";
    config.dynamic_replication_heat_window_seconds = 10;
    config.dynamic_replication_admission_qps_threshold = 0.1;
    config.dynamic_replication_max_memory_replicas = 1;
    MasterService service(config);

    auto source = PrepareSegment(service, "segment_0", 0x700000000);
    (void)PrepareSegment(service, "segment_1", 0x800000000);
    PutObject(service, source.client_id, "max-key", source.segment_name);

    auto lease =
        service.SubmitReplicaActionProposal(BuildProposal(service, "max-key"));
    ASSERT_FALSE(lease.has_value());
    auto tasks = service.FetchTasks(source.client_id, 16);
    ASSERT_TRUE(tasks.has_value());
    EXPECT_TRUE(tasks->empty());
}

TEST_F(DynamicReplicationTest, BelowFrequencyThresholdDoesNotQueueCopy) {
    MasterServiceConfig config;
    config.dynamic_replication_mode = "enforce";
    config.dynamic_replication_heat_window_seconds = 10;
    config.dynamic_replication_admission_qps_threshold = 0.3;
    config.dynamic_replication_max_memory_replicas = 2;
    MasterService service(config);

    auto source = PrepareSegment(service, "segment_0", 0x900000000);
    (void)PrepareSegment(service, "segment_1", 0xa00000000);
    PutObject(service, source.client_id, "warm-key", source.segment_name);

    auto proposal = BuildProposal(service, "warm-key", std::nullopt, false);
    auto lease = service.SubmitReplicaActionProposal(proposal);
    ASSERT_FALSE(lease.has_value());

    auto tasks = service.FetchTasks(source.client_id, 16);
    ASSERT_TRUE(tasks.has_value());
    EXPECT_TRUE(tasks->empty());
}

TEST_F(DynamicReplicationTest, RejectDomainHintsBeforeDomainAwarePlacement) {
    MasterServiceConfig config;
    config.dynamic_replication_mode = "enforce";
    config.dynamic_replication_heat_window_seconds = 10;
    config.dynamic_replication_admission_qps_threshold = 0.2;
    config.dynamic_replication_max_memory_replicas = 2;
    MasterService service(config);

    auto source = PrepareSegment(service, "segment_0", 0x870000000);
    PrepareSegment(service, "segment_1", 0x880000000);
    PutObject(service, source.client_id, "domain-hint-key",
              source.segment_name);

    auto proposal = BuildProposal(service, "domain-hint-key");
    proposal.target_domain = "domain-a";

    auto lease = service.SubmitReplicaActionProposal(proposal);
    ASSERT_FALSE(lease.has_value());
    EXPECT_EQ(lease.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(DynamicReplicationTest, LeaseDeadlineDoesNotExceedProposalDeadline) {
    MasterServiceConfig config;
    config.dynamic_replication_mode = "enforce";
    config.dynamic_replication_heat_window_seconds = 10;
    config.dynamic_replication_admission_qps_threshold = 0.2;
    config.dynamic_replication_max_memory_replicas = 2;
    MasterService service(config);

    auto source = PrepareSegment(service, "segment_0", 0x890000000);
    PrepareSegment(service, "segment_1", 0x8A0000000);
    PutObject(service, source.client_id, "short-deadline-key",
              source.segment_name);

    auto proposal = BuildProposal(service, "short-deadline-key");
    proposal.expire_at_ms_epoch =
        static_cast<int64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count()) +
        std::chrono::seconds(1).count() * 1000;

    auto lease = service.SubmitReplicaActionProposal(proposal);
    ASSERT_TRUE(lease.has_value());
    EXPECT_LE(lease->expire_at_ms_epoch, proposal.expire_at_ms_epoch);
}

TEST_F(DynamicReplicationTest, ProposalIdempotencyReturnsSameLease) {
    MasterServiceConfig config;
    config.dynamic_replication_mode = "enforce";
    config.dynamic_replication_max_memory_replicas = 2;
    MasterService service(config);

    auto source = PrepareSegment(service, "segment_0", 0xb00000000);
    auto target = PrepareSegment(service, "segment_1", 0xc00000000);
    PutObject(service, source.client_id, "proposal-key", source.segment_name);
    auto proposal = BuildProposal(service, "proposal-key", target.segment_name);

    auto first = service.SubmitReplicaActionProposal(proposal);
    ASSERT_TRUE(first.has_value());
    auto second = service.SubmitReplicaActionProposal(proposal);
    ASSERT_TRUE(second.has_value());

    EXPECT_EQ(second->proposal_id, first->proposal_id);
    EXPECT_EQ(second->lease_id, first->lease_id);
    EXPECT_EQ(second->task_id, first->task_id);
    EXPECT_EQ(second->source_segment, source.segment_name);
    EXPECT_EQ(second->target_segment, target.segment_name);

    auto tasks = service.FetchTasks(source.client_id, 16);
    ASSERT_TRUE(tasks.has_value());
    ASSERT_EQ(tasks->size(), 1u);
    EXPECT_EQ(tasks->front().id, first->task_id);
}

TEST_F(DynamicReplicationTest, ProposalIdempotencyRejectsConflictingRequest) {
    MasterServiceConfig config;
    config.dynamic_replication_mode = "enforce";
    config.dynamic_replication_max_memory_replicas = 2;
    MasterService service(config);

    auto source = PrepareSegment(service, "segment_0", 0xc10000000);
    auto target = PrepareSegment(service, "segment_1", 0xc20000000);
    auto other_target = PrepareSegment(service, "segment_2", 0xc30000000);
    PutObject(service, source.client_id, "proposal-key", source.segment_name);
    auto proposal = BuildProposal(service, "proposal-key", target.segment_name);

    auto first = service.SubmitReplicaActionProposal(proposal);
    ASSERT_TRUE(first.has_value());

    auto conflicting = proposal;
    conflicting.preferred_target_segment = other_target.segment_name;
    auto second = service.SubmitReplicaActionProposal(conflicting);
    ASSERT_FALSE(second.has_value());
    EXPECT_EQ(second.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(DynamicReplicationTest, CopyLifecycleMarksDynamicReplicaComplete) {
    MasterServiceConfig config;
    config.dynamic_replication_mode = "enforce";
    config.dynamic_replication_max_memory_replicas = 2;
    MasterService service(config);

    auto source = PrepareSegment(service, "segment_0", 0xd00000000);
    auto target = PrepareSegment(service, "segment_1", 0xe00000000);
    PutObject(service, source.client_id, "copy-key", source.segment_name);
    auto proposal = BuildProposal(service, "copy-key", target.segment_name);

    auto lease = service.SubmitReplicaActionProposal(proposal);
    ASSERT_TRUE(lease.has_value());
    auto copy_start =
        service.CopyStart(source.client_id, "copy-key", TenantId::Default(),
                          lease->source_segment, {lease->target_segment},
                          lease->lease_id, lease->version_epoch);
    ASSERT_TRUE(copy_start.has_value());
    EXPECT_EQ(DynamicReplicaCount(service, "copy-key"), 1u);
    EXPECT_FALSE(
        HasCompleteDynamicReplica(service, "copy-key", lease->target_segment));

    auto copy_end =
        service.CopyEnd(source.client_id, "copy-key", TenantId::Default(),
                        lease->lease_id, lease->version_epoch);
    ASSERT_TRUE(copy_end.has_value());
    EXPECT_EQ(DynamicReplicaCount(service, "copy-key"), 1u);
    EXPECT_TRUE(
        HasCompleteDynamicReplica(service, "copy-key", lease->target_segment));
    EXPECT_FALSE(HasDynamicState(service, "copy-key"));
}

TEST_F(DynamicReplicationTest,
       CopyEndForInvalidTargetClearsIncompleteDynamicReplica) {
    MasterServiceConfig config;
    config.dynamic_replication_mode = "enforce";
    config.dynamic_replication_max_memory_replicas = 2;
    MasterService service(config);

    auto source = PrepareSegment(service, "segment_0", 0xd70000000);
    auto target = PrepareSegment(service, "segment_1", 0xd80000000);
    PutObject(service, source.client_id, "invalid-target-key",
              source.segment_name);

    auto proposal =
        BuildProposal(service, "invalid-target-key", target.segment_name);
    auto lease = service.SubmitReplicaActionProposal(proposal);
    ASSERT_TRUE(lease.has_value());

    auto copy_start = service.CopyStart(
        source.client_id, "invalid-target-key", TenantId::Default(),
        lease->source_segment, {lease->target_segment}, lease->lease_id,
        lease->version_epoch);
    ASSERT_TRUE(copy_start.has_value());
    ASSERT_TRUE(HasIncompleteDynamicReplica(service, "invalid-target-key",
                                            lease->target_segment));

    ASSERT_TRUE(service.UnmountSegment(target.segment_id, target.client_id)
                    .has_value());

    auto copy_end = service.CopyEnd(source.client_id, "invalid-target-key",
                                    TenantId::Default(), lease->lease_id,
                                    lease->version_epoch);
    ASSERT_FALSE(copy_end.has_value());
    EXPECT_EQ(copy_end.error(), ErrorCode::REPLICA_IS_GONE);
    EXPECT_FALSE(HasIncompleteDynamicReplica(service, "invalid-target-key",
                                             lease->target_segment));
    EXPECT_FALSE(HasDynamicState(service, "invalid-target-key"));
}

TEST_F(DynamicReplicationTest, ObserveWindowDropsNewKeysAtEntryLimit) {
    MasterServiceConfig config;
    config.dynamic_replication_mode = "observe";
    MasterService service(config);

    for (size_t i = 0; i < DynamicReplicationWindowEntryLimit(); ++i) {
        ObserveDynamicReplicationAccess(service,
                                        "window-key-" + std::to_string(i));
    }
    EXPECT_EQ(DynamicReplicationWindowCount(service),
              DynamicReplicationWindowEntryLimit());

    ObserveDynamicReplicationAccess(service, "window-overflow-key");
    EXPECT_EQ(DynamicReplicationWindowCount(service),
              DynamicReplicationWindowEntryLimit());
}

TEST_F(DynamicReplicationTest, ObserveWindowCleanupIsBoundedAtLimit) {
    MasterServiceConfig config;
    config.dynamic_replication_mode = "observe";
    config.dynamic_replication_heat_window_seconds = 1;
    MasterService service(config);

    for (size_t i = 0; i < DynamicReplicationWindowEntryLimit(); ++i) {
        ObserveDynamicReplicationAccess(
            service, "stale-window-key-" + std::to_string(i));
    }
    ASSERT_EQ(DynamicReplicationWindowCount(service),
              DynamicReplicationWindowEntryLimit());

    ExpireDynamicReplicationWindows(service);

    ObserveDynamicReplicationAccess(service, "fresh-window-key");
    EXPECT_LT(DynamicReplicationWindowCount(service),
              DynamicReplicationWindowEntryLimit());
    EXPECT_GT(DynamicReplicationWindowCount(service), 1u);
}

TEST_F(DynamicReplicationTest,
       DynamicCopyStartRejectsStaleLeaseWithoutPending) {
    MasterServiceConfig config;
    config.dynamic_replication_mode = "enforce";
    config.dynamic_replication_max_memory_replicas = 2;
    MasterService service(config);

    auto source = PrepareSegment(service, "segment_0", 0xdd0000000);
    auto target = PrepareSegment(service, "segment_1", 0xde0000000);
    PutObject(service, source.client_id, "stale-dynamic-task-key",
              source.segment_name);

    auto lease = service.SubmitReplicaActionProposal(
        BuildProposal(service, "stale-dynamic-task-key", target.segment_name));
    ASSERT_TRUE(lease.has_value());
    auto tasks = service.FetchTasks(source.client_id, 16);
    ASSERT_TRUE(tasks.has_value());
    ASSERT_EQ(tasks->size(), 1u);

    ReplicaCopyPayload payload;
    struct_json::from_json(payload, tasks->front().payload);
    EXPECT_EQ(UUID(payload.dynamic_replication_lease_id_high,
                   payload.dynamic_replication_lease_id_low),
              lease->lease_id);
    EXPECT_EQ(payload.dynamic_replication_version_epoch, lease->version_epoch);

    ClearDynamicReplicationState(service, "stale-dynamic-task-key");

    auto copy_start =
        service.CopyStart(source.client_id, "stale-dynamic-task-key",
                          TenantId::Default(), payload.source, payload.targets,
                          UUID(payload.dynamic_replication_lease_id_high,
                               payload.dynamic_replication_lease_id_low),
                          payload.dynamic_replication_version_epoch);
    ASSERT_FALSE(copy_start.has_value());
    EXPECT_EQ(copy_start.error(), ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    EXPECT_EQ(DynamicReplicaCount(service, "stale-dynamic-task-key"), 0u);
}

TEST_F(DynamicReplicationTest, DynamicCopyStartRejectsNonSourceClient) {
    MasterServiceConfig config;
    config.dynamic_replication_mode = "enforce";
    config.dynamic_replication_admission_qps_threshold = 0.1;
    MasterService service(config);

    auto source = PrepareSegment(service, "segment_0", 0xb10000000);
    auto target = PrepareSegment(service, "segment_1", 0xb20000000);
    auto other = PrepareSegment(service, "segment_2", 0xb30000000);
    PutObject(service, source.client_id, "source-client-key",
              source.segment_name);

    auto lease = service.SubmitReplicaActionProposal(
        BuildProposal(service, "source-client-key", target.segment_name));
    ASSERT_TRUE(lease.has_value());

    auto rejected = service.CopyStart(other.client_id, "source-client-key",
                                      TenantId::Default(), source.segment_name,
                                      {target.segment_name}, lease->lease_id,
                                      lease->version_epoch);
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error(), ErrorCode::ILLEGAL_CLIENT);

    auto start = service.CopyStart(source.client_id, "source-client-key",
                                   TenantId::Default(), source.segment_name,
                                   {target.segment_name}, lease->lease_id,
                                   lease->version_epoch);
    ASSERT_TRUE(start.has_value());
    auto revoke = service.CopyRevoke(source.client_id, "source-client-key",
                                     TenantId::Default(), lease->lease_id,
                                     lease->version_epoch);
    ASSERT_TRUE(revoke.has_value());
}

TEST_F(DynamicReplicationTest, DynamicCopyStartFailureClearsPending) {
    MasterServiceConfig config;
    config.dynamic_replication_mode = "enforce";
    config.dynamic_replication_admission_qps_threshold = 0.1;
    MasterService service(config);

    auto source = PrepareSegment(service, "segment_0", 0xc10000000);
    auto target = PrepareSegment(service, "segment_1", 0xc20000000);
    auto fallback = PrepareSegment(service, "segment_2", 0xc30000000);
    PutObject(service, source.client_id, "copy-start-fail-key",
              source.segment_name);

    auto lease = service.SubmitReplicaActionProposal(
        BuildProposal(service, "copy-start-fail-key", target.segment_name));
    ASSERT_TRUE(lease.has_value());
    ASSERT_TRUE(service.UnmountSegment(target.segment_id, target.client_id)
                    .has_value());

    auto failed = service.CopyStart(source.client_id, "copy-start-fail-key",
                                    TenantId::Default(), source.segment_name,
                                    {target.segment_name}, lease->lease_id,
                                    lease->version_epoch);
    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(failed.error(), ErrorCode::SEGMENT_NOT_FOUND);

    auto regular = service.CopyStart(source.client_id, "copy-start-fail-key",
                                     TenantId::Default(), source.segment_name,
                                     {fallback.segment_name});
    ASSERT_TRUE(regular.has_value());
    auto revoke = service.CopyRevoke(source.client_id, "copy-start-fail-key",
                                     TenantId::Default());
    ASSERT_TRUE(revoke.has_value());
}

TEST_F(DynamicReplicationTest,
       StaleDynamicCopyStartDoesNotClearCurrentPending) {
    MasterServiceConfig config;
    config.dynamic_replication_mode = "enforce";
    config.dynamic_replication_max_memory_replicas = 3;
    MasterService service(config);

    auto source = PrepareSegment(service, "segment_0", 0xdf0000000);
    auto stale_target = PrepareSegment(service, "segment_1", 0xe00000000);
    auto current_target = PrepareSegment(service, "segment_2", 0xe10000000);
    PutObject(service, source.client_id, "stale-vs-current-key",
              source.segment_name);

    auto stale_lease = service.SubmitReplicaActionProposal(BuildProposal(
        service, "stale-vs-current-key", stale_target.segment_name));
    ASSERT_TRUE(stale_lease.has_value());
    auto stale_tasks = service.FetchTasks(source.client_id, 16);
    ASSERT_TRUE(stale_tasks.has_value());
    ASSERT_EQ(stale_tasks->size(), 1u);
    ReplicaCopyPayload stale_payload;
    struct_json::from_json(stale_payload, stale_tasks->front().payload);

    ClearDynamicReplicationState(service, "stale-vs-current-key");

    auto current_lease = service.SubmitReplicaActionProposal(BuildProposal(
        service, "stale-vs-current-key", current_target.segment_name));
    ASSERT_TRUE(current_lease.has_value());

    auto stale_copy_start = service.CopyStart(
        source.client_id, "stale-vs-current-key", TenantId::Default(),
        stale_payload.source, stale_payload.targets,
        UUID(stale_payload.dynamic_replication_lease_id_high,
             stale_payload.dynamic_replication_lease_id_low),
        stale_payload.dynamic_replication_version_epoch);
    ASSERT_FALSE(stale_copy_start.has_value());
    EXPECT_EQ(stale_copy_start.error(), ErrorCode::INVALID_VERSION);
    EXPECT_TRUE(HasDynamicState(service, "stale-vs-current-key"));

    auto current_copy_start = service.CopyStart(
        source.client_id, "stale-vs-current-key", TenantId::Default(),
        current_lease->source_segment, {current_lease->target_segment},
        current_lease->lease_id, current_lease->version_epoch);
    ASSERT_TRUE(current_copy_start.has_value());
    EXPECT_TRUE(HasIncompleteDynamicReplica(service, "stale-vs-current-key",
                                            current_lease->target_segment));
}

TEST_F(DynamicReplicationTest, ExpiredDynamicCopyTaskClearsDynamicState) {
    MasterServiceConfig config;
    config.dynamic_replication_mode = "enforce";
    config.dynamic_replication_max_memory_replicas = 2;
    config.put_start_discard_timeout_sec = 0;
    config.put_start_release_timeout_sec = 1;
    MasterService service(config);

    auto source = PrepareSegment(service, "segment_0", 0xe10000000);
    auto target = PrepareSegment(service, "segment_1", 0xe20000000);
    PutObject(service, source.client_id, "expired-copy-task-key",
              source.segment_name);

    auto lease = service.SubmitReplicaActionProposal(
        BuildProposal(service, "expired-copy-task-key", target.segment_name));
    ASSERT_TRUE(lease.has_value());
    auto copy_start = service.CopyStart(
        source.client_id, "expired-copy-task-key", TenantId::Default(),
        lease->source_segment, {lease->target_segment}, lease->lease_id,
        lease->version_epoch);
    ASSERT_TRUE(copy_start.has_value());
    ASSERT_TRUE(HasIncompleteDynamicReplica(service, "expired-copy-task-key",
                                            lease->target_segment));

    DiscardExpiredProcessingReplicas(service, "expired-copy-task-key");

    EXPECT_EQ(DynamicReplicaCount(service, "expired-copy-task-key"), 0u);
    EXPECT_FALSE(HasDynamicState(service, "expired-copy-task-key"));

    auto retry = service.SubmitReplicaActionProposal(
        BuildProposal(service, "expired-copy-task-key", target.segment_name));
    ASSERT_FALSE(retry.has_value());
    EXPECT_EQ(retry.error(), ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
}

TEST_F(DynamicReplicationTest, DynamicCopyEndRejectsMismatchedLease) {
    MasterServiceConfig config;
    config.dynamic_replication_mode = "enforce";
    config.dynamic_replication_max_memory_replicas = 2;
    MasterService service(config);

    auto source = PrepareSegment(service, "segment_0", 0xe30000000);
    auto target = PrepareSegment(service, "segment_1", 0xe40000000);
    PutObject(service, source.client_id, "copy-end-fence-key",
              source.segment_name);

    auto lease = service.SubmitReplicaActionProposal(
        BuildProposal(service, "copy-end-fence-key", target.segment_name));
    ASSERT_TRUE(lease.has_value());
    auto copy_start = service.CopyStart(
        source.client_id, "copy-end-fence-key", TenantId::Default(),
        lease->source_segment, {lease->target_segment}, lease->lease_id,
        lease->version_epoch);
    ASSERT_TRUE(copy_start.has_value());

    auto stale_end = service.CopyEnd(source.client_id, "copy-end-fence-key",
                                     TenantId::Default(), generate_uuid(),
                                     lease->version_epoch);
    ASSERT_FALSE(stale_end.has_value());
    EXPECT_EQ(stale_end.error(), ErrorCode::INVALID_VERSION);
    EXPECT_TRUE(HasIncompleteDynamicReplica(service, "copy-end-fence-key",
                                            lease->target_segment));

    auto copy_end = service.CopyEnd(source.client_id, "copy-end-fence-key",
                                    TenantId::Default(), lease->lease_id,
                                    lease->version_epoch);
    ASSERT_TRUE(copy_end.has_value());
    EXPECT_TRUE(HasCompleteDynamicReplica(service, "copy-end-fence-key",
                                          lease->target_segment));
}

TEST_F(DynamicReplicationTest, DynamicCopyRevokeRejectsMismatchedLease) {
    MasterServiceConfig config;
    config.dynamic_replication_mode = "enforce";
    config.dynamic_replication_max_memory_replicas = 2;
    MasterService service(config);

    auto source = PrepareSegment(service, "segment_0", 0xe50000000);
    auto target = PrepareSegment(service, "segment_1", 0xe60000000);
    PutObject(service, source.client_id, "copy-revoke-fence-key",
              source.segment_name);

    auto lease = service.SubmitReplicaActionProposal(
        BuildProposal(service, "copy-revoke-fence-key", target.segment_name));
    ASSERT_TRUE(lease.has_value());
    auto copy_start = service.CopyStart(
        source.client_id, "copy-revoke-fence-key", TenantId::Default(),
        lease->source_segment, {lease->target_segment}, lease->lease_id,
        lease->version_epoch);
    ASSERT_TRUE(copy_start.has_value());

    auto stale_revoke = service.CopyRevoke(
        source.client_id, "copy-revoke-fence-key", TenantId::Default(),
        generate_uuid(), lease->version_epoch);
    ASSERT_FALSE(stale_revoke.has_value());
    EXPECT_EQ(stale_revoke.error(), ErrorCode::INVALID_VERSION);
    EXPECT_TRUE(HasIncompleteDynamicReplica(service, "copy-revoke-fence-key",
                                            lease->target_segment));

    auto copy_revoke = service.CopyRevoke(
        source.client_id, "copy-revoke-fence-key", TenantId::Default(),
        lease->lease_id, lease->version_epoch);
    ASSERT_TRUE(copy_revoke.has_value());
    EXPECT_EQ(DynamicReplicaCount(service, "copy-revoke-fence-key"), 0u);
    EXPECT_FALSE(HasDynamicState(service, "copy-revoke-fence-key"));
}

TEST_F(DynamicReplicationTest, CopyStartRejectsVersionMismatch) {
    MasterServiceConfig config;
    config.dynamic_replication_mode = "enforce";
    config.dynamic_replication_max_memory_replicas = 2;
    MasterService service(config);

    auto source = PrepareSegment(service, "segment_0", 0xe10000000);
    auto target = PrepareSegment(service, "segment_1", 0xe20000000);
    PutObject(service, source.client_id, "version-key", source.segment_name);
    auto proposal = BuildProposal(service, "version-key", target.segment_name);

    auto lease = service.SubmitReplicaActionProposal(proposal);
    ASSERT_TRUE(lease.has_value());
    BumpVersionEpoch(service, "version-key");

    auto copy_start =
        service.CopyStart(source.client_id, "version-key", TenantId::Default(),
                          lease->source_segment, {lease->target_segment},
                          lease->lease_id, lease->version_epoch);
    ASSERT_FALSE(copy_start.has_value());
    EXPECT_EQ(copy_start.error(), ErrorCode::INVALID_VERSION);
    EXPECT_FALSE(HasDynamicState(service, "version-key"));
}

TEST_F(DynamicReplicationTest, ExpiredDynamicPendingDoesNotBlockRegularCopy) {
    MasterServiceConfig config;
    config.dynamic_replication_mode = "enforce";
    config.dynamic_replication_max_memory_replicas = 3;
    MasterService service(config);

    auto source = PrepareSegment(service, "segment_0", 0xe70000000);
    auto expired_target = PrepareSegment(service, "segment_1", 0xe80000000);
    auto regular_target = PrepareSegment(service, "segment_2", 0xe90000000);
    PutObject(service, source.client_id, "expired-pending-regular-copy-key",
              source.segment_name);

    auto lease = service.SubmitReplicaActionProposal(
        BuildProposal(service, "expired-pending-regular-copy-key",
                      expired_target.segment_name));
    ASSERT_TRUE(lease.has_value());
    ExpireDynamicPending(service, "expired-pending-regular-copy-key");

    auto copy_start =
        service.CopyStart(source.client_id, "expired-pending-regular-copy-key",
                          TenantId::Default(), source.segment_name,
                          {regular_target.segment_name});
    ASSERT_TRUE(copy_start.has_value());
    auto copy_revoke =
        service.CopyRevoke(source.client_id, "expired-pending-regular-copy-key",
                           TenantId::Default());
    ASSERT_TRUE(copy_revoke.has_value());
    EXPECT_FALSE(HasDynamicState(service, "expired-pending-regular-copy-key"));
}

TEST_F(DynamicReplicationTest, ExpiredLeaseRejectsCopyStart) {
    MasterServiceConfig config;
    config.dynamic_replication_mode = "enforce";
    config.dynamic_replication_max_memory_replicas = 2;
    MasterService service(config);

    auto source = PrepareSegment(service, "segment_0", 0xf00000000);
    auto target = PrepareSegment(service, "segment_1", 0x1000000000);
    PutObject(service, source.client_id, "expired-key", source.segment_name);
    auto proposal = BuildProposal(service, "expired-key", target.segment_name);

    auto lease = service.SubmitReplicaActionProposal(proposal);
    ASSERT_TRUE(lease.has_value());
    ExpireDynamicPending(service, "expired-key");

    auto copy_start =
        service.CopyStart(source.client_id, "expired-key", TenantId::Default(),
                          lease->source_segment, {lease->target_segment},
                          lease->lease_id, lease->version_epoch);
    ASSERT_FALSE(copy_start.has_value());
    EXPECT_EQ(copy_start.error(), ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    EXPECT_EQ(DynamicReplicaCount(service, "expired-key"), 0u);
    EXPECT_FALSE(HasDynamicState(service, "expired-key"));
}

TEST_F(DynamicReplicationTest, EvictedDynamicReplicaBlocksImmediateRecreate) {
    MasterServiceConfig config;
    config.dynamic_replication_mode = "enforce";
    config.dynamic_replication_admission_qps_threshold = 0.1;
    config.dynamic_replication_max_memory_replicas = 3;
    MasterService service(config);

    auto source = PrepareSegment(service, "segment_0", 0x1100000000);
    auto target = PrepareSegment(service, "segment_1", 0x1200000000);
    auto next_target = PrepareSegment(service, "segment_2", 0x1300000000);
    PutObject(service, source.client_id, "evicted-dynamic-key",
              source.segment_name);

    auto proposal =
        BuildProposal(service, "evicted-dynamic-key", target.segment_name);
    auto lease = service.SubmitReplicaActionProposal(proposal);
    ASSERT_TRUE(lease.has_value());
    auto copy_start = service.CopyStart(
        source.client_id, "evicted-dynamic-key", TenantId::Default(),
        lease->source_segment, {lease->target_segment}, lease->lease_id,
        lease->version_epoch);
    ASSERT_TRUE(copy_start.has_value());
    auto copy_end = service.CopyEnd(source.client_id, "evicted-dynamic-key",
                                    TenantId::Default(), lease->lease_id,
                                    lease->version_epoch);
    ASSERT_TRUE(copy_end.has_value());
    ASSERT_EQ(DynamicReplicaCount(service, "evicted-dynamic-key"), 1u);

    EXPECT_EQ(EvictReplicaOnSegment(service, "evicted-dynamic-key",
                                    target.segment_name),
              1u);
    EXPECT_EQ(DynamicReplicaCount(service, "evicted-dynamic-key"), 0u);

    auto immediate_recreate =
        BuildProposal(service, "evicted-dynamic-key", next_target.segment_name);
    auto rejected = service.SubmitReplicaActionProposal(immediate_recreate);
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error(), ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
}

TEST_F(DynamicReplicationTest, SourceSelectionUsesStableTieBreak) {
    MasterServiceConfig config;
    config.dynamic_replication_mode = "enforce";
    config.dynamic_replication_max_memory_replicas = 4;
    MasterService service(config);

    auto source0 = PrepareSegment(service, "segment_0", 0x1400000000);
    auto source1 = PrepareSegment(service, "segment_1", 0x1500000000);
    auto target = PrepareSegment(service, "segment_2", 0x1600000000);
    PutObject(service, source0.client_id, "source-fanout-key",
              source0.segment_name);

    auto copy_start = service.CopyStart(
        source0.client_id, "source-fanout-key", TenantId::Default(),
        source0.segment_name, {source1.segment_name});
    ASSERT_TRUE(copy_start.has_value());
    auto copy_end = service.CopyEnd(source0.client_id, "source-fanout-key",
                                    TenantId::Default());
    ASSERT_TRUE(copy_end.has_value());

    auto proposal =
        BuildProposal(service, "source-fanout-key", target.segment_name);
    auto lease = service.SubmitReplicaActionProposal(proposal);
    ASSERT_TRUE(lease.has_value());

    auto stable_score = [](std::string_view key, std::string_view segment) {
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
    };
    const auto score0 = stable_score("source-fanout-key", source0.segment_name);
    const auto score1 = stable_score("source-fanout-key", source1.segment_name);
    EXPECT_EQ(lease->source_segment,
              score0 < score1 ? source0.segment_name : source1.segment_name);
}

TEST_F(DynamicReplicationTest, TargetSelectionPrefersDifferentHost) {
    MasterServiceConfig config;
    config.dynamic_replication_mode = "enforce";
    config.dynamic_replication_max_memory_replicas = 2;
    MasterService service(config);

    Segment source_segment;
    source_segment.id = generate_uuid();
    source_segment.name = "source_segment";
    source_segment.base = 0x1700000000;
    source_segment.size = kDefaultSegmentSize;
    source_segment.te_endpoint = source_segment.name;
    source_segment.host_id = "host-a";
    UUID source_client = generate_uuid();
    ASSERT_TRUE(
        service.MountSegment(source_segment, source_client).has_value());

    Segment same_host_target;
    same_host_target.id = generate_uuid();
    same_host_target.name = "same_host_target";
    same_host_target.base = 0x1800000000;
    same_host_target.size = kDefaultSegmentSize;
    same_host_target.te_endpoint = same_host_target.name;
    same_host_target.host_id = "host-a";
    ASSERT_TRUE(
        service.MountSegment(same_host_target, generate_uuid()).has_value());

    Segment other_host_target;
    other_host_target.id = generate_uuid();
    other_host_target.name = "other_host_target";
    other_host_target.base = 0x1900000000;
    other_host_target.size = kDefaultSegmentSize;
    other_host_target.te_endpoint = other_host_target.name;
    other_host_target.host_id = "host-b";
    ASSERT_TRUE(
        service.MountSegment(other_host_target, generate_uuid()).has_value());

    PutObject(service, source_client, "target-host-key", source_segment.name);
    auto lease = service.SubmitReplicaActionProposal(
        BuildProposal(service, "target-host-key"));
    ASSERT_TRUE(lease.has_value());
    EXPECT_EQ(lease->target_segment, other_host_target.name);
}

}  // namespace mooncake::test

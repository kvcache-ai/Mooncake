// Unit tests for dynamic MEMORY replica fanout on hot reads.

#include "master_service.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <optional>
#include <string>
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
        std::string segment_name;
    };

    MountedSegmentContext PrepareSegment(MasterService& service,
                                         std::string name, size_t base,
                                         std::string domain = "domain-a",
                                         std::string host_id = {}) const {
        Segment segment;
        segment.id = generate_uuid();
        segment.name = std::move(name);
        segment.base = base;
        segment.size = kDefaultSegmentSize;
        segment.te_endpoint = segment.name;
        segment.domain = std::move(domain);
        segment.host_id = std::move(host_id);
        UUID client_id = generate_uuid();
        auto mount_result = service.MountSegment(segment, client_id);
        EXPECT_TRUE(mount_result.has_value());
        return {.client_id = client_id, .segment_name = segment.name};
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

    ReplicaActionProposal BuildProposal(
        MasterService& service, const std::string& key,
        const std::optional<std::string>& preferred_target_segment =
            std::nullopt) const {
        ReplicaActionProposal proposal;
        proposal.action = ReplicaActionType::ADD;
        proposal.proposal_id = generate_uuid();
        proposal.tenant_id = TenantId::Default().value();
        proposal.key = key;
        proposal.requester_domain = "domain-a";
        proposal.target_domain = "domain-a";
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
            proposal.access_frequency_qps = 1.0;
            proposal.hits = 10;
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
        const auto control_key =
            MasterService::DynamicReplicationControlKey(key, "domain-a");
        return tenant_state.dynamic_replication_pending.contains(control_key) ||
               tenant_state.dynamic_replication_cooldowns.contains(
                   control_key) ||
               has_lease;
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
        auto pending_it = tenant_state.dynamic_replication_pending.find(
            MasterService::DynamicReplicationControlKey(key, "domain-a"));
        ASSERT_NE(pending_it, tenant_state.dynamic_replication_pending.end());
        pending_it->second.expire_at_ms_epoch = 1;
    }

    bool ClearStateMatchesDomainControlKeyExactly(
        MasterService& service) const {
        MasterService::MetadataAccessorRW accessor(
            &service,
            MasterService::ObjectIdentity{TenantId::Default(), "bar"});
        auto& tenant_state = accessor.GetTenantState();
        const auto bar_control_key =
            MasterService::DynamicReplicationControlKey("bar", "domain-a");
        const auto foo_bar_control_key =
            MasterService::DynamicReplicationControlKey("foo:bar", "domain-a");
        tenant_state.dynamic_replication_cooldowns[bar_control_key] =
            std::chrono::steady_clock::now() + std::chrono::seconds(1);
        tenant_state.dynamic_replication_cooldowns[foo_bar_control_key] =
            std::chrono::steady_clock::now() + std::chrono::seconds(1);

        service.ClearDynamicReplicationStateForKey(tenant_state, "bar");

        return !tenant_state.dynamic_replication_cooldowns.contains(
                   bar_control_key) &&
               tenant_state.dynamic_replication_cooldowns.contains(
                   foo_bar_control_key);
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
    auto tasks = service.FetchTasks(source.client_id, 16);
    ASSERT_TRUE(tasks.has_value());
    ASSERT_EQ(tasks->size(), 1u);
    EXPECT_EQ(tasks->front().type, TaskType::REPLICA_COPY);

    ReplicaCopyPayload payload;
    struct_json::from_json(payload, tasks->front().payload);
    EXPECT_EQ(payload.key, "hot-key");
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

    auto proposal = BuildProposal(service, "warm-key");
    proposal.access_frequency_qps = 0.1;
    proposal.hits = 1;
    auto lease = service.SubmitReplicaActionProposal(proposal);
    ASSERT_FALSE(lease.has_value());

    auto tasks = service.FetchTasks(source.client_id, 16);
    ASSERT_TRUE(tasks.has_value());
    EXPECT_TRUE(tasks->empty());
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
                          lease->source_segment, {lease->target_segment});
    ASSERT_TRUE(copy_start.has_value());
    EXPECT_EQ(DynamicReplicaCount(service, "copy-key"), 1u);
    EXPECT_FALSE(
        HasCompleteDynamicReplica(service, "copy-key", lease->target_segment));

    auto copy_end =
        service.CopyEnd(source.client_id, "copy-key", TenantId::Default());
    ASSERT_TRUE(copy_end.has_value());
    EXPECT_EQ(DynamicReplicaCount(service, "copy-key"), 1u);
    EXPECT_TRUE(
        HasCompleteDynamicReplica(service, "copy-key", lease->target_segment));
    EXPECT_FALSE(HasDynamicState(service, "copy-key"));
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
                          lease->source_segment, {lease->target_segment});
    ASSERT_FALSE(copy_start.has_value());
    EXPECT_EQ(copy_start.error(), ErrorCode::INVALID_VERSION);
    EXPECT_FALSE(HasDynamicState(service, "version-key"));
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
                          lease->source_segment, {lease->target_segment});
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
        lease->source_segment, {lease->target_segment});
    ASSERT_TRUE(copy_start.has_value());
    auto copy_end = service.CopyEnd(source.client_id, "evicted-dynamic-key",
                                    TenantId::Default());
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

TEST_F(DynamicReplicationTest, TargetDomainChoosesSegmentInRequesterDomain) {
    MasterServiceConfig config;
    config.dynamic_replication_mode = "enforce";
    config.dynamic_replication_admission_qps_threshold = 0.1;
    config.dynamic_replication_max_memory_replicas = 3;
    MasterService service(config);

    auto source =
        PrepareSegment(service, "domain_a_source", 0x1400000000, "domain-a");
    (void)PrepareSegment(service, "domain_a_target", 0x1500000000, "domain-a");
    auto domain_b_target =
        PrepareSegment(service, "domain_b_target", 0x1600000000, "domain-b");
    PutObject(service, source.client_id, "domain-target-key",
              source.segment_name);

    auto proposal = BuildProposal(service, "domain-target-key");
    proposal.requester_domain = "domain-b";
    proposal.target_domain.clear();
    auto lease = service.SubmitReplicaActionProposal(proposal);
    ASSERT_TRUE(lease.has_value());
    EXPECT_EQ(lease->target_domain, "domain-b");
    EXPECT_EQ(lease->target_segment, domain_b_target.segment_name);
}

TEST_F(DynamicReplicationTest, TargetDomainRejectsWhenNoSegmentInDomain) {
    MasterServiceConfig config;
    config.dynamic_replication_mode = "enforce";
    config.dynamic_replication_admission_qps_threshold = 0.1;
    config.dynamic_replication_max_memory_replicas = 3;
    MasterService service(config);

    auto source =
        PrepareSegment(service, "domain_a_source", 0x1700000000, "domain-a");
    (void)PrepareSegment(service, "domain_a_target", 0x1800000000, "domain-a");
    PutObject(service, source.client_id, "missing-domain-key",
              source.segment_name);

    auto proposal = BuildProposal(service, "missing-domain-key");
    proposal.requester_domain = "domain-c";
    proposal.target_domain.clear();
    auto lease = service.SubmitReplicaActionProposal(proposal);
    ASSERT_FALSE(lease.has_value());
    EXPECT_EQ(lease.error(), ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
}

TEST_F(DynamicReplicationTest, PreferredTargetOutsideDomainFallsBack) {
    MasterServiceConfig config;
    config.dynamic_replication_mode = "enforce";
    config.dynamic_replication_admission_qps_threshold = 0.1;
    config.dynamic_replication_max_memory_replicas = 3;
    MasterService service(config);

    auto source =
        PrepareSegment(service, "domain_a_source", 0x1900000000, "domain-a");
    auto wrong_domain_target =
        PrepareSegment(service, "domain_a_target", 0x1a00000000, "domain-a");
    auto domain_b_target =
        PrepareSegment(service, "domain_b_target", 0x1b00000000, "domain-b");
    PutObject(service, source.client_id, "preferred-domain-key",
              source.segment_name);

    auto proposal = BuildProposal(service, "preferred-domain-key",
                                  wrong_domain_target.segment_name);
    proposal.requester_domain = "domain-b";
    proposal.target_domain.clear();
    auto lease = service.SubmitReplicaActionProposal(proposal);
    ASSERT_TRUE(lease.has_value());
    EXPECT_EQ(lease->target_domain, "domain-b");
    EXPECT_EQ(lease->target_segment, domain_b_target.segment_name);
}

TEST_F(DynamicReplicationTest,
       ExistingReplicaInTargetDomainSuppressesCrossDomainCopy) {
    MasterServiceConfig config;
    config.dynamic_replication_mode = "enforce";
    config.dynamic_replication_admission_qps_threshold = 0.1;
    config.dynamic_replication_max_memory_replicas = 4;
    MasterService service(config);

    auto source =
        PrepareSegment(service, "domain_a_source", 0x1c00000000, "domain-a");
    auto first_b =
        PrepareSegment(service, "domain_b_first", 0x1d00000000, "domain-b");
    (void)PrepareSegment(service, "domain_b_second", 0x1e00000000, "domain-b");
    PutObject(service, source.client_id, "target-domain-existing-key",
              source.segment_name);

    auto first = BuildProposal(service, "target-domain-existing-key",
                               first_b.segment_name);
    first.requester_domain = "domain-b";
    first.target_domain = "domain-b";
    auto first_lease = service.SubmitReplicaActionProposal(first);
    ASSERT_TRUE(first_lease.has_value());
    ASSERT_TRUE(service
                    .CopyStart(source.client_id, "target-domain-existing-key",
                               TenantId::Default(), first_lease->source_segment,
                               {first_lease->target_segment})
                    .has_value());
    ASSERT_TRUE(service
                    .CopyEnd(source.client_id, "target-domain-existing-key",
                             TenantId::Default())
                    .has_value());

    auto duplicate = BuildProposal(service, "target-domain-existing-key");
    duplicate.requester_domain = "domain-b";
    duplicate.target_domain = "domain-b";
    auto rejected = service.SubmitReplicaActionProposal(duplicate);
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error(), ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
}

TEST_F(DynamicReplicationTest, EvictionCooldownIsScopedToTargetDomain) {
    MasterServiceConfig config;
    config.dynamic_replication_mode = "enforce";
    config.dynamic_replication_admission_qps_threshold = 0.1;
    config.dynamic_replication_max_memory_replicas = 4;
    MasterService service(config);

    auto source =
        PrepareSegment(service, "domain_a_source", 0x1f00000000, "domain-a");
    auto domain_b_target =
        PrepareSegment(service, "domain_b_target", 0x2000000000, "domain-b");
    auto domain_a_target =
        PrepareSegment(service, "domain_a_target", 0x2100000000, "domain-a");
    PutObject(service, source.client_id, "domain-cooldown-key",
              source.segment_name);

    auto to_b = BuildProposal(service, "domain-cooldown-key",
                              domain_b_target.segment_name);
    to_b.requester_domain = "domain-b";
    to_b.target_domain = "domain-b";
    auto b_lease = service.SubmitReplicaActionProposal(to_b);
    ASSERT_TRUE(b_lease.has_value());
    ASSERT_TRUE(service
                    .CopyStart(source.client_id, "domain-cooldown-key",
                               TenantId::Default(), b_lease->source_segment,
                               {b_lease->target_segment})
                    .has_value());
    ASSERT_TRUE(service
                    .CopyEnd(source.client_id, "domain-cooldown-key",
                             TenantId::Default())
                    .has_value());

    EXPECT_EQ(EvictReplicaOnSegment(service, "domain-cooldown-key",
                                    domain_b_target.segment_name),
              1u);

    auto retry_b = BuildProposal(service, "domain-cooldown-key",
                                 domain_b_target.segment_name);
    retry_b.requester_domain = "domain-b";
    retry_b.target_domain = "domain-b";
    auto rejected_b = service.SubmitReplicaActionProposal(retry_b);
    ASSERT_FALSE(rejected_b.has_value());
    EXPECT_EQ(rejected_b.error(), ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);

    auto same_domain = BuildProposal(service, "domain-cooldown-key",
                                     domain_a_target.segment_name);
    same_domain.requester_domain = "domain-a";
    same_domain.target_domain = "domain-a";
    auto accepted_a = service.SubmitReplicaActionProposal(same_domain);
    ASSERT_TRUE(accepted_a.has_value());
    EXPECT_EQ(accepted_a->target_segment, domain_a_target.segment_name);
}

TEST_F(DynamicReplicationTest, ClearStateMatchesDomainControlKeyExactly) {
    MasterServiceConfig config;
    MasterService service(config);

    EXPECT_TRUE(ClearStateMatchesDomainControlKeyExactly(service));
}

}  // namespace mooncake::test

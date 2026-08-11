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

    void BumpVersionEpoch(MasterService& service, const std::string& key) const {
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

}  // namespace mooncake::test

#include "master_service_test_fixture.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "weight_management.h"

namespace mooncake::test {
namespace {

class WeightGroupLifecycleTest : public MasterServiceTest {
   protected:
    static WeightRevisionIdentity Identity() {
        return WeightRevisionIdentity{
            .tenant_id = "default",
            .name_space = "production",
            .resource_id = "llama-70b",
            .revision = "step-100",
            .weight_generation = 7,
        };
    }

    static std::string ManifestKey() {
        return "weights/production/llama-70b/step-100/7/manifest";
    }

    WeightRevisionMetadata PublishReady(MasterService& service,
                                        const UUID& client_id) {
        auto importing = service.BeginWeightImport(BeginWeightImportRequest{
            .identity = Identity(),
            .payload_group_id = {},
            .expected_payload_count = 1,
            .expected_logical_bytes = 1024,
        });
        EXPECT_TRUE(importing.has_value());
        ReplicateConfig config;
        config.replica_num = 1;
        config.with_hard_pin = true;
        config.group_ids =
            std::vector<std::string>{importing->manifest.payload_group_id};
        config.data_type = ObjectDataType::WEIGHT;
        PutCompletedObject(service, client_id, "payload-a", config, 1024);
        config.data_type = ObjectDataType::METADATA;
        PutCompletedObject(service, client_id, ManifestKey(), config, 128);
        auto ready = service.CommitWeightImport(CommitWeightImportRequest{
            .identity = Identity(),
            .expected_metadata_generation = importing->metadata_generation,
            .manifest =
                WeightManifestReference{
                    .manifest_key = ManifestKey(),
                    .manifest_sha256 = std::string(64, 'a'),
                    .payload_group_id = importing->manifest.payload_group_id,
                    .payload_keys_sha256 =
                        ComputeWeightPayloadKeysSha256({"payload-a"}),
                    .payload_count = 1,
                    .logical_bytes = 1024,
                },
        });
        EXPECT_TRUE(ready.has_value());
        return *ready;
    }

    static void AddLocalDiskReplica(MasterService& service,
                                    const UUID& client_id,
                                    const std::string& key, int64_t size,
                                    const std::string& endpoint) {
        std::vector<OffloadTaskItem> tasks{
            OffloadTaskItem{.tenant_id = "default", .key = key, .size = size}};
        StorageObjectMetadata metadata;
        metadata.key_size = key.size();
        metadata.data_size = size;
        metadata.transport_endpoint = endpoint;
        ASSERT_TRUE(service.NotifyOffloadSuccess(client_id, tasks, {metadata}));
    }
};

TEST_F(WeightGroupLifecycleTest, LeaseBlocksOperationAndDelete) {
    MasterService service;
    [[maybe_unused]] const auto context = PrepareSimpleSegment(service);
    const UUID client_id = generate_uuid();
    auto ready = PublishReady(service, client_id);
    auto lease =
        service.AcquireWeightRevisionLease(AcquireWeightRevisionLeaseRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation,
            .holder = "worker-0",
            .ttl_ms = 60'000,
        });
    ASSERT_TRUE(lease.has_value());

    auto operation = service.StartWeightResidencyOperation(
        StartWeightResidencyOperationRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation,
            .target_residency = WeightResidencyState::COLD,
        });
    ASSERT_FALSE(operation.has_value());
    EXPECT_EQ(WeightManagementError::BUSY, operation.error());
    auto deleted = service.DeleteWeightRevision(DeleteWeightRevisionRequest{
        .identity = ready.identity,
        .expected_metadata_generation = ready.metadata_generation,
    });
    ASSERT_FALSE(deleted.has_value());
    EXPECT_EQ(WeightManagementError::BUSY, deleted.error());
}

TEST_F(WeightGroupLifecycleTest,
       RejectsColdOperationWithoutReadableColdCopies) {
    MasterService service;
    [[maybe_unused]] const auto context = PrepareSimpleSegment(service);
    const auto ready = PublishReady(service, context.client_id);
    const auto started = service.StartWeightResidencyOperation({
        .identity = ready.identity,
        .expected_metadata_generation = ready.metadata_generation,
        .target_residency = WeightResidencyState::COLD,
    });
    ASSERT_FALSE(started.has_value());
    EXPECT_EQ(WeightManagementError::NOT_READY, started.error());
}

TEST_F(WeightGroupLifecycleTest, DeleteCancelsPendingResidencyOperation) {
    MasterService service;
    [[maybe_unused]] const auto context = PrepareSimpleSegment(service);
    const auto ready = PublishReady(service, context.client_id);
    const auto started = service.StartWeightResidencyOperation({
        .identity = ready.identity,
        .expected_metadata_generation = ready.metadata_generation,
        .target_residency = WeightResidencyState::HOT,
    });
    ASSERT_TRUE(started.has_value());
    const auto deleted = service.DeleteWeightRevision({
        .identity = ready.identity,
        .expected_metadata_generation = started->fenced_metadata_generation,
    });
    ASSERT_TRUE(deleted.has_value());
    EXPECT_EQ(WeightAvailabilityState::DELETED, deleted->availability);
    const auto operation = service.QueryWeightOperation({
        .operation_id = started->operation_id,
    });
    ASSERT_TRUE(operation.has_value());
    EXPECT_EQ("cancelled", operation->message);
}

TEST_F(WeightGroupLifecycleTest, DeleteCancelsOperationAfterMemberLoss) {
    MasterService service;
    const auto context = PrepareSimpleSegment(service);
    const auto ready = PublishReady(service, context.client_id);
    const auto started = service.StartWeightResidencyOperation({
        .identity = ready.identity,
        .expected_metadata_generation = ready.metadata_generation,
        .target_residency = WeightResidencyState::HOT,
    });
    ASSERT_TRUE(started);
    ASSERT_TRUE(MasterServiceTestPeer(service).DropWeightGroupMemberForTesting(
        ready.identity, "payload-a"));
    const auto damaged =
        service.ReconcileWeightRevision({.identity = ready.identity});
    ASSERT_TRUE(damaged);
    ASSERT_EQ(WeightAvailabilityState::DEGRADED, damaged->availability);
    const auto stale = service.DeleteWeightRevision({
        .identity = ready.identity,
        .expected_metadata_generation = ready.metadata_generation,
    });
    ASSERT_FALSE(stale);
    EXPECT_EQ(WeightManagementError::STALE_GENERATION, stale.error());
    const auto before =
        service.QueryWeightOperation({.operation_id = started->operation_id});
    ASSERT_TRUE(before);
    const auto deleted = service.DeleteWeightRevision({
        .identity = ready.identity,
        .expected_metadata_generation = damaged->metadata_generation,
    });
    ASSERT_TRUE(deleted);
    EXPECT_EQ(WeightAvailabilityState::DELETED, deleted->availability);
    const auto after =
        service.QueryWeightOperation({.operation_id = started->operation_id});
    ASSERT_TRUE(after);
    EXPECT_EQ("cancelled", after->message);
    EXPECT_EQ(before->target_residency, after->target_residency);
    EXPECT_EQ(before->processed_members, after->processed_members);
    EXPECT_EQ(before->cursor, after->cursor);
    EXPECT_TRUE(service.DeleteWeightRevision({
        .identity = ready.identity,
        .expected_metadata_generation = deleted->metadata_generation,
    }));
}

TEST_F(WeightGroupLifecycleTest, OperationRemainsPendingUntilTargetObserved) {
    MasterService service(MasterServiceConfig::builder()
                              .set_root_fs_dir("/mnt/ssd")
                              .set_default_kv_lease_ttl(60'000)
                              .build());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(service);
    const UUID client_id = generate_uuid();
    auto ready = PublishReady(service, client_id);

    for (const auto& key : {std::string("payload-a"), ManifestKey()}) {
        ASSERT_TRUE(service.PutEnd(client_id, key, TenantId::Default(),
                                   ReplicaType::DISK));
        ASSERT_TRUE(service.GetReplicaList(key, TenantId::Default()));
    }
    auto started = service.StartWeightResidencyOperation(
        StartWeightResidencyOperationRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation,
            .target_residency = WeightResidencyState::COLD,
        });
    ASSERT_TRUE(started.has_value());
    EXPECT_EQ(WeightOperationState::EVICTING, started->operation);
    EXPECT_EQ(0, started->processed_members);
    EXPECT_EQ(2, started->total_members);
    EXPECT_EQ(*started,
              *service.QueryWeightOperation(QueryWeightOperationRequest{
                  .operation_id = started->operation_id,
              }));

    auto reconciled = service.ReconcileWeightRevision(
        ReconcileWeightRevisionRequest{.identity = ready.identity});
    ASSERT_TRUE(reconciled.has_value());
    EXPECT_EQ(WeightOperationState::EVICTING, reconciled->operation);
    EXPECT_EQ(WeightResidencyState::HOT, reconciled->residency);
    auto progress = service.QueryWeightOperation(
        QueryWeightOperationRequest{.operation_id = started->operation_id});
    ASSERT_TRUE(progress.has_value());
    EXPECT_EQ(0, progress->processed_members);
    EXPECT_EQ(2, progress->total_members);
}

TEST_F(WeightGroupLifecycleTest,
       ReadableColdMembersDetermineDegradedResidency) {
    MasterService service(MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(0)
                              .set_root_fs_dir("/mnt/ssd")
                              .build());
    const auto context = PrepareSimpleSegment(service);
    const auto ready = PublishReady(service, context.client_id);
    for (const auto& key : {std::string("payload-a"), ManifestKey()}) {
        ASSERT_TRUE(service.PutEnd(context.client_id, key, TenantId::Default(),
                                   ReplicaType::DISK));
    }
    ASSERT_TRUE(service.StartWeightResidencyOperation({
        .identity = ready.identity,
        .expected_metadata_generation = ready.metadata_generation,
        .target_residency = WeightResidencyState::COLD,
    }));
    const auto cold =
        service.ReconcileWeightRevision({.identity = ready.identity});
    ASSERT_TRUE(cold);
    ASSERT_EQ(WeightOperationState::NONE, cold->operation);
    ASSERT_EQ(WeightResidencyState::COLD, cold->residency);
    for (const auto& key : {std::string("payload-a"), ManifestKey()}) {
        {
            MasterServiceTestPeer::MetadataAccessorRW member(
                &service, MasterServiceTestPeer::ObjectIdentity{
                              TenantId::Default(), key});
            ASSERT_TRUE(member.Exists());
            member.Get().VisitReplicas(
                [](const Replica&) { return true; },
                [](Replica& replica) { replica.mark_processing(); });
        }
        const auto observed =
            service.ReconcileWeightRevision({.identity = ready.identity});
        ASSERT_TRUE(observed);
        EXPECT_EQ(WeightAvailabilityState::DEGRADED, observed->availability);
        EXPECT_EQ(key == "payload-a" ? WeightResidencyState::COLD
                                     : WeightResidencyState::ABSENT,
                  observed->residency);
    }
}

TEST_F(WeightGroupLifecycleTest, ColdOperationEvictsWholeManagedGroup) {
    auto config = MasterServiceConfig::builder()
                      .set_default_kv_lease_ttl(0)
                      .set_root_fs_dir("/mnt/ssd")
                      .build();
    MasterService service(config);
    [[maybe_unused]] const auto context = PrepareSimpleSegment(service);
    const UUID client_id = generate_uuid();
    auto ready = PublishReady(service, client_id);
    ASSERT_TRUE(service
                    .PutEnd(client_id, "payload-a", TenantId::Default(),
                            ReplicaType::DISK)
                    .has_value());
    ASSERT_TRUE(service
                    .PutEnd(client_id, ManifestKey(), TenantId::Default(),
                            ReplicaType::DISK)
                    .has_value());
    auto started = service.StartWeightResidencyOperation(
        StartWeightResidencyOperationRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation,
            .target_residency = WeightResidencyState::COLD,
        });
    ASSERT_TRUE(started.has_value());

    auto reconciled = service.ReconcileWeightRevision(
        ReconcileWeightRevisionRequest{.identity = ready.identity});
    ASSERT_TRUE(reconciled.has_value());
    EXPECT_EQ(WeightAvailabilityState::READY, reconciled->availability);
    EXPECT_EQ(WeightResidencyState::COLD, reconciled->residency);
    EXPECT_EQ(WeightOperationState::NONE, reconciled->operation);
    auto completed = service.QueryWeightOperation(
        QueryWeightOperationRequest{.operation_id = started->operation_id});
    ASSERT_TRUE(completed.has_value());
    EXPECT_EQ("completed", completed->message);
    EXPECT_EQ(completed->total_members, completed->processed_members);
}

TEST_F(WeightGroupLifecycleTest, RehydrateQueuesAndCompletesWholeManagedGroup) {
    auto config = MasterServiceConfig::builder()
                      .set_default_kv_lease_ttl(0)
                      .set_enable_offload(true)
                      .set_root_fs_dir("/mnt/ssd")
                      .build();
    MasterService service(config);
    [[maybe_unused]] const auto context = PrepareSimpleSegment(service);
    const UUID client_id = generate_uuid();
    ASSERT_TRUE(service.MountLocalDiskSegment(client_id, true));
    auto ready = PublishReady(service, client_id);
    ASSERT_TRUE(service
                    .PutEnd(client_id, "payload-a", TenantId::Default(),
                            ReplicaType::DISK)
                    .has_value());
    ASSERT_TRUE(service
                    .PutEnd(client_id, ManifestKey(), TenantId::Default(),
                            ReplicaType::DISK)
                    .has_value());
    AddLocalDiskReplica(service, client_id, "payload-a", 1024, "test_segment");
    AddLocalDiskReplica(service, client_id, ManifestKey(), 128, "test_segment");
    ASSERT_TRUE(service.StartWeightResidencyOperation(
        StartWeightResidencyOperationRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation,
            .target_residency = WeightResidencyState::COLD,
        }));
    auto cold = service.ReconcileWeightRevision(
        ReconcileWeightRevisionRequest{.identity = ready.identity});
    ASSERT_TRUE(cold.has_value());
    ASSERT_EQ(WeightResidencyState::COLD, cold->residency);

    auto started = service.StartWeightResidencyOperation(
        StartWeightResidencyOperationRequest{
            .identity = cold->identity,
            .expected_metadata_generation = cold->metadata_generation,
            .target_residency = WeightResidencyState::HOT,
        });
    ASSERT_TRUE(started.has_value());
    EXPECT_EQ(WeightOperationState::REHYDRATING, started->operation);
    ASSERT_TRUE(service.ReconcileWeightRevision(
        ReconcileWeightRevisionRequest{.identity = cold->identity}));

    size_t promoted = 0;
    while (promoted < 2) {
        auto pending = service.PromotionObjectHeartbeat(client_id);
        ASSERT_TRUE(pending.has_value());
        ASSERT_FALSE(pending->empty());
        for (const auto& task : *pending) {
            ASSERT_TRUE(service.PromotionAllocStart(
                client_id, task.key, TenantId(task.tenant_id), task.size, {}));
            ASSERT_TRUE(service.NotifyPromotionSuccess(
                client_id, task.key, TenantId(task.tenant_id)));
            ++promoted;
        }
    }

    auto hot = service.ReconcileWeightRevision(
        ReconcileWeightRevisionRequest{.identity = cold->identity});
    ASSERT_TRUE(hot.has_value());
    EXPECT_EQ(WeightAvailabilityState::READY, hot->availability);
    EXPECT_EQ(WeightResidencyState::HOT, hot->residency);
    EXPECT_EQ(WeightOperationState::NONE, hot->operation);
    auto completed = service.QueryWeightOperation(
        QueryWeightOperationRequest{.operation_id = started->operation_id});
    ASSERT_TRUE(completed.has_value());
    EXPECT_EQ("completed", completed->message);
}

TEST_F(WeightGroupLifecycleTest, RecreatedMetadataStartsFreshMemberSelection) {
    const auto config = MasterServiceConfig::builder()
                            .set_default_kv_lease_ttl(0)
                            .set_enable_offload(true)
                            .set_root_fs_dir("/mnt/ssd")
                            .build();
    MasterService service(config);
    const auto context = PrepareSimpleSegment(service);
    ASSERT_TRUE(service.MountLocalDiskSegment(context.client_id, true));
    const auto ready = PublishReady(service, context.client_id);
    const auto attach_cold = [&](const std::string& key, uint64_t size) {
        ASSERT_TRUE(service.PutEnd(context.client_id, key, TenantId::Default(),
                                   ReplicaType::DISK));
        AddLocalDiskReplica(service, context.client_id, key, size,
                            "test_segment");
    };
    attach_cold("payload-a", 1024);
    attach_cold(ManifestKey(), 128);
    ASSERT_TRUE(service.StartWeightResidencyOperation({
        .identity = ready.identity,
        .expected_metadata_generation = ready.metadata_generation,
        .target_residency = WeightResidencyState::COLD,
    }));
    auto& manager = MasterServiceTestPeer::WeightManager(service);
    const auto initial = manager.ExportSnapshot();
    ASSERT_EQ(1, manager.ReconcileWeightMetadataStoreOnce(
                     ready.updated_at_ms + 1, 1));

    // Replace the metadata registry, then let its real sweep observe absence.
    ASSERT_TRUE(manager.RestoreSnapshot(WeightMetadataSnapshot{}));
    EXPECT_EQ(0, manager.ReconcileWeightMetadataStoreOnce(
                     ready.updated_at_ms + 2, 1));
    // Restore the physical initial state through ordinary object APIs while
    // the old revision no longer owns the group.
    ReplicateConfig put;
    put.replica_num = 1;
    put.with_hard_pin = true;
    put.group_ids = std::vector<std::string>{ready.manifest.payload_group_id};
    for (const auto& key : {std::string("payload-a"), ManifestKey()}) {
        ASSERT_TRUE(service.Remove(key, TenantId::Default(), true));
        put.data_type = key == "payload-a" ? ObjectDataType::WEIGHT
                                           : ObjectDataType::METADATA;
        const uint64_t size = key == "payload-a" ? 1024 : 128;
        PutCompletedObject(service, context.client_id, key, put, size);
        attach_cold(key, size);
    }
    ASSERT_TRUE(manager.RestoreSnapshot(initial));
    ASSERT_EQ(1, manager.ReconcileWeightMetadataStoreOnce(
                     ready.updated_at_ms + 3, 1));
    const auto payload =
        service.GetReplicaList("payload-a", TenantId::Default());
    ASSERT_TRUE(payload);
    bool payload_has_memory = false;
    for (const auto& replica : payload->replicas) {
        payload_has_memory |= replica.is_memory_replica();
    }
    EXPECT_FALSE(payload_has_memory);
    const auto manifest =
        service.GetReplicaList(ManifestKey(), TenantId::Default());
    ASSERT_TRUE(manifest);
    bool manifest_has_memory = false;
    for (const auto& replica : manifest->replicas) {
        manifest_has_memory |= replica.is_memory_replica();
    }
    EXPECT_TRUE(manifest_has_memory);
}

TEST_F(WeightGroupLifecycleTest, MemberBudgetBoundsEvictionAndPromotion) {
    const auto config = MasterServiceConfig::builder()
                            .set_default_kv_lease_ttl(0)
                            .set_enable_offload(true)
                            .set_root_fs_dir("/mnt/ssd")
                            .build();
    MasterService service(config);
    const auto context = PrepareSimpleSegment(service);
    const auto client_id = context.client_id;
    ASSERT_TRUE(service.MountLocalDiskSegment(client_id, true));
    const auto ready = PublishReady(service, client_id);
    for (const auto& key : {std::string("payload-a"), ManifestKey()}) {
        ASSERT_TRUE(service.PutEnd(client_id, key, TenantId::Default(),
                                   ReplicaType::DISK));
        AddLocalDiskReplica(service, client_id, key,
                            key == "payload-a" ? 1024 : 128, "test_segment");
    }
    ASSERT_TRUE(service.StartWeightResidencyOperation({
        .identity = ready.identity,
        .expected_metadata_generation = ready.metadata_generation,
        .target_residency = WeightResidencyState::COLD,
    }));
    MasterServiceTestPeer(service).RunWeightReconciliationForTesting(
        ready.updated_at_ms + 1, 1);
    size_t hot_members = 0;
    for (const auto& key : {std::string("payload-a"), ManifestKey()}) {
        const auto replicas = service.GetReplicaList(key, TenantId::Default());
        ASSERT_TRUE(replicas);
        for (const auto& replica : replicas->replicas) {
            hot_members += replica.is_memory_replica();
        }
    }
    EXPECT_EQ(1, hot_members);
    MasterServiceTestPeer(service).RunWeightReconciliationForTesting(
        ready.updated_at_ms + 2, 1);
    const auto cold = service.GetWeightRevision({.identity = ready.identity});
    ASSERT_TRUE(cold);
    ASSERT_EQ(WeightResidencyState::COLD, cold->metadata.residency);
    ASSERT_TRUE(service.StartWeightResidencyOperation({
        .identity = ready.identity,
        .expected_metadata_generation = cold->metadata.metadata_generation,
        .target_residency = WeightResidencyState::HOT,
    }));
    for (int i = 0; i < 2; ++i) {
        MasterServiceTestPeer(service).RunWeightReconciliationForTesting(
            ready.updated_at_ms + 3 + i, 1);
        const auto pending = service.PromotionObjectHeartbeat(client_id);
        ASSERT_TRUE(pending);
        ASSERT_EQ(1, pending->size());
        const auto& task = pending->front();
        ASSERT_TRUE(service.PromotionAllocStart(
            client_id, task.key, TenantId(task.tenant_id), task.size, {}));
        ASSERT_TRUE(service.NotifyPromotionSuccess(client_id, task.key,
                                                   TenantId(task.tenant_id)));
    }
    MasterServiceTestPeer(service).RunWeightReconciliationForTesting(
        ready.updated_at_ms + 5, 1);
    const auto hot = service.GetWeightRevision({.identity = ready.identity});
    ASSERT_TRUE(hot);
    EXPECT_EQ(WeightResidencyState::HOT, hot->metadata.residency);
    EXPECT_EQ(WeightOperationState::NONE, hot->metadata.operation);
}

TEST_F(WeightGroupLifecycleTest, PendingOperationReflectsLostReadableMembers) {
    MasterService service;
    const auto segment = PrepareSimpleSegment(service);
    const auto ready = PublishReady(service, segment.client_id);
    const auto started = service.StartWeightResidencyOperation({
        .identity = ready.identity,
        .expected_metadata_generation = ready.metadata_generation,
        .target_residency = WeightResidencyState::HOT,
    });
    ASSERT_TRUE(started);
    ASSERT_TRUE(service.UnmountSegment(segment.segment_id, segment.client_id));
    for (const auto& key : {std::string("payload-a"), ManifestKey()}) {
        const auto readable = service.ExistKey(key, TenantId::Default());
        ASSERT_TRUE(readable);
        ASSERT_FALSE(*readable);
    }

    const auto reconciled =
        service.ReconcileWeightRevision({.identity = ready.identity});
    ASSERT_TRUE(reconciled);
    EXPECT_EQ(WeightAvailabilityState::DEGRADED, reconciled->availability);
    EXPECT_EQ(WeightResidencyState::ABSENT, reconciled->residency);
    EXPECT_EQ(WeightOperationState::REHYDRATING, reconciled->operation);
    EXPECT_EQ(started->operation_id, reconciled->operation_id);
    EXPECT_EQ(started->fenced_metadata_generation + 1,
              reconciled->metadata_generation);
    const auto view = service.GetWeightRevision({.identity = ready.identity});
    ASSERT_TRUE(view);
    EXPECT_EQ(*reconciled, view->metadata);
    const auto progress = service.QueryWeightOperation({
        .operation_id = started->operation_id,
    });
    ASSERT_TRUE(progress);
    EXPECT_EQ(reconciled->metadata_generation,
              progress->fenced_metadata_generation);
    EXPECT_NE("completed", progress->message);

    const auto retry =
        service.ReconcileWeightRevision({.identity = ready.identity});
    ASSERT_TRUE(retry);
    EXPECT_EQ(*reconciled, *retry);
}

TEST_F(WeightGroupLifecycleTest,
       DeleteRemovesPayloadAndManifestThenTombstones) {
    MasterService service;
    [[maybe_unused]] const auto context = PrepareSimpleSegment(service);
    const UUID client_id = generate_uuid();
    auto ready = PublishReady(service, client_id);

    auto deleted = service.DeleteWeightRevision(DeleteWeightRevisionRequest{
        .identity = ready.identity,
        .expected_metadata_generation = ready.metadata_generation,
    });
    ASSERT_TRUE(deleted.has_value());
    EXPECT_EQ(WeightAvailabilityState::DELETED, deleted->availability);
    EXPECT_EQ(WeightResidencyState::ABSENT, deleted->residency);
    EXPECT_FALSE(
        service.ExistKey("payload-a", TenantId::Default()).value_or(false));
    EXPECT_FALSE(
        service.ExistKey(ManifestKey(), TenantId::Default()).value_or(false));
}

}  // namespace
}  // namespace mooncake::test

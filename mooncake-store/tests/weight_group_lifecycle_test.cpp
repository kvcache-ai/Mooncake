#include "master_service_test_fixture.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <utility>
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

    struct PayloadSpec {
        std::string key;
        uint64_t size;
        std::string affinity_id;
    };

    WeightRevisionMetadata PublishReadyWithPayloads(
        MasterService& service, const UUID& client_id,
        const std::vector<PayloadSpec>& payloads) {
        uint64_t logical_bytes = 0;
        std::vector<std::string> payload_keys;
        std::set<std::string> affinity_ids;
        for (const auto& payload : payloads) {
            logical_bytes += payload.size;
            payload_keys.push_back(payload.key);
            affinity_ids.insert(payload.affinity_id);
        }
        auto importing = service.BeginWeightImport(BeginWeightImportRequest{
            .identity = Identity(),
            .payload_group_id = {},
            .expected_payload_count = payloads.size(),
            .expected_logical_bytes = logical_bytes,
            .policy =
                WeightStoragePolicy{
                    .preferred_residency = WeightResidencyState::HOT,
                    .migration_mode = WeightMigrationMode::MANUAL,
                },
            .affinity_summary =
                WeightAffinitySummary{
                    .affinity_count = affinity_ids.size(),
                    .affinity_digest = std::string(64, 'c'),
                },
        });
        EXPECT_TRUE(importing.has_value());
        for (const auto& payload : payloads) {
            ReplicateConfig config;
            config.replica_num = 1;
            config.with_hard_pin = true;
            config.group_ids =
                std::vector<std::string>{importing->manifest.payload_group_id};
            config.residency_affinity_ids =
                std::vector<std::string>{payload.affinity_id};
            config.data_type = ObjectDataType::WEIGHT;
            PutCompletedObject(service, client_id, payload.key, config,
                               payload.size);
        }
        ReplicateConfig manifest_config;
        manifest_config.replica_num = 1;
        manifest_config.with_hard_pin = true;
        manifest_config.group_ids =
            std::vector<std::string>{importing->manifest.payload_group_id};
        manifest_config.data_type = ObjectDataType::METADATA;
        PutCompletedObject(service, client_id, ManifestKey(), manifest_config,
                           128);
        auto ready = service.CommitWeightImport(CommitWeightImportRequest{
            .identity = Identity(),
            .expected_metadata_generation = importing->metadata_generation,
            .manifest =
                WeightManifestReference{
                    .manifest_key = ManifestKey(),
                    .manifest_sha256 = std::string(64, 'a'),
                    .payload_group_id = importing->manifest.payload_group_id,
                    .payload_keys_sha256 =
                        ComputeWeightPayloadKeysSha256(payload_keys),
                    .payload_count = payloads.size(),
                    .logical_bytes = logical_bytes,
                },
        });
        EXPECT_TRUE(ready.has_value());
        return *ready;
    }

    WeightRevisionMetadata PublishReady(MasterService& service,
                                        const UUID& client_id) {
        return PublishReadyWithPayloads(
            service, client_id,
            {{.key = "payload-a", .size = 1024, .affinity_id = "affinity-a"}});
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
            .mixed_hot_ratio = std::nullopt,
        });
    ASSERT_TRUE(operation.has_value());
    auto migration_lease =
        service.AcquireWeightRevisionLease(AcquireWeightRevisionLeaseRequest{
            .identity = ready.identity,
            .expected_metadata_generation =
                operation->fenced_metadata_generation,
            .holder = "worker-1",
            .ttl_ms = 60'000,
        });
    ASSERT_TRUE(migration_lease.has_value());
    auto deleted = service.DeleteWeightRevision(DeleteWeightRevisionRequest{
        .identity = ready.identity,
        .expected_metadata_generation = ready.metadata_generation + 1,
    });
    ASSERT_FALSE(deleted.has_value());
    EXPECT_EQ(WeightManagementError::BUSY, deleted.error());
}

TEST_F(WeightGroupLifecycleTest, SnapshotExposesOpaqueResidencyAffinityId) {
    MasterService service;
    [[maybe_unused]] const auto context = PrepareSimpleSegment(service);
    const UUID client_id = generate_uuid();
    auto importing = service.BeginWeightImport(BeginWeightImportRequest{
        .identity = Identity(),
        .payload_group_id = {},
        .expected_payload_count = 1,
        .expected_logical_bytes = 1024,
        .policy =
            WeightStoragePolicy{
                .preferred_residency = WeightResidencyState::HOT,
                .migration_mode = WeightMigrationMode::MANUAL,
            },
        .affinity_summary =
            WeightAffinitySummary{
                .affinity_count = 1,
                .affinity_digest = std::string(64, 'c'),
            },
    });
    ASSERT_TRUE(importing.has_value());

    ReplicateConfig config;
    config.replica_num = 1;
    config.group_ids =
        std::vector<std::string>{importing->manifest.payload_group_id};
    config.residency_affinity_ids =
        std::vector<std::string>{"opaque-affinity-id"};
    config.data_type = ObjectDataType::WEIGHT;
    PutCompletedObject(service, client_id, "payload-a", config, 1024);

    auto affinity_ids = GetWeightGroupAffinityIdsForTest(
        service, Identity(), importing->manifest.payload_group_id);
    ASSERT_EQ(1u, affinity_ids.size());
    EXPECT_EQ("opaque-affinity-id", affinity_ids.front());
}

TEST_F(WeightGroupLifecycleTest,
       AcceptsPendingColdOperationWithoutReadableColdCopies) {
    MasterService service;
    [[maybe_unused]] const auto context = PrepareSimpleSegment(service);
    const auto ready = PublishReady(service, context.client_id);
    const auto started = service.StartWeightResidencyOperation({
        .identity = ready.identity,
        .expected_metadata_generation = ready.metadata_generation,
        .target_residency = WeightResidencyState::COLD,
    });
    ASSERT_TRUE(started.has_value());
    const auto observed =
        service.ReconcileWeightRevision({.identity = ready.identity});
    ASSERT_TRUE(observed);
    EXPECT_EQ(WeightResidencyState::HOT, observed->residency);
    EXPECT_EQ(started->operation_id, observed->operation_id);
    const auto progress =
        service.QueryWeightOperation({.operation_id = started->operation_id});
    ASSERT_TRUE(progress);
    EXPECT_EQ(0, progress->processed_units);
    const auto members = GetWeightGroupResidencyForTest(
        service, ready.identity, ready.manifest.payload_group_id);
    ASSERT_EQ(2u, members.size());
    for (const auto& member : members) {
        EXPECT_TRUE(member.has_memory);
        EXPECT_FALSE(member.has_cold);
    }
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
    EXPECT_EQ(before->processed_units, after->processed_units);
    EXPECT_EQ(before->processed_bytes, after->processed_bytes);
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
            .mixed_hot_ratio = std::nullopt,
        });
    ASSERT_TRUE(started.has_value());
    EXPECT_EQ(WeightOperationKind::MIGRATING, started->kind);
    EXPECT_EQ(0, started->processed_units);
    EXPECT_EQ(1, started->total_units);
    EXPECT_EQ(*started,
              *service.QueryWeightOperation(QueryWeightOperationRequest{
                  .operation_id = started->operation_id,
              }));

    auto reconciled = service.ReconcileWeightRevision(
        ReconcileWeightRevisionRequest{.identity = ready.identity});
    ASSERT_TRUE(reconciled.has_value());
    EXPECT_TRUE(reconciled->operation_id.has_value());
    EXPECT_EQ(WeightResidencyState::HOT, reconciled->residency);
    auto progress = service.QueryWeightOperation(
        QueryWeightOperationRequest{.operation_id = started->operation_id});
    ASSERT_TRUE(progress.has_value());
    EXPECT_EQ(0, progress->processed_units);
    EXPECT_EQ(1, progress->total_units);
}

TEST_F(WeightGroupLifecycleTest, ColdOperationCompletesAfterReadLeaseExpires) {
    MasterService service(MasterServiceConfig::builder()
                              .set_root_fs_dir("/mnt/ssd")
                              .set_default_kv_lease_ttl(100)
                              .build());
    const auto context = PrepareSimpleSegment(service);
    const auto ready = PublishReady(service, context.client_id);
    ASSERT_TRUE(service.PutEnd(context.client_id, "payload-a",
                               TenantId::Default(), ReplicaType::DISK));
    ASSERT_TRUE(service.GetReplicaList("payload-a", TenantId::Default()));
    const auto started = service.StartWeightResidencyOperation({
        .identity = ready.identity,
        .expected_metadata_generation = ready.metadata_generation,
        .target_residency = WeightResidencyState::COLD,
    });
    ASSERT_TRUE(started);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    const auto observed =
        service.ReconcileWeightRevision({.identity = ready.identity});
    ASSERT_TRUE(observed);
    EXPECT_FALSE(observed->operation_id.has_value());
    EXPECT_EQ(WeightResidencyState::COLD, observed->residency);
    const auto members = GetWeightGroupResidencyForTest(
        service, ready.identity, ready.manifest.payload_group_id);
    ASSERT_EQ(2u, members.size());
    for (const auto& member : members) {
        if (member.data_type == ObjectDataType::WEIGHT) {
            EXPECT_FALSE(member.has_memory);
            EXPECT_TRUE(member.has_cold);
        }
    }
}

TEST_F(WeightGroupLifecycleTest, ColdOperationEvictsWholeManagedGroup) {
    MasterServiceConfig config;
    config.default_kv_lease_ttl = 0;
    config.enable_offload = true;
    config.offload_on_evict = true;
    MasterService service(config);
    const auto context = PrepareSimpleSegment(service);
    const UUID client_id = context.client_id;
    ASSERT_TRUE(service.MountLocalDiskSegment(client_id, true));
    auto ready = PublishReady(service, client_id);
    EXPECT_FALSE(IsObjectProcessingForTest(service, "payload-a"));
    AddLocalDiskReplica(service, client_id, "payload-a", 1024, "test_segment");
    auto started = service.StartWeightResidencyOperation(
        StartWeightResidencyOperationRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation,
            .target_residency = WeightResidencyState::COLD,
            .mixed_hot_ratio = std::nullopt,
        });
    ASSERT_TRUE(started.has_value());

    auto reconciled = service.ReconcileWeightRevision(
        ReconcileWeightRevisionRequest{.identity = ready.identity});
    ASSERT_TRUE(reconciled.has_value());
    EXPECT_EQ(WeightAvailabilityState::READY, reconciled->availability);
    EXPECT_EQ(WeightResidencyState::COLD, reconciled->residency);
    EXPECT_FALSE(reconciled->operation_id.has_value());
    auto completed = service.QueryWeightOperation(
        QueryWeightOperationRequest{.operation_id = started->operation_id});
    ASSERT_TRUE(completed.has_value());
    EXPECT_EQ("completed", completed->message);
    EXPECT_EQ(completed->total_units, completed->processed_units);
    EXPECT_EQ(completed->total_bytes, completed->processed_bytes);

    const auto members = GetWeightGroupResidencyForTest(
        service, ready.identity, ready.manifest.payload_group_id);
    const auto manifest =
        std::find_if(members.begin(), members.end(), [](const auto& member) {
            return member.data_type == ObjectDataType::METADATA;
        });
    ASSERT_NE(members.end(), manifest);
    EXPECT_TRUE(manifest->has_memory);
}

TEST_F(WeightGroupLifecycleTest,
       PublicLeaseAllowsMigrationButPreventsPhysicalDemotion) {
    MasterServiceConfig config;
    config.default_kv_lease_ttl = 0;
    config.enable_offload = true;
    config.offload_on_evict = true;
    MasterService service(config);
    const auto context = PrepareSimpleSegment(service);
    ASSERT_TRUE(service.MountLocalDiskSegment(context.client_id, true));
    auto ready = PublishReady(service, context.client_id);
    AddLocalDiskReplica(service, context.client_id, "payload-a", 1024,
                        "test_segment");
    auto lease =
        service.AcquireWeightRevisionLease(AcquireWeightRevisionLeaseRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation,
            .holder = "reader",
            .ttl_ms = 60'000,
        });
    ASSERT_TRUE(lease.has_value());
    auto operation = service.StartWeightResidencyOperation(
        StartWeightResidencyOperationRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation,
            .target_residency = WeightResidencyState::COLD,
        });
    ASSERT_TRUE(operation.has_value());
    auto pending = service.ReconcileWeightRevision(
        ReconcileWeightRevisionRequest{.identity = ready.identity});
    ASSERT_TRUE(pending.has_value());
    EXPECT_EQ(WeightResidencyState::HOT, pending->residency);
    EXPECT_TRUE(pending->operation_id.has_value());
    auto payload = service.GetReplicaList("payload-a", TenantId::Default());
    ASSERT_TRUE(payload.has_value());
    bool has_memory = false;
    for (const auto& replica : payload->replicas) {
        has_memory = has_memory || replica.is_memory_replica();
    }
    EXPECT_TRUE(has_memory);
    ASSERT_TRUE(service.ReleaseWeightRevisionLease(
        ReleaseWeightRevisionLeaseRequest{.lease_id = lease->lease_id}));
    auto cold = service.ReconcileWeightRevision(
        ReconcileWeightRevisionRequest{.identity = ready.identity});
    ASSERT_TRUE(cold.has_value());
    EXPECT_EQ(WeightResidencyState::COLD, cold->residency);
    EXPECT_FALSE(cold->operation_id.has_value());
    payload = service.GetReplicaList("payload-a", TenantId::Default());
    ASSERT_TRUE(payload.has_value());
    ASSERT_FALSE(payload->replicas.empty());
    for (const auto& replica : payload->replicas) {
        EXPECT_FALSE(replica.is_memory_replica());
    }
    auto manifest = service.GetReplicaList(
        MakeWeightManifestKey(ready.identity), TenantId::Default());
    ASSERT_TRUE(manifest.has_value());
    bool manifest_has_memory = false;
    for (const auto& replica : manifest->replicas) {
        manifest_has_memory =
            manifest_has_memory || replica.is_memory_replica();
    }
    EXPECT_TRUE(manifest_has_memory);
}

TEST_F(WeightGroupLifecycleTest,
       ReadableColdMembersDetermineDegradedResidency) {
    MasterServiceConfig config;
    config.default_kv_lease_ttl = 0;
    config.enable_offload = true;
    config.offload_on_evict = true;
    MasterService service(config);
    const auto context = PrepareSimpleSegment(service);
    ASSERT_TRUE(service.MountLocalDiskSegment(context.client_id, true));
    const auto ready = PublishReadyWithPayloads(
        service, context.client_id,
        {{"payload-a", 1024, "affinity-a"}, {"payload-b", 1024, "affinity-b"}});
    ASSERT_TRUE(service.StartWeightResidencyOperation({
        .identity = ready.identity,
        .expected_metadata_generation = ready.metadata_generation,
        .target_residency = WeightResidencyState::COLD,
    }));
    ASSERT_TRUE(service.ReconcileWeightRevision({.identity = ready.identity}));
    const auto tasks = service.OffloadObjectHeartbeat(context.client_id, true);
    ASSERT_TRUE(tasks);
    ASSERT_EQ(2u, tasks->size());
    for (const auto& task : *tasks) {
        AddLocalDiskReplica(service, context.client_id, task.key, task.size,
                            "test_segment");
    }
    const auto cold =
        service.ReconcileWeightRevision({.identity = ready.identity});
    ASSERT_TRUE(cold);
    ASSERT_FALSE(cold->operation_id.has_value());
    ASSERT_EQ(WeightResidencyState::COLD, cold->residency);
    for (const auto& key :
         {std::string("payload-a"), std::string("payload-b")}) {
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

TEST_F(WeightGroupLifecycleTest,
       ColdOperationOffloadsOnlyWeightsAndWaitsForLeaseBeforeDemotion) {
    MasterServiceConfig config;
    config.default_kv_lease_ttl = 0;
    config.enable_offload = true;
    config.offload_on_evict = true;
    MasterService service(config);
    const auto context = PrepareSimpleSegment(service);
    ASSERT_TRUE(service.MountLocalDiskSegment(context.client_id, true));
    auto ready = PublishReady(service, context.client_id);
    auto lease =
        service.AcquireWeightRevisionLease(AcquireWeightRevisionLeaseRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation,
            .holder = "reader",
            .ttl_ms = 60'000,
        });
    ASSERT_TRUE(lease.has_value());
    auto started = service.StartWeightResidencyOperation(
        StartWeightResidencyOperationRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation,
            .target_residency = WeightResidencyState::COLD,
            .mixed_hot_ratio = std::nullopt,
        });
    ASSERT_TRUE(started.has_value());

    ASSERT_TRUE(service.ReconcileWeightRevision(
        ReconcileWeightRevisionRequest{.identity = ready.identity}));
    auto queue = service.OffloadObjectHeartbeat(context.client_id, true);
    ASSERT_TRUE(queue.has_value());
    ASSERT_EQ(1u, queue->size());
    EXPECT_EQ("payload-a", queue->front().key);

    StorageObjectMetadata disk_metadata;
    disk_metadata.key_size = queue->front().key.size();
    disk_metadata.data_size = queue->front().size;
    disk_metadata.transport_endpoint = "test_segment";
    ASSERT_TRUE(service.NotifyOffloadSuccess(context.client_id, *queue,
                                             {disk_metadata}));
    auto leased = service.ReconcileWeightRevision(
        ReconcileWeightRevisionRequest{.identity = ready.identity});
    ASSERT_TRUE(leased.has_value());
    EXPECT_EQ(WeightResidencyState::HOT, leased->residency);
    auto leased_members = GetWeightGroupResidencyForTest(
        service, ready.identity, ready.manifest.payload_group_id);
    for (const auto& member : leased_members) {
        EXPECT_TRUE(member.has_memory);
    }

    ASSERT_TRUE(service.ReleaseWeightRevisionLease(
        ReleaseWeightRevisionLeaseRequest{.lease_id = lease->lease_id}));
    auto cold = service.ReconcileWeightRevision(
        ReconcileWeightRevisionRequest{.identity = ready.identity});
    ASSERT_TRUE(cold.has_value());
    EXPECT_EQ(WeightResidencyState::COLD, cold->residency);

    const auto members = GetWeightGroupResidencyForTest(
        service, ready.identity, ready.manifest.payload_group_id);
    for (const auto& member : members) {
        if (member.data_type == ObjectDataType::METADATA) {
            EXPECT_TRUE(member.has_memory);
        } else {
            EXPECT_FALSE(member.has_memory);
            EXPECT_TRUE(member.has_cold);
        }
    }
}

TEST_F(WeightGroupLifecycleTest,
       ColdWriteFailureStaysReadyAndOperationRemainsRetryable) {
    MasterServiceConfig config;
    config.default_kv_lease_ttl = 0;
    config.enable_offload = true;
    config.offload_on_evict = true;
    MasterService service(config);
    const auto context = PrepareSimpleSegment(service);
    ASSERT_TRUE(service.MountLocalDiskSegment(context.client_id, true));
    auto ready = PublishReady(service, context.client_id);
    auto started = service.StartWeightResidencyOperation(
        StartWeightResidencyOperationRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation,
            .target_residency = WeightResidencyState::COLD,
            .mixed_hot_ratio = std::nullopt,
        });
    ASSERT_TRUE(started.has_value());
    ASSERT_TRUE(service.ReconcileWeightRevision(
        ReconcileWeightRevisionRequest{.identity = ready.identity}));
    auto failed_task = service.OffloadObjectHeartbeat(context.client_id, true);
    ASSERT_TRUE(failed_task.has_value());
    ASSERT_EQ(1u, failed_task->size());
    ASSERT_TRUE(service.NotifyOffloadSuccess(
        context.client_id, *failed_task,
        {StorageObjectMetadata{-1, 0, 0, -1, ""}}));

    auto failed = service.GetWeightRevision(
        GetWeightRevisionRequest{.identity = ready.identity});
    ASSERT_TRUE(failed.has_value());
    EXPECT_EQ(WeightAvailabilityState::READY, failed->metadata.availability);
    EXPECT_EQ(WeightResidencyState::HOT, failed->metadata.residency);
    ASSERT_TRUE(failed->metadata.operation_id.has_value());
    auto failed_operation =
        service.QueryWeightOperation(QueryWeightOperationRequest{
            .operation_id = *failed->metadata.operation_id,
        });
    ASSERT_TRUE(failed_operation.has_value());
    EXPECT_EQ("cold replica write failed", failed_operation->message);
    const auto readable_members = GetWeightGroupResidencyForTest(
        service, ready.identity, ready.manifest.payload_group_id);
    for (const auto& member : readable_members) {
        EXPECT_TRUE(member.has_memory);
    }

    ASSERT_TRUE(service.ReconcileWeightRevision(
        ReconcileWeightRevisionRequest{.identity = ready.identity}));
    auto retry_task = service.OffloadObjectHeartbeat(context.client_id, true);
    ASSERT_TRUE(retry_task.has_value());
    ASSERT_EQ(1u, retry_task->size());
    StorageObjectMetadata disk_metadata;
    disk_metadata.key_size = retry_task->front().key.size();
    disk_metadata.data_size = retry_task->front().size;
    disk_metadata.transport_endpoint = "test_segment";
    ASSERT_TRUE(service.NotifyOffloadSuccess(context.client_id, *retry_task,
                                             {disk_metadata}));
    auto cold = service.ReconcileWeightRevision(
        ReconcileWeightRevisionRequest{.identity = ready.identity});
    ASSERT_TRUE(cold.has_value());
    EXPECT_EQ(WeightResidencyState::COLD, cold->residency);
    EXPECT_FALSE(cold->operation_id.has_value());
}

TEST_F(WeightGroupLifecycleTest,
       MixedOperationKeepsEveryAffinityWholeAndReportsActualRatio) {
    MasterServiceConfig config;
    config.default_kv_lease_ttl = 0;
    config.enable_offload = true;
    config.offload_on_evict = true;
    MasterService service(config);
    const auto context = PrepareSimpleSegment(service);
    ASSERT_TRUE(service.MountLocalDiskSegment(context.client_id, true));
    auto ready = PublishReadyWithPayloads(
        service, context.client_id,
        {
            {.key = "payload-a0", .size = 300, .affinity_id = "a"},
            {.key = "payload-a1", .size = 200, .affinity_id = "a"},
            {.key = "payload-b", .size = 400, .affinity_id = "b"},
            {.key = "payload-c", .size = 100, .affinity_id = "c"},
        });
    for (const auto& [key, size] : std::vector<std::pair<std::string, int64_t>>{
             {"payload-a0", 300},
             {"payload-a1", 200},
             {"payload-b", 400},
             {"payload-c", 100},
         }) {
        AddLocalDiskReplica(service, context.client_id, key, size,
                            "test_segment");
    }
    auto started = service.StartWeightResidencyOperation(
        StartWeightResidencyOperationRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation,
            .target_residency = WeightResidencyState::MIXED,
            .mixed_hot_ratio = 0.5,
        });
    ASSERT_TRUE(started.has_value());

    auto mixed = service.ReconcileWeightRevision(
        ReconcileWeightRevisionRequest{.identity = ready.identity});

    ASSERT_TRUE(mixed.has_value());
    EXPECT_EQ(WeightResidencyState::MIXED, mixed->residency);
    EXPECT_DOUBLE_EQ(0.5, mixed->observed_hot_ratio);
    EXPECT_FALSE(mixed->operation_id.has_value());
    auto completed = service.QueryWeightOperation(
        QueryWeightOperationRequest{.operation_id = started->operation_id});
    ASSERT_TRUE(completed.has_value());
    EXPECT_EQ(3, completed->processed_units);
    EXPECT_EQ(1000, completed->processed_bytes);

    std::map<std::string, std::optional<bool>> affinity_memory;
    const auto members = GetWeightGroupResidencyForTest(
        service, ready.identity, ready.manifest.payload_group_id);
    for (const auto& member : members) {
        if (member.data_type == ObjectDataType::METADATA) {
            EXPECT_TRUE(member.has_memory);
            continue;
        }
        auto& expected = affinity_memory[member.residency_affinity_id];
        if (!expected.has_value()) {
            expected = member.has_memory;
        }
        EXPECT_EQ(*expected, member.has_memory);
    }
}

TEST_F(WeightGroupLifecycleTest, RehydrateQueuesAndCompletesWholeManagedGroup) {
    MasterServiceConfig config;
    config.default_kv_lease_ttl = 0;
    config.enable_offload = true;
    config.offload_on_evict = true;
    MasterService service(config);
    const auto context = PrepareSimpleSegment(service);
    const UUID client_id = context.client_id;
    ASSERT_TRUE(service.MountLocalDiskSegment(client_id, true));
    auto ready = PublishReady(service, client_id);
    EXPECT_FALSE(IsObjectProcessingForTest(service, "payload-a"));
    ASSERT_TRUE(service.StartWeightResidencyOperation(
        StartWeightResidencyOperationRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation,
            .target_residency = WeightResidencyState::COLD,
            .mixed_hot_ratio = std::nullopt,
        }));
    ASSERT_TRUE(service.ReconcileWeightRevision(
        ReconcileWeightRevisionRequest{.identity = ready.identity}));
    auto offload = service.OffloadObjectHeartbeat(client_id, true);
    ASSERT_TRUE(offload.has_value());
    ASSERT_EQ(1u, offload->size());
    StorageObjectMetadata disk_metadata;
    disk_metadata.key_size = offload->front().key.size();
    disk_metadata.data_size = offload->front().size;
    disk_metadata.transport_endpoint = "test_segment";
    ASSERT_TRUE(
        service.NotifyOffloadSuccess(client_id, *offload, {disk_metadata}));
    auto cold = service.ReconcileWeightRevision(
        ReconcileWeightRevisionRequest{.identity = ready.identity});
    ASSERT_TRUE(cold.has_value());
    ASSERT_EQ(WeightResidencyState::COLD, cold->residency);
    EXPECT_FALSE(IsObjectProcessingForTest(service, "payload-a"));

    auto started = service.StartWeightResidencyOperation(
        StartWeightResidencyOperationRequest{
            .identity = cold->identity,
            .expected_metadata_generation = cold->metadata_generation,
            .target_residency = WeightResidencyState::HOT,
            .mixed_hot_ratio = std::nullopt,
        });
    ASSERT_TRUE(started.has_value());
    EXPECT_EQ(WeightOperationKind::MIGRATING, started->kind);
    ASSERT_TRUE(service.ReconcileWeightRevision(
        ReconcileWeightRevisionRequest{.identity = cold->identity}));

    size_t promoted = 0;
    while (promoted < 1) {
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
    EXPECT_FALSE(hot->operation_id.has_value());
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
        if (key == "payload-a") {
            put.residency_affinity_ids = std::vector<std::string>{"affinity-a"};
        } else {
            put.residency_affinity_ids.reset();
        }
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

TEST_F(WeightGroupLifecycleTest, UnitBudgetBoundsEvictionAndPromotion) {
    const auto config = MasterServiceConfig::builder()
                            .set_default_kv_lease_ttl(0)
                            .set_enable_offload(true)
                            .set_root_fs_dir("/mnt/ssd")
                            .build();
    MasterService service(config);
    const auto context = PrepareSimpleSegment(service);
    const auto client_id = context.client_id;
    ASSERT_TRUE(service.MountLocalDiskSegment(client_id, true));
    const std::vector<std::string> keys{"unit-a-1", "unit-a-2", "unit-b-1",
                                        "unit-b-2"};
    const auto ready = PublishReadyWithPayloads(service, client_id,
                                                {{keys[0], 1024, "unit-a"},
                                                 {keys[1], 1024, "unit-a"},
                                                 {keys[2], 1024, "unit-b"},
                                                 {keys[3], 1024, "unit-b"}});
    for (const auto& key : keys) {
        ASSERT_TRUE(service.PutEnd(client_id, key, TenantId::Default(),
                                   ReplicaType::DISK));
        AddLocalDiskReplica(service, client_id, key, 1024, "test_segment");
    }
    ASSERT_TRUE(service.StartWeightResidencyOperation({
        .identity = ready.identity,
        .expected_metadata_generation = ready.metadata_generation,
        .target_residency = WeightResidencyState::COLD,
        .mixed_hot_ratio = std::nullopt,
    }));
    auto has_memory = [&](const std::string& key) {
        const auto replicas =
            service.GetReplicaListForAdmin(key, TenantId::Default());
        EXPECT_TRUE(replicas);
        return replicas &&
               std::any_of(replicas->replicas.begin(), replicas->replicas.end(),
                           [](const auto& replica) {
                               return replica.is_memory_replica();
                           });
    };
    MasterServiceTestPeer(service).RunWeightReconciliationForTesting(
        ready.updated_at_ms + 1, 1);
    EXPECT_EQ(2, std::count_if(keys.begin(), keys.end(), has_memory));
    EXPECT_EQ(has_memory(keys[0]), has_memory(keys[1]));
    EXPECT_EQ(has_memory(keys[2]), has_memory(keys[3]));
    EXPECT_TRUE(has_memory(MakeWeightManifestKey(ready.identity)));
    MasterServiceTestPeer(service).RunWeightReconciliationForTesting(
        ready.updated_at_ms + 2, 1);
    const auto cold = service.GetWeightRevision({.identity = ready.identity});
    ASSERT_TRUE(cold);
    ASSERT_EQ(WeightResidencyState::COLD, cold->metadata.residency);
    EXPECT_EQ(0, std::count_if(keys.begin(), keys.end(), has_memory));
    EXPECT_TRUE(has_memory(MakeWeightManifestKey(ready.identity)));
    ASSERT_TRUE(service.StartWeightResidencyOperation({
        .identity = ready.identity,
        .expected_metadata_generation = cold->metadata.metadata_generation,
        .target_residency = WeightResidencyState::HOT,
        .mixed_hot_ratio = std::nullopt,
    }));
    std::string first_affinity;
    for (int i = 0; i < 2; ++i) {
        MasterServiceTestPeer(service).RunWeightReconciliationForTesting(
            ready.updated_at_ms + 3 + i, 1);
        std::vector<PromotionTaskItem> pending;
        for (int heartbeat = 0; heartbeat < 2; ++heartbeat) {
            const auto tasks = service.PromotionObjectHeartbeat(client_id);
            ASSERT_TRUE(tasks);
            ASSERT_EQ(1u, tasks->size());
            pending.push_back(tasks->front());
        }
        const auto extra = service.PromotionObjectHeartbeat(client_id);
        ASSERT_TRUE(extra);
        EXPECT_TRUE(extra->empty());
        const auto affinity = pending.front().key.substr(0, 6);
        EXPECT_EQ(affinity, pending.back().key.substr(0, 6));
        if (i == 0)
            first_affinity = affinity;
        else
            EXPECT_NE(first_affinity, affinity);
        for (const auto& task : pending) {
            ASSERT_TRUE(service.PromotionAllocStart(
                client_id, task.key, TenantId(task.tenant_id), task.size, {}));
            ASSERT_TRUE(service.NotifyPromotionSuccess(
                client_id, task.key, TenantId(task.tenant_id)));
        }
        EXPECT_EQ(static_cast<size_t>((i + 1) * 2),
                  std::count_if(keys.begin(), keys.end(), has_memory));
        EXPECT_TRUE(has_memory(MakeWeightManifestKey(ready.identity)));
    }
    MasterServiceTestPeer(service).RunWeightReconciliationForTesting(
        ready.updated_at_ms + 5, 1);
    const auto hot = service.GetWeightRevision({.identity = ready.identity});
    ASSERT_TRUE(hot);
    EXPECT_EQ(WeightResidencyState::HOT, hot->metadata.residency);
    EXPECT_FALSE(hot->metadata.operation_id.has_value());
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
    EXPECT_TRUE(reconciled->operation_id.has_value());
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

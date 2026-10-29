#include "master_service_test_fixture.h"

#include <gtest/gtest.h>

#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include "master_metric_manager.h"
#include "weight_management.h"

namespace mooncake::test {
namespace {

class WeightReconciliationTest : public MasterServiceTest {
   protected:
    static WeightRevisionIdentity Identity(std::string revision) {
        return WeightRevisionIdentity{
            .tenant_id = "default",
            .name_space = "production",
            .resource_id = "llama-70b",
            .revision = std::move(revision),
            .weight_generation = 7,
        };
    }

    static std::string ManifestKey(const WeightRevisionIdentity& identity) {
        return "weights/" + identity.name_space + "/" + identity.resource_id +
               "/" + identity.revision + "/" +
               std::to_string(identity.weight_generation) + "/manifest";
    }

    static BeginWeightImportRequest ImportRequest(
        const WeightRevisionIdentity& identity, uint64_t payload_count,
        uint64_t logical_bytes) {
        return {
            .identity = identity,
            .expected_payload_count = payload_count,
            .expected_logical_bytes = logical_bytes,
            .policy =
                WeightStoragePolicy{
                    .preferred_residency = WeightResidencyState::HOT,
                    .migration_mode = WeightMigrationMode::MANUAL},
            .affinity_summary = {.affinity_count = 1,
                                 .affinity_digest = std::string(64, 'c')},
        };
    }

    WeightRevisionMetadata PublishReady(MasterService& service,
                                        const UUID& client_id,
                                        std::string revision) {
        const auto identity = Identity(std::move(revision));
        auto importing = service.BeginWeightImport(BeginWeightImportRequest{
            .identity = identity,
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
        EXPECT_TRUE(importing.has_value());
        const auto payload_key = identity.revision + "-payload";
        ReplicateConfig config;
        config.replica_num = 1;
        config.with_hard_pin = true;
        config.group_ids =
            std::vector<std::string>{importing->manifest.payload_group_id};
        config.residency_affinity_ids =
            std::vector<std::string>{"affinity-" + identity.revision};
        config.data_type = ObjectDataType::WEIGHT;
        PutCompletedObject(service, client_id, payload_key, config, 1024);
        config.data_type = ObjectDataType::METADATA;
        config.residency_affinity_ids.reset();
        PutCompletedObject(service, client_id, ManifestKey(identity), config,
                           128);
        auto ready = service.CommitWeightImport(CommitWeightImportRequest{
            .identity = identity,
            .expected_metadata_generation = importing->metadata_generation,
            .manifest =
                WeightManifestReference{
                    .manifest_key = ManifestKey(identity),
                    .manifest_sha256 = std::string(64, 'a'),
                    .payload_group_id = importing->manifest.payload_group_id,
                    .payload_keys_sha256 =
                        ComputeWeightPayloadKeysSha256({payload_key}),
                    .payload_count = 1,
                    .logical_bytes = 1024,
                },
        });
        EXPECT_TRUE(ready.has_value());
        return *ready;
    }

    void AssertStaleGroupMember(MasterService& service, const std::string& key,
                                const std::string& group_id) {
        const auto tenant = TenantId::Default();
        const auto member = MasterServiceTestPeer::ReadObject(
            service, MasterServiceTestPeer::ObjectIdentity{tenant, key});
        ASSERT_TRUE(member.has_value());
        const auto& metadata = member->metadata();
        ASSERT_EQ(group_id, metadata.group_id);
        ASSERT_TRUE(metadata.HasReplica([](const Replica& replica) {
            return replica.is_memory_replica();
        }));
        ASSERT_TRUE(metadata.AllReplicas([](const Replica& replica) {
            return replica.is_memory_replica() &&
                   replica.has_invalid_mem_handle();
        }));
    }

    void CheckContinuousLeaseTraffic(size_t limit) {
        MasterService service;
        const auto context = PrepareSimpleSegment(service);
        const auto damaged =
            PublishReady(service, context.client_id, "a-lease-traffic-damaged");
        const auto leased =
            PublishReady(service, context.client_id, "z-lease-traffic-healthy");
        ASSERT_TRUE(
            MasterServiceTestPeer(service).DropWeightGroupMemberForTesting(
                damaged.identity, "a-lease-traffic-damaged-payload"));
        size_t created = 0;
        size_t previously_released = 0;
        for (int round = 0; round < 6; ++round) {
            uint64_t sweep_time = 0;
            for (size_t i = 0; i < limit; ++i) {
                const auto lease = service.AcquireWeightRevisionLease({
                    .identity = leased.identity,
                    .expected_metadata_generation = leased.metadata_generation,
                    .holder = "lease-traffic-" + std::to_string(round) + "-" +
                              std::to_string(i),
                    .ttl_ms = 60'000,
                });
                ASSERT_TRUE(lease);
                ++created;
                sweep_time = std::max(sweep_time, lease->expires_at_ms + 1);
            }
            EXPECT_LE(MasterServiceTestPeer(service)
                          .RunWeightReconciliationForTesting(sweep_time, limit),
                      limit);
            if ((round + 1) % 2 == 0) {
                const auto snapshot =
                    MasterServiceTestPeer::WeightManager(service)
                        .ExportSnapshot();
                ASSERT_LE(snapshot.leases.size(), created);
                const auto released = created - snapshot.leases.size();
                EXPECT_GT(released, previously_released) << "round=" << round;
                previously_released = released;
            }
        }
        const auto observed =
            service.GetWeightRevision({.identity = damaged.identity});
        ASSERT_TRUE(observed);
        EXPECT_EQ(WeightAvailabilityState::DEGRADED,
                  observed->metadata.availability);
    }
    void CheckPublicPayloadLoss(size_t lost_payloads, bool start_cold,
                                WeightResidencyState expected_residency,
                                double expected_hot_ratio,
                                bool lose_completed_member = false) {
        MasterServiceConfig config;
        config.default_kv_lease_ttl = 0;
        config.enable_offload = start_cold;
        config.offload_on_evict = start_cold;
        MasterService service(config);
        const auto context = PrepareSimpleSegment(service);
        if (start_cold) {
            ASSERT_TRUE(service.MountLocalDiskSegment(context.client_id, true));
        }
        const auto identity = Identity("step-public-payload-loss");
        const std::vector<std::string> payload_keys{"payload-a", "payload-b"};
        const auto importing =
            service.BeginWeightImport(BeginWeightImportRequest{
                .identity = identity,
                .expected_payload_count = 2,
                .expected_logical_bytes = 2048,
                .policy =
                    WeightStoragePolicy{
                        .preferred_residency = WeightResidencyState::HOT,
                        .mixed_hot_ratio = 0.5,
                        .migration_mode = WeightMigrationMode::MANUAL,
                    },
                .affinity_summary =
                    WeightAffinitySummary{
                        .affinity_count = 2,
                        .affinity_digest = std::string(64, 'c'),
                    },
            });
        ASSERT_TRUE(importing.has_value());
        ReplicateConfig put_config;
        put_config.replica_num = 1;
        put_config.with_hard_pin = true;
        put_config.group_ids =
            std::vector<std::string>{importing->manifest.payload_group_id};
        put_config.data_type = ObjectDataType::WEIGHT;
        for (const auto& key : payload_keys) {
            put_config.residency_affinity_ids =
                std::vector<std::string>{"affinity-" + key};
            PutCompletedObject(service, context.client_id, key, put_config,
                               1024);
        }
        put_config.data_type = ObjectDataType::METADATA;
        put_config.residency_affinity_ids.reset();
        PutCompletedObject(service, context.client_id, ManifestKey(identity),
                           put_config, 128);
        const auto ready = service.CommitWeightImport(CommitWeightImportRequest{
            .identity = identity,
            .expected_metadata_generation = importing->metadata_generation,
            .manifest =
                WeightManifestReference{
                    .manifest_key = ManifestKey(identity),
                    .manifest_sha256 = std::string(64, 'a'),
                    .payload_group_id = importing->manifest.payload_group_id,
                    .payload_keys_sha256 =
                        ComputeWeightPayloadKeysSha256(payload_keys),
                    .payload_count = 2,
                    .logical_bytes = 2048,
                },
        });
        ASSERT_TRUE(ready.has_value());

        if (start_cold) {
            const auto started = service.StartWeightResidencyOperation(
                StartWeightResidencyOperationRequest{
                    .identity = identity,
                    .expected_metadata_generation = ready->metadata_generation,
                    .target_residency = WeightResidencyState::COLD,
                    .mixed_hot_ratio = std::nullopt,
                });
            ASSERT_TRUE(started.has_value());
            ASSERT_TRUE(service.ReconcileWeightRevision(
                ReconcileWeightRevisionRequest{.identity = identity}));
            const auto tasks =
                service.OffloadObjectHeartbeat(context.client_id, true);
            ASSERT_TRUE(tasks.has_value());
            ASSERT_EQ(2u, tasks->size());
            if (lose_completed_member) {
                const auto completed_task = tasks->front();
                StorageObjectMetadata completed_metadata{};
                completed_metadata.key_size = completed_task.key.size();
                completed_metadata.data_size = completed_task.size;
                completed_metadata.transport_endpoint = "test_segment";
                ASSERT_TRUE(service.NotifyOffloadSuccess(
                    context.client_id, {completed_task}, {completed_metadata}));
                const auto partial = service.ReconcileWeightRevision(
                    ReconcileWeightRevisionRequest{.identity = identity});
                ASSERT_TRUE(partial.has_value());
                ASSERT_TRUE(partial->operation_id.has_value());
                ASSERT_EQ(WeightAvailabilityState::READY,
                          partial->availability);
                ASSERT_EQ(WeightResidencyState::MIXED, partial->residency);
                ASSERT_DOUBLE_EQ(0.5, partial->observed_hot_ratio);
                const auto before =
                    service.QueryWeightOperation(QueryWeightOperationRequest{
                        .operation_id = started->operation_id,
                    });
                ASSERT_TRUE(before.has_value());
                ASSERT_EQ(1u, before->processed_units);
                ASSERT_EQ(1024u, before->processed_bytes);
                ASSERT_EQ(2u, before->total_units);
                ASSERT_EQ(2048u, before->total_bytes);
                ASSERT_NE("completed", before->message);
                const auto cleared = service.BatchReplicaClear(
                    {completed_task.key}, context.client_id, "");
                ASSERT_TRUE(cleared.has_value());
                ASSERT_EQ(std::vector<std::string>{completed_task.key},
                          *cleared);
                ASSERT_FALSE(
                    service.ExistKey(completed_task.key, TenantId::Default())
                        .value_or(true));
                const auto remaining_key = completed_task.key == payload_keys[0]
                                               ? payload_keys[1]
                                               : payload_keys[0];
                ASSERT_TRUE(service.ExistKey(remaining_key, TenantId::Default())
                                .value_or(false));
                const auto reconciled = service.ReconcileWeightRevision(
                    ReconcileWeightRevisionRequest{.identity = identity});
                EXPECT_TRUE(reconciled.has_value());
                const auto after = service.GetWeightRevision(
                    GetWeightRevisionRequest{.identity = identity});
                ASSERT_TRUE(after.has_value());
                EXPECT_EQ(WeightAvailabilityState::DEGRADED,
                          after->metadata.availability);
                EXPECT_EQ(expected_residency, after->metadata.residency);
                EXPECT_DOUBLE_EQ(expected_hot_ratio,
                                 after->metadata.observed_hot_ratio);
                EXPECT_EQ(started->operation_id, after->metadata.operation_id);
                const auto operation =
                    service.QueryWeightOperation(QueryWeightOperationRequest{
                        .operation_id = started->operation_id,
                    });
                ASSERT_TRUE(operation.has_value());
                EXPECT_EQ(before->processed_units, operation->processed_units);
                EXPECT_EQ(before->processed_bytes, operation->processed_bytes);
                EXPECT_EQ(before->cursor, operation->cursor);
                EXPECT_NE("completed", operation->message);
                EXPECT_EQ(after->metadata.metadata_generation,
                          operation->fenced_metadata_generation);
                return;
            }
            std::vector<StorageObjectMetadata> disk_metadata;
            for (const auto& task : *tasks) {
                ASSERT_TRUE(task.key == payload_keys[0] ||
                            task.key == payload_keys[1]);
                StorageObjectMetadata metadata{};
                metadata.key_size = task.key.size();
                metadata.data_size = task.size;
                metadata.transport_endpoint = "test_segment";
                disk_metadata.push_back(std::move(metadata));
            }
            ASSERT_TRUE(service.NotifyOffloadSuccess(context.client_id, *tasks,
                                                     disk_metadata));
            const auto cold = service.ReconcileWeightRevision(
                ReconcileWeightRevisionRequest{.identity = identity});
            ASSERT_TRUE(cold.has_value());
            ASSERT_EQ(WeightAvailabilityState::READY, cold->availability);
            ASSERT_EQ(WeightResidencyState::COLD, cold->residency);
            ASSERT_FALSE(cold->operation_id.has_value());
        }

        const std::vector<std::string> removed_keys(
            payload_keys.begin(), payload_keys.begin() + lost_payloads);
        const auto cleared =
            service.BatchReplicaClear(removed_keys, context.client_id, "");
        ASSERT_TRUE(cleared.has_value());
        ASSERT_EQ(lost_payloads, cleared->size());
        for (const auto& key : removed_keys) {
            EXPECT_FALSE(
                service.ExistKey(key, TenantId::Default()).value_or(true));
        }
        for (size_t i = lost_payloads; i < payload_keys.size(); ++i) {
            EXPECT_TRUE(service.ExistKey(payload_keys[i], TenantId::Default())
                            .value_or(false));
        }
        EXPECT_TRUE(service.ExistKey(ManifestKey(identity), TenantId::Default())
                        .value_or(false));
        const auto reconciled = service.ReconcileWeightRevision(
            ReconcileWeightRevisionRequest{.identity = identity});
        ASSERT_TRUE(reconciled.has_value());
        EXPECT_EQ(WeightAvailabilityState::DEGRADED, reconciled->availability);
        EXPECT_EQ(expected_residency, reconciled->residency);
        EXPECT_DOUBLE_EQ(expected_hot_ratio, reconciled->observed_hot_ratio);
        const auto view = service.GetWeightRevision(
            GetWeightRevisionRequest{.identity = identity});
        ASSERT_TRUE(view.has_value());
        EXPECT_EQ(*reconciled, view->metadata);
    }
};

TEST_F(WeightReconciliationTest, ConfiguredIdleTimeoutControlsAbandonment) {
    auto config = MasterServiceConfig::builder()
                      .set_weight_import_idle_timeout_ms(50)
                      .build();
    MasterService service(config);
    const auto importing = service.BeginWeightImport(
        ImportRequest(Identity("configured-idle"), 1, 1024));
    ASSERT_TRUE(importing);
    MasterServiceTestPeer(service).RunWeightReconciliationForTesting(
        importing->updated_at_ms + 100, 32);
    const auto observed =
        service.GetWeightRevision({.identity = importing->identity});
    ASSERT_TRUE(observed);
    EXPECT_EQ(WeightAvailabilityState::DELETING,
              observed->metadata.availability);
    config.weight_import_idle_timeout_ms = 0;
    EXPECT_THROW(MasterService rejected(config), std::invalid_argument);
}

TEST_F(WeightReconciliationTest, AcceptedPutStartRefreshesImportInactivity) {
    MasterService service;
    const auto context = PrepareSimpleSegment(service);
    const auto importing = service.BeginWeightImport(
        ImportRequest(Identity("active-start"), 1, 1024));
    ASSERT_TRUE(importing);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    ReplicateConfig config;
    config.replica_num = 1;
    config.data_type = ObjectDataType::WEIGHT;
    config.residency_affinity_ids =
        std::vector<std::string>{"affinity-" + importing->identity.revision};
    config.group_ids =
        std::vector<std::string>{importing->manifest.payload_group_id};
    ASSERT_TRUE(service.PutStart(context.client_id, "active-start-payload",
                                 TenantId::Default(), 1024, config));
    MasterServiceTestPeer(service).RunWeightReconciliationForTesting(
        importing->updated_at_ms + 300'001, 32);
    const auto observed =
        service.GetWeightRevision({.identity = importing->identity});
    ASSERT_TRUE(observed);
    EXPECT_EQ(WeightAvailabilityState::IMPORTING,
              observed->metadata.availability);
}

TEST_F(WeightReconciliationTest, AcceptedPutEndRefreshesImportInactivity) {
    MasterService service;
    const auto context = PrepareSimpleSegment(service);
    const auto importing = service.BeginWeightImport(
        ImportRequest(Identity("active-end"), 1, 1024));
    ASSERT_TRUE(importing);
    ReplicateConfig config;
    config.replica_num = 1;
    config.data_type = ObjectDataType::WEIGHT;
    config.residency_affinity_ids =
        std::vector<std::string>{"affinity-" + importing->identity.revision};
    config.group_ids =
        std::vector<std::string>{importing->manifest.payload_group_id};
    ASSERT_TRUE(service.PutStart(context.client_id, "active-end-payload",
                                 TenantId::Default(), 1024, config));
    const auto before_end_ms = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    ASSERT_TRUE(service.PutEnd(context.client_id, "active-end-payload",
                               TenantId::Default(), ReplicaType::ALL));
    MasterServiceTestPeer(service).RunWeightReconciliationForTesting(
        before_end_ms + 300'001, 32);
    const auto observed =
        service.GetWeightRevision({.identity = importing->identity});
    ASSERT_TRUE(observed);
    EXPECT_EQ(WeightAvailabilityState::IMPORTING,
              observed->metadata.availability);
}

TEST_F(WeightReconciliationTest,
       UpsertWithExplicitGroupRefreshesImportInactivity) {
    MasterService service;
    const auto context = PrepareSimpleSegment(service);
    const auto importing = service.BeginWeightImport(
        ImportRequest(Identity("active-upsert"), 1, 1024));
    ASSERT_TRUE(importing);
    ReplicateConfig config;
    config.replica_num = 1;
    config.data_type = ObjectDataType::WEIGHT;
    config.residency_affinity_ids =
        std::vector<std::string>{"affinity-" + importing->identity.revision};
    config.group_ids =
        std::vector<std::string>{importing->manifest.payload_group_id};
    PutCompletedObject(service, context.client_id, "active-upsert-payload",
                       config, 1024);
    const auto before_upsert_ms = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    ASSERT_TRUE(service.UpsertStart(context.client_id, "active-upsert-payload",
                                    TenantId::Default(), 1024, config));
    MasterServiceTestPeer(service).RunWeightReconciliationForTesting(
        before_upsert_ms + 300'001, 32);
    const auto observed =
        service.GetWeightRevision({.identity = importing->identity});
    ASSERT_TRUE(observed);
    EXPECT_EQ(WeightAvailabilityState::IMPORTING,
              observed->metadata.availability);
}

TEST_F(WeightReconciliationTest, UpsertWithInheritedManagedGroupRejectsReady) {
    for (const bool manifest : {false, true}) {
        for (const bool resize : {false, true}) {
            SCOPED_TRACE(::testing::Message()
                         << "manifest=" << manifest << ", resize=" << resize);
            MasterService service;
            const auto context = PrepareSimpleSegment(service);
            const auto ready =
                PublishReady(service, context.client_id, "upsert-ready");
            const auto key =
                manifest ? ManifestKey(ready.identity) : "upsert-ready-payload";
            ReplicateConfig config;
            config.replica_num = 1;
            config.data_type =
                manifest ? ObjectDataType::METADATA : ObjectDataType::WEIGHT;
            const size_t length = manifest ? 128 : 1024;
            const auto result =
                service.UpsertStart(context.client_id, key, TenantId::Default(),
                                    resize ? length * 2 : length, config);
            EXPECT_FALSE(result);
            if (!result) {
                EXPECT_EQ(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS,
                          result.error());
            }
            EXPECT_TRUE(service.GetReplicaList(key, TenantId::Default()));
            const auto observed =
                service.GetWeightRevision({.identity = ready.identity});
            ASSERT_TRUE(observed);
            EXPECT_EQ(WeightAvailabilityState::READY,
                      observed->metadata.availability);
        }
    }
}

TEST_F(WeightReconciliationTest,
       UpsertWithInheritedManagedGroupRejectsDeleting) {
    MasterService service;
    const auto context = PrepareSimpleSegment(service);
    const auto importing = service.BeginWeightImport(
        ImportRequest(Identity("upsert-deleting"), 1, 1024));
    ASSERT_TRUE(importing);
    ReplicateConfig config;
    config.replica_num = 1;
    config.data_type = ObjectDataType::WEIGHT;
    config.residency_affinity_ids =
        std::vector<std::string>{"affinity-" + importing->identity.revision};
    config.group_ids =
        std::vector<std::string>{importing->manifest.payload_group_id};
    PutCompletedObject(service, context.client_id, "upsert-deleting-payload",
                       config, 1024);
    const auto deleting = service.AbortWeightImport({
        .identity = importing->identity,
        .expected_metadata_generation = importing->metadata_generation,
    });
    ASSERT_TRUE(deleting);
    ASSERT_EQ(WeightAvailabilityState::DELETING, deleting->availability);
    ASSERT_TRUE(
        service.GetReplicaList("upsert-deleting-payload", TenantId::Default()));
    config.group_ids.reset();
    const auto result =
        service.UpsertStart(context.client_id, "upsert-deleting-payload",
                            TenantId::Default(), 1024, config);
    ASSERT_FALSE(result);
    EXPECT_EQ(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS, result.error());
}

TEST_F(WeightReconciliationTest,
       UpsertWithInheritedManagedGroupRejectsStaleReplicas) {
    MasterService service;
    const auto context = PrepareSimpleSegment(service);
    const auto ready = PublishReady(service, context.client_id, "upsert-stale");
    PauseReplicaCleanup(service);
    ASSERT_TRUE(service.UnmountSegment(context.segment_id, context.client_id));
    const auto replacement = PrepareSimpleSegment(service, "replacement");
    ASSERT_NO_FATAL_FAILURE(AssertStaleGroupMember(
        service, "upsert-stale-payload", ready.manifest.payload_group_id));
    ReplicateConfig config;
    config.replica_num = 1;
    config.data_type = ObjectDataType::WEIGHT;
    const auto result =
        service.UpsertStart(replacement.client_id, "upsert-stale-payload",
                            TenantId::Default(), 1024, config);
    ASSERT_FALSE(result);
    EXPECT_EQ(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS, result.error());
    const auto members =
        GetGroupMemberKeysForTest(service, ready.manifest.payload_group_id);
    EXPECT_EQ(2, members.size());
    EXPECT_NE(members.end(), std::find(members.begin(), members.end(),
                                       "upsert-stale-payload"));
}

TEST_F(WeightReconciliationTest,
       UpsertWithInheritedManagedGroupRejectsImporting) {
    MasterService service;
    const auto context = PrepareSimpleSegment(service);
    const auto importing = service.BeginWeightImport(
        ImportRequest(Identity("upsert-importing"), 1, 1024));
    ASSERT_TRUE(importing);
    ReplicateConfig config;
    config.replica_num = 1;
    config.data_type = ObjectDataType::WEIGHT;
    config.residency_affinity_ids =
        std::vector<std::string>{"affinity-" + importing->identity.revision};
    config.group_ids =
        std::vector<std::string>{importing->manifest.payload_group_id};
    PutCompletedObject(service, context.client_id, "upsert-importing-payload",
                       config, 1024);
    config.group_ids.reset();
    const auto result =
        service.UpsertStart(context.client_id, "upsert-importing-payload",
                            TenantId::Default(), 1024, config);
    ASSERT_FALSE(result);
    EXPECT_EQ(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS, result.error());
    config.group_ids =
        std::vector<std::string>{importing->manifest.payload_group_id};
    ASSERT_TRUE(service.UpsertStart(context.client_id,
                                    "upsert-importing-payload",
                                    TenantId::Default(), 1024, config));
    EXPECT_TRUE(service.UpsertEnd(context.client_id, "upsert-importing-payload",
                                  TenantId::Default(), ReplicaType::ALL));
}

TEST_F(WeightReconciliationTest,
       UpsertWithDifferentManagedGroupRejectsLiveAndStaleMembers) {
    for (const bool manifest : {false, true}) {
        SCOPED_TRACE(::testing::Message() << "manifest=" << manifest);
        MasterService service;
        const auto context = PrepareSimpleSegment(service);
        const auto ready =
            PublishReady(service, context.client_id, "upsert-original");
        const auto importing = service.BeginWeightImport(
            ImportRequest(Identity("upsert-other"), 1, 1024));
        ASSERT_TRUE(importing);
        const auto key =
            manifest ? ManifestKey(ready.identity) : "upsert-original-payload";
        ReplicateConfig config;
        config.replica_num = 1;
        config.data_type =
            manifest ? ObjectDataType::METADATA : ObjectDataType::WEIGHT;
        config.group_ids =
            std::vector<std::string>{importing->manifest.payload_group_id};
        const size_t length = manifest ? 128 : 1024;
        const auto live = service.UpsertStart(
            context.client_id, key, TenantId::Default(), length, config);
        ASSERT_FALSE(live);
        ASSERT_EQ(ErrorCode::INVALID_PARAMS, live.error());

        PauseReplicaCleanup(service);
        ASSERT_TRUE(
            service.UnmountSegment(context.segment_id, context.client_id));
        const auto replacement = PrepareSimpleSegment(service, "replacement");
        ASSERT_NO_FATAL_FAILURE(AssertStaleGroupMember(
            service, key, ready.manifest.payload_group_id));
        const auto stale = service.UpsertStart(
            replacement.client_id, key, TenantId::Default(), length, config);
        EXPECT_FALSE(stale);
        if (!stale) {
            EXPECT_EQ(ErrorCode::INVALID_PARAMS, stale.error());
        }
        const auto original_members =
            GetGroupMemberKeysForTest(service, ready.manifest.payload_group_id);
        EXPECT_EQ(2, original_members.size());
        EXPECT_NE(
            original_members.end(),
            std::find(original_members.begin(), original_members.end(), key));
        const auto other_members = GetGroupMemberKeysForTest(
            service, importing->manifest.payload_group_id);
        EXPECT_TRUE(other_members.empty());
    }
}

TEST_F(WeightReconciliationTest,
       UpsertWithInheritedUnmanagedGroupStillSucceeds) {
    for (const size_t length : {1024, 2048}) {
        MasterService service;
        const auto context = PrepareSimpleSegment(service);
        ReplicateConfig config;
        config.replica_num = 1;
        config.group_ids = std::vector<std::string>{"ordinary-group"};
        PutCompletedObject(service, context.client_id, "ordinary-payload",
                           config, 1024);
        config.group_ids.reset();
        ASSERT_TRUE(service.UpsertStart(context.client_id, "ordinary-payload",
                                        TenantId::Default(), length, config));
        ASSERT_TRUE(service.UpsertEnd(context.client_id, "ordinary-payload",
                                      TenantId::Default(), ReplicaType::ALL));
        EXPECT_TRUE(
            service.GetReplicaList("ordinary-payload", TenantId::Default()));
        EXPECT_EQ(std::vector<std::string>{"ordinary-payload"},
                  GetGroupMemberKeysForTest(service, "ordinary-group"));
    }
}

TEST_F(WeightReconciliationTest,
       RetriesAndRejectedWritesDoNotRefreshInactivity) {
    MasterService service;
    const auto context = PrepareSimpleSegment(service);
    const auto request = ImportRequest(Identity("idle-retries"), 1, 1024);
    const auto importing = service.BeginWeightImport(request);
    ASSERT_TRUE(importing);
    ReplicateConfig config;
    config.replica_num = 1;
    config.data_type = ObjectDataType::WEIGHT;
    config.residency_affinity_ids =
        std::vector<std::string>{"affinity-" + importing->identity.revision};
    config.group_ids =
        std::vector<std::string>{importing->manifest.payload_group_id};
    PutCompletedObject(service, context.client_id, "idle-retries-payload",
                       config, 1024);
    const auto last_write_ms = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    ASSERT_TRUE(service.BeginWeightImport(request));
    ASSERT_TRUE(service.PutEnd(context.client_id, "idle-retries-payload",
                               TenantId::Default(), ReplicaType::ALL));
    const auto rejected =
        service.PutEnd(generate_uuid(), "idle-retries-payload",
                       TenantId::Default(), ReplicaType::ALL);
    ASSERT_FALSE(rejected);
    EXPECT_EQ(ErrorCode::ILLEGAL_CLIENT, rejected.error());
    ASSERT_FALSE(service.PutStart(context.client_id, "idle-retries-payload",
                                  TenantId::Default(), 1024, config));
    MasterServiceTestPeer(service).RunWeightReconciliationForTesting(
        last_write_ms + 300'001, 32);
    const auto observed =
        service.GetWeightRevision({.identity = importing->identity});
    ASSERT_TRUE(observed);
    EXPECT_EQ(WeightAvailabilityState::DELETING,
              observed->metadata.availability);
}

TEST_F(WeightReconciliationTest, UnreadableMemberDoesNotChangeHotResidency) {
    MasterService service;
    const auto context = PrepareSimpleSegment(service);
    const auto ready = PublishReady(service, context.client_id, "unreadable");
    {
        MasterServiceTestPeer::MetadataAccessorRW member(
            &service, MasterServiceTestPeer::ObjectIdentity{
                          TenantId::Default(), ManifestKey(ready.identity)});
        ASSERT_TRUE(member.Exists());
        member.Get().VisitReplicas(
            [](const Replica&) { return true; },
            [](Replica& replica) { replica.mark_processing(); });
    }
    const auto observed =
        service.ReconcileWeightRevision({.identity = ready.identity});
    ASSERT_TRUE(observed);
    EXPECT_EQ(WeightAvailabilityState::DEGRADED, observed->availability);
    EXPECT_EQ(WeightResidencyState::HOT, observed->residency);
}

TEST_F(WeightReconciliationTest, UnreadableOnlyPayloadBecomesAbsent) {
    MasterService service;
    const auto context = PrepareSimpleSegment(service);
    const auto ready = PublishReady(service, context.client_id, "unreadable");
    {
        MasterServiceTestPeer::MetadataAccessorRW member(
            &service, MasterServiceTestPeer::ObjectIdentity{
                          TenantId::Default(), "unreadable-payload"});
        ASSERT_TRUE(member.Exists());
        member.Get().VisitReplicas(
            [](const Replica&) { return true; },
            [](Replica& replica) { replica.mark_processing(); });
    }
    const auto observed =
        service.ReconcileWeightRevision({.identity = ready.identity});
    ASSERT_TRUE(observed);
    EXPECT_EQ(WeightAvailabilityState::DEGRADED, observed->availability);
    EXPECT_EQ(WeightResidencyState::ABSENT, observed->residency);
    EXPECT_DOUBLE_EQ(0.0, observed->observed_hot_ratio);
}

TEST_F(WeightReconciliationTest, RestoredImportGetsFullInactivityGrace) {
    MasterService service;
    auto importing = service.BeginWeightImport(
        ImportRequest(Identity("restored-import"), 1, 1024));
    ASSERT_TRUE(importing);
    auto snapshot =
        MasterServiceTestPeer::WeightManager(service).ExportSnapshot();
    snapshot.metadata.front().created_at_ms -= 600'000;
    snapshot.metadata.front().updated_at_ms -= 600'000;
    ASSERT_TRUE(MasterServiceTestPeer::WeightManager(service).RestoreSnapshot(
        snapshot));
    MasterServiceTestPeer(service).RunWeightReconciliationForTesting(
        importing->updated_at_ms + 1, 32);
    const auto restored =
        service.GetWeightRevision({.identity = importing->identity});
    ASSERT_TRUE(restored);
    EXPECT_EQ(WeightAvailabilityState::IMPORTING,
              restored->metadata.availability);
}
TEST_F(WeightReconciliationTest,
       PublicMemberLossDuringColdMigrationBecomesDegraded) {
    for (const bool lose_manifest : {true, false}) {
        SCOPED_TRACE(lose_manifest ? "manifest loss" : "payload loss");
        MasterServiceConfig config;
        config.default_kv_lease_ttl = 0;
        config.enable_offload = true;
        config.offload_on_evict = true;
        MasterService service(config);
        const auto context = PrepareSimpleSegment(service);
        ASSERT_TRUE(service.MountLocalDiskSegment(context.client_id, true));
        const auto ready = PublishReady(service, context.client_id,
                                        "step-active-migration-loss");
        const auto started = service.StartWeightResidencyOperation(
            StartWeightResidencyOperationRequest{
                .identity = ready.identity,
                .expected_metadata_generation = ready.metadata_generation,
                .target_residency = WeightResidencyState::COLD,
                .mixed_hot_ratio = std::nullopt,
            });
        ASSERT_TRUE(started.has_value());
        const auto lost_key = lose_manifest
                                  ? ManifestKey(ready.identity)
                                  : ready.identity.revision + "-payload";
        const auto cleared =
            service.BatchReplicaClear({lost_key}, context.client_id, "");
        ASSERT_TRUE(cleared.has_value());
        ASSERT_EQ(std::vector<std::string>{lost_key}, *cleared);
        ASSERT_FALSE(
            service.ExistKey(lost_key, TenantId::Default()).value_or(true));
        const auto reconciled = service.ReconcileWeightRevision(
            ReconcileWeightRevisionRequest{.identity = ready.identity});
        EXPECT_TRUE(reconciled.has_value());
        const auto view = service.GetWeightRevision(
            GetWeightRevisionRequest{.identity = ready.identity});
        ASSERT_TRUE(view.has_value());
        EXPECT_EQ(WeightAvailabilityState::DEGRADED,
                  view->metadata.availability);
        EXPECT_EQ(lose_manifest ? WeightResidencyState::HOT
                                : WeightResidencyState::ABSENT,
                  view->metadata.residency);
        EXPECT_DOUBLE_EQ(lose_manifest ? 1.0 : 0.0,
                         view->metadata.observed_hot_ratio);
        ASSERT_TRUE(view->metadata.operation_id.has_value());
        EXPECT_EQ(started->operation_id, *view->metadata.operation_id);
        const auto operation =
            service.QueryWeightOperation(QueryWeightOperationRequest{
                .operation_id = started->operation_id,
            });
        ASSERT_TRUE(operation.has_value());
        EXPECT_EQ(WeightOperationKind::MIGRATING, operation->kind);
        EXPECT_EQ(WeightResidencyState::COLD, operation->target_residency);
        EXPECT_NE("completed", operation->message);
    }
}

TEST_F(WeightReconciliationTest,
       PublicCompletedMigrationMemberLossStillUpdatesAvailability) {
    CheckPublicPayloadLoss(1, true, WeightResidencyState::HOT, 1.0, true);
}
TEST_F(WeightReconciliationTest, PublicPayloadLossRetainsPartialHotCoverage) {
    CheckPublicPayloadLoss(1, false, WeightResidencyState::HOT, 1.0);
}

TEST_F(WeightReconciliationTest, PublicPayloadLossRetainsColdResidency) {
    CheckPublicPayloadLoss(1, true, WeightResidencyState::COLD, 0.0);
}

TEST_F(WeightReconciliationTest, PublicAllPayloadLossBecomesAbsent) {
    CheckPublicPayloadLoss(2, false, WeightResidencyState::ABSENT, 0.0);
}

TEST_F(WeightReconciliationTest, RevisionWindowsAdvancePastExaminedRows) {
    MasterService service;
    const auto context = PrepareSimpleSegment(service);
    for (const auto& revision : {"00-fresh-import", "01-fresh-import"}) {
        ASSERT_TRUE(service.BeginWeightImport(
            ImportRequest(Identity(revision), 1, 1024)));
    }
    std::vector<WeightRevisionIdentity> identities;
    uint64_t now_ms = 0;
    for (int i = 0; i < 6; ++i) {
        const auto revision = "round-robin-" + std::to_string(i);
        const auto ready = PublishReady(service, context.client_id, revision);
        identities.push_back(ready.identity);
        now_ms = std::max(now_ms, ready.updated_at_ms + 1);
        ASSERT_TRUE(
            MasterServiceTestPeer(service).DropWeightGroupMemberForTesting(
                ready.identity, revision + "-payload"));
    }
    for (int round = 0; round < 3; ++round) {
        EXPECT_EQ(
            2, MasterServiceTestPeer(service).RunWeightReconciliationForTesting(
                   now_ms, 2));
    }
    for (const auto& identity : identities) {
        const auto observed = service.GetWeightRevision({.identity = identity});
        ASSERT_TRUE(observed);
        EXPECT_EQ(WeightAvailabilityState::DEGRADED,
                  observed->metadata.availability)
            << identity.revision;
    }
}

TEST_F(WeightReconciliationTest, ExpiredLeaseTrafficLeavesRevisionBudget) {
    CheckContinuousLeaseTraffic(2);
}

TEST_F(WeightReconciliationTest, UnitBudgetAlternatesUnderExpiredLeaseTraffic) {
    CheckContinuousLeaseTraffic(1);
}

TEST_F(WeightReconciliationTest, UnitBudgetWithoutLeasesDoesRevisionWork) {
    MasterService service;
    const auto context = PrepareSimpleSegment(service);
    const auto ready = PublishReady(service, context.client_id, "no-lease");
    ASSERT_TRUE(MasterServiceTestPeer(service).DropWeightGroupMemberForTesting(
        ready.identity, "no-lease-payload"));
    EXPECT_EQ(1,
              MasterServiceTestPeer(service).RunWeightReconciliationForTesting(
                  ready.updated_at_ms + 1, 1));
    const auto observed =
        service.GetWeightRevision({.identity = ready.identity});
    ASSERT_TRUE(observed);
    EXPECT_EQ(WeightAvailabilityState::DEGRADED,
              observed->metadata.availability);
}

TEST_F(WeightReconciliationTest,
       UnboundedReconcileCompletesAbortedImportDelete) {
    MasterService service;
    const auto context = PrepareSimpleSegment(service);
    const auto identity = Identity("unbounded-delete");
    const auto importing =
        service.BeginWeightImport(ImportRequest(identity, 1, 1024));
    ASSERT_TRUE(importing);
    ReplicateConfig config;
    config.replica_num = 1;
    config.with_hard_pin = true;
    config.group_ids =
        std::vector<std::string>{importing->manifest.payload_group_id};
    config.residency_affinity_ids =
        std::vector<std::string>{"affinity-" + importing->identity.revision};
    config.data_type = ObjectDataType::WEIGHT;
    PutCompletedObject(service, context.client_id, "unbounded-delete-payload",
                       config, 1024);
    config.data_type = ObjectDataType::METADATA;
    config.residency_affinity_ids.reset();
    PutCompletedObject(service, context.client_id, ManifestKey(identity),
                       config, 128);
    ASSERT_TRUE(service.AbortWeightImport({
        .identity = identity,
        .expected_metadata_generation = importing->metadata_generation,
    }));
    const auto deleted =
        service.ReconcileWeightRevision({.identity = identity});
    ASSERT_TRUE(deleted);
    EXPECT_EQ(WeightAvailabilityState::DELETED, deleted->availability);
    for (const auto& key :
         {std::string("unbounded-delete-payload"), ManifestKey(identity)}) {
        const auto exists = service.ExistKey(key, TenantId::Default());
        ASSERT_TRUE(exists);
        EXPECT_FALSE(*exists) << key;
    }
    const auto retried =
        service.ReconcileWeightRevision({.identity = identity});
    ASSERT_TRUE(retried);
    EXPECT_EQ(*deleted, *retried);
}

TEST_F(WeightReconciliationTest, ExpiresLeasesAndAbortsAbandonedImports) {
    MasterService service;
    [[maybe_unused]] const auto context = PrepareSimpleSegment(service);
    const UUID client_id = generate_uuid();
    auto ready = PublishReady(service, client_id, "step-ready");
    auto lease =
        service.AcquireWeightRevisionLease(AcquireWeightRevisionLeaseRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation,
            .holder = "worker-0",
            .ttl_ms = 1,
        });
    ASSERT_TRUE(lease.has_value());
    auto importing = service.BeginWeightImport(BeginWeightImportRequest{
        .identity = Identity("step-abandoned"),
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
    const uint64_t now_ms = std::max(
        lease->expires_at_ms + 1, importing->updated_at_ms + 5 * 60 * 1000 + 1);

    MasterServiceTestPeer(service).RunWeightReconciliationForTesting(now_ms,
                                                                     32);
    auto ready_view = service.GetWeightRevision(
        GetWeightRevisionRequest{.identity = ready.identity});
    ASSERT_TRUE(ready_view.has_value());
    EXPECT_EQ(0, ready_view->active_lease_count);
    auto abandoned = service.GetWeightRevision(
        GetWeightRevisionRequest{.identity = importing->identity});
    ASSERT_TRUE(abandoned.has_value());
    EXPECT_EQ(WeightAvailabilityState::DELETING,
              abandoned->metadata.availability);

    MasterServiceTestPeer(service).RunWeightReconciliationForTesting(now_ms + 1,
                                                                     32);
    abandoned = service.GetWeightRevision(
        GetWeightRevisionRequest{.identity = importing->identity});
    ASSERT_TRUE(abandoned.has_value());
    EXPECT_EQ(WeightAvailabilityState::DELETED,
              abandoned->metadata.availability);
}

TEST_F(WeightReconciliationTest, WorkLimitBoundsAbandonedImportTransitions) {
    MasterService service;
    auto first = service.BeginWeightImport(BeginWeightImportRequest{
        .identity = Identity("step-a"),
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
    auto second = service.BeginWeightImport(BeginWeightImportRequest{
        .identity = Identity("step-b"),
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
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());
    const auto now_ms = std::max(first->updated_at_ms, second->updated_at_ms) +
                        5 * 60 * 1000 + 1;
    EXPECT_EQ(1,
              MasterServiceTestPeer(service).RunWeightReconciliationForTesting(
                  now_ms, 1));
    auto first_view = service.GetWeightRevision(
        GetWeightRevisionRequest{.identity = first->identity});
    auto second_view = service.GetWeightRevision(
        GetWeightRevisionRequest{.identity = second->identity});
    ASSERT_TRUE(first_view.has_value());
    ASSERT_TRUE(second_view.has_value());
    const int deleting = (first_view->metadata.availability ==
                          WeightAvailabilityState::DELETING) +
                         (second_view->metadata.availability ==
                          WeightAvailabilityState::DELETING);
    const int importing = (first_view->metadata.availability ==
                           WeightAvailabilityState::IMPORTING) +
                          (second_view->metadata.availability ==
                           WeightAvailabilityState::IMPORTING);
    EXPECT_EQ(1, deleting);
    EXPECT_EQ(1, importing);
}

TEST_F(WeightReconciliationTest,
       PublicManifestReplicaClearReconcilesToDegraded) {
    auto config =
        MasterServiceConfig::builder().set_default_kv_lease_ttl(100).build();
    MasterService service(config);
    const auto context = PrepareSimpleSegment(service);
    const auto ready =
        PublishReady(service, context.client_id, "step-public-manifest-clear");
    const auto manifest_key = ManifestKey(ready.identity);
    const auto payload_key = ready.identity.revision + "-payload";
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    bool cleared = false;
    do {
        const auto result =
            service.BatchReplicaClear({manifest_key}, context.client_id, "");
        ASSERT_TRUE(result.has_value());
        cleared = result->size() == 1 && result->front() == manifest_key;
        if (!cleared) {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
    } while (!cleared && std::chrono::steady_clock::now() < deadline);
    ASSERT_TRUE(cleared);
    EXPECT_FALSE(
        service.ExistKey(manifest_key, TenantId::Default()).value_or(true));
    EXPECT_TRUE(
        service.ExistKey(payload_key, TenantId::Default()).value_or(false));
    const auto reconciled = service.ReconcileWeightRevision(
        ReconcileWeightRevisionRequest{.identity = ready.identity});
    EXPECT_TRUE(reconciled.has_value());
    const auto view = service.GetWeightRevision(
        GetWeightRevisionRequest{.identity = ready.identity});
    ASSERT_TRUE(view.has_value());
    EXPECT_EQ(WeightAvailabilityState::DEGRADED, view->metadata.availability);
    EXPECT_EQ(WeightResidencyState::HOT, view->metadata.residency);
    EXPECT_DOUBLE_EQ(1.0, view->metadata.observed_hot_ratio);
}

TEST_F(WeightReconciliationTest, MissingPayloadOrManifestBecomesDegraded) {
    MasterService service;
    [[maybe_unused]] const auto context = PrepareSimpleSegment(service);
    const UUID client_id = generate_uuid();
    auto payload_loss = PublishReady(service, client_id, "step-payload-loss");
    auto manifest_loss = PublishReady(service, client_id, "step-manifest-loss");
    ASSERT_TRUE(MasterServiceTestPeer(service).DropWeightGroupMemberForTesting(
        payload_loss.identity, "step-payload-loss-payload"));
    ASSERT_TRUE(MasterServiceTestPeer(service).DropWeightGroupMemberForTesting(
        manifest_loss.identity, ManifestKey(manifest_loss.identity)));

    MasterServiceTestPeer(service).RunWeightReconciliationForTesting(
        std::max(payload_loss.updated_at_ms, manifest_loss.updated_at_ms) + 1,
        32);
    auto payload_loss_view = service.GetWeightRevision(
        GetWeightRevisionRequest{.identity = payload_loss.identity});
    ASSERT_TRUE(payload_loss_view.has_value());
    EXPECT_EQ(WeightAvailabilityState::DEGRADED,
              payload_loss_view->metadata.availability);
    EXPECT_EQ(WeightResidencyState::ABSENT,
              payload_loss_view->metadata.residency);

    auto manifest_loss_view = service.GetWeightRevision(
        GetWeightRevisionRequest{.identity = manifest_loss.identity});
    ASSERT_TRUE(manifest_loss_view.has_value());
    EXPECT_EQ(WeightAvailabilityState::DEGRADED,
              manifest_loss_view->metadata.availability);
    EXPECT_EQ(WeightResidencyState::HOT,
              manifest_loss_view->metadata.residency);
    EXPECT_EQ(2, MasterMetricManager::instance().get_weight_revision_count(
                     "degraded"));
    EXPECT_EQ(1, MasterMetricManager::instance().get_weight_residency_count(
                     "absent"));
    EXPECT_EQ(
        1, MasterMetricManager::instance().get_weight_residency_count("hot"));
}

TEST_F(WeightReconciliationTest, MemberBudgetBoundsDeletionAndResumes) {
    MasterService service;
    const auto context = PrepareSimpleSegment(service);
    const auto identity = Identity("bounded-delete");
    auto importing =
        service.BeginWeightImport(ImportRequest(identity, 2, 2048));
    ASSERT_TRUE(importing);
    ReplicateConfig config;
    config.replica_num = 1;
    config.with_hard_pin = true;
    config.group_ids =
        std::vector<std::string>{importing->manifest.payload_group_id};
    config.residency_affinity_ids =
        std::vector<std::string>{"affinity-" + importing->identity.revision};
    config.data_type = ObjectDataType::WEIGHT;
    PutCompletedObject(service, context.client_id, "bounded-a", config, 1024);
    PutCompletedObject(service, context.client_id, "bounded-b", config, 1024);
    config.data_type = ObjectDataType::METADATA;
    config.residency_affinity_ids.reset();
    PutCompletedObject(service, context.client_id, ManifestKey(identity),
                       config, 128);
    ASSERT_TRUE(service.AbortWeightImport({
        .identity = identity,
        .expected_metadata_generation = importing->metadata_generation,
    }));
    auto exists = [&](const std::string& key) {
        auto result = service.ExistKey(key, TenantId::Default());
        return result && *result;
    };
    MasterServiceTestPeer(service).RunWeightReconciliationForTesting(
        importing->updated_at_ms + 1, 1);
    EXPECT_EQ(1, int(exists("bounded-a")) + int(exists("bounded-b")));
    EXPECT_TRUE(exists(ManifestKey(identity)));
    for (int i = 0; i < 4; ++i) {
        MasterServiceTestPeer(service).RunWeightReconciliationForTesting(
            importing->updated_at_ms + 2 + i, 1);
    }
    auto deleted = service.GetWeightRevision({.identity = identity});
    ASSERT_TRUE(deleted);
    EXPECT_EQ(WeightAvailabilityState::DELETED, deleted->metadata.availability);
    const auto generation = deleted->metadata.metadata_generation;
    MasterServiceTestPeer(service).RunWeightReconciliationForTesting(
        importing->updated_at_ms + 10, 1);
    deleted = service.GetWeightRevision({.identity = identity});
    ASSERT_TRUE(deleted);
    EXPECT_EQ(generation, deleted->metadata.metadata_generation);
}

TEST_F(WeightReconciliationTest, MemberBudgetIsSharedAcrossRevisions) {
    MasterService service;
    const auto context = PrepareSimpleSegment(service);
    std::vector<std::string> keys;
    for (const auto& revision : {"shared-a", "shared-b"}) {
        const auto identity = Identity(revision);
        const auto importing =
            service.BeginWeightImport(ImportRequest(identity, 2, 2048));
        ASSERT_TRUE(importing);
        ReplicateConfig config;
        config.replica_num = 1;
        config.with_hard_pin = true;
        config.group_ids =
            std::vector<std::string>{importing->manifest.payload_group_id};
        config.residency_affinity_ids = std::vector<std::string>{
            "affinity-" + importing->identity.revision};
        config.data_type = ObjectDataType::WEIGHT;
        for (int i = 0; i < 2; ++i) {
            keys.push_back(std::string(revision) + std::to_string(i));
            PutCompletedObject(service, context.client_id, keys.back(), config,
                               1024);
        }
        config.data_type = ObjectDataType::METADATA;
        config.residency_affinity_ids.reset();
        keys.push_back(ManifestKey(identity));
        PutCompletedObject(service, context.client_id, keys.back(), config,
                           128);
        ASSERT_TRUE(service.AbortWeightImport({
            .identity = identity,
            .expected_metadata_generation = importing->metadata_generation,
        }));
    }
    MasterServiceTestPeer(service).RunWeightReconciliationForTesting(1, 3);
    size_t remaining = 0;
    for (const auto& key : keys) {
        const auto exists = service.ExistKey(key, TenantId::Default());
        remaining += exists && *exists;
    }
    EXPECT_EQ(3, remaining);
    MasterServiceTestPeer(service).RunWeightReconciliationForTesting(2, 3);
    for (const auto& key : keys) {
        const auto exists = service.ExistKey(key, TenantId::Default());
        EXPECT_FALSE(exists && *exists);
    }
}

TEST_F(WeightReconciliationTest, ProjectsLeaseAndOperationMetrics) {
    MasterService service;
    [[maybe_unused]] const auto context = PrepareSimpleSegment(service);
    const UUID client_id = generate_uuid();
    auto ready = PublishReady(service, client_id, "step-metrics");
    auto leased = PublishReady(service, client_id, "step-leased-metrics");
    auto lease =
        service.AcquireWeightRevisionLease(AcquireWeightRevisionLeaseRequest{
            .identity = leased.identity,
            .expected_metadata_generation = leased.metadata_generation,
            .holder = "worker-metrics",
            .ttl_ms = 1000,
        });
    ASSERT_TRUE(lease.has_value());
    auto operation = service.StartWeightResidencyOperation(
        StartWeightResidencyOperationRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation,
            .target_residency = WeightResidencyState::HOT,
            .mixed_hot_ratio = std::nullopt,
        });
    ASSERT_TRUE(operation.has_value());
    ASSERT_TRUE(MasterServiceTestPeer(service).DropWeightGroupMemberForTesting(
        ready.identity, ready.identity.revision + "-payload"));
    const auto now_ms = operation->started_at_ms + 100;
    MasterServiceTestPeer(service).RunWeightReconciliationForTesting(now_ms,
                                                                     32);
    EXPECT_EQ(1, MasterMetricManager::instance().get_weight_active_leases());
    EXPECT_EQ(1,
              MasterMetricManager::instance().get_weight_pending_operations());
    EXPECT_GE(
        MasterMetricManager::instance().get_weight_oldest_operation_age_ms(),
        100);
}

}  // namespace
}  // namespace mooncake::test

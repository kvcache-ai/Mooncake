#include "master_service_test_fixture.h"

#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <optional>
#include <string>
#include <vector>

#include "weight_management.h"

namespace mooncake::test {
namespace {

class MasterServiceWeightManagementTest : public MasterServiceTest {
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

    static BeginWeightImportRequest BeginRequest(
        std::optional<WeightStoragePolicy> policy) {
        return BeginWeightImportRequest{
            .identity = Identity(),
            .payload_group_id = {},
            .expected_payload_count = 2,
            .expected_logical_bytes = 2048,
            .policy = policy,
            .affinity_summary =
                WeightAffinitySummary{
                    .affinity_count = 2,
                    .affinity_digest = std::string(64, 'c'),
                },
        };
    }

    WeightRevisionMetadata Begin(MasterService& service, uint64_t payload_count,
                                 uint64_t logical_bytes) {
        auto result = service.BeginWeightImport(BeginWeightImportRequest{
            .identity = Identity(),
            .payload_group_id = {},
            .expected_payload_count = payload_count,
            .expected_logical_bytes = logical_bytes,
            .policy =
                WeightStoragePolicy{
                    .preferred_residency = WeightResidencyState::HOT,
                    .migration_mode = WeightMigrationMode::MANUAL,
                },
            .affinity_summary =
                WeightAffinitySummary{
                    .affinity_count = payload_count,
                    .affinity_digest = std::string(64, 'c'),
                },
        });
        EXPECT_TRUE(result.has_value());
        return *result;
    }

    void PutObject(MasterService& service, const UUID& client_id,
                   const std::string& key, const std::string& group_id,
                   ObjectDataType data_type, uint64_t size) {
        ReplicateConfig config;
        config.replica_num = 1;
        config.with_hard_pin = true;
        config.data_type = data_type;
        config.group_ids = std::vector<std::string>{group_id};
        if (data_type == ObjectDataType::WEIGHT) {
            config.residency_affinity_ids = std::vector<std::string>{key};
        }
        PutCompletedObject(service, client_id, key, config, size);
    }

    static CommitWeightImportRequest CommitRequest(
        const WeightRevisionMetadata& importing,
        const std::vector<std::string>& payload_keys, uint64_t logical_bytes) {
        return CommitWeightImportRequest{
            .identity = importing.identity,
            .expected_metadata_generation = importing.metadata_generation,
            .manifest =
                WeightManifestReference{
                    .manifest_key = ManifestKey(),
                    .manifest_sha256 = std::string(64, 'a'),
                    .payload_group_id = importing.manifest.payload_group_id,
                    .payload_keys_sha256 =
                        ComputeWeightPayloadKeysSha256(payload_keys),
                    .payload_count = payload_keys.size(),
                    .logical_bytes = logical_bytes,
                },
        };
    }
};

TEST_F(MasterServiceWeightManagementTest,
       UsesConfiguredDefaultPolicyWhenRequestOmitsPolicy) {
    auto config = MasterServiceConfig::builder()
                      .set_default_weight_storage_policy(WeightStoragePolicy{
                          .preferred_residency = WeightResidencyState::COLD,
                          .mixed_hot_ratio = 0.25,
                          .migration_mode = WeightMigrationMode::MANUAL,
                      })
                      .build();
    MasterService service(config);

    auto importing = service.BeginWeightImport(BeginRequest(std::nullopt));

    ASSERT_TRUE(importing.has_value());
    EXPECT_EQ(WeightResidencyState::COLD,
              importing->policy.preferred_residency);
    EXPECT_DOUBLE_EQ(0.25, importing->policy.mixed_hot_ratio);
    EXPECT_EQ(WeightMigrationMode::MANUAL, importing->policy.migration_mode);
}

TEST_F(MasterServiceWeightManagementTest,
       RequestPolicyOverridesConfiguredDefault) {
    auto config = MasterServiceConfig::builder()
                      .set_default_weight_storage_policy(WeightStoragePolicy{
                          .preferred_residency = WeightResidencyState::COLD,
                          .mixed_hot_ratio = 0.25,
                          .migration_mode = WeightMigrationMode::MANUAL,
                      })
                      .build();
    MasterService service(config);
    const WeightStoragePolicy request_policy{
        .preferred_residency = WeightResidencyState::HOT,
        .mixed_hot_ratio = 0.75,
        .migration_mode = WeightMigrationMode::PINNED,
    };

    auto importing = service.BeginWeightImport(BeginRequest(request_policy));

    ASSERT_TRUE(importing.has_value());
    EXPECT_EQ(request_policy, importing->policy);
}

TEST_F(MasterServiceWeightManagementTest,
       RejectsInvalidConfiguredDefaultPolicy) {
    MasterServiceConfig config;
    config.default_weight_storage_policy.mixed_hot_ratio = 1.0;

    EXPECT_THROW(MasterService service(config), std::invalid_argument);
}

TEST_F(MasterServiceWeightManagementTest,
       RejectsUnsatisfiableSingleAffinityMixedImport) {
    MasterService service;
    auto request = BeginRequest(std::nullopt);
    request.expected_payload_count = 1;
    request.expected_logical_bytes = 1024;
    request.affinity_summary.affinity_count = 1;

    auto importing = service.BeginWeightImport(request);

    ASSERT_FALSE(importing.has_value());
    EXPECT_EQ(WeightManagementError::POLICY_UNSATISFIABLE, importing.error());
}

TEST_F(MasterServiceWeightManagementTest,
       PublishesReadyAndRetriesAfterResponseLoss) {
    MasterService service;
    [[maybe_unused]] const auto context = PrepareSimpleSegment(service);
    const UUID client_id = generate_uuid();
    auto importing = Begin(service, 2, 2048);
    const std::vector<std::string> payload_keys{"payload-b", "payload-a"};
    for (const auto& key : payload_keys) {
        PutObject(service, client_id, key, importing.manifest.payload_group_id,
                  ObjectDataType::WEIGHT, 1024);
    }
    PutObject(service, client_id, ManifestKey(),
              importing.manifest.payload_group_id, ObjectDataType::METADATA,
              128);

    const auto request = CommitRequest(importing, payload_keys, 2048);
    auto committed = service.CommitWeightImport(request);
    ASSERT_TRUE(committed.has_value());
    EXPECT_EQ(WeightAvailabilityState::READY, committed->availability);
    EXPECT_EQ(WeightResidencyState::HOT, committed->residency);
    EXPECT_EQ(2, committed->metadata_generation);

    auto retry = service.CommitWeightImport(request);
    ASSERT_TRUE(retry.has_value());
    EXPECT_EQ(*committed, *retry);

    auto discovered = service.GetWeightRevision(
        GetWeightRevisionRequest{.identity = Identity()});
    ASSERT_TRUE(discovered.has_value());
    EXPECT_EQ(*committed, discovered->metadata);
}

TEST_F(MasterServiceWeightManagementTest, RejectsMissingManifest) {
    MasterService service;
    [[maybe_unused]] const auto context = PrepareSimpleSegment(service);
    const UUID client_id = generate_uuid();
    auto importing = Begin(service, 1, 1024);
    PutObject(service, client_id, "payload-a",
              importing.manifest.payload_group_id, ObjectDataType::WEIGHT,
              1024);

    auto result = service.CommitWeightImport(
        CommitRequest(importing, {"payload-a"}, 1024));
    EXPECT_FALSE(result.has_value());
}

TEST_F(MasterServiceWeightManagementTest, CommitsPercentEncodedIdentity) {
    MasterService service;
    [[maybe_unused]] const auto context = PrepareSimpleSegment(service);
    const UUID client_id = generate_uuid();
    auto identity = Identity();
    identity.name_space = "production + staging";
    identity.resource_id = "llama/70b";
    identity.revision = "step:100";
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
    ASSERT_TRUE(importing.has_value());
    const auto manifest_key = MakeWeightManifestKey(identity);
    PutObject(service, client_id, "encoded-payload",
              importing->manifest.payload_group_id, ObjectDataType::WEIGHT,
              1024);
    PutObject(service, client_id, manifest_key,
              importing->manifest.payload_group_id, ObjectDataType::METADATA,
              128);
    auto request = CommitRequest(*importing, {"encoded-payload"}, 1024);
    request.manifest.manifest_key = manifest_key;
    auto committed = service.CommitWeightImport(request);
    ASSERT_TRUE(committed.has_value());
    EXPECT_EQ(WeightAvailabilityState::READY, committed->availability);
    auto discovered = service.GetWeightRevision(
        GetWeightRevisionRequest{.identity = identity});
    ASSERT_TRUE(discovered.has_value());
    EXPECT_EQ(manifest_key, discovered->metadata.manifest.manifest_key);
}

TEST_F(MasterServiceWeightManagementTest,
       RejectsNonDefaultTenantWhenMultiTenancyIsDisabled) {
    MasterService service;
    auto identity = Identity();
    identity.tenant_id = "tenant-a";
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
    ASSERT_FALSE(importing.has_value());
    EXPECT_EQ(WeightManagementError::INVALID_ARGUMENT, importing.error());
    EXPECT_FALSE(
        service
            .GetWeightRevision(GetWeightRevisionRequest{.identity = identity})
            .has_value());
}

TEST_F(MasterServiceWeightManagementTest,
       PreservesTenantIdentityWhenMultiTenancyIsEnabled) {
    MasterService service(MakeStrictTenantConfig({"default", "tenant-a"}));
    auto identity = Identity();
    identity.tenant_id = "tenant-a";
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
    ASSERT_TRUE(importing.has_value());
    EXPECT_EQ(identity, importing->identity);
    auto discovered = service.GetWeightRevision(
        GetWeightRevisionRequest{.identity = identity});
    ASSERT_TRUE(discovered.has_value());
    EXPECT_EQ(identity, discovered->metadata.identity);
    EXPECT_FALSE(
        service
            .GetWeightRevision(GetWeightRevisionRequest{.identity = Identity()})
            .has_value());
}

TEST_F(MasterServiceWeightManagementTest, RejectsWrongManifestType) {
    MasterService service;
    [[maybe_unused]] const auto context = PrepareSimpleSegment(service);
    const UUID client_id = generate_uuid();
    auto importing = Begin(service, 1, 1024);
    PutObject(service, client_id, "payload-a",
              importing.manifest.payload_group_id, ObjectDataType::WEIGHT,
              1024);
    PutObject(service, client_id, ManifestKey(),
              importing.manifest.payload_group_id, ObjectDataType::WEIGHT, 128);

    auto result = service.CommitWeightImport(
        CommitRequest(importing, {"payload-a"}, 1024));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(WeightManagementError::CONFLICT, result.error());
}

TEST_F(MasterServiceWeightManagementTest, RejectsManifestInWrongGroup) {
    MasterService service;
    [[maybe_unused]] const auto context = PrepareSimpleSegment(service);
    const UUID client_id = generate_uuid();
    auto importing = Begin(service, 1, 1024);
    PutObject(service, client_id, "payload-a",
              importing.manifest.payload_group_id, ObjectDataType::WEIGHT,
              1024);
    PutObject(service, client_id, ManifestKey(), "other-group",
              ObjectDataType::METADATA, 128);

    auto result = service.CommitWeightImport(
        CommitRequest(importing, {"payload-a"}, 1024));
    EXPECT_FALSE(result.has_value());
}

TEST_F(MasterServiceWeightManagementTest, RejectsIncompletePayloadGroup) {
    MasterService service;
    [[maybe_unused]] const auto context = PrepareSimpleSegment(service);
    const UUID client_id = generate_uuid();
    auto importing = Begin(service, 2, 2048);
    PutObject(service, client_id, "payload-a",
              importing.manifest.payload_group_id, ObjectDataType::WEIGHT,
              1024);
    PutObject(service, client_id, ManifestKey(),
              importing.manifest.payload_group_id, ObjectDataType::METADATA,
              128);

    auto result = service.CommitWeightImport(
        CommitRequest(importing, {"payload-a", "payload-b"}, 2048));
    EXPECT_FALSE(result.has_value());
}

TEST_F(MasterServiceWeightManagementTest, RejectsExtraOrphanPayload) {
    MasterService service;
    [[maybe_unused]] const auto context = PrepareSimpleSegment(service);
    const UUID client_id = generate_uuid();
    auto importing = Begin(service, 2, 2048);
    for (const auto& key : {"payload-a", "payload-b", "payload-orphan"}) {
        PutObject(service, client_id, key, importing.manifest.payload_group_id,
                  ObjectDataType::WEIGHT, 1024);
    }
    PutObject(service, client_id, ManifestKey(),
              importing.manifest.payload_group_id, ObjectDataType::METADATA,
              128);

    auto result = service.CommitWeightImport(
        CommitRequest(importing, {"payload-a", "payload-b"}, 2048));
    EXPECT_FALSE(result.has_value());
}

TEST_F(MasterServiceWeightManagementTest, RejectsMismatchedSummaryAndDigest) {
    MasterService service;
    [[maybe_unused]] const auto context = PrepareSimpleSegment(service);
    const UUID client_id = generate_uuid();
    auto importing = Begin(service, 1, 1024);
    PutObject(service, client_id, "payload-a",
              importing.manifest.payload_group_id, ObjectDataType::WEIGHT,
              1024);
    PutObject(service, client_id, ManifestKey(),
              importing.manifest.payload_group_id, ObjectDataType::METADATA,
              128);

    auto count_mismatch = CommitRequest(importing, {"payload-a"}, 1024);
    count_mismatch.manifest.payload_count = 2;
    EXPECT_FALSE(service.CommitWeightImport(count_mismatch).has_value());

    auto bytes_mismatch = CommitRequest(importing, {"payload-a"}, 2048);
    EXPECT_FALSE(service.CommitWeightImport(bytes_mismatch).has_value());

    auto digest_mismatch = CommitRequest(importing, {"payload-a"}, 1024);
    digest_mismatch.manifest.payload_keys_sha256 = std::string(64, 'f');
    EXPECT_FALSE(service.CommitWeightImport(digest_mismatch).has_value());
}

TEST_F(MasterServiceWeightManagementTest, RejectsStaleGeneration) {
    MasterService service;
    [[maybe_unused]] const auto context = PrepareSimpleSegment(service);
    const UUID client_id = generate_uuid();
    auto importing = Begin(service, 1, 1024);
    PutObject(service, client_id, "payload-a",
              importing.manifest.payload_group_id, ObjectDataType::WEIGHT,
              1024);
    PutObject(service, client_id, ManifestKey(),
              importing.manifest.payload_group_id, ObjectDataType::METADATA,
              128);

    auto request = CommitRequest(importing, {"payload-a"}, 1024);
    ++request.expected_metadata_generation;
    auto result = service.CommitWeightImport(request);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(WeightManagementError::STALE_GENERATION, result.error());
}

TEST_F(MasterServiceWeightManagementTest,
       RevisionLeaseLifecycleUsesPublishedMetadataState) {
    MasterService service;
    [[maybe_unused]] const auto context = PrepareSimpleSegment(service);
    const UUID client_id = generate_uuid();
    auto importing = Begin(service, 1, 1024);
    PutObject(service, client_id, "payload-a",
              importing.manifest.payload_group_id, ObjectDataType::WEIGHT,
              1024);
    PutObject(service, client_id, ManifestKey(),
              importing.manifest.payload_group_id, ObjectDataType::METADATA,
              128);
    auto ready = service.CommitWeightImport(
        CommitRequest(importing, {"payload-a"}, 1024));
    ASSERT_TRUE(ready.has_value());

    const AcquireWeightRevisionLeaseRequest acquire_request{
        .identity = ready->identity,
        .expected_metadata_generation = ready->metadata_generation,
        .holder = "worker-0",
        .ttl_ms = 60'000,
    };
    auto acquired = service.AcquireWeightRevisionLease(acquire_request);
    ASSERT_TRUE(acquired.has_value());
    auto retry = service.AcquireWeightRevisionLease(acquire_request);
    ASSERT_TRUE(retry.has_value());
    EXPECT_EQ(*acquired, *retry);

    auto renewed =
        service.RenewWeightRevisionLease(RenewWeightRevisionLeaseRequest{
            .lease_id = acquired->lease_id,
            .ttl_ms = 120'000,
        });
    ASSERT_TRUE(renewed.has_value());
    EXPECT_GT(renewed->expires_at_ms, acquired->expires_at_ms);
    auto view = service.GetWeightRevision(
        GetWeightRevisionRequest{.identity = ready->identity});
    ASSERT_TRUE(view.has_value());
    EXPECT_EQ(1u, view->active_lease_count);

    ASSERT_TRUE(service.ReleaseWeightRevisionLease(
        ReleaseWeightRevisionLeaseRequest{.lease_id = acquired->lease_id}));
    ASSERT_TRUE(service.ReleaseWeightRevisionLease(
        ReleaseWeightRevisionLeaseRequest{.lease_id = acquired->lease_id}));
    view = service.GetWeightRevision(
        GetWeightRevisionRequest{.identity = ready->identity});
    ASSERT_TRUE(view.has_value());
    EXPECT_EQ(0u, view->active_lease_count);
}

TEST_F(MasterServiceWeightManagementTest,
       LeaseMutationsSerializeWithWeightGroupLifecycle) {
    MasterService service;
    [[maybe_unused]] const auto context = PrepareSimpleSegment(service);
    const UUID client_id = generate_uuid();
    auto importing = Begin(service, 1, 1024);
    PutObject(service, client_id, "payload-a",
              importing.manifest.payload_group_id, ObjectDataType::WEIGHT,
              1024);
    PutObject(service, client_id, ManifestKey(),
              importing.manifest.payload_group_id, ObjectDataType::METADATA,
              128);
    auto ready = service.CommitWeightImport(
        CommitRequest(importing, {"payload-a"}, 1024));
    ASSERT_TRUE(ready.has_value());
    auto acquired =
        service.AcquireWeightRevisionLease(AcquireWeightRevisionLeaseRequest{
            .identity = ready->identity,
            .expected_metadata_generation = ready->metadata_generation,
            .holder = "worker-0",
            .ttl_ms = 60'000,
        });
    ASSERT_TRUE(acquired.has_value());

    auto group_lock = AcquireWeightGroupOperationLockForTest(
        service, TenantId(ready->identity.tenant_id),
        ready->manifest.payload_group_id);
    auto renewing = std::async(std::launch::async, [&] {
        return service.RenewWeightRevisionLease(RenewWeightRevisionLeaseRequest{
            .lease_id = acquired->lease_id,
            .ttl_ms = 120'000,
        });
    });
    EXPECT_EQ(std::future_status::timeout,
              renewing.wait_for(std::chrono::milliseconds(250)));
    group_lock.unlock();
    ASSERT_TRUE(renewing.get().has_value());

    group_lock = AcquireWeightGroupOperationLockForTest(
        service, TenantId(ready->identity.tenant_id),
        ready->manifest.payload_group_id);
    auto releasing = std::async(std::launch::async, [&] {
        return service.ReleaseWeightRevisionLease(
            ReleaseWeightRevisionLeaseRequest{
                .lease_id = acquired->lease_id,
            });
    });
    EXPECT_EQ(std::future_status::timeout,
              releasing.wait_for(std::chrono::milliseconds(250)));
    group_lock.unlock();
    EXPECT_TRUE(releasing.get().has_value());
}

TEST_F(MasterServiceWeightManagementTest,
       ResidencyOperationSerializesWithWeightGroupLifecycle) {
    MasterService service;
    [[maybe_unused]] const auto context = PrepareSimpleSegment(service);
    const UUID client_id = generate_uuid();
    auto importing = Begin(service, 1, 1024);
    PutObject(service, client_id, "payload-a",
              importing.manifest.payload_group_id, ObjectDataType::WEIGHT,
              1024);
    PutObject(service, client_id, ManifestKey(),
              importing.manifest.payload_group_id, ObjectDataType::METADATA,
              128);
    auto ready = service.CommitWeightImport(
        CommitRequest(importing, {"payload-a"}, 1024));
    ASSERT_TRUE(ready.has_value());

    auto group_lock = AcquireWeightGroupOperationLockForTest(
        service, TenantId(ready->identity.tenant_id),
        ready->manifest.payload_group_id);
    std::promise<void> started_call;
    auto entered = started_call.get_future();
    auto operation = std::async(std::launch::async, [&] {
        started_call.set_value();
        return service.StartWeightResidencyOperation(
            StartWeightResidencyOperationRequest{
                .identity = ready->identity,
                .expected_metadata_generation = ready->metadata_generation,
                .target_residency = WeightResidencyState::HOT,
            });
    });
    entered.wait();

    EXPECT_EQ(std::future_status::timeout,
              operation.wait_for(std::chrono::milliseconds(250)));
    group_lock.unlock();

    auto result = operation.get();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(WeightOperationKind::MIGRATING, result->kind);
}

}  // namespace
}  // namespace mooncake::test

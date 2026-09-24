#include "master_service_test_fixture.h"

#include <gtest/gtest.h>

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

    WeightRevisionMetadata Begin(MasterService& service, uint64_t payload_count,
                                 uint64_t logical_bytes) {
        auto result = service.BeginWeightImport(BeginWeightImportRequest{
            .identity = Identity(),
            .payload_group_id = {},
            .expected_payload_count = payload_count,
            .expected_logical_bytes = logical_bytes,
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

}  // namespace
}  // namespace mooncake::test

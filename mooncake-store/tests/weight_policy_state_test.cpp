#include <gtest/gtest.h>

#include "weight_metadata_store.h"

namespace mooncake {
namespace {

WeightRevisionIdentity PolicyIdentity() {
    return WeightRevisionIdentity{
        .tenant_id = "tenant-policy",
        .name_space = "production",
        .resource_id = "llama-70b",
        .revision = "step-200",
        .weight_generation = 9,
    };
}

WeightManifestReference PolicyManifest() {
    const auto identity = PolicyIdentity();
    return WeightManifestReference{
        .manifest_key = MakeWeightManifestKey(identity),
        .manifest_sha256 = std::string(64, 'a'),
        .payload_group_id = MakeWeightPayloadGroupId(identity),
        .payload_keys_sha256 = std::string(64, 'b'),
        .payload_count = 3,
        .logical_bytes = 4096,
    };
}

BeginWeightImportRequest PolicyBeginRequest(
    std::optional<WeightStoragePolicy> policy = std::nullopt) {
    const auto identity = PolicyIdentity();
    return BeginWeightImportRequest{
        .identity = identity,
        .payload_group_id = MakeWeightPayloadGroupId(identity),
        .expected_payload_count = 3,
        .expected_logical_bytes = 4096,
        .policy = policy,
        .affinity_summary =
            WeightAffinitySummary{
                .affinity_count = 2,
                .affinity_digest = std::string(64, 'c'),
            },
    };
}

WeightRevisionMetadata PublishPolicyBegin(
    WeightMetadataStore& metadata_store,
    std::optional<WeightStoragePolicy> policy = std::nullopt) {
    auto mutation =
        metadata_store.PrepareBeginImport(PolicyBeginRequest(policy), 100);
    EXPECT_TRUE(mutation.has_value());
    auto published = metadata_store.Publish(*mutation);
    EXPECT_TRUE(published.has_value());
    return *published;
}

WeightRevisionMetadata PublishPolicyCommit(
    WeightMetadataStore& metadata_store,
    std::optional<WeightStoragePolicy> policy = std::nullopt) {
    const auto importing = PublishPolicyBegin(metadata_store, policy);
    auto mutation = metadata_store.PrepareCommitImport(
        CommitWeightImportRequest{
            .identity = importing.identity,
            .expected_metadata_generation = importing.metadata_generation,
            .manifest = PolicyManifest(),
        },
        200);
    EXPECT_TRUE(mutation.has_value());
    auto published = metadata_store.Publish(*mutation);
    EXPECT_TRUE(published.has_value());
    return *published;
}

TEST(WeightPolicyStateTest, DefaultsPolicyAndPersistsAffinitySummary) {
    WeightMetadataStore metadata_store;
    const auto importing = PublishPolicyBegin(metadata_store);

    EXPECT_EQ(WeightResidencyState::MIXED,
              importing.policy.preferred_residency);
    EXPECT_DOUBLE_EQ(0.5, importing.policy.mixed_hot_ratio);
    EXPECT_EQ(WeightMigrationMode::MANUAL, importing.policy.migration_mode);
    EXPECT_EQ(WeightResidencyState::UNKNOWN, importing.residency);
    EXPECT_FALSE(importing.operation_id.has_value());
    EXPECT_EQ(2, importing.affinity_count);
    EXPECT_EQ(std::string(64, 'c'), importing.affinity_digest);
    EXPECT_DOUBLE_EQ(0.0, importing.observed_hot_ratio);
}

TEST(WeightPolicyStateTest, CommitPublishesReadyHotAndMigrationAtomically) {
    WeightMetadataStore metadata_store;
    const auto importing = PublishPolicyBegin(metadata_store);
    auto mutation = metadata_store.PrepareCommitImport(
        CommitWeightImportRequest{
            .identity = importing.identity,
            .expected_metadata_generation = importing.metadata_generation,
            .manifest = PolicyManifest(),
        },
        200);
    ASSERT_TRUE(mutation.has_value());
    ASSERT_TRUE(mutation->operation.has_value());

    auto ready = metadata_store.Publish(*mutation);
    ASSERT_TRUE(ready.has_value());
    EXPECT_EQ(WeightAvailabilityState::READY, ready->availability);
    EXPECT_EQ(WeightResidencyState::HOT, ready->residency);
    EXPECT_DOUBLE_EQ(1.0, ready->observed_hot_ratio);
    ASSERT_TRUE(ready->operation_id.has_value());

    auto operation = metadata_store.QueryOperation(*ready->operation_id);
    ASSERT_TRUE(operation.has_value());
    EXPECT_EQ(WeightOperationKind::MIGRATING, operation->kind);
    EXPECT_EQ(WeightResidencyState::MIXED, operation->target_residency);
    ASSERT_TRUE(operation->target_hot_ratio.has_value());
    EXPECT_DOUBLE_EQ(0.5, *operation->target_hot_ratio);
    EXPECT_EQ(2, operation->total_units);
    EXPECT_EQ(4096, operation->total_bytes);
}

TEST(WeightPolicyStateTest, HotCommitDoesNotCreateMigration) {
    WeightMetadataStore metadata_store;
    const auto ready = PublishPolicyCommit(
        metadata_store, WeightStoragePolicy{
                            .preferred_residency = WeightResidencyState::HOT,
                            .mixed_hot_ratio = 0.5,
                            .migration_mode = WeightMigrationMode::PINNED,
                        });

    EXPECT_EQ(WeightResidencyState::HOT, ready.residency);
    EXPECT_FALSE(ready.operation_id.has_value());
}

TEST(WeightPolicyStateTest, PinnedPolicyRejectsExplicitMigration) {
    WeightMetadataStore metadata_store;
    const auto ready = PublishPolicyCommit(
        metadata_store, WeightStoragePolicy{
                            .preferred_residency = WeightResidencyState::HOT,
                            .mixed_hot_ratio = 0.5,
                            .migration_mode = WeightMigrationMode::PINNED,
                        });

    auto migration = metadata_store.PrepareStartOperation(
        StartWeightResidencyOperationRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation,
            .target_residency = WeightResidencyState::COLD,
            .mixed_hot_ratio = std::nullopt,
        },
        300);

    ASSERT_FALSE(migration.has_value());
    EXPECT_EQ(WeightManagementError::POLICY_UNSATISFIABLE, migration.error());
    const auto unchanged = metadata_store.Get(ready.identity, 301);
    ASSERT_TRUE(unchanged.has_value());
    EXPECT_EQ(ready.metadata_generation,
              unchanged->metadata.metadata_generation);
    EXPECT_FALSE(unchanged->metadata.operation_id.has_value());
}

TEST(WeightPolicyStateTest, UpdatePolicyIsCasFencedIdempotentAndExclusive) {
    WeightMetadataStore metadata_store;
    auto ready = PublishPolicyCommit(
        metadata_store, WeightStoragePolicy{
                            .preferred_residency = WeightResidencyState::HOT,
                            .mixed_hot_ratio = 0.5,
                            .migration_mode = WeightMigrationMode::MANUAL,
                        });
    const UpdateWeightPolicyRequest request{
        .identity = ready.identity,
        .expected_metadata_generation = ready.metadata_generation,
        .policy =
            WeightStoragePolicy{
                .preferred_residency = WeightResidencyState::COLD,
                .mixed_hot_ratio = 0.5,
                .migration_mode = WeightMigrationMode::MANUAL,
            },
    };
    auto update = metadata_store.PrepareUpdatePolicy(request, 300);
    ASSERT_TRUE(update.has_value());
    auto updated = metadata_store.Publish(*update);
    ASSERT_TRUE(updated.has_value());
    EXPECT_EQ(WeightResidencyState::COLD, updated->policy.preferred_residency);

    auto retry = metadata_store.PrepareUpdatePolicy(request, 301);
    ASSERT_TRUE(retry.has_value());
    EXPECT_TRUE(retry->no_op);

    auto stale = request;
    stale.expected_metadata_generation = 99;
    auto stale_result = metadata_store.PrepareUpdatePolicy(stale, 302);
    ASSERT_FALSE(stale_result.has_value());
    EXPECT_EQ(WeightManagementError::STALE_GENERATION, stale_result.error());

    auto migration = metadata_store.PrepareStartOperation(
        StartWeightResidencyOperationRequest{
            .identity = updated->identity,
            .expected_metadata_generation = updated->metadata_generation,
            .target_residency = WeightResidencyState::MIXED,
            .mixed_hot_ratio = 0.25,
        },
        400);
    ASSERT_TRUE(migration.has_value());
    ASSERT_TRUE(metadata_store.Publish(*migration).has_value());

    auto update_while_migrating = request;
    update_while_migrating.expected_metadata_generation =
        updated->metadata_generation + 1;
    auto blocked_update =
        metadata_store.PrepareUpdatePolicy(update_while_migrating, 401);
    ASSERT_FALSE(blocked_update.has_value());
    EXPECT_EQ(WeightManagementError::BUSY, blocked_update.error());

    auto second_migration = metadata_store.PrepareStartOperation(
        StartWeightResidencyOperationRequest{
            .identity = updated->identity,
            .expected_metadata_generation = updated->metadata_generation + 1,
            .target_residency = WeightResidencyState::COLD,
            .mixed_hot_ratio = std::nullopt,
        },
        401);
    ASSERT_FALSE(second_migration.has_value());
    EXPECT_EQ(WeightManagementError::BUSY, second_migration.error());

    auto deletion = metadata_store.PrepareDelete(
        DeleteWeightRevisionRequest{
            .identity = updated->identity,
            .expected_metadata_generation = updated->metadata_generation + 1,
        },
        401);
    ASSERT_FALSE(deletion.has_value());
    EXPECT_EQ(WeightManagementError::BUSY, deletion.error());
}

TEST(WeightPolicyStateTest, LeaseRemainsAvailableDuringMigration) {
    WeightMetadataStore metadata_store;
    const auto ready = PublishPolicyCommit(
        metadata_store, WeightStoragePolicy{
                            .preferred_residency = WeightResidencyState::HOT,
                            .mixed_hot_ratio = 0.5,
                            .migration_mode = WeightMigrationMode::MANUAL,
                        });
    auto migration = metadata_store.PrepareStartOperation(
        StartWeightResidencyOperationRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation,
            .target_residency = WeightResidencyState::MIXED,
            .mixed_hot_ratio = 0.5,
        },
        300);
    ASSERT_TRUE(migration.has_value());
    auto operation = metadata_store.Publish(*migration);
    ASSERT_TRUE(operation.has_value());

    auto lease = metadata_store.PrepareAcquireLease(
        AcquireWeightRevisionLeaseRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation + 1,
            .holder = "worker-1",
            .ttl_ms = 100,
        },
        301);
    ASSERT_TRUE(lease.has_value());
    EXPECT_TRUE(metadata_store.Publish(*lease).has_value());
}

TEST(WeightPolicyStateTest, TracksUnitAndByteProgress) {
    WeightMetadataStore metadata_store;
    const auto ready = PublishPolicyCommit(
        metadata_store, WeightStoragePolicy{
                            .preferred_residency = WeightResidencyState::HOT,
                            .mixed_hot_ratio = 0.5,
                            .migration_mode = WeightMigrationMode::MANUAL,
                        });
    auto migration = metadata_store.PrepareStartOperation(
        StartWeightResidencyOperationRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation,
            .target_residency = WeightResidencyState::MIXED,
            .mixed_hot_ratio = 0.5,
        },
        300);
    ASSERT_TRUE(migration.has_value());
    auto operation = metadata_store.Publish(*migration);
    ASSERT_TRUE(operation.has_value());

    auto progress = metadata_store.PrepareUpdateOperationProgress(
        operation->operation_id, 1, 2, 1024, 4096, {},
        WeightAvailabilityState::READY, WeightResidencyState::HOT, 1.0, 301);
    ASSERT_TRUE(progress.has_value());
    auto published = metadata_store.Publish(*progress);
    ASSERT_TRUE(published.has_value());
    EXPECT_EQ(1, published->processed_units);
    EXPECT_EQ(2, published->total_units);
    EXPECT_EQ(1024, published->processed_bytes);
    EXPECT_EQ(4096, published->total_bytes);
}

}  // namespace
}  // namespace mooncake

#include <gtest/gtest.h>

#include <type_traits>

#include "weight_management.h"
#include "ylt/struct_pack.hpp"

namespace mooncake {
namespace {

template <typename T>
concept HasTensorsField = requires(T value) { value.tensors; };

template <typename T>
concept HasRuntimeEndpointField = requires(T value) { value.runtime_endpoint; };

WeightRevisionIdentity ValidIdentity() {
    return WeightRevisionIdentity{
        .tenant_id = "tenant-a",
        .name_space = "production",
        .resource_id = "llama-70b",
        .revision = "step-100",
        .weight_generation = 7,
    };
}

WeightManifestReference ValidManifest() {
    const auto identity = ValidIdentity();
    return WeightManifestReference{
        .manifest_key = MakeWeightManifestKey(identity),
        .manifest_sha256 = std::string(64, 'a'),
        .payload_group_id = MakeWeightPayloadGroupId(identity),
        .payload_keys_sha256 = std::string(64, 'b'),
        .payload_count = 3,
        .logical_bytes = 4096,
    };
}

TEST(WeightManagementContractTest, ValidatesIdentityComponentsStrictly) {
    auto identity = ValidIdentity();
    EXPECT_TRUE(ValidateWeightRevisionIdentity(identity).ok());

    identity.name_space.clear();
    EXPECT_FALSE(ValidateWeightRevisionIdentity(identity).ok());
    identity = ValidIdentity();
    identity.resource_id.clear();
    EXPECT_FALSE(ValidateWeightRevisionIdentity(identity).ok());
    identity = ValidIdentity();
    identity.revision.clear();
    EXPECT_FALSE(ValidateWeightRevisionIdentity(identity).ok());
    identity = ValidIdentity();
    identity.weight_generation = 0;
    EXPECT_FALSE(ValidateWeightRevisionIdentity(identity).ok());
    identity = ValidIdentity();
    identity.tenant_id = "_reserved";
    EXPECT_FALSE(ValidateWeightRevisionIdentity(identity).ok());
}

TEST(WeightManagementContractTest, ValidatesSha256AndGeneration) {
    auto manifest = ValidManifest();
    EXPECT_TRUE(ValidateWeightManifestReference(manifest).ok());

    manifest.manifest_sha256 = std::string(63, 'a');
    EXPECT_FALSE(ValidateWeightManifestReference(manifest).ok());
    manifest = ValidManifest();
    manifest.manifest_sha256[4] = 'G';
    EXPECT_FALSE(ValidateWeightManifestReference(manifest).ok());
    manifest = ValidManifest();
    manifest.payload_keys_sha256[9] = 'A';
    EXPECT_FALSE(ValidateWeightManifestReference(manifest).ok());

    WeightRevisionMetadata metadata{
        .identity = ValidIdentity(),
        .manifest = ValidManifest(),
        .availability = WeightAvailabilityState::READY,
        .residency = WeightResidencyState::HOT,
        .affinity_count = 2,
        .affinity_digest = std::string(64, 'c'),
        .observed_hot_ratio = 1.0,
        .metadata_generation = 0,
        .created_at_ms = 1,
        .updated_at_ms = 1,
    };
    EXPECT_FALSE(ValidateWeightRevisionMetadata(metadata).ok());
    metadata.metadata_generation = 1;
    EXPECT_TRUE(ValidateWeightRevisionMetadata(metadata).ok());
    EXPECT_TRUE(CanAdvanceWeightMetadataGeneration(1));
    EXPECT_FALSE(CanAdvanceWeightMetadataGeneration(0));
    EXPECT_FALSE(CanAdvanceWeightMetadataGeneration(
        std::numeric_limits<uint64_t>::max() - 1));
}

TEST(WeightManagementContractTest, RejectsNonCanonicalWeightObjectNames) {
    WeightRevisionMetadata metadata{
        .identity = ValidIdentity(),
        .manifest = ValidManifest(),
        .availability = WeightAvailabilityState::READY,
        .residency = WeightResidencyState::HOT,
        .affinity_count = 2,
        .affinity_digest = std::string(64, 'c'),
        .observed_hot_ratio = 1.0,
        .metadata_generation = 1,
        .created_at_ms = 1,
        .updated_at_ms = 1,
    };
    EXPECT_TRUE(ValidateWeightRevisionMetadata(metadata).ok());

    metadata.manifest.payload_group_id = "valid-but-non-canonical-group";
    EXPECT_FALSE(ValidateWeightRevisionMetadata(metadata).ok());

    metadata.manifest = ValidManifest();
    metadata.manifest.manifest_key = "valid-but-non-canonical-manifest";
    EXPECT_FALSE(ValidateWeightRevisionMetadata(metadata).ok());
}

TEST(WeightManagementContractTest, RejectsInvalidStoragePolicyEnums) {
    auto policy = WeightStoragePolicy{};
    EXPECT_TRUE(ValidateWeightStoragePolicy(policy).ok());
    EXPECT_EQ(WeightMigrationMode::MANUAL, policy.migration_mode);
    policy.migration_mode = WeightMigrationMode::AUTO;
    EXPECT_FALSE(ValidateWeightStoragePolicy(policy).ok());
    policy.migration_mode = static_cast<WeightMigrationMode>(255);
    EXPECT_FALSE(ValidateWeightStoragePolicy(policy).ok());
}

TEST(WeightManagementContractTest, EnforcesStateCombinationAndOperationIds) {
    WeightRevisionMetadata metadata{
        .identity = ValidIdentity(),
        .manifest = ValidManifest(),
        .availability = WeightAvailabilityState::READY,
        .residency = WeightResidencyState::HOT,
        .operation_id = 10,
        .affinity_count = 2,
        .affinity_digest = std::string(64, 'c'),
        .observed_hot_ratio = 1.0,
        .metadata_generation = 1,
        .created_at_ms = 1,
        .updated_at_ms = 1,
    };
    EXPECT_TRUE(ValidateWeightRevisionMetadata(metadata).ok());

    metadata.operation_id.reset();
    metadata.residency = WeightResidencyState::ABSENT;
    metadata.observed_hot_ratio = 0.0;
    EXPECT_FALSE(ValidateWeightRevisionMetadata(metadata).ok());
    metadata.residency = WeightResidencyState::UNKNOWN;
    EXPECT_FALSE(ValidateWeightRevisionMetadata(metadata).ok());
    metadata.residency = WeightResidencyState::HOT;
    metadata.observed_hot_ratio = 1.0;
    EXPECT_TRUE(ValidateWeightRevisionMetadata(metadata).ok());
    metadata.availability = WeightAvailabilityState::DEGRADED;
    metadata.residency = WeightResidencyState::UNKNOWN;
    EXPECT_FALSE(ValidateWeightRevisionMetadata(metadata).ok());
    metadata.residency = WeightResidencyState::ABSENT;
    metadata.observed_hot_ratio = 0.0;
    EXPECT_TRUE(ValidateWeightRevisionMetadata(metadata).ok());

    metadata.availability = WeightAvailabilityState::DELETED;
    metadata.residency = WeightResidencyState::HOT;
    EXPECT_FALSE(ValidateWeightRevisionMetadata(metadata).ok());
    metadata.residency = WeightResidencyState::ABSENT;
    metadata.observed_hot_ratio = 0.0;
    EXPECT_TRUE(ValidateWeightRevisionMetadata(metadata).ok());

    metadata.availability = static_cast<WeightAvailabilityState>(255);
    EXPECT_FALSE(ValidateWeightRevisionMetadata(metadata).ok());
    metadata.availability = WeightAvailabilityState::DELETED;
    metadata.residency = static_cast<WeightResidencyState>(255);
    EXPECT_FALSE(ValidateWeightRevisionMetadata(metadata).ok());
    metadata.residency = WeightResidencyState::ABSENT;
    metadata.availability = WeightAvailabilityState::IMPORTING;
    metadata.residency = WeightResidencyState::ABSENT;
    EXPECT_FALSE(ValidateWeightRevisionMetadata(metadata).ok());
    metadata.residency = WeightResidencyState::UNKNOWN;
    EXPECT_TRUE(ValidateWeightRevisionMetadata(metadata).ok());
    metadata.operation_id = 10;
    EXPECT_FALSE(ValidateWeightRevisionMetadata(metadata).ok());

    metadata.operation_id.reset();
    metadata.availability = WeightAvailabilityState::READY;
    EXPECT_FALSE(ValidateWeightRevisionMetadata(metadata).ok());
    metadata.residency = static_cast<WeightResidencyState>(255);
    EXPECT_FALSE(ValidateWeightRevisionMetadata(metadata).ok());
    metadata.residency = WeightResidencyState::HOT;
    metadata.observed_hot_ratio = 1.0;
    metadata.availability = static_cast<WeightAvailabilityState>(255);
    EXPECT_FALSE(ValidateWeightRevisionMetadata(metadata).ok());
}

TEST(WeightManagementContractTest, RestrictsAvailabilityTransitions) {
    using State = WeightAvailabilityState;
    EXPECT_TRUE(
        IsValidWeightAvailabilityTransition(State::IMPORTING, State::READY));
    EXPECT_TRUE(
        IsValidWeightAvailabilityTransition(State::IMPORTING, State::DELETING));
    EXPECT_TRUE(
        IsValidWeightAvailabilityTransition(State::READY, State::DEGRADED));
    EXPECT_TRUE(
        IsValidWeightAvailabilityTransition(State::READY, State::DELETING));
    EXPECT_TRUE(
        IsValidWeightAvailabilityTransition(State::DEGRADED, State::READY));
    EXPECT_TRUE(
        IsValidWeightAvailabilityTransition(State::DEGRADED, State::DELETING));
    EXPECT_TRUE(
        IsValidWeightAvailabilityTransition(State::DELETING, State::DELETED));

    EXPECT_FALSE(
        IsValidWeightAvailabilityTransition(State::IMPORTING, State::DEGRADED));
    EXPECT_FALSE(
        IsValidWeightAvailabilityTransition(State::READY, State::DELETED));
    EXPECT_FALSE(
        IsValidWeightAvailabilityTransition(State::DELETED, State::READY));
    EXPECT_FALSE(
        IsValidWeightAvailabilityTransition(State::READY, State::READY));
}

TEST(WeightManagementContractTest, ValidatesIntrinsicOperationShape) {
    WeightResidencyOperation valid{
        .kind = WeightOperationKind::MIGRATING,
        .target_residency = WeightResidencyState::HOT,
        .processed_units = 1,
        .total_units = 2,
        .processed_bytes = 128,
        .total_bytes = 256,
    };
    EXPECT_TRUE(IsValidWeightOperationShape(valid));
    auto operation = valid;
    operation.kind = WeightOperationKind::REPAIRING;
    EXPECT_TRUE(IsValidWeightOperationShape(operation));
    operation.kind = static_cast<WeightOperationKind>(255);
    EXPECT_FALSE(IsValidWeightOperationShape(operation));
    for (const auto target :
         {WeightResidencyState::UNKNOWN, WeightResidencyState::ABSENT}) {
        operation = valid;
        operation.target_residency = target;
        EXPECT_FALSE(IsValidWeightOperationShape(operation));
    }
    for (const auto ratio :
         {0.0, 1.0, -0.1, std::numeric_limits<double>::infinity(),
          std::numeric_limits<double>::quiet_NaN()}) {
        operation = valid;
        operation.target_residency = WeightResidencyState::MIXED;
        operation.target_hot_ratio = ratio;
        EXPECT_FALSE(IsValidWeightOperationShape(operation));
    }
    operation = valid;
    operation.target_residency = WeightResidencyState::MIXED;
    EXPECT_FALSE(IsValidWeightOperationShape(operation));
    operation.target_hot_ratio = 0.5;
    EXPECT_TRUE(IsValidWeightOperationShape(operation));
    operation.target_residency = WeightResidencyState::COLD;
    EXPECT_FALSE(IsValidWeightOperationShape(operation));
    operation.target_hot_ratio.reset();
    EXPECT_TRUE(IsValidWeightOperationShape(operation));
    operation = valid;
    operation.total_units = 0;
    EXPECT_FALSE(IsValidWeightOperationShape(operation));
    operation = valid;
    operation.total_bytes = 0;
    EXPECT_FALSE(IsValidWeightOperationShape(operation));
    operation = valid;
    operation.processed_units = operation.total_units + 1;
    EXPECT_FALSE(IsValidWeightOperationShape(operation));
    operation = valid;
    operation.processed_bytes = operation.total_bytes + 1;
    EXPECT_FALSE(IsValidWeightOperationShape(operation));
    operation = valid;
    operation.cursor = std::string(1, '\0');
    EXPECT_FALSE(IsValidWeightOperationShape(operation));
    operation.cursor = "affinity-a";
    EXPECT_TRUE(IsValidWeightOperationShape(operation));
}

TEST(WeightManagementContractTest, RoundTripsWireEnumsAndMetadata) {
    WeightRevisionMetadata metadata{
        .identity = ValidIdentity(),
        .manifest = ValidManifest(),
        .availability = WeightAvailabilityState::DEGRADED,
        .residency = WeightResidencyState::MIXED,
        .operation_id = 42,
        .affinity_count = 2,
        .affinity_digest = std::string(64, 'c'),
        .observed_hot_ratio = 0.5,
        .metadata_generation = 9,
        .created_at_ms = 100,
        .updated_at_ms = 200,
    };

    auto encoded = struct_pack::serialize(metadata);
    WeightRevisionMetadata decoded;
    ASSERT_EQ(struct_pack::errc::ok,
              struct_pack::deserialize_to(decoded, encoded));
    EXPECT_EQ(metadata, decoded);
}

TEST(WeightManagementContractTest, CanonicalDigestsAreStableAndUnambiguous) {
    EXPECT_EQ(
        "ff2c110b8f18291ea676be2af036320944751c4b3e6a65b3c1153721c61c2b26",
        ComputeWeightPayloadKeysSha256({"p2", "p1"}));

    auto identity = ValidIdentity();
    identity.tenant_id = "default";
    EXPECT_EQ(
        "weight:5dbbea14aa75c98a5b3fbb576a9c612d64fc6bc8f490f24a7ff0"
        "17fcfe321b7d",
        MakeWeightPayloadGroupId(identity));
}

TEST(WeightManagementContractTest, ManifestKeyUsesCanonicalUrlEncoding) {
    auto identity = ValidIdentity();
    identity.name_space = "prod east";
    identity.resource_id = "family/model";
    identity.revision = "v1%candidate";
    EXPECT_EQ("weights/prod%20east/family%2Fmodel/v1%25candidate/7/manifest",
              MakeWeightManifestKey(identity));
}

TEST(WeightManagementContractTest, MetadataDoesNotDuplicateManifestChildren) {
    static_assert(!HasTensorsField<WeightRevisionMetadata>);
    static_assert(!HasRuntimeEndpointField<WeightRevisionMetadata>);
    SUCCEED();
}

}  // namespace
}  // namespace mooncake

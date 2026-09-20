#include <gtest/gtest.h>

#include <string>

#include "master_client.h"
#include "test_server_helpers.h"

namespace mooncake::testing {
namespace {

WeightRevisionIdentity Identity() {
    return WeightRevisionIdentity{
        .tenant_id = "forged-tenant",
        .name_space = "production",
        .resource_id = "llama-70b",
        .revision = "step-100",
        .weight_generation = 7,
    };
}

template <typename T>
const tl::expected<T, WeightManagementError>& Domain(
    const WeightRpcResult<T>& result) {
    EXPECT_TRUE(result.has_value());
    return *result;
}

TEST(WeightManagementRpcTest, RegistersEveryApiAndBindsTenant) {
    InProcMaster server;
    ASSERT_TRUE(server.Start(InProcMasterConfigBuilder().build()));
    MasterClient client(generate_uuid(), nullptr, "default");
    ASSERT_EQ(ErrorCode::OK, client.Connect(server.master_address()));

    const auto begin_request = BeginWeightImportRequest{
        .identity = Identity(),
        .payload_group_id = {},
        .expected_payload_count = 1,
        .expected_logical_bytes = 1024,
    };
    auto begin = client.BeginWeightImport(begin_request);
    ASSERT_TRUE(Domain(begin).has_value());
    EXPECT_EQ("default", Domain(begin)->identity.tenant_id);

    auto retry = client.BeginWeightImport(begin_request);
    ASSERT_TRUE(Domain(retry).has_value());
    EXPECT_EQ(*Domain(begin), *Domain(retry));

    auto get = client.GetWeightRevision(
        GetWeightRevisionRequest{.identity = Identity()});
    ASSERT_TRUE(Domain(get).has_value());
    EXPECT_EQ(Domain(begin)->metadata_generation,
              Domain(get)->metadata.metadata_generation);

    auto list = client.ListWeightRevisions(ListWeightRevisionsRequest{
        .tenant_id = "forged-tenant",
        .name_space = "production",
        .resource_id = "llama-70b",
        .limit = 1,
    });
    ASSERT_TRUE(Domain(list).has_value());
    ASSERT_EQ(1, Domain(list)->revisions.size());

    auto commit = client.CommitWeightImport(CommitWeightImportRequest{
        .identity = Identity(),
        .expected_metadata_generation = Domain(begin)->metadata_generation,
        .manifest =
            WeightManifestReference{
                .manifest_key =
                    "weights/production/llama-70b/step-100/7/manifest",
                .manifest_sha256 = std::string(64, 'a'),
                .payload_group_id = Domain(begin)->manifest.payload_group_id,
                .payload_keys_sha256 = std::string(64, 'b'),
                .payload_count = 1,
                .logical_bytes = 1024,
            },
    });
    ASSERT_FALSE(Domain(commit).has_value());
    EXPECT_EQ(WeightManagementError::NOT_FOUND, Domain(commit).error());

    auto acquire = client.AcquireWeightRevisionLease(
        AcquireWeightRevisionLeaseRequest{
            .identity = Identity(),
            .expected_metadata_generation =
                Domain(begin)->metadata_generation,
            .holder = "worker-0",
            .ttl_ms = 1000,
        });
    ASSERT_FALSE(Domain(acquire).has_value());
    EXPECT_EQ(WeightManagementError::NOT_READY, Domain(acquire).error());

    auto renew = client.RenewWeightRevisionLease(
        RenewWeightRevisionLeaseRequest{.lease_id = 999, .ttl_ms = 1000});
    ASSERT_FALSE(Domain(renew).has_value());
    EXPECT_EQ(WeightManagementError::NOT_FOUND, Domain(renew).error());

    auto release = client.ReleaseWeightRevisionLease(
        ReleaseWeightRevisionLeaseRequest{.lease_id = 999});
    EXPECT_TRUE(Domain(release).has_value());

    auto start = client.StartWeightResidencyOperation(
        StartWeightResidencyOperationRequest{
            .identity = Identity(),
            .expected_metadata_generation =
                Domain(begin)->metadata_generation,
            .target_residency = WeightResidencyState::COLD,
        });
    ASSERT_FALSE(Domain(start).has_value());
    EXPECT_EQ(WeightManagementError::NOT_READY, Domain(start).error());

    auto query = client.QueryWeightOperation(
        QueryWeightOperationRequest{.operation_id = 999});
    ASSERT_FALSE(Domain(query).has_value());
    EXPECT_EQ(WeightManagementError::NOT_FOUND, Domain(query).error());

    auto abort = client.AbortWeightImport(AbortWeightImportRequest{
        .identity = Identity(),
        .expected_metadata_generation = Domain(begin)->metadata_generation,
    });
    ASSERT_TRUE(Domain(abort).has_value());
    EXPECT_EQ(WeightAvailabilityState::DELETING,
              Domain(abort)->availability);

    auto reconcile = client.ReconcileWeightRevision(
        ReconcileWeightRevisionRequest{.identity = Identity()});
    ASSERT_TRUE(Domain(reconcile).has_value());
    EXPECT_EQ(WeightAvailabilityState::DELETED,
              Domain(reconcile)->availability);

    auto deleted = client.DeleteWeightRevision(DeleteWeightRevisionRequest{
        .identity = Identity(),
        .expected_metadata_generation =
            Domain(reconcile)->metadata_generation,
    });
    ASSERT_TRUE(Domain(deleted).has_value());
    EXPECT_EQ(WeightAvailabilityState::DELETED,
              Domain(deleted)->availability);
}

TEST(WeightManagementRpcTest, RejectsUnboundedPaginationExactly) {
    InProcMaster server;
    ASSERT_TRUE(server.Start(InProcMasterConfigBuilder().build()));
    MasterClient client(generate_uuid());
    ASSERT_EQ(ErrorCode::OK, client.Connect(server.master_address()));

    auto list = client.ListWeightRevisions(ListWeightRevisionsRequest{
        .name_space = "production",
        .resource_id = "llama-70b",
        .limit = 1001,
    });
    ASSERT_FALSE(Domain(list).has_value());
    EXPECT_EQ(WeightManagementError::INVALID_ARGUMENT, Domain(list).error());
}

}  // namespace
}  // namespace mooncake::testing

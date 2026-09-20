#include "master_service_test_fixture.h"

#include <gtest/gtest.h>

#include <string>
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

    WeightRevisionMetadata PublishReady(MasterService& service,
                                        const UUID& client_id,
                                        std::string revision) {
        const auto identity = Identity(std::move(revision));
        auto importing = service.BeginWeightImport(BeginWeightImportRequest{
            .identity = identity,
            .payload_group_id = {},
            .expected_payload_count = 1,
            .expected_logical_bytes = 1024,
        });
        EXPECT_TRUE(importing.has_value());
        const auto payload_key = identity.revision + "-payload";
        ReplicateConfig config;
        config.replica_num = 1;
        config.with_hard_pin = true;
        config.group_ids =
            std::vector<std::string>{importing->manifest.payload_group_id};
        config.data_type = ObjectDataType::WEIGHT;
        PutCompletedObject(service, client_id, payload_key, config, 1024);
        config.data_type = ObjectDataType::METADATA;
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
};

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
    });
    auto second = service.BeginWeightImport(BeginWeightImportRequest{
        .identity = Identity("step-b"),
        .payload_group_id = {},
        .expected_payload_count = 1,
        .expected_logical_bytes = 1024,
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
    for (const auto& metadata : {payload_loss, manifest_loss}) {
        auto view = service.GetWeightRevision(
            GetWeightRevisionRequest{.identity = metadata.identity});
        ASSERT_TRUE(view.has_value());
        EXPECT_EQ(WeightAvailabilityState::DEGRADED,
                  view->metadata.availability);
        EXPECT_EQ(WeightResidencyState::MIXED, view->metadata.residency);
    }
    EXPECT_EQ(2, MasterMetricManager::instance().get_weight_revision_count(
                     "degraded"));
    EXPECT_EQ(
        2, MasterMetricManager::instance().get_weight_residency_count("mixed"));
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
            .target_residency = WeightResidencyState::COLD,
        });
    ASSERT_TRUE(operation.has_value());
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

#include <gtest/gtest.h>

#include <atomic>
#include <limits>
#include <thread>
#include <vector>

#include "weight_metadata_store.h"
#include "tenant_id.h"

namespace mooncake {
namespace {

WeightRevisionIdentity Identity(std::string revision = "step-100",
                                uint64_t generation = 7) {
    return WeightRevisionIdentity{
        .tenant_id = "tenant-a",
        .name_space = "production",
        .resource_id = "llama-70b",
        .revision = std::move(revision),
        .weight_generation = generation,
    };
}

BeginWeightImportRequest BeginRequest(
    const WeightRevisionIdentity& identity = Identity()) {
    return BeginWeightImportRequest{
        .identity = identity,
        .payload_group_id = MakeWeightPayloadGroupId(identity),
        .expected_payload_count = 3,
        .expected_logical_bytes = 4096,
        .policy =
            WeightStoragePolicy{
                .preferred_residency = WeightResidencyState::HOT,
                .mixed_hot_ratio = 0.5,
                .migration_mode = WeightMigrationMode::MANUAL,
            },
        .affinity_summary =
            WeightAffinitySummary{
                .affinity_count = 3,
                .affinity_digest = std::string(64, 'c'),
            },
    };
}

WeightManifestReference Manifest() {
    const auto identity = Identity();
    return WeightManifestReference{
        .manifest_key = MakeWeightManifestKey(identity),
        .manifest_sha256 = std::string(64, 'a'),
        .payload_group_id = MakeWeightPayloadGroupId(identity),
        .payload_keys_sha256 = std::string(64, 'b'),
        .payload_count = 3,
        .logical_bytes = 4096,
    };
}

WeightRevisionMetadata PublishBegin(WeightMetadataStore& metadata_store,
                                    const BeginWeightImportRequest& request,
                                    uint64_t now_ms = 100) {
    auto candidate = metadata_store.PrepareBeginImport(request, now_ms);
    EXPECT_TRUE(candidate.has_value());
    auto published = metadata_store.Publish(*candidate);
    EXPECT_TRUE(published.has_value());
    return *published;
}

WeightRevisionMetadata PublishReady(WeightMetadataStore& metadata_store) {
    auto importing = PublishBegin(metadata_store, BeginRequest());
    auto candidate = metadata_store.PrepareCommitImport(
        CommitWeightImportRequest{
            .identity = importing.identity,
            .expected_metadata_generation = importing.metadata_generation,
            .manifest = Manifest(),
        },
        200);
    EXPECT_TRUE(candidate.has_value());
    auto published = metadata_store.Publish(*candidate);
    EXPECT_TRUE(published.has_value());
    return *published;
}

TEST(WeightMetadataStoreTest, BeginIsIdempotent) {
    WeightMetadataStore metadata_store;
    auto first = PublishBegin(metadata_store, BeginRequest());
    EXPECT_EQ(WeightAvailabilityState::IMPORTING, first.availability);
    EXPECT_EQ(1, first.metadata_generation);

    auto retry = metadata_store.PrepareBeginImport(BeginRequest(), 150);
    ASSERT_TRUE(retry.has_value());
    EXPECT_TRUE(retry->no_op);
    auto retried = metadata_store.Publish(*retry);
    ASSERT_TRUE(retried.has_value());
    EXPECT_EQ(first, *retried);
}

TEST(WeightMetadataStoreTest, RejectsNonCanonicalWeightObjectNames) {
    WeightMetadataStore metadata_store;
    auto noncanonical_begin = BeginRequest();
    noncanonical_begin.payload_group_id = "valid-but-non-canonical-group";
    auto rejected = metadata_store.PrepareBeginImport(noncanonical_begin, 100);
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(WeightManagementError::INVALID_ARGUMENT, rejected.error());

    auto importing = PublishBegin(metadata_store, BeginRequest());
    auto noncanonical_manifest = Manifest();
    noncanonical_manifest.manifest_key = "valid-but-non-canonical-manifest";
    rejected = metadata_store.PrepareCommitImport(
        CommitWeightImportRequest{
            .identity = importing.identity,
            .expected_metadata_generation = importing.metadata_generation,
            .manifest = std::move(noncanonical_manifest),
        },
        200);
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(WeightManagementError::INVALID_ARGUMENT, rejected.error());

    auto ready = PublishReady(metadata_store);
    auto snapshot = metadata_store.ExportSnapshot();
    snapshot.metadata[0].manifest.payload_group_id =
        "valid-but-non-canonical-group";
    WeightMetadataStore restored;
    EXPECT_EQ(WeightManagementError::INVALID_ARGUMENT,
              restored.RestoreSnapshot(snapshot).error());

    snapshot = metadata_store.ExportSnapshot();
    snapshot.metadata[0].manifest.manifest_key =
        "valid-but-non-canonical-manifest";
    EXPECT_EQ(WeightManagementError::INVALID_ARGUMENT,
              restored.RestoreSnapshot(snapshot).error());
    EXPECT_EQ(ready, metadata_store.Get(ready.identity, 200)->metadata);
}

TEST(WeightMetadataStoreTest, CommitUsesCasAndIsRetryableAfterResponseLoss) {
    WeightMetadataStore metadata_store;
    auto importing = PublishBegin(metadata_store, BeginRequest());
    auto request = CommitWeightImportRequest{
        .identity = importing.identity,
        .expected_metadata_generation = importing.metadata_generation,
        .manifest = Manifest(),
    };
    auto candidate = metadata_store.PrepareCommitImport(request, 200);
    ASSERT_TRUE(candidate.has_value());

    auto stale = request;
    stale.expected_metadata_generation = importing.metadata_generation + 1;
    auto rejected = metadata_store.PrepareCommitImport(stale, 200);
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(WeightManagementError::STALE_GENERATION, rejected.error());

    auto published = metadata_store.Publish(*candidate);
    ASSERT_TRUE(published.has_value());
    EXPECT_EQ(WeightAvailabilityState::READY, published->availability);
    EXPECT_EQ(2, published->metadata_generation);

    auto response_lost_retry = metadata_store.PrepareCommitImport(request, 300);
    ASSERT_TRUE(response_lost_retry.has_value());
    EXPECT_TRUE(response_lost_retry->no_op);
    auto retry_result = metadata_store.Publish(*response_lost_retry);
    ASSERT_TRUE(retry_result.has_value());
    EXPECT_EQ(*published, *retry_result);

    auto unrelated_generation = request;
    unrelated_generation.expected_metadata_generation = 99;
    auto wrong_retry =
        metadata_store.PrepareCommitImport(unrelated_generation, 301);
    ASSERT_FALSE(wrong_retry.has_value());
    EXPECT_EQ(WeightManagementError::STALE_GENERATION, wrong_retry.error());
}

TEST(WeightMetadataStoreTest, AbortRetryRequiresAdjacentGeneration) {
    WeightMetadataStore metadata_store;
    auto importing = PublishBegin(metadata_store, BeginRequest());
    const AbortWeightImportRequest request{
        .identity = importing.identity,
        .expected_metadata_generation = importing.metadata_generation,
    };
    auto candidate = metadata_store.PrepareAbortImport(request, 200);
    ASSERT_TRUE(candidate.has_value());
    ASSERT_TRUE(metadata_store.Publish(*candidate).has_value());

    auto retry = metadata_store.PrepareAbortImport(request, 201);
    ASSERT_TRUE(retry.has_value());
    EXPECT_TRUE(retry->no_op);

    auto unrelated_generation = request;
    unrelated_generation.expected_metadata_generation = 99;
    auto rejected =
        metadata_store.PrepareAbortImport(unrelated_generation, 202);
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(WeightManagementError::STALE_GENERATION, rejected.error());
}

TEST(WeightMetadataStoreTest, LookupAndPaginationAreExactAndDeterministic) {
    WeightMetadataStore metadata_store;
    for (const auto& [revision, generation] :
         std::vector<std::pair<std::string, uint64_t>>{
             {"step-20", 2}, {"step-10", 3}, {"step-10", 1}}) {
        auto request = BeginRequest(Identity(revision, generation));
        PublishBegin(metadata_store, request);
    }

    auto exact = metadata_store.Get(Identity("step-10", 3), 200);
    ASSERT_TRUE(exact.has_value());
    EXPECT_EQ(3, exact->metadata.identity.weight_generation);

    ListWeightRevisionsRequest request{
        .tenant_id = "tenant-a",
        .name_space = "production",
        .resource_id = "llama-70b",
        .page_token = {},
        .limit = 2,
    };
    auto first = metadata_store.List(request, 200);
    ASSERT_TRUE(first.has_value());
    ASSERT_EQ(2, first->revisions.size());
    EXPECT_EQ("step-10", first->revisions[0].metadata.identity.revision);
    EXPECT_EQ(1, first->revisions[0].metadata.identity.weight_generation);
    EXPECT_EQ("step-10", first->revisions[1].metadata.identity.revision);
    EXPECT_EQ(3, first->revisions[1].metadata.identity.weight_generation);
    ASSERT_FALSE(first->next_page_token.empty());

    request.page_token = first->next_page_token;
    auto second = metadata_store.List(request, 200);
    ASSERT_TRUE(second.has_value());
    ASSERT_EQ(1, second->revisions.size());
    EXPECT_EQ("step-20", second->revisions[0].metadata.identity.revision);
    EXPECT_TRUE(second->next_page_token.empty());
}

TEST(WeightMetadataStoreTest, LeaseExpiryIsGenerationFencedAndIdempotent) {
    WeightMetadataStore metadata_store;
    auto ready = PublishReady(metadata_store);
    auto candidate = metadata_store.PrepareAcquireLease(
        AcquireWeightRevisionLeaseRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation,
            .holder = "worker-1",
            .ttl_ms = 50,
        },
        300);
    ASSERT_TRUE(candidate.has_value());
    auto lease = metadata_store.Publish(*candidate);
    ASSERT_TRUE(lease.has_value());
    EXPECT_TRUE(metadata_store.HasActiveLease(ready.identity,
                                              ready.metadata_generation, 349));
    EXPECT_TRUE(metadata_store.HasActiveLease(
        ready.identity, ready.metadata_generation + 1, 349));

    auto expired = metadata_store.PrepareExpireLeases(350);
    ASSERT_EQ(1, expired.size());
    ASSERT_TRUE(metadata_store.Publish(expired.front()).has_value());
    EXPECT_FALSE(metadata_store.HasActiveLease(ready.identity,
                                               ready.metadata_generation, 350));
    EXPECT_TRUE(metadata_store.PrepareExpireLeases(350).empty());
}

TEST(WeightMetadataStoreTest, RenewalNeverShortensLeaseExpiry) {
    WeightMetadataStore metadata_store;
    const auto ready = PublishReady(metadata_store);
    auto candidate = metadata_store.PrepareAcquireLease(
        AcquireWeightRevisionLeaseRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation,
            .holder = "worker-1",
            .ttl_ms = 100,
        },
        300);
    ASSERT_TRUE(candidate.has_value());
    auto lease = metadata_store.Publish(*candidate);
    ASSERT_TRUE(lease.has_value());

    for (const uint64_t ttl_ms : {10, 50, 100}) {
        SCOPED_TRACE(ttl_ms);
        auto renewed = metadata_store.PrepareRenewLease(
            RenewWeightRevisionLeaseRequest{
                .tenant_id = ready.identity.tenant_id,
                .lease_id = lease->lease_id,
                .ttl_ms = ttl_ms,
            },
            350);
        ASSERT_TRUE(renewed.has_value());
        auto published = metadata_store.Publish(*renewed);
        ASSERT_TRUE(published.has_value());
        EXPECT_EQ(ttl_ms == 100 ? 450u : 400u, published->expires_at_ms);
        EXPECT_EQ(lease->fenced_metadata_generation,
                  published->fenced_metadata_generation);
    }
    auto expired = metadata_store.PrepareRenewLease(
        RenewWeightRevisionLeaseRequest{
            .tenant_id = ready.identity.tenant_id,
            .lease_id = lease->lease_id,
            .ttl_ms = 100,
        },
        450);
    ASSERT_FALSE(expired.has_value());
    EXPECT_EQ(WeightManagementError::LEASE_EXPIRED, expired.error());
}

TEST(WeightMetadataStoreTest, RenewalSaturatesExpiryOnOverflow) {
    WeightMetadataStore metadata_store;
    const auto ready = PublishReady(metadata_store);
    const auto maximum = std::numeric_limits<uint64_t>::max();
    auto candidate = metadata_store.PrepareAcquireLease(
        AcquireWeightRevisionLeaseRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation,
            .holder = "worker-1",
            .ttl_ms = 50,
        },
        maximum - 100);
    ASSERT_TRUE(candidate.has_value());
    auto lease = metadata_store.Publish(*candidate);
    ASSERT_TRUE(lease.has_value());
    auto renewed = metadata_store.PrepareRenewLease(
        RenewWeightRevisionLeaseRequest{
            .tenant_id = ready.identity.tenant_id,
            .lease_id = lease->lease_id,
            .ttl_ms = 100,
        },
        maximum - 75);
    ASSERT_TRUE(renewed.has_value());
    auto published = metadata_store.Publish(*renewed);
    ASSERT_TRUE(published.has_value());
    EXPECT_EQ(maximum, published->expires_at_ms);
}

TEST(WeightMetadataStoreTest, LeaseReplayAdvancesAllocatorWatermark) {
    WeightMetadataStore source;
    WeightMetadataStore replay_target;
    auto source_ready = PublishReady(source);
    auto target_ready = PublishReady(replay_target);
    ASSERT_EQ(source_ready, target_ready);

    auto replayed = source.PrepareAcquireLease(
        AcquireWeightRevisionLeaseRequest{
            .identity = source_ready.identity,
            .expected_metadata_generation = source_ready.metadata_generation,
            .holder = "worker-1",
            .ttl_ms = 100,
        },
        300);
    ASSERT_TRUE(replayed.has_value());
    ASSERT_TRUE(replay_target.Publish(*replayed).has_value());

    const auto snapshot = replay_target.ExportSnapshot();
    EXPECT_EQ(replayed->lease_id + 1, snapshot.next_lease_id);
    WeightMetadataStore restored;
    EXPECT_TRUE(restored.RestoreSnapshot(snapshot).has_value());

    auto next = replay_target.PrepareAcquireLease(
        AcquireWeightRevisionLeaseRequest{
            .identity = target_ready.identity,
            .expected_metadata_generation = target_ready.metadata_generation,
            .holder = "worker-2",
            .ttl_ms = 100,
        },
        301);
    ASSERT_TRUE(next.has_value());
    EXPECT_NE(replayed->lease_id, next->lease_id);
}

TEST(WeightMetadataStoreTest, LeaseReplayRejectsExhaustedAllocatorId) {
    WeightMetadataStore source;
    WeightMetadataStore replay_target;
    auto ready = PublishReady(source);
    ASSERT_EQ(ready, PublishReady(replay_target));

    auto replayed = source.PrepareAcquireLease(
        AcquireWeightRevisionLeaseRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation,
            .holder = "worker-1",
            .ttl_ms = 100,
        },
        300);
    ASSERT_TRUE(replayed.has_value());
    replayed->lease_id = std::numeric_limits<uint64_t>::max();
    replayed->next->lease_id = replayed->lease_id;

    auto published = replay_target.Publish(*replayed);
    ASSERT_FALSE(published.has_value());
    EXPECT_EQ(WeightManagementError::GENERATION_EXHAUSTED, published.error());
}

TEST(WeightMetadataStoreTest, ExcludesConcurrentResidencyOperations) {
    WeightMetadataStore metadata_store;
    auto ready = PublishReady(metadata_store);
    auto first = metadata_store.PrepareStartOperation(
        StartWeightResidencyOperationRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation,
            .target_residency = WeightResidencyState::COLD,
        },
        300);
    ASSERT_TRUE(first.has_value());
    auto operation = metadata_store.Publish(*first);
    ASSERT_TRUE(operation.has_value());
    EXPECT_EQ(WeightOperationKind::MIGRATING, operation->kind);

    auto unrelated_generation = metadata_store.PrepareStartOperation(
        StartWeightResidencyOperationRequest{
            .identity = ready.identity,
            .expected_metadata_generation = 99,
            .target_residency = WeightResidencyState::COLD,
        },
        301);
    ASSERT_FALSE(unrelated_generation.has_value());
    EXPECT_EQ(WeightManagementError::STALE_GENERATION,
              unrelated_generation.error());

    auto conflicting = metadata_store.PrepareStartOperation(
        StartWeightResidencyOperationRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation + 1,
            .target_residency = WeightResidencyState::HOT,
        },
        301);
    ASSERT_FALSE(conflicting.has_value());
    EXPECT_EQ(WeightManagementError::BUSY, conflicting.error());
}

TEST(WeightMetadataStoreTest, UnchangedOperationProgressIsIdempotent) {
    WeightMetadataStore metadata_store;
    auto ready = PublishReady(metadata_store);
    auto started = metadata_store.PrepareStartOperation(
        StartWeightResidencyOperationRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation,
            .target_residency = WeightResidencyState::COLD,
        },
        300);
    ASSERT_TRUE(started.has_value());
    auto operation = metadata_store.Publish(*started);
    ASSERT_TRUE(operation.has_value());

    auto progress = metadata_store.PrepareUpdateOperationProgress(
        operation->operation_id, 1, operation->total_units, 1024,
        operation->total_bytes, "unit-1", {}, WeightAvailabilityState::READY,
        WeightResidencyState::MIXED, 0.75, 400);
    ASSERT_TRUE(progress.has_value());
    EXPECT_FALSE(progress->no_op);
    auto published = metadata_store.Publish(*progress);
    ASSERT_TRUE(published.has_value());
    EXPECT_EQ(400, published->updated_at_ms);

    auto retry = metadata_store.PrepareUpdateOperationProgress(
        operation->operation_id, 1, operation->total_units, 1024,
        operation->total_bytes, "unit-1", {}, WeightAvailabilityState::READY,
        WeightResidencyState::MIXED, 0.75, 500);
    ASSERT_TRUE(retry.has_value());
    EXPECT_TRUE(retry->no_op);
    auto retried = metadata_store.Publish(*retry);
    ASSERT_TRUE(retried.has_value());
    EXPECT_EQ(400, retried->updated_at_ms);
}

TEST(WeightMetadataStoreTest, OperationObservationPreservesRecoveryFences) {
    WeightMetadataStore metadata_store;
    auto ready = PublishReady(metadata_store);
    auto start = metadata_store.PrepareStartOperation(
        {.identity = ready.identity,
         .expected_metadata_generation = ready.metadata_generation,
         .target_residency = WeightResidencyState::COLD},
        300);
    ASSERT_TRUE(start);
    auto operation = metadata_store.Publish(*start);
    ASSERT_TRUE(operation);

    auto invalid = metadata_store.PrepareUpdateOperationProgress(
        operation->operation_id, 0, operation->total_units, 0, 4096, {}, {},
        WeightAvailabilityState::READY, WeightResidencyState::ABSENT, 0.0, 400);
    ASSERT_FALSE(invalid);
    EXPECT_EQ(WeightManagementError::INVALID_ARGUMENT, invalid.error());
    auto loss = metadata_store.PrepareUpdateOperationProgress(
        operation->operation_id, 0, operation->total_units, 0, 4096, {}, {},
        WeightAvailabilityState::DEGRADED, WeightResidencyState::ABSENT, 0.0,
        400);
    ASSERT_TRUE(loss);
    EXPECT_EQ(start->metadata.next->metadata_generation + 1,
              loss->metadata.next->metadata_generation);
    EXPECT_EQ(loss->metadata.next->metadata_generation,
              loss->next->fenced_metadata_generation);
    ASSERT_TRUE(metadata_store.Publish(*loss));
    WeightMetadataStore restored;
    ASSERT_TRUE(restored.RestoreSnapshot(metadata_store.ExportSnapshot()));
    auto retry = restored.PrepareUpdateOperationProgress(
        operation->operation_id, 0, operation->total_units, 0, 4096, {}, {},
        WeightAvailabilityState::DEGRADED, WeightResidencyState::ABSENT, 0.0,
        500);
    ASSERT_TRUE(retry);
    EXPECT_TRUE(retry->no_op);

    auto exhausted_snapshot = restored.ExportSnapshot();
    exhausted_snapshot.metadata.front().metadata_generation =
        std::numeric_limits<uint64_t>::max() - 1;
    exhausted_snapshot.operations.front().fenced_metadata_generation =
        std::numeric_limits<uint64_t>::max() - 1;
    WeightMetadataStore exhausted;
    ASSERT_TRUE(exhausted.RestoreSnapshot(exhausted_snapshot));
    auto rejected = exhausted.PrepareUpdateOperationProgress(
        operation->operation_id, 0, operation->total_units, 0, 4096, {}, {},
        WeightAvailabilityState::READY, WeightResidencyState::HOT, 1.0, 600);
    ASSERT_FALSE(rejected);
    EXPECT_EQ(WeightManagementError::GENERATION_EXHAUSTED, rejected.error());
    EXPECT_EQ(exhausted_snapshot, exhausted.ExportSnapshot());

    auto finish = restored.PrepareFinishOperation(
        operation->operation_id, WeightResidencyState::COLD, 0.0, 600);
    ASSERT_TRUE(finish);
    EXPECT_EQ(WeightAvailabilityState::READY,
              finish->metadata.next->availability);
    ASSERT_TRUE(restored.Publish(*finish));
    WeightMetadataStore completed;
    EXPECT_TRUE(completed.RestoreSnapshot(restored.ExportSnapshot()));
}

TEST(WeightMetadataStoreTest, ActiveDegradedOperationRoundTripsAndRecovers) {
    for (const auto residency :
         {WeightResidencyState::HOT, WeightResidencyState::ABSENT}) {
        SCOPED_TRACE(static_cast<int>(residency));
        WeightMetadataStore metadata_store;
        const auto ready = PublishReady(metadata_store);
        const auto start = metadata_store.PrepareStartOperation(
            StartWeightResidencyOperationRequest{
                .identity = ready.identity,
                .expected_metadata_generation = ready.metadata_generation,
                .target_residency = WeightResidencyState::COLD,
            },
            300);
        ASSERT_TRUE(start.has_value());
        const auto operation = metadata_store.Publish(*start);
        ASSERT_TRUE(operation.has_value());
        const double ratio = residency == WeightResidencyState::HOT ? 1.0 : 0.0;
        const auto degraded = metadata_store.PrepareUpdateOperationProgress(
            operation->operation_id, 0, operation->total_units, 0,
            operation->total_bytes, {}, {}, WeightAvailabilityState::DEGRADED,
            residency, ratio, 400);
        ASSERT_TRUE(degraded.has_value());
        EXPECT_FALSE(degraded->no_op);
        const auto published = metadata_store.Publish(*degraded);
        ASSERT_TRUE(published.has_value());
        const auto view = metadata_store.Get(ready.identity, 400);
        ASSERT_TRUE(view.has_value());
        EXPECT_EQ(WeightAvailabilityState::DEGRADED,
                  view->metadata.availability);
        EXPECT_EQ(residency, view->metadata.residency);
        EXPECT_EQ(operation->operation_id, view->metadata.operation_id);
        EXPECT_EQ(ready.metadata_generation + 2,
                  view->metadata.metadata_generation);
        EXPECT_EQ(view->metadata.metadata_generation,
                  published->fenced_metadata_generation);
        EXPECT_NE("completed", published->message);

        WeightMetadataStore restored;
        ASSERT_TRUE(restored.RestoreSnapshot(metadata_store.ExportSnapshot()));
        ASSERT_TRUE(restored.Get(ready.identity, 400));
        EXPECT_EQ(view->metadata, restored.Get(ready.identity, 400)->metadata);
        ASSERT_TRUE(restored.QueryOperation(operation->operation_id));
        EXPECT_EQ(*published,
                  *restored.QueryOperation(operation->operation_id));
        const auto retry = restored.PrepareUpdateOperationProgress(
            operation->operation_id, 0, operation->total_units, 0,
            operation->total_bytes, {}, {}, WeightAvailabilityState::DEGRADED,
            residency, ratio, 500);
        ASSERT_TRUE(retry.has_value());
        EXPECT_TRUE(retry->no_op);
        ASSERT_TRUE(restored.Publish(*retry));
        EXPECT_EQ(view->metadata, restored.Get(ready.identity, 500)->metadata);

        const auto recovered = restored.PrepareUpdateOperationProgress(
            operation->operation_id, 0, operation->total_units, 0,
            operation->total_bytes, {}, {}, WeightAvailabilityState::READY,
            WeightResidencyState::HOT, 1.0, 600);
        ASSERT_TRUE(recovered.has_value());
        EXPECT_FALSE(recovered->no_op);
        const auto recovered_operation = restored.Publish(*recovered);
        ASSERT_TRUE(recovered_operation.has_value());
        const auto recovered_view = restored.Get(ready.identity, 600);
        ASSERT_TRUE(recovered_view.has_value());
        EXPECT_EQ(WeightAvailabilityState::READY,
                  recovered_view->metadata.availability);
        EXPECT_EQ(view->metadata.metadata_generation + 1,
                  recovered_view->metadata.metadata_generation);
        EXPECT_EQ(recovered_view->metadata.metadata_generation,
                  recovered_operation->fenced_metadata_generation);
        EXPECT_EQ(operation->operation_id,
                  recovered_view->metadata.operation_id);
    }
}

TEST(WeightMetadataStoreTest, OperationProgressRejectsInvalidObservedState) {
    WeightMetadataStore metadata_store;
    const auto ready = PublishReady(metadata_store);
    const auto start = metadata_store.PrepareStartOperation(
        StartWeightResidencyOperationRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation,
            .target_residency = WeightResidencyState::COLD,
        },
        300);
    ASSERT_TRUE(start.has_value());
    const auto operation = metadata_store.Publish(*start);
    ASSERT_TRUE(operation.has_value());
    const auto before = metadata_store.ExportSnapshot();
    for (const auto availability :
         {WeightAvailabilityState::READY, WeightAvailabilityState::IMPORTING,
          WeightAvailabilityState::DELETED}) {
        const auto invalid = metadata_store.PrepareUpdateOperationProgress(
            operation->operation_id, 0, operation->total_units, 0,
            operation->total_bytes, {}, {}, availability,
            WeightResidencyState::ABSENT, 0.0, 400);
        ASSERT_FALSE(invalid.has_value());
        EXPECT_EQ(WeightManagementError::INVALID_ARGUMENT, invalid.error());
    }
    const auto invalid_ratio = metadata_store.PrepareUpdateOperationProgress(
        operation->operation_id, 0, operation->total_units, 0,
        operation->total_bytes, {}, {}, WeightAvailabilityState::DEGRADED,
        WeightResidencyState::ABSENT, 0.5, 400);
    ASSERT_FALSE(invalid_ratio.has_value());
    EXPECT_EQ(WeightManagementError::INVALID_ARGUMENT, invalid_ratio.error());
    const auto invalid_target = metadata_store.PrepareStartOperation(
        StartWeightResidencyOperationRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation + 1,
            .target_residency = WeightResidencyState::ABSENT,
        },
        400);
    ASSERT_FALSE(invalid_target.has_value());
    EXPECT_EQ(WeightManagementError::INVALID_ARGUMENT, invalid_target.error());
    EXPECT_EQ(before, metadata_store.ExportSnapshot());
}

TEST(WeightMetadataStoreTest, OperationTimestampsRemainMonotonic) {
    WeightMetadataStore metadata_store;
    auto ready = PublishReady(metadata_store);
    auto start = metadata_store.PrepareStartOperation(
        StartWeightResidencyOperationRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation,
            .target_residency = WeightResidencyState::COLD,
        },
        300);
    ASSERT_TRUE(start.has_value());
    auto operation = metadata_store.Publish(*start);
    ASSERT_TRUE(operation.has_value());

    auto progress = metadata_store.PrepareUpdateOperationProgress(
        operation->operation_id, 1, operation->total_units, 1024,
        operation->total_bytes, "member-1", {}, WeightAvailabilityState::READY,
        WeightResidencyState::MIXED, 0.5, 250);
    ASSERT_TRUE(progress.has_value());
    operation = metadata_store.Publish(*progress);
    ASSERT_TRUE(operation.has_value());
    EXPECT_EQ(300, operation->updated_at_ms);
    WeightMetadataStore restored;
    EXPECT_TRUE(
        restored.RestoreSnapshot(metadata_store.ExportSnapshot()).has_value());

    auto finish = metadata_store.PrepareFinishOperation(
        operation->operation_id, WeightResidencyState::COLD, 0.0, 200);
    ASSERT_TRUE(finish.has_value());
    operation = metadata_store.Publish(*finish);
    ASSERT_TRUE(operation.has_value());
    EXPECT_EQ(300, operation->updated_at_ms);
    EXPECT_TRUE(
        restored.RestoreSnapshot(metadata_store.ExportSnapshot()).has_value());
}

TEST(WeightMetadataStoreTest, OperationProgressRejectsRegression) {
    WeightMetadataStore metadata_store;
    auto ready = PublishReady(metadata_store);
    auto started = metadata_store.PrepareStartOperation(
        StartWeightResidencyOperationRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation,
            .target_residency = WeightResidencyState::COLD,
        },
        300);
    ASSERT_TRUE(started.has_value());
    auto operation = metadata_store.Publish(*started);
    ASSERT_TRUE(operation.has_value());

    auto progress = metadata_store.PrepareUpdateOperationProgress(
        operation->operation_id, 2, operation->total_units, 2048,
        operation->total_bytes, "unit-2", {}, WeightAvailabilityState::READY,
        WeightResidencyState::MIXED, 0.5, 400);
    ASSERT_TRUE(progress.has_value());
    ASSERT_TRUE(metadata_store.Publish(*progress).has_value());

    auto units_regressed = metadata_store.PrepareUpdateOperationProgress(
        operation->operation_id, 1, operation->total_units, 2048,
        operation->total_bytes, "unit-1", {}, WeightAvailabilityState::READY,
        WeightResidencyState::MIXED, 0.5, 500);
    ASSERT_FALSE(units_regressed.has_value());
    EXPECT_EQ(WeightManagementError::CONFLICT, units_regressed.error());

    auto bytes_regressed = metadata_store.PrepareUpdateOperationProgress(
        operation->operation_id, 2, operation->total_units, 1024,
        operation->total_bytes, "unit-2", {}, WeightAvailabilityState::READY,
        WeightResidencyState::MIXED, 0.5, 500);
    ASSERT_FALSE(bytes_regressed.has_value());
    EXPECT_EQ(WeightManagementError::CONFLICT, bytes_regressed.error());

    auto totals_changed = metadata_store.PrepareUpdateOperationProgress(
        operation->operation_id, 2, operation->total_units + 1, 2048,
        operation->total_bytes, "unit-2", {}, WeightAvailabilityState::READY,
        WeightResidencyState::MIXED, 0.5, 500);
    ASSERT_FALSE(totals_changed.has_value());
    EXPECT_EQ(WeightManagementError::CONFLICT, totals_changed.error());
}

TEST(WeightMetadataStoreTest,
     RecordsRetryableOperationErrorWithoutChangingAvailability) {
    WeightMetadataStore metadata_store;
    auto ready = PublishReady(metadata_store);
    auto started = metadata_store.PrepareStartOperation(
        StartWeightResidencyOperationRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation,
            .target_residency = WeightResidencyState::COLD,
        },
        300);
    ASSERT_TRUE(started.has_value());
    auto operation = metadata_store.Publish(*started);
    ASSERT_TRUE(operation.has_value());
    const auto before = metadata_store.Get(ready.identity, 400);
    ASSERT_TRUE(before.has_value());

    auto failure = metadata_store.PrepareRecordOperationError(
        operation->operation_id, "cold replica write failed", 400);
    ASSERT_TRUE(failure.has_value());
    auto published = metadata_store.Publish(*failure);
    ASSERT_TRUE(published.has_value());
    EXPECT_EQ("cold replica write failed", published->message);
    EXPECT_EQ(400, published->updated_at_ms);

    const auto after = metadata_store.Get(ready.identity, 400);
    ASSERT_TRUE(after.has_value());
    EXPECT_EQ(before->metadata, after->metadata);
    EXPECT_EQ(WeightAvailabilityState::READY, after->metadata.availability);
    EXPECT_EQ(WeightResidencyState::HOT, after->metadata.residency);

    auto retry = metadata_store.PrepareRecordOperationError(
        operation->operation_id, "cold replica write failed", 500);
    ASSERT_TRUE(retry.has_value());
    EXPECT_TRUE(retry->no_op);
    auto retried = metadata_store.Publish(*retry);
    ASSERT_TRUE(retried.has_value());
    EXPECT_EQ(400, retried->updated_at_ms);
}

TEST(WeightMetadataStoreTest, GroupProjectionPreservesTenantAndDeletingGroups) {
    WeightMetadataStore metadata_store;
    auto first_identity = Identity();
    auto second_identity = first_identity;
    second_identity.tenant_id = "tenant-b";
    const auto first =
        PublishBegin(metadata_store, BeginRequest(first_identity));
    const auto second =
        PublishBegin(metadata_store, BeginRequest(second_identity));
    auto abort = metadata_store.PrepareAbortImport(
        AbortWeightImportRequest{
            .identity = first_identity,
            .expected_metadata_generation = first.metadata_generation,
        },
        200);
    ASSERT_TRUE(abort.has_value());
    ASSERT_TRUE(metadata_store.Publish(*abort).has_value());
    EXPECT_NE(first.manifest.payload_group_id,
              second.manifest.payload_group_id);
    EXPECT_TRUE(metadata_store.IsManagedGroup(first.manifest.payload_group_id));
    EXPECT_TRUE(
        metadata_store.IsManagedGroup(second.manifest.payload_group_id));
    EXPECT_FALSE(metadata_store.AllowsGroupMemberMutation(
        first.manifest.payload_group_id));
    EXPECT_TRUE(metadata_store.AllowsGroupMemberMutation(
        second.manifest.payload_group_id));
}

TEST(WeightMetadataStoreTest, ExpiredLeaseSelectionIsRevisionScoped) {
    WeightMetadataStore metadata_store;
    const auto first = PublishReady(metadata_store);
    auto second_identity = Identity("step-101", 8);
    auto importing =
        PublishBegin(metadata_store, BeginRequest(second_identity));
    auto manifest = Manifest();
    manifest.manifest_key = MakeWeightManifestKey(second_identity);
    manifest.payload_group_id = MakeWeightPayloadGroupId(second_identity);
    auto commit = metadata_store.PrepareCommitImport(
        CommitWeightImportRequest{
            .identity = second_identity,
            .expected_metadata_generation = importing.metadata_generation,
            .manifest = manifest,
        },
        200);
    ASSERT_TRUE(commit.has_value());
    auto second = metadata_store.Publish(*commit);
    ASSERT_TRUE(second.has_value());
    for (const auto& revision : {first, *second}) {
        for (uint64_t ttl : {5, 100}) {
            auto lease = metadata_store.PrepareAcquireLease(
                AcquireWeightRevisionLeaseRequest{
                    .identity = revision.identity,
                    .expected_metadata_generation =
                        revision.metadata_generation,
                    .holder = "reader-" + std::to_string(ttl),
                    .ttl_ms = ttl,
                },
                300);
            ASSERT_TRUE(lease.has_value());
            ASSERT_TRUE(metadata_store.Publish(*lease).has_value());
        }
    }
    auto expired = metadata_store.PrepareExpireLeases(305, first.identity);
    ASSERT_EQ(1u, expired.size());
    EXPECT_EQ(first.identity, expired.front().previous->identity);
    ASSERT_TRUE(metadata_store.Publish(expired.front()).has_value());
    EXPECT_TRUE(
        metadata_store.PrepareExpireLeases(305, first.identity).empty());
    EXPECT_EQ(1u, metadata_store.PrepareExpireLeases(305).size());
    EXPECT_EQ(3u, metadata_store.ExportSnapshot().leases.size());
}

TEST(WeightMetadataStoreTest, ActiveLeaseAllowsMigrationButBlocksDelete) {
    WeightMetadataStore metadata_store;
    auto ready = PublishReady(metadata_store);
    auto lease_mutation = metadata_store.PrepareAcquireLease(
        AcquireWeightRevisionLeaseRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation,
            .holder = "worker-1",
            .ttl_ms = 100,
        },
        300);
    ASSERT_TRUE(lease_mutation.has_value());
    ASSERT_TRUE(metadata_store.Publish(*lease_mutation).has_value());

    auto operation = metadata_store.PrepareStartOperation(
        StartWeightResidencyOperationRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation,
            .target_residency = WeightResidencyState::COLD,
        },
        301);
    ASSERT_TRUE(operation.has_value());
    ASSERT_TRUE(metadata_store.Publish(*operation).has_value());

    auto deletion = metadata_store.PrepareDelete(
        DeleteWeightRevisionRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation + 1,
        },
        301);
    ASSERT_FALSE(deletion.has_value());
    EXPECT_EQ(WeightManagementError::BUSY, deletion.error());
}

TEST(WeightMetadataStoreTest, LeasePublicationFencesPreparedDeletion) {
    WeightMetadataStore metadata_store;
    auto ready = PublishReady(metadata_store);
    auto lease = metadata_store.PrepareAcquireLease(
        AcquireWeightRevisionLeaseRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation,
            .holder = "worker-1",
            .ttl_ms = 100,
        },
        300);
    auto deletion = metadata_store.PrepareDelete(
        DeleteWeightRevisionRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation,
        },
        301);
    ASSERT_TRUE(lease.has_value());
    ASSERT_TRUE(deletion.has_value());
    ASSERT_TRUE(metadata_store.Publish(*lease).has_value());

    auto published = metadata_store.Publish(*deletion);
    ASSERT_FALSE(published.has_value());
    EXPECT_EQ(WeightManagementError::BUSY, published.error());
    EXPECT_EQ(WeightAvailabilityState::READY,
              metadata_store.Get(ready.identity, 301)->metadata.availability);
}

TEST(WeightMetadataStoreTest,
     MetadataMutationFencesPreparedResidencyOperation) {
    WeightMetadataStore metadata_store;
    auto ready = PublishReady(metadata_store);
    auto operation = metadata_store.PrepareStartOperation(
        StartWeightResidencyOperationRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation,
            .target_residency = WeightResidencyState::COLD,
        },
        301);
    ASSERT_TRUE(operation.has_value());
    auto changed = metadata_store.PrepareReconcile(
        ready.identity, ready.metadata_generation,
        WeightAvailabilityState::DEGRADED, WeightResidencyState::HOT, 1.0, 302);
    ASSERT_TRUE(changed.has_value());
    ASSERT_TRUE(metadata_store.Publish(*changed).has_value());

    auto published = metadata_store.Publish(*operation);
    ASSERT_FALSE(published.has_value());
    EXPECT_EQ(WeightManagementError::STALE_GENERATION, published.error());
    EXPECT_FALSE(metadata_store.Get(ready.identity, 302)
                     ->metadata.operation_id.has_value());
}

TEST(WeightMetadataStoreTest,
     LeaseWithoutMetadataChangeAllowsPreparedMigration) {
    WeightMetadataStore metadata_store;
    auto ready = PublishReady(metadata_store);
    const auto now_ms = ready.updated_at_ms;
    auto lease = metadata_store.PrepareAcquireLease(
        AcquireWeightRevisionLeaseRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation,
            .holder = "same-timestamp-reader",
            .ttl_ms = 100,
        },
        now_ms);
    auto operation = metadata_store.PrepareStartOperation(
        StartWeightResidencyOperationRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation,
            .target_residency = WeightResidencyState::COLD,
        },
        now_ms + 1);
    ASSERT_TRUE(lease.has_value());
    ASSERT_TRUE(operation.has_value());
    ASSERT_TRUE(metadata_store.Publish(*lease).has_value());
    auto leased = metadata_store.Get(ready.identity, now_ms + 1);
    ASSERT_TRUE(leased.has_value());
    ASSERT_EQ(ready, leased->metadata);
    ASSERT_EQ(1u, leased->active_lease_count);
    auto published = metadata_store.Publish(*operation);
    ASSERT_TRUE(published.has_value());
    auto migrating = metadata_store.Get(ready.identity, now_ms + 1);
    ASSERT_TRUE(migrating.has_value());
    EXPECT_EQ(1u, migrating->active_lease_count);
    EXPECT_EQ(WeightResidencyState::HOT, migrating->metadata.residency);
    EXPECT_EQ(published->operation_id, migrating->metadata.operation_id);
    EXPECT_EQ(WeightResidencyState::COLD, published->target_residency);
}

TEST(WeightMetadataStoreTest, RejectsReadyRevisionWithoutReadableResidency) {
    WeightMetadataStore metadata_store;
    auto ready = PublishReady(metadata_store);
    auto reconcile = metadata_store.PrepareReconcile(
        ready.identity, ready.metadata_generation,
        WeightAvailabilityState::READY, WeightResidencyState::ABSENT, 0.0, 300);
    ASSERT_FALSE(reconcile.has_value());
    EXPECT_EQ(WeightManagementError::INVALID_ARGUMENT, reconcile.error());
    EXPECT_EQ(WeightResidencyState::HOT,
              metadata_store.Get(ready.identity, 300)->metadata.residency);
}

TEST(WeightMetadataStoreTest, CompletesResidencyOperationAndRetainsRecord) {
    WeightMetadataStore metadata_store;
    auto ready = PublishReady(metadata_store);
    auto start = metadata_store.PrepareStartOperation(
        StartWeightResidencyOperationRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation,
            .target_residency = WeightResidencyState::COLD,
        },
        300);
    ASSERT_TRUE(start.has_value());
    auto operation = metadata_store.Publish(*start);
    ASSERT_TRUE(operation.has_value());

    auto finish = metadata_store.PrepareFinishOperation(
        operation->operation_id, WeightResidencyState::COLD, 0.0, 400);
    ASSERT_TRUE(finish.has_value());
    auto completed = metadata_store.Publish(*finish);
    ASSERT_TRUE(completed.has_value());
    EXPECT_EQ("completed", completed->message);

    auto view = metadata_store.Get(ready.identity, 400);
    ASSERT_TRUE(view.has_value());
    EXPECT_FALSE(view->metadata.operation_id.has_value());
    EXPECT_EQ(WeightResidencyState::COLD, view->metadata.residency);
    EXPECT_EQ(ready.metadata_generation + 2,
              view->metadata.metadata_generation);
    EXPECT_EQ(*completed,
              *metadata_store.QueryOperation(completed->operation_id));
}

TEST(WeightMetadataStoreTest, OperationReplayAdvancesAllocatorWatermark) {
    WeightMetadataStore source;
    WeightMetadataStore replay_target;
    auto source_ready = PublishReady(source);
    auto target_ready = PublishReady(replay_target);
    ASSERT_EQ(source_ready, target_ready);

    auto replayed = source.PrepareStartOperation(
        StartWeightResidencyOperationRequest{
            .identity = source_ready.identity,
            .expected_metadata_generation = source_ready.metadata_generation,
            .target_residency = WeightResidencyState::COLD,
        },
        300);
    ASSERT_TRUE(replayed.has_value());
    ASSERT_TRUE(replay_target.Publish(*replayed).has_value());

    const auto snapshot = replay_target.ExportSnapshot();
    EXPECT_EQ(replayed->next->operation_id + 1, snapshot.next_operation_id);
    WeightMetadataStore restored;
    EXPECT_TRUE(restored.RestoreSnapshot(snapshot).has_value());

    auto finished = replay_target.PrepareFinishOperation(
        replayed->next->operation_id, WeightResidencyState::COLD, 0.0, 400);
    ASSERT_TRUE(finished.has_value());
    ASSERT_TRUE(replay_target.Publish(*finished).has_value());
    auto current = replay_target.Get(target_ready.identity, 400);
    ASSERT_TRUE(current.has_value());
    auto next = replay_target.PrepareStartOperation(
        StartWeightResidencyOperationRequest{
            .identity = target_ready.identity,
            .expected_metadata_generation =
                current->metadata.metadata_generation,
            .target_residency = WeightResidencyState::HOT,
        },
        500);
    ASSERT_TRUE(next.has_value());
    EXPECT_NE(replayed->next->operation_id, next->next->operation_id);
}

TEST(WeightMetadataStoreTest, OperationReplayRejectsExhaustedAllocatorId) {
    WeightMetadataStore source;
    WeightMetadataStore replay_target;
    auto ready = PublishReady(source);
    ASSERT_EQ(ready, PublishReady(replay_target));

    auto replayed = source.PrepareStartOperation(
        StartWeightResidencyOperationRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation,
            .target_residency = WeightResidencyState::COLD,
        },
        300);
    ASSERT_TRUE(replayed.has_value());
    const auto exhausted = std::numeric_limits<uint64_t>::max();
    replayed->metadata.next->operation_id = exhausted;
    replayed->next->operation_id = exhausted;

    auto published = replay_target.Publish(*replayed);
    ASSERT_FALSE(published.has_value());
    EXPECT_EQ(WeightManagementError::GENERATION_EXHAUSTED, published.error());
}

TEST(WeightMetadataStoreTest, RestoresMultipleCompletedOperations) {
    WeightMetadataStore metadata_store;
    auto metadata = PublishReady(metadata_store);
    for (const auto target :
         {WeightResidencyState::COLD, WeightResidencyState::HOT}) {
        auto start = metadata_store.PrepareStartOperation(
            StartWeightResidencyOperationRequest{
                .identity = metadata.identity,
                .expected_metadata_generation = metadata.metadata_generation,
                .target_residency = target,
            },
            300 + metadata.metadata_generation);
        ASSERT_TRUE(start.has_value());
        auto operation = metadata_store.Publish(*start);
        ASSERT_TRUE(operation.has_value());
        auto finish = metadata_store.PrepareFinishOperation(
            operation->operation_id, target,
            target == WeightResidencyState::HOT ? 1.0 : 0.0,
            400 + metadata.metadata_generation);
        ASSERT_TRUE(finish.has_value());
        ASSERT_TRUE(metadata_store.Publish(*finish).has_value());
        auto view = metadata_store.Get(metadata.identity, 500);
        ASSERT_TRUE(view.has_value());
        metadata = view->metadata;
    }

    WeightMetadataStore restored;
    ASSERT_TRUE(
        restored.RestoreSnapshot(metadata_store.ExportSnapshot()).has_value());
    EXPECT_EQ(metadata, restored.Get(metadata.identity, 500)->metadata);
    EXPECT_EQ("completed", restored.QueryOperation(1)->message);
    EXPECT_EQ("completed", restored.QueryOperation(2)->message);
}

TEST(WeightMetadataStoreTest, RejectsUnknownSnapshotEnums) {
    WeightMetadataStore metadata_store;
    auto ready = PublishReady(metadata_store);
    auto snapshot = metadata_store.ExportSnapshot();

    snapshot.metadata[0].availability =
        static_cast<WeightAvailabilityState>(255);
    WeightMetadataStore restored;
    EXPECT_EQ(WeightManagementError::INVALID_ARGUMENT,
              restored.RestoreSnapshot(snapshot).error());

    auto start = metadata_store.PrepareStartOperation(
        StartWeightResidencyOperationRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation,
            .target_residency = WeightResidencyState::COLD,
        },
        300);
    ASSERT_TRUE(start.has_value());
    ASSERT_TRUE(metadata_store.Publish(*start).has_value());
    snapshot = metadata_store.ExportSnapshot();
    snapshot.operations[0].kind = static_cast<WeightOperationKind>(255);
    EXPECT_EQ(WeightManagementError::INVALID_ARGUMENT,
              restored.RestoreSnapshot(snapshot).error());

    snapshot = metadata_store.ExportSnapshot();
    snapshot.operations[0].target_residency = WeightResidencyState::MIXED;
    EXPECT_EQ(WeightManagementError::INVALID_ARGUMENT,
              restored.RestoreSnapshot(snapshot).error());
}

TEST(WeightMetadataStoreTest, ValidatesSnapshotsWithoutReplacingExistingState) {
    WeightMetadataStore metadata_store;
    PublishReady(metadata_store);
    const auto baseline = metadata_store.ExportSnapshot();
    EXPECT_TRUE(ValidateWeightMetadataSnapshot(baseline).has_value());

    auto invalid = baseline;
    invalid.metadata.push_back(invalid.metadata.front());
    EXPECT_FALSE(ValidateWeightMetadataSnapshot(invalid).has_value());
    EXPECT_FALSE(metadata_store.RestoreSnapshot(invalid).has_value());
    EXPECT_EQ(baseline, metadata_store.ExportSnapshot());

    invalid = baseline;
    invalid.next_lease_id = 0;
    EXPECT_FALSE(ValidateWeightMetadataSnapshot(invalid).has_value());
    EXPECT_FALSE(metadata_store.RestoreSnapshot(invalid).has_value());
    EXPECT_EQ(baseline, metadata_store.ExportSnapshot());
}

TEST(WeightMetadataStoreTest, RejectsSnapshotWithLeaseOnDeletedRevision) {
    WeightMetadataStore metadata_store;
    auto ready = PublishReady(metadata_store);
    auto lease = metadata_store.PrepareAcquireLease(
        AcquireWeightRevisionLeaseRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation,
            .holder = "worker-1",
            .ttl_ms = 100,
        },
        300);
    ASSERT_TRUE(lease.has_value());
    ASSERT_TRUE(metadata_store.Publish(*lease).has_value());
    auto snapshot = metadata_store.ExportSnapshot();
    ASSERT_EQ(1, snapshot.metadata.size());
    snapshot.metadata[0].availability = WeightAvailabilityState::DELETED;
    snapshot.metadata[0].residency = WeightResidencyState::ABSENT;
    snapshot.metadata[0].observed_hot_ratio = 0.0;

    WeightMetadataStore restored;
    auto result = restored.RestoreSnapshot(snapshot);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(WeightManagementError::INVALID_ARGUMENT, result.error());
}

TEST(WeightMetadataStoreTest, RejectsSnapshotWithMismatchedOperationTotals) {
    WeightMetadataStore metadata_store;
    auto ready = PublishReady(metadata_store);
    auto started = metadata_store.PrepareStartOperation(
        StartWeightResidencyOperationRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation,
            .target_residency = WeightResidencyState::COLD,
        },
        300);
    ASSERT_TRUE(started.has_value());
    ASSERT_TRUE(metadata_store.Publish(*started).has_value());
    auto snapshot = metadata_store.ExportSnapshot();
    ASSERT_EQ(1, snapshot.operations.size());
    ++snapshot.operations[0].total_units;

    WeightMetadataStore restored;
    auto result = restored.RestoreSnapshot(snapshot);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(WeightManagementError::INVALID_ARGUMENT, result.error());
}

TEST(WeightMetadataStoreTest, DeleteRetainsAbsentTombstone) {
    WeightMetadataStore metadata_store;
    auto ready = PublishReady(metadata_store);
    auto start = metadata_store.PrepareDelete(
        DeleteWeightRevisionRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation,
        },
        300);
    ASSERT_TRUE(start.has_value());
    auto deleting = metadata_store.Publish(*start);
    ASSERT_TRUE(deleting.has_value());
    EXPECT_EQ(WeightAvailabilityState::DELETING, deleting->availability);

    auto finish = metadata_store.PrepareFinishDelete(
        ready.identity, deleting->metadata_generation, 400);
    ASSERT_TRUE(finish.has_value());
    auto deleted = metadata_store.Publish(*finish);
    ASSERT_TRUE(deleted.has_value());
    EXPECT_EQ(WeightAvailabilityState::DELETED, deleted->availability);
    EXPECT_EQ(WeightResidencyState::ABSENT, deleted->residency);
    EXPECT_TRUE(
        metadata_store.IsManagedGroup(deleted->manifest.payload_group_id));
}

TEST(WeightMetadataStoreTest, DeletedTombstoneRetryRequiresGenerationFence) {
    WeightMetadataStore metadata_store;
    auto ready = PublishReady(metadata_store);
    auto start = metadata_store.PrepareDelete(
        DeleteWeightRevisionRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation,
        },
        300);
    ASSERT_TRUE(start.has_value());
    auto deleting = metadata_store.Publish(*start);
    ASSERT_TRUE(deleting.has_value());
    auto finish = metadata_store.PrepareFinishDelete(
        ready.identity, deleting->metadata_generation, 400);
    ASSERT_TRUE(finish.has_value());
    auto deleted = metadata_store.Publish(*finish);
    ASSERT_TRUE(deleted.has_value());

    auto current = metadata_store.PrepareDelete(
        DeleteWeightRevisionRequest{
            .identity = ready.identity,
            .expected_metadata_generation = deleted->metadata_generation,
        },
        500);
    ASSERT_TRUE(current.has_value());
    EXPECT_TRUE(current->no_op);

    auto adjacent = metadata_store.PrepareDelete(
        DeleteWeightRevisionRequest{
            .identity = ready.identity,
            .expected_metadata_generation = deleted->metadata_generation - 1,
        },
        500);
    ASSERT_TRUE(adjacent.has_value());
    EXPECT_TRUE(adjacent->no_op);

    auto stale = metadata_store.PrepareDelete(
        DeleteWeightRevisionRequest{
            .identity = ready.identity,
            .expected_metadata_generation = deleted->metadata_generation - 2,
        },
        500);
    ASSERT_FALSE(stale.has_value());
    EXPECT_EQ(WeightManagementError::STALE_GENERATION, stale.error());
}

TEST(WeightMetadataStoreTest, OnlyOneConcurrentCasCandidatePublishes) {
    WeightMetadataStore metadata_store;
    auto importing = PublishBegin(metadata_store, BeginRequest());
    auto request = CommitWeightImportRequest{
        .identity = importing.identity,
        .expected_metadata_generation = importing.metadata_generation,
        .manifest = Manifest(),
    };
    auto first = metadata_store.PrepareCommitImport(request, 200);
    auto second = metadata_store.PrepareCommitImport(request, 201);
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());

    std::atomic<int> successes{0};
    std::vector<std::thread> threads;
    threads.emplace_back([&] {
        if (metadata_store.Publish(*first).has_value()) {
            ++successes;
        }
    });
    threads.emplace_back([&] {
        if (metadata_store.Publish(*second).has_value()) {
            ++successes;
        }
    });
    for (auto& thread : threads) {
        thread.join();
    }
    EXPECT_EQ(1, successes.load());
}

}  // namespace
}  // namespace mooncake

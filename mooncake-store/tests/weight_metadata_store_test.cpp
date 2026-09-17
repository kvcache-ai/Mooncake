#include <gtest/gtest.h>

#include <atomic>
#include <limits>
#include <thread>
#include <vector>

#include "weight_metadata_store.h"

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
    auto rejected =
        metadata_store.PrepareBeginImport(noncanonical_begin, 100);
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(WeightManagementError::INVALID_ARGUMENT, rejected.error());

    auto importing = PublishBegin(metadata_store, BeginRequest());
    auto noncanonical_manifest = Manifest();
    noncanonical_manifest.manifest_key =
        "valid-but-non-canonical-manifest";
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

TEST(WeightMetadataStoreTest, LeaseReplayAdvancesAllocatorWatermark) {
    WeightMetadataStore source;
    WeightMetadataStore replay_target;
    auto source_ready = PublishReady(source);
    auto target_ready = PublishReady(replay_target);
    ASSERT_EQ(source_ready, target_ready);

    auto replayed = source.PrepareAcquireLease(
        AcquireWeightRevisionLeaseRequest{
            .identity = source_ready.identity,
            .expected_metadata_generation =
                source_ready.metadata_generation,
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
            .expected_metadata_generation =
                target_ready.metadata_generation,
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
    EXPECT_EQ(WeightManagementError::GENERATION_EXHAUSTED,
              published.error());
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
    EXPECT_EQ(WeightOperationState::EVICTING, operation->operation);

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
        operation->operation_id, 0, 4, {}, 400);
    ASSERT_TRUE(progress.has_value());
    EXPECT_FALSE(progress->no_op);
    auto published = metadata_store.Publish(*progress);
    ASSERT_TRUE(published.has_value());
    EXPECT_EQ(400, published->updated_at_ms);

    auto retry = metadata_store.PrepareUpdateOperationProgress(
        operation->operation_id, 0, 4, {}, 500);
    ASSERT_TRUE(retry.has_value());
    EXPECT_TRUE(retry->no_op);
    auto retried = metadata_store.Publish(*retry);
    ASSERT_TRUE(retried.has_value());
    EXPECT_EQ(400, retried->updated_at_ms);
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
        operation->operation_id, 1, 2, "member-1", 250);
    ASSERT_TRUE(progress.has_value());
    operation = metadata_store.Publish(*progress);
    ASSERT_TRUE(operation.has_value());
    EXPECT_EQ(300, operation->updated_at_ms);
    WeightMetadataStore restored;
    EXPECT_TRUE(
        restored.RestoreSnapshot(metadata_store.ExportSnapshot()).has_value());

    auto finish = metadata_store.PrepareFinishOperation(
        operation->operation_id, WeightResidencyState::COLD, 200);
    ASSERT_TRUE(finish.has_value());
    operation = metadata_store.Publish(*finish);
    ASSERT_TRUE(operation.has_value());
    EXPECT_EQ(300, operation->updated_at_ms);
    EXPECT_TRUE(
        restored.RestoreSnapshot(metadata_store.ExportSnapshot()).has_value());
}

TEST(WeightMetadataStoreTest, ActiveLeaseBlocksResidencyAndDelete) {
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
    ASSERT_FALSE(operation.has_value());
    EXPECT_EQ(WeightManagementError::BUSY, operation.error());

    auto deletion = metadata_store.PrepareDelete(
        DeleteWeightRevisionRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation,
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
     LeasePublicationFencesPreparedResidencyOperation) {
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
    auto operation = metadata_store.PrepareStartOperation(
        StartWeightResidencyOperationRequest{
            .identity = ready.identity,
            .expected_metadata_generation = ready.metadata_generation,
            .target_residency = WeightResidencyState::COLD,
        },
        301);
    ASSERT_TRUE(lease.has_value());
    ASSERT_TRUE(operation.has_value());
    ASSERT_TRUE(metadata_store.Publish(*lease).has_value());

    auto published = metadata_store.Publish(*operation);
    ASSERT_FALSE(published.has_value());
    EXPECT_EQ(WeightManagementError::BUSY, published.error());
    EXPECT_EQ(WeightOperationState::NONE,
              metadata_store.Get(ready.identity, 301)->metadata.operation);
}

TEST(WeightMetadataStoreTest, RejectsReadyRevisionWithoutReadableResidency) {
    WeightMetadataStore metadata_store;
    auto ready = PublishReady(metadata_store);
    auto reconcile = metadata_store.PrepareReconcile(
        ready.identity, ready.metadata_generation,
        WeightAvailabilityState::READY, WeightResidencyState::ABSENT, 300);
    ASSERT_TRUE(reconcile.has_value());

    auto published = metadata_store.Publish(*reconcile);
    ASSERT_FALSE(published.has_value());
    EXPECT_EQ(WeightManagementError::INVALID_ARGUMENT, published.error());
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
        operation->operation_id, WeightResidencyState::COLD, 400);
    ASSERT_TRUE(finish.has_value());
    auto completed = metadata_store.Publish(*finish);
    ASSERT_TRUE(completed.has_value());
    EXPECT_EQ("completed", completed->message);

    auto view = metadata_store.Get(ready.identity, 400);
    ASSERT_TRUE(view.has_value());
    EXPECT_EQ(WeightOperationState::NONE, view->metadata.operation);
    EXPECT_EQ(0, view->metadata.operation_id);
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
            .expected_metadata_generation =
                source_ready.metadata_generation,
            .target_residency = WeightResidencyState::COLD,
        },
        300);
    ASSERT_TRUE(replayed.has_value());
    ASSERT_TRUE(replay_target.Publish(*replayed).has_value());

    const auto snapshot = replay_target.ExportSnapshot();
    EXPECT_EQ(replayed->next->operation_id + 1,
              snapshot.next_operation_id);
    WeightMetadataStore restored;
    EXPECT_TRUE(restored.RestoreSnapshot(snapshot).has_value());

    auto finished = replay_target.PrepareFinishOperation(
        replayed->next->operation_id, WeightResidencyState::COLD, 400);
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
    EXPECT_EQ(WeightManagementError::GENERATION_EXHAUSTED,
              published.error());
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
    snapshot.operations[0].operation = static_cast<WeightOperationState>(255);
    EXPECT_EQ(WeightManagementError::INVALID_ARGUMENT,
              restored.RestoreSnapshot(snapshot).error());

    snapshot = metadata_store.ExportSnapshot();
    snapshot.operations[0].target_residency = WeightResidencyState::HOT;
    EXPECT_EQ(WeightManagementError::INVALID_ARGUMENT,
              restored.RestoreSnapshot(snapshot).error());
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

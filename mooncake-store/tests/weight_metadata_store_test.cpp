#include "weight_metadata_store.h"

#include <gtest/gtest.h>

#include "types.h"

namespace mooncake::test {

class WeightMetadataStoreTest : public ::testing::Test {
   protected:
    WeightRevisionIdentity MakeIdentity(const std::string& resource = "model-a",
                                        const std::string& revision = "v1",
                                        uint64_t generation = 1) {
        WeightRevisionIdentity identity;
        identity.tenant_id = "default";
        identity.ns = "ns";
        identity.resource_id = resource;
        identity.revision = revision;
        identity.weight_generation = generation;
        return identity;
    }

    WeightStoragePolicy MakePolicy() {
        WeightStoragePolicy policy;
        policy.preferred_residency = WeightResidencyState::HOT;
        policy.mixed_hot_ratio = 0.5;
        policy.migration_mode = WeightMigrationMode::MANUAL;
        return policy;
    }

    WeightMetadataStore store_;
};

TEST_F(WeightMetadataStoreTest, BeginCommitGetListUpdateHappyPath) {
    const auto identity = MakeIdentity();
    BeginWeightImportRequest begin_req;
    begin_req.identity = identity;
    begin_req.policy = MakePolicy();
    begin_req.payload_group_id = "group-1";

    auto begin = store_.BeginImport(begin_req);
    ASSERT_TRUE(begin.has_value());
    EXPECT_EQ(begin->availability, WeightAvailabilityState::IMPORTING);
    EXPECT_EQ(begin->observed_residency, WeightResidencyState::UNKNOWN);
    EXPECT_EQ(begin->metadata_generation, 1u);

    // IMPORTING is invisible to Get/List.
    EXPECT_EQ(store_.Get(identity).error(), ErrorCode::WEIGHT_NOT_FOUND);
    EXPECT_TRUE(
        store_.List(ListWeightRevisionsRequest{"default", "ns", "", 0, 10})
            .revisions.empty());

    CommitWeightImportRequest commit_req;
    commit_req.identity = identity;
    commit_req.expected_metadata_generation = 1;
    commit_req.manifest_key = "weights/ns/model-a/v1/1/manifest";
    commit_req.manifest_sha256 = "abc";
    commit_req.payload_keys_digest = "digest";
    commit_req.payload_count = 2;
    commit_req.logical_payload_bytes = 1024;
    commit_req.payload_keys = {"payload-0", "payload-1"};

    auto commit = store_.CommitImport(commit_req);
    ASSERT_TRUE(commit.has_value());
    EXPECT_EQ(commit->availability, WeightAvailabilityState::READY);
    EXPECT_EQ(commit->observed_residency, WeightResidencyState::HOT);
    EXPECT_EQ(commit->metadata_generation, 2u);
    EXPECT_EQ(commit->payload_keys, commit_req.payload_keys);

    auto got = store_.Get(identity);
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(got->manifest_key, commit_req.manifest_key);
    EXPECT_EQ(got->payload_keys, commit_req.payload_keys);

    auto listed = store_.List(
        ListWeightRevisionsRequest{"default", "ns", "model-a", 0, 10});
    ASSERT_EQ(listed.revisions.size(), 1u);
    EXPECT_FALSE(listed.has_more);

    UpdateWeightPolicyRequest update_req;
    update_req.identity = identity;
    update_req.expected_metadata_generation = 2;
    update_req.policy = MakePolicy();
    update_req.policy.preferred_residency = WeightResidencyState::COLD;
    auto updated = store_.UpdatePolicy(update_req);
    ASSERT_TRUE(updated.has_value());
    EXPECT_EQ(updated->policy.preferred_residency, WeightResidencyState::COLD);
    EXPECT_EQ(updated->metadata_generation, 3u);
}

TEST_F(WeightMetadataStoreTest, StaleGenerationRejected) {
    const auto identity = MakeIdentity();
    BeginWeightImportRequest begin_req;
    begin_req.identity = identity;
    begin_req.policy = MakePolicy();
    begin_req.payload_group_id = "group-1";
    ASSERT_TRUE(store_.BeginImport(begin_req).has_value());

    CommitWeightImportRequest commit_req;
    commit_req.identity = identity;
    commit_req.expected_metadata_generation = 1;
    commit_req.manifest_key = "manifest";
    commit_req.payload_keys_digest = "d";
    commit_req.payload_count = 1;
    commit_req.payload_keys = {"p0"};
    ASSERT_TRUE(store_.CommitImport(commit_req).has_value());

    UpdateWeightPolicyRequest update_req;
    update_req.identity = identity;
    update_req.expected_metadata_generation = 1;  // stale
    update_req.policy = MakePolicy();
    auto updated = store_.UpdatePolicy(update_req);
    ASSERT_FALSE(updated.has_value());
    EXPECT_EQ(updated.error(), ErrorCode::WEIGHT_STALE_GENERATION);
}

TEST_F(WeightMetadataStoreTest, DuplicateReadyImportConflicts) {
    const auto identity = MakeIdentity();
    BeginWeightImportRequest begin_req;
    begin_req.identity = identity;
    begin_req.policy = MakePolicy();
    begin_req.payload_group_id = "group-1";
    ASSERT_TRUE(store_.BeginImport(begin_req).has_value());

    CommitWeightImportRequest commit_req;
    commit_req.identity = identity;
    commit_req.expected_metadata_generation = 1;
    commit_req.manifest_key = "manifest";
    commit_req.payload_keys_digest = "d";
    commit_req.payload_count = 1;
    commit_req.payload_keys = {"p0"};
    ASSERT_TRUE(store_.CommitImport(commit_req).has_value());

    auto again = store_.BeginImport(begin_req);
    ASSERT_FALSE(again.has_value());
    EXPECT_EQ(again.error(), ErrorCode::OBJECT_ALREADY_EXISTS);
}

TEST_F(WeightMetadataStoreTest, IdempotentBeginWhileImporting) {
    const auto identity = MakeIdentity();
    BeginWeightImportRequest begin_req;
    begin_req.identity = identity;
    begin_req.policy = MakePolicy();
    begin_req.payload_group_id = "group-1";
    ASSERT_TRUE(store_.BeginImport(begin_req).has_value());
    auto again = store_.BeginImport(begin_req);
    ASSERT_TRUE(again.has_value());
    EXPECT_EQ(again->availability, WeightAvailabilityState::IMPORTING);
}

TEST_F(WeightMetadataStoreTest, AbortImportingCleansUpAndAllowsRetry) {
    const auto identity = MakeIdentity("model-abort");
    BeginWeightImportRequest begin_req;
    begin_req.identity = identity;
    begin_req.policy = MakePolicy();
    begin_req.payload_group_id = "group-abort";
    ASSERT_TRUE(store_.BeginImport(begin_req).has_value());

    AbortWeightImportRequest abort_req;
    abort_req.identity = identity;
    abort_req.expected_metadata_generation = 1;
    abort_req.keys_to_remove = {"partial-0", "partial-1", "partial-0"};
    auto aborted = store_.AbortImport(abort_req);
    ASSERT_TRUE(aborted.has_value());
    EXPECT_EQ(aborted->first.availability, WeightAvailabilityState::DELETED);
    EXPECT_EQ(aborted->first.metadata_generation, 2u);
    ASSERT_EQ(aborted->second.size(), 2u);

    auto raw = store_.GetRawForTesting(identity);
    ASSERT_TRUE(raw.has_value());
    EXPECT_EQ(raw->availability, WeightAvailabilityState::DELETED);

    // Fresh import is allowed on the DELETED tombstone.
    auto again = store_.BeginImport(begin_req);
    ASSERT_TRUE(again.has_value());
    EXPECT_EQ(again->availability, WeightAvailabilityState::IMPORTING);
    EXPECT_EQ(again->metadata_generation, 3u);
}

TEST_F(WeightMetadataStoreTest, RemoveReadyReturnsStoredPayloadKeys) {
    const auto identity = MakeIdentity("model-remove");
    BeginWeightImportRequest begin_req;
    begin_req.identity = identity;
    begin_req.policy = MakePolicy();
    begin_req.payload_group_id = "group-remove";
    ASSERT_TRUE(store_.BeginImport(begin_req).has_value());

    CommitWeightImportRequest commit_req;
    commit_req.identity = identity;
    commit_req.expected_metadata_generation = 1;
    commit_req.manifest_key = "manifest";
    commit_req.payload_keys_digest = "d";
    commit_req.payload_count = 2;
    commit_req.payload_keys = {"p0", "p1"};
    ASSERT_TRUE(store_.CommitImport(commit_req).has_value());

    RemoveWeightRevisionRequest remove_req;
    remove_req.identity = identity;
    remove_req.expected_metadata_generation = 2;
    auto removed = store_.RemoveRevision(remove_req);
    ASSERT_TRUE(removed.has_value());
    EXPECT_EQ(removed->first.availability, WeightAvailabilityState::DELETED);
    EXPECT_EQ(removed->second,
              (std::vector<std::string>{"p0", "p1", "manifest"}));
    EXPECT_EQ(store_.Get(identity).error(), ErrorCode::WEIGHT_NOT_FOUND);
}

}  // namespace mooncake::test

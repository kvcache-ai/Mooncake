#include "master_service.h"
#include "weight_management.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "replica.h"
#include "types.h"

namespace mooncake::test {

class MasterServiceWeightImportTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("MasterServiceWeightImportTest");
        FLAGS_logtostderr = true;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }

    static constexpr size_t kSegmentBase = 0x300000000;
    static constexpr size_t kSegmentSize = 16 * 1024 * 1024;

    Segment MakeSegment(std::string name = "weight_test_segment") const {
        Segment segment;
        segment.id = generate_uuid();
        segment.name = std::move(name);
        segment.base = kSegmentBase;
        segment.size = kSegmentSize;
        segment.te_endpoint = segment.name;
        return segment;
    }

    UUID MountSegment(MasterService& service, const Segment& segment) {
        UUID client_id = generate_uuid();
        auto mount = service.MountSegment(segment, client_id);
        EXPECT_TRUE(mount.has_value()) << toString(mount.error());
        return client_id;
    }

    void PutCompletedObject(MasterService& service, const UUID& client_id,
                            const std::string& key,
                            const std::string& segment_name,
                            ObjectDataType data_type,
                            const std::string& group_id,
                            uint64_t slice_length = 1024) {
        ReplicateConfig config;
        config.replica_num = 1;
        config.preferred_segment = segment_name;
        config.data_type = data_type;
        config.with_hard_pin = true;
        config.group_ids = std::vector<std::string>{group_id};

        auto put_start = service.PutStart(client_id, key, TenantId::Default(),
                                          slice_length, config);
        ASSERT_TRUE(put_start.has_value()) << toString(put_start.error());
        auto put_end = service.PutEnd(client_id, key, TenantId::Default(),
                                      ReplicaType::MEMORY);
        ASSERT_TRUE(put_end.has_value()) << toString(put_end.error());
    }

    WeightRevisionIdentity MakeIdentity() const {
        WeightRevisionIdentity identity;
        identity.tenant_id = "default";
        identity.ns = "demo";
        identity.resource_id = "model-a";
        identity.revision = "v1";
        identity.weight_generation = 1;
        return identity;
    }
};

TEST_F(MasterServiceWeightImportTest, BeginCommitGetListUpdate) {
    MasterServiceConfig config;
    MasterService service(config);

    const Segment segment = MakeSegment();
    const UUID client_id = MountSegment(service, segment);

    const std::string group_id = "weight-group-1";
    const std::string payload0 = "weights/demo/model-a/v1/1/payload-0";
    const std::string payload1 = "weights/demo/model-a/v1/1/payload-1";
    const std::string manifest = "weights/demo/model-a/v1/1/manifest";

    PutCompletedObject(service, client_id, payload0, segment.name,
                       ObjectDataType::WEIGHT, group_id);
    PutCompletedObject(service, client_id, payload1, segment.name,
                       ObjectDataType::WEIGHT, group_id);
    PutCompletedObject(service, client_id, manifest, segment.name,
                       ObjectDataType::METADATA, group_id);

    const auto identity = MakeIdentity();
    BeginWeightImportRequest begin_req;
    begin_req.identity = identity;
    begin_req.payload_group_id = group_id;
    begin_req.policy.preferred_residency = WeightResidencyState::HOT;
    begin_req.policy.mixed_hot_ratio = 0.5;
    begin_req.policy.migration_mode = WeightMigrationMode::MANUAL;

    auto begin = service.BeginWeightImport(begin_req);
    ASSERT_TRUE(begin.has_value()) << toString(begin.error());
    EXPECT_EQ(begin->metadata.availability, WeightAvailabilityState::IMPORTING);

    // IMPORTING revisions are not visible via Get/List.
    auto missing =
        service.GetWeightMetadata(GetWeightMetadataRequest{identity});
    ASSERT_FALSE(missing.has_value());
    EXPECT_EQ(missing.error(), ErrorCode::WEIGHT_NOT_FOUND);

    CommitWeightImportRequest commit_req;
    commit_req.identity = identity;
    commit_req.expected_metadata_generation = 1;
    commit_req.manifest_key = manifest;
    commit_req.manifest_sha256 = "sha256-demo";
    commit_req.payload_keys_digest = "digest-demo";
    commit_req.payload_count = 2;
    commit_req.logical_payload_bytes = 2048;
    commit_req.payload_keys = {payload0, payload1};

    auto commit = service.CommitWeightImport(commit_req);
    ASSERT_TRUE(commit.has_value()) << toString(commit.error());
    EXPECT_EQ(commit->metadata.availability, WeightAvailabilityState::READY);
    EXPECT_EQ(commit->metadata.observed_residency, WeightResidencyState::HOT);
    EXPECT_EQ(commit->metadata.metadata_generation, 2u);

    auto got = service.GetWeightMetadata(GetWeightMetadataRequest{identity});
    ASSERT_TRUE(got.has_value()) << toString(got.error());
    EXPECT_EQ(got->metadata.manifest_key, manifest);

    auto listed = service.ListWeightRevisions(
        ListWeightRevisionsRequest{"default", "demo", "model-a", 0, 10});
    ASSERT_EQ(listed.revisions.size(), 1u);
    EXPECT_EQ(listed.revisions[0].identity.resource_id, "model-a");

    UpdateWeightPolicyRequest update_req;
    update_req.identity = identity;
    update_req.expected_metadata_generation = 2;
    update_req.policy = begin_req.policy;
    update_req.policy.preferred_residency = WeightResidencyState::COLD;
    auto updated = service.UpdateWeightPolicy(update_req);
    ASSERT_TRUE(updated.has_value()) << toString(updated.error());
    EXPECT_EQ(updated->metadata.policy.preferred_residency,
              WeightResidencyState::COLD);
    EXPECT_EQ(updated->metadata.metadata_generation, 3u);
}

TEST_F(MasterServiceWeightImportTest, CommitFailsWhenPayloadMissing) {
    MasterServiceConfig config;
    MasterService service(config);

    const Segment segment = MakeSegment("weight_missing_segment");
    const UUID client_id = MountSegment(service, segment);
    const std::string group_id = "weight-group-missing";
    const std::string manifest = "weights/demo/model-b/v1/1/manifest";

    PutCompletedObject(service, client_id, manifest, segment.name,
                       ObjectDataType::METADATA, group_id);

    const auto identity = MakeIdentity();
    // Use a distinct resource so it does not collide with other tests
    // in-process.
    WeightRevisionIdentity id = identity;
    id.resource_id = "model-b";

    BeginWeightImportRequest begin_req;
    begin_req.identity = id;
    begin_req.payload_group_id = group_id;
    begin_req.policy.preferred_residency = WeightResidencyState::HOT;
    begin_req.policy.mixed_hot_ratio = 0.5;
    begin_req.policy.migration_mode = WeightMigrationMode::MANUAL;
    ASSERT_TRUE(service.BeginWeightImport(begin_req).has_value());

    CommitWeightImportRequest commit_req;
    commit_req.identity = id;
    commit_req.expected_metadata_generation = 1;
    commit_req.manifest_key = manifest;
    commit_req.manifest_sha256 = "sha";
    commit_req.payload_keys_digest = "digest";
    commit_req.payload_count = 1;
    commit_req.logical_payload_bytes = 512;
    commit_req.payload_keys = {"weights/demo/model-b/v1/1/missing-payload"};

    auto commit = service.CommitWeightImport(commit_req);
    ASSERT_FALSE(commit.has_value());
    EXPECT_EQ(commit.error(), ErrorCode::OBJECT_NOT_FOUND);

    // Still not visible because commit never published READY.
    auto got = service.GetWeightMetadata(GetWeightMetadataRequest{id});
    ASSERT_FALSE(got.has_value());
    EXPECT_EQ(got.error(), ErrorCode::WEIGHT_NOT_FOUND);

    // Failed transfer path: abort IMPORTING and delete partial objects.
    const std::string partial = "weights/demo/model-b/v1/1/partial-payload";
    PutCompletedObject(service, client_id, partial, segment.name,
                       ObjectDataType::WEIGHT, group_id);

    AbortWeightImportRequest abort_req;
    abort_req.identity = id;
    abort_req.expected_metadata_generation = 1;
    abort_req.keys_to_remove = {partial, manifest};
    auto aborted = service.AbortWeightImport(abort_req);
    ASSERT_TRUE(aborted.has_value()) << toString(aborted.error());
    EXPECT_EQ(aborted->metadata.availability, WeightAvailabilityState::DELETED);
    EXPECT_FALSE(aborted->removed_keys.empty());

    auto partial_exists = service.ExistKey(partial, TenantId::Default());
    ASSERT_TRUE(partial_exists.has_value());
    EXPECT_FALSE(*partial_exists);
}

TEST_F(MasterServiceWeightImportTest, RemovePublishedRevisionDeletesPayload) {
    MasterServiceConfig config;
    MasterService service(config);

    const Segment segment = MakeSegment("weight_remove_segment");
    const UUID client_id = MountSegment(service, segment);

    const std::string group_id = "weight-group-remove";
    const std::string payload0 = "weights/demo/model-c/v1/1/payload-0";
    const std::string manifest = "weights/demo/model-c/v1/1/manifest";

    PutCompletedObject(service, client_id, payload0, segment.name,
                       ObjectDataType::WEIGHT, group_id);
    PutCompletedObject(service, client_id, manifest, segment.name,
                       ObjectDataType::METADATA, group_id);

    WeightRevisionIdentity id = MakeIdentity();
    id.resource_id = "model-c";

    BeginWeightImportRequest begin_req;
    begin_req.identity = id;
    begin_req.payload_group_id = group_id;
    begin_req.policy.preferred_residency = WeightResidencyState::HOT;
    begin_req.policy.mixed_hot_ratio = 0.5;
    begin_req.policy.migration_mode = WeightMigrationMode::MANUAL;
    ASSERT_TRUE(service.BeginWeightImport(begin_req).has_value());

    CommitWeightImportRequest commit_req;
    commit_req.identity = id;
    commit_req.expected_metadata_generation = 1;
    commit_req.manifest_key = manifest;
    commit_req.manifest_sha256 = "sha";
    commit_req.payload_keys_digest = "digest";
    commit_req.payload_count = 1;
    commit_req.logical_payload_bytes = 1024;
    commit_req.payload_keys = {payload0};
    ASSERT_TRUE(service.CommitWeightImport(commit_req).has_value());

    RemoveWeightRevisionRequest remove_req;
    remove_req.identity = id;
    remove_req.expected_metadata_generation = 2;
    auto removed = service.RemoveWeightRevision(remove_req);
    ASSERT_TRUE(removed.has_value()) << toString(removed.error());
    EXPECT_EQ(removed->metadata.availability, WeightAvailabilityState::DELETED);
    EXPECT_EQ(removed->removed_keys.size(), 2u);

    auto got = service.GetWeightMetadata(GetWeightMetadataRequest{id});
    ASSERT_FALSE(got.has_value());
    EXPECT_EQ(got.error(), ErrorCode::WEIGHT_NOT_FOUND);

    auto payload_exists = service.ExistKey(payload0, TenantId::Default());
    ASSERT_TRUE(payload_exists.has_value());
    EXPECT_FALSE(*payload_exists);
}

}  // namespace mooncake::test

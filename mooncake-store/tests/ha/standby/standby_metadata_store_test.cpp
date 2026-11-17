#include "ha/standby_metadata_store.h"

#include <gtest/gtest.h>
#include <limits>

#include "ha/oplog/oplog_applier.h"
#include "ha/standby_controller.h"

#include <set>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace mooncake::test {
namespace {

StandbyObjectMetadata MakeMetadata(uint64_t size) {
    StandbyObjectMetadata metadata;
    metadata.size = size;
    return metadata;
}

}  // namespace

static_assert(
    std::is_move_constructible_v<StandbyMetadataStore::SnapshotCursor>);
static_assert(
    !std::is_copy_constructible_v<StandbyMetadataStore::SnapshotCursor>);

TEST(StandbyMetadataStoreTest, EmptySnapshotCompletesWithoutObjects) {
    StandbyMetadataStore store;
    auto cursor = store.BeginSnapshotTraversal();
    std::vector<StandbyObjectEntry> chunk;

    ASSERT_TRUE(store.CopyNextSnapshotChunk(2, cursor, chunk));
    EXPECT_TRUE(chunk.empty());
    EXPECT_TRUE(cursor.done());
}

TEST(StandbyMetadataStoreTest, CopiesMultipleTenantsAcrossBoundedChunks) {
    StandbyMetadataStore store;
    ASSERT_TRUE(store.PutMetadata("tenant-a", "a1", MakeMetadata(1)));
    ASSERT_TRUE(store.PutMetadata("tenant-a", "a2", MakeMetadata(2)));
    ASSERT_TRUE(store.PutMetadata("tenant-b", "b1", MakeMetadata(3)));
    ASSERT_TRUE(store.PutMetadata("tenant-b", "b2", MakeMetadata(4)));
    ASSERT_TRUE(store.PutMetadata("tenant-c", "c1", MakeMetadata(5)));

    auto cursor = store.BeginSnapshotTraversal();
    std::set<std::pair<std::string, std::string>> copied;
    while (!cursor.done()) {
        std::vector<StandbyObjectEntry> chunk;
        ASSERT_TRUE(store.CopyNextSnapshotChunk(2, cursor, chunk));
        ASSERT_LE(chunk.size(), 2u);
        for (const auto& entry : chunk) {
            EXPECT_TRUE(copied.emplace(entry.tenant_id, entry.key).second);
        }
    }

    EXPECT_EQ(5u, copied.size());
    EXPECT_EQ(5u, store.GetKeyCount());
}

TEST(StandbyMetadataStoreTest, RejectsZeroSizedChunkWithoutAdvancing) {
    StandbyMetadataStore store;
    ASSERT_TRUE(store.PutMetadata("tenant", "key", MakeMetadata(1)));
    auto cursor = store.BeginSnapshotTraversal();
    std::vector<StandbyObjectEntry> chunk;

    EXPECT_FALSE(store.CopyNextSnapshotChunk(0, cursor, chunk));
    EXPECT_TRUE(chunk.empty());
    EXPECT_FALSE(cursor.done());
}

TEST(StandbyMetadataStoreTest, RestoreInsertRejectsDuplicateWithoutOverwrite) {
    StandbyMetadataStore store;
    ASSERT_TRUE(store.RestoreMetadata("tenant", "key", MakeMetadata(1)));

    EXPECT_FALSE(store.RestoreMetadata("tenant", "key", MakeMetadata(2)));
    auto restored = store.GetMetadata("tenant", "key");
    ASSERT_TRUE(restored.has_value());
    EXPECT_EQ(1u, restored->size);
}

TEST(StandbyMetadataStoreTest, WeightMetadataSnapshotRoundTripsCompleteState) {
    const WeightRevisionIdentity identity{
        .tenant_id = "default",
        .name_space = "production",
        .resource_id = "llama-70b",
        .revision = "step-100",
        .weight_generation = 7,
    };
    const WeightMetadataSnapshot snapshot{
        .metadata = {WeightRevisionMetadata{
            .identity = identity,
            .manifest =
                WeightManifestReference{
                    .manifest_key =
                        "weights/production/llama-70b/step-100/7/manifest",
                    .manifest_sha256 = std::string(64, 'a'),
                    .payload_group_id = MakeWeightPayloadGroupId(identity),
                    .payload_keys_sha256 = std::string(64, 'b'),
                    .payload_count = 1,
                    .logical_bytes = 1024,
                },
            .availability = WeightAvailabilityState::READY,
            .residency = WeightResidencyState::HOT,
            .operation_id = 3,
            .affinity_count = 1,
            .affinity_digest = std::string(64, 'c'),
            .observed_hot_ratio = 1.0,
            .metadata_generation = 4,
            .created_at_ms = 100,
            .updated_at_ms = 200,
        }},
        .leases = {WeightRevisionLease{
            .lease_id = 5,
            .identity = identity,
            .holder = "worker-0",
            .expires_at_ms = 300,
            .fenced_metadata_generation = 2,
        }},
        .operations = {WeightResidencyOperation{
            .operation_id = 3,
            .identity = identity,
            .kind = WeightOperationKind::MIGRATING,
            .target_residency = WeightResidencyState::COLD,
            .fenced_metadata_generation = 4,
            .started_at_ms = 150,
            .updated_at_ms = 200,
            .processed_units = 0,
            .total_units = 1,
            .processed_bytes = 0,
            .total_bytes = 1024,
            .cursor = {},
            .message = {},
        }},
        .next_lease_id = 6,
        .next_operation_id = 4,
    };

    StandbyMetadataStore store;
    ASSERT_TRUE(store.RestoreWeightMetadata(snapshot));
    EXPECT_EQ(snapshot, store.SnapshotWeightMetadata());
}

TEST(StandbyMetadataStoreTest,
     RejectsInvalidWeightMetadataStoreWithoutMutation) {
    StandbyMetadataStore store;
    const WeightMetadataSnapshot empty;
    ASSERT_TRUE(store.RestoreWeightMetadata(empty));

    WeightMetadataSnapshot invalid;
    invalid.next_lease_id = 0;
    EXPECT_FALSE(store.RestoreWeightMetadata(invalid));
    EXPECT_EQ(empty, store.SnapshotWeightMetadata());
}

}  // namespace mooncake::test

namespace mooncake {
namespace {

ObjectReplicaTarget Target() {
    return {"s3", "http://localhost:9000", "objects", "us-east-1", "test/",
            true};
}

ObjectUploadRecord Upload() {
    return {Target(),
            "default",
            "key",
            {1, 2},
            7,
            {"attempt-7", 1024},
            ObjectUploadPhase::AUTHORIZED,
            0,
            false};
}

TEST(ObjectReplicaStateTest, CommitAndRetirementShareTheRecoveryAuthority) {
    StandbyMetadataStore store;
    auto record = Upload();
    ObjectReplicaMutation prepare{Target(), {record}, {}, {}, false};
    ASSERT_TRUE(store.ApplyObjectReplicaMutation("", "key", prepare));
    EXPECT_FALSE(store.Exists("default", "key"));

    MetadataPayload payload;
    payload.client_id = record.writer;
    payload.size = record.object.object_size;
    Replica::Descriptor replica;
    replica.descriptor_variant = DiskDescriptor{"existing-disk", payload.size};
    payload.replicas = {replica};
    payload.replicas[0].id = record.replica_id;
    record.phase = ObjectUploadPhase::UPLOADED;
    record.upload_finished = true;
    ObjectReplicaMutation commit{Target(),
                                 {record},
                                 {},
                                 struct_pack::serialize<std::string>(payload),
                                 false};
    ASSERT_TRUE(store.ApplyObjectReplicaMutation("default", "key", commit));
    ASSERT_TRUE(store.Exists("default", "key"));
    const auto recovered = store.GetMetadata("default", "key");
    ASSERT_TRUE(recovered);
    EXPECT_TRUE(recovered->replicas[0].is_disk_replica());

    // The existing logical REMOVE must not erase upload/cleanup obligations.
    ASSERT_TRUE(store.Remove("default", "key"));
    EXPECT_EQ(store.SnapshotObjectReplicas().records.size(), 1);
    record.phase = ObjectUploadPhase::RETIRED;
    record.not_before_ms = 100;
    ASSERT_TRUE(store.ApplyObjectReplicaMutation(
        "default", "key", {Target(), {record}, {}, {}, false}));
    EXPECT_FALSE(store.Exists("default", "key"));
}

TEST(ObjectReplicaStateTest, RejectsInvalidRecordsWithoutChangingState) {
    StandbyMetadataStore store;
    const auto original = Upload();
    auto other = original;
    other.object.object_key = "second-name";
    EXPECT_FALSE(store.ApplyObjectReplicaMutation(
        "default", "key", {Target(), {original, other}, {}, {}, false}));
    EXPECT_FALSE(store.HasObjectReplicaState());
    ASSERT_TRUE(store.ApplyObjectReplicaMutation(
        "default", "key", {Target(), {original}, {}, {}, false}));
    auto wrong_size = original;
    wrong_size.object.object_size++;
    auto wrong_id = original;
    wrong_id.replica_id++;
    auto unfinished = original;
    unfinished.phase = ObjectUploadPhase::UPLOADED;
    for (const auto& invalid : {other, wrong_size, wrong_id, unfinished}) {
        EXPECT_FALSE(store.ApplyObjectReplicaMutation(
            "default", "key", {Target(), {invalid}, {}, {}, false}));
    }
    auto retired = original;
    retired.phase = ObjectUploadPhase::RETIRED;
    retired.not_before_ms = 100;
    ASSERT_TRUE(store.ApplyObjectReplicaMutation(
        "default", "key", {Target(), {retired}, {}, {}, false}));
    EXPECT_FALSE(store.ApplyObjectReplicaMutation(
        "default", "key", {Target(), {original}, {}, {}, false}));
    auto target = Target();
    target.bucket = "another-bucket";
    EXPECT_FALSE(store.ApplyObjectReplicaMutation(
        "default", "key", {target, {}, {"attempt-7"}, {}, false}));
    const auto snapshot = store.SnapshotObjectReplicas();
    ASSERT_EQ(snapshot.records.size(), 1);
    EXPECT_EQ(snapshot.records.front().not_before_ms, 100);
    ReplicaID maximum = 0;
    ASSERT_TRUE(store.ValidateReplicaIds(maximum));
    EXPECT_EQ(maximum, original.replica_id);
    store.Clear();
    EXPECT_FALSE(store.HasObjectReplicaState());
    EXPECT_TRUE(store.SnapshotObjectReplicas().records.empty());
}

TEST(ObjectReplicaStateTest, RejectsInvalidMetadataWithoutPartialApply) {
    StandbyMetadataStore store;
    auto record = Upload();
    ASSERT_TRUE(store.ApplyObjectReplicaMutation(
        "default", "key", {Target(), {record}, {}, {}, false}));
    MetadataPayload payload;
    payload.client_id = record.writer;
    payload.size = record.object.object_size;
    Replica::Descriptor replica;
    replica.descriptor_variant = DiskDescriptor{"existing-disk", payload.size};
    replica.id = record.replica_id;
    payload.replicas = {replica};
    ASSERT_TRUE(
        store.PutMetadata("default", "key", payload.ToStandbyMetadata()));
    const auto original_uploads =
        struct_pack::serialize<std::string>(store.SnapshotObjectReplicas());
    const auto original_value = struct_pack::serialize<std::string>(
        *store.GetMetadata("default", "key"));
    record.phase = ObjectUploadPhase::UPLOADED;
    record.upload_finished = true;
    auto wrong_writer = payload;
    wrong_writer.client_id = {9, 9};
    auto zero_id = payload;
    zero_id.replicas.front().id = 0;
    auto reserved_id = payload;
    reserved_id.replicas.front().id = std::numeric_limits<ReplicaID>::max();
    auto duplicate_id = payload;
    duplicate_id.replicas.push_back(replica);
    ObjectReplicaMutation commit{Target(), {record}, {}, {}, false};
    for (const auto& invalid :
         {wrong_writer, zero_id, reserved_id, duplicate_id}) {
        commit.metadata_payload = struct_pack::serialize<std::string>(invalid);
        EXPECT_FALSE(
            store.ApplyObjectReplicaMutation("default", "key", commit));
        EXPECT_EQ(original_uploads, struct_pack::serialize<std::string>(
                                        store.SnapshotObjectReplicas()));
        const auto current = store.GetMetadata("default", "key");
        ASSERT_TRUE(current);
        EXPECT_EQ(original_value,
                  struct_pack::serialize<std::string>(*current));
    }
    commit.metadata_payload = struct_pack::serialize<std::string>(payload);
    ASSERT_TRUE(store.ApplyObjectReplicaMutation("default", "key", commit));
    record.phase = ObjectUploadPhase::RETIRED;
    record.not_before_ms = 100;
    ObjectReplicaMutation remove{
        Target(), {record}, {"missing-name"}, {}, true};
    EXPECT_FALSE(store.ApplyObjectReplicaMutation("default", "key", remove));
    EXPECT_TRUE(store.Exists("default", "key"));
    EXPECT_EQ(store.SnapshotObjectReplicas().records.front().phase,
              ObjectUploadPhase::UPLOADED);
    remove.forget.clear();
    ASSERT_TRUE(store.ApplyObjectReplicaMutation("default", "key", remove));
    EXPECT_FALSE(store.Exists("default", "key"));
    EXPECT_EQ(store.SnapshotObjectReplicas().records.front().phase,
              ObjectUploadPhase::RETIRED);
}

TEST(ObjectReplicaStateTest, UncertainRetirementCannotBeForgotten) {
    StandbyMetadataStore store;
    auto record = Upload();
    ASSERT_TRUE(store.ApplyObjectReplicaMutation(
        "default", "key", {Target(), {record}, {}, {}, false}));
    record.phase = ObjectUploadPhase::RETIRED;
    record.not_before_ms = 100;
    ASSERT_TRUE(store.ApplyObjectReplicaMutation(
        "default", "key", {Target(), {record}, {}, {}, true}));
    EXPECT_FALSE(store.ApplyObjectReplicaMutation(
        "default", "key",
        {Target(), {}, {record.object.object_key}, {}, false}));
    auto snapshot = store.SnapshotObjectReplicas();
    ASSERT_TRUE(store.RestoreObjectReplicas(snapshot));
    snapshot.records.push_back(record);
    EXPECT_FALSE(store.RestoreObjectReplicas(snapshot));
    EXPECT_EQ(store.SnapshotObjectReplicas().records.size(), 1);
    EXPECT_FALSE(
        store.SnapshotObjectReplicas().records.front().upload_finished);
    record.upload_finished = true;
    ASSERT_TRUE(store.ApplyObjectReplicaMutation(
        "default", "key", {Target(), {record}, {}, {}, false}));
    ASSERT_TRUE(store.ApplyObjectReplicaMutation(
        "default", "key",
        {Target(), {}, {record.object.object_key}, {}, false}));
    EXPECT_TRUE(store.SnapshotObjectReplicas().records.empty());
    EXPECT_TRUE(store.SnapshotObjectReplicas().target);
}

OpLogEntry MutationEntry(uint64_t sequence,
                         const ObjectReplicaMutation& mutation) {
    OpLogEntry entry;
    entry.sequence_id = sequence;
    entry.tenant_id = "default";
    entry.object_key = "key";
    entry.op_type = OpType::OBJECT_STORAGE_MUTATION;
    entry.payload = struct_pack::serialize<std::string>(mutation);
    entry.checksum = ComputeOpLogChecksum(entry.payload);
    return entry;
}

TEST(ObjectReplicaStateTest, OrderedReplayRejectsInvalidEntries) {
    StandbyMetadataStore store;
    OpLogApplier applier(&store);
    auto record = Upload();
    auto authorize = MutationEntry(1, {Target(), {record}, {}, {}, false});
    ASSERT_TRUE(applier.ApplyOpLogEntry(authorize));
    EXPECT_TRUE(applier.ApplyOpLogEntry(authorize));  // duplicate delivery
    record.phase = ObjectUploadPhase::RETIRED;
    record.not_before_ms = 100;
    auto retire = MutationEntry(2, {Target(), {record}, {}, {}, false});
    ASSERT_TRUE(applier.ApplyOpLogEntry(retire));
    auto invalid = MutationEntry(3, {Target(), {}, {"attempt-7"}, {}, false});
    EXPECT_FALSE(applier.ApplyOpLogEntry(invalid));
    EXPECT_EQ(applier.GetExpectedSequenceId(), 3);
    EXPECT_FALSE(
        store.SnapshotObjectReplicas().records.front().upload_finished);
}

TEST(ObjectReplicaStateTest, PromotionDetectsBothRecoveryHandoffs) {
    ha::PromotionContext handoff;
    EXPECT_FALSE(handoff.HasObjectReplicaState());
    handoff.object_storage = {Target(), {}};
    EXPECT_TRUE(handoff.HasObjectReplicaState());
    handoff.object_storage = {};
    handoff.metadata_store = std::make_unique<StandbyMetadataStore>();
    EXPECT_FALSE(handoff.HasObjectReplicaState());
    ASSERT_TRUE(handoff.metadata_store->RestoreObjectReplicas({Target(), {}}));
    EXPECT_TRUE(handoff.HasObjectReplicaState());
}

struct PreviousStandbySnapshot {
    uint64_t oplog_sequence_id{0};
    std::vector<StandbySegmentInfo> segments;
    std::vector<StandbyObjectEntry> objects;
    struct_pack::compatible<WeightMetadataSnapshot, 1> weight_metadata;
    YLT_REFL(PreviousStandbySnapshot, oplog_sequence_id, segments, objects,
             weight_metadata);
};

TEST(ObjectReplicaStateTest, ExistingSnapshotWireShapeRemainsReadable) {
    PreviousStandbySnapshot old;
    old.oplog_sequence_id = 100;
    StandbySnapshot current;
    ASSERT_EQ(struct_pack::deserialize_to(current, struct_pack::serialize(old)),
              struct_pack::errc::ok);
    EXPECT_FALSE(current.object_storage);
    current.object_storage = ObjectReplicaSnapshot{Target(), {Upload()}};
    ASSERT_EQ(struct_pack::deserialize_to(old, struct_pack::serialize(current)),
              struct_pack::errc::ok);
    EXPECT_EQ(old.oplog_sequence_id, 100);
}

}  // namespace
}  // namespace mooncake

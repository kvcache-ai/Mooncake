#include "ha/standby_metadata_store.h"

#include <gtest/gtest.h>
#include <future>
#include <limits>

#include "ha/oplog/oplog_applier.h"
#include "ha/oplog/ordered_oplog_writer.h"
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
            .operation = WeightOperationState::EVICTING,
            .operation_id = 3,
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
            .operation = WeightOperationState::EVICTING,
            .target_residency = WeightResidencyState::COLD,
            .fenced_metadata_generation = 4,
            .started_at_ms = 150,
            .updated_at_ms = 200,
            .processed_members = 0,
            .total_members = 2,
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
    auto snapshot = store.SnapshotObjectReplicas();
    ASSERT_EQ(snapshot.records.size(), 1);
    EXPECT_TRUE(snapshot.records[0].upload_finished);
    record.phase = ObjectUploadPhase::RETIRED;
    record.not_before_ms = 100;
    ASSERT_TRUE(store.ApplyObjectReplicaMutation(
        "default", "key", {Target(), {record}, {}, {}, false}));
    EXPECT_FALSE(store.Exists("default", "key"));

    StandbyMetadataStore restarted;
    ASSERT_TRUE(
        restarted.RestoreObjectReplicas(store.SnapshotObjectReplicas()));
    EXPECT_EQ(restarted.SnapshotObjectReplicas().records[0].not_before_ms, 100);
    ASSERT_TRUE(restarted.ApplyObjectReplicaMutation(
        "default", "key", {Target(), {}, {"attempt-7"}, {}, false}));
    EXPECT_TRUE(restarted.SnapshotObjectReplicas().records.empty());
    EXPECT_TRUE(restarted.SnapshotObjectReplicas().target.has_value());
}

TEST(ObjectReplicaStateTest, RejectsNamespaceChangesAndStaleTransitions) {
    StandbyMetadataStore store;
    auto record = Upload();
    ASSERT_TRUE(store.ApplyObjectReplicaMutation(
        "default", "key", {Target(), {record}, {}, {}, false}));
    auto wrong = record;
    wrong.object.object_size++;
    EXPECT_FALSE(store.ApplyObjectReplicaMutation(
        "default", "key", {Target(), {wrong}, {}, {}, false}));
    wrong = record;
    wrong.replica_id++;
    EXPECT_FALSE(store.ApplyObjectReplicaMutation(
        "default", "key", {Target(), {wrong}, {}, {}, false}));
    record.phase = ObjectUploadPhase::RETIRED;
    record.not_before_ms = 100;
    ASSERT_TRUE(store.ApplyObjectReplicaMutation(
        "default", "key", {Target(), {record}, {}, {}, false}));
    EXPECT_FALSE(store.ApplyObjectReplicaMutation(
        "default", "key", {Target(), {Upload()}, {}, {}, false}));
    auto target = Target();
    target.bucket = "another-bucket";
    EXPECT_FALSE(store.ApplyObjectReplicaMutation(
        "default", "key", {target, {}, {"attempt-7"}, {}, false}));
    EXPECT_EQ(store.SnapshotObjectReplicas().records[0].not_before_ms, 100);
}

TEST(ObjectReplicaStateTest,
     RejectsMetadataFromAnotherWriterWithoutPartialApply) {
    StandbyMetadataStore store;
    auto record = Upload();
    ASSERT_TRUE(store.ApplyObjectReplicaMutation(
        "default", "key", {Target(), {record}, {}, {}, false}));
    record.phase = ObjectUploadPhase::UPLOADED;
    record.upload_finished = true;
    MetadataPayload payload;
    payload.client_id = record.writer;
    payload.size = record.object.object_size;
    Replica::Descriptor replica;
    replica.descriptor_variant = DiskDescriptor{"existing-disk", payload.size};
    payload.replicas = {replica};
    payload.client_id = {9, 9};
    payload.replicas[0].id = record.replica_id;
    ObjectReplicaMutation mutation{Target(),
                                   {record},
                                   {},
                                   struct_pack::serialize<std::string>(payload),
                                   false};
    EXPECT_FALSE(store.ApplyObjectReplicaMutation("default", "key", mutation));
    EXPECT_FALSE(store.Exists("default", "key"));
    EXPECT_EQ(store.SnapshotObjectReplicas().records.front().phase,
              ObjectUploadPhase::AUTHORIZED);
    payload.client_id = record.writer;
    mutation.metadata_payload = struct_pack::serialize<std::string>(payload);
    ASSERT_TRUE(store.ApplyObjectReplicaMutation("default", "key", mutation));
    record.phase = ObjectUploadPhase::RETIRED;
    record.not_before_ms = 100;
    mutation = {Target(), {record}, {"missing-name"}, {}, true};
    EXPECT_FALSE(store.ApplyObjectReplicaMutation("default", "key", mutation));
    EXPECT_TRUE(store.Exists("default", "key"));
    EXPECT_EQ(store.SnapshotObjectReplicas().records.front().phase,
              ObjectUploadPhase::UPLOADED);
    mutation.forget.clear();
    ASSERT_TRUE(store.ApplyObjectReplicaMutation("default", "key", mutation));
    EXPECT_FALSE(store.Exists("default", "key"));
    EXPECT_EQ(store.SnapshotObjectReplicas().records.front().phase,
              ObjectUploadPhase::RETIRED);
}

TEST(ObjectReplicaStateTest,
     RejectsInvalidMetadataReplicaIdsWithoutPartialApply) {
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
    const auto original_metadata = payload.ToStandbyMetadata();
    ASSERT_TRUE(store.PutMetadata("default", "key", original_metadata));
    const auto original_uploads =
        struct_pack::serialize<std::string>(store.SnapshotObjectReplicas());
    const auto original_value =
        struct_pack::serialize<std::string>(original_metadata);

    record.phase = ObjectUploadPhase::UPLOADED;
    record.upload_finished = true;
    for (const auto& ids : std::vector<std::vector<ReplicaID>>{
             {0},
             {std::numeric_limits<ReplicaID>::max()},
             {record.replica_id, record.replica_id}}) {
        payload.replicas.clear();
        for (const auto id : ids) {
            replica.id = id;
            payload.replicas.push_back(replica);
        }
        EXPECT_FALSE(store.ApplyObjectReplicaMutation(
            "default", "key",
            {Target(),
             {record},
             {},
             struct_pack::serialize<std::string>(payload),
             false}));
        EXPECT_EQ(original_uploads, struct_pack::serialize<std::string>(
                                        store.SnapshotObjectReplicas()));
        const auto current = store.GetMetadata("default", "key");
        ASSERT_TRUE(current);
        EXPECT_EQ(original_value,
                  struct_pack::serialize<std::string>(*current));
    }

    replica.id = record.replica_id;
    payload.replicas = {replica};
    ASSERT_TRUE(store.ApplyObjectReplicaMutation(
        "default", "key",
        {Target(),
         {record},
         {},
         struct_pack::serialize<std::string>(payload),
         false}));
    StandbyMetadataStore restored;
    ASSERT_TRUE(restored.RestoreObjectReplicas(store.SnapshotObjectReplicas()));
    ASSERT_TRUE(restored.RestoreMetadata("default", "key",
                                         *store.GetMetadata("default", "key")));
    ReplicaID maximum = 0;
    EXPECT_TRUE(restored.ValidateReplicaIds(maximum));
    EXPECT_EQ(record.replica_id, maximum);
}

TEST(ObjectReplicaStateTest, RestoresUncertainUploadsWithoutDiscardingThem) {
    auto record = Upload();
    record.phase = ObjectUploadPhase::RETIRED;
    record.not_before_ms = 10;
    ObjectReplicaSnapshot snapshot{Target(), {record}};
    StandbyMetadataStore store;
    ASSERT_TRUE(store.RestoreObjectReplicas(snapshot));
    snapshot.records.push_back(record);
    EXPECT_FALSE(store.RestoreObjectReplicas(snapshot));
    auto state = store.SnapshotObjectReplicas();
    ASSERT_EQ(state.records.size(), 1);
    EXPECT_FALSE(state.records[0].upload_finished);
    const auto encoded = struct_pack::serialize<std::string>(state);
    ObjectReplicaSnapshot decoded;
    ASSERT_EQ(struct_pack::deserialize_to(decoded, encoded),
              struct_pack::errc::ok);
    ASSERT_TRUE(store.RestoreObjectReplicas(decoded));
    EXPECT_FALSE(store.SnapshotObjectReplicas().records[0].upload_finished);
}

TEST(ObjectReplicaStateTest, RejectsDuplicateIdsAndPreservesStateOnFailure) {
    StandbyMetadataStore store;
    const auto original = Upload();
    auto other = original;
    other.object.object_key = "second-name";
    EXPECT_FALSE(store.ApplyObjectReplicaMutation(
        "default", "key", {Target(), {original, other}, {}, {}, false}));
    EXPECT_FALSE(store.SnapshotObjectReplicas().target);
    ASSERT_TRUE(store.ApplyObjectReplicaMutation(
        "default", "key", {Target(), {original}, {}, {}, false}));
    EXPECT_FALSE(store.ApplyObjectReplicaMutation(
        "default", "key", {Target(), {other}, {}, {}, false}));
    EXPECT_FALSE(store.ApplyObjectReplicaMutation(
        "default", "key", {Target(), {}, {"attempt-7"}, {}, false}));
    auto invalid = original;
    invalid.phase = ObjectUploadPhase::UPLOADED;
    EXPECT_FALSE(store.ApplyObjectReplicaMutation(
        "default", "key", {Target(), {invalid}, {}, {}, false}));
    ReplicaID maximum = 0;
    ASSERT_TRUE(store.ValidateReplicaIds(maximum));
    EXPECT_EQ(maximum, original.replica_id);
    EXPECT_EQ(store.SnapshotObjectReplicas().records.size(), 1);
    store.Clear();
    EXPECT_FALSE(store.SnapshotObjectReplicas().target);
    ASSERT_TRUE(store.ApplyObjectReplicaMutation(
        "default", "key", {Target(), {other}, {}, {}, false}));
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
    EXPECT_EQ(store.SnapshotObjectReplicas().records.size(), 1);
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

TEST(ObjectReplicaStateTest, OrderedReplayRejectsGapsAndInvalidEntries) {
    StandbyMetadataStore store;
    OpLogApplier applier(&store);
    auto record = Upload();
    auto authorize = MutationEntry(1, {Target(), {record}, {}, {}, false});
    EXPECT_FALSE(applier.ApplyOpLogEntry(
        MutationEntry(2, {Target(), {record}, {}, {}, false})));
    ASSERT_TRUE(applier.ApplyOpLogEntry(authorize));
    EXPECT_TRUE(applier.ApplyOpLogEntry(authorize));  // duplicate delivery
    record.phase = ObjectUploadPhase::RETIRED;
    record.not_before_ms = 100;
    auto retire = MutationEntry(2, {Target(), {record}, {}, {}, false});
    retire.checksum++;
    EXPECT_FALSE(applier.ApplyOpLogEntry(retire));
    EXPECT_EQ(applier.GetExpectedSequenceId(), 2);
    retire.checksum = ComputeOpLogChecksum(retire.payload);
    ASSERT_TRUE(applier.ApplyOpLogEntry(retire));
    auto invalid = MutationEntry(3, {Target(), {}, {"attempt-7"}, {}, false});
    EXPECT_FALSE(applier.ApplyOpLogEntry(invalid));
    EXPECT_EQ(applier.GetExpectedSequenceId(), 3);
    EXPECT_FALSE(
        store.SnapshotObjectReplicas().records.front().upload_finished);
}

TEST(ObjectReplicaStateTest, LogAcknowledgementWaitsForDurability) {
    std::promise<void> entered;
    std::promise<void> release;
    auto gate = release.get_future().share();
    std::vector<OpLogEntry> persisted;
    OrderedOpLogWriter writer(
        {}, [&](const OpLogBatchRecord& batch, const DurablePrefix&) {
            entered.set_value();
            gate.wait();
            persisted = batch.entries;
            return ErrorCode::OK;
        });
    auto reservation = writer.Reserve();
    ASSERT_TRUE(reservation);
    auto handle = writer.Commit(
        std::move(*reservation),
        MutationEntry(0, {Target(), {Upload()}, {}, {}, false}), {});
    ASSERT_TRUE(handle);
    auto durable = writer.AwaitDurable(handle->sequence_id());
    std::promise<ErrorCode> result;
    auto completed = result.get_future();
    auto continuation = std::move(durable).thenValue(
        [&](ErrorCode error) { result.set_value(error); });
    writer.Start();
    const auto started = entered.get_future().wait_for(std::chrono::seconds(5));
    EXPECT_EQ(started, std::future_status::ready);
    EXPECT_EQ(completed.wait_for(std::chrono::milliseconds(0)),
              std::future_status::timeout);
    release.set_value();
    ASSERT_EQ(completed.wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    EXPECT_EQ(completed.get(), ErrorCode::OK);
    writer.Stop();
    ASSERT_EQ(persisted.size(), 1);
    StandbyMetadataStore restarted;
    OpLogApplier applier(&restarted);
    ASSERT_TRUE(applier.ApplyOpLogEntry(persisted.front()));
    EXPECT_EQ(
        restarted.SnapshotObjectReplicas().records.front().object.object_key,
        "attempt-7");
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

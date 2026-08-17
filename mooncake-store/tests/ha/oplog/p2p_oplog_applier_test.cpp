#include "ha/oplog/p2p_oplog_applier.h"

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <xxhash.h>

#include <cstdint>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include "ha/oplog/oplog_manager.h"
#include "ha/oplog/p2p_oplog_types.h"
#include "ha/oplog/p2p_standby_metadata_store.h"
#include "mock_oplog_store.h"
#include "types.h"

using mooncake::test::MockOpLogStore;

namespace mooncake::test {

namespace {
// Helper to create a valid OpLogEntry with checksum.
OpLogEntry MakeEntry(uint64_t seq, OpType type, const std::string& key,
                     const std::string& payload) {
    OpLogEntry e;
    e.sequence_id = seq;
    e.op_type = type;
    e.object_key = key;
    e.payload = payload;
    e.timestamp_ms = 1000 + seq;
    e.checksum = XXH32(payload.data(), payload.size(), 0);
    e.prefix_hash = XXH32(key.data(), key.size(), 0);
    return e;
}

// Helper to create a valid ADD_REPLICA entry.
OpLogEntry MakeAddReplicaEntry(uint64_t seq, const std::string& object_key,
                               const AddReplicaPayload& payload) {
    std::string data = SerializeP2PPayload(payload);
    return MakeEntry(seq, OpType_ADD_REPLICA, object_key, data);
}

// Helper to create a valid REMOVE_REPLICA entry.
OpLogEntry MakeRemoveReplicaEntry(uint64_t seq, const std::string& object_key,
                                  const RemoveReplicaPayload& payload) {
    std::string data = SerializeP2PPayload(payload);
    return MakeEntry(seq, OpType_REMOVE_REPLICA, object_key, data);
}

// Helper to create a valid MOUNT_SEGMENT entry.
OpLogEntry MakeMountSegmentEntry(uint64_t seq,
                                 const MountSegmentPayload& payload) {
    std::string data = SerializeP2PPayload(payload);
    return MakeEntry(seq, OpType_MOUNT_SEGMENT, "", data);
}

// Helper to create a valid UNMOUNT_SEGMENT entry.
OpLogEntry MakeUnmountSegmentEntry(uint64_t seq,
                                   const UnmountSegmentPayload& payload) {
    std::string data = SerializeP2PPayload(payload);
    return MakeEntry(seq, OpType_UNMOUNT_SEGMENT, "", data);
}

// Helper to create a valid REGISTER_CLIENT entry.
OpLogEntry MakeRegisterClientEntry(uint64_t seq,
                                   const RegisterClientPayload& payload) {
    std::string data = SerializeP2PPayload(payload);
    return MakeEntry(seq, OpType_REGISTER_CLIENT, "", data);
}

// Helper to create a valid UNREGISTER_CLIENT entry.
OpLogEntry MakeUnregisterClientEntry(uint64_t seq,
                                     const UnregisterClientPayload& payload) {
    std::string data = SerializeP2PPayload(payload);
    return MakeEntry(seq, OpType_UNREGISTER_CLIENT, "", data);
}

// Helper to create REMOVE entry (main branch OpType).
OpLogEntry MakeRemoveEntry(uint64_t seq, const std::string& key) {
    return MakeEntry(seq, OpType::REMOVE, key, "");
}

UUID MakeUUID(uint64_t hi, uint64_t lo) { return UUID{hi, lo}; }

Segment MakeSegment(const UUID& id, size_t size) {
    Segment segment;
    segment.id = id;
    segment.size = size;
    return segment;
}

void SkipSequenceWithFutureAdd(P2POpLogApplier& applier,
                               uint64_t skipped_sequence_id,
                               const std::string& future_key) {
    AddReplicaPayload future;
    future.object_key = future_key;
    future.client_id = MakeUUID(1000 + skipped_sequence_id, 0);
    future.segment_id = MakeUUID(2000 + skipped_sequence_id, 0);
    future.size = 1024;

    ASSERT_FALSE(applier.ApplyOpLogEntry(
        MakeAddReplicaEntry(skipped_sequence_id + 1, future_key, future)));
    applier.ConfirmMissingSequenceIds({skipped_sequence_id});
    EXPECT_EQ(applier.ProcessPendingEntries(), 1u);
    EXPECT_EQ(applier.GetExpectedSequenceId(), skipped_sequence_id + 2);
}
}  // namespace

// ============================================================================
// P2POpLogApplier - Basic Apply
// ============================================================================

TEST(P2POpLogApplierTest, ApplyAddReplica) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store, "test-cluster");

    auto client = MakeUUID(1, 0);
    auto seg = MakeUUID(10, 0);
    AddReplicaPayload payload;
    payload.object_key = "model-weights";
    payload.client_id = client;
    payload.segment_id = seg;
    payload.size = 4096;

    auto entry = MakeAddReplicaEntry(1, "model-weights", payload);
    EXPECT_TRUE(applier.ApplyOpLogEntry(entry));

    auto objects = store.GetObjects();
    ASSERT_EQ(objects.size(), 1u);
    EXPECT_NE(objects.find("model-weights"), objects.end());
    EXPECT_EQ(objects.at("model-weights").replicas.size(), 1u);
    EXPECT_EQ(objects.at("model-weights").last_sequence_id, 1u);
}

TEST(P2POpLogApplierTest, ApplyRemoveReplica) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store, "test-cluster");

    auto client = MakeUUID(1, 0);
    auto seg = MakeUUID(10, 0);

    // First add, then remove
    AddReplicaPayload add_payload;
    add_payload.object_key = "key1";
    add_payload.client_id = client;
    add_payload.segment_id = seg;
    add_payload.size = 1024;
    EXPECT_TRUE(
        applier.ApplyOpLogEntry(MakeAddReplicaEntry(1, "key1", add_payload)));

    RemoveReplicaPayload rm_payload;
    rm_payload.object_key = "key1";
    rm_payload.client_id = client;
    rm_payload.segment_id = seg;
    EXPECT_TRUE(
        applier.ApplyOpLogEntry(MakeRemoveReplicaEntry(2, "key1", rm_payload)));

    // Object should be removed (no replicas left)
    EXPECT_EQ(store.GetKeyCount(), 0u);
}

TEST(P2POpLogApplierTest, ApplyRemove_DelegatesToBaseClass) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store, "test-cluster");

    // Add P2P metadata first. PutMetadata is a compatibility no-op for
    // P2PStandbyMetadataStore.
    store.AddReplica("key1", MakeUUID(1, 0), MakeUUID(10, 0), 100, 1);
    ASSERT_TRUE(store.GetMetadata("key1").has_value());

    // REMOVE should delegate to base class
    auto entry = MakeRemoveEntry(1, "key1");
    EXPECT_TRUE(applier.ApplyOpLogEntry(entry));
    EXPECT_FALSE(store.GetMetadata("key1").has_value());
}

TEST(P2POpLogApplierTest, ApplyRemoveAll) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store, "test-cluster");

    auto client = MakeUUID(1, 0);
    auto seg = MakeUUID(10, 0);

    AddReplicaPayload payload;
    payload.object_key = "key1";
    payload.client_id = client;
    payload.segment_id = seg;
    payload.size = 1024;
    applier.ApplyOpLogEntry(MakeAddReplicaEntry(1, "key1", payload));

    AddReplicaPayload payload2;
    payload2.object_key = "key2";
    payload2.client_id = client;
    payload2.segment_id = seg;
    payload2.size = 2048;
    applier.ApplyOpLogEntry(MakeAddReplicaEntry(2, "key2", payload2));

    EXPECT_EQ(store.GetKeyCount(), 2u);

    // REMOVE_ALL
    auto entry = MakeEntry(3, OpType_REMOVE_ALL, "", "");
    EXPECT_TRUE(applier.ApplyOpLogEntry(entry));

    EXPECT_EQ(store.GetKeyCount(), 0u);
    EXPECT_EQ(store.GetClients().size(), 0u);
}

TEST(P2POpLogApplierTest, ApplyMountSegment) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store, "test-cluster");

    auto client = MakeUUID(1, 0);
    auto seg_id = MakeUUID(100, 0);

    Segment segment;
    segment.id = seg_id;
    segment.size = 4096;

    MountSegmentPayload payload;
    payload.client_id = client;
    payload.segment = segment;

    auto entry = MakeMountSegmentEntry(1, payload);
    EXPECT_TRUE(applier.ApplyOpLogEntry(entry));

    auto info = store.GetClient(client);
    ASSERT_NE(info, nullptr);
    ASSERT_EQ(info->segments.size(), 1u);
    EXPECT_EQ(info->segments[0].id, seg_id);
}

TEST(P2POpLogApplierTest, ApplyUnmountSegment) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store, "test-cluster");

    auto client = MakeUUID(1, 0);
    auto seg_id = MakeUUID(100, 0);

    // First register client and mount
    store.RegisterClient(client, "1.2.3.4", 50051, {});

    Segment segment;
    segment.id = seg_id;
    segment.size = 4096;

    MountSegmentPayload mount_payload;
    mount_payload.client_id = client;
    mount_payload.segment = segment;
    applier.ApplyOpLogEntry(MakeMountSegmentEntry(1, mount_payload));

    // Add a replica on this segment
    AddReplicaPayload add_payload;
    add_payload.object_key = "key1";
    add_payload.client_id = client;
    add_payload.segment_id = seg_id;
    add_payload.size = 1024;
    applier.ApplyOpLogEntry(MakeAddReplicaEntry(2, "key1", add_payload));

    ASSERT_EQ(store.GetKeyCount(), 1u);

    // Unmount — should cascade delete replica
    UnmountSegmentPayload umount_payload;
    umount_payload.segment_id = seg_id;
    umount_payload.client_id = client;
    applier.ApplyOpLogEntry(MakeUnmountSegmentEntry(3, umount_payload));

    EXPECT_EQ(store.GetKeyCount(), 0u);  // Object removed (no replicas)
    auto info = store.GetClient(client);
    ASSERT_NE(info, nullptr);
    EXPECT_EQ(info->segments.size(), 0u);  // Segment removed
}

TEST(P2POpLogApplierTest, ApplyRegisterClient) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store, "test-cluster");

    auto client = MakeUUID(1, 0);
    auto seg_id = MakeUUID(100, 0);

    Segment segment;
    segment.id = seg_id;
    segment.size = 2048;

    RegisterClientPayload payload;
    payload.client_id = client;
    payload.ip_address = "192.168.1.100";
    payload.rpc_port = 50051;
    payload.last_mutation_id = 77;
    payload.segments = {segment};

    auto entry = MakeRegisterClientEntry(1, payload);
    EXPECT_TRUE(applier.ApplyOpLogEntry(entry));

    auto info = store.GetClient(client);
    ASSERT_NE(info, nullptr);
    EXPECT_EQ(info->ip_address, "192.168.1.100");
    EXPECT_EQ(info->rpc_port, 50051u);
    EXPECT_EQ(info->last_mutation_id, 77u);
    ASSERT_EQ(info->segments.size(), 1u);
    EXPECT_EQ(info->segments[0].id, seg_id);
}

TEST(P2POpLogApplierTest, ApplyUnregisterClient) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store, "test-cluster");

    auto client = MakeUUID(1, 0);
    auto other_client = MakeUUID(2, 0);
    auto seg = MakeUUID(100, 0);
    auto other_seg = MakeUUID(200, 0);

    RegisterClientPayload register_payload;
    register_payload.client_id = client;
    register_payload.ip_address = "192.168.1.100";
    register_payload.rpc_port = 50051;
    register_payload.segments = {MakeSegment(seg, 2048)};
    EXPECT_TRUE(
        applier.ApplyOpLogEntry(MakeRegisterClientEntry(1, register_payload)));

    RegisterClientPayload other_register_payload;
    other_register_payload.client_id = other_client;
    other_register_payload.ip_address = "192.168.1.101";
    other_register_payload.rpc_port = 50052;
    other_register_payload.segments = {MakeSegment(other_seg, 4096)};
    EXPECT_TRUE(applier.ApplyOpLogEntry(
        MakeRegisterClientEntry(2, other_register_payload)));

    AddReplicaPayload replica_payload;
    replica_payload.object_key = "shared-key";
    replica_payload.client_id = client;
    replica_payload.segment_id = seg;
    replica_payload.size = 1024;
    EXPECT_TRUE(applier.ApplyOpLogEntry(
        MakeAddReplicaEntry(3, "shared-key", replica_payload)));

    AddReplicaPayload other_replica_payload;
    other_replica_payload.object_key = "shared-key";
    other_replica_payload.client_id = other_client;
    other_replica_payload.segment_id = other_seg;
    other_replica_payload.size = 1024;
    EXPECT_TRUE(applier.ApplyOpLogEntry(
        MakeAddReplicaEntry(4, "shared-key", other_replica_payload)));

    UnregisterClientPayload unregister_payload;
    unregister_payload.client_id = client;
    EXPECT_TRUE(applier.ApplyOpLogEntry(
        MakeUnregisterClientEntry(5, unregister_payload)));

    EXPECT_EQ(store.GetClient(client), nullptr);
    ASSERT_NE(store.GetClient(other_client), nullptr);
    auto objects = store.GetObjects();
    auto object_it = objects.find("shared-key");
    ASSERT_NE(object_it, objects.end());
    ASSERT_EQ(object_it->second.replicas.size(), 1u);
    const auto& p2p = std::get<P2PProxyDescriptor>(
        object_it->second.replicas[0].descriptor_variant);
    EXPECT_EQ(p2p.client_id, other_client);
}

// ============================================================================
// P2POpLogApplier - Snapshot replay idempotency
// ============================================================================

TEST(P2POpLogApplierTest, ReplayAddReplicaAlreadyInSnapshotIsNoOp) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store, "test-cluster");

    auto client = MakeUUID(1, 0);
    auto seg = MakeUUID(10, 0);
    store.RegisterClient(client, "192.168.1.100", 50051,
                         {MakeSegment(seg, 4096)});
    store.AddReplica("snapshot-key", client, seg, 1024, 101);
    applier.Recover(100);

    AddReplicaPayload payload;
    payload.object_key = "snapshot-key";
    payload.client_id = client;
    payload.segment_id = seg;
    payload.size = 1024;
    EXPECT_TRUE(applier.ApplyOpLogEntry(
        MakeAddReplicaEntry(101, "snapshot-key", payload)));

    auto objects = store.GetObjects();
    ASSERT_EQ(objects.size(), 1u);
    ASSERT_EQ(objects.at("snapshot-key").replicas.size(), 1u);
    EXPECT_EQ(objects.at("snapshot-key").last_sequence_id, 101u);
    EXPECT_EQ(applier.GetExpectedSequenceId(), 102u);
}

TEST(P2POpLogApplierTest, ReplayMountSegmentAlreadyInSnapshotIsNoOp) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store, "test-cluster");

    auto client = MakeUUID(1, 0);
    auto seg = MakeUUID(10, 0);
    Segment segment = MakeSegment(seg, 4096);
    store.RegisterClient(client, "192.168.1.100", 50051, {segment});
    applier.Recover(100);

    MountSegmentPayload payload;
    payload.client_id = client;
    payload.segment = segment;
    EXPECT_TRUE(applier.ApplyOpLogEntry(MakeMountSegmentEntry(101, payload)));

    auto info = store.GetClient(client);
    ASSERT_NE(info, nullptr);
    ASSERT_EQ(info->segments.size(), 1u);
    EXPECT_EQ(info->segments[0].id, seg);
    EXPECT_EQ(applier.GetExpectedSequenceId(), 102u);
}

TEST(P2POpLogApplierTest, ReplayRegisterClientAlreadyInSnapshotIsStable) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store, "test-cluster");

    auto client = MakeUUID(1, 0);
    auto registration_seg = MakeUUID(10, 0);
    auto later_seg = MakeUUID(20, 0);
    Segment registration_segment = MakeSegment(registration_seg, 4096);
    Segment later_segment = MakeSegment(later_seg, 8192);
    store.RegisterClient(client, "192.168.1.100", 50051,
                         {registration_segment, later_segment});
    applier.Recover(100);

    RegisterClientPayload payload;
    payload.client_id = client;
    payload.ip_address = "192.168.1.100";
    payload.rpc_port = 50051;
    payload.segments = {registration_segment};
    EXPECT_TRUE(applier.ApplyOpLogEntry(MakeRegisterClientEntry(101, payload)));

    auto info = store.GetClient(client);
    ASSERT_NE(info, nullptr);
    EXPECT_EQ(info->ip_address, "192.168.1.100");
    EXPECT_EQ(info->rpc_port, 50051u);
    ASSERT_EQ(info->segments.size(), 2u);
    EXPECT_EQ(info->segments[0].id, registration_seg);
    EXPECT_EQ(info->segments[1].id, later_seg);
    EXPECT_EQ(applier.GetExpectedSequenceId(), 102u);
}

TEST(P2POpLogApplierTest, ReplayRemoveReplicaAlreadyReflectedInSnapshotIsNoOp) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store, "test-cluster");
    applier.Recover(100);

    RemoveReplicaPayload payload;
    payload.object_key = "removed-key";
    payload.client_id = MakeUUID(1, 0);
    payload.segment_id = MakeUUID(10, 0);
    EXPECT_TRUE(applier.ApplyOpLogEntry(
        MakeRemoveReplicaEntry(101, "removed-key", payload)));

    EXPECT_EQ(store.GetKeyCount(), 0u);
    EXPECT_EQ(applier.GetExpectedSequenceId(), 102u);
}

TEST(P2POpLogApplierTest,
     ReplayUnmountSegmentAlreadyReflectedInSnapshotIsNoOp) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store, "test-cluster");
    applier.Recover(100);

    UnmountSegmentPayload payload;
    payload.client_id = MakeUUID(1, 0);
    payload.segment_id = MakeUUID(10, 0);
    EXPECT_TRUE(applier.ApplyOpLogEntry(MakeUnmountSegmentEntry(101, payload)));

    EXPECT_EQ(store.GetKeyCount(), 0u);
    EXPECT_EQ(store.GetClients().size(), 0u);
    EXPECT_EQ(applier.GetExpectedSequenceId(), 102u);
}

TEST(P2POpLogApplierTest,
     ReplayUnregisterClientAlreadyReflectedInSnapshotIsNoOp) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store, "test-cluster");
    auto client = MakeUUID(1, 0);
    applier.Recover(100);

    UnregisterClientPayload payload;
    payload.client_id = client;
    EXPECT_TRUE(
        applier.ApplyOpLogEntry(MakeUnregisterClientEntry(101, payload)));

    EXPECT_EQ(store.GetClient(client), nullptr);
    EXPECT_EQ(store.GetKeyCount(), 0u);
    EXPECT_EQ(applier.GetExpectedSequenceId(), 102u);
}

TEST(P2POpLogApplierTest, ReplayRemoveAllAlreadyReflectedInSnapshotIsNoOp) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store, "test-cluster");
    applier.Recover(100);

    EXPECT_TRUE(
        applier.ApplyOpLogEntry(MakeEntry(101, OpType_REMOVE_ALL, "", "")));

    EXPECT_EQ(store.GetKeyCount(), 0u);
    EXPECT_EQ(store.GetClients().size(), 0u);
    EXPECT_EQ(applier.GetExpectedSequenceId(), 102u);
}

TEST(P2POpLogApplierTest, AlreadyAppliedP2PSequenceIsNoOp) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store, "test-cluster");

    auto client = MakeUUID(1, 0);
    auto seg = MakeUUID(10, 0);

    AddReplicaPayload first;
    first.object_key = "committed-key";
    first.client_id = client;
    first.segment_id = seg;
    first.size = 1024;
    EXPECT_TRUE(applier.ApplyOpLogEntry(
        MakeAddReplicaEntry(1, "committed-key", first)));
    EXPECT_EQ(applier.GetExpectedSequenceId(), 2u);

    AddReplicaPayload stale;
    stale.object_key = "stale-key";
    stale.client_id = MakeUUID(2, 0);
    stale.segment_id = MakeUUID(20, 0);
    stale.size = 2048;
    EXPECT_TRUE(
        applier.ApplyOpLogEntry(MakeAddReplicaEntry(1, "stale-key", stale)));

    auto objects = store.GetObjects();
    ASSERT_EQ(objects.size(), 1u);
    EXPECT_NE(objects.find("committed-key"), objects.end());
    EXPECT_EQ(objects.find("stale-key"), objects.end());
    EXPECT_EQ(applier.GetExpectedSequenceId(), 2u);
}

// ============================================================================
// P2POpLogApplier - Ordering and gap detection
// ============================================================================

TEST(P2POpLogApplierTest, EntriesAppliedInOrder) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store, "test-cluster");

    auto client = MakeUUID(1, 0);
    auto seg = MakeUUID(10, 0);

    // Apply entries in order
    AddReplicaPayload p1;
    p1.object_key = "key1";
    p1.client_id = client;
    p1.segment_id = seg;
    p1.size = 1024;

    AddReplicaPayload p2;
    p2.object_key = "key2";
    p2.client_id = client;
    p2.segment_id = seg;
    p2.size = 2048;

    EXPECT_TRUE(applier.ApplyOpLogEntry(MakeAddReplicaEntry(1, "key1", p1)));
    EXPECT_TRUE(applier.ApplyOpLogEntry(MakeAddReplicaEntry(2, "key2", p2)));

    EXPECT_EQ(store.GetKeyCount(), 2u);
    EXPECT_EQ(applier.GetExpectedSequenceId(), 3u);
}

TEST(P2POpLogApplierTest, OutOfOrderEntryRejected) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store, "test-cluster");

    auto client = MakeUUID(1, 0);
    auto seg = MakeUUID(10, 0);

    AddReplicaPayload p2;
    p2.object_key = "key2";
    p2.client_id = client;
    p2.segment_id = seg;
    p2.size = 2048;

    // Entry seq=2 arrives before seq=1 — should be rejected, not applied
    EXPECT_FALSE(applier.ApplyOpLogEntry(MakeAddReplicaEntry(2, "key2", p2)));

    EXPECT_EQ(store.GetKeyCount(), 0u);  // Not applied yet
    EXPECT_EQ(applier.GetExpectedSequenceId(), 1u);
}

TEST(P2POpLogApplierTest, OutOfOrderEntryBufferedThenGapFillDrainsIt) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store, "test-cluster");

    auto client = MakeUUID(1, 0);
    auto seg = MakeUUID(10, 0);

    AddReplicaPayload p1;
    p1.object_key = "key1";
    p1.client_id = client;
    p1.segment_id = seg;
    p1.size = 1024;

    AddReplicaPayload p2;
    p2.object_key = "key2";
    p2.client_id = client;
    p2.segment_id = seg;
    p2.size = 2048;

    // seq=2 first — reported pending and buffered by the common applier.
    EXPECT_FALSE(applier.ApplyOpLogEntry(MakeAddReplicaEntry(2, "key2", p2)));
    EXPECT_EQ(store.GetKeyCount(), 0u);

    // seq=1 fills the gap; the common pending queue then applies seq=2.
    EXPECT_TRUE(applier.ApplyOpLogEntry(MakeAddReplicaEntry(1, "key1", p1)));
    EXPECT_EQ(store.GetKeyCount(), 2u);
    EXPECT_EQ(applier.GetExpectedSequenceId(), 3u);
}

TEST(P2POpLogApplierTest, MissingEntryTimeoutSkipsGapAndDrainsP2PEntry) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store, "test-cluster");

    AddReplicaPayload payload;
    payload.object_key = "after-gap";
    payload.client_id = MakeUUID(1, 0);
    payload.segment_id = MakeUUID(10, 0);
    payload.size = 1024;

    EXPECT_FALSE(
        applier.ApplyOpLogEntry(MakeAddReplicaEntry(2, "after-gap", payload)));
    applier.ProcessPendingEntries();  // start the common gap timer
    std::this_thread::sleep_for(std::chrono::milliseconds(3100));
    EXPECT_EQ(1u, applier.ProcessPendingEntries());
    EXPECT_EQ(1u, store.GetKeyCount());
    EXPECT_EQ(3u, applier.GetExpectedSequenceId());
}

TEST(P2POpLogApplierTest, LateSkippedP2PDeleteLikeEntriesAreApplied) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store, "test-cluster");
    applier.Recover(1);

    auto remove_client = MakeUUID(1, 0);
    auto remove_seg = MakeUUID(10, 0);
    store.RegisterClient(remove_client, "192.168.1.100", 50051,
                         {MakeSegment(remove_seg, 4096)});
    store.AddReplica("late-remove-replica", remove_client, remove_seg, 1024, 1);
    SkipSequenceWithFutureAdd(applier, 2, "future-after-remove-replica");

    RemoveReplicaPayload remove_payload;
    remove_payload.object_key = "late-remove-replica";
    remove_payload.client_id = remove_client;
    remove_payload.segment_id = remove_seg;
    EXPECT_TRUE(applier.ApplyOpLogEntry(
        MakeRemoveReplicaEntry(2, "late-remove-replica", remove_payload)));
    EXPECT_EQ(store.GetObjects().count("late-remove-replica"), 0u);
    EXPECT_EQ(applier.GetExpectedSequenceId(), 4u);

    auto unmount_client = MakeUUID(2, 0);
    auto unmount_seg = MakeUUID(20, 0);
    store.RegisterClient(unmount_client, "192.168.1.101", 50052,
                         {MakeSegment(unmount_seg, 4096)});
    store.AddReplica("late-unmount-segment", unmount_client, unmount_seg, 1024,
                     3);
    SkipSequenceWithFutureAdd(applier, 4, "future-after-unmount-segment");

    UnmountSegmentPayload unmount_payload;
    unmount_payload.client_id = unmount_client;
    unmount_payload.segment_id = unmount_seg;
    EXPECT_TRUE(
        applier.ApplyOpLogEntry(MakeUnmountSegmentEntry(4, unmount_payload)));
    EXPECT_EQ(store.GetObjects().count("late-unmount-segment"), 0u);
    auto unmount_info = store.GetClient(unmount_client);
    ASSERT_NE(unmount_info, nullptr);
    EXPECT_TRUE(unmount_info->segments.empty());
    EXPECT_EQ(applier.GetExpectedSequenceId(), 6u);

    auto unregister_client = MakeUUID(3, 0);
    auto unregister_seg = MakeUUID(30, 0);
    store.RegisterClient(unregister_client, "192.168.1.102", 50053,
                         {MakeSegment(unregister_seg, 4096)});
    store.AddReplica("late-unregister-client", unregister_client,
                     unregister_seg, 1024, 5);
    SkipSequenceWithFutureAdd(applier, 6, "future-after-unregister-client");

    UnregisterClientPayload unregister_payload;
    unregister_payload.client_id = unregister_client;
    EXPECT_TRUE(applier.ApplyOpLogEntry(
        MakeUnregisterClientEntry(6, unregister_payload)));
    EXPECT_EQ(store.GetClient(unregister_client), nullptr);
    EXPECT_EQ(store.GetObjects().count("late-unregister-client"), 0u);
    EXPECT_EQ(applier.GetExpectedSequenceId(), 8u);
}

TEST(P2POpLogApplierTest, LateSkippedP2PAddLikeEntryIsDiscarded) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store, "test-cluster");
    applier.Recover(1);

    SkipSequenceWithFutureAdd(applier, 2, "future-after-add-like");

    AddReplicaPayload stale;
    stale.object_key = "late-add-replica";
    stale.client_id = MakeUUID(1, 0);
    stale.segment_id = MakeUUID(10, 0);
    stale.size = 1024;
    EXPECT_TRUE(
        applier.ApplyOpLogEntry(MakeAddReplicaEntry(2, "late-add", stale)));

    auto objects = store.GetObjects();
    EXPECT_EQ(objects.count("late-add-replica"), 0u);
    EXPECT_EQ(objects.count("future-after-add-like"), 1u);
    EXPECT_EQ(applier.GetExpectedSequenceId(), 4u);
}

// ============================================================================
// P2POpLogApplier - Unknown OpType
// ============================================================================

TEST(P2POpLogApplierTest, UnknownOpTypeReturnsFalse) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store, "test-cluster");

    // Use an OpType value that doesn't exist
    auto entry = MakeEntry(1, static_cast<OpType>(99), "key1", "");
    EXPECT_FALSE(applier.ApplyOpLogEntry(entry));
    EXPECT_FALSE(applier.IsHealthy());
    EXPECT_EQ(1u, applier.GetFailedSequenceId());
    EXPECT_EQ(99, applier.GetFailedOpType());
    EXPECT_EQ("operation apply failed", applier.GetFailureReason());
}

TEST(P2POpLogApplierTest, InvalidAddReplicaIsSkippedAsBestEffort) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store, "test-cluster");

    auto entry = MakeEntry(1, OpType_ADD_REPLICA, "key1", "invalid");
    EXPECT_TRUE(applier.ApplyOpLogEntry(entry));
    EXPECT_TRUE(applier.IsHealthy());
    EXPECT_EQ(2u, applier.GetExpectedSequenceId());
    EXPECT_EQ(0u, store.GetKeyCount());
}

TEST(P2POpLogApplierTest, FutureInvalidAddReplicaPreservesOrdering) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store, "test-cluster");

    auto invalid = MakeEntry(2, OpType_ADD_REPLICA, "key2", "invalid");
    EXPECT_FALSE(applier.ApplyOpLogEntry(invalid));
    EXPECT_EQ(1u, applier.GetExpectedSequenceId());

    EXPECT_TRUE(
        applier.ApplyOpLogEntry(MakeEntry(1, OpType::REMOVE, "key1", "")));
    EXPECT_EQ(3u, applier.GetExpectedSequenceId());
    EXPECT_TRUE(applier.IsHealthy());
}

}  // namespace mooncake::test

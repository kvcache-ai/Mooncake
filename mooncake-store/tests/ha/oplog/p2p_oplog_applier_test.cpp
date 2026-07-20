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
    payload.segments = {segment};

    auto entry = MakeRegisterClientEntry(1, payload);
    EXPECT_TRUE(applier.ApplyOpLogEntry(entry));

    auto info = store.GetClient(client);
    ASSERT_NE(info, nullptr);
    EXPECT_EQ(info->ip_address, "192.168.1.100");
    EXPECT_EQ(info->rpc_port, 50051u);
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

// ============================================================================
// P2POpLogApplier - Unknown OpType
// ============================================================================

TEST(P2POpLogApplierTest, UnknownOpTypeReturnsFalse) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store, "test-cluster");

    // Use an OpType value that doesn't exist
    auto entry = MakeEntry(1, static_cast<OpType>(99), "key1", "");
    EXPECT_FALSE(applier.ApplyOpLogEntry(entry));
}

}  // namespace mooncake::test

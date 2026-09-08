#include "p2p/ha/oplog/p2p_oplog_applier.h"

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <xxhash.h>

#include <cstdint>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include "p2p/ha/oplog/oplog_manager.h"
#include "p2p/ha/oplog/p2p_oplog_types.h"
#include "p2p/ha/oplog/p2p_standby_metadata_store.h"
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

// Helper to create a valid PUBLISH_ROUTE entry.
OpLogEntry MakePublishRouteEntry(uint64_t seq, const std::string& object_key,
                                 const PublishRoutePayload& payload) {
    std::string data = SerializeP2PPayload(payload);
    return MakeEntry(seq, OpType_PUBLISH_ROUTE, object_key, data);
}

// Helper to create a valid WITHDRAW_ROUTE entry.
OpLogEntry MakeWithdrawRouteEntry(uint64_t seq, const std::string& object_key,
                                  const WithdrawRoutePayload& payload) {
    std::string data = SerializeP2PPayload(payload);
    return MakeEntry(seq, OpType_WITHDRAW_ROUTE, object_key, data);
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

P2PSegment MakeSegment(const UUID& id, size_t size) {
    P2PSegment segment;
    segment.id = id;
    segment.size = size;
    return segment;
}

void SkipSequenceWithFutureAdd(P2POpLogApplier& applier,
                               uint64_t skipped_sequence_id,
                               const std::string& future_key) {
    PublishRoutePayload future;
    future.object_key = future_key;
    future.client_id = MakeUUID(1000 + skipped_sequence_id, 0);
    future.segment_id = MakeUUID(2000 + skipped_sequence_id, 0);
    future.size = 1024;

    ASSERT_FALSE(applier.ApplyOpLogEntry(
        MakePublishRouteEntry(skipped_sequence_id + 1, future_key, future)));
    applier.ConfirmMissingSequenceIds({skipped_sequence_id});
    EXPECT_EQ(applier.ProcessPendingEntries(), 1u);
    EXPECT_EQ(applier.GetExpectedSequenceId(), skipped_sequence_id + 2);
}
}  // namespace

// ============================================================================
// P2POpLogApplier - Basic Apply
// ============================================================================

TEST(P2POpLogApplierTest, ApplyPublishRoute) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store, "test-cluster");

    auto client = MakeUUID(1, 0);
    auto seg = MakeUUID(10, 0);
    PublishRoutePayload payload;
    payload.object_key = "model-weights";
    payload.client_id = client;
    payload.segment_id = seg;
    payload.size = 4096;

    auto entry = MakePublishRouteEntry(1, "model-weights", payload);
    EXPECT_TRUE(applier.ApplyOpLogEntry(entry));

    auto objects = store.GetRoutes();
    ASSERT_EQ(objects.size(), 1u);
    EXPECT_NE(objects.find("model-weights"), objects.end());
    EXPECT_EQ(objects.at("model-weights").locations.size(), 1u);
    EXPECT_EQ(objects.at("model-weights").last_sequence_id, 1u);
}

TEST(P2POpLogApplierTest, ApplyWithdrawRoute) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store, "test-cluster");

    auto client = MakeUUID(1, 0);
    auto seg = MakeUUID(10, 0);

    // First add, then remove
    PublishRoutePayload add_payload;
    add_payload.object_key = "key1";
    add_payload.client_id = client;
    add_payload.segment_id = seg;
    add_payload.size = 1024;
    EXPECT_TRUE(
        applier.ApplyOpLogEntry(MakePublishRouteEntry(1, "key1", add_payload)));

    WithdrawRoutePayload rm_payload;
    rm_payload.object_key = "key1";
    rm_payload.client_id = client;
    rm_payload.segment_id = seg;
    EXPECT_TRUE(
        applier.ApplyOpLogEntry(MakeWithdrawRouteEntry(2, "key1", rm_payload)));

    // Object should be removed (no replicas left)
    EXPECT_EQ(store.GetRouteKeyCount(), 0u);
}

TEST(P2POpLogApplierTest, RejectsCentralizedRemoveWithoutMutatingRoutes) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store, "test-cluster");

    // Centralized operations cannot mutate P2P route state.
    ASSERT_TRUE(store.PublishRoute(
        "key1", P2PRouteLocation{MakeUUID(1, 0), MakeUUID(10, 0)}, 100, 1));
    ASSERT_TRUE(store.GetRoute("key1").has_value());

    auto entry = MakeRemoveEntry(1, "key1");
    EXPECT_FALSE(applier.ApplyOpLogEntry(entry));
    EXPECT_TRUE(store.GetRoute("key1").has_value());
    EXPECT_FALSE(applier.IsHealthy());
}

TEST(P2POpLogApplierTest, PendingEntriesUseP2POperationDispatch) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store, "test-cluster");
    ASSERT_TRUE(store.PublishRoute(
        "key", P2PRouteLocation{MakeUUID(1, 0), MakeUUID(10, 0)}, 1024, 1));
    EXPECT_FALSE(applier.ApplyOpLogEntry(MakeRemoveEntry(2, "key")));

    UnregisterClientPayload first;
    first.client_id = MakeUUID(2, 0);
    EXPECT_TRUE(applier.ApplyOpLogEntry(MakeUnregisterClientEntry(1, first)));
    EXPECT_FALSE(applier.IsHealthy());
    EXPECT_EQ(applier.GetFailedSequenceId(), 2);
    EXPECT_EQ(applier.GetExpectedSequenceId(), 2);
    EXPECT_TRUE(store.RouteExists("key"));
}

TEST(P2POpLogApplierTest, PromotionGapsOnlyApplyP2PDeletes) {
    MockOpLogStore oplog;
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store, "test-cluster", &oplog);
    ASSERT_TRUE(store.PublishRoute(
        "key", P2PRouteLocation{MakeUUID(1, 0), MakeUUID(10, 0)}, 1024, 1));
    SkipSequenceWithFutureAdd(applier, 1, "future");
    ASSERT_EQ(oplog.WriteOpLog(MakeRemoveEntry(1, "key")), ErrorCode::OK);

    const auto gaps = applier.TryResolveGapsOnceForPromotion();
    EXPECT_EQ(gaps.fetched, 1);
    EXPECT_EQ(gaps.applied_deletes, 0);
    EXPECT_TRUE(applier.IsHealthy());
    EXPECT_TRUE(store.RouteExists("key"));
}

TEST(P2POpLogApplierTest, ApplyMountSegment) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store, "test-cluster");

    auto client = MakeUUID(1, 0);
    auto seg_id = MakeUUID(100, 0);

    P2PSegment segment;
    segment.id = seg_id;
    segment.size = 4096;

    MountSegmentPayload payload;
    payload.client_id = client;
    payload.segment = segment;

    auto entry = MakeMountSegmentEntry(1, payload);
    EXPECT_TRUE(applier.ApplyOpLogEntry(entry));

    auto info = store.GetClientInfo(client);
    ASSERT_TRUE(info.has_value());
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

    P2PSegment segment;
    segment.id = seg_id;
    segment.size = 4096;

    MountSegmentPayload mount_payload;
    mount_payload.client_id = client;
    mount_payload.segment = segment;
    applier.ApplyOpLogEntry(MakeMountSegmentEntry(1, mount_payload));

    // Add a replica on this segment
    PublishRoutePayload add_payload;
    add_payload.object_key = "key1";
    add_payload.client_id = client;
    add_payload.segment_id = seg_id;
    add_payload.size = 1024;
    applier.ApplyOpLogEntry(MakePublishRouteEntry(2, "key1", add_payload));

    ASSERT_EQ(store.GetRouteKeyCount(), 1u);

    // Unmount — should cascade delete replica
    UnmountSegmentPayload umount_payload;
    umount_payload.segment_id = seg_id;
    umount_payload.client_id = client;
    applier.ApplyOpLogEntry(MakeUnmountSegmentEntry(3, umount_payload));

    EXPECT_EQ(store.GetRouteKeyCount(), 0u);  // Object removed (no replicas)
    auto info = store.GetClientInfo(client);
    ASSERT_TRUE(info.has_value());
    EXPECT_EQ(info->segments.size(), 0u);  // Segment removed
}

TEST(P2POpLogApplierTest, ApplyRegisterClient) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store, "test-cluster");

    auto client = MakeUUID(1, 0);
    auto seg_id = MakeUUID(100, 0);

    P2PSegment segment;
    segment.id = seg_id;
    segment.size = 2048;

    RegisterClientPayload payload;
    payload.client_id = client;
    payload.ip_address = "192.168.1.100";
    payload.rpc_port = 50051;
    payload.segments = {segment};

    auto entry = MakeRegisterClientEntry(1, payload);
    EXPECT_TRUE(applier.ApplyOpLogEntry(entry));

    auto info = store.GetClientInfo(client);
    ASSERT_TRUE(info.has_value());
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

    PublishRoutePayload replica_payload;
    replica_payload.object_key = "shared-key";
    replica_payload.client_id = client;
    replica_payload.segment_id = seg;
    replica_payload.size = 1024;
    EXPECT_TRUE(applier.ApplyOpLogEntry(
        MakePublishRouteEntry(3, "shared-key", replica_payload)));

    PublishRoutePayload other_replica_payload;
    other_replica_payload.object_key = "shared-key";
    other_replica_payload.client_id = other_client;
    other_replica_payload.segment_id = other_seg;
    other_replica_payload.size = 1024;
    EXPECT_TRUE(applier.ApplyOpLogEntry(
        MakePublishRouteEntry(4, "shared-key", other_replica_payload)));

    UnregisterClientPayload unregister_payload;
    unregister_payload.client_id = client;
    EXPECT_TRUE(applier.ApplyOpLogEntry(
        MakeUnregisterClientEntry(5, unregister_payload)));

    EXPECT_FALSE(store.GetClientInfo(client).has_value());
    ASSERT_TRUE(store.GetClientInfo(other_client).has_value());
    auto objects = store.GetRoutes();
    auto object_it = objects.find("shared-key");
    ASSERT_NE(object_it, objects.end());
    ASSERT_EQ(object_it->second.locations.size(), 1u);
    const auto& p2p = object_it->second.locations[0];
    EXPECT_EQ(p2p.client_id, other_client);
}

// ============================================================================
// P2POpLogApplier - Snapshot replay idempotency
// ============================================================================

TEST(P2POpLogApplierTest, ReplayPublishRouteAlreadyInSnapshotIsNoOp) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store, "test-cluster");

    auto client = MakeUUID(1, 0);
    auto seg = MakeUUID(10, 0);
    store.RegisterClient(client, "192.168.1.100", 50051,
                         {MakeSegment(seg, 4096)});
    ASSERT_TRUE(store.PublishRoute("snapshot-key",
                                   P2PRouteLocation{client, seg}, 1024, 101));
    applier.Recover(100);

    PublishRoutePayload payload;
    payload.object_key = "snapshot-key";
    payload.client_id = client;
    payload.segment_id = seg;
    payload.size = 1024;
    EXPECT_TRUE(applier.ApplyOpLogEntry(
        MakePublishRouteEntry(101, "snapshot-key", payload)));

    auto objects = store.GetRoutes();
    ASSERT_EQ(objects.size(), 1u);
    ASSERT_EQ(objects.at("snapshot-key").locations.size(), 1u);
    EXPECT_EQ(objects.at("snapshot-key").last_sequence_id, 101u);
    EXPECT_EQ(applier.GetExpectedSequenceId(), 102u);
}

TEST(P2POpLogApplierTest, ReplayMountSegmentAlreadyInSnapshotIsNoOp) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store, "test-cluster");

    auto client = MakeUUID(1, 0);
    auto seg = MakeUUID(10, 0);
    P2PSegment segment = MakeSegment(seg, 4096);
    store.RegisterClient(client, "192.168.1.100", 50051, {segment});
    applier.Recover(100);

    MountSegmentPayload payload;
    payload.client_id = client;
    payload.segment = segment;
    EXPECT_TRUE(applier.ApplyOpLogEntry(MakeMountSegmentEntry(101, payload)));

    auto info = store.GetClientInfo(client);
    ASSERT_TRUE(info.has_value());
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
    P2PSegment registration_segment = MakeSegment(registration_seg, 4096);
    P2PSegment later_segment = MakeSegment(later_seg, 8192);
    store.RegisterClient(client, "192.168.1.100", 50051,
                         {registration_segment, later_segment});
    applier.Recover(100);

    RegisterClientPayload payload;
    payload.client_id = client;
    payload.ip_address = "192.168.1.100";
    payload.rpc_port = 50051;
    payload.segments = {registration_segment};
    EXPECT_TRUE(applier.ApplyOpLogEntry(MakeRegisterClientEntry(101, payload)));

    auto info = store.GetClientInfo(client);
    ASSERT_TRUE(info.has_value());
    EXPECT_EQ(info->ip_address, "192.168.1.100");
    EXPECT_EQ(info->rpc_port, 50051u);
    ASSERT_EQ(info->segments.size(), 2u);
    EXPECT_EQ(info->segments[0].id, registration_seg);
    EXPECT_EQ(info->segments[1].id, later_seg);
    EXPECT_EQ(applier.GetExpectedSequenceId(), 102u);
}

TEST(P2POpLogApplierTest, ReplayWithdrawRouteAlreadyReflectedInSnapshotIsNoOp) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store, "test-cluster");
    applier.Recover(100);

    WithdrawRoutePayload payload;
    payload.object_key = "removed-key";
    payload.client_id = MakeUUID(1, 0);
    payload.segment_id = MakeUUID(10, 0);
    EXPECT_TRUE(applier.ApplyOpLogEntry(
        MakeWithdrawRouteEntry(101, "removed-key", payload)));

    EXPECT_EQ(store.GetRouteKeyCount(), 0u);
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

    EXPECT_EQ(store.GetRouteKeyCount(), 0u);
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

    EXPECT_FALSE(store.GetClientInfo(client).has_value());
    EXPECT_EQ(store.GetRouteKeyCount(), 0u);
    EXPECT_EQ(applier.GetExpectedSequenceId(), 102u);
}

TEST(P2POpLogApplierTest, AlreadyAppliedP2PSequenceIsNoOp) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store, "test-cluster");

    auto client = MakeUUID(1, 0);
    auto seg = MakeUUID(10, 0);

    PublishRoutePayload first;
    first.object_key = "committed-key";
    first.client_id = client;
    first.segment_id = seg;
    first.size = 1024;
    EXPECT_TRUE(applier.ApplyOpLogEntry(
        MakePublishRouteEntry(1, "committed-key", first)));
    EXPECT_EQ(applier.GetExpectedSequenceId(), 2u);

    PublishRoutePayload stale;
    stale.object_key = "stale-key";
    stale.client_id = MakeUUID(2, 0);
    stale.segment_id = MakeUUID(20, 0);
    stale.size = 2048;
    EXPECT_TRUE(
        applier.ApplyOpLogEntry(MakePublishRouteEntry(1, "stale-key", stale)));

    auto objects = store.GetRoutes();
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
    PublishRoutePayload p1;
    p1.object_key = "key1";
    p1.client_id = client;
    p1.segment_id = seg;
    p1.size = 1024;

    PublishRoutePayload p2;
    p2.object_key = "key2";
    p2.client_id = client;
    p2.segment_id = seg;
    p2.size = 2048;

    EXPECT_TRUE(applier.ApplyOpLogEntry(MakePublishRouteEntry(1, "key1", p1)));
    EXPECT_TRUE(applier.ApplyOpLogEntry(MakePublishRouteEntry(2, "key2", p2)));

    EXPECT_EQ(store.GetRouteKeyCount(), 2u);
    EXPECT_EQ(applier.GetExpectedSequenceId(), 3u);
}

TEST(P2POpLogApplierTest, OutOfOrderEntryRejected) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store, "test-cluster");

    auto client = MakeUUID(1, 0);
    auto seg = MakeUUID(10, 0);

    PublishRoutePayload p2;
    p2.object_key = "key2";
    p2.client_id = client;
    p2.segment_id = seg;
    p2.size = 2048;

    // Entry seq=2 arrives before seq=1 — should be rejected, not applied
    EXPECT_FALSE(applier.ApplyOpLogEntry(MakePublishRouteEntry(2, "key2", p2)));

    EXPECT_EQ(store.GetRouteKeyCount(), 0u);  // Not applied yet
    EXPECT_EQ(applier.GetExpectedSequenceId(), 1u);
}

TEST(P2POpLogApplierTest, OutOfOrderEntryBufferedThenGapFillDrainsIt) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store, "test-cluster");

    auto client = MakeUUID(1, 0);
    auto seg = MakeUUID(10, 0);

    PublishRoutePayload p1;
    p1.object_key = "key1";
    p1.client_id = client;
    p1.segment_id = seg;
    p1.size = 1024;

    PublishRoutePayload p2;
    p2.object_key = "key2";
    p2.client_id = client;
    p2.segment_id = seg;
    p2.size = 2048;

    // seq=2 first — reported pending and buffered by the common applier.
    EXPECT_FALSE(applier.ApplyOpLogEntry(MakePublishRouteEntry(2, "key2", p2)));
    EXPECT_EQ(store.GetRouteKeyCount(), 0u);

    // seq=1 fills the gap; the common pending queue then applies seq=2.
    EXPECT_TRUE(applier.ApplyOpLogEntry(MakePublishRouteEntry(1, "key1", p1)));
    EXPECT_EQ(store.GetRouteKeyCount(), 2u);
    EXPECT_EQ(applier.GetExpectedSequenceId(), 3u);
}

TEST(P2POpLogApplierTest, MissingEntryTimeoutSkipsGapAndDrainsP2PEntry) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store, "test-cluster");

    PublishRoutePayload payload;
    payload.object_key = "after-gap";
    payload.client_id = MakeUUID(1, 0);
    payload.segment_id = MakeUUID(10, 0);
    payload.size = 1024;

    EXPECT_FALSE(applier.ApplyOpLogEntry(
        MakePublishRouteEntry(2, "after-gap", payload)));
    applier.ProcessPendingEntries();  // start the common gap timer
    std::this_thread::sleep_for(std::chrono::milliseconds(3100));
    EXPECT_EQ(1u, applier.ProcessPendingEntries());
    EXPECT_EQ(1u, store.GetRouteKeyCount());
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
    ASSERT_TRUE(store.PublishRoute("late-remove-replica",
                                   P2PRouteLocation{remove_client, remove_seg},
                                   1024, 1));
    SkipSequenceWithFutureAdd(applier, 2, "future-after-remove-replica");

    WithdrawRoutePayload remove_payload;
    remove_payload.object_key = "late-remove-replica";
    remove_payload.client_id = remove_client;
    remove_payload.segment_id = remove_seg;
    EXPECT_TRUE(applier.ApplyOpLogEntry(
        MakeWithdrawRouteEntry(2, "late-remove-replica", remove_payload)));
    EXPECT_EQ(store.GetRoutes().count("late-remove-replica"), 0u);
    EXPECT_EQ(applier.GetExpectedSequenceId(), 4u);

    auto unmount_client = MakeUUID(2, 0);
    auto unmount_seg = MakeUUID(20, 0);
    store.RegisterClient(unmount_client, "192.168.1.101", 50052,
                         {MakeSegment(unmount_seg, 4096)});
    ASSERT_TRUE(store.PublishRoute(
        "late-unmount-segment", P2PRouteLocation{unmount_client, unmount_seg},
        1024, 3));
    SkipSequenceWithFutureAdd(applier, 4, "future-after-unmount-segment");

    UnmountSegmentPayload unmount_payload;
    unmount_payload.client_id = unmount_client;
    unmount_payload.segment_id = unmount_seg;
    EXPECT_TRUE(
        applier.ApplyOpLogEntry(MakeUnmountSegmentEntry(4, unmount_payload)));
    EXPECT_EQ(store.GetRoutes().count("late-unmount-segment"), 0u);
    auto unmount_info = store.GetClientInfo(unmount_client);
    ASSERT_TRUE(unmount_info.has_value());
    EXPECT_TRUE(unmount_info->segments.empty());
    EXPECT_EQ(applier.GetExpectedSequenceId(), 6u);

    auto unregister_client = MakeUUID(3, 0);
    auto unregister_seg = MakeUUID(30, 0);
    store.RegisterClient(unregister_client, "192.168.1.102", 50053,
                         {MakeSegment(unregister_seg, 4096)});
    ASSERT_TRUE(store.PublishRoute(
        "late-unregister-client",
        P2PRouteLocation{unregister_client, unregister_seg}, 1024, 5));
    SkipSequenceWithFutureAdd(applier, 6, "future-after-unregister-client");

    UnregisterClientPayload unregister_payload;
    unregister_payload.client_id = unregister_client;
    EXPECT_TRUE(applier.ApplyOpLogEntry(
        MakeUnregisterClientEntry(6, unregister_payload)));
    EXPECT_FALSE(store.GetClientInfo(unregister_client).has_value());
    EXPECT_EQ(store.GetRoutes().count("late-unregister-client"), 0u);
    EXPECT_EQ(applier.GetExpectedSequenceId(), 8u);
}

TEST(P2POpLogApplierTest, LateSkippedP2PAddLikeEntryIsDiscarded) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store, "test-cluster");
    applier.Recover(1);

    SkipSequenceWithFutureAdd(applier, 2, "future-after-add-like");

    PublishRoutePayload stale;
    stale.object_key = "late-add-replica";
    stale.client_id = MakeUUID(1, 0);
    stale.segment_id = MakeUUID(10, 0);
    stale.size = 1024;
    EXPECT_TRUE(
        applier.ApplyOpLogEntry(MakePublishRouteEntry(2, "late-add", stale)));

    auto objects = store.GetRoutes();
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

TEST(P2POpLogApplierTest, InvalidPublishRouteIsSkippedAsBestEffort) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store, "test-cluster");

    auto entry = MakeEntry(1, OpType_PUBLISH_ROUTE, "key1", "invalid");
    EXPECT_TRUE(applier.ApplyOpLogEntry(entry));
    EXPECT_TRUE(applier.IsHealthy());
    EXPECT_EQ(2u, applier.GetExpectedSequenceId());
    EXPECT_EQ(0u, store.GetRouteKeyCount());
}

TEST(P2POpLogApplierTest, FutureInvalidPublishRoutePreservesOrdering) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store, "test-cluster");

    auto invalid = MakeEntry(2, OpType_PUBLISH_ROUTE, "key2", "invalid");
    EXPECT_FALSE(applier.ApplyOpLogEntry(invalid));
    EXPECT_EQ(1u, applier.GetExpectedSequenceId());

    EXPECT_TRUE(applier.ApplyOpLogEntry(
        MakeEntry(1, OpType_UNREGISTER_CLIENT, "",
                  SerializeP2PPayload(UnregisterClientPayload{}))));
    EXPECT_EQ(3u, applier.GetExpectedSequenceId());
    EXPECT_TRUE(applier.IsHealthy());
}

TEST(P2POpLogApplierTest, PublishSizeMismatchRemainsBestEffort) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store, "test-cluster");
    PublishRoutePayload payload;
    payload.object_key = "key";
    payload.size = 1024;
    ASSERT_TRUE(
        applier.ApplyOpLogEntry(MakePublishRouteEntry(1, "key", payload)));
    payload.size = 2048;
    EXPECT_TRUE(
        applier.ApplyOpLogEntry(MakePublishRouteEntry(2, "key", payload)));
    EXPECT_TRUE(applier.IsHealthy());
    EXPECT_EQ(applier.GetExpectedSequenceId(), 3);
    ASSERT_TRUE(store.GetRoute("key").has_value());
    EXPECT_EQ(store.GetRoute("key")->object_size, 1024);
}

TEST(P2POpLogApplierTest, UnmountUsesClientAndSegmentIdentity) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store, "test-cluster");
    const UUID segment{9, 9};
    const P2PRouteLocation first{{1, 1}, segment};
    const P2PRouteLocation second{{2, 2}, segment};
    ASSERT_TRUE(store.PublishRoute("key", first, 1024, 1));
    ASSERT_TRUE(store.PublishRoute("key", second, 1024, 1));
    UnmountSegmentPayload unmount;
    unmount.client_id = first.client_id;
    unmount.segment_id = segment;
    ASSERT_TRUE(applier.ApplyOpLogEntry(MakeUnmountSegmentEntry(1, unmount)));
    const auto route = store.GetRoute("key");
    ASSERT_TRUE(route.has_value());
    ASSERT_EQ(route->locations.size(), 1);
    EXPECT_EQ(route->locations.front(), second);
}

}  // namespace mooncake::test

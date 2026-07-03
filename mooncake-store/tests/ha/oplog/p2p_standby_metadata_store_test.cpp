#include "ha/oplog/p2p_standby_metadata_store.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "replica.h"
#include "types.h"

namespace mooncake::test {

namespace {
// Helper to create a UUID from two uint64_t values.
UUID MakeUUID(uint64_t hi, uint64_t lo) { return UUID{hi, lo}; }

// Helper to create a basic Segment.
Segment MakeSegment(const UUID& id, size_t size = 1024) {
    Segment seg;
    seg.id = id;
    seg.size = size;
    return seg;
}
}  // namespace

// ============================================================================
// P2PStandbyMetadataStore - Basic MetadataStore interface
// ============================================================================

TEST(P2PStandbyMetadataStoreTest, PutMetadataIsCompatibilityNoop) {
    P2PStandbyMetadataStore store;
    StandbyObjectMetadata meta;
    meta.size = 4096;
    meta.last_sequence_id = 42;

    EXPECT_TRUE(store.PutMetadata("key1", meta));
    EXPECT_FALSE(store.GetMetadata("key1").has_value());
    EXPECT_EQ(store.GetKeyCount(), 0u);
}

TEST(P2PStandbyMetadataStoreTest, GetMetadataNotFound) {
    P2PStandbyMetadataStore store;
    auto result = store.GetMetadata("nonexistent");
    EXPECT_FALSE(result.has_value());
}

TEST(P2PStandbyMetadataStoreTest, Remove) {
    P2PStandbyMetadataStore store;
    StandbyObjectMetadata meta;
    meta.size = 100;
    store.AddReplica("key1", MakeUUID(1, 0), MakeUUID(10, 0), 100, 0, {},
                     MemoryType::DRAM);

    EXPECT_TRUE(store.Remove("key1"));
    EXPECT_FALSE(store.GetMetadata("key1").has_value());
    EXPECT_FALSE(store.Remove("key1"));  // Already removed
}

TEST(P2PStandbyMetadataStoreTest, Exists) {
    P2PStandbyMetadataStore store;
    EXPECT_FALSE(store.Exists("key1"));
    store.AddReplica("key1", MakeUUID(1, 0), MakeUUID(10, 0), 100, 0, {},
                     MemoryType::DRAM);
    EXPECT_TRUE(store.Exists("key1"));
}

TEST(P2PStandbyMetadataStoreTest, GetKeyCount) {
    P2PStandbyMetadataStore store;
    EXPECT_EQ(store.GetKeyCount(), 0u);

    store.AddReplica("key1", MakeUUID(1, 0), MakeUUID(10, 0), 100, 0, {},
                     MemoryType::DRAM);
    EXPECT_EQ(store.GetKeyCount(), 1u);
    store.AddReplica("key2", MakeUUID(1, 0), MakeUUID(11, 0), 200, 0, {},
                     MemoryType::DRAM);
    EXPECT_EQ(store.GetKeyCount(), 2u);
    store.Remove("key1");
    EXPECT_EQ(store.GetKeyCount(), 1u);
}

// ============================================================================
// AddReplica / RemoveReplica
// ============================================================================

TEST(P2PStandbyMetadataStoreTest, PutIsCompatibilityNoop) {
    P2PStandbyMetadataStore store;
    EXPECT_TRUE(store.Put("key1"));
    EXPECT_FALSE(store.GetMetadata("key1").has_value());
    EXPECT_EQ(store.GetKeyCount(), 0u);
}

TEST(P2PStandbyMetadataStoreTest, AddReplicaAfterClientRegistered) {
    P2PStandbyMetadataStore store;
    auto client_id = MakeUUID(1, 0);
    auto seg_id = MakeUUID(10, 0);

    // Register client first, then add replica
    store.RegisterClient(client_id, "10.0.0.1", 50051, {});
    store.AddReplica("key1", client_id, seg_id, 1024, 0, {}, MemoryType::DRAM);

    auto it = store.GetObjects().find("key1");
    ASSERT_NE(it, store.GetObjects().end());
    ASSERT_EQ(it->second.replicas.size(), 1u);
    auto& p2p =
        std::get<P2PProxyDescriptor>(it->second.replicas[0].descriptor_variant);
    // IP/port should be populated since client was registered first
    EXPECT_EQ(p2p.ip_address, "10.0.0.1");
    EXPECT_EQ(p2p.rpc_port, 50051u);
}

TEST(P2PStandbyMetadataStoreTest, AddReplicaUpdatesSegmentExtra) {
    P2PStandbyMetadataStore store;
    auto client_id = MakeUUID(1, 0);
    auto seg_id = MakeUUID(10, 0);
    auto seg = MakeSegment(seg_id, 4096);
    seg.GetP2PExtra().usage = 256;
    store.RegisterClient(client_id, "10.0.0.1", 50051, {seg});

    store.AddReplica("key1", client_id, seg_id, 1024, 7, {"hot", "ssd"},
                     MemoryType::NVME);

    auto* info = store.GetClient(client_id);
    ASSERT_NE(info, nullptr);
    ASSERT_EQ(info->segments.size(), 1u);
    ASSERT_TRUE(info->segments[0].IsP2PSegment());
    const auto& extra = info->segments[0].GetP2PExtra();
    EXPECT_EQ(extra.priority, 7);
    EXPECT_EQ(extra.tags, std::vector<std::string>({"hot", "ssd"}));
    EXPECT_EQ(extra.memory_type, MemoryType::NVME);
    EXPECT_EQ(extra.usage, 256u);
}

TEST(P2PStandbyMetadataStoreTest, AddReplicaCreatesObject) {
    P2PStandbyMetadataStore store;
    auto client_id = MakeUUID(1, 0);
    auto seg_id = MakeUUID(10, 0);

    store.AddReplica("model-weights", client_id, seg_id, 4096, 0, {},
                     MemoryType::DRAM);

    const auto& objects = store.GetObjects();
    ASSERT_EQ(objects.size(), 1u);
    auto it = objects.find("model-weights");
    ASSERT_NE(it, objects.end());
    EXPECT_EQ(it->second.replicas.size(), 1u);
}

TEST(P2PStandbyMetadataStoreTest, AddReplicaMultiple) {
    P2PStandbyMetadataStore store;
    auto client1 = MakeUUID(1, 0);
    auto client2 = MakeUUID(2, 0);
    auto seg1 = MakeUUID(10, 0);
    auto seg2 = MakeUUID(11, 0);

    store.AddReplica("key1", client1, seg1, 1024, 0, {}, MemoryType::DRAM);
    store.AddReplica("key1", client2, seg2, 2048, 0, {}, MemoryType::DRAM);

    auto it = store.GetObjects().find("key1");
    ASSERT_NE(it, store.GetObjects().end());
    EXPECT_EQ(it->second.replicas.size(), 2u);
}

TEST(P2PStandbyMetadataStoreTest, AddReplicaDuplicateIgnored) {
    P2PStandbyMetadataStore store;
    auto client = MakeUUID(1, 0);
    auto seg = MakeUUID(10, 0);

    store.AddReplica("key1", client, seg, 1024, 0, {}, MemoryType::DRAM);
    store.AddReplica("key1", client, seg, 1024, 0, {}, MemoryType::DRAM);

    auto it = store.GetObjects().find("key1");
    ASSERT_NE(it, store.GetObjects().end());
    EXPECT_EQ(it->second.replicas.size(), 1u);
}

TEST(P2PStandbyMetadataStoreTest, RemoveReplica) {
    P2PStandbyMetadataStore store;
    auto client1 = MakeUUID(1, 0);
    auto client2 = MakeUUID(2, 0);
    auto seg1 = MakeUUID(10, 0);
    auto seg2 = MakeUUID(11, 0);

    store.AddReplica("key1", client1, seg1, 1024, 0, {}, MemoryType::DRAM);
    store.AddReplica("key1", client2, seg2, 2048, 0, {}, MemoryType::DRAM);

    // Remove one replica
    store.RemoveReplica("key1", client1, seg1);

    auto it = store.GetObjects().find("key1");
    ASSERT_NE(it, store.GetObjects().end());
    EXPECT_EQ(it->second.replicas.size(), 1u);
}

TEST(P2PStandbyMetadataStoreTest, RemoveReplicaRemovesEmptyObject) {
    P2PStandbyMetadataStore store;
    auto client = MakeUUID(1, 0);
    auto seg = MakeUUID(10, 0);

    store.AddReplica("key1", client, seg, 1024, 0, {}, MemoryType::DRAM);
    store.RemoveReplica("key1", client, seg);

    // Object with no replicas should be removed
    EXPECT_FALSE(store.GetMetadata("key1").has_value());
    EXPECT_EQ(store.GetKeyCount(), 0u);
}

TEST(P2PStandbyMetadataStoreTest, RemoveReplicaNonexistentKey) {
    P2PStandbyMetadataStore store;
    auto client = MakeUUID(1, 0);
    auto seg = MakeUUID(10, 0);

    // Should not crash
    store.RemoveReplica("nonexistent", client, seg);
    EXPECT_EQ(store.GetKeyCount(), 0u);
}

// ============================================================================
// RegisterClient
// ============================================================================

TEST(P2PStandbyMetadataStoreTest, RegisterClientBasic) {
    P2PStandbyMetadataStore store;
    auto client_id = MakeUUID(1, 0);
    std::vector<Segment> segments;
    segments.push_back(MakeSegment(MakeUUID(100, 0)));

    store.RegisterClient(client_id, "192.168.1.1", 50051, segments);

    auto* info = store.GetClient(client_id);
    ASSERT_NE(info, nullptr);
    EXPECT_EQ(info->ip_address, "192.168.1.1");
    EXPECT_EQ(info->rpc_port, 50051u);
    EXPECT_EQ(info->segments.size(), 1u);
}

TEST(P2PStandbyMetadataStoreTest, RegisterClientUpdatesExisting) {
    P2PStandbyMetadataStore store;
    auto client_id = MakeUUID(1, 0);

    store.RegisterClient(client_id, "192.168.1.1", 50051, {});
    store.RegisterClient(client_id, "10.0.0.1", 50052,
                         {MakeSegment(MakeUUID(100, 0))});

    auto* info = store.GetClient(client_id);
    ASSERT_NE(info, nullptr);
    EXPECT_EQ(info->ip_address, "10.0.0.1");
    EXPECT_EQ(info->rpc_port, 50052u);
    EXPECT_EQ(info->segments.size(), 1u);
}

TEST(P2PStandbyMetadataStoreTest, RegisterClientPreservesEarlyMountSegment) {
    P2PStandbyMetadataStore store;
    auto client_id = MakeUUID(1, 0);
    auto early_seg_id = MakeUUID(50, 0);

    // MOUNT_SEGMENT arrives before REGISTER_CLIENT
    Segment early_seg;
    early_seg.id = early_seg_id;
    early_seg.size = 2048;
    store.AddSegment(client_id, early_seg);

    // REGISTER_CLIENT with a different segment
    auto reg_seg = MakeSegment(MakeUUID(100, 0));
    store.RegisterClient(client_id, "192.168.1.1", 50051, {reg_seg});

    // Both segments should be preserved (merge, not overwrite)
    auto* info = store.GetClient(client_id);
    ASSERT_NE(info, nullptr);
    EXPECT_EQ(info->segments.size(), 2u);
}

TEST(P2PStandbyMetadataStoreTest, RegisterClientDuplicateSegmentIgnored) {
    P2PStandbyMetadataStore store;
    auto client_id = MakeUUID(1, 0);
    auto seg_id = MakeUUID(100, 0);

    // MOUNT_SEGMENT first
    Segment seg = MakeSegment(seg_id, 4096);
    store.AddSegment(client_id, seg);

    // REGISTER_CLIENT with same segment — should not duplicate
    store.RegisterClient(client_id, "10.0.0.1", 50051, {seg});

    auto* info = store.GetClient(client_id);
    ASSERT_NE(info, nullptr);
    EXPECT_EQ(info->segments.size(), 1u);
}

TEST(P2PStandbyMetadataStoreTest, RegisterClientUpdatesReplicaIPs) {
    P2PStandbyMetadataStore store;
    auto client_id = MakeUUID(1, 0);
    auto seg_id = MakeUUID(10, 0);

    // Add replica BEFORE client is registered (ip/port unknown)
    store.AddReplica("key1", client_id, seg_id, 1024, 0, {}, MemoryType::DRAM);

    // Verify replica has empty ip/port (backfilling deferred to ExportMetadata)
    auto it = store.GetObjects().find("key1");
    ASSERT_NE(it, store.GetObjects().end());
    ASSERT_EQ(it->second.replicas.size(), 1u);
    auto& desc = it->second.replicas[0];
    auto& p2p = std::get<P2PProxyDescriptor>(desc.descriptor_variant);
    EXPECT_TRUE(p2p.ip_address.empty());
    EXPECT_EQ(p2p.rpc_port, 0u);

    // Register client — does NOT update replicas in-place (deferred)
    store.RegisterClient(client_id, "192.168.1.1", 50051, {});

    // In-place replicas still have empty ip/port
    it = store.GetObjects().find("key1");
    ASSERT_NE(it, store.GetObjects().end());
    auto& in_place_p2p =
        std::get<P2PProxyDescriptor>(it->second.replicas[0].descriptor_variant);
    EXPECT_TRUE(in_place_p2p.ip_address.empty());
    EXPECT_EQ(in_place_p2p.rpc_port, 0u);

    // Verify IP/port is backfilled in exported metadata (during promotion)
    auto exported = store.ExportMetadata();
    auto obj_it = exported.objects.find("key1");
    ASSERT_NE(obj_it, exported.objects.end());
    ASSERT_EQ(obj_it->second.replicas.size(), 1u);
    auto& exported_p2p = std::get<P2PProxyDescriptor>(
        obj_it->second.replicas[0].descriptor_variant);
    EXPECT_EQ(exported_p2p.ip_address, "192.168.1.1");
    EXPECT_EQ(exported_p2p.rpc_port, 50051u);
}

// ============================================================================
// UnRegisterClient
// ============================================================================

TEST(P2PStandbyMetadataStoreTest, UnRegisterClientUnknownClient) {
    P2PStandbyMetadataStore store;
    auto client_id = MakeUUID(1, 0);

    // UnRegisterClient with unregistered client — should not crash
    store.UnRegisterClient(client_id);

    EXPECT_EQ(store.GetClient(client_id), nullptr);
    EXPECT_EQ(store.GetKeyCount(), 0u);
}

TEST(P2PStandbyMetadataStoreTest, UnRegisterClientRemovesClientInfo) {
    P2PStandbyMetadataStore store;
    auto client_id = MakeUUID(1, 0);
    auto seg_id = MakeUUID(10, 0);

    store.RegisterClient(client_id, "10.0.0.1", 50051,
                         {MakeSegment(seg_id, 1024)});
    ASSERT_NE(store.GetClient(client_id), nullptr);

    store.UnRegisterClient(client_id);

    EXPECT_EQ(store.GetClient(client_id), nullptr);
    EXPECT_TRUE(store.GetClients().empty());
}

TEST(P2PStandbyMetadataStoreTest, UnRegisterClientCascadeDeletesReplicas) {
    P2PStandbyMetadataStore store;
    auto client_id = MakeUUID(1, 0);
    auto seg1 = MakeUUID(10, 0);
    auto seg2 = MakeUUID(11, 0);

    store.RegisterClient(client_id, "10.0.0.1", 50051,
                         {MakeSegment(seg1, 1024), MakeSegment(seg2, 2048)});
    store.AddReplica("key1", client_id, seg1, 1024, 0, {}, MemoryType::DRAM);
    store.AddReplica("key2", client_id, seg2, 2048, 0, {}, MemoryType::DRAM);
    ASSERT_EQ(store.GetKeyCount(), 2u);

    store.UnRegisterClient(client_id);

    EXPECT_EQ(store.GetClient(client_id), nullptr);
    EXPECT_EQ(store.GetKeyCount(), 0u);
}

TEST(P2PStandbyMetadataStoreTest, UnRegisterClientPreservesOtherClients) {
    P2PStandbyMetadataStore store;
    auto client1 = MakeUUID(1, 0);
    auto client2 = MakeUUID(2, 0);
    auto seg1 = MakeUUID(10, 0);
    auto seg2 = MakeUUID(20, 0);

    store.RegisterClient(client1, "10.0.0.1", 50051, {MakeSegment(seg1, 1024)});
    store.RegisterClient(client2, "10.0.0.2", 50052, {MakeSegment(seg2, 2048)});
    store.AddReplica("shared-key", client1, seg1, 1024, 0, {},
                     MemoryType::DRAM);
    store.AddReplica("shared-key", client2, seg2, 2048, 0, {},
                     MemoryType::DRAM);
    store.AddReplica("client1-only", client1, seg1, 512, 0, {},
                     MemoryType::DRAM);

    store.UnRegisterClient(client1);

    EXPECT_EQ(store.GetClient(client1), nullptr);
    ASSERT_NE(store.GetClient(client2), nullptr);
    EXPECT_EQ(store.GetKeyCount(), 1u);

    auto shared_it = store.GetObjects().find("shared-key");
    ASSERT_NE(shared_it, store.GetObjects().end());
    ASSERT_EQ(shared_it->second.replicas.size(), 1u);
    const auto& p2p = std::get<P2PProxyDescriptor>(
        shared_it->second.replicas[0].descriptor_variant);
    EXPECT_EQ(p2p.client_id, client2);
    EXPECT_EQ(store.GetObjects().find("client1-only"),
              store.GetObjects().end());
}

// ============================================================================
// AddSegment / RemoveSegment
// ============================================================================

TEST(P2PStandbyMetadataStoreTest, AddSegmentDuplicateIgnored) {
    P2PStandbyMetadataStore store;
    auto client_id = MakeUUID(1, 0);
    auto seg_id = MakeUUID(100, 0);
    Segment seg = MakeSegment(seg_id, 4096);

    store.AddSegment(client_id, seg);
    store.AddSegment(client_id, seg);  // duplicate — should be ignored

    auto* info = store.GetClient(client_id);
    ASSERT_NE(info, nullptr);
    EXPECT_EQ(info->segments.size(), 1u);
}

TEST(P2PStandbyMetadataStoreTest, RemoveSegmentUnknownClient) {
    P2PStandbyMetadataStore store;
    auto seg_id = MakeUUID(100, 0);
    auto client_id = MakeUUID(1, 0);

    // RemoveSegment with unregistered client — should not crash
    store.RemoveSegment(seg_id, client_id);
    EXPECT_EQ(store.GetKeyCount(), 0u);
}

TEST(P2PStandbyMetadataStoreTest, AddAndRemoveSegment) {
    P2PStandbyMetadataStore store;
    auto client_id = MakeUUID(1, 0);
    auto seg_id = MakeUUID(100, 0);
    Segment seg = MakeSegment(seg_id, 4096);

    store.AddSegment(client_id, seg);

    auto* info = store.GetClient(client_id);
    ASSERT_NE(info, nullptr);
    EXPECT_EQ(info->segments.size(), 1u);
    EXPECT_EQ(info->segments[0].id, seg_id);

    store.RemoveSegment(seg_id, client_id);

    info = store.GetClient(client_id);
    ASSERT_NE(info, nullptr);
    EXPECT_EQ(info->segments.size(), 0u);
}

TEST(P2PStandbyMetadataStoreTest, RemoveSegmentCascadeDeletesReplicas) {
    P2PStandbyMetadataStore store;
    auto client_id = MakeUUID(1, 0);
    auto seg_id = MakeUUID(100, 0);

    // Add replica on this segment
    store.AddReplica("key1", client_id, seg_id, 1024, 0, {}, MemoryType::DRAM);
    ASSERT_EQ(store.GetObjects().size(), 1u);

    // Unmount the segment — should cascade delete replicas
    store.RemoveSegment(seg_id, client_id);

    // Object should be gone (no replicas left)
    EXPECT_EQ(store.GetObjects().size(), 0u);
}

TEST(P2PStandbyMetadataStoreTest, RemoveSegmentDoesNotAffectOtherSegments) {
    P2PStandbyMetadataStore store;
    auto client_id = MakeUUID(1, 0);
    auto seg1 = MakeUUID(100, 0);
    auto seg2 = MakeUUID(200, 0);

    // Add replicas on two different segments
    store.AddReplica("key1", client_id, seg1, 1024, 0, {}, MemoryType::DRAM);
    store.AddReplica("key1", client_id, seg2, 2048, 0, {}, MemoryType::DRAM);

    ASSERT_EQ(store.GetObjects().find("key1")->second.replicas.size(), 2u);

    // Remove seg1 — should only remove replica on seg1
    store.RemoveSegment(seg1, client_id);

    auto it = store.GetObjects().find("key1");
    ASSERT_NE(it, store.GetObjects().end());
    EXPECT_EQ(it->second.replicas.size(), 1u);
}

// ============================================================================
// RemoveAllMetadata
// ============================================================================

TEST(P2PStandbyMetadataStoreTest, RemoveAllMetadata) {
    P2PStandbyMetadataStore store;
    auto client = MakeUUID(1, 0);
    auto seg = MakeUUID(10, 0);

    store.AddReplica("key1", client, seg, 1024, 0, {}, MemoryType::DRAM);
    store.AddReplica("key2", client, seg, 2048, 0, {}, MemoryType::DRAM);
    store.RegisterClient(client, "1.2.3.4", 50051, {MakeSegment(seg, 1024)});

    EXPECT_EQ(store.GetKeyCount(), 2u);
    EXPECT_NE(store.GetClient(client), nullptr);

    store.RemoveAllMetadata();

    EXPECT_EQ(store.GetKeyCount(), 0u);
    EXPECT_EQ(store.GetClient(client), nullptr);
}

// ============================================================================
// ExportMetadata
// ============================================================================

TEST(P2PStandbyMetadataStoreTest, ExportMetadata) {
    P2PStandbyMetadataStore store;
    auto client = MakeUUID(1, 0);
    auto seg = MakeUUID(10, 0);

    store.AddReplica("key1", client, seg, 1024, 0, {}, MemoryType::DRAM);
    store.RegisterClient(client, "1.2.3.4", 50051, {MakeSegment(seg, 1024)});

    auto exported = store.ExportMetadata();

    EXPECT_EQ(exported.objects.size(), 1u);
    EXPECT_NE(exported.objects.find("key1"), exported.objects.end());
    EXPECT_EQ(exported.clients.size(), 1u);
    EXPECT_NE(exported.clients.find(client), exported.clients.end());
    EXPECT_EQ(exported.clients.at(client).ip_address, "1.2.3.4");
}

TEST(P2PStandbyMetadataStoreTest, ExportMetadataNoBackfillForUnknownClient) {
    P2PStandbyMetadataStore store;
    auto client_id = MakeUUID(1, 0);
    auto seg_id = MakeUUID(10, 0);

    // Add replica but never register the client
    store.AddReplica("key1", client_id, seg_id, 1024, 0, {}, MemoryType::DRAM);

    auto exported = store.ExportMetadata();
    auto obj_it = exported.objects.find("key1");
    ASSERT_NE(obj_it, exported.objects.end());
    ASSERT_EQ(obj_it->second.replicas.size(), 1u);
    auto& p2p = std::get<P2PProxyDescriptor>(
        obj_it->second.replicas[0].descriptor_variant);
    // Client not registered — ip/port stays empty
    EXPECT_TRUE(p2p.ip_address.empty());
    EXPECT_EQ(p2p.rpc_port, 0u);
}

}  // namespace mooncake::test

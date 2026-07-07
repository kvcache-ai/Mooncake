#include "ha/oplog/p2p_oplog_types.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <vector>

namespace mooncake::test {

// ============================================================================
// OpType constants
// ============================================================================

TEST(P2POpLogTypesTest, OpTypeValuesDoNotConflictWithMain) {
    // Main branch uses OpType 1-4 (PUT_END, PUT_REVOKE, REMOVE, LEASE_RENEW).
    // P2P OpTypes start from 10.
    EXPECT_EQ(static_cast<int>(OpType_ADD_REPLICA), 10);
    EXPECT_EQ(static_cast<int>(OpType_REMOVE_REPLICA), 11);
    EXPECT_EQ(static_cast<int>(OpType_MOUNT_SEGMENT), 12);
    EXPECT_EQ(static_cast<int>(OpType_UNMOUNT_SEGMENT), 13);
    EXPECT_EQ(static_cast<int>(OpType_REMOVE_ALL), 14);
    EXPECT_EQ(static_cast<int>(OpType_REGISTER_CLIENT), 15);
}

TEST(P2POpLogTypesTest, OpTypeValuesAreDistinct) {
    std::vector<int> values = {
        static_cast<int>(OpType_ADD_REPLICA),
        static_cast<int>(OpType_REMOVE_REPLICA),
        static_cast<int>(OpType_MOUNT_SEGMENT),
        static_cast<int>(OpType_UNMOUNT_SEGMENT),
        static_cast<int>(OpType_REMOVE_ALL),
        static_cast<int>(OpType_REGISTER_CLIENT),
    };
    std::sort(values.begin(), values.end());
    for (size_t i = 1; i < values.size(); ++i) {
        EXPECT_NE(values[i], values[i - 1])
            << "Duplicate OpType value: " << values[i];
    }
}

// ============================================================================
// RegisterClientPayload round-trip
// ============================================================================

TEST(P2POpLogTypesTest, RoundTrip_RegisterClientPayload) {
    RegisterClientPayload original;
    original.client_id = {100, 200};
    original.ip_address = "192.168.1.1";
    original.rpc_port = 50051;
    Segment seg;
    seg.id = {300, 400};
    seg.name = "seg_001";
    seg.size = 1024;
    seg.extra = P2PSegmentExtraData{1, {"tag1"}, MemoryType::DRAM, 0};
    original.segments = {seg};

    std::string data = SerializeP2PPayload(original);
    ASSERT_FALSE(data.empty());

    RegisterClientPayload decoded;
    ASSERT_TRUE(DeserializeP2PPayload(data, decoded));

    EXPECT_EQ(decoded.client_id.first, 100u);
    EXPECT_EQ(decoded.client_id.second, 200u);
    EXPECT_EQ(decoded.ip_address, "192.168.1.1");
    EXPECT_EQ(decoded.rpc_port, 50051u);
    ASSERT_EQ(decoded.segments.size(), 1u);
    EXPECT_EQ(decoded.segments[0].id.first, 300u);
    EXPECT_EQ(decoded.segments[0].id.second, 400u);
    EXPECT_EQ(decoded.segments[0].name, "seg_001");
    EXPECT_EQ(decoded.segments[0].size, 1024u);
}

TEST(P2POpLogTypesTest, RoundTrip_RegisterClientPayload_EmptyFields) {
    RegisterClientPayload original;
    // Use default values, including empty segments
    original.client_id = {0, 0};
    original.ip_address = "";
    original.rpc_port = 0;

    std::string data = SerializeP2PPayload(original);
    RegisterClientPayload decoded;
    ASSERT_TRUE(DeserializeP2PPayload(data, decoded));

    EXPECT_EQ(decoded.client_id.first, 0u);
    EXPECT_EQ(decoded.client_id.second, 0u);
    EXPECT_EQ(decoded.ip_address, "");
    EXPECT_EQ(decoded.rpc_port, 0u);
    EXPECT_EQ(decoded.segments.size(), 0u);
}

TEST(P2POpLogTypesTest, RoundTrip_RegisterClientPayload_MultipleSegments) {
    RegisterClientPayload original;
    original.client_id = {100, 200};
    original.ip_address = "10.0.0.1";
    original.rpc_port = 50052;

    Segment seg1;
    seg1.id = {1, 2};
    seg1.name = "dram_seg";
    seg1.size = 4096;
    seg1.extra = P2PSegmentExtraData{2, {"ssd", "gpu"}, MemoryType::DRAM, 512};

    Segment seg2;
    seg2.id = {3, 4};
    seg2.name = "nvme_seg";
    seg2.size = 8192;
    seg2.extra = P2PSegmentExtraData{3, {"nvme"}, MemoryType::NVME, 1024};

    original.segments = {seg1, seg2};

    std::string data = SerializeP2PPayload(original);
    RegisterClientPayload decoded;
    ASSERT_TRUE(DeserializeP2PPayload(data, decoded));

    ASSERT_EQ(decoded.segments.size(), 2u);
    EXPECT_EQ(decoded.segments[0].id.first, 1u);
    EXPECT_EQ(decoded.segments[0].name, "dram_seg");
    EXPECT_EQ(decoded.segments[0].size, 4096u);
    EXPECT_EQ(decoded.segments[1].id.first, 3u);
    EXPECT_EQ(decoded.segments[1].name, "nvme_seg");
    EXPECT_EQ(decoded.segments[1].size, 8192u);
}

// ============================================================================
// AddReplicaPayload round-trip
// ============================================================================

TEST(P2POpLogTypesTest, RoundTrip_AddReplicaPayload) {
    AddReplicaPayload original;
    original.object_key = "obj_001";
    original.client_id = {10, 20};
    original.segment_id = {30, 40};
    original.size = 4096;
    original.priority = 1;
    original.tags = {"tag1", "tag2"};
    original.memory_type = MemoryType::DRAM;

    std::string data = SerializeP2PPayload(original);
    AddReplicaPayload decoded;
    ASSERT_TRUE(DeserializeP2PPayload(data, decoded));

    EXPECT_EQ(decoded.object_key, "obj_001");
    EXPECT_EQ(decoded.client_id.first, 10u);
    EXPECT_EQ(decoded.client_id.second, 20u);
    EXPECT_EQ(decoded.segment_id.first, 30u);
    EXPECT_EQ(decoded.segment_id.second, 40u);
    EXPECT_EQ(decoded.size, 4096u);
    EXPECT_EQ(decoded.priority, 1);
    ASSERT_EQ(decoded.tags.size(), 2u);
    EXPECT_EQ(decoded.tags[0], "tag1");
    EXPECT_EQ(decoded.tags[1], "tag2");
    EXPECT_EQ(decoded.memory_type, MemoryType::DRAM);
}

TEST(P2POpLogTypesTest, RoundTrip_AddReplicaPayload_EmptyTags) {
    AddReplicaPayload original;
    original.object_key = "obj_empty_tags";
    original.client_id = {1, 2};
    original.segment_id = {3, 4};
    original.size = 1024;
    original.priority = 0;
    // tags intentionally left empty
    original.memory_type = MemoryType::DRAM;

    std::string data = SerializeP2PPayload(original);
    AddReplicaPayload decoded;
    ASSERT_TRUE(DeserializeP2PPayload(data, decoded));

    EXPECT_EQ(decoded.tags.size(), 0u);
}

// ============================================================================
// RemoveReplicaPayload round-trip
// ============================================================================

TEST(P2POpLogTypesTest, RoundTrip_RemoveReplicaPayload) {
    RemoveReplicaPayload original;
    original.object_key = "obj_to_remove";
    original.client_id = {100, 200};
    original.segment_id = {300, 400};

    std::string data = SerializeP2PPayload(original);
    RemoveReplicaPayload decoded;
    ASSERT_TRUE(DeserializeP2PPayload(data, decoded));

    EXPECT_EQ(decoded.object_key, "obj_to_remove");
    EXPECT_EQ(decoded.client_id.first, 100u);
    EXPECT_EQ(decoded.client_id.second, 200u);
    EXPECT_EQ(decoded.segment_id.first, 300u);
    EXPECT_EQ(decoded.segment_id.second, 400u);
}

// ============================================================================
// MountSegmentPayload round-trip
// ============================================================================

TEST(P2POpLogTypesTest, RoundTrip_MountSegmentPayload) {
    MountSegmentPayload original;
    original.client_id = {50, 60};
    original.segment.id = {70, 80};
    original.segment.name = "test_segment";
    original.segment.size = 2048;
    original.segment.extra =
        P2PSegmentExtraData{1, {"tag1"}, MemoryType::DRAM, 0};

    std::string data = SerializeP2PPayload(original);
    MountSegmentPayload decoded;
    ASSERT_TRUE(DeserializeP2PPayload(data, decoded));

    EXPECT_EQ(decoded.client_id.first, 50u);
    EXPECT_EQ(decoded.client_id.second, 60u);
    EXPECT_EQ(decoded.segment.id.first, 70u);
    EXPECT_EQ(decoded.segment.id.second, 80u);
    EXPECT_EQ(decoded.segment.name, "test_segment");
    EXPECT_EQ(decoded.segment.size, 2048u);
}

TEST(P2POpLogTypesTest, RoundTrip_MountSegmentPayload_MonostateExtra) {
    // Segment with default (monostate) extra data
    MountSegmentPayload original;
    original.client_id = {1, 2};
    original.segment.id = {3, 4};
    original.segment.name = "default_seg";
    original.segment.size = 512;

    std::string data = SerializeP2PPayload(original);
    MountSegmentPayload decoded;
    ASSERT_TRUE(DeserializeP2PPayload(data, decoded));

    EXPECT_EQ(decoded.segment.id.first, 3u);
    EXPECT_EQ(decoded.segment.name, "default_seg");
    EXPECT_EQ(decoded.segment.size, 512u);
    EXPECT_TRUE(decoded.segment.IsEmpty());
}

// ============================================================================
// UnmountSegmentPayload round-trip
// ============================================================================

TEST(P2POpLogTypesTest, RoundTrip_UnmountSegmentPayload) {
    UnmountSegmentPayload original;
    original.segment_id = {70, 80};
    original.client_id = {90, 100};

    std::string data = SerializeP2PPayload(original);
    UnmountSegmentPayload decoded;
    ASSERT_TRUE(DeserializeP2PPayload(data, decoded));

    EXPECT_EQ(decoded.segment_id.first, 70u);
    EXPECT_EQ(decoded.segment_id.second, 80u);
    EXPECT_EQ(decoded.client_id.first, 90u);
    EXPECT_EQ(decoded.client_id.second, 100u);
}

// ============================================================================
// Deserialization error handling
// ============================================================================

TEST(P2POpLogTypesTest, Deserialize_GarbageData_ReturnsFalse) {
    std::string garbage = "this is not valid struct_pack data";
    RegisterClientPayload p1;
    EXPECT_FALSE(DeserializeP2PPayload(garbage, p1));

    AddReplicaPayload p2;
    EXPECT_FALSE(DeserializeP2PPayload(garbage, p2));
}

TEST(P2POpLogTypesTest, Deserialize_EmptyData_ReturnsFalse) {
    std::string empty;
    RemoveReplicaPayload p1;
    EXPECT_FALSE(DeserializeP2PPayload(empty, p1));

    MountSegmentPayload p2;
    EXPECT_FALSE(DeserializeP2PPayload(empty, p2));
}

TEST(P2POpLogTypesTest, Deserialize_WrongType_ReturnsFalse) {
    // Serialize an AddReplicaPayload, try to deserialize as
    // RemoveReplicaPayload
    AddReplicaPayload original;
    original.object_key = "cross_type_test";
    original.client_id = {1, 2};
    original.segment_id = {3, 4};
    original.size = 100;

    std::string data = SerializeP2PPayload(original);
    RemoveReplicaPayload wrong;
    // struct_pack should detect the type mismatch
    EXPECT_FALSE(DeserializeP2PPayload(data, wrong));
}

}  // namespace mooncake::test
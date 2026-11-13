#include "ha/snapshot/batch_oplog/codec.h"
#include "metadata_wire_compat.h"

#include <gtest/gtest.h>

#include <string>
#include <utility>
#include <vector>

namespace mooncake::test {

namespace {

struct LegacyStandbyObjectMetadata {
    UUID client_id{0, 0};
    uint64_t size{0};
    std::vector<Replica::Descriptor> replicas;
    std::string group_id;
    ObjectDataType data_type{ObjectDataType::UNKNOWN};

    YLT_REFL(LegacyStandbyObjectMetadata, client_id, size, replicas, group_id,
             data_type);
};

struct LegacyStandbyObjectEntry {
    std::string tenant_id{"default"};
    std::string key;
    LegacyStandbyObjectMetadata metadata;

    YLT_REFL(LegacyStandbyObjectEntry, tenant_id, key, metadata);
};

struct LegacyBatchOpLogSnapshotObjectChunk {
    uint64_t chunk_index{0};
    std::vector<LegacyStandbyObjectEntry> objects;

    YLT_REFL(LegacyBatchOpLogSnapshotObjectChunk, chunk_index, objects);
};

}  // namespace

TEST(BatchOpLogSnapshotCodecTest, SegmentsRoundTrip) {
    std::vector<StandbySegmentInfo> segments{{
        .segment_name = "segment-1",
        .transport_endpoint = "127.0.0.1:12345",
        .capacity = 4096,
        .is_memory_segment = true,
        .file_path = "",
    }};

    auto decoded = DecodeBatchOpLogSnapshotSegments(
        EncodeBatchOpLogSnapshotSegments(segments));

    ASSERT_TRUE(decoded) << decoded.error();
    ASSERT_EQ(1u, decoded->size());
    EXPECT_EQ("segment-1", decoded->front().segment_name);
    EXPECT_EQ("127.0.0.1:12345", decoded->front().transport_endpoint);
    EXPECT_EQ(4096u, decoded->front().capacity);
    EXPECT_TRUE(decoded->front().is_memory_segment);
}

TEST(BatchOpLogSnapshotCodecTest, ObjectChunkRoundTripsAndChecksEnvelope) {
    StandbyObjectMetadata metadata;
    metadata.client_id = {1, 2};
    metadata.size = 8192;
    metadata.group_id = "group";
    metadata.hard_pinned = true;
    metadata.session_id = "session-snapshot";
    metadata.object_checksum = 0;
    metadata.residency_affinity_id = "snapshot-affinity";
    std::vector<StandbyObjectEntry> objects{
        {.tenant_id = "tenant", .key = "key", .metadata = metadata}};
    auto encoded = EncodeBatchOpLogSnapshotObjectChunk(7, std::move(objects));

    auto decoded = DecodeBatchOpLogSnapshotObjectChunk(encoded, 7, 1);

    ASSERT_TRUE(decoded) << decoded.error();
    EXPECT_EQ(7u, decoded->chunk_index);
    ASSERT_EQ(1u, decoded->objects.size());
    EXPECT_EQ("tenant", decoded->objects[0].tenant_id);
    EXPECT_EQ("key", decoded->objects[0].key);
    EXPECT_EQ(UUID(1, 2), decoded->objects[0].metadata.client_id);
    EXPECT_EQ(8192u, decoded->objects[0].metadata.size);
    EXPECT_EQ("group", decoded->objects[0].metadata.group_id);
    EXPECT_TRUE(decoded->objects[0].metadata.hard_pinned.value_or(false));
    EXPECT_EQ(decoded->objects[0].metadata.session_id.value_or(""),
              "session-snapshot");
    ASSERT_TRUE(decoded->objects[0].metadata.object_checksum.has_value());
    EXPECT_EQ(decoded->objects[0].metadata.object_checksum.value(), 0U);
    EXPECT_EQ(decoded->objects[0].metadata.residency_affinity_id.value_or(""),
              "snapshot-affinity");

    EXPECT_FALSE(DecodeBatchOpLogSnapshotObjectChunk(encoded, 8, 1));
    EXPECT_FALSE(DecodeBatchOpLogSnapshotObjectChunk(encoded, 7, 300000000));
}

TEST(BatchOpLogSnapshotCodecTest, OldObjectChunkDefaultsHardPinnedToFalse) {
    LegacyBatchOpLogSnapshotObjectChunk old_chunk{
        0, {{"tenant", "key", {{1, 2}, 8192, {}, "group", {}}}}};
    auto old_encoded = struct_pack::serialize(old_chunk);
    std::vector<uint8_t> encoded(old_encoded.begin(), old_encoded.end());

    auto decoded = DecodeBatchOpLogSnapshotObjectChunk(encoded, 0, 1);

    ASSERT_TRUE(decoded) << decoded.error();
    EXPECT_FALSE(decoded->objects[0].metadata.hard_pinned.value_or(false));
    EXPECT_FALSE(decoded->objects[0].metadata.session_id.has_value());
    EXPECT_FALSE(decoded->objects[0].metadata.object_checksum.has_value());
}

TEST(BatchOpLogSnapshotCodecTest, RejectsTruncatedArtifacts) {
    auto segments = EncodeBatchOpLogSnapshotSegments({});
    auto objects = EncodeBatchOpLogSnapshotObjectChunk(
        0, std::vector<StandbyObjectEntry>{
               {.tenant_id = "default", .key = "key", .metadata = {}}});
    segments.pop_back();
    objects.pop_back();

    EXPECT_FALSE(DecodeBatchOpLogSnapshotSegments(segments));
    EXPECT_FALSE(DecodeBatchOpLogSnapshotObjectChunk(objects, 0, 1));
}

}  // namespace mooncake::test

// The previous tests vary compatible fields but use the current descriptor
// variant. This fixture freezes the actual pre-object five-alternative shape.

TEST(BatchOpLogSnapshotCodecTest, ReadsPreObjectDescriptorVariant) {
    mooncake::metadata_wire_v2::Chunk old;
    old.chunk_index = 7;
    mooncake::metadata_wire_v2::Entry entry;
    entry.key = "legacy-key";
    entry.metadata.size = 4096;
    entry.metadata.object_checksum = 123;
    mooncake::metadata_wire_v2::ReplicaDescriptor replica;
    replica.id = 19;
    replica.status = mooncake::ReplicaStatus::COMPLETE;
    mooncake::MemoryDescriptor memory;
    memory.buffer_descriptor.size_ = 4096;
    memory.buffer_descriptor.transport_endpoint_ = "127.0.0.1:9000";
    replica.descriptor_variant = memory;
    entry.metadata.replicas.push_back(replica);
    old.objects.push_back(entry);
    auto encoded = struct_pack::serialize<std::vector<uint8_t>>(old);
    auto decoded = mooncake::DecodeBatchOpLogSnapshotObjectChunk(encoded, 7, 1);
    ASSERT_TRUE(decoded) << decoded.error();
    ASSERT_EQ(decoded->objects.size(), 1);
    const auto& metadata = decoded->objects.front().metadata;
    EXPECT_EQ(metadata.object_checksum.value(), 123);
    ASSERT_EQ(metadata.replicas.size(), 1);
    EXPECT_EQ(metadata.replicas.front().id, 19);
    EXPECT_TRUE(metadata.replicas.front().is_memory_replica());
}

TEST(BatchOpLogSnapshotCodecTest, ReadsPreObjectMetadataLog) {
    mooncake::metadata_wire_v2::Payload old;
    old.size = 16;
    old.session_id = "legacy-session";
    old.object_checksum = 456;
    mooncake::metadata_wire_v2::ReplicaDescriptor replica;
    replica.id = 17;
    replica.status = mooncake::ReplicaStatus::COMPLETE;
    replica.descriptor_variant = mooncake::DiskDescriptor{"legacy.data", 16};
    old.replicas.push_back(replica);
    auto bytes = struct_pack::serialize(old);
    mooncake::MetadataPayload decoded;
    ASSERT_EQ(mooncake::DeserializeMetadataPayload(
                  decoded, {bytes.data(), bytes.size()}),
              struct_pack::errc::ok);
    EXPECT_EQ(decoded.session_id.value(), "legacy-session");
    EXPECT_EQ(decoded.object_checksum.value(), 456);
    ASSERT_EQ(decoded.replicas.size(), 1);
    EXPECT_EQ(decoded.replicas.front().id, 17);
    EXPECT_EQ(decoded.replicas.front().get_disk_descriptor().file_path,
              "legacy.data");
}

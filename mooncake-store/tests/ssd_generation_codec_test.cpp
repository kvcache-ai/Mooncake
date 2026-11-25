#include <gtest/gtest.h>
#include <ylt/struct_pack.hpp>

#include "metadata_store.h"
#include "offload_storage_key.h"
#include "rpc_types.h"

namespace mooncake {
namespace {

TEST(SsdGenerationCodec, PromotionHeartbeatPreservesTaskAuthority) {
    PromotionTaskItem task{
        "tenant",     "user-key",
        4096,         MakeOffloadStorageKey("user-key", "generation"),
        "generation", "task-token"};
    auto bytes = struct_pack::serialize(std::vector<PromotionTaskItem>{task});
    auto decoded =
        struct_pack::deserialize<std::vector<PromotionTaskItem>>(bytes);
    ASSERT_TRUE(decoded);
    ASSERT_EQ(decoded->size(), 1u);
    EXPECT_EQ(decoded->front(), task);
    EXPECT_EQ(decoded->front().task_id.value_or(""), "task-token");
}

struct LegacyObjectMeta {
    std::string key;
    std::optional<uint64_t> object_checksum;
};
YLT_REFL(LegacyObjectMeta, key, object_checksum);

struct LegacyOffloadTaskItem {
    std::string tenant_id;
    std::string key;
    int64_t size;
};
YLT_REFL(LegacyOffloadTaskItem, tenant_id, key, size);

struct LegacyPromotionTaskItem {
    std::string tenant_id;
    std::string key;
    int64_t size;
};
YLT_REFL(LegacyPromotionTaskItem, tenant_id, key, size);

struct LegacyLocalDiskDescriptor {
    UUID client_id;
    uint64_t object_size = 0;
    std::string transport_endpoint;
};
YLT_REFL(LegacyLocalDiskDescriptor, client_id, object_size, transport_endpoint);

// Exact upstream/main wire layouts before PR 5 added version-5 fields.
struct LegacyReplicaDescriptor {
    ReplicaID id{0};
    std::variant<MemoryDescriptor, NoFDescriptor, DiskDescriptor,
                 LegacyLocalDiskDescriptor, DistributedFSDescriptor>
        descriptor_variant;
    ReplicaStatus status{ReplicaStatus::UNDEFINED};
};
YLT_REFL(LegacyReplicaDescriptor, id, descriptor_variant, status);

struct LegacyGetReplicaListResponse {
    std::vector<LegacyReplicaDescriptor> replicas;
    uint64_t lease_ttl_ms{0};
    std::optional<uint64_t> object_checksum;
    struct_pack::compatible<std::string, 1> resolved_tenant_id;
};
YLT_REFL(LegacyGetReplicaListResponse, replicas, lease_ttl_ms, object_checksum,
         resolved_tenant_id);

struct LegacyPromotionAllocStartResponse {
    LegacyReplicaDescriptor memory_descriptor;
};
YLT_REFL(LegacyPromotionAllocStartResponse, memory_descriptor);

struct LegacyMetadataPayload {
    UUID client_id{0, 0};
    uint64_t size{0};
    std::vector<LegacyReplicaDescriptor> replicas;
    struct_pack::compatible<std::string, 1> group_id;
    struct_pack::compatible<ObjectDataType, 1> data_type;
    struct_pack::compatible<bool, 1> hard_pinned;
    struct_pack::compatible<std::string, 2> session_id;
    struct_pack::compatible<uint64_t, 3> object_checksum;
    struct_pack::compatible<std::string, 4> residency_affinity_id;
};
YLT_REFL(LegacyMetadataPayload, client_id, size, replicas, group_id, data_type,
         hard_pinned, session_id, object_checksum, residency_affinity_id);

struct LegacyStandbyObjectMetadata {
    UUID client_id{0, 0};
    uint64_t size{0};
    std::vector<LegacyReplicaDescriptor> replicas;
    std::string group_id;
    ObjectDataType data_type{ObjectDataType::UNKNOWN};
    struct_pack::compatible<bool, 1> hard_pinned;
    struct_pack::compatible<std::string, 2> session_id;
    struct_pack::compatible<uint64_t, 3> object_checksum;
    struct_pack::compatible<std::string, 4> residency_affinity_id;
};
YLT_REFL(LegacyStandbyObjectMetadata, client_id, size, replicas, group_id,
         data_type, hard_pinned, session_id, object_checksum,
         residency_affinity_id);

struct LegacyStandbyObjectEntry {
    std::string tenant_id{"default"};
    std::string key;
    LegacyStandbyObjectMetadata metadata;
};
YLT_REFL(LegacyStandbyObjectEntry, tenant_id, key, metadata);

struct LegacyStandbySnapshot {
    uint64_t oplog_sequence_id{0};
    std::vector<StandbySegmentInfo> segments;
    std::vector<LegacyStandbyObjectEntry> objects;
    struct_pack::compatible<WeightMetadataSnapshot, 1> weight_metadata;
    struct_pack::compatible<ObjectReplicaSnapshot, 2> object_storage;
};
YLT_REFL(LegacyStandbySnapshot, oplog_sequence_id, segments, objects,
         weight_metadata, object_storage);

TEST(SsdGenerationCodec, WireSchemasPreserveStructPackCompatibility) {
    ObjectMeta meta{"user-key", uint64_t{42}, "gen-1"};
    auto legacy_meta = struct_pack::deserialize<LegacyObjectMeta>(
        struct_pack::serialize(meta));
    ASSERT_TRUE(legacy_meta);
    EXPECT_EQ(legacy_meta->key, "user-key");
    EXPECT_EQ(legacy_meta->object_checksum, uint64_t{42});
    auto decoded_meta = struct_pack::deserialize<ObjectMeta>(
        struct_pack::serialize(*legacy_meta));
    ASSERT_TRUE(decoded_meta);
    EXPECT_EQ(decoded_meta->key, "user-key");
    EXPECT_FALSE(decoded_meta->write_generation.has_value());

    OffloadTaskItem offload{.tenant_id = "default",
                            .key = "k",
                            .size = 1024,
                            .object_generation = "gen-1",
                            .task_id = "task-1",
                            .restoring = true};
    auto legacy_offload = struct_pack::deserialize<LegacyOffloadTaskItem>(
        struct_pack::serialize(offload));
    ASSERT_TRUE(legacy_offload);
    EXPECT_EQ(legacy_offload->key, "k");
    auto decoded_offload = struct_pack::deserialize<OffloadTaskItem>(
        struct_pack::serialize(*legacy_offload));
    ASSERT_TRUE(decoded_offload);
    EXPECT_FALSE(decoded_offload->object_generation.has_value());
    EXPECT_FALSE(decoded_offload->restoring.value_or(false));

    PromotionTaskItem promo{"default", "k",
                            1024,      MakeOffloadStorageKey("k", "gen-1"),
                            "gen-1",   "promo-1"};
    auto legacy_promo = struct_pack::deserialize<LegacyPromotionTaskItem>(
        struct_pack::serialize(promo));
    ASSERT_TRUE(legacy_promo);
    EXPECT_EQ(legacy_promo->key, "k");
    auto decoded_promo = struct_pack::deserialize<PromotionTaskItem>(
        struct_pack::serialize(*legacy_promo));
    ASSERT_TRUE(decoded_promo);
    EXPECT_FALSE(decoded_promo->task_id.has_value());

    LocalDiskDescriptor disk{generate_uuid(), 2048, "127.0.0.1:9000",
                             MakeOffloadStorageKey("k", "gen-1"), "gen-1"};
    auto legacy_disk = struct_pack::deserialize<LegacyLocalDiskDescriptor>(
        struct_pack::serialize(disk));
    ASSERT_TRUE(legacy_disk);
    EXPECT_EQ(legacy_disk->object_size, 2048u);
    auto decoded_disk = struct_pack::deserialize<LocalDiskDescriptor>(
        struct_pack::serialize(*legacy_disk));
    ASSERT_TRUE(decoded_disk);
    EXPECT_EQ(decoded_disk->transport_endpoint, "127.0.0.1:9000");
    EXPECT_FALSE(decoded_disk->storage_key.has_value());

    // Verify bidirectional Legacy<->New wire compatibility across nested
    // multi-replica structs that already have compatible v1..v4 fields.
    MemoryDescriptor mem;
    mem.buffer_descriptor.size_ = 2048;
    mem.buffer_descriptor.buffer_address_ = 0x10000;
    mem.buffer_descriptor.transport_endpoint_ = "127.0.0.1:8000";
    mem.locality = SegmentLocality{2, {"mlx5_0", "mlx5_1"}};

    Replica::Descriptor mem_desc;
    mem_desc.id = 76;
    mem_desc.status = ReplicaStatus::COMPLETE;
    mem_desc.descriptor_variant = mem;
    mem_desc.write_generation = "gen-1";

    Replica::Descriptor desc;
    desc.id = 77;
    desc.status = ReplicaStatus::COMPLETE;
    desc.descriptor_variant = disk;
    desc.write_generation = "gen-1";

    // Multi-element std::vector<Replica::Descriptor> (PutStart / UpsertStart)
    {
        std::vector<Replica::Descriptor> vec{mem_desc, desc};
        auto legacy_vec =
            struct_pack::deserialize<std::vector<LegacyReplicaDescriptor>>(
                struct_pack::serialize(vec));
        ASSERT_TRUE(legacy_vec);
        ASSERT_EQ(legacy_vec->size(), 2u);
        EXPECT_EQ((*legacy_vec)[0].id, 76u);
        EXPECT_EQ(
            std::get<MemoryDescriptor>((*legacy_vec)[0].descriptor_variant)
                .locality->numa_node,
            2);
        EXPECT_EQ((*legacy_vec)[1].id, 77u);
        EXPECT_EQ(std::get<LegacyLocalDiskDescriptor>(
                      (*legacy_vec)[1].descriptor_variant)
                      .transport_endpoint,
                  "127.0.0.1:9000");

        auto decoded_vec =
            struct_pack::deserialize<std::vector<Replica::Descriptor>>(
                struct_pack::serialize(*legacy_vec));
        ASSERT_TRUE(decoded_vec);
        ASSERT_EQ(decoded_vec->size(), 2u);
        EXPECT_FALSE((*decoded_vec)[0].write_generation.has_value());
        EXPECT_EQ((*decoded_vec)[0].get_memory_descriptor().locality->numa_node,
                  2);
        EXPECT_FALSE((*decoded_vec)[1]
                         .get_local_disk_descriptor()
                         .storage_key.has_value());
    }

    // GetReplicaListResponse (has v1 resolved_tenant_id + v5 object_generation)
    GetReplicaListResponse resp;
    resp.replicas = {mem_desc, desc};
    resp.lease_ttl_ms = 1234;
    resp.object_checksum = uint64_t{99};
    resp.resolved_tenant_id = "tenant-a";
    resp.object_generation = "gen-1";
    {
        auto decoded_resp = struct_pack::deserialize<GetReplicaListResponse>(
            struct_pack::serialize(resp));
        ASSERT_TRUE(decoded_resp);
        ASSERT_EQ(decoded_resp->replicas.size(), 2u);
        EXPECT_EQ(decoded_resp->resolved_tenant_id.value_or(""), "tenant-a");
        EXPECT_EQ(decoded_resp->object_generation.value_or(""), "gen-1");
        EXPECT_EQ(decoded_resp->replicas[1].write_generation.value_or(""),
                  "gen-1");
        ASSERT_TRUE(decoded_resp->replicas[1].is_local_disk_replica());
        EXPECT_EQ(decoded_resp->replicas[1]
                      .get_local_disk_descriptor()
                      .storage_key.value_or(""),
                  MakeOffloadStorageKey("k", "gen-1"));
        EXPECT_EQ(decoded_resp->replicas[1]
                      .get_local_disk_descriptor()
                      .object_generation.value_or(""),
                  "gen-1");

        auto legacy_resp =
            struct_pack::deserialize<LegacyGetReplicaListResponse>(
                struct_pack::serialize(resp));
        ASSERT_TRUE(legacy_resp);
        ASSERT_EQ(legacy_resp->replicas.size(), 2u);
        EXPECT_EQ(legacy_resp->resolved_tenant_id.value_or(""), "tenant-a");
        EXPECT_EQ(legacy_resp->lease_ttl_ms, 1234u);

        auto from_legacy_resp =
            struct_pack::deserialize<GetReplicaListResponse>(
                struct_pack::serialize(*legacy_resp));
        ASSERT_TRUE(from_legacy_resp);
        ASSERT_EQ(from_legacy_resp->replicas.size(), 2u);
        EXPECT_EQ(from_legacy_resp->resolved_tenant_id.value_or(""),
                  "tenant-a");
        EXPECT_FALSE(from_legacy_resp->object_generation.has_value());
    }

    // PromotionAllocStartResponse (contains Replica::Descriptor + v5 fields)
    {
        PromotionAllocStartResponse promo_resp;
        promo_resp.memory_descriptor = mem_desc;
        promo_resp.object_generation = "gen-1";
        promo_resp.task_id = "task-1";
        promo_resp.storage_key = MakeOffloadStorageKey("k", "gen-1");
        auto legacy_promo_resp =
            struct_pack::deserialize<LegacyPromotionAllocStartResponse>(
                struct_pack::serialize(promo_resp));
        ASSERT_TRUE(legacy_promo_resp);
        EXPECT_EQ(legacy_promo_resp->memory_descriptor.id, 76u);
        EXPECT_EQ(std::get<MemoryDescriptor>(
                      legacy_promo_resp->memory_descriptor.descriptor_variant)
                      .locality->numa_node,
                  2);
        auto from_legacy =
            struct_pack::deserialize<PromotionAllocStartResponse>(
                struct_pack::serialize(*legacy_promo_resp));
        ASSERT_TRUE(from_legacy);
        EXPECT_EQ(from_legacy->memory_descriptor.id, 76u);
        EXPECT_FALSE(from_legacy->object_generation.has_value());
        EXPECT_FALSE(from_legacy->task_id.has_value());
        EXPECT_FALSE(from_legacy->storage_key.has_value());
    }

    // MetadataPayload, StandbyObjectMetadata, and StandbySnapshot (v1..v4 + v5)
    {
        MetadataPayload payload;
        payload.client_id = disk.client_id;
        payload.size = 2048;
        payload.replicas = {mem_desc, desc};
        payload.group_id = "grp-1";
        payload.data_type = ObjectDataType::KVCACHE;
        payload.hard_pinned = true;
        payload.session_id = "sess-1";
        payload.object_checksum = uint64_t{0x1234};
        payload.residency_affinity_id = "aff-1";
        payload.object_generation = "gen-1";

        auto legacy_payload = struct_pack::deserialize<LegacyMetadataPayload>(
            struct_pack::serialize(payload));
        ASSERT_TRUE(legacy_payload);
        ASSERT_EQ(legacy_payload->replicas.size(), 2u);
        EXPECT_EQ(legacy_payload->group_id.value_or(""), "grp-1");
        EXPECT_TRUE(legacy_payload->hard_pinned.value_or(false));
        EXPECT_EQ(legacy_payload->session_id.value_or(""), "sess-1");
        EXPECT_EQ(legacy_payload->object_checksum.value_or(0),
                  uint64_t{0x1234});
        EXPECT_EQ(legacy_payload->residency_affinity_id.value_or(""), "aff-1");

        auto from_legacy_payload = struct_pack::deserialize<MetadataPayload>(
            struct_pack::serialize(*legacy_payload));
        ASSERT_TRUE(from_legacy_payload);
        ASSERT_EQ(from_legacy_payload->replicas.size(), 2u);
        EXPECT_EQ(from_legacy_payload->group_id.value_or(""), "grp-1");
        EXPECT_EQ(from_legacy_payload->session_id.value_or(""), "sess-1");
        EXPECT_EQ(from_legacy_payload->residency_affinity_id.value_or(""),
                  "aff-1");
        EXPECT_FALSE(from_legacy_payload->object_generation.has_value());

        StandbySnapshot snap;
        snap.oplog_sequence_id = 42;
        snap.objects.push_back({"default", "k", payload.ToStandbyMetadata()});
        snap.weight_metadata = WeightMetadataSnapshot{};
        snap.object_storage = ObjectReplicaSnapshot{};

        auto legacy_snap = struct_pack::deserialize<LegacyStandbySnapshot>(
            struct_pack::serialize(snap));
        ASSERT_TRUE(legacy_snap);
        EXPECT_EQ(legacy_snap->oplog_sequence_id, 42u);
        ASSERT_EQ(legacy_snap->objects.size(), 1u);
        EXPECT_EQ(legacy_snap->objects[0].metadata.session_id.value_or(""),
                  "sess-1");
        EXPECT_EQ(
            legacy_snap->objects[0].metadata.residency_affinity_id.value_or(""),
            "aff-1");
        EXPECT_TRUE(legacy_snap->weight_metadata.has_value());
        EXPECT_TRUE(legacy_snap->object_storage.has_value());

        auto from_legacy_snap = struct_pack::deserialize<StandbySnapshot>(
            struct_pack::serialize(*legacy_snap));
        ASSERT_TRUE(from_legacy_snap);
        EXPECT_EQ(from_legacy_snap->oplog_sequence_id, 42u);
        ASSERT_EQ(from_legacy_snap->objects.size(), 1u);
        EXPECT_FALSE(from_legacy_snap->objects[0]
                         .metadata.object_generation.has_value());
        EXPECT_TRUE(from_legacy_snap->weight_metadata.has_value());
        EXPECT_TRUE(from_legacy_snap->object_storage.has_value());
    }
}

TEST(SsdGenerationCodec, PromotionAllocationReturnsCapturedAuthority) {
    PromotionAllocStartResponse response;
    response.object_generation = "generation";
    response.task_id = "task-token";
    response.storage_key = MakeOffloadStorageKey("user-key", "generation");
    auto decoded = struct_pack::deserialize<PromotionAllocStartResponse>(
        struct_pack::serialize(response));
    ASSERT_TRUE(decoded);
    EXPECT_EQ(decoded->object_generation, response.object_generation);
    EXPECT_EQ(decoded->task_id, response.task_id);
    EXPECT_EQ(decoded->storage_key, response.storage_key);
}

TEST(SsdGenerationCodec, MetadataKeepsSessionChecksumAffinityAndGeneration) {
    MetadataPayload metadata;
    metadata.session_id = "session";
    metadata.object_checksum = uint64_t{0x12345678};
    metadata.residency_affinity_id = "affinity";
    metadata.object_generation = "generation";
    auto decoded = struct_pack::deserialize<MetadataPayload>(
        struct_pack::serialize(metadata));
    ASSERT_TRUE(decoded);
    auto standby = decoded->ToStandbyMetadata();
    EXPECT_EQ(standby.session_id.value_or(""), "session");
    EXPECT_EQ(standby.object_checksum.value_or(0), uint64_t{0x12345678});
    EXPECT_EQ(standby.residency_affinity_id.value_or(""), "affinity");
    EXPECT_EQ(standby.object_generation.value_or(""), "generation");
    auto snapshot = struct_pack::deserialize<StandbyObjectMetadata>(
        struct_pack::serialize(standby));
    ASSERT_TRUE(snapshot);
    EXPECT_EQ(snapshot->object_generation.value_or(""), "generation");
}

TEST(SsdGenerationCodec,
     PhysicalKeysSeparateRewritesAndPreserveOpaqueUserKeys) {
    const std::string key("@mcssd2:3:old\0user", 18);
    const auto first = MakeOffloadStorageKey(key, "generation-one");
    const auto second = MakeOffloadStorageKey(key, "generation-two");
    EXPECT_NE(first, second);
    auto parsed = ParseOffloadStorageKey(first);
    ASSERT_TRUE(parsed);
    EXPECT_EQ(parsed->key, key);
    EXPECT_EQ(parsed->generation, "generation-one");
    EXPECT_EQ(MakeOffloadStorageKey(parsed->key, parsed->generation), first);
    EXPECT_FALSE(ParseOffloadStorageKey("@mcssd2:999:short"));
    EXPECT_FALSE(ParseOffloadStorageKey("@mcssd2:0:key"));
    EXPECT_FALSE(ParseOffloadStorageKey("ordinary-key"));
    EXPECT_TRUE(MakeOffloadStorageKey("k", std::string(65, 'g')).empty());
    EXPECT_FALSE(MakeOffloadStorageKey("k", std::string(64, 'g')).empty());
}

}  // namespace
}  // namespace mooncake

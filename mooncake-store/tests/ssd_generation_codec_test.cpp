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

    // Verify nested compatible fields inside Replica::Descriptor (std::variant
    // holding LocalDiskDescriptor + top-level write_generation) and
    // GetReplicaListResponse round-trip cleanly.
    Replica::Descriptor desc;
    desc.id = 77;
    desc.status = ReplicaStatus::COMPLETE;
    desc.descriptor_variant = disk;
    desc.write_generation = "gen-1";
    GetReplicaListResponse resp;
    resp.replicas.push_back(desc);
    resp.lease_ttl_ms = 1234;
    resp.object_checksum = uint64_t{99};
    resp.resolved_tenant_id = "default";
    resp.object_generation = "gen-1";
    auto decoded_resp = struct_pack::deserialize<GetReplicaListResponse>(
        struct_pack::serialize(resp));
    ASSERT_TRUE(decoded_resp);
    ASSERT_EQ(decoded_resp->replicas.size(), 1u);
    EXPECT_EQ(decoded_resp->object_generation.value_or(""), "gen-1");
    EXPECT_EQ(decoded_resp->replicas[0].write_generation.value_or(""), "gen-1");
    ASSERT_TRUE(decoded_resp->replicas[0].is_local_disk_replica());
    EXPECT_EQ(decoded_resp->replicas[0]
                  .get_local_disk_descriptor()
                  .storage_key.value_or(""),
              MakeOffloadStorageKey("k", "gen-1"));
    EXPECT_EQ(decoded_resp->replicas[0]
                  .get_local_disk_descriptor()
                  .object_generation.value_or(""),
              "gen-1");
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
}

}  // namespace
}  // namespace mooncake

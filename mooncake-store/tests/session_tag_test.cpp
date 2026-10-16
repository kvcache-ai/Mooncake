#include "master_service_test_fixture.h"
#include "metadata_store.h"
#include "rpc_service.h"

namespace mooncake::test {
namespace {
// Pre-session RPC shape. Keep this fixture independent of ReplicateConfig.
struct LegacyReplicateConfig {
    size_t replica_num{1};
    size_t nof_replica_num{0};
    size_t dfs_replica_num{0};
    SoftPinAction soft_pin_action{SoftPinAction::PRESERVE};
    std::optional<uint64_t> soft_pin_ttl_ms;
    bool with_hard_pin{false};
    std::vector<std::string> preferred_segments;
    std::string preferred_segment;
    std::vector<std::string> preferred_nof_segments;
    bool prefer_alloc_in_same_node{false};
    ObjectDataType data_type{ObjectDataType::UNKNOWN};
    std::string host_id;
    std::optional<std::vector<std::string>> group_ids;
};

struct LegacyMetadataPayload {
    UUID client_id{0, 0};
    uint64_t size{0};
    std::vector<Replica::Descriptor> replicas;
    struct_pack::compatible<std::string, 1> group_id;
    struct_pack::compatible<ObjectDataType, 1> data_type;
    struct_pack::compatible<bool, 1> hard_pinned;
};

std::string SessionTag(MasterService& service, const std::string& key) {
    MasterServiceTestPeer::MetadataAccessorRO access(
        &service, MasterServiceTestPeer(service).MakeObjectIdentityForRequest(
                      key, TenantId::Default()));
    EXPECT_TRUE(access.Exists());
    return access.Exists() ? access.Get().session_id : "<missing>";
}

ReplicateConfig Tagged(std::string id) {
    ReplicateConfig config;
    config.session_ids = std::vector<std::string>{std::move(id)};
    return config;
}
}  // namespace

TEST(SessionTagTest, RpcCompatibilityAndBatchValidation) {
    EXPECT_EQ(struct_pack::get_type_code<ReplicateConfig>(),
              struct_pack::get_type_code<LegacyReplicateConfig>());
    LegacyReplicateConfig legacy;
    legacy.group_ids = std::vector<std::string>{"group"};
    auto decoded = struct_pack::deserialize<ReplicateConfig>(
        struct_pack::serialize(legacy));
    ASSERT_TRUE(decoded);
    EXPECT_FALSE(decoded->session_ids);
    EXPECT_EQ(decoded->group_ids, legacy.group_ids);

    auto config = Tagged("session");
    config.group_ids = legacy.group_ids;
    auto old_reader = struct_pack::deserialize<LegacyReplicateConfig>(
        struct_pack::serialize(config));
    ASSERT_TRUE(old_reader);
    EXPECT_EQ(old_reader->group_ids, legacy.group_ids);
    auto new_reader = struct_pack::deserialize<ReplicateConfig>(
        struct_pack::serialize(config));
    ASSERT_TRUE(new_reader);
    EXPECT_EQ(new_reader->SessionId(), "session");
    config.session_ids = std::vector<std::string>{"a", "", "b"};
    config.group_ids = std::vector<std::string>{"g1", "g2", "g3"};
    EXPECT_TRUE(config.ValidSessionIds(3));
    EXPECT_FALSE(config.ValidSessionIds(2));
    EXPECT_EQ(config.ForSingleKey(2).SessionId(), "b");
    EXPECT_EQ(config.ForSingleKey(2).group_ids->at(0), "g3");
    EXPECT_TRUE(Tagged(std::string(256, 'x')).ValidSessionIds(1));
    EXPECT_FALSE(Tagged(std::string(257, 'x')).ValidSessionIds(1));
}

TEST(SessionTagTest, MetadataPayloadCompatibility) {
    LegacyMetadataPayload legacy;
    legacy.group_id = "group";
    legacy.hard_pinned = true;
    auto decoded = struct_pack::deserialize<MetadataPayload>(
        struct_pack::serialize(legacy));
    ASSERT_TRUE(decoded);
    EXPECT_FALSE(decoded->session_id);
    EXPECT_EQ(decoded->group_id.value_or(""), "group");
    decoded->session_id = "session";
    auto old_reader = struct_pack::deserialize<LegacyMetadataPayload>(
        struct_pack::serialize(*decoded));
    ASSERT_TRUE(old_reader);
    EXPECT_TRUE(old_reader->hard_pinned.value_or(false));
    EXPECT_EQ(old_reader->group_id.value_or(""), "group");
}

TEST_F(MasterServiceTest, SessionTagSurvivesAddingDiskReplica) {
    MasterService service;
    const auto client = PrepareSimpleSegment(service).client_id;
    PutCompletedObject(service, client, "key", Tagged("writer"));
    Replica disk(client, 1024, "local://session", ReplicaStatus::COMPLETE);
    ASSERT_TRUE(service.AddReplica(client, "key", TenantId::Default(), disk));
    EXPECT_EQ(SessionTag(service, "key"), "writer");
}

TEST_F(MasterServiceTest, SessionTagCommitsAndSurvivesUpsertReplacement) {
    MasterService service(
        MasterServiceConfig::builder().set_default_kv_lease_ttl(0).build());
    PrepareSimpleSegment(service);
    const auto client = generate_uuid();
    auto a = Tagged("a");
    ASSERT_TRUE(service.PutStart(client, "key", TenantId::Default(), 1024, a));
    EXPECT_EQ(SessionTag(service, "key"), "");
    ASSERT_TRUE(service.PutEnd(client, "key", TenantId::Default(),
                               ReplicaType::MEMORY));
    EXPECT_EQ(SessionTag(service, "key"), "a");
    EXPECT_FALSE(service.PutStart(client, "key", TenantId::Default(), 1024,
                                  Tagged("b")));
    EXPECT_EQ(SessionTag(service, "key"), "a");

    // Same-size, absent tag; then size-changing, explicit tag.
    ASSERT_TRUE(
        service.UpsertStart(client, "key", TenantId::Default(), 1024, {}));
    ASSERT_TRUE(service.UpsertEnd(client, "key", TenantId::Default(),
                                  ReplicaType::MEMORY));
    EXPECT_EQ(SessionTag(service, "key"), "a");
    ASSERT_TRUE(service.UpsertStart(client, "key", TenantId::Default(), 2048,
                                    Tagged("b")));
    EXPECT_EQ(SessionTag(service, "key"), "a");
    EXPECT_FALSE(service.UpsertEnd(generate_uuid(), "key", TenantId::Default(),
                                   ReplicaType::MEMORY));
    EXPECT_EQ(SessionTag(service, "key"), "a");
    ASSERT_TRUE(service.UpsertEnd(client, "key", TenantId::Default(),
                                  ReplicaType::MEMORY));
    EXPECT_EQ(SessionTag(service, "key"), "b");

    ASSERT_TRUE(service.UpsertStart(client, "key", TenantId::Default(), 4096,
                                    Tagged("")));
    ASSERT_TRUE(service.UpsertEnd(client, "key", TenantId::Default(),
                                  ReplicaType::MEMORY));
    EXPECT_EQ(SessionTag(service, "key"), "b");
    // Preempt an unfinished tagged write; its pending tag must not survive.
    ASSERT_TRUE(service.UpsertStart(client, "key", TenantId::Default(), 4096,
                                    Tagged("failed")));
    EXPECT_EQ(SessionTag(service, "key"), "b");
    ASSERT_TRUE(
        service.UpsertStart(client, "key", TenantId::Default(), 4096, {}));
    ASSERT_TRUE(service.UpsertEnd(client, "key", TenantId::Default(),
                                  ReplicaType::MEMORY));
    EXPECT_EQ(SessionTag(service, "key"), "b");
    ASSERT_TRUE(service.Remove("key", TenantId::Default(), true));
    PutCompletedObject(service, client, "key", {});
    EXPECT_EQ(SessionTag(service, "key"), "");
}

TEST_F(MasterServiceTest, SessionTagValidationAndBatchUpsert) {
    MasterService service;
    PrepareSimpleSegment(service);
    const auto client = generate_uuid();
    EXPECT_FALSE(service.PutStart(client, "bad", TenantId::Default(), 1024,
                                  Tagged(std::string(257, 'x'))));
    EXPECT_FALSE(service.ExistKey("bad", TenantId::Default()).value_or(false));
    auto config = Tagged("a");
    auto results = service.BatchUpsertStart(
        client, {"a", "b"}, TenantId::Default(), {1024, 1024}, config);
    ASSERT_EQ(results.size(), 2);
    EXPECT_FALSE(results[0]);
    EXPECT_FALSE(results[1]);
    config.session_ids = std::vector<std::string>{"a", "b"};
    results = service.BatchUpsertStart(client, {"a", "b"}, TenantId::Default(),
                                       {1024, 1024}, config);
    ASSERT_TRUE(results[0]);
    ASSERT_TRUE(results[1]);
    for (const auto& key : {"a", "b"}) {
        ASSERT_TRUE(service.UpsertEnd(client, key, TenantId::Default(),
                                      ReplicaType::MEMORY));
        EXPECT_EQ(SessionTag(service, key), key);
    }
}

TEST_F(MasterServiceTest, SessionTagRevokedWriteDoesNotTagRetry) {
    MasterService service;
    PrepareSimpleSegment(service);
    const auto client = generate_uuid();
    ASSERT_TRUE(service.PutStart(client, "key", TenantId::Default(), 1024,
                                 Tagged("revoked")));
    ASSERT_TRUE(service.PutRevoke(client, "key", TenantId::Default(),
                                  ReplicaType::MEMORY));
    PutCompletedObject(service, client, "key", {});
    EXPECT_EQ(SessionTag(service, "key"), "");
}

TEST_F(MasterServiceTest, SessionTagOpLogAndStandbyRestore) {
    MasterService service(
        MasterServiceConfig::builder().set_default_kv_lease_ttl(0).build());
    PrepareSimpleSegment(service);
    const auto client = generate_uuid();
    PutCompletedObject(service, client, "key", Tagged("committed"));
    ASSERT_TRUE(service.UpsertStart(client, "key", TenantId::Default(), 1024,
                                    Tagged("pending")));
    std::string payload;
    {
        MasterServiceTestPeer::MetadataAccessorRO access(
            &service,
            MasterServiceTestPeer(service).MakeObjectIdentityForRequest(
                "key", TenantId::Default()));
        payload = MasterServiceTestPeer::SerializeMetadataForOpLogForTesting(
            service, access.Get());
    }
    auto decoded = struct_pack::deserialize<MetadataPayload>(payload);
    ASSERT_TRUE(decoded);
    EXPECT_EQ(decoded->session_id.value_or(""), "committed");
    auto restored_meta = decoded->ToStandbyMetadata();
    // Disk descriptor avoids coupling this test to an original allocator
    // address.
    Replica disk(generate_uuid(), 1024, "local://session",
                 ReplicaStatus::COMPLETE);
    restored_meta.replicas = {disk.get_descriptor()};
    MasterService restored;
    ASSERT_TRUE(restored.RestoreFromStandbySnapshot(
        {{"default", "key", restored_meta}}, 0, {}));
    EXPECT_EQ(SessionTag(restored, "key"), "committed");
}
}  // namespace mooncake::test

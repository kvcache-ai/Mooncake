#include <cstdlib>
#include <new>

#include "master_service_test_fixture.h"
#include "metadata_store.h"
#include "rpc_service.h"

namespace {
// Count only allocations made by the config-splitting test on its own thread.
thread_local bool track_allocations = false;
thread_local size_t tracked_allocation_bytes = 0;
}  // namespace

void* operator new(size_t size) {
    if (void* ptr = std::malloc(size ? size : 1)) {
        if (track_allocations) tracked_allocation_bytes += size;
        return ptr;
    }
    throw std::bad_alloc();
}

void operator delete(void* ptr) noexcept { std::free(ptr); }
void operator delete(void* ptr, size_t) noexcept { std::free(ptr); }

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
    config.dfs_replica_num = 2;
    config.soft_pin_action = SoftPinAction::ENABLE;
    config.soft_pin_ttl_ms = 60000;
    auto key_config = config;
    key_config.preferred_segment = "selected-segment";
    for (size_t i = 0; i < 3; ++i) {
        key_config.SetKeyIds(config, i);
        ASSERT_EQ(key_config.session_ids->size(), 1);
        EXPECT_EQ(key_config.SessionId(), config.session_ids->at(i));
        ASSERT_EQ(key_config.group_ids->size(), 1);
        EXPECT_EQ(key_config.group_ids->front(), config.group_ids->at(i));
        EXPECT_EQ(key_config.preferred_segment, "selected-segment");
        EXPECT_EQ(key_config.dfs_replica_num, 2);
        EXPECT_EQ(key_config.soft_pin_action, SoftPinAction::ENABLE);
        EXPECT_EQ(key_config.soft_pin_ttl_ms, 60000);
    }
    EXPECT_EQ(config.session_ids->size(), 3);
    EXPECT_EQ(config.group_ids->size(), 3);
    EXPECT_TRUE(config.preferred_segment.empty());
    EXPECT_TRUE(Tagged(std::string(256, 'x')).ValidSessionIds(1));
    EXPECT_FALSE(Tagged(std::string(257, 'x')).ValidSessionIds(1));
}

TEST(SessionTagTest, BatchSessionIdSelectionAllocatesLinearly) {
    size_t previous_bytes = 0;
    for (size_t count : {1024, 4096}) {
        ReplicateConfig config;
        config.session_ids =
            std::vector<std::string>(count, std::string(64, 's'));
        size_t selected_bytes = 0;
        tracked_allocation_bytes = 0;
        track_allocations = true;
        {
            auto key_config = config;
            for (size_t i = 0; i < count; ++i) {
                key_config.SetKeyIds(config, i);
                selected_bytes += key_config.session_ids->front().size();
            }
        }
        track_allocations = false;
        const size_t allocated_bytes = tracked_allocation_bytes;
        EXPECT_EQ(selected_bytes, count * 64);
        EXPECT_LT(allocated_bytes, count * 512);
        if (previous_bytes) {
            EXPECT_LE(allocated_bytes, previous_bytes * 5);
        }
        previous_bytes = allocated_bytes;
        RecordProperty("allocated_bytes_" + std::to_string(count),
                       std::to_string(allocated_bytes));
    }
}

TEST(SessionTagTest, SessionIdsRequireValidUtf8) {
    for (const std::string id : {"", "ascii", "\xc2\x80", "会",
                                 "\xf0\x9f\x98\x80", "\xf4\x8f\xbf\xbf"}) {
        EXPECT_TRUE(Tagged(id).ValidSessionIds(1));
    }
    for (const std::string id : {"\xff", "\x80", "\xc2", "\xe4\xb8", "\xc2x",
                                 "\xc0\x80", "\xe0\x80\x80", "\xed\xa0\x80",
                                 "\xf0\x80\x80\x80", "\xf4\x90\x80\x80"}) {
        EXPECT_FALSE(Tagged(id).ValidSessionIds(1));
    }
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

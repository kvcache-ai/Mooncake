#include <atomic>
#include <barrier>
#include <thread>
#include <fstream>
#include <future>
#include <gtest/gtest.h>

#include "kv_session.h"
#include "master_service_test_fixture.h"

namespace mooncake::test {

TEST(KvSessionRegistryTest, ImplicitAttachUnionAndObjectMembershipRemoval) {
    auto registry = std::make_shared<KvSessionRegistry>();
    const auto tenant = TenantId::Default();
    std::unique_ptr<KvSessionMembership> member;
    ASSERT_TRUE(registry->Attach(member, tenant, "shared", {"a", "b", "a"}));
    EXPECT_EQ(registry->Get(tenant, "a")->member_count, 1);
    ASSERT_TRUE(registry->SetPin(tenant, "a", true));
    ASSERT_TRUE(registry->Attach(member, tenant, "shared", {"a"}));
    EXPECT_TRUE(member->IsPinned());
    ASSERT_TRUE(registry->SetPin(tenant, "b", true));
    ASSERT_TRUE(registry->SetPin(tenant, "a", false));
    EXPECT_TRUE(member->IsPinned());
    member->RemoveSession("b");
    EXPECT_FALSE(member->IsPinned());
    member->RemoveSession("a");
    EXPECT_EQ(registry->Get(tenant, "a").error(), ErrorCode::SESSION_NOT_FOUND);
    member->RemoveSession("a");
    EXPECT_EQ(registry->SetPin(tenant, "a", true).error(),
              ErrorCode::SESSION_NOT_FOUND);
}

TEST(KvSessionRegistryTest, LastObjectRemovesSessionAndPinIntent) {
    auto registry = std::make_shared<KvSessionRegistry>();
    const auto tenant = TenantId::Default();
    std::unique_ptr<KvSessionMembership> member, second;
    ASSERT_TRUE(registry->Attach(member, tenant, "x", {"a"}));
    ASSERT_TRUE(registry->Attach(second, tenant, "y", {"a"}));
    ASSERT_TRUE(registry->SetPin(tenant, "a", true));
    member.reset();
    EXPECT_EQ(registry->Get(tenant, "a")->member_count, 1);
    EXPECT_TRUE(second->IsPinned());
    second.reset();
    EXPECT_EQ(registry->Get(tenant, "a").error(), ErrorCode::SESSION_NOT_FOUND);
    ASSERT_TRUE(registry->Attach(member, tenant, "x", {"a"}));
    EXPECT_FALSE(member->IsPinned());
    // Upsert moves the same membership while holding the metadata shard lock.
    auto replacement = std::move(member);
    EXPECT_EQ(registry->Get(tenant, "a")->member_count, 1);
    std::weak_ptr<KvSessionRegistry> weak = registry;
    registry.reset();
    EXPECT_FALSE(weak.expired());
    replacement.reset();
    EXPECT_TRUE(weak.expired());
}

TEST(KvSessionRegistryTest, TenantIsolationPaginationLimitsAndFailedAdmission) {
    auto registry =
        std::make_shared<KvSessionRegistry>(KvSessionLimits{2, 2, 2, 2});
    const TenantId a("a"), b("b");
    std::unique_ptr<KvSessionMembership> x, y, z, other;
    ASSERT_TRUE(registry->Attach(x, a, "x", {"same"}));
    ASSERT_TRUE(registry->Attach(other, b, "x", {"same"}));
    ASSERT_TRUE(registry->SetPin(b, "same", true));
    EXPECT_FALSE(registry->Get(a, "same")->pinned);
    EXPECT_TRUE(registry->Get(b, "same")->pinned);
    EXPECT_EQ(registry->Attach(z, a, "z", {"new", "same"}).error(),
              ErrorCode::SESSION_LIMIT_EXCEEDED);
    EXPECT_EQ(registry->Get(a, "new").error(), ErrorCode::SESSION_NOT_FOUND);
    EXPECT_EQ(registry->Get(a, "same")->member_count, 1);
    ASSERT_TRUE(registry->Attach(y, a, "y", {"same"}));
    EXPECT_EQ(registry->Attach(z, a, "z", {"same"}).error(),
              ErrorCode::SESSION_LIMIT_EXCEEDED);
    auto page = registry->List(a, "same", "", 1).value();
    EXPECT_EQ(page.keys, std::vector<std::string>{"x"});
    auto next = registry->List(a, "same", page.next_cursor, 1).value();
    EXPECT_EQ(next.keys, std::vector<std::string>{"y"});
    EXPECT_TRUE(next.next_cursor.empty());
    EXPECT_FALSE(registry->List(a, "same", "", 0));
    EXPECT_FALSE(registry->List(a, "same", "", 3));
    other.reset();
    // Space from the empty session is immediately reusable.
    z.reset();
    ASSERT_TRUE(registry->Attach(z, b, "z", {"replacement"}));
    EXPECT_FALSE(registry->Validate({""}));
    EXPECT_FALSE(registry->Validate({std::string(4097, 'x')}));
    EXPECT_EQ(registry->Get(a, "").error(), ErrorCode::SESSION_NOT_FOUND);
}

TEST(KvSessionRegistryTest, ConcurrentIndexReadsAttachPinAndDestruction) {
    auto registry =
        std::make_shared<KvSessionRegistry>(KvSessionLimits{1, 100, 2, 100});
    auto tenant = TenantId::Default();
    for (int round = 0; round < 100; ++round) {
        const auto id = "racing-" + std::to_string(round);
        std::barrier start(3);
        std::thread writer([&] {
            start.arrive_and_wait();
            for (int i = 0; i < 20; ++i) {
                std::unique_ptr<KvSessionMembership> member;
                auto result =
                    registry->Attach(member, tenant, std::to_string(i), {id});
                EXPECT_TRUE(result);
                if (result) (void)member->IsPinned();
            }
        });
        std::thread pinner([&] {
            start.arrive_and_wait();
            for (int i = 0; i < 20; ++i)
                (void)registry->SetPin(tenant, id, i % 2);
        });
        start.arrive_and_wait();
        (void)registry->List(tenant, id, "", 10);
        writer.join();
        pinner.join();
        EXPECT_EQ(registry->Get(tenant, id).error(),
                  ErrorCode::SESSION_NOT_FOUND);
    }
}

TEST_F(MasterServiceTest, KvSessionsUpdateRetainsIntersectionAndSharedPin) {
    MasterService service(MasterServiceConfig{});
    const auto context = PrepareSimpleSegment(service);
    const auto tenant = TenantId::Default();
    for (const auto& key : {"keep", "old", "shared", "unrelated"}) {
        ReplicateConfig config;
        config.kv_sessions = KvSessionTags{{"a"}};
        if (std::string(key) == "shared")
            config.kv_sessions = KvSessionTags{{"a", "b"}};
        if (std::string(key) == "unrelated")
            config.kv_sessions = KvSessionTags{{"b"}};
        ASSERT_TRUE(
            service.PutStart(context.client_id, key, tenant, 1024, config));
        ASSERT_TRUE(service.PutEnd(context.client_id, key, tenant,
                                   ReplicaType::MEMORY));
    }
    ASSERT_TRUE(service.SetKvSessionPin("a", true, tenant));
    ASSERT_TRUE(service.SetKvSessionPin("b", true, tenant));
    const std::vector<std::string> keep{"keep", "keep", "missing", "unrelated"};
    ASSERT_TRUE(service.UpdateKvSession("a", keep, tenant));
    ASSERT_TRUE(service.UpdateKvSession("a", keep, tenant));
    EXPECT_EQ(service.ListKvSessionKeys("a", "", 10, tenant)->keys,
              std::vector<std::string>{"keep"});
    EXPECT_TRUE(service.GetKvSession("a", tenant)->pinned);
    EXPECT_TRUE(service.GetKvSession("b", tenant)->pinned);
    EXPECT_EQ(service.GetKvSession("b", tenant)->member_count, 2);
    EXPECT_TRUE(service.ExistKey("old", tenant).value());
    EXPECT_TRUE(service.ExistKey("shared", tenant).value());
    ASSERT_TRUE(service.AttachKvSession("a", {"old"}, tenant).front());
    EXPECT_TRUE(service.GetKvSession("a", tenant)->pinned);
    ASSERT_TRUE(service.UpdateKvSession("a", {}, tenant));
    EXPECT_EQ(service.GetKvSession("a", tenant).error(),
              ErrorCode::SESSION_NOT_FOUND);
    ASSERT_TRUE(service.UpdateKvSession("a", keep, tenant));
    EXPECT_EQ(service.GetKvSession("a", tenant).error(),
              ErrorCode::SESSION_NOT_FOUND);
    ASSERT_TRUE(service.AttachKvSession("a", {"old"}, tenant).front());
    EXPECT_FALSE(service.GetKvSession("a", tenant)->pinned);
}

TEST(KvSessionRegistryTest, UpdateValidatesBeforeMutationAndIsolatesTenants) {
    // Complete updates use the member limit, not the smaller batch limit.
    auto registry =
        std::make_shared<KvSessionRegistry>(KvSessionLimits{4, 3, 2, 1});
    const TenantId a("a"), b("b");
    std::unique_ptr<KvSessionMembership> x, y, other;
    ASSERT_TRUE(registry->Attach(x, a, "x", {"s"}));
    ASSERT_TRUE(registry->Attach(y, a, "y", {"s"}));
    ASSERT_TRUE(registry->Attach(other, b, "x", {"s"}));
    ASSERT_TRUE(registry->SetPin(b, "s", true));
    EXPECT_EQ(registry->ValidateUpdate("s", {"x", "y", "z", "w"}).error(),
              ErrorCode::SESSION_LIMIT_EXCEEDED);
    EXPECT_EQ(registry->Get(a, "s")->member_count, 2);
    EXPECT_EQ(registry->ValidateUpdate("", {}).error(),
              ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(registry->ValidateUpdate(std::string(4097, 's'), {}).error(),
              ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(registry->ValidateUpdate(std::string("s\0x", 3), {}).error(),
              ErrorCode::INVALID_PARAMS);
    ASSERT_TRUE(registry->ValidateUpdate("s", {"x", "missing"}));
    y->RemoveSession("s");
    EXPECT_EQ(registry->Get(a, "s")->member_count, 1);
    EXPECT_FALSE(registry->Get(a, "s")->pinned);
    x->RemoveSession("s");
    EXPECT_EQ(registry->Get(a, "s").error(), ErrorCode::SESSION_NOT_FOUND);
    EXPECT_EQ(registry->Get(b, "s")->member_count, 1);
    EXPECT_TRUE(other->IsPinned());
}

TEST_F(MasterServiceTest, KvSessionsUpdateSkipsObjectRemovedAfterEnumeration) {
    MasterService service(MasterServiceConfig{});
    const auto context = PrepareSimpleSegment(service);
    const auto tenant = TenantId::Default();
    MasterServiceTestPeer peer(service);
    // Force update to release the index lock, remove A, then wait on B's
    // existing object lock. A and B must not share a metadata shard.
    const std::string first = "a", keep = "z";
    std::string removed = "b";
    while (peer.getShardIndex(tenant, removed) ==
           peer.getShardIndex(tenant, first)) {
        removed += "b";
    }
    ReplicateConfig config;
    config.kv_sessions = KvSessionTags{{"s"}};
    for (const auto& key : {first, removed, keep}) {
        ASSERT_TRUE(
            service.PutStart(context.client_id, key, tenant, 1024, config));
        ASSERT_TRUE(service.PutEnd(context.client_id, key, tenant,
                                   ReplicaType::MEMORY));
    }
    auto& shard = MasterServiceTestPeer::MetadataShards(
        service)[peer.getShardIndex(tenant, removed)];
    std::unique_lock object_lock(shard.mutex);
    auto update = std::async(std::launch::async, [&] {
        return service.UpdateKvSession("s", {keep}, tenant);
    });
    // A separate task makes a lock regression fail with a timeout instead of
    // hanging the test. Index access and pin must work while update waits.
    auto progress = std::async(std::launch::async, [&] {
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (std::chrono::steady_clock::now() < deadline) {
            auto info = service.GetKvSession("s", tenant);
            if (info && info->member_count == 2) {
                return bool(service.SetKvSessionPin("s", true, tenant));
            }
            std::this_thread::yield();
        }
        return false;
    });
    const auto ready = progress.wait_for(std::chrono::seconds(6));
    EXPECT_EQ(ready, std::future_status::ready);
    if (ready == std::future_status::ready) EXPECT_TRUE(progress.get());
    EXPECT_EQ(update.wait_for(std::chrono::milliseconds(0)),
              std::future_status::timeout);
    // Simulate background object cleanup while owning its normal shard lock.
    shard.tenants.at(tenant).metadata.erase(removed);
    object_lock.unlock();
    EXPECT_TRUE(update.get());
    EXPECT_EQ(service.ListKvSessionKeys("s", "", 10, tenant)->keys,
              std::vector<std::string>{keep});
    EXPECT_TRUE(service.GetKvSession("s", tenant)->pinned);
    EXPECT_FALSE(service.ExistKey(removed, tenant).value());
    EXPECT_TRUE(service.ExistKey(first, tenant).value());
}

TEST_F(MasterServiceTest, KvSessionsUpdateAndCloseTraverseMultiplePages) {
    MasterService service(
        MasterServiceConfig::builder().set_default_kv_lease_ttl(0).build());
    const auto context = PrepareSimpleSegment(service);
    const auto tenant = TenantId::Default();
    ReplicateConfig config;
    config.kv_sessions = KvSessionTags{{"s"}};
    std::vector<std::string> keep;
    for (int i = 0; i < 600; ++i) {
        const auto key = "page-" + std::to_string(i);
        ASSERT_TRUE(
            service.PutStart(context.client_id, key, tenant, 1024, config));
        ASSERT_TRUE(service.PutEnd(context.client_id, key, tenant,
                                   ReplicaType::MEMORY));
        if (i % 2 == 0) keep.push_back(key);
    }
    ASSERT_TRUE(service.SetKvSessionPin("s", true, tenant));
    ASSERT_TRUE(service.UpdateKvSession("s", keep, tenant));
    EXPECT_EQ(service.GetKvSession("s", tenant)->member_count, keep.size());
    EXPECT_TRUE(service.GetKvSession("s", tenant)->pinned);
    ASSERT_TRUE(service.CloseKvSession("s", tenant));
    EXPECT_EQ(service.GetKvSession("s", tenant).error(),
              ErrorCode::SESSION_NOT_FOUND);
    EXPECT_TRUE(service.ExistKey("page-1", tenant).value());
    EXPECT_TRUE(service.ExistKey("page-598", tenant).value());
    ASSERT_TRUE(service.CloseKvSession("s", tenant));
}

// Exact pre-feature wire layout, to exercise struct_pack compatibility.
struct LegacyReplicateConfig {
    size_t replica_num{1}, nof_replica_num{0}, dfs_replica_num{0};
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

TEST(KvSessionRegistryTest, OrdinaryWriteWireCompatibilityAndPerKeyTags) {
    LegacyReplicateConfig old;
    old.replica_num = 2;
    old.host_id = "host";
    auto decoded =
        struct_pack::deserialize<ReplicateConfig>(struct_pack::serialize(old));
    ASSERT_TRUE(decoded);
    EXPECT_EQ(decoded->replica_num, 2);
    EXPECT_EQ(decoded->host_id, "host");
    EXPECT_FALSE(decoded->kv_sessions.has_value());
    ReplicateConfig config;
    config.kv_sessions = KvSessionTags{{"a"}, {"b"}};
    auto legacy_decoded = struct_pack::deserialize<LegacyReplicateConfig>(
        struct_pack::serialize(config));
    ASSERT_TRUE(legacy_decoded);
    auto roundtrip = struct_pack::deserialize<ReplicateConfig>(
        struct_pack::serialize(config));
    ASSERT_TRUE(roundtrip);
    EXPECT_EQ(*roundtrip->kv_sessions, *config.kv_sessions);
    EXPECT_EQ(
        config.ForKeys(std::vector<size_t>{1, 0, 1}).kv_sessions->at(0).front(),
        "b");
    EXPECT_EQ(config.ForSingleKey(1).kv_sessions->size(), 1);
}

TEST(KvSessionRegistryTest, SharedPrefixSupportsOneHundredThousandSessions) {
    KvSessionLimits limits;
    ASSERT_EQ(limits.sessions_per_object, 100000);
    limits.sessions = limits.sessions_per_object + 1;
    auto registry = std::make_shared<KvSessionRegistry>(limits);
    const auto tenant = TenantId::Default();
    std::unique_ptr<KvSessionMembership> member;
    std::vector<std::string> ids;
    for (size_t i = 0; i < limits.sessions_per_object; ++i) {
        ids.push_back("session-" + std::to_string(i));
        ASSERT_TRUE(registry->Attach(member, tenant, "shared", {ids.back()}));
    }
    ASSERT_TRUE(registry->Attach(member, tenant, "shared", ids));
    EXPECT_FALSE(member->IsPinned());
    ASSERT_TRUE(registry->SetPin(tenant, ids.back(), true));
    EXPECT_TRUE(member->IsPinned());
    EXPECT_EQ(registry->Attach(member, tenant, "shared", {"overflow"}).error(),
              ErrorCode::SESSION_LIMIT_EXCEEDED);
    EXPECT_EQ(registry->Get(tenant, "overflow").error(),
              ErrorCode::SESSION_NOT_FOUND);
    member->RemoveSession(ids.back());
    EXPECT_FALSE(member->IsPinned());
    ASSERT_TRUE(registry->Attach(member, tenant, "shared", {"overflow"}));
    member.reset();
    EXPECT_EQ(registry->Get(tenant, ids.front()).error(),
              ErrorCode::SESSION_NOT_FOUND);
    EXPECT_EQ(registry->Get(tenant, "overflow").error(),
              ErrorCode::SESSION_NOT_FOUND);
}

TEST(KvSessionRegistryTest, IndexedConfigPreservesOptionsAndPhysicalKeyTags) {
    ReplicateConfig config;
    config.replica_num = 2;
    config.nof_replica_num = 3;
    config.dfs_replica_num = 1;
    config.soft_pin_action = SoftPinAction::ENABLE;
    config.soft_pin_ttl_ms = 500;
    config.with_hard_pin = true;
    config.preferred_segments = {"m1", "m2"};
    config.preferred_segment = "m1";
    config.preferred_nof_segments = {"n1"};
    config.prefer_alloc_in_same_node = true;
    config.data_type = ObjectDataType::KVCACHE;
    config.host_id = "host";
    config.group_ids = std::vector<std::string>{"g0", "g1", "g2"};
    config.kv_sessions = KvSessionTags{{"a"}, {}, {"b", "c"}};
    const auto original = struct_pack::serialize(config);
    auto expected = config;
    expected.group_ids = std::vector<std::string>{"g2", "g2", "g0", "g0"};
    expected.kv_sessions = KvSessionTags{{"b", "c"}, {"b", "c"}, {"a"}, {"a"}};
    EXPECT_EQ(
        struct_pack::serialize(config.ForKeys(std::vector<size_t>{2, 0}, 2)),
        struct_pack::serialize(expected));
    expected.group_ids = std::vector<std::string>{"g1"};
    expected.kv_sessions = KvSessionTags{{}};
    EXPECT_EQ(struct_pack::serialize(config.ForSingleKey(1)),
              struct_pack::serialize(expected));
    EXPECT_EQ(struct_pack::serialize(config), original);
    EXPECT_THROW(config.ForSingleKey(3), std::out_of_range);
    config.group_ids.reset();
    config.kv_sessions.reset();
    EXPECT_EQ(struct_pack::serialize(config.ForSingleKey(0)),
              struct_pack::serialize(config));
}

TEST_F(MasterServiceTest, KvSessionsRejectedUpsertDoesNotAttach) {
    MasterService service(MasterServiceConfig::builder()
                              .set_memory_allocator(BufferAllocatorType::OFFSET)
                              .set_default_kv_lease_ttl(10000)
                              .build());
    const auto context = PrepareSimpleSegment(service);
    const auto tenant = TenantId::Default();
    ReplicateConfig original;
    original.group_ids = std::vector<std::string>{"group-a"};
    original.kv_sessions = KvSessionTags{{"original"}};
    ASSERT_TRUE(
        service.PutStart(context.client_id, "x", tenant, 1024, original));
    ASSERT_TRUE(
        service.PutEnd(context.client_id, "x", tenant, ReplicaType::MEMORY));
    ASSERT_TRUE(service.SetKvSessionPin("original", true, tenant));
    ReplicateConfig rejected;
    rejected.group_ids = std::vector<std::string>{"group-b"};
    rejected.kv_sessions = KvSessionTags{{"rejected"}};
    EXPECT_EQ(
        service.UpsertStart(context.client_id, "x", tenant, 1024, rejected)
            .error(),
        ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(service.GetKvSession("rejected", tenant).error(),
              ErrorCode::SESSION_NOT_FOUND);
    rejected.group_ids = original.group_ids;
    {
        MasterServiceTestPeer::MetadataAccessorRW accessor(
            &service, MasterServiceTestPeer::ObjectIdentity{tenant, "x"});
        accessor.Get().VisitReplicas(
            &Replica::fn_is_completed,
            [](Replica& replica) { replica.inc_refcnt(); });
    }
    EXPECT_EQ(
        service.UpsertStart(context.client_id, "x", tenant, 1024, rejected)
            .error(),
        ErrorCode::OBJECT_REPLICA_BUSY);
    EXPECT_EQ(service.GetKvSession("rejected", tenant).error(),
              ErrorCode::SESSION_NOT_FOUND);
    {
        MasterServiceTestPeer::MetadataAccessorRW accessor(
            &service, MasterServiceTestPeer::ObjectIdentity{tenant, "x"});
        accessor.Get().VisitReplicas(
            &Replica::fn_is_completed,
            [](Replica& replica) { replica.dec_refcnt(); });
    }
    ASSERT_TRUE(service.GetReplicaList("x", tenant));
    auto rejected_result = service.UpsertStart(
        context.client_id, "x", tenant, kDefaultSegmentSize * 2, rejected);
    ASSERT_FALSE(rejected_result);
    EXPECT_EQ(rejected_result.error(), ErrorCode::NO_AVAILABLE_HANDLE);
    EXPECT_EQ(service.GetKvSession("rejected", tenant).error(),
              ErrorCode::SESSION_NOT_FOUND);
    EXPECT_TRUE(service.GetKvSession("original", tenant)->pinned);
    EXPECT_TRUE(service.ExistKey("x", tenant).value());
}

TEST_F(MasterServiceTest, KvSessionsPreemptionPreservesMembershipAndPin) {
    MasterService service(MasterServiceConfig::builder()
                              .set_memory_allocator(BufferAllocatorType::OFFSET)
                              .set_default_kv_lease_ttl(0)
                              .build());
    const auto context = PrepareSimpleSegment(service);
    const auto tenant = TenantId::Default();
    for (bool complete_first : {false, true}) {
        for (bool tagged : {false, true}) {
            const auto key =
                std::to_string(complete_first) + std::to_string(tagged);
            const auto a = key + "-a", b = key + "-b", c = key + "-c";
            ReplicateConfig original;
            original.kv_sessions = KvSessionTags{{a, b}};
            ASSERT_TRUE(service.PutStart(context.client_id, key, tenant, 1024,
                                         original));
            ASSERT_TRUE(service.SetKvSessionPin(a, true, tenant));
            if (complete_first) {
                ASSERT_TRUE(service.PutEnd(context.client_id, key, tenant,
                                           ReplicaType::MEMORY));
                ASSERT_TRUE(service.UpsertStart(context.client_id, key, tenant,
                                                1024, {}));
            }
            ReplicateConfig replacement;
            if (tagged) replacement.kv_sessions = KvSessionTags{{b, c}};
            const auto writer = generate_uuid();
            ASSERT_TRUE(
                service.UpsertStart(writer, key, tenant, 2048, replacement));
            ASSERT_TRUE(
                service.UpsertEnd(writer, key, tenant, ReplicaType::MEMORY));
            ASSERT_TRUE(service.GetKvSession(a, tenant));
            EXPECT_TRUE(service.GetKvSession(a, tenant)->pinned);
            EXPECT_EQ(service.GetKvSession(a, tenant)->member_count, 1);
            EXPECT_EQ(service.GetKvSession(b, tenant)->member_count, 1);
            if (tagged) {
                EXPECT_EQ(service.GetKvSession(c, tenant)->member_count, 1);
            }
            ASSERT_TRUE(service.Remove(key, tenant, true));
            EXPECT_EQ(service.GetKvSession(a, tenant).error(),
                      ErrorCode::SESSION_NOT_FOUND);
        }
    }
    ReplicateConfig original;
    original.kv_sessions = KvSessionTags{{"failed"}};
    ASSERT_TRUE(
        service.PutStart(context.client_id, "failed", tenant, 1024, original));
    ASSERT_TRUE(service.SetKvSessionPin("failed", true, tenant));
    ReplicateConfig replacement;
    replacement.kv_sessions = KvSessionTags{{"new"}};
    auto failed_result =
        service.UpsertStart(context.client_id, "failed", tenant,
                            kDefaultSegmentSize * 2, replacement);
    ASSERT_FALSE(failed_result);
    EXPECT_EQ(failed_result.error(), ErrorCode::NO_AVAILABLE_HANDLE);
    EXPECT_EQ(service.GetKvSession("failed", tenant).error(),
              ErrorCode::SESSION_NOT_FOUND);
    EXPECT_EQ(service.GetKvSession("new", tenant).error(),
              ErrorCode::SESSION_NOT_FOUND);
}

TEST_F(MasterServiceTest, KvSessionsWriteDedupUpsertRevokeAndClose) {
    MasterService service(
        MasterServiceConfig::builder().set_default_kv_lease_ttl(0).build());
    const auto context = PrepareSimpleSegment(service);
    const auto tenant = TenantId::Default();
    const std::string a = "a";
    const std::string b = "b";
    ReplicateConfig config;
    config.kv_sessions = KvSessionTags{{a}};
    ASSERT_TRUE(service.PutStart(context.client_id, "x", tenant, 1024, config));
    ASSERT_TRUE(service.SetKvSessionPin(a, true, tenant));
    EXPECT_EQ(service.GetKvSession(a, tenant)->member_count, 1);
    ASSERT_TRUE(
        service.PutEnd(context.client_id, "x", tenant, ReplicaType::MEMORY));
    config.kv_sessions = KvSessionTags{{b}};
    EXPECT_EQ(
        service.PutStart(context.client_id, "x", tenant, 1024, config).error(),
        ErrorCode::OBJECT_ALREADY_EXISTS);
    EXPECT_EQ(service.GetKvSession(b, tenant)->member_count, 1);
    // Reallocation keeps the accepted memberships without new tags.
    ASSERT_TRUE(service.UpsertStart(context.client_id, "x", tenant, 2048,
                                    ReplicateConfig{}));
    ASSERT_TRUE(
        service.UpsertEnd(context.client_id, "x", tenant, ReplicaType::MEMORY));
    EXPECT_EQ(service.GetKvSession(a, tenant)->member_count, 1);
    EXPECT_EQ(service.GetKvSession(b, tenant)->member_count, 1);
    ASSERT_TRUE(service.CloseKvSession(a, tenant));
    EXPECT_TRUE(service.ExistKey("x", tenant).value());
    EXPECT_EQ(service.GetKvSession(b, tenant)->member_count, 1);
    ASSERT_TRUE(service.Remove("x", tenant, true));
    EXPECT_EQ(service.GetKvSession(b, tenant).error(),
              ErrorCode::SESSION_NOT_FOUND);
    ASSERT_TRUE(
        service.PutStart(context.client_id, "aborted", tenant, 1024, config));
    ASSERT_TRUE(service.PutRevoke(context.client_id, "aborted", tenant,
                                  ReplicaType::MEMORY));
    EXPECT_EQ(service.GetKvSession(b, tenant).error(),
              ErrorCode::SESSION_NOT_FOUND);
}

TEST_F(MasterServiceTest, KvSessionsStandbyPromotionRequiresExplicitRebuild) {
    const auto tenant = TenantId::Default();
    const std::string old = "recover";
    MasterService promoted(MasterServiceConfig{});
    Replica replica(generate_uuid(), 128, "local://standby",
                    ReplicaStatus::COMPLETE);
    StandbyObjectMetadata metadata;
    metadata.client_id = generate_uuid();
    metadata.size = 128;
    metadata.replicas.push_back(replica.get_descriptor());
    ASSERT_TRUE(promoted.RestoreFromStandbySnapshot(
        {{tenant.value(), "restored", metadata}}, 0, {}));
    ASSERT_TRUE(promoted.ExistKey("restored", tenant).value());
    EXPECT_EQ(promoted.GetKvSession(old, tenant).error(),
              ErrorCode::SESSION_NOT_FOUND);
    const auto current = old;
    ASSERT_TRUE(
        promoted.AttachKvSession(current, {"restored"}, tenant).front());
    EXPECT_FALSE(promoted.GetKvSession(current, tenant)->pinned);
    ASSERT_TRUE(promoted.SetKvSessionPin(current, true, tenant));
    EXPECT_EQ(promoted.GetKvSession(current, tenant)->member_count, 1);
}

TEST_F(MasterServiceTest, KvSessionsPreferUnpinnedThenPressureFallback) {
    MasterService service(
        MasterServiceConfig::builder().set_default_kv_lease_ttl(0).build());
    const auto context = PrepareSimpleSegment(service);
    auto tenant = TenantId::Default();
    const std::string a = "pinned";
    ReplicateConfig pinned;
    pinned.kv_sessions = KvSessionTags{{a}};
    // Pinned object is older, but ordinary object must be evicted first.
    ASSERT_TRUE(
        service.PutStart(context.client_id, "pinned", tenant, 1024, pinned));
    ASSERT_TRUE(service.PutEnd(context.client_id, "pinned", tenant,
                               ReplicaType::MEMORY));
    ASSERT_TRUE(service.SetKvSessionPin(a, true, tenant));
    ASSERT_TRUE(service.PutStart(context.client_id, "ordinary", tenant, 1024,
                                 ReplicateConfig{}));
    ASSERT_TRUE(service.PutEnd(context.client_id, "ordinary", tenant,
                               ReplicaType::MEMORY));
    MasterServiceTestPeer(service).RunBatchEvictForTesting(0.5, 0.5);
    EXPECT_TRUE(service.ExistKey("pinned", tenant).value());
    EXPECT_FALSE(service.ExistKey("ordinary", tenant).value());
    MasterServiceTestPeer(service).RunBatchEvictForTesting(1.0, 1.0);
    EXPECT_FALSE(service.ExistKey("pinned", tenant).value());
    EXPECT_EQ(service.GetKvSession(a, tenant).error(),
              ErrorCode::SESSION_NOT_FOUND);
}

TEST_F(MasterServiceTest, KvSessionsUpdateReleasesOnlyRemovedSessionPin) {
    MasterService service(MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(0)
                              .set_allow_evict_soft_pinned_objects(false)
                              .build());
    const auto context = PrepareSimpleSegment(service);
    const auto tenant = TenantId::Default();
    for (const auto& key : {"keep", "old", "shared", "ttl"}) {
        ReplicateConfig config;
        config.kv_sessions = KvSessionTags{{"a"}};
        if (std::string(key) == "shared")
            config.kv_sessions = KvSessionTags{{"a", "b"}};
        if (std::string(key) == "ttl") {
            config.soft_pin_action = SoftPinAction::ENABLE;
            config.soft_pin_ttl_ms = 60000;
        }
        ASSERT_TRUE(
            service.PutStart(context.client_id, key, tenant, 1024, config));
        ASSERT_TRUE(service.PutEnd(context.client_id, key, tenant,
                                   ReplicaType::MEMORY));
    }
    ASSERT_TRUE(service.SetKvSessionPin("a", true, tenant));
    ASSERT_TRUE(service.SetKvSessionPin("b", true, tenant));
    ASSERT_TRUE(service.UpdateKvSession("a", {"keep"}, tenant));
    EXPECT_TRUE(service.ExistKey("old", tenant).value());
    EXPECT_TRUE(service.GetKvSession("a", tenant)->pinned);
    EXPECT_EQ(service.GetKvSession("a", tenant)->member_count, 1);
    MasterServiceTestPeer(service).RunBatchEvictForTesting(1.0, 1.0);
    EXPECT_FALSE(service.ExistKey("old", tenant).value());
    EXPECT_TRUE(service.ExistKey("keep", tenant).value());
    EXPECT_TRUE(service.ExistKey("shared", tenant).value());
    EXPECT_TRUE(service.ExistKey("ttl", tenant).value());
    ASSERT_TRUE(service.UpdateKvSession("a", {}, tenant));
    MasterServiceTestPeer(service).RunBatchEvictForTesting(1.0, 1.0);
    EXPECT_FALSE(service.ExistKey("keep", tenant).value());
    EXPECT_TRUE(service.ExistKey("shared", tenant).value());
    EXPECT_TRUE(service.ExistKey("ttl", tenant).value());
    EXPECT_EQ(service.GetKvSession("a", tenant).error(),
              ErrorCode::SESSION_NOT_FOUND);
}

TEST_F(MasterServiceTest, KvSessionsCloseDuringWriteAndPartialAttach) {
    MasterService service(MasterServiceConfig{});
    const auto context = PrepareSimpleSegment(service);
    auto tenant = TenantId::Default();
    const std::string a = "a";
    ReplicateConfig config;
    config.kv_sessions = KvSessionTags{{a}};
    ASSERT_TRUE(service.PutStart(context.client_id, "x", tenant, 1024, config));
    ASSERT_TRUE(service.CloseKvSession(a, tenant));
    const std::string next = "b";
    ASSERT_TRUE(
        service.PutEnd(context.client_id, "x", tenant, ReplicaType::MEMORY));
    EXPECT_EQ(service.GetKvSession(next, tenant).error(),
              ErrorCode::SESSION_NOT_FOUND);
    auto attached = service.AttachKvSession(next, {"x", "missing"}, tenant);
    ASSERT_EQ(attached.size(), 2);
    EXPECT_TRUE(attached[0]);
    EXPECT_EQ(attached[1].error(), ErrorCode::OBJECT_NOT_FOUND);
    EXPECT_EQ(service.GetKvSession(next, tenant)->member_count, 1);
    // A new tagged write can establish membership again after close.
    ASSERT_TRUE(
        service.PutStart(context.client_id, "late", tenant, 1024, config));
    EXPECT_FALSE(service.GetKvSession(a, tenant)->pinned);
    EXPECT_EQ(service.GetKvSession(a, tenant)->member_count, 1);
}

TEST_F(MasterServiceTest, KvSessionsRespectNativeTtlHardPinAndFallbackSwitch) {
    auto config = MasterServiceConfig::builder()
                      .set_default_kv_lease_ttl(0)
                      .set_allow_evict_soft_pinned_objects(false)
                      .build();
    MasterService service(config);
    const auto context = PrepareSimpleSegment(service);
    const auto tenant = TenantId::Default();
    const std::string session = "session";
    ReplicateConfig tagged;
    tagged.kv_sessions = KvSessionTags{{session}};
    tagged.group_ids = std::vector<std::string>{"group"};
    ASSERT_TRUE(
        service.PutStart(context.client_id, "session", tenant, 1024, tagged));
    ASSERT_TRUE(service.PutEnd(context.client_id, "session", tenant,
                               ReplicaType::MEMORY));
    ASSERT_TRUE(service.SetKvSessionPin(session, true, tenant));
    ReplicateConfig ttl;
    ttl.soft_pin_action = SoftPinAction::ENABLE;
    ttl.soft_pin_ttl_ms = 10000;
    ttl.kv_sessions = KvSessionTags{{session}};
    ASSERT_TRUE(service.PutStart(context.client_id, "ttl", tenant, 1024, ttl));
    ASSERT_TRUE(
        service.PutEnd(context.client_id, "ttl", tenant, ReplicaType::MEMORY));
    ReplicateConfig hard;
    hard.with_hard_pin = true;
    ASSERT_TRUE(
        service.PutStart(context.client_id, "hard", tenant, 1024, hard));
    ASSERT_TRUE(
        service.PutEnd(context.client_id, "hard", tenant, ReplicaType::MEMORY));
    MasterServiceTestPeer(service).RunBatchEvictForTesting(1.0, 1.0);
    EXPECT_TRUE(service.ExistKey("session", tenant).value());
    ASSERT_TRUE(service.SetKvSessionPin(session, false, tenant));
    MasterServiceTestPeer(service).RunBatchEvictForTesting(1.0, 1.0);
    EXPECT_FALSE(service.ExistKey("session", tenant).value());
    EXPECT_TRUE(service.ExistKey("ttl", tenant).value());
    EXPECT_TRUE(service.ExistKey("hard", tenant).value());
    EXPECT_EQ(service.GetKvSession(session, tenant)->member_count, 1);
}

TEST_F(MasterServiceTest, KvSessionsKeepMembershipUntilLastReplicaDisappears) {
    MasterService service(MasterServiceConfig{});
    auto first = PrepareSimpleSegment(service, "first");
    auto second = PrepareSimpleSegment(
        service, "second", kDefaultSegmentBase + kDefaultSegmentSize);
    auto tenant = TenantId::Default();
    const std::string session = "s";
    ReplicateConfig config;
    config.replica_num = 2;
    config.kv_sessions = KvSessionTags{{session}};
    ASSERT_TRUE(service.PutStart(first.client_id, "x", tenant, 1024, config));
    ASSERT_TRUE(
        service.PutEnd(first.client_id, "x", tenant, ReplicaType::MEMORY));
    ASSERT_TRUE(service.UnmountSegment(first.segment_id, first.client_id));
    ClearInvalidHandlesForTest(service);
    EXPECT_TRUE(service.ExistKey("x", tenant).value());
    EXPECT_EQ(service.GetKvSession(session, tenant)->member_count, 1);
    ASSERT_TRUE(service.UnmountSegment(second.segment_id, second.client_id));
    ClearInvalidHandlesForTest(service);
    EXPECT_FALSE(service.ExistKey("x", tenant).value());
    EXPECT_EQ(service.GetKvSession(session, tenant).error(),
              ErrorCode::SESSION_NOT_FOUND);
}

TEST_F(MasterServiceTest, KvSessionsUseExistingTenantContext) {
    MasterService service(MakeStrictTenantConfig({"a", "b"}));
    auto context = PrepareSimpleSegment(service);
    const std::string a = "same";
    const std::string b = "same";
    ReplicateConfig config;
    config.kv_sessions = KvSessionTags{{a}};
    ASSERT_TRUE(service.PutStart(context.client_id, "same", TenantId("a"), 1024,
                                 config));
    ASSERT_TRUE(service.PutEnd(context.client_id, "same", TenantId("a"),
                               ReplicaType::MEMORY));
    EXPECT_EQ(a, b);
    EXPECT_EQ(service.GetKvSession(b, TenantId("b")).error(),
              ErrorCode::SESSION_NOT_FOUND);
    ASSERT_TRUE(service.SetKvSessionPin(a, true, TenantId("a")));
    config.kv_sessions = KvSessionTags{{b}};
    ASSERT_TRUE(service.PutStart(context.client_id, "same", TenantId("b"), 1024,
                                 config));
    ASSERT_TRUE(service.PutEnd(context.client_id, "same", TenantId("b"),
                               ReplicaType::MEMORY));
    EXPECT_FALSE(service.GetKvSession(b, TenantId("b"))->pinned);
    ASSERT_GE(service.RemoveAll(TenantId("a"), true), 1);
    EXPECT_EQ(service.GetKvSession(a, TenantId("a")).error(),
              ErrorCode::SESSION_NOT_FOUND);
    EXPECT_EQ(service.GetKvSession(b, TenantId("b"))->member_count, 1);
}

}  // namespace mooncake::test

#include "fixture.h"

namespace mooncake::test {
TEST_F(MasterServiceTest, PutStartGroupIdsValidation) {
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    ReplicateConfig config;
    config.replica_num = 1;

    config.group_ids = std::vector<std::string>{};
    auto empty_group_ids = service_->PutStart(client_id, "empty_group_ids",
                                              "default", 1024, config);
    EXPECT_FALSE(empty_group_ids.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, empty_group_ids.error());

    config.group_ids = std::vector<std::string>{"g0", "g1"};
    auto too_many_group_ids = service_->PutStart(
        client_id, "too_many_group_ids", "default", 1024, config);
    EXPECT_FALSE(too_many_group_ids.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, too_many_group_ids.error());

    config.group_ids = std::vector<std::string>{""};
    auto ungrouped = service_->PutStart(client_id, "explicit_ungrouped",
                                        "default", 1024, config);
    ASSERT_TRUE(ungrouped.has_value());
    ASSERT_TRUE(service_
                    ->PutEnd(client_id, "explicit_ungrouped", "default",
                             ReplicaType::MEMORY)
                    .has_value());
    auto exists = service_->ExistKey("explicit_ungrouped", "default");
    ASSERT_TRUE(exists.has_value());
    EXPECT_TRUE(exists.value());
}

TEST_F(MasterServiceTest, GroupedObjectRoutesKeyLevelLookupAndRemove) {
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    const std::string key = "grouped_route_key";
    ReplicateConfig config;
    config.replica_num = 1;
    config.group_ids =
        std::vector<std::string>{FindGroupIdOnDifferentShard(key)};

    PutCompletedObject(*service_, client_id, key, config);

    auto exists = service_->ExistKey(key, "default");
    ASSERT_TRUE(exists.has_value());
    EXPECT_TRUE(exists.value());
    EXPECT_TRUE(service_->GetReplicaList(key, "default").has_value());

    ASSERT_TRUE(service_->Remove(key, "default", /*force=*/true).has_value());
    auto exists_after_remove = service_->ExistKey(key, "default");
    ASSERT_TRUE(exists_after_remove.has_value());
    EXPECT_FALSE(exists_after_remove.value());
}

TEST_F(MasterServiceTest, GroupRoutingIsTenantScopedForSameUserKey) {
    const std::string key = "tenant_grouped_shared_user_key";
    const std::string tenant_a = "tenant_group_route_a";
    const std::string tenant_b = "tenant_group_route_b";
    auto service_ = std::make_unique<MasterService>(
        MakeStrictTenantConfig({tenant_a, tenant_b}));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    const std::string group_a = FindGroupIdOnDifferentShard(key);
    std::string group_b;
    for (int i = 0; i < 10000; ++i) {
        group_b = key + "_tenant_b_group_" + std::to_string(i);
        if (std::hash<std::string>{}(group_b) % 1024 !=
            std::hash<std::string>{}(group_a) % 1024) {
            break;
        }
    }

    ReplicateConfig config_a;
    config_a.replica_num = 1;
    config_a.group_ids = std::vector<std::string>{group_a};
    ReplicateConfig config_b;
    config_b.replica_num = 1;
    config_b.group_ids = std::vector<std::string>{group_b};

    ASSERT_TRUE(service_->PutStart(client_id, key, tenant_a, 1024, config_a)
                    .has_value());
    ASSERT_TRUE(service_->PutEnd(client_id, key, tenant_a, ReplicaType::MEMORY)
                    .has_value());
    ASSERT_TRUE(service_->PutStart(client_id, key, tenant_b, 2048, config_b)
                    .has_value());
    ASSERT_TRUE(service_->PutEnd(client_id, key, tenant_b, ReplicaType::MEMORY)
                    .has_value());

    EXPECT_TRUE(service_->ExistKey(key, tenant_a).value_or(false));
    EXPECT_TRUE(service_->ExistKey(key, tenant_b).value_or(false));
    EXPECT_TRUE(service_->GetReplicaList(key, tenant_a).has_value());
    EXPECT_TRUE(service_->GetReplicaList(key, tenant_b).has_value());

    ASSERT_TRUE(service_->Remove(key, tenant_a, /*force=*/true).has_value());
    EXPECT_FALSE(service_->GetReplicaList(key, tenant_a).has_value());
    EXPECT_TRUE(service_->GetReplicaList(key, tenant_b).has_value());
}

TEST_F(MasterServiceTest, BatchGetReplicaListPreservesOrderWithGroupedKeys) {
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    const std::string grouped_key_a = "batch_get_grouped_a";
    const std::string missing_key = "batch_get_missing";
    const std::string ungrouped_key = "batch_get_ungrouped";
    const std::string grouped_key_b = "batch_get_grouped_b";
    const std::string pending_key = "batch_get_pending";

    ReplicateConfig grouped_config_a;
    grouped_config_a.replica_num = 1;
    grouped_config_a.group_ids =
        std::vector<std::string>{FindGroupIdOnDifferentShard(grouped_key_a)};
    PutCompletedObject(*service_, client_id, grouped_key_a, grouped_config_a);

    ReplicateConfig ungrouped_config;
    ungrouped_config.replica_num = 1;
    PutCompletedObject(*service_, client_id, ungrouped_key, ungrouped_config);

    ReplicateConfig grouped_config_b;
    grouped_config_b.replica_num = 1;
    grouped_config_b.group_ids =
        std::vector<std::string>{FindGroupIdOnDifferentShard(grouped_key_b)};
    PutCompletedObject(*service_, client_id, grouped_key_b, grouped_config_b);

    ASSERT_TRUE(service_
                    ->PutStart(client_id, pending_key, "default", 1024,
                               ungrouped_config)
                    .has_value());

    const std::vector<std::string> keys = {
        grouped_key_a, missing_key, ungrouped_key, grouped_key_b, pending_key};
    auto results = service_->BatchGetReplicaList(keys, "default");

    ASSERT_EQ(results.size(), keys.size());
    ASSERT_TRUE(results[0].has_value());
    EXPECT_FALSE(results[0]->replicas.empty());
    ASSERT_FALSE(results[1].has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, results[1].error());
    ASSERT_TRUE(results[2].has_value());
    EXPECT_FALSE(results[2]->replicas.empty());
    ASSERT_TRUE(results[3].has_value());
    EXPECT_FALSE(results[3]->replicas.empty());
    ASSERT_FALSE(results[4].has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_NOT_READY, results[4].error());
}

TEST_F(MasterServiceTest,
       ConcurrentGroupedAndUngroupedFirstCreateDoesNotDuplicateMetadata) {
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    const std::string key = "concurrent_grouped_ungrouped_first_create";
    const std::string tenant_id = "tenant_concurrent_first_create";
    ReplicateConfig ungrouped_config;
    ungrouped_config.replica_num = 1;
    ReplicateConfig grouped_config;
    grouped_config.replica_num = 1;
    grouped_config.group_ids =
        std::vector<std::string>{FindGroupIdOnDifferentShard(key)};

    static constexpr size_t kThreadCount = 16;
    std::atomic<size_t> ready{0};
    std::atomic<bool> start{false};
    std::vector<int> put_start_success(kThreadCount, 0);
    std::vector<int> put_end_success(kThreadCount, 0);
    std::vector<std::thread> threads;
    threads.reserve(kThreadCount);

    for (size_t i = 0; i < kThreadCount; ++i) {
        threads.emplace_back([&, i]() {
            ready.fetch_add(1, std::memory_order_acq_rel);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            const auto& config =
                (i % 2 == 0) ? grouped_config : ungrouped_config;
            auto put_start =
                service_->PutStart(client_id, key, tenant_id, 1024, config);
            put_start_success[i] = put_start.has_value() ? 1 : 0;
            if (put_start.has_value()) {
                put_end_success[i] = service_->PutEnd(client_id, key, tenant_id,
                                                      ReplicaType::MEMORY)
                                             .has_value()
                                         ? 1
                                         : 0;
            } else {
                EXPECT_EQ(ErrorCode::OBJECT_ALREADY_EXISTS, put_start.error());
            }
        });
    }

    while (ready.load(std::memory_order_acquire) < kThreadCount) {
        std::this_thread::yield();
    }
    start.store(true, std::memory_order_release);
    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(std::count(put_start_success.begin(), put_start_success.end(), 1),
              1);
    EXPECT_EQ(std::count(put_end_success.begin(), put_end_success.end(), 1), 1);
    EXPECT_EQ(service_->GetKeyCount(), 1u);
    EXPECT_TRUE(service_->GetReplicaList(key, tenant_id).has_value());
}

TEST_F(MasterServiceTest,
       ConcurrentDifferentGroupedFirstCreateDoesNotDuplicateMetadata) {
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    const std::string key = "concurrent_different_grouped_first_create";
    const std::string tenant_id = "tenant_concurrent_grouped_first_create";
    const std::string group_a = FindGroupIdOnDifferentShard(key);
    std::string group_b;
    for (int i = 0; i < 10000; ++i) {
        group_b = key + "_other_group_" + std::to_string(i);
        if (std::hash<std::string>{}(group_b) % 1024 !=
            std::hash<std::string>{}(group_a) % 1024) {
            break;
        }
    }
    ReplicateConfig config_a;
    config_a.replica_num = 1;
    config_a.group_ids = std::vector<std::string>{group_a};
    ReplicateConfig config_b;
    config_b.replica_num = 1;
    config_b.group_ids = std::vector<std::string>{group_b};

    static constexpr size_t kThreadCount = 16;
    std::atomic<size_t> ready{0};
    std::atomic<bool> start{false};
    std::vector<int> put_start_success(kThreadCount, 0);
    std::vector<int> put_end_success(kThreadCount, 0);
    std::vector<std::thread> threads;
    threads.reserve(kThreadCount);

    for (size_t i = 0; i < kThreadCount; ++i) {
        threads.emplace_back([&, i]() {
            ready.fetch_add(1, std::memory_order_acq_rel);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            const auto& config = (i % 2 == 0) ? config_a : config_b;
            auto put_start =
                service_->PutStart(client_id, key, tenant_id, 1024, config);
            put_start_success[i] = put_start.has_value() ? 1 : 0;
            if (put_start.has_value()) {
                put_end_success[i] = service_->PutEnd(client_id, key, tenant_id,
                                                      ReplicaType::MEMORY)
                                             .has_value()
                                         ? 1
                                         : 0;
            } else {
                EXPECT_EQ(ErrorCode::OBJECT_ALREADY_EXISTS, put_start.error());
            }
        });
    }

    while (ready.load(std::memory_order_acquire) < kThreadCount) {
        std::this_thread::yield();
    }
    start.store(true, std::memory_order_release);
    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(std::count(put_start_success.begin(), put_start_success.end(), 1),
              1);
    EXPECT_EQ(std::count(put_end_success.begin(), put_end_success.end(), 1), 1);
    EXPECT_EQ(service_->GetKeyCount(), 1u);
    EXPECT_TRUE(service_->GetReplicaList(key, tenant_id).has_value());
}

TEST_F(MasterServiceTest, ExpiredGroupedPutCanBeReplacedByUngroupedPut) {
    auto service_config = MasterServiceConfig::builder()
                              .set_put_start_discard_timeout_sec(0)
                              .set_put_start_release_timeout_sec(1)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = context.client_id;

    const std::string key = "expired_grouped_put_to_ungrouped";
    ReplicateConfig grouped_config;
    grouped_config.replica_num = 1;
    grouped_config.group_ids =
        std::vector<std::string>{FindGroupIdOnDifferentShard(key)};

    ASSERT_TRUE(
        service_->PutStart(client_id, key, "default", 1024, grouped_config)
            .has_value());
    std::this_thread::sleep_for(std::chrono::milliseconds(2));

    ReplicateConfig ungrouped_config;
    ungrouped_config.replica_num = 1;
    auto put_start =
        service_->PutStart(client_id, key, "default", 1024, ungrouped_config);
    ASSERT_TRUE(put_start.has_value()) << toString(put_start.error());
    ASSERT_TRUE(service_->PutEnd(client_id, key, "default", ReplicaType::MEMORY)
                    .has_value());
    EXPECT_TRUE(service_->ExistKey(key, "default").value_or(false));
}

TEST_F(MasterServiceTest, BatchRemoveUnregistersGroupedRoute) {
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    const std::string key = "batch_remove_grouped_route";
    ReplicateConfig grouped_config;
    grouped_config.replica_num = 1;
    grouped_config.group_ids =
        std::vector<std::string>{FindGroupIdOnDifferentShard(key)};
    PutCompletedObject(*service_, client_id, key, grouped_config);

    auto remove_results = service_->BatchRemove(std::vector<std::string>{key},
                                                "default", /*force=*/true);
    ASSERT_EQ(remove_results.size(), 1u);
    ASSERT_TRUE(remove_results[0].has_value());

    ReplicateConfig ungrouped_config;
    ungrouped_config.replica_num = 1;
    PutCompletedObject(*service_, client_id, key, ungrouped_config);
    EXPECT_TRUE(service_->GetReplicaList(key, "default").has_value());
}

TEST_F(MasterServiceTest, RemoveByRegexUnregistersGroupedRoute) {
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    const std::string key = "regex_remove_grouped_route";
    ReplicateConfig grouped_config;
    grouped_config.replica_num = 1;
    grouped_config.group_ids =
        std::vector<std::string>{FindGroupIdOnDifferentShard(key)};
    PutCompletedObject(*service_, client_id, key, grouped_config);

    auto removed =
        service_->RemoveByRegex("^regex_remove_grouped_route$", "default",
                                /*force=*/true);
    ASSERT_TRUE(removed.has_value());
    EXPECT_EQ(removed.value(), 1);

    ReplicateConfig ungrouped_config;
    ungrouped_config.replica_num = 1;
    PutCompletedObject(*service_, client_id, key, ungrouped_config);
    EXPECT_TRUE(service_->GetReplicaList(key, "default").has_value());
}

TEST_F(MasterServiceTest, GroupedLeaseRefreshNearExpiryProtectsCurrentMembers) {
    auto service_config =
        MasterServiceConfig::builder().set_default_kv_lease_ttl(200).build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    const std::string key_a = "lease_group_key_a";
    const std::string key_b = "lease_group_key_b";
    const std::string group_id = FindGroupIdOnDifferentShard(key_a);

    ReplicateConfig config_a;
    config_a.replica_num = 1;
    config_a.group_ids = std::vector<std::string>{group_id};
    ReplicateConfig config_b = config_a;

    PutCompletedObject(*service_, client_id, key_a, config_a);
    PutCompletedObject(*service_, client_id, key_b, config_b);

    auto exists = service_->ExistKey(key_a, "default");
    ASSERT_TRUE(exists.has_value());
    ASSERT_TRUE(exists.value());

    std::this_thread::sleep_for(std::chrono::milliseconds(120));
    exists = service_->ExistKey(key_a, "default");
    ASSERT_TRUE(exists.has_value());
    ASSERT_TRUE(exists.value());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    auto remove_group_peer = service_->Remove(key_b, "default");
    ASSERT_FALSE(remove_group_peer.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_HAS_LEASE, remove_group_peer.error());

    EXPECT_TRUE(service_->Remove(key_a, "default", /*force=*/true).has_value());
    EXPECT_TRUE(service_->Remove(key_b, "default", /*force=*/true).has_value());
}

TEST_F(MasterServiceTest,
       GroupedLeaseRefreshAfterMembershipChangeDoesNotWaitForTriggerExpiry) {
    auto service_config =
        MasterServiceConfig::builder().set_default_kv_lease_ttl(500).build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    const std::string key_a = "lease_group_dirty_key_a";
    const std::string key_b = "lease_group_dirty_key_b";
    const std::string group_id = FindGroupIdOnDifferentShard(key_a);

    ReplicateConfig config;
    config.replica_num = 1;
    config.group_ids = std::vector<std::string>{group_id};

    PutCompletedObject(*service_, client_id, key_a, config);
    ASSERT_TRUE(service_->ExistKey(key_a, "default").value_or(false));

    PutCompletedObject(*service_, client_id, key_b, config);
    std::this_thread::sleep_for(std::chrono::milliseconds(150));

    auto exists = service_->ExistKey(key_a, "default");
    ASSERT_TRUE(exists.has_value());
    ASSERT_TRUE(exists.value());
    std::this_thread::sleep_for(std::chrono::milliseconds(390));

    auto remove_group_peer = service_->Remove(key_b, "default");
    ASSERT_FALSE(remove_group_peer.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_HAS_LEASE, remove_group_peer.error());

    EXPECT_TRUE(service_->Remove(key_a, "default", /*force=*/true).has_value());
    EXPECT_TRUE(service_->Remove(key_b, "default", /*force=*/true).has_value());
}

TEST_F(MasterServiceTest, RemoveGroupedMemberPreservesOtherMembers) {
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    const std::string key_a = "remove_group_key_a";
    const std::string key_b = "remove_group_key_b";
    const std::string group_id = FindGroupIdOnDifferentShard(key_a);

    ReplicateConfig config;
    config.replica_num = 1;
    config.group_ids = std::vector<std::string>{group_id};
    PutCompletedObject(*service_, client_id, key_a, config);
    PutCompletedObject(*service_, client_id, key_b, config);

    ASSERT_TRUE(service_->Remove(key_a, "default", /*force=*/true).has_value());

    auto removed_exists = service_->ExistKey(key_a, "default");
    ASSERT_TRUE(removed_exists.has_value());
    EXPECT_FALSE(removed_exists.value());
    EXPECT_TRUE(service_->GetReplicaList(key_b, "default").has_value());

    ASSERT_TRUE(service_->Remove(key_b, "default", /*force=*/true).has_value());
    auto group_empty = service_->ExistKey(key_b, "default");
    ASSERT_TRUE(group_empty.has_value());
    EXPECT_FALSE(group_empty.value());
}

TEST_F(MasterServiceTest, UpsertPreservesGroupMembership) {
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    const std::string key = "upsert_group_key";
    const std::string group_id = FindGroupIdOnDifferentShard(key);

    ReplicateConfig grouped_config;
    grouped_config.replica_num = 1;
    grouped_config.group_ids = std::vector<std::string>{group_id};
    PutCompletedObject(*service_, client_id, key, grouped_config);

    ReplicateConfig unset_group_config;
    unset_group_config.replica_num = 1;
    auto preserve_result = service_->UpsertStart(client_id, key, "default",
                                                 1024, unset_group_config);
    ASSERT_TRUE(preserve_result.has_value())
        << "Unset group_ids should preserve existing group membership";
    ASSERT_TRUE(
        service_->UpsertEnd(client_id, key, "default", ReplicaType::MEMORY)
            .has_value());
    EXPECT_TRUE(service_->GetReplicaList(key, "default").has_value());

    ReplicateConfig different_group_config;
    different_group_config.replica_num = 1;
    different_group_config.group_ids =
        std::vector<std::string>{FindGroupIdOnDifferentShard(key + "_other")};
    auto different_group_result = service_->UpsertStart(
        client_id, key, "default", 1024, different_group_config);
    ASSERT_FALSE(different_group_result.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, different_group_result.error());

    ReplicateConfig explicit_ungrouped_config;
    explicit_ungrouped_config.replica_num = 1;
    explicit_ungrouped_config.group_ids = std::vector<std::string>{""};
    auto explicit_ungrouped_result = service_->UpsertStart(
        client_id, key, "default", 1024, explicit_ungrouped_config);
    ASSERT_FALSE(explicit_ungrouped_result.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, explicit_ungrouped_result.error());
}

TEST_F(MasterServiceTest, IncompleteGroupedUpsertCanBecomeUngrouped) {
    std::unique_ptr<MasterService> service_(new MasterService());
    const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = context.client_id;

    const std::string key = "incomplete_grouped_upsert_to_ungrouped";
    ReplicateConfig grouped_config;
    grouped_config.replica_num = 1;
    grouped_config.group_ids =
        std::vector<std::string>{FindGroupIdOnDifferentShard(key)};

    ASSERT_TRUE(
        service_->PutStart(client_id, key, "default", 1024, grouped_config)
            .has_value());
    std::this_thread::sleep_for(std::chrono::milliseconds(2));

    ReplicateConfig ungrouped_config;
    ungrouped_config.replica_num = 1;
    auto upsert_start = service_->UpsertStart(client_id, key, "default", 1024,
                                              ungrouped_config);
    ASSERT_TRUE(upsert_start.has_value()) << toString(upsert_start.error());
    ASSERT_TRUE(
        service_->UpsertEnd(client_id, key, "default", ReplicaType::MEMORY)
            .has_value());
    EXPECT_TRUE(service_->ExistKey(key, "default").value_or(false));
}

TEST_F(MasterServiceTest, UpsertRejectsExistingUngroupedToGrouped) {
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    const std::string key = "upsert_ungrouped_to_grouped";
    ReplicateConfig ungrouped_config;
    ungrouped_config.replica_num = 1;
    PutCompletedObject(*service_, client_id, key, ungrouped_config);

    ReplicateConfig grouped_config;
    grouped_config.replica_num = 1;
    grouped_config.group_ids =
        std::vector<std::string>{FindGroupIdOnDifferentShard(key)};
    auto upsert_start =
        service_->UpsertStart(client_id, key, "default", 2048, grouped_config);
    ASSERT_FALSE(upsert_start.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, upsert_start.error());

    EXPECT_TRUE(service_->GetReplicaList(key, "default").has_value());
}

TEST_F(MasterServiceTest,
       GroupedEvictionExpandsSafeMembersAndSkipsLeasedGroup) {
    auto service_config =
        MasterServiceConfig::builder().set_default_kv_lease_ttl(1000).build();
    constexpr size_t kSegmentSize = 4 * 1024 * 1024;
    constexpr size_t kObjectSize = 2 * 1024 * 1024;

    {
        std::unique_ptr<MasterService> service_(
            new MasterService(service_config));
        [[maybe_unused]] const auto context =
            PrepareSimpleSegment(*service_, "grouped_evict_segment",
                                 kDefaultSegmentBase, kSegmentSize);
        const UUID client_id = generate_uuid();

        const std::string evict_key_a = "grouped_evict_key_a";
        const std::string evict_key_b = "grouped_evict_key_b";
        ReplicateConfig evict_config;
        evict_config.replica_num = 1;
        evict_config.group_ids =
            std::vector<std::string>{FindGroupIdOnDifferentShard(evict_key_a)};
        PutCompletedObject(*service_, client_id, evict_key_a, evict_config,
                           kObjectSize);
        PutCompletedObject(*service_, client_id, evict_key_b, evict_config,
                           kObjectSize);

        ReplicateConfig trigger_config;
        trigger_config.replica_num = 1;
        auto trigger_result =
            service_->PutStart(client_id, "trigger_grouped_eviction", "default",
                               kObjectSize, trigger_config);
        ASSERT_FALSE(trigger_result.has_value());
        EXPECT_EQ(ErrorCode::NO_AVAILABLE_HANDLE, trigger_result.error());

        std::this_thread::sleep_for(std::chrono::milliseconds(200));

        EXPECT_FALSE(service_->ExistKey(evict_key_a, "default").value_or(true));
        EXPECT_FALSE(service_->ExistKey(evict_key_b, "default").value_or(true));
    }

    {
        std::unique_ptr<MasterService> service_(
            new MasterService(service_config));
        [[maybe_unused]] const auto context =
            PrepareSimpleSegment(*service_, "grouped_lease_segment",
                                 kDefaultSegmentBase, kSegmentSize);
        const UUID client_id = generate_uuid();

        const std::string leased_key_a = "grouped_leased_key_a";
        const std::string leased_key_b = "grouped_leased_key_b";
        ReplicateConfig leased_config;
        leased_config.replica_num = 1;
        leased_config.group_ids =
            std::vector<std::string>{FindGroupIdOnDifferentShard(leased_key_a)};
        PutCompletedObject(*service_, client_id, leased_key_a, leased_config,
                           kObjectSize);
        PutCompletedObject(*service_, client_id, leased_key_b, leased_config,
                           kObjectSize);

        auto exists = service_->ExistKey(leased_key_a, "default");
        ASSERT_TRUE(exists.has_value());
        ASSERT_TRUE(exists.value());

        ReplicateConfig trigger_config;
        trigger_config.replica_num = 1;
        auto trigger_result =
            service_->PutStart(client_id, "trigger_leased_group_eviction",
                               "default", kObjectSize, trigger_config);
        ASSERT_FALSE(trigger_result.has_value());
        EXPECT_EQ(ErrorCode::NO_AVAILABLE_HANDLE, trigger_result.error());

        std::this_thread::sleep_for(std::chrono::milliseconds(200));

        EXPECT_TRUE(
            service_->GetReplicaList(leased_key_a, "default").has_value());
        EXPECT_TRUE(
            service_->GetReplicaList(leased_key_b, "default").has_value());
    }
}

TEST_F(MasterServiceTest, GroupedEvictionSkipsUnsafeMembersAndEvictsSafePeers) {
    constexpr size_t kSegmentSize = 4 * 1024 * 1024;
    constexpr size_t kObjectSize = 2 * 1024 * 1024;
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto context =
        PrepareSimpleSegment(*service_, "grouped_mixed_safety_segment",
                             kDefaultSegmentBase, kSegmentSize);
    const UUID client_id = generate_uuid();

    const std::string safe_key = "grouped_mixed_safe_key";
    const std::string hard_pinned_key = "grouped_mixed_hard_pinned_key";
    const std::string group_id = FindGroupIdOnDifferentShard(safe_key);

    ReplicateConfig safe_config;
    safe_config.replica_num = 1;
    safe_config.group_ids = std::vector<std::string>{group_id};
    PutCompletedObject(*service_, client_id, safe_key, safe_config,
                       kObjectSize);

    ReplicateConfig hard_pinned_config = safe_config;
    hard_pinned_config.with_hard_pin = true;
    PutCompletedObject(*service_, client_id, hard_pinned_key,
                       hard_pinned_config, kObjectSize);

    ReplicateConfig trigger_config;
    trigger_config.replica_num = 1;
    auto trigger_result =
        service_->PutStart(client_id, "trigger_mixed_safety_group_eviction",
                           "default", kObjectSize, trigger_config);
    ASSERT_FALSE(trigger_result.has_value());
    EXPECT_EQ(ErrorCode::NO_AVAILABLE_HANDLE, trigger_result.error());

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    EXPECT_FALSE(service_->ExistKey(safe_key, "default").value_or(true));
    EXPECT_TRUE(
        service_->GetReplicaList(hard_pinned_key, "default").has_value());
    EXPECT_TRUE(service_->Remove(hard_pinned_key, "default", /*force=*/true)
                    .has_value());
}

TEST_F(MasterServiceTest, BatchUpsertStartMixedGroupIdsPreservesOrder) {
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    const std::vector<std::string> keys = {
        "batch_grouped_a",
        "batch_ungrouped",
        "batch_grouped_b",
    };
    const std::vector<uint64_t> sizes = {1024, 2048, 4096};

    ReplicateConfig config;
    config.replica_num = 1;
    config.group_ids =
        std::vector<std::string>{FindGroupIdOnDifferentShard(keys[0]), "",
                                 FindGroupIdOnDifferentShard(keys[2])};

    auto results =
        service_->BatchUpsertStart(client_id, keys, "default", sizes, config);
    ASSERT_EQ(results.size(), keys.size());
    for (const auto& result : results) {
        ASSERT_TRUE(result.has_value());
    }

    auto end_results = service_->BatchUpsertEnd(client_id, keys, "default");
    ASSERT_EQ(end_results.size(), keys.size());
    for (const auto& result : end_results) {
        ASSERT_TRUE(result.has_value());
    }

    for (const auto& key : keys) {
        EXPECT_TRUE(service_->GetReplicaList(key, "default").has_value());
    }

    ReplicateConfig invalid_config = config;
    invalid_config.group_ids = std::vector<std::string>{"only_one"};
    auto invalid_results = service_->BatchUpsertStart(
        client_id, keys, "default", sizes, invalid_config);
    ASSERT_EQ(invalid_results.size(), keys.size());
    for (const auto& result : invalid_results) {
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(ErrorCode::INVALID_PARAMS, result.error());
    }
}

TEST_F(MasterServiceTest, WrappedBatchPutStartMixedGroupIdsPreservesOrder) {
    WrappedMasterServiceConfig service_config;
    service_config.default_kv_lease_ttl = 100;
    service_config.enable_metric_reporting = false;
    WrappedMasterService service_(service_config);

    Segment segment = MakeSegment("wrapped_batch_group_segment");
    const UUID client_id = generate_uuid();
    ASSERT_TRUE(service_.MountSegment(segment, client_id).has_value());

    const std::vector<std::string> keys = {
        "wrapped_batch_grouped_a",
        "wrapped_batch_ungrouped",
        "wrapped_batch_grouped_b",
    };
    const std::vector<uint64_t> sizes = {1024, 2048, 4096};

    ReplicateConfig config;
    config.replica_num = 1;
    config.group_ids =
        std::vector<std::string>{FindGroupIdOnDifferentShard(keys[0]), "",
                                 FindGroupIdOnDifferentShard(keys[2])};

    auto results = service_.BatchPutStart(client_id, keys, sizes, config);
    ASSERT_EQ(results.size(), keys.size());
    for (const auto& result : results) {
        ASSERT_TRUE(result.has_value()) << toString(result.error());
    }

    auto end_results = service_.BatchPutEnd(client_id, keys);
    ASSERT_EQ(end_results.size(), keys.size());
    for (const auto& result : end_results) {
        ASSERT_TRUE(result.has_value());
    }

    for (const auto& key : keys) {
        EXPECT_TRUE(service_.GetReplicaList(key, "default").has_value());
    }

    ReplicateConfig invalid_config = config;
    invalid_config.group_ids = std::vector<std::string>{"only_one"};
    auto invalid_group_results =
        service_.BatchPutStart(client_id, keys, sizes, invalid_config);
    ASSERT_EQ(invalid_group_results.size(), keys.size());
    for (const auto& result : invalid_group_results) {
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(ErrorCode::INVALID_PARAMS, result.error());
    }

    auto invalid_size_results =
        service_.BatchPutStart(client_id, keys, {1024}, config);
    ASSERT_EQ(invalid_size_results.size(), keys.size());
    for (const auto& result : invalid_size_results) {
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(ErrorCode::INVALID_PARAMS, result.error());
    }
}

TEST_F(MasterServiceTest, BatchExistKeyGroupedAndIncompletePreservesOrder) {
    std::unique_ptr<MasterService> service_(new MasterService());
    const UUID client_id = generate_uuid();

    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 128;
    [[maybe_unused]] const auto context =
        PrepareSimpleSegment(*service_, "test_segment", buffer, size);

    const std::string grouped_key_a = "batch_grouped_key_a";
    const std::string grouped_key_b = "batch_grouped_key_b";
    const std::string group_id = FindGroupIdOnDifferentShard(grouped_key_a);
    ReplicateConfig grouped_config;
    grouped_config.replica_num = 1;
    grouped_config.group_ids = std::vector<std::string>{group_id};
    PutCompletedObject(*service_, client_id, grouped_key_a, grouped_config);
    PutCompletedObject(*service_, client_id, grouped_key_b, grouped_config);

    const std::string completed_key = "batch_completed_key";
    ReplicateConfig config;
    config.replica_num = 1;
    PutCompletedObject(*service_, client_id, completed_key, config);

    const std::string incomplete_key = "batch_incomplete_key";
    ASSERT_TRUE(
        service_->PutStart(client_id, incomplete_key, "default", 1024, config)
            .has_value());

    const std::string missing_key = "batch_missing_key";
    std::vector<std::string> keys = {grouped_key_a, completed_key,
                                     incomplete_key, missing_key,
                                     grouped_key_b};

    auto resp = service_->BatchExistKey(keys, "default");
    ASSERT_EQ(resp.size(), keys.size());
    ASSERT_TRUE(resp[0].has_value());
    ASSERT_TRUE(resp[1].has_value());
    ASSERT_TRUE(resp[2].has_value());
    ASSERT_TRUE(resp[3].has_value());
    ASSERT_TRUE(resp[4].has_value());
    EXPECT_TRUE(resp[0].value());
    EXPECT_TRUE(resp[1].value());
    EXPECT_FALSE(resp[2].value());
    EXPECT_FALSE(resp[3].value());
    EXPECT_TRUE(resp[4].value());
}

}  // namespace mooncake::test

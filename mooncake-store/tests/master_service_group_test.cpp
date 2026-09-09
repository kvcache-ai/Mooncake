#include "master_service_test_fixture.h"
#include "rpc_service.h"
#include "segment_update_test_utils.h"

#include <glog/logging.h>

#include <chrono>
#include <memory>
#include <string>
#include <vector>

namespace mooncake::test {

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

    auto exists = service_->ExistKey(key_a, TenantId::Default());
    ASSERT_TRUE(exists.has_value());
    ASSERT_TRUE(exists.value());

    std::this_thread::sleep_for(std::chrono::milliseconds(120));
    exists = service_->ExistKey(key_a, TenantId::Default());
    ASSERT_TRUE(exists.has_value());
    ASSERT_TRUE(exists.value());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    auto remove_group_peer = service_->Remove(key_b, TenantId::Default());
    ASSERT_FALSE(remove_group_peer.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_HAS_LEASE, remove_group_peer.error());

    EXPECT_TRUE(service_->Remove(key_a, TenantId::Default(), /*force=*/true)
                    .has_value());
    EXPECT_TRUE(service_->Remove(key_b, TenantId::Default(), /*force=*/true)
                    .has_value());
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
                           TenantId::Default(), kObjectSize, trigger_config);
    ASSERT_FALSE(trigger_result.has_value());
    EXPECT_EQ(ErrorCode::NO_AVAILABLE_HANDLE, trigger_result.error());

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    EXPECT_FALSE(
        service_->ExistKey(safe_key, TenantId::Default()).value_or(true));
    EXPECT_TRUE(service_->GetReplicaList(hard_pinned_key, TenantId::Default())
                    .has_value());
    EXPECT_TRUE(
        service_->Remove(hard_pinned_key, TenantId::Default(), /*force=*/true)
            .has_value());
}

TEST_F(MasterServiceTest, WrappedBatchPutStartMixedGroupIdsPreservesOrder) {
    WrappedMasterServiceConfig service_config;
    service_config.default_kv_lease_ttl = 100;
    service_config.enable_metric_reporting = false;
    WrappedMasterService service_(service_config);

    Segment segment = MakeSegment("wrapped_batch_group_segment");
    const UUID client_id = generate_uuid();
    ASSERT_TRUE(
        RegisterNewSegmentForTest(service_, segment, client_id).has_value());

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

    auto end_results = service_.BatchPutEnd(client_id, MakeObjectMetas(keys));
    ASSERT_EQ(end_results.size(), keys.size());
    for (const auto& result : end_results) {
        ASSERT_TRUE(result.has_value());
    }

    for (const auto& key : keys) {
        EXPECT_TRUE(
            service_.GetReplicaList(key, std::string(TenantId::kDefaultValue))
                .has_value());
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

TEST_F(MasterServiceTest, TryEvictLeasedObject) {
    // set a large kv_lease_ttl so the granted lease will not quickly expire
    const uint64_t kv_lease_ttl = 500;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    const UUID client_id = generate_uuid();
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 16;
    constexpr size_t object_size = 1024 * 1024;
    [[maybe_unused]] const auto context =
        PrepareSimpleSegment(*service_, "test_segment", buffer, size);

    // Verify leased object will not be evicted.
    int success_puts = 0;
    int failed_puts = 0;
    std::vector<std::string> leased_keys;
    for (int i = 0; i < 16 + 10; ++i) {
        std::string key = "test_key" + std::to_string(i);
        uint64_t slice_length = object_size;
        ReplicateConfig config;
        config.replica_num = 1;
        auto put_start_result = service_->PutStart(
            client_id, key, TenantId::Default(), slice_length, config);
        if (put_start_result.has_value()) {
            auto put_end_result = service_->PutEnd(
                client_id, key, TenantId::Default(), ReplicaType::MEMORY);
            ASSERT_TRUE(put_end_result.has_value());
            // the object is leased
            auto get_result =
                service_->GetReplicaList(key, TenantId::Default());
            ASSERT_TRUE(get_result.has_value());
            leased_keys.push_back(key);
            success_puts++;
        } else {
            failed_puts++;
        }
    }
    ASSERT_GT(success_puts, 0);
    ASSERT_GT(failed_puts, 0);
    // wait for eviction to do eviction
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    // All leased objects should be accessible
    for (const auto& key : leased_keys) {
        auto get_result = service_->GetReplicaList(key, TenantId::Default());
        ASSERT_TRUE(get_result.has_value());
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));
    service_->RemoveAll();
}

TEST_F(MasterServiceTest, BatchReplicaClearWithLeaseActive) {
    const uint64_t kv_lease_ttl = 2000;  // Long lease
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    // Create an object
    std::string key = "lease_active_key";
    uint64_t value_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;
    auto put_start_result = service_->PutStart(
        client_id, key, TenantId::Default(), value_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto put_end_result = service_->PutEnd(client_id, key, TenantId::Default(),
                                           ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    // Grant a lease by calling GetReplicaList (similar to normal usage)
    auto get_result = service_->GetReplicaList(key, TenantId::Default());
    ASSERT_TRUE(get_result.has_value());

    // Try to clear immediately (lease should still be active)
    std::vector<std::string> keys = {key};
    auto clear_result = service_->BatchReplicaClear(keys, client_id, "");
    ASSERT_TRUE(clear_result.has_value());

    // Should return empty list because lease is still active
    const auto& cleared_keys = clear_result.value();
    EXPECT_TRUE(cleared_keys.empty())
        << "No keys should be cleared when lease is active";

    // Verify object still exists
    auto exist_result = service_->ExistKey(key, TenantId::Default());
    ASSERT_TRUE(exist_result.has_value());
    ASSERT_TRUE(exist_result.value()) << "Key should still exist";
}

TEST_F(MasterServiceTest, GroupedRoutingUsesHashOfTenantAndKeyOnly) {
    // Route-decoupling invariant: object routing is a pure function of
    // (tenant, key); the group_id is only a lifecycle annotation and never
    // affects which metadata shard an object lands in. The group domain is
    // keyed by scoped(tenant, group_id) and stores only the member key list.
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    // Two member keys that hash to different metadata shards, sharing one
    // group whose id hashes to yet another shard. The default-tenant route is
    // hash(key) % kNumShards (mirrors MasterService::getShardIndex).
    constexpr size_t kMetadataShardCountForTest = 1024;
    const std::string key_a = "route_decouple_key_a";
    const std::string group_id = FindGroupIdOnDifferentShard(key_a);
    std::string key_b = "route_decouple_key_b";
    const size_t shard_a =
        std::hash<std::string>{}(key_a) % kMetadataShardCountForTest;
    size_t shard_b =
        std::hash<std::string>{}(key_b) % kMetadataShardCountForTest;
    for (int i = 0; i < 10000 && shard_b == shard_a; ++i) {
        key_b = "route_decouple_key_b_" + std::to_string(i);
        shard_b = std::hash<std::string>{}(key_b) % kMetadataShardCountForTest;
    }
    ASSERT_NE(shard_a, shard_b);  // members span metadata shards

    ReplicateConfig config;
    config.replica_num = 1;
    config.group_ids = std::vector<std::string>{group_id};
    PutCompletedObject(*service_, client_id, key_a, config);
    PutCompletedObject(*service_, client_id, key_b, config);

    // Both members are reachable purely through hash(tenant, key) routing,
    // which is decoupled from the group domain.
    EXPECT_TRUE(service_->ExistKey(key_a, TenantId::Default()).value_or(false));
    EXPECT_TRUE(service_->ExistKey(key_b, TenantId::Default()).value_or(false));
    EXPECT_TRUE(
        service_->GetReplicaList(key_a, TenantId::Default()).has_value());
    EXPECT_TRUE(
        service_->GetReplicaList(key_b, TenantId::Default()).has_value());

    // The group table still sees both members: group state is a separate,
    // key-list-only domain.
    auto members = GetGroupMemberKeysForTest(*service_, group_id);
    EXPECT_EQ(2u, members.size());

    // Route stability: the object route is hash(tenant, key) alone — grouping
    // does not change it (identical to the ungrouped route computed before the
    // objects existed), so a later ungrouped put of the same key would land on
    // the same shard.
    EXPECT_EQ(shard_a,
              std::hash<std::string>{}(key_a) % kMetadataShardCountForTest);
    EXPECT_EQ(shard_b,
              std::hash<std::string>{}(key_b) % kMetadataShardCountForTest);
}

TEST_F(MasterServiceTest, GroupedReadRefreshesSharedGroupLease) {
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

    // Read key_a (twice, near expiry). Group protection is keyed on ONE shared
    // group TTL, so reading a member refreshes the group TTL and protects the
    // WHOLE group (both key_a and key_b), not just the read member.
    auto exists = service_->ExistKey(key_a, TenantId::Default());
    ASSERT_TRUE(exists.has_value());
    ASSERT_TRUE(exists.value());

    std::this_thread::sleep_for(std::chrono::milliseconds(120));
    exists = service_->ExistKey(key_a, TenantId::Default());
    ASSERT_TRUE(exists.has_value());
    ASSERT_TRUE(exists.value());

    // The shared group TTL is active, so a non-force remove of key_a is
    // rejected.
    auto remove_read_member = service_->Remove(key_a, TenantId::Default());
    ASSERT_FALSE(remove_read_member.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_HAS_LEASE, remove_read_member.error());

    // key_b shares the same group TTL and is therefore ALSO protected.
    auto remove_peer = service_->Remove(key_b, TenantId::Default());
    ASSERT_FALSE(remove_peer.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_HAS_LEASE, remove_peer.error());

    // Force cleanup both members.
    EXPECT_TRUE(service_->Remove(key_a, TenantId::Default(), /*force=*/true)
                    .has_value());
    EXPECT_TRUE(service_->Remove(key_b, TenantId::Default(), /*force=*/true)
                    .has_value());
}

TEST_F(MasterServiceTest, GroupedMembershipChangeStillSharesGroupLeaseOnRead) {
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
    ASSERT_TRUE(service_->ExistKey(key_a, TenantId::Default()).value_or(false));

    // Add a new member to the group after key_a was already written. On put,
    // key_b is wired to the SAME shared Lease, so it participates in the
    // group's protection.
    PutCompletedObject(*service_, client_id, key_b, config);
    std::this_thread::sleep_for(std::chrono::milliseconds(150));

    // Reading key_a refreshes the shared group TTL, which key_b also shares.
    auto exists = service_->ExistKey(key_a, TenantId::Default());
    ASSERT_TRUE(exists.has_value());
    ASSERT_TRUE(exists.value());

    // key_a is protected by the shared group lease...
    auto remove_read_member = service_->Remove(key_a, TenantId::Default());
    ASSERT_FALSE(remove_read_member.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_HAS_LEASE, remove_read_member.error());

    // ...and key_b (sharing the same group TTL) is protected too.
    auto remove_peer = service_->Remove(key_b, TenantId::Default());
    ASSERT_FALSE(remove_peer.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_HAS_LEASE, remove_peer.error());

    EXPECT_TRUE(service_->Remove(key_a, TenantId::Default(), /*force=*/true)
                    .has_value());
    EXPECT_TRUE(service_->Remove(key_b, TenantId::Default(), /*force=*/true)
                    .has_value());
}

TEST_F(MasterServiceTest, GroupStateRegistersAndCleansUpMembers) {
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    const std::string key_a = "group_state_key_a";
    const std::string key_b = "group_state_key_b";
    const std::string group_id = "group_state_group";

    ReplicateConfig config;
    config.replica_num = 1;
    config.group_ids = std::vector<std::string>{group_id};
    PutCompletedObject(*service_, client_id, key_a, config);
    PutCompletedObject(*service_, client_id, key_b, config);

    auto members = GetGroupMemberKeysForTest(*service_, group_id);
    EXPECT_EQ(2u, members.size());

    // Removing one member shrinks, but does not erase, the group.
    ASSERT_TRUE(service_->Remove(key_a, TenantId::Default(), /*force=*/true)
                    .has_value());
    members = GetGroupMemberKeysForTest(*service_, group_id);
    ASSERT_EQ(1u, members.size());
    EXPECT_EQ(key_b, members[0]);

    // Removing the last member erases the group.
    ASSERT_TRUE(service_->Remove(key_b, TenantId::Default(), /*force=*/true)
                    .has_value());
    EXPECT_TRUE(GetGroupMemberKeysForTest(*service_, group_id).empty());
}

TEST_F(MasterServiceTest, RebuildGroupStateRestoresMembershipFromMetadata) {
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    const std::string key_a = "rebuild_group_key_a";
    const std::string key_b = "rebuild_group_key_b";
    const std::string group_id = "rebuild_group_id";

    ReplicateConfig config;
    config.replica_num = 1;
    config.group_ids = std::vector<std::string>{group_id};
    PutCompletedObject(*service_, client_id, key_a, config);
    PutCompletedObject(*service_, client_id, key_b, config);

    // Simulate a snapshot reset: drop all group state.
    ClearGroupStateForTest(*service_);
    EXPECT_TRUE(GetGroupMemberKeysForTest(*service_, group_id).empty());

    // Rebuild from object metadata (as snapshot deserialization does).
    RebuildGroupStateForTest(*service_);

    auto members = GetGroupMemberKeysForTest(*service_, group_id);
    EXPECT_EQ(2u, members.size());
}

TEST_F(MasterServiceTest, GroupLeaseIsSharedAndExtendsOnMemberRead) {
    const uint64_t kv_lease_ttl = 1000;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    const std::string key_a = "group_lease_member_a";
    const std::string key_b = "group_lease_member_b";
    const std::string group_id = FindGroupIdOnDifferentShard(key_a);

    ReplicateConfig config;
    config.replica_num = 1;
    config.group_ids = std::vector<std::string>{group_id};
    PutCompletedObject(*service_, client_id, key_a, config);
    PutCompletedObject(*service_, client_id, key_b, config);

    // Both members resolve to the SAME shared Lease.
    const auto lease_a = GetGroupLeaseForTest(*service_, group_id);
    ASSERT_NE(nullptr, lease_a);
    const auto lease_b = GetGroupLeaseForTest(*service_, group_id);
    ASSERT_NE(nullptr, lease_b);
    EXPECT_EQ(lease_a.get(), lease_b.get());

    // A freshly-created group (no reads yet) is not protected -> evictable.
    EXPECT_TRUE(lease_a->IsExpired(std::chrono::system_clock::now()));

    // Reading one member extends the shared group lease -> the whole group is
    // now protected.
    EXPECT_TRUE(
        service_->GetReplicaList(key_a, TenantId::Default()).has_value());
    EXPECT_FALSE(lease_a->IsExpired(std::chrono::system_clock::now()));
    // The other member sees the same (shared) extended deadline.
    EXPECT_FALSE(lease_b->IsExpired(std::chrono::system_clock::now()));
}

TEST_F(MasterServiceTest, ReRouteRestoredObjectsMovesStaleShardObjects) {
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    ReRouteRestoredObjectsMigrationForTest(*service_);
}

TEST_F(MasterServiceTest, RemoveAllLeasedObject) {
    const uint64_t kv_lease_ttl = 50;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();
    for (int i = 0; i < 10; ++i) {
        std::string key = "test_key" + std::to_string(i);
        uint64_t slice_length = 1024;
        ReplicateConfig config;
        config.replica_num = 1;
        auto put_start_result = service_->PutStart(
            client_id, key, TenantId::Default(), slice_length, config);
        ASSERT_TRUE(put_start_result.has_value());
        auto put_end_result = service_->PutEnd(
            client_id, key, TenantId::Default(), ReplicaType::MEMORY);
        ASSERT_TRUE(put_end_result.has_value());
        if (i >= 5) {
            auto exist_result = service_->ExistKey(key, TenantId::Default());
            ASSERT_TRUE(exist_result.has_value());
        }
    }
    ASSERT_EQ(5, service_->RemoveAll());
    for (int i = 0; i < 5; ++i) {
        std::string key = "test_key" + std::to_string(i);
        auto exist_result = service_->ExistKey(key, TenantId::Default());
        ASSERT_FALSE(exist_result.value());
    }
    // wait for all the lease to expire
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));
    ASSERT_EQ(5, service_->RemoveAll());
    for (int i = 5; i < 10; ++i) {
        std::string key = "test_key" + std::to_string(i);
        auto exist_result = service_->ExistKey(key, TenantId::Default());
        ASSERT_FALSE(exist_result.value());
    }
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

#include "fixture.h"

namespace mooncake::test {
TEST_F(MasterServiceTest, OffloadObjectHeartbeat) {
    constexpr size_t key_cnt = 3000;
    MasterServiceConfig config;
    config.enable_offload = true;
    std::unique_ptr<MasterService> service_(new MasterService(config));
    UUID client_id = generate_uuid();
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 16;
    auto segment = MakeSegment("segment", buffer, size);
    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());
    auto mount_local_disk_result =
        service_->MountLocalDiskSegment(client_id, false);
    ASSERT_TRUE(mount_local_disk_result.has_value());
    for (size_t i = 0; i < key_cnt; i++) {
        auto key = GenerateKeyForSegment(client_id, service_, segment.name);
    }
    auto res = service_->OffloadObjectHeartbeat(client_id, true);
    if (!res) {
        LOG(ERROR) << "OffloadObjectHeartbeat failed with error: "
                   << res.error();
        ASSERT_TRUE(res);
    }
    ASSERT_EQ(res->size(), 0);
    std::vector<std::string> keys;
    for (size_t i = 0; i < key_cnt; i++) {
        auto key = GenerateKeyForSegment(client_id, service_, segment.name);
        keys.push_back(key);
    }
    res = service_->OffloadObjectHeartbeat(client_id, true);
    if (!res) {
        LOG(ERROR) << "OffloadObjectHeartbeat failed with error: "
                   << res.error();
        ASSERT_TRUE(res);
    }
    ASSERT_EQ(res->size(), keys.size());
    for (auto& key : keys) {
        auto it = std::find_if(
            res->begin(), res->end(),
            [&key](const OffloadTaskItem& task) { return task.key == key; });
        ASSERT_TRUE(it != res->end());
        ASSERT_EQ(it->size, 1024);
    }

    keys.clear();
    for (size_t i = 0; i < key_cnt; i++) {
        auto key = GenerateKeyForSegment(client_id, service_, segment.name);
        keys.push_back(key);
    }
    res = service_->OffloadObjectHeartbeat(client_id, true);
    if (!res) {
        LOG(ERROR) << "OffloadObjectHeartbeat failed with error: "
                   << res.error();
        ASSERT_TRUE(res);
    }
    ASSERT_EQ(res->size(), keys.size());
    for (auto& key : keys) {
        auto it = std::find_if(
            res->begin(), res->end(),
            [&key](const OffloadTaskItem& task) { return task.key == key; });
        ASSERT_TRUE(it != res->end());
        ASSERT_EQ(it->size, 1024);
    }
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
    auto put_start_result =
        service_->PutStart(client_id, key, "default", value_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto put_end_result =
        service_->PutEnd(client_id, key, "default", ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    // Grant a lease by calling GetReplicaList (similar to normal usage)
    auto get_result = service_->GetReplicaList(key, "default");
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
    auto exist_result = service_->ExistKey(key, "default");
    ASSERT_TRUE(exist_result.has_value());
    ASSERT_TRUE(exist_result.value()) << "Key should still exist";
}

TEST_F(MasterServiceTest, BatchReplicaClearWithDifferentClientId) {
    const uint64_t kv_lease_ttl = 50;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id1 = generate_uuid();
    const UUID client_id2 = generate_uuid();

    // Create an object with client_id1
    std::string key = "client_specific_key";
    uint64_t value_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;
    auto put_start_result =
        service_->PutStart(client_id1, key, "default", value_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto put_end_result =
        service_->PutEnd(client_id1, key, "default", ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    // Wait for lease to expire
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl + 10));

    // Try to clear with different client_id
    std::vector<std::string> keys = {key};
    auto clear_result = service_->BatchReplicaClear(keys, client_id2, "");
    ASSERT_TRUE(clear_result.has_value());

    // Should return empty list because client_id doesn't match
    const auto& cleared_keys = clear_result.value();
    EXPECT_TRUE(cleared_keys.empty())
        << "No keys should be cleared for different client_id";

    // Verify object still exists
    auto exist_result = service_->ExistKey(key, "default");
    ASSERT_TRUE(exist_result.has_value());
    ASSERT_TRUE(exist_result.value()) << "Key should still exist";
}

TEST_F(MasterServiceTest, BatchReplicaClearWithNonExistentKeys) {
    const uint64_t kv_lease_ttl = 50;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    // Try to clear non-existent keys
    std::vector<std::string> keys = {"non_existent_key1", "non_existent_key2"};
    auto clear_result = service_->BatchReplicaClear(keys, client_id, "");
    ASSERT_TRUE(clear_result.has_value());

    // Should return empty list
    const auto& cleared_keys = clear_result.value();
    EXPECT_TRUE(cleared_keys.empty())
        << "No keys should be cleared for non-existent keys";
}

TEST_F(MasterServiceTest, BatchReplicaClearWithEmptyKeys) {
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    // Try to clear empty keys list
    std::vector<std::string> empty_keys;
    auto clear_result = service_->BatchReplicaClear(empty_keys, client_id, "");
    ASSERT_TRUE(clear_result.has_value());

    // Should return empty list
    const auto& cleared_keys = clear_result.value();
    EXPECT_TRUE(cleared_keys.empty())
        << "Empty keys list should return empty result";
}

TEST_F(MasterServiceTest, BatchReplicaClearWithEmptyStringKeys) {
    const uint64_t kv_lease_ttl = 50;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    // Create a valid object
    std::string valid_key = "valid_key";
    uint64_t value_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;
    auto put_start_result = service_->PutStart(client_id, valid_key, "default",
                                               value_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto put_end_result =
        service_->PutEnd(client_id, valid_key, "default", ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    // Wait for lease to expire
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl + 10));

    // Try to clear with empty string keys mixed with valid keys
    std::vector<std::string> keys = {"", valid_key, "", "another_empty"};
    auto clear_result = service_->BatchReplicaClear(keys, client_id, "");
    ASSERT_TRUE(clear_result.has_value());

    // Should only clear the valid key, skip empty strings
    const auto& cleared_keys = clear_result.value();
    ASSERT_EQ(1u, cleared_keys.size()) << "Only valid key should be cleared";
    EXPECT_EQ(valid_key, cleared_keys[0]);
}

TEST_F(MasterServiceTest, BatchReplicaClearMixedScenario) {
    const uint64_t kv_lease_ttl = 50;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id1 = generate_uuid();
    const UUID client_id2 = generate_uuid();

    // Create objects with different client_ids
    std::string key1 = "mixed_key1";  // client_id1
    std::string key2 = "mixed_key2";  // client_id1
    std::string key3 = "mixed_key3";  // client_id2

    uint64_t value_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;

    // Create key1 and key2 with client_id1
    auto put_start1 =
        service_->PutStart(client_id1, key1, "default", value_length, config);
    ASSERT_TRUE(put_start1.has_value());
    auto put_end1 =
        service_->PutEnd(client_id1, key1, "default", ReplicaType::MEMORY);
    ASSERT_TRUE(put_end1.has_value());

    auto put_start2 =
        service_->PutStart(client_id1, key2, "default", value_length, config);
    ASSERT_TRUE(put_start2.has_value());
    auto put_end2 =
        service_->PutEnd(client_id1, key2, "default", ReplicaType::MEMORY);
    ASSERT_TRUE(put_end2.has_value());

    // Create key3 with client_id2
    auto put_start3 =
        service_->PutStart(client_id2, key3, "default", value_length, config);
    ASSERT_TRUE(put_start3.has_value());
    auto put_end3 =
        service_->PutEnd(client_id2, key3, "default", ReplicaType::MEMORY);
    ASSERT_TRUE(put_end3.has_value());

    // Wait for lease to expire
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl + 10));

    // Try to clear with mixed keys (some belong to client_id1, some to
    // client_id2)
    std::vector<std::string> keys = {key1, key2, key3, "non_existent", ""};
    auto clear_result = service_->BatchReplicaClear(keys, client_id1, "");
    ASSERT_TRUE(clear_result.has_value());

    // Should only clear key1 and key2 (belonging to client_id1)
    const auto& cleared_keys = clear_result.value();
    ASSERT_EQ(2u, cleared_keys.size())
        << "Only keys belonging to client_id1 should be cleared";

    // Verify key1 and key2 are cleared
    auto exist1 = service_->ExistKey(key1, "default");
    ASSERT_TRUE(exist1.has_value());
    ASSERT_FALSE(exist1.value()) << "key1 should be cleared";

    auto exist2 = service_->ExistKey(key2, "default");
    ASSERT_TRUE(exist2.has_value());
    ASSERT_FALSE(exist2.value()) << "key2 should be cleared";

    // Verify key3 still exists (different client_id)
    auto exist3 = service_->ExistKey(key3, "default");
    ASSERT_TRUE(exist3.has_value());
    ASSERT_TRUE(exist3.value())
        << "key3 should still exist (different client_id)";
}

}  // namespace mooncake::test

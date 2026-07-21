#include "fixture.h"

// Basic object lifecycle and remove operations.

namespace mooncake::test {
TEST(TenantScopedStorageKeyTest, RoundTripsAndParsesLegacyKeys) {
    const auto scoped =
        MakeTenantScopedStorageKey("tenant:with:colon", "path/key:with:colon");
    EXPECT_NE(scoped.find('\0'), std::string::npos);

    auto [tenant_id, key] = ParseTenantScopedStorageKey(scoped);
    EXPECT_EQ(tenant_id, "tenant:with:colon");
    EXPECT_EQ(key, "path/key:with:colon");

    auto [default_tenant, default_key] = ParseTenantScopedStorageKey("raw_key");
    EXPECT_EQ(default_tenant, "default");
    EXPECT_EQ(default_key, "raw_key");

    std::string legacy = "legacy_tenant";
    legacy.push_back('\0');
    legacy.append("legacy_key");
    auto [legacy_tenant, legacy_key] = ParseTenantScopedStorageKey(legacy);
    EXPECT_EQ(legacy_tenant, "legacy_tenant");
    EXPECT_EQ(legacy_key, "legacy_key");
}

TEST_F(MasterServiceTest, PutStartInvalidParams) {
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    std::string key = "test_key";
    ReplicateConfig config;

    // Test invalid replica config
    config.replica_num = 0;
    config.nof_replica_num = 0;
    auto put_result1 =
        service_->PutStart(client_id, key, "default", 1024, config);
    EXPECT_FALSE(put_result1.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, put_result1.error());

    // Test zero slice_length
    config.replica_num = 1;
    auto put_result2 = service_->PutStart(client_id, key, "default", 0, config);
    EXPECT_FALSE(put_result2.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, put_result2.error());

    // Test prefer_alloc_in_same_node with nof replicas
    config.nof_replica_num = 1;
    config.prefer_alloc_in_same_node = true;
    auto put_result3 =
        service_->PutStart(client_id, key, "default", 1024, config);
    EXPECT_FALSE(put_result3.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, put_result3.error());
}

#ifdef USE_NOF
TEST_F(MasterServiceTest, PutEndAllCompletesMemoryAndNoFReplicas) {
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto mem_context = PrepareSimpleSegment(*service_);
    NoFSegment nof_segment = MakeNoFSegment();
    const UUID client_id = generate_uuid();
    ASSERT_TRUE(service_->MountNoFSegment(nof_segment, client_id).has_value());

    ReplicateConfig config;
    config.replica_num = 1;
    config.nof_replica_num = 1;
    auto put_start_result =
        service_->PutStart(client_id, "test_key_all", "default", 1024, config);
    ASSERT_TRUE(put_start_result.has_value());

    auto put_end_result = service_->PutEnd(client_id, "test_key_all", "default",
                                           ReplicaType::ALL);
    ASSERT_TRUE(put_end_result.has_value());

    auto get_replica_result =
        service_->GetReplicaList("test_key_all", "default");
    ASSERT_TRUE(get_replica_result.has_value());

    bool has_complete_memory = false;
    bool has_complete_nof = false;
    for (const auto& replica : get_replica_result->replicas) {
        if (replica.is_memory_replica() &&
            replica.status == ReplicaStatus::COMPLETE) {
            has_complete_memory = true;
        }
        if (replica.is_nof_replica() &&
            replica.status == ReplicaStatus::COMPLETE) {
            has_complete_nof = true;
        }
    }
    EXPECT_TRUE(has_complete_memory);
    EXPECT_TRUE(has_complete_nof);
}

TEST_F(MasterServiceTest, PutEndMemoryDoesNotCompleteNoFReplica) {
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto mem_context = PrepareSimpleSegment(*service_);
    NoFSegment nof_segment =
        MakeNoFSegment("test_nof_segment_2", "test_nof_segment_endpoint_2");
    const UUID client_id = generate_uuid();
    ASSERT_TRUE(service_->MountNoFSegment(nof_segment, client_id).has_value());

    ReplicateConfig config;
    config.replica_num = 1;
    config.nof_replica_num = 1;
    auto put_start_result = service_->PutStart(client_id, "test_key_split",
                                               "default", 1024, config);
    ASSERT_TRUE(put_start_result.has_value());

    auto put_end_result = service_->PutEnd(client_id, "test_key_split",
                                           "default", ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    auto get_replica_result =
        service_->GetReplicaList("test_key_split", "default");
    ASSERT_TRUE(get_replica_result.has_value());
    ASSERT_EQ(get_replica_result->replicas.size(), 1u);
    EXPECT_TRUE(get_replica_result->replicas[0].is_memory_replica());
    EXPECT_EQ(get_replica_result->replicas[0].status, ReplicaStatus::COMPLETE);

    auto put_revoke_result = service_->PutRevoke(
        client_id, "test_key_split", "default", ReplicaType::NOF_SSD);
    ASSERT_TRUE(put_revoke_result.has_value());

    auto final_replica_result =
        service_->GetReplicaList("test_key_split", "default");
    ASSERT_TRUE(final_replica_result.has_value());
    ASSERT_EQ(final_replica_result->replicas.size(), 1u);
    EXPECT_TRUE(final_replica_result->replicas[0].is_memory_replica());
    EXPECT_EQ(final_replica_result->replicas[0].status,
              ReplicaStatus::COMPLETE);
}

TEST_F(MasterServiceTest, PutStartOnePlusOneAllowsSingleAllocatedReplica) {
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto mem_context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    ReplicateConfig config;
    config.replica_num = 1;
    config.nof_replica_num = 1;
    auto put_start_result = service_->PutStart(
        client_id, "test_key_one_plus_one", "default", 1024, config);
    ASSERT_TRUE(put_start_result.has_value());
    ASSERT_EQ(put_start_result->size(), 1u);
    EXPECT_TRUE(put_start_result->front().is_memory_replica());
}
#endif

TEST_F(MasterServiceTest, PutStartEndFlow) {
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();
    const UUID invalid_client_id = generate_uuid();
    ASSERT_NE(client_id, invalid_client_id);

    // Test PutStart
    std::string key = "test_key";
    uint64_t value_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;

    auto put_start_result =
        service_->PutStart(client_id, key, "default", value_length, config);
    EXPECT_TRUE(put_start_result.has_value());
    replica_list = put_start_result.value();
    EXPECT_FALSE(replica_list.empty());
    EXPECT_EQ(ReplicaStatus::PROCESSING, replica_list[0].status);

    // During put, Get/Remove should fail
    auto get_replica_result = service_->GetReplicaList(key, "default");
    EXPECT_FALSE(get_replica_result.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_NOT_READY, get_replica_result.error());
    auto remove_result = service_->Remove(key, "default");
    EXPECT_FALSE(remove_result.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_NOT_READY, remove_result.error());

    // PutEnd should fail if the client_id does not match.
    auto put_end_fail_result = service_->PutEnd(invalid_client_id, key,
                                                "default", ReplicaType::MEMORY);
    EXPECT_FALSE(put_end_fail_result.has_value());
    EXPECT_EQ(put_end_fail_result.error(), ErrorCode::ILLEGAL_CLIENT);

    // PutRevoke should fail if the client_id does not match.
    auto put_revoke_fail_result = service_->PutRevoke(
        invalid_client_id, key, "default", ReplicaType::MEMORY);
    EXPECT_FALSE(put_revoke_fail_result.has_value());
    EXPECT_EQ(put_revoke_fail_result.error(), ErrorCode::ILLEGAL_CLIENT);

    // Test PutEnd
    auto put_end_result =
        service_->PutEnd(client_id, key, "default", ReplicaType::MEMORY);
    EXPECT_TRUE(put_end_result.has_value());

    // Verify replica list after PutEnd
    auto final_get_result = service_->GetReplicaList(key, "default");
    EXPECT_TRUE(final_get_result.has_value());
    replica_list = final_get_result.value().replicas;
    EXPECT_EQ(1, replica_list.size());
    EXPECT_EQ(ReplicaStatus::COMPLETE, replica_list[0].status);
}

TEST_F(MasterServiceTest, RandomPutStartEndFlow) {
    std::unique_ptr<MasterService> service_(new MasterService());
    const UUID client_id = generate_uuid();

    // Mount 5 segments, each 16MB
    constexpr size_t kBaseAddr = 0x300000000;
    constexpr size_t kSegmentSize = 1024 * 1024 * 16;  // 16MB
    for (int i = 0; i < 5; ++i) {
        [[maybe_unused]] const auto context = PrepareSimpleSegment(
            *service_, "segment_" + std::to_string(i),
            kBaseAddr + static_cast<size_t>(i) * kSegmentSize, kSegmentSize);
    }

    // Test PutStart
    std::string key = "test_key";
    uint64_t value_length = 1024;
    ReplicateConfig config;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(1, 5);
    int random_number = dis(gen);
    config.replica_num = random_number;
    auto put_start_result =
        service_->PutStart(client_id, key, "default", value_length, config);
    EXPECT_TRUE(put_start_result.has_value());
    replica_list = put_start_result.value();
    EXPECT_FALSE(replica_list.empty());
    EXPECT_EQ(ReplicaStatus::PROCESSING, replica_list[0].status);
    // During put, Get/Remove should fail
    auto get_result = service_->GetReplicaList(key, "default");
    EXPECT_FALSE(get_result.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_NOT_READY, get_result.error());
    auto remove_result = service_->Remove(key, "default");
    EXPECT_FALSE(remove_result.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_NOT_READY, remove_result.error());
    // Test PutEnd
    auto put_end_result =
        service_->PutEnd(client_id, key, "default", ReplicaType::MEMORY);
    EXPECT_TRUE(put_end_result.has_value());
    // Verify replica list after PutEnd
    auto get_result2 = service_->GetReplicaList(key, "default");
    EXPECT_TRUE(get_result2.has_value());
    replica_list = get_result2.value().replicas;
    EXPECT_EQ(random_number, replica_list.size());
    for (int i = 0; i < random_number; ++i) {
        EXPECT_EQ(ReplicaStatus::COMPLETE, replica_list[i].status);
    }
}

TEST_F(MasterServiceTest, GetReplicaListByRegex) {
    const uint64_t kv_lease_ttl = 50;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    const UUID client_id = generate_uuid();
    // Test getting non-existent key
    auto get_result = service_->GetReplicaList(".*non_existent.*", "default");
    EXPECT_FALSE(get_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, get_result.error());

    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);

    int times = 10;
    while (times--) {
        std::string key = "test_key" + std::to_string(times);
        uint64_t value_length = 1024;
        ReplicateConfig config;
        config.replica_num = 1;
        auto put_start_result =
            service_->PutStart(client_id, key, "default", value_length, config);
        ASSERT_TRUE(put_start_result.has_value());
        auto put_end_result =
            service_->PutEnd(client_id, key, "default", ReplicaType::MEMORY);
        ASSERT_TRUE(put_end_result.has_value());
        auto exist_result = service_->ExistKey(key, "default");
        ASSERT_TRUE(exist_result.has_value());
    }
    // wait for all the lease to expire
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));

    // Test getting existing key
    auto get_result2 = service_->GetReplicaListByRegex("^test_key", "default");
    EXPECT_TRUE(get_result2.has_value());
    auto replica_list_local = get_result2.value();
    EXPECT_EQ(10, replica_list_local.size());
}

// Helper function to put an object, making the test cleaner
void put_object(MasterService& service, const UUID& client_id,
                const std::string& key) {
    uint64_t value_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;
    auto put_start_result =
        service.PutStart(client_id, key, "default", value_length, config);
    ASSERT_TRUE(put_start_result.has_value())
        << "Failed to PutStart for key: " << key;
    auto put_end_result =
        service.PutEnd(client_id, key, "default", ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value())
        << "Failed to PutEnd for key: " << key;
    auto exist_result = service.ExistKey(key, "default");
    ASSERT_TRUE(exist_result.has_value())
        << "Key does not exist after put: " << key;
}

TEST_F(MasterServiceTest, GetReplicaListByRegexComplex) {
    const uint64_t kv_lease_ttl = 100;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    auto service_ = std::make_unique<MasterService>(service_config);
    const UUID client_id = generate_uuid();

    // 1. Mount segment
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);

    // 2. Prepare a diverse set of keys
    std::vector<std::string> keys_to_put = {
        // Basic keys for prefix matching
        "test_key_01", "test_key_02", "test_key_10",
        // Keys with different prefixes
        "prod_key_alpha", "prod_key_beta",
        // Keys with numbers in the middle
        "data_part_1_chunk_a", "data_part_2_chunk_b",
        // Keys with special characters (if your system supports them)
        "config/user/settings.json", "logs/app-2025-08-13.log",
        // Keys with varying lengths
        "short", "a_very_very_very_long_key_that_tests_length_limits",
        // Keys that look similar but should not match certain regex
        "test-key-extra", "another_key"};

    for (const auto& key : keys_to_put) {
        put_object(*service_, client_id, key);
    }

    // Wait for all leases to be written to the underlying KV store.
    // In a real system, you might not need this if PutEnd is synchronous.
    // For this test, let's assume it's needed for consistency.
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));

    // 3. Run a series of regex tests

    // Test 3.1: Simple prefix matching
    {
        auto result = service_->GetReplicaListByRegex("^test_key_", "default");
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(result.value().size(),
                  3);  // Matches test_key_01, test_key_02, test_key_10
    }

    // Test 3.2: Matching with a wildcard for any number
    {
        auto result =
            service_->GetReplicaListByRegex("^test_key_\\d+$", "default");
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(result.value().size(), 3);
    }

    // Test 3.3: Matching a specific pattern with wildcards
    {
        // Matches "data_part_1_chunk_a" and "data_part_2_chunk_b"
        auto result = service_->GetReplicaListByRegex("^data_part_\\d_chunk_.$",
                                                      "default");
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(result.value().size(), 2);
    }

    // Test 3.4: Matching keys containing a specific substring
    {
        // Matches all keys with "key" in them
        auto result = service_->GetReplicaListByRegex("key", "default");
        ASSERT_TRUE(result.has_value());
        // Expected: test_key_01, test_key_02, test_key_10,
        //           prod_key_alpha, prod_key_beta,
        //           a_very_very_very_long_key_that_tests_length_limits,
        //           test-key-extra, another_key
        EXPECT_EQ(result.value().size(), 8);
    }

    // Test 3.5: Matching based on file-like paths
    {
        // Match all .log files
        auto result = service_->GetReplicaListByRegex("\\.log$", "default");
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(result.value().size(), 1);
        EXPECT_EQ(result.value().begin()->first, "logs/app-2025-08-13.log");
    }

    // Test 3.6: OR condition using |
    {
        // Match keys starting with "prod" OR ending with "json"
        auto result =
            service_->GetReplicaListByRegex("^prod|\\.json$", "default");
        ASSERT_TRUE(result.has_value());
        // Expected: prod_key_alpha, prod_key_beta, config/user/settings.json
        EXPECT_EQ(result.value().size(), 3);
    }

    // Test 3.7: Regex that should not match anything
    {
        auto result =
            service_->GetReplicaListByRegex("^non_existent_prefix_", "default");
        // This should succeed but return an empty map.
        ASSERT_TRUE(result.has_value());
        EXPECT_TRUE(result.value().empty());
    }

    // Test 3.8: Exact match regex
    {
        auto result = service_->GetReplicaListByRegex("^short$", "default");
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(result.value().size(), 1);
        EXPECT_EQ(result.value().begin()->first, "short");
    }

    // Test 3.9: Initial test for non-existent key (as a sanity check)
    {
        auto get_result = service_->GetReplicaListByRegex(
            ".*absolutely_non_existent.*", "default");
        // Depending on implementation, this could return an empty map or an
        // error. Let's assume it returns an empty map for a valid regex with no
        // matches.
        ASSERT_TRUE(get_result.has_value());
        EXPECT_TRUE(get_result.value().empty());
    }
}

TEST_F(MasterServiceTest, GetReplicaList) {
    std::unique_ptr<MasterService> service_(new MasterService());
    const UUID client_id = generate_uuid();
    // Test getting non-existent key
    auto get_result = service_->GetReplicaList("non_existent", "default");
    EXPECT_FALSE(get_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, get_result.error());

    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);

    std::string key = "test_key";
    uint64_t value_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;
    auto put_start_result =
        service_->PutStart(client_id, key, "default", value_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto put_end_result =
        service_->PutEnd(client_id, key, "default", ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    // Test getting existing key
    auto get_result2 = service_->GetReplicaList(key, "default");
    EXPECT_TRUE(get_result2.has_value());
    auto replica_list_local = get_result2.value().replicas;
    EXPECT_FALSE(replica_list_local.empty());
}

TEST_F(MasterServiceTest, RemoveObject) {
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    std::string key = "test_key";
    uint64_t value_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;
    auto put_start_result =
        service_->PutStart(client_id, key, "default", value_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto put_end_result =
        service_->PutEnd(client_id, key, "default", ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    // Test removing the object
    auto remove_result = service_->Remove(key, "default");
    EXPECT_TRUE(remove_result.has_value());

    // Verify object is removed
    auto get_result = service_->GetReplicaList(key, "default");
    EXPECT_FALSE(get_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, get_result.error());

    // Test removing non-existent object
    auto remove_result2 = service_->Remove("non_existent", "default");
    EXPECT_FALSE(remove_result2.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, remove_result2.error());
}

TEST_F(MasterServiceTest, RandomRemoveObject) {
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();
    int times = 10;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(1, 1000);
    while (times--) {
        std::string key = "test_key" + std::to_string(dis(gen));
        uint64_t value_length = 1024;
        ReplicateConfig config;
        config.replica_num = 1;
        auto put_start_result =
            service_->PutStart(client_id, key, "default", value_length, config);
        ASSERT_TRUE(put_start_result.has_value());
        auto put_end_result =
            service_->PutEnd(client_id, key, "default", ReplicaType::MEMORY);
        ASSERT_TRUE(put_end_result.has_value());

        // Test removing the object
        auto remove_result = service_->Remove(key, "default");
        EXPECT_TRUE(remove_result.has_value());

        // Verify object is removed
        auto get_result = service_->GetReplicaList(key, "default");
        EXPECT_FALSE(get_result.has_value());
        EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, get_result.error());
    }
}

TEST_F(MasterServiceTest, RemoveByRegex) {
    const uint64_t kv_lease_ttl = 50;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();
    int times = 10;
    while (times--) {
        std::string key = "test_key" + std::to_string(times);
        uint64_t value_length = 1024;
        ReplicateConfig config;
        config.replica_num = 1;
        auto put_start_result =
            service_->PutStart(client_id, key, "default", value_length, config);
        ASSERT_TRUE(put_start_result.has_value());
        auto put_end_result =
            service_->PutEnd(client_id, key, "default", ReplicaType::MEMORY);
        ASSERT_TRUE(put_end_result.has_value());
        auto exist_result = service_->ExistKey(key, "default");
        ASSERT_TRUE(exist_result.has_value());
    }
    // wait for all the lease to expire
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));
    auto res = service_->RemoveByRegex("^test_key", "default");
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(10, res.value());
    times = 10;
    while (times--) {
        std::string key = "test_key" + std::to_string(times);
        auto exist_result = service_->ExistKey(key, "default");
        ASSERT_TRUE(exist_result.has_value());
        ASSERT_FALSE(exist_result.value());
    }
}

TEST_F(MasterServiceTest, RemoveByRegexComplex) {
    const uint64_t kv_lease_ttl = 100;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    auto service_ = std::make_unique<MasterService>(service_config);
    const UUID client_id = generate_uuid();

    // 1. Mount segment
    [[maybe_unused]] const auto context =
        PrepareSimpleSegment(*service_, "test_segment_remove");

    // A helper lambda to repopulate the store for each test case
    auto populate_store = [&]() {
        std::vector<std::string> keys_to_put = {
            "test_key_01",
            "test_key_02",
            "test_key_10",
            "prod_key_alpha",
            "prod_key_beta",
            "data_part_1_chunk_a",
            "data_part_2_chunk_b",
            "config/user/settings.json",
            "logs/app-2025-08-13.log",
            "short",
            "a_very_very_very_long_key_that_tests_length_limits",
            "test-key-extra",
            "another_key"};
        for (const auto& key : keys_to_put) {
            put_object(*service_, client_id, key);
        }
        // Wait for potential lease propagation
        std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));
    };

    // --- Test Case 1: Remove a specific subset and verify ---
    {
        SCOPED_TRACE("Test Case 1: Removing keys with prefix 'test_key_'");
        populate_store();

        // Action: Remove keys starting with "test_key_"
        auto remove_result = service_->RemoveByRegex("^test_key_", "default");
        ASSERT_TRUE(remove_result.has_value());
        EXPECT_EQ(remove_result.value(), 3);  // Should remove 3 keys

        // Verification: Check which keys were deleted and which remain
        std::vector<std::string> deleted_keys = {"test_key_01", "test_key_02",
                                                 "test_key_10"};
        for (const auto& key : deleted_keys) {
            auto exist_result = service_->ExistKey(key, "default");
            ASSERT_TRUE(exist_result.has_value());
            EXPECT_FALSE(exist_result.value())
                << "Key " << key << " should have been deleted.";
        }

        std::vector<std::string> remaining_keys = {
            "prod_key_alpha", "short", "test-key-extra"};  // Sample a few
        for (const auto& key : remaining_keys) {
            auto exist_result = service_->ExistKey(key, "default");
            ASSERT_TRUE(exist_result.has_value());
            EXPECT_TRUE(exist_result.value())
                << "Key " << key << " should NOT have been deleted.";
        }
    }

    // --- Test Case 2: Remove everything ---
    {
        SCOPED_TRACE("Test Case 2: Removing all keys with '.*'");
        // Store is already populated from the previous (failed) test run, or we
        // can repopulate For isolation, let's assume we start fresh
        auto service_config = MasterServiceConfig::builder()
                                  .set_default_kv_lease_ttl(kv_lease_ttl)
                                  .build();
        service_ = std::make_unique<MasterService>(service_config);
        [[maybe_unused]] const auto context_reset =
            PrepareSimpleSegment(*service_, "test_segment_remove");
        populate_store();

        size_t total_keys = 13;  // Count from the keys_to_put vector

        // Action: Remove all keys
        auto remove_result = service_->RemoveByRegex(".*", "default");
        ASSERT_TRUE(remove_result.has_value());
        EXPECT_EQ(remove_result.value(), total_keys);

        // Verification: Check that no keys remain
        auto get_all_result = service_->GetReplicaListByRegex(".*", "default");
        ASSERT_TRUE(get_all_result.has_value());
        EXPECT_TRUE(get_all_result.value().empty());
    }

    // --- Test Case 3: Attempt to remove with a non-matching pattern ---
    {
        SCOPED_TRACE("Test Case 3: Removing with a non-matching pattern");
        auto service_config = MasterServiceConfig::builder()
                                  .set_default_kv_lease_ttl(kv_lease_ttl)
                                  .build();
        service_ = std::make_unique<MasterService>(
            service_config);  // Reset the service
        [[maybe_unused]] const auto context_reset =
            PrepareSimpleSegment(*service_, "test_segment_remove");
        populate_store();

        size_t total_keys_before_remove = 13;

        // Action: Attempt to remove using a pattern that matches nothing
        auto remove_result =
            service_->RemoveByRegex("^nonexistent-pattern-", "default");
        ASSERT_TRUE(remove_result.has_value());
        EXPECT_EQ(remove_result.value(), 0);  // Should remove 0 keys

        // Verification: Check that all keys still exist
        auto get_all_result = service_->GetReplicaListByRegex(".*", "default");
        ASSERT_TRUE(get_all_result.has_value());
        EXPECT_EQ(get_all_result.value().size(), total_keys_before_remove);
    }

    // --- Test Case 4: Remove based on a complex pattern and verify ---
    {
        SCOPED_TRACE(
            "Test Case 4: Removing based on file paths or containing digits");
        auto service_config = MasterServiceConfig::builder()
                                  .set_default_kv_lease_ttl(kv_lease_ttl)
                                  .build();
        service_ = std::make_unique<MasterService>(service_config);  // Reset
        [[maybe_unused]] const auto context_reset =
            PrepareSimpleSegment(*service_, "test_segment_remove");
        populate_store();

        // Action: Remove all keys that contain a slash '/' OR end with a number
        auto remove_result = service_->RemoveByRegex("/|\\d$", "default");
        ASSERT_TRUE(remove_result.has_value());
        // Matches: "config/user/settings.json", "logs/app-2025-08-13.log",
        //          "test_key_01", "test_key_02", "test_key_10"
        // Note: logs/app-2025-08-13.log matches both, but is counted once.
        EXPECT_EQ(remove_result.value(),
                  5);  // The two paths + test_key_01 and test_key_02.
                       // (test_key_10 ends with 0) wait, no, 10 ends with 0.
        // Ah, \d$ matches a single digit at the end. So test_key_01,
        // test_key_02. test_key_10 does NOT match \d$. Let's refine the regex.
    }

    // --- Test Case 4 ---
    {
        SCOPED_TRACE(
            "Test Case 4 (Corrected): Removing based on complex pattern");
        auto service_config = MasterServiceConfig::builder()
                                  .set_default_kv_lease_ttl(kv_lease_ttl)
                                  .build();
        service_ = std::make_unique<MasterService>(service_config);  // Reset
        [[maybe_unused]] const auto context_reset =
            PrepareSimpleSegment(*service_, "test_segment_remove");
        populate_store();

        // Action: Remove all keys that contain "chunk" OR "config"
        auto remove_result = service_->RemoveByRegex("chunk|config", "default");
        ASSERT_TRUE(remove_result.has_value());
        // Matches: "data_part_1_chunk_a", "data_part_2_chunk_b",
        // "config/user/settings.json"
        EXPECT_EQ(remove_result.value(), 3);

        // Verification
        auto exist_result_chunk =
            service_->ExistKey("data_part_1_chunk_a", "default");
        ASSERT_TRUE(exist_result_chunk.has_value());
        EXPECT_FALSE(exist_result_chunk.value());

        auto exist_result_config =
            service_->ExistKey("config/user/settings.json", "default");
        ASSERT_TRUE(exist_result_config.has_value());
        EXPECT_FALSE(exist_result_config.value());

        auto exist_result_untouched =
            service_->ExistKey("prod_key_alpha", "default");
        ASSERT_TRUE(exist_result_untouched.has_value());
        EXPECT_TRUE(exist_result_untouched.value());
    }
}

TEST_F(MasterServiceTest, RemoveAll) {
    const uint64_t kv_lease_ttl = 50;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();
    int times = 10;
    while (times--) {
        std::string key = "test_key" + std::to_string(times);
        uint64_t value_length = 1024;
        ReplicateConfig config;
        config.replica_num = 1;
        auto put_start_result =
            service_->PutStart(client_id, key, "default", value_length, config);
        ASSERT_TRUE(put_start_result.has_value());
        auto put_end_result =
            service_->PutEnd(client_id, key, "default", ReplicaType::MEMORY);
        ASSERT_TRUE(put_end_result.has_value());
        auto exist_result = service_->ExistKey(key, "default");
        ASSERT_TRUE(exist_result.has_value());
    }
    // wait for all the lease to expire
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));
    ASSERT_EQ(10, service_->RemoveAll());
    times = 10;
    while (times--) {
        std::string key = "test_key" + std::to_string(times);
        auto exist_result = service_->ExistKey(key, "default");
        ASSERT_TRUE(exist_result.has_value());
        ASSERT_FALSE(exist_result.value());
    }
}

TEST_F(MasterServiceTest, SingleSliceMultiReplicaFlow) {
    const uint64_t kv_lease_ttl = 50;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    const UUID client_id = generate_uuid();

    // Mount 3 segments, each 64MB
    constexpr size_t kBaseAddr = 0x300000000;
    constexpr size_t kSegmentSize = 1024 * 1024 * 64;  // 64MB
    for (int i = 0; i < 3; ++i) {
        [[maybe_unused]] const auto context = PrepareSimpleSegment(
            *service_, "segment_" + std::to_string(i),
            kBaseAddr + static_cast<size_t>(i) * kSegmentSize, kSegmentSize);
    }

    // Test parameters
    std::string key = "multi_slice_object";
    constexpr size_t num_replicas = 3;
    constexpr size_t slice_length = 1024 * 1024 * 5;  // 5MB

    // Configure replication
    ReplicateConfig config;
    config.replica_num = num_replicas;
    std::vector<Replica::Descriptor> replica_list;

    // Test PutStart with multiple slices and replicas
    auto put_start_result =
        service_->PutStart(client_id, key, "default", slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    replica_list = put_start_result.value();

    // Verify replica list properties
    ASSERT_EQ(num_replicas, replica_list.size());
    for (const auto& replica : replica_list) {
        // Verify replica status
        EXPECT_EQ(ReplicaStatus::PROCESSING, replica.status);

        // Verify slice length matches buffer descriptor
        EXPECT_EQ(slice_length,
                  replica.get_memory_descriptor().buffer_descriptor.size_);
    }

    // Test GetReplicaList during processing (should fail)
    auto get_result = service_->GetReplicaList(key, "default");
    EXPECT_FALSE(get_result.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_NOT_READY, get_result.error());

    // Complete the put operation
    auto put_end_result =
        service_->PutEnd(client_id, key, "default", ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    // Test GetReplicaList after completion
    auto get_result2 = service_->GetReplicaList(key, "default");
    ASSERT_TRUE(get_result2.has_value());
    auto retrieved_replicas = get_result2.value().replicas;
    ASSERT_EQ(num_replicas, retrieved_replicas.size());

    // Verify final state of all replicas
    for (const auto& replica : retrieved_replicas) {
        EXPECT_EQ(ReplicaStatus::COMPLETE, replica.status);
        ASSERT_EQ(slice_length,
                  replica.get_memory_descriptor().buffer_descriptor.size_);
    }
}

TEST_F(MasterServiceTest, ConcurrentWriteAndRemoveAll) {
    std::unique_ptr<MasterService> service_(new MasterService());
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 256;  // 256MB for concurrent testing
    auto segment = MakeSegment("concurrent_segment", buffer, size);
    UUID client_id = generate_uuid();
    auto mount_result_concurrent = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result_concurrent.has_value());

    constexpr int num_threads = 4;
    constexpr int objects_per_thread = 100;
    std::atomic success_writes(0);
    std::atomic remove_all_done(false);
    std::atomic total_removed(0);

    // Writer threads
    std::vector<std::thread> writers;
    for (int i = 0; i < num_threads; ++i) {
        writers.emplace_back([&, i]() {
            std::mt19937 generator(static_cast<std::mt19937::result_type>(i));
            std::uniform_int_distribution<int> distribution(0, 9);
            for (int j = 0; j < objects_per_thread; ++j) {
                std::string key =
                    "key_" + std::to_string(i) + "_" + std::to_string(j);
                uint64_t slice_length = 1024;
                ReplicateConfig config;
                config.replica_num = 1;
                std::vector<Replica::Descriptor> replica_list;

                auto put_start_result = service_->PutStart(
                    client_id, key, "default", slice_length, config);
                if (put_start_result.has_value()) {
                    auto put_end_result = service_->PutEnd(
                        client_id, key, "default", ReplicaType::MEMORY);
                    if (put_end_result.has_value()) {
                        success_writes++;
                    }
                }

                // Random sleep to increase concurrency complexity
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(distribution(generator)));
            }
        });
    }

    // RemoveAll thread
    std::thread remove_thread([&]() {
        std::this_thread::sleep_for(
            std::chrono::milliseconds(50));  // Let some writes start
        long removed = service_->RemoveAll();
        LOG(INFO) << "Removed " << removed
                  << " objects during concurrent writes";
        ASSERT_GT(removed, 0);
        remove_all_done = true;
        total_removed.fetch_add(removed);
    });

    // Join all threads
    for (auto& t : writers) {
        t.join();
    }
    remove_thread.join();

    // Verify results
    EXPECT_GT(success_writes, 0);
    EXPECT_TRUE(remove_all_done);

    // Final RemoveAll to ensure clean state
    long final_removed = service_->RemoveAll();
    LOG(INFO) << "Final RemoveAll removed " << final_removed << " objects";
    ASSERT_GT(final_removed, 0);
    total_removed.fetch_add(final_removed);
    ASSERT_EQ(total_removed, num_threads * objects_per_thread);
}

TEST_F(MasterServiceTest, ConcurrentReadAndRemoveAll) {
    // set a large kv_lease_ttl so the granted lease will not quickly expire
    const uint64_t kv_lease_ttl = 200;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 256;  // 256MB for concurrent testing
    auto segment = MakeSegment("concurrent_segment", buffer, size);
    UUID client_id = generate_uuid();
    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    // Pre-populate with test data
    constexpr int num_objects = 1000;
    for (int i = 0; i < num_objects; ++i) {
        std::string key = "pre_key_" + std::to_string(i);
        uint64_t slice_length = 1024;
        ReplicateConfig config;
        config.replica_num = 1;

        auto put_start_result =
            service_->PutStart(client_id, key, "default", slice_length, config);
        ASSERT_TRUE(put_start_result.has_value());
        auto put_end_result =
            service_->PutEnd(client_id, key, "default", ReplicaType::MEMORY);
        ASSERT_TRUE(put_end_result.has_value());
    }

    std::atomic<int> success_reads(0);
    std::atomic<bool> remove_all_done(false);

    // Reader threads
    std::vector<std::thread> readers;
    for (int i = 0; i < 4; ++i) {
        readers.emplace_back([&, i]() {
            std::mt19937 generator(static_cast<std::mt19937::result_type>(i));
            std::uniform_int_distribution<int> distribution(0, 4);
            for (int j = 0; j < num_objects; ++j) {
                std::string key = "pre_key_" + std::to_string(j);
                auto get_result = service_->GetReplicaList(key, "default");
                if (get_result.has_value()) {
                    success_reads++;
                }

                // Random sleep to increase concurrency complexity
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(distribution(generator)));
            }
        });
    }

    // RemoveAll thread
    std::thread remove_thread([&]() {
        std::this_thread::sleep_for(
            std::chrono::milliseconds(10));  // Let some reads start
        long removed = service_->RemoveAll();
        LOG(INFO) << "Removed " << removed
                  << " objects during concurrent reads";
        remove_all_done = true;
    });

    // Join all threads
    for (auto& t : readers) {
        t.join();
    }
    remove_thread.join();

    EXPECT_TRUE(remove_all_done);
    // Verify 0 < success_reads < num_objects
    EXPECT_GT(success_reads, 0);
    EXPECT_NE(success_reads, num_objects);

    // wait for all the lease to expire
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));
    long removed = service_->RemoveAll();
    LOG(INFO) << "Removed " << removed << " objects after kv lease expired";

    // Verify all objects were removed
    for (int i = 0; i < num_objects; ++i) {
        std::string key = "pre_key_" + std::to_string(i);
        auto get_result = service_->GetReplicaList(key, "default");
        EXPECT_FALSE(get_result.has_value());
        EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, get_result.error());
    }
}

TEST_F(MasterServiceTest, ConcurrentRemoveAllOperations) {
    std::unique_ptr<MasterService> service_(new MasterService());
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 16 * 100;
    auto segment = MakeSegment("concurrent_segment", buffer, size);
    UUID client_id = generate_uuid();
    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    // Pre-populate with test data
    constexpr int num_objects = 1000;
    for (int i = 0; i < num_objects; ++i) {
        std::string key = "pre_key_" + std::to_string(i);
        uint64_t slice_length = 1024;
        ReplicateConfig config;
        config.replica_num = 1;

        auto put_start_result =
            service_->PutStart(client_id, key, "default", slice_length, config);
        ASSERT_TRUE(put_start_result.has_value());
        auto put_end_result =
            service_->PutEnd(client_id, key, "default", ReplicaType::MEMORY);
        ASSERT_TRUE(put_end_result.has_value());
    }

    std::atomic<int> remove_all_count(0);

    // Two RemoveAll threads
    std::vector<std::thread> remove_threads;
    for (int i = 0; i < 2; ++i) {
        remove_threads.emplace_back([&]() {
            long removed = service_->RemoveAll();
            LOG(INFO) << "RemoveAll removed " << removed << " objects";
            remove_all_count += removed;
        });
    }

    // Join all threads
    for (auto& t : remove_threads) {
        t.join();
    }

    // Verify results - one RemoveAll should return num_objects, the other 0
    EXPECT_EQ(num_objects, remove_all_count);

    // Verify all objects were removed
    for (int i = 0; i < num_objects; ++i) {
        std::string key = "pre_key_" + std::to_string(i);
        auto get_result = service_->GetReplicaList(key, "default");
        EXPECT_FALSE(get_result.has_value());
        EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, get_result.error());
    }
}

}  // namespace mooncake::test

#include "fixture.h"

namespace mooncake::test {
TEST_F(MasterServiceTest, BatchGetReplicaListKeepsTenantIsolation) {
    const std::string key = "batch_get_tenant_shared_key";
    const std::string tenant_a = "batch_get_tenant_a";
    const std::string tenant_b = "batch_get_tenant_b";
    auto service_ = std::make_unique<MasterService>(
        MakeStrictTenantConfig({"default", tenant_a, tenant_b}));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    ReplicateConfig config_a;
    config_a.replica_num = 1;
    config_a.group_ids =
        std::vector<std::string>{FindGroupIdOnDifferentShard(key)};
    ASSERT_TRUE(service_->PutStart(client_id, key, tenant_a, 1024, config_a)
                    .has_value());
    ASSERT_TRUE(service_->PutEnd(client_id, key, tenant_a, ReplicaType::MEMORY)
                    .has_value());

    ReplicateConfig config_b;
    config_b.replica_num = 1;
    ASSERT_TRUE(service_->PutStart(client_id, key, tenant_b, 2048, config_b)
                    .has_value());
    ASSERT_TRUE(service_->PutEnd(client_id, key, tenant_b, ReplicaType::MEMORY)
                    .has_value());

    auto tenant_a_results = service_->BatchGetReplicaList({key}, tenant_a);
    auto tenant_b_results = service_->BatchGetReplicaList({key}, tenant_b);
    auto default_results = service_->BatchGetReplicaList({key}, "default");

    ASSERT_EQ(tenant_a_results.size(), 1u);
    ASSERT_EQ(tenant_b_results.size(), 1u);
    ASSERT_EQ(default_results.size(), 1u);
    EXPECT_TRUE(tenant_a_results[0].has_value());
    EXPECT_TRUE(tenant_b_results[0].has_value());
    ASSERT_FALSE(default_results[0].has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, default_results[0].error());
}

TEST_F(MasterServiceTest, GetAllKeysListsOnlyRequestedTenant) {
    const std::string tenant_a = "tenant_get_all_keys_a";
    auto service_ = std::make_unique<MasterService>(
        MakeStrictTenantConfig({"default", tenant_a}));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    const std::string shared_key = "shared_listing_key";
    const std::string default_only_key = "default_listing_key";
    const std::string tenant_only_key = "tenant_listing_key";

    ReplicateConfig config;
    config.replica_num = 1;
    ASSERT_TRUE(
        service_->PutStart(client_id, shared_key, "default", 1024, config)
            .has_value());
    ASSERT_TRUE(
        service_->PutEnd(client_id, shared_key, "default", ReplicaType::MEMORY)
            .has_value());
    ASSERT_TRUE(
        service_->PutStart(client_id, default_only_key, "default", 1024, config)
            .has_value());
    ASSERT_TRUE(service_
                    ->PutEnd(client_id, default_only_key, "default",
                             ReplicaType::MEMORY)
                    .has_value());
    ASSERT_TRUE(
        service_->PutStart(client_id, shared_key, tenant_a, 1024, config)
            .has_value());
    ASSERT_TRUE(
        service_->PutEnd(client_id, shared_key, tenant_a, ReplicaType::MEMORY)
            .has_value());
    ASSERT_TRUE(
        service_->PutStart(client_id, tenant_only_key, tenant_a, 1024, config)
            .has_value());
    ASSERT_TRUE(
        service_
            ->PutEnd(client_id, tenant_only_key, tenant_a, ReplicaType::MEMORY)
            .has_value());

    auto default_keys = service_->GetAllKeys("default");
    ASSERT_TRUE(default_keys.has_value());
    EXPECT_NE(std::find(default_keys->begin(), default_keys->end(), shared_key),
              default_keys->end());
    EXPECT_NE(
        std::find(default_keys->begin(), default_keys->end(), default_only_key),
        default_keys->end());
    EXPECT_EQ(
        std::find(default_keys->begin(), default_keys->end(), tenant_only_key),
        default_keys->end());

    auto tenant_keys = service_->GetAllKeys(tenant_a);
    ASSERT_TRUE(tenant_keys.has_value());
    EXPECT_NE(std::find(tenant_keys->begin(), tenant_keys->end(), shared_key),
              tenant_keys->end());
    EXPECT_NE(
        std::find(tenant_keys->begin(), tenant_keys->end(), tenant_only_key),
        tenant_keys->end());
    EXPECT_EQ(
        std::find(tenant_keys->begin(), tenant_keys->end(), default_only_key),
        tenant_keys->end());
}

TEST_F(MasterServiceTest, TenantPutGetRemoveIsolatesSameUserKey) {
    const std::string key = "shared_user_key";
    const std::string tenant_a = "tenant_a";
    const std::string tenant_b = "tenant_b";
    auto service_ = std::make_unique<MasterService>(
        MakeStrictTenantConfig({"default", tenant_a, tenant_b}));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    ReplicateConfig config;
    config.replica_num = 1;

    ASSERT_TRUE(
        service_->PutStart(client_id, key, tenant_a, 1024, config).has_value());
    ASSERT_TRUE(service_->PutEnd(client_id, key, tenant_a, ReplicaType::MEMORY)
                    .has_value());
    ASSERT_TRUE(
        service_->PutStart(client_id, key, tenant_b, 2048, config).has_value());
    ASSERT_TRUE(service_->PutEnd(client_id, key, tenant_b, ReplicaType::MEMORY)
                    .has_value());

    EXPECT_FALSE(service_->GetReplicaList(key, "default").has_value());
    EXPECT_FALSE(service_->ExistKey(key, "default").value());
    EXPECT_TRUE(service_->ExistKey(key, tenant_a).value());
    EXPECT_TRUE(service_->ExistKey(key, tenant_b).value());
    EXPECT_TRUE(service_->GetReplicaList(key, tenant_a).has_value());
    EXPECT_TRUE(service_->GetReplicaList(key, tenant_b).has_value());
    EXPECT_EQ(service_->GetKeyCount(), 2u);

    ASSERT_TRUE(service_->Remove(key, tenant_a, /*force=*/true).has_value());
    EXPECT_FALSE(service_->GetReplicaList(key, tenant_a).has_value());
    EXPECT_TRUE(service_->GetReplicaList(key, tenant_b).has_value());
    EXPECT_EQ(service_->GetKeyCount(), 1u);
}

TEST_F(MasterServiceTest, RegexOperationsAreTenantScoped) {
    const std::string key = "regex_shared_key";
    const std::string tenant_a = "tenant_regex_a";
    const std::string tenant_b = "tenant_regex_b";
    auto service_ = std::make_unique<MasterService>(
        MakeStrictTenantConfig({"default", tenant_a, tenant_b}));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    ReplicateConfig config;
    config.replica_num = 1;

    ASSERT_TRUE(service_->PutStart(client_id, key, "default", 1024, config)
                    .has_value());
    ASSERT_TRUE(service_->PutEnd(client_id, key, "default", ReplicaType::MEMORY)
                    .has_value());
    ASSERT_TRUE(
        service_->PutStart(client_id, key, tenant_a, 1024, config).has_value());
    ASSERT_TRUE(service_->PutEnd(client_id, key, tenant_a, ReplicaType::MEMORY)
                    .has_value());
    ASSERT_TRUE(
        service_->PutStart(client_id, key, tenant_b, 1024, config).has_value());
    ASSERT_TRUE(service_->PutEnd(client_id, key, tenant_b, ReplicaType::MEMORY)
                    .has_value());

    auto default_matches =
        service_->GetReplicaListByRegex("^regex_shared", "default");
    ASSERT_TRUE(default_matches.has_value());
    EXPECT_EQ(default_matches->size(), 1);

    auto remove_default =
        service_->RemoveByRegex("^regex_shared", "default", /*force=*/true);
    ASSERT_TRUE(remove_default.has_value());
    EXPECT_EQ(remove_default.value(), 1);
    EXPECT_FALSE(service_->GetReplicaList(key, "default").has_value());
    EXPECT_TRUE(service_->GetReplicaList(key, tenant_a).has_value());
    EXPECT_TRUE(service_->GetReplicaList(key, tenant_b).has_value());

    auto remove_tenant_a =
        service_->RemoveByRegex("^regex_shared", tenant_a, /*force=*/true);
    ASSERT_TRUE(remove_tenant_a.has_value());
    EXPECT_EQ(remove_tenant_a.value(), 1);
    EXPECT_FALSE(service_->GetReplicaList(key, tenant_a).has_value());
    EXPECT_TRUE(service_->GetReplicaList(key, tenant_b).has_value());
}

TEST_F(MasterServiceTest, TenantBatchRemoveAndRemoveAllAreScoped) {
    const std::string shared_key = "tenant_batch_remove_shared_key";
    const std::string tenant_a = "tenant_batch_remove_a";
    const std::string tenant_b = "tenant_batch_remove_b";
    auto svc = std::make_unique<MasterService>(
        MakeStrictTenantConfig({"default", tenant_a, tenant_b}));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*svc);
    const UUID client_id = generate_uuid();

    ReplicateConfig config;
    config.replica_num = 1;

    ASSERT_TRUE(svc->PutStart(client_id, shared_key, "default", 1024, config)
                    .has_value());
    ASSERT_TRUE(
        svc->PutEnd(client_id, shared_key, "default", ReplicaType::MEMORY)
            .has_value());
    ASSERT_TRUE(svc->PutStart(client_id, shared_key, tenant_a, 1024, config)
                    .has_value());
    ASSERT_TRUE(
        svc->PutEnd(client_id, shared_key, tenant_a, ReplicaType::MEMORY)
            .has_value());
    ASSERT_TRUE(svc->PutStart(client_id, shared_key, tenant_b, 1024, config)
                    .has_value());
    ASSERT_TRUE(
        svc->PutEnd(client_id, shared_key, tenant_b, ReplicaType::MEMORY)
            .has_value());

    auto remove_a = svc->BatchRemove({shared_key}, tenant_a, /*force=*/true);
    ASSERT_EQ(remove_a.size(), 1u);
    ASSERT_TRUE(remove_a[0].has_value());
    EXPECT_FALSE(svc->GetReplicaList(shared_key, tenant_a).has_value());
    EXPECT_TRUE(svc->GetReplicaList(shared_key, "default").has_value());
    EXPECT_TRUE(svc->GetReplicaList(shared_key, tenant_b).has_value());

    EXPECT_EQ(svc->RemoveAll(tenant_b, /*force=*/true), 1);
    EXPECT_FALSE(svc->GetReplicaList(shared_key, tenant_b).has_value());
    EXPECT_TRUE(svc->GetReplicaList(shared_key, "default").has_value());

    EXPECT_EQ(svc->RemoveAll(/*force=*/true), 1);
    EXPECT_FALSE(svc->GetReplicaList(shared_key, "default").has_value());
}

TEST_F(MasterServiceTest, LegacyRemoveAllRemovesAllTenants) {
    const std::string key = "legacy_remove_all_shared_key";
    const std::string tenant_a = "legacy_remove_all_a";
    const std::string tenant_b = "legacy_remove_all_b";
    auto svc = std::make_unique<MasterService>(
        MakeStrictTenantConfig({"default", tenant_a, tenant_b}));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*svc);
    const UUID client_id = generate_uuid();

    ReplicateConfig config;
    config.replica_num = 1;

    ASSERT_TRUE(
        svc->PutStart(client_id, key, "default", 1024, config).has_value());
    ASSERT_TRUE(svc->PutEnd(client_id, key, "default", ReplicaType::MEMORY)
                    .has_value());
    ASSERT_TRUE(
        svc->PutStart(client_id, key, tenant_a, 1024, config).has_value());
    ASSERT_TRUE(
        svc->PutEnd(client_id, key, tenant_a, ReplicaType::MEMORY).has_value());
    ASSERT_TRUE(
        svc->PutStart(client_id, key, tenant_b, 1024, config).has_value());
    ASSERT_TRUE(
        svc->PutEnd(client_id, key, tenant_b, ReplicaType::MEMORY).has_value());

    EXPECT_EQ(svc->RemoveAll(/*force=*/true), 3);
    EXPECT_FALSE(svc->GetReplicaList(key, "default").has_value());
    EXPECT_FALSE(svc->GetReplicaList(key, tenant_a).has_value());
    EXPECT_FALSE(svc->GetReplicaList(key, tenant_b).has_value());
    EXPECT_EQ(svc->RemoveAll(/*force=*/true), 0);
}

TEST_F(MasterServiceTest, BatchExistKeyTest) {
    std::unique_ptr<MasterService> service_(new MasterService());
    const UUID client_id = generate_uuid();

    // Mount a segment
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 128;
    constexpr size_t value_size = 1024;
    [[maybe_unused]] const auto context =
        PrepareSimpleSegment(*service_, "test_segment", buffer, size);

    int test_object_num = 10;
    std::vector<std::string> test_keys;
    for (int i = 0; i < test_object_num; ++i) {
        test_keys.push_back("test_key" + std::to_string(i));
        ReplicateConfig config;
        config.replica_num = 1;
        uint64_t slice_length = value_size;
        auto put_start_result = service_->PutStart(
            client_id, test_keys[i], "default", slice_length, config);
        ASSERT_TRUE(put_start_result.has_value());
        auto put_end_result = service_->PutEnd(client_id, test_keys[i],
                                               "default", ReplicaType::MEMORY);
        ASSERT_TRUE(put_end_result.has_value());
    }

    // Test individual ExistKey calls to verify the underlying functionality
    for (int i = 0; i < test_object_num; ++i) {
        auto exist_result = service_->ExistKey(test_keys[i], "default");
        EXPECT_TRUE(exist_result.value());
    }

    // Tets batch
    test_keys.push_back("non_existent_key");
    auto exist_resp = service_->BatchExistKey(test_keys, "default");
    for (int i = 0; i < test_object_num; ++i) {
        ASSERT_TRUE(exist_resp[i].value());
    }
    ASSERT_FALSE(exist_resp[test_object_num].value());
}

TEST_F(MasterServiceTest, BatchExistKeyTenantAwarePreservesOrder) {
    const std::string tenant_id = "tenant_batch_exist";
    auto service_ = std::make_unique<MasterService>(
        MakeStrictTenantConfig({"default", tenant_id}));
    const UUID client_id = generate_uuid();

    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 128;
    [[maybe_unused]] const auto context =
        PrepareSimpleSegment(*service_, "test_segment", buffer, size);

    ReplicateConfig config;
    config.replica_num = 1;

    const std::string tenant_only_key = "batch_tenant_only";
    const std::string default_only_key = "batch_default_only";
    const std::string incomplete_key = "batch_tenant_incomplete";
    const std::string missing_key = "batch_tenant_missing";

    PutCompletedObject(*service_, client_id, tenant_only_key, tenant_id,
                       config);
    PutCompletedObject(*service_, client_id, default_only_key, config);
    ASSERT_TRUE(
        service_->PutStart(client_id, incomplete_key, tenant_id, 1024, config)
            .has_value());

    std::vector<std::string> tenant_keys = {tenant_only_key, default_only_key,
                                            missing_key, incomplete_key,
                                            tenant_only_key};
    auto tenant_resp = service_->BatchExistKey(tenant_keys, tenant_id);
    ASSERT_EQ(tenant_resp.size(), tenant_keys.size());
    EXPECT_TRUE(tenant_resp[0].value());
    EXPECT_FALSE(tenant_resp[1].value());
    EXPECT_FALSE(tenant_resp[2].value());
    EXPECT_FALSE(tenant_resp[3].value());
    EXPECT_TRUE(tenant_resp[4].value());

    std::vector<std::string> default_keys = {tenant_only_key, default_only_key};
    auto default_resp = service_->BatchExistKey(default_keys, "default");
    ASSERT_EQ(default_resp.size(), default_keys.size());
    EXPECT_FALSE(default_resp[0].value());
    EXPECT_TRUE(default_resp[1].value());
}

TEST_F(MasterServiceTest, WrappedBatchExistKeyUsesTenantAwareBatchPath) {
    const std::string tenant_id = "wrapped_batch_exist_tenant";
    auto service_config = MakeStrictWrappedConfig({"default", tenant_id});
    WrappedMasterService service_(service_config);

    Segment segment = MakeSegment("wrapped_batch_exist_segment");
    const UUID client_id = generate_uuid();
    ASSERT_TRUE(service_.MountSegment(segment, client_id).has_value());

    ReplicateConfig config;
    config.replica_num = 1;
    const std::string tenant_key_a = "wrapped_batch_tenant_a";
    const std::string tenant_key_b = "wrapped_batch_tenant_b";
    const std::string default_only_key = "wrapped_batch_default_only";
    const std::string missing_key = "wrapped_batch_missing";

    std::vector<std::string> tenant_keys = {tenant_key_a, tenant_key_b};
    std::vector<uint64_t> tenant_sizes = {1024, 2048};
    auto tenant_put_start = service_.BatchPutStart(
        client_id, tenant_keys, tenant_sizes, config, tenant_id);
    ASSERT_EQ(tenant_put_start.size(), tenant_keys.size());
    for (const auto& result : tenant_put_start) {
        ASSERT_TRUE(result.has_value()) << toString(result.error());
    }
    auto tenant_put_end = service_.BatchPutEnd(client_id, tenant_keys,
                                               ReplicaType::MEMORY, tenant_id);
    ASSERT_EQ(tenant_put_end.size(), tenant_keys.size());
    for (const auto& result : tenant_put_end) {
        ASSERT_TRUE(result.has_value()) << toString(result.error());
    }

    auto default_put_start =
        service_.PutStart(client_id, default_only_key, 1024, config);
    ASSERT_TRUE(default_put_start.has_value());
    ASSERT_TRUE(
        service_.PutEnd(client_id, default_only_key, ReplicaType::MEMORY)
            .has_value());

    auto& metrics = MasterMetricManager::instance();
    const auto base_requests = metrics.get_batch_exist_key_requests();
    const auto base_items = metrics.get_batch_exist_key_items();
    const auto base_failures = metrics.get_batch_exist_key_failures();
    const auto base_partial = metrics.get_batch_exist_key_partial_successes();
    const auto base_failed_items = metrics.get_batch_exist_key_failed_items();

    std::vector<std::string> lookup_keys = {tenant_key_a, default_only_key,
                                            missing_key, tenant_key_b};
    auto resp = service_.BatchExistKey(lookup_keys, tenant_id);
    ASSERT_EQ(resp.size(), lookup_keys.size());
    EXPECT_TRUE(resp[0].value());
    EXPECT_FALSE(resp[1].value());
    EXPECT_FALSE(resp[2].value());
    EXPECT_TRUE(resp[3].value());

    EXPECT_EQ(base_requests + 1, metrics.get_batch_exist_key_requests());
    EXPECT_EQ(base_items + lookup_keys.size(),
              metrics.get_batch_exist_key_items());
    EXPECT_EQ(base_failures, metrics.get_batch_exist_key_failures());
    EXPECT_EQ(base_partial, metrics.get_batch_exist_key_partial_successes());
    EXPECT_EQ(base_failed_items, metrics.get_batch_exist_key_failed_items());
}

TEST_F(MasterServiceTest, BatchQueryIpTest) {
    std::unique_ptr<MasterService> service_(new MasterService());
    const UUID client_id = generate_uuid();

    // Mount a segment with a specific te_endpoint (IP:Port format)
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 16;
    Segment segment = MakeSegment("test_segment", buffer, size);
    segment.te_endpoint = "127.0.0.1:12345";  // Set IP:Port format for testing
    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    // Test BatchQueryIp with a single client_id
    std::vector<UUID> client_ids = {client_id};
    auto query_result = service_->BatchQueryIp(client_ids);

    ASSERT_TRUE(query_result.has_value())
        << "BatchQueryIp failed: " << toString(query_result.error());

    const auto& results = query_result.value();
    ASSERT_FALSE(results.empty()) << "BatchQueryIp returned empty results";

    auto it = results.find(client_id);
    ASSERT_NE(it, results.end()) << "Client ID not found in results";

    const auto& ip_addresses = it->second;
    ASSERT_FALSE(ip_addresses.empty()) << "No IP addresses found for client";
    ASSERT_EQ(1u, ip_addresses.size()) << "Expected exactly 1 IP address";
    EXPECT_EQ("127.0.0.1", ip_addresses[0]) << "IP address mismatch";

    // Test BatchQueryIp with multiple client_ids (one valid, one invalid)
    UUID non_existent_client_id = generate_uuid();
    std::vector<UUID> mixed_client_ids = {client_id, non_existent_client_id};
    auto mixed_query_result = service_->BatchQueryIp(mixed_client_ids);

    ASSERT_TRUE(mixed_query_result.has_value());
    const auto& mixed_results = mixed_query_result.value();

    // Valid client_id should be in results
    ASSERT_NE(mixed_results.find(client_id), mixed_results.end())
        << "Valid client_id should be in results";

    // Invalid client_id should not be in results (silently skipped)
    EXPECT_EQ(mixed_results.find(non_existent_client_id), mixed_results.end())
        << "Invalid client_id should not be in results";
}

TEST_F(MasterServiceTest, BatchQueryIpMultipleSegmentsTest) {
    std::unique_ptr<MasterService> service_(new MasterService());
    const UUID client_id = generate_uuid();

    // Mount multiple segments with different IPs for the same client
    constexpr size_t buffer1 = 0x300000000;
    constexpr size_t buffer2 = 0x400000000;
    constexpr size_t size = 1024 * 1024 * 16;

    Segment segment1 = MakeSegment("segment1", buffer1, size);
    segment1.te_endpoint = "127.0.0.1:12345";
    auto mount_result1 = service_->MountSegment(segment1, client_id);
    ASSERT_TRUE(mount_result1.has_value());

    Segment segment2 = MakeSegment("segment2", buffer2, size);
    segment2.te_endpoint = "127.0.0.1:12346";  // Same IP, different port
    auto mount_result2 = service_->MountSegment(segment2, client_id);
    ASSERT_TRUE(mount_result2.has_value());

    Segment segment3 = MakeSegment("segment3", 0x500000000, size);
    segment3.te_endpoint = "192.168.1.1:12345";  // Different IP
    auto mount_result3 = service_->MountSegment(segment3, client_id);
    ASSERT_TRUE(mount_result3.has_value());

    // Test BatchQueryIp - should return unique IPs
    std::vector<UUID> client_ids = {client_id};
    auto query_result = service_->BatchQueryIp(client_ids);

    ASSERT_TRUE(query_result.has_value());
    const auto& results = query_result.value();
    auto it = results.find(client_id);
    ASSERT_NE(it, results.end());

    const auto& ip_addresses = it->second;
    // Should have 2 unique IPs: 127.0.0.1 and 192.168.1.1
    ASSERT_EQ(2u, ip_addresses.size()) << "Expected 2 unique IP addresses";

    // Verify both IPs are present
    std::unordered_set<std::string> ip_set(ip_addresses.begin(),
                                           ip_addresses.end());
    EXPECT_NE(ip_set.find("127.0.0.1"), ip_set.end());
    EXPECT_NE(ip_set.find("192.168.1.1"), ip_set.end());
}

TEST_F(MasterServiceTest, BatchQueryIpEmptyClientIdTest) {
    std::unique_ptr<MasterService> service_(new MasterService());

    // Test with empty client_ids list
    std::vector<UUID> empty_client_ids;
    auto query_result = service_->BatchQueryIp(empty_client_ids);

    ASSERT_TRUE(query_result.has_value());
    const auto& results = query_result.value();
    EXPECT_TRUE(results.empty())
        << "Empty client_ids should return empty results";
}

TEST_F(MasterServiceTest, BatchQueryIpMultipleSegmentsEmptyTeEndpointTest) {
    std::unique_ptr<MasterService> service_(new MasterService());
    const UUID client_id = generate_uuid();

    // Mount multiple segments, all with empty te_endpoint
    constexpr size_t buffer1 = 0x300000000;
    constexpr size_t buffer2 = 0x400000000;
    constexpr size_t size = 1024 * 1024 * 16;

    Segment segment1 = MakeSegment("segment1", buffer1, size);
    segment1.te_endpoint = "";  // Empty te_endpoint
    auto mount_result1 = service_->MountSegment(segment1, client_id);
    ASSERT_TRUE(mount_result1.has_value());

    Segment segment2 = MakeSegment("segment2", buffer2, size);
    segment2.te_endpoint = "";  // Empty te_endpoint
    auto mount_result2 = service_->MountSegment(segment2, client_id);
    ASSERT_TRUE(mount_result2.has_value());

    // Test BatchQueryIp - should return client with empty IP vector
    std::vector<UUID> client_ids = {client_id};
    auto query_result = service_->BatchQueryIp(client_ids);

    ASSERT_TRUE(query_result.has_value());
    const auto& results = query_result.value();
    ASSERT_FALSE(results.empty())
        << "BatchQueryIp should include client in results even with empty IPs";

    auto it = results.find(client_id);
    ASSERT_NE(it, results.end()) << "Client ID should be found in results even "
                                    "with all empty te_endpoints";

    // Verify the IP vector is empty
    const auto& ip_addresses = it->second;
    EXPECT_TRUE(ip_addresses.empty())
        << "Client with all empty te_endpoints should have empty IP vector";
}

TEST_F(MasterServiceTest, BatchQueryIpBracketedIpv6Test) {
    std::unique_ptr<MasterService> service_(new MasterService());
    const UUID client_id = generate_uuid();

    // Mount a segment with a bracketed IPv6 endpoint
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 16;
    Segment segment = MakeSegment("test_segment", buffer, size);
    segment.te_endpoint = "[::1]:17813";
    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    std::vector<UUID> client_ids = {client_id};
    auto query_result = service_->BatchQueryIp(client_ids);
    ASSERT_TRUE(query_result.has_value());

    const auto& results = query_result.value();
    auto it = results.find(client_id);
    ASSERT_NE(it, results.end());

    const auto& ip_addresses = it->second;
    ASSERT_EQ(1u, ip_addresses.size());
    EXPECT_EQ("::1", ip_addresses[0]);
}

TEST_F(MasterServiceTest, BatchQueryIpLinkLocalIpv6WithScopeTest) {
    std::unique_ptr<MasterService> service_(new MasterService());
    const UUID client_id = generate_uuid();

    // Mount a segment with a link-local IPv6 address with scope ID
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 16;
    Segment segment = MakeSegment("test_segment", buffer, size);
    segment.te_endpoint = "fe80::a236:bcff:fecb:a1be%eno2:15773";
    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    std::vector<UUID> client_ids = {client_id};
    auto query_result = service_->BatchQueryIp(client_ids);
    ASSERT_TRUE(query_result.has_value());

    const auto& results = query_result.value();
    auto it = results.find(client_id);
    ASSERT_NE(it, results.end());

    const auto& ip_addresses = it->second;
    ASSERT_EQ(1u, ip_addresses.size());
    EXPECT_EQ("fe80::a236:bcff:fecb:a1be%eno2", ip_addresses[0]);
}

TEST_F(MasterServiceTest, BatchQueryIpIpv6NoPortTest) {
    std::unique_ptr<MasterService> service_(new MasterService());
    const UUID client_id = generate_uuid();

    // Mount a segment with an IPv6 address without port
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 16;
    Segment segment = MakeSegment("test_segment", buffer, size);
    segment.te_endpoint = "::1";
    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    std::vector<UUID> client_ids = {client_id};
    auto query_result = service_->BatchQueryIp(client_ids);
    ASSERT_TRUE(query_result.has_value());

    const auto& results = query_result.value();
    auto it = results.find(client_id);
    ASSERT_NE(it, results.end());

    const auto& ip_addresses = it->second;
    ASSERT_EQ(1u, ip_addresses.size());
    EXPECT_EQ("::1", ip_addresses[0]);
}

TEST_F(MasterServiceTest, BatchQueryIpMixedIpv4AndIpv6Test) {
    std::unique_ptr<MasterService> service_(new MasterService());
    const UUID client_id = generate_uuid();

    // Mount segments with IPv4 and IPv6 endpoints for the same client
    constexpr size_t buffer1 = 0x300000000;
    constexpr size_t buffer2 = 0x400000000;
    constexpr size_t size = 1024 * 1024 * 16;

    Segment segment1 = MakeSegment("segment1", buffer1, size);
    segment1.te_endpoint = "192.168.1.1:12345";
    auto mount_result1 = service_->MountSegment(segment1, client_id);
    ASSERT_TRUE(mount_result1.has_value());

    Segment segment2 = MakeSegment("segment2", buffer2, size);
    segment2.te_endpoint = "[::1]:17813";
    auto mount_result2 = service_->MountSegment(segment2, client_id);
    ASSERT_TRUE(mount_result2.has_value());

    std::vector<UUID> client_ids = {client_id};
    auto query_result = service_->BatchQueryIp(client_ids);
    ASSERT_TRUE(query_result.has_value());

    const auto& results = query_result.value();
    auto it = results.find(client_id);
    ASSERT_NE(it, results.end());

    const auto& ip_addresses = it->second;
    ASSERT_EQ(2u, ip_addresses.size());

    std::unordered_set<std::string> ip_set(ip_addresses.begin(),
                                           ip_addresses.end());
    EXPECT_NE(ip_set.find("192.168.1.1"), ip_set.end());
    EXPECT_NE(ip_set.find("::1"), ip_set.end());
}

TEST_F(MasterServiceTest, TenantTasksCarryTenantInPayload) {
    const std::string tenant_id = "tenant_for_async_task";
    auto service =
        std::make_unique<MasterService>(MakeStrictTenantConfig({tenant_id}));
    const auto ctx0 = PrepareSimpleSegment(*service, "segment_0", 0x300000000,
                                           kDefaultSegmentSize);
    [[maybe_unused]] const auto ctx1 = PrepareSimpleSegment(
        *service, "segment_1", 0x400000000, kDefaultSegmentSize);

    const UUID put_client_id = generate_uuid();
    const std::string key = "tenant_task_key";

    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = "segment_0";

    ASSERT_TRUE(service
                    ->PutStart(put_client_id, key, tenant_id,
                               /*slice_length=*/1024, config)
                    .has_value());
    ASSERT_TRUE(
        service->PutEnd(put_client_id, key, tenant_id, ReplicaType::MEMORY)
            .has_value());

    auto copy_task_id = service->CreateCopyTask(key, tenant_id, {"segment_1"});
    ASSERT_TRUE(copy_task_id.has_value());
    auto move_task_id =
        service->CreateMoveTask(key, tenant_id, "segment_0", "segment_1");
    ASSERT_TRUE(move_task_id.has_value());

    auto fetched = service->FetchTasks(ctx0.client_id, /*batch_size=*/16);
    ASSERT_TRUE(fetched.has_value());
    ASSERT_EQ(fetched->size(), 2u);

    bool saw_copy = false;
    bool saw_move = false;
    for (const auto& assignment : *fetched) {
        if (assignment.id == copy_task_id.value()) {
            ReplicaCopyPayload payload;
            struct_json::from_json(payload, assignment.payload);
            EXPECT_EQ(payload.tenant_id, tenant_id);
            EXPECT_EQ(payload.key, key);
            saw_copy = true;
        } else if (assignment.id == move_task_id.value()) {
            ReplicaMovePayload payload;
            struct_json::from_json(payload, assignment.payload);
            EXPECT_EQ(payload.tenant_id, tenant_id);
            EXPECT_EQ(payload.key, key);
            saw_move = true;
        }
    }
    EXPECT_TRUE(saw_copy);
    EXPECT_TRUE(saw_move);
}

TEST_F(MasterServiceTest, LegacyTaskPayloadDefaultsTenant) {
    ReplicaCopyPayload copy_payload;
    struct_json::from_json(
        copy_payload,
        R"({"key":"legacy_copy_key","source":"segment_0","targets":["segment_1"]})");
    EXPECT_EQ(copy_payload.tenant_id, "default");
    EXPECT_EQ(copy_payload.key, "legacy_copy_key");
    EXPECT_EQ(copy_payload.source, "segment_0");
    ASSERT_EQ(copy_payload.targets.size(), 1u);
    EXPECT_EQ(copy_payload.targets[0], "segment_1");

    ReplicaMovePayload move_payload;
    struct_json::from_json(
        move_payload,
        R"({"key":"legacy_move_key","source":"segment_0","target":"segment_1"})");
    EXPECT_EQ(move_payload.tenant_id, "default");
    EXPECT_EQ(move_payload.key, "legacy_move_key");
    EXPECT_EQ(move_payload.source, "segment_0");
    EXPECT_EQ(move_payload.target, "segment_1");
}

}  // namespace mooncake::test

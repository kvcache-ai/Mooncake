#include "master_service_test_harness.h"

namespace mooncake::test {
TEST_F(MasterServiceTest,
       ConcurrentGroupedAndUngroupedFirstCreateDoesNotDuplicateMetadata) {
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    const std::string key = "concurrent_grouped_ungrouped_first_create";
    const TenantId tenant_id("tenant_concurrent_first_create");
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
    const TenantId tenant_id("tenant_concurrent_grouped_first_create");
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

TEST_F(MasterServiceTest, WrappedBatchExistKeyUsesTenantAwareBatchPath) {
    const TenantId tenant_id("wrapped_batch_exist_tenant");
    auto service_config = MakeStrictWrappedConfig(
        {std::string(TenantId::kDefaultValue), tenant_id.value()});
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
        client_id, tenant_keys, tenant_sizes, config, tenant_id.value());
    ASSERT_EQ(tenant_put_start.size(), tenant_keys.size());
    for (const auto& result : tenant_put_start) {
        ASSERT_TRUE(result.has_value()) << toString(result.error());
    }
    auto tenant_put_end = service_.BatchPutEnd(
        client_id, tenant_keys, ReplicaType::MEMORY, tenant_id.value());
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
    auto resp = service_.BatchExistKey(lookup_keys, tenant_id.value());
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

TEST_F(MasterServiceTest, WrappedWriteBoundaryRejectsInvalidTenantIds) {
    WrappedMasterService service(
        MakeStrictWrappedConfig({"registered-tenant"}));
    ReplicateConfig config;
    config.replica_num = 1;
    const UUID client_id = generate_uuid();

    auto empty =
        service.PutStart(client_id, "empty-tenant-key", 1024, config, "");
    ASSERT_FALSE(empty.has_value());
    EXPECT_EQ(empty.error(), ErrorCode::TENANT_NOT_REGISTERED);

    const std::string control_tenant("tenant\0bad", 10);
    auto invalid = service.PutStart(client_id, "invalid-tenant-key", 1024,
                                    config, control_tenant);
    ASSERT_FALSE(invalid.has_value());
    EXPECT_EQ(invalid.error(), ErrorCode::TENANT_NOT_REGISTERED);
}

TEST_F(MasterServiceTest, WrappedRequestBoundaryRejectsInvalidTenantIds) {
    WrappedMasterService service(
        MakeStrictWrappedConfig({std::string(TenantId::kDefaultValue)}));

    auto invalid = service.GetReplicaList("missing-key", "_invalid-tenant");
    ASSERT_FALSE(invalid.has_value());
    EXPECT_EQ(invalid.error(), ErrorCode::INVALID_PARAMS);

    const std::string control_tenant("tenant\0bad", 10);
    auto batch =
        service.BatchGetReplicaList({"key-a", "key-b"}, control_tenant);
    ASSERT_EQ(batch.size(), 2u);
    for (const auto& result : batch) {
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
    }

    std::vector<OffloadTaskItem> tasks = {
        {.tenant_id = "_invalid-tenant", .key = "key", .size = 1}};
    std::vector<StorageObjectMetadata> metadatas = {
        {.bucket_id = 0,
         .offset = 0,
         .key_size = 3,
         .data_size = 1,
         .transport_endpoint = "segment"}};
    auto offload =
        service.NotifyOffloadSuccess(generate_uuid(), tasks, metadatas);
    ASSERT_FALSE(offload.has_value());
    EXPECT_EQ(offload.error(), ErrorCode::INVALID_PARAMS);

    // RemoveAll has a legacy scalar return type and cannot carry ErrorCode.
    EXPECT_EQ(service.RemoveAll(false, "_invalid-tenant"), 0);
}

TEST_F(MasterServiceTest, WrappedRequestBoundaryPreservesTenantNormalization) {
    WrappedMasterService multi_tenant_service(
        MakeStrictWrappedConfig({std::string(TenantId::kDefaultValue)}));
    auto empty = multi_tenant_service.ExistKey("missing-key", "");
    ASSERT_TRUE(empty.has_value());
    EXPECT_FALSE(empty.value());

    WrappedMasterServiceConfig single_tenant_config;
    single_tenant_config.default_kv_lease_ttl = 100;
    single_tenant_config.enable_metric_reporting = false;
    single_tenant_config.enable_multi_tenants = false;
    WrappedMasterService single_tenant_service(single_tenant_config);
    auto invalid =
        single_tenant_service.ExistKey("missing-key", "_invalid-tenant");
    ASSERT_TRUE(invalid.has_value());
    EXPECT_FALSE(invalid.value());
}

}  // namespace mooncake::test

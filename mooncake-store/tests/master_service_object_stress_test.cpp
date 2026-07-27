#include "master_service_test_harness.h"

namespace mooncake::test {

TEST(TenantScopedStorageKeyTest, RoundTripsAndParsesLegacyKeys) {
    const auto scoped =
        TenantId("tenant:with:colon").MakeScopedKey("path/key:with:colon");
    EXPECT_NE(scoped.find('\0'), std::string::npos);

    auto [tenant_id, key] = TenantId::ParseScopedKey(scoped);
    EXPECT_EQ(tenant_id.value(), "tenant:with:colon");
    EXPECT_EQ(key, "path/key:with:colon");

    auto [default_tenant, default_key] = TenantId::ParseScopedKey("raw_key");
    EXPECT_EQ(default_tenant.value(), TenantId::kDefaultValue);
    EXPECT_EQ(default_key, "raw_key");

    std::string legacy = "legacy_tenant";
    legacy.push_back('\0');
    legacy.append("legacy_key");
    auto [legacy_tenant, legacy_key] = TenantId::ParseScopedKey(legacy);
    EXPECT_EQ(legacy_tenant.value(), "legacy_tenant");
    EXPECT_EQ(legacy_key, "legacy_key");
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
            for (int j = 0; j < objects_per_thread; ++j) {
                std::string key =
                    "key_" + std::to_string(i) + "_" + std::to_string(j);
                uint64_t slice_length = 1024;
                ReplicateConfig config;
                config.replica_num = 1;
                std::vector<Replica::Descriptor> replica_list;

                auto put_start_result = service_->PutStart(
                    client_id, key, TenantId::Default(), slice_length, config);
                if (put_start_result.has_value()) {
                    auto put_end_result =
                        service_->PutEnd(client_id, key, TenantId::Default(),
                                         ReplicaType::MEMORY);
                    if (put_end_result.has_value()) {
                        success_writes++;
                    }
                }

                // Random sleep to increase concurrency complexity
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(rand() % 10));
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

        auto put_start_result = service_->PutStart(
            client_id, key, TenantId::Default(), slice_length, config);
        ASSERT_TRUE(put_start_result.has_value());
        auto put_end_result = service_->PutEnd(
            client_id, key, TenantId::Default(), ReplicaType::MEMORY);
        ASSERT_TRUE(put_end_result.has_value());
    }

    std::atomic<int> success_reads(0);
    std::atomic<bool> remove_all_done(false);

    // Reader threads
    std::vector<std::thread> readers;
    for (int i = 0; i < 4; ++i) {
        readers.emplace_back([&]() {
            for (int j = 0; j < num_objects; ++j) {
                std::string key = "pre_key_" + std::to_string(j);
                auto get_result =
                    service_->GetReplicaList(key, TenantId::Default());
                if (get_result.has_value()) {
                    success_reads++;
                }

                // Random sleep to increase concurrency complexity
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(rand() % 5));
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
        auto get_result = service_->GetReplicaList(key, TenantId::Default());
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

        auto put_start_result = service_->PutStart(
            client_id, key, TenantId::Default(), slice_length, config);
        ASSERT_TRUE(put_start_result.has_value());
        auto put_end_result = service_->PutEnd(
            client_id, key, TenantId::Default(), ReplicaType::MEMORY);
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
        auto get_result = service_->GetReplicaList(key, TenantId::Default());
        EXPECT_FALSE(get_result.has_value());
        EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, get_result.error());
    }
}
TEST_F(MasterServiceTest, PutStartExpiringTest) {
    // Reset storage space metrics.
    MasterMetricManager::instance().reset_allocated_mem_size();
    MasterMetricManager::instance().reset_total_mem_capacity();

    MasterServiceConfig master_config;
    master_config.put_start_discard_timeout_sec = 3;
    master_config.put_start_release_timeout_sec = 5;
    std::unique_ptr<MasterService> service_(new MasterService(master_config));

    constexpr size_t kReplicaCnt = 3;
    constexpr size_t kBaseAddr = 0x300000000;
    constexpr size_t kSegmentSize = 1024 * 1024 * 16;  // 16MB

    // Mount 3 segments.
    std::vector<MountedSegmentContext> contexts;
    contexts.reserve(kReplicaCnt);
    for (size_t i = 0; i < kReplicaCnt; ++i) {
        auto context = PrepareSimpleSegment(
            *service_, "segment_" + std::to_string(i),
            kBaseAddr + static_cast<size_t>(i) * kSegmentSize, kSegmentSize);
        contexts.push_back(context);
    }

    // The client_id used to put objects.
    auto client_id = generate_uuid();
    std::string key_1 = "test_key_1", key_2 = "test_key_2";
    uint64_t value_length = 6 * 1024 * 1024;  // 6MB
    uint64_t slice_length = value_length;
    ReplicateConfig config;
    config.replica_num = kReplicaCnt;

    // Put key_1, should success.
    auto put_start_result = service_->PutStart(
        client_id, key_1, TenantId::Default(), slice_length, config);
    EXPECT_TRUE(put_start_result.has_value());
    replica_list = put_start_result.value();
    EXPECT_EQ(replica_list.size(), kReplicaCnt);
    for (size_t i = 0; i < kReplicaCnt; i++) {
        EXPECT_EQ(ReplicaStatus::PROCESSING, replica_list[i].status);
    }

    // Put key_1 again, should fail because the key exists.
    put_start_result = service_->PutStart(client_id, key_1, TenantId::Default(),
                                          slice_length, config);
    EXPECT_FALSE(put_start_result.has_value());
    EXPECT_EQ(put_start_result.error(), ErrorCode::OBJECT_ALREADY_EXISTS);

    // Wait for a while until the put-start expired.
    for (size_t i = 0; i <= master_config.put_start_discard_timeout_sec; i++) {
        for (auto& context : contexts) {
            auto result = service_->Ping(context.client_id);
            EXPECT_TRUE(result.has_value());
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // Put key_1 again, should success because the old one has expired and will
    // be discarded by this put.
    put_start_result = service_->PutStart(client_id, key_1, TenantId::Default(),
                                          slice_length, config);
    EXPECT_TRUE(put_start_result.has_value());
    replica_list = put_start_result.value();
    EXPECT_EQ(replica_list.size(), kReplicaCnt);
    for (size_t i = 0; i < kReplicaCnt; i++) {
        EXPECT_EQ(ReplicaStatus::PROCESSING, replica_list[i].status);
    }

    // Complete key_1.
    auto put_end_result = service_->PutEnd(
        client_id, key_1, TenantId::Default(), ReplicaType::MEMORY);
    EXPECT_TRUE(put_end_result.has_value());

    // Protect key_1 from eviction.
    auto get_result = service_->GetReplicaList(key_1, TenantId::Default());
    EXPECT_TRUE(get_result.has_value());

    // Put key_2, should fail because the key_1 occupied 12MB (6MB processing,
    // 6MB discarded but not yet released) on each segment.
    put_start_result = service_->PutStart(client_id, key_2, TenantId::Default(),
                                          slice_length, config);
    EXPECT_FALSE(put_start_result.has_value());
    EXPECT_EQ(put_start_result.error(), ErrorCode::NO_AVAILABLE_HANDLE);

    // Wait for a while until the discarded replicas are released.
    for (size_t i = 0; i <= master_config.put_start_release_timeout_sec -
                                master_config.put_start_discard_timeout_sec;
         i++) {
        for (auto& context : contexts) {
            auto result = service_->Ping(context.client_id);
            EXPECT_TRUE(result.has_value());
        }
        // Protect key_1 from eviction.
        auto get_result = service_->GetReplicaList(key_1, TenantId::Default());
        EXPECT_TRUE(get_result.has_value());
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // Put key_2 again, should success because the discarded replica has been
    // released.
    put_start_result = service_->PutStart(client_id, key_2, TenantId::Default(),
                                          slice_length, config);
    EXPECT_TRUE(put_start_result.has_value());
    replica_list = put_start_result.value();
    EXPECT_EQ(replica_list.size(), kReplicaCnt);
    for (size_t i = 0; i < kReplicaCnt; i++) {
        EXPECT_EQ(ReplicaStatus::PROCESSING, replica_list[i].status);
    }

    // Wait for a while until key_2 can be discarded and released.
    for (size_t i = 0; i <= master_config.put_start_release_timeout_sec; i++) {
        for (auto& context : contexts) {
            auto result = service_->Ping(context.client_id);
            EXPECT_TRUE(result.has_value());
        }
        // Protect key_1 from eviction.
        auto get_result = service_->GetReplicaList(key_1, TenantId::Default());
        EXPECT_TRUE(get_result.has_value());
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // Put key_2 again, should fail because eviction has not been triggered. And
    // this PutStart should trigger the eviction.
    put_start_result = service_->PutStart(client_id, key_2, TenantId::Default(),
                                          slice_length, config);
    EXPECT_FALSE(put_start_result.has_value());
    EXPECT_EQ(put_start_result.error(), ErrorCode::NO_AVAILABLE_HANDLE);

    // Wait a moment for the eviction to complete.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Put key_2 again, should success because the previous one has been
    // discarded and released.
    put_start_result = service_->PutStart(client_id, key_2, TenantId::Default(),
                                          slice_length, config);
    EXPECT_TRUE(put_start_result.has_value());
    replica_list = put_start_result.value();
    EXPECT_EQ(replica_list.size(), kReplicaCnt);
    for (size_t i = 0; i < kReplicaCnt; i++) {
        EXPECT_EQ(ReplicaStatus::PROCESSING, replica_list[i].status);
    }

    // Complete key_2.
    put_end_result = service_->PutEnd(client_id, key_2, TenantId::Default(),
                                      ReplicaType::MEMORY);
    EXPECT_TRUE(put_end_result.has_value());
}

}  // namespace mooncake::test

#include "fixture.h"

namespace mooncake::test {
TEST_F(MasterServiceSnapshotTest, MountUnmountSegmentWithOffsetAllocator) {
    auto service_config = MasterServiceConfig::builder()
                              .set_memory_allocator(BufferAllocatorType::OFFSET)
                              .build();
    service_.reset(new MasterService(service_config));
    auto segment = MakeSegment();
    UUID client_id = generate_uuid();
    const auto original_base = segment.base;
    const auto original_size = segment.size;

    // Test invalid parameters.
    // Invalid buffer address (0).
    segment.base = 0;
    segment.size = original_size;
    auto mount_result1 = service_->MountSegment(segment, client_id);
    EXPECT_FALSE(mount_result1.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, mount_result1.error());

    // Invalid segment size (0).
    segment.base = original_base;
    segment.size = 0;
    auto mount_result2 = service_->MountSegment(segment, client_id);
    EXPECT_FALSE(mount_result2.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, mount_result2.error());

    // Test normal mount operation.
    segment.base = original_base;
    segment.size = original_size;
    auto mount_result5 = service_->MountSegment(segment, client_id);
    EXPECT_TRUE(mount_result5.has_value());

    // Test mounting the same segment again (idempotent request should succeed).
    auto mount_result6 = service_->MountSegment(segment, client_id);
    EXPECT_TRUE(mount_result6.has_value());

    // Test unmounting the segment.
    auto unmount_result1 = service_->UnmountSegment(segment.id, client_id);
    EXPECT_TRUE(unmount_result1.has_value());

    // Test unmounting the same segment again (idempotent request should
    // succeed).
    auto unmount_result2 = service_->UnmountSegment(segment.id, client_id);
    EXPECT_TRUE(unmount_result2.has_value());

    // Test unmounting a non-existent segment (idempotent request should
    // succeed).
    UUID non_existent_id = generate_uuid();
    auto unmount_result3 = service_->UnmountSegment(non_existent_id, client_id);
    EXPECT_TRUE(unmount_result3.has_value());

    // Test remounting after unmount.
    auto mount_result7 = service_->MountSegment(segment, client_id);
    EXPECT_TRUE(mount_result7.has_value());
    auto unmount_result4 = service_->UnmountSegment(segment.id, client_id);
    EXPECT_TRUE(unmount_result4.has_value());
}

TEST_F(MasterServiceSnapshotTest, RandomMountUnmountSegment) {
    service_.reset(new MasterService());
    // Define a constant buffer address for the segment.
    constexpr size_t kBufferAddress = 0x300000000;
    // Define the name of the test segment.
    std::string segment_name = "test_random_segment";
    UUID segment_id = generate_uuid();
    UUID client_id = generate_uuid();
    size_t times = 10;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(1, 10);
    while (times--) {
        int random_number = dis(gen);
        // Define the size of the segment (16MB).
        size_t kSegmentSize = 1024 * 1024 * 16 * random_number;

        auto segment = MakeSegment(segment_name, kBufferAddress, kSegmentSize);
        segment.id = segment_id;

        // Test remounting after unmount.
        auto mount_result = service_->MountSegment(segment, client_id);
        EXPECT_TRUE(mount_result.has_value());
        auto unmount_result = service_->UnmountSegment(segment.id, client_id);
        EXPECT_TRUE(unmount_result.has_value());
    }
}

TEST_F(MasterServiceSnapshotTest, ConcurrentMountUnmount) {
    service_.reset(new MasterService());
    constexpr size_t num_threads = 4;
    constexpr size_t iterations = 100;
    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};

    // Launch multiple threads to mount/unmount segments concurrently
    for (size_t i = 0; i < num_threads; i++) {
        threads.emplace_back([i, &success_count, this]() {
            auto segment =
                MakeSegment("segment_" + std::to_string(i),
                            0x300000000 + i * 0x10000000, 16 * 1024 * 1024);
            UUID client_id = generate_uuid();

            for (size_t j = 0; j < iterations; j++) {
                auto mount_result = service_->MountSegment(segment, client_id);
                if (mount_result.has_value()) {
                    auto unmount_result =
                        service_->UnmountSegment(segment.id, client_id);
                    EXPECT_TRUE(unmount_result.has_value());
                    success_count++;
                }
            }
        });
    }

    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }

    // Verify that some mount/unmount operations succeeded
    EXPECT_GT(success_count, 0);
}

TEST_F(MasterServiceSnapshotTest, UnmountSegmentImmediateCleanup) {
    service_.reset(new MasterService());

    // Mount two segments for testing
    constexpr size_t buffer1 = 0x300000000;
    constexpr size_t buffer2 = 0x400000000;
    constexpr size_t size = 1024 * 1024 * 16;

    auto segment1 = MakeSegment("segment1", buffer1, size);
    auto segment2 = MakeSegment("segment2", buffer2, size);
    UUID client_id = generate_uuid();
    auto mount_result1 = service_->MountSegment(segment1, client_id);
    ASSERT_TRUE(mount_result1.has_value());
    auto mount_result2 = service_->MountSegment(segment2, client_id);
    ASSERT_TRUE(mount_result2.has_value());

    // Create two objects in the two segments
    std::string key1 =
        GenerateKeyForSegment(client_id, service_, segment1.name);
    std::string key2 =
        GenerateKeyForSegment(client_id, service_, segment2.name);
    uint64_t slice_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;

    // Unmount segment1
    auto unmount_result1 = service_->UnmountSegment(segment1.id, client_id);
    ASSERT_TRUE(unmount_result1.has_value());
    // Umount will remove all objects in the segment, include the key1
    ASSERT_EQ(1, service_->GetKeyCount());
    // Verify objects in segment1 is gone
    auto get_result1 = service_->GetReplicaList(key1, "default");
    ASSERT_FALSE(get_result1.has_value());
    ASSERT_EQ(ErrorCode::OBJECT_NOT_FOUND, get_result1.error());

    // Verify objects in segment2 is still there
    auto get_result2 = service_->GetReplicaList(key2, "default");
    ASSERT_TRUE(get_result2.has_value());

    // Verify put key1 will put into segment2 rather than segment1
    auto put_start_result =
        service_->PutStart(client_id, key1, "default", slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    replica_list = put_start_result.value();
    auto put_end_result =
        service_->PutEnd(client_id, key1, "default", ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());
    auto get_result3 = service_->GetReplicaList(key1, "default");
    ASSERT_TRUE(get_result3.has_value());
    auto retrieved = get_result3.value();
    ASSERT_EQ(replica_list[0]
                  .get_memory_descriptor()
                  .buffer_descriptor.transport_endpoint_,
              segment2.name);
}

TEST_F(MasterServiceSnapshotTest, ReadableAfterPartialUnmountWithReplication) {
    service_.reset(new MasterService());

    // Mount two large segments
    constexpr size_t buffer1 = 0x300000000;
    constexpr size_t buffer2 = 0x400000000;
    constexpr size_t segment_size = 1024 * 1024 * 64;  // 64MB
    constexpr size_t object_size = 1024 * 1024;        // 1MB

    auto segment1 = MakeSegment("segment1", buffer1, segment_size);
    auto segment2 = MakeSegment("segment2", buffer2, segment_size);
    UUID client_id = generate_uuid();
    auto mount_result1 = service_->MountSegment(segment1, client_id);
    ASSERT_TRUE(mount_result1.has_value());
    auto mount_result2 = service_->MountSegment(segment2, client_id);
    ASSERT_TRUE(mount_result2.has_value());

    // Put a key with 2 replicas
    std::string key = "replicated_key";
    uint64_t slice_length = object_size;
    ReplicateConfig config;
    config.replica_num = 2;

    auto put_start_result =
        service_->PutStart(client_id, key, "default", slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    ASSERT_EQ(2u, put_start_result->size());
    ASSERT_TRUE(service_->PutEnd(client_id, key, "default", ReplicaType::MEMORY)
                    .has_value());

    // Verify two replicas exist and they are on distinct segments
    auto get_result = service_->GetReplicaList(key, "default");
    ASSERT_TRUE(get_result.has_value());
    auto replicas = get_result.value().replicas;
    ASSERT_EQ(2u, replicas.size());
    std::unordered_set<std::string> seg_names;
    for (const auto& rep : replicas) {
        ASSERT_EQ(ReplicaStatus::COMPLETE, rep.status);
        const auto& mem = rep.get_memory_descriptor();
        ASSERT_EQ(slice_length, mem.buffer_descriptor.size_);
        seg_names.insert(mem.buffer_descriptor.transport_endpoint_);
    }
    ASSERT_EQ(2u, seg_names.size())
        << "Replicas should be on different segments";

    // Unmount one segment
    ASSERT_TRUE(service_->UnmountSegment(segment1.id, client_id).has_value());

    // Key should still be readable via the remaining replica
    auto get_after_unmount = service_->GetReplicaList(key, "default");
    ASSERT_TRUE(get_after_unmount.has_value())
        << "Object should remain accessible with surviving replica";
}

TEST_F(MasterServiceSnapshotTest, UnmountSegmentPerformance) {
    service_.reset(new MasterService());
    constexpr size_t kBufferAddress = 0x300000000;
    constexpr size_t kSegmentSize = 1024 * 1024 * 256;  // 256MB
    std::string segment_name = "perf_test_segment";
    auto segment = MakeSegment(segment_name, kBufferAddress, kSegmentSize);
    UUID client_id = generate_uuid();

    // Mount a segment for testing
    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    // Create 10000 keys for testing
    constexpr int kNumKeys = 1000;
    std::vector<std::string> keys;
    keys.reserve(kNumKeys);

    auto start = std::chrono::steady_clock::now();

    // Create `kNumKeys` keys
    for (int i = 0; i < kNumKeys; ++i) {
        std::string key =
            GenerateKeyForSegment(client_id, service_, segment_name);
        keys.push_back(key);
    }

    auto create_end = std::chrono::steady_clock::now();

    // Execute unmount operation and record operation time
    auto unmount_start = std::chrono::steady_clock::now();
    auto unmount_result = service_->UnmountSegment(segment.id, client_id);
    EXPECT_TRUE(unmount_result.has_value());
    auto unmount_end = std::chrono::steady_clock::now();

    auto unmount_duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(unmount_end -
                                                              unmount_start);

    // Unmount operation should be very fast, so we set 1s limit
    EXPECT_LE(unmount_duration.count(), 1000)
        << "Unmount operation took " << unmount_duration.count()
        << "ms which exceeds 1 second limit";

    // Verify all keys are gone
    for (const auto& key : keys) {
        auto get_result = service_->GetReplicaList(key, "default");
        EXPECT_FALSE(get_result.has_value());
        EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, get_result.error());
    }

    // Output performance report
    auto total_create_duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(create_end -
                                                              start);
    std::cout << "\nPerformance Metrics:\n"
              << "Keys created: " << kNumKeys << "\n"
              << "Creation time: " << total_create_duration.count() << "ms\n"
              << "Unmount time: " << unmount_duration.count() << "ms\n";
}

TEST_F(MasterServiceSnapshotTest, ReplicaSegmentsAreUnique) {
    service_.reset(new MasterService());
    const UUID client_id = generate_uuid();

    // Mount 20 segments, each 16MB and slab-aligned
    constexpr size_t kBaseAddr = 0x300000000;
    constexpr size_t kSegmentSize = 1024 * 1024 * 16;  // 16MB
    for (int i = 0; i < 20; ++i) {
        [[maybe_unused]] const auto context = PrepareSimpleSegment(
            *service_, "segment_" + std::to_string(i),
            kBaseAddr + static_cast<size_t>(i) * kSegmentSize, kSegmentSize);
    }

    // Object with 16 slices of ~1MB and replication factor 10
    const std::string key = "replica_uniqueness_test_key";
    uint64_t slice_length = 1024 * 1024 - 16;
    ReplicateConfig config;
    config.replica_num = 10;

    auto put_start_result =
        service_->PutStart(client_id, key, "default", slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto replica_list_local = put_start_result.value();
    ASSERT_EQ(config.replica_num, replica_list_local.size());

    // Segment names across replicas must be unique
    std::unordered_set<std::string> segment_names;
    for (const auto& replica : replica_list_local) {
        ASSERT_TRUE(replica.is_memory_replica());
        const auto& mem = replica.get_memory_descriptor();
        ASSERT_EQ(slice_length, mem.buffer_descriptor.size_);
        segment_names.insert(mem.buffer_descriptor.transport_endpoint_);
    }
    EXPECT_EQ(segment_names.size(), config.replica_num)
        << "Duplicate segment found";

    ASSERT_TRUE(service_->PutEnd(client_id, key, "default", ReplicaType::MEMORY)
                    .has_value());
}

TEST_F(MasterServiceSnapshotTest, ReplicationFactorTwoWithSingleSegment) {
    service_.reset(new MasterService());
    const UUID client_id = generate_uuid();

    // Mount a single 16MB segment
    constexpr size_t kBaseAddr = 0x300000000;
    constexpr size_t kSegmentSize = 1024 * 1024 * 16;  // 16MB
    [[maybe_unused]] const auto context = PrepareSimpleSegment(
        *service_, "single_segment", kBaseAddr, kSegmentSize);

    // Request replication factor 2 with a single 1KB slice
    // With best-effort semantics, should succeed with 1 replica
    const std::string key = "replication_factor_two_single_segment";
    uint64_t slice_length = 1024;
    ReplicateConfig config;
    config.replica_num = 2;

    auto put_start_result =
        service_->PutStart(client_id, key, "default", slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto replicas = put_start_result.value();

    // Should get 1 replica instead of the requested 2 (best-effort)
    EXPECT_EQ(1u, replicas.size());
    EXPECT_TRUE(replicas[0].is_memory_replica());

    // Verify the replica is properly allocated on the single segment
    auto mem_desc = replicas[0].get_memory_descriptor();
    EXPECT_EQ("single_segment", mem_desc.buffer_descriptor.transport_endpoint_);
    EXPECT_EQ(1024u, mem_desc.buffer_descriptor.size_);
}

TEST_F(MasterServiceSnapshotTest, BatchExistKeyTest) {
    service_.reset(new MasterService());
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

TEST_F(MasterServiceSnapshotTest, BatchQueryIpTest) {
    service_.reset(new MasterService());
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

TEST_F(MasterServiceSnapshotTest, BatchQueryIpMultipleSegmentsTest) {
    service_.reset(new MasterService());
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

TEST_F(MasterServiceSnapshotTest, BatchQueryIpEmptyClientIdTest) {
    service_.reset(new MasterService());

    // Test with empty client_ids list
    std::vector<UUID> empty_client_ids;
    auto query_result = service_->BatchQueryIp(empty_client_ids);

    ASSERT_TRUE(query_result.has_value());
    const auto& results = query_result.value();
    EXPECT_TRUE(results.empty())
        << "Empty client_ids should return empty results";
}

TEST_F(MasterServiceSnapshotTest,
       BatchQueryIpMultipleSegmentsEmptyTeEndpointTest) {
    service_.reset(new MasterService());
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

TEST_F(MasterServiceSnapshotTest, BatchQueryIpBracketedIpv6Test) {
    service_.reset(new MasterService());
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

TEST_F(MasterServiceSnapshotTest, BatchQueryIpLinkLocalIpv6WithScopeTest) {
    service_.reset(new MasterService());
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

TEST_F(MasterServiceSnapshotTest, BatchQueryIpIpv6NoPortTest) {
    service_.reset(new MasterService());
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

TEST_F(MasterServiceSnapshotTest, BatchQueryIpMixedIpv4AndIpv6Test) {
    service_.reset(new MasterService());
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

TEST_F(MasterServiceSnapshotTest, ConcurrentMountLocalDiskSegment) {
    MasterServiceConfig config;
    config.enable_offload = true;
    service_.reset(new MasterService(config));

    constexpr size_t num_threads = 100;
    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};

    // Launch multiple threads to mount local disk segments concurrently
    for (size_t i = 0; i < num_threads; i++) {
        threads.emplace_back([this, i, &success_count]() {
            UUID client_id = generate_uuid();
            auto mount_result =
                service_->MountLocalDiskSegment(client_id, true);
            ASSERT_TRUE(mount_result.has_value());
            ++success_count;
        });
    }

    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }

    // Verify that some mount/unmount operations succeeded
    EXPECT_GT(success_count, 0);
}

TEST_F(MasterServiceSnapshotTest, BatchReplicaClearAllSegments) {
    const uint64_t kv_lease_ttl = 50;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    service_.reset(new MasterService(service_config));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    // Create multiple objects
    std::vector<std::string> keys;
    const int num_objects = 5;
    for (int i = 0; i < num_objects; ++i) {
        std::string key = "batch_clear_key_" + std::to_string(i);
        keys.push_back(key);
        uint64_t value_length = 1024;
        ReplicateConfig config;
        config.replica_num = 1;
        auto put_start_result =
            service_->PutStart(client_id, key, "default", value_length, config);
        ASSERT_TRUE(put_start_result.has_value());
        auto put_end_result =
            service_->PutEnd(client_id, key, "default", ReplicaType::MEMORY);
        ASSERT_TRUE(put_end_result.has_value());
    }

    // Verify objects exist
    for (const auto& key : keys) {
        auto exist_result = service_->ExistKey(key, "default");
        ASSERT_TRUE(exist_result.has_value());
        ASSERT_TRUE(exist_result.value());
    }

    // Wait for lease to expire
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl + 10));

    // Clear all replicas (empty segment_name means clear all segments)
    auto clear_result = service_->BatchReplicaClear(keys, client_id, "");
    ASSERT_TRUE(clear_result.has_value());

    const auto& cleared_keys = clear_result.value();
    ASSERT_EQ(num_objects, cleared_keys.size()) << "All keys should be cleared";

    // Verify objects are removed
    for (const auto& key : keys) {
        auto exist_result = service_->ExistKey(key, "default");
        ASSERT_TRUE(exist_result.has_value());
        ASSERT_FALSE(exist_result.value())
            << "Key " << key << " should be removed";
    }
}

TEST_F(MasterServiceSnapshotTest, BatchReplicaClearSpecificSegment) {
    // 1. Setup: Control the lease time
    const uint64_t kv_lease_ttl = 200;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    service_.reset(new MasterService(service_config));
    const UUID client_id = generate_uuid();

    // 2. Setup: Mount segments
    Segment segment1 = MakeSegment("segment1", 0x300000000, 1024 * 1024 * 16);
    Segment segment2 = MakeSegment("segment2", 0x400000000, 1024 * 1024 * 16);
    ASSERT_TRUE(service_->MountSegment(segment1, client_id).has_value());
    ASSERT_TRUE(service_->MountSegment(segment2, client_id).has_value());

    // 3. Setup: Create the object on segment1 using preferred_segment
    std::string key = "segment_specific_key";
    std::string segment_name = segment1.name;
    uint64_t value_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment =
        segment_name;  // Ensure object is placed on segment1
    auto put_start_result =
        service_->PutStart(client_id, key, "default", value_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto put_end_result =
        service_->PutEnd(client_id, key, "default", ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    // 4. Wait for lease to expire and verify it's actually expired
    // PutEnd calls GrantLease(0, ...) which sets lease_timeout to now.
    // Due to clock precision and timing, we need to ensure the lease is
    // actually expired before calling BatchReplicaClear.
    // Use a small delay and then poll to ensure lease is expired.
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    // Poll until lease is expired (with timeout to avoid infinite loop)
    const auto timeout = std::chrono::seconds(5);
    const auto start_time = std::chrono::steady_clock::now();
    bool lease_expired = false;
    std::vector<std::string> keys = {key};
    tl::expected<std::vector<std::string>, ErrorCode> clear_result;

    while (std::chrono::steady_clock::now() - start_time < timeout) {
        // Try to clear - if it succeeds, lease is expired
        clear_result =
            service_->BatchReplicaClear(keys, client_id, segment_name);
        ASSERT_TRUE(clear_result.has_value());

        if (clear_result.value().size() == 1) {
            lease_expired = true;
            break;
        }

        // Lease not expired yet, wait a bit and retry
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    ASSERT_TRUE(lease_expired) << "Lease did not expire within timeout period";

    // 5. Verify the key was cleared
    const auto& cleared_keys = clear_result.value();
    ASSERT_EQ(1u, cleared_keys.size()) << "Key should be cleared";

    auto exist_result = service_->ExistKey(key, "default");
    ASSERT_TRUE(exist_result.has_value());
    ASSERT_FALSE(exist_result.value())
        << "Key should be removed after being cleared.";
}

}  // namespace mooncake::test

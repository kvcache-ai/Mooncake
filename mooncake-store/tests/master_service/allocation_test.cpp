#include "fixture.h"

namespace mooncake::test {
TEST_F(MasterServiceTest, MountUnmountSegmentWithCachelibAllocator) {
    // Create a MasterService instance for testing.
    auto service_config =
        MasterServiceConfig::builder()
            .set_memory_allocator(BufferAllocatorType::CACHELIB)
            .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
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

    // Base is not aligned
    segment.base = original_base + 1;
    segment.size = original_size;
    auto mount_result3 = service_->MountSegment(segment, client_id);
    EXPECT_FALSE(mount_result3.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, mount_result3.error());

    // Size is not aligned
    segment.base = original_base;
    segment.size = original_size + 1;
    auto mount_result4 = service_->MountSegment(segment, client_id);
    EXPECT_FALSE(mount_result4.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, mount_result4.error());

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

TEST_F(MasterServiceTest, MountUnmountSegmentWithOffsetAllocator) {
    // Create a MasterService instance for testing.
    auto service_config = MasterServiceConfig::builder()
                              .set_memory_allocator(BufferAllocatorType::OFFSET)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
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

TEST_F(MasterServiceTest, RandomMountUnmountSegment) {
    // Create a MasterService instance for testing.
    std::unique_ptr<MasterService> service_(new MasterService());
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

TEST_F(MasterServiceTest, ConcurrentMountUnmount) {
    std::unique_ptr<MasterService> service_(new MasterService());
    constexpr size_t num_threads = 4;
    constexpr size_t iterations = 100;
    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};

    // Launch multiple threads to mount/unmount segments concurrently
    for (size_t i = 0; i < num_threads; i++) {
        threads.emplace_back([&service_, i, &success_count, this]() {
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

TEST_F(MasterServiceTest, PutWithPreferredSegment) {
    // For backward compatibility, test the deprecated single preferred_segment
    std::unique_ptr<MasterService> service_(new MasterService());
    const UUID client_id = generate_uuid();

    // Mount 3 segments, each 16MB
    constexpr size_t kBaseAddr = 0x300000000;
    constexpr size_t kSegmentSize = 1024 * 1024 * 16;  // 16MB
    for (int i = 0; i < 3; ++i) {
        [[maybe_unused]] const auto context = PrepareSimpleSegment(
            *service_, "segment_" + std::to_string(i),
            kBaseAddr + static_cast<size_t>(i) * kSegmentSize, kSegmentSize);
    }

    // Prepare preferred segments
    std::string preferred_segment = "segment_1";

    // Test PutStart with multiple preferred segments
    std::string key = "test_key";
    uint64_t value_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = preferred_segment;

    auto put_start_result =
        service_->PutStart(client_id, key, "default", value_length, config);
    EXPECT_TRUE(put_start_result.has_value());
    replica_list = put_start_result.value();
    EXPECT_EQ(1, replica_list.size());
    EXPECT_EQ(ReplicaStatus::PROCESSING, replica_list[0].status);
    const auto& mem_desc = replica_list[0].get_memory_descriptor();
    EXPECT_EQ(preferred_segment,
              mem_desc.buffer_descriptor.transport_endpoint_);

    // Complete the Put operation
    auto put_end_result =
        service_->PutEnd(client_id, key, "default", ReplicaType::MEMORY);
    EXPECT_TRUE(put_end_result.has_value());
}

TEST_F(MasterServiceTest, PutWithPreferredSegments) {
    std::unique_ptr<MasterService> service_(new MasterService());
    const UUID client_id = generate_uuid();

    // Mount 3 segments, each 16MB
    constexpr size_t kBaseAddr = 0x300000000;
    constexpr size_t kSegmentSize = 1024 * 1024 * 16;  // 16MB
    for (int i = 0; i < 3; ++i) {
        [[maybe_unused]] const auto context = PrepareSimpleSegment(
            *service_, "segment_" + std::to_string(i),
            kBaseAddr + static_cast<size_t>(i) * kSegmentSize, kSegmentSize);
    }

    // Prepare preferred segments
    std::vector<std::string> preferred_segments = {"segment_0", "segment_1"};

    // Test PutStart with multiple preferred segments
    std::string key = "test_key";
    uint64_t value_length = 1024;
    ReplicateConfig config;
    config.replica_num = 2;
    config.preferred_segments = preferred_segments;

    auto put_start_result =
        service_->PutStart(client_id, key, "default", value_length, config);
    EXPECT_TRUE(put_start_result.has_value());
    replica_list = put_start_result.value();
    EXPECT_EQ(2, replica_list.size());
    EXPECT_EQ(ReplicaStatus::PROCESSING, replica_list[0].status);
    EXPECT_EQ(ReplicaStatus::PROCESSING, replica_list[1].status);
    const auto& mem_desc1 = replica_list[0].get_memory_descriptor();
    const auto& mem_desc2 = replica_list[1].get_memory_descriptor();
    std::unordered_set<std::string> used_segments = {
        mem_desc1.buffer_descriptor.transport_endpoint_,
        mem_desc2.buffer_descriptor.transport_endpoint_};
    EXPECT_TRUE(used_segments.find("segment_0") != used_segments.end());
    EXPECT_TRUE(used_segments.find("segment_1") != used_segments.end());

    // Complete the Put operation
    auto put_end_result =
        service_->PutEnd(client_id, key, "default", ReplicaType::MEMORY);
    EXPECT_TRUE(put_end_result.has_value());
}

TEST_F(MasterServiceTest,
       ResolveMooncakeHostIdUsesLocalHostnameAndRejectsLoopback) {
    {
        EXPECT_EQ(ResolveMooncakeHostId("hostB:5000"), "hostB");
        EXPECT_EQ(ResolveMooncakeHostId("hostB:5001"), "hostB");
        EXPECT_EQ(ResolveMooncakeHostId("[2001:db8::1]:5000"), "2001:db8::1");
        EXPECT_TRUE(ResolveMooncakeHostId("localhost:5000").empty());
        EXPECT_TRUE(ResolveMooncakeHostId("127.0.0.1:5000").empty());
        EXPECT_TRUE(ResolveMooncakeHostId("0.0.0.0:5000").empty());
        EXPECT_TRUE(ResolveMooncakeHostId("::1").empty());
        EXPECT_TRUE(ResolveMooncakeHostId("[::1]:5000").empty());
        EXPECT_TRUE(ResolveMooncakeHostId("::").empty());
        EXPECT_TRUE(ResolveMooncakeHostId("[::]").empty());
        EXPECT_TRUE(ResolveMooncakeHostId("[::]:5000").empty());
    }
}

TEST_F(MasterServiceTest, MasterConfigParsesLocalFirstStrategy) {
    MasterConfig config{};
    config.allocation_strategy = "local_first";

    WrappedMasterServiceConfig wrapped_config(config, 0);
    MasterServiceConfig service_config(wrapped_config);
    EXPECT_EQ(service_config.allocation_strategy_type,
              AllocationStrategyType::LOCAL_FIRST);
}

TEST_F(MasterServiceTest, LocalFirstPutPrefersWriterHost) {
    auto service_config =
        MasterServiceConfig::builder()
            .set_allocation_strategy_type(AllocationStrategyType::LOCAL_FIRST)
            .build();
    MasterService service(service_config);
    const UUID writer_client_id = generate_uuid();

    [[maybe_unused]] const auto host0 = PrepareSimpleSegment(
        service, "segment_host0", 0x300000000, kDefaultSegmentSize, "host0");
    [[maybe_unused]] const auto host1 = PrepareSimpleSegment(
        service, "segment_host1", 0x400000000, kDefaultSegmentSize, "host1");

    ReplicateConfig config;
    config.replica_num = 1;
    config.host_id = "host1";

    auto put_start = service.PutStart(writer_client_id, "local_first_key",
                                      "default", 1024, config);
    ASSERT_TRUE(put_start.has_value());
    ASSERT_EQ(put_start->size(), 1u);
    EXPECT_EQ((*put_start)[0]
                  .get_memory_descriptor()
                  .buffer_descriptor.transport_endpoint_,
              "segment_host1");
}

TEST_F(MasterServiceTest, LocalFirstPutFallsBackToNextOrderedHost) {
    auto service_config =
        MasterServiceConfig::builder()
            .set_allocation_strategy_type(AllocationStrategyType::LOCAL_FIRST)
            .build();
    MasterService service(service_config);
    const UUID writer_client_id = generate_uuid();

    [[maybe_unused]] const auto host0 = PrepareSimpleSegment(
        service, "segment_host0", 0x300000000, kDefaultSegmentSize, "host0");
    [[maybe_unused]] const auto host2 = PrepareSimpleSegment(
        service, "segment_host2", 0x400000000, kDefaultSegmentSize, "host2");

    ReplicateConfig config;
    config.replica_num = 1;
    config.host_id = "host1";

    auto put_start = service.PutStart(writer_client_id, "ordered_fallback_key",
                                      "default", 1024, config);
    ASSERT_TRUE(put_start.has_value());
    ASSERT_EQ(put_start->size(), 1u);
    EXPECT_EQ((*put_start)[0]
                  .get_memory_descriptor()
                  .buffer_descriptor.transport_endpoint_,
              "segment_host2");
}

TEST_F(MasterServiceTest, LocalFirstPutFallsBackWhenLocalSegmentIsFull) {
    auto service_config =
        MasterServiceConfig::builder()
            .set_allocation_strategy_type(AllocationStrategyType::LOCAL_FIRST)
            .build();
    MasterService service(service_config);
    const UUID writer_client_id = generate_uuid();

    [[maybe_unused]] const auto local = PrepareSimpleSegment(
        service, "segment_host1", 0x300000000, 1024, "host1");
    [[maybe_unused]] const auto remote = PrepareSimpleSegment(
        service, "segment_host2", 0x400000000, kDefaultSegmentSize, "host2");

    ReplicateConfig config;
    config.replica_num = 1;
    config.host_id = "host1";

    auto fill_start = service.PutStart(writer_client_id, "fill_local_segment",
                                       "default", 1024, config);
    ASSERT_TRUE(fill_start.has_value());
    ASSERT_EQ(fill_start->size(), 1u);
    EXPECT_EQ((*fill_start)[0]
                  .get_memory_descriptor()
                  .buffer_descriptor.transport_endpoint_,
              "segment_host1");
    ASSERT_TRUE(service
                    .PutEnd(writer_client_id, "fill_local_segment", "default",
                            ReplicaType::MEMORY)
                    .has_value());

    auto fallback_start = service.PutStart(
        writer_client_id, "fallback_after_local_full", "default", 1, config);
    ASSERT_TRUE(fallback_start.has_value());
    ASSERT_EQ(fallback_start->size(), 1u);
    EXPECT_EQ((*fallback_start)[0]
                  .get_memory_descriptor()
                  .buffer_descriptor.transport_endpoint_,
              "segment_host2");
}

TEST_F(MasterServiceTest, ExplicitPreferredSegmentOverridesLocalFirst) {
    auto service_config =
        MasterServiceConfig::builder()
            .set_allocation_strategy_type(AllocationStrategyType::LOCAL_FIRST)
            .build();
    MasterService service(service_config);
    const UUID writer_client_id = generate_uuid();

    [[maybe_unused]] const auto host0 = PrepareSimpleSegment(
        service, "segment_host0", 0x300000000, kDefaultSegmentSize, "host0");
    [[maybe_unused]] const auto host1 = PrepareSimpleSegment(
        service, "segment_host1", 0x400000000, kDefaultSegmentSize, "host1");

    ReplicateConfig config;
    config.replica_num = 1;
    config.host_id = "host1";
    config.preferred_segment = "segment_host0";

    auto put_start = service.PutStart(
        writer_client_id, "explicit_preferred_key", "default", 1024, config);
    ASSERT_TRUE(put_start.has_value());
    ASSERT_EQ(put_start->size(), 1u);
    EXPECT_EQ((*put_start)[0]
                  .get_memory_descriptor()
                  .buffer_descriptor.transport_endpoint_,
              "segment_host0");
}

TEST_F(MasterServiceTest, ExplicitPreferredSegmentFallsBackToLocalFirst) {
    auto service_config =
        MasterServiceConfig::builder()
            .set_allocation_strategy_type(AllocationStrategyType::LOCAL_FIRST)
            .build();
    MasterService service(service_config);
    const UUID writer_client_id = generate_uuid();

    [[maybe_unused]] const auto preferred = PrepareSimpleSegment(
        service, "segment_host0", 0x300000000, 1024, "host0");
    [[maybe_unused]] const auto local = PrepareSimpleSegment(
        service, "segment_host1", 0x400000000, kDefaultSegmentSize, "host1");

    ReplicateConfig config;
    config.replica_num = 1;
    config.host_id = "host1";
    config.preferred_segment = "segment_host0";

    auto fill_start = service.PutStart(writer_client_id, "fill_preferred",
                                       "default", 1024, config);
    ASSERT_TRUE(fill_start.has_value());
    ASSERT_EQ(fill_start->size(), 1u);
    EXPECT_EQ((*fill_start)[0]
                  .get_memory_descriptor()
                  .buffer_descriptor.transport_endpoint_,
              "segment_host0");
    ASSERT_TRUE(service
                    .PutEnd(writer_client_id, "fill_preferred", "default",
                            ReplicaType::MEMORY)
                    .has_value());

    auto fallback_start =
        service.PutStart(writer_client_id, "fallback_after_preferred_full",
                         "default", 1, config);
    ASSERT_TRUE(fallback_start.has_value());
    ASSERT_EQ(fallback_start->size(), 1u);
    EXPECT_EQ((*fallback_start)[0]
                  .get_memory_descriptor()
                  .buffer_descriptor.transport_endpoint_,
              "segment_host1");
}

TEST_F(MasterServiceTest, UnmountSegmentImmediateCleanup) {
    std::unique_ptr<MasterService> service_(new MasterService());

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

TEST_F(MasterServiceTest, ReadableAfterPartialUnmountWithReplication) {
    std::unique_ptr<MasterService> service_(new MasterService());

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

TEST_F(MasterServiceTest, UnmountSegmentPerformance) {
    std::unique_ptr<MasterService> service_(new MasterService());
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

TEST_F(MasterServiceTest, ReplicaSegmentsAreUnique) {
    std::unique_ptr<MasterService> service_(new MasterService());
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

TEST_F(MasterServiceTest, ReplicationFactorTwoWithSingleSegment) {
    std::unique_ptr<MasterService> service_(new MasterService());
    const UUID client_id = generate_uuid();

    // Mount a single 16MB segment
    constexpr size_t kBaseAddr = 0x300000000;
    constexpr size_t kSegmentSize = 1024 * 1024 * 16;  // 16MB
    [[maybe_unused]] const auto context = PrepareSimpleSegment(
        *service_, "single_segment", kBaseAddr, kSegmentSize);

    // Request replication factor 2 with a single 1KB slice.
    // With best-effort semantics, should succeed with 1 replica.
    const std::string key = "replication_factor_two_single_segment";
    uint64_t slice_length = 1024;
    ReplicateConfig config;
    config.replica_num = 2;

    auto put_start_result =
        service_->PutStart(client_id, key, "default", slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto replicas = put_start_result.value();

    // Should get 1 replica instead of the requested 2 (best-effort).
    EXPECT_EQ(1u, replicas.size());
    EXPECT_TRUE(replicas[0].is_memory_replica());

    // Verify the replica is properly allocated on the single segment.
    auto mem_desc = replicas[0].get_memory_descriptor();
    EXPECT_EQ("single_segment", mem_desc.buffer_descriptor.transport_endpoint_);
    EXPECT_EQ(1024u, mem_desc.buffer_descriptor.size_);
}

TEST_F(MasterServiceTest, ConcurrentMountLocalDiskSegment) {
    MasterServiceConfig config;
    config.enable_offload = true;
    std::unique_ptr<MasterService> service_(new MasterService(config));

    constexpr size_t num_threads = 100;
    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};

    // Launch multiple threads to mount local disk segments concurrently
    for (size_t i = 0; i < num_threads; i++) {
        threads.emplace_back([&service_, i, &success_count, this]() {
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

TEST_F(MasterServiceTest, BatchReplicaClearAllSegments) {
    const uint64_t kv_lease_ttl = 50;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
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

TEST_F(MasterServiceTest, BatchReplicaClearSpecificSegment) {
    // 1. Setup: Control the lease time
    const uint64_t kv_lease_ttl = 200;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
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

TEST_F(MasterServiceTest, GracefulUnmountSegment_SetsCorrectStatus) {
    std::unique_ptr<MasterService> service_(new MasterService());
    auto segment = MakeSegment("graceful_test_segment");
    UUID client_id = generate_uuid();

    // Mount segment
    ASSERT_TRUE(service_->MountSegment(segment, client_id).has_value());

    // Verify initial status
    auto status_before = service_->QuerySegmentStatus(segment.name);
    ASSERT_TRUE(status_before.has_value());
    EXPECT_EQ(status_before.value(), SegmentStatus::OK);

    // Graceful unmount with 1 second grace period
    auto graceful_result = service_->GracefulUnmountSegment(
        segment.id, client_id, /*grace_period_ms=*/1000);
    ASSERT_TRUE(graceful_result.has_value())
        << "Graceful unmount should succeed: "
        << toString(graceful_result.error());

    // Verify status is GRACEFULLY_UNMOUNTING
    auto status_after = service_->QuerySegmentStatus(segment.name);
    ASSERT_TRUE(status_after.has_value());
    EXPECT_EQ(status_after.value(), SegmentStatus::GRACEFULLY_UNMOUNTING);

    // Wait for timer to expire and clean up
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
}

TEST_F(MasterServiceTest, GracefulUnmountSegment_RejectWrongClient) {
    std::unique_ptr<MasterService> service_(new MasterService());
    auto segment = MakeSegment("graceful_owner_segment");
    UUID owner_client = generate_uuid();
    UUID wrong_client = generate_uuid();

    ASSERT_TRUE(service_->MountSegment(segment, owner_client).has_value());

    // Wrong client trying to graceful unmount should fail
    auto graceful_result = service_->GracefulUnmountSegment(
        segment.id, wrong_client, /*grace_period_ms=*/1000);
    ASSERT_FALSE(graceful_result.has_value());
    EXPECT_EQ(graceful_result.error(), ErrorCode::SEGMENT_NOT_FOUND);

    // Owner should still be able to unmount
    auto owner_result = service_->GracefulUnmountSegment(
        segment.id, owner_client, /*grace_period_ms=*/1000);
    EXPECT_TRUE(owner_result.has_value());

    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
}

TEST_F(MasterServiceTest, GracefulUnmountSegment_Idempotent) {
    std::unique_ptr<MasterService> service_(new MasterService());
    auto segment = MakeSegment("graceful_idempotent_segment");
    UUID client_id = generate_uuid();

    ASSERT_TRUE(service_->MountSegment(segment, client_id).has_value());

    // First graceful unmount should succeed
    auto result1 = service_->GracefulUnmountSegment(segment.id, client_id,
                                                    /*grace_period_ms=*/1000);
    ASSERT_TRUE(result1.has_value());

    // Second graceful unmount on the same segment should also succeed
    // (idempotent)
    auto result2 = service_->GracefulUnmountSegment(segment.id, client_id,
                                                    /*grace_period_ms=*/1000);
    EXPECT_TRUE(result2.has_value()) << "Graceful unmount should be idempotent";

    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
}

TEST_F(MasterServiceTest, GracefulUnmountSegment_TimerExpiresAndUnmounts) {
    std::unique_ptr<MasterService> service_(new MasterService());
    auto segment = MakeSegment("graceful_timer_segment");
    UUID client_id = generate_uuid();

    ASSERT_TRUE(service_->MountSegment(segment, client_id).has_value());

    // Graceful unmount with a short grace period (50ms)
    auto graceful_result = service_->GracefulUnmountSegment(
        segment.id, client_id, /*grace_period_ms=*/50);
    ASSERT_TRUE(graceful_result.has_value());

    // Immediately after graceful unmount, segment should still exist
    auto status_immediate = service_->QuerySegmentStatus(segment.name);
    ASSERT_TRUE(status_immediate.has_value());
    EXPECT_EQ(status_immediate.value(), SegmentStatus::GRACEFULLY_UNMOUNTING);

    // Wait for timer to expire and unmount (give some margin)
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    // After timer expires, segment should be fully unmounted (UNDEFINED or
    // error)
    auto status_after = service_->QuerySegmentStatus(segment.name);
    // Segment may be UNDEFINED (not found) or return an error
    EXPECT_TRUE(!status_after.has_value() ||
                status_after.value() == SegmentStatus::UNDEFINED)
        << "Segment should be unmounted after timer expires, got status="
        << (status_after.has_value() ? static_cast<int>(status_after.value())
                                     : -1);
}

TEST_F(MasterServiceTest,
       GracefulUnmountSegment_QueryStatusByIdWithReusedName) {
    std::unique_ptr<MasterService> service_(new MasterService());
    auto old_segment = MakeSegment("graceful_reused_name_segment");
    auto new_segment = MakeSegment(old_segment.name, /*base=*/0x400000000);
    UUID client_id = generate_uuid();

    ASSERT_TRUE(service_->MountSegment(old_segment, client_id).has_value());
    ASSERT_TRUE(service_
                    ->GracefulUnmountSegment(old_segment.id, client_id,
                                             /*grace_period_ms=*/50)
                    .has_value());
    ASSERT_TRUE(service_->MountSegment(new_segment, client_id).has_value());

    auto old_status = service_->QuerySegmentStatusById(old_segment.id);
    ASSERT_TRUE(old_status.has_value());
    EXPECT_EQ(old_status.value(), SegmentStatus::GRACEFULLY_UNMOUNTING);

    auto new_status = service_->QuerySegmentStatusById(new_segment.id);
    ASSERT_TRUE(new_status.has_value());
    EXPECT_EQ(new_status.value(), SegmentStatus::OK);

    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    EXPECT_FALSE(service_->QuerySegmentStatusById(old_segment.id).has_value());
    ASSERT_TRUE(service_->QuerySegmentStatusById(new_segment.id).has_value());

    auto status_by_name = service_->QuerySegmentStatus(new_segment.name);
    ASSERT_TRUE(status_by_name.has_value());
    EXPECT_EQ(status_by_name.value(), SegmentStatus::OK);
}

TEST_F(MasterServiceTest, GracefulUnmountSegment_EarlierTimerPreemptsWait) {
    std::unique_ptr<MasterService> service_(new MasterService());
    auto long_segment = MakeSegment("graceful_long_timer_segment");
    auto short_segment =
        MakeSegment("graceful_short_timer_segment", /*base=*/0x400000000);
    UUID client_id = generate_uuid();

    ASSERT_TRUE(service_->MountSegment(long_segment, client_id).has_value());
    ASSERT_TRUE(service_->MountSegment(short_segment, client_id).has_value());

    ASSERT_TRUE(service_
                    ->GracefulUnmountSegment(long_segment.id, client_id,
                                             /*grace_period_ms=*/1000)
                    .has_value());
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    ASSERT_TRUE(service_
                    ->GracefulUnmountSegment(short_segment.id, client_id,
                                             /*grace_period_ms=*/50)
                    .has_value());

    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    auto short_status = service_->QuerySegmentStatus(short_segment.name);
    EXPECT_TRUE(!short_status.has_value() ||
                short_status.value() == SegmentStatus::UNDEFINED);

    auto long_status = service_->QuerySegmentStatus(long_segment.name);
    ASSERT_TRUE(long_status.has_value());
    EXPECT_EQ(long_status.value(), SegmentStatus::GRACEFULLY_UNMOUNTING);
}

TEST_F(MasterServiceTest, GracefulUnmountSegment_PreventAllocation) {
    std::unique_ptr<MasterService> service_(new MasterService());
    auto segment1 = MakeSegment("graceful_seg1");
    auto segment2 = MakeSegment("graceful_seg2", /*base=*/0x400000000);
    UUID client_id = generate_uuid();

    ASSERT_TRUE(service_->MountSegment(segment1, client_id).has_value());
    ASSERT_TRUE(service_->MountSegment(segment2, client_id).has_value());

    // Put an object on segment1
    std::string key = "test_key_prevent_alloc";
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = segment1.name;

    auto put_start =
        service_->PutStart(client_id, key, "default", 1024, config);
    ASSERT_TRUE(put_start.has_value());
    ASSERT_TRUE(service_->PutEnd(client_id, key, "default", ReplicaType::MEMORY)
                    .has_value());

    // Graceful unmount segment1
    ASSERT_TRUE(service_->GracefulUnmountSegment(segment1.id, client_id, 1000)
                    .has_value());

    // Segment1 status should be GRACEFULLY_UNMOUNTING
    auto status1 = service_->QuerySegmentStatus(segment1.name);
    ASSERT_TRUE(status1.has_value());
    EXPECT_EQ(status1.value(), SegmentStatus::GRACEFULLY_UNMOUNTING);

    // Existing replicas on the graceful segment should remain readable during
    // the grace window.
    auto existing_replicas = service_->GetReplicaList(key, "default");
    ASSERT_TRUE(existing_replicas.has_value());
    ASSERT_EQ(existing_replicas->replicas.size(), 1u);
    EXPECT_EQ(existing_replicas->replicas[0]
                  .get_memory_descriptor()
                  .buffer_descriptor.transport_endpoint_,
              segment1.name);

    // Segment2 status should still be OK
    auto status2 = service_->QuerySegmentStatus(segment2.name);
    ASSERT_TRUE(status2.has_value());
    EXPECT_EQ(status2.value(), SegmentStatus::OK);

    // New put without preferred_segment should succeed on segment2
    std::string key2 = "test_key_after_graceful";
    ReplicateConfig config2;
    config2.replica_num = 1;

    auto put_start2 =
        service_->PutStart(client_id, key2, "default", 1024, config2);
    ASSERT_TRUE(put_start2.has_value());
    auto replicas = put_start2.value();
    ASSERT_EQ(replicas.size(), 1u);
    // Should be placed on segment2, not segment1
    EXPECT_EQ(replicas[0]
                  .get_memory_descriptor()
                  .buffer_descriptor.transport_endpoint_,
              segment2.name);

    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
}

}  // namespace mooncake::test

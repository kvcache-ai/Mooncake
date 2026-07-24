#include "master_service_test_harness.h"

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
    auto put_start_result = service_->PutStart(
        client_id, "test_key_all", TenantId::Default(), 1024, config);
    ASSERT_TRUE(put_start_result.has_value());

    auto put_end_result = service_->PutEnd(
        client_id, "test_key_all", TenantId::Default(), ReplicaType::ALL);
    ASSERT_TRUE(put_end_result.has_value());

    auto get_replica_result =
        service_->GetReplicaList("test_key_all", TenantId::Default());
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
#endif

#ifdef USE_NOF
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
    auto put_start_result = service_->PutStart(
        client_id, "test_key_split", TenantId::Default(), 1024, config);
    ASSERT_TRUE(put_start_result.has_value());

    auto put_end_result = service_->PutEnd(
        client_id, "test_key_split", TenantId::Default(), ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    auto get_replica_result =
        service_->GetReplicaList("test_key_split", TenantId::Default());
    ASSERT_TRUE(get_replica_result.has_value());
    ASSERT_EQ(get_replica_result->replicas.size(), 1u);
    EXPECT_TRUE(get_replica_result->replicas[0].is_memory_replica());
    EXPECT_EQ(get_replica_result->replicas[0].status, ReplicaStatus::COMPLETE);

    auto put_revoke_result = service_->PutRevoke(
        client_id, "test_key_split", TenantId::Default(), ReplicaType::NOF_SSD);
    ASSERT_TRUE(put_revoke_result.has_value());

    auto final_replica_result =
        service_->GetReplicaList("test_key_split", TenantId::Default());
    ASSERT_TRUE(final_replica_result.has_value());
    ASSERT_EQ(final_replica_result->replicas.size(), 1u);
    EXPECT_TRUE(final_replica_result->replicas[0].is_memory_replica());
    EXPECT_EQ(final_replica_result->replicas[0].status,
              ReplicaStatus::COMPLETE);
}
#endif

#ifdef USE_NOF
TEST_F(MasterServiceTest, PutStartOnePlusOneAllowsSingleAllocatedReplica) {
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto mem_context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    ReplicateConfig config;
    config.replica_num = 1;
    config.nof_replica_num = 1;
    auto put_start_result = service_->PutStart(
        client_id, "test_key_one_plus_one", TenantId::Default(), 1024, config);
    ASSERT_TRUE(put_start_result.has_value());
    ASSERT_EQ(put_start_result->size(), 1u);
    EXPECT_TRUE(put_start_result->front().is_memory_replica());
}
#endif

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
        auto get_result = service_->GetReplicaList(key, TenantId::Default());
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

}  // namespace mooncake::test

#include "fixture.h"

namespace mooncake::test {
TEST_F(MasterServiceSSDTest, PutEndBothReplica) {
    auto service_ = CreateMasterServiceWithSSDFeat("/mnt/ssd");

    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 64;
    std::string segment_name = "test_segment";
    Segment segment;
    segment.id = generate_uuid();
    segment.name = segment_name;
    segment.base = buffer;
    segment.size = size;
    segment.te_endpoint = segment.name;
    UUID client_id = generate_uuid();

    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    std::string key = "disk_key";
    uint64_t slice_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;

    auto put_start_result =
        service_->PutStart(client_id, key, "default", slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto replicas = put_start_result.value();
    ASSERT_EQ(2, replicas.size());

    bool has_mem = false, has_disk = false;
    for (const auto& r : replicas) {
        if (r.is_memory_replica()) has_mem = true;
        if (r.is_disk_replica()) has_disk = true;
    }
    EXPECT_TRUE(has_mem);
    EXPECT_TRUE(has_disk);

    auto get_result = service_->GetReplicaList(key, "default");
    ASSERT_FALSE(get_result.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_NOT_READY, get_result.error());

    // PutEnd for both memory and disk
    EXPECT_TRUE(service_->PutEnd(client_id, key, "default", ReplicaType::MEMORY)
                    .has_value());
    EXPECT_TRUE(service_->PutEnd(client_id, key, "default", ReplicaType::DISK)
                    .has_value());

    get_result = service_->GetReplicaList(key, "default");
    ASSERT_TRUE(get_result.has_value());
    EXPECT_EQ(2, get_result.value().replicas.size());

    for (const auto& r : get_result.value().replicas) {
        EXPECT_EQ(ReplicaStatus::COMPLETE, r.status);
    }
}

TEST_F(MasterServiceSSDTest, PutRevokeDiskReplica) {
    auto service_ = CreateMasterServiceWithSSDFeat("/mnt/ssd");

    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 64;
    std::string segment_name = "test_segment";
    Segment segment;
    segment.id = generate_uuid();
    segment.name = segment_name;
    segment.base = buffer;
    segment.size = size;
    segment.te_endpoint = segment.name;
    UUID client_id = generate_uuid();

    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    std::string key = "revoke_key";
    uint64_t slice_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;

    ASSERT_TRUE(
        service_->PutStart(client_id, key, "default", slice_length, config)
            .has_value());
    EXPECT_TRUE(service_->PutEnd(client_id, key, "default", ReplicaType::MEMORY)
                    .has_value());

    auto get_result = service_->GetReplicaList(key, "default");
    ASSERT_TRUE(get_result.has_value());
    EXPECT_EQ(1, get_result.value().replicas.size());
    ASSERT_TRUE(get_result.value().replicas[0].is_memory_replica());

    EXPECT_TRUE(
        service_->PutRevoke(client_id, key, "default", ReplicaType::DISK)
            .has_value());

    get_result = service_->GetReplicaList(key, "default");
    ASSERT_TRUE(get_result.has_value());
    EXPECT_EQ(1, get_result.value().replicas.size());
    ASSERT_TRUE(get_result.value().replicas[0].is_memory_replica());
}

TEST_F(MasterServiceSSDTest, PutRevokeProcessingDiskKeepsSsdTotal) {
    auto service_ = CreateMasterServiceWithSSDFeat("/mnt/ssd");
    auto& metrics = MasterMetricManager::instance();
    using CacheHitStat = MasterMetricManager::CacheHitStat;
    const auto base_stats = metrics.calculate_cache_stats();
    const double base_memory_total = base_stats.at(CacheHitStat::MEMORY_TOTAL);
    const double base_ssd_total = base_stats.at(CacheHitStat::SSD_TOTAL);

    constexpr size_t buffer = 0x310000000;
    constexpr size_t size = 1024 * 1024 * 64;
    Segment segment;
    segment.id = generate_uuid();
    segment.name = "test_segment_revoke_processing_disk";
    segment.base = buffer;
    segment.size = size;
    segment.te_endpoint = segment.name;
    UUID client_id = generate_uuid();

    ASSERT_TRUE(service_->MountSegment(segment, client_id).has_value());

    std::string key = "revoke_processing_disk_metric_key";
    ASSERT_TRUE(
        service_->PutStart(client_id, key, "default", 1024, {.replica_num = 1})
            .has_value());
    EXPECT_TRUE(service_->PutEnd(client_id, key, "default", ReplicaType::MEMORY)
                    .has_value());

    auto stats = metrics.calculate_cache_stats();
    EXPECT_EQ(stats[CacheHitStat::MEMORY_TOTAL], base_memory_total + 1);
    EXPECT_EQ(stats[CacheHitStat::SSD_TOTAL], base_ssd_total);

    EXPECT_TRUE(
        service_->PutRevoke(client_id, key, "default", ReplicaType::DISK)
            .has_value());

    stats = metrics.calculate_cache_stats();
    EXPECT_EQ(stats[CacheHitStat::MEMORY_TOTAL], base_memory_total + 1);
    EXPECT_EQ(stats[CacheHitStat::SSD_TOTAL], base_ssd_total);

    ASSERT_TRUE(service_->Remove(key, "default", /*force=*/true).has_value());
    stats = metrics.calculate_cache_stats();
    EXPECT_EQ(stats[CacheHitStat::MEMORY_TOTAL], base_memory_total);
    EXPECT_EQ(stats[CacheHitStat::SSD_TOTAL], base_ssd_total);
}

TEST_F(MasterServiceSSDTest, PutRevokeMemoryReplica) {
    auto service_ = CreateMasterServiceWithSSDFeat("/mnt/ssd");

    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 64;
    std::string segment_name = "test_segment";
    Segment segment;
    segment.id = generate_uuid();
    segment.name = segment_name;
    segment.base = buffer;
    segment.size = size;
    segment.te_endpoint = segment.name;
    UUID client_id = generate_uuid();

    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    std::string key = "revoke_key";
    uint64_t slice_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;

    ASSERT_TRUE(
        service_->PutStart(client_id, key, "default", slice_length, config)
            .has_value());
    EXPECT_TRUE(
        service_->PutRevoke(client_id, key, "default", ReplicaType::MEMORY)
            .has_value());

    auto get_result = service_->GetReplicaList(key, "default");
    ASSERT_FALSE(get_result.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_NOT_READY, get_result.error());

    EXPECT_TRUE(service_->PutEnd(client_id, key, "default", ReplicaType::DISK)
                    .has_value());
    get_result = service_->GetReplicaList(key, "default");
    ASSERT_TRUE(get_result.has_value());
    EXPECT_EQ(1, get_result.value().replicas.size());
    ASSERT_TRUE(get_result.value().replicas[0].is_disk_replica());
}

TEST_F(MasterServiceSSDTest, PutRevokeBothReplica) {
    auto service_ = CreateMasterServiceWithSSDFeat("/mnt/ssd");

    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 64;
    std::string segment_name = "test_segment";
    Segment segment;
    segment.id = generate_uuid();
    segment.name = segment_name;
    segment.base = buffer;
    segment.size = size;
    segment.te_endpoint = segment.name;
    UUID client_id = generate_uuid();

    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    std::string key = "revoke_key";
    uint64_t slice_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;

    ASSERT_TRUE(
        service_->PutStart(client_id, key, "default", slice_length, config)
            .has_value());
    EXPECT_TRUE(
        service_->PutRevoke(client_id, key, "default", ReplicaType::DISK)
            .has_value());

    auto get_result = service_->GetReplicaList(key, "default");
    ASSERT_FALSE(get_result.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_NOT_READY, get_result.error());

    EXPECT_TRUE(
        service_->PutRevoke(client_id, key, "default", ReplicaType::MEMORY)
            .has_value());
    get_result = service_->GetReplicaList(key, "default");
    ASSERT_FALSE(get_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, get_result.error());
}

TEST_F(MasterServiceSSDTest, RemoveKey) {
    auto service_ = CreateMasterServiceWithSSDFeat("/mnt/ssd");

    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 64;
    std::string segment_name = "test_segment";
    Segment segment;
    segment.id = generate_uuid();
    segment.name = segment_name;
    segment.base = buffer;
    segment.size = size;
    segment.te_endpoint = segment.name;
    UUID client_id = generate_uuid();

    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    std::string key = "remove_key";
    uint64_t slice_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;

    ASSERT_TRUE(
        service_->PutStart(client_id, key, "default", slice_length, config)
            .has_value());
    EXPECT_TRUE(service_->PutEnd(client_id, key, "default", ReplicaType::MEMORY)
                    .has_value());
    EXPECT_TRUE(service_->PutEnd(client_id, key, "default", ReplicaType::DISK)
                    .has_value());

    EXPECT_TRUE(service_->Remove(key, "default").has_value());

    auto get_result = service_->GetReplicaList(key, "default");
    EXPECT_FALSE(get_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, get_result.error());
}

TEST_F(MasterServiceSSDTest, EvictObject) {
    auto service_ = CreateMasterServiceWithSSDFeat("/mnt/ssd");
    // Mount a segment that can hold about 1024 * 16 objects.
    // As the eviction is processed separately for each shard,
    // we need to fill each shard with enough objects to thoroughly
    // test the eviction process.
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 16 * 15;
    constexpr size_t object_size = 1024 * 15;
    std::string segment_name = "test_segment";
    Segment segment;
    segment.id = generate_uuid();
    segment.name = segment_name;
    segment.base = buffer;
    segment.size = size;
    segment.te_endpoint = segment.name;
    UUID client_id = generate_uuid();
    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    // Verify if we can put objects more than the segment can hold
    int success_puts = 0;
    for (int i = 0; i < 1024 * 16 + 50; ++i) {
        std::string key = "test_key" + std::to_string(i);
        uint64_t slice_length = object_size;
        ReplicateConfig config;
        config.replica_num = 1;
        auto put_start_result =
            service_->PutStart(client_id, key, "default", slice_length, config);
        if (put_start_result.has_value()) {
            auto put_end_mem_result = service_->PutEnd(
                client_id, key, "default", ReplicaType::MEMORY);
            auto put_end_disk_result =
                service_->PutEnd(client_id, key, "default", ReplicaType::DISK);
            ASSERT_TRUE(put_end_mem_result.has_value());
            ASSERT_TRUE(put_end_disk_result.has_value());
            success_puts++;
        } else {
            // wait for eviction to work
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }
    ASSERT_GT(success_puts, 1024 * 16);

    // Verify if we can get objects more than the segment can hold
    int success_gets = 0;
    for (int i = 0; i < 1024 * 16 + 50; ++i) {
        std::string key = "test_key" + std::to_string(i);
        auto get_result = service_->GetReplicaList(key, "default");
        if (get_result.has_value()) {
            success_gets++;
        }
    }
    ASSERT_GT(success_gets, 1024 * 16);

    std::this_thread::sleep_for(
        std::chrono::milliseconds(DEFAULT_DEFAULT_KV_LEASE_TTL));
    service_->RemoveAll();
}

TEST_F(MasterServiceSSDTest, EvictDiskReplica_RemovesDiskReplica) {
    auto service_ = CreateMasterServiceWithSSDFeat("/mnt/ssd");

    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 64;
    Segment segment;
    segment.id = generate_uuid();
    segment.name = "test_segment";
    segment.base = buffer;
    segment.size = size;
    segment.te_endpoint = segment.name;
    UUID client_id = generate_uuid();

    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    std::string key = "evict_disk_key";
    auto put_result =
        service_->PutStart(client_id, key, "default", 1024, {.replica_num = 1});
    ASSERT_TRUE(put_result.has_value());

    // Complete both replicas
    EXPECT_TRUE(service_->PutEnd(client_id, key, "default", ReplicaType::MEMORY)
                    .has_value());
    EXPECT_TRUE(service_->PutEnd(client_id, key, "default", ReplicaType::DISK)
                    .has_value());

    // Verify we have 2 replicas (MEM + DISK)
    auto get_result = service_->GetReplicaList(key, "default");
    ASSERT_TRUE(get_result.has_value());
    EXPECT_EQ(2, get_result.value().replicas.size());

    // Evict disk replica
    auto evict_result = service_->EvictDiskReplica(client_id, key, "default",
                                                   ReplicaType::DISK);
    ASSERT_TRUE(evict_result.has_value());

    // Verify only memory replica remains
    get_result = service_->GetReplicaList(key, "default");
    ASSERT_TRUE(get_result.has_value());
    EXPECT_EQ(1, get_result.value().replicas.size());
    EXPECT_TRUE(get_result.value().replicas[0].is_memory_replica());
}

TEST_F(MasterServiceSSDTest, EvictDiskReplica_NonExistentKeyReturnsError) {
    auto service_ = CreateMasterServiceWithSSDFeat("/mnt/ssd");

    UUID client_id = generate_uuid();
    auto evict_result = service_->EvictDiskReplica(
        client_id, "nonexistent_key", "default", ReplicaType::DISK);
    EXPECT_FALSE(evict_result.has_value());
    EXPECT_EQ(evict_result.error(), ErrorCode::OBJECT_NOT_FOUND);
}

TEST_F(MasterServiceSSDTest, EvictDiskReplica_InvalidReplicaTypeReturnsError) {
    auto service_ = CreateMasterServiceWithSSDFeat("/mnt/ssd");

    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 64;
    Segment segment;
    segment.id = generate_uuid();
    segment.name = "test_segment";
    segment.base = buffer;
    segment.size = size;
    segment.te_endpoint = segment.name;
    UUID client_id = generate_uuid();

    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    std::string key = "evict_invalid_type_key";
    auto put_result =
        service_->PutStart(client_id, key, "default", 1024, {.replica_num = 1});
    ASSERT_TRUE(put_result.has_value());
    EXPECT_TRUE(service_->PutEnd(client_id, key, "default", ReplicaType::MEMORY)
                    .has_value());
    EXPECT_TRUE(service_->PutEnd(client_id, key, "default", ReplicaType::DISK)
                    .has_value());

    // Attempting to evict with MEMORY type should fail
    auto evict_result = service_->EvictDiskReplica(client_id, key, "default",
                                                   ReplicaType::MEMORY);
    EXPECT_FALSE(evict_result.has_value());
    EXPECT_EQ(evict_result.error(), ErrorCode::INVALID_PARAMS);
}

}  // namespace mooncake::test

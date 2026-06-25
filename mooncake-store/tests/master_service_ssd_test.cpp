#include "master_service.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <memory>
#include <thread>
#include <vector>

#include "master_metric_manager.h"
#include "types.h"

namespace mooncake::test {

std::unique_ptr<MasterService> CreateMasterServiceWithSSDFeat(
    const std::string& root_fs_dir) {
    return std::make_unique<MasterService>(
        MasterServiceConfig::builder().set_root_fs_dir(root_fs_dir).build());
}

class MasterServiceSSDTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("MasterServiceTest");
        FLAGS_logtostderr = true;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }
};

std::unique_ptr<MasterService> CreateSsdAwareOffloadService() {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.default_kv_lease_ttl = 0;
    config.allocation_strategy_type =
        AllocationStrategyType::SSD_FREE_RATIO_FIRST;
    return std::make_unique<MasterService>(config);
}

void MountMemoryAndLocalDisk(MasterService& service, const UUID& client_id,
                             const std::string& segment_name,
                             size_t base_addr) {
    Segment segment;
    segment.id = generate_uuid();
    segment.name = segment_name;
    segment.base = base_addr;
    segment.size = 64 * 1024 * 1024;
    segment.te_endpoint = segment.name;

    ASSERT_TRUE(service.MountSegment(segment, client_id).has_value());
    ASSERT_TRUE(service.MountLocalDiskSegment(client_id, true).has_value());
    ASSERT_TRUE(service.ReportSsdCapacity(client_id, 1000).has_value());
}

void PutAndOffload(MasterService& service, const UUID& client_id,
                   const std::string& key, int64_t object_size,
                   const std::string& local_disk_endpoint) {
    ReplicateConfig config;
    config.replica_num = 1;

    ASSERT_TRUE(service.PutStart(client_id, key, "default", object_size, config)
                    .has_value());
    ASSERT_TRUE(service.PutEnd(client_id, key, "default", ReplicaType::MEMORY)
                    .has_value());

    StorageObjectMetadata metadata;
    metadata.data_size = object_size;
    metadata.transport_endpoint = local_disk_endpoint;
    OffloadTaskItem task{
        .tenant_id = "default", .key = key, .size = object_size};
    ASSERT_TRUE(service.NotifyOffloadSuccess(client_id, {task}, {metadata})
                    .has_value());
}

void ExpectNextAllocationOnSegment(MasterService& service,
                                   const UUID& client_id,
                                   const std::string& key,
                                   const std::string& expected_segment) {
    ReplicateConfig config;
    config.replica_num = 1;
    auto result = service.PutStart(client_id, key, "default", 64, config);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->size(), 1u);
    ASSERT_TRUE((*result)[0].is_memory_replica());
    EXPECT_EQ((*result)[0]
                  .get_memory_descriptor()
                  .buffer_descriptor.transport_endpoint_,
              expected_segment);
}

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

TEST_F(MasterServiceSSDTest, PutStartExpires) {
    // Reset storage space metrics.
    MasterMetricManager::instance().reset_allocated_mem_size();
    MasterMetricManager::instance().reset_total_mem_capacity();

    MasterServiceConfig master_config;
    master_config.root_fs_dir = "/mnt/ssd";
    master_config.put_start_discard_timeout_sec = 3;
    master_config.put_start_release_timeout_sec = 5;
    std::unique_ptr<MasterService> service_(new MasterService(master_config));

    constexpr size_t kReplicaCnt = 2;  // 1 memory replica + 1 disk replica
    constexpr size_t kBaseAddr = 0x300000000;
    constexpr size_t kSegmentSize = 1024 * 1024 * 16;  // 16MB

    // Mount a segment.
    std::string segment_name = "test_segment";
    Segment segment;
    segment.id = generate_uuid();
    segment.name = segment_name;
    segment.base = kBaseAddr;
    segment.size = kSegmentSize;
    segment.te_endpoint = segment.name;
    auto client_id = generate_uuid();
    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    std::string key = "test_key";
    uint64_t value_length = 16 * 1024 * 1024;  // 16MB
    uint64_t slice_length = value_length;
    ReplicateConfig config;

    auto test_discard_replica = [&](ReplicaType discard_type) {
        const auto reserve_type = discard_type == ReplicaType::MEMORY
                                      ? ReplicaType::DISK
                                      : ReplicaType::MEMORY;

        // Put key, should success.
        auto put_start_result =
            service_->PutStart(client_id, key, "default", slice_length, config);
        EXPECT_TRUE(put_start_result.has_value());
        auto replica_list = put_start_result.value();
        EXPECT_EQ(replica_list.size(), kReplicaCnt);
        for (size_t i = 0; i < kReplicaCnt; i++) {
            EXPECT_EQ(ReplicaStatus::PROCESSING, replica_list[i].status);
        }

        // Complete the reserved replica.
        auto put_end_result =
            service_->PutEnd(client_id, key, "default", reserve_type);
        EXPECT_TRUE(put_end_result.has_value());

        // Wait for a while until the put-start expired.
        for (size_t i = 0; i <= master_config.put_start_discard_timeout_sec;
             i++) {
            // Keep mounted segments alive.
            auto result = service_->Ping(client_id);
            EXPECT_TRUE(result.has_value());
            // Protect the key from eviction.
            auto get_result = service_->GetReplicaList(key, "default");
            EXPECT_TRUE(get_result.has_value());
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        // Put key again, should fail because the object has had an completed
        // replica.
        put_start_result =
            service_->PutStart(client_id, key, "default", slice_length, config);
        EXPECT_FALSE(put_start_result.has_value());
        EXPECT_EQ(put_start_result.error(), ErrorCode::OBJECT_ALREADY_EXISTS);

        // Wait for a while until the discarded replicas are released.
        for (size_t i = 0; i <= master_config.put_start_release_timeout_sec;
             i++) {
            // Keep mounted segments alive.
            auto result = service_->Ping(client_id);
            EXPECT_TRUE(result.has_value());
            // Protect the key from eviction.
            auto get_result = service_->GetReplicaList(key, "default");
            EXPECT_TRUE(get_result.has_value());
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        // Try PutEnd the discarded replica.
        put_end_result =
            service_->PutEnd(client_id, key, "default", discard_type);
        EXPECT_TRUE(put_end_result.has_value());

        // Check that the key has only one replica.
        auto get_result = service_->GetReplicaList(key, "default");
        EXPECT_TRUE(get_result.has_value());
        EXPECT_EQ(get_result.value().replicas.size(), 1);
        if (reserve_type == ReplicaType::MEMORY) {
            EXPECT_TRUE(get_result.value().replicas[0].is_memory_replica());
        } else {
            EXPECT_TRUE(get_result.value().replicas[0].is_disk_replica());
        }

        // Wait for the key to expire.
        for (size_t i = 0; i <= DEFAULT_DEFAULT_KV_LEASE_TTL / 1000; i++) {
            auto result = service_->Ping(client_id);
            EXPECT_TRUE(result.has_value());
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        service_->RemoveAll();
    };

    test_discard_replica(ReplicaType::DISK);
    test_discard_replica(ReplicaType::MEMORY);
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

TEST_F(MasterServiceSSDTest, RemoveDecrementsCacheTotalMetrics) {
    auto service_ = CreateMasterServiceWithSSDFeat("/mnt/ssd");
    auto& metrics = MasterMetricManager::instance();
    using CacheHitStat = MasterMetricManager::CacheHitStat;
    const auto base_stats = metrics.calculate_cache_stats();
    const double base_memory_total = base_stats.at(CacheHitStat::MEMORY_TOTAL);
    const double base_ssd_total = base_stats.at(CacheHitStat::SSD_TOTAL);

    constexpr size_t buffer = 0x320000000;
    constexpr size_t size = 1024 * 1024 * 64;
    Segment segment;
    segment.id = generate_uuid();
    segment.name = "test_segment_remove_metrics";
    segment.base = buffer;
    segment.size = size;
    segment.te_endpoint = segment.name;
    UUID client_id = generate_uuid();

    ASSERT_TRUE(service_->MountSegment(segment, client_id).has_value());

    std::string key = "remove_cache_total_metric_key";
    ASSERT_TRUE(
        service_->PutStart(client_id, key, "default", 1024, {.replica_num = 1})
            .has_value());
    EXPECT_TRUE(service_->PutEnd(client_id, key, "default", ReplicaType::MEMORY)
                    .has_value());
    EXPECT_TRUE(service_->PutEnd(client_id, key, "default", ReplicaType::DISK)
                    .has_value());

    auto stats = metrics.calculate_cache_stats();
    EXPECT_EQ(stats[CacheHitStat::MEMORY_TOTAL], base_memory_total + 1);
    EXPECT_EQ(stats[CacheHitStat::SSD_TOTAL], base_ssd_total + 1);

    ASSERT_TRUE(service_->Remove(key, "default", /*force=*/true).has_value());

    stats = metrics.calculate_cache_stats();
    EXPECT_EQ(stats[CacheHitStat::MEMORY_TOTAL], base_memory_total);
    EXPECT_EQ(stats[CacheHitStat::SSD_TOTAL], base_ssd_total);
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

TEST_F(MasterServiceSSDTest, RemoveReleasesLocalDiskUsageTracking) {
    auto service = CreateSsdAwareOffloadService();
    UUID client1 = generate_uuid();
    UUID client2 = generate_uuid();
    const std::string segment1 = "ssd_remove_segment_1";
    const std::string segment2 = "ssd_remove_segment_2";
    MountMemoryAndLocalDisk(*service, client1, segment1, 0x400000000);
    MountMemoryAndLocalDisk(*service, client2, segment2, 0x500000000);

    PutAndOffload(*service, client1, "ssd_remove_released", 800, segment1);
    PutAndOffload(*service, client2, "ssd_remove_baseline", 100, segment2);

    ASSERT_TRUE(service->Remove("ssd_remove_released", "default").has_value());

    ExpectNextAllocationOnSegment(*service, client1, "ssd_remove_probe",
                                  segment1);
}

TEST_F(MasterServiceSSDTest,
       BatchReplicaClearAllSegmentsReleasesLocalDiskUsageTracking) {
    auto service = CreateSsdAwareOffloadService();
    UUID client1 = generate_uuid();
    UUID client2 = generate_uuid();
    const std::string segment1 = "ssd_clear_segment_1";
    const std::string segment2 = "ssd_clear_segment_2";
    MountMemoryAndLocalDisk(*service, client1, segment1, 0x600000000);
    MountMemoryAndLocalDisk(*service, client2, segment2, 0x700000000);

    PutAndOffload(*service, client1, "ssd_clear_released", 800, segment1);
    PutAndOffload(*service, client2, "ssd_clear_baseline", 100, segment2);

    auto clear_result =
        service->BatchReplicaClear({"ssd_clear_released"}, client1, "");
    ASSERT_TRUE(clear_result.has_value());
    ASSERT_EQ(clear_result->size(), 1u);
    EXPECT_EQ((*clear_result)[0], "ssd_clear_released");

    ExpectNextAllocationOnSegment(*service, client1, "ssd_clear_probe",
                                  segment1);
}

// Test that after offloading more data to segment1, the next allocation prefers
// segment2 which has more SSD free space.
TEST_F(MasterServiceSSDTest, SsdFreeRatioFirstPrefersFresherSsdAfterOffload) {
    auto service = CreateSsdAwareOffloadService();
    UUID client1 = generate_uuid();
    UUID client2 = generate_uuid();
    const std::string segment1 = "ssd_fresher_seg_1";
    const std::string segment2 = "ssd_fresher_seg_2";
    // Each segment reports total SSD capacity = 1000 bytes
    MountMemoryAndLocalDisk(*service, client1, segment1, 0x800000000);
    MountMemoryAndLocalDisk(*service, client2, segment2, 0x900000000);

    // Offload 800 bytes to segment1 → ssd_used[seg1]=800, free=20%
    PutAndOffload(*service, client1, "ssd_fresher_heavy", 800, segment1);
    // Offload 100 bytes to segment2 → ssd_used[seg2]=100, free=90%
    PutAndOffload(*service, client2, "ssd_fresher_light", 100, segment2);

    // segment2 has higher SSD free ratio → allocation should prefer segment2
    ExpectNextAllocationOnSegment(*service, client2, "ssd_fresher_probe",
                                  segment2);
}

// Test that EvictDiskReplica decrements ssd_used_bytes so that the evicted
// segment becomes preferred again for the next allocation.
TEST_F(MasterServiceSSDTest, EvictDiskReplicaDecrementsLocalDiskUsageTracking) {
    auto service = CreateSsdAwareOffloadService();
    UUID client1 = generate_uuid();
    UUID client2 = generate_uuid();
    const std::string segment1 = "ssd_evict_dec_seg_1";
    const std::string segment2 = "ssd_evict_dec_seg_2";
    MountMemoryAndLocalDisk(*service, client1, segment1, 0xa00000000);
    MountMemoryAndLocalDisk(*service, client2, segment2, 0xb00000000);

    // Offload 800 bytes to segment1 → ssd_used[seg1]=800 (20% free)
    PutAndOffload(*service, client1, "ssd_evict_dec_heavy", 800, segment1);
    // Offload 100 bytes to segment2 → ssd_used[seg2]=100 (90% free)
    PutAndOffload(*service, client2, "ssd_evict_dec_light", 100, segment2);

    // segment2 has more SSD free space → should be preferred
    ExpectNextAllocationOnSegment(*service, client2, "ssd_evict_dec_probe1",
                                  segment2);

    // Evict the LOCAL_DISK replica of the heavy object from segment1.
    // NotifyOffloadSuccess creates a LOCAL_DISK replica (not DISK).
    // This decrements ssd_used[seg1] by 800 → ssd_used[seg1]=0 (100% free)
    auto evict_result = service->EvictDiskReplica(
        client1, "ssd_evict_dec_heavy", "default", ReplicaType::LOCAL_DISK);
    ASSERT_TRUE(evict_result.has_value());

    // After eviction: segment1 has 100% free, segment2 has 90% free
    // → segment1 should now be preferred
    ExpectNextAllocationOnSegment(*service, client1, "ssd_evict_dec_probe2",
                                  segment1);
}

// Real-path performance comparison: MasterService PutStart throughput for
// three configurations:
//   (A) RANDOM, no offload        — baseline, original behavior
//   (B) RANDOM, with offload      — isolates disk-replica creation overhead
//   (C) SSD_FREE_RATIO_FIRST, with offload — adds SSD metrics lock + sorting
//
// Comparing A→B separates the cost of mounting LocalDisk segments.
// Comparing B→C isolates the pure SSD-ranking strategy overhead.
//
// Each round: PutStart → PutEnd(MEMORY) (timed) → Remove (not timed).
TEST_F(MasterServiceSSDTest,
       SsdFreeRatioFirstVsRandomMasterServicePerformance) {
    constexpr int kNumNodes = 32;
    constexpr size_t kSegmentSize = 8 * 1024 * 1024;  // 8 MiB each
    constexpr size_t kSliceSize = 512;  // 512 B – focus on strategy cost
    constexpr int kWarmupRounds = 50;
    constexpr int kBenchmarkRounds = 300;

    // Build a MasterService with kNumNodes segments. with_ssd=true also
    // mounts LocalDisk and reports varied SSD capacity per node.
    auto buildAndMount =
        [&](AllocationStrategyType strategy, bool with_ssd, size_t base_start,
            const std::string& tag) -> std::unique_ptr<MasterService> {
        MasterServiceConfig config;
        config.enable_offload = with_ssd;
        config.default_kv_lease_ttl = 10000;
        config.allocation_strategy_type = strategy;
        auto svc = std::make_unique<MasterService>(config);

        for (int i = 0; i < kNumNodes; i++) {
            UUID cid = generate_uuid();
            Segment seg;
            seg.id = generate_uuid();
            seg.name = "ms_perf_" + std::to_string(i) + "_" + tag;
            seg.base = base_start + static_cast<size_t>(i) * kSegmentSize;
            seg.size = kSegmentSize;
            seg.te_endpoint = seg.name;
            (void)svc->MountSegment(seg, cid);
            if (with_ssd) {
                (void)svc->MountLocalDiskSegment(cid, true);
                // Vary total SSD capacity so nodes have distinct free ratios
                (void)svc->ReportSsdCapacity(
                    cid, static_cast<int64_t>(1024 * 1024) * (i + 1));
            }
        }
        return svc;
    };

    // Measure kRounds of PutStart + PutEnd(MEMORY). Remove is called after
    // timing to free allocator space without inflating the measurement.
    auto runBenchmark = [&](MasterService& svc, const std::string& key_pfx,
                            int rounds) -> std::chrono::microseconds {
        const UUID writer = generate_uuid();
        ReplicateConfig cfg;
        cfg.replica_num = 1;
        std::chrono::microseconds total{0};

        for (int i = 0; i < rounds; i++) {
            const std::string key = key_pfx + std::to_string(i);
            auto t0 = std::chrono::steady_clock::now();
            (void)svc.PutStart(writer, key, "default", kSliceSize, cfg);
            (void)svc.PutEnd(writer, key, "default", ReplicaType::MEMORY);
            total += std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - t0);
            (void)svc.Remove(key, "default", /*force=*/true);
        }
        return total;
    };

    // (A) RANDOM, no offload – baseline
    auto svc_a = buildAndMount(AllocationStrategyType::RANDOM, false,
                               0xc00000000ULL, "A");
    (void)runBenchmark(*svc_a, "ms_A_wu_", kWarmupRounds);
    auto elapsed_a = runBenchmark(*svc_a, "ms_A_bm_", kBenchmarkRounds);

    // (B) RANDOM, with offload – quantify disk-replica overhead alone
    auto svc_b = buildAndMount(AllocationStrategyType::RANDOM, true,
                               0xd00000000ULL, "B");
    (void)runBenchmark(*svc_b, "ms_B_wu_", kWarmupRounds);
    auto elapsed_b = runBenchmark(*svc_b, "ms_B_bm_", kBenchmarkRounds);

    // (C) SSD_FREE_RATIO_FIRST, with offload – full new feature
    auto svc_c = buildAndMount(AllocationStrategyType::SSD_FREE_RATIO_FIRST,
                               true, 0xe00000000ULL, "C");
    (void)runBenchmark(*svc_c, "ms_C_wu_", kWarmupRounds);
    auto elapsed_c = runBenchmark(*svc_c, "ms_C_bm_", kBenchmarkRounds);

    auto us_per_op = [&](std::chrono::microseconds us) {
        return static_cast<double>(us.count()) / kBenchmarkRounds;
    };
    double ratio_b_a =
        static_cast<double>(elapsed_b.count()) / elapsed_a.count();
    double ratio_c_b =
        static_cast<double>(elapsed_c.count()) / elapsed_b.count();
    double ratio_c_a =
        static_cast<double>(elapsed_c.count()) / elapsed_a.count();

    std::cout
        << "\n=== MasterService Real-Path Performance (PutStart+PutEnd) ===\n"
        << "Nodes: " << kNumNodes << " | Slice: " << kSliceSize
        << " B | Rounds: " << kBenchmarkRounds << "\n\n"
        << "  (A) RANDOM, offload=OFF (baseline):         " << elapsed_a.count()
        << " us  |  " << std::fixed << std::setprecision(3)
        << us_per_op(elapsed_a) << " us/op\n"
        << "  (B) RANDOM, offload=ON  (disk replica cost):" << elapsed_b.count()
        << " us  |  " << us_per_op(elapsed_b) << " us/op  ["
        << std::setprecision(2) << ratio_b_a << "x vs A]\n"
        << "  (C) SSD_FREE_RATIO_FIRST, offload=ON:       " << elapsed_c.count()
        << " us  |  " << us_per_op(elapsed_c) << " us/op  [" << ratio_c_b
        << "x vs B]\n\n"
        << "  A→B  disk-replica overhead:   " << std::setprecision(1)
        << (ratio_b_a - 1.0) * 100.0 << "%\n"
        << "  B→C  SSD-ranking overhead:    " << (ratio_c_b - 1.0) * 100.0
        << "%\n"
        << "  A→C  total overhead vs origin:" << (ratio_c_a - 1.0) * 100.0
        << "%\n\n";
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

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

std::unique_ptr<MasterService> CreateSsdRejoinService(
    int64_t client_live_ttl_sec, int64_t local_disk_rejoin_grace_sec) {
    MasterServiceConfig config;
    config.enable_offload = true;
    config.default_kv_lease_ttl = 0;
    config.client_live_ttl_sec = client_live_ttl_sec;
    config.local_disk_rejoin_grace_sec = local_disk_rejoin_grace_sec;
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

void MountMemoryAndLocalDisk(MasterService& service, const UUID& client_id,
                             const std::string& segment_name, size_t base_addr,
                             const std::string& local_disk_segment_id,
                             const std::string& transport_endpoint) {
    Segment segment;
    segment.id = generate_uuid();
    segment.name = segment_name;
    segment.base = base_addr;
    segment.size = 64 * 1024 * 1024;
    segment.te_endpoint = segment.name;

    ASSERT_TRUE(service.MountSegment(segment, client_id).has_value());
    ASSERT_TRUE(service
                    .MountLocalDiskSegment(client_id, true,
                                           local_disk_segment_id,
                                           transport_endpoint)
                    .has_value());
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

void WaitForReplicaNotReady(MasterService& service, const std::string& key,
                            std::chrono::seconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        auto result = service.GetReplicaList(key, "default");
        if (!result.has_value() &&
            result.error() == ErrorCode::REPLICA_IS_NOT_READY) {
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    auto result = service.GetReplicaList(key, "default");
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::REPLICA_IS_NOT_READY);
}

void WaitForObjectNotFound(MasterService& service, const std::string& key,
                           std::chrono::seconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        auto result = service.GetReplicaList(key, "default");
        if (!result.has_value() &&
            result.error() == ErrorCode::OBJECT_NOT_FOUND) {
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    auto result = service.GetReplicaList(key, "default");
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::OBJECT_NOT_FOUND);
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

// Evicting a LOCAL_DISK replica via EvictDiskReplica must decrement
// file_cache_nums_ even when the object still has a MEMORY replica (so
// accessor.Erase() does not run). Without SyncCacheTotalAccounting in the
// LOCAL_DISK eviction branch, the gauge would stay over-counted.
TEST_F(MasterServiceSSDTest, EvictDiskReplicaDecrementsFileCacheNums) {
    auto& metrics = MasterMetricManager::instance();
    auto service = CreateSsdAwareOffloadService();
    UUID client_id = generate_uuid();
    const std::string segment = "ssd_evict_cache_total_segment";
    MountMemoryAndLocalDisk(*service, client_id, segment, 0xc00000000);

    const int64_t baseline = metrics.get_file_cache_nums();
    const int64_t baseline_mem = metrics.get_mem_cache_nums();

    PutAndOffload(*service, client_id, "ssd_evict_cache_total_key", 128,
                  segment);

    // After offload: file_cache_nums_ increments by 1 (LOCAL_DISK replica),
    // mem_cache_nums_ also increments by 1 (MEMORY replica from PutEnd).
    EXPECT_EQ(metrics.get_file_cache_nums(), baseline + 1);
    EXPECT_EQ(metrics.get_mem_cache_nums(), baseline_mem + 1);

    auto evict_result =
        service->EvictDiskReplica(client_id, "ssd_evict_cache_total_key",
                                  "default", ReplicaType::LOCAL_DISK);
    ASSERT_TRUE(evict_result.has_value());

    // After evicting LOCAL_DISK: file_cache_nums_ returns to baseline,
    // mem_cache_nums_ unchanged (MEMORY replica still present).
    EXPECT_EQ(metrics.get_file_cache_nums(), baseline);
    EXPECT_EQ(metrics.get_mem_cache_nums(), baseline_mem + 1);
}

TEST_F(MasterServiceSSDTest,
       LocalDiskReplicaDisconnectInvisibleThenReattachesBySegmentId) {
    auto service = CreateSsdRejoinService(/*client_live_ttl_sec=*/1,
                                          /*local_disk_rejoin_grace_sec=*/10);
    const UUID old_client = generate_uuid();
    const UUID new_client = generate_uuid();
    const std::string marker = "local-disk-segment-rejoin-ok";
    const std::string key = "ssd_rejoin_invisible_then_ok";

    const std::string old_segment = "ssd_rejoin_old";
    MountMemoryAndLocalDisk(*service, old_client, old_segment, 0xf00000000ULL,
                            marker, "old_endpoint");
    PutAndOffload(*service, old_client, key, 256, "old_endpoint");

    auto clear_result =
        service->BatchReplicaClear({key}, old_client, old_segment);
    ASSERT_TRUE(clear_result.has_value());
    ASSERT_EQ(clear_result->size(), 1u);
    EXPECT_EQ((*clear_result)[0], key);
    auto before_expiry = service->GetReplicaList(key, "default");
    ASSERT_TRUE(before_expiry.has_value());
    ASSERT_EQ(before_expiry->replicas.size(), 1u);
    ASSERT_TRUE(before_expiry->replicas[0].is_local_disk_replica());

    WaitForReplicaNotReady(*service, key, std::chrono::seconds(4));

    MountMemoryAndLocalDisk(*service, new_client, "ssd_rejoin_new",
                            0x1000000000ULL, marker, "new_endpoint");
    auto reattached = service->GetReplicaList(key, "default");
    ASSERT_TRUE(reattached.has_value());
    ASSERT_EQ(reattached->replicas.size(), 1u);
    const auto descriptor = reattached->replicas[0].get_local_disk_descriptor();
    EXPECT_EQ(descriptor.client_id, new_client);
    EXPECT_EQ(descriptor.transport_endpoint, "new_endpoint");
    EXPECT_EQ(descriptor.local_disk_segment_id, marker);
}

TEST_F(MasterServiceSSDTest,
       LocalDiskReplicaGraceExpiryRemovesAndMarkerMismatchCannotReattach) {
    auto service = CreateSsdRejoinService(/*client_live_ttl_sec=*/1,
                                          /*local_disk_rejoin_grace_sec=*/1);
    const UUID old_client = generate_uuid();
    const UUID mismatch_client = generate_uuid();
    const UUID same_marker_late_client = generate_uuid();
    const std::string marker = "local-disk-segment-expire";
    const std::string key = "ssd_rejoin_expire_no_reattach";

    const std::string old_segment = "ssd_expire_old";
    MountMemoryAndLocalDisk(*service, old_client, old_segment, 0x1100000000ULL,
                            marker, "old_endpoint");
    PutAndOffload(*service, old_client, key, 256, "old_endpoint");
    auto clear_result =
        service->BatchReplicaClear({key}, old_client, old_segment);
    ASSERT_TRUE(clear_result.has_value());
    ASSERT_EQ(clear_result->size(), 1u);
    EXPECT_EQ((*clear_result)[0], key);

    WaitForReplicaNotReady(*service, key, std::chrono::seconds(4));

    MountMemoryAndLocalDisk(*service, mismatch_client, "ssd_expire_mismatch",
                            0x1200000000ULL, "different-marker",
                            "mismatch_endpoint");
    auto mismatch_result = service->GetReplicaList(key, "default");
    ASSERT_FALSE(mismatch_result.has_value());
    EXPECT_EQ(mismatch_result.error(), ErrorCode::REPLICA_IS_NOT_READY);

    WaitForObjectNotFound(*service, key, std::chrono::seconds(4));

    MountMemoryAndLocalDisk(*service, same_marker_late_client,
                            "ssd_expire_same_marker_late", 0x1300000000ULL,
                            marker, "late_endpoint");
    auto late_result = service->GetReplicaList(key, "default");
    ASSERT_FALSE(late_result.has_value());
    EXPECT_EQ(late_result.error(), ErrorCode::OBJECT_NOT_FOUND);
}

TEST_F(MasterServiceSSDTest,
       LocalDiskReplicaReattachesReplacementBeforeOldClientExpires) {
    auto service = CreateSsdRejoinService(/*client_live_ttl_sec=*/5,
                                          /*local_disk_rejoin_grace_sec=*/10);
    const UUID old_client = generate_uuid();
    const UUID new_client = generate_uuid();
    const std::string marker = "local-disk-segment-before-ttl";
    const std::string key = "ssd_rejoin_before_ttl";

    MountMemoryAndLocalDisk(*service, old_client, "ssd_before_ttl_old",
                            0x1900000000ULL, marker, "old_endpoint");
    PutAndOffload(*service, old_client, key, 256, "old_endpoint");
    auto clear_result =
        service->BatchReplicaClear({key}, old_client, "ssd_before_ttl_old");
    ASSERT_TRUE(clear_result.has_value());
    ASSERT_EQ(clear_result->size(), 1u);

    auto before_replacement = service->GetReplicaList(key, "default");
    ASSERT_TRUE(before_replacement.has_value());
    ASSERT_EQ(before_replacement->replicas.size(), 1u);
    EXPECT_EQ(
        before_replacement->replicas[0].get_local_disk_descriptor().client_id,
        old_client);

    MountMemoryAndLocalDisk(*service, new_client, "ssd_before_ttl_new",
                            0x1a00000000ULL, marker, "new_endpoint");
    auto reattached = service->GetReplicaList(key, "default");
    ASSERT_TRUE(reattached.has_value());
    ASSERT_EQ(reattached->replicas.size(), 1u);
    const auto descriptor = reattached->replicas[0].get_local_disk_descriptor();
    EXPECT_EQ(descriptor.client_id, new_client);
    EXPECT_EQ(descriptor.transport_endpoint, "new_endpoint");
    EXPECT_EQ(descriptor.local_disk_segment_id, marker);
}

TEST_F(MasterServiceSSDTest, ReattachRestoresLocalDiskUsageTracking) {
    auto service = CreateSsdRejoinService(/*client_live_ttl_sec=*/1,
                                          /*local_disk_rejoin_grace_sec=*/10);
    const UUID old_client = generate_uuid();
    const UUID new_client = generate_uuid();
    const UUID other_client = generate_uuid();
    const std::string marker = "local-disk-segment-usage-restore";
    const std::string old_segment = "ssd_usage_old";
    const std::string new_segment = "ssd_usage_new";
    const std::string other_segment = "ssd_usage_other";
    const std::string key = "ssd_rejoin_usage_restore";

    MountMemoryAndLocalDisk(*service, old_client, old_segment, 0x1b00000000ULL,
                            marker, "old_endpoint");
    PutAndOffload(*service, old_client, key, 800, "old_endpoint");

    auto clear_result =
        service->BatchReplicaClear({key}, old_client, old_segment);
    ASSERT_TRUE(clear_result.has_value());
    ASSERT_EQ(clear_result->size(), 1u);
    WaitForReplicaNotReady(*service, key, std::chrono::seconds(4));

    MountMemoryAndLocalDisk(*service, other_client, other_segment,
                            0x1c00000000ULL, "other-marker", "other_endpoint");
    PutAndOffload(*service, other_client, "ssd_usage_other_baseline", 100,
                  "other_endpoint");

    MountMemoryAndLocalDisk(*service, new_client, new_segment, 0x1d00000000ULL,
                            marker, "new_endpoint");
    auto reattached = service->GetReplicaList(key, "default");
    ASSERT_TRUE(reattached.has_value());

    ExpectNextAllocationOnSegment(*service, other_client,
                                  "ssd_usage_restore_probe", other_segment);
}

TEST_F(MasterServiceSSDTest, UpsertRemovesDisconnectedLocalDiskReplica) {
    auto service = CreateSsdRejoinService(/*client_live_ttl_sec=*/1,
                                          /*local_disk_rejoin_grace_sec=*/10);
    const UUID old_client = generate_uuid();
    const UUID writer_client = generate_uuid();
    const UUID rejoin_client = generate_uuid();
    const std::string marker = "local-disk-segment-upsert-stale";
    const std::string key = "ssd_rejoin_upsert_stale";

    MountMemoryAndLocalDisk(*service, old_client, "ssd_upsert_old",
                            0x1e00000000ULL, marker, "old_endpoint");
    PutAndOffload(*service, old_client, key, 256, "old_endpoint");
    auto clear_result =
        service->BatchReplicaClear({key}, old_client, "ssd_upsert_old");
    ASSERT_TRUE(clear_result.has_value());
    ASSERT_EQ(clear_result->size(), 1u);
    WaitForReplicaNotReady(*service, key, std::chrono::seconds(4));

    Segment writer_segment;
    writer_segment.id = generate_uuid();
    writer_segment.name = "ssd_upsert_writer";
    writer_segment.base = 0x1f00000000ULL;
    writer_segment.size = 64 * 1024 * 1024;
    writer_segment.te_endpoint = writer_segment.name;
    ASSERT_TRUE(
        service->MountSegment(writer_segment, writer_client).has_value());

    ReplicateConfig config;
    config.replica_num = 1;
    auto upsert_result =
        service->UpsertStart(writer_client, key, "default", 256, config);
    ASSERT_TRUE(upsert_result.has_value());
    ASSERT_TRUE(
        service->UpsertEnd(writer_client, key, "default", ReplicaType::MEMORY)
            .has_value());

    MountMemoryAndLocalDisk(*service, rejoin_client, "ssd_upsert_rejoin",
                            0x2000000000ULL, marker, "rejoin_endpoint");
    auto result = service->GetReplicaList(key, "default");
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->replicas.size(), 1u);
    EXPECT_TRUE(result->replicas[0].is_memory_replica());
}

TEST_F(MasterServiceSSDTest,
       ForceBatchRemoveDropsDisconnectedLocalDiskReplica) {
    auto service = CreateSsdRejoinService(/*client_live_ttl_sec=*/1,
                                          /*local_disk_rejoin_grace_sec=*/10);
    const UUID old_client = generate_uuid();
    const std::string marker = "local-disk-segment-force-remove";
    const std::string key = "ssd_rejoin_force_remove";

    MountMemoryAndLocalDisk(*service, old_client, "ssd_force_remove_old",
                            0x2100000000ULL, marker, "old_endpoint");
    PutAndOffload(*service, old_client, key, 256, "old_endpoint");
    auto clear_result =
        service->BatchReplicaClear({key}, old_client, "ssd_force_remove_old");
    ASSERT_TRUE(clear_result.has_value());
    ASSERT_EQ(clear_result->size(), 1u);
    WaitForReplicaNotReady(*service, key, std::chrono::seconds(4));

    auto remove_result = service->BatchRemove({key}, "default", /*force=*/true);
    ASSERT_EQ(remove_result.size(), 1u);
    ASSERT_TRUE(remove_result[0].has_value());

    auto get_result = service->GetReplicaList(key, "default");
    ASSERT_FALSE(get_result.has_value());
    EXPECT_EQ(get_result.error(), ErrorCode::OBJECT_NOT_FOUND);
}

TEST_F(MasterServiceSSDTest,
       OffloadSuccessAddsDiskReplicaWhenOnlyStaleDiskOwnerExists) {
    auto service = CreateSsdRejoinService(/*client_live_ttl_sec=*/1,
                                          /*local_disk_rejoin_grace_sec=*/10);
    const UUID writer_client = generate_uuid();
    const UUID old_disk_client = generate_uuid();
    const UUID new_disk_client = generate_uuid();
    const std::string key = "ssd_rejoin_offload_after_stale";

    Segment writer_segment;
    writer_segment.id = generate_uuid();
    writer_segment.name = "ssd_stale_writer";
    writer_segment.base = 0x2200000000ULL;
    writer_segment.size = 64 * 1024 * 1024;
    writer_segment.te_endpoint = writer_segment.name;
    ASSERT_TRUE(
        service->MountSegment(writer_segment, writer_client).has_value());

    MountMemoryAndLocalDisk(*service, old_disk_client, "ssd_stale_old_disk",
                            0x2300000000ULL, "old-stale-marker",
                            "old_endpoint");

    ReplicateConfig config;
    config.replica_num = 1;
    ASSERT_TRUE(service->PutStart(writer_client, key, "default", 256, config)
                    .has_value());
    ASSERT_TRUE(
        service->PutEnd(writer_client, key, "default", ReplicaType::MEMORY)
            .has_value());

    StorageObjectMetadata old_metadata;
    old_metadata.data_size = 256;
    old_metadata.transport_endpoint = "old_endpoint";
    OffloadTaskItem task{.tenant_id = "default", .key = key, .size = 256};
    ASSERT_TRUE(
        service->NotifyOffloadSuccess(old_disk_client, {task}, {old_metadata})
            .has_value());

    for (int i = 0; i < 5; ++i) {
        ASSERT_TRUE(service->Ping(writer_client).has_value());
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    MountMemoryAndLocalDisk(*service, new_disk_client, "ssd_stale_new_disk",
                            0x2400000000ULL, "new-stale-marker",
                            "new_endpoint");

    StorageObjectMetadata new_metadata;
    new_metadata.data_size = 256;
    new_metadata.transport_endpoint = "new_endpoint";
    ASSERT_TRUE(
        service->NotifyOffloadSuccess(new_disk_client, {task}, {new_metadata})
            .has_value());

    auto after_new_offload = service->GetReplicaList(key, "default");
    ASSERT_TRUE(after_new_offload.has_value());
    bool found_new_disk = false;
    for (const auto& replica : after_new_offload->replicas) {
        if (!replica.is_local_disk_replica()) {
            continue;
        }
        const auto desc = replica.get_local_disk_descriptor();
        if (desc.client_id == new_disk_client &&
            desc.transport_endpoint == "new_endpoint") {
            found_new_disk = true;
        }
    }
    EXPECT_TRUE(found_new_disk);
}

TEST_F(MasterServiceSSDTest, EmptyLocalDiskSegmentIdDoesNotReattach) {
    auto service = CreateSsdRejoinService(/*client_live_ttl_sec=*/1,
                                          /*local_disk_rejoin_grace_sec=*/10);
    const UUID old_client = generate_uuid();
    const UUID old_style_client = generate_uuid();
    const UUID new_client = generate_uuid();
    const std::string marker = "local-disk-segment-empty-old-style";
    const std::string key = "ssd_rejoin_empty_marker_no_adopt";

    MountMemoryAndLocalDisk(*service, old_client, "ssd_empty_old",
                            0x1400000000ULL, marker, "old_endpoint");
    PutAndOffload(*service, old_client, key, 256, "old_endpoint");
    auto clear_result =
        service->BatchReplicaClear({key}, old_client, "ssd_empty_old");
    ASSERT_TRUE(clear_result.has_value());
    ASSERT_EQ(clear_result->size(), 1u);
    EXPECT_EQ((*clear_result)[0], key);

    WaitForReplicaNotReady(*service, key, std::chrono::seconds(4));

    MountMemoryAndLocalDisk(*service, old_style_client, "ssd_empty_old_style",
                            0x1500000000ULL, "", "old_style_endpoint");
    auto old_style_result = service->GetReplicaList(key, "default");
    ASSERT_FALSE(old_style_result.has_value());
    EXPECT_EQ(old_style_result.error(), ErrorCode::REPLICA_IS_NOT_READY);

    MountMemoryAndLocalDisk(*service, new_client, "ssd_empty_new",
                            0x1600000000ULL, marker, "new_endpoint");
    auto reattached = service->GetReplicaList(key, "default");
    ASSERT_TRUE(reattached.has_value());
    ASSERT_EQ(reattached->replicas.size(), 1u);
    const auto descriptor = reattached->replicas[0].get_local_disk_descriptor();
    EXPECT_EQ(descriptor.client_id, new_client);
    EXPECT_EQ(descriptor.transport_endpoint, "new_endpoint");
    EXPECT_EQ(descriptor.local_disk_segment_id, marker);
}

TEST_F(MasterServiceSSDTest, ReattachFiveThousandLocalDiskReplicasQuickly) {
    auto service = CreateSsdRejoinService(/*client_live_ttl_sec=*/1,
                                          /*local_disk_rejoin_grace_sec=*/10);
    const UUID old_client = generate_uuid();
    const UUID new_client = generate_uuid();
    const std::string marker = "local-disk-segment-scale-5k";
    const std::string old_segment = "ssd_scale_5k_old";
    constexpr size_t kKeyCount = 5000;

    MountMemoryAndLocalDisk(*service, old_client, old_segment, 0x1700000000ULL,
                            marker, "old_endpoint");

    std::vector<std::string> keys;
    keys.reserve(kKeyCount);
    for (size_t i = 0; i < kKeyCount; ++i) {
        auto key = "ssd_rejoin_scale_5k_" + std::to_string(i);
        PutAndOffload(*service, old_client, key, 256, "old_endpoint");
        keys.emplace_back(std::move(key));
    }

    auto clear_result =
        service->BatchReplicaClear(keys, old_client, old_segment);
    ASSERT_TRUE(clear_result.has_value());
    ASSERT_EQ(clear_result->size(), kKeyCount);

    WaitForReplicaNotReady(*service, keys.front(), std::chrono::seconds(4));

    const auto start = std::chrono::steady_clock::now();
    MountMemoryAndLocalDisk(*service, new_client, "ssd_scale_5k_new",
                            0x1800000000ULL, marker, "new_endpoint");
    const auto elapsed_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start)
            .count();
    EXPECT_LT(elapsed_ms, 1000);

    for (const auto& key : {keys.front(), keys[kKeyCount / 2], keys.back()}) {
        auto reattached = service->GetReplicaList(key, "default");
        ASSERT_TRUE(reattached.has_value());
        ASSERT_EQ(reattached->replicas.size(), 1u);
        const auto descriptor =
            reattached->replicas[0].get_local_disk_descriptor();
        EXPECT_EQ(descriptor.client_id, new_client);
        EXPECT_EQ(descriptor.transport_endpoint, "new_endpoint");
        EXPECT_EQ(descriptor.local_disk_segment_id, marker);
    }
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

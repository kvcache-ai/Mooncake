#include "fixture.h"

namespace mooncake::test {
TEST_F(MasterServiceSSDSnapshotTest, RestorePreservesCacheTotalMetrics) {
    MasterMetricManager::instance().reset_cache_total_nums();

    CreateMasterServiceWithSSDFeat("/mnt/ssd");
    EnsureSnapshotStores(service_.get());

    constexpr size_t buffer = 0x330000000;
    constexpr size_t size = 1024 * 1024 * 64;
    Segment segment;
    segment.id = generate_uuid();
    segment.name = "test_segment_restore_cache_totals";
    segment.base = buffer;
    segment.size = size;
    segment.te_endpoint = segment.name;
    UUID client_id = generate_uuid();

    ASSERT_TRUE(service_->MountSegment(segment, client_id).has_value());

    std::string key = "restore_cache_total_metric_key";
    ASSERT_TRUE(
        service_->PutStart(client_id, key, "default", 1024, {.replica_num = 1})
            .has_value());
    EXPECT_TRUE(service_->PutEnd(client_id, key, "default", ReplicaType::MEMORY)
                    .has_value());
    EXPECT_TRUE(service_->PutEnd(client_id, key, "default", ReplicaType::DISK)
                    .has_value());

    auto& metrics = MasterMetricManager::instance();
    using CacheHitStat = MasterMetricManager::CacheHitStat;
    auto stats = metrics.calculate_cache_stats();
    ASSERT_EQ(stats[CacheHitStat::MEMORY_TOTAL], 1);
    ASSERT_EQ(stats[CacheHitStat::SSD_TOTAL], 1);

    std::string snapshot_id = GenerateSnapshotId();
    auto persist_result = CallPersistState(service_.get(), snapshot_id);
    ASSERT_TRUE(persist_result.has_value())
        << "Failed to persist state: " << persist_result.error().message;

    metrics.reset_cache_total_nums();

    ::setenv("MOONCAKE_MASTER_SERVICE_SNAPSHOT_TEST_SKIP_CLEANUP", "1", 1);
    auto restore_config = MasterServiceConfig::builder()
                              .set_enable_snapshot_restore(true)
                              .set_snapshot_object_store_type("local")
                              .set_root_fs_dir("/mnt/ssd")
                              .build();
    std::unique_ptr<MasterService> restored_service(
        new MasterService(restore_config));
    ::unsetenv("MOONCAKE_MASTER_SERVICE_SNAPSHOT_TEST_SKIP_CLEANUP");

    stats = metrics.calculate_cache_stats();
    EXPECT_EQ(stats[CacheHitStat::MEMORY_TOTAL], 1);
    EXPECT_EQ(stats[CacheHitStat::SSD_TOTAL], 1);

    auto get_result = restored_service->GetReplicaList(key, "default");
    ASSERT_TRUE(get_result.has_value());
    EXPECT_EQ(2, get_result.value().replicas.size());
}

}  // namespace mooncake::test

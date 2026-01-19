#include <glog/logging.h>
#include <gtest/gtest.h>

#include <thread>
#include <vector>

#include "rpc_service.h"
#include "types.h"
#include "master_config.h"
#include "master_metric_manager.h"

namespace mooncake::test {

class MasterMetricsTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("MasterMetricsTest");
        FLAGS_logtostderr = true;
    }

    std::vector<Replica::Descriptor> replica_list;

    void TearDown() override { google::ShutdownGoogleLogging(); }
};

TEST_F(MasterMetricsTest, InitialStatusTest) {
    auto& metrics = MasterMetricManager::instance();

    // Mem Storage Metrics
    ASSERT_EQ(metrics.get_allocated_mem_size(), 0);
    ASSERT_EQ(metrics.get_total_mem_capacity(), 0);
    ASSERT_DOUBLE_EQ(metrics.get_global_mem_used_ratio(), 0.0);

    // File Storage Metrics
    ASSERT_EQ(metrics.get_allocated_file_size(), 0);
    ASSERT_EQ(metrics.get_total_file_capacity(), 0);
    ASSERT_DOUBLE_EQ(metrics.get_global_file_used_ratio(), 0.0);

    // Key/Value Metrics
    ASSERT_EQ(metrics.get_key_count(), 0);

    // Operation Statistics
    ASSERT_EQ(metrics.get_put_start_requests(), 0);
    ASSERT_EQ(metrics.get_put_start_failures(), 0);
    ASSERT_EQ(metrics.get_put_end_requests(), 0);
    ASSERT_EQ(metrics.get_put_end_failures(), 0);
    ASSERT_EQ(metrics.get_put_revoke_requests(), 0);
    ASSERT_EQ(metrics.get_put_revoke_failures(), 0);
    ASSERT_EQ(metrics.get_get_replica_list_requests(), 0);
    ASSERT_EQ(metrics.get_get_replica_list_failures(), 0);
    ASSERT_EQ(metrics.get_exist_key_requests(), 0);
    ASSERT_EQ(metrics.get_exist_key_failures(), 0);
    ASSERT_EQ(metrics.get_remove_requests(), 0);
    ASSERT_EQ(metrics.get_remove_failures(), 0);
    ASSERT_EQ(metrics.get_remove_all_requests(), 0);
    ASSERT_EQ(metrics.get_remove_all_failures(), 0);
    ASSERT_EQ(metrics.get_mount_segment_requests(), 0);
    ASSERT_EQ(metrics.get_mount_segment_failures(), 0);
    ASSERT_EQ(metrics.get_unmount_segment_requests(), 0);
    ASSERT_EQ(metrics.get_unmount_segment_failures(), 0);

    // CopyStart, CopyEnd, CopyRevoke, MoveStart, MoveEnd, MoveRevoke Metrics
    ASSERT_EQ(metrics.get_copy_start_requests(), 0);
    ASSERT_EQ(metrics.get_copy_start_failures(), 0);
    ASSERT_EQ(metrics.get_copy_end_requests(), 0);
    ASSERT_EQ(metrics.get_copy_end_failures(), 0);
    ASSERT_EQ(metrics.get_copy_revoke_requests(), 0);
    ASSERT_EQ(metrics.get_copy_revoke_failures(), 0);
    ASSERT_EQ(metrics.get_move_start_requests(), 0);
    ASSERT_EQ(metrics.get_move_start_failures(), 0);
    ASSERT_EQ(metrics.get_move_end_requests(), 0);
    ASSERT_EQ(metrics.get_move_end_failures(), 0);
    ASSERT_EQ(metrics.get_move_revoke_requests(), 0);
    ASSERT_EQ(metrics.get_move_revoke_failures(), 0);

    // Eviction Metrics
    ASSERT_EQ(metrics.get_eviction_success(), 0);
    ASSERT_EQ(metrics.get_eviction_attempts(), 0);
    ASSERT_EQ(metrics.get_evicted_key_count(), 0);
    ASSERT_EQ(metrics.get_evicted_size(), 0);

    // Batch RPC Metrics
    ASSERT_EQ(metrics.get_batch_exist_key_requests(), 0);
    ASSERT_EQ(metrics.get_batch_exist_key_failures(), 0);
    ASSERT_EQ(metrics.get_batch_exist_key_partial_successes(), 0);
    ASSERT_EQ(metrics.get_batch_exist_key_items(), 0);
    ASSERT_EQ(metrics.get_batch_exist_key_failed_items(), 0);
    ASSERT_EQ(metrics.get_batch_get_replica_list_requests(), 0);
    ASSERT_EQ(metrics.get_batch_get_replica_list_failures(), 0);
    ASSERT_EQ(metrics.get_batch_get_replica_list_partial_successes(), 0);
    ASSERT_EQ(metrics.get_batch_get_replica_list_items(), 0);
    ASSERT_EQ(metrics.get_batch_get_replica_list_failed_items(), 0);
    ASSERT_EQ(metrics.get_batch_put_start_requests(), 0);
    ASSERT_EQ(metrics.get_batch_put_start_failures(), 0);
    ASSERT_EQ(metrics.get_batch_put_start_partial_successes(), 0);
    ASSERT_EQ(metrics.get_batch_put_start_items(), 0);
    ASSERT_EQ(metrics.get_batch_put_start_failed_items(), 0);
    ASSERT_EQ(metrics.get_batch_put_end_requests(), 0);
    ASSERT_EQ(metrics.get_batch_put_end_failures(), 0);
    ASSERT_EQ(metrics.get_batch_put_end_partial_successes(), 0);
    ASSERT_EQ(metrics.get_batch_put_end_items(), 0);
    ASSERT_EQ(metrics.get_batch_put_end_failed_items(), 0);
    ASSERT_EQ(metrics.get_batch_put_revoke_requests(), 0);
    ASSERT_EQ(metrics.get_batch_put_revoke_failures(), 0);
    ASSERT_EQ(metrics.get_batch_put_revoke_partial_successes(), 0);
    ASSERT_EQ(metrics.get_batch_put_revoke_items(), 0);
    ASSERT_EQ(metrics.get_batch_put_revoke_failed_items(), 0);

    // PutStart Discard Metrics
    ASSERT_EQ(metrics.get_put_start_discard_cnt(), 0);
    ASSERT_EQ(metrics.get_put_start_release_cnt(), 0);
    ASSERT_EQ(metrics.get_put_start_discarded_staging_size(), 0);
}

TEST_F(MasterMetricsTest, BasicRequestTest) {
    const uint64_t default_kv_lease_ttl = 100;
    auto& metrics = MasterMetricManager::instance();
    // Use a wrapped master service to test the metrics manager
    WrappedMasterServiceConfig service_config;
    service_config.default_kv_lease_ttl = default_kv_lease_ttl;
    service_config.enable_metric_reporting = true;
    WrappedMasterService service_(service_config);

    constexpr size_t kBufferAddress = 0x300000000;
    constexpr size_t kSegmentSize = 1024 * 1024 * 16;
    std::string segment_name = "test_segment";
    UUID segment_id = generate_uuid();
    Segment segment;
    segment.id = segment_id;
    segment.name = segment_name;
    segment.base = kBufferAddress;
    segment.size = kSegmentSize;
    UUID client_id = generate_uuid();

    std::string key = "test_key";
    uint64_t value_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;

    // Test MountSegment request
    auto mount_result = service_.MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());
    ASSERT_EQ(metrics.get_allocated_mem_size(), 0);
    ASSERT_EQ(metrics.get_total_mem_capacity(), kSegmentSize);
    ASSERT_DOUBLE_EQ(metrics.get_global_mem_used_ratio(), 0.0);
    ASSERT_EQ(metrics.get_segment_allocated_mem_size(segment.name), 0);
    ASSERT_EQ(metrics.get_segment_total_mem_capacity(segment.name),
              kSegmentSize);
    ASSERT_DOUBLE_EQ(metrics.get_segment_mem_used_ratio(segment.name), 0.0);
    ASSERT_EQ(metrics.get_mount_segment_requests(), 1);
    ASSERT_EQ(metrics.get_mount_segment_failures(), 0);

    // Test PutStart and PutRevoke request
    auto put_start_result1 =
        service_.PutStart(client_id, key, value_length, config);
    ASSERT_TRUE(put_start_result1.has_value());
    ASSERT_EQ(metrics.get_key_count(), 1);
    ASSERT_EQ(metrics.get_allocated_mem_size(), value_length);
    ASSERT_EQ(metrics.get_segment_allocated_mem_size(segment.name),
              value_length);
    ASSERT_EQ(metrics.get_put_start_requests(), 1);
    ASSERT_EQ(metrics.get_put_start_failures(), 0);
    auto put_revoke_result =
        service_.PutRevoke(client_id, key, ReplicaType::MEMORY);
    ASSERT_TRUE(put_revoke_result.has_value());
    ASSERT_EQ(metrics.get_key_count(), 0);
    ASSERT_EQ(metrics.get_allocated_mem_size(), 0);
    ASSERT_EQ(metrics.get_segment_allocated_mem_size(segment.name), 0);
    ASSERT_EQ(metrics.get_put_revoke_requests(), 1);
    ASSERT_EQ(metrics.get_put_revoke_failures(), 0);

    // Test PutStart and PutEnd request
    auto put_start_result2 =
        service_.PutStart(client_id, key, value_length, config);
    ASSERT_TRUE(put_start_result2.has_value());
    ASSERT_EQ(metrics.get_key_count(), 1);
    ASSERT_EQ(metrics.get_allocated_mem_size(), value_length);
    ASSERT_EQ(metrics.get_segment_allocated_mem_size(segment.name),
              value_length);
    ASSERT_EQ(metrics.get_put_start_requests(), 2);
    ASSERT_EQ(metrics.get_put_start_failures(), 0);
    auto put_end_result = service_.PutEnd(client_id, key, ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());
    ASSERT_EQ(metrics.get_key_count(), 1);
    ASSERT_EQ(metrics.get_allocated_mem_size(), value_length);
    ASSERT_EQ(metrics.get_segment_allocated_mem_size(segment.name),
              value_length);
    ASSERT_EQ(metrics.get_put_end_requests(), 1);
    ASSERT_EQ(metrics.get_put_end_failures(), 0);

    // Test ExistKey request
    auto exist_result = service_.ExistKey(key);
    ASSERT_TRUE(exist_result.has_value() && exist_result.value());
    ASSERT_EQ(metrics.get_exist_key_requests(), 1);
    ASSERT_EQ(metrics.get_exist_key_failures(), 0);

    // Test GetReplicaList request
    auto get_replica_result = service_.GetReplicaList(key);
    ASSERT_TRUE(get_replica_result.has_value());
    ASSERT_EQ(metrics.get_get_replica_list_requests(), 1);
    ASSERT_EQ(metrics.get_get_replica_list_failures(), 0);

    // Test Remove request
    std::this_thread::sleep_for(
        std::chrono::milliseconds(default_kv_lease_ttl));
    auto remove_result = service_.Remove(key);
    ASSERT_TRUE(remove_result.has_value());
    ASSERT_EQ(metrics.get_remove_requests(), 1);
    ASSERT_EQ(metrics.get_remove_failures(), 0);
    ASSERT_EQ(metrics.get_key_count(), 0);
    ASSERT_EQ(metrics.get_allocated_mem_size(), 0);
    ASSERT_EQ(metrics.get_segment_allocated_mem_size(segment.name), 0);

    // Test RemoveAll request
    auto put_start_result3 =
        service_.PutStart(client_id, key, value_length, config);
    ASSERT_TRUE(put_start_result3.has_value());
    auto put_end_result2 = service_.PutEnd(client_id, key, ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result2.has_value());
    ASSERT_EQ(metrics.get_key_count(), 1);
    ASSERT_EQ(1, service_.RemoveAll());
    ASSERT_EQ(metrics.get_remove_all_requests(), 1);
    ASSERT_EQ(metrics.get_remove_all_failures(), 0);
    ASSERT_EQ(metrics.get_key_count(), 0);
    ASSERT_EQ(metrics.get_allocated_mem_size(), 0);
    ASSERT_EQ(metrics.get_segment_allocated_mem_size(segment.name), 0);

    // Test UnmountSegment request
    auto put_start_result4 =
        service_.PutStart(client_id, key, value_length, config);
    ASSERT_TRUE(put_start_result4.has_value());
    auto put_end_result3 = service_.PutEnd(client_id, key, ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result3.has_value());
    auto unmount_result = service_.UnmountSegment(segment_id, client_id);
    ASSERT_TRUE(unmount_result.has_value());
    ASSERT_EQ(metrics.get_unmount_segment_requests(), 1);
    ASSERT_EQ(metrics.get_unmount_segment_failures(), 0);
    ASSERT_EQ(metrics.get_key_count(), 0);
    ASSERT_EQ(metrics.get_allocated_mem_size(), 0);
    ASSERT_EQ(metrics.get_total_mem_capacity(), 0);
    ASSERT_DOUBLE_EQ(metrics.get_global_mem_used_ratio(), 0.0);
    ASSERT_EQ(metrics.get_segment_allocated_mem_size(segment.name), 0);
    ASSERT_EQ(metrics.get_segment_total_mem_capacity(segment.name), 0);
    ASSERT_DOUBLE_EQ(metrics.get_segment_mem_used_ratio(segment.name), 0.0);

    // check segment mem used ratio for non-existent segment
    ASSERT_DOUBLE_EQ(metrics.get_segment_mem_used_ratio(""), 0.0);
    ASSERT_DOUBLE_EQ(metrics.get_segment_mem_used_ratio("xxxxxx_segment"), 0.0);
}

TEST_F(MasterMetricsTest, CalcCacheStatsTest) {
    const uint64_t default_kv_lease_ttl = 100;
    auto& metrics = MasterMetricManager::instance();
    // Use a wrapped master service to test the metrics manager
    WrappedMasterServiceConfig service_config;
    service_config.default_kv_lease_ttl = default_kv_lease_ttl;
    service_config.enable_metric_reporting = true;
    WrappedMasterService service_(service_config);

    constexpr size_t kBufferAddress = 0x300000000;
    constexpr size_t kSegmentSize = 1024 * 1024 * 16;
    std::string segment_name = "test_segment";
    UUID segment_id = generate_uuid();
    Segment segment;
    segment.id = segment_id;
    segment.name = segment_name;
    segment.base = kBufferAddress;
    segment.size = kSegmentSize;
    UUID client_id = generate_uuid();

    std::string key = "test_key";
    uint64_t value_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;

    auto stats_dict = metrics.calculate_cache_stats();
    ASSERT_EQ(stats_dict[MasterMetricManager::CacheHitStat::MEMORY_HITS], 1);
    ASSERT_EQ(stats_dict[MasterMetricManager::CacheHitStat::SSD_HITS], 0);
    ASSERT_EQ(stats_dict[MasterMetricManager::CacheHitStat::MEMORY_TOTAL], 2);
    ASSERT_EQ(stats_dict[MasterMetricManager::CacheHitStat::SSD_TOTAL], 0);
    ASSERT_EQ(stats_dict[MasterMetricManager::CacheHitStat::MEMORY_HIT_RATE],
              0.5);
    ASSERT_EQ(stats_dict[MasterMetricManager::CacheHitStat::SSD_HIT_RATE], 0);
    ASSERT_EQ(stats_dict[MasterMetricManager::CacheHitStat::OVERALL_HIT_RATE],
              0.5);
    ASSERT_EQ(stats_dict[MasterMetricManager::CacheHitStat::VALID_GET_RATE], 1);

    auto mount_result = service_.MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());
    auto put_start_result1 =
        service_.PutStart(client_id, key, value_length, config);
    ASSERT_TRUE(put_start_result1.has_value());
    auto put_end_result1 = service_.PutEnd(client_id, key, ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result1.has_value());
    stats_dict = metrics.calculate_cache_stats();

    ASSERT_EQ(stats_dict[MasterMetricManager::CacheHitStat::MEMORY_TOTAL], 3);

    auto get_replica_result = service_.GetReplicaList(key);
    stats_dict = metrics.calculate_cache_stats();
    ASSERT_EQ(stats_dict[MasterMetricManager::CacheHitStat::MEMORY_HITS], 2);
    ASSERT_EQ(stats_dict[MasterMetricManager::CacheHitStat::SSD_HITS], 0);
    ASSERT_EQ(stats_dict[MasterMetricManager::CacheHitStat::MEMORY_TOTAL], 3);
    ASSERT_EQ(stats_dict[MasterMetricManager::CacheHitStat::SSD_TOTAL], 0);
    ASSERT_NEAR(stats_dict[MasterMetricManager::CacheHitStat::MEMORY_HIT_RATE],
                0.67, 0.01);
    ASSERT_EQ(stats_dict[MasterMetricManager::CacheHitStat::SSD_HIT_RATE], 0);
    ASSERT_NEAR(stats_dict[MasterMetricManager::CacheHitStat::OVERALL_HIT_RATE],
                0.67, 0.01);
    ASSERT_EQ(stats_dict[MasterMetricManager::CacheHitStat::VALID_GET_RATE], 1);

    std::this_thread::sleep_for(
        std::chrono::milliseconds(default_kv_lease_ttl));
    auto remove_result = service_.Remove(key);
    ASSERT_TRUE(remove_result.has_value());
}

TEST_F(MasterMetricsTest, BatchRequestTest) {
    const uint64_t default_kv_lease_ttl = 100;
    auto& metrics = MasterMetricManager::instance();
    WrappedMasterServiceConfig service_config;
    service_config.default_kv_lease_ttl = default_kv_lease_ttl;
    WrappedMasterService service_(service_config);

    constexpr size_t kBufferAddress = 0x300000000;
    constexpr size_t kSegmentSize = 1024 * 1024 * 64;
    std::string segment_name = "test_segment";
    UUID segment_id = generate_uuid();
    Segment segment;
    segment.id = segment_id;
    segment.name = segment_name;
    segment.base = kBufferAddress;
    segment.size = kSegmentSize;
    UUID client_id = generate_uuid();

    std::vector<std::string> keys = {"test_key1", "test_key2", "test_key3"};
    std::vector<uint64_t> value_lengths = {1024, 2048, 512};
    ReplicateConfig config;
    config.replica_num = 1;

    // Mount segment
    auto mount_result = service_.MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    // Test BatchExistKey request (should all return false initially)
    auto batch_exist_result = service_.BatchExistKey(keys);
    ASSERT_EQ(batch_exist_result.size(), 3);
    ASSERT_EQ(metrics.get_batch_exist_key_requests(), 1);
    ASSERT_EQ(metrics.get_batch_exist_key_partial_successes(), 0);
    ASSERT_EQ(metrics.get_batch_exist_key_failures(), 0);
    ASSERT_EQ(metrics.get_batch_exist_key_items(), 3);
    ASSERT_EQ(metrics.get_batch_exist_key_failed_items(), 0);

    // Test BatchPutStart request
    auto batch_put_start_result =
        service_.BatchPutStart(client_id, keys, value_lengths, config);
    ASSERT_EQ(batch_put_start_result.size(), 3);
    ASSERT_EQ(metrics.get_batch_put_start_requests(), 1);
    ASSERT_EQ(metrics.get_batch_put_start_partial_successes(), 0);
    ASSERT_EQ(metrics.get_batch_put_start_failures(), 0);
    ASSERT_EQ(metrics.get_batch_put_start_items(), 3);
    ASSERT_EQ(metrics.get_batch_put_start_failed_items(), 0);

    // Test BatchGetReplicaList request (should all fail)
    auto batch_get_replica_result = service_.BatchGetReplicaList(keys);
    ASSERT_EQ(batch_get_replica_result.size(), 3);
    ASSERT_EQ(metrics.get_batch_get_replica_list_requests(), 1);
    ASSERT_EQ(metrics.get_batch_get_replica_list_partial_successes(), 0);
    ASSERT_EQ(metrics.get_batch_get_replica_list_failures(), 1);
    ASSERT_EQ(metrics.get_batch_get_replica_list_items(), 3);
    ASSERT_EQ(metrics.get_batch_get_replica_list_failed_items(), 3);

    // Test BatchPutEnd request
    auto batch_put_end_result = service_.BatchPutEnd(client_id, keys);
    ASSERT_EQ(batch_put_end_result.size(), 3);
    ASSERT_EQ(metrics.get_batch_put_end_requests(), 1);
    ASSERT_EQ(metrics.get_batch_put_end_partial_successes(), 0);
    ASSERT_EQ(metrics.get_batch_put_end_failures(), 0);
    ASSERT_EQ(metrics.get_batch_put_end_items(), 3);
    ASSERT_EQ(metrics.get_batch_put_end_failed_items(), 0);

    // Test BatchExistKey again (should all return true now)
    auto batch_exist_result2 = service_.BatchExistKey(keys);
    ASSERT_EQ(batch_exist_result2.size(), 3);
    ASSERT_EQ(metrics.get_batch_exist_key_requests(), 2);
    ASSERT_EQ(metrics.get_batch_exist_key_partial_successes(), 0);
    ASSERT_EQ(metrics.get_batch_exist_key_failures(), 0);
    ASSERT_EQ(metrics.get_batch_exist_key_items(), 6);
    ASSERT_EQ(metrics.get_batch_exist_key_failed_items(), 0);

    // Test BatchGetReplicaList again (should all succeed now)
    auto batch_get_replica_result2 = service_.BatchGetReplicaList(keys);
    ASSERT_EQ(batch_get_replica_result2.size(), 3);
    ASSERT_EQ(metrics.get_batch_get_replica_list_requests(), 2);
    ASSERT_EQ(metrics.get_batch_get_replica_list_partial_successes(), 0);
    ASSERT_EQ(metrics.get_batch_get_replica_list_failures(), 1);
    ASSERT_EQ(metrics.get_batch_get_replica_list_items(), 6);
    ASSERT_EQ(metrics.get_batch_get_replica_list_failed_items(), 3);

    // Test BatchPutRevoke request (should all fail)
    auto batch_put_revoke_result = service_.BatchPutRevoke(client_id, keys);
    ASSERT_EQ(batch_put_revoke_result.size(), 3);
    ASSERT_EQ(metrics.get_batch_put_revoke_requests(), 1);
    ASSERT_EQ(metrics.get_batch_put_revoke_partial_successes(), 0);
    ASSERT_EQ(metrics.get_batch_put_revoke_failures(), 1);
    ASSERT_EQ(metrics.get_batch_put_revoke_items(), 3);
    ASSERT_EQ(metrics.get_batch_put_revoke_failed_items(), 3);

    // Test partial success
    keys.push_back("test_key4");
    value_lengths.push_back(512);
    auto batch_get_replica_result3 = service_.BatchGetReplicaList(keys);
    ASSERT_EQ(batch_get_replica_result3.size(), 4);
    ASSERT_EQ(metrics.get_batch_get_replica_list_requests(), 3);
    ASSERT_EQ(metrics.get_batch_get_replica_list_partial_successes(), 1);
    ASSERT_EQ(metrics.get_batch_get_replica_list_failures(), 1);
    ASSERT_EQ(metrics.get_batch_get_replica_list_items(), 10);
    ASSERT_EQ(metrics.get_batch_get_replica_list_failed_items(), 4);

    auto batch_put_start_result2 =
        service_.BatchPutStart(client_id, keys, value_lengths, config);
    ASSERT_EQ(batch_put_start_result2.size(), 4);
    ASSERT_EQ(metrics.get_batch_put_start_requests(), 2);
    ASSERT_EQ(metrics.get_batch_put_start_partial_successes(), 1);
    ASSERT_EQ(metrics.get_batch_put_start_failures(), 0);
    ASSERT_EQ(metrics.get_batch_put_start_items(), 7);
    ASSERT_EQ(metrics.get_batch_put_start_failed_items(), 3);
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

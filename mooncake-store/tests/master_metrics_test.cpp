#include "master_service.h"
#include "rpc_service.h"
#include "types.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <random>
#include <thread>
#include <vector>

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

    // Storage Metrics
    ASSERT_EQ(metrics.get_allocated_size(), 0);
    ASSERT_EQ(metrics.get_total_capacity(),0);
    ASSERT_DOUBLE_EQ(metrics.get_global_used_ratio(), 0.0);

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

    // Eviction Metrics
    ASSERT_EQ(metrics.get_eviction_success(), 0);
    ASSERT_EQ(metrics.get_eviction_attempts(), 0);
    ASSERT_EQ(metrics.get_evicted_key_count(), 0);
    ASSERT_EQ(metrics.get_evicted_size(), 0);
}

TEST_F(MasterMetricsTest, BasicRequestTest) {
    const uint64_t default_kv_lease_ttl = 100;
    auto& metrics = MasterMetricManager::instance();
    // Use a wrapped master service to test the metrics manager
    WrappedMasterService service_(
        false, default_kv_lease_ttl, true);

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
    std::vector<uint64_t> slice_lengths = {value_length};
    ReplicateConfig config;
    config.replica_num = 1;

    // Test MountSegment request
    ASSERT_EQ(ErrorCode::OK,
            service_.MountSegment(segment, client_id).error_code);
    ASSERT_EQ(metrics.get_allocated_size(), 0);
    ASSERT_EQ(metrics.get_total_capacity(), kSegmentSize);
    ASSERT_DOUBLE_EQ(metrics.get_global_used_ratio(), 0.0);
    ASSERT_EQ(metrics.get_mount_segment_requests(), 1);
    ASSERT_EQ(metrics.get_mount_segment_failures(), 0);

    // Test PutStart and PutRevoke request
    ASSERT_EQ(ErrorCode::OK,
              service_.PutStart(key, value_length, slice_lengths, config).error_code);
    ASSERT_EQ(metrics.get_key_count(), 1);
    ASSERT_EQ(metrics.get_allocated_size(), value_length);
    ASSERT_EQ(metrics.get_put_start_requests(), 1);
    ASSERT_EQ(metrics.get_put_start_failures(), 0);
    ASSERT_EQ(ErrorCode::OK, service_.PutRevoke(key).error_code);
    ASSERT_EQ(metrics.get_key_count(), 0);
    ASSERT_EQ(metrics.get_allocated_size(), 0);
    ASSERT_EQ(metrics.get_put_revoke_requests(), 1);
    ASSERT_EQ(metrics.get_put_revoke_failures(), 0);

    // Test PutStart and PutEnd request
    ASSERT_EQ(ErrorCode::OK,
              service_.PutStart(key, value_length, slice_lengths, config).error_code);
    ASSERT_EQ(metrics.get_key_count(), 1);
    ASSERT_EQ(metrics.get_allocated_size(), value_length);
    ASSERT_EQ(metrics.get_put_start_requests(), 2);
    ASSERT_EQ(metrics.get_put_start_failures(), 0);
    ASSERT_EQ(ErrorCode::OK, service_.PutEnd(key).error_code);
    ASSERT_EQ(metrics.get_key_count(), 1);
    ASSERT_EQ(metrics.get_allocated_size(), value_length);
    ASSERT_EQ(metrics.get_put_end_requests(), 1);
    ASSERT_EQ(metrics.get_put_end_failures(), 0);

    // Test ExistKey request
    ASSERT_EQ(ErrorCode::OK, service_.ExistKey(key).error_code);
    ASSERT_EQ(metrics.get_exist_key_requests(), 1);
    ASSERT_EQ(metrics.get_exist_key_failures(), 0);

    // Test GetReplicaList request
    ASSERT_EQ(ErrorCode::OK, service_.GetReplicaList(key).error_code);
    ASSERT_EQ(metrics.get_get_replica_list_requests(), 1);
    ASSERT_EQ(metrics.get_get_replica_list_failures(), 0);

    // Test Remove request
    std::this_thread::sleep_for(std::chrono::milliseconds(default_kv_lease_ttl));
    ASSERT_EQ(ErrorCode::OK, service_.Remove(key).error_code);
    ASSERT_EQ(metrics.get_remove_requests(), 1);
    ASSERT_EQ(metrics.get_remove_failures(), 0);
    ASSERT_EQ(metrics.get_key_count(), 0);
    ASSERT_EQ(metrics.get_allocated_size(), 0);

    // Test RemoveAll request
    ASSERT_EQ(ErrorCode::OK,
              service_.PutStart(key, value_length, slice_lengths, config).error_code);
    ASSERT_EQ(ErrorCode::OK, service_.PutEnd(key).error_code);
    ASSERT_EQ(metrics.get_key_count(), 1);
    ASSERT_EQ(1, service_.RemoveAll().removed_count);
    ASSERT_EQ(metrics.get_remove_all_requests(), 1);
    ASSERT_EQ(metrics.get_remove_all_failures(), 0);
    ASSERT_EQ(metrics.get_key_count(), 0);
    ASSERT_EQ(metrics.get_allocated_size(), 0);

    // Test UnmountSegment request
    ASSERT_EQ(ErrorCode::OK,
              service_.PutStart(key, value_length, slice_lengths, config).error_code);
    ASSERT_EQ(ErrorCode::OK, service_.PutEnd(key).error_code);
    ASSERT_EQ(ErrorCode::OK, service_.UnmountSegment(segment_id, client_id).error_code);
    ASSERT_EQ(metrics.get_unmount_segment_requests(), 1);
    ASSERT_EQ(metrics.get_unmount_segment_failures(), 0);
    ASSERT_EQ(metrics.get_key_count(), 0);
    ASSERT_EQ(metrics.get_allocated_size(), 0);
    ASSERT_EQ(metrics.get_total_capacity(), 0);
    ASSERT_DOUBLE_EQ(metrics.get_global_used_ratio(), 0.0);
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

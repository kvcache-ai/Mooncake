#include "master_service.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <memory>
#include <vector>
#include <thread>
#include <atomic>
#include <mutex>

#include "types.h"

namespace mooncake::test {

class MasterServiceTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("MasterServiceTest");
        FLAGS_logtostderr = true;
    }

    std::vector<ReplicaInfo> replica_list;

    void TearDown() override { google::ShutdownGoogleLogging(); }
};

TEST_F(MasterServiceTest, MountUnmountSegment) {
    // Create a MasterService instance for testing.
    std::unique_ptr<MasterService> service_(new MasterService());
    // Define a constant buffer address for the segment.
    constexpr size_t kBufferAddress = 0x300000000;
    // Define the size of the segment (16MB).
    constexpr size_t kSegmentSize = 1024 * 1024 * 16;
    // Define the name of the test segment.
    std::string segment_name = "test_segment";

    // Test invalid parameters.
    // Invalid buffer address (0).
    EXPECT_EQ(ErrorCode::INVALID_PARAMS,
              service_->MountSegment(0, kSegmentSize, segment_name));
    // Invalid segment size (0).
    EXPECT_EQ(ErrorCode::INVALID_PARAMS,
              service_->MountSegment(kBufferAddress, 0, segment_name));

    // Test normal mount operation.
    EXPECT_EQ(ErrorCode::OK, service_->MountSegment(
                                 kBufferAddress, kSegmentSize, segment_name));

    // Test mounting the same segment again (should fail).
    EXPECT_EQ(
        ErrorCode::INVALID_PARAMS,
        service_->MountSegment(kBufferAddress, kSegmentSize, segment_name));

    // Test unmounting the segment.
    EXPECT_EQ(ErrorCode::OK, service_->UnmountSegment(segment_name));

    // Test unmounting a non-existent segment (should fail).
    EXPECT_EQ(ErrorCode::INVALID_PARAMS,
              service_->UnmountSegment("non_existent"));

    // Test remounting after unmount.
    EXPECT_EQ(ErrorCode::OK, service_->MountSegment(
                                 kBufferAddress, kSegmentSize, segment_name));
    EXPECT_EQ(ErrorCode::OK, service_->UnmountSegment(segment_name));
}

TEST_F(MasterServiceTest, ConcurrentMountUnmount) {
    std::unique_ptr<MasterService> service_(new MasterService());
    constexpr size_t num_threads = 4;
    constexpr size_t iterations = 100;
    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};

    // Launch multiple threads to mount/unmount segments concurrently
    for (size_t i = 0; i < num_threads; i++) {
        threads.emplace_back([&service_, i, &success_count]() {
            std::string segment_name = "segment_" + std::to_string(i);
            size_t buffer = 0x300000000 + i * 0x10000000;
            constexpr size_t size = 16 * 1024 * 1024;

            for (size_t j = 0; j < iterations; j++) {
                if (service_->MountSegment(buffer, size, segment_name) ==
                    ErrorCode::OK) {
                    EXPECT_EQ(ErrorCode::OK,
                              service_->UnmountSegment(segment_name));
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

TEST_F(MasterServiceTest, PutStartInvalidParams) {
    std::unique_ptr<MasterService> service_(new MasterService());
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 16;
    std::string segment_name = "test_segment";
    ASSERT_EQ(ErrorCode::OK,
              service_->MountSegment(buffer, size, segment_name));

    std::string key = "test_key";
    ReplicateConfig config;

    // Test invalid replica_num
    config.replica_num = 0;
    EXPECT_EQ(ErrorCode::INVALID_PARAMS,
              service_->PutStart(key, 1024, {1024}, config, replica_list));

    // Test empty slice_lengths
    config.replica_num = 1;
    std::vector<uint64_t> empty_slices;
    EXPECT_EQ(
        ErrorCode::INVALID_PARAMS,
        service_->PutStart(key, 1024, empty_slices, config, replica_list));

    // Test slice_lengths sum mismatch
    EXPECT_EQ(ErrorCode::INVALID_PARAMS,
              service_->PutStart(key, 1024, {512}, config, replica_list));
}

TEST_F(MasterServiceTest, PutStartEndFlow) {
    std::unique_ptr<MasterService> service_(new MasterService());
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 16;
    std::string segment_name = "test_segment";

    ASSERT_EQ(ErrorCode::OK,
              service_->MountSegment(buffer, size, segment_name));

    // Test PutStart
    std::string key = "test_key";
    uint64_t value_length = 1024;
    std::vector<uint64_t> slice_lengths = {value_length};
    ReplicateConfig config;
    config.replica_num = 1;

    EXPECT_EQ(ErrorCode::OK,
              service_->PutStart(key, value_length, slice_lengths, config,
                                 replica_list));
    EXPECT_FALSE(replica_list.empty());
    EXPECT_EQ(ReplicaStatus::PROCESSING, replica_list[0].status);

    // During put, Get/Remove should fail
    EXPECT_EQ(ErrorCode::REPLICA_IS_NOT_READY,
              service_->GetReplicaList(key, replica_list));
    EXPECT_EQ(ErrorCode::REPLICA_IS_NOT_READY, service_->Remove(key));

    // Test PutEnd
    EXPECT_EQ(ErrorCode::OK, service_->PutEnd(key));

    // Verify replica list after PutEnd
    EXPECT_EQ(ErrorCode::OK, service_->GetReplicaList(key, replica_list));
    EXPECT_EQ(1, replica_list.size());
    EXPECT_EQ(ReplicaStatus::COMPLETE, replica_list[0].status);
}

TEST_F(MasterServiceTest, GetReplicaList) {
    std::unique_ptr<MasterService> service_(new MasterService());
    // Test getting non-existent key
    std::vector<ReplicaInfo> replica_list_local;
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND,
              service_->GetReplicaList("non_existent", replica_list_local));
    EXPECT_TRUE(replica_list_local.empty());

    // Mount segment and put an object
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 16;
    std::string segment_name = "test_segment";
    ASSERT_EQ(ErrorCode::OK,
              service_->MountSegment(buffer, size, segment_name));

    std::string key = "test_key";
    std::vector<uint64_t> slice_lengths = {1024};
    ReplicateConfig config;
    config.replica_num = 1;
    std::vector<ReplicaInfo> replica_list;
    ASSERT_EQ(ErrorCode::OK, service_->PutStart(key, 1024, slice_lengths,
                                                config, replica_list));
    ASSERT_EQ(ErrorCode::OK, service_->PutEnd(key));

    // Test getting existing key
    EXPECT_EQ(ErrorCode::OK, service_->GetReplicaList(key, replica_list_local));
    EXPECT_FALSE(replica_list_local.empty());
}

TEST_F(MasterServiceTest, RemoveObject) {
    std::unique_ptr<MasterService> service_(new MasterService());
    // Mount segment and put an object
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 16;
    std::string segment_name = "test_segment";
    ASSERT_EQ(ErrorCode::OK,
              service_->MountSegment(buffer, size, segment_name));

    std::string key = "test_key";
    std::vector<uint64_t> slice_lengths = {1024};
    ReplicateConfig config;
    config.replica_num = 1;
    std::vector<ReplicaInfo> replica_list;
    ASSERT_EQ(ErrorCode::OK, service_->PutStart(key, 1024, slice_lengths,
                                                config, replica_list));
    ASSERT_EQ(ErrorCode::OK, service_->PutEnd(key));

    // Test removing the object
    EXPECT_EQ(ErrorCode::OK, service_->Remove(key));

    // Verify object is removed
    std::vector<ReplicaInfo> replica_list_local;
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND,
              service_->GetReplicaList(key, replica_list_local));

    // Test removing non-existent object
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, service_->Remove("non_existent"));
}

TEST_F(MasterServiceTest, MultiSliceMultiReplicaFlow) {
    std::unique_ptr<MasterService> service_(new MasterService());
    
    // Mount a segment with sufficient size for multiple replicas
    constexpr size_t buffer = 0x300000000;
    constexpr size_t segment_size = 1024 * 1024 * 64;  // 64MB to accommodate multiple replicas
    std::string segment_name = "test_segment_multi";
    ASSERT_EQ(ErrorCode::OK, service_->MountSegment(buffer, segment_size, segment_name));

    // Test parameters
    std::string key = "multi_slice_object";
    constexpr size_t num_replicas = 3;
    constexpr size_t total_size = 1024 * 1024 * 5;  // 5MB total size
    
    // Create multiple slices of different sizes
    std::vector<uint64_t> slice_lengths = {
        1024 * 1024 * 2,     // 2MB
        1024 * 1024 * 1,     // 1MB
        1024 * 1024 * 1,     // 1MB
        1024 * 1024 * 1      // 1MB
    };
    
    // Verify total size matches sum of slices
    uint64_t sum_slices = 0;
    for (const auto& size : slice_lengths) {
        sum_slices += size;
    }
    ASSERT_EQ(total_size, sum_slices);

    // Configure replication
    ReplicateConfig config;
    config.replica_num = num_replicas;
    std::vector<ReplicaInfo> replica_list;

    // Test PutStart with multiple slices and replicas
    ASSERT_EQ(ErrorCode::OK, 
              service_->PutStart(key, total_size, slice_lengths, config, replica_list));

    // Verify replica list properties
    ASSERT_EQ(num_replicas, replica_list.size());
    for (const auto& replica : replica_list) {
        // Verify replica status
        EXPECT_EQ(ReplicaStatus::PROCESSING, replica.status);
        
        // Verify number of handles matches number of slices
        ASSERT_EQ(slice_lengths.size(), replica.handles.size());
        
        // Verify each handle's properties
        for (size_t i = 0; i < replica.handles.size(); i++) {
            const auto& handle = replica.handles[i];
            EXPECT_EQ(BufStatus::INIT, handle->status);
            EXPECT_EQ(key, handle->replica_meta.object_name);
            EXPECT_EQ(slice_lengths[i], handle->size);
        }
    }

    // Test GetReplicaList during processing (should fail)
    std::vector<ReplicaInfo> retrieved_replicas;
    EXPECT_EQ(ErrorCode::REPLICA_IS_NOT_READY,
              service_->GetReplicaList(key, retrieved_replicas));

    // Complete the put operation
    ASSERT_EQ(ErrorCode::OK, service_->PutEnd(key));

    // Test GetReplicaList after completion
    ASSERT_EQ(ErrorCode::OK, service_->GetReplicaList(key, retrieved_replicas));
    ASSERT_EQ(num_replicas, retrieved_replicas.size());

    // Verify final state of all replicas
    for (const auto& replica : retrieved_replicas) {
        EXPECT_EQ(ReplicaStatus::COMPLETE, replica.status);
        ASSERT_EQ(slice_lengths.size(), replica.handles.size());
        
        for (const auto& handle : replica.handles) {
            EXPECT_EQ(BufStatus::COMPLETE, handle->status);
            EXPECT_EQ(key, handle->replica_meta.object_name);
        }
    }

    // Test successful removal
    EXPECT_EQ(ErrorCode::OK, service_->Remove(key));

    // Verify object is truly removed
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND,
              service_->GetReplicaList(key, retrieved_replicas));
}

}  // namespace mooncake::test

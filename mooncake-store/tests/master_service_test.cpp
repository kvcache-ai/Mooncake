#include "master_service.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <random>
#include <thread>
#include <vector>

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
    // Base is not aligned
    EXPECT_EQ(
        ErrorCode::INVALID_PARAMS,
        service_->MountSegment(kBufferAddress + 1, kSegmentSize, segment_name));
    // Size is not aligned
    EXPECT_EQ(
        ErrorCode::INVALID_PARAMS,
        service_->MountSegment(kBufferAddress, kSegmentSize + 1, segment_name));

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

TEST_F(MasterServiceTest, RandomMountUnmountSegment) {
    // Create a MasterService instance for testing.
    std::unique_ptr<MasterService> service_(new MasterService());
    // Define a constant buffer address for the segment.
    constexpr size_t kBufferAddress = 0x300000000;
    // Define the name of the test segment.
    std::string segment_name = "test_random_segment";
    size_t times = 10;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(1, 10);
    while (times--) {
        int random_number = dis(gen);
        // Define the size of the segment (16MB).
        size_t kSegmentSize = 1024 * 1024 * 16 * random_number;
        // Test remounting after unmount.
        EXPECT_EQ(
            ErrorCode::OK,
            service_->MountSegment(kBufferAddress, kSegmentSize, segment_name));
        EXPECT_EQ(ErrorCode::OK, service_->UnmountSegment(segment_name));
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

TEST_F(MasterServiceTest, RandomPutStartEndFlow) {
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
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(1, 5);
    int random_number = dis(gen);
    config.replica_num = random_number;
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
    EXPECT_EQ(random_number, replica_list.size());
    for (int i = 0; i < random_number; ++i) {
        EXPECT_EQ(ReplicaStatus::COMPLETE, replica_list[i].status);
    }
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

TEST_F(MasterServiceTest, RandomRemoveObject) {
    std::unique_ptr<MasterService> service_(new MasterService());
    // Mount segment and put an object
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 16;
    std::string segment_name = "test_segment";
    ASSERT_EQ(ErrorCode::OK,
              service_->MountSegment(buffer, size, segment_name));
    int times = 10;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(1, 1000);
    while (times--) {
        std::string key = "test_key" + std::to_string(dis(gen));
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
    }
}

TEST_F(MasterServiceTest, MultiSliceMultiReplicaFlow) {
    std::unique_ptr<MasterService> service_(new MasterService());

    // Mount a segment with sufficient size for multiple replicas
    constexpr size_t buffer = 0x300000000;
    constexpr size_t segment_size =
        1024 * 1024 * 64;  // 64MB to accommodate multiple replicas
    std::string segment_name = "test_segment_multi";
    ASSERT_EQ(ErrorCode::OK,
              service_->MountSegment(buffer, segment_size, segment_name));

    // Test parameters
    std::string key = "multi_slice_object";
    constexpr size_t num_replicas = 3;
    constexpr size_t total_size = 1024 * 1024 * 5;  // 5MB total size

    // Create multiple slices of different sizes
    std::vector<uint64_t> slice_lengths = {
        1024 * 1024 * 2,  // 2MB
        1024 * 1024 * 1,  // 1MB
        1024 * 1024 * 1,  // 1MB
        1024 * 1024 * 1   // 1MB
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
    ASSERT_EQ(ErrorCode::OK, service_->PutStart(key, total_size, slice_lengths,
                                                config, replica_list));

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

    // Sleep for 2 seconds to ensure the object is marked for GC
    std::this_thread::sleep_for(std::chrono::seconds(2));
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, service_->Remove(key));

    // Verify object is truly removed
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND,
              service_->GetReplicaList(key, retrieved_replicas));
}

TEST_F(MasterServiceTest, ConcurrentGarbageCollectionTest) {
    std::unique_ptr<MasterService> service_(new MasterService());

    // Mount segment for testing
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size =
        1024 * 1024 * 256;  // Larger segment for concurrent use
    std::string segment_name = "concurrent_gc_segment";
    ASSERT_EQ(ErrorCode::OK,
              service_->MountSegment(buffer, size, segment_name));

    constexpr size_t num_threads = 4;
    constexpr size_t objects_per_thread = 25;
    constexpr size_t total_objects = num_threads * objects_per_thread;

    // Create a vector to track all created objects
    std::vector<std::string> all_keys;
    all_keys.reserve(total_objects);

    // Phase 1: Create all objects
    {
        std::vector<std::thread> create_threads;
        std::mutex keys_mutex;

        for (size_t t = 0; t < num_threads; t++) {
            create_threads.emplace_back([&, t]() {
                for (size_t i = 0; i < objects_per_thread; i++) {
                    std::string key = "concurrent_gc_key_" + std::to_string(t) +
                                      "_" + std::to_string(i);
                    std::vector<uint64_t> slice_lengths = {1024};
                    ReplicateConfig config;
                    config.replica_num = 1;
                    std::vector<ReplicaInfo> replica_list;

                    // Create the object
                    ASSERT_EQ(ErrorCode::OK,
                              service_->PutStart(key, 1024, slice_lengths,
                                                 config, replica_list));
                    ASSERT_EQ(ErrorCode::OK, service_->PutEnd(key));

                    // Add the key to the tracking list
                    {
                        std::lock_guard<std::mutex> lock(keys_mutex);
                        all_keys.push_back(key);
                    }
                }
            });
        }

        // Wait for all object creation to complete
        for (auto& thread : create_threads) {
            thread.join();
        }
    }
    // Verify all objects were created
    ASSERT_EQ(total_objects, all_keys.size());

    // Check that all objects exist
    for (const auto& key : all_keys) {
        std::vector<ReplicaInfo> retrieved_replicas;
        EXPECT_EQ(ErrorCode::OK,
                  service_->GetReplicaList(key, retrieved_replicas));
    }

    // Sleep for 2 seconds to ensure the object is marked for GC
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // Verify all objects are gone after GC
    size_t found_count = 0;
    for (const auto& key : all_keys) {
        std::vector<ReplicaInfo> retrieved_replicas;
        if (service_->GetReplicaList(key, retrieved_replicas) ==
            ErrorCode::OK) {
            found_count++;
        }
    }

    // All objects should have been garbage collected
    EXPECT_EQ(0, found_count);
}
TEST_F(MasterServiceTest, CleanupStaleHandlesTest) {
    std::unique_ptr<MasterService> service_(new MasterService());

    // Mount a segment for testing
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 16;  // 16MB
    std::string segment_name = "test_segment";

    // Mount the segment
    ASSERT_EQ(ErrorCode::OK,
              service_->MountSegment(buffer, size, segment_name));

    // Create an object that will be stored in the segment
    std::string key = "segment_object";
    std::vector<uint64_t> slice_lengths = {1024 * 1024};  // One 1MB slice
    ReplicateConfig config;
    config.replica_num = 1;  // One replica

    // Create the object
    std::vector<ReplicaInfo> replica_list;
    ASSERT_EQ(ErrorCode::OK, service_->PutStart(key, 1024 * 1024, slice_lengths,
                                                config, replica_list));
    ASSERT_EQ(ErrorCode::OK, service_->PutEnd(key));

    // Verify object exists
    std::vector<ReplicaInfo> retrieved_replicas;
    ASSERT_EQ(ErrorCode::OK, service_->GetReplicaList(key, retrieved_replicas));
    ASSERT_EQ(1, retrieved_replicas.size());

    // Unmount the segment
    ASSERT_EQ(ErrorCode::OK, service_->UnmountSegment(segment_name));

    // Try to get the object - it should be automatically removed since the
    // replica is invalid
    retrieved_replicas.clear();
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND,
              service_->GetReplicaList(key, retrieved_replicas));
    EXPECT_TRUE(retrieved_replicas.empty());

    // Mount the segment again
    ASSERT_EQ(ErrorCode::OK,
              service_->MountSegment(buffer, size, segment_name));

    // Create another object
    std::string key2 = "another_segment_object";
    ASSERT_EQ(ErrorCode::OK,
              service_->PutStart(key2, 1024 * 1024, slice_lengths, config,
                                 replica_list));
    ASSERT_EQ(ErrorCode::OK, service_->PutEnd(key2));

    // Verify we can get it
    retrieved_replicas.clear();
    ASSERT_EQ(ErrorCode::OK,
              service_->GetReplicaList(key2, retrieved_replicas));

    // Unmount the segment
    ASSERT_EQ(ErrorCode::OK, service_->UnmountSegment(segment_name));

    // Try to remove the object that should already be cleaned up
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, service_->Remove(key2));
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

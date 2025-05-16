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

    std::vector<Replica::Descriptor> replica_list;

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
    std::vector<Replica::Descriptor> replica_list_local;
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
    std::vector<Replica::Descriptor> replica_list;
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
    std::vector<Replica::Descriptor> replica_list;
    ASSERT_EQ(ErrorCode::OK, service_->PutStart(key, 1024, slice_lengths,
                                                config, replica_list));
    ASSERT_EQ(ErrorCode::OK, service_->PutEnd(key));

    // Test removing the object
    EXPECT_EQ(ErrorCode::OK, service_->Remove(key));

    // Verify object is removed
    std::vector<Replica::Descriptor> replica_list_local;
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
        std::vector<Replica::Descriptor> replica_list;
        ASSERT_EQ(ErrorCode::OK, service_->PutStart(key, 1024, slice_lengths,
                                                    config, replica_list));
        ASSERT_EQ(ErrorCode::OK, service_->PutEnd(key));

        // Test removing the object
        EXPECT_EQ(ErrorCode::OK, service_->Remove(key));

        // Verify object is removed
        std::vector<Replica::Descriptor> replica_list_local;
        EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND,
                  service_->GetReplicaList(key, replica_list_local));
    }
}

TEST_F(MasterServiceTest, RemoveAll) {
    const uint64_t kv_lease_ttl = 50;
    std::unique_ptr<MasterService> service_(new MasterService(false, kv_lease_ttl));
    // Mount segment and put 10 objects
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 16;
    std::string segment_name = "test_segment";
    ASSERT_EQ(ErrorCode::OK,
              service_->MountSegment(buffer, size, segment_name));
    int times = 10;
    while (times--) {
        std::string key = "test_key" + std::to_string(times);
        std::vector<uint64_t> slice_lengths = {1024};
        ReplicateConfig config;
        config.replica_num = 1;
        std::vector<Replica::Descriptor> replica_list;
        ASSERT_EQ(ErrorCode::OK, service_->PutStart(key, 1024, slice_lengths,
                                                    config, replica_list));
        ASSERT_EQ(ErrorCode::OK, service_->PutEnd(key));
        ASSERT_EQ(ErrorCode::OK, service_->ExistKey(key));
    }
    // wait for all the lease to expire
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));
    ASSERT_EQ(10, service_->RemoveAll());
    times = 10;
    while (times--) {
        std::string key = "test_key" + std::to_string(times);
        ASSERT_EQ(ErrorCode::OBJECT_NOT_FOUND, service_->ExistKey(key));
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
    std::vector<Replica::Descriptor> replica_list;

    // Test PutStart with multiple slices and replicas
    ASSERT_EQ(ErrorCode::OK, service_->PutStart(key, total_size, slice_lengths,
                                                config, replica_list));

    // Verify replica list properties
    ASSERT_EQ(num_replicas, replica_list.size());
    for (const auto& replica : replica_list) {
        // Verify replica status
        EXPECT_EQ(ReplicaStatus::PROCESSING, replica.status);

        // Verify number of handles matches number of slices
        ASSERT_EQ(slice_lengths.size(), replica.buffer_descriptors.size());

        // Verify each handle's properties
        for (size_t i = 0; i < replica.buffer_descriptors.size(); i++) {
            const auto& handle = replica.buffer_descriptors[i];
            EXPECT_EQ(BufStatus::INIT, handle.status_);

            EXPECT_EQ(slice_lengths[i], handle.size_);
        }
    }

    // Test GetReplicaList during processing (should fail)
    std::vector<Replica::Descriptor> retrieved_replicas;
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
        ASSERT_EQ(slice_lengths.size(), replica.buffer_descriptors.size());
        for (const auto& handle : replica.buffer_descriptors) {
            EXPECT_EQ(BufStatus::COMPLETE, handle.status_);
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
                    std::vector<Replica::Descriptor> replica_list;

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
        std::vector<Replica::Descriptor> retrieved_replicas;
        EXPECT_EQ(ErrorCode::OK,
                  service_->GetReplicaList(key, retrieved_replicas));
    }

    // Sleep for 2 seconds to ensure the object is marked for GC
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // Verify all objects are gone after GC
    size_t found_count = 0;
    for (const auto& key : all_keys) {
        std::vector<Replica::Descriptor> retrieved_replicas;
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
    std::vector<Replica::Descriptor> replica_list;
    ASSERT_EQ(ErrorCode::OK, service_->PutStart(key, 1024 * 1024, slice_lengths,
                                                config, replica_list));
    ASSERT_EQ(ErrorCode::OK, service_->PutEnd(key));

    // Verify object exists
    std::vector<Replica::Descriptor> retrieved_replicas;
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


TEST_F(MasterServiceTest, ConcurrentWriteAndRemoveAll) {
    std::unique_ptr<MasterService> service_(new MasterService());
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 256;  // 256MB for concurrent testing
    std::string segment_name = "concurrent_segment";
    ASSERT_EQ(ErrorCode::OK, service_->MountSegment(buffer, size, segment_name));

    constexpr int num_threads = 4;
    constexpr int objects_per_thread = 100;
    std::atomic success_writes(0);
    std::atomic remove_all_done(false);
    std::atomic total_removed(0);

    // Writer threads
    std::vector<std::thread> writers;
    for (int i = 0; i < num_threads; ++i) {
        writers.emplace_back([&, i]() {
            for (int j = 0; j < objects_per_thread; ++j) {
                std::string key = "key_" + std::to_string(i) + "_" + std::to_string(j);
                std::vector<uint64_t> slice_lengths = {1024};
                ReplicateConfig config;
                config.replica_num = 1;
                std::vector<Replica::Descriptor> replica_list;

                if (service_->PutStart(key, 1024, slice_lengths, config, replica_list) == ErrorCode::OK &&
                    service_->PutEnd(key) == ErrorCode::OK) {
                    success_writes++;
                }

                // Random sleep to increase concurrency complexity
                std::this_thread::sleep_for(std::chrono::milliseconds(rand() % 10));
            }
        });
    }

    // RemoveAll thread
    std::thread remove_thread([&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(50)); // Let some writes start
        long removed = service_->RemoveAll();
        LOG(INFO) << "Removed " << removed << " objects during concurrent writes";
        ASSERT_GT(removed, 0);
        remove_all_done = true;
        total_removed.fetch_add(removed);
    });

    // Join all threads
    for (auto& t : writers) {
        t.join();
    }
    remove_thread.join();

    // Verify results
    EXPECT_GT(success_writes, 0);
    EXPECT_TRUE(remove_all_done);

    // Final RemoveAll to ensure clean state
    long final_removed = service_->RemoveAll();
    LOG(INFO) << "Final RemoveAll removed " << final_removed << " objects";
    ASSERT_GT(final_removed, 0);
    total_removed.fetch_add(final_removed);
    ASSERT_EQ(total_removed, num_threads * objects_per_thread);
}


TEST_F(MasterServiceTest, ConcurrentReadAndRemoveAll) {
    // set a large kv_lease_ttl so the granted lease will not quickly expire 
    const uint64_t kv_lease_ttl = 200;
    std::unique_ptr<MasterService> service_(new MasterService(false, kv_lease_ttl));
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 256;  // 256MB for concurrent testing
    std::string segment_name = "concurrent_segment";
    ASSERT_EQ(ErrorCode::OK, service_->MountSegment(buffer, size, segment_name));

    // Pre-populate with test data
    constexpr int num_objects = 1000;
    for (int i = 0; i < num_objects; ++i) {
        std::string key = "pre_key_" + std::to_string(i);
        std::vector<uint64_t> slice_lengths = {1024};
        ReplicateConfig config;
        config.replica_num = 1;
        std::vector<Replica::Descriptor> replica_list;

        ASSERT_EQ(ErrorCode::OK, service_->PutStart(key, 1024, slice_lengths, config, replica_list));
        ASSERT_EQ(ErrorCode::OK, service_->PutEnd(key));
    }

    std::atomic<int> success_reads(0);
    std::atomic<bool> remove_all_done(false);

    // Reader threads
    std::vector<std::thread> readers;
    for (int i = 0; i < 4; ++i) {
        readers.emplace_back([&, i]() {
            std::vector<Replica::Descriptor> replica_list;
            for (int j = 0; j < num_objects; ++j) {
                std::string key = "pre_key_" + std::to_string(j);
                if (service_->GetReplicaList(key, replica_list) == ErrorCode::OK) {
                    success_reads++;
                }

                // Random sleep to increase concurrency complexity
                std::this_thread::sleep_for(std::chrono::milliseconds(rand() % 5));
            }
        });
    }

    // RemoveAll thread
    std::thread remove_thread([&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(10)); // Let some reads start
        long removed = service_->RemoveAll();
        LOG(INFO) << "Removed " << removed << " objects during concurrent reads";
        remove_all_done = true;
    });

    // Join all threads
    for (auto& t : readers) {
        t.join();
    }
    remove_thread.join();

    EXPECT_TRUE(remove_all_done);
    // Verify 0 < success_reads < num_objects
    EXPECT_GT(success_reads, 0);
    EXPECT_NE(success_reads, num_objects);

    // wait for all the lease to expire
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));
    long removed = service_->RemoveAll();
    LOG(INFO) << "Removed " << removed << " objects after kv lease expired";

    // Verify all objects were removed
    std::vector<Replica::Descriptor> replica_list;
    for (int i = 0; i < num_objects; ++i) {
        std::string key = "pre_key_" + std::to_string(i);
        EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, service_->GetReplicaList(key, replica_list));
    }
}

TEST_F(MasterServiceTest, ConcurrentRemoveAllOperations) {
    std::unique_ptr<MasterService> service_(new MasterService());
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 16 * 100;  // 256MB for concurrent testing
    std::string segment_name = "concurrent_segment";
    ASSERT_EQ(ErrorCode::OK, service_->MountSegment(buffer, size, segment_name));

    // Pre-populate with test data
    constexpr int num_objects = 1000000;
    for (int i = 0; i < num_objects; ++i) {
        std::string key = "pre_key_" + std::to_string(i);
        std::vector<uint64_t> slice_lengths = {1024};
        ReplicateConfig config;
        config.replica_num = 1;
        std::vector<Replica::Descriptor> replica_list;

        ASSERT_EQ(ErrorCode::OK, service_->PutStart(key, 1024, slice_lengths, config, replica_list));
        ASSERT_EQ(ErrorCode::OK, service_->PutEnd(key));
    }

    std::atomic<int> remove_all_count(0);

    // Two RemoveAll threads
    std::vector<std::thread> remove_threads;
    for (int i = 0; i < 2; ++i) {
        remove_threads.emplace_back([&]() {
            long removed = service_->RemoveAll();
            LOG(INFO) << "RemoveAll removed " << removed << " objects";
            remove_all_count += removed;
        });
    }

    // Join all threads
    for (auto& t : remove_threads) {
        t.join();
    }

    // Verify results - one RemoveAll should return num_objects, the other 0
    EXPECT_EQ(num_objects, remove_all_count);

    // Verify all objects were removed
    std::vector<Replica::Descriptor> replica_list;
    for (int i = 0; i < num_objects; ++i) {
        std::string key = "pre_key_" + std::to_string(i);
        EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, service_->GetReplicaList(key, replica_list));
    }
}

TEST_F(MasterServiceTest, RemoveLeasedObject) {
    const uint64_t kv_lease_ttl = 50;
    std::unique_ptr<MasterService> service_(new MasterService(false, kv_lease_ttl));
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
    std::vector<Replica::Descriptor> replica_list;

    // Verify lease is granted on ExistsKey
    ASSERT_EQ(ErrorCode::OK, service_->PutStart(key, 1024, slice_lengths,
                                                config, replica_list));
    ASSERT_EQ(ErrorCode::OK, service_->PutEnd(key));
    ASSERT_EQ(ErrorCode::OK, service_->ExistKey(key));
    EXPECT_EQ(ErrorCode::OBJECT_HAS_LEASE, service_->Remove(key));
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));
    EXPECT_EQ(ErrorCode::OK, service_->Remove(key));

    // Verify lease is extended on successive ExistsKey
    ASSERT_EQ(ErrorCode::OK, service_->PutStart(key, 1024, slice_lengths,
                                                config, replica_list));
    ASSERT_EQ(ErrorCode::OK, service_->PutEnd(key));
    ASSERT_EQ(ErrorCode::OK, service_->ExistKey(key));
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));
    ASSERT_EQ(ErrorCode::OK, service_->ExistKey(key));
    EXPECT_EQ(ErrorCode::OBJECT_HAS_LEASE, service_->Remove(key));
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));
    EXPECT_EQ(ErrorCode::OK, service_->Remove(key));

    // Verify lease is granted on GetReplicaList
    ASSERT_EQ(ErrorCode::OK, service_->PutStart(key, 1024, slice_lengths,
                                                config, replica_list));
    ASSERT_EQ(ErrorCode::OK, service_->PutEnd(key));
    ASSERT_EQ(ErrorCode::OK, service_->GetReplicaList(key, replica_list));
    EXPECT_EQ(ErrorCode::OBJECT_HAS_LEASE, service_->Remove(key));
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));
    EXPECT_EQ(ErrorCode::OK, service_->Remove(key));

    // Verify lease is extended on successive GetReplicaList
    ASSERT_EQ(ErrorCode::OK, service_->PutStart(key, 1024, slice_lengths,
                                                config, replica_list));
    ASSERT_EQ(ErrorCode::OK, service_->PutEnd(key));
    ASSERT_EQ(ErrorCode::OK, service_->GetReplicaList(key, replica_list));
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));
    ASSERT_EQ(ErrorCode::OK, service_->GetReplicaList(key, replica_list));
    EXPECT_EQ(ErrorCode::OBJECT_HAS_LEASE, service_->Remove(key));
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));
    EXPECT_EQ(ErrorCode::OK, service_->Remove(key));

    // Verify object is removed
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND,
              service_->GetReplicaList(key, replica_list));
}

TEST_F(MasterServiceTest, RemoveAllLeasedObject) {
    const uint64_t kv_lease_ttl = 50;
    std::unique_ptr<MasterService> service_(new MasterService(false, kv_lease_ttl));
    // Mount segment and put 10 objects, with 5 of them having lease
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 16;
    std::string segment_name = "test_segment";
    ASSERT_EQ(ErrorCode::OK,
              service_->MountSegment(buffer, size, segment_name));
    for (int i = 0; i < 10; ++i) {
        std::string key = "test_key" + std::to_string(i);
        std::vector<uint64_t> slice_lengths = {1024};
        ReplicateConfig config;
        config.replica_num = 1;
        std::vector<Replica::Descriptor> replica_list;
        ASSERT_EQ(ErrorCode::OK, service_->PutStart(key, 1024, slice_lengths,
                                                    config, replica_list));
        ASSERT_EQ(ErrorCode::OK, service_->PutEnd(key));
        if (i >= 5) {
            ASSERT_EQ(ErrorCode::OK, service_->ExistKey(key));
        }
    }
    ASSERT_EQ(5, service_->RemoveAll());
    for (int i = 0; i < 5; ++i) {
        std::string key = "test_key" + std::to_string(i);
        ASSERT_EQ(ErrorCode::OBJECT_NOT_FOUND, service_->ExistKey(key));
    }
    // wait for all the lease to expire
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));
    ASSERT_EQ(5, service_->RemoveAll());
    for (int i = 5; i < 10; ++i) {
        std::string key = "test_key" + std::to_string(i);
        ASSERT_EQ(ErrorCode::OBJECT_NOT_FOUND, service_->ExistKey(key));
    }
}

TEST_F(MasterServiceTest, ObjectEviction) {
    // set a large kv_lease_ttl so the granted lease will not quickly expire
    const uint64_t kv_lease_ttl = 2000;
    std::unique_ptr<MasterService> service_(new MasterService(false, kv_lease_ttl));
    // Mount a segment that can hold about 1024 * 16 objects.
    // As the eviction is processed separately for each shard,
    // we need to fill each shard with enough objects to thoroughly
    // test the eviction process.
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 16 * 15;
    constexpr size_t object_size = 1024 * 15;
    std::string segment_name = "test_segment";
    ASSERT_EQ(ErrorCode::OK,
              service_->MountSegment(buffer, size, segment_name));

    // Verify if we can put objects more than the segment can hold
    // and the last added object is accessible
    int success_puts = 0;
    std::string last_success_key;
    for (int i = 0; i < 1024 * 16 + 10; ++i) {
        std::string key = "test_key" + std::to_string(i);
        std::vector<uint64_t> slice_lengths = {object_size};
        ReplicateConfig config;
        config.replica_num = 1;
        std::vector<Replica::Descriptor> replica_list;
        if (ErrorCode::OK == service_->PutStart(key, object_size,
            slice_lengths, config, replica_list)) {
            ASSERT_EQ(ErrorCode::OK, service_->PutEnd(key));
            success_puts++;
            last_success_key = key;
        } else {
            // wait for gc thread to evict some objects
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }
    ASSERT_GT(success_puts, 1024 * 16);
    ASSERT_EQ(ErrorCode::OK, service_->GetReplicaList(last_success_key, replica_list));
    service_->RemoveAll();
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));
    ASSERT_EQ(service_->RemoveAll(), 1);

    // Verify leased object will not be evicted.
    // 10 failed puts are enough to trigger the eviction.
    // Too many failed puts will cause the master service to print too many logs.
    success_puts = 0;
    int failed_puts = 0;
    for (int i = 0; i < 1024 * 16 + 10 && failed_puts < 10; ++i) {
        std::string key = "test_key" + std::to_string(i);
        std::vector<uint64_t> slice_lengths = {object_size};
        ReplicateConfig config;
        config.replica_num = 1;
        std::vector<Replica::Descriptor> replica_list;
        if (ErrorCode::OK == service_->PutStart(key, object_size,
            slice_lengths, config, replica_list)) {
            ASSERT_EQ(ErrorCode::OK, service_->PutEnd(key));
            // the object is leased
            ASSERT_EQ(ErrorCode::OK, service_->GetReplicaList(key, replica_list));
            success_puts++;
        } else {
            failed_puts++;
        }
    }
    ASSERT_GT(success_puts, 0);
    // wait for gc thread to do eviction
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    // No object should be removed as all objects are leased
    ASSERT_EQ(0, service_->RemoveAll());
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

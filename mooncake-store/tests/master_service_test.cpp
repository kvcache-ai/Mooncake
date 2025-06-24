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

std::string GenerateKeyForSegment(const std::unique_ptr<MasterService>& service,
                                  const std::string& segment_name) {
    static std::atomic<uint64_t> counter(0);

    while (true) {
        std::string key = "key_" + std::to_string(counter.fetch_add(1));
        std::vector<Replica::Descriptor> replica_list;

        // Check if the key already exists.
        if (service->ExistKey(key) == ErrorCode::OK) {
            continue;  // Retry if the key already exists
        }

        // Attempt to put the key.
        ErrorCode code = service->PutStart(key, 1024, {1024},
                                           {.replica_num = 1}, replica_list);

        if (code == ErrorCode::OBJECT_ALREADY_EXISTS) {
            continue;  // Retry if the key already exists
        }
        if (code != ErrorCode::OK) {
            throw std::runtime_error("PutStart failed with code: " +
                                     std::to_string(static_cast<int>(code)));
        }
        service->PutEnd(key);
        if (replica_list[0].buffer_descriptors[0].segment_name_ ==
            segment_name) {
            return key;
        }
        // Clean up failed attempt
        service->Remove(key);
    }
}

TEST_F(MasterServiceTest, MountUnmountSegment) {
    // Create a MasterService instance for testing.
    std::unique_ptr<MasterService> service_(new MasterService());
    // Define a constant buffer address for the segment.
    constexpr size_t kBufferAddress = 0x300000000;
    // Define the size of the segment (16MB).
    constexpr size_t kSegmentSize = 1024 * 1024 * 16;
    // Define the name of the test segment.
    std::string segment_name = "test_segment";
    Segment segment(generate_uuid(), segment_name, kBufferAddress,
                    kSegmentSize);
    UUID client_id = generate_uuid();

    // Test invalid parameters.
    // Invalid buffer address (0).
    segment.base = 0;
    segment.size = kSegmentSize;
    EXPECT_EQ(ErrorCode::INVALID_PARAMS,
              service_->MountSegment(segment, client_id));

    // Invalid segment size (0).
    segment.base = kBufferAddress;
    segment.size = 0;
    EXPECT_EQ(ErrorCode::INVALID_PARAMS,
              service_->MountSegment(segment, client_id));

    // Base is not aligned
    segment.base = kBufferAddress + 1;
    segment.size = kSegmentSize;
    EXPECT_EQ(ErrorCode::INVALID_PARAMS,
              service_->MountSegment(segment, client_id));

    // Size is not aligned
    segment.base = kBufferAddress;
    segment.size = kSegmentSize + 1;
    EXPECT_EQ(ErrorCode::INVALID_PARAMS,
              service_->MountSegment(segment, client_id));

    // Test normal mount operation.
    segment.base = kBufferAddress;
    segment.size = kSegmentSize;
    EXPECT_EQ(ErrorCode::OK, service_->MountSegment(segment, client_id));

    // Test mounting the same segment again (idempotent request should succeed).
    EXPECT_EQ(ErrorCode::OK, service_->MountSegment(segment, client_id));

    // Test unmounting the segment.
    EXPECT_EQ(ErrorCode::OK, service_->UnmountSegment(segment.id, client_id));

    // Test unmounting the same segment again (idempotent request should
    // succeed).
    EXPECT_EQ(ErrorCode::OK, service_->UnmountSegment(segment.id, client_id));

    // Test unmounting a non-existent segment (idempotent request should
    // succeed).
    UUID non_existent_id = generate_uuid();
    EXPECT_EQ(ErrorCode::OK,
              service_->UnmountSegment(non_existent_id, client_id));

    // Test remounting after unmount.
    EXPECT_EQ(ErrorCode::OK, service_->MountSegment(segment, client_id));
    EXPECT_EQ(ErrorCode::OK, service_->UnmountSegment(segment.id, client_id));
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

        Segment segment(segment_id, segment_name, kBufferAddress, kSegmentSize);

        // Test remounting after unmount.
        EXPECT_EQ(ErrorCode::OK, service_->MountSegment(segment, client_id));
        EXPECT_EQ(ErrorCode::OK,
                  service_->UnmountSegment(segment.id, client_id));
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
            Segment segment;
            segment.name = "segment_" + std::to_string(i);
            segment.id = generate_uuid();
            segment.base = 0x300000000 + i * 0x10000000;
            segment.size = 16 * 1024 * 1024;
            UUID client_id = generate_uuid();

            for (size_t j = 0; j < iterations; j++) {
                if (service_->MountSegment(segment, client_id) ==
                    ErrorCode::OK) {
                    EXPECT_EQ(ErrorCode::OK,
                              service_->UnmountSegment(segment.id, client_id));
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

    Segment segment(generate_uuid(), segment_name, buffer, size);
    UUID client_id = generate_uuid();

    ASSERT_EQ(ErrorCode::OK, service_->MountSegment(segment, client_id));

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

    Segment segment(generate_uuid(), segment_name, buffer, size);
    UUID client_id = generate_uuid();

    ASSERT_EQ(ErrorCode::OK, service_->MountSegment(segment, client_id));

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

    Segment segment(generate_uuid(), segment_name, buffer, size);
    UUID client_id = generate_uuid();

    ASSERT_EQ(ErrorCode::OK, service_->MountSegment(segment, client_id));

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

    Segment segment(generate_uuid(), segment_name, buffer, size);
    UUID client_id = generate_uuid();

    ASSERT_EQ(ErrorCode::OK, service_->MountSegment(segment, client_id));

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

    Segment segment(generate_uuid(), segment_name, buffer, size);
    UUID client_id = generate_uuid();

    ASSERT_EQ(ErrorCode::OK, service_->MountSegment(segment, client_id));

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

    Segment segment(generate_uuid(), segment_name, buffer, size);
    UUID client_id = generate_uuid();

    ASSERT_EQ(ErrorCode::OK, service_->MountSegment(segment, client_id));
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
    std::unique_ptr<MasterService> service_(
        new MasterService(false, kv_lease_ttl));
    // Mount segment and put 10 objects
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 16;
    std::string segment_name = "test_segment";

    Segment segment(generate_uuid(), segment_name, buffer, size);
    UUID client_id = generate_uuid();

    ASSERT_EQ(ErrorCode::OK, service_->MountSegment(segment, client_id));
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

    Segment segment(generate_uuid(), segment_name, buffer, segment_size);
    UUID client_id = generate_uuid();

    ASSERT_EQ(ErrorCode::OK, service_->MountSegment(segment, client_id));

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
    Segment segment(generate_uuid(), segment_name, buffer, size);
    UUID client_id = generate_uuid();
    ASSERT_EQ(ErrorCode::OK, service_->MountSegment(segment, client_id));

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

    Segment segment(generate_uuid(), segment_name, buffer, size);
    UUID client_id = generate_uuid();

    // Mount the segment
    ASSERT_EQ(ErrorCode::OK, service_->MountSegment(segment, client_id));

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
    ASSERT_EQ(ErrorCode::OK, service_->UnmountSegment(segment.id, client_id));

    // Try to get the object - it should be automatically removed since the
    // replica is invalid
    retrieved_replicas.clear();
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND,
              service_->GetReplicaList(key, retrieved_replicas));
    EXPECT_TRUE(retrieved_replicas.empty());

    // Mount the segment again
    ASSERT_EQ(ErrorCode::OK, service_->MountSegment(segment, client_id));

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
    ASSERT_EQ(ErrorCode::OK, service_->UnmountSegment(segment.id, client_id));

    // Try to remove the object that should already be cleaned up
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, service_->Remove(key2));
}

TEST_F(MasterServiceTest, ConcurrentWriteAndRemoveAll) {
    std::unique_ptr<MasterService> service_(new MasterService());
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 256;  // 256MB for concurrent testing
    std::string segment_name = "concurrent_segment";
    Segment segment(generate_uuid(), segment_name, buffer, size);
    UUID client_id = generate_uuid();
    ASSERT_EQ(ErrorCode::OK, service_->MountSegment(segment, client_id));

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
                std::string key =
                    "key_" + std::to_string(i) + "_" + std::to_string(j);
                std::vector<uint64_t> slice_lengths = {1024};
                ReplicateConfig config;
                config.replica_num = 1;
                std::vector<Replica::Descriptor> replica_list;

                if (service_->PutStart(key, 1024, slice_lengths, config,
                                       replica_list) == ErrorCode::OK &&
                    service_->PutEnd(key) == ErrorCode::OK) {
                    success_writes++;
                }

                // Random sleep to increase concurrency complexity
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(rand() % 10));
            }
        });
    }

    // RemoveAll thread
    std::thread remove_thread([&]() {
        std::this_thread::sleep_for(
            std::chrono::milliseconds(50));  // Let some writes start
        long removed = service_->RemoveAll();
        LOG(INFO) << "Removed " << removed
                  << " objects during concurrent writes";
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
    std::unique_ptr<MasterService> service_(
        new MasterService(false, kv_lease_ttl));
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 256;  // 256MB for concurrent testing
    std::string segment_name = "concurrent_segment";
    Segment segment(generate_uuid(), segment_name, buffer, size);
    UUID client_id = generate_uuid();
    ASSERT_EQ(ErrorCode::OK, service_->MountSegment(segment, client_id));

    // Pre-populate with test data
    constexpr int num_objects = 1000;
    for (int i = 0; i < num_objects; ++i) {
        std::string key = "pre_key_" + std::to_string(i);
        std::vector<uint64_t> slice_lengths = {1024};
        ReplicateConfig config;
        config.replica_num = 1;
        std::vector<Replica::Descriptor> replica_list;

        ASSERT_EQ(ErrorCode::OK, service_->PutStart(key, 1024, slice_lengths,
                                                    config, replica_list));
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
                if (service_->GetReplicaList(key, replica_list) ==
                    ErrorCode::OK) {
                    success_reads++;
                }

                // Random sleep to increase concurrency complexity
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(rand() % 5));
            }
        });
    }

    // RemoveAll thread
    std::thread remove_thread([&]() {
        std::this_thread::sleep_for(
            std::chrono::milliseconds(10));  // Let some reads start
        long removed = service_->RemoveAll();
        LOG(INFO) << "Removed " << removed
                  << " objects during concurrent reads";
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
        EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND,
                  service_->GetReplicaList(key, replica_list));
    }
}

TEST_F(MasterServiceTest, ConcurrentRemoveAllOperations) {
    std::unique_ptr<MasterService> service_(new MasterService());
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size =
        1024 * 1024 * 16 * 100;  // 256MB for concurrent testing
    std::string segment_name = "concurrent_segment";
    Segment segment(generate_uuid(), segment_name, buffer, size);
    UUID client_id = generate_uuid();
    ASSERT_EQ(ErrorCode::OK, service_->MountSegment(segment, client_id));

    // Pre-populate with test data
    constexpr int num_objects = 1000000;
    for (int i = 0; i < num_objects; ++i) {
        std::string key = "pre_key_" + std::to_string(i);
        std::vector<uint64_t> slice_lengths = {1024};
        ReplicateConfig config;
        config.replica_num = 1;
        std::vector<Replica::Descriptor> replica_list;

        ASSERT_EQ(ErrorCode::OK, service_->PutStart(key, 1024, slice_lengths,
                                                    config, replica_list));
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
        EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND,
                  service_->GetReplicaList(key, replica_list));
    }
}

TEST_F(MasterServiceTest, UnmountSegmentImmediateCleanup) {
    std::unique_ptr<MasterService> service_(new MasterService());

    // Mount two segments for testing
    constexpr size_t buffer1 = 0x300000000;
    constexpr size_t buffer2 = 0x400000000;
    constexpr size_t size = 1024 * 1024 * 16;

    Segment segment1(generate_uuid(), "segment1", buffer1, size);
    Segment segment2(generate_uuid(), "segment2", buffer2, size);
    UUID client_id = generate_uuid();
    ASSERT_EQ(ErrorCode::OK, service_->MountSegment(segment1, client_id));
    ASSERT_EQ(ErrorCode::OK, service_->MountSegment(segment2, client_id));

    // Create two objects in the two segments
    std::string key1 = GenerateKeyForSegment(service_, segment1.name);
    std::string key2 = GenerateKeyForSegment(service_, segment2.name);
    std::vector<uint64_t> slice_lengths = {1024};
    ReplicateConfig config;
    config.replica_num = 1;

    // Unmount segment1
    ASSERT_EQ(ErrorCode::OK, service_->UnmountSegment(segment1.id, client_id));
    // Umount will remove all objects in the segment, include the key1
    ASSERT_EQ(1, service_->GetKeyCount());
    // Verify objects in segment1 is gone
    std::vector<Replica::Descriptor> retrieved;
    ASSERT_EQ(ErrorCode::OBJECT_NOT_FOUND,
              service_->GetReplicaList(key1, retrieved));

    // Verify objects in segment2 is still there
    ASSERT_EQ(ErrorCode::OK, service_->GetReplicaList(key2, retrieved));

    // Verify put key1 will put into segment2 rather than segment1
    ASSERT_EQ(ErrorCode::OK, service_->PutStart(key1, 1024, slice_lengths,
                                                config, replica_list));
    ASSERT_EQ(ErrorCode::OK, service_->PutEnd(key1));
    ASSERT_EQ(ErrorCode::OK, service_->GetReplicaList(key1, retrieved));
    ASSERT_EQ(replica_list[0].buffer_descriptors[0].segment_name_,
              segment2.name);
}

TEST_F(MasterServiceTest, UnmountSegmentPerformance) {
    std::unique_ptr<MasterService> service_(new MasterService());
    constexpr size_t kBufferAddress = 0x300000000;
    constexpr size_t kSegmentSize = 1024 * 1024 * 256;  // 256MB
    std::string segment_name = "perf_test_segment";
    Segment segment(generate_uuid(), segment_name, kBufferAddress,
                    kSegmentSize);
    UUID client_id = generate_uuid();

    // Mount a segment for testing
    ASSERT_EQ(ErrorCode::OK, service_->MountSegment(segment, client_id));

    // Create 10000 keys for testing
    constexpr int kNumKeys = 1000;
    std::vector<std::string> keys;
    keys.reserve(kNumKeys);

    auto start = std::chrono::steady_clock::now();

    // Create `kNumKeys` keys
    for (int i = 0; i < kNumKeys; ++i) {
        std::string key = GenerateKeyForSegment(service_, segment_name);
        keys.push_back(key);
    }

    auto create_end = std::chrono::steady_clock::now();

    // Execute unmount operation and record operation time
    auto unmount_start = std::chrono::steady_clock::now();
    EXPECT_EQ(ErrorCode::OK, service_->UnmountSegment(segment.id, client_id));
    auto unmount_end = std::chrono::steady_clock::now();

    auto unmount_duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(unmount_end -
                                                              unmount_start);

    // Unmount operation should be very fast, so we set 1s limit
    EXPECT_LE(unmount_duration.count(), 1000)
        << "Unmount operation took " << unmount_duration.count()
        << "ms which exceeds 1 second limit";

    // Verify all keys are gone
    std::vector<Replica::Descriptor> retrieved;
    for (const auto& key : keys) {
        EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND,
                  service_->GetReplicaList(key, retrieved));
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

TEST_F(MasterServiceTest, RemoveLeasedObject) {
    const uint64_t kv_lease_ttl = 50;
    std::unique_ptr<MasterService> service_(
        new MasterService(false, kv_lease_ttl));
    // Mount segment and put an object
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 16;
    std::string segment_name = "test_segment";
    Segment segment(generate_uuid(), segment_name, buffer, size);
    UUID client_id = generate_uuid();
    ASSERT_EQ(ErrorCode::OK, service_->MountSegment(segment, client_id));

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
    std::unique_ptr<MasterService> service_(
        new MasterService(false, kv_lease_ttl));
    // Mount segment and put 10 objects, with 5 of them having lease
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 16;
    std::string segment_name = "test_segment";
    Segment segment(generate_uuid(), segment_name, buffer, size);
    UUID client_id = generate_uuid();
    ASSERT_EQ(ErrorCode::OK, service_->MountSegment(segment, client_id));
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

TEST_F(MasterServiceTest, EvictObject) {
    // set a large kv_lease_ttl so the granted lease will not quickly expire
    const uint64_t kv_lease_ttl = 2000;
    std::unique_ptr<MasterService> service_(
        new MasterService(false, kv_lease_ttl));
    // Mount a segment that can hold about 1024 * 16 objects.
    // As the eviction is processed separately for each shard,
    // we need to fill each shard with enough objects to thoroughly
    // test the eviction process.
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 16 * 15;
    constexpr size_t object_size = 1024 * 15;
    std::string segment_name = "test_segment";
    Segment segment(generate_uuid(), segment_name, buffer, size);
    UUID client_id = generate_uuid();
    ASSERT_EQ(ErrorCode::OK, service_->MountSegment(segment, client_id));

    // Verify if we can put objects more than the segment can hold
    int success_puts = 0;
    for (int i = 0; i < 1024 * 16 + 50; ++i) {
        std::string key = "test_key" + std::to_string(i);
        std::vector<uint64_t> slice_lengths = {object_size};
        ReplicateConfig config;
        config.replica_num = 1;
        std::vector<Replica::Descriptor> replica_list;
        if (ErrorCode::OK == service_->PutStart(key, object_size, slice_lengths,
                                                config, replica_list)) {
            ASSERT_EQ(ErrorCode::OK, service_->PutEnd(key));
            success_puts++;
        } else {
            // wait for gc thread to work
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }
    ASSERT_GT(success_puts, 1024 * 16);
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));
    service_->RemoveAll();
}

TEST_F(MasterServiceTest, TryEvictLeasedObject) {
    // set a large kv_lease_ttl so the granted lease will not quickly expire
    const uint64_t kv_lease_ttl = 500;
    std::unique_ptr<MasterService> service_(
        new MasterService(false, kv_lease_ttl));
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 16;
    constexpr size_t object_size = 1024 * 1024;
    std::string segment_name = "test_segment";
    Segment segment(generate_uuid(), segment_name, buffer, size);
    UUID client_id = generate_uuid();
    ASSERT_EQ(ErrorCode::OK, service_->MountSegment(segment, client_id));

    // Verify leased object will not be evicted.
    int success_puts = 0;
    int failed_puts = 0;
    std::vector<std::string> leased_keys;
    for (int i = 0; i < 16 + 10; ++i) {
        std::string key = "test_key" + std::to_string(i);
        std::vector<uint64_t> slice_lengths = {object_size};
        ReplicateConfig config;
        config.replica_num = 1;
        std::vector<Replica::Descriptor> replica_list;
        if (ErrorCode::OK == service_->PutStart(key, object_size, slice_lengths,
                                                config, replica_list)) {
            ASSERT_EQ(ErrorCode::OK, service_->PutEnd(key));
            // the object is leased
            ASSERT_EQ(ErrorCode::OK,
                      service_->GetReplicaList(key, replica_list));
            leased_keys.push_back(key);
            success_puts++;
        } else {
            failed_puts++;
        }
    }
    ASSERT_GT(success_puts, 0);
    ASSERT_GT(failed_puts, 0);
    // wait for gc thread to do eviction
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    // All leased objects should be accessible
    for (const auto& key : leased_keys) {
        std::vector<Replica::Descriptor> replica_list;
        ASSERT_EQ(ErrorCode::OK, service_->GetReplicaList(key, replica_list));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));
    service_->RemoveAll();
}

TEST_F(MasterServiceTest, BatchExistKeyTest) {
    std::unique_ptr<MasterService> service_(new MasterService());

    // Mount a segment
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 128;
    std::string segment_name = "test_segment";
    Segment segment(generate_uuid(), segment_name, buffer, size);
    UUID client_id = generate_uuid();
    ASSERT_EQ(ErrorCode::OK, service_->MountSegment(segment, client_id));

    int test_object_num = 10;
    std::vector<std::string> test_keys;
    for (int i = 0; i < test_object_num; ++i) {
        test_keys.push_back("test_key" + std::to_string(i));
        ReplicateConfig config;
        config.replica_num = 1;
        std::vector<uint64_t> slice_lengths = {1024};
        std::vector<Replica::Descriptor> replica_list;
        ASSERT_EQ(ErrorCode::OK,
                  service_->PutStart(test_keys[i], 1024, slice_lengths, config,
                                     replica_list));
        ASSERT_EQ(ErrorCode::OK, service_->PutEnd(test_keys[i]));
    }

    // Test individual ExistKey calls to verify the underlying functionality
    for (int i = 0; i < test_object_num; ++i) {
        EXPECT_EQ(ErrorCode::OK, service_->ExistKey(test_keys[i]));
    }

    // Tets batch
    test_keys.push_back("non_existent_key");
    auto exist_resp = service_->BatchExistKey(test_keys);
    for (int i = 0; i < test_object_num; ++i) {
        ASSERT_EQ(ErrorCode::OK, exist_resp[i]);
    }
    ASSERT_EQ(ErrorCode::OBJECT_NOT_FOUND, exist_resp[test_object_num]);
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

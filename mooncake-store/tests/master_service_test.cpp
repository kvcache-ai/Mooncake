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
        auto exist_result = service->ExistKey(key);
        if (exist_result.has_value() && exist_result.value()) {
            continue;  // Retry if the key already exists
        }

        // Attempt to put the key.
        auto put_result =
            service->PutStart(key, 1024, {1024}, {.replica_num = 1});
        if (put_result.has_value()) {
            replica_list = std::move(put_result.value());
        }
        ErrorCode code =
            put_result.has_value() ? ErrorCode::OK : put_result.error();

        if (code == ErrorCode::OBJECT_ALREADY_EXISTS) {
            continue;  // Retry if the key already exists
        }
        if (code != ErrorCode::OK) {
            throw std::runtime_error("PutStart failed with code: " +
                                     std::to_string(static_cast<int>(code)));
        }
        auto put_end_result = service->PutEnd(key);
        if (!put_end_result.has_value()) {
            throw std::runtime_error("PutEnd failed");
        }
        if (replica_list[0]
                .get_memory_descriptor()
                .buffer_descriptors[0]
                .segment_name_ == segment_name) {
            return key;
        }
        // Clean up failed attempt
        auto remove_result = service->Remove(key);
        if (!remove_result.has_value()) {
            // Ignore cleanup failure
        }
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
    auto mount_result1 = service_->MountSegment(segment, client_id);
    EXPECT_FALSE(mount_result1.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, mount_result1.error());

    // Invalid segment size (0).
    segment.base = kBufferAddress;
    segment.size = 0;
    auto mount_result2 = service_->MountSegment(segment, client_id);
    EXPECT_FALSE(mount_result2.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, mount_result2.error());

    // Base is not aligned
    segment.base = kBufferAddress + 1;
    segment.size = kSegmentSize;
    auto mount_result3 = service_->MountSegment(segment, client_id);
    EXPECT_FALSE(mount_result3.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, mount_result3.error());

    // Size is not aligned
    segment.base = kBufferAddress;
    segment.size = kSegmentSize + 1;
    auto mount_result4 = service_->MountSegment(segment, client_id);
    EXPECT_FALSE(mount_result4.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, mount_result4.error());

    // Test normal mount operation.
    segment.base = kBufferAddress;
    segment.size = kSegmentSize;
    auto mount_result5 = service_->MountSegment(segment, client_id);
    EXPECT_TRUE(mount_result5.has_value());

    // Test mounting the same segment again (idempotent request should succeed).
    auto mount_result6 = service_->MountSegment(segment, client_id);
    EXPECT_TRUE(mount_result6.has_value());

    // Test unmounting the segment.
    auto unmount_result1 = service_->UnmountSegment(segment.id, client_id);
    EXPECT_TRUE(unmount_result1.has_value());

    // Test unmounting the same segment again (idempotent request should
    // succeed).
    auto unmount_result2 = service_->UnmountSegment(segment.id, client_id);
    EXPECT_TRUE(unmount_result2.has_value());

    // Test unmounting a non-existent segment (idempotent request should
    // succeed).
    UUID non_existent_id = generate_uuid();
    auto unmount_result3 = service_->UnmountSegment(non_existent_id, client_id);
    EXPECT_TRUE(unmount_result3.has_value());

    // Test remounting after unmount.
    auto mount_result7 = service_->MountSegment(segment, client_id);
    EXPECT_TRUE(mount_result7.has_value());
    auto unmount_result4 = service_->UnmountSegment(segment.id, client_id);
    EXPECT_TRUE(unmount_result4.has_value());
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
        auto mount_result = service_->MountSegment(segment, client_id);
        EXPECT_TRUE(mount_result.has_value());
        auto unmount_result = service_->UnmountSegment(segment.id, client_id);
        EXPECT_TRUE(unmount_result.has_value());
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
                auto mount_result = service_->MountSegment(segment, client_id);
                if (mount_result.has_value()) {
                    auto unmount_result =
                        service_->UnmountSegment(segment.id, client_id);
                    EXPECT_TRUE(unmount_result.has_value());
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

    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    std::string key = "test_key";
    ReplicateConfig config;

    // Test invalid replica_num
    config.replica_num = 0;
    auto put_result1 = service_->PutStart(key, 1024, {1024}, config);
    EXPECT_FALSE(put_result1.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, put_result1.error());

    // Test empty slice_lengths
    config.replica_num = 1;
    std::vector<uint64_t> empty_slices;
    auto put_result2 = service_->PutStart(key, 1024, empty_slices, config);
    EXPECT_FALSE(put_result2.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, put_result2.error());

    // Test slice_lengths sum mismatch
    auto put_result3 = service_->PutStart(key, 1024, {512}, config);
    EXPECT_FALSE(put_result3.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, put_result3.error());
}

TEST_F(MasterServiceTest, PutStartEndFlow) {
    std::unique_ptr<MasterService> service_(new MasterService());
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 16;
    std::string segment_name = "test_segment";

    Segment segment(generate_uuid(), segment_name, buffer, size);
    UUID client_id = generate_uuid();

    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    // Test PutStart
    std::string key = "test_key";
    uint64_t value_length = 1024;
    std::vector<uint64_t> slice_lengths = {value_length};
    ReplicateConfig config;
    config.replica_num = 1;

    auto put_start_result =
        service_->PutStart(key, value_length, slice_lengths, config);
    EXPECT_TRUE(put_start_result.has_value());
    replica_list = put_start_result.value();
    EXPECT_FALSE(replica_list.empty());
    EXPECT_EQ(ReplicaStatus::PROCESSING, replica_list[0].status);

    // During put, Get/Remove should fail
    auto get_replica_result = service_->GetReplicaList(key);
    EXPECT_FALSE(get_replica_result.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_NOT_READY, get_replica_result.error());
    auto remove_result = service_->Remove(key);
    EXPECT_FALSE(remove_result.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_NOT_READY, remove_result.error());

    // Test PutEnd
    auto put_end_result = service_->PutEnd(key);
    EXPECT_TRUE(put_end_result.has_value());

    // Verify replica list after PutEnd
    auto final_get_result = service_->GetReplicaList(key);
    EXPECT_TRUE(final_get_result.has_value());
    replica_list = final_get_result.value();
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

    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

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
    auto put_start_result =
        service_->PutStart(key, value_length, slice_lengths, config);
    EXPECT_TRUE(put_start_result.has_value());
    replica_list = put_start_result.value();
    EXPECT_FALSE(replica_list.empty());
    EXPECT_EQ(ReplicaStatus::PROCESSING, replica_list[0].status);
    // During put, Get/Remove should fail
    auto get_result = service_->GetReplicaList(key);
    EXPECT_FALSE(get_result.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_NOT_READY, get_result.error());
    auto remove_result = service_->Remove(key);
    EXPECT_FALSE(remove_result.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_NOT_READY, remove_result.error());
    // Test PutEnd
    auto put_end_result = service_->PutEnd(key);
    EXPECT_TRUE(put_end_result.has_value());
    // Verify replica list after PutEnd
    auto get_result2 = service_->GetReplicaList(key);
    EXPECT_TRUE(get_result2.has_value());
    replica_list = get_result2.value();
    EXPECT_EQ(random_number, replica_list.size());
    for (int i = 0; i < random_number; ++i) {
        EXPECT_EQ(ReplicaStatus::COMPLETE, replica_list[i].status);
    }
}

TEST_F(MasterServiceTest, GetReplicaList) {
    std::unique_ptr<MasterService> service_(new MasterService());
    // Test getting non-existent key
    auto get_result = service_->GetReplicaList("non_existent");
    EXPECT_FALSE(get_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, get_result.error());

    // Mount segment and put an object
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 16;
    std::string segment_name = "test_segment";

    Segment segment(generate_uuid(), segment_name, buffer, size);
    UUID client_id = generate_uuid();

    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    std::string key = "test_key";
    std::vector<uint64_t> slice_lengths = {1024};
    ReplicateConfig config;
    config.replica_num = 1;
    auto put_start_result =
        service_->PutStart(key, 1024, slice_lengths, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto put_end_result = service_->PutEnd(key);
    ASSERT_TRUE(put_end_result.has_value());

    // Test getting existing key
    auto get_result2 = service_->GetReplicaList(key);
    EXPECT_TRUE(get_result2.has_value());
    auto replica_list_local = get_result2.value();
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

    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    std::string key = "test_key";
    std::vector<uint64_t> slice_lengths = {1024};
    ReplicateConfig config;
    config.replica_num = 1;
    auto put_start_result =
        service_->PutStart(key, 1024, slice_lengths, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto put_end_result = service_->PutEnd(key);
    ASSERT_TRUE(put_end_result.has_value());

    // Test removing the object
    auto remove_result = service_->Remove(key);
    EXPECT_TRUE(remove_result.has_value());

    // Verify object is removed
    auto get_result = service_->GetReplicaList(key);
    EXPECT_FALSE(get_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, get_result.error());

    // Test removing non-existent object
    auto remove_result2 = service_->Remove("non_existent");
    EXPECT_FALSE(remove_result2.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, remove_result2.error());
}

TEST_F(MasterServiceTest, RandomRemoveObject) {
    std::unique_ptr<MasterService> service_(new MasterService());
    // Mount segment and put an object
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 16;
    std::string segment_name = "test_segment";

    Segment segment(generate_uuid(), segment_name, buffer, size);
    UUID client_id = generate_uuid();

    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());
    int times = 10;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(1, 1000);
    while (times--) {
        std::string key = "test_key" + std::to_string(dis(gen));
        std::vector<uint64_t> slice_lengths = {1024};
        ReplicateConfig config;
        config.replica_num = 1;
        auto put_start_result =
            service_->PutStart(key, 1024, slice_lengths, config);
        ASSERT_TRUE(put_start_result.has_value());
        auto put_end_result = service_->PutEnd(key);
        ASSERT_TRUE(put_end_result.has_value());

        // Test removing the object
        auto remove_result = service_->Remove(key);
        EXPECT_TRUE(remove_result.has_value());

        // Verify object is removed
        auto get_result = service_->GetReplicaList(key);
        EXPECT_FALSE(get_result.has_value());
        EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, get_result.error());
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

    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());
    int times = 10;
    while (times--) {
        std::string key = "test_key" + std::to_string(times);
        std::vector<uint64_t> slice_lengths = {1024};
        ReplicateConfig config;
        config.replica_num = 1;
        auto put_start_result =
            service_->PutStart(key, 1024, slice_lengths, config);
        ASSERT_TRUE(put_start_result.has_value());
        auto put_end_result = service_->PutEnd(key);
        ASSERT_TRUE(put_end_result.has_value());
        auto exist_result = service_->ExistKey(key);
        ASSERT_TRUE(exist_result.has_value());
    }
    // wait for all the lease to expire
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));
    ASSERT_EQ(10, service_->RemoveAll());
    times = 10;
    while (times--) {
        std::string key = "test_key" + std::to_string(times);
        auto exist_result = service_->ExistKey(key);
        ASSERT_TRUE(exist_result.has_value());
        ASSERT_FALSE(exist_result.value());
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

    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

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
    auto put_start_result =
        service_->PutStart(key, total_size, slice_lengths, config);
    ASSERT_TRUE(put_start_result.has_value());
    replica_list = put_start_result.value();

    // Verify replica list properties
    ASSERT_EQ(num_replicas, replica_list.size());
    for (const auto& replica : replica_list) {
        // Verify replica status
        EXPECT_EQ(ReplicaStatus::PROCESSING, replica.status);

        // Verify number of handles matches number of slices
        ASSERT_EQ(slice_lengths.size(),
                  replica.get_memory_descriptor().buffer_descriptors.size());

        // Verify each handle's properties
        for (size_t i = 0;
             i < replica.get_memory_descriptor().buffer_descriptors.size();
             i++) {
            const auto& handle =
                replica.get_memory_descriptor().buffer_descriptors[i];
            EXPECT_EQ(BufStatus::INIT, handle.status_);

            EXPECT_EQ(slice_lengths[i], handle.size_);
        }
    }

    // Test GetReplicaList during processing (should fail)
    auto get_result = service_->GetReplicaList(key);
    EXPECT_FALSE(get_result.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_NOT_READY, get_result.error());

    // Complete the put operation
    auto put_end_result = service_->PutEnd(key);
    ASSERT_TRUE(put_end_result.has_value());

    // Test GetReplicaList after completion
    auto get_result2 = service_->GetReplicaList(key);
    ASSERT_TRUE(get_result2.has_value());
    auto retrieved_replicas = get_result2.value();
    ASSERT_EQ(num_replicas, retrieved_replicas.size());

    // Verify final state of all replicas
    for (const auto& replica : retrieved_replicas) {
        EXPECT_EQ(ReplicaStatus::COMPLETE, replica.status);
        ASSERT_EQ(slice_lengths.size(),
                  replica.get_memory_descriptor().buffer_descriptors.size());
        for (const auto& handle :
             replica.get_memory_descriptor().buffer_descriptors) {
            EXPECT_EQ(BufStatus::COMPLETE, handle.status_);
        }
    }

    // Sleep for 2 seconds to ensure the object is marked for GC
    std::this_thread::sleep_for(std::chrono::seconds(2));
    auto remove_result = service_->Remove(key);
    EXPECT_FALSE(remove_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, remove_result.error());

    // Verify object is truly removed
    auto get_result3 = service_->GetReplicaList(key);
    EXPECT_FALSE(get_result3.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, get_result3.error());
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
    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

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
                    auto put_start_result =
                        service_->PutStart(key, 1024, slice_lengths, config);
                    ASSERT_TRUE(put_start_result.has_value());
                    auto put_end_result = service_->PutEnd(key);
                    ASSERT_TRUE(put_end_result.has_value());

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
        auto get_result = service_->GetReplicaList(key);
        EXPECT_TRUE(get_result.has_value());
    }

    // Sleep for 2 seconds to ensure the object is marked for GC
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // Verify all objects are gone after GC
    size_t found_count = 0;
    for (const auto& key : all_keys) {
        auto get_result = service_->GetReplicaList(key);
        if (get_result.has_value()) {
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
    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    // Create an object that will be stored in the segment
    std::string key = "segment_object";
    std::vector<uint64_t> slice_lengths = {1024 * 1024};  // One 1MB slice
    ReplicateConfig config;
    config.replica_num = 1;  // One replica

    // Create the object
    auto put_start_result =
        service_->PutStart(key, 1024 * 1024, slice_lengths, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto put_end_result = service_->PutEnd(key);
    ASSERT_TRUE(put_end_result.has_value());

    // Verify object exists
    auto get_result = service_->GetReplicaList(key);
    ASSERT_TRUE(get_result.has_value());
    auto retrieved_replicas = get_result.value();
    ASSERT_EQ(1, retrieved_replicas.size());

    // Unmount the segment
    auto unmount_result1 = service_->UnmountSegment(segment.id, client_id);
    ASSERT_TRUE(unmount_result1.has_value());

    // Try to get the object - it should be automatically removed since the
    // replica is invalid
    auto get_result2 = service_->GetReplicaList(key);
    EXPECT_FALSE(get_result2.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, get_result2.error());

    // Mount the segment again
    mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    // Create another object
    std::string key2 = "another_segment_object";
    auto put_start_result2 =
        service_->PutStart(key2, 1024 * 1024, slice_lengths, config);
    ASSERT_TRUE(put_start_result2.has_value());
    auto put_end_result2 = service_->PutEnd(key2);
    ASSERT_TRUE(put_end_result2.has_value());

    // Verify we can get it
    auto get_result3 = service_->GetReplicaList(key2);
    ASSERT_TRUE(get_result3.has_value());

    // Unmount the segment
    auto unmount_result2 = service_->UnmountSegment(segment.id, client_id);
    ASSERT_TRUE(unmount_result2.has_value());

    // Try to remove the object that should already be cleaned up
    auto remove_result = service_->Remove(key2);
    EXPECT_FALSE(remove_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, remove_result.error());
}

TEST_F(MasterServiceTest, ConcurrentWriteAndRemoveAll) {
    std::unique_ptr<MasterService> service_(new MasterService());
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 256;  // 256MB for concurrent testing
    std::string segment_name = "concurrent_segment";
    Segment segment(generate_uuid(), segment_name, buffer, size);
    UUID client_id = generate_uuid();
    auto mount_result_concurrent = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result_concurrent.has_value());

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

                auto put_start_result =
                    service_->PutStart(key, 1024, slice_lengths, config);
                if (put_start_result.has_value()) {
                    auto put_end_result = service_->PutEnd(key);
                    if (put_end_result.has_value()) {
                        success_writes++;
                    }
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
    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    // Pre-populate with test data
    constexpr int num_objects = 1000;
    for (int i = 0; i < num_objects; ++i) {
        std::string key = "pre_key_" + std::to_string(i);
        std::vector<uint64_t> slice_lengths = {1024};
        ReplicateConfig config;
        config.replica_num = 1;

        auto put_start_result =
            service_->PutStart(key, 1024, slice_lengths, config);
        ASSERT_TRUE(put_start_result.has_value());
        auto put_end_result = service_->PutEnd(key);
        ASSERT_TRUE(put_end_result.has_value());
    }

    std::atomic<int> success_reads(0);
    std::atomic<bool> remove_all_done(false);

    // Reader threads
    std::vector<std::thread> readers;
    for (int i = 0; i < 4; ++i) {
        readers.emplace_back([&]() {
            for (int j = 0; j < num_objects; ++j) {
                std::string key = "pre_key_" + std::to_string(j);
                auto get_result = service_->GetReplicaList(key);
                if (get_result.has_value()) {
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
    for (int i = 0; i < num_objects; ++i) {
        std::string key = "pre_key_" + std::to_string(i);
        auto get_result = service_->GetReplicaList(key);
        EXPECT_FALSE(get_result.has_value());
        EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, get_result.error());
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
    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    // Pre-populate with test data
    constexpr int num_objects = 1000000;
    for (int i = 0; i < num_objects; ++i) {
        std::string key = "pre_key_" + std::to_string(i);
        std::vector<uint64_t> slice_lengths = {1024};
        ReplicateConfig config;
        config.replica_num = 1;

        auto put_start_result =
            service_->PutStart(key, 1024, slice_lengths, config);
        ASSERT_TRUE(put_start_result.has_value());
        auto put_end_result = service_->PutEnd(key);
        ASSERT_TRUE(put_end_result.has_value());
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
    for (int i = 0; i < num_objects; ++i) {
        std::string key = "pre_key_" + std::to_string(i);
        auto get_result = service_->GetReplicaList(key);
        EXPECT_FALSE(get_result.has_value());
        EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, get_result.error());
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
    auto mount_result1 = service_->MountSegment(segment1, client_id);
    ASSERT_TRUE(mount_result1.has_value());
    auto mount_result2 = service_->MountSegment(segment2, client_id);
    ASSERT_TRUE(mount_result2.has_value());

    // Create two objects in the two segments
    std::string key1 = GenerateKeyForSegment(service_, segment1.name);
    std::string key2 = GenerateKeyForSegment(service_, segment2.name);
    std::vector<uint64_t> slice_lengths = {1024};
    ReplicateConfig config;
    config.replica_num = 1;

    // Unmount segment1
    auto unmount_result1 = service_->UnmountSegment(segment1.id, client_id);
    ASSERT_TRUE(unmount_result1.has_value());
    // Umount will remove all objects in the segment, include the key1
    ASSERT_EQ(1, service_->GetKeyCount());
    // Verify objects in segment1 is gone
    auto get_result1 = service_->GetReplicaList(key1);
    ASSERT_FALSE(get_result1.has_value());
    ASSERT_EQ(ErrorCode::OBJECT_NOT_FOUND, get_result1.error());

    // Verify objects in segment2 is still there
    auto get_result2 = service_->GetReplicaList(key2);
    ASSERT_TRUE(get_result2.has_value());

    // Verify put key1 will put into segment2 rather than segment1
    auto put_start_result =
        service_->PutStart(key1, 1024, slice_lengths, config);
    ASSERT_TRUE(put_start_result.has_value());
    replica_list = put_start_result.value();
    auto put_end_result = service_->PutEnd(key1);
    ASSERT_TRUE(put_end_result.has_value());
    auto get_result3 = service_->GetReplicaList(key1);
    ASSERT_TRUE(get_result3.has_value());
    auto retrieved = get_result3.value();
    ASSERT_EQ(replica_list[0]
                  .get_memory_descriptor()
                  .buffer_descriptors[0]
                  .segment_name_,
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
    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

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
    auto unmount_result = service_->UnmountSegment(segment.id, client_id);
    EXPECT_TRUE(unmount_result.has_value());
    auto unmount_end = std::chrono::steady_clock::now();

    auto unmount_duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(unmount_end -
                                                              unmount_start);

    // Unmount operation should be very fast, so we set 1s limit
    EXPECT_LE(unmount_duration.count(), 1000)
        << "Unmount operation took " << unmount_duration.count()
        << "ms which exceeds 1 second limit";

    // Verify all keys are gone
    for (const auto& key : keys) {
        auto get_result = service_->GetReplicaList(key);
        EXPECT_FALSE(get_result.has_value());
        EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, get_result.error());
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
    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    std::string key = "test_key";
    std::vector<uint64_t> slice_lengths = {1024};
    ReplicateConfig config;
    config.replica_num = 1;

    // Verify lease is granted on ExistsKey
    auto put_start_result =
        service_->PutStart(key, 1024, slice_lengths, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto put_end_result = service_->PutEnd(key);
    ASSERT_TRUE(put_end_result.has_value());
    auto exist_result = service_->ExistKey(key);
    ASSERT_TRUE(exist_result.has_value());
    auto remove_result = service_->Remove(key);
    EXPECT_FALSE(remove_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_HAS_LEASE, remove_result.error());
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));
    auto remove_result2 = service_->Remove(key);
    EXPECT_TRUE(remove_result2.has_value());

    // Verify lease is extended on successive ExistsKey
    auto put_start_result2 =
        service_->PutStart(key, 1024, slice_lengths, config);
    ASSERT_TRUE(put_start_result2.has_value());
    auto put_end_result2 = service_->PutEnd(key);
    ASSERT_TRUE(put_end_result2.has_value());
    auto exist_result2 = service_->ExistKey(key);
    ASSERT_TRUE(exist_result2.has_value());
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));
    auto exist_result3 = service_->ExistKey(key);
    ASSERT_TRUE(exist_result3.has_value());
    auto remove_result3 = service_->Remove(key);
    EXPECT_FALSE(remove_result3.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_HAS_LEASE, remove_result3.error());
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));
    auto remove_result4 = service_->Remove(key);
    EXPECT_TRUE(remove_result4.has_value());

    // Verify lease is granted on GetReplicaList
    auto put_start_result3 =
        service_->PutStart(key, 1024, slice_lengths, config);
    ASSERT_TRUE(put_start_result3.has_value());
    auto put_end_result3 = service_->PutEnd(key);
    ASSERT_TRUE(put_end_result3.has_value());
    auto get_result = service_->GetReplicaList(key);
    ASSERT_TRUE(get_result.has_value());
    auto remove_result5 = service_->Remove(key);
    EXPECT_FALSE(remove_result5.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_HAS_LEASE, remove_result5.error());
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));
    auto remove_result6 = service_->Remove(key);
    EXPECT_TRUE(remove_result6.has_value());

    // Verify lease is extended on successive GetReplicaList
    auto put_start_result4 =
        service_->PutStart(key, 1024, slice_lengths, config);
    ASSERT_TRUE(put_start_result4.has_value());
    auto put_end_result4 = service_->PutEnd(key);
    ASSERT_TRUE(put_end_result4.has_value());
    auto get_result2 = service_->GetReplicaList(key);
    ASSERT_TRUE(get_result2.has_value());
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));
    auto get_result3 = service_->GetReplicaList(key);
    ASSERT_TRUE(get_result3.has_value());
    auto remove_result7 = service_->Remove(key);
    EXPECT_FALSE(remove_result7.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_HAS_LEASE, remove_result7.error());
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));
    auto remove_result8 = service_->Remove(key);
    EXPECT_TRUE(remove_result8.has_value());

    // Verify object is removed
    auto get_result4 = service_->GetReplicaList(key);
    EXPECT_FALSE(get_result4.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, get_result4.error());
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
    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());
    for (int i = 0; i < 10; ++i) {
        std::string key = "test_key" + std::to_string(i);
        std::vector<uint64_t> slice_lengths = {1024};
        ReplicateConfig config;
        config.replica_num = 1;
        auto put_start_result =
            service_->PutStart(key, 1024, slice_lengths, config);
        ASSERT_TRUE(put_start_result.has_value());
        auto put_end_result = service_->PutEnd(key);
        ASSERT_TRUE(put_end_result.has_value());
        if (i >= 5) {
            auto exist_result = service_->ExistKey(key);
            ASSERT_TRUE(exist_result.has_value());
        }
    }
    ASSERT_EQ(5, service_->RemoveAll());
    for (int i = 0; i < 5; ++i) {
        std::string key = "test_key" + std::to_string(i);
        auto exist_result = service_->ExistKey(key);
        ASSERT_FALSE(exist_result.value());
    }
    // wait for all the lease to expire
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));
    ASSERT_EQ(5, service_->RemoveAll());
    for (int i = 5; i < 10; ++i) {
        std::string key = "test_key" + std::to_string(i);
        auto exist_result = service_->ExistKey(key);
        ASSERT_FALSE(exist_result.value());
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
    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    // Verify if we can put objects more than the segment can hold
    int success_puts = 0;
    for (int i = 0; i < 1024 * 16 + 50; ++i) {
        std::string key = "test_key" + std::to_string(i);
        std::vector<uint64_t> slice_lengths = {object_size};
        ReplicateConfig config;
        config.replica_num = 1;
        auto put_start_result =
            service_->PutStart(key, object_size, slice_lengths, config);
        if (put_start_result.has_value()) {
            auto put_end_result = service_->PutEnd(key);
            ASSERT_TRUE(put_end_result.has_value());
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
    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    // Verify leased object will not be evicted.
    int success_puts = 0;
    int failed_puts = 0;
    std::vector<std::string> leased_keys;
    for (int i = 0; i < 16 + 10; ++i) {
        std::string key = "test_key" + std::to_string(i);
        std::vector<uint64_t> slice_lengths = {object_size};
        ReplicateConfig config;
        config.replica_num = 1;
        auto put_start_result =
            service_->PutStart(key, object_size, slice_lengths, config);
        if (put_start_result.has_value()) {
            auto put_end_result = service_->PutEnd(key);
            ASSERT_TRUE(put_end_result.has_value());
            // the object is leased
            auto get_result = service_->GetReplicaList(key);
            ASSERT_TRUE(get_result.has_value());
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
        auto get_result = service_->GetReplicaList(key);
        ASSERT_TRUE(get_result.has_value());
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
    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    int test_object_num = 10;
    std::vector<std::string> test_keys;
    for (int i = 0; i < test_object_num; ++i) {
        test_keys.push_back("test_key" + std::to_string(i));
        ReplicateConfig config;
        config.replica_num = 1;
        std::vector<uint64_t> slice_lengths = {1024};
        auto put_start_result =
            service_->PutStart(test_keys[i], 1024, slice_lengths, config);
        ASSERT_TRUE(put_start_result.has_value());
        auto put_end_result = service_->PutEnd(test_keys[i]);
        ASSERT_TRUE(put_end_result.has_value());
    }

    // Test individual ExistKey calls to verify the underlying functionality
    for (int i = 0; i < test_object_num; ++i) {
        auto exist_result = service_->ExistKey(test_keys[i]);
        EXPECT_TRUE(exist_result.value());
    }

    // Tets batch
    test_keys.push_back("non_existent_key");
    auto exist_resp = service_->BatchExistKey(test_keys);
    for (int i = 0; i < test_object_num; ++i) {
        ASSERT_TRUE(exist_resp[i].value());
    }
    ASSERT_FALSE(exist_resp[test_object_num].value());
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

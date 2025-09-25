#include "master_service.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <random>
#include <thread>
#include <vector>
#include <unordered_set>
#include <utility>

#include "types.h"

namespace mooncake::test {

class MasterServiceTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("MasterServiceTest");
        FLAGS_logtostderr = true;
    }

    struct MountedSegmentContext {
        UUID segment_id;
        UUID client_id;
    };

    static constexpr size_t kDefaultSegmentBase = 0x300000000;
    static constexpr size_t kDefaultSegmentSize = 1024 * 1024 * 16;

    Segment MakeSegment(std::string name = "test_segment",
                        size_t base = kDefaultSegmentBase,
                        size_t size = kDefaultSegmentSize) const {
        Segment segment;
        segment.id = generate_uuid();
        segment.name = std::move(name);
        segment.base = base;
        segment.size = size;
        segment.te_endpoint = segment.name;
        return segment;
    }

    MountedSegmentContext PrepareSimpleSegment(
        MasterService& service, std::string name = "test_segment",
        size_t base = kDefaultSegmentBase,
        size_t size = kDefaultSegmentSize) const {
        Segment segment = MakeSegment(std::move(name), base, size);
        UUID client_id = generate_uuid();
        auto mount_result = service.MountSegment(segment, client_id);
        EXPECT_TRUE(mount_result.has_value());
        return {.segment_id = segment.id, .client_id = client_id};
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
        auto put_result = service->PutStart(key, {1024}, {.replica_num = 1});
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
        auto put_end_result = service->PutEnd(key, ReplicaType::MEMORY);
        if (!put_end_result.has_value()) {
            throw std::runtime_error("PutEnd failed");
        }
        if (replica_list[0]
                .get_memory_descriptor()
                .buffer_descriptors[0]
                .transport_endpoint_ == segment_name) {
            return key;
        }
        // Clean up failed attempt
        auto remove_result = service->Remove(key);
        if (!remove_result.has_value()) {
            // Ignore cleanup failure
        }
    }
}

TEST_F(MasterServiceTest, MountUnmountSegmentWithCachelibAllocator) {
    // Create a MasterService instance for testing.
    auto service_config =
        MasterServiceConfig::builder()
            .set_memory_allocator(BufferAllocatorType::CACHELIB)
            .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    auto segment = MakeSegment();
    UUID client_id = generate_uuid();
    const auto original_base = segment.base;
    const auto original_size = segment.size;

    // Test invalid parameters.
    // Invalid buffer address (0).
    segment.base = 0;
    segment.size = original_size;
    auto mount_result1 = service_->MountSegment(segment, client_id);
    EXPECT_FALSE(mount_result1.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, mount_result1.error());

    // Invalid segment size (0).
    segment.base = original_base;
    segment.size = 0;
    auto mount_result2 = service_->MountSegment(segment, client_id);
    EXPECT_FALSE(mount_result2.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, mount_result2.error());

    // Base is not aligned
    segment.base = original_base + 1;
    segment.size = original_size;
    auto mount_result3 = service_->MountSegment(segment, client_id);
    EXPECT_FALSE(mount_result3.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, mount_result3.error());

    // Size is not aligned
    segment.base = original_base;
    segment.size = original_size + 1;
    auto mount_result4 = service_->MountSegment(segment, client_id);
    EXPECT_FALSE(mount_result4.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, mount_result4.error());

    // Test normal mount operation.
    segment.base = original_base;
    segment.size = original_size;
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

TEST_F(MasterServiceTest, MountUnmountSegmentWithOffsetAllocator) {
    // Create a MasterService instance for testing.
    auto service_config = MasterServiceConfig::builder()
                              .set_memory_allocator(BufferAllocatorType::OFFSET)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    auto segment = MakeSegment();
    UUID client_id = generate_uuid();
    const auto original_base = segment.base;
    const auto original_size = segment.size;

    // Test invalid parameters.
    // Invalid buffer address (0).
    segment.base = 0;
    segment.size = original_size;
    auto mount_result1 = service_->MountSegment(segment, client_id);
    EXPECT_FALSE(mount_result1.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, mount_result1.error());

    // Invalid segment size (0).
    segment.base = original_base;
    segment.size = 0;
    auto mount_result2 = service_->MountSegment(segment, client_id);
    EXPECT_FALSE(mount_result2.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, mount_result2.error());

    // Test normal mount operation.
    segment.base = original_base;
    segment.size = original_size;
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

        auto segment = MakeSegment(segment_name, kBufferAddress, kSegmentSize);
        segment.id = segment_id;

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
        threads.emplace_back([&service_, i, &success_count, this]() {
            auto segment =
                MakeSegment("segment_" + std::to_string(i),
                            0x300000000 + i * 0x10000000, 16 * 1024 * 1024);
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
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);

    std::string key = "test_key";
    ReplicateConfig config;

    // Test invalid replica_num
    config.replica_num = 0;
    auto put_result1 = service_->PutStart(key, {1024}, config);
    EXPECT_FALSE(put_result1.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, put_result1.error());

    // Test empty slice_lengths
    config.replica_num = 1;
    std::vector<uint64_t> empty_slices;
    auto put_result2 = service_->PutStart(key, empty_slices, config);
    EXPECT_FALSE(put_result2.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, put_result2.error());
}

TEST_F(MasterServiceTest, PutStartEndFlow) {
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);

    // Test PutStart
    std::string key = "test_key";
    uint64_t value_length = 1024;
    std::vector<uint64_t> slice_lengths = {value_length};
    ReplicateConfig config;
    config.replica_num = 1;

    auto put_start_result = service_->PutStart(key, slice_lengths, config);
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
    auto put_end_result = service_->PutEnd(key, ReplicaType::MEMORY);
    EXPECT_TRUE(put_end_result.has_value());

    // Verify replica list after PutEnd
    auto final_get_result = service_->GetReplicaList(key);
    EXPECT_TRUE(final_get_result.has_value());
    replica_list = final_get_result.value().replicas;
    EXPECT_EQ(1, replica_list.size());
    EXPECT_EQ(ReplicaStatus::COMPLETE, replica_list[0].status);
}

TEST_F(MasterServiceTest, RandomPutStartEndFlow) {
    std::unique_ptr<MasterService> service_(new MasterService());

    // Mount 5 segments, each 16MB
    constexpr size_t kBaseAddr = 0x300000000;
    constexpr size_t kSegmentSize = 1024 * 1024 * 16;  // 16MB
    for (int i = 0; i < 5; ++i) {
        [[maybe_unused]] const auto context = PrepareSimpleSegment(
            *service_, "segment_" + std::to_string(i),
            kBaseAddr + static_cast<size_t>(i) * kSegmentSize, kSegmentSize);
    }

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
    auto put_start_result = service_->PutStart(key, slice_lengths, config);
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
    auto put_end_result = service_->PutEnd(key, ReplicaType::MEMORY);
    EXPECT_TRUE(put_end_result.has_value());
    // Verify replica list after PutEnd
    auto get_result2 = service_->GetReplicaList(key);
    EXPECT_TRUE(get_result2.has_value());
    replica_list = get_result2.value().replicas;
    EXPECT_EQ(random_number, replica_list.size());
    for (int i = 0; i < random_number; ++i) {
        EXPECT_EQ(ReplicaStatus::COMPLETE, replica_list[i].status);
    }
}

TEST_F(MasterServiceTest, GetReplicaListByRegex) {
    const uint64_t kv_lease_ttl = 50;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    // Test getting non-existent key
    auto get_result = service_->GetReplicaList(".*non_existent.*");
    EXPECT_FALSE(get_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, get_result.error());

    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);

    int times = 10;
    while (times--) {
        std::string key = "test_key" + std::to_string(times);
        std::vector<uint64_t> slice_lengths = {1024};
        ReplicateConfig config;
        config.replica_num = 1;
        auto put_start_result = service_->PutStart(key, slice_lengths, config);
        ASSERT_TRUE(put_start_result.has_value());
        auto put_end_result = service_->PutEnd(key, ReplicaType::MEMORY);
        ASSERT_TRUE(put_end_result.has_value());
        auto exist_result = service_->ExistKey(key);
        ASSERT_TRUE(exist_result.has_value());
    }
    // wait for all the lease to expire
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));

    // Test getting existing key
    auto get_result2 = service_->GetReplicaListByRegex("^test_key");
    EXPECT_TRUE(get_result2.has_value());
    auto replica_list_local = get_result2.value();
    EXPECT_EQ(10, replica_list_local.size());
}

// Helper function to put an object, making the test cleaner
void put_object(MasterService& service, const std::string& key) {
    std::vector<uint64_t> slice_lengths = {1024};
    ReplicateConfig config;
    config.replica_num = 1;
    auto put_start_result = service.PutStart(key, slice_lengths, config);
    ASSERT_TRUE(put_start_result.has_value())
        << "Failed to PutStart for key: " << key;
    auto put_end_result = service.PutEnd(key, ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value())
        << "Failed to PutEnd for key: " << key;
    auto exist_result = service.ExistKey(key);
    ASSERT_TRUE(exist_result.has_value())
        << "Key does not exist after put: " << key;
}

TEST_F(MasterServiceTest, GetReplicaListByRegexComplex) {
    const uint64_t kv_lease_ttl = 100;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    auto service_ = std::make_unique<MasterService>(service_config);

    // 1. Mount segment
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);

    // 2. Prepare a diverse set of keys
    std::vector<std::string> keys_to_put = {
        // Basic keys for prefix matching
        "test_key_01", "test_key_02", "test_key_10",
        // Keys with different prefixes
        "prod_key_alpha", "prod_key_beta",
        // Keys with numbers in the middle
        "data_part_1_chunk_a", "data_part_2_chunk_b",
        // Keys with special characters (if your system supports them)
        "config/user/settings.json", "logs/app-2025-08-13.log",
        // Keys with varying lengths
        "short", "a_very_very_very_long_key_that_tests_length_limits",
        // Keys that look similar but should not match certain regex
        "test-key-extra", "another_key"};

    for (const auto& key : keys_to_put) {
        put_object(*service_, key);
    }

    // Wait for all leases to be written to the underlying KV store.
    // In a real system, you might not need this if PutEnd is synchronous.
    // For this test, let's assume it's needed for consistency.
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));

    // 3. Run a series of regex tests

    // Test 3.1: Simple prefix matching
    {
        auto result = service_->GetReplicaListByRegex("^test_key_");
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(result.value().size(),
                  3);  // Matches test_key_01, test_key_02, test_key_10
    }

    // Test 3.2: Matching with a wildcard for any number
    {
        auto result = service_->GetReplicaListByRegex("^test_key_\\d+$");
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(result.value().size(), 3);
    }

    // Test 3.3: Matching a specific pattern with wildcards
    {
        // Matches "data_part_1_chunk_a" and "data_part_2_chunk_b"
        auto result =
            service_->GetReplicaListByRegex("^data_part_\\d_chunk_.$");
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(result.value().size(), 2);
    }

    // Test 3.4: Matching keys containing a specific substring
    {
        // Matches all keys with "key" in them
        auto result = service_->GetReplicaListByRegex("key");
        ASSERT_TRUE(result.has_value());
        // Expected: test_key_01, test_key_02, test_key_10,
        //           prod_key_alpha, prod_key_beta,
        //           a_very_very_very_long_key_that_tests_length_limits,
        //           test-key-extra, another_key
        EXPECT_EQ(result.value().size(), 8);
    }

    // Test 3.5: Matching based on file-like paths
    {
        // Match all .log files
        auto result = service_->GetReplicaListByRegex("\\.log$");
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(result.value().size(), 1);
        EXPECT_EQ(result.value().begin()->first, "logs/app-2025-08-13.log");
    }

    // Test 3.6: OR condition using |
    {
        // Match keys starting with "prod" OR ending with "json"
        auto result = service_->GetReplicaListByRegex("^prod|\\.json$");
        ASSERT_TRUE(result.has_value());
        // Expected: prod_key_alpha, prod_key_beta, config/user/settings.json
        EXPECT_EQ(result.value().size(), 3);
    }

    // Test 3.7: Regex that should not match anything
    {
        auto result = service_->GetReplicaListByRegex("^non_existent_prefix_");
        // This should succeed but return an empty map.
        ASSERT_TRUE(result.has_value());
        EXPECT_TRUE(result.value().empty());
    }

    // Test 3.8: Exact match regex
    {
        auto result = service_->GetReplicaListByRegex("^short$");
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(result.value().size(), 1);
        EXPECT_EQ(result.value().begin()->first, "short");
    }

    // Test 3.9: Initial test for non-existent key (as a sanity check)
    {
        auto get_result =
            service_->GetReplicaListByRegex(".*absolutely_non_existent.*");
        // Depending on implementation, this could return an empty map or an
        // error. Let's assume it returns an empty map for a valid regex with no
        // matches.
        ASSERT_TRUE(get_result.has_value());
        EXPECT_TRUE(get_result.value().empty());
    }
}

TEST_F(MasterServiceTest, GetReplicaList) {
    std::unique_ptr<MasterService> service_(new MasterService());
    // Test getting non-existent key
    auto get_result = service_->GetReplicaList("non_existent");
    EXPECT_FALSE(get_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, get_result.error());

    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);

    std::string key = "test_key";
    std::vector<uint64_t> slice_lengths = {1024};
    ReplicateConfig config;
    config.replica_num = 1;
    auto put_start_result = service_->PutStart(key, slice_lengths, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto put_end_result = service_->PutEnd(key, ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    // Test getting existing key
    auto get_result2 = service_->GetReplicaList(key);
    EXPECT_TRUE(get_result2.has_value());
    auto replica_list_local = get_result2.value().replicas;
    EXPECT_FALSE(replica_list_local.empty());
}

TEST_F(MasterServiceTest, RemoveObject) {
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);

    std::string key = "test_key";
    std::vector<uint64_t> slice_lengths = {1024};
    ReplicateConfig config;
    config.replica_num = 1;
    auto put_start_result = service_->PutStart(key, slice_lengths, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto put_end_result = service_->PutEnd(key, ReplicaType::MEMORY);
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
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    int times = 10;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(1, 1000);
    while (times--) {
        std::string key = "test_key" + std::to_string(dis(gen));
        std::vector<uint64_t> slice_lengths = {1024};
        ReplicateConfig config;
        config.replica_num = 1;
        auto put_start_result = service_->PutStart(key, slice_lengths, config);
        ASSERT_TRUE(put_start_result.has_value());
        auto put_end_result = service_->PutEnd(key, ReplicaType::MEMORY);
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

TEST_F(MasterServiceTest, RemoveByRegex) {
    const uint64_t kv_lease_ttl = 50;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    int times = 10;
    while (times--) {
        std::string key = "test_key" + std::to_string(times);
        std::vector<uint64_t> slice_lengths = {1024};
        ReplicateConfig config;
        config.replica_num = 1;
        auto put_start_result = service_->PutStart(key, slice_lengths, config);
        ASSERT_TRUE(put_start_result.has_value());
        auto put_end_result = service_->PutEnd(key, ReplicaType::MEMORY);
        ASSERT_TRUE(put_end_result.has_value());
        auto exist_result = service_->ExistKey(key);
        ASSERT_TRUE(exist_result.has_value());
    }
    // wait for all the lease to expire
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));
    auto res = service_->RemoveByRegex("^test_key");
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(10, res.value());
    times = 10;
    while (times--) {
        std::string key = "test_key" + std::to_string(times);
        auto exist_result = service_->ExistKey(key);
        ASSERT_TRUE(exist_result.has_value());
        ASSERT_FALSE(exist_result.value());
    }
}

TEST_F(MasterServiceTest, RemoveByRegexComplex) {
    const uint64_t kv_lease_ttl = 100;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    auto service_ = std::make_unique<MasterService>(service_config);

    // 1. Mount segment
    [[maybe_unused]] const auto context =
        PrepareSimpleSegment(*service_, "test_segment_remove");

    // A helper lambda to repopulate the store for each test case
    auto populate_store = [&]() {
        std::vector<std::string> keys_to_put = {
            "test_key_01",
            "test_key_02",
            "test_key_10",
            "prod_key_alpha",
            "prod_key_beta",
            "data_part_1_chunk_a",
            "data_part_2_chunk_b",
            "config/user/settings.json",
            "logs/app-2025-08-13.log",
            "short",
            "a_very_very_very_long_key_that_tests_length_limits",
            "test-key-extra",
            "another_key"};
        for (const auto& key : keys_to_put) {
            put_object(*service_, key);
        }
        // Wait for potential lease propagation
        std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));
    };

    // --- Test Case 1: Remove a specific subset and verify ---
    {
        SCOPED_TRACE("Test Case 1: Removing keys with prefix 'test_key_'");
        populate_store();

        // Action: Remove keys starting with "test_key_"
        auto remove_result = service_->RemoveByRegex("^test_key_");
        ASSERT_TRUE(remove_result.has_value());
        EXPECT_EQ(remove_result.value(), 3);  // Should remove 3 keys

        // Verification: Check which keys were deleted and which remain
        std::vector<std::string> deleted_keys = {"test_key_01", "test_key_02",
                                                 "test_key_10"};
        for (const auto& key : deleted_keys) {
            auto exist_result = service_->ExistKey(key);
            ASSERT_TRUE(exist_result.has_value());
            EXPECT_FALSE(exist_result.value())
                << "Key " << key << " should have been deleted.";
        }

        std::vector<std::string> remaining_keys = {
            "prod_key_alpha", "short", "test-key-extra"};  // Sample a few
        for (const auto& key : remaining_keys) {
            auto exist_result = service_->ExistKey(key);
            ASSERT_TRUE(exist_result.has_value());
            EXPECT_TRUE(exist_result.value())
                << "Key " << key << " should NOT have been deleted.";
        }
    }

    // --- Test Case 2: Remove everything ---
    {
        SCOPED_TRACE("Test Case 2: Removing all keys with '.*'");
        // Store is already populated from the previous (failed) test run, or we
        // can repopulate For isolation, let's assume we start fresh
        auto service_config = MasterServiceConfig::builder()
                                  .set_default_kv_lease_ttl(kv_lease_ttl)
                                  .build();
        service_ = std::make_unique<MasterService>(service_config);
        [[maybe_unused]] const auto context_reset =
            PrepareSimpleSegment(*service_, "test_segment_remove");
        populate_store();

        size_t total_keys = 13;  // Count from the keys_to_put vector

        // Action: Remove all keys
        auto remove_result = service_->RemoveByRegex(".*");
        ASSERT_TRUE(remove_result.has_value());
        EXPECT_EQ(remove_result.value(), total_keys);

        // Verification: Check that no keys remain
        auto get_all_result = service_->GetReplicaListByRegex(".*");
        ASSERT_TRUE(get_all_result.has_value());
        EXPECT_TRUE(get_all_result.value().empty());
    }

    // --- Test Case 3: Attempt to remove with a non-matching pattern ---
    {
        SCOPED_TRACE("Test Case 3: Removing with a non-matching pattern");
        auto service_config = MasterServiceConfig::builder()
                                  .set_default_kv_lease_ttl(kv_lease_ttl)
                                  .build();
        service_ = std::make_unique<MasterService>(
            service_config);  // Reset the service
        [[maybe_unused]] const auto context_reset =
            PrepareSimpleSegment(*service_, "test_segment_remove");
        populate_store();

        size_t total_keys_before_remove = 13;

        // Action: Attempt to remove using a pattern that matches nothing
        auto remove_result = service_->RemoveByRegex("^nonexistent-pattern-");
        ASSERT_TRUE(remove_result.has_value());
        EXPECT_EQ(remove_result.value(), 0);  // Should remove 0 keys

        // Verification: Check that all keys still exist
        auto get_all_result = service_->GetReplicaListByRegex(".*");
        ASSERT_TRUE(get_all_result.has_value());
        EXPECT_EQ(get_all_result.value().size(), total_keys_before_remove);
    }

    // --- Test Case 4: Remove based on a complex pattern and verify ---
    {
        SCOPED_TRACE(
            "Test Case 4: Removing based on file paths or containing digits");
        auto service_config = MasterServiceConfig::builder()
                                  .set_default_kv_lease_ttl(kv_lease_ttl)
                                  .build();
        service_ = std::make_unique<MasterService>(service_config);  // Reset
        [[maybe_unused]] const auto context_reset =
            PrepareSimpleSegment(*service_, "test_segment_remove");
        populate_store();

        // Action: Remove all keys that contain a slash '/' OR end with a number
        auto remove_result = service_->RemoveByRegex("/|\\d$");
        ASSERT_TRUE(remove_result.has_value());
        // Matches: "config/user/settings.json", "logs/app-2025-08-13.log",
        //          "test_key_01", "test_key_02", "test_key_10"
        // Note: logs/app-2025-08-13.log matches both, but is counted once.
        EXPECT_EQ(remove_result.value(),
                  5);  // The two paths + test_key_01 and test_key_02.
                       // (test_key_10 ends with 0) wait, no, 10 ends with 0.
        // Ah, \d$ matches a single digit at the end. So test_key_01,
        // test_key_02. test_key_10 does NOT match \d$. Let's refine the regex.
    }

    // --- Test Case 4 ---
    {
        SCOPED_TRACE(
            "Test Case 4 (Corrected): Removing based on complex pattern");
        auto service_config = MasterServiceConfig::builder()
                                  .set_default_kv_lease_ttl(kv_lease_ttl)
                                  .build();
        service_ = std::make_unique<MasterService>(service_config);  // Reset
        [[maybe_unused]] const auto context_reset =
            PrepareSimpleSegment(*service_, "test_segment_remove");
        populate_store();

        // Action: Remove all keys that contain "chunk" OR "config"
        auto remove_result = service_->RemoveByRegex("chunk|config");
        ASSERT_TRUE(remove_result.has_value());
        // Matches: "data_part_1_chunk_a", "data_part_2_chunk_b",
        // "config/user/settings.json"
        EXPECT_EQ(remove_result.value(), 3);

        // Verification
        auto exist_result_chunk = service_->ExistKey("data_part_1_chunk_a");
        ASSERT_TRUE(exist_result_chunk.has_value());
        EXPECT_FALSE(exist_result_chunk.value());

        auto exist_result_config =
            service_->ExistKey("config/user/settings.json");
        ASSERT_TRUE(exist_result_config.has_value());
        EXPECT_FALSE(exist_result_config.value());

        auto exist_result_untouched = service_->ExistKey("prod_key_alpha");
        ASSERT_TRUE(exist_result_untouched.has_value());
        EXPECT_TRUE(exist_result_untouched.value());
    }
}

TEST_F(MasterServiceTest, RemoveAll) {
    const uint64_t kv_lease_ttl = 50;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    int times = 10;
    while (times--) {
        std::string key = "test_key" + std::to_string(times);
        std::vector<uint64_t> slice_lengths = {1024};
        ReplicateConfig config;
        config.replica_num = 1;
        auto put_start_result = service_->PutStart(key, slice_lengths, config);
        ASSERT_TRUE(put_start_result.has_value());
        auto put_end_result = service_->PutEnd(key, ReplicaType::MEMORY);
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
    const uint64_t kv_lease_ttl = 50;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));

    // Mount 3 segments, each 64MB
    constexpr size_t kBaseAddr = 0x300000000;
    constexpr size_t kSegmentSize = 1024 * 1024 * 64;  // 64MB
    for (int i = 0; i < 3; ++i) {
        [[maybe_unused]] const auto context = PrepareSimpleSegment(
            *service_, "segment_" + std::to_string(i),
            kBaseAddr + static_cast<size_t>(i) * kSegmentSize, kSegmentSize);
    }

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
    auto put_start_result = service_->PutStart(key, slice_lengths, config);
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

            EXPECT_EQ(slice_lengths[i], handle.size_);
        }
    }

    // Test GetReplicaList during processing (should fail)
    auto get_result = service_->GetReplicaList(key);
    EXPECT_FALSE(get_result.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_NOT_READY, get_result.error());

    // Complete the put operation
    auto put_end_result = service_->PutEnd(key, ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    // Test GetReplicaList after completion
    auto get_result2 = service_->GetReplicaList(key);
    ASSERT_TRUE(get_result2.has_value());
    auto retrieved_replicas = get_result2.value().replicas;
    ASSERT_EQ(num_replicas, retrieved_replicas.size());

    // Verify final state of all replicas
    for (const auto& replica : retrieved_replicas) {
        EXPECT_EQ(ReplicaStatus::COMPLETE, replica.status);
        ASSERT_EQ(slice_lengths.size(),
                  replica.get_memory_descriptor().buffer_descriptors.size());
    }
}

TEST_F(MasterServiceTest, CleanupStaleHandlesTest) {
    std::unique_ptr<MasterService> service_(new MasterService());

    // Mount a segment for testing
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 16;  // 16MB
    auto segment = MakeSegment("test_segment", buffer, size);
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
    auto put_start_result = service_->PutStart(key, slice_lengths, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto put_end_result = service_->PutEnd(key, ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    // Verify object exists
    auto get_result = service_->GetReplicaList(key);
    ASSERT_TRUE(get_result.has_value());
    auto retrieved_replicas = get_result.value().replicas;
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
    auto put_start_result2 = service_->PutStart(key2, slice_lengths, config);
    ASSERT_TRUE(put_start_result2.has_value());
    auto put_end_result2 = service_->PutEnd(key2, ReplicaType::MEMORY);
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
    auto segment = MakeSegment("concurrent_segment", buffer, size);
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
                    service_->PutStart(key, slice_lengths, config);
                if (put_start_result.has_value()) {
                    auto put_end_result =
                        service_->PutEnd(key, ReplicaType::MEMORY);
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
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 256;  // 256MB for concurrent testing
    auto segment = MakeSegment("concurrent_segment", buffer, size);
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

        auto put_start_result = service_->PutStart(key, slice_lengths, config);
        ASSERT_TRUE(put_start_result.has_value());
        auto put_end_result = service_->PutEnd(key, ReplicaType::MEMORY);
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
    constexpr size_t size = 1024 * 1024 * 16 * 100;
    auto segment = MakeSegment("concurrent_segment", buffer, size);
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

        auto put_start_result = service_->PutStart(key, slice_lengths, config);
        ASSERT_TRUE(put_start_result.has_value());
        auto put_end_result = service_->PutEnd(key, ReplicaType::MEMORY);
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

    auto segment1 = MakeSegment("segment1", buffer1, size);
    auto segment2 = MakeSegment("segment2", buffer2, size);
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
    auto put_start_result = service_->PutStart(key1, slice_lengths, config);
    ASSERT_TRUE(put_start_result.has_value());
    replica_list = put_start_result.value();
    auto put_end_result = service_->PutEnd(key1, ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());
    auto get_result3 = service_->GetReplicaList(key1);
    ASSERT_TRUE(get_result3.has_value());
    auto retrieved = get_result3.value();
    ASSERT_EQ(replica_list[0]
                  .get_memory_descriptor()
                  .buffer_descriptors[0]
                  .transport_endpoint_,
              segment2.name);
}

TEST_F(MasterServiceTest, ReadableAfterPartialUnmountWithReplication) {
    std::unique_ptr<MasterService> service_(new MasterService());

    // Mount two large segments
    constexpr size_t buffer1 = 0x300000000;
    constexpr size_t buffer2 = 0x400000000;
    constexpr size_t segment_size = 1024 * 1024 * 64;  // 64MB
    constexpr size_t object_size = 1024 * 1024;        // 1MB

    auto segment1 = MakeSegment("segment1", buffer1, segment_size);
    auto segment2 = MakeSegment("segment2", buffer2, segment_size);
    UUID client_id = generate_uuid();
    auto mount_result1 = service_->MountSegment(segment1, client_id);
    ASSERT_TRUE(mount_result1.has_value());
    auto mount_result2 = service_->MountSegment(segment2, client_id);
    ASSERT_TRUE(mount_result2.has_value());

    // Put a key with 2 replicas
    std::string key = "replicated_key";
    std::vector<uint64_t> slice_lengths = {object_size};
    ReplicateConfig config;
    config.replica_num = 2;

    auto put_start_result = service_->PutStart(key, slice_lengths, config);
    ASSERT_TRUE(put_start_result.has_value());
    ASSERT_EQ(2u, put_start_result->size());
    ASSERT_TRUE(service_->PutEnd(key, ReplicaType::MEMORY).has_value());

    // Verify two replicas exist and they are on distinct segments
    auto get_result = service_->GetReplicaList(key);
    ASSERT_TRUE(get_result.has_value());
    auto replicas = get_result.value().replicas;
    ASSERT_EQ(2u, replicas.size());
    std::unordered_set<std::string> seg_names;
    for (const auto& rep : replicas) {
        ASSERT_EQ(ReplicaStatus::COMPLETE, rep.status);
        const auto& mem = rep.get_memory_descriptor();
        ASSERT_EQ(1u, mem.buffer_descriptors.size());
        seg_names.insert(mem.buffer_descriptors[0].transport_endpoint_);
    }
    ASSERT_EQ(2u, seg_names.size())
        << "Replicas should be on different segments";

    // Unmount one segment
    ASSERT_TRUE(service_->UnmountSegment(segment1.id, client_id).has_value());

    // Key should still be readable via the remaining replica
    auto get_after_unmount = service_->GetReplicaList(key);
    ASSERT_TRUE(get_after_unmount.has_value())
        << "Object should remain accessible with surviving replica";
}

TEST_F(MasterServiceTest, UnmountSegmentPerformance) {
    std::unique_ptr<MasterService> service_(new MasterService());
    constexpr size_t kBufferAddress = 0x300000000;
    constexpr size_t kSegmentSize = 1024 * 1024 * 256;  // 256MB
    std::string segment_name = "perf_test_segment";
    auto segment = MakeSegment(segment_name, kBufferAddress, kSegmentSize);
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
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);

    std::string key = "test_key";
    std::vector<uint64_t> slice_lengths = {1024};
    ReplicateConfig config;
    config.replica_num = 1;

    // Verify lease is granted on ExistsKey
    auto put_start_result = service_->PutStart(key, slice_lengths, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto put_end_result = service_->PutEnd(key, ReplicaType::MEMORY);
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
    auto put_start_result2 = service_->PutStart(key, slice_lengths, config);
    ASSERT_TRUE(put_start_result2.has_value());
    auto put_end_result2 = service_->PutEnd(key, ReplicaType::MEMORY);
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
    auto put_start_result3 = service_->PutStart(key, slice_lengths, config);
    ASSERT_TRUE(put_start_result3.has_value());
    auto put_end_result3 = service_->PutEnd(key, ReplicaType::MEMORY);
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
    auto put_start_result4 = service_->PutStart(key, slice_lengths, config);
    ASSERT_TRUE(put_start_result4.has_value());
    auto put_end_result4 = service_->PutEnd(key, ReplicaType::MEMORY);
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
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    for (int i = 0; i < 10; ++i) {
        std::string key = "test_key" + std::to_string(i);
        std::vector<uint64_t> slice_lengths = {1024};
        ReplicateConfig config;
        config.replica_num = 1;
        auto put_start_result = service_->PutStart(key, slice_lengths, config);
        ASSERT_TRUE(put_start_result.has_value());
        auto put_end_result = service_->PutEnd(key, ReplicaType::MEMORY);
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
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    // Mount a segment that can hold about 1024 * 16 objects.
    // As the eviction is processed separately for each shard,
    // we need to fill each shard with enough objects to thoroughly
    // test the eviction process.
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 16 * 15;
    constexpr size_t object_size = 1024 * 15;
    [[maybe_unused]] const auto context =
        PrepareSimpleSegment(*service_, "test_segment", buffer, size);

    // Verify if we can put objects more than the segment can hold
    int success_puts = 0;
    for (int i = 0; i < 1024 * 16 + 50; ++i) {
        std::string key = "test_key" + std::to_string(i);
        std::vector<uint64_t> slice_lengths = {object_size};
        ReplicateConfig config;
        config.replica_num = 1;
        auto put_start_result = service_->PutStart(key, slice_lengths, config);
        if (put_start_result.has_value()) {
            auto put_end_result = service_->PutEnd(key, ReplicaType::MEMORY);
            ASSERT_TRUE(put_end_result.has_value());
            success_puts++;
        } else {
            // wait for eviction to work
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
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 16;
    constexpr size_t object_size = 1024 * 1024;
    [[maybe_unused]] const auto context =
        PrepareSimpleSegment(*service_, "test_segment", buffer, size);

    // Verify leased object will not be evicted.
    int success_puts = 0;
    int failed_puts = 0;
    std::vector<std::string> leased_keys;
    for (int i = 0; i < 16 + 10; ++i) {
        std::string key = "test_key" + std::to_string(i);
        std::vector<uint64_t> slice_lengths = {object_size};
        ReplicateConfig config;
        config.replica_num = 1;
        auto put_start_result = service_->PutStart(key, slice_lengths, config);
        if (put_start_result.has_value()) {
            auto put_end_result = service_->PutEnd(key, ReplicaType::MEMORY);
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
    // wait for eviction to do eviction
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    // All leased objects should be accessible
    for (const auto& key : leased_keys) {
        auto get_result = service_->GetReplicaList(key);
        ASSERT_TRUE(get_result.has_value());
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));
    service_->RemoveAll();
}

TEST_F(MasterServiceTest, RemoveSoftPinObject) {
    const uint64_t kv_lease_ttl = 200;
    // set a large soft_pin_ttl so the granted soft pin will not quickly expire
    const uint64_t kv_soft_pin_ttl = 10000;
    const bool allow_evict_soft_pinned_objects = true;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .set_default_kv_soft_pin_ttl(kv_soft_pin_ttl)
                              .set_allow_evict_soft_pinned_objects(
                                  allow_evict_soft_pinned_objects)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    // Mount segment and put an object
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 16;
    [[maybe_unused]] const auto context =
        PrepareSimpleSegment(*service_, "test_segment", buffer, size);

    std::string key = "test_key";
    std::vector<uint64_t> slice_lengths = {1024};
    ReplicateConfig config;
    config.replica_num = 1;
    config.with_soft_pin = true;

    // Verify soft pin does not block remove
    ASSERT_TRUE(service_->PutStart(key, slice_lengths, config).has_value());
    ASSERT_TRUE(service_->PutEnd(key, ReplicaType::MEMORY).has_value());
    EXPECT_TRUE(service_->Remove(key).has_value());

    // Verify soft pin does not block RemoveAll
    ASSERT_TRUE(service_->PutStart(key, slice_lengths, config).has_value());
    ASSERT_TRUE(service_->PutEnd(key, ReplicaType::MEMORY).has_value());
    EXPECT_EQ(1, service_->RemoveAll());
}

TEST_F(MasterServiceTest, SoftPinObjectsNotEvictedBeforeOtherObjects) {
    const uint64_t kv_lease_ttl = 200;
    // set a large soft_pin_ttl so the granted soft pin will not quickly expire
    const uint64_t kv_soft_pin_ttl = 10000;
    const double eviction_ratio = 0.5;
    const bool allow_evict_soft_pinned_objects = true;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .set_default_kv_soft_pin_ttl(kv_soft_pin_ttl)
                              .set_allow_evict_soft_pinned_objects(
                                  allow_evict_soft_pinned_objects)
                              .set_eviction_ratio(eviction_ratio)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));

    // Mount segment and put an object
    constexpr size_t buffer = 0x300000000;
    constexpr size_t segment_size = 1024 * 1024 * 16;
    constexpr size_t value_size = 1024 * 1024;
    [[maybe_unused]] const auto context =
        PrepareSimpleSegment(*service_, "test_segment", buffer, segment_size);

    // The eviction has random factors, so test 5 times
    for (int test_i = 0; test_i < 5; test_i++) {
        // Put pin_key first
        for (int i = 0; i < 2; i++) {
            std::string pin_key = "pin_key" + std::to_string(i);
            std::vector<uint64_t> slice_lengths = {value_size};
            ReplicateConfig soft_pin_config;
            soft_pin_config.replica_num = 1;
            soft_pin_config.with_soft_pin = true;

            ASSERT_TRUE(
                service_->PutStart(pin_key, slice_lengths, soft_pin_config)
                    .has_value());
            ASSERT_TRUE(
                service_->PutEnd(pin_key, ReplicaType::MEMORY).has_value());
        }

        // Fill the segment to trigger eviction
        int failed_puts = 0;
        for (int i = 0; i < 20; i++) {
            std::string key = "key" + std::to_string(i);
            std::vector<uint64_t> slice_lengths = {value_size};
            ReplicateConfig config;
            config.replica_num = 1;
            if (service_->PutStart(key, slice_lengths, config).has_value()) {
                ASSERT_TRUE(
                    service_->PutEnd(key, ReplicaType::MEMORY).has_value());
            } else {
                failed_puts++;
            }
        }
        ASSERT_GT(failed_puts, 0);
        // wait for eviction to do eviction
        std::this_thread::sleep_for(
            std::chrono::milliseconds(kv_lease_ttl + 1000));
        // pin_key should still be accessible
        for (int i = 0; i < 2; i++) {
            std::string pin_key = "pin_key" + std::to_string(i);
            ASSERT_TRUE(service_->GetReplicaList(pin_key).has_value());
        }

        // wait for the lease to expire
        std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));
        // remove all objects before the next turn
        service_->RemoveAll();
    }
}

TEST_F(MasterServiceTest, SoftPinObjectsCanBeEvicted) {
    const uint64_t kv_lease_ttl = 200;
    // set a large soft_pin_ttl so the granted soft pin will not quickly expire
    const uint64_t kv_soft_pin_ttl = 10000;
    const bool allow_evict_soft_pinned_objects = true;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .set_default_kv_soft_pin_ttl(kv_soft_pin_ttl)
                              .set_allow_evict_soft_pinned_objects(
                                  allow_evict_soft_pinned_objects)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));

    // Mount segment and put an object
    constexpr size_t buffer = 0x300000000;
    constexpr size_t segment_size = 1024 * 1024 * 16;
    constexpr size_t value_size = 1024 * 1024;
    [[maybe_unused]] const auto context =
        PrepareSimpleSegment(*service_, "test_segment", buffer, segment_size);

    // Verify if we can put objects more than the segment can hold
    int success_puts = 0;
    for (int i = 0; i < 16 + 50; ++i) {
        std::string key = "test_key" + std::to_string(i);
        std::vector<uint64_t> slice_lengths = {value_size};
        ReplicateConfig config;
        config.replica_num = 1;
        config.with_soft_pin = true;
        if (service_->PutStart(key, slice_lengths, config).has_value()) {
            ASSERT_TRUE(service_->PutEnd(key, ReplicaType::MEMORY).has_value());
            success_puts++;
        } else {
            // wait for eviction to work
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }
    ASSERT_GT(success_puts, 16);
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));
    service_->RemoveAll();
}

TEST_F(MasterServiceTest, SoftPinExtendedOnGet) {
    const uint64_t kv_lease_ttl = 200;
    // The soft pin ttl shall not be too large, otherwise the test will take too
    // long
    const uint64_t kv_soft_pin_ttl = 1000;
    static_assert(
        kv_soft_pin_ttl > kv_lease_ttl,
        "kv_soft_pin_ttl must be larger than kv_lease_ttl in this test");
    const double eviction_ratio = 0.5;
    const bool allow_evict_soft_pinned_objects = true;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .set_default_kv_soft_pin_ttl(kv_soft_pin_ttl)
                              .set_allow_evict_soft_pinned_objects(
                                  allow_evict_soft_pinned_objects)
                              .set_eviction_ratio(eviction_ratio)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));

    // Mount segment and put an object
    constexpr size_t buffer = 0x300000000;
    constexpr size_t segment_size = 1024 * 1024 * 16;
    constexpr size_t value_size = 1024 * 1024;
    [[maybe_unused]] const auto context =
        PrepareSimpleSegment(*service_, "test_segment", buffer, segment_size);

    // The eviction has random factors, so test 3 times
    for (int test_i = 0; test_i < 3; test_i++) {
        // Put pin_key first
        for (int i = 0; i < 2; i++) {
            std::string pin_key = "pin_key" + std::to_string(i);
            std::vector<uint64_t> slice_lengths = {value_size};
            ReplicateConfig soft_pin_config;
            soft_pin_config.replica_num = 1;
            soft_pin_config.with_soft_pin = true;

            ASSERT_TRUE(
                service_->PutStart(pin_key, slice_lengths, soft_pin_config));
            ASSERT_TRUE(
                service_->PutEnd(pin_key, ReplicaType::MEMORY).has_value());
        }

        // Wait for the soft pin to expire
        std::this_thread::sleep_for(std::chrono::milliseconds(kv_soft_pin_ttl));

        // Get the pin_key to extend the soft pin
        for (int i = 0; i < 2; i++) {
            std::string pin_key = "pin_key" + std::to_string(i);
            ASSERT_TRUE(service_->GetReplicaList(pin_key).has_value());
        }

        // Fill the segment to trigger eviction
        int failed_puts = 0;
        for (int i = 0; i < 16; i++) {
            std::string key = "key" + std::to_string(i);
            std::vector<uint64_t> slice_lengths = {value_size};
            ReplicateConfig config;
            config.replica_num = 1;
            if (service_->PutStart(key, slice_lengths, config).has_value()) {
                ASSERT_TRUE(
                    service_->PutEnd(key, ReplicaType::MEMORY).has_value());
            } else {
                failed_puts++;
            }
        }
        ASSERT_GT(failed_puts, 0);

        // wait for eviction
        std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));

        // pin_key should still be accessible
        for (int i = 0; i < 2; i++) {
            std::string pin_key = "pin_key" + std::to_string(i);
            ASSERT_TRUE(service_->GetReplicaList(pin_key).has_value());
        }

        // wait for the lease to expire
        std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));
        // remove all objects before the next turn
        service_->RemoveAll();
    }
}

TEST_F(MasterServiceTest, SoftPinObjectsNotAllowEvict) {
    const uint64_t kv_lease_ttl = 200;
    // set a large soft_pin_ttl so the granted soft pin will not quickly expire
    const uint64_t kv_soft_pin_ttl = 10000;
    // set allow_evict_soft_pinned_objects to false to disable eviction of soft
    // pinned objects
    const bool allow_evict_soft_pinned_objects = false;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .set_default_kv_soft_pin_ttl(kv_soft_pin_ttl)
                              .set_allow_evict_soft_pinned_objects(
                                  allow_evict_soft_pinned_objects)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));

    // Mount segment and put an object
    constexpr size_t buffer = 0x300000000;
    constexpr size_t segment_size = 1024 * 1024 * 16;
    constexpr size_t value_size = 1024 * 1024;
    [[maybe_unused]] const auto context =
        PrepareSimpleSegment(*service_, "test_segment", buffer, segment_size);

    // Put objects more than the segment can hold
    std::vector<std::string> success_keys;
    for (int i = 0; i < 16 + 50; ++i) {
        std::string key = "test_key" + std::to_string(i);
        std::vector<uint64_t> slice_lengths = {value_size};
        ReplicateConfig config;
        config.replica_num = 1;
        config.with_soft_pin = true;
        if (service_->PutStart(key, slice_lengths, config).has_value()) {
            ASSERT_TRUE(service_->PutEnd(key, ReplicaType::MEMORY).has_value());
            success_keys.push_back(key);
        } else {
            // wait for eviction to work
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }
    ASSERT_LE(success_keys.size(), 17);
    // All soft pinned objects should be accessible
    for (const auto& key : success_keys) {
        ASSERT_TRUE(service_->GetReplicaList(key).has_value());
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl));
    service_->RemoveAll();
}

TEST_F(MasterServiceTest, PerSliceReplicaSegmentsAreUnique) {
    std::unique_ptr<MasterService> service_(new MasterService());

    // Mount 20 segments, each 16MB and slab-aligned
    constexpr size_t kBaseAddr = 0x300000000;
    constexpr size_t kSegmentSize = 1024 * 1024 * 16;  // 16MB
    for (int i = 0; i < 20; ++i) {
        [[maybe_unused]] const auto context = PrepareSimpleSegment(
            *service_, "segment_" + std::to_string(i),
            kBaseAddr + static_cast<size_t>(i) * kSegmentSize, kSegmentSize);
    }

    // Object with 16 slices of ~1MB and replication factor 10
    const std::string key = "replica_uniqueness_test_key";
    std::vector<uint64_t> slice_lengths(16, 1024 * 1024 - 16);
    ReplicateConfig config;
    config.replica_num = 10;

    auto put_start_result = service_->PutStart(key, slice_lengths, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto replica_list_local = put_start_result.value();
    ASSERT_EQ(config.replica_num, replica_list_local.size());

    // For each slice index, segment names across replicas must be unique
    for (size_t slice_idx = 0; slice_idx < slice_lengths.size(); ++slice_idx) {
        std::unordered_set<std::string> segment_names;
        for (const auto& replica : replica_list_local) {
            ASSERT_TRUE(replica.is_memory_replica());
            const auto& mem = replica.get_memory_descriptor();
            ASSERT_EQ(slice_lengths.size(), mem.buffer_descriptors.size());
            segment_names.insert(
                mem.buffer_descriptors[slice_idx].transport_endpoint_);
        }
        EXPECT_EQ(segment_names.size(), config.replica_num)
            << "Duplicate segment found for slice index " << slice_idx;
    }

    ASSERT_TRUE(service_->PutEnd(key, ReplicaType::MEMORY).has_value());
}

TEST_F(MasterServiceTest, ReplicationFactorTwoWithSingleSegment) {
    std::unique_ptr<MasterService> service_(new MasterService());

    // Mount a single 16MB segment
    constexpr size_t kBaseAddr = 0x300000000;
    constexpr size_t kSegmentSize = 1024 * 1024 * 16;  // 16MB
    [[maybe_unused]] const auto context = PrepareSimpleSegment(
        *service_, "single_segment", kBaseAddr, kSegmentSize);

    // Request replication factor 2 with a single 1KB slice
    // With best-effort semantics, should succeed with 1 replica
    const std::string key = "replication_factor_two_single_segment";
    std::vector<uint64_t> slice_lengths{1024};
    ReplicateConfig config;
    config.replica_num = 2;

    auto put_start_result = service_->PutStart(key, slice_lengths, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto replicas = put_start_result.value();

    // Should get 1 replica instead of the requested 2 (best-effort)
    EXPECT_EQ(1u, replicas.size());
    EXPECT_TRUE(replicas[0].is_memory_replica());

    // Verify the replica is properly allocated on the single segment
    auto mem_desc = replicas[0].get_memory_descriptor();
    EXPECT_EQ(1u, mem_desc.buffer_descriptors.size());
    EXPECT_EQ("single_segment",
              mem_desc.buffer_descriptors[0].transport_endpoint_);
    EXPECT_EQ(1024u, mem_desc.buffer_descriptors[0].size_);
}

TEST_F(MasterServiceTest, BatchExistKeyTest) {
    std::unique_ptr<MasterService> service_(new MasterService());

    // Mount a segment
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 128;
    constexpr size_t value_size = 1024;
    [[maybe_unused]] const auto context =
        PrepareSimpleSegment(*service_, "test_segment", buffer, size);

    int test_object_num = 10;
    std::vector<std::string> test_keys;
    for (int i = 0; i < test_object_num; ++i) {
        test_keys.push_back("test_key" + std::to_string(i));
        ReplicateConfig config;
        config.replica_num = 1;
        std::vector<uint64_t> slice_lengths = {value_size};
        auto put_start_result =
            service_->PutStart(test_keys[i], slice_lengths, config);
        ASSERT_TRUE(put_start_result.has_value());
        auto put_end_result =
            service_->PutEnd(test_keys[i], ReplicaType::MEMORY);
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

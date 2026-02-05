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

std::string GenerateKeyForSegment(const UUID& client_id,
                                  const std::unique_ptr<MasterService>& service,
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
            service->PutStart(client_id, key, {1024}, {.replica_num = 1});
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
        auto put_end_result =
            service->PutEnd(client_id, key, ReplicaType::MEMORY);
        if (!put_end_result.has_value()) {
            throw std::runtime_error("PutEnd failed");
        }
        if (replica_list[0]
                .get_memory_descriptor()
                .buffer_descriptor.transport_endpoint_ == segment_name) {
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
    const UUID client_id = generate_uuid();

    std::string key = "test_key";
    ReplicateConfig config;

    // Test invalid replica_num
    config.replica_num = 0;
    auto put_result1 = service_->PutStart(client_id, key, 1024, config);
    EXPECT_FALSE(put_result1.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, put_result1.error());

    // Test zero slice_length
    config.replica_num = 1;
    auto put_result2 = service_->PutStart(client_id, key, 0, config);
    EXPECT_FALSE(put_result2.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, put_result2.error());
}

TEST_F(MasterServiceTest, PutStartEndFlow) {
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();
    const UUID invalid_client_id = generate_uuid();
    ASSERT_NE(client_id, invalid_client_id);

    // Test PutStart
    std::string key = "test_key";
    uint64_t value_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;

    auto put_start_result =
        service_->PutStart(client_id, key, value_length, config);
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

    // PutEnd should fail if the client_id does not match.
    auto put_end_fail_result =
        service_->PutEnd(invalid_client_id, key, ReplicaType::MEMORY);
    EXPECT_FALSE(put_end_fail_result.has_value());
    EXPECT_EQ(put_end_fail_result.error(), ErrorCode::ILLEGAL_CLIENT);

    // PutRevoke should fail if the client_id does not match.
    auto put_revoke_fail_result =
        service_->PutRevoke(invalid_client_id, key, ReplicaType::MEMORY);
    EXPECT_FALSE(put_revoke_fail_result.has_value());
    EXPECT_EQ(put_revoke_fail_result.error(), ErrorCode::ILLEGAL_CLIENT);

    // Test PutEnd
    auto put_end_result = service_->PutEnd(client_id, key, ReplicaType::MEMORY);
    EXPECT_TRUE(put_end_result.has_value());

    // Verify replica list after PutEnd
    auto final_get_result = service_->GetReplicaList(key);
    EXPECT_TRUE(final_get_result.has_value());
    replica_list = final_get_result.value().replicas;
    EXPECT_EQ(1, replica_list.size());
    EXPECT_EQ(ReplicaStatus::COMPLETE, replica_list[0].status);
}

TEST_F(MasterServiceTest, PutWithPreferredSegment) {
    // For backward compatibility, test the deprecated single preferred_segment
    std::unique_ptr<MasterService> service_(new MasterService());
    const UUID client_id = generate_uuid();

    // Mount 3 segments, each 16MB
    constexpr size_t kBaseAddr = 0x300000000;
    constexpr size_t kSegmentSize = 1024 * 1024 * 16;  // 16MB
    for (int i = 0; i < 3; ++i) {
        [[maybe_unused]] const auto context = PrepareSimpleSegment(
            *service_, "segment_" + std::to_string(i),
            kBaseAddr + static_cast<size_t>(i) * kSegmentSize, kSegmentSize);
    }

    // Prepare preferred segments
    std::string preferred_segment = "segment_1";

    // Test PutStart with multiple preferred segments
    std::string key = "test_key";
    uint64_t value_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = preferred_segment;

    auto put_start_result =
        service_->PutStart(client_id, key, value_length, config);
    EXPECT_TRUE(put_start_result.has_value());
    replica_list = put_start_result.value();
    EXPECT_EQ(1, replica_list.size());
    EXPECT_EQ(ReplicaStatus::PROCESSING, replica_list[0].status);
    const auto& mem_desc = replica_list[0].get_memory_descriptor();
    EXPECT_EQ(preferred_segment,
              mem_desc.buffer_descriptor.transport_endpoint_);

    // Complete the Put operation
    auto put_end_result = service_->PutEnd(client_id, key, ReplicaType::MEMORY);
    EXPECT_TRUE(put_end_result.has_value());
}

TEST_F(MasterServiceTest, PutWithPreferredSegments) {
    std::unique_ptr<MasterService> service_(new MasterService());
    const UUID client_id = generate_uuid();

    // Mount 3 segments, each 16MB
    constexpr size_t kBaseAddr = 0x300000000;
    constexpr size_t kSegmentSize = 1024 * 1024 * 16;  // 16MB
    for (int i = 0; i < 3; ++i) {
        [[maybe_unused]] const auto context = PrepareSimpleSegment(
            *service_, "segment_" + std::to_string(i),
            kBaseAddr + static_cast<size_t>(i) * kSegmentSize, kSegmentSize);
    }

    // Prepare preferred segments
    std::vector<std::string> preferred_segments = {"segment_0", "segment_1"};

    // Test PutStart with multiple preferred segments
    std::string key = "test_key";
    uint64_t value_length = 1024;
    ReplicateConfig config;
    config.replica_num = 2;
    config.preferred_segments = preferred_segments;

    auto put_start_result =
        service_->PutStart(client_id, key, value_length, config);
    EXPECT_TRUE(put_start_result.has_value());
    replica_list = put_start_result.value();
    EXPECT_EQ(2, replica_list.size());
    EXPECT_EQ(ReplicaStatus::PROCESSING, replica_list[0].status);
    EXPECT_EQ(ReplicaStatus::PROCESSING, replica_list[1].status);
    const auto& mem_desc1 = replica_list[0].get_memory_descriptor();
    const auto& mem_desc2 = replica_list[1].get_memory_descriptor();
    std::unordered_set<std::string> used_segments = {
        mem_desc1.buffer_descriptor.transport_endpoint_,
        mem_desc2.buffer_descriptor.transport_endpoint_};
    EXPECT_TRUE(used_segments.find("segment_0") != used_segments.end());
    EXPECT_TRUE(used_segments.find("segment_1") != used_segments.end());

    // Complete the Put operation
    auto put_end_result = service_->PutEnd(client_id, key, ReplicaType::MEMORY);
    EXPECT_TRUE(put_end_result.has_value());
}

TEST_F(MasterServiceTest, RandomPutStartEndFlow) {
    std::unique_ptr<MasterService> service_(new MasterService());
    const UUID client_id = generate_uuid();

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
    ReplicateConfig config;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(1, 5);
    int random_number = dis(gen);
    config.replica_num = random_number;
    auto put_start_result =
        service_->PutStart(client_id, key, value_length, config);
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
    auto put_end_result = service_->PutEnd(client_id, key, ReplicaType::MEMORY);
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
    const UUID client_id = generate_uuid();
    // Test getting non-existent key
    auto get_result = service_->GetReplicaList(".*non_existent.*");
    EXPECT_FALSE(get_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, get_result.error());

    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);

    int times = 10;
    while (times--) {
        std::string key = "test_key" + std::to_string(times);
        uint64_t value_length = 1024;
        ReplicateConfig config;
        config.replica_num = 1;
        auto put_start_result =
            service_->PutStart(client_id, key, value_length, config);
        ASSERT_TRUE(put_start_result.has_value());
        auto put_end_result =
            service_->PutEnd(client_id, key, ReplicaType::MEMORY);
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
void put_object(MasterService& service, const UUID& client_id,
                const std::string& key) {
    uint64_t value_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;
    auto put_start_result =
        service.PutStart(client_id, key, value_length, config);
    ASSERT_TRUE(put_start_result.has_value())
        << "Failed to PutStart for key: " << key;
    auto put_end_result = service.PutEnd(client_id, key, ReplicaType::MEMORY);
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
    const UUID client_id = generate_uuid();

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
        put_object(*service_, client_id, key);
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
    const UUID client_id = generate_uuid();
    // Test getting non-existent key
    auto get_result = service_->GetReplicaList("non_existent");
    EXPECT_FALSE(get_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, get_result.error());

    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);

    std::string key = "test_key";
    uint64_t value_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;
    auto put_start_result =
        service_->PutStart(client_id, key, value_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto put_end_result = service_->PutEnd(client_id, key, ReplicaType::MEMORY);
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
    const UUID client_id = generate_uuid();

    std::string key = "test_key";
    uint64_t value_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;
    auto put_start_result =
        service_->PutStart(client_id, key, value_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto put_end_result = service_->PutEnd(client_id, key, ReplicaType::MEMORY);
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
    const UUID client_id = generate_uuid();
    int times = 10;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(1, 1000);
    while (times--) {
        std::string key = "test_key" + std::to_string(dis(gen));
        uint64_t value_length = 1024;
        ReplicateConfig config;
        config.replica_num = 1;
        auto put_start_result =
            service_->PutStart(client_id, key, value_length, config);
        ASSERT_TRUE(put_start_result.has_value());
        auto put_end_result =
            service_->PutEnd(client_id, key, ReplicaType::MEMORY);
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
    const UUID client_id = generate_uuid();
    int times = 10;
    while (times--) {
        std::string key = "test_key" + std::to_string(times);
        uint64_t value_length = 1024;
        ReplicateConfig config;
        config.replica_num = 1;
        auto put_start_result =
            service_->PutStart(client_id, key, value_length, config);
        ASSERT_TRUE(put_start_result.has_value());
        auto put_end_result =
            service_->PutEnd(client_id, key, ReplicaType::MEMORY);
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

TEST_F(MasterServiceTest, CopyStart) {
    const uint64_t kv_lease_ttl = 50;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));

    // Mount 4 segments (segment_1, segment_2, segment_3, segment_4) with
    // PrepareSimpleSegment
    [[maybe_unused]] const auto context1 =
        PrepareSimpleSegment(*service_, "segment_1");
    [[maybe_unused]] const auto context2 =
        PrepareSimpleSegment(*service_, "segment_2");
    [[maybe_unused]] const auto context3 =
        PrepareSimpleSegment(*service_, "segment_3");
    [[maybe_unused]] const auto context4 =
        PrepareSimpleSegment(*service_, "segment_4");

    UUID client_id = generate_uuid();

    // Test Case 1: CopyStart a non-existent key, should fail.
    auto copy_result = service_->CopyStart(client_id, "non_existent_key",
                                           "segment_1", {"segment_2"});
    EXPECT_FALSE(copy_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, copy_result.error());

    // PutStart an object with 1 replica and preferred_segment=segment_1 for
    // testing
    std::string key = "test_key";
    uint64_t slice_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = "segment_1";

    auto put_start_result =
        service_->PutStart(client_id, key, slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());

    // Test Case 2: CopyStart to segment_2 and segment_3, should fail because
    // the only replica is not completed.
    copy_result = service_->CopyStart(client_id, key, "segment_1",
                                      {"segment_2", "segment_3"});
    EXPECT_FALSE(copy_result.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_NOT_FOUND, copy_result.error());

    // PutEnd the object.
    auto put_end_result = service_->PutEnd(client_id, key, ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    // Test Case 3: CopyStart to segment_2 and segment_3, should success.
    copy_result = service_->CopyStart(client_id, key, "segment_1",
                                      {"segment_2", "segment_3"});
    EXPECT_TRUE(copy_result.has_value());
    auto copy_response = copy_result.value();
    EXPECT_EQ("segment_1", copy_response.source.get_memory_descriptor()
                               .buffer_descriptor.transport_endpoint_);
    EXPECT_EQ(2, copy_response.targets.size());

    // Test Case 4: Try remove the object, should fail because it is copying.
    auto remove_result = service_->Remove(key);
    EXPECT_FALSE(remove_result.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_NOT_READY, remove_result.error());

    // Test Case 5: CopyStart to segment_4, should fail because there is an
    // ongoing copy task.
    copy_result =
        service_->CopyStart(client_id, key, "segment_1", {"segment_4"});
    EXPECT_FALSE(copy_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_HAS_REPLICATION_TASK, copy_result.error());

    // Test Case 6: CopyEnd, should success and the object now has 3 replicas.
    auto copy_end_result = service_->CopyEnd(client_id, key);
    EXPECT_TRUE(copy_end_result.has_value());
    auto get_result = service_->GetReplicaList(key);
    EXPECT_TRUE(get_result.has_value());
    EXPECT_EQ(3, get_result.value().replicas.size());

    // Test Case 7: Copy from a non-existent replica to segment_3 and
    // segment_4, should fail.
    copy_result = service_->CopyStart(client_id, key, "non_existent_segment",
                                      {"segment_3", "segment_4"});
    EXPECT_FALSE(copy_result.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_NOT_FOUND, copy_result.error());

    // Test Case 8: Copy to segment_4 and a non-existent segment, should fail.
    copy_result = service_->CopyStart(client_id, key, "segment_1",
                                      {"segment_4", "non_existent_segment"});
    EXPECT_FALSE(copy_result.has_value());
    EXPECT_EQ(ErrorCode::SEGMENT_NOT_FOUND, copy_result.error());

    // Test Case 9: Copy to segment_3 and segment_4, should skip segment_3 and
    // successfully copy to segment_4.
    copy_result = service_->CopyStart(client_id, key, "segment_1",
                                      {"segment_3", "segment_4"});
    EXPECT_TRUE(copy_result.has_value());
    copy_response = copy_result.value();
    EXPECT_EQ("segment_1", copy_response.source.get_memory_descriptor()
                               .buffer_descriptor.transport_endpoint_);
    EXPECT_EQ(1, copy_response.targets
                     .size());  // Only 1 replica since segment_3 is skipped
    EXPECT_EQ("segment_4", copy_response.targets[0]
                               .get_memory_descriptor()
                               .buffer_descriptor.transport_endpoint_);

    // End the copy operation to clean up state
    copy_end_result = service_->CopyEnd(client_id, key);
    EXPECT_TRUE(copy_end_result.has_value());
    get_result = service_->GetReplicaList(key);
    EXPECT_TRUE(get_result.has_value());
    EXPECT_EQ(4, get_result.value().replicas.size());

    // Test Case 10: Copy to segment_4 again, should skip because it's already
    // used.
    copy_result =
        service_->CopyStart(client_id, key, "segment_1", {"segment_4"});
    EXPECT_TRUE(copy_result.has_value());
    copy_response = copy_result.value();
    EXPECT_EQ("segment_1", copy_response.source.get_memory_descriptor()
                               .buffer_descriptor.transport_endpoint_);
    EXPECT_EQ(0,
              copy_response.targets
                  .size());  // No replicas since segment_4 is already used

    // Wait for the lease to expire
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl * 2));

    // Test Case 11: Try remove the object, should fail because it is copying.
    remove_result = service_->Remove(key);
    EXPECT_FALSE(remove_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_HAS_REPLICATION_TASK, remove_result.error());

    // Clean up the copy operation
    copy_end_result = service_->CopyEnd(client_id, key);
    EXPECT_TRUE(copy_end_result.has_value());

    // Wait for the lease to expire
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl * 2));

    // Test Case 12: Try remove the object, should success.
    remove_result = service_->Remove(key);
    EXPECT_TRUE(remove_result.has_value());
}

TEST_F(MasterServiceTest, CopyEnd) {
    std::unique_ptr<MasterService> service_(new MasterService());

    // Mount 3 segments (segment_1, segment_2, segment_3) with
    // PrepareSimpleSegment
    [[maybe_unused]] const auto context1 =
        PrepareSimpleSegment(*service_, "segment_1");
    [[maybe_unused]] const auto context2 =
        PrepareSimpleSegment(*service_, "segment_2");
    [[maybe_unused]] const auto context3 =
        PrepareSimpleSegment(*service_, "segment_3");

    UUID client_id = generate_uuid();
    UUID invalid_client_id = generate_uuid();

    // Test Case 1: CopyEnd a non-existent key, should fail.
    auto copy_end_result = service_->CopyEnd(client_id, "non_existent_key");
    EXPECT_FALSE(copy_end_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, copy_end_result.error());

    // Put an object with 1 replica and preferred_segment=segment_1 for testing
    std::string key = "test_key";
    uint64_t slice_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = "segment_1";

    auto put_start_result =
        service_->PutStart(client_id, key, slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto put_end_result = service_->PutEnd(client_id, key, ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    // Test Case 2: CopyEnd the object, should fail because there is no ongoing
    // copy task.
    copy_end_result = service_->CopyEnd(client_id, key);
    EXPECT_FALSE(copy_end_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NO_REPLICATION_TASK, copy_end_result.error());

    // CopyStart the object to segment_2
    auto copy_start_result =
        service_->CopyStart(client_id, key, "segment_1", {"segment_2"});
    ASSERT_TRUE(copy_start_result.has_value());

    // Test Case 3: CopyEnd with an invalid client id, should fail.
    copy_end_result = service_->CopyEnd(invalid_client_id, key);
    EXPECT_FALSE(copy_end_result.has_value());
    EXPECT_EQ(ErrorCode::ILLEGAL_CLIENT, copy_end_result.error());

    // Test Case 4: MoveEnd the object, should fail because the ongoing task is
    // Copy.
    auto move_end_result = service_->MoveEnd(client_id, key);
    EXPECT_FALSE(move_end_result.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, move_end_result.error());

    // Test Case 5: CopyEnd, should success.
    copy_end_result = service_->CopyEnd(client_id, key);
    EXPECT_TRUE(copy_end_result.has_value());

    // Verify we now have 2 replicas
    auto get_result = service_->GetReplicaList(key);
    EXPECT_TRUE(get_result.has_value());
    EXPECT_EQ(2, get_result.value().replicas.size());

    // CopyStart the object from segment_1 to segment_3, then unmount segment_1
    copy_start_result =
        service_->CopyStart(client_id, key, "segment_1", {"segment_3"});
    ASSERT_TRUE(copy_start_result.has_value());

    // Unmount segment_1 to simulate source gone
    auto unmount_result =
        service_->UnmountSegment(context1.segment_id, context1.client_id);
    ASSERT_TRUE(unmount_result.has_value());

    // Test Case 6: CopyEnd, should fail because the source is gone, the object
    // should have only 1 replica from segment_2.
    copy_end_result = service_->CopyEnd(client_id, key);
    EXPECT_FALSE(copy_end_result.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_GONE, copy_end_result.error());
    get_result = service_->GetReplicaList(key);
    EXPECT_TRUE(get_result.has_value());
    auto& replicas = get_result.value().replicas;
    EXPECT_EQ(1, replicas.size());
    EXPECT_EQ("segment_2", replicas[0]
                               .get_memory_descriptor()
                               .buffer_descriptor.transport_endpoint_);

    // CopyStart the object from segment_2 to segment_3, then unmount segment_3
    copy_start_result =
        service_->CopyStart(client_id, key, "segment_2", {"segment_3"});
    ASSERT_TRUE(copy_start_result.has_value());

    // Unmount segment_3 to simulate target gone
    unmount_result =
        service_->UnmountSegment(context3.segment_id, context3.client_id);
    ASSERT_TRUE(unmount_result.has_value());

    // Test Case 7: CopyEnd, should fail because the target is gone, the object
    // should have only 1 replica from segment_2.
    copy_end_result = service_->CopyEnd(client_id, key);
    EXPECT_FALSE(copy_end_result.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_GONE, copy_end_result.error());
    get_result = service_->GetReplicaList(key);
    EXPECT_TRUE(get_result.has_value());
    replicas = get_result.value().replicas;
    EXPECT_EQ(1, replicas.size());
    EXPECT_EQ("segment_2", replicas[0]
                               .get_memory_descriptor()
                               .buffer_descriptor.transport_endpoint_);
}

TEST_F(MasterServiceTest, CopyRevoke) {
    std::unique_ptr<MasterService> service_(new MasterService());

    // Mount 2 segments (segment_1, segment_2) with
    // PrepareSimpleSegment
    [[maybe_unused]] const auto context1 =
        PrepareSimpleSegment(*service_, "segment_1");
    [[maybe_unused]] const auto context2 =
        PrepareSimpleSegment(*service_, "segment_2");

    UUID client_id = generate_uuid();
    UUID invalid_client_id = generate_uuid();

    // Test Case 1: CopyRevoke a non-existent key, should fail.
    auto copy_revoke_result =
        service_->CopyRevoke(client_id, "non_existent_key");
    EXPECT_FALSE(copy_revoke_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, copy_revoke_result.error());

    // Put an object with 1 replica and preferred_segment=segment_1 for testing
    std::string key = "test_key";
    uint64_t slice_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = "segment_1";

    auto put_start_result =
        service_->PutStart(client_id, key, slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto put_end_result = service_->PutEnd(client_id, key, ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    // Test Case 2: CopyRevoke the object, should fail because there is no
    // ongoing copy task.
    copy_revoke_result = service_->CopyRevoke(client_id, key);
    EXPECT_FALSE(copy_revoke_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NO_REPLICATION_TASK,
              copy_revoke_result.error());

    // CopyStart the object to segment_2
    auto copy_start_result =
        service_->CopyStart(client_id, key, "segment_1", {"segment_2"});
    ASSERT_TRUE(copy_start_result.has_value());

    // Test Case 3: CopyRevoke with an invalid client id, should fail.
    copy_revoke_result = service_->CopyRevoke(invalid_client_id, key);
    EXPECT_FALSE(copy_revoke_result.has_value());
    EXPECT_EQ(ErrorCode::ILLEGAL_CLIENT, copy_revoke_result.error());

    // Test Case 4: MoveRevoke the object, should fail because the ongoing task
    // is Copy.
    auto move_revoke_result = service_->MoveRevoke(client_id, key);
    EXPECT_FALSE(move_revoke_result.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, move_revoke_result.error());

    // Test Case 5: CopyRevoke, should success.
    copy_revoke_result = service_->CopyRevoke(client_id, key);
    EXPECT_TRUE(copy_revoke_result.has_value());

    // Verify we still have 1 replica (the copy was revoked)
    auto get_result = service_->GetReplicaList(key);
    EXPECT_TRUE(get_result.has_value());
    EXPECT_EQ(1, get_result.value().replicas.size());

    // CopyStart the object from segment_1 to segment_2 again, then unmount
    // segment_1
    copy_start_result =
        service_->CopyStart(client_id, key, "segment_1", {"segment_2"});
    ASSERT_TRUE(copy_start_result.has_value());

    // Unmount segment_1 to simulate source gone
    auto unmount_result =
        service_->UnmountSegment(context1.segment_id, context1.client_id);
    ASSERT_TRUE(unmount_result.has_value());

    // Test Case 6: CopyRevoke, should success even though the source is gone,
    // the object should be erased too.
    copy_revoke_result = service_->CopyRevoke(client_id, key);
    EXPECT_TRUE(copy_revoke_result.has_value());

    // Verify the object has been removed.
    get_result = service_->GetReplicaList(key);
    EXPECT_FALSE(get_result.has_value());
}

TEST_F(MasterServiceTest, MoveStart) {
    const uint64_t kv_lease_ttl = 50;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));

    // Mount 3 segments (segment_1, segment_2, segment_3) with
    // PrepareSimpleSegment
    [[maybe_unused]] const auto context1 =
        PrepareSimpleSegment(*service_, "segment_1");
    [[maybe_unused]] const auto context2 =
        PrepareSimpleSegment(*service_, "segment_2");
    [[maybe_unused]] const auto context3 =
        PrepareSimpleSegment(*service_, "segment_3");

    UUID client_id = generate_uuid();

    // Test Case 1: MoveStart a non-existent key, should fail.
    auto move_start_result = service_->MoveStart(client_id, "non_existent_key",
                                                 "segment_1", "segment_2");
    EXPECT_FALSE(move_start_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, move_start_result.error());

    // Put an object with 1 replica and preferred_segment=segment_1 for testing
    std::string key = "test_key";
    uint64_t slice_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = "segment_1";

    auto put_start_result =
        service_->PutStart(client_id, key, slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());

    // Test Case 2: MoveStart the object, should fail because the only replica
    // is not completed.
    move_start_result =
        service_->MoveStart(client_id, key, "segment_1", "segment_2");
    EXPECT_FALSE(move_start_result.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_NOT_FOUND, move_start_result.error());

    // PutEnd the object.
    auto put_end_result = service_->PutEnd(client_id, key, ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    // Copy the object to segment_3.
    auto copy_start_result =
        service_->CopyStart(client_id, key, "segment_1", {"segment_3"});
    ASSERT_TRUE(copy_start_result.has_value());
    auto copy_end_result = service_->CopyEnd(client_id, key);
    ASSERT_TRUE(copy_end_result.has_value());

    // Test Case 3: MoveStart with source and target be the same, should fail.
    move_start_result =
        service_->MoveStart(client_id, key, "segment_1", "segment_1");
    EXPECT_FALSE(move_start_result.has_value());
    EXPECT_EQ(move_start_result.error(), ErrorCode::INVALID_PARAMS);

    // Test Case 4: MoveStart to segment_2, should succeed.
    move_start_result =
        service_->MoveStart(client_id, key, "segment_1", "segment_2");
    EXPECT_TRUE(move_start_result.has_value());
    auto move_response = move_start_result.value();
    EXPECT_EQ("segment_1", move_response.source.get_memory_descriptor()
                               .buffer_descriptor.transport_endpoint_);
    EXPECT_TRUE(move_response.target.has_value());
    EXPECT_EQ("segment_2", move_response.target.value()
                               .get_memory_descriptor()
                               .buffer_descriptor.transport_endpoint_);

    // Test Case 5: Try remove the object, should fail because it is moving.
    auto remove_result = service_->Remove(key);
    EXPECT_FALSE(remove_result.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_NOT_READY, remove_result.error());

    // Test Case 6: MoveStart again, should fail because there is an ongoing
    // move task.
    move_start_result =
        service_->MoveStart(client_id, key, "segment_1", "segment_3");
    EXPECT_FALSE(move_start_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_HAS_REPLICATION_TASK,
              move_start_result.error());

    // Test Case 7: MoveEnd, should succeed and the object now has 2 replicas
    // from segment_2 and segment_3
    auto move_end_result = service_->MoveEnd(client_id, key);
    EXPECT_TRUE(move_end_result.has_value());

    auto get_result = service_->GetReplicaList(key);
    EXPECT_TRUE(get_result.has_value());
    auto& replicas = get_result.value().replicas;
    EXPECT_EQ(2, replicas.size());

    // Test Case 8: Move from a non-existent replica to segment_1, should fail.
    move_start_result = service_->MoveStart(
        client_id, key, "non_existent_segment", "segment_1");
    EXPECT_FALSE(move_start_result.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_NOT_FOUND, move_start_result.error());

    // Test Case 9: Move to an already existing segment, should succeed but
    // return nullopt.
    move_start_result =
        service_->MoveStart(client_id, key, "segment_2", "segment_3");
    EXPECT_TRUE(move_start_result.has_value());
    move_response = move_start_result.value();
    EXPECT_EQ("segment_2", move_response.source.get_memory_descriptor()
                               .buffer_descriptor.transport_endpoint_);
    EXPECT_FALSE(move_response.target.has_value());

    // Test Case 10: Try remove the object, should fail because it is moving.
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl * 2));
    remove_result = service_->Remove(key);
    EXPECT_FALSE(remove_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_HAS_REPLICATION_TASK, remove_result.error());

    // End the move.
    move_end_result = service_->MoveEnd(client_id, key);
    EXPECT_TRUE(move_end_result.has_value());

    // Now the object should have only 1 replica on segment_3.
    get_result = service_->GetReplicaList(key);
    EXPECT_TRUE(get_result.has_value());
    replicas = get_result.value().replicas;
    EXPECT_EQ(1, replicas.size());
    EXPECT_EQ("segment_3", replicas[0]
                               .get_memory_descriptor()
                               .buffer_descriptor.transport_endpoint_);

    // Test Case 11: Try remove the object, should succeed after lease expires.
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl * 2));
    remove_result = service_->Remove(key);
    EXPECT_TRUE(remove_result.has_value());
}

TEST_F(MasterServiceTest, MoveEnd) {
    std::unique_ptr<MasterService> service_(new MasterService());

    // Mount 2 segments (segment_1, segment_2) with
    // PrepareSimpleSegment
    [[maybe_unused]] const auto context1 =
        PrepareSimpleSegment(*service_, "segment_1");
    [[maybe_unused]] const auto context2 =
        PrepareSimpleSegment(*service_, "segment_2");

    UUID client_id = generate_uuid();
    UUID invalid_client_id = generate_uuid();

    // Test Case 1: MoveEnd a non-existent key, should fail.
    auto move_end_result = service_->MoveEnd(client_id, "non_existent_key");
    EXPECT_FALSE(move_end_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, move_end_result.error());

    // Put an object with 1 replica and preferred_segment=segment_1 for testing
    std::string key = "test_key";
    uint64_t slice_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = "segment_1";

    auto put_start_result =
        service_->PutStart(client_id, key, slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto put_end_result = service_->PutEnd(client_id, key, ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    // Test Case 2: MoveEnd the object, should fail because there is no ongoing
    // move task.
    move_end_result = service_->MoveEnd(client_id, key);
    EXPECT_FALSE(move_end_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NO_REPLICATION_TASK, move_end_result.error());

    // MoveStart the object to segment_2
    auto move_start_result =
        service_->MoveStart(client_id, key, "segment_1", "segment_2");
    ASSERT_TRUE(move_start_result.has_value());

    // Test Case 3: MoveEnd with an invalid client id, should fail.
    move_end_result = service_->MoveEnd(invalid_client_id, key);
    EXPECT_FALSE(move_end_result.has_value());
    EXPECT_EQ(ErrorCode::ILLEGAL_CLIENT, move_end_result.error());

    // Test Case 4: CopyEnd the object, should fail because the ongoing task is
    // Move.
    auto copy_end_result = service_->CopyEnd(client_id, key);
    EXPECT_FALSE(copy_end_result.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, copy_end_result.error());

    // Test Case 5: MoveEnd, should success.
    move_end_result = service_->MoveEnd(client_id, key);
    EXPECT_TRUE(move_end_result.has_value());

    // Verify we still have 1 replica (the move was successful)
    auto get_result = service_->GetReplicaList(key);
    EXPECT_TRUE(get_result.has_value());
    EXPECT_EQ(1, get_result.value().replicas.size());

    // MoveStart the object from segment_2 to segment_1 again, then unmount
    // segment_2
    move_start_result =
        service_->MoveStart(client_id, key, "segment_2", "segment_1");
    ASSERT_TRUE(move_start_result.has_value());

    // Unmount segment_2 to simulate source gone
    auto unmount_result =
        service_->UnmountSegment(context2.segment_id, context2.client_id);
    ASSERT_TRUE(unmount_result.has_value());

    // Test Case 6: MoveEnd, should fail because the source is gone.
    move_end_result = service_->MoveEnd(client_id, key);
    EXPECT_FALSE(move_end_result.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_GONE, move_end_result.error());
}

TEST_F(MasterServiceTest, MoveRevoke) {
    std::unique_ptr<MasterService> service_(new MasterService());

    // Mount 2 segments (segment_1, segment_2) with PrepareSimpleSegment
    [[maybe_unused]] const auto context1 =
        PrepareSimpleSegment(*service_, "segment_1");
    [[maybe_unused]] const auto context2 =
        PrepareSimpleSegment(*service_, "segment_2");

    UUID client_id = generate_uuid();
    UUID invalid_client_id = generate_uuid();

    // Test Case 1: MoveRevoke a non-existent key, should fail.
    auto move_revoke_result =
        service_->MoveRevoke(client_id, "non_existent_key");
    EXPECT_FALSE(move_revoke_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, move_revoke_result.error());

    // Put an object with 1 replica and preferred_segment=segment_1 for testing
    std::string key = "test_key";
    uint64_t slice_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = "segment_1";

    auto put_start_result =
        service_->PutStart(client_id, key, slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto put_end_result = service_->PutEnd(client_id, key, ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    // Test Case 2: MoveRevoke the object, should fail because there is no
    // ongoing move task.
    move_revoke_result = service_->MoveRevoke(client_id, key);
    EXPECT_FALSE(move_revoke_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NO_REPLICATION_TASK,
              move_revoke_result.error());

    // MoveStart the object from segment_1 to segment_2
    auto move_start_result =
        service_->MoveStart(client_id, key, "segment_1", "segment_2");
    ASSERT_TRUE(move_start_result.has_value());

    // Test Case 3: MoveRevoke with an invalid client id, should fail.
    move_revoke_result = service_->MoveRevoke(invalid_client_id, key);
    EXPECT_FALSE(move_revoke_result.has_value());
    EXPECT_EQ(ErrorCode::ILLEGAL_CLIENT, move_revoke_result.error());

    // Test Case 4: CopyRevoke the object, should fail because the ongoing task
    // is Move.
    auto copy_revoke_result = service_->CopyRevoke(client_id, key);
    EXPECT_FALSE(copy_revoke_result.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, copy_revoke_result.error());

    // Test Case 5: MoveRevoke, should succeed.
    move_revoke_result = service_->MoveRevoke(client_id, key);
    EXPECT_TRUE(move_revoke_result.has_value());

    // Verify we still have 1 replica (the move was revoked)
    auto get_result = service_->GetReplicaList(key);
    EXPECT_TRUE(get_result.has_value());
    auto& replicas = get_result.value().replicas;
    EXPECT_EQ(1, replicas.size());
    EXPECT_EQ("segment_1", replicas[0]
                               .get_memory_descriptor()
                               .buffer_descriptor.transport_endpoint_);

    // MoveStart the object from segment_1 to segment_2 again, then unmount
    // segment_1
    move_start_result =
        service_->MoveStart(client_id, key, "segment_1", "segment_2");
    ASSERT_TRUE(move_start_result.has_value());

    // Unmount segment_1 to simulate source gone
    auto unmount_result =
        service_->UnmountSegment(context1.segment_id, context1.client_id);
    ASSERT_TRUE(unmount_result.has_value());

    // Test Case 6: MoveRevoke, should succeed even though the source is gone.
    move_revoke_result = service_->MoveRevoke(client_id, key);
    EXPECT_TRUE(move_revoke_result.has_value());

    // The object should be erased as there is no replica left.
    get_result = service_->GetReplicaList(key);
    EXPECT_FALSE(get_result.has_value());
}

TEST_F(MasterServiceTest, ProtectCopyMoveSourceFromEviction) {
    const uint64_t kv_lease_ttl = 100;
    const uint64_t client_live_ttl = 600;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .set_client_live_ttl_sec(client_live_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));

    // Mount 2 segments (segment_1, segment_2) with PrepareSimpleSegment, each
    // 16 MB
    constexpr size_t kBaseAddr = 0x100000000;
    constexpr size_t kSegmentSize = 16 * 1024 * 1024;  // 16 MB
    [[maybe_unused]] const auto context1 =
        PrepareSimpleSegment(*service_, "segment_1", kBaseAddr, kSegmentSize);
    [[maybe_unused]] const auto context2 =
        PrepareSimpleSegment(*service_, "segment_2", kBaseAddr, kSegmentSize);

    UUID client_id = generate_uuid();

    const std::string copy_key = "copy_key";
    const std::string move_key = "move_key";
    uint64_t slice_length = 1024 * 1024;
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = "segment_1";

    // Put two objects for move and copy tests.
    auto put_start_result =
        service_->PutStart(client_id, copy_key, slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto put_end_result =
        service_->PutEnd(client_id, copy_key, ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    put_start_result =
        service_->PutStart(client_id, move_key, slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    put_end_result = service_->PutEnd(client_id, move_key, ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    // Start copy and move operations.
    auto copy_start_result =
        service_->CopyStart(client_id, copy_key, "segment_1", {"segment_2"});
    ASSERT_TRUE(copy_start_result.has_value());

    auto move_start_result =
        service_->MoveStart(client_id, move_key, "segment_1", "segment_2");
    ASSERT_TRUE(move_start_result.has_value());

    // Put more objects to trigger eviction. Do not prefer any segments.
    config.preferred_segment = "";
    for (size_t i = 0; i < 128 * (kSegmentSize * 2 / slice_length); ++i) {
        std::string key = "test_key_" + std::to_string(i);
        auto put_start_result =
            service_->PutStart(client_id, key, slice_length, config);
        if (put_start_result.has_value()) {
            auto put_end_result =
                service_->PutEnd(client_id, key, ReplicaType::MEMORY);
            ASSERT_TRUE(put_end_result.has_value());
        } else {
            // wait for eviction to work
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }

    // Wait all objects lease expiring and then remove them.
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl * 2));
    auto remove_all_result = service_->RemoveAll();
    ASSERT_TRUE(remove_all_result > 0);

    // Try end copy and move operations, should success.
    auto copy_end_result = service_->CopyEnd(client_id, copy_key);
    EXPECT_TRUE(copy_end_result.has_value());

    auto move_end_result = service_->MoveEnd(client_id, move_key);
    EXPECT_TRUE(move_end_result.has_value());
}

TEST_F(MasterServiceTest, DiscardTimeoutCopyMove) {
    const uint64_t kv_lease_ttl = 100;
    const uint64_t client_live_ttl = 600;
    const uint64_t put_discard_timeout = 1;
    const uint64_t put_release_timeout = 2;
    auto service_config =
        MasterServiceConfig::builder()
            .set_default_kv_lease_ttl(kv_lease_ttl)
            .set_client_live_ttl_sec(client_live_ttl)
            .set_put_start_discard_timeout_sec(put_discard_timeout)
            .set_put_start_release_timeout_sec(put_release_timeout)
            .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));

    // Mount 2 segments (segment_1, segment_2) with PrepareSimpleSegment, each
    // 16 MB
    constexpr size_t kBaseAddr = 0x100000000;
    constexpr size_t kSegmentSize = 16 * 1024 * 1024;  // 16 MB
    [[maybe_unused]] const auto context1 =
        PrepareSimpleSegment(*service_, "segment_1", kBaseAddr, kSegmentSize);
    [[maybe_unused]] const auto context2 =
        PrepareSimpleSegment(*service_, "segment_2", kBaseAddr, kSegmentSize);

    UUID client_id = generate_uuid();

    const std::string copy_key = "copy_key";
    const std::string move_key = "move_key";
    uint64_t slice_length = 1024 * 1024;
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = "segment_1";

    // Put two objects for move and copy tests.
    auto put_start_result =
        service_->PutStart(client_id, copy_key, slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto put_end_result =
        service_->PutEnd(client_id, copy_key, ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    put_start_result =
        service_->PutStart(client_id, move_key, slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    put_end_result = service_->PutEnd(client_id, move_key, ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    // Start copy and move operations.
    auto copy_start_result =
        service_->CopyStart(client_id, copy_key, "segment_1", {"segment_2"});
    ASSERT_TRUE(copy_start_result.has_value());

    auto move_start_result =
        service_->MoveStart(client_id, move_key, "segment_1", "segment_2");
    ASSERT_TRUE(move_start_result.has_value());

    // Wait for the operations timeout.
    std::this_thread::sleep_for(std::chrono::seconds(put_release_timeout));

    // Put more objects to trigger eviction. Do not prefer any segments.
    config.preferred_segment = "";
    for (size_t i = 0; i < 128 * (kSegmentSize * 2 / slice_length); ++i) {
        std::string key = "test_key_" + std::to_string(i);
        auto put_start_result =
            service_->PutStart(client_id, key, slice_length, config);
        if (put_start_result.has_value()) {
            auto put_end_result =
                service_->PutEnd(client_id, key, ReplicaType::MEMORY);
            ASSERT_TRUE(put_end_result.has_value());
        } else {
            // wait for eviction to work
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }

    // Try end copy and move operations, should fail because the objects are
    // evicted.
    auto copy_end_result = service_->CopyEnd(client_id, copy_key);
    EXPECT_FALSE(copy_end_result.has_value());
    EXPECT_EQ(copy_end_result.error(), ErrorCode::OBJECT_NOT_FOUND);

    auto move_end_result = service_->MoveEnd(client_id, move_key);
    EXPECT_FALSE(move_end_result.has_value());
    EXPECT_EQ(move_end_result.error(), ErrorCode::OBJECT_NOT_FOUND);
}

TEST_F(MasterServiceTest, RemoveByRegexComplex) {
    const uint64_t kv_lease_ttl = 100;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    auto service_ = std::make_unique<MasterService>(service_config);
    const UUID client_id = generate_uuid();

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
            put_object(*service_, client_id, key);
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
    const UUID client_id = generate_uuid();
    int times = 10;
    while (times--) {
        std::string key = "test_key" + std::to_string(times);
        uint64_t value_length = 1024;
        ReplicateConfig config;
        config.replica_num = 1;
        auto put_start_result =
            service_->PutStart(client_id, key, value_length, config);
        ASSERT_TRUE(put_start_result.has_value());
        auto put_end_result =
            service_->PutEnd(client_id, key, ReplicaType::MEMORY);
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

TEST_F(MasterServiceTest, SingleSliceMultiReplicaFlow) {
    const uint64_t kv_lease_ttl = 50;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    const UUID client_id = generate_uuid();

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
    constexpr size_t slice_length = 1024 * 1024 * 5;  // 5MB

    // Configure replication
    ReplicateConfig config;
    config.replica_num = num_replicas;
    std::vector<Replica::Descriptor> replica_list;

    // Test PutStart with multiple slices and replicas
    auto put_start_result =
        service_->PutStart(client_id, key, slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    replica_list = put_start_result.value();

    // Verify replica list properties
    ASSERT_EQ(num_replicas, replica_list.size());
    for (const auto& replica : replica_list) {
        // Verify replica status
        EXPECT_EQ(ReplicaStatus::PROCESSING, replica.status);

        // Verify slice length matches buffer descriptor
        EXPECT_EQ(slice_length,
                  replica.get_memory_descriptor().buffer_descriptor.size_);
    }

    // Test GetReplicaList during processing (should fail)
    auto get_result = service_->GetReplicaList(key);
    EXPECT_FALSE(get_result.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_NOT_READY, get_result.error());

    // Complete the put operation
    auto put_end_result = service_->PutEnd(client_id, key, ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    // Test GetReplicaList after completion
    auto get_result2 = service_->GetReplicaList(key);
    ASSERT_TRUE(get_result2.has_value());
    auto retrieved_replicas = get_result2.value().replicas;
    ASSERT_EQ(num_replicas, retrieved_replicas.size());

    // Verify final state of all replicas
    for (const auto& replica : retrieved_replicas) {
        EXPECT_EQ(ReplicaStatus::COMPLETE, replica.status);
        ASSERT_EQ(slice_length,
                  replica.get_memory_descriptor().buffer_descriptor.size_);
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
    uint64_t slice_length = 1024 * 1024;  // One 1MB slice
    ReplicateConfig config;
    config.replica_num = 1;  // One replica

    // Create the object
    auto put_start_result =
        service_->PutStart(client_id, key, slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto put_end_result = service_->PutEnd(client_id, key, ReplicaType::MEMORY);
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
    auto put_start_result2 =
        service_->PutStart(client_id, key2, slice_length, config);
    ASSERT_TRUE(put_start_result2.has_value());
    auto put_end_result2 =
        service_->PutEnd(client_id, key2, ReplicaType::MEMORY);
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
                uint64_t slice_length = 1024;
                ReplicateConfig config;
                config.replica_num = 1;
                std::vector<Replica::Descriptor> replica_list;

                auto put_start_result =
                    service_->PutStart(client_id, key, slice_length, config);
                if (put_start_result.has_value()) {
                    auto put_end_result =
                        service_->PutEnd(client_id, key, ReplicaType::MEMORY);
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
        uint64_t slice_length = 1024;
        ReplicateConfig config;
        config.replica_num = 1;

        auto put_start_result =
            service_->PutStart(client_id, key, slice_length, config);
        ASSERT_TRUE(put_start_result.has_value());
        auto put_end_result =
            service_->PutEnd(client_id, key, ReplicaType::MEMORY);
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
        uint64_t slice_length = 1024;
        ReplicateConfig config;
        config.replica_num = 1;

        auto put_start_result =
            service_->PutStart(client_id, key, slice_length, config);
        ASSERT_TRUE(put_start_result.has_value());
        auto put_end_result =
            service_->PutEnd(client_id, key, ReplicaType::MEMORY);
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
    std::string key1 =
        GenerateKeyForSegment(client_id, service_, segment1.name);
    std::string key2 =
        GenerateKeyForSegment(client_id, service_, segment2.name);
    uint64_t slice_length = 1024;
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
        service_->PutStart(client_id, key1, slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    replica_list = put_start_result.value();
    auto put_end_result =
        service_->PutEnd(client_id, key1, ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());
    auto get_result3 = service_->GetReplicaList(key1);
    ASSERT_TRUE(get_result3.has_value());
    auto retrieved = get_result3.value();
    ASSERT_EQ(replica_list[0]
                  .get_memory_descriptor()
                  .buffer_descriptor.transport_endpoint_,
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
    uint64_t slice_length = object_size;
    ReplicateConfig config;
    config.replica_num = 2;

    auto put_start_result =
        service_->PutStart(client_id, key, slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    ASSERT_EQ(2u, put_start_result->size());
    ASSERT_TRUE(
        service_->PutEnd(client_id, key, ReplicaType::MEMORY).has_value());

    // Verify two replicas exist and they are on distinct segments
    auto get_result = service_->GetReplicaList(key);
    ASSERT_TRUE(get_result.has_value());
    auto replicas = get_result.value().replicas;
    ASSERT_EQ(2u, replicas.size());
    std::unordered_set<std::string> seg_names;
    for (const auto& rep : replicas) {
        ASSERT_EQ(ReplicaStatus::COMPLETE, rep.status);
        const auto& mem = rep.get_memory_descriptor();
        ASSERT_EQ(slice_length, mem.buffer_descriptor.size_);
        seg_names.insert(mem.buffer_descriptor.transport_endpoint_);
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
        std::string key =
            GenerateKeyForSegment(client_id, service_, segment_name);
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
    const UUID client_id = generate_uuid();

    std::string key = "test_key";
    uint64_t slice_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;

    // Verify lease is granted on ExistsKey
    auto put_start_result =
        service_->PutStart(client_id, key, slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto put_end_result = service_->PutEnd(client_id, key, ReplicaType::MEMORY);
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
        service_->PutStart(client_id, key, slice_length, config);
    ASSERT_TRUE(put_start_result2.has_value());
    auto put_end_result2 =
        service_->PutEnd(client_id, key, ReplicaType::MEMORY);
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
        service_->PutStart(client_id, key, slice_length, config);
    ASSERT_TRUE(put_start_result3.has_value());
    auto put_end_result3 =
        service_->PutEnd(client_id, key, ReplicaType::MEMORY);
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
        service_->PutStart(client_id, key, slice_length, config);
    ASSERT_TRUE(put_start_result4.has_value());
    auto put_end_result4 =
        service_->PutEnd(client_id, key, ReplicaType::MEMORY);
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
    const UUID client_id = generate_uuid();
    for (int i = 0; i < 10; ++i) {
        std::string key = "test_key" + std::to_string(i);
        uint64_t slice_length = 1024;
        ReplicateConfig config;
        config.replica_num = 1;
        auto put_start_result =
            service_->PutStart(client_id, key, slice_length, config);
        ASSERT_TRUE(put_start_result.has_value());
        auto put_end_result =
            service_->PutEnd(client_id, key, ReplicaType::MEMORY);
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
    const UUID client_id = generate_uuid();
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
        uint64_t slice_length = object_size;
        ReplicateConfig config;
        config.replica_num = 1;
        auto put_start_result =
            service_->PutStart(client_id, key, slice_length, config);
        if (put_start_result.has_value()) {
            auto put_end_result =
                service_->PutEnd(client_id, key, ReplicaType::MEMORY);
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
    const UUID client_id = generate_uuid();
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
        uint64_t slice_length = object_size;
        ReplicateConfig config;
        config.replica_num = 1;
        auto put_start_result =
            service_->PutStart(client_id, key, slice_length, config);
        if (put_start_result.has_value()) {
            auto put_end_result =
                service_->PutEnd(client_id, key, ReplicaType::MEMORY);
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
    const UUID client_id = generate_uuid();
    // Mount segment and put an object
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 16;
    [[maybe_unused]] const auto context =
        PrepareSimpleSegment(*service_, "test_segment", buffer, size);

    std::string key = "test_key";
    uint64_t slice_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;
    config.with_soft_pin = true;

    // Verify soft pin does not block remove
    ASSERT_TRUE(
        service_->PutStart(client_id, key, slice_length, config).has_value());
    ASSERT_TRUE(
        service_->PutEnd(client_id, key, ReplicaType::MEMORY).has_value());
    EXPECT_TRUE(service_->Remove(key).has_value());

    // Verify soft pin does not block RemoveAll
    ASSERT_TRUE(
        service_->PutStart(client_id, key, slice_length, config).has_value());
    ASSERT_TRUE(
        service_->PutEnd(client_id, key, ReplicaType::MEMORY).has_value());
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
    const UUID client_id = generate_uuid();

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
            uint64_t slice_length = value_size;
            ReplicateConfig soft_pin_config;
            soft_pin_config.replica_num = 1;
            soft_pin_config.with_soft_pin = true;

            ASSERT_TRUE(service_
                            ->PutStart(client_id, pin_key, slice_length,
                                       soft_pin_config)
                            .has_value());
            ASSERT_TRUE(
                service_->PutEnd(client_id, pin_key, ReplicaType::MEMORY)
                    .has_value());
        }

        // Fill the segment to trigger eviction
        int failed_puts = 0;
        for (int i = 0; i < 20; i++) {
            std::string key = "key" + std::to_string(i);
            uint64_t slice_length = value_size;
            ReplicateConfig config;
            config.replica_num = 1;
            if (service_->PutStart(client_id, key, slice_length, config)
                    .has_value()) {
                ASSERT_TRUE(
                    service_->PutEnd(client_id, key, ReplicaType::MEMORY)
                        .has_value());
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
    const UUID client_id = generate_uuid();

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
        uint64_t slice_length = value_size;
        ReplicateConfig config;
        config.replica_num = 1;
        config.with_soft_pin = true;
        if (service_->PutStart(client_id, key, slice_length, config)
                .has_value()) {
            ASSERT_TRUE(service_->PutEnd(client_id, key, ReplicaType::MEMORY)
                            .has_value());
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
    const UUID client_id = generate_uuid();

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
            uint64_t slice_length = value_size;
            ReplicateConfig soft_pin_config;
            soft_pin_config.replica_num = 1;
            soft_pin_config.with_soft_pin = true;

            ASSERT_TRUE(service_->PutStart(client_id, pin_key, slice_length,
                                           soft_pin_config));
            ASSERT_TRUE(
                service_->PutEnd(client_id, pin_key, ReplicaType::MEMORY)
                    .has_value());
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
            uint64_t slice_length = value_size;
            ReplicateConfig config;
            config.replica_num = 1;
            if (service_->PutStart(client_id, key, slice_length, config)
                    .has_value()) {
                ASSERT_TRUE(
                    service_->PutEnd(client_id, key, ReplicaType::MEMORY)
                        .has_value());
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
    const UUID client_id = generate_uuid();

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
        uint64_t slice_length = value_size;
        ReplicateConfig config;
        config.replica_num = 1;
        config.with_soft_pin = true;
        if (service_->PutStart(client_id, key, slice_length, config)
                .has_value()) {
            ASSERT_TRUE(service_->PutEnd(client_id, key, ReplicaType::MEMORY)
                            .has_value());
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

TEST_F(MasterServiceTest, ReplicaSegmentsAreUnique) {
    std::unique_ptr<MasterService> service_(new MasterService());
    const UUID client_id = generate_uuid();

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
    uint64_t slice_length = 1024 * 1024 - 16;
    ReplicateConfig config;
    config.replica_num = 10;

    auto put_start_result =
        service_->PutStart(client_id, key, slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto replica_list_local = put_start_result.value();
    ASSERT_EQ(config.replica_num, replica_list_local.size());

    // Segment names across replicas must be unique
    std::unordered_set<std::string> segment_names;
    for (const auto& replica : replica_list_local) {
        ASSERT_TRUE(replica.is_memory_replica());
        const auto& mem = replica.get_memory_descriptor();
        ASSERT_EQ(slice_length, mem.buffer_descriptor.size_);
        segment_names.insert(mem.buffer_descriptor.transport_endpoint_);
    }
    EXPECT_EQ(segment_names.size(), config.replica_num)
        << "Duplicate segment found";

    ASSERT_TRUE(
        service_->PutEnd(client_id, key, ReplicaType::MEMORY).has_value());
}

TEST_F(MasterServiceTest, ReplicationFactorTwoWithSingleSegment) {
    std::unique_ptr<MasterService> service_(new MasterService());
    const UUID client_id = generate_uuid();

    // Mount a single 16MB segment
    constexpr size_t kBaseAddr = 0x300000000;
    constexpr size_t kSegmentSize = 1024 * 1024 * 16;  // 16MB
    [[maybe_unused]] const auto context = PrepareSimpleSegment(
        *service_, "single_segment", kBaseAddr, kSegmentSize);

    // Request replication factor 2 with a single 1KB slice
    // With best-effort semantics, should succeed with 1 replica
    const std::string key = "replication_factor_two_single_segment";
    uint64_t slice_length = 1024;
    ReplicateConfig config;
    config.replica_num = 2;

    auto put_start_result =
        service_->PutStart(client_id, key, slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto replicas = put_start_result.value();

    // Should get 1 replica instead of the requested 2 (best-effort)
    EXPECT_EQ(1u, replicas.size());
    EXPECT_TRUE(replicas[0].is_memory_replica());

    // Verify the replica is properly allocated on the single segment
    auto mem_desc = replicas[0].get_memory_descriptor();
    EXPECT_EQ("single_segment", mem_desc.buffer_descriptor.transport_endpoint_);
    EXPECT_EQ(1024u, mem_desc.buffer_descriptor.size_);
}

TEST_F(MasterServiceTest, BatchExistKeyTest) {
    std::unique_ptr<MasterService> service_(new MasterService());
    const UUID client_id = generate_uuid();

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
        uint64_t slice_length = value_size;
        auto put_start_result =
            service_->PutStart(client_id, test_keys[i], slice_length, config);
        ASSERT_TRUE(put_start_result.has_value());
        auto put_end_result =
            service_->PutEnd(client_id, test_keys[i], ReplicaType::MEMORY);
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

TEST_F(MasterServiceTest, BatchQueryIpTest) {
    std::unique_ptr<MasterService> service_(new MasterService());
    const UUID client_id = generate_uuid();

    // Mount a segment with a specific te_endpoint (IP:Port format)
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 16;
    Segment segment = MakeSegment("test_segment", buffer, size);
    segment.te_endpoint = "127.0.0.1:12345";  // Set IP:Port format for testing
    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    // Test BatchQueryIp with a single client_id
    std::vector<UUID> client_ids = {client_id};
    auto query_result = service_->BatchQueryIp(client_ids);

    ASSERT_TRUE(query_result.has_value())
        << "BatchQueryIp failed: " << toString(query_result.error());

    const auto& results = query_result.value();
    ASSERT_FALSE(results.empty()) << "BatchQueryIp returned empty results";

    auto it = results.find(client_id);
    ASSERT_NE(it, results.end()) << "Client ID not found in results";

    const auto& ip_addresses = it->second;
    ASSERT_FALSE(ip_addresses.empty()) << "No IP addresses found for client";
    ASSERT_EQ(1u, ip_addresses.size()) << "Expected exactly 1 IP address";
    EXPECT_EQ("127.0.0.1", ip_addresses[0]) << "IP address mismatch";

    // Test BatchQueryIp with multiple client_ids (one valid, one invalid)
    UUID non_existent_client_id = generate_uuid();
    std::vector<UUID> mixed_client_ids = {client_id, non_existent_client_id};
    auto mixed_query_result = service_->BatchQueryIp(mixed_client_ids);

    ASSERT_TRUE(mixed_query_result.has_value());
    const auto& mixed_results = mixed_query_result.value();

    // Valid client_id should be in results
    ASSERT_NE(mixed_results.find(client_id), mixed_results.end())
        << "Valid client_id should be in results";

    // Invalid client_id should not be in results (silently skipped)
    EXPECT_EQ(mixed_results.find(non_existent_client_id), mixed_results.end())
        << "Invalid client_id should not be in results";
}

TEST_F(MasterServiceTest, BatchQueryIpMultipleSegmentsTest) {
    std::unique_ptr<MasterService> service_(new MasterService());
    const UUID client_id = generate_uuid();

    // Mount multiple segments with different IPs for the same client
    constexpr size_t buffer1 = 0x300000000;
    constexpr size_t buffer2 = 0x400000000;
    constexpr size_t size = 1024 * 1024 * 16;

    Segment segment1 = MakeSegment("segment1", buffer1, size);
    segment1.te_endpoint = "127.0.0.1:12345";
    auto mount_result1 = service_->MountSegment(segment1, client_id);
    ASSERT_TRUE(mount_result1.has_value());

    Segment segment2 = MakeSegment("segment2", buffer2, size);
    segment2.te_endpoint = "127.0.0.1:12346";  // Same IP, different port
    auto mount_result2 = service_->MountSegment(segment2, client_id);
    ASSERT_TRUE(mount_result2.has_value());

    Segment segment3 = MakeSegment("segment3", 0x500000000, size);
    segment3.te_endpoint = "192.168.1.1:12345";  // Different IP
    auto mount_result3 = service_->MountSegment(segment3, client_id);
    ASSERT_TRUE(mount_result3.has_value());

    // Test BatchQueryIp - should return unique IPs
    std::vector<UUID> client_ids = {client_id};
    auto query_result = service_->BatchQueryIp(client_ids);

    ASSERT_TRUE(query_result.has_value());
    const auto& results = query_result.value();
    auto it = results.find(client_id);
    ASSERT_NE(it, results.end());

    const auto& ip_addresses = it->second;
    // Should have 2 unique IPs: 127.0.0.1 and 192.168.1.1
    ASSERT_EQ(2u, ip_addresses.size()) << "Expected 2 unique IP addresses";

    // Verify both IPs are present
    std::unordered_set<std::string> ip_set(ip_addresses.begin(),
                                           ip_addresses.end());
    EXPECT_NE(ip_set.find("127.0.0.1"), ip_set.end());
    EXPECT_NE(ip_set.find("192.168.1.1"), ip_set.end());
}

TEST_F(MasterServiceTest, BatchQueryIpEmptyClientIdTest) {
    std::unique_ptr<MasterService> service_(new MasterService());

    // Test with empty client_ids list
    std::vector<UUID> empty_client_ids;
    auto query_result = service_->BatchQueryIp(empty_client_ids);

    ASSERT_TRUE(query_result.has_value());
    const auto& results = query_result.value();
    EXPECT_TRUE(results.empty())
        << "Empty client_ids should return empty results";
}

TEST_F(MasterServiceTest, BatchQueryIpMultipleSegmentsEmptyTeEndpointTest) {
    std::unique_ptr<MasterService> service_(new MasterService());
    const UUID client_id = generate_uuid();

    // Mount multiple segments, all with empty te_endpoint
    constexpr size_t buffer1 = 0x300000000;
    constexpr size_t buffer2 = 0x400000000;
    constexpr size_t size = 1024 * 1024 * 16;

    Segment segment1 = MakeSegment("segment1", buffer1, size);
    segment1.te_endpoint = "";  // Empty te_endpoint
    auto mount_result1 = service_->MountSegment(segment1, client_id);
    ASSERT_TRUE(mount_result1.has_value());

    Segment segment2 = MakeSegment("segment2", buffer2, size);
    segment2.te_endpoint = "";  // Empty te_endpoint
    auto mount_result2 = service_->MountSegment(segment2, client_id);
    ASSERT_TRUE(mount_result2.has_value());

    // Test BatchQueryIp - should return client with empty IP vector
    std::vector<UUID> client_ids = {client_id};
    auto query_result = service_->BatchQueryIp(client_ids);

    ASSERT_TRUE(query_result.has_value());
    const auto& results = query_result.value();
    ASSERT_FALSE(results.empty())
        << "BatchQueryIp should include client in results even with empty IPs";

    auto it = results.find(client_id);
    ASSERT_NE(it, results.end()) << "Client ID should be found in results even "
                                    "with all empty te_endpoints";

    // Verify the IP vector is empty
    const auto& ip_addresses = it->second;
    EXPECT_TRUE(ip_addresses.empty())
        << "Client with all empty te_endpoints should have empty IP vector";
}

TEST_F(MasterServiceTest, PutStartExpiringTest) {
    // Reset storage space metrics.
    MasterMetricManager::instance().reset_allocated_mem_size();
    MasterMetricManager::instance().reset_total_mem_capacity();

    MasterServiceConfig master_config;
    master_config.put_start_discard_timeout_sec = 3;
    master_config.put_start_release_timeout_sec = 5;
    std::unique_ptr<MasterService> service_(new MasterService(master_config));

    constexpr size_t kReplicaCnt = 3;
    constexpr size_t kBaseAddr = 0x300000000;
    constexpr size_t kSegmentSize = 1024 * 1024 * 16;  // 16MB

    // Mount 3 segments.
    std::vector<MountedSegmentContext> contexts;
    contexts.reserve(kReplicaCnt);
    for (size_t i = 0; i < kReplicaCnt; ++i) {
        auto context = PrepareSimpleSegment(
            *service_, "segment_" + std::to_string(i),
            kBaseAddr + static_cast<size_t>(i) * kSegmentSize, kSegmentSize);
        contexts.push_back(context);
    }

    // The client_id used to put objects.
    auto client_id = generate_uuid();
    std::string key_1 = "test_key_1", key_2 = "test_key_2";
    uint64_t value_length = 6 * 1024 * 1024;  // 6MB
    uint64_t slice_length = value_length;
    ReplicateConfig config;
    config.replica_num = kReplicaCnt;

    // Put key_1, should success.
    auto put_start_result =
        service_->PutStart(client_id, key_1, slice_length, config);
    EXPECT_TRUE(put_start_result.has_value());
    replica_list = put_start_result.value();
    EXPECT_EQ(replica_list.size(), kReplicaCnt);
    for (size_t i = 0; i < kReplicaCnt; i++) {
        EXPECT_EQ(ReplicaStatus::PROCESSING, replica_list[i].status);
    }

    // Put key_1 again, should fail because the key exists.
    put_start_result =
        service_->PutStart(client_id, key_1, slice_length, config);
    EXPECT_FALSE(put_start_result.has_value());
    EXPECT_EQ(put_start_result.error(), ErrorCode::OBJECT_ALREADY_EXISTS);

    // Wait for a while until the put-start expired.
    for (size_t i = 0; i <= master_config.put_start_discard_timeout_sec; i++) {
        for (auto& context : contexts) {
            auto result = service_->Ping(context.client_id);
            EXPECT_TRUE(result.has_value());
        }
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // Put key_1 again, should success because the old one has expired and will
    // be discarded by this put.
    put_start_result =
        service_->PutStart(client_id, key_1, slice_length, config);
    EXPECT_TRUE(put_start_result.has_value());
    replica_list = put_start_result.value();
    EXPECT_EQ(replica_list.size(), kReplicaCnt);
    for (size_t i = 0; i < kReplicaCnt; i++) {
        EXPECT_EQ(ReplicaStatus::PROCESSING, replica_list[i].status);
    }

    // Complete key_1.
    auto put_end_result =
        service_->PutEnd(client_id, key_1, ReplicaType::MEMORY);
    EXPECT_TRUE(put_end_result.has_value());

    // Protect key_1 from eviction.
    auto get_result = service_->GetReplicaList(key_1);
    EXPECT_TRUE(get_result.has_value());

    // Put key_2, should fail because the key_1 occupied 12MB (6MB processing,
    // 6MB discarded but not yet released) on each segment.
    put_start_result =
        service_->PutStart(client_id, key_2, slice_length, config);
    EXPECT_FALSE(put_start_result.has_value());
    EXPECT_EQ(put_start_result.error(), ErrorCode::NO_AVAILABLE_HANDLE);

    // Wait for a while until the discarded replicas are released.
    for (size_t i = 0; i <= master_config.put_start_release_timeout_sec -
                                master_config.put_start_discard_timeout_sec;
         i++) {
        for (auto& context : contexts) {
            auto result = service_->Ping(context.client_id);
            EXPECT_TRUE(result.has_value());
        }
        // Protect key_1 from eviction.
        auto get_result = service_->GetReplicaList(key_1);
        EXPECT_TRUE(get_result.has_value());
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // Put key_2 again, should success because the discarded replica has been
    // released.
    put_start_result =
        service_->PutStart(client_id, key_2, slice_length, config);
    EXPECT_TRUE(put_start_result.has_value());
    replica_list = put_start_result.value();
    EXPECT_EQ(replica_list.size(), kReplicaCnt);
    for (size_t i = 0; i < kReplicaCnt; i++) {
        EXPECT_EQ(ReplicaStatus::PROCESSING, replica_list[i].status);
    }

    // Wait for a while until key_2 can be discarded and released.
    for (size_t i = 0; i <= master_config.put_start_release_timeout_sec; i++) {
        for (auto& context : contexts) {
            auto result = service_->Ping(context.client_id);
            EXPECT_TRUE(result.has_value());
        }
        // Protect key_1 from eviction.
        auto get_result = service_->GetReplicaList(key_1);
        EXPECT_TRUE(get_result.has_value());
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // Put key_2 again, should fail because eviction has not been triggered. And
    // this PutStart should trigger the eviction.
    put_start_result =
        service_->PutStart(client_id, key_2, slice_length, config);
    EXPECT_FALSE(put_start_result.has_value());
    EXPECT_EQ(put_start_result.error(), ErrorCode::NO_AVAILABLE_HANDLE);

    // Wait a moment for the eviction to complete.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Put key_2 again, should success because the previous one has been
    // discarded and released.
    put_start_result =
        service_->PutStart(client_id, key_2, slice_length, config);
    EXPECT_TRUE(put_start_result.has_value());
    replica_list = put_start_result.value();
    EXPECT_EQ(replica_list.size(), kReplicaCnt);
    for (size_t i = 0; i < kReplicaCnt; i++) {
        EXPECT_EQ(ReplicaStatus::PROCESSING, replica_list[i].status);
    }

    // Complete key_2.
    put_end_result = service_->PutEnd(client_id, key_2, ReplicaType::MEMORY);
    EXPECT_TRUE(put_end_result.has_value());
}

TEST_F(MasterServiceTest, ConcurrentMountLocalDiskSegment) {
    MasterServiceConfig config;
    config.enable_offload = true;
    std::unique_ptr<MasterService> service_(new MasterService(config));

    constexpr size_t num_threads = 100;
    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};

    // Launch multiple threads to mount local disk segments concurrently
    for (size_t i = 0; i < num_threads; i++) {
        threads.emplace_back([&service_, i, &success_count, this]() {
            UUID client_id = generate_uuid();
            auto mount_result =
                service_->MountLocalDiskSegment(client_id, true);
            ASSERT_TRUE(mount_result.has_value());
            ++success_count;
        });
    }

    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }

    // Verify that some mount/unmount operations succeeded
    EXPECT_GT(success_count, 0);
}

TEST_F(MasterServiceTest, OffloadObjectHeartbeat) {
    constexpr size_t key_cnt = 3000;
    MasterServiceConfig config;
    config.enable_offload = true;
    std::unique_ptr<MasterService> service_(new MasterService(config));
    UUID client_id = generate_uuid();
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 16;
    auto segment = MakeSegment("segment", buffer, size);
    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());
    auto mount_local_disk_result =
        service_->MountLocalDiskSegment(client_id, false);
    ASSERT_TRUE(mount_local_disk_result.has_value());
    for (size_t i = 0; i < key_cnt; i++) {
        auto key = GenerateKeyForSegment(client_id, service_, segment.name);
    }
    auto res = service_->OffloadObjectHeartbeat(client_id, true);
    if (!res) {
        LOG(ERROR) << "OffloadObjectHeartbeat failed with error: "
                   << res.error();
        ASSERT_TRUE(res);
    }
    ASSERT_EQ(res->size(), 0);
    std::vector<std::string> keys;
    for (size_t i = 0; i < key_cnt; i++) {
        auto key = GenerateKeyForSegment(client_id, service_, segment.name);
        keys.push_back(key);
    }
    res = service_->OffloadObjectHeartbeat(client_id, true);
    if (!res) {
        LOG(ERROR) << "OffloadObjectHeartbeat failed with error: "
                   << res.error();
        ASSERT_TRUE(res);
    }
    ASSERT_EQ(res->size(), keys.size());
    for (auto& key : keys) {
        ASSERT_TRUE(res.value().find(key) != res.value().end());
        ASSERT_EQ(res.value().find(key)->second, 1024);
    }

    keys.clear();
    for (size_t i = 0; i < key_cnt; i++) {
        auto key = GenerateKeyForSegment(client_id, service_, segment.name);
        keys.push_back(key);
    }
    res = service_->OffloadObjectHeartbeat(client_id, true);
    if (!res) {
        LOG(ERROR) << "OffloadObjectHeartbeat failed with error: "
                   << res.error();
        ASSERT_TRUE(res);
    }
    ASSERT_EQ(res->size(), keys.size());
    for (auto& key : keys) {
        ASSERT_TRUE(res.value().find(key) != res.value().end());
        ASSERT_EQ(res.value().find(key)->second, 1024);
    }
}

TEST_F(MasterServiceTest, BatchReplicaClearAllSegments) {
    const uint64_t kv_lease_ttl = 50;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    // Create multiple objects
    std::vector<std::string> keys;
    const int num_objects = 5;
    for (int i = 0; i < num_objects; ++i) {
        std::string key = "batch_clear_key_" + std::to_string(i);
        keys.push_back(key);
        uint64_t value_length = 1024;
        ReplicateConfig config;
        config.replica_num = 1;
        auto put_start_result =
            service_->PutStart(client_id, key, value_length, config);
        ASSERT_TRUE(put_start_result.has_value());
        auto put_end_result =
            service_->PutEnd(client_id, key, ReplicaType::MEMORY);
        ASSERT_TRUE(put_end_result.has_value());
    }

    // Verify objects exist
    for (const auto& key : keys) {
        auto exist_result = service_->ExistKey(key);
        ASSERT_TRUE(exist_result.has_value());
        ASSERT_TRUE(exist_result.value());
    }

    // Wait for lease to expire
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl + 10));

    // Clear all replicas (empty segment_name means clear all segments)
    auto clear_result = service_->BatchReplicaClear(keys, client_id, "");
    ASSERT_TRUE(clear_result.has_value());

    const auto& cleared_keys = clear_result.value();
    ASSERT_EQ(num_objects, cleared_keys.size()) << "All keys should be cleared";

    // Verify objects are removed
    for (const auto& key : keys) {
        auto exist_result = service_->ExistKey(key);
        ASSERT_TRUE(exist_result.has_value());
        ASSERT_FALSE(exist_result.value())
            << "Key " << key << " should be removed";
    }
}

TEST_F(MasterServiceTest, BatchReplicaClearSpecificSegment) {
    // 1. Setup: Control the lease time
    const uint64_t kv_lease_ttl = 200;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    const UUID client_id = generate_uuid();

    // 2. Setup: Mount segments
    Segment segment1 = MakeSegment("segment1", 0x300000000, 1024 * 1024 * 16);
    Segment segment2 = MakeSegment("segment2", 0x400000000, 1024 * 1024 * 16);
    ASSERT_TRUE(service_->MountSegment(segment1, client_id).has_value());
    ASSERT_TRUE(service_->MountSegment(segment2, client_id).has_value());

    // 3. Setup: Create the object on segment1 using preferred_segment
    std::string key = "segment_specific_key";
    std::string segment_name = segment1.name;
    uint64_t value_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment =
        segment_name;  // Ensure object is placed on segment1
    auto put_start_result =
        service_->PutStart(client_id, key, value_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto put_end_result = service_->PutEnd(client_id, key, ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    // 4. Wait for lease to expire and verify it's actually expired
    // PutEnd calls GrantLease(0, ...) which sets lease_timeout to now.
    // Due to clock precision and timing, we need to ensure the lease is
    // actually expired before calling BatchReplicaClear.
    // Use a small delay and then poll to ensure lease is expired.
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    // Poll until lease is expired (with timeout to avoid infinite loop)
    const auto timeout = std::chrono::seconds(5);
    const auto start_time = std::chrono::steady_clock::now();
    bool lease_expired = false;
    std::vector<std::string> keys = {key};
    tl::expected<std::vector<std::string>, ErrorCode> clear_result;

    while (std::chrono::steady_clock::now() - start_time < timeout) {
        // Try to clear - if it succeeds, lease is expired
        clear_result =
            service_->BatchReplicaClear(keys, client_id, segment_name);
        ASSERT_TRUE(clear_result.has_value());

        if (clear_result.value().size() == 1) {
            lease_expired = true;
            break;
        }

        // Lease not expired yet, wait a bit and retry
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    ASSERT_TRUE(lease_expired) << "Lease did not expire within timeout period";

    // 5. Verify the key was cleared
    const auto& cleared_keys = clear_result.value();
    ASSERT_EQ(1u, cleared_keys.size()) << "Key should be cleared";

    auto exist_result = service_->ExistKey(key);
    ASSERT_TRUE(exist_result.has_value());
    ASSERT_FALSE(exist_result.value())
        << "Key should be removed after being cleared.";
}

TEST_F(MasterServiceTest, BatchReplicaClearWithLeaseActive) {
    const uint64_t kv_lease_ttl = 2000;  // Long lease
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    // Create an object
    std::string key = "lease_active_key";
    uint64_t value_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;
    auto put_start_result =
        service_->PutStart(client_id, key, value_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto put_end_result = service_->PutEnd(client_id, key, ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    // Grant a lease by calling GetReplicaList (similar to normal usage)
    auto get_result = service_->GetReplicaList(key);
    ASSERT_TRUE(get_result.has_value());

    // Try to clear immediately (lease should still be active)
    std::vector<std::string> keys = {key};
    auto clear_result = service_->BatchReplicaClear(keys, client_id, "");
    ASSERT_TRUE(clear_result.has_value());

    // Should return empty list because lease is still active
    const auto& cleared_keys = clear_result.value();
    EXPECT_TRUE(cleared_keys.empty())
        << "No keys should be cleared when lease is active";

    // Verify object still exists
    auto exist_result = service_->ExistKey(key);
    ASSERT_TRUE(exist_result.has_value());
    ASSERT_TRUE(exist_result.value()) << "Key should still exist";
}

TEST_F(MasterServiceTest, BatchReplicaClearWithDifferentClientId) {
    const uint64_t kv_lease_ttl = 50;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id1 = generate_uuid();
    const UUID client_id2 = generate_uuid();

    // Create an object with client_id1
    std::string key = "client_specific_key";
    uint64_t value_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;
    auto put_start_result =
        service_->PutStart(client_id1, key, value_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto put_end_result =
        service_->PutEnd(client_id1, key, ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    // Wait for lease to expire
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl + 10));

    // Try to clear with different client_id
    std::vector<std::string> keys = {key};
    auto clear_result = service_->BatchReplicaClear(keys, client_id2, "");
    ASSERT_TRUE(clear_result.has_value());

    // Should return empty list because client_id doesn't match
    const auto& cleared_keys = clear_result.value();
    EXPECT_TRUE(cleared_keys.empty())
        << "No keys should be cleared for different client_id";

    // Verify object still exists
    auto exist_result = service_->ExistKey(key);
    ASSERT_TRUE(exist_result.has_value());
    ASSERT_TRUE(exist_result.value()) << "Key should still exist";
}

TEST_F(MasterServiceTest, BatchReplicaClearWithNonExistentKeys) {
    const uint64_t kv_lease_ttl = 50;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    // Try to clear non-existent keys
    std::vector<std::string> keys = {"non_existent_key1", "non_existent_key2"};
    auto clear_result = service_->BatchReplicaClear(keys, client_id, "");
    ASSERT_TRUE(clear_result.has_value());

    // Should return empty list
    const auto& cleared_keys = clear_result.value();
    EXPECT_TRUE(cleared_keys.empty())
        << "No keys should be cleared for non-existent keys";
}

TEST_F(MasterServiceTest, BatchReplicaClearWithEmptyKeys) {
    std::unique_ptr<MasterService> service_(new MasterService());
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    // Try to clear empty keys list
    std::vector<std::string> empty_keys;
    auto clear_result = service_->BatchReplicaClear(empty_keys, client_id, "");
    ASSERT_TRUE(clear_result.has_value());

    // Should return empty list
    const auto& cleared_keys = clear_result.value();
    EXPECT_TRUE(cleared_keys.empty())
        << "Empty keys list should return empty result";
}

TEST_F(MasterServiceTest, BatchReplicaClearWithEmptyStringKeys) {
    const uint64_t kv_lease_ttl = 50;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    // Create a valid object
    std::string valid_key = "valid_key";
    uint64_t value_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;
    auto put_start_result =
        service_->PutStart(client_id, valid_key, value_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto put_end_result =
        service_->PutEnd(client_id, valid_key, ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    // Wait for lease to expire
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl + 10));

    // Try to clear with empty string keys mixed with valid keys
    std::vector<std::string> keys = {"", valid_key, "", "another_empty"};
    auto clear_result = service_->BatchReplicaClear(keys, client_id, "");
    ASSERT_TRUE(clear_result.has_value());

    // Should only clear the valid key, skip empty strings
    const auto& cleared_keys = clear_result.value();
    ASSERT_EQ(1u, cleared_keys.size()) << "Only valid key should be cleared";
    EXPECT_EQ(valid_key, cleared_keys[0]);
}

TEST_F(MasterServiceTest, BatchReplicaClearMixedScenario) {
    const uint64_t kv_lease_ttl = 50;
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id1 = generate_uuid();
    const UUID client_id2 = generate_uuid();

    // Create objects with different client_ids
    std::string key1 = "mixed_key1";  // client_id1
    std::string key2 = "mixed_key2";  // client_id1
    std::string key3 = "mixed_key3";  // client_id2

    uint64_t value_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;

    // Create key1 and key2 with client_id1
    auto put_start1 =
        service_->PutStart(client_id1, key1, value_length, config);
    ASSERT_TRUE(put_start1.has_value());
    auto put_end1 = service_->PutEnd(client_id1, key1, ReplicaType::MEMORY);
    ASSERT_TRUE(put_end1.has_value());

    auto put_start2 =
        service_->PutStart(client_id1, key2, value_length, config);
    ASSERT_TRUE(put_start2.has_value());
    auto put_end2 = service_->PutEnd(client_id1, key2, ReplicaType::MEMORY);
    ASSERT_TRUE(put_end2.has_value());

    // Create key3 with client_id2
    auto put_start3 =
        service_->PutStart(client_id2, key3, value_length, config);
    ASSERT_TRUE(put_start3.has_value());
    auto put_end3 = service_->PutEnd(client_id2, key3, ReplicaType::MEMORY);
    ASSERT_TRUE(put_end3.has_value());

    // Wait for lease to expire
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl + 10));

    // Try to clear with mixed keys (some belong to client_id1, some to
    // client_id2)
    std::vector<std::string> keys = {key1, key2, key3, "non_existent", ""};
    auto clear_result = service_->BatchReplicaClear(keys, client_id1, "");
    ASSERT_TRUE(clear_result.has_value());

    // Should only clear key1 and key2 (belonging to client_id1)
    const auto& cleared_keys = clear_result.value();
    ASSERT_EQ(2u, cleared_keys.size())
        << "Only keys belonging to client_id1 should be cleared";

    // Verify key1 and key2 are cleared
    auto exist1 = service_->ExistKey(key1);
    ASSERT_TRUE(exist1.has_value());
    ASSERT_FALSE(exist1.value()) << "key1 should be cleared";

    auto exist2 = service_->ExistKey(key2);
    ASSERT_TRUE(exist2.has_value());
    ASSERT_FALSE(exist2.value()) << "key2 should be cleared";

    // Verify key3 still exists (different client_id)
    auto exist3 = service_->ExistKey(key3);
    ASSERT_TRUE(exist3.has_value());
    ASSERT_TRUE(exist3.value())
        << "key3 should still exist (different client_id)";
}

TEST_F(MasterServiceTest, CreateCopyTaskTest) {
    // Reset storage space metrics.
    MasterMetricManager::instance().reset_allocated_mem_size();
    MasterMetricManager::instance().reset_total_mem_capacity();

    // Create MasterService
    std::unique_ptr<MasterService> service_(new MasterService());

    // Mount 3 segments.
    constexpr size_t kReplicaCnt = 3;
    constexpr size_t kBaseAddr = 0x300000000;
    constexpr size_t kSegmentSize = 1024 * 1024 * 16;  // 16MB
    std::vector<MountedSegmentContext> contexts;
    contexts.reserve(kReplicaCnt);
    for (size_t i = 0; i < kReplicaCnt; ++i) {
        const auto context = PrepareSimpleSegment(
            *service_, "segment_" + std::to_string(i),
            kBaseAddr + static_cast<size_t>(i) * kSegmentSize, kSegmentSize);
        contexts.push_back(context);
    }

    // The client putting the object to segment_0
    auto client_id = generate_uuid();
    std::string key1 = "test_key_1";
    uint64_t value_length = 6 * 1024 * 1024;  // 6MB
    uint64_t slice_length = value_length;
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = "segment_0";
    auto put_start_result =
        service_->PutStart(client_id, key1, slice_length, config);
    EXPECT_TRUE(put_start_result.has_value());
    auto put_end_result =
        service_->PutEnd(client_id, key1, ReplicaType::MEMORY);
    EXPECT_TRUE(put_end_result.has_value());

    // Copy key1 to "segment_1" and "segment_2"
    auto copy_result =
        service_->CreateCopyTask(key1, {"segment_1", "segment_2"});
    EXPECT_TRUE(copy_result.has_value());

    // verify the copy task is created and assigned to the client who executed
    // the copy
    auto task = service_->QueryTask(copy_result.value());
    EXPECT_TRUE(task.has_value());
    EXPECT_EQ(TaskType::REPLICA_COPY, task.value().type);
    EXPECT_EQ(contexts[0].client_id, task.value().assigned_client);

    // Copy with empty targets should fail
    auto copy_result1 = service_->CreateCopyTask(key1, {});
    EXPECT_FALSE(copy_result1.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, copy_result1.error());

    // Copy not exist key should fail
    auto copy_result2 =
        service_->CreateCopyTask("not_exist_key", {"segment_1"});
    EXPECT_FALSE(copy_result2.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, copy_result2.error());

    // Copy to segment that not mounted should fail
    auto copy_result3 = service_->CreateCopyTask(key1, {"not_mounted_segment"});
    EXPECT_FALSE(copy_result3.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, copy_result3.error());
}

TEST_F(MasterServiceTest, CreateMoveTaskTest) {
    // Reset storage space metrics.
    MasterMetricManager::instance().reset_allocated_mem_size();
    MasterMetricManager::instance().reset_total_mem_capacity();

    // Create MasterService
    std::unique_ptr<MasterService> service_(new MasterService());

    // Mount 3 segments.
    constexpr size_t kReplicaCnt = 3;
    constexpr size_t kBaseAddr = 0x300000000;
    constexpr size_t kSegmentSize = 1024 * 1024 * 16;  // 16MB
    std::vector<MountedSegmentContext> contexts;
    contexts.reserve(kReplicaCnt);
    for (size_t i = 0; i < kReplicaCnt; ++i) {
        const auto context = PrepareSimpleSegment(
            *service_, "segment_" + std::to_string(i),
            kBaseAddr + static_cast<size_t>(i) * kSegmentSize, kSegmentSize);
        contexts.push_back(context);
    }

    // The client putting the object to segment_0
    auto client_id = generate_uuid();
    std::string key1 = "test_key_1";
    uint64_t value_length = 6 * 1024 * 1024;  // 6MB
    uint64_t slice_length = value_length;
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = "segment_0";
    auto put_start_result =
        service_->PutStart(client_id, key1, slice_length, config);
    EXPECT_TRUE(put_start_result.has_value());
    auto put_end_result =
        service_->PutEnd(client_id, key1, ReplicaType::MEMORY);
    EXPECT_TRUE(put_end_result.has_value());

    // Move key1 from "segment_0" to "segment_1"
    auto move_result = service_->CreateMoveTask(key1, "segment_0", "segment_1");
    EXPECT_TRUE(move_result.has_value());

    // Verify the move task is created and assigned to the client owning the
    // source segment
    auto task = service_->QueryTask(move_result.value());
    EXPECT_TRUE(task.has_value());
    EXPECT_EQ(TaskType::REPLICA_MOVE, task.value().type);
    EXPECT_EQ(contexts[0].client_id, task.value().assigned_client);

    // Move non-existent key should fail
    auto move_result1 =
        service_->CreateMoveTask("not_exist_key", "segment_0", "segment_1");
    EXPECT_FALSE(move_result1.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, move_result1.error());

    // Move to segment that is same as source should fail
    auto move_result_same =
        service_->CreateMoveTask(key1, "segment_1", "segment_1");
    EXPECT_FALSE(move_result_same.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, move_result_same.error());

    // Move to segment that is not mounted should fail
    auto move_result2 =
        service_->CreateMoveTask(key1, "segment_0", "not_mounted_segment");
    EXPECT_FALSE(move_result2.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, move_result2.error());

    // Move from segment that does not have the replica should fail
    auto move_result3 =
        service_->CreateMoveTask(key1, "segment_2", "segment_1");
    EXPECT_FALSE(move_result3.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, move_result3.error());

    // Move from segment that is not mounted should fail
    auto move_result4 =
        service_->CreateMoveTask(key1, "not_mounted_segment", "segment_1");
    EXPECT_FALSE(move_result4.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, move_result4.error());
}

TEST_F(MasterServiceTest, QueryTaskTest) {
    // Reset storage space metrics.
    MasterMetricManager::instance().reset_allocated_mem_size();
    MasterMetricManager::instance().reset_total_mem_capacity();

    // Create MasterService
    std::unique_ptr<MasterService> service_(new MasterService());

    // Mount 3 segments.
    constexpr size_t kReplicaCnt = 3;
    constexpr size_t kBaseAddr = 0x300000000;
    constexpr size_t kSegmentSize = 1024 * 1024 * 16;  // 16MB
    std::vector<MountedSegmentContext> contexts;
    contexts.reserve(kReplicaCnt);
    for (size_t i = 0; i < kReplicaCnt; ++i) {
        const auto context = PrepareSimpleSegment(
            *service_, "segment_" + std::to_string(i),
            kBaseAddr + static_cast<size_t>(i) * kSegmentSize, kSegmentSize);
        contexts.push_back(context);
    }

    // The client putting the object to segment_0
    auto client_id = generate_uuid();
    std::string key1 = "test_key_1";
    uint64_t value_length = 6 * 1024 * 1024;  // 6MB
    uint64_t slice_length = value_length;
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = "segment_0";
    auto put_start_result =
        service_->PutStart(client_id, key1, slice_length, config);
    EXPECT_TRUE(put_start_result.has_value());
    auto put_end_result =
        service_->PutEnd(client_id, key1, ReplicaType::MEMORY);
    EXPECT_TRUE(put_end_result.has_value());

    // Move key1 from "segment_0" to "segment_1"
    auto move_result = service_->CreateMoveTask(key1, "segment_0", "segment_1");
    EXPECT_TRUE(move_result.has_value());

    // Query non-existent task should fail
    auto query_result = service_->QueryTask(UUID{0, 0});
    EXPECT_FALSE(query_result.has_value());
    EXPECT_EQ(ErrorCode::TASK_NOT_FOUND, query_result.error());

    // Query the move task
    auto query_result_move = service_->QueryTask(move_result.value());
    EXPECT_TRUE(query_result_move.has_value());
    EXPECT_EQ(TaskType::REPLICA_MOVE, query_result_move.value().type);
    EXPECT_EQ(contexts[0].client_id, query_result_move.value().assigned_client);
}

TEST_F(MasterServiceTest, FetchTasksEmptyWhenNoTasks) {
    std::unique_ptr<MasterService> service_(new MasterService());
    const auto ctx0 = PrepareSimpleSegment(*service_, "segment_0", 0x300000000,
                                           kDefaultSegmentSize);

    auto fetch = service_->FetchTasks(ctx0.client_id, /*batch_size=*/16);
    ASSERT_TRUE(fetch.has_value());
    EXPECT_TRUE(fetch->empty());
}

TEST_F(MasterServiceTest, FetchTasksReturnsAssignedTasksOnlyAndDrainsQueue) {
    std::unique_ptr<MasterService> service_(new MasterService());
    const auto ctx0 = PrepareSimpleSegment(*service_, "segment_0", 0x300000000,
                                           kDefaultSegmentSize);
    const auto ctx1 = PrepareSimpleSegment(*service_, "segment_1", 0x400000000,
                                           kDefaultSegmentSize);
    // Put an object with its (only) replica on segment_0 so Copy/Move
    // assignment is deterministic.
    const UUID put_client_id = generate_uuid();
    const std::string key = "fetch_tasks_key_0";

    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = "segment_0";

    ASSERT_TRUE(
        service_->PutStart(put_client_id, key, /*slice_length=*/1024, config)
            .has_value());
    ASSERT_TRUE(
        service_->PutEnd(put_client_id, key, ReplicaType::MEMORY).has_value());

    // Create two tasks; both should be assigned to the client owning source
    // segment_0.
    auto copy_task_id = service_->CreateCopyTask(key, {"segment_1"});
    ASSERT_TRUE(copy_task_id.has_value());

    auto move_task_id = service_->CreateMoveTask(key, "segment_0", "segment_1");
    ASSERT_TRUE(move_task_id.has_value());

    // Fetch from client_0 should get both tasks (order not guaranteed).
    auto fetch0 = service_->FetchTasks(ctx0.client_id, /*batch_size=*/16);
    ASSERT_TRUE(fetch0.has_value());
    ASSERT_EQ(fetch0->size(), 2u);

    std::vector<UUID> fetched_ids;
    fetched_ids.reserve(fetch0->size());
    for (const auto& a : *fetch0) {
        fetched_ids.push_back(
            a.id);  // TaskAssignment is expected to carry id/type/payload
    }

    EXPECT_NE(
        std::find(fetched_ids.begin(), fetched_ids.end(), copy_task_id.value()),
        fetched_ids.end());
    EXPECT_NE(
        std::find(fetched_ids.begin(), fetched_ids.end(), move_task_id.value()),
        fetched_ids.end());

    // Fetch from client_1 should return empty (no tasks assigned to it).
    auto fetch1 = service_->FetchTasks(ctx1.client_id, /*batch_size=*/16);
    ASSERT_TRUE(fetch1.has_value());
    EXPECT_TRUE(fetch1->empty());

    // Fetch again from client_0 should be empty if pop_tasks drains pending
    // queue.
    auto fetch0_again = service_->FetchTasks(ctx0.client_id, /*batch_size=*/16);
    ASSERT_TRUE(fetch0_again.has_value());
    EXPECT_TRUE(fetch0_again->empty());
}

TEST_F(MasterServiceTest, FetchTasksRespectsBatchSize) {
    std::unique_ptr<MasterService> service_(new MasterService());

    const auto ctx0 = PrepareSimpleSegment(*service_, "segment_0", 0x300000000,
                                           kDefaultSegmentSize);
    [[maybe_unused]] const auto ctx1 = PrepareSimpleSegment(
        *service_, "segment_1", 0x400000000, kDefaultSegmentSize);

    const UUID put_client_id = generate_uuid();
    const std::string key = "fetch_tasks_key_1";

    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = "segment_0";

    ASSERT_TRUE(
        service_->PutStart(put_client_id, key, /*slice_length=*/1024, config)
            .has_value());
    ASSERT_TRUE(
        service_->PutEnd(put_client_id, key, ReplicaType::MEMORY).has_value());

    auto t1 = service_->CreateCopyTask(key, {"segment_1"});
    ASSERT_TRUE(t1.has_value());
    auto t2 = service_->CreateMoveTask(key, "segment_0", "segment_1");
    ASSERT_TRUE(t2.has_value());

    auto fetch_first = service_->FetchTasks(ctx0.client_id, /*batch_size=*/1);
    ASSERT_TRUE(fetch_first.has_value());
    ASSERT_EQ(fetch_first->size(), 1u);

    auto fetch_second = service_->FetchTasks(ctx0.client_id, /*batch_size=*/1);
    ASSERT_TRUE(fetch_second.has_value());
    ASSERT_EQ(fetch_second->size(), 1u);

    // Combined should contain both task ids (order not guaranteed).
    std::vector<UUID> ids;
    ids.push_back(fetch_first->at(0).id);
    ids.push_back(fetch_second->at(0).id);

    EXPECT_NE(std::find(ids.begin(), ids.end(), t1.value()), ids.end());
    EXPECT_NE(std::find(ids.begin(), ids.end(), t2.value()), ids.end());

    auto fetch_third = service_->FetchTasks(ctx0.client_id, /*batch_size=*/1);
    ASSERT_TRUE(fetch_third.has_value());
    EXPECT_TRUE(fetch_third->empty());
}

TEST_F(MasterServiceTest, UpdateTaskSuccessFlow) {
    auto service_ = std::make_unique<MasterService>();

    const auto ctx0 = PrepareSimpleSegment(*service_, "segment_0", 0x300000000,
                                           kDefaultSegmentSize);
    [[maybe_unused]] const auto ctx1 = PrepareSimpleSegment(
        *service_, "segment_1", 0x400000000, kDefaultSegmentSize);

    // Put an object with its (only) replica on segment_0 so task assignment is
    // deterministic.
    const UUID put_client_id = generate_uuid();
    const std::string key = "update_task_key_success";

    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = "segment_0";

    ASSERT_TRUE(
        service_->PutStart(put_client_id, key, /*slice_length=*/1024, config)
            .has_value());
    ASSERT_TRUE(
        service_->PutEnd(put_client_id, key, ReplicaType::MEMORY).has_value());

    // Create a task assigned to client owning segment_0.
    auto task_id_res = service_->CreateCopyTask(key, {"segment_1"});
    ASSERT_TRUE(task_id_res.has_value());
    const UUID task_id = task_id_res.value();

    // Poll once so the task transitions to PROCESSING (typical semantics).
    auto fetched = service_->FetchTasks(ctx0.client_id, /*batch_size=*/16);
    ASSERT_TRUE(fetched.has_value());
    ASSERT_EQ(fetched->size(), 1u);
    EXPECT_EQ(fetched->at(0).id, task_id);

    // Update task to SUCCESS.
    TaskCompleteRequest req{};
    req.id = task_id;
    req.status = TaskStatus::SUCCESS;
    req.message = "done";

    auto update_res = service_->MarkTaskToComplete(ctx0.client_id, req);
    ASSERT_TRUE(update_res.has_value()) << "MarkTaskToComplete failed";

    // Verify task state via QueryTask.
    auto qt = service_->QueryTask(task_id);
    ASSERT_TRUE(qt.has_value());
    EXPECT_EQ(qt->id, task_id);
    EXPECT_EQ(qt->status, TaskStatus::SUCCESS);
    EXPECT_EQ(qt->assigned_client, ctx0.client_id);
    EXPECT_EQ(qt->message, "done");

    // Queue should be drained for that client.
    auto fetched_again =
        service_->FetchTasks(ctx0.client_id, /*batch_size=*/16);
    ASSERT_TRUE(fetched_again.has_value());
    EXPECT_TRUE(fetched_again->empty());
}

TEST_F(MasterServiceTest, UpdateTaskRejectsWrongClient) {
    auto service_ = std::make_unique<MasterService>();

    const auto ctx0 = PrepareSimpleSegment(*service_, "segment_0", 0x300000000,
                                           kDefaultSegmentSize);
    const auto ctx1 = PrepareSimpleSegment(*service_, "segment_1", 0x400000000,
                                           kDefaultSegmentSize);

    const UUID put_client_id = generate_uuid();
    const std::string key = "update_task_wrong_client";

    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = "segment_0";

    ASSERT_TRUE(
        service_->PutStart(put_client_id, key, /*slice_length=*/1024, config)
            .has_value());
    ASSERT_TRUE(
        service_->PutEnd(put_client_id, key, ReplicaType::MEMORY).has_value());

    auto task_id_res = service_->CreateMoveTask(key, "segment_0", "segment_1");
    ASSERT_TRUE(task_id_res.has_value());
    const UUID task_id = task_id_res.value();

    // Poll by the correct client to take the task.
    auto fetched = service_->FetchTasks(ctx0.client_id, /*batch_size=*/16);
    ASSERT_TRUE(fetched.has_value());
    ASSERT_EQ(fetched->size(), 1u);
    EXPECT_EQ(fetched->at(0).id, task_id);

    // Try to update with a different client id, should fail.
    TaskCompleteRequest req{};
    req.id = task_id;
    req.status = TaskStatus::SUCCESS;
    req.message = "should_not_work";

    auto update_res = service_->MarkTaskToComplete(ctx1.client_id, req);
    ASSERT_FALSE(update_res.has_value());
    EXPECT_EQ(update_res.error(), ErrorCode::ILLEGAL_CLIENT);
}

TEST_F(MasterServiceTest, UpdateTaskNotFound) {
    auto service_ = std::make_unique<MasterService>();
    const auto ctx0 = PrepareSimpleSegment(*service_, "segment_0", 0x300000000,
                                           kDefaultSegmentSize);

    TaskCompleteRequest req{};
    req.id = generate_uuid();  // non-existent task id
    req.status = TaskStatus::FAILED;
    req.message = "not_found";

    auto update_res = service_->MarkTaskToComplete(ctx0.client_id, req);
    ASSERT_FALSE(update_res.has_value());
    EXPECT_EQ(update_res.error(), ErrorCode::TASK_NOT_FOUND);
}

// Test force Remove - should bypass lease check
TEST_F(MasterServiceTest, ForceRemoveLeasedObject) {
    // Set a long lease TTL so objects will have active leases
    const uint64_t kv_lease_ttl = 10000;  // 10 seconds
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    // Put an object
    std::string key = "leased_key";
    uint64_t slice_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;
    auto put_start_result =
        service_->PutStart(client_id, key, slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto put_end_result = service_->PutEnd(client_id, key, ReplicaType::MEMORY);
    ASSERT_TRUE(put_end_result.has_value());

    // Verify object exists
    auto exist_result = service_->ExistKey(key);
    ASSERT_TRUE(exist_result.has_value());
    ASSERT_TRUE(exist_result.value());

    // Normal remove should fail because object has active lease
    auto remove_result_no_force = service_->Remove(key, false);
    EXPECT_FALSE(remove_result_no_force.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_HAS_LEASE, remove_result_no_force.error());

    // Force remove should succeed even with active lease
    auto remove_result_force = service_->Remove(key, true);
    EXPECT_TRUE(remove_result_force.has_value());

    // Verify object is removed
    auto get_result = service_->GetReplicaList(key);
    EXPECT_FALSE(get_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, get_result.error());
}

// Test force RemoveByRegex - should bypass lease check
TEST_F(MasterServiceTest, ForceRemoveByRegexLeasedObjects) {
    // Set a long lease TTL so objects will have active leases
    const uint64_t kv_lease_ttl = 10000;  // 10 seconds
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    // Put 5 objects and grant them active leases by reading
    for (int i = 0; i < 5; ++i) {
        std::string key = "force_regex_key_" + std::to_string(i);
        uint64_t slice_length = 1024;
        ReplicateConfig config;
        config.replica_num = 1;
        auto put_start_result =
            service_->PutStart(client_id, key, slice_length, config);
        ASSERT_TRUE(put_start_result.has_value());
        auto put_end_result =
            service_->PutEnd(client_id, key, ReplicaType::MEMORY);
        ASSERT_TRUE(put_end_result.has_value());
        // Grant lease by reading the object
        auto exist_result = service_->ExistKey(key);
        ASSERT_TRUE(exist_result.has_value());
        ASSERT_TRUE(exist_result.value());
    }

    // Normal RemoveByRegex should remove 0 because all objects have active
    // leases
    auto remove_result_no_force =
        service_->RemoveByRegex("^force_regex_key_", false);
    ASSERT_TRUE(remove_result_no_force.has_value());
    EXPECT_EQ(0, remove_result_no_force.value());

    // All objects should still exist
    for (int i = 0; i < 5; ++i) {
        std::string key = "force_regex_key_" + std::to_string(i);
        auto exist_result = service_->ExistKey(key);
        ASSERT_TRUE(exist_result.has_value());
        ASSERT_TRUE(exist_result.value());
    }

    // Force RemoveByRegex should remove all 5 objects
    auto remove_result_force =
        service_->RemoveByRegex("^force_regex_key_", true);
    ASSERT_TRUE(remove_result_force.has_value());
    EXPECT_EQ(5, remove_result_force.value());

    // All objects should be removed
    for (int i = 0; i < 5; ++i) {
        std::string key = "force_regex_key_" + std::to_string(i);
        auto exist_result = service_->ExistKey(key);
        ASSERT_TRUE(exist_result.has_value());
        ASSERT_FALSE(exist_result.value());
    }
}

// Test force RemoveAll - should bypass lease check
TEST_F(MasterServiceTest, ForceRemoveAllLeasedObjects) {
    // Set a long lease TTL so objects will have active leases
    const uint64_t kv_lease_ttl = 10000;  // 10 seconds
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(kv_lease_ttl)
                              .build();
    std::unique_ptr<MasterService> service_(new MasterService(service_config));
    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service_);
    const UUID client_id = generate_uuid();

    // Put 10 objects and grant them active leases by reading
    for (int i = 0; i < 10; ++i) {
        std::string key = "force_all_key_" + std::to_string(i);
        uint64_t slice_length = 1024;
        ReplicateConfig config;
        config.replica_num = 1;
        auto put_start_result =
            service_->PutStart(client_id, key, slice_length, config);
        ASSERT_TRUE(put_start_result.has_value());
        auto put_end_result =
            service_->PutEnd(client_id, key, ReplicaType::MEMORY);
        ASSERT_TRUE(put_end_result.has_value());
        // Grant lease by reading the object
        auto exist_result = service_->ExistKey(key);
        ASSERT_TRUE(exist_result.has_value());
        ASSERT_TRUE(exist_result.value());
    }

    // Normal RemoveAll should remove 0 because all objects have active leases
    EXPECT_EQ(0, service_->RemoveAll(false));

    // All objects should still exist
    for (int i = 0; i < 10; ++i) {
        std::string key = "force_all_key_" + std::to_string(i);
        auto exist_result = service_->ExistKey(key);
        ASSERT_TRUE(exist_result.has_value());
        ASSERT_TRUE(exist_result.value());
    }

    // Force RemoveAll should remove all 10 objects
    EXPECT_EQ(10, service_->RemoveAll(true));

    // All objects should be removed
    for (int i = 0; i < 10; ++i) {
        std::string key = "force_all_key_" + std::to_string(i);
        auto exist_result = service_->ExistKey(key);
        ASSERT_TRUE(exist_result.has_value());
        ASSERT_FALSE(exist_result.value());
    }
}
}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

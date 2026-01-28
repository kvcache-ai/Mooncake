#include "centralized_master_service.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <memory>
#include <thread>
#include <vector>

#include "types.h"

namespace mooncake::test {

std::unique_ptr<CentralizedMasterService> CreateMasterServiceWithSSDFeat(
    const std::string& root_fs_dir) {
    return std::make_unique<CentralizedMasterService>(
        MasterServiceConfig::builder().set_root_fs_dir(root_fs_dir).build());
}

class MasterServiceSSDTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("MasterServiceTest");
        FLAGS_logtostderr = true;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }
};

TEST_F(MasterServiceSSDTest, PutEndBothReplica) {
    auto service_ = CreateMasterServiceWithSSDFeat("/mnt/ssd");

    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 64;
    std::string segment_name = "test_segment";
    Segment segment;
    segment.id = generate_uuid();
    segment.name = segment_name;
    segment.base = buffer;
    segment.size = size;
    segment.te_endpoint = segment.name;
    UUID client_id = generate_uuid();

    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    std::string key = "disk_key";
    uint64_t slice_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;

    auto put_start_result =
        service_->PutStart(client_id, key, slice_length, config);
    ASSERT_TRUE(put_start_result.has_value());
    auto replicas = put_start_result.value();
    ASSERT_EQ(2, replicas.size());

    bool has_mem = false, has_disk = false;
    for (const auto& r : replicas) {
        if (r.is_memory_replica()) has_mem = true;
        if (r.is_disk_replica()) has_disk = true;
    }
    EXPECT_TRUE(has_mem);
    EXPECT_TRUE(has_disk);

    auto get_result = service_->GetReplicaList(key);
    ASSERT_FALSE(get_result.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_NOT_READY, get_result.error());

    // PutEnd for both memory and disk
    EXPECT_TRUE(
        service_->PutEnd(client_id, key, ReplicaType::MEMORY).has_value());
    EXPECT_TRUE(
        service_->PutEnd(client_id, key, ReplicaType::DISK).has_value());

    get_result = service_->GetReplicaList(key);
    ASSERT_TRUE(get_result.has_value());
    EXPECT_EQ(2, get_result.value().replicas.size());

    for (const auto& r : get_result.value().replicas) {
        EXPECT_EQ(ReplicaStatus::COMPLETE, r.status);
    }
}

TEST_F(MasterServiceSSDTest, PutRevokeDiskReplica) {
    auto service_ = CreateMasterServiceWithSSDFeat("/mnt/ssd");

    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 64;
    std::string segment_name = "test_segment";
    Segment segment;
    segment.id = generate_uuid();
    segment.name = segment_name;
    segment.base = buffer;
    segment.size = size;
    segment.te_endpoint = segment.name;
    UUID client_id = generate_uuid();

    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    std::string key = "revoke_key";
    uint64_t slice_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;

    ASSERT_TRUE(
        service_->PutStart(client_id, key, slice_length, config).has_value());
    EXPECT_TRUE(
        service_->PutEnd(client_id, key, ReplicaType::MEMORY).has_value());

    auto get_result = service_->GetReplicaList(key);
    ASSERT_TRUE(get_result.has_value());
    EXPECT_EQ(1, get_result.value().replicas.size());
    ASSERT_TRUE(get_result.value().replicas[0].is_memory_replica());

    EXPECT_TRUE(
        service_->PutRevoke(client_id, key, ReplicaType::DISK).has_value());

    get_result = service_->GetReplicaList(key);
    ASSERT_TRUE(get_result.has_value());
    EXPECT_EQ(1, get_result.value().replicas.size());
    ASSERT_TRUE(get_result.value().replicas[0].is_memory_replica());
}

TEST_F(MasterServiceSSDTest, PutRevokeMemoryReplica) {
    auto service_ = CreateMasterServiceWithSSDFeat("/mnt/ssd");

    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 64;
    std::string segment_name = "test_segment";
    Segment segment;
    segment.id = generate_uuid();
    segment.name = segment_name;
    segment.base = buffer;
    segment.size = size;
    segment.te_endpoint = segment.name;
    UUID client_id = generate_uuid();

    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    std::string key = "revoke_key";
    uint64_t slice_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;

    ASSERT_TRUE(
        service_->PutStart(client_id, key, slice_length, config).has_value());
    EXPECT_TRUE(
        service_->PutRevoke(client_id, key, ReplicaType::MEMORY).has_value());

    auto get_result = service_->GetReplicaList(key);
    ASSERT_FALSE(get_result.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_NOT_READY, get_result.error());

    EXPECT_TRUE(
        service_->PutEnd(client_id, key, ReplicaType::DISK).has_value());
    get_result = service_->GetReplicaList(key);
    ASSERT_TRUE(get_result.has_value());
    EXPECT_EQ(1, get_result.value().replicas.size());
    ASSERT_TRUE(get_result.value().replicas[0].is_disk_replica());
}

TEST_F(MasterServiceSSDTest, PutRevokeBothReplica) {
    auto service_ = CreateMasterServiceWithSSDFeat("/mnt/ssd");

    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 64;
    std::string segment_name = "test_segment";
    Segment segment;
    segment.id = generate_uuid();
    segment.name = segment_name;
    segment.base = buffer;
    segment.size = size;
    segment.te_endpoint = segment.name;
    UUID client_id = generate_uuid();

    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    std::string key = "revoke_key";
    uint64_t slice_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;

    ASSERT_TRUE(
        service_->PutStart(client_id, key, slice_length, config).has_value());
    EXPECT_TRUE(
        service_->PutRevoke(client_id, key, ReplicaType::DISK).has_value());

    auto get_result = service_->GetReplicaList(key);
    ASSERT_FALSE(get_result.has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_NOT_READY, get_result.error());

    EXPECT_TRUE(
        service_->PutRevoke(client_id, key, ReplicaType::MEMORY).has_value());
    get_result = service_->GetReplicaList(key);
    ASSERT_FALSE(get_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, get_result.error());
}

TEST_F(MasterServiceSSDTest, RemoveKey) {
    auto service_ = CreateMasterServiceWithSSDFeat("/mnt/ssd");

    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 64;
    std::string segment_name = "test_segment";
    Segment segment;
    segment.id = generate_uuid();
    segment.name = segment_name;
    segment.base = buffer;
    segment.size = size;
    segment.te_endpoint = segment.name;
    UUID client_id = generate_uuid();

    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    std::string key = "remove_key";
    uint64_t slice_length = 1024;
    ReplicateConfig config;
    config.replica_num = 1;

    ASSERT_TRUE(
        service_->PutStart(client_id, key, slice_length, config).has_value());
    EXPECT_TRUE(
        service_->PutEnd(client_id, key, ReplicaType::MEMORY).has_value());
    EXPECT_TRUE(
        service_->PutEnd(client_id, key, ReplicaType::DISK).has_value());

    EXPECT_TRUE(service_->Remove(key).has_value());

    auto get_result = service_->GetReplicaList(key);
    EXPECT_FALSE(get_result.has_value());
    EXPECT_EQ(ErrorCode::OBJECT_NOT_FOUND, get_result.error());
}

TEST_F(MasterServiceSSDTest, EvictObject) {
    auto service_ = CreateMasterServiceWithSSDFeat("/mnt/ssd");
    // Mount a segment that can hold about 1024 * 16 objects.
    // As the eviction is processed separately for each shard,
    // we need to fill each shard with enough objects to thoroughly
    // test the eviction process.
    constexpr size_t buffer = 0x300000000;
    constexpr size_t size = 1024 * 1024 * 16 * 15;
    constexpr size_t object_size = 1024 * 15;
    std::string segment_name = "test_segment";
    Segment segment;
    segment.id = generate_uuid();
    segment.name = segment_name;
    segment.base = buffer;
    segment.size = size;
    segment.te_endpoint = segment.name;
    UUID client_id = generate_uuid();
    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

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
            auto put_end_mem_result =
                service_->PutEnd(client_id, key, ReplicaType::MEMORY);
            auto put_end_disk_result =
                service_->PutEnd(client_id, key, ReplicaType::DISK);
            ASSERT_TRUE(put_end_mem_result.has_value());
            ASSERT_TRUE(put_end_disk_result.has_value());
            success_puts++;
        } else {
            // wait for eviction to work
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    }
    ASSERT_GT(success_puts, 1024 * 16);

    // Verify if we can get objects more than the segment can hold
    int success_gets = 0;
    for (int i = 0; i < 1024 * 16 + 50; ++i) {
        std::string key = "test_key" + std::to_string(i);
        auto get_result = service_->GetReplicaList(key);
        if (get_result.has_value()) {
            success_gets++;
        }
    }
    ASSERT_GT(success_gets, 1024 * 16);

    std::this_thread::sleep_for(
        std::chrono::milliseconds(DEFAULT_DEFAULT_KV_LEASE_TTL));
    service_->RemoveAll();
}

TEST_F(MasterServiceSSDTest, PutStartExpires) {
    // Reset storage space metrics.
    MasterMetricManager::instance().reset_allocated_mem_size();
    MasterMetricManager::instance().reset_total_mem_capacity();

    MasterServiceConfig master_config;
    master_config.root_fs_dir = "/mnt/ssd";
    master_config.put_start_discard_timeout_sec = 3;
    master_config.put_start_release_timeout_sec = 5;
    std::unique_ptr<CentralizedMasterService> service_(new CentralizedMasterService(master_config));

    constexpr size_t kReplicaCnt = 2;  // 1 memory replica + 1 disk replica
    constexpr size_t kBaseAddr = 0x300000000;
    constexpr size_t kSegmentSize = 1024 * 1024 * 16;  // 16MB

    // Mount a segment.
    std::string segment_name = "test_segment";
    Segment segment;
    segment.id = generate_uuid();
    segment.name = segment_name;
    segment.base = kBaseAddr;
    segment.size = kSegmentSize;
    segment.te_endpoint = segment.name;
    auto client_id = generate_uuid();
    auto mount_result = service_->MountSegment(segment, client_id);
    ASSERT_TRUE(mount_result.has_value());

    std::string key = "test_key";
    uint64_t value_length = 16 * 1024 * 1024;  // 16MB
    uint64_t slice_length = value_length;
    ReplicateConfig config;

    auto test_discard_replica = [&](ReplicaType discard_type) {
        const auto reserve_type = discard_type == ReplicaType::MEMORY
                                      ? ReplicaType::DISK
                                      : ReplicaType::MEMORY;

        // Put key, should success.
        auto put_start_result =
            service_->PutStart(client_id, key, slice_length, config);
        EXPECT_TRUE(put_start_result.has_value());
        auto replica_list = put_start_result.value();
        EXPECT_EQ(replica_list.size(), kReplicaCnt);
        for (size_t i = 0; i < kReplicaCnt; i++) {
            EXPECT_EQ(ReplicaStatus::PROCESSING, replica_list[i].status);
        }

        // Complete the reserved replica.
        auto put_end_result = service_->PutEnd(client_id, key, reserve_type);
        EXPECT_TRUE(put_end_result.has_value());

        // Wait for a while until the put-start expired.
        for (size_t i = 0; i <= master_config.put_start_discard_timeout_sec;
             i++) {
            // Keep mounted segments alive.
            auto result = service_->Ping(client_id);
            EXPECT_TRUE(result.has_value());
            // Protect the key from eviction.
            auto get_result = service_->GetReplicaList(key);
            EXPECT_TRUE(get_result.has_value());
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        // Put key again, should fail because the object has had an completed
        // replica.
        put_start_result =
            service_->PutStart(client_id, key, slice_length, config);
        EXPECT_FALSE(put_start_result.has_value());
        EXPECT_EQ(put_start_result.error(), ErrorCode::OBJECT_ALREADY_EXISTS);

        // Wait for a while until the discarded replicas are released.
        for (size_t i = 0; i <= master_config.put_start_release_timeout_sec;
             i++) {
            // Keep mounted segments alive.
            auto result = service_->Ping(client_id);
            EXPECT_TRUE(result.has_value());
            // Protect the key from eviction.
            auto get_result = service_->GetReplicaList(key);
            EXPECT_TRUE(get_result.has_value());
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        // Try PutEnd the discarded replica.
        put_end_result = service_->PutEnd(client_id, key, discard_type);
        EXPECT_TRUE(put_end_result.has_value());

        // Check that the key has only one replica.
        auto get_result = service_->GetReplicaList(key);
        EXPECT_TRUE(get_result.has_value());
        EXPECT_EQ(get_result.value().replicas.size(), 1);
        if (reserve_type == ReplicaType::MEMORY) {
            EXPECT_TRUE(get_result.value().replicas[0].is_memory_replica());
        } else {
            EXPECT_TRUE(get_result.value().replicas[0].is_disk_replica());
        }

        // Wait for the key to expire.
        for (size_t i = 0; i <= DEFAULT_DEFAULT_KV_LEASE_TTL / 1000; i++) {
            auto result = service_->Ping(client_id);
            EXPECT_TRUE(result.has_value());
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        service_->RemoveAll();
    };

    test_discard_replica(ReplicaType::DISK);
    test_discard_replica(ReplicaType::MEMORY);
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

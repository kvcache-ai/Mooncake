#include "master_service.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include "ha/oplog/mock_oplog_store.h"
#include "types.h"

namespace mooncake::test {

class MasterServiceHATest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("MasterServiceHATest");
        FLAGS_logtostderr = 1;
    }

    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

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

    struct MountedSegmentContext {
        UUID segment_id;
        UUID client_id;
    };

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

    std::string PutObject(MasterService& service, const UUID& client_id,
                          const std::string& key,
                          size_t slice_length = 1024) const {
        ReplicateConfig config;
        config.replica_num = 1;
        auto put_start = service.PutStart(client_id, key, slice_length, config);
        EXPECT_TRUE(put_start.has_value());
        EXPECT_TRUE(
            service.PutEnd(client_id, key, ReplicaType::MEMORY).has_value());
        return key;
    }

    std::string PutObjectOnSegment(MasterService& service,
                                   const UUID& client_id,
                                   const std::string& key,
                                   const std::string& segment_name,
                                   size_t slice_length = 1024) const {
        ReplicateConfig config;
        config.replica_num = 1;
        config.preferred_segments = {segment_name};
        auto put_start = service.PutStart(client_id, key, slice_length, config);
        EXPECT_TRUE(put_start.has_value());
        EXPECT_TRUE(
            service.PutEnd(client_id, key, ReplicaType::MEMORY).has_value());
        return key;
    }

    Replica::Descriptor MakeStandbyMemoryReplica(const std::string& endpoint,
                                                 size_t size = 1024) const {
        Replica::Descriptor replica;
        replica.id = 1;
        replica.status = ReplicaStatus::COMPLETE;

        MemoryDescriptor mem_desc;
        mem_desc.buffer_descriptor.transport_endpoint_ = endpoint;
        mem_desc.buffer_descriptor.buffer_address_ = 0;
        mem_desc.buffer_descriptor.size_ = size;
        replica.descriptor_variant = std::move(mem_desc);
        return replica;
    }

    StandbyObjectEntry MakeStandbyObject(const std::string& key,
                                         const std::string& endpoint,
                                         size_t size = 1024) const {
        StandbyObjectMetadata metadata;
        metadata.client_id = generate_uuid();
        metadata.size = size;
        metadata.last_sequence_id = 1;
        metadata.replicas.push_back(MakeStandbyMemoryReplica(endpoint, size));
        return StandbyObjectEntry{"default", key, std::move(metadata)};
    }

    StandbySegmentInfo MakeStandbyMemorySegment(
        const std::string& endpoint,
        size_t capacity = kDefaultSegmentSize) const {
        StandbySegmentInfo segment;
        segment.segment_name = endpoint;
        segment.transport_endpoint = endpoint;
        segment.capacity = capacity;
        segment.is_memory_segment = true;
        return segment;
    }
};

TEST_F(MasterServiceHATest, RestoreFromStandbySnapshotClearsInvalidEndpoints) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id("test_cluster")
                              .build();
    MasterService service(service_config);
    service.SetOpLogStoreForTesting(std::make_shared<MockOpLogStore>());

    const std::string endpoint = "restored_segment";
    [[maybe_unused]] const auto context =
        PrepareSimpleSegment(service, endpoint);

    service.RestoreFromStandbySnapshot(
        {MakeStandbyObject("stale_restore_key", endpoint)}, 1, {});

    service.RestoreFromStandbySnapshot(
        {MakeStandbyObject("valid_restore_key", endpoint)}, 2,
        {MakeStandbyMemorySegment(endpoint)});

    auto valid_result = service.GetReplicaList("valid_restore_key");
    ASSERT_TRUE(valid_result.has_value()) << toString(valid_result.error());
    ASSERT_EQ(1u, valid_result->replicas.size());
    EXPECT_EQ(endpoint, valid_result->replicas.front()
                            .get_memory_descriptor()
                            .buffer_descriptor.transport_endpoint_);
}

// Test that RemoveByRegex publishes REMOVE OpLog entries for matched keys.
TEST_F(MasterServiceHATest, RemoveByRegexPublishesRemoveOpLog) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id("test_cluster")
                              .build();
    std::unique_ptr<MasterService> service(new MasterService(service_config));

    auto mock_store = std::make_shared<MockOpLogStore>();
    service->SetOpLogStoreForTesting(mock_store);

    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const UUID client_id = generate_uuid();

    std::vector<std::string> keys;
    for (int i = 0; i < 5; ++i) {
        keys.push_back("regex_key_" + std::to_string(i));
        PutObject(*service, client_id, keys.back());
    }

    // Wait for leases to expire so RemoveByRegex can remove them.
    std::this_thread::sleep_for(std::chrono::milliseconds(60));

    size_t entries_before = mock_store->EntryCount();
    auto res = service->RemoveByRegex("^regex_key_", /*force=*/true);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(5, res.value());

    size_t entries_after = mock_store->EntryCount();
    EXPECT_EQ(entries_before + 5, entries_after);

    // Verify all entries are REMOVE ops for the expected keys.
    std::vector<std::string> removed_keys;
    uint64_t max_seq = 0;
    EXPECT_EQ(ErrorCode::OK, mock_store->GetMaxSequenceId(max_seq));
    for (uint64_t seq = entries_before + 1; seq <= max_seq; ++seq) {
        OpLogEntry entry;
        EXPECT_EQ(ErrorCode::OK, mock_store->ReadOpLog(seq, entry));
        EXPECT_EQ(OpType::REMOVE, entry.op_type);
        removed_keys.push_back(entry.object_key);
    }
    EXPECT_EQ(5u, removed_keys.size());
    for (const auto& key : keys) {
        EXPECT_TRUE(std::find(removed_keys.begin(), removed_keys.end(), key) !=
                    removed_keys.end())
            << "Missing REMOVE OpLog for key=" << key;
    }
}

// Test that BatchRemove skips erase when OpLog persist fails.
TEST_F(MasterServiceHATest, BatchRemovePersistFailureSkipsErase) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id("test_cluster")
                              .build();
    std::unique_ptr<MasterService> service(new MasterService(service_config));

    auto mock_store = std::make_shared<MockOpLogStore>();
    service->SetOpLogStoreForTesting(mock_store);
    service->SetOpLogRetryConfigForTesting(2, 50);

    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const UUID client_id = generate_uuid();

    std::vector<std::string> keys;
    for (int i = 0; i < 3; ++i) {
        keys.push_back("batch_key_" + std::to_string(i));
        PutObject(*service, client_id, keys.back());
    }

    // Wait for leases to expire.
    std::this_thread::sleep_for(std::chrono::milliseconds(60));

    // Make the OpLog store fail on write.
    mock_store->SetWriteError(ErrorCode::PERSISTENT_FAIL);

    auto results = service->BatchRemove(keys, /*force=*/true);
    ASSERT_EQ(keys.size(), results.size());

    // All results should report failure because persist failed.
    for (size_t i = 0; i < results.size(); ++i) {
        EXPECT_FALSE(results[i].has_value())
            << "Expected failure for key=" << keys[i];
    }

    // Keys should still exist in metadata because erase was skipped.
    for (const auto& key : keys) {
        auto exist = service->ExistKey(key);
        ASSERT_TRUE(exist.has_value());
        EXPECT_TRUE(exist.value()) << "Key should still exist: " << key;
    }

    // Restore the store and retry — removal should succeed now.
    mock_store->SetWriteError(ErrorCode::OK);
    results = service->BatchRemove(keys, /*force=*/true);
    for (size_t i = 0; i < results.size(); ++i) {
        EXPECT_TRUE(results[i].has_value())
            << "Retry should succeed for key=" << keys[i];
    }
    for (const auto& key : keys) {
        auto exist = service->ExistKey(key);
        ASSERT_TRUE(exist.has_value());
        EXPECT_FALSE(exist.value()) << "Key should be removed: " << key;
    }
}

// PutRevoke on an object with a PROCESSING MEMORY replica publishes REMOVE
// OpLog.
TEST_F(MasterServiceHATest, PutRevokeSingleReplicaPublishesRemoveOpLog) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id("test_cluster")
                              .build();
    std::unique_ptr<MasterService> service(new MasterService(service_config));

    auto mock_store = std::make_shared<MockOpLogStore>();
    service->SetOpLogStoreForTesting(mock_store);

    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const UUID client_id = generate_uuid();
    const std::string key = "put_revoke_single_key";

    ReplicateConfig config;
    config.replica_num = 1;
    auto put_start = service->PutStart(client_id, key, 1024, config);
    ASSERT_TRUE(put_start.has_value());

    auto res = service->PutRevoke(client_id, key, ReplicaType::MEMORY);
    ASSERT_TRUE(res.has_value());

    OpLogEntry entry;
    EXPECT_EQ(ErrorCode::OK, mock_store->FindLatestEntryForKey(key, entry));
    EXPECT_EQ(OpType::REMOVE, entry.op_type);
}

// PutRevoke(MEMORY) on an object with PROCESSING MEMORY + LOCAL_DISK publishes
// PUT_END with the LOCAL_DISK descriptor.
TEST_F(MasterServiceHATest, PutRevokeKeepsLocalDiskPublishesPutEndOpLog) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id("test_cluster")
                              .build();
    std::unique_ptr<MasterService> service(new MasterService(service_config));

    auto mock_store = std::make_shared<MockOpLogStore>();
    service->SetOpLogStoreForTesting(mock_store);

    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const UUID client_id = generate_uuid();
    const std::string key = "put_revoke_mixed_key";

    ReplicateConfig config;
    config.replica_num = 1;
    auto put_start = service->PutStart(client_id, key, 1024, config);
    ASSERT_TRUE(put_start.has_value());

    Replica local_disk_replica(client_id, 1024, "local_disk_endpoint",
                               ReplicaStatus::COMPLETE);
    auto add_res = service->AddReplica(client_id, key, local_disk_replica);
    ASSERT_TRUE(add_res.has_value());

    auto res = service->PutRevoke(client_id, key, ReplicaType::MEMORY);
    ASSERT_TRUE(res.has_value());

    OpLogEntry entry;
    EXPECT_EQ(ErrorCode::OK, mock_store->FindLatestEntryForKey(key, entry));
    EXPECT_EQ(OpType::PUT_END, entry.op_type);
    EXPECT_FALSE(entry.payload.empty());
}

// EvictDiskReplica(LOCAL_DISK) on an object with MEMORY + LOCAL_DISK publishes
// PUT_END with the MEMORY descriptor.
TEST_F(MasterServiceHATest, EvictDiskReplicaPublishesPutEndOpLog) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id("test_cluster")
                              .build();
    std::unique_ptr<MasterService> service(new MasterService(service_config));

    auto mock_store = std::make_shared<MockOpLogStore>();
    service->SetOpLogStoreForTesting(mock_store);

    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const UUID client_id = generate_uuid();
    const std::string key = "evict_disk_key";
    PutObject(*service, client_id, key);

    Replica local_disk_replica(client_id, 1024, "local_disk_endpoint",
                               ReplicaStatus::COMPLETE);
    auto add_res = service->AddReplica(client_id, key, local_disk_replica);
    ASSERT_TRUE(add_res.has_value());

    auto res =
        service->EvictDiskReplica(client_id, key, ReplicaType::LOCAL_DISK);
    ASSERT_TRUE(res.has_value());

    OpLogEntry entry;
    EXPECT_EQ(ErrorCode::OK, mock_store->FindLatestEntryForKey(key, entry));
    EXPECT_EQ(OpType::PUT_END, entry.op_type);
    EXPECT_FALSE(entry.payload.empty());
}

// BatchReplicaClear with empty segment_name clears all replicas and publishes
// REMOVE OpLog.
TEST_F(MasterServiceHATest, BatchReplicaClearAllPublishesRemoveOpLog) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id("test_cluster")
                              .build();
    std::unique_ptr<MasterService> service(new MasterService(service_config));

    auto mock_store = std::make_shared<MockOpLogStore>();
    service->SetOpLogStoreForTesting(mock_store);

    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const UUID client_id = generate_uuid();
    const std::string key = "batch_clear_all_key";
    PutObject(*service, client_id, key);

    std::this_thread::sleep_for(std::chrono::milliseconds(60));

    auto res = service->BatchReplicaClear({key}, client_id, "");
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(1u, res->size());
    EXPECT_EQ(key, (*res)[0]);

    OpLogEntry entry;
    EXPECT_EQ(ErrorCode::OK, mock_store->FindLatestEntryForKey(key, entry));
    EXPECT_EQ(OpType::REMOVE, entry.op_type);
}

// CopyEnd publishes PUT_END OpLog with the updated replica set.
TEST_F(MasterServiceHATest, CopyEndPublishesPutEndOpLog) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id("test_cluster")
                              .build();
    std::unique_ptr<MasterService> service(new MasterService(service_config));

    auto mock_store = std::make_shared<MockOpLogStore>();
    service->SetOpLogStoreForTesting(mock_store);

    PrepareSimpleSegment(*service, "seg1", kDefaultSegmentBase);
    const std::string key = "copy_end_key";
    const UUID client_id = generate_uuid();
    PutObjectOnSegment(*service, client_id, key, "seg1");

    PrepareSimpleSegment(*service, "seg2",
                         kDefaultSegmentBase + kDefaultSegmentSize);

    auto copy_start = service->CopyStart(client_id, key, "seg1", {"seg2"});
    ASSERT_TRUE(copy_start.has_value());

    auto res = service->CopyEnd(client_id, key);
    ASSERT_TRUE(res.has_value());

    OpLogEntry entry;
    EXPECT_EQ(ErrorCode::OK, mock_store->FindLatestEntryForKey(key, entry));
    EXPECT_EQ(OpType::PUT_END, entry.op_type);
    EXPECT_FALSE(entry.payload.empty());
}

// MoveEnd publishes PUT_END OpLog with the updated replica set (source
// removed).
TEST_F(MasterServiceHATest, MoveEndPublishesPutEndOpLog) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id("test_cluster")
                              .build();
    std::unique_ptr<MasterService> service(new MasterService(service_config));

    auto mock_store = std::make_shared<MockOpLogStore>();
    service->SetOpLogStoreForTesting(mock_store);

    PrepareSimpleSegment(*service, "seg1", kDefaultSegmentBase);
    const std::string key = "move_end_key";
    const UUID client_id = generate_uuid();
    PutObjectOnSegment(*service, client_id, key, "seg1");

    PrepareSimpleSegment(*service, "seg2",
                         kDefaultSegmentBase + kDefaultSegmentSize);

    auto move_start = service->MoveStart(client_id, key, "seg1", "seg2");
    ASSERT_TRUE(move_start.has_value());

    auto res = service->MoveEnd(client_id, key);
    ASSERT_TRUE(res.has_value());

    OpLogEntry entry;
    EXPECT_EQ(ErrorCode::OK, mock_store->FindLatestEntryForKey(key, entry));
    EXPECT_EQ(OpType::PUT_END, entry.op_type);
    EXPECT_FALSE(entry.payload.empty());
}

// BatchReplicaClear on one segment keeps replicas on other segments and
// publishes PUT_END.
TEST_F(MasterServiceHATest,
       BatchReplicaClearSegmentKeepsOtherSegmentsPublishesPutEndOpLog) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id("test_cluster")
                              .build();
    std::unique_ptr<MasterService> service(new MasterService(service_config));

    auto mock_store = std::make_shared<MockOpLogStore>();
    service->SetOpLogStoreForTesting(mock_store);

    PrepareSimpleSegment(*service, "seg1", kDefaultSegmentBase);
    const std::string key = "batch_clear_segment_key";
    const UUID client_id = generate_uuid();
    PutObjectOnSegment(*service, client_id, key, "seg1");

    PrepareSimpleSegment(*service, "seg2",
                         kDefaultSegmentBase + kDefaultSegmentSize);

    auto copy_start = service->CopyStart(client_id, key, "seg1", {"seg2"});
    ASSERT_TRUE(copy_start.has_value());

    auto copy_end = service->CopyEnd(client_id, key);
    ASSERT_TRUE(copy_end.has_value());

    std::this_thread::sleep_for(std::chrono::milliseconds(60));

    auto res = service->BatchReplicaClear({key}, client_id, "seg1");
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(1u, res->size());
    EXPECT_EQ(key, (*res)[0]);

    OpLogEntry entry;
    EXPECT_EQ(ErrorCode::OK, mock_store->FindLatestEntryForKey(key, entry));
    EXPECT_EQ(OpType::PUT_END, entry.op_type);
    EXPECT_FALSE(entry.payload.empty());
}

// ===== Step 1: persist-before-local-commit for replica mutations =====

// AddReplica must NOT add the LOCAL_DISK descriptor when OpLog persist fails.
TEST_F(MasterServiceHATest, AddReplicaPersistFailureSkipsLocalMutation) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id("test_cluster")
                              .build();
    std::unique_ptr<MasterService> service(new MasterService(service_config));

    auto mock_store = std::make_shared<MockOpLogStore>();
    service->SetOpLogStoreForTesting(mock_store);
    service->SetOpLogRetryConfigForTesting(2, 50);

    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const UUID client_id = generate_uuid();
    const std::string key = "add_replica_persist_fail_key";
    PutObject(*service, client_id, key);

    mock_store->SetWriteError(ErrorCode::PERSISTENT_FAIL);

    Replica local_disk_replica(client_id, 1024, "local_disk_endpoint",
                               ReplicaStatus::COMPLETE);
    auto add_res = service->AddReplica(client_id, key, local_disk_replica);
    EXPECT_FALSE(add_res.has_value())
        << "AddReplica must return error when OpLog persist fails";

    // The LOCAL_DISK descriptor must NOT be visible in metadata.
    auto list = service->GetReplicaList(key);
    ASSERT_TRUE(list.has_value());
    for (const auto& desc : list->replicas) {
        EXPECT_FALSE(desc.is_local_disk_replica())
            << "LOCAL_DISK replica must not be present after persist failure";
    }
}

// CopyEnd must NOT mark target replicas COMPLETE or erase the task when
// OpLog persist fails.
TEST_F(MasterServiceHATest, CopyEndPersistFailureSkipsLocalMutation) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id("test_cluster")
                              .build();
    std::unique_ptr<MasterService> service(new MasterService(service_config));

    auto mock_store = std::make_shared<MockOpLogStore>();
    service->SetOpLogStoreForTesting(mock_store);
    service->SetOpLogRetryConfigForTesting(2, 50);

    PrepareSimpleSegment(*service, "seg1", kDefaultSegmentBase);
    const std::string key = "copy_end_persist_fail_key";
    const UUID client_id = generate_uuid();
    PutObjectOnSegment(*service, client_id, key, "seg1");

    PrepareSimpleSegment(*service, "seg2",
                         kDefaultSegmentBase + kDefaultSegmentSize);

    auto copy_start = service->CopyStart(client_id, key, "seg1", {"seg2"});
    ASSERT_TRUE(copy_start.has_value());

    mock_store->SetWriteError(ErrorCode::PERSISTENT_FAIL);

    auto res = service->CopyEnd(client_id, key);
    EXPECT_FALSE(res.has_value())
        << "CopyEnd must return error when OpLog persist fails";

    // Target replicas must remain PROCESSING (not COMPLETE).
    auto list = service->GetReplicaList(key);
    ASSERT_TRUE(list.has_value());
    // Only the original COMPLETE memory replica should be in the published
    // descriptor list — target replicas are still PROCESSING and excluded.
    EXPECT_EQ(1u, list->replicas.size())
        << "Only the original source replica should be COMPLETE";

    // Retrying after restoring persist should succeed.
    mock_store->SetWriteError(ErrorCode::OK);
    auto retry = service->CopyEnd(client_id, key);
    EXPECT_TRUE(retry.has_value())
        << "CopyEnd retry should succeed after persist is restored";
}

// MoveEnd must NOT decrement source refcnt or mark target COMPLETE when
// OpLog persist fails.
TEST_F(MasterServiceHATest, MoveEndPersistFailureSkipsLocalMutation) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id("test_cluster")
                              .build();
    std::unique_ptr<MasterService> service(new MasterService(service_config));

    auto mock_store = std::make_shared<MockOpLogStore>();
    service->SetOpLogStoreForTesting(mock_store);
    service->SetOpLogRetryConfigForTesting(2, 50);

    PrepareSimpleSegment(*service, "seg1", kDefaultSegmentBase);
    const std::string key = "move_end_persist_fail_key";
    const UUID client_id = generate_uuid();
    PutObjectOnSegment(*service, client_id, key, "seg1");

    PrepareSimpleSegment(*service, "seg2",
                         kDefaultSegmentBase + kDefaultSegmentSize);

    auto move_start = service->MoveStart(client_id, key, "seg1", "seg2");
    ASSERT_TRUE(move_start.has_value());

    mock_store->SetWriteError(ErrorCode::PERSISTENT_FAIL);

    auto res = service->MoveEnd(client_id, key);
    EXPECT_FALSE(res.has_value())
        << "MoveEnd must return error when OpLog persist fails";

    // Source must still be COMPLETE and present (PopReplicas was not called),
    // and target must still be PROCESSING (mark_complete was not called).
    // The visible descriptor list contains only COMPLETE replicas; therefore
    // exactly the original source descriptor should appear.
    auto list = service->GetReplicaList(key);
    ASSERT_TRUE(list.has_value());
    EXPECT_EQ(1u, list->replicas.size())
        << "Source must still be present and target still PROCESSING";

    // Retrying after restoring persist should succeed.
    mock_store->SetWriteError(ErrorCode::OK);
    auto retry = service->MoveEnd(client_id, key);
    EXPECT_TRUE(retry.has_value())
        << "MoveEnd retry should succeed after persist is restored";
}

// PutRevoke must NOT mutate metadata when OpLog persist fails (also
// implicitly checks H.1 metric ordering: dec_mem_cache_nums must not run
// when persist fails). The PROCESSING replica should still be revokable
// after we restore persist.
TEST_F(MasterServiceHATest, PutRevokePersistFailureSkipsLocalMutation) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id("test_cluster")
                              .build();
    std::unique_ptr<MasterService> service(new MasterService(service_config));

    auto mock_store = std::make_shared<MockOpLogStore>();
    service->SetOpLogStoreForTesting(mock_store);
    service->SetOpLogRetryConfigForTesting(2, 50);

    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const UUID client_id = generate_uuid();
    const std::string key = "put_revoke_persist_fail_key";

    ReplicateConfig config;
    config.replica_num = 1;
    auto put_start = service->PutStart(client_id, key, 1024, config);
    ASSERT_TRUE(put_start.has_value());

    mock_store->SetWriteError(ErrorCode::PERSISTENT_FAIL);

    auto res = service->PutRevoke(client_id, key, ReplicaType::MEMORY);
    EXPECT_FALSE(res.has_value())
        << "PutRevoke must return error when OpLog persist fails";

    // Restore persist; PutRevoke must succeed because the PROCESSING replica
    // was NOT erased on the failed attempt.
    mock_store->SetWriteError(ErrorCode::OK);
    auto retry = service->PutRevoke(client_id, key, ReplicaType::MEMORY);
    EXPECT_TRUE(retry.has_value())
        << "PutRevoke retry must succeed because the PROCESSING replica "
           "was preserved after persist failure";
}

// ===== Step 2: strong-consistency eviction =====

// BatchEvict must NOT erase MEMORY replicas when OpLog persist fails.
TEST_F(MasterServiceHATest, BatchEvictPersistFailureSkipsMemReplicaErase) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id("test_cluster")
                              .build();
    std::unique_ptr<MasterService> service(new MasterService(service_config));

    auto mock_store = std::make_shared<MockOpLogStore>();
    service->SetOpLogStoreForTesting(mock_store);
    service->SetOpLogRetryConfigForTesting(2, 50);

    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const UUID client_id = generate_uuid();

    std::vector<std::string> keys;
    for (int i = 0; i < 5; ++i) {
        keys.push_back("evict_persist_fail_key_" + std::to_string(i));
        PutObject(*service, client_id, keys.back());
    }
    // Wait for leases to expire so they become evictable.
    std::this_thread::sleep_for(std::chrono::milliseconds(60));

    mock_store->SetWriteError(ErrorCode::PERSISTENT_FAIL);

    // Aggressive ratios so all eligible objects are picked.
    service->RunBatchEvictForTesting(/*evict_ratio_target=*/1.0,
                                     /*evict_ratio_lowerbound=*/1.0);

    // Memory replicas must remain because persist failed.
    for (const auto& key : keys) {
        auto list = service->GetReplicaList(key);
        ASSERT_TRUE(list.has_value())
            << "Key must still be retrievable: " << key;
        bool has_memory = false;
        for (const auto& desc : list->replicas) {
            if (desc.is_memory_replica()) {
                has_memory = true;
                break;
            }
        }
        EXPECT_TRUE(has_memory)
            << "Memory replica must remain after persist failure: " << key;
    }
}

// ===== Step 3: direct-erase + stale-cleanup + plan/apply =====

// PutStart against an existing PROCESSING-only key triggers the overwrite-
// REMOVE path once put_start_discard_timeout_sec elapses. With persist
// failing, the path must NOT erase the metadata or pop replicas; the next
// PutStart attempt (after the same timeout) must still see the old key.
TEST_F(MasterServiceHATest, PutStartOverwritePersistFailureSkipsErase) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id("test_cluster")
                              .set_put_start_discard_timeout_sec(1)
                              .set_put_start_release_timeout_sec(2)
                              .build();
    std::unique_ptr<MasterService> service(new MasterService(service_config));

    auto mock_store = std::make_shared<MockOpLogStore>();
    service->SetOpLogStoreForTesting(mock_store);
    service->SetOpLogRetryConfigForTesting(2, 50);

    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const UUID client_id = generate_uuid();
    const std::string key = "put_start_overwrite_persist_fail_key";

    // PutStart but never PutEnd — leaves a PROCESSING-only metadata.
    ReplicateConfig config;
    config.replica_num = 1;
    auto put_start = service->PutStart(client_id, key, 1024, config);
    ASSERT_TRUE(put_start.has_value());

    // Wait for put_start_discard_timeout to elapse.
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));

    mock_store->SetWriteError(ErrorCode::PERSISTENT_FAIL);

    // Second PutStart triggers the overwrite-REMOVE branch. With persist
    // failing it must return error (not silently drop the old metadata).
    const UUID client_id2 = generate_uuid();
    auto retry_start = service->PutStart(client_id2, key, 1024, config);
    EXPECT_FALSE(retry_start.has_value())
        << "PutStart overwrite must return error when REMOVE persist fails";

    // Restore persist; the overwrite must now succeed and the new
    // PutStart must allocate a fresh PROCESSING replica for client_id2.
    mock_store->SetWriteError(ErrorCode::OK);
    auto retry2 = service->PutStart(client_id2, key, 1024, config);
    EXPECT_TRUE(retry2.has_value())
        << "PutStart overwrite must succeed once persist is restored";
}

// DiscardExpiredProcessingReplicas (Part 1) drops PROCESSING replicas of
// PutStart-expired keys. When the key has been published to standby
// (had_complete_replica == true via an existing LOCAL_DISK replica),
// persist failure must NOT pop the PROCESSING memory replica locally.
TEST_F(MasterServiceHATest,
       DiscardExpiredProcessingReplicasPersistFailureSkipsPop) {
    auto service_config = MasterServiceConfig::builder()
                              .set_default_kv_lease_ttl(50)
                              .set_enable_ha(true)
                              .set_cluster_id("test_cluster")
                              .set_put_start_discard_timeout_sec(1)
                              .set_put_start_release_timeout_sec(2)
                              .build();
    std::unique_ptr<MasterService> service(new MasterService(service_config));

    auto mock_store = std::make_shared<MockOpLogStore>();
    service->SetOpLogStoreForTesting(mock_store);
    service->SetOpLogRetryConfigForTesting(2, 50);

    [[maybe_unused]] const auto context = PrepareSimpleSegment(*service);
    const UUID client_id = generate_uuid();
    const std::string key = "discard_expired_persist_fail_key";

    // PutStart but never PutEnd — leaves a PROCESSING memory replica.
    ReplicateConfig config;
    config.replica_num = 1;
    auto put_start = service->PutStart(client_id, key, 1024, config);
    ASSERT_TRUE(put_start.has_value());

    // AddReplica a COMPLETE LOCAL_DISK so the metadata has both a
    // PROCESSING memory replica AND a COMPLETE local-disk descriptor.
    // had_complete_replica will be true when the reaper runs.
    Replica local_disk_replica(client_id, 1024, "ld_endpoint",
                               ReplicaStatus::COMPLETE);
    auto add_res = service->AddReplica(client_id, key, local_disk_replica);
    ASSERT_TRUE(add_res.has_value());

    // Wait for put_start_release_timeout to elapse so Part 1 considers
    // the PROCESSING replica expired.
    std::this_thread::sleep_for(std::chrono::milliseconds(2100));

    mock_store->SetWriteError(ErrorCode::PERSISTENT_FAIL);

    // Drive an eviction cycle which calls DiscardExpiredProcessingReplicas
    // up-front. With persist failing, the PROCESSING memory replica must
    // NOT be popped from metadata.
    service->RunBatchEvictForTesting(/*evict_ratio_target=*/1.0,
                                     /*evict_ratio_lowerbound=*/1.0);

    // Restore persist and try PutEnd. If the reaper popped the
    // PROCESSING memory replica, PutEnd will be a no-op and
    // GetReplicaList will only see the LOCAL_DISK descriptor; if the
    // PROCESSING memory replica was preserved, PutEnd marks it COMPLETE
    // and GetReplicaList will return BOTH descriptors.
    mock_store->SetWriteError(ErrorCode::OK);
    auto end_res = service->PutEnd(client_id, key, ReplicaType::MEMORY);
    ASSERT_TRUE(end_res.has_value());

    auto after = service->GetReplicaList(key);
    ASSERT_TRUE(after.has_value());
    bool has_memory = false;
    bool has_local_disk = false;
    for (const auto& desc : after->replicas) {
        if (desc.is_memory_replica()) has_memory = true;
        if (desc.is_local_disk_replica()) has_local_disk = true;
    }
    EXPECT_TRUE(has_memory)
        << "Memory replica must still be present (and now COMPLETE) after "
           "PutEnd, proving the reaper preserved the PROCESSING replica on "
           "persist failure";
    EXPECT_TRUE(has_local_disk) << "LOCAL_DISK replica must still be present";
}

// ===== Step 4: SEGMENT_UNMOUNT retry-on-failure =====
//
// The F fix (UnmountSegment / MountSegment / ReMountSegment now use
// PersistSegmentOpForHAOrEnqueue: durable persist up-front, enqueue the
// same OpLogEntry on failure preserving its sequence_id) is verified
// by code review rather than a unit test. A direct test that drives
// UnmountSegment after MountSegment in this fixture reliably triggers
// "Resource deadlock avoided" inside the segment manager's lock graph
// when `enable_ha_=true` (etcd backend init failure path), even when
// glog init/shutdown is moved to SetUpTestSuite/TearDownTestSuite.
// The behaviour is independent of the F changes and pre-dates them; a
// proper test would need an integration harness that exercises the
// retry queue across primary/standby boundaries (covered by
// localfs_hot_standby_integration_test).

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

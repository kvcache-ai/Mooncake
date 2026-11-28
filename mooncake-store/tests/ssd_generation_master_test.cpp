#include "master_service_test_fixture.h"

namespace mooncake::test {
class SsdGenerationMasterTest : public MasterServiceTest {};

TEST_F(SsdGenerationMasterTest, RejectsUnrepresentableDurabilityWait) {
    auto config = MasterServiceConfig::builder().build();
    config.oplog_durable_wait_timeout_ms = UINT64_MAX;
    EXPECT_THROW(MasterService{config}, std::invalid_argument);
    config.oplog_durable_wait_timeout_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::duration::max() / 2)
            .count() +
        1;
    EXPECT_THROW(MasterService{config}, std::invalid_argument);
}

TEST_F(SsdGenerationMasterTest, DeletesCancelUndeliveredOffloadSources) {
    for (int mode = 0; mode < 4; ++mode) {
        SCOPED_TRACE(mode);
        auto config = MasterServiceConfig::builder()
                          .set_enable_offload(true)
                          .set_default_kv_lease_ttl(0)
                          .build();
        MasterService service(config);
        const auto owner = PrepareSimpleSegment(service);
        ASSERT_TRUE(service.MountLocalDiskSegment(owner.client_id, true));
        auto start = service.PutStart(owner.client_id, "queued",
                                      TenantId::Default(), 1024, {});
        ASSERT_TRUE(start);
        auto generation = GetPrimaryWriteGeneration(*start);
        ASSERT_TRUE(generation);
        ASSERT_TRUE(service.PutEnd(owner.client_id, {"queued", {}, *generation},
                                   TenantId::Default(), ReplicaType::MEMORY));
        EXPECT_EQ(MasterServiceTestPeer(service)
                      .InspectOffloadForTesting(TenantId::Default(), "queued")
                      .memory_refs,
                  1u);
        if (mode == 0) {
            ASSERT_TRUE(service.Remove("queued", TenantId::Default(), true));
        } else if (mode == 1) {
            auto removed =
                service.BatchRemove({"queued"}, TenantId::Default(), true);
            ASSERT_EQ(removed.size(), 1u);
            ASSERT_TRUE(removed.front());
        } else if (mode == 2) {
            auto removed =
                service.RemoveByRegex("queued", TenantId::Default(), true);
            ASSERT_TRUE(removed);
            EXPECT_EQ(*removed, 1);
        } else {
            EXPECT_EQ(service.RemoveAll(TenantId::Default(), true), 1);
        }
        EXPECT_FALSE(
            service.ExistKey("queued", TenantId::Default()).value_or(true));
        auto tasks = service.OffloadObjectHeartbeat(owner.client_id, true);
        ASSERT_TRUE(tasks);
        EXPECT_TRUE(tasks->empty());
    }
}

TEST_F(SsdGenerationMasterTest, DisablingOneMirrorRetainsOtherDeliveredSource) {
    auto config = MasterServiceConfig::builder()
                      .set_enable_offload(true)
                      .set_default_kv_lease_ttl(0)
                      .build();
    MasterService service(config);
    const auto first = PrepareSimpleSegment(service, "ssd-first");
    const auto second = PrepareSimpleSegment(
        service, "ssd-second", kDefaultSegmentBase + kDefaultSegmentSize);
    ASSERT_TRUE(service.MountLocalDiskSegment(first.client_id, true));
    ASSERT_TRUE(service.MountLocalDiskSegment(second.client_id, true));
    ReplicateConfig replication;
    replication.replica_num = 2;
    replication.preferred_segments = {"ssd-first", "ssd-second"};
    auto start = service.PutStart(first.client_id, "key", TenantId::Default(),
                                  1024, replication);
    ASSERT_TRUE(start);
    auto generation = GetPrimaryWriteGeneration(*start);
    ASSERT_TRUE(generation);
    ASSERT_TRUE(service.PutEnd(first.client_id, {"key", {}, *generation},
                               TenantId::Default(), ReplicaType::MEMORY));
    auto delivered = service.OffloadObjectHeartbeat(second.client_id, true);
    ASSERT_TRUE(delivered);
    ASSERT_EQ(delivered->size(), 1u);
    auto disabled = service.OffloadObjectHeartbeat(first.client_id, false);
    ASSERT_TRUE(disabled);
    EXPECT_TRUE(disabled->empty());
    auto inspection = MasterServiceTestPeer(service).InspectOffloadForTesting(
        TenantId::Default(), "key");
    EXPECT_EQ(inspection.task_count, 1u);
    EXPECT_EQ(inspection.memory_refs, 1u);
    MasterServiceTestPeer(service).DiscardExpiredProcessingReplicas(
        TenantId::Default(),
        std::chrono::system_clock::now() + std::chrono::hours(24));
    EXPECT_EQ(MasterServiceTestPeer(service)
                  .InspectOffloadForTesting(TenantId::Default(), "key")
                  .memory_refs,
              1u);
    auto remove = service.Remove("key", TenantId::Default(), true);
    ASSERT_FALSE(remove);
    EXPECT_EQ(remove.error(), ErrorCode::OBJECT_HAS_REPLICATION_TASK);
    auto batch_remove = service.BatchRemove({"key"}, TenantId::Default(), true);
    ASSERT_EQ(batch_remove.size(), 1u);
    EXPECT_FALSE(batch_remove.front());
    auto regex_remove = service.RemoveByRegex("key", TenantId::Default(), true);
    ASSERT_TRUE(regex_remove);
    EXPECT_EQ(*regex_remove, 0);
    EXPECT_EQ(service.RemoveAll(TenantId::Default(), true), 0);
    auto stale = delivered->front();
    stale.task_id = stale.task_id.value_or("") + "-old";
    StorageObjectMetadata failure{-1, 0, 0, -1, ""};
    ASSERT_TRUE(
        service.NotifyOffloadSuccess(second.client_id, {stale}, {failure}));
    EXPECT_EQ(MasterServiceTestPeer(service)
                  .InspectOffloadForTesting(TenantId::Default(), "key")
                  .memory_refs,
              1u);
    ASSERT_TRUE(
        service.NotifyOffloadSuccess(second.client_id, *delivered, {failure}));
    inspection = MasterServiceTestPeer(service).InspectOffloadForTesting(
        TenantId::Default(), "key");
    EXPECT_EQ(inspection.task_count, 0u);
    EXPECT_EQ(inspection.memory_refs, 0u);
    EXPECT_TRUE(service.Remove("key", TenantId::Default(), true));
}

TEST_F(SsdGenerationMasterTest, SameWriterStaleEndCannotCompleteReplacement) {
    MasterService service;
    const auto owner = PrepareSimpleSegment(service);
    auto old =
        service.PutStart(owner.client_id, "key", TenantId::Default(), 1024, {});
    ASSERT_TRUE(old);
    auto previous = GetPrimaryWriteGeneration(*old);
    ASSERT_TRUE(previous);
    auto replacement = service.UpsertStart(owner.client_id, "key",
                                           TenantId::Default(), 1024, {});
    ASSERT_TRUE(replacement);
    auto current = GetPrimaryWriteGeneration(*replacement);
    ASSERT_TRUE(current);
    ASSERT_NE(*previous, *current);
    auto stale = service.PutEnd(owner.client_id, {"key", {}, *previous},
                                TenantId::Default(), ReplicaType::MEMORY);
    ASSERT_FALSE(stale);
    EXPECT_EQ(stale.error(), ErrorCode::INVALID_VERSION);
    EXPECT_TRUE(service.UpsertEnd(owner.client_id, {"key", {}, *current},
                                  TenantId::Default(), ReplicaType::MEMORY));
}
}  // namespace mooncake::test

#include "master_service.h"
#include "master_service/master_service_test_peer.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "types.h"

namespace mooncake::test {

class OffloadOnEvictTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("OffloadOnEvictTest");
        FLAGS_logtostderr = true;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }

    static constexpr size_t kDefaultSegmentBase = 0x300000000;

    Segment MakeSegment(std::string name, size_t base, size_t size) const {
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

    MountedSegmentContext PrepareSegment(MasterService& service,
                                         std::string name, size_t base,
                                         size_t size) const {
        Segment segment = MakeSegment(std::move(name), base, size);
        UUID client_id = generate_uuid();
        auto mount_result = service.MountSegment(segment, client_id);
        EXPECT_TRUE(mount_result.has_value());
        return {.segment_id = segment.id, .client_id = client_id};
    }

    // Put an object and complete it.
    void PutObject(MasterService& service, const UUID& client_id,
                   const std::string& key, size_t size = 1024) {
        ReplicateConfig config;
        config.replica_num = 1;
        auto put_start =
            service.PutStart(client_id, key, TenantId::Default(), size, config);
        ASSERT_TRUE(put_start.has_value()) << "PutStart failed for key=" << key;
        auto put_end = service.PutEnd(client_id, key, TenantId::Default(),
                                      ReplicaType::MEMORY);
        ASSERT_TRUE(put_end.has_value()) << "PutEnd failed for key=" << key;
    }

    // Drain the offload queue via OffloadObjectHeartbeat.
    std::unordered_map<std::string, int64_t> DrainOffloadQueue(
        MasterService& service, const UUID& client_id) {
        auto res = service.OffloadObjectHeartbeat(client_id, true);
        if (!res) {
            return {};
        }
        std::unordered_map<std::string, int64_t> queued;
        for (const auto& task : res.value()) {
            queued[task.key] = task.size;
        }
        return queued;
    }

    std::unique_ptr<MasterService> MakeEvictingService() const {
        MasterServiceConfig config;
        config.enable_offload = true;
        config.offload_on_evict = true;
        config.default_kv_lease_ttl = kLeaseTtlMs;
        return std::make_unique<MasterService>(config);
    }

    std::vector<OffloadTaskItem> TakeOffloadsQueuedByEviction(
        MasterService& service, const UUID& client_id) {
        std::this_thread::sleep_for(std::chrono::milliseconds(kLeaseTtlMs * 2));
        MasterServiceTestPeer(service).RunBatchEvictForTesting(1.0, 1.0);
        auto taken = service.OffloadObjectHeartbeat(client_id, true);
        EXPECT_TRUE(taken.has_value());
        return taken ? std::move(taken.value())
                     : std::vector<OffloadTaskItem>{};
    }

    std::vector<OffloadTaskItem> PutAndTakeOffloads(MasterService& service,
                                                    const UUID& client_id) {
        for (int i = 0; i < kObjects; ++i) {
            PutObject(service, client_id, "in_flight_" + std::to_string(i),
                      kObjectSize);
        }
        EXPECT_TRUE(DrainOffloadQueue(service, client_id).empty());
        auto tasks = TakeOffloadsQueuedByEviction(service, client_id);
        EXPECT_EQ(tasks.size(), static_cast<size_t>(kObjects));
        return tasks;
    }

    static bool HasLocalDiskReplica(MasterService& service,
                                    const std::string& key) {
        auto replicas =
            service.GetReplicaListForAdmin(key, TenantId::Default());
        return replicas.has_value() &&
               std::any_of(replicas->replicas.begin(), replicas->replicas.end(),
                           [](const auto& replica) {
                               return replica.is_local_disk_replica();
                           });
    }

    static std::vector<StorageObjectMetadata> DiskMetadatas(
        const std::vector<OffloadTaskItem>& tasks) {
        std::vector<StorageObjectMetadata> metadatas;
        for (const auto& task : tasks) {
            StorageObjectMetadata metadata;
            metadata.data_size = task.size;
            metadata.transport_endpoint = "holder:1";
            metadatas.push_back(metadata);
        }
        return metadatas;
    }

    static constexpr int kObjects = 8;
    static constexpr uint64_t kLeaseTtlMs = 500;
    static constexpr size_t kSegmentSize = 1024 * 1024 * 16;
    static constexpr size_t kObjectSize = 1024 * 1024;
};

// =============================================================================
// UpsertStart interaction with an outstanding offload task.
//
// The object's offloading task passes through two observable states:
//   QUEUED    - mirror entry still present in offloading_objects; the store
//               worker has not observed the task. UpsertStart cancels the
//               task in place and allocates a fresh replica.
//   IN-FLIGHT - mirror already drained by OffloadObjectHeartbeat; the worker
//               is reading the source buffer for SSD write. UpsertStart
//               returns OBJECT_HAS_REPLICATION_TASK; the caller retries
//               after NotifyOffloadSuccess clears the marker.
// =============================================================================

TEST_F(OffloadOnEvictTest, UpsertPreemptsQueuedOffloadWithOffloadOnEvict) {
    // With offload_on_evict the task is queued by eviction rather than by
    // PutEnd.
    const uint64_t kv_lease_ttl = 500;
    MasterServiceConfig config;
    config.enable_offload = true;
    config.offload_on_evict = true;
    config.default_kv_lease_ttl = kv_lease_ttl;
    auto service = std::make_unique<MasterService>(config);

    constexpr size_t seg_size = 1024 * 1024 * 16;
    constexpr size_t object_size = 1024 * 1024;
    auto ctx =
        PrepareSegment(*service, "test_segment", kDefaultSegmentBase, seg_size);
    ASSERT_TRUE(service->MountLocalDiskSegment(ctx.client_id, true));

    std::vector<std::string> keys;
    for (int i = 0; i < 8; ++i) {
        keys.push_back("evict_upsert_" + std::to_string(i));
        PutObject(*service, ctx.client_id, keys.back(), object_size);
    }

    // PutEnd does not queue offloads in this mode; eviction does.
    auto queued = DrainOffloadQueue(*service, ctx.client_id);
    ASSERT_TRUE(queued.empty())
        << "offload_on_evict: PutEnd must not queue offloads";

    // Let leases expire so the keys are evictable, then run one eviction
    // cycle to queue their offloads without draining the mirrors.
    std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl * 2));
    MasterServiceTestPeer(*service).RunBatchEvictForTesting(1.0, 1.0);

    std::string offloading_key;
    for (const auto& k : keys) {
        auto upsert =
            service->UpsertStart(ctx.client_id, k, TenantId::Default(),
                                 object_size, ReplicateConfig{});
        if (upsert.has_value()) {
            offloading_key = k;
            EXPECT_EQ(upsert->size(), 1u);
            EXPECT_EQ(upsert->at(0).status, ReplicaStatus::PROCESSING);
            ASSERT_TRUE(service
                            ->PutEnd(ctx.client_id, k, TenantId::Default(),
                                     ReplicaType::MEMORY)
                            .has_value());
            break;
        }
    }
    ASSERT_FALSE(offloading_key.empty())
        << "expected UpsertStart to preempt an offload queued by eviction";

    // Preempt cleared the marker and the mirror, so no stale entry is handed
    // to the store worker for the preempted key.
    auto stale = DrainOffloadQueue(*service, ctx.client_id);
    EXPECT_EQ(stale.count(offloading_key), 0u)
        << "preempted key must not remain queued";

    service->RemoveAll();
}

TEST_F(OffloadOnEvictTest, RemoveDuringInFlightOffloadStaysRemoved) {
    auto service = MakeEvictingService();
    auto ctx = PrepareSegment(*service, "test_segment", kDefaultSegmentBase,
                              kSegmentSize);
    ASSERT_TRUE(service->MountLocalDiskSegment(ctx.client_id, true));
    auto tasks = PutAndTakeOffloads(*service, ctx.client_id);
    ASSERT_FALSE(tasks.empty());

    for (const auto& task : tasks) {
        ASSERT_TRUE(service->Remove(task.key, TenantId::Default(), true));
    }
    for (int delivery = 0; delivery < 2; ++delivery) {
        ASSERT_TRUE(service->NotifyOffloadSuccess(ctx.client_id, tasks,
                                                  DiskMetadatas(tasks)));
        for (const auto& task : tasks) {
            auto exists = service->ExistKey(task.key, TenantId::Default());
            ASSERT_TRUE(exists.has_value());
            EXPECT_FALSE(exists.value()) << task.key;
        }
    }

    service->RemoveAll();
}

TEST_F(OffloadOnEvictTest, StaleOffloadCompletionSkipsRecreatedObject) {
    auto service = MakeEvictingService();
    auto ctx = PrepareSegment(*service, "test_segment", kDefaultSegmentBase,
                              kSegmentSize);
    ASSERT_TRUE(service->MountLocalDiskSegment(ctx.client_id, true));
    auto tasks = PutAndTakeOffloads(*service, ctx.client_id);
    ASSERT_FALSE(tasks.empty());
    std::vector<OffloadTaskItem> stale{tasks.front()};
    const std::string& key = stale[0].key;

    ASSERT_TRUE(service->Remove(key, TenantId::Default(), true));
    PutObject(*service, ctx.client_id, key, kObjectSize);
    ASSERT_TRUE(service->NotifyOffloadSuccess(ctx.client_id, stale,
                                              DiskMetadatas(stale)));
    EXPECT_FALSE(HasLocalDiskReplica(*service, key));

    auto fresh = TakeOffloadsQueuedByEviction(*service, ctx.client_id);
    ASSERT_EQ(fresh.size(), 1u);
    ASSERT_EQ(fresh[0].key, key);
    ASSERT_TRUE(service->NotifyOffloadSuccess(ctx.client_id, fresh,
                                              DiskMetadatas(fresh)));
    EXPECT_TRUE(HasLocalDiskReplica(*service, key));

    service->RemoveAll();
}

TEST_F(OffloadOnEvictTest, StaleOffloadNackKeepsRecreatedObjectsMarker) {
    auto service = MakeEvictingService();
    auto ctx = PrepareSegment(*service, "test_segment", kDefaultSegmentBase,
                              kSegmentSize);
    ASSERT_TRUE(service->MountLocalDiskSegment(ctx.client_id, true));
    auto tasks = PutAndTakeOffloads(*service, ctx.client_id);
    ASSERT_FALSE(tasks.empty());
    std::vector<OffloadTaskItem> stale{tasks.front()};
    const std::string key = stale[0].key;

    ASSERT_TRUE(service->Remove(key, TenantId::Default(), true));
    PutObject(*service, ctx.client_id, key, kObjectSize);
    std::this_thread::sleep_for(std::chrono::milliseconds(kLeaseTtlMs * 2));
    MasterServiceTestPeer(*service).RunBatchEvictForTesting(1.0, 1.0);
    stale[0].size = -1;
    ASSERT_TRUE(service->NotifyOffloadSuccess(ctx.client_id, stale,
                                              DiskMetadatas(stale)));

    auto fresh = service->OffloadObjectHeartbeat(ctx.client_id, true);
    ASSERT_TRUE(fresh.has_value());
    ASSERT_EQ(fresh->size(), 1u);
    ASSERT_EQ(fresh->front().key, key);
    EXPECT_FALSE(service->UpsertStart(ctx.client_id, key, TenantId::Default(),
                                      kObjectSize, ReplicateConfig{}));

    service->RemoveAll();
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

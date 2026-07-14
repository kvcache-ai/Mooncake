#include "master_service.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "types.h"

namespace mooncake::test {

class DirectSsdTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("DirectSsdTest");
        FLAGS_logtostderr = true;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }

    static constexpr size_t kDefaultSegmentBase = 0x300000000;

    Segment MakeSegment(const std::string& name, size_t base,
                        size_t size) const {
        Segment seg;
        seg.id = generate_uuid();
        seg.name = name;
        seg.base = base;
        seg.size = size;
        seg.te_endpoint = name;
        return seg;
    }

    struct MountedContext {
        UUID segment_id;
        UUID client_id;
    };

    MountedContext PrepareMemorySegment(MasterService& service,
                                        const std::string& name, size_t base,
                                        size_t size) const {
        auto seg = MakeSegment(name, base, size);
        UUID cid = generate_uuid();
        EXPECT_TRUE(service.MountSegment(seg, cid).has_value());
        return {seg.id, cid};
    }

    UUID PrepareLocalDiskSegment(
        MasterService& service, int64_t total_capacity,
        const std::string& rpc_endpoint = "10.0.0.1:50052") {
        UUID cid = generate_uuid();
        EXPECT_TRUE(
            service.MountLocalDiskSegment(cid, true, rpc_endpoint).has_value());
        EXPECT_TRUE(service.ReportSsdCapacity(cid, total_capacity).has_value());
        return cid;
    }

    // Put via memory path (normal offload flow, for comparison).
    void PutObjectMemory(MasterService& service, const UUID& client_id,
                         const std::string& key, size_t size = 1024) {
        ReplicateConfig cfg;
        cfg.replica_num = 1;
        auto start = service.PutStart(client_id, key, "default", size, cfg);
        ASSERT_TRUE(start.has_value()) << key;
        auto end =
            service.PutEnd(client_id, key, "default", ReplicaType::MEMORY);
        ASSERT_TRUE(end.has_value()) << key;
    }

    // Put via direct_ssd path. Returns allocated replicas.
    std::vector<Replica::Descriptor> PutDirectSsd(MasterService& service,
                                                  const UUID& client_id,
                                                  const std::string& key,
                                                  size_t size,
                                                  size_t replica_num = 1) {
        ReplicateConfig cfg;
        cfg.replica_num = replica_num;
        cfg.direct_ssd = true;
        auto start = service.PutStart(client_id, key, "default", size, cfg);
        if (!start.has_value()) {
            LOG(ERROR) << "PutStart failed for key=" << key
                       << ", error=" << toString(start.error());
        }
        EXPECT_TRUE(start.has_value()) << key;
        if (!start.has_value()) return {};
        return start.value();
    }

    // Drain offload queue.
    std::vector<OffloadTaskItem> DrainOffloadQueue(MasterService& service,
                                                   const UUID& client_id) {
        auto res = service.OffloadObjectHeartbeat(client_id, true);
        if (!res.has_value()) return {};
        return res.value();
    }
};

// =========================================================================
// 1. Basic direct_ssd Put: allocate LOCAL_DISK, no MEMORY
// =========================================================================

TEST_F(DirectSsdTest, PutDirectSsdAllocatesLocalDiskReplica) {
    auto service = std::make_unique<MasterService>(
        MasterServiceConfig::builder().set_enable_offload(true).build());
    auto mem_ctx = PrepareMemorySegment(*service, "mem_seg",
                                        kDefaultSegmentBase, 16 * 1024 * 1024);
    // Use same client_id for memory segment and LocalDiskSegment so the
    // offload queue push works correctly (same as existing test pattern).
    auto ssd_client = PrepareLocalDiskSegment(*service, 10ULL * 1024 * 1024);

    // Phase A (MEMORY) skipped. Phase C allocates LOCAL_DISK.
    auto replicas = PutDirectSsd(*service, mem_ctx.client_id, "key1", 1024, 1);
    ASSERT_GE(replicas.size(), 1u);
    EXPECT_TRUE(replicas[0].is_local_disk_replica());
    EXPECT_FALSE(replicas[0].is_memory_replica());

    // PutEnd marks COMPLETE.
    auto end = service->PutEnd(mem_ctx.client_id, "key1", "default",
                               ReplicaType::LOCAL_DISK);
    ASSERT_TRUE(end.has_value());

    // Offload queue must be empty — direct_ssd skips the offload queue.
    auto queue = DrainOffloadQueue(*service, ssd_client);
    EXPECT_TRUE(queue.empty());
}

// =========================================================================
// 2. Multiple LOCAL_DISK replicas
// =========================================================================

TEST_F(DirectSsdTest, PutDirectSsdMultipleReplicas) {
    auto service = std::make_unique<MasterService>(
        MasterServiceConfig::builder().set_enable_offload(true).build());
    auto mem_ctx = PrepareMemorySegment(*service, "mem_seg",
                                        kDefaultSegmentBase, 16 * 1024 * 1024);

    auto c1 = PrepareLocalDiskSegment(*service, 1000, "10.0.0.1:50052");
    auto c2 = PrepareLocalDiskSegment(*service, 2000, "10.0.0.2:50052");
    auto c3 = PrepareLocalDiskSegment(*service, 500, "10.0.0.3:50052");

    auto replicas =
        PutDirectSsd(*service, mem_ctx.client_id, "key_multi", 1024, 3);
    EXPECT_EQ(replicas.size(), 3u);
    for (const auto& r : replicas) {
        EXPECT_TRUE(r.is_local_disk_replica());
    }

    auto end = service->PutEnd(mem_ctx.client_id, "key_multi", "default",
                               ReplicaType::LOCAL_DISK);
    ASSERT_TRUE(end.has_value());
}

// =========================================================================
// 3. Phase C caps at available SSD count (no duplicate clients)
// =========================================================================

TEST_F(DirectSsdTest, PhaseCCapsAtAvailableSsdCount) {
    auto service = std::make_unique<MasterService>(
        MasterServiceConfig::builder().set_enable_offload(true).build());
    auto mem_ctx = PrepareMemorySegment(*service, "mem_seg",
                                        kDefaultSegmentBase, 16 * 1024 * 1024);

    // Only 1 SSD but request 3 replicas → should get 1.
    PrepareLocalDiskSegment(*service, 1000, "10.0.0.1:50052");
    auto replicas =
        PutDirectSsd(*service, mem_ctx.client_id, "key_cap", 100, 3);
    EXPECT_EQ(replicas.size(), 1u);
}

// =========================================================================
// 4. Fails cleanly when no SSD clients registered
// =========================================================================

TEST_F(DirectSsdTest, FailsWithoutSsdClients) {
    auto service = std::make_unique<MasterService>(
        MasterServiceConfig::builder().set_enable_offload(true).build());
    auto mem_ctx = PrepareMemorySegment(*service, "mem_seg",
                                        kDefaultSegmentBase, 16 * 1024 * 1024);

    ReplicateConfig cfg;
    cfg.replica_num = 1;
    cfg.direct_ssd = true;
    auto start =
        service->PutStart(mem_ctx.client_id, "key_none", "default", 1024, cfg);
    EXPECT_FALSE(start.has_value());
    EXPECT_EQ(start.error(), ErrorCode::NO_AVAILABLE_HANDLE);
}

// =========================================================================
// 5. direct_ssd PutEnd skips offload queue; normal Put enters it
// =========================================================================

TEST_F(DirectSsdTest, DirectSsdSkipsOffloadQueue) {
    auto service = std::make_unique<MasterService>(
        MasterServiceConfig::builder().set_enable_offload(true).build());
    auto mem_ctx = PrepareMemorySegment(*service, "mem_seg",
                                        kDefaultSegmentBase, 16 * 1024 * 1024);
    // Mount LocalDiskSegment under the SAME client_id as the memory segment.
    // This is how existing tests (offload_on_evict_test) structure it:
    // PushOffloadingQueue needs the memory segment's owner to also have
    // a LocalDiskSegment for the offload task to land.
    ASSERT_TRUE(
        service->MountLocalDiskSegment(mem_ctx.client_id, true).has_value());
    ASSERT_TRUE(
        service->ReportSsdCapacity(mem_ctx.client_id, 10000).has_value());

    // direct_ssd write.
    auto reps = PutDirectSsd(*service, mem_ctx.client_id, "key_ds", 1024, 1);
    ASSERT_GE(reps.size(), 1u);
    ASSERT_TRUE(service
                    ->PutEnd(mem_ctx.client_id, "key_ds", "default",
                             ReplicaType::LOCAL_DISK)
                    .has_value());

    // Normal (non-direct_ssd) write.
    PutObjectMemory(*service, mem_ctx.client_id, "key_mem", 1024);

    // Offload queue should only contain key_mem (key_ds was direct_ssd).
    auto queue = DrainOffloadQueue(*service, mem_ctx.client_id);
    EXPECT_EQ(queue.size(), 1u);
    if (!queue.empty()) {
        EXPECT_EQ(queue[0].key, "key_mem");
    }
}

// =========================================================================
// 6. PutRevoke cleans up PROCESSING LOCAL_DISK without error
// =========================================================================

TEST_F(DirectSsdTest, PutRevokeCleansUpLocalDisk) {
    auto service = std::make_unique<MasterService>(
        MasterServiceConfig::builder().set_enable_offload(true).build());
    auto mem_ctx = PrepareMemorySegment(*service, "mem_seg",
                                        kDefaultSegmentBase, 16 * 1024 * 1024);
    PrepareLocalDiskSegment(*service, 10000, "10.0.0.1:50052");

    auto reps = PutDirectSsd(*service, mem_ctx.client_id, "key_rv", 1024, 1);
    ASSERT_GE(reps.size(), 1u);

    // Revoke instead of PutEnd.
    auto revoke = service->PutRevoke(mem_ctx.client_id, "key_rv", "default",
                                     ReplicaType::ALL);
    EXPECT_TRUE(revoke.has_value());
}

// =========================================================================
// 7. Transport endpoint correctly stored in replica descriptor
// =========================================================================

TEST_F(DirectSsdTest, TransportEndpointInReplica) {
    auto service = std::make_unique<MasterService>(
        MasterServiceConfig::builder().set_enable_offload(true).build());
    auto mem_ctx = PrepareMemorySegment(*service, "mem_seg",
                                        kDefaultSegmentBase, 16 * 1024 * 1024);
    const std::string rpc_ep = "192.168.1.100:50052";
    auto ssd_client = PrepareLocalDiskSegment(*service, 10000, rpc_ep);

    auto replicas =
        PutDirectSsd(*service, mem_ctx.client_id, "key_ep", 1024, 1);
    ASSERT_GE(replicas.size(), 1u);
    auto desc = replicas[0].get_local_disk_descriptor();
    EXPECT_EQ(desc.client_id, ssd_client);
    EXPECT_EQ(desc.transport_endpoint, rpc_ep);
}

}  // namespace mooncake::test

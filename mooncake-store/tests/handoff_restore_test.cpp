// Tests for the shutdown handoff snapshot: writer, restore with real
// allocators, remount identity check, quarantine.

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <string>
#include <thread>
#include <vector>

#include "crc32c.h"
#include "ha/snapshot/batch_oplog/codec.h"
#include "ha/snapshot/batch_oplog/metadata.h"
#include "handoff/handoff_format.h"
#include "handoff/handoff_writer.h"
#include "master_service.h"
#include "tenant/quota_policy_store.h"

namespace mooncake::test {
namespace {

namespace fs = std::filesystem;

constexpr uint64_t kSegmentSize = 256ULL * 1024 * 1024;
constexpr uint64_t kObjectSize = 64 * 1024;

MasterServiceConfig MakeConfig(
    int64_t live_ttl_sec = 3600, int64_t suspicion_ttl_sec = 3600,
    BufferAllocatorType allocator = BufferAllocatorType::OFFSET) {
    return MasterServiceConfig::builder()
        .set_memory_allocator(allocator)
        .set_eviction_ratio(0.0)
        .set_eviction_high_watermark_ratio(1.0)
        .set_client_live_ttl_sec(live_ttl_sec)
        .set_client_suspicion_ttl_sec(suspicion_ttl_sec)
        .build();
}

MasterServiceConfig MakeL4Config(int64_t live_ttl_sec = 3600,
                                 int64_t suspicion_ttl_sec = 3600) {
    return MasterServiceConfig::builder()
        .set_memory_allocator(BufferAllocatorType::OFFSET)
        .set_eviction_ratio(0.0)
        .set_eviction_high_watermark_ratio(1.0)
        .set_client_live_ttl_sec(live_ttl_sec)
        .set_client_suspicion_ttl_sec(suspicion_ttl_sec)
        .set_enable_offload(true)
        .build();
}

Segment MakeSegment(int index) {
    Segment segment;
    segment.id = generate_uuid();
    segment.name = "10.0.0." + std::to_string(10 + index) + ":15651";
    segment.base = 0x100000000000ULL + index * 0x10000000000ULL;
    segment.size = kSegmentSize;
    segment.te_endpoint = segment.name;
    segment.protocol = "rdma";
    segment.host_id = "host-" + std::to_string(index);
    return segment;
}

std::string Key(int segment, int i) {
    return "key-" + std::to_string(segment) + "-" + std::to_string(i);
}

class TempDir {
   public:
    TempDir() {
        std::string pattern =
            (fs::temp_directory_path() / "handoff-test-XXXXXX").string();
        path_ = mkdtemp(pattern.data());
    }
    ~TempDir() {
        std::error_code ec;
        fs::remove_all(path_, ec);
    }
    const std::string& path() const { return path_; }

   private:
    std::string path_;
};

// A store process as seen by the master: its client id and segments.
struct Store {
    UUID client_id;
    std::vector<Segment> segments;
};

class HandoffRestoreTest : public ::testing::Test {
   protected:
    // Mounts `num_stores` stores with one segment each and puts
    // `objects_per_store` objects on each.
    void Populate(MasterService& service, int num_stores,
                  int objects_per_store) {
        for (int s = 0; s < num_stores; ++s) {
            Store store{generate_uuid(), {MakeSegment(s)}};
            ASSERT_TRUE(service.MountSegment(store.segments[0], store.client_id)
                            .has_value());
            stores_.push_back(store);
            for (int i = 0; i < objects_per_store; ++i) {
                Put(service, store, Key(s, i));
            }
        }
    }

    Replica::Descriptor Put(MasterService& service, const Store& store,
                            const std::string& key, bool end = true) {
        ReplicateConfig config;
        config.replica_num = 1;
        config.preferred_segment = store.segments[0].name;
        auto started = service.PutStart(
            store.client_id, key, TenantId::Default(), kObjectSize, config);
        EXPECT_TRUE(started.has_value()) << toString(started.error());
        if (!started) return {};
        if (end) {
            EXPECT_TRUE(service
                            .PutEnd(store.client_id, key, TenantId::Default(),
                                    ReplicaType::MEMORY)
                            .has_value());
        }
        return started->front();
    }

    HandoffWriteResult Write(MasterService& service, const std::string& dir) {
        HandoffWriteOptions options;
        options.dir = dir;
        options.workers = 3;
        options.durable = false;
        auto written = WriteHandoffSnapshot(service, options);
        EXPECT_TRUE(written.has_value()) << written.error();
        return written ? *written : HandoffWriteResult{};
    }

    static bool Readable(MasterService& service, const std::string& key) {
        return service.GetReplicaList(key, TenantId::Default()).has_value();
    }

    static uint64_t Used(MasterService& service, const Segment& segment) {
        auto q = service.QuerySegments(segment.name);
        return q ? q->first : UINT64_MAX;
    }

    static bool Remount(MasterService& service, const Store& store) {
        return service.ReMountSegment(store.segments, store.client_id)
            .has_value();
    }

    // Registers the store's LocalSSD (L4) tier with the master.
    static void EnableL4(MasterService& service, const Store& store,
                         int64_t capacity = 1LL << 30) {
        ASSERT_TRUE(
            service.MountLocalDiskSegment(store.client_id, true).has_value());
        ASSERT_TRUE(
            service.ReportSsdCapacity(store.client_id, capacity).has_value());
    }

    static StorageObjectMetadata DiskMeta(const std::string& endpoint,
                                          int64_t size = kObjectSize) {
        StorageObjectMetadata meta;
        meta.data_size = size;
        meta.transport_endpoint = endpoint;
        return meta;
    }

    // Takes up to `count` queued offload tasks of the store and reports them
    // done (task-backed completions), as FileStorage::Heartbeat does.
    static std::vector<std::string> Offload(MasterService& service,
                                            const Store& store,
                                            const std::string& endpoint,
                                            size_t count) {
        auto tasks = service.OffloadObjectHeartbeat(store.client_id, true);
        EXPECT_TRUE(tasks.has_value());
        if (!tasks) return {};
        std::vector<OffloadTaskItem> done(
            tasks->begin(), tasks->begin() + std::min(count, tasks->size()));
        std::vector<StorageObjectMetadata> metas(done.size(),
                                                 DiskMeta(endpoint));
        EXPECT_TRUE(service.NotifyOffloadSuccess(store.client_id, done, metas)
                        .has_value());
        std::vector<std::string> keys;
        for (const auto& task : done) keys.push_back(task.key);
        return keys;
    }

    // Disk-only objects of `store`: put, offloaded (task-backed completion),
    // then the memory copy dropped.
    void PutDiskOnly(MasterService& service, const Store& store,
                     const std::vector<std::string>& keys,
                     const std::string& endpoint) {
        for (const auto& key : keys) Put(service, store, key);
        auto done = Offload(service, store, endpoint, keys.size());
        ASSERT_EQ(done.size(), keys.size());
        auto cleared = service.BatchReplicaClear(keys, store.client_id,
                                                 store.segments[0].name);
        ASSERT_TRUE(cleared.has_value());
        ASSERT_EQ(cleared->size(), keys.size());
    }

    // A registration without an offload task (the rescan of a store older
    // than the L4 fix).
    static bool Register(MasterService& service, const UUID& client_id,
                         const std::string& key, const std::string& endpoint,
                         int64_t size = kObjectSize) {
        std::vector<OffloadTaskItem> tasks{
            OffloadTaskItem{.tenant_id = "default", .key = key, .size = size}};
        return service
            .NotifyOffloadSuccess(client_id, tasks, {DiskMeta(endpoint, size)})
            .has_value();
    }

    static std::vector<Replica::Descriptor> Replicas(MasterService& service,
                                                     const std::string& key) {
        auto r = service.GetReplicaList(key, TenantId::Default());
        return r ? r->replicas : std::vector<Replica::Descriptor>{};
    }

    static bool HasDisk(MasterService& service, const std::string& key,
                        const UUID& owner) {
        for (const auto& d : Replicas(service, key)) {
            if (d.is_local_disk_replica() &&
                d.get_local_disk_descriptor().client_id == owner) {
                return true;
            }
        }
        return false;
    }

    // Old master with two stores: store 0 has L4 (20 offloaded objects, 10
    // with a queued offload, 5 disk-only), store 1 is memory only (10).
    void WriteL4Snapshot(const std::string& dir,
                         HandoffWriteResult* out = nullptr) {
        MasterService old_master(MakeL4Config());
        for (int s = 0; s < 2; ++s) {
            Store store{generate_uuid(), {MakeSegment(s)}};
            ASSERT_TRUE(
                old_master.MountSegment(store.segments[0], store.client_id)
                    .has_value());
            stores_.push_back(store);
        }
        EnableL4(old_master, stores_[0]);
        for (int i = 0; i < 30; ++i) Put(old_master, stores_[0], Key(0, i));
        for (int i = 0; i < 10; ++i) Put(old_master, stores_[1], Key(1, i));
        // The heartbeat takes all 30 tasks; 20 complete, 10 are in flight.
        offloaded_ = Offload(old_master, stores_[0], "disk-0", 20);
        ASSERT_EQ(offloaded_.size(), 20u);
        std::vector<std::string> disk_only;
        for (int i = 0; i < 5; ++i) {
            disk_only.push_back("disk-only-" + std::to_string(i));
        }
        PutDiskOnly(old_master, stores_[0], disk_only, "disk-0");
        auto written = Write(old_master, dir);
        if (out) *out = written;
    }

    std::vector<Store> stores_;
    std::vector<std::string> offloaded_;
};

TEST_F(HandoffRestoreTest, RestoredObjectsAreGatedUntilOwnerRemounts) {
    TempDir dir;
    std::map<std::string, Replica::Descriptor> before;
    std::vector<uint64_t> used_before;
    {
        MasterService old_master(MakeConfig());
        Populate(old_master, 3, 200);
        for (int s = 0; s < 3; ++s) {
            used_before.push_back(Used(old_master, stores_[s].segments[0]));
            for (int i = 0; i < 200; ++i) {
                auto r =
                    old_master.GetReplicaList(Key(s, i), TenantId::Default());
                ASSERT_TRUE(r.has_value());
                before[Key(s, i)] = r->replicas.front();
            }
        }
        auto written = Write(old_master, dir.path());
        EXPECT_EQ(written.stats.objects, 600u);
    }

    MasterService master(MakeConfig());
    auto restored = master.RestoreFromHandoff(dir.path(), {4});
    ASSERT_TRUE(restored.has_value()) << restored.error();
    EXPECT_EQ(restored->objects, 600u);
    EXPECT_EQ(master.GetKeyCount(), 600u);
    EXPECT_EQ(master.HandoffPendingSegmentCount(), 3u);
    for (int s = 0; s < 3; ++s) {
        EXPECT_EQ(Used(master, stores_[s].segments[0]), used_before[s]);
    }

    // Gated: unreadable, and no segment accepts allocations.
    auto gated = master.GetReplicaList(Key(0, 0), TenantId::Default());
    ASSERT_FALSE(gated.has_value());
    EXPECT_EQ(gated.error(), ErrorCode::REPLICA_IS_NOT_READY);
    ReplicateConfig config;
    config.replica_num = 1;
    EXPECT_FALSE(master
                     .PutStart(stores_[0].client_id, "new", TenantId::Default(),
                               kObjectSize, config)
                     .has_value());

    // Ping of a restored owner is accepted and asks for a remount.
    auto ping = master.Ping(stores_[0].client_id);
    ASSERT_TRUE(ping.has_value());
    EXPECT_EQ(ping->client_status, ClientStatus::NEED_REMOUNT);

    ASSERT_TRUE(Remount(master, stores_[0]));
    EXPECT_EQ(master.HandoffSegmentsConfirmed(), 1u);
    for (int i = 0; i < 200; ++i) {
        auto r = master.GetReplicaList(Key(0, i), TenantId::Default());
        ASSERT_TRUE(r.has_value()) << toString(r.error());
        const auto& got =
            r->replicas.front().get_memory_descriptor().buffer_descriptor;
        const auto& want =
            before[Key(0, i)].get_memory_descriptor().buffer_descriptor;
        EXPECT_EQ(got.buffer_address_, want.buffer_address_);
        EXPECT_EQ(got.size_, want.size_);
        EXPECT_EQ(got.transport_endpoint_, want.transport_endpoint_);
        EXPECT_EQ(got.protocol_, want.protocol_);
    }
    // Other stores are still gated.
    EXPECT_FALSE(Readable(master, Key(1, 0)));

    // New allocations on the confirmed segment avoid every restored range.
    auto fresh = Put(master, stores_[0], "new");
    const auto& fb = fresh.get_memory_descriptor().buffer_descriptor;
    for (int i = 0; i < 200; ++i) {
        const auto& b =
            before[Key(0, i)].get_memory_descriptor().buffer_descriptor;
        EXPECT_TRUE(fb.buffer_address_ + fb.size_ <= b.buffer_address_ ||
                    b.buffer_address_ + b.size_ <= fb.buffer_address_);
    }
    // Replica ids continue above the old master's.
    for (const auto& [key, desc] : before) {
        EXPECT_GT(fresh.id, desc.id) << key;
    }

    ASSERT_TRUE(Remount(master, stores_[1]));
    ASSERT_TRUE(Remount(master, stores_[2]));
    EXPECT_EQ(master.HandoffPendingSegmentCount(), 0u);
    EXPECT_TRUE(Readable(master, Key(2, 199)));
}

TEST_F(HandoffRestoreTest, InFlightPutIsExcludedAndItsRangeQuarantined) {
    TempDir dir;
    Replica::Descriptor in_flight;
    uint64_t used_with_in_flight = 0;
    {
        MasterService old_master(MakeConfig());
        Populate(old_master, 1, 50);
        in_flight = Put(old_master, stores_[0], "in-flight", /*end=*/false);
        used_with_in_flight = Used(old_master, stores_[0].segments[0]);
        auto written = Write(old_master, dir.path());
        EXPECT_EQ(written.stats.objects, 50u);
        EXPECT_EQ(written.stats.skipped_processing, 1u);
        EXPECT_EQ(written.stats.quarantine_ranges, 1u);
    }
    MasterService master(MakeConfig());
    ASSERT_TRUE(master.RestoreFromHandoff(dir.path(), {2}).has_value());
    EXPECT_FALSE(
        master.GetReplicaList("in-flight", TenantId::Default()).has_value());
    EXPECT_EQ(master.GetKeyCount(), 50u);
    EXPECT_EQ(master.HandoffQuarantineBufferCount(), 1u);
    // The quarantined range is still allocated.
    EXPECT_EQ(Used(master, stores_[0].segments[0]), used_with_in_flight);
    ASSERT_TRUE(Remount(master, stores_[0]));

    // Fill the segment: no allocation may land on the quarantined range.
    const auto& q = in_flight.get_memory_descriptor().buffer_descriptor;
    for (int i = 0; i < 1000; ++i) {
        ReplicateConfig config;
        config.replica_num = 1;
        auto r =
            master.PutStart(stores_[0].client_id, "fill-" + std::to_string(i),
                            TenantId::Default(), kObjectSize, config);
        if (!r) break;
        const auto& b = r->front().get_memory_descriptor().buffer_descriptor;
        ASSERT_TRUE(b.buffer_address_ + b.size_ <= q.buffer_address_ ||
                    q.buffer_address_ + q.size_ <= b.buffer_address_);
    }
    // Not yet expired: nothing released.
    master.ReleaseExpiredHandoffQuarantine(std::chrono::steady_clock::now());
    EXPECT_EQ(master.HandoffQuarantineBufferCount(), 1u);
    master.ReleaseExpiredHandoffQuarantine(std::chrono::steady_clock::now() +
                                           std::chrono::hours(1));
    EXPECT_EQ(master.HandoffQuarantineBufferCount(), 0u);
}

TEST_F(HandoffRestoreTest, LeaseExpiredObjectsAreKept) {
    TempDir dir;
    {
        MasterService old_master(MakeConfig());
        Populate(old_master, 1, 20);
        // Leases granted by reads have long expired by the time of a roll.
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        Write(old_master, dir.path());
    }
    MasterService master(MakeConfig());
    ASSERT_TRUE(master.RestoreFromHandoff(dir.path(), {1}).has_value());
    EXPECT_EQ(master.GetKeyCount(), 20u);
}

TEST_F(HandoffRestoreTest, RestartedStoreDropsItsRestoredSegment) {
    TempDir dir;
    {
        MasterService old_master(MakeConfig());
        Populate(old_master, 2, 100);
        Write(old_master, dir.path());
    }
    MasterService master(MakeConfig());
    ASSERT_TRUE(master.RestoreFromHandoff(dir.path(), {2}).has_value());

    // Store 0 restarted during the roll: new process, new client id, new
    // segment id, same endpoint, same base.
    Store restarted{generate_uuid(), {stores_[0].segments[0]}};
    restarted.segments[0].id = generate_uuid();
    ASSERT_TRUE(master.MountSegment(restarted.segments[0], restarted.client_id)
                    .has_value());
    EXPECT_EQ(master.HandoffSegmentsDropped(), 1u);
    EXPECT_FALSE(Readable(master, Key(0, 0)));

    // The new process's own objects are readable at once (the endpoint gate
    // belonged to the dropped segment).
    Put(master, restarted, "fresh");
    EXPECT_TRUE(Readable(master, "fresh"));

    // Store 1 is unaffected.
    ASSERT_TRUE(Remount(master, stores_[1]));
    EXPECT_TRUE(Readable(master, Key(1, 99)));

    // The dropped objects are erased by the cleanup worker.
    for (int i = 0; i < 100 && master.GetKeyCount() != 101; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    EXPECT_EQ(master.GetKeyCount(), 101u);
}

TEST_F(HandoffRestoreTest, OwnerRemountWithChangedIdentityDrops) {
    TempDir dir;
    {
        MasterService old_master(MakeConfig());
        Populate(old_master, 1, 30);
        // Second segment of the same store.
        stores_[0].segments.push_back(MakeSegment(1));
        stores_[0].segments[1].name = stores_[0].segments[0].name;
        stores_[0].segments[1].te_endpoint = stores_[0].segments[0].te_endpoint;
        ASSERT_TRUE(
            old_master
                .MountSegment(stores_[0].segments[1], stores_[0].client_id)
                .has_value());
        Write(old_master, dir.path());
    }
    {
        // Same id, different size: drop, then the new identity mounts fresh.
        MasterService master(MakeConfig());
        ASSERT_TRUE(master.RestoreFromHandoff(dir.path(), {1}).has_value());
        Store changed = stores_[0];
        changed.segments[0].size = kSegmentSize / 2;
        ASSERT_TRUE(Remount(master, changed));
        EXPECT_EQ(master.HandoffSegmentsDropped(), 1u);
        EXPECT_EQ(master.HandoffSegmentsConfirmed(), 1u);
        EXPECT_FALSE(Readable(master, Key(0, 0)));
    }
    {
        // The owner no longer lists its first segment: drop it, keep the
        // listed one.
        MasterService master(MakeConfig());
        ASSERT_TRUE(master.RestoreFromHandoff(dir.path(), {1}).has_value());
        Store subset{stores_[0].client_id, {stores_[0].segments[1]}};
        ASSERT_TRUE(Remount(master, subset));
        EXPECT_EQ(master.HandoffSegmentsDropped(), 1u);
        EXPECT_EQ(master.HandoffPendingSegmentCount(), 0u);
        EXPECT_FALSE(Readable(master, Key(0, 0)));
    }
}

TEST_F(HandoffRestoreTest, SegmentLocalityIsCarriedToTheOwnersRemount) {
    TempDir dir;
    {
        MasterService old_master(MakeConfig());
        Store store{generate_uuid(), {MakeSegment(0)}};
        store.segments[0].locality =
            SegmentLocality{.numa_node = 1, .hca_names = {"mlx5_2", "mlx5_3"}};
        ASSERT_TRUE(old_master.MountSegment(store.segments[0], store.client_id)
                        .has_value());
        stores_.push_back(store);
        for (int i = 0; i < 20; ++i) Put(old_master, store, Key(0, i));
        Write(old_master, dir.path());
    }
    {
        // The owner presents the segment it mounted, locality included: the
        // restored segment opens.
        MasterService master(MakeConfig());
        ASSERT_TRUE(master.RestoreFromHandoff(dir.path(), {2}).has_value());
        ASSERT_TRUE(Remount(master, stores_[0]));
        EXPECT_EQ(master.HandoffSegmentsDropped(), 0u);
        EXPECT_EQ(master.HandoffSegmentsConfirmed(), 1u);
        EXPECT_TRUE(Readable(master, Key(0, 19)));
    }
    {
        // Another locality is another segment: drop.
        MasterService master(MakeConfig());
        ASSERT_TRUE(master.RestoreFromHandoff(dir.path(), {2}).has_value());
        Store changed = stores_[0];
        changed.segments[0].locality->numa_node = 0;
        ASSERT_TRUE(Remount(master, changed));
        EXPECT_EQ(master.HandoffSegmentsDropped(), 1u);
        EXPECT_FALSE(Readable(master, Key(0, 0)));
    }
}

TEST_F(HandoffRestoreTest, OwnerThatNeverReturnsExpires) {
    TempDir dir;
    {
        MasterService old_master(MakeConfig());
        Populate(old_master, 2, 40);
        Write(old_master, dir.path());
    }
    MasterService master(MakeConfig(/*live=*/1, /*suspicion=*/1));
    ASSERT_TRUE(master.RestoreFromHandoff(dir.path(), {2}).has_value());
    ASSERT_TRUE(Remount(master, stores_[1]));
    // Store 1 keeps pinging; store 0 never comes back.
    for (int i = 0; i < 80 && master.GetKeyCount() != 40; ++i) {
        (void)master.Ping(stores_[1].client_id);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    EXPECT_EQ(master.GetKeyCount(), 40u);
    EXPECT_TRUE(Readable(master, Key(1, 39)));
    EXPECT_FALSE(Readable(master, Key(0, 0)));
}

TEST_F(HandoffRestoreTest, RemoveWhileGatedFreesTheRange) {
    TempDir dir;
    {
        MasterService old_master(MakeConfig());
        Populate(old_master, 1, 10);
        Write(old_master, dir.path());
    }
    MasterService master(MakeConfig());
    ASSERT_TRUE(master.RestoreFromHandoff(dir.path(), {1}).has_value());
    const uint64_t used = Used(master, stores_[0].segments[0]);
    ASSERT_TRUE(
        master.Remove(Key(0, 0), TenantId::Default(), true).has_value());
    EXPECT_LT(Used(master, stores_[0].segments[0]), used);
    ASSERT_TRUE(Remount(master, stores_[0]));
    EXPECT_EQ(master.GetKeyCount(), 9u);
}

TEST_F(HandoffRestoreTest, DiskReplicasOfUnregisteredOwnersAreDropped) {
    TempDir dir;
    {
        MasterService old_master(MakeConfig());
        Populate(old_master, 1, 2);
        // Key(0,0) gains a LOCAL_DISK replica; "disk-only" has only one.
        Replica disk_copy(stores_[0].client_id, kObjectSize, "disk-endpoint",
                          ReplicaStatus::COMPLETE);
        ASSERT_TRUE(old_master
                        .AddReplica(stores_[0].client_id, Key(0, 0),
                                    TenantId::Default(), disk_copy)
                        .has_value());
        Replica disk_only(stores_[0].client_id, kObjectSize, "disk-endpoint",
                          ReplicaStatus::COMPLETE);
        ASSERT_TRUE(old_master
                        .AddReplica(stores_[0].client_id, "disk-only",
                                    TenantId::Default(), disk_only)
                        .has_value());
        auto written = Write(old_master, dir.path());
        // Offload is off, so the owner has no LocalSSD registration: its L4
        // state is not exported (it would re-register on the new master).
        EXPECT_EQ(written.stats.objects, 2u);
        EXPECT_EQ(written.stats.dropped_local_disk_replicas, 2u);
        EXPECT_EQ(written.stats.local_disk_replicas, 0u);
        EXPECT_EQ(written.stats.skipped_no_exportable_replica, 1u);
    }
    MasterService master(MakeConfig());
    ASSERT_TRUE(master.RestoreFromHandoff(dir.path(), {1}).has_value());
    ASSERT_TRUE(Remount(master, stores_[0]));
    auto r = master.GetReplicaList(Key(0, 0), TenantId::Default());
    ASSERT_TRUE(r.has_value());
    ASSERT_EQ(r->replicas.size(), 1u);
    EXPECT_TRUE(r->replicas.front().is_memory_replica());
    EXPECT_FALSE(
        master.GetReplicaList("disk-only", TenantId::Default()).has_value());
}

TEST_F(HandoffRestoreTest, UnmountedSegmentIsNotExported) {
    TempDir dir;
    {
        MasterService old_master(MakeConfig());
        Populate(old_master, 2, 10);
        ASSERT_TRUE(
            old_master
                .UnmountSegment(stores_[0].segments[0].id, stores_[0].client_id)
                .has_value());
        auto written = Write(old_master, dir.path());
        EXPECT_EQ(written.stats.quarantine_ranges, 0u);
    }
    MasterService master(MakeConfig());
    auto restored = master.RestoreFromHandoff(dir.path(), {1});
    ASSERT_TRUE(restored.has_value()) << restored.error();
    EXPECT_EQ(restored->segments, 1u);
    EXPECT_EQ(master.GetKeyCount(), 10u);
}

// ---- Corrupt or incompatible snapshots: cold start, never a crash.

// ---- L4 (LOCAL_DISK) state.

TEST_F(HandoffRestoreTest, L4StateIsRestoredAndGatedUntilTheOwnerShowsUp) {
    TempDir dir;
    HandoffWriteResult written;
    WriteL4Snapshot(dir.path(), &written);
    EXPECT_EQ(written.stats.objects, 45u);
    EXPECT_EQ(written.stats.local_disk_replicas, 25u);
    EXPECT_EQ(written.stats.disk_only_objects, 5u);
    EXPECT_EQ(written.stats.local_disk_clients, 1u);

    MasterService master(MakeL4Config());
    auto restored = master.RestoreFromHandoff(dir.path(), {3});
    ASSERT_TRUE(restored.has_value()) << restored.error();
    EXPECT_EQ(restored->local_disk_clients, 1u);
    EXPECT_EQ(restored->local_disk_replicas, 25u);
    EXPECT_EQ(restored->disk_only_objects, 5u);
    EXPECT_EQ(master.GetKeyCount(), 45u);
    EXPECT_EQ(master.HandoffLocalDiskObjectCount(), 25u);
    EXPECT_EQ(master.HandoffDiskClientsPending(), 1u);

    // Gated: neither the disk-only object nor the disk copy is readable.
    auto gated = master.GetReplicaList("disk-only-0", TenantId::Default());
    ASSERT_FALSE(gated.has_value());
    EXPECT_EQ(gated.error(), ErrorCode::REPLICA_IS_NOT_READY);

    // Another store's RPCs do not open store 0's gate. Store 1 had no L4
    // registration: its heartbeat gets SEGMENT_NOT_FOUND (register anew and
    // drop its files, as after a cold start).
    ASSERT_TRUE(Remount(master, stores_[1]));
    auto other = master.OffloadObjectHeartbeat(stores_[1].client_id, true);
    ASSERT_FALSE(other.has_value());
    EXPECT_EQ(other.error(), ErrorCode::SEGMENT_NOT_FOUND);
    EXPECT_FALSE(Readable(master, "disk-only-0"));

    // Store 0's next offload heartbeat succeeds (so FileStorage keeps its
    // files) and returns no task: the old queue is not carried over.
    auto heartbeat = master.OffloadObjectHeartbeat(stores_[0].client_id, true);
    ASSERT_TRUE(heartbeat.has_value()) << toString(heartbeat.error());
    EXPECT_TRUE(heartbeat->empty());
    EXPECT_EQ(master.HandoffDiskClientsPending(), 0u);
    auto pending_remove_all = master.PollRemoveAll(stores_[0].client_id);
    ASSERT_TRUE(pending_remove_all.has_value());
    EXPECT_FALSE(*pending_remove_all);

    // Disk-only objects are readable from the owner's disk.
    auto disk_only = Replicas(master, "disk-only-0");
    ASSERT_EQ(disk_only.size(), 1u);
    ASSERT_TRUE(disk_only[0].is_local_disk_replica());
    const auto& disk = disk_only[0].get_local_disk_descriptor();
    EXPECT_EQ(disk.client_id, stores_[0].client_id);
    EXPECT_EQ(disk.object_size, kObjectSize);
    EXPECT_EQ(disk.transport_endpoint, "disk-0");
    // An offloaded object: only its disk copy until the memory segment is
    // confirmed, then both.
    const std::string offloaded = offloaded_.front();
    std::string in_flight;
    for (int i = 0; i < 30 && in_flight.empty(); ++i) {
        if (std::find(offloaded_.begin(), offloaded_.end(), Key(0, i)) ==
            offloaded_.end()) {
            in_flight = Key(0, i);
        }
    }
    auto before_remount = Replicas(master, offloaded);
    ASSERT_EQ(before_remount.size(), 1u);
    EXPECT_TRUE(before_remount[0].is_local_disk_replica());
    ASSERT_TRUE(Remount(master, stores_[0]));
    EXPECT_EQ(Replicas(master, offloaded).size(), 2u);
    // Objects whose offload was in flight: memory only.
    EXPECT_EQ(Replicas(master, in_flight).size(), 1u);

    // New offload work flows normally after the restore.
    Put(master, stores_[0], "after");
    EXPECT_EQ(Offload(master, stores_[0], "disk-0", 10).size(), 1u);
    EXPECT_TRUE(HasDisk(master, "after", stores_[0].client_id));
}

TEST_F(HandoffRestoreTest, L4GateOpensOnPing) {
    TempDir dir;
    WriteL4Snapshot(dir.path());
    MasterService master(MakeL4Config());
    ASSERT_TRUE(master.RestoreFromHandoff(dir.path(), {2}).has_value());
    EXPECT_FALSE(Readable(master, "disk-only-1"));
    ASSERT_TRUE(master.Ping(stores_[0].client_id).has_value());
    EXPECT_TRUE(Readable(master, "disk-only-1"));
}

TEST_F(HandoffRestoreTest, L4OfStoreRestartedDuringTheRollIsNeverTrusted) {
    TempDir dir;
    WriteL4Snapshot(dir.path());
    MasterService master(MakeL4Config());
    ASSERT_TRUE(master.RestoreFromHandoff(dir.path(), {2}).has_value());

    // New process at the same endpoint: the restored segment is dropped and
    // so is the old process's L4 registration.
    Store restarted{generate_uuid(), {stores_[0].segments[0]}};
    restarted.segments[0].id = generate_uuid();
    ASSERT_TRUE(master.MountSegment(restarted.segments[0], restarted.client_id)
                    .has_value());
    auto old_heartbeat =
        master.OffloadObjectHeartbeat(stores_[0].client_id, true);
    ASSERT_FALSE(old_heartbeat.has_value());
    EXPECT_EQ(old_heartbeat.error(), ErrorCode::SEGMENT_NOT_FOUND);
    // The old entries stay unreadable, whatever the new process does.
    EXPECT_FALSE(Readable(master, "disk-only-0"));
    EXPECT_FALSE(HasDisk(master, offloaded_.front(), stores_[0].client_id));

    // The new process registers its LocalSSD tier and re-registers its disk
    // keys as after a cold start. A re-registration (no offload task taken)
    // never registers a disk replica: neither a new object nor one attached
    // to the restored object whose disk replica belonged to the old process,
    // which stays unreadable until the TTL offboarding erases it.
    EnableL4(master, restarted);
    ASSERT_TRUE(Register(master, restarted.client_id, "fresh-disk", "disk-0b"));
    EXPECT_FALSE(Readable(master, "fresh-disk"));
    ASSERT_TRUE(
        Register(master, restarted.client_id, "disk-only-1", "disk-0b"));
    EXPECT_FALSE(HasDisk(master, "disk-only-1", restarted.client_id));
    EXPECT_FALSE(Readable(master, "disk-only-1"));
    EXPECT_FALSE(Readable(master, "disk-only-2"));
}

TEST_F(HandoffRestoreTest, L4OfOwnerThatNeverReturnsExpires) {
    TempDir dir;
    WriteL4Snapshot(dir.path());
    MasterService master(MakeL4Config(/*live=*/1, /*suspicion=*/1));
    ASSERT_TRUE(master.RestoreFromHandoff(dir.path(), {2}).has_value());
    ASSERT_TRUE(Remount(master, stores_[1]));
    for (int i = 0; i < 80 && master.GetKeyCount() != 10; ++i) {
        (void)master.Ping(stores_[1].client_id);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    EXPECT_EQ(master.GetKeyCount(), 10u);
    EXPECT_EQ(master.HandoffLocalDiskObjectCount(), 0u);
    EXPECT_FALSE(
        master.OffloadObjectHeartbeat(stores_[0].client_id, true).has_value());
}

TEST_F(HandoffRestoreTest, PendingRemoveAllIsCarried) {
    TempDir dir;
    {
        MasterService old_master(MakeL4Config());
        Store store{generate_uuid(), {MakeSegment(0)}};
        ASSERT_TRUE(old_master.MountSegment(store.segments[0], store.client_id)
                        .has_value());
        stores_.push_back(store);
        EnableL4(old_master, store);
        Put(old_master, store, Key(0, 0));
        ASSERT_EQ(Offload(old_master, store, "disk-0", 1).size(), 1u);
        Put(old_master, store, Key(0, 1));
        EXPECT_GT(old_master.RemoveAll(/*force=*/true), 0);
        Put(old_master, store, Key(0, 2));
        Write(old_master, dir.path());
    }
    MasterService master(MakeL4Config());
    ASSERT_TRUE(master.RestoreFromHandoff(dir.path(), {1}).has_value());
    auto poll = master.PollRemoveAll(stores_[0].client_id);
    ASSERT_TRUE(poll.has_value());
    EXPECT_TRUE(*poll);
}

TEST_F(HandoffRestoreTest, L4SnapshotNeedsOffloadOnTheNewMaster) {
    TempDir dir;
    WriteL4Snapshot(dir.path());
    MasterService master(MakeConfig());  // --enable_offload=false
    auto restored = master.RestoreFromHandoff(dir.path(), {2});
    ASSERT_FALSE(restored.has_value());
    EXPECT_NE(restored.error().find("offload is disabled"), std::string::npos)
        << restored.error();
}

TEST_F(HandoffRestoreTest, L4ColdStartOnBadL4State) {
    auto expect_cold = [this](auto&& corrupt, const std::string& reason) {
        TempDir dir;
        stores_.clear();
        WriteL4Snapshot(dir.path());
        const std::string path =
            dir.path() + "/" + handoff::kHandoffManifestFile;
        auto bytes = handoff::ReadFileBytes(path);
        ASSERT_TRUE(bytes.has_value());
        auto manifest = handoff::DecodeManifest(std::string_view(
            reinterpret_cast<const char*>(bytes->data()), bytes->size()));
        ASSERT_TRUE(manifest.has_value());
        corrupt(dir.path(), *manifest);
        std::ofstream(path, std::ios::trunc)
            << handoff::EncodeManifest(*manifest);
        MasterService master(MakeL4Config());
        auto restored = master.RestoreFromHandoff(dir.path(), {2});
        ASSERT_FALSE(restored.has_value());
        EXPECT_NE(restored.error().find(reason), std::string::npos)
            << restored.error();
    };
    // Local disk blob corrupted.
    expect_cold(
        [](const std::string& dir, handoff::Manifest& m) {
            std::fstream f(dir + "/" + m.local_disk->key,
                           std::ios::in | std::ios::out | std::ios::binary);
            f.seekp(static_cast<std::streamoff>(m.local_disk->bytes - 1));
            f.put('\x7f');
        },
        "crc mismatch");
    // An L4 client that owns no restored memory segment.
    expect_cold(
        [](const std::string& dir, handoff::Manifest& m) {
            std::vector<LocalSsdHandoffClient> clients{
                {generate_uuid(), true, 1, false}};
            auto blob = handoff::EncodeLocalDiskClients(clients);
            std::ofstream(dir + "/" + m.local_disk->key,
                          std::ios::trunc | std::ios::binary)
                .write(reinterpret_cast<const char*>(blob.data()),
                       static_cast<std::streamsize>(blob.size()));
            m.local_disk->bytes = blob.size();
            m.local_disk->crc32c = Crc32cValue(blob.data(), blob.size());
        },
        "invalid local disk client entry");
    // LOCAL_DISK replicas without any L4 owner.
    expect_cold(
        [](const std::string& dir, handoff::Manifest& m) {
            auto blob = handoff::EncodeLocalDiskClients({});
            std::ofstream(dir + "/" + m.local_disk->key,
                          std::ios::trunc | std::ios::binary)
                .write(reinterpret_cast<const char*>(blob.data()),
                       static_cast<std::streamsize>(blob.size()));
            m.local_disk->bytes = blob.size();
            m.local_disk->crc32c = Crc32cValue(blob.data(), blob.size());
            m.local_disk_clients = 0;
        },
        "without owners");
    // Disk replica count in the manifest does not match the chunks.
    expect_cold([](const std::string&,
                   handoff::Manifest& m) { m.local_disk_replicas += 1; },
                "count mismatch");
}

TEST_F(HandoffRestoreTest, SizeMismatchedDiskReplicaIsNotExported) {
    TempDir dir;
    {
        MasterService old_master(MakeL4Config());
        Store store{generate_uuid(), {MakeSegment(0)}};
        ASSERT_TRUE(old_master.MountSegment(store.segments[0], store.client_id)
                        .has_value());
        stores_.push_back(store);
        Put(old_master, store, Key(0, 0));
        EnableL4(old_master, store);
        // A disk replica whose size does not match the object (e.g. a stale
        // file attached by an older master).
        Replica stale(store.client_id, kObjectSize / 2, "disk-0",
                      ReplicaStatus::COMPLETE);
        ASSERT_TRUE(old_master
                        .AddReplica(store.client_id, Key(0, 0),
                                    TenantId::Default(), stale)
                        .has_value());
        auto written = Write(old_master, dir.path());
        EXPECT_EQ(written.stats.dropped_local_disk_replicas, 1u);
        EXPECT_EQ(written.stats.local_disk_replicas, 0u);
    }
    MasterService master(MakeL4Config());
    ASSERT_TRUE(master.RestoreFromHandoff(dir.path(), {1}).has_value());
    ASSERT_TRUE(Remount(master, stores_[0]));
    EXPECT_FALSE(HasDisk(master, Key(0, 0), stores_[0].client_id));
}

TEST_F(HandoffRestoreTest, RollCarriesNoOffloadStateAndRefusesOldReports) {
    // No offload state crosses the roll. A report of an offload the store
    // took from the old master can still reach the new master; it names a
    // task of the old master (its epoch), so the new master refuses it, also
    // when the store has meanwhile taken the new master's task of the same
    // key. The store takes the new master's tasks right away.
    TempDir dir;
    std::map<std::string, OffloadTaskItem> old_items;
    uint64_t old_epoch = 0;
    {
        MasterService old_master(MakeL4Config());
        old_epoch = old_master.L4TaskEpoch();
        Store store{generate_uuid(), {MakeSegment(0)}};
        ASSERT_TRUE(old_master.MountSegment(store.segments[0], store.client_id)
                        .has_value());
        stores_.push_back(store);
        EnableL4(old_master, store);
        for (int i = 0; i < 4; ++i) Put(old_master, store, Key(0, i));
        // The store takes all four: key 0 completes, key 1 is NACKed (its
        // timed-out completion may still be processed later), keys 2 and 3
        // are still being copied when the old master stops.
        auto tasks = old_master.OffloadObjectHeartbeat(store.client_id, true);
        ASSERT_TRUE(tasks.has_value());
        ASSERT_EQ(tasks->size(), 4u);
        for (const auto& task : *tasks) old_items.emplace(task.key, task);
        ASSERT_TRUE(old_master
                        .NotifyOffloadSuccess(store.client_id,
                                              {old_items.at(Key(0, 0))},
                                              {DiskMeta("disk-0")})
                        .has_value());
        StorageObjectMetadata nack;
        nack.data_size = -1;
        ASSERT_TRUE(old_master
                        .NotifyOffloadSuccess(store.client_id,
                                              {old_items.at(Key(0, 1))}, {nack})
                        .has_value());
        Write(old_master, dir.path());
    }

    MasterService master(MakeL4Config());
    ASSERT_NE(master.L4TaskEpoch(), old_epoch);
    auto restored = master.RestoreFromHandoff(dir.path(), {1});
    ASSERT_TRUE(restored.has_value()) << restored.error();
    ASSERT_TRUE(Remount(master, stores_[0]));
    EXPECT_TRUE(HasDisk(master, Key(0, 0), stores_[0].client_id));

    auto late = [&](int i) {
        ASSERT_TRUE(master
                        .NotifyOffloadSuccess(stores_[0].client_id,
                                              {old_items.at(Key(0, i))},
                                              {DiskMeta("disk-0")})
                        .has_value());
    };
    // Late reports of the old takes: the objects exist, without a task.
    for (int i = 1; i < 4; ++i) late(i);
    for (int i = 1; i < 4; ++i) {
        EXPECT_FALSE(HasDisk(master, Key(0, i), stores_[0].client_id)) << i;
    }

    // Keys 1-3 are removed and put again (new tasks of the new master); key 4
    // is new. The store takes all four right away.
    for (int i = 1; i < 4; ++i) {
        ASSERT_TRUE(
            master.Remove(Key(0, i), TenantId::Default(), true).has_value());
        Put(master, stores_[0], Key(0, i));
    }
    Put(master, stores_[0], Key(0, 4));
    auto taken = master.OffloadObjectHeartbeat(stores_[0].client_id, true);
    ASSERT_TRUE(taken.has_value());
    ASSERT_EQ(taken->size(), 4u);
    for (const auto& item : *taken) {
        ASSERT_TRUE(item.task_id.has_value());
        EXPECT_EQ(item.task_id->epoch, master.L4TaskEpoch());
    }
    // The old takes' reports arrive while the store holds the new takes of
    // the same keys: still refused.
    for (int i = 1; i < 4; ++i) late(i);
    for (int i = 1; i < 4; ++i) {
        EXPECT_FALSE(HasDisk(master, Key(0, i), stores_[0].client_id)) << i;
    }
    // The new takes complete.
    std::vector<StorageObjectMetadata> metas(taken->size(), DiskMeta("disk-0"));
    ASSERT_TRUE(master.NotifyOffloadSuccess(stores_[0].client_id, *taken, metas)
                    .has_value());
    for (int i = 1; i < 5; ++i) {
        EXPECT_TRUE(HasDisk(master, Key(0, i), stores_[0].client_id)) << i;
    }
}

// A schema-1 snapshot (written before L4 was carried) holds memory replicas
// only and is restored as such; its stores register their disk tier anew.
TEST_F(HandoffRestoreTest, Schema1SnapshotIsRestoredWithoutL4) {
    TempDir dir;
    {
        MasterService old_master(MakeConfig());
        Populate(old_master, 1, 20);
        Write(old_master, dir.path());
    }
    const std::string path = dir.path() + "/" + handoff::kHandoffManifestFile;
    auto bytes = handoff::ReadFileBytes(path);
    ASSERT_TRUE(bytes.has_value());
    auto manifest = handoff::DecodeManifest(std::string_view(
        reinterpret_cast<const char*>(bytes->data()), bytes->size()));
    ASSERT_TRUE(manifest.has_value());
    manifest->schema_version = 1;
    manifest->local_disk.reset();
    manifest->local_disk_clients = 0;
    manifest->local_disk_replicas = 0;
    const std::string text = handoff::EncodeManifest(*manifest);
    EXPECT_EQ(text.find("\"local_disk\""), std::string::npos);
    std::ofstream(path, std::ios::trunc) << text;

    MasterService master(MakeL4Config());
    auto restored = master.RestoreFromHandoff(dir.path(), {1});
    ASSERT_TRUE(restored.has_value()) << restored.error();
    EXPECT_EQ(restored->objects, 20u);
    EXPECT_EQ(restored->local_disk_clients, 0u);
    ASSERT_TRUE(Remount(master, stores_[0]));
    EXPECT_TRUE(Readable(master, Key(0, 0)));
    auto heartbeat = master.OffloadObjectHeartbeat(stores_[0].client_id, true);
    ASSERT_FALSE(heartbeat.has_value());
    EXPECT_EQ(heartbeat.error(), ErrorCode::SEGMENT_NOT_FOUND);
}

class HandoffColdStartTest : public HandoffRestoreTest {
   protected:
    void SetUp() override {
        MasterService old_master(MakeConfig());
        Populate(old_master, 2, 50);
        Write(old_master, dir_.path());
        auto bytes =
            handoff::ReadFileBytes(Path(handoff::kHandoffManifestFile));
        ASSERT_TRUE(bytes.has_value());
        auto manifest = handoff::DecodeManifest(std::string_view(
            reinterpret_cast<const char*>(bytes->data()), bytes->size()));
        ASSERT_TRUE(manifest.has_value()) << manifest.error();
        manifest_ = *manifest;
        auto batch_bytes =
            handoff::ReadFileBytes(Path(manifest_.batch_manifest.key));
        ASSERT_TRUE(batch_bytes.has_value());
        auto batch = ha::DecodeBatchOpLogSnapshotManifest(
            std::string_view(reinterpret_cast<const char*>(batch_bytes->data()),
                             batch_bytes->size()));
        ASSERT_TRUE(batch.has_value()) << batch.error();
        batch_ = *batch;
        auto segment_bytes = handoff::ReadFileBytes(Path(batch_.segments.key));
        ASSERT_TRUE(segment_bytes.has_value());
        auto segments = DecodeBatchOpLogSnapshotSegments(*segment_bytes);
        ASSERT_TRUE(segments.has_value());
        segments_ = *segments;
    }
    std::string Path(const std::string& key) const {
        return dir_.path() + "/" + key;
    }
    static void WriteBytes(const std::string& path,
                           const std::vector<uint8_t>& bytes) {
        std::ofstream(path, std::ios::trunc | std::ios::binary)
            .write(reinterpret_cast<const char*>(bytes.data()),
                   static_cast<std::streamsize>(bytes.size()));
    }
    void WriteManifest(const handoff::Manifest& m) {
        std::ofstream(Path(handoff::kHandoffManifestFile), std::ios::trunc)
            << handoff::EncodeManifest(m);
    }
    // Writes `segments_` and re-signs the batch and handoff manifests, so
    // that only the segment records differ from what the writer produced.
    void WriteSegments() {
        const auto bytes = EncodeBatchOpLogSnapshotSegments(segments_);
        WriteBytes(Path(batch_.segments.key), bytes);
        batch_.segments.stored_size = bytes.size();
        batch_.segments.crc32c = Crc32cValue(bytes.data(), bytes.size());
        const std::string json = ha::EncodeBatchOpLogSnapshotManifest(batch_);
        std::ofstream(Path(manifest_.batch_manifest.key), std::ios::trunc)
            << json;
        manifest_.batch_manifest.bytes = json.size();
        manifest_.batch_manifest.crc32c = Crc32cValue(json.data(), json.size());
        WriteManifest(manifest_);
    }
    // The restore fails. Its master is then discarded (master.cpp starts
    // from a new, empty one), so nothing else is checked here.
    void ExpectColdStart(
        BufferAllocatorType allocator = BufferAllocatorType::OFFSET,
        const std::string& reason = "") {
        MasterService master(MakeConfig(3600, 3600, allocator));
        auto restored = master.RestoreFromHandoff(dir_.path(), {2});
        ASSERT_FALSE(restored.has_value());
        if (!reason.empty()) {
            EXPECT_NE(restored.error().find(reason), std::string::npos)
                << restored.error();
        }
    }
    TempDir dir_;
    handoff::Manifest manifest_;
    ha::BatchOpLogSnapshotManifest batch_;
    std::vector<StandbySegmentInfo> segments_;
};

TEST_F(HandoffColdStartTest, MissingManifest) {
    fs::remove(Path(handoff::kHandoffManifestFile));
    ExpectColdStart();
}

TEST_F(HandoffColdStartTest, CorruptManifestBody) {
    std::fstream f(Path(handoff::kHandoffManifestFile),
                   std::ios::in | std::ios::out);
    f.seekp(100);
    f.put('#');
    f.close();
    ExpectColdStart();
}

TEST_F(HandoffColdStartTest, UnknownSchemaVersion) {
    std::string text = handoff::EncodeManifest(manifest_);
    const std::string current =
        " " + std::to_string(handoff::kHandoffSchemaVersion) + " ";
    ASSERT_NE(text.find(current), std::string::npos);
    text.replace(text.find(current), current.size(), " 9 ");
    std::ofstream(Path(handoff::kHandoffManifestFile), std::ios::trunc) << text;
    ExpectColdStart();
}

TEST_F(HandoffColdStartTest, CodecFingerprintMismatch) {
    manifest_.codec.object_entry ^= 1;
    WriteManifest(manifest_);
    ExpectColdStart();
}

TEST_F(HandoffColdStartTest, ChunkCrcMismatch) {
    for (const auto& chunk : batch_.object_chunks) {
        if (chunk.object_count == 0) continue;
        std::fstream f(Path(chunk.key),
                       std::ios::in | std::ios::out | std::ios::binary);
        f.seekp(static_cast<std::streamoff>(chunk.stored_size / 2));
        f.put('\x5a');
        break;
    }
    ExpectColdStart();
}

TEST_F(HandoffColdStartTest, TruncatedChunk) {
    for (const auto& chunk : batch_.object_chunks) {
        if (chunk.object_count == 0) continue;
        fs::resize_file(Path(chunk.key), chunk.stored_size / 2);
        break;
    }
    ExpectColdStart();
}

TEST_F(HandoffColdStartTest, DescriptorOutsideItsSegment) {
    segments_[0].capacity = 4096;
    WriteSegments();
    ExpectColdStart();
}

TEST_F(HandoffColdStartTest, SegmentIdentityMissingOwner) {
    segments_[1].owner_client_id = std::nullopt;
    WriteSegments();
    ExpectColdStart(BufferAllocatorType::OFFSET, "segment without identity");
}

TEST_F(HandoffColdStartTest, SegmentIdentityMissingId) {
    segments_[0].segment_id = std::nullopt;
    WriteSegments();
    ExpectColdStart(BufferAllocatorType::OFFSET, "segment without identity");
}

TEST_F(HandoffColdStartTest, SegmentRecordsTamperedWithoutResigning) {
    // The batch manifest's CRC covers the segment records.
    segments_[0].base = *segments_[0].base + 4096;
    WriteBytes(Path(batch_.segments.key),
               EncodeBatchOpLogSnapshotSegments(segments_));
    ExpectColdStart(BufferAllocatorType::OFFSET, "mismatch");
}

TEST_F(HandoffColdStartTest, BatchManifestCrcMismatch) {
    std::fstream f(Path(manifest_.batch_manifest.key),
                   std::ios::in | std::ios::out);
    f.seekp(10);
    f.put('#');
    f.close();
    ExpectColdStart(BufferAllocatorType::OFFSET, "crc mismatch");
}

TEST_F(HandoffColdStartTest, SegmentAttributesDoNotMatchTheRecords) {
    manifest_.segments.pop_back();
    WriteManifest(manifest_);
    ExpectColdStart(BufferAllocatorType::OFFSET, "segment count mismatch");
}

TEST_F(HandoffColdStartTest, QuarantineOverlappingALiveRange) {
    // Find one live descriptor and claim its range is quarantined.
    MasterService probe(MakeConfig());
    auto restored = probe.RestoreFromHandoff(dir_.path(), {1});
    ASSERT_TRUE(restored.has_value()) << restored.error();
    ASSERT_TRUE(Remount(probe, stores_[0]));
    auto r = probe.GetReplicaList(Key(0, 7), TenantId::Default());
    ASSERT_TRUE(r.has_value());
    const auto& b =
        r->replicas.front().get_memory_descriptor().buffer_descriptor;
    uint32_t segment_index = 0;
    while (segments_[segment_index].transport_endpoint !=
           b.transport_endpoint_) {
        ++segment_index;
    }
    std::vector<handoff::QuarantineRange> ranges{
        {segment_index, b.buffer_address_, b.size_}};
    const auto bytes = handoff::EncodeQuarantine(ranges);
    std::ofstream(Path(handoff::kHandoffQuarantineFile),
                  std::ios::trunc | std::ios::binary)
        .write(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    manifest_.quarantine.bytes = bytes.size();
    manifest_.quarantine.crc32c = Crc32cValue(bytes.data(), bytes.size());
    manifest_.quarantine_ranges = 1;
    WriteManifest(manifest_);
    ExpectColdStart(BufferAllocatorType::OFFSET, "overlapping");
}

TEST_F(HandoffColdStartTest, AllocatorTypeMismatch) {
    ExpectColdStart(BufferAllocatorType::CACHELIB);
}

TEST_F(HandoffColdStartTest, MasterNotEmpty) {
    MasterService master(MakeConfig());
    Segment segment = MakeSegment(9);
    ASSERT_TRUE(master.MountSegment(segment, generate_uuid()).has_value());
    EXPECT_FALSE(master.RestoreFromHandoff(dir_.path(), {1}).has_value());
}

// ---- Held-back ranges, DRAINING segments, pending segments that are
// unmounted, a roll during a roll, identity rules.

MasterServiceConfig MakePutTimeoutConfig(uint64_t discard_sec,
                                         uint64_t release_sec) {
    return MasterServiceConfig::builder()
        .set_memory_allocator(BufferAllocatorType::OFFSET)
        .set_eviction_ratio(0.0)
        .set_eviction_high_watermark_ratio(1.0)
        .set_client_live_ttl_sec(3600)
        .set_client_suspicion_ttl_sec(3600)
        .set_put_start_discard_timeout_sec(discard_sec)
        .set_put_start_release_timeout_sec(release_sec)
        .build();
}

std::vector<handoff::QuarantineRange> ReadQuarantine(const std::string& dir) {
    auto bytes =
        handoff::ReadFileBytes(dir + "/" + handoff::kHandoffQuarantineFile);
    EXPECT_TRUE(bytes.has_value());
    if (!bytes) return {};
    auto ranges = handoff::DecodeQuarantine(*bytes);
    EXPECT_TRUE(ranges.has_value());
    return ranges ? *ranges : std::vector<handoff::QuarantineRange>{};
}

const AllocatedBuffer::Descriptor& Buf(const Replica::Descriptor& d) {
    return d.get_memory_descriptor().buffer_descriptor;
}

bool Disjoint(const AllocatedBuffer::Descriptor& a,
              const AllocatedBuffer::Descriptor& b) {
    return a.buffer_address_ + a.size_ <= b.buffer_address_ ||
           b.buffer_address_ + b.size_ <= a.buffer_address_;
}

// Puts on `store`'s first segment until it is full; every new range must be
// disjoint from all of `held`.
void FillAvoiding(MasterService& master, const Store& store,
                  const std::vector<AllocatedBuffer::Descriptor>& held,
                  const std::string& prefix) {
    for (int i = 0; i < 5000; ++i) {
        ReplicateConfig config;
        config.replica_num = 1;
        config.preferred_segment = store.segments[0].name;
        auto r = master.PutStart(store.client_id, prefix + std::to_string(i),
                                 TenantId::Default(), kObjectSize, config);
        if (!r) break;
        const auto& b = Buf(r->front());
        if (b.transport_endpoint_ != store.segments[0].te_endpoint) continue;
        for (const auto& h : held) {
            ASSERT_TRUE(Disjoint(b, h)) << prefix << i;
        }
    }
}

TEST_F(HandoffRestoreTest,
       DiscardedRangesAreQuarantinedWithTheirRemainingTime) {
    TempDir dir;
    std::vector<AllocatedBuffer::Descriptor> discarded, in_flight;
    uint64_t used = 0;
    {
        // 1 s discard / 30 s release timeouts in the old master.
        MasterService old_master(MakePutTimeoutConfig(1, 30));
        Populate(old_master, 1, 10);
        const Store& store = stores_[0];
        ReplicateConfig config;
        config.replica_num = 1;
        config.preferred_segment = store.segments[0].name;
        // Writer W1 is still writing "upserted" when W2's upsert preempts
        // it: W1's range goes to the discarded list, held 30 s.
        discarded.push_back(Buf(Put(old_master, store, "upserted", false)));
        auto upsert =
            old_master.UpsertStart(store.client_id, "upserted",
                                   TenantId::Default(), kObjectSize, config);
        ASSERT_TRUE(upsert.has_value()) << toString(upsert.error());
        in_flight.push_back(Buf(upsert->front()));
        // A put that never ended is replaced after the discard timeout: its
        // range is held until put start + 30 s.
        discarded.push_back(Buf(Put(old_master, store, "stale", false)));
        std::this_thread::sleep_for(std::chrono::milliseconds(1100));
        in_flight.push_back(Buf(Put(old_master, store, "stale", false)));
        used = Used(old_master, store.segments[0]);
        auto written = Write(old_master, dir.path());
        EXPECT_EQ(written.stats.discarded_ranges, 2u);
        EXPECT_EQ(written.stats.quarantine_ranges, 4u);
        EXPECT_EQ(written.stats.objects, 10u);
    }
    // In-flight ranges carry the full timeout, discarded ones what was left.
    const auto ranges = ReadQuarantine(dir.path());
    ASSERT_EQ(ranges.size(), 4u);
    for (const auto& range : ranges) {
        bool is_discarded = false;
        for (const auto& d : discarded) {
            if (d.buffer_address_ == range.address) is_discarded = true;
        }
        if (is_discarded) {
            EXPECT_LT(range.hold_ms, 29500u);
            EXPECT_GT(range.hold_ms, 20000u);
        } else {
            EXPECT_EQ(range.hold_ms, 30000u);
        }
    }

    MasterService master(MakeConfig());
    ASSERT_TRUE(master.RestoreFromHandoff(dir.path(), {2}).has_value());
    EXPECT_EQ(master.HandoffQuarantineBufferCount(), 4u);
    EXPECT_EQ(Used(master, stores_[0].segments[0]), used);
    ASSERT_TRUE(Remount(master, stores_[0]));
    std::vector<AllocatedBuffer::Descriptor> held = discarded;
    held.insert(held.end(), in_flight.begin(), in_flight.end());
    FillAvoiding(master, stores_[0], held, "fill-");

    const auto now = std::chrono::steady_clock::now();
    master.ReleaseExpiredHandoffQuarantine(now + std::chrono::seconds(20));
    EXPECT_EQ(master.HandoffQuarantineBufferCount(), 4u);
    // The discarded ranges end with the old master's deadline, the in-flight
    // ones 30 s after the restore.
    master.ReleaseExpiredHandoffQuarantine(now +
                                           std::chrono::milliseconds(29500));
    EXPECT_EQ(master.HandoffQuarantineBufferCount(), 2u);
    master.ReleaseExpiredHandoffQuarantine(now + std::chrono::seconds(31));
    EXPECT_EQ(master.HandoffQuarantineBufferCount(), 0u);
}

TEST_F(HandoffRestoreTest, DrainingSegmentIsRestoredDrainingWithItsQuarantine) {
    TempDir dir;
    AllocatedBuffer::Descriptor in_flight;
    {
        MasterService old_master(MakeConfig());
        Populate(old_master, 2, 10);
        // A put granted before an admin set the segment to DRAINING.
        in_flight = Buf(Put(old_master, stores_[0], "in-flight", false));
        ASSERT_TRUE(old_master
                        .SetSegmentStatus(stores_[0].segments[0].name,
                                          SegmentStatus::DRAINING)
                        .has_value());
        auto written = Write(old_master, dir.path());
        EXPECT_EQ(written.stats.objects, 20u);
        EXPECT_EQ(written.stats.quarantine_ranges, 1u);
        EXPECT_EQ(written.stats.dropped_unknown_segment_replicas, 0u);
    }
    MasterService master(MakeConfig());
    ASSERT_TRUE(master.RestoreFromHandoff(dir.path(), {2}).has_value());
    EXPECT_EQ(master.HandoffPendingSegmentCount(), 2u);
    EXPECT_EQ(master.HandoffQuarantineBufferCount(), 1u);
    const auto& seg0 = stores_[0].segments[0].name;
    const auto& seg1 = stores_[1].segments[0].name;
    EXPECT_EQ(master.QuerySegmentStatus(seg0).value_or(SegmentStatus::OK),
              SegmentStatus::DRAINING);

    // Nothing opens a pending segment: neither the admin status change nor
    // a drain job (which would set DRAINING and later OK).
    auto set = master.SetSegmentStatus(seg0, SegmentStatus::OK);
    ASSERT_FALSE(set.has_value());
    EXPECT_EQ(set.error(), ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    set = master.SetSegmentStatus(seg1, SegmentStatus::DRAINING);
    ASSERT_FALSE(set.has_value());
    EXPECT_EQ(set.error(), ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    CreateDrainJobRequest drain;
    drain.segments = {seg1};
    EXPECT_FALSE(master.CreateDrainJob(drain).has_value());
    EXPECT_EQ(
        master.QuerySegmentStatus(seg1).value_or(SegmentStatus::UNDEFINED),
        SegmentStatus::OK);

    // The owner confirms: objects readable, the segment stays DRAINING and
    // is not allocatable (store 1 is still pending, so no put succeeds).
    ASSERT_TRUE(Remount(master, stores_[0]));
    EXPECT_EQ(master.HandoffPendingSegmentCount(), 1u);
    EXPECT_TRUE(Readable(master, Key(0, 9)));
    EXPECT_EQ(master.QuerySegmentStatus(seg0).value_or(SegmentStatus::OK),
              SegmentStatus::DRAINING);
    ReplicateConfig config;
    config.replica_num = 1;
    config.preferred_segment = seg0;
    EXPECT_FALSE(master
                     .PutStart(stores_[0].client_id, "nowhere",
                               TenantId::Default(), kObjectSize, config)
                     .has_value());
    ASSERT_TRUE(Remount(master, stores_[1]));
    auto elsewhere = master.PutStart(stores_[0].client_id, "elsewhere",
                                     TenantId::Default(), kObjectSize, config);
    ASSERT_TRUE(elsewhere.has_value());
    EXPECT_EQ(Buf(elsewhere->front()).transport_endpoint_,
              stores_[1].segments[0].te_endpoint);

    // Confirmed: the admin may open it again; the in-flight range stays held.
    ASSERT_TRUE(master.SetSegmentStatus(seg0, SegmentStatus::OK).has_value());
    FillAvoiding(master, stores_[0], {in_flight}, "fill-");
    EXPECT_EQ(master.HandoffQuarantineBufferCount(), 1u);
}

TEST_F(HandoffRestoreTest, GracefullyUnmountingSegmentIsNotExported) {
    TempDir dir;
    {
        MasterService old_master(MakeConfig());
        Populate(old_master, 2, 5);
        ASSERT_TRUE(old_master
                        .GracefulUnmountSegment(stores_[0].segments[0].id,
                                                stores_[0].client_id, 60000)
                        .has_value());
        auto written = Write(old_master, dir.path());
        EXPECT_EQ(written.stats.objects, 5u);
    }
    MasterService master(MakeConfig());
    auto restored = master.RestoreFromHandoff(dir.path(), {1});
    ASSERT_TRUE(restored.has_value()) << restored.error();
    EXPECT_EQ(restored->segments, 1u);
    EXPECT_EQ(master.HandoffPendingSegmentCount(), 1u);
}

TEST_F(HandoffRestoreTest, PendingSegmentOfOffboardedOwnerIsForgotten) {
    TempDir dir;
    {
        MasterService old_master(MakeConfig());
        Populate(old_master, 2, 40);
        Put(old_master, stores_[0], "in-flight", false);
        Write(old_master, dir.path());
    }
    MasterService master(MakeConfig(/*live=*/1, /*suspicion=*/1));
    ASSERT_TRUE(master.RestoreFromHandoff(dir.path(), {2}).has_value());
    EXPECT_EQ(master.HandoffQuarantineBufferCount(), 1u);
    ASSERT_TRUE(Remount(master, stores_[1]));
    for (int i = 0; i < 80 && master.GetKeyCount() != 40; ++i) {
        (void)master.Ping(stores_[1].client_id);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    ASSERT_EQ(master.GetKeyCount(), 40u);
    // The TTL offboarding unmounted the segment; the pending entry, its read
    // gate and its quarantine go with it.
    EXPECT_EQ(master.ReconcileHandoffPendingSegments(), 1u);
    EXPECT_EQ(master.HandoffPendingSegmentCount(), 0u);
    EXPECT_EQ(master.HandoffQuarantineBufferCount(), 0u);
    EXPECT_EQ(master.ReconcileHandoffPendingSegments(), 0u);

    // A new store at the old endpoint (different range, so no identity rule
    // applies) is readable at once.
    Store next{generate_uuid(), {MakeSegment(7)}};
    next.segments[0].name = stores_[0].segments[0].name;
    next.segments[0].te_endpoint = stores_[0].segments[0].te_endpoint;
    ASSERT_TRUE(
        master.MountSegment(next.segments[0], next.client_id).has_value());
    Put(master, next, "next");
    EXPECT_TRUE(Readable(master, "next"));
    (void)master.Ping(next.client_id);
    EXPECT_FALSE(Readable(master, Key(0, 0)));
}

TEST_F(HandoffRestoreTest, OwnerReturningAfterOffboardingMountsFresh) {
    TempDir dir;
    {
        MasterService old_master(MakeConfig());
        Populate(old_master, 2, 20);
        Write(old_master, dir.path());
    }
    MasterService master(MakeConfig(/*live=*/1, /*suspicion=*/1));
    ASSERT_TRUE(master.RestoreFromHandoff(dir.path(), {2}).has_value());
    ASSERT_TRUE(Remount(master, stores_[1]));
    for (int i = 0; i < 80 && master.GetKeyCount() != 20; ++i) {
        (void)master.Ping(stores_[1].client_id);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    ASSERT_EQ(master.GetKeyCount(), 20u);
    (void)master.ReconcileHandoffPendingSegments();
    // The owner comes back with exactly the restored identity: it gets an
    // empty segment, never its old objects.
    bool remounted = false;
    for (int i = 0; i < 50 && !remounted; ++i) {
        remounted = Remount(master, stores_[0]);
        if (!remounted) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
    ASSERT_TRUE(remounted);
    EXPECT_EQ(master.HandoffPendingSegmentCount(), 0u);
    EXPECT_FALSE(Readable(master, Key(0, 0)));
    Put(master, stores_[0], "back");
    EXPECT_TRUE(Readable(master, "back"));
}

TEST_F(HandoffRestoreTest, RestoredOwnerIsNotOffboardedBeforeItsTtl) {
    TempDir dir;
    {
        MasterService old_master(MakeConfig());
        Populate(old_master, 1, 20);
        Write(old_master, dir.path());
    }
    // Liveness clocks start when the restore ends, not when the old master
    // last heard from the store.
    MasterService master(MakeConfig(/*live=*/2, /*suspicion=*/2));
    ASSERT_TRUE(master.RestoreFromHandoff(dir.path(), {1}).has_value());
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
    ASSERT_TRUE(Remount(master, stores_[0]));
    EXPECT_EQ(master.HandoffSegmentsConfirmed(), 1u);
    EXPECT_EQ(master.GetKeyCount(), 20u);
    EXPECT_TRUE(Readable(master, Key(0, 19)));
}

TEST_F(HandoffRestoreTest, RollDuringRollKeepsPendingSegmentsAndQuarantine) {
    TempDir first, second;
    AllocatedBuffer::Descriptor in_flight;
    uint64_t used0 = 0;
    {
        MasterService old_master(MakeConfig());
        Populate(old_master, 2, 30);
        in_flight = Buf(Put(old_master, stores_[0], "in-flight", false));
        used0 = Used(old_master, stores_[0].segments[0]);
        Write(old_master, first.path());
    }
    {
        // Restored master: store 1 confirmed, store 0 still pending when the
        // next SIGTERM arrives.
        MasterService middle(MakeConfig());
        ASSERT_TRUE(middle.RestoreFromHandoff(first.path(), {2}).has_value());
        ASSERT_TRUE(Remount(middle, stores_[1]));
        Put(middle, stores_[1], "between");
        auto written = Write(middle, second.path());
        EXPECT_EQ(written.stats.objects, 61u);
        EXPECT_EQ(written.stats.quarantine_ranges, 1u);
    }
    const auto ranges = ReadQuarantine(second.path());
    ASSERT_EQ(ranges.size(), 1u);
    EXPECT_EQ(ranges[0].address, in_flight.buffer_address_);
    // What was left of the first restore's hold, not a new full timeout.
    EXPECT_LE(ranges[0].hold_ms, 600000u);
    EXPECT_GT(ranges[0].hold_ms, 590000u);

    MasterService master(MakeConfig());
    ASSERT_TRUE(master.RestoreFromHandoff(second.path(), {2}).has_value());
    EXPECT_EQ(master.HandoffPendingSegmentCount(), 2u);
    EXPECT_EQ(master.HandoffQuarantineBufferCount(), 1u);
    EXPECT_EQ(Used(master, stores_[0].segments[0]), used0);
    ASSERT_TRUE(Remount(master, stores_[0]));
    ASSERT_TRUE(Remount(master, stores_[1]));
    EXPECT_TRUE(Readable(master, Key(0, 29)));
    EXPECT_TRUE(Readable(master, "between"));
    FillAvoiding(master, stores_[0], {in_flight}, "fill-");
}

TEST_F(HandoffRestoreTest, OwnerReRegisteringOverlappingMemoryDrops) {
    TempDir dir;
    {
        MasterService old_master(MakeConfig());
        Populate(old_master, 2, 10);
        Write(old_master, dir.path());
    }
    // Rule 3 via MountSegment: the owner mounts a new segment id over the
    // restored range.
    {
        MasterService master(MakeConfig());
        ASSERT_TRUE(master.RestoreFromHandoff(dir.path(), {1}).has_value());
        Segment again = stores_[0].segments[0];
        again.id = generate_uuid();
        again.base += kObjectSize;
        again.size -= kObjectSize;
        ASSERT_TRUE(
            master.MountSegment(again, stores_[0].client_id).has_value());
        EXPECT_EQ(master.HandoffSegmentsDropped(), 1u);
        EXPECT_FALSE(Readable(master, Key(0, 0)));
        ASSERT_TRUE(Remount(master, stores_[1]));
        EXPECT_TRUE(Readable(master, Key(1, 0)));
    }
    // Rule 3 via ReMountSegment (with rule 4: the old id is not listed).
    {
        MasterService master(MakeConfig());
        ASSERT_TRUE(master.RestoreFromHandoff(dir.path(), {1}).has_value());
        Store changed = stores_[0];
        changed.segments[0].id = generate_uuid();
        ASSERT_TRUE(Remount(master, changed));
        EXPECT_EQ(master.HandoffSegmentsDropped(), 1u);
        EXPECT_FALSE(Readable(master, Key(0, 0)));
    }
    // Rule 1 via ReMountSegment: another process remounts at the endpoint.
    {
        MasterService master(MakeConfig());
        ASSERT_TRUE(master.RestoreFromHandoff(dir.path(), {1}).has_value());
        Store restarted{generate_uuid(), {stores_[0].segments[0]}};
        restarted.segments[0].id = generate_uuid();
        ASSERT_TRUE(Remount(master, restarted));
        EXPECT_EQ(master.HandoffSegmentsDropped(), 1u);
        EXPECT_FALSE(Readable(master, Key(0, 0)));
        Put(master, restarted, "fresh");
        EXPECT_TRUE(Readable(master, "fresh"));
    }
}

TEST_F(HandoffRestoreTest, ReadLeasesOfTheOldMasterAreHonoured) {
    auto config = [] {
        return MasterServiceConfig::builder()
            .set_memory_allocator(BufferAllocatorType::OFFSET)
            .set_eviction_ratio(0.0)
            .set_eviction_high_watermark_ratio(1.0)
            .set_client_live_ttl_sec(3600)
            .set_client_suspicion_ttl_sec(3600)
            .set_default_kv_lease_ttl(1500)
            .build();
    };
    TempDir dir;
    {
        MasterService old_master(config());
        Populate(old_master, 1, 5);
        Write(old_master, dir.path());
    }
    MasterService master(config());
    ASSERT_TRUE(master.RestoreFromHandoff(dir.path(), {1}).has_value());
    ASSERT_TRUE(Remount(master, stores_[0]));
    // A reader may still hold a lease from the old master (export + ttl).
    auto removed = master.Remove(Key(0, 0), TenantId::Default(), false);
    ASSERT_FALSE(removed.has_value());
    EXPECT_EQ(removed.error(), ErrorCode::OBJECT_HAS_LEASE);
    std::this_thread::sleep_for(std::chrono::milliseconds(1700));
    EXPECT_TRUE(
        master.Remove(Key(0, 0), TenantId::Default(), false).has_value());
}

TEST_F(HandoffRestoreTest, MultiTenantSnapshotRestoresTenantsAndQuota) {
    TempDir dir;
    const std::string policy = dir.path() + "-quota.yaml";
    {
        TenantQuotaPolicySnapshot snapshot;
        snapshot.tenant_quotas.emplace("tenant-a", 64 * kObjectSize);
        snapshot.tenant_quotas.emplace("tenant-b", 64 * kObjectSize);
        std::ofstream(policy) << FormatTenantQuotaPolicyYaml(snapshot);
    }
    auto config = [&policy](bool multi_tenant) {
        auto builder = MasterServiceConfig::builder();
        builder.set_memory_allocator(BufferAllocatorType::OFFSET)
            .set_eviction_ratio(0.0)
            .set_eviction_high_watermark_ratio(1.0)
            .set_client_live_ttl_sec(3600)
            .set_client_suspicion_ttl_sec(3600)
            .set_enable_multi_tenants(multi_tenant);
        if (multi_tenant) {
            builder.set_tenant_quota_connector_type("file")
                .set_tenant_quota_connector_uri(policy);
        }
        return builder.build();
    };
    const TenantId tenant_a("tenant-a");
    const TenantId tenant_b("tenant-b");
    {
        MasterService old_master(config(true));
        Store store{generate_uuid(), {MakeSegment(0)}};
        ASSERT_TRUE(old_master.MountSegment(store.segments[0], store.client_id)
                        .has_value());
        stores_.push_back(store);
        ReplicateConfig rc;
        rc.replica_num = 1;
        for (const auto* tenant : {&tenant_a, &tenant_b}) {
            for (int i = 0; i < 3; ++i) {
                ASSERT_TRUE(old_master
                                .PutStart(store.client_id, Key(0, i), *tenant,
                                          kObjectSize, rc)
                                .has_value());
                ASSERT_TRUE(old_master
                                .PutEnd(store.client_id, Key(0, i), *tenant,
                                        ReplicaType::MEMORY)
                                .has_value());
            }
        }
        auto written = Write(old_master, dir.path());
        EXPECT_EQ(written.stats.objects, 6u);
    }
    {
        // A master in the other tenancy mode starts cold.
        MasterService master(config(false));
        auto restored = master.RestoreFromHandoff(dir.path(), {1});
        ASSERT_FALSE(restored.has_value());
        EXPECT_NE(restored.error().find("multi-tenant mode mismatch"),
                  std::string::npos);
    }
    MasterService master(config(true));
    ASSERT_TRUE(master.RestoreFromHandoff(dir.path(), {1}).has_value());
    ASSERT_TRUE(Remount(master, stores_[0]));
    EXPECT_EQ(master.GetKeyCount(), 6u);
    for (const auto* tenant : {&tenant_a, &tenant_b}) {
        for (int i = 0; i < 3; ++i) {
            EXPECT_TRUE(master.GetReplicaList(Key(0, i), *tenant).has_value())
                << tenant->value() << " " << i;
        }
    }
    // Same user key, other tenant: still distinct objects.
    EXPECT_FALSE(
        master.GetReplicaList(Key(0, 0), TenantId::Default()).has_value());
    // The quota usage was rebuilt from the restored objects.
    for (const auto* tenant : {&tenant_a, &tenant_b}) {
        auto quota = master.GetTenantQuotaSnapshot(*tenant);
        ASSERT_TRUE(quota.has_value()) << tenant->value();
        EXPECT_EQ(quota->charged_bytes, 3 * kObjectSize) << tenant->value();
    }
    std::filesystem::remove(policy);
}

TEST_F(HandoffRestoreTest, PendingEntryOfSegmentMountedFreshIsForgotten) {
    TempDir dir;
    {
        MasterService old_master(MakeConfig());
        Populate(old_master, 1, 10);
        Write(old_master, dir.path());
    }
    MasterService master(MakeConfig(/*live=*/1, /*suspicion=*/1));
    ASSERT_TRUE(master.RestoreFromHandoff(dir.path(), {1}).has_value());
    for (int i = 0; i < 80 && master.GetKeyCount() != 0; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    ASSERT_EQ(master.GetKeyCount(), 0u);
    // The owner mounts its old identity again (fresh, empty) before the
    // cleanup thread looked at the pending entry.
    bool mounted = false;
    for (int i = 0; i < 50 && !mounted; ++i) {
        mounted =
            master.MountSegment(stores_[0].segments[0], stores_[0].client_id)
                .has_value();
        if (!mounted) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
    ASSERT_TRUE(mounted);
    EXPECT_EQ(master.ReconcileHandoffPendingSegments(), 1u);
    EXPECT_EQ(master.HandoffPendingSegmentCount(), 0u);
    Put(master, stores_[0], "fresh");
    EXPECT_TRUE(Readable(master, "fresh"));
}

}  // namespace
}  // namespace mooncake::test

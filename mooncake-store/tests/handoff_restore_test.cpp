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

    static std::vector<Replica::Descriptor> Replicas(MasterService& service,
                                                     const std::string& key) {
        auto r = service.GetReplicaList(key, TenantId::Default());
        return r ? r->replicas : std::vector<Replica::Descriptor>{};
    }

    std::vector<Store> stores_;
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

TEST_F(HandoffRestoreTest, OnlyMemoryReplicasAreCarried) {
    // An object with a memory and a LOCAL_DISK replica is restored with its
    // memory replica only; an object whose only replica is on disk is not
    // exported. The store registers its disk tier again with the new master,
    // as after a cold start.
    auto l4_config = [] {
        return MasterServiceConfig::builder()
            .set_memory_allocator(BufferAllocatorType::OFFSET)
            .set_eviction_ratio(0.0)
            .set_eviction_high_watermark_ratio(1.0)
            .set_client_live_ttl_sec(3600)
            .set_client_suspicion_ttl_sec(3600)
            .set_enable_offload(true)
            .build();
    };
    TempDir dir;
    {
        MasterService old_master(l4_config());
        Populate(old_master, 1, 3);
        const Store& store = stores_[0];
        ASSERT_TRUE(old_master.MountLocalDiskSegment(store.client_id, true)
                        .has_value());
        StorageObjectMetadata disk;
        disk.data_size = kObjectSize;
        disk.transport_endpoint = "disk-0";
        const std::vector<OffloadTaskItem> items{
            {"default", Key(0, 0), static_cast<int64_t>(kObjectSize)},
            {"default", "disk-only", static_cast<int64_t>(kObjectSize)}};
        ASSERT_TRUE(
            old_master
                .NotifyOffloadSuccess(store.client_id, items, {disk, disk})
                .has_value());
        bool has_disk = false;
        for (const auto& d : Replicas(old_master, Key(0, 0))) {
            has_disk = has_disk || d.is_local_disk_replica();
        }
        ASSERT_TRUE(has_disk);
        auto written = Write(old_master, dir.path());
        EXPECT_EQ(written.stats.objects, 3u);
        EXPECT_EQ(written.stats.replicas, 3u);
        EXPECT_EQ(written.stats.dropped_non_memory_replicas, 2u);
        EXPECT_EQ(written.stats.skipped_no_exportable_replica, 1u);
    }
    MasterService master(l4_config());
    ASSERT_TRUE(master.RestoreFromHandoff(dir.path(), {1}).has_value());
    ASSERT_TRUE(Remount(master, stores_[0]));
    auto replicas = Replicas(master, Key(0, 0));
    ASSERT_EQ(replicas.size(), 1u);
    EXPECT_TRUE(replicas.front().is_memory_replica());
    EXPECT_FALSE(Readable(master, "disk-only"));
    EXPECT_EQ(master.GetKeyCount(), 3u);
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

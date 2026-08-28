#include "segment/pool.h"

#include <gtest/gtest.h>
#include <msgpack.hpp>

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <thread>

#include "placement/domain.h"
#include "ha/snapshot/store_resource_snapshot_codec.h"
#include "segment/pool_read_access.h"
#include "segment/snapshot_view.h"
#include "segment/pool_write_access.h"
#include "test_buffer_allocator.h"
#include "utils/zstd_util.h"

namespace mooncake::test {
namespace {

constexpr size_t kRegionSize = 16U * 1024 * 1024;

RegionDriverRegistry Drivers(bool enable_cxl = false) {
    RegionDriverConfig config;
    config.memory_allocator = BufferAllocatorType::OFFSET;
    config.enable_cxl = enable_cxl;
    config.cxl_path = "cxl-test-path";
    config.cxl_size = kRegionSize;
    return CreateRegionDrivers(config);
}

Segment MakeSegment(size_t index, std::string name,
                    std::string protocol = "tcp", std::string host = {}) {
    Segment segment;
    segment.id = generate_uuid();
    segment.name = std::move(name);
    segment.base = 0x100000000ULL + index * 0x2000000ULL;
    segment.size = kRegionSize;
    segment.te_endpoint = segment.name + "-endpoint";
    segment.protocol = std::move(protocol);
    segment.host_id = std::move(host);
    return segment;
}

void CommitUnmount(SegmentPool& pool, const Segment& segment,
                   const UUID& client_id) {
    auto access = pool.AcquireWriteAccess();
    auto transaction = access.PrepareUnmount(segment.id, client_id);
    ASSERT_TRUE(transaction.has_value());
    ASSERT_EQ(access.CommitUnmount(*transaction), ErrorCode::OK);
}

std::vector<uint8_t> ReplaceSnapshotActiveNames(
    const std::vector<uint8_t>& snapshot,
    const std::vector<std::string>& active_names) {
    const auto decompressed = zstd_decompress(snapshot);
    const auto unpacked =
        msgpack::unpack(reinterpret_cast<const char*>(decompressed.data()),
                        decompressed.size());
    const auto& root = unpacked.get();

    msgpack::sbuffer buffer;
    MsgpackPacker packer(&buffer);
    packer.pack_map(root.via.map.size);
    for (uint32_t i = 0; i < root.via.map.size; ++i) {
        const auto& item = root.via.map.ptr[i];
        packer.pack(item.key);
        if (item.key.type == msgpack::type::STR &&
            std::string_view(item.key.via.str.ptr, item.key.via.str.size) ==
                "an") {
            packer.pack_array(active_names.size());
            for (const auto& name : active_names) {
                packer.pack(name);
            }
        } else {
            packer.pack(item.val);
        }
    }
    return zstd_compress(reinterpret_cast<const uint8_t*>(buffer.data()),
                         buffer.size(), 3);
}

}  // namespace

TEST(SegmentPoolTest, ClassifiesOnlyCxlProtocolAsCxl) {
    SegmentPool pool(Drivers(true));
    const UUID client = generate_uuid();
    auto host = MakeSegment(0, "host", "rdma");
    auto cxl = MakeSegment(1, "cxl", "cxl");
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.MountSegment(host, client), ErrorCode::OK);
        ASSERT_EQ(access.MountSegment(cxl, client), ErrorCode::OK);
    }

    {
        MountedRegion mounted;
        auto view = pool.AcquireReadAccess();
        ASSERT_EQ(view.Catalog().GetMountedRegion(host.id, mounted),
                  ErrorCode::OK);
        EXPECT_EQ(mounted.kind, RegionKind::HOST_MEMORY);
        ASSERT_EQ(view.Catalog().GetMountedRegion(cxl.id, mounted),
                  ErrorCode::OK);
        EXPECT_EQ(mounted.kind, RegionKind::CXL);
        EXPECT_NE(view.Placement().Find(host.name), nullptr);
        EXPECT_NE(view.Placement().Find(cxl.name), nullptr);
    }

    CommitUnmount(pool, host, client);
    CommitUnmount(pool, cxl, client);
}

TEST(SegmentPoolTest, PreparedMountPublishesOnlyOnCommit) {
    SegmentPool pool(Drivers());
    const UUID client = generate_uuid();
    auto segment = MakeSegment(0, "prepared");
    {
        auto access = pool.AcquireWriteAccess();
        auto transaction = access.PrepareMount(segment, client);
        ASSERT_TRUE(transaction.has_value());
    }
    MountedRegion mounted;
    EXPECT_EQ(pool.AcquireReadAccess().Catalog().GetMountedRegion(segment.id,
                                                                  mounted),
              ErrorCode::SEGMENT_NOT_FOUND);

    {
        auto access = pool.AcquireWriteAccess();
        auto transaction = access.PrepareMount(segment, client);
        ASSERT_TRUE(transaction.has_value());
        access.CommitMount(*transaction);
    }
    EXPECT_EQ(pool.AcquireReadAccess().Catalog().GetMountedRegion(segment.id,
                                                                  mounted),
              ErrorCode::OK);
    CommitUnmount(pool, segment, client);
}

TEST(SegmentPoolTest, RestoreReturnsImportedBuffersInInputOrder) {
    SegmentPool pool(Drivers());
    const UUID client = generate_uuid();
    auto segment = MakeSegment(0, "restore");
    std::vector<AllocatedBuffer::Descriptor> descriptors{
        {4096, segment.base + 8192, "tcp", segment.te_endpoint},
        {4096, segment.base, "tcp", segment.te_endpoint}};
    {
        auto access = pool.AcquireWriteAccess();
        auto transaction = access.PrepareRestore(segment, client, descriptors);
        ASSERT_TRUE(transaction.has_value());
        ASSERT_EQ(transaction->imported_buffers().size(), 2U);
        EXPECT_EQ(reinterpret_cast<uintptr_t>(
                      transaction->imported_buffers()[0]->data()),
                  segment.base + 8192);
        EXPECT_EQ(reinterpret_cast<uintptr_t>(
                      transaction->imported_buffers()[1]->data()),
                  segment.base);
        access.CommitMount(*transaction);
        auto buffers = transaction->TakeImportedBuffers();
        buffers.clear();
    }
    CommitUnmount(pool, segment, client);
}

TEST(SegmentPoolTest, RollbackRestoresOriginalLifecycleState) {
    SegmentPool pool(Drivers());
    const UUID client = generate_uuid();
    auto segment = MakeSegment(0, "draining", "tcp", "host");
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.MountSegment(segment, client), ErrorCode::OK);
        ASSERT_EQ(access.SetSegmentStatusByName(segment.name,
                                                SegmentStatus::DRAINING),
                  ErrorCode::OK);
        auto transaction = access.PrepareUnmount(segment.id, client);
        ASSERT_TRUE(transaction.has_value());
        ASSERT_EQ(access.RollbackUnmount(*transaction), ErrorCode::OK);
    }

    {
        SegmentStatus status = SegmentStatus::UNDEFINED;
        auto view = pool.AcquireReadAccess();
        EXPECT_EQ(view.Catalog().GetRegionStatus(segment.id, status),
                  ErrorCode::OK);
        EXPECT_EQ(status, SegmentStatus::DRAINING);
        EXPECT_EQ(view.Placement().Find(segment.name), nullptr);
    }

    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(
            access.SetSegmentStatusByName(segment.name, SegmentStatus::OK),
            ErrorCode::OK);
    }
    CommitUnmount(pool, segment, client);
}

TEST(SegmentPoolTest, ImmediateUnmountRollbackRestoresPlacement) {
    SegmentPool pool(Drivers());
    const UUID client = generate_uuid();
    auto segment = MakeSegment(0, "rollback", "tcp", "host");
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.MountSegment(segment, client), ErrorCode::OK);
        auto transaction = access.PrepareUnmount(segment.id, client);
        ASSERT_TRUE(transaction.has_value());
        ASSERT_EQ(access.RollbackUnmount(*transaction), ErrorCode::OK);
    }
    {
        auto view = pool.AcquireReadAccess();
        EXPECT_NE(view.Placement().Find(segment.name), nullptr);
    }
    CommitUnmount(pool, segment, client);
}

TEST(SegmentPoolTest, AllocationKeepsReadLockAcrossAllocatorCall) {
    SegmentPool pool(Drivers());
    const UUID client = generate_uuid();
    auto segment = MakeSegment(0, "blocking");
    auto allocator = std::make_shared<TestBufferAllocator>(
        segment.name, segment.te_endpoint, kRegionSize, segment.base);
    {
        MountedRegion mounted{segment, client, SegmentStatus::OK,
                              RegionKind::HOST_MEMORY};
        auto access = pool.AcquireWriteAccess();
        auto transaction = access.PrepareAdopt(mounted, allocator, true);
        ASSERT_TRUE(transaction.has_value());
        access.CommitMount(*transaction);
    }

    allocator->BlockNext();
    auto started = allocator->AllocationStarted();
    auto allocation = std::async(std::launch::async, [&] {
        ReplicaPlacement placement(pool, PlacementPolicyType::RANDOM);
        ReplicaAllocationRequest request;
        request.size = 4096;
        return placement.Allocate(request);
    });
    ASSERT_EQ(started.wait_for(std::chrono::seconds(5)),
              std::future_status::ready);

    std::atomic<bool> unmount_finished{false};
    std::thread unmount([&] {
        CommitUnmount(pool, segment, client);
        unmount_finished.store(true, std::memory_order_release);
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_FALSE(unmount_finished.load(std::memory_order_acquire));
    allocator->AllowAllocation();
    ASSERT_TRUE(allocation.get().has_value());
    unmount.join();
    EXPECT_TRUE(unmount_finished.load(std::memory_order_acquire));
}

TEST(SegmentPoolTest, SnapshotViewDoesNotAcquireInheritedPoolMutex) {
    SegmentPool pool(Drivers());
    const UUID client = generate_uuid();
    auto segment = MakeSegment(0, "fork-safe");
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.MountSegment(segment, client), ErrorCode::OK);

        // A fork child can inherit this mutex as locked by a vanished thread.
        // Encoding must use only the lock-free snapshot view.
        auto encoded = ha::StoreResourceSnapshotCodec::Encode(
            pool.GetSnapshotView(), LocalSsdPersistedState{});
        ASSERT_TRUE(encoded.has_value()) << encoded.error().message;
    }
    CommitUnmount(pool, segment, client);
}

TEST(SegmentPoolTest, SnapshotRoundTripPreservesCatalogAndHost) {
    SegmentPool source(Drivers());
    const UUID client = generate_uuid();
    auto segment = MakeSegment(0, "snapshot", "tcp", "host-a");
    ASSERT_EQ(source.AcquireWriteAccess().MountSegment(segment, client),
              ErrorCode::OK);
    auto encoded = ha::StoreResourceSnapshotCodec::Encode(
        source.GetSnapshotView(), LocalSsdPersistedState{});
    ASSERT_TRUE(encoded.has_value()) << encoded.error().message;

    SegmentPool restored(Drivers());
    auto stale = MakeSegment(1, "stale");
    ASSERT_EQ(restored.AcquireWriteAccess().MountSegment(stale, client),
              ErrorCode::OK);
    auto decoded =
        ha::StoreResourceSnapshotCodec::Decode(restored, *encoded, false);
    ASSERT_TRUE(decoded.has_value()) << decoded.error().message;

    {
        MountedRegion mounted;
        auto view = restored.AcquireReadAccess();
        EXPECT_EQ(view.Catalog().GetMountedRegion(stale.id, mounted),
                  ErrorCode::SEGMENT_NOT_FOUND);
        ASSERT_EQ(view.Catalog().GetMountedRegion(segment.id, mounted),
                  ErrorCode::OK);
        EXPECT_EQ(mounted.segment.host_id, segment.host_id);
        EXPECT_EQ(mounted.client_id, client);
        EXPECT_EQ(mounted.status, SegmentStatus::OK);
    }

    restored.AcquireWriteAccess().Clear();
    CommitUnmount(source, segment, client);
}

TEST(SegmentPoolTest, InvalidSnapshotDoesNotReplacePublishedPool) {
    SegmentPool source(Drivers());
    const UUID client = generate_uuid();
    auto segment = MakeSegment(0, "snapshot-active");
    ASSERT_EQ(source.AcquireWriteAccess().MountSegment(segment, client),
              ErrorCode::OK);
    auto encoded = ha::StoreResourceSnapshotCodec::Encode(
        source.GetSnapshotView(), LocalSsdPersistedState{});
    ASSERT_TRUE(encoded.has_value()) << encoded.error().message;

    SegmentPool restored(Drivers());
    auto stale = MakeSegment(1, "published");
    ASSERT_EQ(restored.AcquireWriteAccess().MountSegment(stale, client),
              ErrorCode::OK);
    auto corrupted = ReplaceSnapshotActiveNames(*encoded, {});
    auto result =
        ha::StoreResourceSnapshotCodec::Decode(restored, corrupted, false);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code, ErrorCode::DESERIALIZE_FAIL);

    {
        MountedRegion mounted;
        auto view = restored.AcquireReadAccess();
        EXPECT_EQ(view.Catalog().GetMountedRegion(stale.id, mounted),
                  ErrorCode::OK);
        EXPECT_EQ(view.Catalog().GetMountedRegion(segment.id, mounted),
                  ErrorCode::SEGMENT_NOT_FOUND);
    }

    CommitUnmount(source, segment, client);
}

}  // namespace mooncake::test

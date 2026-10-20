#include "segment/pool.h"

#include <gtest/gtest.h>
#include <msgpack.hpp>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "common/zstd_util.h"
#include "ha/snapshot/allocator_snapshot_codec.h"
#include "ha/snapshot/store_resource_snapshot_codec.h"
#include "segment/pool_read_access.h"
#include "segment/pool_write_access.h"

namespace mooncake::test {
namespace {

constexpr size_t kRegionSize = 16U * 1024 * 1024;
constexpr size_t kBufferSize = 4096;

RegionDriverRegistry Drivers(bool cxl = false) {
    RegionDriverConfig config;
    config.memory_allocator = BufferAllocatorType::OFFSET;
    if (cxl) {
        config.cxl = CxlRegionDriverConfig{"ownership-cxl", kRegionSize};
    }
    auto drivers = CreateRegionDrivers(config);
    EXPECT_TRUE(drivers.has_value());
    return std::move(drivers.value());
}

MountedRegion Region(uint64_t index, SegmentStatus status = SegmentStatus::OK) {
    Segment segment;
    segment.id = UUID{0, index};
    segment.name = "shared-hostname";
    segment.base = 0x100000000ULL + index * kRegionSize;
    segment.size = kRegionSize;
    segment.te_endpoint = "endpoint-" + std::to_string(index);
    segment.host_id = "shared-host";
    return {std::move(segment), UUID{1, index}, status};
}

// A region served by the driver kind selected by `cxl`.
MountedRegion RegionOnDriver(uint64_t index, bool cxl) {
    auto region = Region(index);
    region.segment.protocol = cxl ? "cxl" : "tcp";
    return region;
}

AllocationCandidateKind KindForDriver(bool cxl) {
    return cxl ? AllocationCandidateKind::CXL : AllocationCandidateKind::NATIVE;
}

// Whether the pool places a new allocation on `segment_name`.
bool AllocatesIn(SegmentPool& pool, std::string_view segment_name,
                 AllocationCandidateKind kind) {
    return pool.AllocateInSegment(segment_name, kind, kBufferSize).has_value();
}

// A mounted region's allocator plus one buffer bound to it. Holding the
// allocator keeps the buffer's memory alive independently of the pool, so a
// test can tell a stale region apart from a dropped allocator.
struct MountedAllocation {
    std::shared_ptr<BufferAllocatorBase> allocator;
    std::unique_ptr<AllocatedBuffer> buffer;
};

MountedAllocation MountRegionAndAllocate(SegmentPool& pool,
                                         const MountedRegion& region) {
    {
        auto access = pool.AcquireWriteAccess();
        EXPECT_EQ(access.MountSegment(region.segment, region.client_id),
                  ErrorCode::OK);
    }
    auto view = pool.AcquireReadAccess();
    MountedAllocation allocation;
    allocation.allocator = view.GetAllocator(region.segment.id);
    allocation.buffer = allocation.allocator->allocate(kBufferSize);
    EXPECT_NE(allocation.buffer, nullptr);
    if (allocation.buffer != nullptr) {
        EXPECT_TRUE(
            view.BindBufferToSegment(region.segment.id, *allocation.buffer));
    }
    return allocation;
}

// A record that already missed a heartbeat, as after a client stopped serving.
std::shared_ptr<ClientLivenessRecord> SuspectedOwner(
    ClientLivenessRecord::TimePoint now = ClientLivenessRecord::TimePoint{}) {
    auto owner = std::make_shared<ClientLivenessRecord>(now);
    EXPECT_EQ(owner->Evaluate(now + std::chrono::seconds(1),
                              std::chrono::seconds(1), std::chrono::seconds(1)),
              ClientLivenessTransition::BECAME_SUSPECTED);
    return owner;
}

void ExpectOwners(const RegionCatalog& catalog,
                  const std::vector<MountedRegion>& regions) {
    ASSERT_EQ(catalog.Regions().size(), regions.size());
    for (const auto& expected : regions) {
        const auto* actual = catalog.Find(expected.segment.id);
        ASSERT_NE(actual, nullptr);
        EXPECT_EQ(actual->segment, expected.segment);
        EXPECT_EQ(actual->client_id, expected.client_id);
        EXPECT_EQ(actual->status, expected.status);
    }
}

// Construct the pre-cutover SegmentSerializer wire shape independently of the
// new pool/codec's mount and ownership validation. The cs relation is per ID;
// MountedSegment arrays did not contain an owner (or, historically, a host).
std::vector<uint8_t> LegacySnapshot(const std::vector<MountedRegion>& regions,
                                    bool include_host) {
    msgpack::sbuffer buffer;
    MsgpackPacker packer(&buffer);
    packer.pack_map(4);
    packer.pack("ma");
    packer.pack(static_cast<int32_t>(BufferAllocatorType::OFFSET));
    packer.pack("an");
    std::set<std::string> active_names;
    for (const auto& region : regions) {
        if (region.status == SegmentStatus::OK) {
            active_names.insert(region.segment.name);
        }
    }
    packer.pack(active_names);
    packer.pack("ms");
    packer.pack_map(regions.size());
    std::map<std::string, std::vector<std::string>> clients;
    for (const auto& region : regions) {
        const auto& segment = region.segment;
        const auto id = UuidToString(segment.id);
        clients[UuidToString(region.client_id)].push_back(id);
        packer.pack(id);
        packer.pack_array(include_host ? 9 : 8);
        packer.pack(id);
        packer.pack(segment.name);
        packer.pack(static_cast<uint64_t>(segment.base));
        packer.pack(static_cast<uint64_t>(segment.size));
        packer.pack(segment.te_endpoint);
        packer.pack(static_cast<int16_t>(region.status));
        packer.pack(true);
        OffsetBufferAllocator allocator(segment.name, segment.base,
                                        segment.size, segment.te_endpoint);
        EXPECT_TRUE(ha::AllocatorSnapshotCodec::Encode(
                        allocator.CaptureSnapshot(), packer)
                        .has_value());
        if (include_host) {
            packer.pack(segment.host_id);
        }
    }
    packer.pack("cs");
    packer.pack(clients);
    return zstd_compress(reinterpret_cast<const uint8_t*>(buffer.data()),
                         buffer.size(), 3);
}

}  // namespace

TEST(SegmentPoolOwnershipTest,
     NameLookupIsDeterministicAndFallsBackAfterErase) {
    std::array<size_t, 3> order{0, 1, 2};
    const std::vector<MountedRegion> regions{Region(1), Region(2), Region(3)};
    do {
        RegionCatalog catalog;
        for (const auto index : order) {
            ASSERT_EQ(catalog.Register(regions[index]), ErrorCode::OK);
        }
        ExpectOwners(catalog, regions);
        EXPECT_EQ(catalog.FindOwnerClientId(regions[0].segment.name),
                  regions[0].client_id);

        auto duplicate = regions[0];
        duplicate.client_id = regions[1].client_id;
        EXPECT_EQ(catalog.Register(duplicate),
                  ErrorCode::SEGMENT_ALREADY_EXISTS);
        EXPECT_EQ(catalog.Find(duplicate.segment.id)->client_id,
                  regions[0].client_id);

        ASSERT_TRUE(catalog.SetStatus(regions[0].segment.id,
                                      SegmentStatus::UNMOUNTING));
        EXPECT_EQ(catalog.FindOwnerClientId(regions[0].segment.name),
                  regions[1].client_id);
        ASSERT_TRUE(
            catalog.SetStatus(regions[1].segment.id, SegmentStatus::DRAINING));
        EXPECT_EQ(catalog.FindOwnerClientId(regions[0].segment.name),
                  regions[2].client_id);
        ASSERT_TRUE(catalog.Erase(regions[2].segment.id));
        // No OK region remains: use the lowest ID, not hash/insertion order.
        EXPECT_EQ(catalog.FindOwnerClientId(regions[0].segment.name),
                  regions[0].client_id);
        ASSERT_TRUE(catalog.Erase(regions[0].segment.id));
        EXPECT_EQ(catalog.FindOwnerClientId(regions[0].segment.name),
                  regions[1].client_id);
        catalog.Clear();
        EXPECT_FALSE(catalog.FindOwnerClientId(regions[0].segment.name));
    } while (std::next_permutation(order.begin(), order.end()));
}

TEST(SegmentPoolOwnershipTest,
     SameNameMountsKeepExactOwnersAndRejectIdTakeover) {
    SegmentPool pool(Drivers());
    const auto first = Region(1);
    const auto second = Region(2);
    const auto third = Region(3);
    {
        auto access = pool.AcquireWriteAccess();
        auto prepared = access.PrepareMount(third.segment, third.client_id);
        ASSERT_TRUE(prepared.has_value());
        ASSERT_EQ(access.MountSegment(first.segment, first.client_id),
                  ErrorCode::OK);
        ASSERT_EQ(access.MountSegment(second.segment, second.client_id),
                  ErrorCode::OK);
        // Another owner registered the name after preparation, not this ID.
        ASSERT_EQ(prepared->Commit(access), ErrorCode::OK);
        EXPECT_EQ(access.MountSegment(first.segment, first.client_id),
                  ErrorCode::SEGMENT_ALREADY_EXISTS);
        EXPECT_EQ(access.MountSegment(first.segment, second.client_id),
                  ErrorCode::INVALID_PARAMS);
        auto takeover = access.PrepareMount(first.segment, second.client_id);
        ASSERT_FALSE(takeover.has_value());
        EXPECT_EQ(takeover.error(), ErrorCode::INVALID_PARAMS);
        auto unmount =
            access.PrepareUnmount(first.segment.id, second.client_id);
        ASSERT_FALSE(unmount.has_value());
        EXPECT_EQ(unmount.error(), ErrorCode::INVALID_PARAMS);
        ExpectOwners(access.Catalog(), {first, second, third});
    }
    auto view = pool.AcquireReadAccess();
    const auto* entry = view.Placement().Find(first.segment.name,
                                              AllocationCandidateKind::NATIVE);
    ASSERT_NE(entry, nullptr);
    EXPECT_EQ(entry->candidates.size(), 3U);
    EXPECT_EQ(pool.GetMemoryUsage().capacity_bytes, 3 * kRegionSize);
}

TEST(SegmentPoolOwnershipTest, AdoptionAndRestoreAcceptDistinctSameNameOwners) {
    SegmentPool pool(Drivers());
    const auto first = Region(1);
    const auto second = Region(2);
    const auto third = Region(3);
    auto access = pool.AcquireWriteAccess();
    ASSERT_EQ(access.MountSegment(first.segment, first.client_id),
              ErrorCode::OK);
    auto allocator = std::make_shared<OffsetBufferAllocator>(
        second.segment.name, second.segment.base, second.segment.size,
        second.segment.te_endpoint);
    auto adopted = access.PrepareAdopt(second, allocator, true);
    ASSERT_TRUE(adopted.has_value());
    ASSERT_EQ(adopted->Commit(access), ErrorCode::OK);

    const std::vector<AllocatedBuffer::Descriptor> descriptors{
        {4096, third.segment.base, "tcp", third.segment.te_endpoint}};
    auto restored =
        access.PrepareRestore(third.segment, third.client_id, descriptors);
    ASSERT_TRUE(restored.has_value());
    ASSERT_EQ(restored->Commit(access), ErrorCode::OK);
    auto buffers = restored->TakeImportedBuffers();
    ASSERT_EQ(buffers.size(), 1U);
    EXPECT_EQ(buffers.front()->get_descriptor().transport_endpoint_,
              third.segment.te_endpoint);
    auto remounted = access.PrepareMount(second.segment, second.client_id);
    ASSERT_TRUE(remounted.has_value());
    ASSERT_EQ(remounted->Commit(access), ErrorCode::OK);
    auto takeover =
        access.PrepareRestore(third.segment, first.client_id, descriptors);
    ASSERT_FALSE(takeover.has_value());
    EXPECT_EQ(takeover.error(), ErrorCode::INVALID_PARAMS);
    ExpectOwners(access.Catalog(), {first, second, third});
}

TEST(SegmentPoolOwnershipTest,
     ClientExpiryAndUnmountDoNotAffectSameNameSibling) {
    for (bool cxl : {false, true}) {
        SCOPED_TRACE(cxl);
        SegmentPool pool(Drivers(cxl));
        const auto first = RegionOnDriver(1, cxl);
        const auto second = RegionOnDriver(2, cxl);
        auto first_allocation = MountRegionAndAllocate(pool, first);
        auto second_allocation = MountRegionAndAllocate(pool, second);
        auto& first_buffer = first_allocation.buffer;
        auto& second_buffer = second_allocation.buffer;
        ASSERT_NE(first_buffer, nullptr);
        ASSERT_NE(second_buffer, nullptr);
        const auto now = ClientLivenessRecord::TimePoint{};
        auto first_liveness = std::make_shared<ClientLivenessRecord>(now);
        auto second_liveness = std::make_shared<ClientLivenessRecord>(now);
        {
            auto access = pool.AcquireWriteAccess();
            access.BindClientLiveness(first.client_id, first_liveness);
            access.BindClientLiveness(second.client_id, second_liveness);
        }
        // Each buffer keeps its own region's identity, even though both
        // regions share one CXL allocator and one name.
        {
            auto access = pool.AcquireWriteAccess();
            ASSERT_TRUE(access.HasBufferBinding(*second_buffer));
        }
        EXPECT_TRUE(first_buffer->isAvailable());
        EXPECT_TRUE(second_buffer->isAvailable());
        EXPECT_EQ(first_liveness->Evaluate(now + std::chrono::seconds(1),
                                           std::chrono::seconds(1),
                                           std::chrono::seconds(1)),
                  ClientLivenessTransition::BECAME_SUSPECTED);
        EXPECT_EQ(first_liveness->Evaluate(now + std::chrono::seconds(2),
                                           std::chrono::seconds(1),
                                           std::chrono::seconds(1)),
                  ClientLivenessTransition::BECAME_OFFLINE);
        EXPECT_FALSE(first_buffer->isAvailable());
        EXPECT_TRUE(second_buffer->isAvailable());
        EXPECT_TRUE(second_liveness->IsServing());
        const auto kind = KindForDriver(cxl);
        auto allocated = pool.AllocateInSegment(first.segment.name, kind, 4096);
        ASSERT_TRUE(allocated.has_value());
        EXPECT_EQ(allocated->getClientLiveness(), second_liveness);
        {
            auto access = pool.AcquireWriteAccess();
            auto unmount =
                access.PrepareUnmount(first.segment.id, first.client_id);
            ASSERT_TRUE(unmount.has_value());
            EXPECT_EQ(access.Catalog().FindOwnerClientId(first.segment.name),
                      second.client_id);
            ASSERT_EQ(std::move(*unmount).Commit(access), ErrorCode::OK);
            ExpectOwners(access.Catalog(), {second});
        }
        EXPECT_TRUE(second_buffer->isAvailable());
        auto remaining =
            pool.AllocateInSegment(second.segment.name, kind, 4096);
        ASSERT_TRUE(remaining.has_value());
        EXPECT_EQ(remaining->getClientLiveness(), second_liveness);
    }
}

TEST(SegmentPoolOwnershipTest, RebindingSegmentOwnerUpdatesExistingBuffers) {
    for (bool cxl : {false, true}) {
        SCOPED_TRACE(cxl);
        SegmentPool pool(Drivers(cxl));
        const auto region = RegionOnDriver(1, cxl);
        auto allocation = MountRegionAndAllocate(pool, region);
        auto& buffer = allocation.buffer;
        ASSERT_NE(buffer, nullptr);
        const auto kind = KindForDriver(cxl);

        // Snapshot restore rebuilds client records after the buffers already
        // reference their region, so the buffers follow the new binding.
        const auto now = ClientLivenessRecord::TimePoint{};
        auto owner = SuspectedOwner(now);
        pool.AcquireWriteAccess().BindClientLiveness(region.client_id, owner);
        EXPECT_TRUE(buffer->isHandleUsable());
        EXPECT_FALSE(buffer->isAvailable());
        EXPECT_FALSE(AllocatesIn(pool, region.segment.name, kind));

        ASSERT_EQ(owner->Observe(now + std::chrono::seconds(2)),
                  ClientLivenessObservation::RECOVERED_ACTIVE);
        EXPECT_TRUE(buffer->isAvailable());
        EXPECT_TRUE(AllocatesIn(pool, region.segment.name, kind));

        // Replacing the allocator keeps the region identity, so the buffer
        // still belongs to the region whose allocator has just been replaced.
        {
            auto access = pool.AcquireWriteAccess();
            auto remount =
                access.PrepareMount(region.segment, region.client_id);
            ASSERT_TRUE(remount.has_value());
            ASSERT_EQ(remount->Commit(access), ErrorCode::OK);
            EXPECT_TRUE(access.HasBufferBinding(*buffer));
        }

        // A replacement record from recovery takes the buffers over, and the
        // record it replaced no longer gates them.
        auto restored_owner = SuspectedOwner(now);
        pool.AcquireWriteAccess().BindClientLiveness(region.client_id,
                                                     restored_owner);
        EXPECT_FALSE(buffer->isAvailable());
        EXPECT_TRUE(buffer->isHandleUsable());
        EXPECT_TRUE(owner->IsServing());
    }
}

TEST(SegmentPoolOwnershipTest, DrainRollbackAndNewMountKeepDistinctLifetimes) {
    for (bool cxl : {false, true}) {
        SCOPED_TRACE(cxl);
        SegmentPool pool(Drivers(cxl));
        const auto region = RegionOnDriver(1, cxl);
        auto allocation = MountRegionAndAllocate(pool, region);
        auto& buffer = allocation.buffer;
        ASSERT_NE(buffer, nullptr);
        const auto kind = KindForDriver(cxl);

        // Draining stops new allocations but keeps existing buffers readable.
        {
            auto access = pool.AcquireWriteAccess();
            ASSERT_EQ(access.SetSegmentStatusByName(region.segment.name,
                                                    SegmentStatus::DRAINING),
                      ErrorCode::OK);
        }
        EXPECT_TRUE(buffer->isAvailable());
        EXPECT_FALSE(AllocatesIn(pool, region.segment.name, kind));

        // A rolled back unmount leaves the region usable for its buffers.
        {
            auto access = pool.AcquireWriteAccess();
            auto unmount =
                access.PrepareUnmount(region.segment.id, region.client_id);
            ASSERT_TRUE(unmount.has_value());
            EXPECT_FALSE(buffer->isHandleUsable());
            ASSERT_EQ(std::move(*unmount).Rollback(access), ErrorCode::OK);
        }
        EXPECT_TRUE(buffer->isAvailable());

        // Reuse the same ID and name while the old allocator is still alive:
        // the fresh mount must not revive the buffers of the old region.
        {
            auto access = pool.AcquireWriteAccess();
            auto unmount =
                access.PrepareUnmount(region.segment.id, region.client_id);
            ASSERT_TRUE(unmount.has_value());
            ASSERT_EQ(std::move(*unmount).Commit(access), ErrorCode::OK);
            ASSERT_EQ(access.MountSegment(region.segment, region.client_id),
                      ErrorCode::OK);
            access.BindClientLiveness(region.client_id,
                                      std::make_shared<ClientLivenessRecord>(
                                          ClientLivenessRecord::Clock::now()));
        }
        EXPECT_FALSE(buffer->isHandleUsable());
        EXPECT_FALSE(buffer->isAvailable());
        EXPECT_TRUE(AllocatesIn(pool, region.segment.name, kind));
    }
}

TEST(SegmentPoolOwnershipTest, SnapshotRoundTripPreservesEverySameNameOwner) {
    SegmentPool source(Drivers());
    const auto first = Region(1);
    const auto second = Region(2);
    {
        auto access = source.AcquireWriteAccess();
        ASSERT_EQ(access.MountSegment(second.segment, second.client_id),
                  ErrorCode::OK);
        ASSERT_EQ(access.MountSegment(first.segment, first.client_id),
                  ErrorCode::OK);
    }
    auto snapshot = source.CaptureSnapshot();
    ASSERT_TRUE(snapshot.has_value());
    auto encoded = ha::StoreResourceSnapshotCodec::Encode(*snapshot, {});
    ASSERT_TRUE(encoded.has_value());
    auto decoded = ha::StoreResourceSnapshotCodec::Decode(*encoded);
    ASSERT_TRUE(decoded.has_value()) << decoded.error().message;
    SegmentPool restored(Drivers());
    ASSERT_EQ(restored.AcquireWriteAccess().MountSegment(first.segment,
                                                         second.client_id),
              ErrorCode::OK);
    ASSERT_TRUE(
        restored.RestoreSnapshot(std::move(decoded->segment_pool), false)
            .has_value());
    {
        auto view = restored.AcquireReadAccess();
        ExpectOwners(view.Catalog(), {first, second});
        EXPECT_EQ(view.Catalog().FindOwnerClientId(first.segment.name),
                  first.client_id);
        auto* entry = view.Placement().Find(first.segment.name,
                                            AllocationCandidateKind::NATIVE);
        ASSERT_NE(entry, nullptr);
        EXPECT_EQ(entry->candidates.size(), 2U);
    }
    auto access = restored.AcquireWriteAccess();
    auto unmount = access.PrepareUnmount(first.segment.id, first.client_id);
    ASSERT_TRUE(unmount.has_value());
    ASSERT_EQ(std::move(*unmount).Commit(access), ErrorCode::OK);
    EXPECT_EQ(access.Catalog().FindOwnerClientId(first.segment.name),
              second.client_id);
    ExpectOwners(access.Catalog(), {second});
}

TEST(SegmentPoolOwnershipTest,
     RestoresLegacySharedNamesWithMixedLifecycleStates) {
    for (bool include_host : {false, true}) {
        SCOPED_TRACE(include_host);
        std::vector<MountedRegion> regions{
            Region(1, SegmentStatus::UNMOUNTING),
            Region(2, SegmentStatus::DRAINING),
            Region(3, SegmentStatus::DRAINED),
            Region(4, SegmentStatus::GRACEFULLY_UNMOUNTING),
            Region(5, SegmentStatus::UNDEFINED),
            Region(6),
            Region(7)};
        const auto bytes = LegacySnapshot(regions, include_host);
        auto decoded = ha::StoreResourceSnapshotCodec::Decode(bytes);
        ASSERT_TRUE(decoded.has_value()) << decoded.error().message;
        EXPECT_EQ(decoded->segment_pool.active_names,
                  (std::vector<std::string>{regions.front().segment.name}));
        EXPECT_TRUE(decoded->local_ssd.empty());
        SegmentPool restored(Drivers());
        ASSERT_TRUE(
            restored.RestoreSnapshot(std::move(decoded->segment_pool), false)
                .has_value());
        if (!include_host) {
            for (auto& region : regions) region.segment.host_id.clear();
        }
        auto view = restored.AcquireReadAccess();
        ExpectOwners(view.Catalog(), regions);
        EXPECT_EQ(view.Catalog().FindOwnerClientId(regions[0].segment.name),
                  regions[5].client_id);
        const auto* entry = view.Placement().Find(
            regions[0].segment.name, AllocationCandidateKind::NATIVE);
        ASSERT_NE(entry, nullptr);
        EXPECT_EQ(entry->candidates.size(), 2U);
    }
}

TEST(SegmentPoolOwnershipTest, LegacySnapshotStillRejectsTwoOwnersForOneId) {
    const auto bytes =
        zstd_decompress(LegacySnapshot({Region(1), Region(2)}, true));
    auto unpacked = msgpack::unpack(reinterpret_cast<const char*>(bytes.data()),
                                    bytes.size());
    auto root = unpacked.get();
    for (uint32_t i = 0; i < root.via.map.size; ++i) {
        auto& field = root.via.map.ptr[i];
        if (field.key.as<std::string>() == "cs") {
            // Both distinct client UUIDs now claim the first region ID.
            field.val.via.map.ptr[1].val = field.val.via.map.ptr[0].val;
        }
    }
    msgpack::sbuffer buffer;
    msgpack::pack(buffer, root);
    auto corrupted = zstd_compress(
        reinterpret_cast<const uint8_t*>(buffer.data()), buffer.size(), 3);
    auto decoded = ha::StoreResourceSnapshotCodec::Decode(corrupted);
    ASSERT_FALSE(decoded.has_value());
    EXPECT_EQ(decoded.error().code, ErrorCode::DESERIALIZE_FAIL);
    EXPECT_EQ(decoded.error().message,
              "snapshot contains invalid region ownership");
}

}  // namespace mooncake::test

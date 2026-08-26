#include "nof_segment_manager.h"
#include "placement/domain.h"
#include "segment/pool.h"
#include "segment/pool_write_access.h"
#include "segment/pool_read_access.h"

#include <gtest/gtest.h>
#include <msgpack.hpp>

#include <chrono>
#include <future>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "ha/snapshot/store_resource_snapshot_codec.h"
#include "master_metric_manager.h"
#include "segment/region_initial_state.h"
#include "test_buffer_allocator.h"
#include "utils/zstd_util.h"

namespace mooncake {
namespace {

constexpr size_t kRegionSize = 16U * 1024 * 1024;

RegionDriverRegistry OffsetDrivers(bool enable_cxl = false) {
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

tl::expected<std::vector<Replica>, ErrorCode> Allocate(
    SegmentPool& pool, PlacementPolicyType policy_type,
    const ReplicaAllocationRequest& request,
    PlacementDiagnostics* diagnostics = nullptr) {
    ReplicaPlacement placement(pool, policy_type);
    return placement.Allocate(request, diagnostics);
}

std::vector<uint8_t> ReplaceSnapshotActiveNames(
    const std::vector<uint8_t>& snapshot,
    const std::vector<std::string>& active_names) {
    const auto decompressed = zstd_decompress(snapshot);
    const auto unpacked =
        msgpack::unpack(reinterpret_cast<const char*>(decompressed.data()),
                        decompressed.size());
    const auto& root = unpacked.get();
    EXPECT_EQ(root.type, msgpack::type::MAP);

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

class SegmentTest : public ::testing::Test {
   protected:
    const RegionResource* GetResource(const SegmentPool& pool,
                                      const UUID& id) const {
        auto mounted = pool.catalog_.mounted_regions_.find(id);
        return mounted == pool.catalog_.mounted_regions_.end()
                   ? nullptr
                   : pool.GetResource(mounted->second);
    }
};

TEST_F(SegmentTest, MemoryDriverLifecycleAndPrepareRollback) {
    MemoryRegionDriver driver(BufferAllocatorType::OFFSET);
    RegionResourceSpec spec{generate_uuid(), "memory", 0x100000000ULL,
                            kRegionSize, "memory-endpoint"};

    {
        auto prepared = driver.PrepareOpen(spec, {});
        ASSERT_TRUE(prepared.has_value());
        ASSERT_NE(prepared->resource(), nullptr);
        EXPECT_EQ(driver.GetResource(spec.id), nullptr);
    }
    EXPECT_EQ(driver.GetResource(spec.id), nullptr);

    auto prepared = driver.PrepareOpen(spec, {});
    ASSERT_TRUE(prepared.has_value());
    prepared->Commit();
    auto* resource = driver.GetResource(spec.id);
    ASSERT_NE(resource, nullptr);
    EXPECT_TRUE(resource->active);
    EXPECT_TRUE(driver.Deactivate(spec.id));
    EXPECT_FALSE(resource->active);
    EXPECT_TRUE(driver.Reactivate(spec.id));
    EXPECT_TRUE(resource->active);
    EXPECT_TRUE(driver.Erase(spec.id));
    EXPECT_EQ(driver.GetResource(spec.id), nullptr);
}

TEST_F(SegmentTest, ReplacementRollbackKeepsCommittedResource) {
    MemoryRegionDriver driver(BufferAllocatorType::OFFSET);
    RegionResourceSpec spec{generate_uuid(), "memory", 0x100000000ULL,
                            kRegionSize, "memory-endpoint"};
    auto first = driver.PrepareOpen(spec, {});
    ASSERT_TRUE(first.has_value());
    first->Commit();
    auto* committed = driver.GetResource(spec.id);
    ASSERT_NE(committed, nullptr);

    {
        auto replacement = driver.PrepareOpen(spec, {});
        ASSERT_TRUE(replacement.has_value());
        EXPECT_NE(replacement->resource(), committed);
    }
    EXPECT_EQ(driver.GetResource(spec.id), committed);
    EXPECT_TRUE(driver.Erase(spec.id));
}

TEST_F(SegmentTest, InitialStateValidatesAliasesBoundsAndPreservesOrder) {
    RegionResourceSpec spec{generate_uuid(), "logical", 0x200000000ULL,
                            kRegionSize, "transport"};
    std::vector<AllocatedBuffer::Descriptor> descriptors{
        {4096, spec.base + 8192, "tcp", spec.name},
        {4096, spec.base, "tcp", spec.transport_endpoint}};
    auto state = BuildRegionInitialState(spec, descriptors);
    ASSERT_TRUE(state.has_value());
    ASSERT_EQ(state->allocations.size(), 2U);
    EXPECT_EQ(state->allocations[0].offset_bytes, 8192U);
    EXPECT_EQ(state->allocations[1].offset_bytes, 0U);
    EXPECT_EQ(state->allocations[0].requested_bytes, 4096U);

    auto bad_endpoint = descriptors;
    bad_endpoint[0].transport_endpoint_ = "other";
    EXPECT_EQ(BuildRegionInitialState(spec, bad_endpoint).error(),
              ErrorCode::INVALID_PARAMS);
    auto out_of_bounds = descriptors;
    out_of_bounds[0].buffer_address_ = spec.base + spec.size - 1024;
    EXPECT_EQ(BuildRegionInitialState(spec, out_of_bounds).error(),
              ErrorCode::INVALID_PARAMS);

    // Structural validation deliberately leaves overlap/alignment decisions
    // to the concrete allocator import.
    auto overlapping = descriptors;
    overlapping[0].buffer_address_ = spec.base;
    EXPECT_TRUE(BuildRegionInitialState(spec, overlapping).has_value());
}

TEST_F(SegmentTest, OffsetImportReturnsBuffersInDescriptorOrder) {
    MemoryRegionDriver driver(BufferAllocatorType::OFFSET);
    RegionResourceSpec spec{generate_uuid(), "logical", 0x300000000ULL,
                            kRegionSize, "transport"};
    RegionInitialState state{{{8192, 4096}, {0, 4096}}};
    auto prepared = driver.PrepareOpen(spec, state);
    ASSERT_TRUE(prepared.has_value());
    ASSERT_EQ(prepared->imported_buffers().size(), 2U);
    EXPECT_EQ(
        reinterpret_cast<uintptr_t>(prepared->imported_buffers()[0]->data()),
        spec.base + 8192);
    EXPECT_EQ(
        reinterpret_cast<uintptr_t>(prepared->imported_buffers()[1]->data()),
        spec.base);
    prepared->Commit();
    auto buffers = prepared->TakeImportedBuffers();
    buffers.clear();
    EXPECT_TRUE(driver.Erase(spec.id));
}

TEST_F(SegmentTest, ConcreteAllocatorRejectsOverlappingImport) {
    MemoryRegionDriver driver(BufferAllocatorType::OFFSET);
    RegionResourceSpec spec{generate_uuid(), "logical", 0x300000000ULL,
                            kRegionSize, "transport"};
    RegionInitialState overlap{{{0, 4096}, {0, 4096}}};
    auto prepared = driver.PrepareOpen(spec, overlap);
    EXPECT_FALSE(prepared.has_value());
    EXPECT_EQ(prepared.error(), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(driver.GetResource(spec.id), nullptr);
}

TEST_F(SegmentTest, CxlDriverRejectsLiveInitialState) {
    CxlRegionDriver driver("cxl-test", kRegionSize);
    RegionResourceSpec spec{generate_uuid(), "binding", 0, kRegionSize,
                            "transport"};
    RegionInitialState live{{{0, 4096}}};
    auto prepared = driver.PrepareOpen(spec, live);
    EXPECT_FALSE(prepared.has_value());
    EXPECT_EQ(prepared.error(), ErrorCode::UNAVAILABLE_IN_CURRENT_MODE);
    EXPECT_EQ(driver.GetResource(spec.id), nullptr);
}

TEST_F(SegmentTest, PoolClassifiesOnlyCxlProtocolAsCxl) {
    SegmentPool pool(OffsetDrivers(true));
    const UUID client = generate_uuid();
    auto host = MakeSegment(0, "host", "rdma");
    auto cxl = MakeSegment(1, "cxl", "cxl");
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.MountSegment(host, client), ErrorCode::OK);
        ASSERT_EQ(access.MountSegment(cxl, client), ErrorCode::OK);
    }
    MountedRegion mounted;
    ASSERT_EQ(
        pool.AcquireReadAccess().Catalog().GetMountedRegion(host.id, mounted),
        ErrorCode::OK);
    EXPECT_EQ(mounted.kind, RegionKind::HOST_MEMORY);
    ASSERT_EQ(
        pool.AcquireReadAccess().Catalog().GetMountedRegion(cxl.id, mounted),
        ErrorCode::OK);
    EXPECT_EQ(mounted.kind, RegionKind::CXL);
    ASSERT_NE(GetResource(pool, cxl.id), nullptr);
    EXPECT_EQ(GetResource(pool, cxl.id)->target.kind(),
              AllocationTargetKind::CXL);

    CommitUnmount(pool, host, client);
    CommitUnmount(pool, cxl, client);
}

TEST_F(SegmentTest, CxlResourceActivityIsScopedToLogicalBinding) {
    SegmentPool pool(OffsetDrivers(true));
    const UUID client = generate_uuid();
    auto first = MakeSegment(0, "cxl-binding-a", "cxl");
    auto second = MakeSegment(1, "cxl-binding-b", "cxl");
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.MountSegment(first, client), ErrorCode::OK);
        ASSERT_EQ(access.MountSegment(second, client), ErrorCode::OK);
    }

    auto first_allocator =
        pool.AcquireReadAccess().Resources().GetAllocator(first.id);
    auto second_allocator =
        pool.AcquireReadAccess().Resources().GetAllocator(second.id);
    ASSERT_NE(first_allocator, nullptr);
    EXPECT_EQ(first_allocator, second_allocator);
    std::optional<RegionUnmountTxn> transaction;
    {
        auto access = pool.AcquireWriteAccess();
        auto prepared = access.PrepareUnmount(first.id, client);
        ASSERT_TRUE(prepared.has_value());
        transaction.emplace(std::move(*prepared));
    }
    {
        auto view = pool.AcquireReadAccess();
        EXPECT_TRUE(view.Resources().IsInactive(first_allocator, first.name));
        EXPECT_FALSE(
            view.Resources().IsInactive(second_allocator, second.name));
    }

    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.CommitUnmount(*transaction), ErrorCode::OK);
    }
    CommitUnmount(pool, second, client);
}

TEST_F(SegmentTest, PoolPrepareRollbackDoesNotPublishCatalogOrPlacement) {
    SegmentPool pool(OffsetDrivers());
    auto segment = MakeSegment(0, "rollback");
    const UUID client = generate_uuid();
    {
        auto access = pool.AcquireWriteAccess();
        auto prepared = access.PrepareMount(segment, client);
        ASSERT_TRUE(prepared.has_value());
        Segment ignored;
        EXPECT_FALSE(access.GetSegment(segment.id, ignored));
    }
    std::vector<std::string> active_groups;
    pool.AcquireReadAccess().Placement().GetActiveGroupNames(active_groups);
    EXPECT_TRUE(active_groups.empty());
}

TEST_F(SegmentTest, BatchRemountValidationRejectsDuplicateRegionIds) {
    SegmentPool pool(OffsetDrivers());
    const UUID client = generate_uuid();
    auto segment = MakeSegment(0, "duplicate");
    std::vector<Segment> segments{segment, segment};

    auto access = pool.AcquireWriteAccess();
    EXPECT_EQ(access.ValidateRemount(segments, client),
              ErrorCode::INVALID_PARAMS);
}

TEST_F(SegmentTest, SnapshotRoundTripPreservesMountedRegionHostId) {
    SegmentPool source(OffsetDrivers());
    auto segment = MakeSegment(0, "snapshot-host", "tcp", "host-a");
    const UUID client = generate_uuid();
    ASSERT_EQ(source.AcquireWriteAccess().MountSegment(segment, client),
              ErrorCode::OK);

    auto encoded = ha::StoreResourceSnapshotCodec::Encode(
        source, LocalSsdPersistedState{});
    ASSERT_TRUE(encoded.has_value()) << encoded.error().message;

    SegmentPool restored(OffsetDrivers());
    auto stale = MakeSegment(1, "stale-snapshot-region");
    ASSERT_EQ(restored.AcquireWriteAccess().MountSegment(stale, client),
              ErrorCode::OK);
    auto decoded =
        ha::StoreResourceSnapshotCodec::Decode(restored, *encoded, false);
    ASSERT_TRUE(decoded.has_value()) << decoded.error().message;

    MountedRegion mounted;
    EXPECT_EQ(restored.AcquireReadAccess().Catalog().GetMountedRegion(stale.id,
                                                                      mounted),
              ErrorCode::SEGMENT_NOT_FOUND);
    ASSERT_EQ(restored.AcquireReadAccess().Catalog().GetMountedRegion(
                  segment.id, mounted),
              ErrorCode::OK);
    EXPECT_EQ(mounted.segment.host_id, segment.host_id);
    EXPECT_EQ(mounted.client_id, client);
    EXPECT_EQ(mounted.status, SegmentStatus::OK);

    restored.AcquireWriteAccess().Clear();
    CommitUnmount(source, segment, client);
}

TEST_F(SegmentTest, SnapshotRejectsInconsistentActiveNamesWithoutMutation) {
    SegmentPool source(OffsetDrivers());
    const UUID client = generate_uuid();
    auto segment = MakeSegment(0, "snapshot-active");
    ASSERT_EQ(source.AcquireWriteAccess().MountSegment(segment, client),
              ErrorCode::OK);

    auto encoded = ha::StoreResourceSnapshotCodec::Encode(
        source, LocalSsdPersistedState{});
    ASSERT_TRUE(encoded.has_value()) << encoded.error().message;

    const auto expect_rejected = [&](const std::vector<std::string>& names) {
        SegmentPool restored(OffsetDrivers());
        auto stale = MakeSegment(1, "snapshot-stale");
        ASSERT_EQ(restored.AcquireWriteAccess().MountSegment(stale, client),
                  ErrorCode::OK);

        auto corrupt = ReplaceSnapshotActiveNames(*encoded, names);
        auto result =
            ha::StoreResourceSnapshotCodec::Decode(restored, corrupt, false);
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error().code, ErrorCode::DESERIALIZE_FAIL);

        MountedRegion mounted;
        EXPECT_EQ(restored.AcquireReadAccess().Catalog().GetMountedRegion(
                      stale.id, mounted),
                  ErrorCode::OK);
        EXPECT_EQ(restored.AcquireReadAccess().Catalog().GetMountedRegion(
                      segment.id, mounted),
                  ErrorCode::SEGMENT_NOT_FOUND);
    };

    expect_rejected({});
    expect_rejected({segment.name, segment.name});
    expect_rejected({segment.name, "snapshot-unknown"});

    CommitUnmount(source, segment, client);
}

TEST_F(SegmentTest, MountReplacementPublishesStableNewTarget) {
    SegmentPool pool(OffsetDrivers());
    auto segment = MakeSegment(0, "replace");
    const UUID client = generate_uuid();
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.MountSegment(segment, client), ErrorCode::OK);
    }
    const AllocationTarget* old_target = nullptr;
    {
        old_target = &GetResource(pool, segment.id)->target;
    }
    {
        auto access = pool.AcquireWriteAccess();
        auto prepared = access.PrepareMount(segment, client);
        ASSERT_TRUE(prepared.has_value());
        access.CommitMount(*prepared);
    }
    {
        ASSERT_NE(GetResource(pool, segment.id), nullptr);
        EXPECT_NE(&GetResource(pool, segment.id)->target, old_target);
    }
    CommitUnmount(pool, segment, client);
}

TEST_F(SegmentTest, UnmountHidesThenRollbackReactivatesResource) {
    SegmentPool pool(OffsetDrivers());
    auto segment = MakeSegment(0, "lifecycle", "tcp", "host-a");
    const UUID client = generate_uuid();
    std::optional<RegionUnmountTxn> transaction;
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.MountSegment(segment, client), ErrorCode::OK);
        auto prepared = access.PrepareUnmount(segment.id, client);
        ASSERT_TRUE(prepared.has_value());
        transaction.emplace(std::move(*prepared));
    }
    {
        ASSERT_NE(GetResource(pool, segment.id), nullptr);
        EXPECT_FALSE(GetResource(pool, segment.id)->active);
    }
    {
        std::vector<std::string> active_groups;
        pool.AcquireReadAccess().Placement().GetActiveGroupNames(active_groups);
        EXPECT_TRUE(active_groups.empty());
    }
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.RollbackUnmount(*transaction), ErrorCode::OK);
    }
    EXPECT_TRUE(GetResource(pool, segment.id)->active);
    {
        std::vector<std::string> active_groups;
        pool.AcquireReadAccess().Placement().GetActiveGroupNames(active_groups);
        EXPECT_EQ(active_groups, std::vector<std::string>{segment.name});
    }
    CommitUnmount(pool, segment, client);
}

TEST_F(SegmentTest, UnmountRejectsNonOwnerBeforeChangingPlacement) {
    SegmentPool pool(OffsetDrivers());
    auto segment = MakeSegment(0, "owner-check");
    const UUID owner = generate_uuid();
    ASSERT_EQ(pool.AcquireWriteAccess().MountSegment(segment, owner),
              ErrorCode::OK);

    {
        auto access = pool.AcquireWriteAccess();
        auto prepared = access.PrepareUnmount(segment.id, generate_uuid());
        ASSERT_FALSE(prepared.has_value());
        EXPECT_EQ(prepared.error(), ErrorCode::INVALID_PARAMS);
        EXPECT_TRUE(access.IsSegmentAllocatable(segment.name));
    }
    ASSERT_NE(GetResource(pool, segment.id), nullptr);
    EXPECT_TRUE(GetResource(pool, segment.id)->active);
    CommitUnmount(pool, segment, owner);
}

TEST_F(SegmentTest, GracefulUnmountRetainsResourceUntilFinalCommit) {
    SegmentPool pool(OffsetDrivers());
    auto segment = MakeSegment(0, "graceful");
    const UUID client = generate_uuid();
    std::shared_ptr<BufferAllocatorBase> allocator;
    std::optional<RegionUnmountTxn> transaction;
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.MountSegment(segment, client), ErrorCode::OK);
        ASSERT_EQ(access.PrepareGracefulUnmountSegment(segment.id, client),
                  ErrorCode::OK);
    }
    {
        ASSERT_NE(GetResource(pool, segment.id), nullptr);
        EXPECT_FALSE(GetResource(pool, segment.id)->active);
        auto view = pool.AcquireReadAccess();
        allocator = view.Resources().GetAllocator(segment.id);
        ASSERT_NE(allocator, nullptr);
        EXPECT_FALSE(view.Resources().IsInactive(allocator, segment.name));
    }
    {
        auto access = pool.AcquireWriteAccess();
        auto prepared = access.PrepareUnmount(segment.id, client);
        ASSERT_TRUE(prepared.has_value());
        transaction.emplace(std::move(*prepared));
    }
    {
        auto view = pool.AcquireReadAccess();
        EXPECT_TRUE(view.Resources().IsInactive(allocator, segment.name));
    }
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.CommitUnmount(*transaction), ErrorCode::OK);
    }
    EXPECT_EQ(GetResource(pool, segment.id), nullptr);
}

TEST_F(SegmentTest, HostOrderingUsesGroupPointersAndTracksSameNameTargets) {
    SegmentPool pool(OffsetDrivers());
    const UUID client = generate_uuid();
    auto remote = MakeSegment(0, "remote", "tcp", "host-b");
    auto local0 = MakeSegment(1, "local", "tcp", "host-a");
    auto local1 = MakeSegment(2, "local", "tcp", "host-a");
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.MountSegment(remote, client), ErrorCode::OK);
        ASSERT_EQ(access.MountSegment(local0, client), ErrorCode::OK);
        ASSERT_EQ(access.MountSegment(local1, client), ErrorCode::OK);
    }
    const ReplicaAllocationRequest request{
        .size = 4096,
        .replica_count = 1,
        .preferred_group = {},
        .preferred_groups = {},
        .excluded_groups = {},
        .replica_type = ReplicaType::MEMORY,
        .writer_host_id = "host-a",
        .object_key = "key",
    };
    auto allocated = Allocate(pool, PlacementPolicyType::LOCAL_FIRST, request);
    ASSERT_TRUE(allocated.has_value());
    ASSERT_EQ(allocated->size(), 1U);
    const auto endpoint = allocated->front()
                              .get_descriptor()
                              .get_memory_descriptor()
                              .buffer_descriptor.transport_endpoint_;
    EXPECT_TRUE(endpoint == local0.te_endpoint ||
                endpoint == local1.te_endpoint);
    {
        auto view = pool.AcquireReadAccess();
        size_t used = 0;
        size_t capacity = 0;
        ASSERT_EQ(view.Resources().QueryGroup("local", used, capacity),
                  ErrorCode::OK);
        EXPECT_EQ(capacity, 2 * kRegionSize);
        size_t local0_used = 0;
        size_t local0_capacity = 0;
        size_t local1_used = 0;
        size_t local1_capacity = 0;
        ASSERT_EQ(view.Resources().QueryRegion(local0.id, local0_used,
                                               local0_capacity),
                  ErrorCode::OK);
        ASSERT_EQ(view.Resources().QueryRegion(local1.id, local1_used,
                                               local1_capacity),
                  ErrorCode::OK);
        EXPECT_EQ(local0_capacity, kRegionSize);
        EXPECT_EQ(local1_capacity, kRegionSize);
        EXPECT_EQ(local0_used + local1_used, used);
    }
    CommitUnmount(pool, local0, client);
    {
        auto view = pool.AcquireReadAccess();
        size_t used = 0;
        size_t capacity = 0;
        ASSERT_EQ(view.Resources().QueryGroup("local", used, capacity),
                  ErrorCode::OK);
        EXPECT_EQ(capacity, kRegionSize);
    }
    CommitUnmount(pool, local1, client);
    CommitUnmount(pool, remote, client);
}

TEST_F(SegmentTest, SameNameRegionsShareLifecycleAndMetrics) {
    SegmentPool pool(OffsetDrivers());
    const UUID client = generate_uuid();
    auto first = MakeSegment(0, "same-name-lifecycle", "tcp", "host-a");
    auto second = MakeSegment(1, "same-name-lifecycle", "tcp", "host-b");
    std::shared_ptr<BufferAllocatorBase> first_allocator;
    std::shared_ptr<BufferAllocatorBase> second_allocator;
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.MountSegment(first, client), ErrorCode::OK);
        ASSERT_EQ(access.MountSegment(second, client), ErrorCode::OK);
    }

    {
        auto view = pool.AcquireReadAccess();
        first_allocator = view.Resources().GetAllocator(first.id);
        second_allocator = view.Resources().GetAllocator(second.id);
        ASSERT_NE(first_allocator, nullptr);
        ASSERT_NE(second_allocator, nullptr);
        std::vector<std::string> names;
        view.Catalog().GetAllGroupNames(names);
        EXPECT_EQ(names, std::vector<std::string>{first.name});
    }

    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(
            access.SetSegmentStatusByName(first.name, SegmentStatus::DRAINING),
            ErrorCode::OK);
        EXPECT_FALSE(access.IsSegmentAllocatable(first.name));
    }
    {
        auto view = pool.AcquireReadAccess();
        SegmentStatus status = SegmentStatus::UNDEFINED;
        ASSERT_EQ(view.Catalog().GetRegionStatus(first.id, status),
                  ErrorCode::OK);
        EXPECT_EQ(status, SegmentStatus::DRAINING);
        ASSERT_EQ(view.Catalog().GetRegionStatus(second.id, status),
                  ErrorCode::OK);
        EXPECT_EQ(status, SegmentStatus::DRAINING);
        EXPECT_FALSE(view.Resources().IsInactive(first_allocator, first.name));
        EXPECT_FALSE(
            view.Resources().IsInactive(second_allocator, second.name));
    }

    {
        auto access = pool.AcquireWriteAccess();
        SegmentStatus status = SegmentStatus::UNDEFINED;
        ASSERT_EQ(access.SetSegmentStatusByName(first.name, SegmentStatus::OK),
                  ErrorCode::OK);
        EXPECT_TRUE(access.IsSegmentAllocatable(first.name));
        ASSERT_EQ(access.PrepareGracefulUnmountSegment(second.id, client),
                  ErrorCode::OK);
        EXPECT_TRUE(access.IsSegmentAllocatable(first.name));
        ASSERT_EQ(access.GetSegmentStatusByName(first.name, status),
                  ErrorCode::OK);
        EXPECT_EQ(status, SegmentStatus::OK);
    }

    auto& metrics = MasterMetricManager::instance();
    EXPECT_EQ(metrics.get_segment_total_mem_capacity(first.name),
              2 * static_cast<int64_t>(kRegionSize));
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.FinalizeGracefulUnmount(second.id, client),
                  ErrorCode::OK);
        EXPECT_TRUE(access.ExistsSegmentName(first.name));
    }
    EXPECT_EQ(metrics.get_segment_total_mem_capacity(first.name),
              static_cast<int64_t>(kRegionSize));

    CommitUnmount(pool, first, client);
    EXPECT_EQ(metrics.get_segment_total_mem_capacity(first.name), 0);
}

TEST_F(SegmentTest, FailedAllocationReportsEnoughLogicalGroups) {
    SegmentPool pool(OffsetDrivers());
    const UUID client = generate_uuid();
    for (size_t i = 0; i < 3; ++i) {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.MountSegment(
                      MakeSegment(i, "group-" + std::to_string(i)), client),
                  ErrorCode::OK);
    }

    const ReplicaAllocationRequest fill_request{
        .size = 12U * 1024 * 1024,
        .replica_count = 3,
        .preferred_group = {},
        .preferred_groups = {},
        .excluded_groups = {},
        .replica_type = ReplicaType::MEMORY,
        .writer_host_id = {},
        .object_key = {},
    };
    auto filled = Allocate(pool, PlacementPolicyType::RANDOM, fill_request);
    ASSERT_TRUE(filled.has_value());

    const ReplicaAllocationRequest exhausted_request{
        .size = 6U * 1024 * 1024,
        .replica_count = 3,
        .preferred_group = {},
        .preferred_groups = {},
        .excluded_groups = {},
        .replica_type = ReplicaType::MEMORY,
        .writer_host_id = {},
        .object_key = {},
    };
    PlacementDiagnostics diagnostics;
    auto exhausted = Allocate(pool, PlacementPolicyType::RANDOM,
                              exhausted_request, &diagnostics);
    EXPECT_FALSE(exhausted.has_value());
    EXPECT_EQ(exhausted.error(), ErrorCode::NO_AVAILABLE_HANDLE);
    EXPECT_TRUE(diagnostics.has_sufficient_active_group_count);
}

TEST_F(SegmentTest, NoFPlacementUsesReplicaPlacementDomain) {
    NoFSegmentManager manager(BufferAllocatorType::OFFSET);
    NoFSegment segment;
    segment.id = generate_uuid();
    segment.name = "nof-external-allocator";
    segment.base = 0x200000000ULL;
    segment.size = kRegionSize;
    segment.te_endpoint = "nof-endpoint";
    const UUID client = generate_uuid();
    {
        auto access = manager.AcquireWriteAccess();
        ASSERT_EQ(access.MountSegment(segment, client), ErrorCode::OK);
    }

    {
        const ReplicaAllocationRequest request{
            .size = 4096,
            .replica_count = 1,
            .preferred_group = segment.name,
            .preferred_groups = {},
            .excluded_groups = {},
            .replica_type = ReplicaType::NOF_SSD,
            .writer_host_id = {},
            .object_key = {},
        };
        ReplicaPlacement placement(manager, PlacementPolicyType::RANDOM);
        auto allocated = placement.Allocate(request);
        ASSERT_TRUE(allocated.has_value());
        ASSERT_EQ(allocated->size(), 1U);
        EXPECT_TRUE(allocated->front().is_nof_replica());
        EXPECT_EQ(allocated->front()
                      .get_descriptor()
                      .get_nof_descriptor()
                      .buffer_descriptor.transport_endpoint_,
                  segment.te_endpoint);
    }

    auto access = manager.AcquireWriteAccess();
    ASSERT_EQ(access.PrepareUnmountSegment(segment.id, client), ErrorCode::OK);
    EXPECT_EQ(access.CommitUnmountSegment(segment.id, client), ErrorCode::OK);
}

TEST_F(SegmentTest, NoFRemountRejectsRegionWhileUnmounting) {
    NoFSegmentManager manager(BufferAllocatorType::OFFSET);
    NoFSegment segment;
    segment.id = generate_uuid();
    segment.name = "nof-unmounting";
    segment.base = 0x220000000ULL;
    segment.size = kRegionSize;
    segment.te_endpoint = "nof-unmounting-endpoint";
    const UUID client = generate_uuid();

    auto access = manager.AcquireWriteAccess();
    ASSERT_EQ(access.MountSegment(segment, client), ErrorCode::OK);
    ASSERT_EQ(access.PrepareUnmountSegment(segment.id, client), ErrorCode::OK);
    EXPECT_EQ(access.MountSegment(segment, client),
              ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    EXPECT_EQ(access.ReMountSegment({segment}, client),
              ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    EXPECT_EQ(access.CommitUnmountSegment(segment.id, client), ErrorCode::OK);
}

TEST_F(SegmentTest, AllocationHoldsPoolReadLockAcrossAllocatorCall) {
    SegmentPool pool(OffsetDrivers());
    auto segment = MakeSegment(0, "blocking");
    const UUID client = generate_uuid();
    auto allocator = std::make_shared<test::TestBufferAllocator>(
        segment.name, segment.te_endpoint, segment.size, segment.base);
    allocator->BlockNext();
    auto allocation_started = allocator->AllocationStarted();
    {
        auto access = pool.AcquireWriteAccess();
        MountedRegion mounted{segment, client, SegmentStatus::OK,
                              RegionKind::HOST_MEMORY};
        auto prepared = access.PrepareAdopt(mounted, allocator, true);
        ASSERT_TRUE(prepared.has_value());
        access.CommitMount(*prepared);
    }

    auto allocation = std::async(std::launch::async, [&] {
        const ReplicaAllocationRequest request{
            .size = 4096,
            .replica_count = 1,
            .preferred_group = {},
            .preferred_groups = {},
            .excluded_groups = {},
            .replica_type = ReplicaType::MEMORY,
            .writer_host_id = {},
            .object_key = {},
        };
        return Allocate(pool, PlacementPolicyType::RANDOM, request);
    });
    ASSERT_EQ(allocation_started.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);

    auto unmount = std::async(std::launch::async, [&] {
        auto access = pool.AcquireWriteAccess();
        return access.PrepareUnmount(segment.id, client);
    });
    EXPECT_EQ(unmount.wait_for(std::chrono::milliseconds(50)),
              std::future_status::timeout);
    allocator->AllowAllocation();
    auto allocated = allocation.get();
    ASSERT_TRUE(allocated.has_value());
    auto transaction = unmount.get();
    ASSERT_TRUE(transaction.has_value());

    auto access = pool.AcquireWriteAccess();
    EXPECT_EQ(access.RollbackUnmount(*transaction), ErrorCode::OK);
}

TEST_F(SegmentTest, CatalogReadViewAllowsConcurrentAllocation) {
    SegmentPool pool(OffsetDrivers());
    auto segment = MakeSegment(0, "read-view");
    const UUID client = generate_uuid();
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.MountSegment(segment, client), ErrorCode::OK);
    }

    auto view = pool.AcquireReadAccess();
    auto allocation = std::async(std::launch::async, [&] {
        const ReplicaAllocationRequest request{
            .size = 4096,
            .replica_count = 1,
            .preferred_group = {},
            .preferred_groups = {},
            .excluded_groups = {},
            .replica_type = ReplicaType::MEMORY,
            .writer_host_id = {},
            .object_key = {},
        };
        return Allocate(pool, PlacementPolicyType::RANDOM, request);
    });
    ASSERT_EQ(allocation.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    EXPECT_TRUE(allocation.get().has_value());
}

TEST_F(SegmentTest, QueryAndClientIndexesFollowLifecycle) {
    SegmentPool pool(OffsetDrivers());
    auto a = MakeSegment(0, "a");
    auto b = MakeSegment(1, "b");
    const UUID client = generate_uuid();
    {
        auto access = pool.AcquireWriteAccess();
        ASSERT_EQ(access.MountSegment(a, client), ErrorCode::OK);
        ASSERT_EQ(access.MountSegment(b, client), ErrorCode::OK);
    }
    {
        auto view = pool.AcquireReadAccess();
        std::vector<Segment> segments;
        ASSERT_EQ(view.Catalog().GetClientSegments(client, segments),
                  ErrorCode::OK);
        EXPECT_EQ(segments.size(), 2U);
        size_t used = 1;
        size_t capacity = 0;
        ASSERT_EQ(view.Resources().QueryGroup(a.name, used, capacity),
                  ErrorCode::OK);
        EXPECT_EQ(used, 0U);
        EXPECT_EQ(capacity, a.size);
    }
    CommitUnmount(pool, a, client);
    CommitUnmount(pool, b, client);
}

}  // namespace mooncake

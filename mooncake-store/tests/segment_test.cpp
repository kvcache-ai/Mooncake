#include "nof_segment_manager.h"
#include "segment/pool.h"
#include "segment/pool_access.h"
#include "segment/pool_view.h"

#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "ha/snapshot/segment_pool_snapshot_codec.h"
#include "master_metric_manager.h"
#include "segment/region_initial_state.h"
#include "test_buffer_allocator.h"

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
    auto access = pool.getSegmentPoolAccess();
    size_t capacity = 0;
    ASSERT_EQ(access.PrepareUnmountSegment(segment.id, capacity),
              ErrorCode::OK);
    ASSERT_EQ(access.CommitUnmountSegment(segment.id, client_id, capacity),
              ErrorCode::OK);
}

}  // namespace

class SegmentTest : public ::testing::Test {};

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
        auto access = pool.getSegmentPoolAccess();
        ASSERT_EQ(access.MountSegment(host, client), ErrorCode::OK);
        ASSERT_EQ(access.MountSegment(cxl, client), ErrorCode::OK);
    }
    MountedRegion mounted;
    ASSERT_EQ(pool.getView().GetMountedRegion(host.id, mounted), ErrorCode::OK);
    EXPECT_EQ(mounted.kind, RegionKind::HOST_MEMORY);
    ASSERT_EQ(pool.getView().GetMountedRegion(cxl.id, mounted), ErrorCode::OK);
    EXPECT_EQ(mounted.kind, RegionKind::CXL);
    ASSERT_TRUE(pool.getView().GetResourceView(cxl.id).has_value());
    EXPECT_EQ(pool.getView().GetResourceView(cxl.id)->target->kind(),
              AllocationTargetKind::CXL);

    CommitUnmount(pool, host, client);
    CommitUnmount(pool, cxl, client);
}

TEST_F(SegmentTest, PoolPrepareRollbackDoesNotPublishCatalogOrPlacement) {
    SegmentPool pool(OffsetDrivers());
    auto segment = MakeSegment(0, "rollback");
    const UUID client = generate_uuid();
    {
        auto access = pool.getSegmentPoolAccess();
        auto prepared = access.PrepareMount(segment, client);
        ASSERT_TRUE(prepared.has_value());
        Segment ignored;
        EXPECT_FALSE(access.GetSegment(segment.id, ignored));
    }
    std::vector<std::string> active_groups;
    pool.getView().GetActiveGroupNames(active_groups);
    EXPECT_TRUE(active_groups.empty());
}

TEST_F(SegmentTest, SnapshotRoundTripPreservesMountedRegionHostId) {
    SegmentPool source(OffsetDrivers());
    auto segment = MakeSegment(0, "snapshot-host", "tcp", "host-a");
    const UUID client = generate_uuid();
    ASSERT_EQ(source.getSegmentPoolAccess().MountSegment(segment, client),
              ErrorCode::OK);

    auto encoded =
        ha::SegmentPoolSnapshotCodec::Encode(source, LocalSsdPersistedState{});
    ASSERT_TRUE(encoded.has_value()) << encoded.error().message;

    SegmentPool restored(OffsetDrivers());
    auto stale = MakeSegment(1, "stale-snapshot-region");
    ASSERT_EQ(restored.getSegmentPoolAccess().MountSegment(stale, client),
              ErrorCode::OK);
    auto decoded = ha::SegmentPoolSnapshotCodec::Decode(restored, *encoded);
    ASSERT_TRUE(decoded.has_value()) << decoded.error().message;

    MountedRegion mounted;
    EXPECT_EQ(restored.getView().GetMountedRegion(stale.id, mounted),
              ErrorCode::SEGMENT_NOT_FOUND);
    ASSERT_EQ(restored.getView().GetMountedRegion(segment.id, mounted),
              ErrorCode::OK);
    EXPECT_EQ(mounted.segment.host_id, segment.host_id);
    EXPECT_EQ(mounted.client_id, client);
    EXPECT_EQ(mounted.status, SegmentStatus::OK);

    restored.getSegmentPoolAccess().Clear();
    CommitUnmount(source, segment, client);
}

TEST_F(SegmentTest, MountReplacementPublishesStableNewTarget) {
    SegmentPool pool(OffsetDrivers());
    auto segment = MakeSegment(0, "replace");
    const UUID client = generate_uuid();
    {
        auto access = pool.getSegmentPoolAccess();
        ASSERT_EQ(access.MountSegment(segment, client), ErrorCode::OK);
    }
    const AllocationTarget* old_target = nullptr;
    {
        auto view = pool.getView();
        old_target = view.GetResourceView(segment.id)->target;
    }
    {
        auto access = pool.getSegmentPoolAccess();
        auto prepared = access.PrepareMount(segment, client);
        ASSERT_TRUE(prepared.has_value());
        access.CommitMount(*prepared);
    }
    {
        auto view = pool.getView();
        ASSERT_TRUE(view.GetResourceView(segment.id).has_value());
        EXPECT_NE(view.GetResourceView(segment.id)->target, old_target);
    }
    CommitUnmount(pool, segment, client);
}

TEST_F(SegmentTest, UnmountHidesThenRollbackReactivatesResource) {
    SegmentPool pool(OffsetDrivers());
    auto segment = MakeSegment(0, "lifecycle", "tcp", "host-a");
    const UUID client = generate_uuid();
    {
        auto access = pool.getSegmentPoolAccess();
        ASSERT_EQ(access.MountSegment(segment, client), ErrorCode::OK);
        size_t capacity = 0;
        ASSERT_EQ(access.PrepareUnmountSegment(segment.id, capacity),
                  ErrorCode::OK);
    }
    {
        auto view = pool.getView();
        ASSERT_TRUE(view.GetResourceView(segment.id).has_value());
        EXPECT_FALSE(view.GetResourceView(segment.id)->active);
    }
    {
        std::vector<std::string> active_groups;
        pool.getView().GetActiveGroupNames(active_groups);
        EXPECT_TRUE(active_groups.empty());
    }
    {
        auto access = pool.getSegmentPoolAccess();
        ASSERT_EQ(access.RollbackUnmountSegment(segment.id), ErrorCode::OK);
    }
    EXPECT_TRUE(pool.getView().GetResourceView(segment.id)->active);
    {
        std::vector<std::string> active_groups;
        pool.getView().GetActiveGroupNames(active_groups);
        EXPECT_EQ(active_groups, std::vector<std::string>{segment.name});
    }
    CommitUnmount(pool, segment, client);
}

TEST_F(SegmentTest, GracefulUnmountRetainsResourceUntilFinalCommit) {
    SegmentPool pool(OffsetDrivers());
    auto segment = MakeSegment(0, "graceful");
    const UUID client = generate_uuid();
    {
        auto access = pool.getSegmentPoolAccess();
        ASSERT_EQ(access.MountSegment(segment, client), ErrorCode::OK);
        ASSERT_EQ(access.PrepareGracefulUnmountSegment(segment.id),
                  ErrorCode::OK);
    }
    {
        auto view = pool.getView();
        ASSERT_TRUE(view.GetResourceView(segment.id).has_value());
        EXPECT_FALSE(view.GetResourceView(segment.id)->active);
    }
    {
        auto access = pool.getSegmentPoolAccess();
        size_t capacity = segment.size;
        ASSERT_EQ(access.CommitUnmountSegment(segment.id, client, capacity),
                  ErrorCode::OK);
    }
    EXPECT_FALSE(pool.getView().GetResourceView(segment.id).has_value());
}

TEST_F(SegmentTest, HostOrderingUsesGroupPointersAndTracksSameNameTargets) {
    SegmentPool pool(OffsetDrivers());
    const UUID client = generate_uuid();
    auto remote = MakeSegment(0, "remote", "tcp", "host-b");
    auto local0 = MakeSegment(1, "local", "tcp", "host-a");
    auto local1 = MakeSegment(2, "local", "tcp", "host-a");
    {
        auto access = pool.getSegmentPoolAccess();
        ASSERT_EQ(access.MountSegment(remote, client), ErrorCode::OK);
        ASSERT_EQ(access.MountSegment(local0, client), ErrorCode::OK);
        ASSERT_EQ(access.MountSegment(local1, client), ErrorCode::OK);
    }
    const SegmentAllocationRequest request{
        .size = 4096,
        .replica_count = 1,
        .preferred_group = {},
        .preferred_groups = {},
        .excluded_groups = {},
        .replica_type = ReplicaType::MEMORY,
        .writer_host_id = "host-a",
        .object_key = "key",
    };
    auto allocated = pool.Allocate(PlacementPolicyType::LOCAL_FIRST, request);
    ASSERT_TRUE(allocated.has_value());
    ASSERT_EQ(allocated->size(), 1U);
    const auto endpoint = allocated->front()
                              .get_descriptor()
                              .get_memory_descriptor()
                              .buffer_descriptor.transport_endpoint_;
    EXPECT_TRUE(endpoint == local0.te_endpoint ||
                endpoint == local1.te_endpoint);
    {
        auto view = pool.getView();
        size_t used = 0;
        size_t capacity = 0;
        ASSERT_EQ(view.QuerySegments("local", used, capacity), ErrorCode::OK);
        EXPECT_EQ(capacity, 2 * kRegionSize);
    }
    CommitUnmount(pool, local0, client);
    {
        auto view = pool.getView();
        size_t used = 0;
        size_t capacity = 0;
        ASSERT_EQ(view.QuerySegments("local", used, capacity), ErrorCode::OK);
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
    {
        auto access = pool.getSegmentPoolAccess();
        ASSERT_EQ(access.MountSegment(first, client), ErrorCode::OK);
        ASSERT_EQ(access.MountSegment(second, client), ErrorCode::OK);
    }

    {
        auto view = pool.getView();
        std::vector<std::string> names;
        view.GetAllSegmentNames(names);
        EXPECT_EQ(names, std::vector<std::string>{first.name});
    }

    {
        auto access = pool.getSegmentPoolAccess();
        ASSERT_EQ(
            access.SetSegmentStatusByName(first.name, SegmentStatus::DRAINING),
            ErrorCode::OK);
        EXPECT_FALSE(access.IsSegmentAllocatable(first.name));
    }
    {
        auto view = pool.getView();
        SegmentStatus status = SegmentStatus::UNDEFINED;
        ASSERT_EQ(view.GetSegmentStatusById(first.id, status), ErrorCode::OK);
        EXPECT_EQ(status, SegmentStatus::DRAINING);
        ASSERT_EQ(view.GetSegmentStatusById(second.id, status), ErrorCode::OK);
        EXPECT_EQ(status, SegmentStatus::DRAINING);
    }

    {
        auto access = pool.getSegmentPoolAccess();
        SegmentStatus status = SegmentStatus::UNDEFINED;
        ASSERT_EQ(access.SetSegmentStatusByName(first.name, SegmentStatus::OK),
                  ErrorCode::OK);
        EXPECT_TRUE(access.IsSegmentAllocatable(first.name));
        ASSERT_EQ(access.PrepareGracefulUnmountSegment(second.id),
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
        auto access = pool.getSegmentPoolAccess();
        ASSERT_EQ(access.CommitUnmountSegment(second.id, client, second.size),
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
        auto access = pool.getSegmentPoolAccess();
        ASSERT_EQ(access.MountSegment(
                      MakeSegment(i, "group-" + std::to_string(i)), client),
                  ErrorCode::OK);
    }

    const SegmentAllocationRequest fill_request{
        .size = 12U * 1024 * 1024,
        .replica_count = 3,
        .preferred_group = {},
        .preferred_groups = {},
        .excluded_groups = {},
        .replica_type = ReplicaType::MEMORY,
        .writer_host_id = {},
        .object_key = {},
    };
    auto filled = pool.Allocate(PlacementPolicyType::RANDOM, fill_request);
    ASSERT_TRUE(filled.has_value());

    const SegmentAllocationRequest exhausted_request{
        .size = 6U * 1024 * 1024,
        .replica_count = 3,
        .preferred_group = {},
        .preferred_groups = {},
        .excluded_groups = {},
        .replica_type = ReplicaType::MEMORY,
        .writer_host_id = {},
        .object_key = {},
    };
    AllocationDiagnostics diagnostics;
    auto exhausted =
        pool.Allocate(PlacementPolicyType::RANDOM, exhausted_request,
                      std::nullopt, &diagnostics);
    EXPECT_FALSE(exhausted.has_value());
    EXPECT_EQ(exhausted.error(), ErrorCode::NO_AVAILABLE_HANDLE);
    EXPECT_TRUE(diagnostics.has_enough_groups);
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
        auto access = pool.getSegmentPoolAccess();
        MountedRegion mounted{segment, client, SegmentStatus::OK,
                              RegionKind::HOST_MEMORY};
        auto prepared = access.PrepareAdopt(mounted, allocator);
        ASSERT_TRUE(prepared.has_value());
        access.CommitMount(*prepared);
    }

    auto allocation = std::async(std::launch::async, [&] {
        const SegmentAllocationRequest request{
            .size = 4096,
            .replica_count = 1,
            .preferred_group = {},
            .preferred_groups = {},
            .excluded_groups = {},
            .replica_type = ReplicaType::MEMORY,
            .writer_host_id = {},
            .object_key = {},
        };
        return pool.Allocate(PlacementPolicyType::RANDOM, request);
    });
    ASSERT_EQ(allocation_started.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);

    auto unmount = std::async(std::launch::async, [&] {
        auto access = pool.getSegmentPoolAccess();
        size_t capacity = 0;
        return access.PrepareUnmountSegment(segment.id, capacity);
    });
    EXPECT_EQ(unmount.wait_for(std::chrono::milliseconds(50)),
              std::future_status::timeout);
    allocator->AllowAllocation();
    auto allocated = allocation.get();
    ASSERT_TRUE(allocated.has_value());
    EXPECT_EQ(unmount.get(), ErrorCode::OK);

    auto access = pool.getSegmentPoolAccess();
    EXPECT_EQ(access.RollbackUnmountSegment(segment.id), ErrorCode::OK);
}

TEST_F(SegmentTest, CatalogReadViewAllowsConcurrentAllocation) {
    SegmentPool pool(OffsetDrivers());
    auto segment = MakeSegment(0, "read-view");
    const UUID client = generate_uuid();
    {
        auto access = pool.getSegmentPoolAccess();
        ASSERT_EQ(access.MountSegment(segment, client), ErrorCode::OK);
    }

    auto view = pool.getView();
    auto allocation = std::async(std::launch::async, [&] {
        const SegmentAllocationRequest request{
            .size = 4096,
            .replica_count = 1,
            .preferred_group = {},
            .preferred_groups = {},
            .excluded_groups = {},
            .replica_type = ReplicaType::MEMORY,
            .writer_host_id = {},
            .object_key = {},
        };
        return pool.Allocate(PlacementPolicyType::RANDOM, request);
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
        auto access = pool.getSegmentPoolAccess();
        ASSERT_EQ(access.MountSegment(a, client), ErrorCode::OK);
        ASSERT_EQ(access.MountSegment(b, client), ErrorCode::OK);
    }
    {
        auto view = pool.getView();
        std::vector<Segment> segments;
        ASSERT_EQ(view.GetClientSegments(client, segments), ErrorCode::OK);
        EXPECT_EQ(segments.size(), 2U);
        size_t used = 1;
        size_t capacity = 0;
        ASSERT_EQ(view.QuerySegments(a.name, used, capacity), ErrorCode::OK);
        EXPECT_EQ(used, 0U);
        EXPECT_EQ(capacity, a.size);
    }
    CommitUnmount(pool, a, client);
    CommitUnmount(pool, b, client);
}

}  // namespace mooncake

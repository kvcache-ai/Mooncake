#include "nof_segment_manager.h"

#include <gtest/gtest.h>

#include "placement/replica_allocator.h"

namespace mooncake {
namespace {

constexpr size_t kRegionSize = 16U * 1024 * 1024;

NoFSegment MakeSegment(std::string name) {
    NoFSegment segment;
    segment.id = generate_uuid();
    segment.name = std::move(name);
    segment.base = 0x200000000ULL;
    segment.size = kRegionSize;
    segment.te_endpoint = segment.name + "-endpoint";
    return segment;
}

}  // namespace

TEST(NoFSegmentManagerTest, PlacementAllocatesAndTracksUsage) {
    NoFSegmentManager manager(BufferAllocatorType::OFFSET);
    const NoFSegment segment = MakeSegment("nof-placement");
    const UUID client = generate_uuid();
    ASSERT_EQ(manager.AcquireWriteAccess().MountSegment(segment, client),
              ErrorCode::OK);

    ReplicaAllocationRequest request;
    request.replicas = {.size = 4096, .count = 1, .type = ReplicaType::NOF_SSD};
    request.placement.preferred_segment_name = segment.name;
    auto allocated = [&] {
        auto placement = manager.AcquirePlacementAccess();
        return ReplicaAllocator(RandomPlacementPolicy{})
            .Allocate(placement, request);
    }();
    ASSERT_TRUE(allocated.has_value());
    ASSERT_EQ(allocated->size(), 1U);
    EXPECT_TRUE(allocated->front().is_nof_replica());
    EXPECT_EQ(allocated->front()
                  .get_descriptor()
                  .get_nof_descriptor()
                  .buffer_descriptor.transport_endpoint_,
              segment.te_endpoint);

    const auto usage = manager.GetUsageSnapshot();
    EXPECT_EQ(usage.capacity_bytes, segment.size);
    EXPECT_EQ(usage.used_bytes, request.replicas.size);
    ASSERT_EQ(usage.segments.size(), 1U);
    EXPECT_EQ(usage.segments.at(segment.name).used_bytes,
              request.replicas.size);

    allocated->clear();
    auto access = manager.AcquireWriteAccess();
    ASSERT_EQ(access.PrepareUnmountSegment(segment.id, client), ErrorCode::OK);
    EXPECT_EQ(access.CommitUnmountSegment(segment.id, client), ErrorCode::OK);
}

TEST(NoFSegmentManagerTest, RemountRejectsRegionWhileUnmounting) {
    NoFSegmentManager manager(BufferAllocatorType::OFFSET);
    const NoFSegment segment = MakeSegment("nof-unmounting");
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

}  // namespace mooncake

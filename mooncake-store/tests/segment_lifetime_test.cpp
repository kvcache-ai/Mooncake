#include "segment/lifetime.h"

#include <array>
#include <chrono>
#include <memory>
#include <utility>

#include <gtest/gtest.h>

namespace mooncake::test {
namespace {

using namespace std::chrono_literals;

std::shared_ptr<ClientLivenessRecord> ServingOwner() {
    return std::make_shared<ClientLivenessRecord>(
        ClientLivenessRecord::TimePoint{});
}

// The state a region owner reaches after missing a heartbeat.
std::shared_ptr<ClientLivenessRecord> SuspectedOwner() {
    const auto now = ClientLivenessRecord::TimePoint{};
    auto owner = std::make_shared<ClientLivenessRecord>(now);
    EXPECT_EQ(owner->Evaluate(now + 1s, 1s, 1s),
              ClientLivenessTransition::BECAME_SUSPECTED);
    return owner;
}

// Access a region publishes per status, spelled out for every status so that a
// new SegmentStatus is flagged here until its effect on mounted buffers is
// decided.
struct ExpectedAccess {
    bool allocatable;
    bool readable;
};

ExpectedAccess ExpectedAccessFor(SegmentStatus status) {
    switch (status) {
        case SegmentStatus::OK:
            return {.allocatable = true, .readable = true};
        case SegmentStatus::DRAINING:
        case SegmentStatus::DRAINED:
        case SegmentStatus::GRACEFULLY_UNMOUNTING:
        case SegmentStatus::UNDEFINED:
            return {.allocatable = false, .readable = true};
        case SegmentStatus::UNMOUNTING:
            return {.allocatable = false, .readable = false};
    }
    return {.allocatable = false, .readable = false};
}

TEST(SegmentLifetimeTest, RegionWithoutOwnerServesBothReadsAndAllocations) {
    const SegmentLifetime lifetime;

    EXPECT_EQ(lifetime.GetClientLiveness(), nullptr);
    EXPECT_TRUE(lifetime.CanAllocate());
    EXPECT_TRUE(lifetime.CanRead());
}

TEST(SegmentLifetimeTest, SuspectedOwnerBlocksAccessButKeepsBuffersReadable) {
    SegmentLifetime lifetime;
    lifetime.BindClientLiveness(SuspectedOwner());

    EXPECT_FALSE(lifetime.CanAllocate());
    EXPECT_FALSE(lifetime.CanRead());
    // Retaining guards must still be able to release the buffers.
    EXPECT_TRUE(lifetime.HasReadableRegion());
}

TEST(SegmentLifetimeTest, StatusDecidesAllocationAndReadability) {
    const std::array<SegmentStatus, 6> statuses{
        SegmentStatus::UNDEFINED,
        SegmentStatus::OK,
        SegmentStatus::DRAINING,
        SegmentStatus::DRAINED,
        SegmentStatus::GRACEFULLY_UNMOUNTING,
        SegmentStatus::UNMOUNTING,
    };

    for (const auto status : statuses) {
        SCOPED_TRACE(status);
        SegmentLifetime lifetime;
        lifetime.BindClientLiveness(ServingOwner());
        lifetime.SetStatus(status);

        const auto expected = ExpectedAccessFor(status);
        EXPECT_EQ(lifetime.CanAllocate(), expected.allocatable);
        EXPECT_EQ(lifetime.CanRead(), expected.readable);
        EXPECT_EQ(lifetime.HasReadableRegion(), expected.readable);
    }
}

TEST(SegmentLifetimeTest, InvalidatedRegionStaysUnusableForAnyOwner) {
    SegmentLifetime lifetime;
    lifetime.Invalidate();

    EXPECT_FALSE(lifetime.CanAllocate());
    EXPECT_FALSE(lifetime.CanRead());
    EXPECT_FALSE(lifetime.HasReadableRegion());

    lifetime.BindClientLiveness(ServingOwner());
    EXPECT_FALSE(lifetime.CanAllocate());
    EXPECT_FALSE(lifetime.CanRead());
}

TEST(SegmentLifetimeTest, CopiesShareStateWhileFreshInstancesStayDistinct) {
    SegmentLifetime first;
    SegmentLifetime copy = first;
    EXPECT_TRUE(copy == first);

    first.SetAllocatable(false);
    EXPECT_FALSE(copy.CanAllocate());

    // A new mount must not inherit the state of the region it replaces.
    const SegmentLifetime fresh;
    EXPECT_FALSE(fresh == first);
    EXPECT_TRUE(fresh.CanAllocate());
    EXPECT_TRUE(fresh.CanRead());
}

}  // namespace
}  // namespace mooncake::test

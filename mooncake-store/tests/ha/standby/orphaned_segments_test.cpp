#include "ha/orphaned_segments.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

namespace mooncake {
namespace {

constexpr uint64_t kCapacity = 1024;

OrphanedSegment MakeOrphan(const std::string& name, const std::string& endpoint,
                           uint64_t charged_bytes = 0, bool readable = false) {
    OrphanedSegment orphan;
    orphan.info.segment_name = name;
    orphan.info.transport_endpoint = endpoint;
    orphan.info.capacity = kCapacity;
    orphan.info.is_memory_segment = true;
    orphan.keepalive = std::make_shared<DummyBufferAllocator>(name, endpoint);
    orphan.charged_bytes = charged_bytes;
    orphan.readable = readable;
    return orphan;
}

std::vector<OrphanedSegment> Orphans(OrphanedSegment orphan) {
    std::vector<OrphanedSegment> orphans;
    orphans.push_back(std::move(orphan));
    return orphans;
}

Segment MakeSegment(const std::string& name, const std::string& endpoint,
                    uint64_t size = kCapacity) {
    Segment segment;
    segment.name = name;
    segment.te_endpoint = endpoint;
    segment.size = size;
    return segment;
}

TEST(OrphanedSegmentsTest, EveryResetAdvancesTheGeneration) {
    OrphanedSegments orphans;
    const uint64_t initial = orphans.generation();

    orphans.Reset(Orphans(MakeOrphan("seg", "ep")));
    const uint64_t after_first = orphans.generation();
    EXPECT_GT(after_first, initial);

    // Identical contents still count: a remount that looked at the previous
    // orphans has to notice that they were swapped out underneath it.
    orphans.Reset(Orphans(MakeOrphan("seg", "ep")));
    EXPECT_GT(orphans.generation(), after_first);
}

TEST(OrphanedSegmentsTest, FindsTheOrphanASegmentReadopts) {
    OrphanedSegments orphans;
    orphans.Reset(Orphans(MakeOrphan("seg", "ep", 512)));

    auto found = orphans.Find(MakeSegment("seg", "ep"));
    ASSERT_TRUE(found.has_value());
    ASSERT_NE(nullptr, *found);
    EXPECT_EQ(512u, (*found)->charged_bytes);

    auto unrelated = orphans.Find(MakeSegment("other", "other_ep"));
    ASSERT_TRUE(unrelated.has_value());
    EXPECT_EQ(nullptr, *unrelated);
}

TEST(OrphanedSegmentsTest, RejectsSegmentsThatContradictTheirOrphan) {
    OrphanedSegments orphans;
    orphans.Reset(Orphans(MakeOrphan("seg", "ep")));

    // Named by endpoint, but the name and capacity have to agree too.
    EXPECT_EQ(ErrorCode::INVALID_PARAMS,
              orphans.Find(MakeSegment("renamed", "ep")).error());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS,
              orphans.Find(MakeSegment("seg", "ep", kCapacity * 2)).error());

    Segment cxl = MakeSegment("seg", "ep");
    cxl.protocol = "cxl";
    EXPECT_EQ(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE,
              orphans.Find(cxl).error());
}

TEST(OrphanedSegmentsTest, RejectsASegmentThatNamesTwoOrphans) {
    std::vector<OrphanedSegment> installed;
    installed.push_back(MakeOrphan("seg", "other_ep"));
    installed.push_back(MakeOrphan("other", "ep"));
    OrphanedSegments orphans;
    orphans.Reset(std::move(installed));

    // Its endpoint names one orphan and its name the other.
    EXPECT_EQ(ErrorCode::INVALID_PARAMS,
              orphans.Find(MakeSegment("seg", "ep")).error());
}

TEST(OrphanedSegmentsTest, UnmountedOrphansAreUnreadableByEndpointAndName) {
    std::vector<OrphanedSegment> installed;
    installed.push_back(MakeOrphan("seg", "ep"));
    installed.push_back(
        MakeOrphan("mounted", "mounted_ep", /*charged_bytes=*/0, true));
    OrphanedSegments orphans;
    orphans.Reset(std::move(installed));

    EXPECT_TRUE(orphans.IsUnreadable("ep"));
    EXPECT_TRUE(orphans.IsUnreadable("seg"));
    EXPECT_FALSE(orphans.IsUnreadable("mounted_ep"));
    EXPECT_FALSE(orphans.IsUnreadable("unrelated"));
}

TEST(OrphanedSegmentsTest, AdoptingEndsTheOrphanAndReleasesItsWholeCharge) {
    OrphanedSegment orphan = MakeOrphan("seg", "ep", 1024);
    std::weak_ptr<BufferAllocatorBase> keepalive = orphan.keepalive;
    OrphanedSegments orphans;
    orphans.Reset(Orphans(std::move(orphan)));

    // All of it comes back, not only the replicas that survived: the
    // re-adopted allocator counts those on its own.
    auto charge = orphans.Adopt(MakeSegment("seg", "ep"));
    ASSERT_TRUE(charge.has_value());
    EXPECT_EQ("seg", charge->segment_name);
    EXPECT_EQ(1024u, charge->bytes);

    EXPECT_TRUE(keepalive.expired());
    EXPECT_FALSE(orphans.IsUnreadable("ep"));
    EXPECT_FALSE(orphans.IsUnreadable("seg"));
    EXPECT_EQ(nullptr, orphans.Find(MakeSegment("seg", "ep")).value());
    EXPECT_FALSE(orphans.Adopt(MakeSegment("seg", "ep")).has_value());
    EXPECT_TRUE(orphans.ReleaseAll().empty());
}

TEST(OrphanedSegmentsTest, AdoptingOnlyEndsTheOrphanItNames) {
    std::vector<OrphanedSegment> installed;
    installed.push_back(MakeOrphan("seg", "ep", 512));
    installed.push_back(MakeOrphan("other", "other_ep", 256));
    OrphanedSegments orphans;
    orphans.Reset(std::move(installed));

    ASSERT_TRUE(orphans.Adopt(MakeSegment("seg", "ep")).has_value());
    EXPECT_TRUE(orphans.IsUnreadable("other_ep"));

    auto remaining = orphans.ReleaseAll();
    ASSERT_EQ(1u, remaining.size());
    EXPECT_EQ("other", remaining[0].segment_name);
    EXPECT_EQ(256u, remaining[0].bytes);
}

TEST(OrphanedSegmentsTest, AdoptingAnUnchargedOrphanStillEndsIt) {
    OrphanedSegments orphans;
    orphans.Reset(Orphans(MakeOrphan("seg", "ep")));

    EXPECT_FALSE(orphans.Adopt(MakeSegment("seg", "ep")).has_value());
    EXPECT_FALSE(orphans.IsUnreadable("ep"));
    EXPECT_EQ(nullptr, orphans.Find(MakeSegment("seg", "ep")).value());
}

TEST(OrphanedSegmentsTest, ResetReturnsWhatWasChargedToTheOrphansItDiscards) {
    OrphanedSegment old_orphan = MakeOrphan("old", "old_ep", 512);
    std::weak_ptr<BufferAllocatorBase> old_keepalive = old_orphan.keepalive;
    OrphanedSegments orphans;
    orphans.Reset(Orphans(std::move(old_orphan)));

    auto discarded = orphans.Reset(Orphans(MakeOrphan("new", "new_ep", 256)));
    ASSERT_EQ(1u, discarded.size());
    EXPECT_EQ("old", discarded[0].segment_name);
    EXPECT_EQ(512u, discarded[0].bytes);
    EXPECT_TRUE(old_keepalive.expired());
    EXPECT_FALSE(orphans.IsUnreadable("old_ep"));
    EXPECT_TRUE(orphans.IsUnreadable("new_ep"));
}

}  // namespace
}  // namespace mooncake

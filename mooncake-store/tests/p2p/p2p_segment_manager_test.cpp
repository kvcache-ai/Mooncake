#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

#include "p2p/master/p2p_segment_manager.h"
#include "p2p/master/p2p_master_metric_manager.h"

namespace mooncake {
namespace {

P2PSegment Segment(UUID id = {1, 1}, std::string name = "segment",
                   size_t size = 4096, size_t usage = 0,
                   MemoryType memory_type = MemoryType::DRAM) {
    return P2PSegment{.id = id,
                      .name = std::move(name),
                      .size = size,
                      .priority = 1,
                      .tags = {},
                      .memory_type = memory_type,
                      .usage = usage};
}

TEST(P2PSegmentManagerTest, StoresSegmentsByValue) {
    P2PSegmentManager manager;
    auto input = Segment();
    ASSERT_TRUE(manager.MountSegment(input).has_value());

    input.name = "changed-by-caller";
    auto snapshot = manager.QuerySegment({1, 1});
    ASSERT_TRUE(snapshot.has_value());
    EXPECT_EQ(snapshot->name, "segment");

    snapshot->usage = 999;
    auto second_snapshot = manager.QuerySegment({1, 1});
    ASSERT_TRUE(second_snapshot.has_value());
    EXPECT_EQ(second_snapshot->usage, 0);
}

TEST(P2PSegmentManagerTest, RejectsDuplicateAndMissingSegments) {
    P2PSegmentManager manager;
    ASSERT_TRUE(manager.MountSegment(Segment()).has_value());
    auto duplicate = manager.MountSegment(Segment());
    ASSERT_FALSE(duplicate.has_value());
    EXPECT_EQ(duplicate.error(), ErrorCode::SEGMENT_ALREADY_EXISTS);

    auto missing = manager.QuerySegment({9, 9});
    ASSERT_FALSE(missing.has_value());
    EXPECT_EQ(missing.error(), ErrorCode::SEGMENT_NOT_FOUND);

    const auto& read_only_manager = manager;
    EXPECT_TRUE(read_only_manager.CheckSegmentExists({1, 1}).has_value());
    auto absent = read_only_manager.CheckSegmentExists({9, 9});
    ASSERT_FALSE(absent.has_value());
    EXPECT_EQ(absent.error(), ErrorCode::SEGMENT_NOT_FOUND);
    ASSERT_TRUE(manager.UnmountSegment({1, 1}).has_value());
    auto removed = read_only_manager.CheckSegmentExists({1, 1});
    ASSERT_FALSE(removed.has_value());
    EXPECT_EQ(removed.error(), ErrorCode::SEGMENT_NOT_FOUND);
}

TEST(P2PSegmentManagerTest, MaintainsCapacityAndUsageAggregate) {
    P2PSegmentManager manager;
    ASSERT_TRUE(
        manager.MountSegment(Segment({1, 1}, "a", 4096, 100)).has_value());
    ASSERT_TRUE(
        manager.MountSegment(Segment({2, 2}, "b", 8192, 200)).has_value());
    EXPECT_EQ(manager.GetCapacityUsage(),
              std::make_pair(size_t{12288}, size_t{300}));

    auto old_usage = manager.UpdateSegmentUsage({1, 1}, 500);
    ASSERT_TRUE(old_usage.has_value());
    EXPECT_EQ(*old_usage, 100);
    EXPECT_EQ(manager.GetCapacityUsage(),
              std::make_pair(size_t{12288}, size_t{700}));

    auto removed = manager.UnmountSegment({2, 2});
    ASSERT_TRUE(removed.has_value());
    EXPECT_EQ(manager.GetCapacityUsage(),
              std::make_pair(size_t{4096}, size_t{500}));
}

TEST(P2PSegmentManagerTest, RejectsUsageBeyondCapacity) {
    P2PSegmentManager manager;
    ASSERT_TRUE(manager.MountSegment(Segment()).has_value());
    auto result = manager.UpdateSegmentUsage({1, 1}, 4097);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(manager.GetCapacityUsage(),
              std::make_pair(size_t{4096}, size_t{0}));
}

TEST(P2PSegmentManagerTest, ReturnsSegmentSnapshots) {
    P2PSegmentManager manager;
    ASSERT_TRUE(manager.MountSegment(Segment({1, 1}, "a")).has_value());
    ASSERT_TRUE(manager.MountSegment(Segment({2, 2}, "b")).has_value());

    auto segments = manager.GetSegments();
    ASSERT_TRUE(segments.has_value());
    ASSERT_EQ(segments->size(), 2);
    segments->clear();
    auto stored_segments = manager.GetSegments();
    ASSERT_TRUE(stored_segments.has_value());
    EXPECT_EQ(stored_segments->size(), 2);

    auto by_name = manager.QuerySegments("b");
    ASSERT_TRUE(by_name.has_value());
    EXPECT_EQ(*by_name, std::make_pair(size_t{0}, size_t{4096}));
}

TEST(P2PSegmentManagerTest, ConcurrentUsageAndUnmountRemainConsistent) {
    P2PSegmentManager manager;
    ASSERT_TRUE(manager.MountSegment(Segment()).has_value());
    std::atomic<bool> start{false};
    std::vector<std::thread> workers;
    for (size_t worker = 0; worker < 8; ++worker) {
        workers.emplace_back([&, worker]() {
            while (!start.load(std::memory_order_acquire)) {
            }
            for (size_t i = 0; i < 1000; ++i) {
                auto result = manager.UpdateSegmentUsage(
                    {1, 1}, (worker * 1000 + i) % 4097);
                if (!result.has_value()) {
                    EXPECT_EQ(result.error(), ErrorCode::SEGMENT_NOT_FOUND);
                    return;
                }
            }
        });
    }
    start.store(true, std::memory_order_release);
    auto removed = manager.UnmountSegment({1, 1});
    for (auto& worker : workers) {
        worker.join();
    }
    ASSERT_TRUE(removed.has_value());
    EXPECT_EQ(manager.GetCapacityUsage(), std::make_pair(size_t{0}, size_t{0}));
}

class P2PSegmentMetricsTest : public ::testing::TestWithParam<MemoryType> {
   protected:
    void SetUp() override { Metrics().reset_all_metrics(); }

    void TearDown() override {
        auto segments = manager_.GetSegments();
        ASSERT_TRUE(segments.has_value());
        for (const auto& segment : *segments) {
            EXPECT_TRUE(manager_.UnmountSegment(segment.id).has_value());
        }
        Metrics().reset_all_metrics();
    }

    static P2PMasterMetricManager& Metrics() {
        return P2PMasterMetricManager::instance();
    }

    void ExpectGauges(int64_t capacity, int64_t usage) {
        const bool dram = GetParam() == MemoryType::DRAM;
        EXPECT_EQ(Metrics().get_total_mem_capacity(), dram ? capacity : 0);
        EXPECT_EQ(Metrics().get_allocated_mem_size(), dram ? usage : 0);
        EXPECT_EQ(Metrics().get_total_file_capacity(), dram ? 0 : capacity);
        EXPECT_EQ(Metrics().get_allocated_file_size(), dram ? 0 : usage);
        if (dram) {
            EXPECT_EQ(Metrics().get_segment_total_mem_capacity("metrics"),
                      capacity);
            EXPECT_EQ(Metrics().get_segment_allocated_mem_size("metrics"),
                      usage);
        }
    }

    P2PSegmentManager manager_;
};

TEST_P(P2PSegmentMetricsTest, MountUsageAndUnmountMaintainGauges) {
    const auto segment = Segment({1, 1}, "metrics", 4096, 100, GetParam());
    ASSERT_TRUE(manager_.MountSegment(segment).has_value());
    ExpectGauges(4096, 100);
    auto duplicate = manager_.MountSegment(segment);
    ASSERT_FALSE(duplicate.has_value());
    EXPECT_EQ(duplicate.error(), ErrorCode::SEGMENT_ALREADY_EXISTS);
    ExpectGauges(4096, 100);
    size_t previous = 100;
    for (size_t usage : {size_t{500}, size_t{20}, size_t{0}, size_t{4096}}) {
        auto old = manager_.UpdateSegmentUsage(segment.id, usage);
        ASSERT_TRUE(old.has_value());
        EXPECT_EQ(*old, previous);
        previous = usage;
        ExpectGauges(4096, usage);
    }
    auto invalid = manager_.UpdateSegmentUsage(segment.id, 4097);
    ASSERT_FALSE(invalid.has_value());
    EXPECT_EQ(invalid.error(), ErrorCode::INVALID_PARAMS);
    ExpectGauges(4096, 4096);
    ASSERT_TRUE(manager_.UnmountSegment(segment.id).has_value());
    ExpectGauges(0, 0);
    auto missing = manager_.UnmountSegment(segment.id);
    ASSERT_FALSE(missing.has_value());
    EXPECT_EQ(missing.error(), ErrorCode::SEGMENT_NOT_FOUND);
    auto late_usage = manager_.UpdateSegmentUsage(segment.id, 20);
    ASSERT_FALSE(late_usage.has_value());
    EXPECT_EQ(late_usage.error(), ErrorCode::SEGMENT_NOT_FOUND);
    ExpectGauges(0, 0);
}

TEST_P(P2PSegmentMetricsTest, UnmountBeforeUsageReportKeepsUsageZero) {
    const auto segment = Segment({1, 1}, "metrics", 4096, 0, GetParam());
    ASSERT_TRUE(manager_.MountSegment(segment).has_value());
    ExpectGauges(4096, 0);
    ASSERT_TRUE(manager_.UnmountSegment(segment.id).has_value());
    ExpectGauges(0, 0);
}

INSTANTIATE_TEST_SUITE_P(StorageTypes, P2PSegmentMetricsTest,
                         ::testing::Values(MemoryType::DRAM, MemoryType::NVME));

}  // namespace
}  // namespace mooncake

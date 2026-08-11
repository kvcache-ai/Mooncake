#include <gtest/gtest.h>
#include <glog/logging.h>

#include <atomic>
#include <thread>
#include <vector>

#define private public
#define protected public
#include "p2p_segment_manager.h"
#undef private
#undef protected

namespace mooncake {

class P2PSegmentManagerTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("P2PSegmentManagerTest");
        FLAGS_logtostderr = 1;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }

    Segment MakeSegment(UUID id, const std::string& name = "seg1",
                        size_t size = 1024 * 1024, int priority = 1) {
        Segment seg;
        seg.id = id;
        seg.name = name;
        seg.size = size;
        seg.extra = P2PSegmentExtraData{
            .priority = priority,
            .tags = {},
            .memory_type = MemoryType::DRAM,
            .usage = 0,
        };
        return seg;
    }
};

// ============================================================
// Mount / Unmount
// ============================================================

TEST_F(P2PSegmentManagerTest, MountSegmentSuccess) {
    P2PSegmentManager mgr;
    auto seg = MakeSegment(generate_uuid());
    auto res = mgr.MountSegment(seg);
    EXPECT_TRUE(res.has_value());
}

TEST_F(P2PSegmentManagerTest, MountDuplicateSegment) {
    P2PSegmentManager mgr;
    auto seg = MakeSegment(generate_uuid());
    ASSERT_TRUE(mgr.MountSegment(seg).has_value());

    auto res = mgr.MountSegment(seg);
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(res.error(), ErrorCode::SEGMENT_ALREADY_EXISTS);
}

TEST_F(P2PSegmentManagerTest, UnmountSegmentSuccess) {
    P2PSegmentManager mgr;
    auto seg1 = MakeSegment(generate_uuid());
    auto seg2 = MakeSegment(generate_uuid());

    ASSERT_TRUE(mgr.MountSegment(seg1).has_value());
    ASSERT_TRUE(mgr.MountSegment(seg2).has_value());

    // Verify consistency between ForEachSegment and GetSegments
    EXPECT_EQ(mgr.mounted_segments_.size(), 2);

    ASSERT_TRUE(mgr.UnmountSegment(seg1.id).has_value());
    EXPECT_EQ(mgr.mounted_segments_.size(), 1);

    ASSERT_TRUE(mgr.UnmountSegment(seg2.id).has_value());
    EXPECT_EQ(mgr.mounted_segments_.size(), 0);
}

TEST_F(P2PSegmentManagerTest, UnmountNonexistentSegment) {
    P2PSegmentManager mgr;
    UUID id = {999, 999};
    auto res = mgr.UnmountSegment(id);
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(res.error(), ErrorCode::SEGMENT_NOT_FOUND);
}

TEST_F(P2PSegmentManagerTest, UnmountDuplicate) {
    P2PSegmentManager mgr;
    auto seg = MakeSegment(generate_uuid());
    ASSERT_TRUE(mgr.MountSegment(seg).has_value());
    ASSERT_TRUE(mgr.UnmountSegment(seg.id).has_value());

    auto res = mgr.UnmountSegment(seg.id);
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(res.error(), ErrorCode::SEGMENT_NOT_FOUND);
}

// ============================================================
// GetSegments / QuerySegment
// ============================================================

TEST_F(P2PSegmentManagerTest, GetSegmentsAfterMount) {
    P2PSegmentManager mgr;
    auto seg1 = MakeSegment({1, 1}, "seg1", 1000);
    auto seg2 = MakeSegment({2, 2}, "seg2", 2000);
    ASSERT_TRUE(mgr.MountSegment(seg1).has_value());
    ASSERT_TRUE(mgr.MountSegment(seg2).has_value());

    auto res = mgr.GetSegments();
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res.value().size(), 2);
}

TEST_F(P2PSegmentManagerTest, QuerySegmentsCheck) {
    P2PSegmentManager mgr;
    auto seg = MakeSegment({1, 1}, "detailed_seg", 10000);
    ASSERT_TRUE(mgr.MountSegment(seg).has_value());
    ASSERT_TRUE(mgr.UpdateSegmentUsage({1, 1}, 4500).has_value());

    auto res = mgr.QuerySegments("detailed_seg");
    ASSERT_TRUE(res.has_value());
    auto [used, capacity] = res.value();
    EXPECT_EQ(used, 4500);
    EXPECT_EQ(capacity, 10000);
}

TEST_F(P2PSegmentManagerTest, QueryNonexistentSegment) {
    P2PSegmentManager mgr;
    auto res = mgr.QuerySegments("nonexistent");
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(res.error(), ErrorCode::SEGMENT_NOT_FOUND);
}

TEST_F(P2PSegmentManagerTest, QuerySegmentByIDSuccess) {
    P2PSegmentManager mgr;
    auto seg = MakeSegment({123, 456}, "id_seg", 8192);
    ASSERT_TRUE(mgr.MountSegment(seg).has_value());

    auto res = mgr.QuerySegment({123, 456});
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ((*res)->id, seg.id);
    EXPECT_EQ((*res)->name, "id_seg");
}

TEST_F(P2PSegmentManagerTest, QuerySegmentByIDNotFound) {
    P2PSegmentManager mgr;
    auto res = mgr.QuerySegment({999, 999});
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(res.error(), ErrorCode::SEGMENT_NOT_FOUND);
}

// ============================================================
// UpdateSegmentUsage / GetSegmentUsage
// ============================================================

TEST_F(P2PSegmentManagerTest, UpdateSegmentUsage) {
    P2PSegmentManager mgr;
    auto seg = MakeSegment({1, 1}, "seg1", 10000);
    ASSERT_TRUE(mgr.MountSegment(seg).has_value());

    auto res = mgr.UpdateSegmentUsage({1, 1}, 4000);
    ASSERT_TRUE(res.has_value());
    // Returns old usage
    EXPECT_EQ(res.value(), 0);

    // Verify new usage
    EXPECT_EQ(mgr.GetSegmentUsage({1, 1}), 4000);
}

TEST_F(P2PSegmentManagerTest, UpdateSegmentUsageMultipleTimes) {
    P2PSegmentManager mgr;
    auto seg = MakeSegment({1, 1}, "seg1", 10000);
    ASSERT_TRUE(mgr.MountSegment(seg).has_value());

    auto res = mgr.UpdateSegmentUsage({1, 1}, 1000);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res.value(), 0);  // old usage
    EXPECT_EQ(mgr.GetSegmentUsage({1, 1}), 1000);

    res = mgr.UpdateSegmentUsage({1, 1}, 3000);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res.value(), 1000);  // old usage
    EXPECT_EQ(mgr.GetSegmentUsage({1, 1}), 3000);
}

TEST_F(P2PSegmentManagerTest, UsageZeroAndMax) {
    P2PSegmentManager mgr;
    auto seg = MakeSegment({1, 1}, "seg1", SIZE_MAX);
    ASSERT_TRUE(mgr.MountSegment(seg).has_value());

    mgr.UpdateSegmentUsage({1, 1}, 0);
    EXPECT_EQ(mgr.GetSegmentUsage({1, 1}), 0);

    mgr.UpdateSegmentUsage({1, 1}, SIZE_MAX);
    EXPECT_EQ(mgr.GetSegmentUsage({1, 1}), SIZE_MAX);
}

TEST_F(P2PSegmentManagerTest, UpdateSegmentUsageNotFound) {
    P2PSegmentManager mgr;
    auto res = mgr.UpdateSegmentUsage(generate_uuid(), 100);
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(res.error(), ErrorCode::SEGMENT_NOT_FOUND);
}

TEST_F(P2PSegmentManagerTest, UpdateSegmentUsageExceedCapacity) {
    P2PSegmentManager mgr;
    auto seg = MakeSegment({0, 0}, "seg1", 1000);
    ASSERT_TRUE(mgr.MountSegment(seg).has_value());

    auto res = mgr.UpdateSegmentUsage(seg.id, 1001);
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(res.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(P2PSegmentManagerTest, GetSegmentUsageNotFound) {
    P2PSegmentManager mgr;
    // Should return 0 and log warning
    EXPECT_EQ(mgr.GetSegmentUsage(generate_uuid()), 0);
}

// ============================================================
// ForEachSegment
// ============================================================

TEST_F(P2PSegmentManagerTest, ForEachSegmentVisitsAll) {
    P2PSegmentManager mgr;
    for (int i = 0; i < 5; i++) {
        auto seg = MakeSegment({static_cast<uint64_t>(i), 0},
                               "seg_" + std::to_string(i), 1024);
        ASSERT_TRUE(mgr.MountSegment(seg).has_value());
    }

    int count = 0;
    mgr.ForEachSegment([&count](const Segment& seg) -> bool {
        count++;
        return false;  // continue
    });
    EXPECT_EQ(count, 5);
}

TEST_F(P2PSegmentManagerTest, ForEachSegmentEmpty) {
    P2PSegmentManager mgr;

    int count = 0;
    mgr.ForEachSegment([&count](const Segment& seg) -> bool {
        count++;
        return false;
    });
    EXPECT_EQ(count, 0);
}

TEST_F(P2PSegmentManagerTest, ForEachSegmentEarlyStop) {
    P2PSegmentManager mgr;
    for (int i = 0; i < 10; i++) {
        auto seg = MakeSegment({static_cast<uint64_t>(i), 0},
                               "seg_" + std::to_string(i), 1024);
        ASSERT_TRUE(mgr.MountSegment(seg).has_value());
    }

    int count = 0;
    mgr.ForEachSegment([&count](const Segment& seg) -> bool {
        count++;
        return count >= 3;  // stop after 3
    });
    EXPECT_EQ(count, 3);
}

// ============================================================
// Callbacks
// ============================================================

TEST_F(P2PSegmentManagerTest, SegmentChangeCallbacksTriggered) {
    P2PSegmentManager mgr;

    int add_count = 0;
    int remove_count = 0;
    mgr.SetSegmentChangeCallbacks(
        [&add_count](const Segment& seg) { add_count++; },
        [&remove_count](const Segment& seg) { remove_count++; });

    auto seg = MakeSegment({0, 0});
    ASSERT_TRUE(mgr.MountSegment(seg).has_value());
    EXPECT_EQ(add_count, 1);
    EXPECT_EQ(remove_count, 0);

    ASSERT_TRUE(mgr.UnmountSegment(seg.id).has_value());
    EXPECT_EQ(add_count, 1);
    EXPECT_EQ(remove_count, 1);
}

TEST_F(P2PSegmentManagerTest, SegmentRemovalCallback) {
    P2PSegmentManager mgr;
    auto seg = MakeSegment({1, 1});
    ASSERT_TRUE(mgr.MountSegment(seg).has_value());

    bool callback_triggered = false;
    mgr.SetSegmentRemovalCallback([&callback_triggered, &seg](const UUID& id) {
        if (id == seg.id) {
            callback_triggered = true;
        }
    });

    ASSERT_TRUE(mgr.UnmountSegment(seg.id).has_value());
    EXPECT_TRUE(callback_triggered);
}

// ============================================================
// Concurrency Tests
// ============================================================

TEST_F(P2PSegmentManagerTest, ConcurrentMountAndUnmount) {
    P2PSegmentManager mgr;
    constexpr int kNumThreads = 16;
    std::vector<std::thread> threads;
    std::atomic<int> mount_success_count{0};

    for (int i = 0; i < kNumThreads; i++) {
        threads.emplace_back([&mgr, &mount_success_count, i]() {
            auto seg = Segment();
            seg.id = {static_cast<uint64_t>(i + 100), 0};
            seg.name = "concurrent_seg_" + std::to_string(i);
            seg.size = 1024 * 1024;
            seg.extra = P2PSegmentExtraData{
                .priority = 1,
                .tags = {},
                .memory_type = MemoryType::DRAM,
                .usage = 0,
            };

            auto mount_result = mgr.MountSegment(seg);
            ASSERT_TRUE(mount_result.has_value());
            mount_success_count.fetch_add(1);

            auto unmount_result = mgr.UnmountSegment(seg.id);
            ASSERT_TRUE(unmount_result.has_value());
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(mount_success_count, kNumThreads);

    // All segments should be unmounted
    auto res = mgr.GetSegments();
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res.value().size(), 0);
}

TEST_F(P2PSegmentManagerTest, UpdateSegmentUsageLifeCycleRace) {
    P2PSegmentManager mgr;
    auto id = generate_uuid();
    auto seg = MakeSegment(id, "race_seg", 10000);

    std::atomic<bool> stop{false};
    // Thread A: Constantly Mount and Unmount this segment
    std::thread mounter([&]() {
        while (!stop) {
            mgr.MountSegment(seg);
            std::this_thread::yield();
            mgr.UnmountSegment(id);
            std::this_thread::yield();
        }
    });

    // Thread B: Concurrent UpdateSegmentUsage attempts
    constexpr int kNumUpdaters = 4;
    std::vector<std::thread> updaters;
    for (int i = 0; i < kNumUpdaters; ++i) {
        updaters.emplace_back([&]() {
            while (!stop) {
                auto res = mgr.UpdateSegmentUsage(id, 500);
                if (!res.has_value()) {
                    // If failed, it must be because segment not found during
                    // unmount window
                    ASSERT_EQ(res.error(), ErrorCode::SEGMENT_NOT_FOUND);
                }
                std::this_thread::yield();
            }
        });
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    stop = true;
    mounter.join();
    for (auto& t : updaters) t.join();
}

}  // namespace mooncake

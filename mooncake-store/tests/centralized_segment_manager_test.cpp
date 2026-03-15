#include "centralized_segment_manager.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

#include <boost/functional/hash.hpp>

namespace mooncake {

// Test fixture for Segment tests
class SegmentTest : public ::testing::Test {
   protected:
    void SetUp() override {
        // Initialize glog for logging
        google::InitGoogleLogging("EvictionStrategyTest");
        FLAGS_logtostderr = 1;  // Output logs to stderr
    }

    void TearDown() override {
        // Cleanup glog
        google::ShutdownGoogleLogging();
    }

    void ValidateMountedSegments(
        const CentralizedSegmentManager& segment_manager,
        const std::vector<Segment>& segments) {
        ASSERT_EQ(segment_manager.mounted_segments_.size(), segments.size());
        for (size_t i = 0; i < segments.size(); i++) {
            auto segment_it =
                segment_manager.mounted_segments_.find(segments[i].id);
            ASSERT_NE(segment_it, segment_manager.mounted_segments_.end());

            std::shared_ptr<MountedCentralizedSegment> seg =
                std::static_pointer_cast<MountedCentralizedSegment>(
                    segment_it->second);
            ASSERT_EQ(seg->id, segments[i].id);
            ASSERT_EQ(seg->name, segments[i].name);
            ASSERT_EQ(seg->size, segments[i].size);
            ASSERT_EQ(seg->GetCentralizedExtra().base,
                      segments[i].GetCentralizedExtra().base);
            ASSERT_EQ(seg->buf_allocator->getSegmentName(), segments[i].name);
            ASSERT_EQ(seg->buf_allocator->capacity(), segments[i].size);
        }

        // validate allocators by checking buf_allocator in
        // MountedCentralizedSegment
        size_t total_num = 0;
        for (const auto& [id, base_seg] : segment_manager.mounted_segments_) {
            auto mounted =
                std::static_pointer_cast<MountedCentralizedSegment>(base_seg);
            ASSERT_NE(mounted->buf_allocator, nullptr);
            total_num++;
        }
        ASSERT_EQ(total_num, segments.size());

        for (const auto& segment : segments) {
            auto seg_it = segment_manager.mounted_segments_.find(segment.id);
            ASSERT_NE(seg_it, segment_manager.mounted_segments_.end());
            std::shared_ptr<MountedCentralizedSegment> mounted_segment =
                std::static_pointer_cast<MountedCentralizedSegment>(
                    seg_it->second);
            ASSERT_NE(mounted_segment->buf_allocator, nullptr);
            ASSERT_EQ(mounted_segment->buf_allocator->getSegmentName(),
                      segment.name);
        }
    }

    void ValidateMountedSegment(
        const CentralizedSegmentManager& segment_manager,
        const Segment segment) {
        std::vector<Segment> segments;
        segments.push_back(segment);
        ValidateMountedSegments(segment_manager, segments);
    }

    void ValidateMountedLocalDiskSegments(
        const CentralizedSegmentManager& segment_manager,
        const std::shared_ptr<LocalDiskSegment>& segment) {
        ASSERT_NE(segment_manager.local_disk_segment_, nullptr);
        ASSERT_EQ(segment_manager.local_disk_segment_->enable_offloading,
                  segment->enable_offloading);
    }
};

// Mount Segment Operations Tests:
TEST_F(SegmentTest, MountSegmentSuccess) {
    CentralizedSegmentManager segment_manager;
    // Create a valid segment and client ID
    Segment segment;
    segment.id = generate_uuid();
    segment.name = "test_segment";
    segment.size = 1024 * 1024 * 16;
    segment.extra =
        CentralizedSegmentExtraData{.base = 0x100000000, .te_endpoint = ""};

    // Get segment access and attempt to mount
    // Get segment access and attempt to mount
    ASSERT_TRUE(segment_manager.MountSegment(segment).has_value());

    // Verify segment is properly mounted
    ValidateMountedSegment(segment_manager, segment);
}

// MountSegmentDuplicate Tests:
// 1. MountSegment with the same segment id. The second mount operation return
// SEGMENT_ALREADY_EXISTS.
// 2. MountSegment with different segment id and the same segment name should be
// considered as different segments. Validate the status of
// CentralizedSegmentManager use ValidateMountedSegments function.
TEST_F(SegmentTest, MountSegmentDuplicate) {
    CentralizedSegmentManager segment_manager;
    // Create a valid segment and client ID
    Segment segment;
    segment.id = generate_uuid();
    segment.name = "test_segment";
    segment.size = 1024 * 1024 * 16;
    segment.extra =
        CentralizedSegmentExtraData{.base = 0x100000000, .te_endpoint = ""};

    // Get segment access and mount first time
    // Get segment access and mount first time
    ASSERT_TRUE(segment_manager.MountSegment(segment).has_value());

    // Verify first mount
    ValidateMountedSegment(segment_manager, segment);

    // Test duplicate mount - mount the same segment again
    ASSERT_EQ(segment_manager.MountSegment(segment).error(),
              ErrorCode::SEGMENT_ALREADY_EXISTS);

    // Verify state remains the same after duplicate mount
    ValidateMountedSegment(segment_manager, segment);

    // Create a new segment with same name but different ID
    Segment segment2;
    segment2.id = generate_uuid();  // Different ID
    segment2.name = segment.name;   // Same name
    segment2.size = segment.size * 2;
    segment2.extra = CentralizedSegmentExtraData{
        .base = segment.GetCentralizedExtra().base + segment.size,
        .te_endpoint = ""};

    // Mount the second segment
    ASSERT_TRUE(segment_manager.MountSegment(segment2).has_value());

    // Verify both segments are mounted correctly
    std::vector<Segment> segments = {segment, segment2};
    ValidateMountedSegments(segment_manager, segments);
}

// UnmountSegmentSuccess:
// 1. Mount a segment and then unmount it. Unmount operation return success.
// 2. Use ValidateMountedSegments function to validate the status of
// CentralizedSegmentManager.
TEST_F(SegmentTest, UnmountSegmentSuccess) {
    CentralizedSegmentManager segment_manager;

    // Create and mount a segment
    Segment segment;
    segment.id = generate_uuid();
    segment.name = "test_segment";
    segment.size = 1024 * 1024 * 16;
    segment.extra =
        CentralizedSegmentExtraData{.base = 0x100000000, .te_endpoint = ""};

    // Get segment access and mount
    ASSERT_TRUE(segment_manager.MountSegment(segment).has_value());

    // Verify segment is mounted correctly
    ValidateMountedSegment(segment_manager, segment);

    // Commit unmount
    ASSERT_TRUE(segment_manager.UnmountSegment(segment.id).has_value());

    // Verify segment is unmounted correctly
    std::vector<Segment> empty_segment_vec;
    ValidateMountedSegments(segment_manager, empty_segment_vec);
}

// UnmountSegmentDuplicate:
// 1. Mount a segment and then unmount it twice. The second unmount operation
// returns SEGMENT_NOT_FOUND.
// 2. Only use ValidateMountedSegments function to validate the status of
// CentralizedSegmentManager. Do not use other interfaces for validation.
TEST_F(SegmentTest, UnmountSegmentDuplicate) {
    CentralizedSegmentManager segment_manager;

    // Create and mount a segment
    Segment segment;
    segment.id = generate_uuid();
    segment.name = "test_segment";
    segment.size = 1024 * 1024 * 16;
    segment.extra =
        CentralizedSegmentExtraData{.base = 0x100000000, .te_endpoint = ""};

    // Get segment access and mount
    ASSERT_TRUE(segment_manager.MountSegment(segment).has_value());

    // Verify initial mounted state
    ValidateMountedSegment(segment_manager, segment);

    // First unmount
    ASSERT_TRUE(segment_manager.UnmountSegment(segment.id).has_value());

    // Verify segment is unmounted after first unmount
    std::vector<Segment> empty_segment_vec;
    ValidateMountedSegments(segment_manager, empty_segment_vec);

    // Second unmount attempt (idempotent)
    ASSERT_EQ(segment_manager.UnmountSegment(segment.id).error(),
              ErrorCode::SEGMENT_NOT_FOUND);

    // Verify segment remains unmounted after second unmount
    ValidateMountedSegments(segment_manager, empty_segment_vec);
}

TEST_F(SegmentTest, GetSegmentsEmpty) {
    CentralizedSegmentManager segment_manager;
    auto result = segment_manager.GetSegments();
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result.value().empty());
}

TEST_F(SegmentTest, QuerySegmentByIDNotFound) {
    CentralizedSegmentManager segment_manager;
    auto res = segment_manager.QuerySegment(generate_uuid());
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(res.error(), ErrorCode::SEGMENT_NOT_FOUND);
}

// QuerySegments:
// 1. Create and mount 10 different segments with different names and different
// client ids;
// 2. Test GetSegments, verify the return value is correct.
// 3. Test GetAllSegments, verify the return value is correct.
// 4. Test QuerySegments, verify the return value is correct.
TEST_F(SegmentTest, QuerySegments) {
    CentralizedSegmentManager segment_manager;
    // Create 10 different segments with different names and client IDs
    std::vector<Segment> segments;
    std::vector<UUID> client_ids;
    std::unordered_map<UUID, UUID, boost::hash<UUID>> expected_client_segments;

    for (int i = 0; i < 10; i++) {
        // Create segment
        Segment segment;
        segment.id = generate_uuid();
        segment.name = "test_segment_" + std::to_string(i);
        segment.size = 1024 * 1024 * 16;
        segment.extra = CentralizedSegmentExtraData{
            .base = static_cast<uintptr_t>(0x100000000 + (i * 0x100000000)),
            .te_endpoint = ""};

        // Create client ID

        // Mount segment
        ASSERT_TRUE(segment_manager.MountSegment(segment).has_value());

        // Store for verification
        segments.push_back(segment);
    }

    // Verify all segments are mounted correctly
    ValidateMountedSegments(segment_manager, segments);

    // Test GetSegments
    auto result = segment_manager.GetSegments();
    ASSERT_TRUE(result.has_value());

    std::vector<Segment>& client_segments = result.value();

    // Verify correct number of segments
    ASSERT_EQ(client_segments.size(), 10);

    // Test QuerySegments for each segment
    for (const auto& segment : segments) {
        auto result = segment_manager.QuerySegments(segment.name);
        ASSERT_TRUE(result.has_value());

        auto [used, capacity] = result.value();

        // Verify capacity matches segment size
        ASSERT_EQ(capacity, segment.size);

        // Verify used space is 0 for newly mounted segments
        ASSERT_EQ(used, 0);
    }

    // Test QuerySegments for non-existent segment
    auto query_result = segment_manager.QuerySegments("non_existent_segment");
    ASSERT_FALSE(query_result.has_value());
    ASSERT_EQ(query_result.error(), ErrorCode::SEGMENT_NOT_FOUND);
}

// Mount Local Disk Segment Operations Tests:
TEST_F(SegmentTest, MountLocalDiskSegmentSuccess) {
    CentralizedSegmentManager segment_manager;
    // Create a valid local disk segment and client ID
    auto segment = std::make_shared<LocalDiskSegment>(true);

    // Get segment access and attempt to mount
    ASSERT_TRUE(segment_manager.MountLocalDiskSegment(true).has_value());

    // Verify segment is properly mounted
    ValidateMountedLocalDiskSegments(segment_manager, segment);
}

// MountLocalDiskSegmentDuplicate Tests:
// 1. MountLocalDiskSegment with the same segment id. The second mount operation
// return SEGMENT_ALREADY_EXISTS.
// 2. MountLocalDiskSegment with different segment id and the same segment name
// should be considered as different segments. Validate the status of
// CentralizedSegmentManager use ValidateMountedLocalDiskSegments function.
TEST_F(SegmentTest, MountLocalDiskSegmentDuplicate) {
    CentralizedSegmentManager segment_manager;
    // Create a valid segment and client ID
    auto segment = std::make_shared<LocalDiskSegment>(true);

    // Get segment access and mount first time
    ASSERT_TRUE(segment_manager.MountLocalDiskSegment(true).has_value());

    // Verify first mount
    ValidateMountedLocalDiskSegments(segment_manager, segment);

    // Test duplicate mount - mount the same segment again
    ASSERT_EQ(segment_manager.MountLocalDiskSegment(true).error(),
              ErrorCode::SEGMENT_ALREADY_EXISTS);

    // Verify state remains the same after duplicate mount
    ValidateMountedLocalDiskSegments(segment_manager, segment);
}

// ============================================================
// Concurrency Tests
// ============================================================

TEST_F(SegmentTest, ConcurrentMountAndUnmount) {
    CentralizedSegmentManager segment_manager;
    constexpr int kNumThreads = 16;
    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};

    for (int i = 0; i < kNumThreads; i++) {
        threads.emplace_back([&segment_manager, &success_count, i]() {
            Segment segment;
            segment.id = generate_uuid();
            segment.name = "concurrent_seg_" + std::to_string(i);
            segment.size = 1024 * 1024 * 16;
            segment.extra = CentralizedSegmentExtraData{
                .base =
                    static_cast<uintptr_t>(0x100000000 + i * 0x100000000ULL),
                .te_endpoint = ""};

            auto mount_result = segment_manager.MountSegment(segment);
            ASSERT_TRUE(mount_result.has_value());
            success_count.fetch_add(1);

            auto unmount_result = segment_manager.UnmountSegment(segment.id);
            ASSERT_TRUE(unmount_result.has_value());
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(success_count, kNumThreads);

    // All segments should be unmounted
    std::vector<Segment> empty_vec;
    ValidateMountedSegments(segment_manager, empty_vec);
}

TEST_F(SegmentTest, ConcurrentMountSameSegment) {
    CentralizedSegmentManager segment_manager;
    constexpr int kNumThreads = 16;

    Segment segment;
    segment.id = generate_uuid();
    segment.name = "same_segment";
    segment.size = 1024 * 1024 * 16;
    segment.extra =
        CentralizedSegmentExtraData{.base = 0x100000000, .te_endpoint = ""};

    std::vector<std::thread> threads;
    std::atomic<int> mount_success{0};
    std::atomic<int> mount_duplicate{0};

    for (int i = 0; i < kNumThreads; i++) {
        threads.emplace_back([&segment_manager, &segment, &mount_success,
                              &mount_duplicate]() {
            auto result = segment_manager.MountSegment(segment);
            if (result.has_value()) {
                mount_success.fetch_add(1);
            } else if (result.error() == ErrorCode::SEGMENT_ALREADY_EXISTS) {
                mount_duplicate.fetch_add(1);
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // Exactly one mount should succeed
    EXPECT_EQ(mount_success, 1);
    EXPECT_EQ(mount_duplicate, kNumThreads - 1);
}

// ============================================================
// Additional Interface Tests
// ============================================================

TEST_F(SegmentTest, QuerySegmentAndIp) {
    CentralizedSegmentManager segment_manager;
    Segment segment;
    segment.id = generate_uuid();
    segment.name = "test_ip_segment";
    segment.size = 1024;
    segment.extra = CentralizedSegmentExtraData{
        .base = 0x100000000, .te_endpoint = "192.168.1.100:1234"};

    ASSERT_TRUE(segment_manager.MountSegment(segment).has_value());

    // Test QuerySegment
    auto query_res = segment_manager.QuerySegment(segment.id);
    ASSERT_TRUE(query_res.has_value());
    EXPECT_EQ(query_res.value()->name, segment.name);

    // Test QueryIp
    auto ip_res = segment_manager.QueryIp();
    ASSERT_TRUE(ip_res.has_value());
    ASSERT_EQ(ip_res.value().size(), 1);
    EXPECT_EQ(ip_res.value()[0], "192.168.1.100");

    // Test QuerySegment Not Found
    EXPECT_EQ(segment_manager.QuerySegment(generate_uuid()).error(),
              ErrorCode::SEGMENT_NOT_FOUND);
}

TEST_F(SegmentTest, Callbacks) {
    CentralizedSegmentManager segment_manager;
    UUID callback_removed_id;
    bool segment_removal_callback_triggered = false;
    segment_manager.SetSegmentRemovalCallback([&](const UUID& id) {
        callback_removed_id = id;
        segment_removal_callback_triggered = true;
    });

    std::string allocator_change_segment_name;
    bool allocator_add_triggered = false;
    bool allocator_remove_triggered = false;
    segment_manager.SetAllocatorChangeCallback(
        [&](const std::string& name,
            const std::shared_ptr<BufferAllocatorBase>& allocator,
            bool is_add) -> tl::expected<void, ErrorCode> {
            allocator_change_segment_name = name;
            if (is_add)
                allocator_add_triggered = true;
            else
                allocator_remove_triggered = true;
            return {};
        });

    Segment segment;
    segment.id = generate_uuid();
    segment.name = "callback_seg";
    segment.size = 1024;
    segment.extra =
        CentralizedSegmentExtraData{.base = 0x100000000, .te_endpoint = ""};

    // Test Mount calls AllocatorChangeCallback
    ASSERT_TRUE(segment_manager.MountSegment(segment).has_value());
    EXPECT_TRUE(allocator_add_triggered);
    EXPECT_EQ(allocator_change_segment_name, segment.name);

    // Test SetGlobalVisibility
    ASSERT_TRUE(segment_manager.SetGlobalVisibility(false).has_value());
    EXPECT_TRUE(allocator_remove_triggered);
    allocator_remove_triggered = false;
    allocator_add_triggered = false;
    ASSERT_TRUE(segment_manager.SetGlobalVisibility(true).has_value());
    EXPECT_TRUE(allocator_add_triggered);

    // Test Unmount calls Local and Removal Callbacks
    ASSERT_TRUE(segment_manager.UnmountSegment(segment.id).has_value());
    EXPECT_TRUE(allocator_remove_triggered);
    EXPECT_TRUE(segment_removal_callback_triggered);
    EXPECT_EQ(callback_removed_id, segment.id);
}

}  // namespace mooncake

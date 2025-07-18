#include "segment.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

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

    void ValidateMountedSegments(const SegmentManager& segment_manager,
                                 const std::vector<Segment>& segments,
                                 const std::vector<UUID>& client_ids) {
        // validate client_segments_ and mounted_segments_
        size_t total_num = 0;
        for (const auto& it : segment_manager.client_segments_) {
            total_num += it.second.size();
        }
        ASSERT_EQ(total_num, segments.size());
        ASSERT_EQ(segment_manager.mounted_segments_.size(), segments.size());
        for (size_t i = 0; i < client_ids.size(); i++) {
            auto client_it =
                segment_manager.client_segments_.find(client_ids[i]);
            ASSERT_NE(client_it, segment_manager.client_segments_.end());
            auto segment_it =
                std::find(client_it->second.begin(), client_it->second.end(),
                          segments[i].id);
            ASSERT_NE(segment_it, client_it->second.end());
            ASSERT_EQ(*segment_it, segments[i].id);

            ASSERT_NE(segment_manager.mounted_segments_.find(segments[i].id),
                      segment_manager.mounted_segments_.end());
            MountedSegment seg =
                segment_manager.mounted_segments_.at(segments[i].id);
            ASSERT_EQ(seg.segment.id, segments[i].id);
            ASSERT_EQ(seg.segment.name, segments[i].name);
            ASSERT_EQ(seg.segment.size, segments[i].size);
            ASSERT_EQ(seg.segment.base, segments[i].base);
            ASSERT_EQ(seg.status, SegmentStatus::OK);
            ASSERT_EQ(seg.buf_allocator->getSegmentName(), segments[i].name);
            ASSERT_EQ(seg.buf_allocator->capacity(), segments[i].size);
        }

        // validate allocators and allocators_by_name
        total_num = 0;
        for (const auto& it : segment_manager.allocators_by_name_) {
            total_num += it.second.size();
        }
        ASSERT_EQ(total_num, segments.size());
        ASSERT_EQ(segment_manager.allocators_.size(), segments.size());
        for (const auto& segment : segments) {
            MountedSegment mounted_segment =
                segment_manager.mounted_segments_.at(segment.id);
            auto allocator = mounted_segment.buf_allocator;

            // validate allocators_
            ASSERT_NE(std::find(segment_manager.allocators_.begin(),
                                segment_manager.allocators_.end(),
                                mounted_segment.buf_allocator),
                      segment_manager.allocators_.end());

            // validate allocators_by_name
            auto map_it =
                segment_manager.allocators_by_name_.find(segment.name);
            ASSERT_NE(map_it, segment_manager.allocators_by_name_.end());
            auto name_allocator_it = map_it->second.begin();
            for (; name_allocator_it != map_it->second.end();
                 name_allocator_it++) {
                if (*name_allocator_it == allocator) {
                    break;
                }
            }
            ASSERT_NE(name_allocator_it, map_it->second.end());
        }
    }

    void ValidateMountedSegment(const SegmentManager& segment_manager,
                                const Segment segment, const UUID& client_id) {
        std::vector<Segment> segments;
        segments.push_back(segment);
        std::vector<UUID> client_ids;
        client_ids.push_back(client_id);
        ValidateMountedSegments(segment_manager, segments, client_ids);
    }
};

// Mount Segment Operations Tests:
TEST_F(SegmentTest, MountSegmentSuccess) {
    SegmentManager segment_manager;
    // Create a valid segment and client ID
    Segment segment;
    segment.id = generate_uuid();
    segment.name = "test_segment";
    segment.size = 1024 * 1024 * 16;
    segment.base = 0x100000000;

    UUID client_id = generate_uuid();

    // Get segment access and attempt to mount
    auto segment_access = segment_manager.getSegmentAccess();
    ASSERT_EQ(segment_access.MountSegment(segment, client_id), ErrorCode::OK);

    // Verify segment is properly mounted
    ValidateMountedSegment(segment_manager, segment, client_id);
}

// MountSegmentDuplicate Tests:
// 1. MountSegment with the same segment id. The second mount operation return
// SEGMENT_ALREADY_EXISTS.
// 2. MountSegment with different segment id and the same segment name should be
// considered as different segments. Validate the status of SegmentManager use
// ValidateMountedSegments function.
TEST_F(SegmentTest, MountSegmentDuplicate) {
    SegmentManager segment_manager;
    // Create a valid segment and client ID
    Segment segment;
    segment.id = generate_uuid();
    segment.name = "test_segment";
    segment.size = 1024 * 1024 * 16;
    segment.base = 0x100000000;

    UUID client_id = generate_uuid();

    // Get segment access and mount first time
    auto segment_access = segment_manager.getSegmentAccess();
    ASSERT_EQ(segment_access.MountSegment(segment, client_id), ErrorCode::OK);

    // Verify first mount
    ValidateMountedSegment(segment_manager, segment, client_id);

    // Test duplicate mount - mount the same segment again
    ASSERT_EQ(segment_access.MountSegment(segment, client_id),
              ErrorCode::SEGMENT_ALREADY_EXISTS);

    // Verify state remains the same after duplicate mount
    ValidateMountedSegment(segment_manager, segment, client_id);

    // Create a new segment with same name but different ID
    Segment segment2;
    segment2.id = generate_uuid();  // Different ID
    segment2.name = segment.name;   // Same name
    segment2.size = segment.size * 2;
    segment2.base = segment.base + segment.size;

    // Mount the second segment
    ASSERT_EQ(segment_access.MountSegment(segment2, client_id), ErrorCode::OK);

    // Verify both segments are mounted correctly
    std::vector<Segment> segments = {segment, segment2};
    std::vector<UUID> client_ids = {client_id, client_id};
    ValidateMountedSegments(segment_manager, segments, client_ids);
}

// UnmountSegmentSuccess:
// 1. Mount a segment and then unmount it. Unmount operation return success.
// 2. Use ValidateMountedSegments function to validate the status of
// SegmentManager.
TEST_F(SegmentTest, UnmountSegmentSuccess) {
    SegmentManager segment_manager;

    // Create and mount a segment
    Segment segment;
    segment.id = generate_uuid();
    segment.name = "test_segment";
    segment.size = 1024 * 1024 * 16;
    segment.base = 0x100000000;

    UUID client_id = generate_uuid();

    // Get segment access and mount
    auto segment_access = segment_manager.getSegmentAccess();
    ASSERT_EQ(segment_access.MountSegment(segment, client_id), ErrorCode::OK);

    // Verify segment is mounted correctly
    ValidateMountedSegment(segment_manager, segment, client_id);

    // Prepare unmount
    size_t metrics_dec_capacity = 0;
    ASSERT_EQ(
        segment_access.PrepareUnmountSegment(segment.id, metrics_dec_capacity),
        ErrorCode::OK);
    ASSERT_EQ(metrics_dec_capacity, segment.size);

    // Commit unmount
    ASSERT_EQ(segment_access.CommitUnmountSegment(segment.id, client_id,
                                                  metrics_dec_capacity),
              ErrorCode::OK);

    // Verify segment is unmounted correctly
    std::vector<Segment> empty_segment_vec;
    std::vector<UUID> empty_client_ids_vec;
    ValidateMountedSegments(segment_manager, empty_segment_vec,
                            empty_client_ids_vec);
}

// UnmountSegmentDuplicate:
// 1. Mount a segment and then unmount it twice. The second unmount operation
// returns SEGMENT_NOT_FOUND.
// 2. Only use ValidateMountedSegments function to validate the status of
// SegmentManager. Do not use other interfaces for validation.
TEST_F(SegmentTest, UnmountSegmentDuplicate) {
    SegmentManager segment_manager;

    // Create and mount a segment
    Segment segment;
    segment.id = generate_uuid();
    segment.name = "test_segment";
    segment.size = 1024 * 1024 * 16;
    segment.base = 0x100000000;

    UUID client_id = generate_uuid();

    // Get segment access and mount
    auto segment_access = segment_manager.getSegmentAccess();
    ASSERT_EQ(segment_access.MountSegment(segment, client_id), ErrorCode::OK);

    // Verify initial mounted state
    ValidateMountedSegment(segment_manager, segment, client_id);

    // First unmount
    size_t metrics_dec_capacity = 0;
    ASSERT_EQ(
        segment_access.PrepareUnmountSegment(segment.id, metrics_dec_capacity),
        ErrorCode::OK);
    ASSERT_EQ(segment_access.CommitUnmountSegment(segment.id, client_id,
                                                  metrics_dec_capacity),
              ErrorCode::OK);

    // Verify segment is unmounted after first unmount
    std::vector<Segment> empty_segment_vec;
    std::vector<UUID> empty_client_ids_vec;
    ValidateMountedSegments(segment_manager, empty_segment_vec,
                            empty_client_ids_vec);

    // Second unmount attempt
    metrics_dec_capacity = 0;
    ASSERT_EQ(
        segment_access.PrepareUnmountSegment(segment.id, metrics_dec_capacity),
        ErrorCode::SEGMENT_NOT_FOUND);

    // Verify segment remains unmounted after second unmount
    ValidateMountedSegments(segment_manager, empty_segment_vec,
                            empty_client_ids_vec);
}

// ReMountSegmentSuccess:
// 1. Mount a segment A;
// 2. Remount two segments: A and B where A is already mounted and B is a new
// segment. The remount operation return success.
// 3. Only use ValidateMountedSegments function to validate the status of
// SegmentManager. Do not use other interfaces for validation.
TEST_F(SegmentTest, ReMountSegmentSuccess) {
    SegmentManager segment_manager;

    // Create and mount segment A
    Segment segment_a;
    segment_a.id = generate_uuid();
    segment_a.name = "test_segment_a";
    segment_a.size = 1024 * 1024 * 16;
    segment_a.base = 0x100000000;

    UUID client_id = generate_uuid();

    // Get segment access and mount segment A
    auto segment_access = segment_manager.getSegmentAccess();
    ASSERT_EQ(segment_access.MountSegment(segment_a, client_id), ErrorCode::OK);

    // Verify segment A is mounted correctly
    ValidateMountedSegment(segment_manager, segment_a, client_id);

    // Create segment B
    Segment segment_b;
    segment_b.id = generate_uuid();
    segment_b.name = "test_segment_b";
    segment_b.size = 1024 * 1024 * 32;
    segment_b.base = 0x200000000;

    // Remount both segments A and B
    std::vector<Segment> segments_to_remount = {segment_a, segment_b};
    ASSERT_EQ(segment_access.ReMountSegment(segments_to_remount, client_id),
              ErrorCode::OK);

    // Verify both segments are mounted correctly
    std::vector<UUID> client_ids = {client_id, client_id};
    ValidateMountedSegments(segment_manager, segments_to_remount, client_ids);
}

// ReMountUnmountingSegment:
// 1. Mount a segment A;
// 2. PrepareUnmount segment A;
// 3. Remount segment A. The remount operation return
// UNAVAILABLE_IN_CURRENT_STATUS.
// 4. CommitUnmount segment A;
// 5. Only use ValidateMountedSegments function to validate the status of
// SegmentManager. Do not use other interfaces for validation.
TEST_F(SegmentTest, ReMountUnmountingSegment) {
    SegmentManager segment_manager;

    // Create and mount segment A
    Segment segment_a;
    segment_a.id = generate_uuid();
    segment_a.name = "test_segment_a";
    segment_a.size = 1024 * 1024 * 16;
    segment_a.base = 0x100000000;

    UUID client_id = generate_uuid();

    // Get segment access and mount segment A
    auto segment_access = segment_manager.getSegmentAccess();
    ASSERT_EQ(segment_access.MountSegment(segment_a, client_id), ErrorCode::OK);

    // Verify segment A is mounted correctly
    ValidateMountedSegment(segment_manager, segment_a, client_id);

    // Prepare unmount segment A
    size_t metrics_dec_capacity = 0;
    ASSERT_EQ(segment_access.PrepareUnmountSegment(segment_a.id,
                                                   metrics_dec_capacity),
              ErrorCode::OK);

    // Attempt to remount segment A while it's in UNMOUNTING state
    std::vector<Segment> segments_to_remount = {segment_a};
    ASSERT_EQ(segment_access.ReMountSegment(segments_to_remount, client_id),
              ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);

    // Complete the unmount process
    ASSERT_EQ(segment_access.CommitUnmountSegment(segment_a.id, client_id,
                                                  metrics_dec_capacity),
              ErrorCode::OK);

    // Verify segment is completely unmounted
    std::vector<Segment> empty_segment_vec;
    std::vector<UUID> empty_client_ids_vec;
    ValidateMountedSegments(segment_manager, empty_segment_vec,
                            empty_client_ids_vec);
}

// QuerySegments:
// 1. Create and mount 10 different segments with different names and different
// client ids;
// 2. Test GetClientSegments, verify the return value is correct.
// 3. Test GetAllSegments, verify the return value is correct.
// 4. Test QuerySegments, verify the return value is correct.
TEST_F(SegmentTest, QuerySegments) {
    SegmentManager segment_manager;
    auto segment_access = segment_manager.getSegmentAccess();

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
        segment.base =
            0x100000000 + (i * 0x100000000);  // Different base addresses

        // Create client ID
        UUID client_id = generate_uuid();

        // Mount segment
        ASSERT_EQ(segment_access.MountSegment(segment, client_id),
                  ErrorCode::OK);

        // Store for verification
        segments.push_back(segment);
        client_ids.push_back(client_id);
        expected_client_segments[client_id] = segment.id;
    }

    // Verify all segments are mounted correctly
    ValidateMountedSegments(segment_manager, segments, client_ids);

    // Test GetClientSegments for each client
    for (size_t i = 0; i < client_ids.size(); i++) {
        std::vector<Segment> client_segments;
        ASSERT_EQ(
            segment_access.GetClientSegments(client_ids[i], client_segments),
            ErrorCode::OK);

        // Verify correct number of segments
        ASSERT_EQ(client_segments.size(), 1);

        // Verify all expected segments are present
        ASSERT_EQ(client_segments[0].id,
                  expected_client_segments[client_ids[i]]);
    }

    // Test GetAllSegments
    std::vector<std::string> all_segments;
    ASSERT_EQ(segment_access.GetAllSegments(all_segments), ErrorCode::OK);

    // Verify correct number of segments
    ASSERT_EQ(all_segments.size(), segments.size());

    // Verify all segment names are present
    for (const auto& segment : segments) {
        ASSERT_NE(
            std::find(all_segments.begin(), all_segments.end(), segment.name),
            all_segments.end());
    }

    // Test QuerySegments for each segment
    for (const auto& segment : segments) {
        size_t used = 0, capacity = 0;
        ASSERT_EQ(segment_access.QuerySegments(segment.name, used, capacity),
                  ErrorCode::OK);

        // Verify capacity matches segment size
        ASSERT_EQ(capacity, segment.size);

        // Verify used space is 0 for newly mounted segments
        ASSERT_EQ(used, 0);
    }

    // Test QuerySegments for non-existent segment
    size_t used = 0, capacity = 0;
    ASSERT_EQ(
        segment_access.QuerySegments("non_existent_segment", used, capacity),
        ErrorCode::SEGMENT_NOT_FOUND);
    ASSERT_EQ(used, 0);
    ASSERT_EQ(capacity, 0);
}

}  // namespace mooncake

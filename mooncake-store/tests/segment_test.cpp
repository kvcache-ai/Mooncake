#include "segment.h"

#include "master_metric_manager.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <boost/functional/hash.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <functional>
#include <future>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

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

        // validate allocator manager
        const auto& allocator_manager = segment_manager.allocator_manager_;

        total_num = 0;
        for (const auto& name : allocator_manager.getNames()) {
            auto allocators = allocator_manager.getAllocators(name);
            ASSERT_NE(allocators, nullptr);
            total_num += allocators->size();
        }
        ASSERT_EQ(total_num, segments.size());

        for (const auto& segment : segments) {
            auto allocators = allocator_manager.getAllocators(segment.name);
            ASSERT_NE(allocators, nullptr);

            // validate allocator exist in allocator_manager
            MountedSegment mounted_segment =
                segment_manager.mounted_segments_.at(segment.id);
            auto allocator = mounted_segment.buf_allocator;
            ASSERT_NE(std::find(allocators->begin(), allocators->end(),
                                mounted_segment.buf_allocator),
                      allocators->end());
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

    bool HasAllocatorForSegment(const SegmentManager& segment_manager,
                                const UUID& segment_id) {
        const auto mounted_it =
            segment_manager.mounted_segments_.find(segment_id);
        if (mounted_it == segment_manager.mounted_segments_.end()) {
            return false;
        }

        const auto& mounted_segment = mounted_it->second;
        const auto* allocators =
            segment_manager.allocator_manager_.getAllocators(
                mounted_segment.segment.name);
        if (allocators == nullptr) {
            return false;
        }

        return std::find(allocators->begin(), allocators->end(),
                         mounted_segment.buf_allocator) != allocators->end();
    }
    void InstallSharedCxlAllocatorForTesting(
        SegmentManager& segment_manager,
        const std::shared_ptr<BufferAllocatorBase>& allocator,
        const std::vector<Segment>& segments) {
        if (segment_manager.cxl_global_allocator_ != allocator) {
            allocator->AttachUsageTracker(segment_manager.usage_tracker_);
            segment_manager.cxl_global_allocator_ = allocator;
        }
        for (const auto& segment : segments) {
            segment_manager.mounted_segments_[segment.id] = {
                segment, SegmentStatus::OK, allocator};
        }
    }

    std::unique_lock<std::shared_mutex> HoldSegmentMutexForTesting(
        SegmentManager& segment_manager) {
        return std::unique_lock<std::shared_mutex>(
            segment_manager.segment_mutex_);
    }

    std::shared_ptr<BufferAllocatorBase> GetNoFAllocatorForTesting(
        NoFSegmentManager& segment_manager, const UUID& segment_id) {
        auto it = segment_manager.mounted_segments_.find(segment_id);
        return it == segment_manager.mounted_segments_.end()
                   ? nullptr
                   : it->second.buf_allocator;
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

TEST_F(SegmentTest, MemoryUsageSnapshotTracksMountedAllocatorState) {
    SegmentManager segment_manager(BufferAllocatorType::OFFSET);
    constexpr size_t kSegmentSize = 16 * 1024 * 1024;
    constexpr size_t kAllocationSize = 4 * 1024 * 1024;

    Segment segment;
    segment.id = generate_uuid();
    segment.name = "usage_snapshot_segment";
    segment.size = kSegmentSize;
    segment.base = 0x100000000;
    UUID client_id = generate_uuid();

    std::shared_ptr<BufferAllocatorBase> allocator;
    {
        auto segment_access = segment_manager.getSegmentAccess();
        ASSERT_EQ(segment_access.MountSegment(segment, client_id),
                  ErrorCode::OK);
        allocator = segment_access.GetAllocator(segment.id);
    }
    ASSERT_NE(allocator, nullptr);

    auto buffer = allocator->allocate(kAllocationSize);
    ASSERT_NE(buffer, nullptr);

    auto snapshot = segment_manager.GetMemoryUsageSnapshot();
    auto usage = segment_manager.GetMemoryUsage();
    EXPECT_EQ(snapshot.used_bytes, kAllocationSize);
    EXPECT_EQ(snapshot.capacity_bytes, kSegmentSize);
    EXPECT_DOUBLE_EQ(snapshot.used_ratio(), 0.25);
    EXPECT_EQ(usage.used_bytes, kAllocationSize);
    EXPECT_EQ(usage.capacity_bytes, kSegmentSize);
    EXPECT_DOUBLE_EQ(usage.used_ratio(), 0.25);
    ASSERT_EQ(snapshot.segments.size(), 1u);
    EXPECT_EQ(snapshot.segments.at(segment.name).used_bytes, kAllocationSize);
    EXPECT_EQ(snapshot.segments.at(segment.name).capacity_bytes, kSegmentSize);

    {
        auto segment_access = segment_manager.getSegmentAccess();
        ASSERT_EQ(segment_access.SetSegmentStatusByName(
                      segment.name, SegmentStatus::DRAINING),
                  ErrorCode::OK);
    }
    snapshot = segment_manager.GetMemoryUsageSnapshot();
    EXPECT_EQ(snapshot.used_bytes, kAllocationSize);
    EXPECT_EQ(snapshot.capacity_bytes, kSegmentSize);

    buffer.reset();
    {
        auto segment_access = segment_manager.getSegmentAccess();
        size_t metrics_dec_capacity = 0;
        ASSERT_EQ(segment_access.PrepareUnmountSegment(segment.id,
                                                       metrics_dec_capacity),
                  ErrorCode::OK);
        ASSERT_EQ(segment_access.CommitUnmountSegment(segment.id, client_id,
                                                      metrics_dec_capacity),
                  ErrorCode::OK);
    }
    allocator.reset();

    snapshot = segment_manager.GetMemoryUsageSnapshot();
    usage = segment_manager.GetMemoryUsage();
    EXPECT_EQ(snapshot.used_bytes, 0u);
    EXPECT_EQ(snapshot.capacity_bytes, 0u);
    EXPECT_DOUBLE_EQ(snapshot.used_ratio(), 0.0);
    EXPECT_EQ(usage.used_bytes, 0u);
    EXPECT_EQ(usage.capacity_bytes, 0u);
}

TEST_F(SegmentTest, AggregateMemoryUsageDoesNotTakeSegmentMutex) {
    SegmentManager segment_manager(BufferAllocatorType::OFFSET);
    auto segment_lock = HoldSegmentMutexForTesting(segment_manager);
    auto usage = std::async(std::launch::async, [&segment_manager] {
        return segment_manager.GetMemoryUsage();
    });

    const auto status = usage.wait_for(std::chrono::seconds(1));
    segment_lock.unlock();

    ASSERT_EQ(status, std::future_status::ready);
    EXPECT_EQ(usage.get().capacity_bytes, 0u);
}

TEST_F(SegmentTest, AggregateUsageSurvivesConcurrentUnmountAndDeallocate) {
    SegmentManager segment_manager(BufferAllocatorType::OFFSET);
    constexpr size_t kSegmentSize = 16 * 1024 * 1024;
    constexpr size_t kAllocationSize = 4 * 1024 * 1024;

    Segment segment;
    segment.id = generate_uuid();
    segment.name = "usage_race_segment";
    segment.size = kSegmentSize;
    segment.base = 0x1C0000000;
    UUID client_id = generate_uuid();

    std::shared_ptr<BufferAllocatorBase> allocator;
    {
        auto segment_access = segment_manager.getSegmentAccess();
        ASSERT_EQ(segment_access.MountSegment(segment, client_id),
                  ErrorCode::OK);
        allocator = segment_access.GetAllocator(segment.id);
    }
    ASSERT_NE(allocator, nullptr);
    auto buffer = allocator->allocate(kAllocationSize);
    ASSERT_NE(buffer, nullptr);

    std::atomic<int> phase{0};
    static std::atomic<int>* phase_ptr = nullptr;
    phase_ptr = &phase;
    BufferAllocatorBase::SetRecordDeallocationHookForTesting([]() {
        phase_ptr->store(1, std::memory_order_release);
        while (phase_ptr->load(std::memory_order_acquire) != 2) {
            std::this_thread::yield();
        }
    });

    std::thread dealloc_thread([&buffer] { buffer.reset(); });
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    struct Cleanup {
        std::atomic<int>& phase;
        std::thread& thread;
        ~Cleanup() {
            phase.store(2, std::memory_order_release);
            if (thread.joinable()) {
                thread.join();
            }
            BufferAllocatorBase::SetRecordDeallocationHookForTesting(nullptr);
        }
    } cleanup{phase, dealloc_thread};

    while (phase.load(std::memory_order_acquire) != 1) {
        ASSERT_LT(std::chrono::steady_clock::now(), deadline)
            << "deallocate did not enter RecordDeallocation";
        std::this_thread::yield();
    }

    {
        auto segment_access = segment_manager.getSegmentAccess();
        size_t metrics_dec_capacity = 0;
        ASSERT_EQ(segment_access.PrepareUnmountSegment(segment.id,
                                                       metrics_dec_capacity),
                  ErrorCode::OK);
        ASSERT_EQ(segment_access.CommitUnmountSegment(segment.id, client_id,
                                                      metrics_dec_capacity),
                  ErrorCode::OK);
    }

    cleanup.phase.store(2, std::memory_order_release);
    dealloc_thread.join();
    BufferAllocatorBase::SetRecordDeallocationHookForTesting(nullptr);
    allocator.reset();

    const auto usage = segment_manager.GetMemoryUsage();
    EXPECT_EQ(usage.used_bytes, 0u);
    EXPECT_EQ(usage.capacity_bytes, 0u);
}

TEST_F(SegmentTest, AggregateMemoryUsageFollowsAllocatorReplacement) {
    SegmentManager segment_manager(BufferAllocatorType::OFFSET);
    constexpr size_t kSegmentSize = 16 * 1024 * 1024;
    constexpr size_t kOldAllocationSize = 4 * 1024 * 1024;
    constexpr size_t kRestoredAllocationSize = 8 * 1024 * 1024;

    Segment segment;
    segment.id = generate_uuid();
    segment.name = "usage_replacement_segment";
    segment.size = kSegmentSize;
    segment.base = 0x180000000;
    UUID client_id = generate_uuid();

    std::shared_ptr<BufferAllocatorBase> old_allocator;
    {
        auto segment_access = segment_manager.getSegmentAccess();
        ASSERT_EQ(segment_access.MountSegment(segment, client_id),
                  ErrorCode::OK);
        old_allocator = segment_access.GetAllocator(segment.id);
    }
    ASSERT_NE(old_allocator, nullptr);
    auto old_buffer = old_allocator->allocate(kOldAllocationSize);
    ASSERT_NE(old_buffer, nullptr);

    auto replacement = std::make_shared<OffsetBufferAllocator>(
        segment.name, segment.base, segment.size, segment.te_endpoint);
    auto restored_buffer = replacement->allocate(kRestoredAllocationSize);
    ASSERT_NE(restored_buffer, nullptr);
    {
        auto segment_access = segment_manager.getSegmentAccess();
        ASSERT_TRUE(segment_access.ReplaceAllocators(
            {{segment.id, old_allocator, replacement}}));
    }
    old_allocator.reset();

    auto usage = segment_manager.GetMemoryUsage();
    EXPECT_EQ(usage.used_bytes, kRestoredAllocationSize);
    EXPECT_EQ(usage.capacity_bytes, kSegmentSize);

    old_buffer.reset();
    usage = segment_manager.GetMemoryUsage();
    EXPECT_EQ(usage.used_bytes, kRestoredAllocationSize);

    restored_buffer.reset();
    {
        auto segment_access = segment_manager.getSegmentAccess();
        size_t metrics_dec_capacity = 0;
        ASSERT_EQ(segment_access.PrepareUnmountSegment(segment.id,
                                                       metrics_dec_capacity),
                  ErrorCode::OK);
        ASSERT_EQ(segment_access.CommitUnmountSegment(segment.id, client_id,
                                                      metrics_dec_capacity),
                  ErrorCode::OK);
    }
    replacement.reset();
    EXPECT_EQ(segment_manager.GetMemoryUsage().capacity_bytes, 0u);
}

TEST_F(SegmentTest, MemoryUsageSnapshotCountsSharedCxlAllocatorOnce) {
    SegmentManager segment_manager(BufferAllocatorType::OFFSET,
                                   /*enable_cxl=*/true);
    constexpr size_t kSegmentSize = 16 * 1024 * 1024;
    constexpr size_t kAllocationSize = 4 * 1024 * 1024;
    auto allocator = std::make_shared<OffsetBufferAllocator>(
        "cxl_pool", 0x200000000, kSegmentSize, "cxl_pool");
    InstallSharedCxlAllocatorForTesting(segment_manager, allocator, {});

    auto buffer = allocator->allocate(kAllocationSize);
    ASSERT_NE(buffer, nullptr);

    auto snapshot = segment_manager.GetMemoryUsageSnapshot();
    auto usage = segment_manager.GetMemoryUsage();
    EXPECT_EQ(snapshot.used_bytes, kAllocationSize);
    EXPECT_EQ(snapshot.capacity_bytes, kSegmentSize);
    EXPECT_EQ(usage.used_bytes, kAllocationSize);
    EXPECT_EQ(usage.capacity_bytes, kSegmentSize);

    Segment first;
    first.id = generate_uuid();
    first.name = "cxl_client_a";
    first.protocol = "cxl";
    Segment second;
    second.id = generate_uuid();
    second.name = "cxl_client_b";
    second.protocol = "cxl";
    InstallSharedCxlAllocatorForTesting(segment_manager, allocator,
                                        {first, second});

    snapshot = segment_manager.GetMemoryUsageSnapshot();
    usage = segment_manager.GetMemoryUsage();
    EXPECT_EQ(snapshot.used_bytes, kAllocationSize);
    EXPECT_EQ(snapshot.capacity_bytes, kSegmentSize);
    EXPECT_EQ(usage.used_bytes, kAllocationSize);
    EXPECT_EQ(usage.capacity_bytes, kSegmentSize);
    ASSERT_EQ(snapshot.segments.size(), 1u);
    EXPECT_EQ(snapshot.segments.at("cxl_pool").used_bytes, kAllocationSize);
    EXPECT_EQ(snapshot.segments.at("cxl_pool").capacity_bytes, kSegmentSize);
}

TEST_F(SegmentTest, NoFUsageSnapshotSurvivesMetricsReset) {
    NoFSegmentManager segment_manager(BufferAllocatorType::OFFSET);
    constexpr size_t kSegmentSize = 16 * 1024 * 1024;
    constexpr size_t kAllocationSize = 4 * 1024 * 1024;

    NoFSegment segment;
    segment.id = generate_uuid();
    segment.name = "nof_usage_snapshot_segment";
    segment.size = kSegmentSize;
    segment.base = 0x300000000;
    segment.te_endpoint = "nof_usage_snapshot_endpoint";
    UUID client_id = generate_uuid();

    {
        auto segment_access = segment_manager.getNoFSegmentAccess();
        ASSERT_EQ(segment_access.MountSegment(segment, client_id),
                  ErrorCode::OK);
    }
    auto allocator = GetNoFAllocatorForTesting(segment_manager, segment.id);
    ASSERT_NE(allocator, nullptr);
    auto buffer = allocator->allocate(kAllocationSize);
    ASSERT_NE(buffer, nullptr);

    auto snapshot = segment_manager.GetUsageSnapshot();
    auto usage = segment_manager.GetUsage();
    EXPECT_EQ(snapshot.used_bytes, kAllocationSize);
    EXPECT_EQ(snapshot.capacity_bytes, kSegmentSize);
    EXPECT_DOUBLE_EQ(snapshot.used_ratio(), 0.25);
    EXPECT_EQ(usage.used_bytes, kAllocationSize);
    EXPECT_EQ(usage.capacity_bytes, kSegmentSize);
    EXPECT_DOUBLE_EQ(usage.used_ratio(), 0.25);

    auto& metrics = MasterMetricManager::instance();
    metrics.reset_allocated_nof_size();
    metrics.reset_total_nof_capacity();
    EXPECT_EQ(metrics.get_allocated_nof_size(), 0);
    EXPECT_EQ(metrics.get_total_nof_capacity(), 0);

    snapshot = segment_manager.GetUsageSnapshot();
    usage = segment_manager.GetUsage();
    EXPECT_EQ(snapshot.used_bytes, kAllocationSize);
    EXPECT_EQ(snapshot.capacity_bytes, kSegmentSize);
    EXPECT_DOUBLE_EQ(snapshot.used_ratio(), 0.25);
    EXPECT_EQ(usage.used_bytes, kAllocationSize);
    EXPECT_EQ(usage.capacity_bytes, kSegmentSize);
    EXPECT_DOUBLE_EQ(usage.used_ratio(), 0.25);

    // Restore global gauges before teardown because allocator and segment
    // cleanup still emit their matching decrements.
    metrics.inc_allocated_nof_size("", kAllocationSize);
    metrics.inc_total_nof_capacity("", kSegmentSize);
    buffer.reset();
    {
        auto segment_access = segment_manager.getNoFSegmentAccess();
        size_t metrics_dec_capacity = 0;
        ASSERT_EQ(segment_access.PrepareUnmountSegment(segment.id,
                                                       metrics_dec_capacity),
                  ErrorCode::OK);
        ASSERT_EQ(segment_access.CommitUnmountSegment(segment.id, client_id,
                                                      metrics_dec_capacity),
                  ErrorCode::OK);
    }
    allocator.reset();
    EXPECT_EQ(segment_manager.GetUsage().used_bytes, 0u);
    EXPECT_EQ(segment_manager.GetUsage().capacity_bytes, 0u);
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

TEST_F(SegmentTest, SegmentLifecycleStatusControlsAllocation) {
    SegmentManager segment_manager;

    Segment segment;
    segment.id = generate_uuid();
    segment.name = "status_segment";
    segment.size = 1024 * 1024 * 16;
    segment.base = 0x100000000;

    UUID client_id = generate_uuid();

    auto segment_access = segment_manager.getSegmentAccess();
    ASSERT_EQ(segment_access.MountSegment(segment, client_id), ErrorCode::OK);

    SegmentStatus status = SegmentStatus::UNDEFINED;
    ASSERT_EQ(segment_access.GetSegmentStatusByName(segment.name, status),
              ErrorCode::OK);
    EXPECT_EQ(status, SegmentStatus::OK);
    EXPECT_TRUE(segment_access.IsSegmentAllocatable(segment.name));
    EXPECT_TRUE(HasAllocatorForSegment(segment_manager, segment.id));

    ASSERT_EQ(segment_access.SetSegmentStatusByName(segment.name,
                                                    SegmentStatus::DRAINING),
              ErrorCode::OK);
    ASSERT_EQ(segment_access.GetSegmentStatusByName(segment.name, status),
              ErrorCode::OK);
    EXPECT_EQ(status, SegmentStatus::DRAINING);
    EXPECT_FALSE(segment_access.IsSegmentAllocatable(segment.name));
    EXPECT_FALSE(HasAllocatorForSegment(segment_manager, segment.id));

    ASSERT_EQ(segment_access.SetSegmentStatusByName(segment.name,
                                                    SegmentStatus::DRAINED),
              ErrorCode::OK);
    ASSERT_EQ(segment_access.GetSegmentStatusByName(segment.name, status),
              ErrorCode::OK);
    EXPECT_EQ(status, SegmentStatus::DRAINED);
    EXPECT_FALSE(segment_access.IsSegmentAllocatable(segment.name));
    EXPECT_FALSE(HasAllocatorForSegment(segment_manager, segment.id));

    ASSERT_EQ(
        segment_access.SetSegmentStatusByName(segment.name, SegmentStatus::OK),
        ErrorCode::OK);
    ASSERT_EQ(segment_access.GetSegmentStatusByName(segment.name, status),
              ErrorCode::OK);
    EXPECT_EQ(status, SegmentStatus::OK);
    EXPECT_TRUE(segment_access.IsSegmentAllocatable(segment.name));
    EXPECT_TRUE(HasAllocatorForSegment(segment_manager, segment.id));
}

TEST_F(SegmentTest, HostOrderedSegmentsTracksMountStatusAndUnmount) {
    SegmentManager segment_manager;

    Segment segment0;
    segment0.id = generate_uuid();
    segment0.name = "host0_segment";
    segment0.size = 1024 * 1024 * 16;
    segment0.base = 0x100000000;
    segment0.host_id = "host0";

    Segment segment1;
    segment1.id = generate_uuid();
    segment1.name = "host1_segment";
    segment1.size = 1024 * 1024 * 16;
    segment1.base = 0x200000000;
    segment1.host_id = "host1";

    UUID client_id = generate_uuid();

    {
        auto segment_access = segment_manager.getSegmentAccess();
        ASSERT_EQ(segment_access.MountSegment(segment0, client_id),
                  ErrorCode::OK);
        ASSERT_EQ(segment_access.MountSegment(segment1, client_id),
                  ErrorCode::OK);
    }

    {
        auto allocator_access = segment_manager.getAllocatorAccess();
        auto ordered =
            allocator_access.GetHostOrderedSegments("host1", "test_key");
        ASSERT_GE(ordered.size(), 2u);
        EXPECT_EQ(ordered[0], segment1.name);
    }

    {
        auto segment_access = segment_manager.getSegmentAccess();
        ASSERT_EQ(segment_access.SetSegmentStatusByName(
                      segment1.name, SegmentStatus::DRAINING),
                  ErrorCode::OK);
    }

    {
        auto allocator_access = segment_manager.getAllocatorAccess();
        auto ordered =
            allocator_access.GetHostOrderedSegments("host1", "test_key");
        ASSERT_EQ(ordered.size(), 1u);
        EXPECT_EQ(ordered[0], segment0.name);
    }

    {
        auto segment_access = segment_manager.getSegmentAccess();
        ASSERT_EQ(segment_access.SetSegmentStatusByName(segment1.name,
                                                        SegmentStatus::OK),
                  ErrorCode::OK);
        size_t metrics_dec_capacity = 0;
        ASSERT_EQ(segment_access.PrepareUnmountSegment(segment1.id,
                                                       metrics_dec_capacity),
                  ErrorCode::OK);
        ASSERT_EQ(segment_access.CommitUnmountSegment(segment1.id, client_id,
                                                      metrics_dec_capacity),
                  ErrorCode::OK);
    }

    {
        auto allocator_access = segment_manager.getAllocatorAccess();
        auto ordered =
            allocator_access.GetHostOrderedSegments("host1", "test_key");
        ASSERT_EQ(ordered.size(), 1u);
        EXPECT_EQ(ordered[0], segment0.name);
    }
}

TEST_F(SegmentTest, HostOrderedSegmentsKeepsNameUntilLastSameNameSegmentGone) {
    SegmentManager segment_manager;

    Segment segment0;
    segment0.id = generate_uuid();
    segment0.name = "shared_host_segment";
    segment0.size = 1024 * 1024 * 16;
    segment0.base = 0x100000000;
    segment0.host_id = "host1";

    Segment segment1;
    segment1.id = generate_uuid();
    segment1.name = segment0.name;
    segment1.size = 1024 * 1024 * 16;
    segment1.base = 0x200000000;
    segment1.host_id = "host1";

    UUID client_id = generate_uuid();
    {
        auto segment_access = segment_manager.getSegmentAccess();
        ASSERT_EQ(segment_access.MountSegment(segment0, client_id),
                  ErrorCode::OK);
        ASSERT_EQ(segment_access.MountSegment(segment1, client_id),
                  ErrorCode::OK);
    }

    {
        auto allocator_access = segment_manager.getAllocatorAccess();
        auto ordered =
            allocator_access.GetHostOrderedSegments("host1", "test_key");
        ASSERT_EQ(ordered.size(), 1u);
        EXPECT_EQ(ordered[0], segment0.name);
    }

    {
        auto segment_access = segment_manager.getSegmentAccess();
        size_t metrics_dec_capacity = 0;
        ASSERT_EQ(segment_access.PrepareUnmountSegment(segment0.id,
                                                       metrics_dec_capacity),
                  ErrorCode::OK);
        ASSERT_EQ(segment_access.CommitUnmountSegment(segment0.id, client_id,
                                                      metrics_dec_capacity),
                  ErrorCode::OK);
    }

    {
        auto allocator_access = segment_manager.getAllocatorAccess();
        auto ordered =
            allocator_access.GetHostOrderedSegments("host1", "test_key");
        ASSERT_EQ(ordered.size(), 1u);
        EXPECT_EQ(ordered[0], segment1.name);
    }
}

TEST_F(SegmentTest, HostOrderedSegmentsRotateWithinSameHostByKey) {
    SegmentManager segment_manager;

    Segment segment_a;
    segment_a.id = generate_uuid();
    segment_a.name = "host1_segment_a";
    segment_a.size = 1024 * 1024 * 16;
    segment_a.base = 0x100000000;
    segment_a.host_id = "host1";

    Segment segment_b;
    segment_b.id = generate_uuid();
    segment_b.name = "host1_segment_b";
    segment_b.size = 1024 * 1024 * 16;
    segment_b.base = 0x200000000;
    segment_b.host_id = "host1";

    UUID client_id = generate_uuid();
    {
        auto segment_access = segment_manager.getSegmentAccess();
        ASSERT_EQ(segment_access.MountSegment(segment_a, client_id),
                  ErrorCode::OK);
        ASSERT_EQ(segment_access.MountSegment(segment_b, client_id),
                  ErrorCode::OK);
    }

    const std::string key = "stable_rotation_key";
    std::vector<std::string> sorted_segments = {segment_a.name, segment_b.name};
    std::sort(sorted_segments.begin(), sorted_segments.end());
    const size_t start = std::hash<std::string>{}(key) % sorted_segments.size();

    auto allocator_access = segment_manager.getAllocatorAccess();
    auto ordered = allocator_access.GetHostOrderedSegments("host1", key);
    ASSERT_EQ(ordered.size(), 2u);
    EXPECT_EQ(ordered[0], sorted_segments[start]);
    EXPECT_EQ(ordered[1],
              sorted_segments[(start + 1) % sorted_segments.size()]);
}

TEST_F(SegmentTest, PrepareUnmountDrainedSegment) {
    SegmentManager segment_manager;

    Segment segment;
    segment.id = generate_uuid();
    segment.name = "drained_segment";
    segment.size = 1024 * 1024 * 16;
    segment.base = 0x100000000;

    UUID client_id = generate_uuid();

    auto segment_access = segment_manager.getSegmentAccess();
    ASSERT_EQ(segment_access.MountSegment(segment, client_id), ErrorCode::OK);
    ASSERT_EQ(segment_access.SetSegmentStatusByName(segment.name,
                                                    SegmentStatus::DRAINED),
              ErrorCode::OK);

    size_t metrics_dec_capacity = 0;
    ASSERT_EQ(
        segment_access.PrepareUnmountSegment(segment.id, metrics_dec_capacity),
        ErrorCode::OK);
    ASSERT_EQ(segment_access.CommitUnmountSegment(segment.id, client_id,
                                                  metrics_dec_capacity),
              ErrorCode::OK);

    std::vector<Segment> empty_segments;
    std::vector<UUID> empty_client_ids;
    ValidateMountedSegments(segment_manager, empty_segments, empty_client_ids);
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

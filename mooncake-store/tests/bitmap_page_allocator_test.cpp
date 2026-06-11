#include "storage/distributed/bitmap_page_allocator.h"

#include <gtest/gtest.h>

#include <mutex>
#include <thread>
#include <unordered_set>
#include <vector>

namespace mooncake {

TEST(BitmapPageAllocatorTest, InitCreatesEmptyBitmap) {
    BitmapPageAllocator allocator;
    allocator.Init(64, 640);

    EXPECT_EQ(allocator.PageSize(), 64);
    EXPECT_EQ(allocator.NumPages(), 10);
    EXPECT_EQ(allocator.AllocatedCount(), 0);
    for (int64_t page = 0; page < allocator.NumPages(); ++page) {
        EXPECT_FALSE(allocator.IsAllocated(page));
    }
}

TEST(BitmapPageAllocatorTest, AllocatesSequentialAndMultiPageRanges) {
    BitmapPageAllocator allocator;
    allocator.Init(1, 8);

    EXPECT_EQ(allocator.Allocate(), 0);
    EXPECT_EQ(allocator.Allocate(3), 1);
    EXPECT_EQ(allocator.AllocatedCount(), 4);

    for (int64_t page = 0; page < 4; ++page) {
        EXPECT_TRUE(allocator.IsAllocated(page));
    }
    for (int64_t page = 4; page < 8; ++page) {
        EXPECT_FALSE(allocator.IsAllocated(page));
    }
}

TEST(BitmapPageAllocatorTest, ReusesFreedRangesAfterWraparound) {
    BitmapPageAllocator allocator;
    allocator.Init(1, 6);

    EXPECT_EQ(allocator.Allocate(4), 0);
    EXPECT_EQ(allocator.Allocate(2), 4);
    EXPECT_EQ(allocator.Allocate(), -1);

    allocator.Free(1, 2);
    EXPECT_EQ(allocator.AllocatedCount(), 4);
    EXPECT_EQ(allocator.Allocate(2), 1);
}

TEST(BitmapPageAllocatorTest, FailsWhenNoContiguousRangeExists) {
    BitmapPageAllocator allocator;
    allocator.Init(1, 5);

    for (int i = 0; i < 5; ++i) {
        EXPECT_EQ(allocator.Allocate(), i);
    }

    allocator.Free(1);
    allocator.Free(3);

    EXPECT_EQ(allocator.AllocatedCount(), 3);
    EXPECT_EQ(allocator.Allocate(2), -1);
}

TEST(BitmapPageAllocatorTest, MarkAllocatedRebuildsState) {
    BitmapPageAllocator allocator;
    allocator.Init(1, 8);

    allocator.MarkAllocated(2, 3);

    EXPECT_EQ(allocator.AllocatedCount(), 3);
    EXPECT_FALSE(allocator.IsAllocated(1));
    EXPECT_TRUE(allocator.IsAllocated(2));
    EXPECT_TRUE(allocator.IsAllocated(3));
    EXPECT_TRUE(allocator.IsAllocated(4));
    EXPECT_FALSE(allocator.IsAllocated(5));

    EXPECT_EQ(allocator.Allocate(2), 0);
    EXPECT_EQ(allocator.Allocate(), 5);
}

TEST(BitmapPageAllocatorTest, DoubleFreeDoesNotUnderflow) {
    BitmapPageAllocator allocator;
    allocator.Init(1, 4);

    EXPECT_EQ(allocator.Allocate(2), 0);
    EXPECT_EQ(allocator.AllocatedCount(), 2);

    allocator.Free(0, 2);
    EXPECT_EQ(allocator.AllocatedCount(), 0);

    allocator.Free(0, 2);
    EXPECT_EQ(allocator.AllocatedCount(), 0);
    EXPECT_FALSE(allocator.IsAllocated(0));
    EXPECT_FALSE(allocator.IsAllocated(1));
}

TEST(BitmapPageAllocatorTest, InvalidRequestsAreSafeNoOps) {
    BitmapPageAllocator allocator;
    allocator.Init(1, 4);

    EXPECT_EQ(allocator.Allocate(0), -1);
    EXPECT_EQ(allocator.Allocate(-1), -1);
    EXPECT_EQ(allocator.Allocate(5), -1);

    allocator.Free(-1);
    allocator.Free(4);
    allocator.Free(3, 2);
    allocator.MarkAllocated(-1);
    allocator.MarkAllocated(4);
    allocator.MarkAllocated(3, 2);

    EXPECT_FALSE(allocator.IsAllocated(-1));
    EXPECT_FALSE(allocator.IsAllocated(4));
    EXPECT_EQ(allocator.AllocatedCount(), 0);
}

TEST(BitmapPageAllocatorTest, ConcurrentAllocationsAreUnique) {
    BitmapPageAllocator allocator;
    allocator.Init(4096, 4096 * 512);

    std::mutex results_mutex;
    std::vector<int64_t> allocations;
    std::vector<std::thread> threads;

    for (int thread_id = 0; thread_id < 8; ++thread_id) {
        threads.emplace_back([&]() {
            std::vector<int64_t> local_allocations;
            local_allocations.reserve(50);
            for (int i = 0; i < 50; ++i) {
                local_allocations.push_back(allocator.Allocate());
            }

            std::lock_guard<std::mutex> lock(results_mutex);
            allocations.insert(allocations.end(), local_allocations.begin(),
                               local_allocations.end());
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    std::unordered_set<int64_t> unique_allocations(allocations.begin(),
                                                   allocations.end());
    EXPECT_EQ(allocations.size(), 400);
    EXPECT_EQ(unique_allocations.size(), allocations.size());
    EXPECT_EQ(unique_allocations.count(-1), 0);
    EXPECT_EQ(allocator.AllocatedCount(), 400);
}

}  // namespace mooncake

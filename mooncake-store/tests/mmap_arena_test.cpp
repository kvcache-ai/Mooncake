// Copyright 2026 KVCache.AI
// Unit tests for MmapArena allocator - Production-grade security and correctness tests

#include <gtest/gtest.h>
#include <glog/logging.h>
#include "mmap_arena.h"
#include <thread>
#include <vector>
#include <atomic>
#include <set>
#include <cstring>

namespace mooncake {

class MmapArenaTest : public ::testing::Test {
protected:
    void SetUp() override {
        FLAGS_logtostderr = 1;
        FLAGS_minloglevel = google::WARNING; // Reduce log noise in tests
    }
};

// ===== BASIC FUNCTIONALITY TESTS =====

TEST_F(MmapArenaTest, BasicInitialization) {
    MmapArena arena;
    ASSERT_TRUE(arena.initialize(1024 * 1024)); // 1MB pool
    ASSERT_TRUE(arena.isInitialized());

    auto stats = arena.getStats();
    EXPECT_EQ(stats.pool_size, 2 * 1024 * 1024); // Aligned to 2MB (huge page)
    EXPECT_EQ(stats.allocated_bytes, 0);
    EXPECT_EQ(stats.num_allocations, 0);
    EXPECT_EQ(stats.num_failed_allocs, 0);
}

TEST_F(MmapArenaTest, BasicAllocation) {
    MmapArena arena;
    ASSERT_TRUE(arena.initialize(1024 * 1024));

    void* ptr = arena.allocate(1024);
    ASSERT_NE(ptr, nullptr);

    auto stats = arena.getStats();
    EXPECT_EQ(stats.num_allocations, 1);
    EXPECT_GE(stats.allocated_bytes, 1024);
    EXPECT_LE(stats.allocated_bytes, 1024 + 64); // Accounting for alignment
}

TEST_F(MmapArenaTest, AllocationAlignment) {
    MmapArena arena;
    ASSERT_TRUE(arena.initialize(1024 * 1024, 64));

    for (size_t size : {1, 63, 64, 65, 100, 1000}) {
        void* ptr = arena.allocate(size);
        ASSERT_NE(ptr, nullptr) << "Failed to allocate size=" << size;

        // Verify 64-byte alignment
        uintptr_t addr = reinterpret_cast<uintptr_t>(ptr);
        EXPECT_EQ(addr % 64, 0) << "Pointer not aligned: " << ptr;

        // Verify memory is writable
        std::memset(ptr, 0xAA, size);
    }
}

TEST_F(MmapArenaTest, ZeroSizeAllocation) {
    MmapArena arena;
    ASSERT_TRUE(arena.initialize(1024 * 1024));

    void* ptr = arena.allocate(0);
    EXPECT_EQ(ptr, nullptr);

    auto stats = arena.getStats();
    EXPECT_EQ(stats.num_allocations, 0);
}

TEST_F(MmapArenaTest, UninitializedAllocation) {
    MmapArena arena;
    EXPECT_FALSE(arena.isInitialized());

    void* ptr = arena.allocate(1024);
    EXPECT_EQ(ptr, nullptr);
}

TEST_F(MmapArenaTest, DoubleInitialization) {
    MmapArena arena;
    ASSERT_TRUE(arena.initialize(1024 * 1024));
    EXPECT_FALSE(arena.initialize(1024 * 1024)); // Second init should fail

    // Arena should still work
    void* ptr = arena.allocate(1024);
    EXPECT_NE(ptr, nullptr);
}

// ===== BUG #1: OOM CHECK RACE CONDITION (CRITICAL) =====

TEST_F(MmapArenaTest, OOMDoesNotCorruptCursor) {
    MmapArena arena;
    ASSERT_TRUE(arena.initialize(1024)); // Requested 1KB, but aligned to 2MB (huge page)

    auto stats_init = arena.getStats();
    size_t pool_size = stats_init.pool_size;  // Actual pool size after alignment

    // Fill the arena completely
    std::vector<void*> ptrs;
    while (true) {
        void* ptr = arena.allocate(64);
        if (ptr == nullptr) break;
        ptrs.push_back(ptr);
    }

    // CRITICAL: Cursor should be at or below pool_size, not corrupted
    auto stats = arena.getStats();
    ASSERT_LE(stats.allocated_bytes, pool_size);
    ASSERT_GT(stats.num_failed_allocs, 0);

    // Subsequent allocations should still fail gracefully, not crash
    void* p_extra = arena.allocate(1);
    EXPECT_EQ(p_extra, nullptr);

    // Verify cursor didn't go past pool_size
    stats = arena.getStats();
    ASSERT_LE(stats.allocated_bytes, pool_size);
}

TEST_F(MmapArenaTest, ConcurrentOOMStressTest) {
    MmapArena arena;
    const size_t requested_pool_size = 1024 * 1024; // 1MB requested
    ASSERT_TRUE(arena.initialize(requested_pool_size));

    auto stats_init = arena.getStats();
    size_t actual_pool_size = stats_init.pool_size;  // Actual pool after alignment

    std::atomic<int> succeeded{0};
    std::atomic<int> failed{0};

    const int num_threads = 16;
    // Allocate enough to guarantee OOM
    const int allocs_per_thread = (actual_pool_size / (64 * num_threads)) + 100;
    const size_t alloc_size = 64;

    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&]() {
            for (int j = 0; j < allocs_per_thread; ++j) {
                void* ptr = arena.allocate(alloc_size);
                if (ptr != nullptr) {
                    succeeded.fetch_add(1, std::memory_order_relaxed);
                    // Write pattern to verify no corruption
                    std::memset(ptr, 0xBB, alloc_size);
                } else {
                    failed.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    for (auto& t : threads) t.join();

    auto stats = arena.getStats();

    // Verify cursor didn't go beyond pool
    ASSERT_LE(stats.allocated_bytes, actual_pool_size);

    // Some allocations should have succeeded
    ASSERT_GT(succeeded.load(), 0);

    // Some should have failed (pool exhausted)
    ASSERT_GT(failed.load(), 0);

    // Total attempts should match
    EXPECT_EQ(succeeded.load() + failed.load(), num_threads * allocs_per_thread);

    LOG(INFO) << "OOM stress test: " << succeeded.load() << " succeeded, "
              << failed.load() << " failed, pool utilization: "
              << (100.0 * stats.allocated_bytes / stats.pool_size) << "%";
}

// ===== BUG #2 & #3: INTEGER OVERFLOW TESTS =====

TEST_F(MmapArenaTest, IntegerOverflowInBoundsCheck) {
    MmapArena arena;
    ASSERT_TRUE(arena.initialize(1024 * 1024));

    // Try to allocate SIZE_MAX (should fail, not wrap around)
    void* ptr = arena.allocate(SIZE_MAX);
    EXPECT_EQ(ptr, nullptr);

    auto stats = arena.getStats();
    EXPECT_EQ(stats.num_failed_allocs, 1);
    EXPECT_EQ(stats.allocated_bytes, 0);
}

TEST_F(MmapArenaTest, AlignmentOverflow) {
    MmapArena arena;
    ASSERT_TRUE(arena.initialize(1024 * 1024));

    // Request size that would overflow during alignment
    // SIZE_MAX - 10 + 64 - 1 = overflow
    void* ptr = arena.allocate(SIZE_MAX - 10);
    EXPECT_EQ(ptr, nullptr);

    auto stats = arena.getStats();
    EXPECT_EQ(stats.num_failed_allocs, 1);
    EXPECT_EQ(stats.allocated_bytes, 0);
}

TEST_F(MmapArenaTest, NearMaxSizeAllocation) {
    MmapArena arena;
    ASSERT_TRUE(arena.initialize(1024 * 1024));

    // Try allocating near SIZE_MAX / 2 (should fail gracefully)
    void* ptr = arena.allocate(SIZE_MAX / 2);
    EXPECT_EQ(ptr, nullptr);

    auto stats = arena.getStats();
    EXPECT_EQ(stats.num_failed_allocs, 1);
}

// ===== BUG #4: ARENA MEMORY CANNOT BE FREED =====

TEST_F(MmapArenaTest, OwnershipDetection) {
    MmapArena arena;
    ASSERT_TRUE(arena.initialize(1024 * 1024));

    void* arena_ptr = arena.allocate(1024);
    ASSERT_NE(arena_ptr, nullptr);

    // Arena should own its allocations
    EXPECT_TRUE(arena.owns(arena_ptr));

    // Null pointer is not owned
    EXPECT_FALSE(arena.owns(nullptr));

    // Pointer outside arena range should not be owned
    char stack_var;
    EXPECT_FALSE(arena.owns(&stack_var));
}

TEST_F(MmapArenaTest, OwnershipBoundaryTest) {
    MmapArena arena;
    ASSERT_TRUE(arena.initialize(1024 * 1024));

    void* first_ptr = arena.allocate(64);
    ASSERT_NE(first_ptr, nullptr);

    void* pool_base = arena.getPoolBase();
    size_t pool_size = arena.getPoolSize();

    // Test boundaries
    EXPECT_TRUE(arena.owns(pool_base)); // Start of pool
    EXPECT_TRUE(arena.owns(static_cast<char*>(pool_base) + 100)); // Middle
    EXPECT_FALSE(arena.owns(static_cast<char*>(pool_base) + pool_size)); // Just past end
    EXPECT_FALSE(arena.owns(static_cast<char*>(pool_base) - 1)); // Just before start
}

// ===== BUG #5: RACE CONDITION IN INITIALIZE =====

TEST_F(MmapArenaTest, ConcurrentInitialization) {
    MmapArena arena;

    std::atomic<int> init_success{0};
    std::atomic<int> init_failure{0};

    const int num_threads = 16;
    std::vector<std::thread> threads;

    // Multiple threads racing to initialize
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&]() {
            if (arena.initialize(1024 * 1024)) {
                init_success.fetch_add(1, std::memory_order_relaxed);
            } else {
                init_failure.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    for (auto& t : threads) t.join();

    // Exactly one initialization should succeed
    EXPECT_EQ(init_success.load(), 1);
    EXPECT_EQ(init_failure.load(), num_threads - 1);

    // Arena should be usable
    EXPECT_TRUE(arena.isInitialized());
    void* ptr = arena.allocate(1024);
    EXPECT_NE(ptr, nullptr);
}

TEST_F(MmapArenaTest, AllocateAfterRacyInit) {
    MmapArena arena;

    const int num_threads = 10;
    std::vector<std::thread> threads;
    std::vector<void*> ptrs(num_threads);

    // Concurrent init + allocate
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i]() {
            arena.initialize(1024 * 1024);
            ptrs[i] = arena.allocate(1024);
        });
    }

    for (auto& t : threads) t.join();

    // All allocations should either succeed or fail consistently
    int non_null = std::count_if(ptrs.begin(), ptrs.end(),
                                  [](void* p) { return p != nullptr; });
    EXPECT_GT(non_null, 0); // At least some should succeed

    // Verify no duplicate pointers
    std::set<void*> unique_ptrs(ptrs.begin(), ptrs.end());
    unique_ptrs.erase(nullptr);
    EXPECT_EQ(unique_ptrs.size(), static_cast<size_t>(non_null));
}

// ===== THREAD SAFETY & CONCURRENCY TESTS =====

TEST_F(MmapArenaTest, ConcurrentAllocations) {
    MmapArena arena;
    ASSERT_TRUE(arena.initialize(64 * 1024 * 1024)); // 64MB

    const int num_threads = 8;
    const int allocs_per_thread = 1000;
    std::vector<std::thread> threads;
    std::vector<std::vector<void*>> all_pointers(num_threads);

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i]() {
            for (int j = 0; j < allocs_per_thread; ++j) {
                void* ptr = arena.allocate(1024);
                if (ptr != nullptr) {
                    all_pointers[i].push_back(ptr);
                    // Write unique pattern
                    std::memset(ptr, 0xCC + i, 1024);
                }
            }
        });
    }

    for (auto& t : threads) t.join();

    // Verify no duplicate pointers (uniqueness)
    std::set<void*> unique_ptrs;
    for (const auto& vec : all_pointers) {
        for (void* ptr : vec) {
            auto [iter, inserted] = unique_ptrs.insert(ptr);
            EXPECT_TRUE(inserted) << "Duplicate pointer detected: " << ptr;
        }
    }

    LOG(INFO) << "Concurrent allocations: " << unique_ptrs.size()
              << " unique allocations from " << num_threads << " threads";
}

TEST_F(MmapArenaTest, StatsConsistencyUnderLoad) {
    MmapArena arena;
    ASSERT_TRUE(arena.initialize(64 * 1024 * 1024));

    std::atomic<bool> stop{false};
    std::atomic<size_t> total_allocated{0};
    std::atomic<int> invariant_violations{0};

    // Allocator threads
    std::vector<std::thread> threads;
    for (int i = 0; i < 8; ++i) {
        threads.emplace_back([&]() {
            while (!stop.load(std::memory_order_relaxed)) {
                void* ptr = arena.allocate(128);
                if (ptr != nullptr) {
                    total_allocated.fetch_add(128, std::memory_order_relaxed);
                }
            }
        });
    }

    // Stats checker thread
    threads.emplace_back([&]() {
        for (int i = 0; i < 100; ++i) {
            auto stats = arena.getStats();
            // Invariants that must always hold
            if (stats.allocated_bytes > stats.pool_size) {
                invariant_violations.fetch_add(1, std::memory_order_relaxed);
            }
            // Note: peak_allocated may temporarily lag behind allocated_bytes
            // due to concurrent updates, so we don't check that invariant here
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        stop.store(true, std::memory_order_relaxed);
    });

    for (auto& t : threads) t.join();

    auto stats = arena.getStats();

    // Critical invariant: cursor never exceeded pool size
    EXPECT_EQ(invariant_violations.load(), 0);
    EXPECT_LE(stats.allocated_bytes, stats.pool_size);

    // After all threads finish, peak should be >= final allocated
    EXPECT_GE(stats.peak_allocated, stats.allocated_bytes);

    LOG(INFO) << "Stats consistency test: " << stats.num_allocations
              << " allocations, " << stats.num_failed_allocs << " failures";
}

// ===== EDGE CASES & STRESS TESTS =====

TEST_F(MmapArenaTest, NearOOMAllocation) {
    MmapArena arena;
    const size_t pool_size = 4096; // Small pool
    ASSERT_TRUE(arena.initialize(pool_size));

    // Fill arena almost completely
    std::vector<void*> ptrs;
    while (true) {
        void* ptr = arena.allocate(64);
        if (ptr == nullptr) break;
        ptrs.push_back(ptr);
    }

    auto stats = arena.getStats();
    EXPECT_LE(stats.allocated_bytes, stats.pool_size);
    EXPECT_GT(stats.num_failed_allocs, 0);

    LOG(INFO) << "Near-OOM test: " << ptrs.size() << " allocations, "
              << stats.allocated_bytes << " / " << stats.pool_size << " bytes used";
}

TEST_F(MmapArenaTest, MixedSizeAllocations) {
    MmapArena arena;
    ASSERT_TRUE(arena.initialize(64 * 1024 * 1024));

    std::vector<size_t> sizes = {1, 16, 64, 256, 1024, 4096, 16384, 65536};
    std::vector<void*> ptrs;

    // Allocate various sizes
    for (size_t size : sizes) {
        for (int i = 0; i < 10; ++i) {
            void* ptr = arena.allocate(size);
            if (ptr != nullptr) {
                ptrs.push_back(ptr);
                std::memset(ptr, 0xDD, size);
            }
        }
    }

    // Verify all pointers are valid and aligned
    for (void* ptr : ptrs) {
        EXPECT_NE(ptr, nullptr);
        EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr) % 64, 0);
        EXPECT_TRUE(arena.owns(ptr));
    }

    LOG(INFO) << "Mixed-size test: " << ptrs.size() << " allocations";
}

TEST_F(MmapArenaTest, PeakAllocationTracking) {
    MmapArena arena;
    ASSERT_TRUE(arena.initialize(1024 * 1024));

    void* p1 = arena.allocate(512);
    auto stats1 = arena.getStats();
    EXPECT_GE(stats1.peak_allocated, 512);

    void* p2 = arena.allocate(1024);
    auto stats2 = arena.getStats();
    EXPECT_GE(stats2.peak_allocated, stats1.peak_allocated);
    EXPECT_GE(stats2.peak_allocated, 512 + 1024);

    LOG(INFO) << "Peak tracking: " << stats2.peak_allocated << " bytes";
}

// ===== MIXED ALIGNMENT TESTS =====

TEST_F(MmapArenaTest, MixedAlignmentSequence) {
    MmapArena arena;
    ASSERT_TRUE(arena.initialize(64 * 1024 * 1024)); // 64MB

    // First allocation moves cursor to 64 (non-trivially aligned).
    // This ensures the subsequent 2MB-aligned allocation actually
    // exercises the offset-alignment logic rather than passing
    // vacuously because cursor 0 is trivially aligned to everything.
    void* p1 = arena.allocate(1, 64);
    ASSERT_NE(p1, nullptr);

    const size_t TWO_MB = 2 * 1024 * 1024;
    void* p2 = arena.allocate(4 * 1024 * 1024, TWO_MB);
    ASSERT_NE(p2, nullptr);

    // The returned pointer MUST be 2MB-aligned
    EXPECT_EQ(reinterpret_cast<uintptr_t>(p2) % TWO_MB, 0)
        << "Pointer not 2MB-aligned: " << p2;

    // p2 must not overlap p1
    EXPECT_GT(reinterpret_cast<uintptr_t>(p2),
              reinterpret_cast<uintptr_t>(p1));
}

} // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;
    return RUN_ALL_TESTS();
}

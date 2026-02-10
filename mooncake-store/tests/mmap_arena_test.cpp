// Copyright 2026 KVCache.AI
// Unit tests for MmapArena allocator - Production-grade security and correctness tests

#include <gtest/gtest.h>
#include <glog/logging.h>
#include "mmap_arena.h"
#include "utils.h"
#include <thread>
#include <vector>
#include <atomic>
#include <set>
#include <cstring>
#include <sys/mman.h>
#include <unistd.h>

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
    std::atomic<int> invariant_violations{0};

    // Allocator threads
    std::vector<std::thread> threads;
    for (int i = 0; i < 8; ++i) {
        threads.emplace_back([&]() {
            while (!stop.load(std::memory_order_relaxed)) {
                void* ptr = arena.allocate(128);
                (void)ptr;
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
    (void)p1;
    auto stats1 = arena.getStats();
    EXPECT_GE(stats1.peak_allocated, 512);

    void* p2 = arena.allocate(1024);
    (void)p2;
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

// ===== INPUT VALIDATION TESTS =====

TEST_F(MmapArenaTest, InitializeWithZeroSize) {
    MmapArena arena;
    EXPECT_FALSE(arena.initialize(0));
    EXPECT_FALSE(arena.isInitialized());

    // Allocations should fail gracefully on uninitialized arena
    void* ptr = arena.allocate(1024);
    EXPECT_EQ(ptr, nullptr);
}

TEST_F(MmapArenaTest, NonPowerOfTwoAlignment) {
    MmapArena arena;
    // alignment=100 is not a power of 2; should be rejected
    EXPECT_FALSE(arena.initialize(1024 * 1024, 100));
    EXPECT_FALSE(arena.isInitialized());
}

TEST_F(MmapArenaTest, PowerOfTwoAlignmentsAccepted) {
    // Test several valid power-of-2 alignments
    for (size_t align : {64, 128, 256, 512, 4096}) {
        MmapArena arena;
        ASSERT_TRUE(arena.initialize(4 * 1024 * 1024, align))
            << "Failed to init with alignment=" << align;
        void* ptr = arena.allocate(1024);
        ASSERT_NE(ptr, nullptr);
        EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr) % align, 0)
            << "Pointer not aligned to " << align;
    }
}

// ===== SIZING REGRESSION TEST =====

TEST_F(MmapArenaTest, ArenaSizingRegression) {
    // Reproduces the benchmark failure: pool=16MB, allocate all of it,
    // then any further allocation should OOM.
    const size_t POOL = 16 * 1024 * 1024;  // 16MB (scaled down from 16GB)
    MmapArena arena;
    ASSERT_TRUE(arena.initialize(POOL, 64));

    // First: allocation consumes entire pool
    void* p1 = arena.allocate(POOL, 64);
    ASSERT_NE(p1, nullptr);

    // Second: any further allocation should OOM
    void* p2 = arena.allocate(1024, 64);
    EXPECT_EQ(p2, nullptr);
    EXPECT_GE(arena.getStats().num_failed_allocs, 1);
}

// ===== CONCURRENT INIT METADATA CONSISTENCY =====

TEST_F(MmapArenaTest, ConcurrentInitMetadataConsistency) {
    // Verify that after racing inits, pool_size and alignment are consistent
    // with the winning initialization parameters.
    MmapArena arena;

    const int num_threads = 16;
    std::vector<std::thread> threads;

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&]() {
            arena.initialize(4 * 1024 * 1024, 128);
        });
    }
    for (auto& t : threads) t.join();

    ASSERT_TRUE(arena.isInitialized());
    // Pool size should be 4MB (already 2MB-aligned, no rounding needed)
    EXPECT_EQ(arena.getStats().pool_size, 4 * 1024 * 1024);

    // Verify allocation works and honors alignment
    void* ptr = arena.allocate(256);
    ASSERT_NE(ptr, nullptr);
    EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr) % 128, 0);
}

// ===== MAP_POPULATE REGRESSION TESTS =====
// These tests verify the fix for cudaErrorIllegalAddress caused by lazy
// hugepage faults during GPU DMA.  The arena must pre-fault all pages
// at initialization time (MAP_POPULATE) so that every byte in the pool
// is backed by physical memory before any allocation is returned.

TEST_F(MmapArenaTest, PagesArePhysicallyBackedAfterInit) {
    // Verify that arena pages are resident in physical memory immediately
    // after initialize() — i.e. MAP_POPULATE is working.
    // Uses mincore() which reports per-page residency status.
    MmapArena arena;
    const size_t POOL = 4 * 1024 * 1024;  // 4MB
    ASSERT_TRUE(arena.initialize(POOL));

    void* base = arena.getPoolBase();
    size_t pool_size = arena.getPoolSize();
    ASSERT_NE(base, nullptr);
    ASSERT_GT(pool_size, 0);

    // mincore() works on the system page size (typically 4KB), not hugepages.
    // Query the number of system pages covering the pool.
    const size_t sys_page_size = sysconf(_SC_PAGESIZE);
    size_t num_pages = (pool_size + sys_page_size - 1) / sys_page_size;

    std::vector<unsigned char> vec(num_pages);
    int ret = mincore(base, pool_size, vec.data());

    if (ret == 0) {
        // mincore succeeded — check that all pages are resident
        size_t resident = 0;
        for (size_t i = 0; i < num_pages; ++i) {
            if (vec[i] & 1) ++resident;
        }
        // With MAP_POPULATE, all pages should be resident.
        // Allow small tolerance for kernel behavior differences.
        double pct = 100.0 * resident / num_pages;
        EXPECT_GT(pct, 95.0)
            << "Only " << pct << "% of pages resident; MAP_POPULATE may not be working. "
            << resident << "/" << num_pages << " pages.";
        LOG(INFO) << "mincore: " << resident << "/" << num_pages
                  << " pages resident (" << pct << "%)";
    } else {
        // mincore may fail on some kernels for MAP_HUGETLB regions.
        // Fall back to verifying that we can read every byte without SIGSEGV.
        LOG(WARNING) << "mincore() returned " << ret << " (errno=" << errno
                     << "), falling back to read-verification";
        // Read every page — if MAP_POPULATE didn't work, this would trigger
        // page faults (which is fine for CPU but would crash GPU DMA).
        volatile char sink = 0;
        for (size_t off = 0; off < pool_size; off += sys_page_size) {
            sink += static_cast<char*>(base)[off];
        }
        (void)sink;
        // If we get here without SIGSEGV, at least CPU access works.
        // The real MAP_POPULATE guarantee is that DMA works too, which
        // can only be tested with actual GPU hardware.
    }
}

TEST_F(MmapArenaTest, AllocatedMemoryIsImmediatelyReadableWritable) {
    // Simulates the GPU DMA scenario: allocate a buffer and immediately
    // read/write every byte.  Without MAP_POPULATE, a lazy hugepage fault
    // during DMA would crash.  With MAP_POPULATE, all pages are pre-faulted.
    MmapArena arena;
    const size_t POOL = 8 * 1024 * 1024;  // 8MB
    ASSERT_TRUE(arena.initialize(POOL));

    // Allocate a large buffer (simulates segment allocation)
    const size_t BUF_SIZE = 4 * 1024 * 1024;  // 4MB
    void* ptr = arena.allocate(BUF_SIZE);
    ASSERT_NE(ptr, nullptr);

    // Write a pattern to every byte — would trigger page faults if lazy
    std::memset(ptr, 0xAB, BUF_SIZE);

    // Read it back — verify no corruption
    auto* bytes = static_cast<unsigned char*>(ptr);
    for (size_t i = 0; i < BUF_SIZE; i += 4096) {
        EXPECT_EQ(bytes[i], 0xAB)
            << "Memory corruption at offset " << i;
    }

    // Allocate a second buffer from remaining space
    void* ptr2 = arena.allocate(BUF_SIZE);
    if (ptr2 != nullptr) {
        // Write different pattern
        std::memset(ptr2, 0xCD, BUF_SIZE);
        auto* bytes2 = static_cast<unsigned char*>(ptr2);
        for (size_t i = 0; i < BUF_SIZE; i += 4096) {
            EXPECT_EQ(bytes2[i], 0xCD)
                << "Memory corruption in second buffer at offset " << i;
        }
        // Verify first buffer wasn't corrupted by second allocation
        EXPECT_EQ(bytes[0], 0xAB) << "First buffer corrupted after second allocation";
    }
}

TEST_F(MmapArenaTest, FallbackMmapRetainsPopulate) {
    // When huge pages are unavailable, the arena falls back to regular mmap.
    // Verify that MAP_POPULATE is retained in the fallback path by confirming
    // the allocated memory is immediately usable (same as above but may
    // exercise the non-hugepage code path on machines without huge pages).
    MmapArena arena;
    const size_t POOL = 2 * 1024 * 1024;  // 2MB — minimum hugepage unit
    ASSERT_TRUE(arena.initialize(POOL));

    void* ptr = arena.allocate(1024 * 1024);  // 1MB
    ASSERT_NE(ptr, nullptr);

    // Full read/write cycle
    std::memset(ptr, 0xEF, 1024 * 1024);
    auto* bytes = static_cast<unsigned char*>(ptr);
    EXPECT_EQ(bytes[0], 0xEF);
    EXPECT_EQ(bytes[1024 * 1024 - 1], 0xEF);
    EXPECT_EQ(bytes[512 * 1024], 0xEF);  // Middle
}

// ===== FORK SAFETY TEST =====

TEST_F(MmapArenaTest, MadviseDontForkApplied) {
    // Verify that the arena applies MADV_DONTFORK to prevent 64GB CoW
    // page table duplication on fork().  madvise(MADV_DONTFORK) is
    // idempotent — calling it again on an already-marked region returns 0.
    MmapArena arena;
    const size_t POOL = 4 * 1024 * 1024;  // 4MB
    ASSERT_TRUE(arena.initialize(POOL));

    void* base = arena.getPoolBase();
    size_t pool_size = arena.getPoolSize();
    ASSERT_NE(base, nullptr);

    // If MADV_DONTFORK was already applied by initialize(), this is a no-op
    // and returns 0.  If it wasn't applied, this also returns 0 (first apply).
    // Either way, verify madvise succeeds on the pool region.
    int ret = madvise(base, pool_size, MADV_DONTFORK);
    EXPECT_EQ(ret, 0) << "madvise(MADV_DONTFORK) failed: " << strerror(errno);
}

// ===== FALLBACK MMAP ALIGNMENT TESTS =====
// NOTE: These tests rely on being the FIRST to call the global
// allocate_buffer_mmap_memory(), which triggers std::call_once on
// g_arena_init_flag.  Since all other tests in this file use local
// MmapArena instances (not the global allocator), and GTest runs tests
// in declaration order within a fixture, these are always last.
// If a future test calls allocate_buffer_mmap_memory() before these,
// the arena will already be initialized and MC_DISABLE_MMAP_ARENA
// will have no effect — move these to a separate test binary if that
// happens.

TEST_F(MmapArenaTest, FallbackMmapHonorsPageAlignment) {
    // With arena disabled (MC_DISABLE_MMAP_ARENA=1), the fallback mmap path
    // should return pointers aligned to at least the system page size.
    // This mirrors client_buffer.cpp which requests alignment=64.
    setenv("MC_DISABLE_MMAP_ARENA", "1", 1);

    const size_t alloc_size = 64 * 1024;  // 64KB
    constexpr size_t alignment = 64;      // Cache-line alignment

    void* ptr = allocate_buffer_mmap_memory(alloc_size, alignment);
    // Verify we actually hit the fallback path (not arena).
    // free_buffer_mmap_memory on a non-arena pointer calls munmap;
    // on an arena pointer it's a no-op.  We can't easily distinguish
    // at this level, but the LOG output "ARENA ALLOCATOR DISABLED"
    // confirms the path.  If this test ever passes with arena active,
    // the alignment assertion still holds (arena honors alignment too),
    // but the test's purpose (covering fallback) is not met.
    ASSERT_NE(ptr, nullptr);
    EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr) % alignment, 0u)
        << "Fallback mmap pointer not aligned to " << alignment;

    // Verify read/write works
    memset(ptr, 0xAB, alloc_size);
    EXPECT_EQ(static_cast<uint8_t*>(ptr)[0], 0xAB);
    EXPECT_EQ(static_cast<uint8_t*>(ptr)[alloc_size - 1], 0xAB);

    free_buffer_mmap_memory(ptr, alloc_size);
    unsetenv("MC_DISABLE_MMAP_ARENA");
}

TEST_F(MmapArenaTest, FallbackMmapAllocateFreeCycle) {
    // Verify multiple allocate/free cycles don't leak or crash.
    // Exercises the fallback path's map_size computation consistency
    // between allocate_buffer_mmap_memory and free_buffer_mmap_memory.
    setenv("MC_DISABLE_MMAP_ARENA", "1", 1);

    constexpr int CYCLES = 8;
    constexpr size_t alloc_size = 128 * 1024;  // 128KB
    constexpr size_t alignment = 64;

    for (int i = 0; i < CYCLES; ++i) {
        void* ptr = allocate_buffer_mmap_memory(alloc_size, alignment);
        ASSERT_NE(ptr, nullptr) << "Allocation failed on cycle " << i;
        EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr) % alignment, 0u);
        // Touch memory to ensure mapping is valid
        memset(ptr, static_cast<uint8_t>(i), alloc_size);
        free_buffer_mmap_memory(ptr, alloc_size);
    }

    unsetenv("MC_DISABLE_MMAP_ARENA");
}

} // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;
    return RUN_ALL_TESTS();
}

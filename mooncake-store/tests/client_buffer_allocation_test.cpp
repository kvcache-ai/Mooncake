// Copyright 2026 Mooncake contributors
// SPDX-License-Identifier: Apache-2.0

#include "common/client_buffer_allocation.h"
#include "nof/dma_buffer_allocator.h"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <mutex>
#include <utility>
#include <vector>

#include <gtest/gtest.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>

namespace {
std::atomic_bool trace_discard{false};
std::atomic_bool fail_discard{false};
std::mutex ranges_mutex;
std::vector<std::pair<uintptr_t, size_t>> ranges;
}  // namespace

// Interpose only this test binary's madvise calls. Real anonymous mappings
// exercise the allocator; the recorder checks that no metadata/edge page is
// discarded and can inject a failed advice without changing production code.
extern "C" int madvise(void *address, size_t size, int advice) noexcept {
    if (trace_discard && advice == MADV_DONTNEED) {
        std::lock_guard<std::mutex> lock(ranges_mutex);
        ranges.emplace_back(reinterpret_cast<uintptr_t>(address), size);
        if (fail_discard) {
            errno = EINVAL;
            return -1;
        }
    }
    return static_cast<int>(syscall(SYS_madvise, address, size, advice));
}

namespace mooncake {

class SizedHostFreeTest : public ::testing::Test {
   protected:
    void SetUp() override {
#ifdef USE_VRAM_SEGMENT
        GTEST_SKIP() << "Tests the host allocator";
#endif
        ranges.clear();
        fail_discard = false;
        trace_discard = true;
    }
    void TearDown() override {
        trace_discard = false;
        fail_discard = false;
        unsetenv("MC_STORE_FREE_THREADS");
    }
    // Keep partial edge pages while satisfying aligned_alloc(64, size).
    static constexpr size_t size = (16ull << 30) + 64;
};

TEST_F(SizedHostFreeTest, DiscardsOnlyFullOwnedPagesAndJoinsBeforeFree) {
    ASSERT_EQ(setenv("MC_STORE_FREE_THREADS", "4", 1), 0);
    void *ptr = allocate_buffer_allocator_memory(size, "tcp", 64);
    ASSERT_NE(ptr, nullptr);
    const size_t page = sysconf(_SC_PAGESIZE);
    uintptr_t begin = reinterpret_cast<uintptr_t>(ptr);
    uintptr_t end = begin + size;
    begin += (page - begin % page) % page;
    end -= end % page;
    free_memory("tcp", ptr, nullptr, size);
    ASSERT_EQ(ranges.size(), 4u);
    std::sort(ranges.begin(), ranges.end());
    uintptr_t next = begin;
    for (auto [address, length] : ranges) {
        EXPECT_EQ(address, next);
        EXPECT_EQ(address % page, 0u);
        EXPECT_EQ(length % page, 0u);
        EXPECT_GT(length, 0u);
        next += length;
    }
    EXPECT_EQ(next, end);
    unsigned char residency = 0;
    EXPECT_EQ(mincore(reinterpret_cast<void *>(begin), page, &residency), -1);
    EXPECT_EQ(errno, ENOMEM);  // The virtual mapping was also released.
}

TEST_F(SizedHostFreeTest, DisabledInvalidAndUnsizedFreeKeepOriginalPath) {
    for (const char *setting : {"0", "1", "65", "4junk"}) {
        SCOPED_TRACE(setting);
        ASSERT_EQ(setenv("MC_STORE_FREE_THREADS", setting, 1), 0);
        void *ptr = allocate_buffer_allocator_memory(size, "tcp", 64);
        ASSERT_NE(ptr, nullptr);
        free_memory("tcp", ptr, nullptr, size);
        EXPECT_TRUE(ranges.empty());
    }
    ASSERT_EQ(setenv("MC_STORE_FREE_THREADS", "4", 1), 0);
    void *ptr = allocate_buffer_allocator_memory(size, "tcp", 64);
    ASSERT_NE(ptr, nullptr);
    free_memory("tcp", ptr);
    EXPECT_TRUE(ranges.empty());
    ptr = allocate_buffer_allocator_memory(4096, "tcp", 64);
    ASSERT_NE(ptr, nullptr);
    free_memory("tcp", ptr, nullptr, 4096);
    free_memory("tcp", nullptr, nullptr, size);
    EXPECT_TRUE(ranges.empty());
}

TEST_F(SizedHostFreeTest, AdviceFailureStillReleasesAllocation) {
    ASSERT_EQ(setenv("MC_STORE_FREE_THREADS", "4", 1), 0);
    fail_discard = true;
    const size_t page = sysconf(_SC_PAGESIZE);
    const size_t allocation_size = size / page * page;
    void *ptr = allocate_buffer_allocator_memory(allocation_size, "tcp", page);
    ASSERT_NE(ptr, nullptr);
    const uintptr_t address = reinterpret_cast<uintptr_t>(ptr);
    free_memory("tcp", ptr, nullptr, allocation_size);
    ASSERT_EQ(ranges.size(), 4u);
    unsigned char residency = 0;
    EXPECT_EQ(mincore(reinterpret_cast<void *>(address), page, &residency), -1);
    EXPECT_EQ(errno, ENOMEM);
}

TEST_F(SizedHostFreeTest, SizedReleasePreservesDmaAllocatorOwnership) {
    class RecordingAllocator : public SystemDmaAllocator {
       public:
        void Free(void *ptr) override {
            freed = ptr;
            SystemDmaAllocator::Free(ptr);
        }
        void *freed = nullptr;
    } allocator;
    ASSERT_EQ(setenv("MC_STORE_FREE_THREADS", "4", 1), 0);
    void *ptr = allocate_buffer_allocator_memory(size, "tcp", 64, &allocator);
    ASSERT_NE(ptr, nullptr);
    free_memory("tcp", ptr, &allocator, size);
    EXPECT_EQ(allocator.freed, ptr);
    EXPECT_TRUE(ranges.empty());
}

}  // namespace mooncake

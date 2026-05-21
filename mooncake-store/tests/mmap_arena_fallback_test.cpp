// Copyright 2026 KVCache.AI
// Fallback-path tests for the global mmap allocator wrapper.

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <fstream>
#include <string>

#include "utils.h"

namespace mooncake {

namespace {

bool HostHasReservedHugepages() {
    std::ifstream meminfo("/proc/meminfo");
    std::string key;
    size_t value = 0;
    std::string unit;
    while (meminfo >> key >> value >> unit) {
        if (key == "HugePages_Total:") {
            return value > 0;
        }
    }
    return false;
}

}  // namespace

class MmapArenaFallbackTest : public ::testing::Test {
   protected:
    void SetUp() override {
        FLAGS_logtostderr = 1;
        FLAGS_minloglevel = google::WARNING;
        setenv("MC_DISABLE_MMAP_ARENA", "1", 1);
    }

    void TearDown() override {
        unsetenv("MC_DISABLE_MMAP_ARENA");
        unsetenv("MC_MMAP_ARENA_POOL_SIZE");
        unsetenv("MC_STORE_USE_HUGEPAGE");
    }
};

TEST_F(MmapArenaFallbackTest, ArenaInitFailureIsStickyForProcessLifetime) {
    unsetenv("MC_DISABLE_MMAP_ARENA");
    unsetenv("MC_STORE_USE_HUGEPAGE");

    // "infinite" parses to UINT64_MAX and deterministically trips the arena's
    // overflow guard before any mmap attempt.
    setenv("MC_MMAP_ARENA_POOL_SIZE", "infinite", 1);
    FLAGS_minloglevel = google::INFO;
    testing::internal::CaptureStderr();
    void* first_ptr = allocate_buffer_mmap_memory(64 * 1024, 64);
    ASSERT_NE(first_ptr, nullptr);
    free_buffer_mmap_memory(first_ptr, 64 * 1024);
    const std::string first_logs = testing::internal::GetCapturedStderr();
    EXPECT_NE(first_logs.find("ARENA INITIALIZATION FAILED"),
              std::string::npos);
    EXPECT_NE(first_logs.find("only attempted once per process"),
              std::string::npos);

    // Fix the env and try again in the same process. std::call_once should
    // keep us on the fallback path without re-running initialization.
    setenv("MC_MMAP_ARENA_POOL_SIZE", "2gb", 1);
    testing::internal::CaptureStderr();
    void* second_ptr = allocate_buffer_mmap_memory(64 * 1024, 64);
    ASSERT_NE(second_ptr, nullptr);
    free_buffer_mmap_memory(second_ptr, 64 * 1024);
    const std::string second_logs = testing::internal::GetCapturedStderr();
    EXPECT_EQ(second_logs.find("ARENA INITIALIZATION FAILED"),
              std::string::npos);
    EXPECT_EQ(second_logs.find("ARENA ALLOCATOR ENABLED"), std::string::npos);
}

TEST_F(MmapArenaFallbackTest,
       ExplicitHugepageRequestDoesNotSilentlyFallbackToRegularPages) {
    if (HostHasReservedHugepages()) {
        GTEST_SKIP() << "Host has reserved hugepages; this strict-fallback "
                        "test only proves itself on hugepage-free hosts";
    }

    unsetenv("MC_DISABLE_MMAP_ARENA");
    setenv("MC_STORE_USE_HUGEPAGE", "1", 1);
    setenv("MC_MMAP_ARENA_POOL_SIZE", "2mb", 1);

    FLAGS_minloglevel = google::INFO;
    testing::internal::CaptureStderr();
    void* ptr = allocate_buffer_mmap_memory(64 * 1024, 64);
    const std::string logs = testing::internal::GetCapturedStderr();

    EXPECT_EQ(ptr, nullptr) << logs;
    EXPECT_EQ(logs.find("retrying without huge pages"), std::string::npos)
        << logs;
}

TEST_F(MmapArenaFallbackTest, HonorsPageAlignment) {
    const size_t alloc_size = 64 * 1024;
    constexpr size_t alignment = 64;

    void* ptr = allocate_buffer_mmap_memory(alloc_size, alignment);
    ASSERT_NE(ptr, nullptr);
    EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr) % alignment, 0u);

    memset(ptr, 0xAB, alloc_size);
    EXPECT_EQ(static_cast<uint8_t*>(ptr)[0], 0xAB);
    EXPECT_EQ(static_cast<uint8_t*>(ptr)[alloc_size - 1], 0xAB);

    free_buffer_mmap_memory(ptr, alloc_size);
}

TEST_F(MmapArenaFallbackTest, NoHugepagesAllocFree) {
    unsetenv("MC_STORE_USE_HUGEPAGE");

    const size_t alloc_size = 65000;
    constexpr size_t alignment = 64;

    void* ptr = allocate_buffer_mmap_memory(alloc_size, alignment);
    ASSERT_NE(ptr, nullptr);
    EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr) % 4096, 0u);

    memset(ptr, 0xCD, alloc_size);
    EXPECT_EQ(static_cast<uint8_t*>(ptr)[0], 0xCD);

    free_buffer_mmap_memory(ptr, alloc_size);
}

TEST_F(MmapArenaFallbackTest, AllocateFreeCycle) {
    constexpr int kCycles = 8;
    constexpr size_t alloc_size = 128 * 1024;
    constexpr size_t alignment = 64;

    for (int i = 0; i < kCycles; ++i) {
        void* ptr = allocate_buffer_mmap_memory(alloc_size, alignment);
        ASSERT_NE(ptr, nullptr) << "Allocation failed on cycle " << i;
        EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr) % alignment, 0u);
        memset(ptr, static_cast<uint8_t>(i), alloc_size);
        free_buffer_mmap_memory(ptr, alloc_size);
    }
}

}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;
    return RUN_ALL_TESTS();
}

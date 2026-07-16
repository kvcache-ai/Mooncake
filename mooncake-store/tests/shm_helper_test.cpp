// Unit tests for ShmHelper, focused on get_shms_snapshot() (Stage 3d): a locked
// snapshot copy that can be iterated safely while other threads allocate()/
// free() concurrently, replacing the unlocked get_shms() reference iteration
// that was UB. Uses memfd_create + mmap, so it runs on Linux/CI.

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <thread>
#include <vector>

#include "dummy_client.h"

namespace mooncake {

// allocate -> get_shm hits within the segment; get_shms_snapshot contains it;
// after free, get_shm no longer finds it.
TEST(ShmHelperTest, AllocateFindFree) {
    auto* h = ShmHelper::getInstance();
    ASSERT_NE(h, nullptr);

    const size_t kSize = 1 << 20;  // 1 MiB
    void* p = h->allocate(kSize);
    ASSERT_NE(p, nullptr);

    // An address inside the segment resolves to it.
    auto seg = h->get_shm(static_cast<uint8_t*>(p) + 16);
    ASSERT_NE(seg, nullptr);
    EXPECT_EQ(seg->base_addr, p);
    EXPECT_GE(seg->size, kSize);

    // The snapshot contains the freshly allocated segment.
    bool found = false;
    for (const auto& s : h->get_shms_snapshot()) {
        if (s->base_addr == p) {
            found = true;
            break;
        }
    }
    EXPECT_TRUE(found);

    EXPECT_EQ(h->free(p), 0);
    EXPECT_EQ(h->get_shm(p), nullptr);
    EXPECT_EQ(h->free(p), -1);  // freeing an unknown address fails
}

// 3d regression (run under TSan): iterating a snapshot while other threads
// allocate/free must not race or invalidate iterators. allocate/free and the
// snapshot copy all serialize on shm_mutex_, and copied shared_ptrs keep each
// segment alive during iteration.
TEST(ShmHelperTest, SnapshotStableUnderConcurrentAllocFree) {
    auto* h = ShmHelper::getInstance();
    ASSERT_NE(h, nullptr);

    std::atomic<bool> stop{false};
    std::thread reader([&] {
        while (!stop.load()) {
            for (const auto& s : h->get_shms_snapshot()) {
                volatile size_t sz = s->size;  // touch through the snapshot
                (void)sz;
            }
        }
    });

    std::vector<std::thread> writers;
    for (int t = 0; t < 4; ++t) {
        writers.emplace_back([&] {
            for (int i = 0; i < 50; ++i) {
                // allocate() throws (never returns nullptr) on failure; an
                // uncaught throw across the thread boundary would terminate the
                // process, so turn it into a gtest failure instead.
                void* p = nullptr;
                try {
                    p = h->allocate(4096 * 4);
                } catch (const std::exception& e) {
                    ADD_FAILURE() << "allocate threw: " << e.what();
                    return;
                }
                std::this_thread::yield();
                EXPECT_EQ(h->free(p), 0);
            }
        });
    }

    for (auto& w : writers) w.join();
    stop.store(true);
    reader.join();
    SUCCEED();
}

}  // namespace mooncake

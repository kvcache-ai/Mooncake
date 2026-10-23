// Single-threaded ShmHelper allocation and lookup tests.
// Uses memfd_create + mmap, so it runs on Linux/CI.

#include <gtest/gtest.h>

#include <cstdint>

#include "dummy_client.h"

namespace mooncake {

// allocate -> get_shm hits within the segment; get_shms contains it;
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

    // The segment list contains the freshly allocated segment.
    bool found = false;
    for (const auto& s : h->get_shms()) {
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

}  // namespace mooncake

#include <gtest/gtest.h>

#include <cstdlib>

#include "shm_helper.h"

namespace mooncake {

TEST(ShmHelperTest, AllocatesAndFindsSegmentWithDefaultPath) {
    unsetenv("MC_STORE_SHM_POPULATE_THREADS");
    unsetenv("MC_STORE_USE_HUGEPAGE");
    auto *helper = ShmHelper::getInstance();
    void *buffer = helper->allocate(2 * 1024 * 1024);
    ASSERT_NE(buffer, nullptr);
    auto segment = helper->get_shm(buffer);
    ASSERT_NE(segment, nullptr);
    EXPECT_EQ(segment->base_addr, buffer);
    EXPECT_EQ(segment->size, 2 * 1024 * 1024);
    EXPECT_EQ(helper->free(buffer), 0);
}

TEST(ShmHelperTest, AllocatesWithParallelPopulationPath) {
    setenv("MC_STORE_SHM_POPULATE_THREADS", "2", 1);
    auto *helper = ShmHelper::getInstance();
    void *buffer = helper->allocate(4 * 1024 * 1024);
    ASSERT_NE(buffer, nullptr);
    EXPECT_NE(helper->get_shm(buffer), nullptr);
    EXPECT_EQ(helper->free(buffer), 0);
    unsetenv("MC_STORE_SHM_POPULATE_THREADS");
}

}  // namespace mooncake

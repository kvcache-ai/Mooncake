#include <gtest/gtest.h>

#include <algorithm>
#include <cstdlib>
#include <sys/mman.h>
#include <unistd.h>
#include <vector>

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
    constexpr size_t size = 4 * 1024 * 1024;
    void *buffer = helper->allocate(size);
    unsetenv("MC_STORE_SHM_POPULATE_THREADS");
    ASSERT_NE(buffer, nullptr);
    EXPECT_NE(helper->get_shm(buffer), nullptr);
    std::vector<unsigned char> resident(size / getpagesize());
    EXPECT_EQ(mincore(buffer, size, resident.data()), 0);
    EXPECT_TRUE(std::all_of(resident.begin(), resident.end(),
                            [](unsigned char page) { return (page & 1) != 0; }));
    EXPECT_EQ(helper->free(buffer), 0);
}

}  // namespace mooncake

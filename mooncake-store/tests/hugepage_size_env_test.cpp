#include "common/client_buffer_allocation.h"

#include <cstdlib>
#include <gtest/gtest.h>

using namespace mooncake;

TEST(HugepageSizeEnvTest, Accepts512Mb) {
    setenv("MC_STORE_USE_HUGEPAGE", "1", 1);
    setenv("MC_STORE_HUGEPAGE_SIZE", "512MB", 1);

    unsigned int flags = 0;
    EXPECT_EQ(get_hugepage_size_from_env(&flags), SZ_512MB);
    EXPECT_TRUE(flags & MAP_HUGETLB);
    EXPECT_TRUE(flags & MAP_HUGE_512MB);

    flags = 0;
    EXPECT_EQ(get_hugepage_size_from_env(&flags, /*use_memfd=*/true), SZ_512MB);
    EXPECT_TRUE(flags & MFD_HUGETLB);
    EXPECT_TRUE(flags & MFD_HUGE_512MB);

    unsetenv("MC_STORE_HUGEPAGE_SIZE");
    unsetenv("MC_STORE_USE_HUGEPAGE");
}

TEST(HugepageSizeEnvTest, FallsBackTo2MbOnInvalidSize) {
    setenv("MC_STORE_USE_HUGEPAGE", "1", 1);
    setenv("MC_STORE_HUGEPAGE_SIZE", "256MB", 1);

    unsigned int flags = 0;
    EXPECT_EQ(get_hugepage_size_from_env(&flags), SZ_2MB);
    EXPECT_TRUE(flags & MAP_HUGE_2MB);

    unsetenv("MC_STORE_HUGEPAGE_SIZE");
    unsetenv("MC_STORE_USE_HUGEPAGE");
}

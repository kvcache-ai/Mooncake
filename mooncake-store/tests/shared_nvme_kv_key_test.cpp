#include "shared_nvme_kv_backend.h"

#include <gtest/gtest.h>

namespace mooncake {

TEST(SharedKvPhysicalKeyTest, EncodesFullAllocationAndPartWithoutHashing) {
    const NvmeKvPhysicalKey expected{1, 2,  3,  4,  5,  6,  7,  8,
                                     9, 10, 11, 12, 13, 14, 15, 16};
    EXPECT_EQ(EncodeSharedNvmeKvPhysicalKey(0x0102030405060708ULL,
                                            0x090a0b0c0d0e0f10ULL),
              expected);
    EXPECT_NE(EncodeSharedNvmeKvPhysicalKey(1),
              EncodeSharedNvmeKvPhysicalKey(1, 1));
    EXPECT_NE(EncodeSharedNvmeKvPhysicalKey(1),
              EncodeSharedNvmeKvPhysicalKey(1ULL << 63));
    EXPECT_NE(EncodeSharedNvmeKvPhysicalKey(UINT64_MAX),
              EncodeSharedNvmeKvPhysicalKey(UINT64_MAX - 1));
}

}  // namespace mooncake

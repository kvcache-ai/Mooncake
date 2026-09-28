#include <gtest/gtest.h>

#include "transport/rdma_transport/ub_segment.h"

#ifdef USE_ASCEND_RDMA
int g_register_calls = 0;
int g_unregister_calls = 0;

extern "C" int aclrtGetLogicDevIdByUserDevId(int32_t user_device_id,
                                               int32_t *logic_device_id) {
    *logic_device_id = user_device_id + 100;
    return 0;
}

extern "C" int halMemRegUbSegment(uint32_t, uint64_t, uint64_t) {
    ++g_register_calls;
    return 0;
}

extern "C" int halMemUnRegUbSegment(uint32_t, uint64_t, uint64_t) {
    ++g_unregister_calls;
    return 0;
}
#endif

namespace mooncake {

TEST(UbSegmentTest, NonNpuLocationsAreNoOp) {
    UbSegment segment;
    EXPECT_EQ(segment.RegUbSegment("cpu:0", 0x1000, 4096), 0);
    EXPECT_EQ(segment.UnRegUbSegment("cpu:0", 0x1000), 0);
}

#ifdef USE_ASCEND_RDMA
TEST(UbSegmentTest, InvalidNpuLocationIsRejected) {
    UbSegment segment;
    EXPECT_NE(segment.RegUbSegment("npu:not-a-device", 0x1000, 4096), 0);
}

TEST(UbSegmentTest, RegistrationIsIdempotent) {
    g_register_calls = 0;
    g_unregister_calls = 0;
    UbSegment segment;
    EXPECT_EQ(segment.RegUbSegment("npu:0", 0x2000, 4096), 0);
    if (g_register_calls == 0)
        GTEST_SKIP() << "Ascend symbols are not available in this test binary";
    EXPECT_EQ(segment.RegUbSegment("npu:0", 0x2000, 4096), 0);
    EXPECT_EQ(g_register_calls, 1);
    EXPECT_EQ(segment.UnRegUbSegment("npu:0", 0x2000), 0);
    EXPECT_EQ(segment.UnRegUbSegment("npu:0", 0x2000), 0);
    EXPECT_EQ(g_unregister_calls, 1);
}

TEST(UbSegmentTest, MismatchedSizeIsRejected) {
    g_register_calls = 0;
    UbSegment segment;
    EXPECT_EQ(segment.RegUbSegment("npu:1", 0x3000, 4096), 0);
    if (g_register_calls == 0)
        GTEST_SKIP() << "Ascend symbols are not available in this test binary";
    EXPECT_NE(segment.RegUbSegment("npu:1", 0x3000, 8192), 0);
    EXPECT_EQ(segment.UnRegUbSegment("npu:1", 0x3000), 0);
}
#else
TEST(UbSegmentTest, DisabledBuildIsInert) {
    UbSegment segment;
    EXPECT_EQ(segment.RegUbSegment("npu:0", 0x1000, 4096), 0);
    EXPECT_EQ(segment.UnRegUbSegment("npu:0", 0x1000), 0);
}
#endif

}  // namespace mooncake

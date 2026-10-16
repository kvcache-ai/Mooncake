#include "transport/sunrise_link_transport/sunrise_link_transport.h"

#include <gtest/gtest.h>

#include <limits>

using namespace mooncake;

TEST(SunriseLinkTransportUnitTest, RejectsWrappedRemoteRanges) {
    EXPECT_FALSE(SunriseLinkTransport::requestRangeFitsInBufferForTest(
        std::numeric_limits<uint64_t>::max() - 50, 80,
        std::numeric_limits<uint64_t>::max() - 100, 64));
}

TEST(SunriseLinkTransportUnitTest, ParsesDeviceIdStrictly) {
    EXPECT_EQ(SunriseLinkTransport::parseDeviceIdForTest("cuda:7"), 7);
    EXPECT_EQ(SunriseLinkTransport::parseDeviceIdForTest("cuda:"), -1);
    EXPECT_EQ(SunriseLinkTransport::parseDeviceIdForTest("cuda:not-a-number"),
              -1);
    EXPECT_EQ(SunriseLinkTransport::parseDeviceIdForTest("cuda:7x"), -1);
}

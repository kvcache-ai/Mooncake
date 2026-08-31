// Copyright 2025 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ib_link_speed.h"

#include <gtest/gtest.h>

namespace mooncake {
namespace {

struct LinkSpeedCase {
    int active_speed;
    int active_width;
    double expected_gbps;
};

// active_speed / active_width are the raw ibv_port_attr encodings from
// ibv_query_port(3): speed is the per-lane rate, width is the lane count.
TEST(IbLinkSpeedTest, ConvertsPortAttrEncodingsToGbps) {
    const LinkSpeedCase cases[] = {
        {1, 1, 2.5},      // SDR x1
        {2, 2, 20.0},     // DDR x4
        {4, 2, 40.0},     // QDR x4
        {8, 2, 40.0},     // FDR10 x4
        {16, 2, 56.0},    // FDR x4
        {32, 2, 100.0},   // EDR x4  (ConnectX-4/5 100G)
        {64, 2, 200.0},   // HDR x4  (ConnectX-6 200G)
        {128, 2, 400.0},  // NDR x4 (ConnectX-7 400G)
        {256, 2, 800.0},  // XDR x4
        {32, 16, 50.0},   // EDR x2
        {64, 1, 50.0},    // HDR x1
        {64, 4, 400.0},   // HDR x8
        {32, 8, 300.0},   // EDR x12
    };
    for (const auto& c : cases) {
        EXPECT_DOUBLE_EQ(ibLinkSpeedGbps(c.active_speed, c.active_width),
                         c.expected_gbps)
            << "speed=" << c.active_speed << " width=" << c.active_width;
    }
}

// An encoding the table does not know must not be guessed at: 0 tells the
// caller the speed is unknown so it can fall back explicitly.
TEST(IbLinkSpeedTest, UnknownEncodingsReportZero) {
    EXPECT_DOUBLE_EQ(ibLinkSpeedGbps(0, 2), 0.0);   // speed unset
    EXPECT_DOUBLE_EQ(ibLinkSpeedGbps(32, 0), 0.0);  // width unset
    EXPECT_DOUBLE_EQ(ibLinkSpeedGbps(3, 2), 0.0);   // not a speed bit
    EXPECT_DOUBLE_EQ(ibLinkSpeedGbps(32, 3), 0.0);  // not a width bit
    EXPECT_DOUBLE_EQ(ibLinkSpeedGbps(-1, 2), 0.0);  // classic TE default
}

}  // namespace
}  // namespace mooncake

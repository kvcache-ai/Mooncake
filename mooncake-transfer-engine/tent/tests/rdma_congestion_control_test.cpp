// Copyright 2026 KVCache.AI
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

#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

#include "adaptive_congestion_control.h"
#include "tent/transport/rdma/congestion_control.h"
#include "tent/transport/rdma/slice.h"

namespace mooncake::tent {
namespace {

adaptive_congestion_control::Config fixedWindow(uint64_t bytes) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    config.min_window_bytes = bytes;
    config.max_window_bytes = bytes;
    return config;
}

class FakeEndpoints {
   public:
    RdmaEndPoint* first() {
        return reinterpret_cast<RdmaEndPoint*>(&first_storage_);
    }
    RdmaEndPoint* second() {
        return reinterpret_cast<RdmaEndPoint*>(&second_storage_);
    }

   private:
    alignas(void*) char first_storage_;
    alignas(void*) char second_storage_;
};

TEST(TentRdmaCongestionControlTest, OffModePreservesOriginalPostingPath) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kOff;
    TentRdmaCongestionControl control(config, 1);
    FakeEndpoints endpoints;
    RdmaSlice first;
    RdmaSlice second;
    first.length = 80;
    second.length = 80;
    std::vector<RdmaSlice*> slices{&first, &second};

    EXPECT_EQ(control.admitPrefix(0, "peer", 0, endpoints.first(), slices), 2u);
    EXPECT_FALSE(first.rdma_congestion_control_permit.active());
    EXPECT_FALSE(second.rdma_congestion_control_permit.active());
}

TEST(TentRdmaCongestionControlTest, AdmitsOnlyTheFifoPrefixThatFits) {
    TentRdmaCongestionControl control(fixedWindow(100), 1);
    FakeEndpoints endpoints;
    RdmaSlice first;
    RdmaSlice second;
    first.length = 60;
    second.length = 60;
    std::vector<RdmaSlice*> slices{&first, &second};

    EXPECT_EQ(control.admitPrefix(0, "peer", 0, endpoints.first(), slices), 1u);
    EXPECT_TRUE(first.rdma_congestion_control_permit.active());
    EXPECT_FALSE(second.rdma_congestion_control_permit.active());
    // A QP-depth-limited pass returns its unposted permit before retrying.
    control.releaseUnposted(&first);
    EXPECT_EQ(control.admitPrefix(0, "peer", 0, endpoints.first(), slices), 1u);

    control.complete(&first, IBV_WC_SUCCESS, 0);
    std::vector<RdmaSlice*> tail{&second};
    EXPECT_EQ(control.admitPrefix(0, "peer", 0, endpoints.first(), tail), 1u);
    control.complete(&second, IBV_WC_SUCCESS, 0);
}

TEST(TentRdmaCongestionControlTest, DeviceWindowIsSharedAcrossPeerRoutes) {
    TentRdmaCongestionControl control(fixedWindow(100), 2);
    FakeEndpoints endpoints;
    RdmaSlice first;
    RdmaSlice same_device;
    RdmaSlice other_device;
    first.length = 60;
    same_device.length = 60;
    other_device.length = 60;
    std::vector<RdmaSlice*> first_list{&first};
    std::vector<RdmaSlice*> same_device_list{&same_device};
    std::vector<RdmaSlice*> other_device_list{&other_device};

    ASSERT_EQ(
        control.admitPrefix(0, "peer-a", 0, endpoints.first(), first_list), 1u);
    EXPECT_EQ(control.admitPrefix(0, "peer-b", 0, endpoints.second(),
                                  same_device_list),
              0u);
    EXPECT_EQ(control.admitPrefix(1, "peer-b", 0, endpoints.second(),
                                  other_device_list),
              1u);

    control.complete(&first, IBV_WC_SUCCESS, 0);
    control.complete(&other_device, IBV_WC_SUCCESS, 0);
    EXPECT_EQ(control.admitPrefix(0, "peer-b", 0, endpoints.second(),
                                  same_device_list),
              1u);
    control.complete(&same_device, IBV_WC_SUCCESS, 0);
}

TEST(TentRdmaCongestionControlTest, EndpointReplacementReacquiresStalePermit) {
    TentRdmaCongestionControl control(fixedWindow(100), 1);
    FakeEndpoints endpoints;
    RdmaSlice slice;
    slice.length = 80;
    std::vector<RdmaSlice*> slices{&slice};

    ASSERT_EQ(control.admitPrefix(0, "peer", 0, endpoints.first(), slices), 1u);
    ASSERT_TRUE(slice.rdma_congestion_control_permit.active());
    // The same queued slice now resolves to a replacement endpoint. Its old
    // generation is returned before a permit for the new generation is kept.
    EXPECT_EQ(control.admitPrefix(0, "peer", 0, endpoints.second(), slices),
              1u);
    EXPECT_TRUE(slice.rdma_congestion_control_permit.active());
    control.complete(&slice, IBV_WC_SUCCESS, 0);

    RdmaSlice next;
    next.length = 80;
    std::vector<RdmaSlice*> next_list{&next};
    EXPECT_EQ(control.admitPrefix(0, "peer", 0, endpoints.second(), next_list),
              1u);
    control.complete(&next, IBV_WC_SUCCESS, 0);
}

TEST(TentRdmaCongestionControlTest, PathChangeReturnsThePreviousPermit) {
    TentRdmaCongestionControl control(fixedWindow(100), 2);
    FakeEndpoints endpoints;
    RdmaSlice moved;
    moved.length = 80;
    std::vector<RdmaSlice*> moved_list{&moved};

    ASSERT_EQ(
        control.admitPrefix(0, "peer-a", 0, endpoints.first(), moved_list), 1u);
    ASSERT_EQ(
        control.admitPrefix(1, "peer-b", 0, endpoints.first(), moved_list), 1u);
    control.complete(&moved, IBV_WC_SUCCESS, 0);

    RdmaSlice original_path;
    original_path.length = 80;
    std::vector<RdmaSlice*> original_path_list{&original_path};
    EXPECT_EQ(control.admitPrefix(0, "peer-a", 0, endpoints.first(),
                                  original_path_list),
              1u);
    control.complete(&original_path, IBV_WC_SUCCESS, 0);
}

TEST(TentRdmaCongestionControlTest, DeviceResetFencesQueuedPermit) {
    TentRdmaCongestionControl control(fixedWindow(100), 1);
    FakeEndpoints endpoints;
    RdmaSlice slice;
    slice.length = 80;
    std::vector<RdmaSlice*> slices{&slice};

    ASSERT_EQ(control.admitPrefix(0, "peer", 0, endpoints.first(), slices), 1u);
    control.resetDevice(0);
    EXPECT_EQ(control.admitPrefix(0, "peer", 0, endpoints.first(), slices), 1u);
    control.complete(&slice, IBV_WC_SUCCESS, 0);
}

TEST(TentRdmaCongestionControlTest, UnpostedSlicesReturnPermits) {
    TentRdmaCongestionControl control(fixedWindow(100), 1);
    FakeEndpoints endpoints;
    RdmaSlice explicit_release;
    explicit_release.length = 80;
    std::vector<RdmaSlice*> first{&explicit_release};
    ASSERT_EQ(control.admitPrefix(0, "peer", 0, endpoints.first(), first), 1u);
    control.releaseUnposted(&explicit_release);

    RdmaSlice after_release;
    after_release.length = 80;
    std::vector<RdmaSlice*> third{&after_release};
    EXPECT_EQ(control.admitPrefix(0, "peer", 0, endpoints.first(), third), 1u);
    control.releaseUnposted(&after_release);
}

}  // namespace
}  // namespace mooncake::tent

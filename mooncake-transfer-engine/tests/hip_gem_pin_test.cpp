// Copyright 2024 KVCache.AI
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

// Which RDMA device hip_gem_allocator.so pins a new buffer through. try_pin
// stands in for allocating the device's protection domain and registering the
// one-page dma-buf MR.

#include <gtest/gtest.h>

#include <vector>

#include "hip_gem_pin.h"

using mooncake::pickPinDevice;

namespace {

struct Devices {
    std::vector<bool> accept;
    std::vector<int> tried;
    bool operator()(int i) {
        tried.push_back(i);
        return accept[i];
    }
};

TEST(HipGemPinDevice, SkipsADeviceThatRejectsTheDmabufMr) {
    Devices d{{false, true, true}, {}};
    EXPECT_EQ(pickPinDevice(3, -1, std::ref(d)), 1);
    EXPECT_EQ(d.tried, (std::vector<int>{0, 1}));
}

TEST(HipGemPinDevice, TriesTheLastDeviceThatPinnedFirst) {
    Devices d{{false, true, true}, {}};
    EXPECT_EQ(pickPinDevice(3, 1, std::ref(d)), 1);
    EXPECT_EQ(d.tried, (std::vector<int>{1}));
}

TEST(HipGemPinDevice, MovesOnWhenTheLastDeviceThatPinnedFails) {
    Devices d{{false, false, true}, {}};
    EXPECT_EQ(pickPinDevice(3, 1, std::ref(d)), 2);
    EXPECT_EQ(d.tried, (std::vector<int>{1, 0, 2}));
}

TEST(HipGemPinDevice, NoDeviceAccepts) {
    Devices d{{false, false}, {}};
    EXPECT_EQ(pickPinDevice(2, -1, std::ref(d)), -1);
    EXPECT_EQ(d.tried, (std::vector<int>{0, 1}));
    EXPECT_EQ(pickPinDevice(0, -1, [](int) { return true; }), -1);
}

}  // namespace

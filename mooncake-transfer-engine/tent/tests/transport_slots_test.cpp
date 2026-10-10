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

#include <gtest/gtest.h>

#include <atomic>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "tent/common/transport_slots.h"
#include "tent/common/types.h"

namespace mooncake {
namespace tent {
namespace {

class TransportSlotsTest : public ::testing::Test {
   protected:
    void SetUp() override { resetPluginTransportSlotsForTest(); }
    void TearDown() override { resetPluginTransportSlotsForTest(); }
};

TEST_F(TransportSlotsTest, LayoutReservesSlotsAfterBuiltins) {
    EXPECT_EQ(static_cast<int>(kFirstPluginSlot),
              static_cast<int>(kNumBuiltinTransportTypes));
    EXPECT_EQ(static_cast<int>(kNumTransportTypes),
              static_cast<int>(kNumBuiltinTransportTypes) + kNumPluginSlots);
    EXPECT_EQ(kSupportedTransportTypes, static_cast<int>(kNumTransportTypes));
}

TEST_F(TransportSlotsTest, BuiltinNamesUnchanged) {
    for (int t = 0; t < kNumBuiltinTransportTypes; ++t) {
        auto type = static_cast<TransportType>(t);
        const std::string name = transportTypeName(type);
        EXPECT_NE(name, "unknown") << t;
        EXPECT_EQ(parseTransportType(name), type) << name;
    }
    EXPECT_STREQ(transportTypeName(TCP), "tcp");
    EXPECT_STREQ(transportTypeName(FABRIC), "fabric");
}

TEST_F(TransportSlotsTest, UnassignedSlotsAreUnknown) {
    for (int t = kFirstPluginSlot; t < kNumTransportTypes; ++t) {
        EXPECT_STREQ(transportTypeName(static_cast<TransportType>(t)),
                     "unknown");
        EXPECT_FALSE(isPluginTransportSlot(t));
        EXPECT_EQ(pluginTransportSlotFlags(t), 0u);
    }
    EXPECT_STREQ(transportTypeName(static_cast<TransportType>(-1)), "unknown");
    EXPECT_STREQ(transportTypeName(kNumTransportTypes), "unknown");
    EXPECT_STREQ(transportTypeName(static_cast<TransportType>(1000)),
                 "unknown");
    EXPECT_EQ(parseTransportType("loopback"), UNSPEC);
    EXPECT_EQ(parseTransportType("unknown"), UNSPEC);
    // An unassigned plugin slot is not a usable C-API hint.
    EXPECT_EQ(c_to_transport_hint(kFirstPluginSlot), UNSPEC);
    EXPECT_EQ(c_to_transport_hint(kNumTransportTypes - 1), UNSPEC);
    EXPECT_EQ(parseTransportType(""), UNSPEC);
    EXPECT_EQ(pluginTransportSlotCount(), 0);
}

TEST_F(TransportSlotsTest, AssignResolvesBothWays) {
    TransportType slot = UNSPEC;
    ASSERT_EQ(assignPluginTransportSlot(
                  "loopback", TENT_XPORT_FLAG_SAME_MACHINE_ONLY, slot),
              SlotAssignResult::kAssigned);
    EXPECT_EQ(slot, kFirstPluginSlot);
    EXPECT_STREQ(transportTypeName(slot), "loopback");
    EXPECT_EQ(parseTransportType("loopback"), slot);
    EXPECT_TRUE(isPluginTransportSlot(slot));
    EXPECT_EQ(pluginTransportSlotFlags(slot),
              static_cast<uint32_t>(TENT_XPORT_FLAG_SAME_MACHINE_ONLY));
    EXPECT_EQ(pluginTransportSlotCount(), 1);

    TransportType second = UNSPEC;
    ASSERT_EQ(assignPluginTransportSlot("vendorx", 0, second),
              SlotAssignResult::kAssigned);
    EXPECT_EQ(static_cast<int>(second), static_cast<int>(slot) + 1);
    EXPECT_EQ(parseTransportType("vendorx"), second);
    // A plugin slot is a valid C-API transport hint.
    EXPECT_EQ(c_to_transport_hint(static_cast<int>(second)), second);
    EXPECT_EQ(c_to_transport_hint(kNumTransportTypes), UNSPEC);
}

TEST_F(TransportSlotsTest, DuplicateNameRejected) {
    TransportType slot = UNSPEC;
    ASSERT_EQ(assignPluginTransportSlot("loopback", 0, slot),
              SlotAssignResult::kAssigned);
    TransportType again = UNSPEC;
    EXPECT_EQ(assignPluginTransportSlot("loopback", 0, again),
              SlotAssignResult::kDuplicateName);
    EXPECT_EQ(again, UNSPEC);
    EXPECT_EQ(pluginTransportSlotCount(), 1);
}

TEST_F(TransportSlotsTest, NoFreeSlotAfterAllAssigned) {
    for (int i = 0; i < kNumPluginSlots; ++i) {
        TransportType slot = UNSPEC;
        ASSERT_EQ(assignPluginTransportSlot("p" + std::to_string(i), 0, slot),
                  SlotAssignResult::kAssigned);
        EXPECT_EQ(static_cast<int>(slot), kFirstPluginSlot + i);
    }
    TransportType slot = UNSPEC;
    EXPECT_EQ(assignPluginTransportSlot("one_too_many", 0, slot),
              SlotAssignResult::kNoFreeSlot);
    EXPECT_EQ(parseTransportType("one_too_many"), UNSPEC);
    EXPECT_EQ(pluginTransportSlotCount(), kNumPluginSlots);
}

TEST_F(TransportSlotsTest, RoutingHelpersOnlyRestrictPluginSlots) {
    TransportType same_machine = UNSPEC, local_only = UNSPEC, staging = UNSPEC;
    ASSERT_EQ(
        assignPluginTransportSlot(
            "same_machine", TENT_XPORT_FLAG_SAME_MACHINE_ONLY, same_machine),
        SlotAssignResult::kAssigned);
    ASSERT_EQ(assignPluginTransportSlot(
                  "local_only", TENT_XPORT_FLAG_LOCAL_SEGMENT_ONLY, local_only),
              SlotAssignResult::kAssigned);
    ASSERT_EQ(assignPluginTransportSlot(
                  "staging", TENT_XPORT_FLAG_HOST_STAGING_CAPABLE, staging),
              SlotAssignResult::kAssigned);

    EXPECT_TRUE(pluginSlotAllowsRoute(same_machine, false, true));
    EXPECT_FALSE(pluginSlotAllowsRoute(same_machine, false, false));
    EXPECT_TRUE(pluginSlotAllowsRoute(local_only, true, true));
    EXPECT_FALSE(pluginSlotAllowsRoute(local_only, false, true));
    EXPECT_TRUE(pluginSlotAllowsRoute(staging, false, false));
    // Built-in types are never restricted by the helper.
    EXPECT_TRUE(pluginSlotAllowsRoute(NVLINK, false, false));
    EXPECT_TRUE(pluginSlotAllowsRoute(XPU, false, false));
    // Unassigned plugin slots carry no flags.
    EXPECT_TRUE(pluginSlotAllowsRoute(kNumTransportTypes - 1, false, false));

    EXPECT_TRUE(
        pluginSlotHasFlag(staging, TENT_XPORT_FLAG_HOST_STAGING_CAPABLE));
    EXPECT_FALSE(
        pluginSlotHasFlag(same_machine, TENT_XPORT_FLAG_HOST_STAGING_CAPABLE));
    EXPECT_FALSE(pluginSlotHasFlag(RDMA, TENT_XPORT_FLAG_HOST_STAGING_CAPABLE));
}

// Readers never lock: while slots are being assigned they must see either
// "unknown" or the final name, never a torn or foreign value.
TEST_F(TransportSlotsTest, ConcurrentReadersSeeUnknownOrFinalName) {
    std::atomic<bool> stop{false};
    std::atomic<int> bad{0};
    std::vector<std::thread> readers;
    for (int r = 0; r < 4; ++r) {
        readers.emplace_back([&] {
            while (!stop.load()) {
                for (int t = kFirstPluginSlot; t < kNumTransportTypes; ++t) {
                    const char* name =
                        transportTypeName(static_cast<TransportType>(t));
                    const std::string expected =
                        "c" + std::to_string(t - kFirstPluginSlot);
                    if (std::strcmp(name, "unknown") != 0 && name != expected)
                        bad.fetch_add(1);
                    auto parsed = parseTransportType(expected);
                    if (parsed != UNSPEC && parsed != t) bad.fetch_add(1);
                }
            }
        });
    }
    for (int i = 0; i < kNumPluginSlots; ++i) {
        TransportType slot = UNSPEC;
        ASSERT_EQ(assignPluginTransportSlot("c" + std::to_string(i), 0, slot),
                  SlotAssignResult::kAssigned);
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    stop.store(true);
    for (auto& t : readers) t.join();
    EXPECT_EQ(bad.load(), 0);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake

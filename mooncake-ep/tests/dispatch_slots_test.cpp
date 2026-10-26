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
#include <mooncake_ep_dispatch_slots.h>

#include <cstdint>

namespace mooncake {
namespace {

constexpr uint64_t kBase = 0x10000;

uint64_t ptr(int i) { return kBase + static_cast<uint64_t>(i) * 0x1000; }

TEST(DispatchSlotsTest, TakesSlotsInRingOrder) {
    DispatchSlots slots;
    for (int i = 0; i < 2 * kEpDispatchSlots; ++i)
        EXPECT_EQ(slots.take(ptr(i)), i % kEpDispatchSlots);
}

TEST(DispatchSlotsTest, FindsTheDispatchOfAHandle) {
    DispatchSlots slots;
    const int a = slots.take(ptr(0));
    const int b = slots.take(ptr(1));
    EXPECT_EQ(slots.find(ptr(0)), a);
    EXPECT_EQ(slots.find(ptr(1)), b);
    EXPECT_NE(a, b);
}

TEST(DispatchSlotsTest, RejectsAnUnknownHandle) {
    DispatchSlots slots;
    slots.take(ptr(0));
    EXPECT_EQ(slots.find(ptr(1)), -1);
}

// The caching allocator can hand a new dispatch the src_info block of a
// finished one. The new dispatch must get a slot of its own, which a second
// dispatch held at the same time must not take.
TEST(DispatchSlotsTest, ReusedSrcInfoKeepsHeldDispatchesApart) {
    DispatchSlots slots;
    for (int i = 0; i < kEpDispatchSlots; ++i) slots.take(ptr(i));
    // The ring has wrapped; ptr(0)'s dispatch is done and its block reused.
    const int a = slots.take(ptr(0));
    const int b = slots.take(ptr(100));
    EXPECT_NE(a, b);
    EXPECT_EQ(slots.find(ptr(0)), a);
    EXPECT_EQ(slots.find(ptr(100)), b);
}

TEST(DispatchSlotsTest, SameSrcInfoTwiceMovesToTheNewSlot) {
    DispatchSlots slots;
    const int first = slots.take(ptr(0));
    const int second = slots.take(ptr(0));
    EXPECT_NE(first, second);
    EXPECT_EQ(slots.find(ptr(0)), second);
    // Retaking `first` later must not drop ptr(0), which now lives in
    // `second`.
    for (int i = 1; i < kEpDispatchSlots - 1; ++i) slots.take(ptr(i));
    EXPECT_EQ(slots.take(ptr(50)), first);
    EXPECT_EQ(slots.find(ptr(0)), second);
}

TEST(DispatchSlotsTest, ForgetsADispatchOnceItsSlotIsRetaken) {
    DispatchSlots slots;
    slots.take(ptr(0));
    for (int i = 1; i <= kEpDispatchSlots; ++i) slots.take(ptr(i));
    EXPECT_EQ(slots.find(ptr(0)), -1);
    EXPECT_EQ(slots.find(ptr(kEpDispatchSlots)), 0);
}

}  // namespace
}  // namespace mooncake

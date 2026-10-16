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

// Backoff policy for transferSync() / waitTransferCompletion().
//
// Both used to poll progressBatch() in a `while (true)` with no pause, and
// progressBatch takes progress_mutex_ every time, so the spin contended with
// every other thread's submit and poll path.
//
// The policy is a pure function so its shape can be pinned down without
// standing up an engine or timing a sleep.

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <limits>

#include "tent/runtime/transfer_engine_impl.h"

namespace mooncake {
namespace tent {
namespace {

constexpr uint64_t kHotPolls = 128;
constexpr auto kMaxDelay = std::chrono::microseconds(200);

// Short transfers must not pay for the fix.
TEST(PollBackoffTest, StaysHotForTheFirstPolls) {
    for (uint64_t i = 0; i < kHotPolls; ++i) {
        EXPECT_EQ(nextPollDelay(i).count(), 0) << "poll " << i;
    }
}

TEST(PollBackoffTest, SleepsOnceHotPhaseEnds) {
    EXPECT_EQ(nextPollDelay(kHotPolls), std::chrono::microseconds(1));
    EXPECT_EQ(nextPollDelay(kHotPolls + 1), std::chrono::microseconds(2));
    EXPECT_EQ(nextPollDelay(kHotPolls + 2), std::chrono::microseconds(4));
}

// The point of the change: a long transfer must stop hammering
// progress_mutex_, so the delay has to grow.
TEST(PollBackoffTest, DelayIsNonDecreasing) {
    auto previous = nextPollDelay(0);
    for (uint64_t i = 1; i < 4096; ++i) {
        const auto current = nextPollDelay(i);
        ASSERT_GE(current, previous) << "poll " << i;
        previous = current;
    }
    EXPECT_GT(previous.count(), 0);
}

// A cap bounds completion latency; without one the backoff reaches seconds.
TEST(PollBackoffTest, DelayIsCapped) {
    for (uint64_t i = kHotPolls; i < 4096; ++i) {
        ASSERT_LE(nextPollDelay(i), kMaxDelay) << "poll " << i;
    }
    EXPECT_EQ(nextPollDelay(4096), kMaxDelay);
}

// A loop that never terminates must not overflow the shift into a zero delay.
TEST(PollBackoffTest, SaturatesInsteadOfOverflowing) {
    EXPECT_EQ(nextPollDelay(std::numeric_limits<uint64_t>::max()), kMaxDelay);
    EXPECT_EQ(nextPollDelay(uint64_t(1) << 62), kMaxDelay);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake

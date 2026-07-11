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

#include "receiver_credit_metrics.h"

#include <cstdio>
#include <fstream>
#include <limits>

#include <gtest/gtest.h>

#include "tent/thirdparty/nlohmann/json.h"

namespace mooncake {
namespace tent {
namespace {

TEST(ReceiverCapacityTrackerTest, ObserveModeExposesAggregateOvercommit) {
    ReceiverCapacityTracker tracker(100, 2);
    EXPECT_TRUE(tracker.tryReserve(60, 1, false));
    EXPECT_TRUE(tracker.tryReserve(60, 1, false));

    const auto state = tracker.snapshot();
    EXPECT_EQ(state.current_bytes, 120u);
    EXPECT_EQ(state.peak_bytes, 120u);
    EXPECT_EQ(state.capacity_violation_total, 1u);
    EXPECT_EQ(state.stalled_total, 0u);
}

TEST(ReceiverCapacityTrackerTest, EnforcedModeBoundsEveryResource) {
    ReceiverCapacityTracker tracker(100, 2);
    EXPECT_TRUE(tracker.tryReserve(60, 1, true));
    EXPECT_FALSE(tracker.tryReserve(60, 1, true));

    auto state = tracker.snapshot();
    EXPECT_EQ(state.current_bytes, 60u);
    EXPECT_EQ(state.current_slots, 1u);
    EXPECT_EQ(state.capacity_violation_total, 0u);
    EXPECT_EQ(state.stalled_total, 1u);

    EXPECT_TRUE(tracker.release(60, 1));
    EXPECT_TRUE(tracker.tryReserve(100, 2, true));
    state = tracker.snapshot();
    EXPECT_LE(state.peak_bytes, state.capacity_bytes);
    EXPECT_LE(state.peak_slots, state.capacity_slots);
}

TEST(ReceiverCapacityTrackerTest, ReservationIsAllOrNothing) {
    ReceiverCapacityTracker tracker(100, 1);
    EXPECT_TRUE(tracker.tryReserve(50, 1, true));
    EXPECT_FALSE(tracker.tryReserve(10, 1, true));

    const auto state = tracker.snapshot();
    EXPECT_EQ(state.current_bytes, 50u);
    EXPECT_EQ(state.current_slots, 1u);
}

TEST(ReceiverCapacityTrackerTest, InvalidReleaseCannotMintCapacity) {
    ReceiverCapacityTracker tracker(100, 2);
    EXPECT_TRUE(tracker.tryReserve(50, 1, true));
    EXPECT_FALSE(tracker.release(51, 1));

    const auto state = tracker.snapshot();
    EXPECT_EQ(state.current_bytes, 50u);
    EXPECT_EQ(state.current_slots, 1u);
    EXPECT_EQ(state.invalid_release_total, 1u);
}

TEST(ReceiverCapacityTrackerTest, AdditionOverflowFailsClosed) {
    ReceiverCapacityTracker tracker(std::numeric_limits<uint64_t>::max(), 2);
    EXPECT_TRUE(
        tracker.tryReserve(std::numeric_limits<uint64_t>::max(), 1, true));
    EXPECT_FALSE(tracker.tryReserve(1, 1, true));

    const auto state = tracker.snapshot();
    EXPECT_EQ(state.current_bytes, std::numeric_limits<uint64_t>::max());
    EXPECT_EQ(state.capacity_violation_total, 0u);
    EXPECT_EQ(state.stalled_total, 1u);
}

TEST(ReceiverCapacityTrackerTest, WritesVersionedJsonl) {
    const std::string path = "tebench_receiver_credit_metrics_test.jsonl";
    std::remove(path.c_str());
    ReceiverCreditRunReport report;
    report.run_id = "unit";
    report.mode = "credit";
    report.condition = "normal";
    report.sender_count = 4;
    report.repetition = 1;
    report.offered = 10;
    report.completed = 10;
    report.throughput_gbps = 12.5;
    report.p99_us = 100.0;
    report.oracle_throughput_gbps = 13.0;
    report.receiver.capacity_bytes = 1024;
    report.receiver.capacity_slots = 2;
    report.receiver.peak_bytes = 1024;
    report.receiver.peak_slots = 2;

    std::string error;
    ASSERT_TRUE(appendReceiverCreditRunJsonl(path, report, &error)) << error;

    std::ifstream input(path);
    nlohmann::json record;
    ASSERT_NO_THROW(input >> record);
    EXPECT_EQ(record["schema_version"], 1);
    EXPECT_EQ(record["mode"], "credit");
    EXPECT_EQ(record["sender_count"], 4);
    EXPECT_EQ(record["receiver"]["peak_bytes"], 1024);
    EXPECT_EQ(record["receiver"]["capacity_violation_total"], 0);
    std::remove(path.c_str());
}

}  // namespace
}  // namespace tent
}  // namespace mooncake

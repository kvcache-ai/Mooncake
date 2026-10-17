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

#include "task_congestion_status.h"
#include "task_congestion_observation.h"

namespace mooncake::adaptive_congestion_control {
namespace {

TaskCongestionEvidence windowEvidence() {
    TaskCongestionEvidence evidence;
    evidence.reason = TaskCongestionReason::kByteWindow;
    evidence.path = "route-a";
    evidence.observed_at_ns = 100;
    evidence.controller_generation = 7;
    return evidence;
}

TEST(TaskCongestionObservationTest,
     WorstUnresolvedSliceWinsAndRecoveryClearsIt) {
    TaskCongestionObservation task(3, 1);
    EXPECT_EQ(task.state(), TaskCongestionState::kNormal);

    EXPECT_TRUE(task.defer(1, 1, windowEvidence()));
    EXPECT_EQ(task.state(), TaskCongestionState::kCongested);

    TaskCongestionEvidence isolation;
    isolation.reason = TaskCongestionReason::kPathQuarantined;
    isolation.path = "route-b";
    isolation.observed_at_ns = 200;
    isolation.controller_generation = 7;
    EXPECT_TRUE(task.avoid(2, 1, isolation));
    EXPECT_EQ(task.state(), TaskCongestionState::kLongUnavailable);

    EXPECT_TRUE(task.resolve(2, 1));
    EXPECT_EQ(task.state(), TaskCongestionState::kCongested);
    EXPECT_TRUE(task.resolve(1, 1));
    EXPECT_EQ(task.state(), TaskCongestionState::kNormal);

    const auto detail = task.detail();
    EXPECT_TRUE(detail.reason.observed);
    EXPECT_EQ(detail.reason.value, TaskCongestionReason::kPathQuarantined);
    EXPECT_TRUE(detail.resolved);
    EXPECT_TRUE(detail.affected_path.observed);
    EXPECT_EQ(detail.affected_path.value, "route-b");
}

TEST(TaskCongestionObservationTest, FailureWithoutCongestionEvidenceIsUnknown) {
    TaskCongestionObservation task(2, 1);
    task.terminalFailure(1);
    EXPECT_EQ(task.state(), TaskCongestionState::kUnknown);
    const auto detail = task.detail();
    EXPECT_FALSE(detail.reason.observed);
    EXPECT_FALSE(detail.retry_after_ns.observed);
    EXPECT_FALSE(detail.failure_scope.observed);
    EXPECT_FALSE(detail.controller_generation.observed);
}

TEST(TaskCongestionObservationTest, DetailNamesTheWorstActiveSlice) {
    TaskCongestionObservation task(2, 1);
    TaskCongestionEvidence isolation;
    isolation.reason = TaskCongestionReason::kPathQuarantined;
    isolation.path = "isolated-route";
    isolation.observed_at_ns = 100;
    EXPECT_TRUE(task.avoid(0, 1, isolation));

    auto window = windowEvidence();
    window.observed_at_ns = 200;
    EXPECT_TRUE(task.defer(1, 1, window));
    const auto detail = task.detail();
    EXPECT_EQ(detail.state, TaskCongestionState::kLongUnavailable);
    ASSERT_TRUE(detail.reason.observed);
    EXPECT_EQ(detail.reason.value, TaskCongestionReason::kPathQuarantined);
    ASSERT_TRUE(detail.affected_path.observed);
    EXPECT_EQ(detail.affected_path.value, "isolated-route");

    EXPECT_TRUE(task.resolve(0, 1));
    const auto recovered = task.detail();
    EXPECT_EQ(recovered.state, TaskCongestionState::kCongested);
    ASSERT_TRUE(recovered.reason.observed);
    EXPECT_EQ(recovered.reason.value, TaskCongestionReason::kByteWindow);
}

TEST(TaskCongestionObservationTest, OldAttemptCannotOverrideCurrentState) {
    TaskCongestionObservation task(2, 1);
    EXPECT_TRUE(task.defer(0, 1, windowEvidence()));
    EXPECT_TRUE(task.beginAttempt(2, 2));
    EXPECT_EQ(task.state(), TaskCongestionState::kNormal);

    EXPECT_FALSE(task.avoid(0, 1, windowEvidence()));
    EXPECT_EQ(task.state(), TaskCongestionState::kNormal);

    EXPECT_TRUE(task.avoid(1, 2, windowEvidence()));
    EXPECT_EQ(task.state(), TaskCongestionState::kLongUnavailable);
    EXPECT_FALSE(task.resolve(1, 1));
    EXPECT_EQ(task.state(), TaskCongestionState::kLongUnavailable);

    EXPECT_FALSE(task.avoid(2, 2, windowEvidence()));
    EXPECT_EQ(task.state(), TaskCongestionState::kUnknown);
}

TEST(TaskCongestionObservationTest, NewAttemptCanUseMoreSubmittedSlices) {
    TaskCongestionObservation task(1, 1);
    EXPECT_TRUE(task.defer(0, 1, windowEvidence()));
    EXPECT_TRUE(task.beginAttempt(2, 2));
    EXPECT_FALSE(task.defer(0, 1, windowEvidence()));

    TaskCongestionEvidence isolation;
    isolation.reason = TaskCongestionReason::kPathQuarantined;
    isolation.path = "second-slice";
    isolation.controller_generation = 8;
    EXPECT_TRUE(task.avoid(1, 2, isolation));
    EXPECT_EQ(task.state(), TaskCongestionState::kLongUnavailable);
    const auto detail = task.detail();
    ASSERT_TRUE(detail.slice_id.observed);
    EXPECT_EQ(detail.slice_id.value, 1);
}

TEST(TaskCongestionObservationTest, PublishedSlicesExtendCurrentAttempt) {
    TaskCongestionObservation task(1, 1);
    EXPECT_TRUE(task.defer(0, 1, windowEvidence()));
    EXPECT_TRUE(task.extendAttempt(1, 3));
    EXPECT_FALSE(task.extendAttempt(0, 4));
    EXPECT_FALSE(task.extendAttempt(1, 2));

    TaskCongestionEvidence isolation;
    isolation.reason = TaskCongestionReason::kPathQuarantined;
    EXPECT_TRUE(task.avoid(2, 1, isolation));
    EXPECT_EQ(task.state(), TaskCongestionState::kLongUnavailable);
    EXPECT_TRUE(task.resolve(2, 1));
    EXPECT_EQ(task.state(), TaskCongestionState::kCongested);
}

TEST(TaskCongestionObservationTest,
     SlicesOnDifferentControllerGenerationsShareAttempt) {
    TaskCongestionObservation task(2, 1);
    EXPECT_TRUE(task.defer(0, 1, windowEvidence()));

    TaskCongestionEvidence isolation;
    isolation.reason = TaskCongestionReason::kPathQuarantined;
    isolation.path = "other-device";
    isolation.controller_generation = 2;
    EXPECT_TRUE(task.avoid(1, 1, isolation));
    EXPECT_EQ(task.state(), TaskCongestionState::kLongUnavailable);
    const auto detail = task.detail();
    ASSERT_TRUE(detail.controller_generation.observed);
    EXPECT_EQ(detail.controller_generation.value, 2);
    EXPECT_FALSE(task.resolve(1, 0));
    EXPECT_EQ(task.state(), TaskCongestionState::kLongUnavailable);
}

}  // namespace
}  // namespace mooncake::adaptive_congestion_control

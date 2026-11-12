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

#include <thread>

#include "task_congestion_status.h"
#include "task_congestion_observation.h"

namespace mooncake::adaptive_congestion_control {
namespace {

TaskCongestionEvidence windowEvidence() {
    TaskCongestionEvidence evidence;
    evidence.reason = {true, TaskCongestionReason::kByteWindow};
    evidence.path = "route-a";
    evidence.observed_at_ns = {true, 100};
    evidence.controller_generation = {true, 7};
    return evidence;
}

TEST(TaskCongestionObservationTest,
     WorstUnresolvedSliceWinsAndRecoveryClearsIt) {
    TaskCongestionObservation task(3);
    EXPECT_EQ(task.state(), TaskCongestionState::kNormal);

    EXPECT_TRUE(task.defer(1, windowEvidence()));
    EXPECT_EQ(task.state(), TaskCongestionState::kCongested);

    TaskCongestionEvidence isolation;
    isolation.reason = {true, TaskCongestionReason::kPathQuarantined};
    isolation.path = "route-b";
    isolation.observed_at_ns = {true, 200};
    isolation.controller_generation = {true, 7};
    EXPECT_TRUE(task.avoid(2, isolation));
    EXPECT_EQ(task.state(), TaskCongestionState::kLongUnavailable);

    EXPECT_TRUE(task.resolve(2));
    EXPECT_EQ(task.state(), TaskCongestionState::kCongested);
    EXPECT_TRUE(task.resolve(1));
    EXPECT_EQ(task.state(), TaskCongestionState::kNormal);

    const auto detail = task.detail();
    EXPECT_TRUE(detail.reason.observed);
    EXPECT_EQ(detail.reason.value, TaskCongestionReason::kPathQuarantined);
    EXPECT_TRUE(detail.resolved);
    EXPECT_TRUE(detail.affected_path.observed);
    EXPECT_EQ(detail.affected_path.value, "route-b");
}

TEST(TaskCongestionObservationTest, HealthyTaskHasNoFailureEvidence) {
    TaskCongestionObservation task(2);
    EXPECT_EQ(task.state(), TaskCongestionState::kNormal);
    const auto detail = task.detail();
    EXPECT_FALSE(detail.reason.observed);
    EXPECT_FALSE(detail.retry_after_ns.observed);
    EXPECT_FALSE(detail.failure_scope.observed);
    EXPECT_FALSE(detail.controller_generation.observed);
}

TEST(TaskCongestionObservationTest, RepeatedEventAfterRecoveryIsUnresolved) {
    TaskCongestionObservation task(1);
    const auto evidence = windowEvidence();
    ASSERT_TRUE(task.defer(0, evidence));
    ASSERT_TRUE(task.resolve(0));
    ASSERT_TRUE(task.detail().resolved);

    ASSERT_TRUE(task.defer(0, evidence));
    const auto detail = task.detail();
    EXPECT_EQ(task.state(), TaskCongestionState::kCongested);
    EXPECT_FALSE(detail.resolved);
    ASSERT_TRUE(detail.affected_path.observed);
    EXPECT_EQ(detail.affected_path.value, "route-a");
    EXPECT_EQ(evidence.path, "route-a");
    EXPECT_TRUE(evidence.observed_at_ns.observed);
    EXPECT_EQ(evidence.observed_at_ns.value, 100u);
}

TEST(TaskCongestionObservationTest, NewEvidenceClearsAbsentOptionalFields) {
    TaskCongestionObservation task(1);
    auto previous = windowEvidence();
    previous.failure_scope = {true, TaskCongestionFailureScope::kDevice};
    previous.controller_mode = {true, TaskCongestionControllerMode::kEnforce};
    previous.window_bytes = {true, 128};
    previous.inflight_bytes = {true, 64};
    previous.retry_after_ns = {true, 500};
    ASSERT_TRUE(task.avoid(0, previous));
    ASSERT_TRUE(task.resolve(0));

    TaskCongestionEvidence next;
    next.reason = {true, TaskCongestionReason::kFatal};
    next.path = "x";
    ASSERT_TRUE(task.avoid(0, next));
    const auto detail = task.detail();
    EXPECT_EQ(detail.state, TaskCongestionState::kLongUnavailable);
    EXPECT_FALSE(detail.resolved);
    ASSERT_TRUE(detail.reason.observed);
    EXPECT_EQ(detail.reason.value, TaskCongestionReason::kFatal);
    ASSERT_TRUE(detail.affected_path.observed);
    EXPECT_EQ(detail.affected_path.value, "x");
    EXPECT_FALSE(detail.failure_scope.observed);
    EXPECT_FALSE(detail.controller_mode.observed);
    EXPECT_FALSE(detail.controller_generation.observed);
    EXPECT_FALSE(detail.observed_at_ns.observed);
    EXPECT_FALSE(detail.window_bytes.observed);
    EXPECT_FALSE(detail.inflight_bytes.observed);
    EXPECT_FALSE(detail.retry_after_ns.observed);
    EXPECT_EQ(next.path, "x");
    EXPECT_TRUE(next.reason.observed);
    EXPECT_EQ(next.reason.value, TaskCongestionReason::kFatal);

    ASSERT_TRUE(task.resolve(0));
    ASSERT_TRUE(task.defer(0, TaskCongestionEvidence{}));
    const auto empty = task.detail();
    EXPECT_FALSE(empty.reason.observed);
    EXPECT_FALSE(empty.affected_path.observed);
    EXPECT_FALSE(empty.resolved);
}

TEST(TaskCongestionObservationTest, DetailNamesTheWorstActiveSlice) {
    TaskCongestionObservation task(2);
    TaskCongestionEvidence isolation;
    isolation.reason = {true, TaskCongestionReason::kPathQuarantined};
    isolation.path = "isolated-route";
    isolation.observed_at_ns = {true, 100};
    EXPECT_TRUE(task.avoid(0, isolation));

    auto window = windowEvidence();
    window.observed_at_ns = {true, 200};
    EXPECT_TRUE(task.defer(1, window));
    const auto detail = task.detail();
    EXPECT_EQ(detail.state, TaskCongestionState::kLongUnavailable);
    ASSERT_TRUE(detail.reason.observed);
    EXPECT_EQ(detail.reason.value, TaskCongestionReason::kPathQuarantined);
    ASSERT_TRUE(detail.affected_path.observed);
    EXPECT_EQ(detail.affected_path.value, "isolated-route");

    EXPECT_TRUE(task.resolve(0));
    const auto recovered = task.detail();
    EXPECT_EQ(recovered.state, TaskCongestionState::kCongested);
    ASSERT_TRUE(recovered.reason.observed);
    EXPECT_EQ(recovered.reason.value, TaskCongestionReason::kByteWindow);
}

TEST(TaskCongestionObservationTest, EveryEvidenceFieldIsReturned) {
    TaskCongestionObservation task(1);
    static const std::string path = "interned-route";
    auto evidence = windowEvidence();
    evidence.path.reset();
    evidence.interned_path = &path;
    evidence.failure_scope = {true, TaskCongestionFailureScope::kDevice};
    evidence.controller_mode = {true, TaskCongestionControllerMode::kEnforce};
    evidence.window_bytes = {true, 128};
    evidence.inflight_bytes = {true, 64};
    evidence.retry_after_ns = {true, 500};
    ASSERT_TRUE(task.defer(0, evidence));
    const auto detail = task.detail();
    EXPECT_TRUE(detail.reason.observed);
    EXPECT_TRUE(detail.failure_scope.observed);
    EXPECT_TRUE(detail.affected_path.observed);
    EXPECT_TRUE(detail.observed_at_ns.observed);
    EXPECT_TRUE(detail.controller_mode.observed);
    EXPECT_TRUE(detail.controller_generation.observed);
    EXPECT_TRUE(detail.window_bytes.observed);
    EXPECT_TRUE(detail.inflight_bytes.observed);
    EXPECT_TRUE(detail.retry_after_ns.observed);
    EXPECT_EQ(detail.reason.value, evidence.reason.value);
    EXPECT_EQ(detail.failure_scope.value, evidence.failure_scope.value);
    EXPECT_EQ(detail.affected_path.value, path);
    EXPECT_EQ(detail.observed_at_ns.value, 100u);
    EXPECT_EQ(detail.controller_mode.value, evidence.controller_mode.value);
    EXPECT_EQ(detail.controller_generation.value, 7u);
    EXPECT_EQ(detail.window_bytes.value, 128u);
    EXPECT_EQ(detail.inflight_bytes.value, 64u);
    EXPECT_EQ(detail.retry_after_ns.value, 500u);
}

TEST(TaskCongestionObservationTest, TimestampOrdersActiveAndRetainedEvidence) {
    TaskCongestionObservation task(3);
    auto recent = windowEvidence();
    recent.observed_at_ns = {true, 200};
    ASSERT_TRUE(task.defer(0, recent));
    ASSERT_TRUE(task.defer(1, windowEvidence()));
    EXPECT_EQ(task.detail().slice_id.value, 0u);
    ASSERT_TRUE(task.resolve(0));
    EXPECT_EQ(task.detail().slice_id.value, 1u);
    ASSERT_TRUE(task.resolve(1));
    EXPECT_EQ(task.detail().slice_id.value, 0u);
    EXPECT_TRUE(task.detail().resolved);
}

TEST(TaskCongestionObservationTest, MissingTimestampsAndTiesUseLargerSliceId) {
    TaskCongestionObservation task(3);
    ASSERT_TRUE(task.defer(0, TaskCongestionEvidence{}));
    ASSERT_TRUE(task.defer(1, TaskCongestionEvidence{}));
    EXPECT_EQ(task.detail().slice_id.value, 1u);
    ASSERT_TRUE(task.resolve(1));
    ASSERT_TRUE(task.resolve(0));
    EXPECT_EQ(task.detail().slice_id.value, 1u);
    ASSERT_TRUE(task.defer(0, windowEvidence()));
    ASSERT_TRUE(task.defer(1, windowEvidence()));
    EXPECT_EQ(task.detail().slice_id.value, 1u);
    ASSERT_TRUE(task.resolve(1));
    ASSERT_TRUE(task.resolve(0));
    EXPECT_EQ(task.detail().slice_id.value, 1u);
    EXPECT_TRUE(task.detail().observed_at_ns.observed);
}

TEST(TaskCongestionObservationTest, ZeroValuedEvidenceRemainsObserved) {
    TaskCongestionObservation task(2);
    TaskCongestionEvidence evidence;
    evidence.reason = {true, TaskCongestionReason::kUnknown};
    evidence.failure_scope = {true, TaskCongestionFailureScope::kOperation};
    evidence.observed_at_ns = {true, 0};
    evidence.controller_mode = {true, TaskCongestionControllerMode::kOff};
    evidence.controller_generation = {true, 0};
    evidence.window_bytes = {true, 0};
    evidence.inflight_bytes = {true, 0};
    evidence.retry_after_ns = {true, 0};
    ASSERT_TRUE(task.defer(0, evidence));
    ASSERT_TRUE(task.defer(1, TaskCongestionEvidence{}));
    const auto detail = task.detail();
    EXPECT_EQ(detail.slice_id.value, 0u);
    EXPECT_TRUE(detail.reason.observed);
    EXPECT_EQ(detail.reason.value, TaskCongestionReason::kUnknown);
    EXPECT_TRUE(detail.failure_scope.observed);
    EXPECT_EQ(detail.failure_scope.value,
              TaskCongestionFailureScope::kOperation);
    EXPECT_TRUE(detail.observed_at_ns.observed);
    EXPECT_EQ(detail.observed_at_ns.value, 0u);
    EXPECT_TRUE(detail.controller_mode.observed);
    EXPECT_EQ(detail.controller_mode.value, TaskCongestionControllerMode::kOff);
    EXPECT_TRUE(detail.controller_generation.observed);
    EXPECT_EQ(detail.controller_generation.value, 0u);
    EXPECT_TRUE(detail.window_bytes.observed);
    EXPECT_EQ(detail.window_bytes.value, 0u);
    EXPECT_TRUE(detail.inflight_bytes.observed);
    EXPECT_EQ(detail.inflight_bytes.value, 0u);
    EXPECT_TRUE(detail.retry_after_ns.observed);
    EXPECT_EQ(detail.retry_after_ns.value, 0u);
    EXPECT_FALSE(detail.affected_path.observed);
}

TEST(TaskCongestionObservationTest,
     EmptyPathIsObservedAndConstReferenceIsOwned) {
    TaskCongestionObservation task(1);
    ASSERT_TRUE(task.defer(0, TaskCongestionEvidence{}));
    EXPECT_FALSE(task.detail().affected_path.observed);
    {
        TaskCongestionEvidence empty;
        empty.path = std::string{};
        ASSERT_TRUE(task.defer(0, empty));
    }
    ASSERT_TRUE(task.detail().affected_path.observed);
    EXPECT_TRUE(task.detail().affected_path.value.empty());
    {
        TaskCongestionEvidence temporary;
        temporary.path = std::string(256, 'a');
        ASSERT_TRUE(task.defer(0, temporary));
    }
    ASSERT_TRUE(task.resolve(0));
    const auto detail = task.detail();
    ASSERT_TRUE(detail.affected_path.observed);
    EXPECT_EQ(detail.affected_path.value, std::string(256, 'a'));
    EXPECT_TRUE(detail.resolved);
}

TEST(TaskCongestionObservationTest,
     ConcurrentSlicesAndQueriesRemainConsistent) {
    TaskCongestionObservation task(2);
    auto record = [&task](size_t slice) {
        for (size_t i = 0; i < 100; ++i) {
            EXPECT_TRUE(task.defer(slice, windowEvidence()));
            EXPECT_TRUE(task.resolve(slice));
            task.detail();
            task.state();
        }
    };
    std::thread first(record, 0);
    std::thread second(record, 1);
    first.join();
    second.join();
    EXPECT_EQ(task.state(), TaskCongestionState::kNormal);
    EXPECT_TRUE(task.detail().resolved);
}

TEST(TaskCongestionObservationTest, OwnedEvidenceGrowsSliceExtent) {
    TaskCongestionObservation task(1);
    ASSERT_TRUE(task.defer(2, windowEvidence(), 3));
    EXPECT_EQ(task.state(), TaskCongestionState::kCongested);
    ASSERT_TRUE(task.resolve(2));
    ASSERT_TRUE(task.avoid(2, windowEvidence(), 1));
    EXPECT_EQ(task.state(), TaskCongestionState::kLongUnavailable);
    EXPECT_EQ(task.detail().slice_id.value, 2u);
    ASSERT_TRUE(task.resolve(2));
    EXPECT_TRUE(task.detail().resolved);
    EXPECT_FALSE(task.avoid(3, windowEvidence()));
    EXPECT_EQ(task.state(), TaskCongestionState::kUnknown);
}

TEST(TaskCongestionObservationTest,
     SlicesOnDifferentControllerGenerationsShareAttempt) {
    TaskCongestionObservation task(2);
    EXPECT_TRUE(task.defer(0, windowEvidence()));

    TaskCongestionEvidence isolation;
    isolation.reason = {true, TaskCongestionReason::kPathQuarantined};
    isolation.path = "other-device";
    isolation.controller_generation = {true, 2};
    EXPECT_TRUE(task.avoid(1, isolation));
    EXPECT_EQ(task.state(), TaskCongestionState::kLongUnavailable);
    const auto detail = task.detail();
    ASSERT_TRUE(detail.controller_generation.observed);
    EXPECT_EQ(detail.controller_generation.value, 2);
    EXPECT_TRUE(task.detail().attempt_id.observed);
    EXPECT_EQ(task.detail().attempt_id.value, 1u);
}

}  // namespace
}  // namespace mooncake::adaptive_congestion_control

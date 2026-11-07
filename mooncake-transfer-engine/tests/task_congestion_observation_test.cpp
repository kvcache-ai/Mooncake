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

TEST(TaskCongestionObservationTest, HealthyTaskHasNoFailureEvidence) {
    TaskCongestionObservation task(2, 1);
    EXPECT_EQ(task.state(), TaskCongestionState::kNormal);
    const auto detail = task.detail();
    EXPECT_FALSE(detail.reason.observed);
    EXPECT_FALSE(detail.retry_after_ns.observed);
    EXPECT_FALSE(detail.failure_scope.observed);
    EXPECT_FALSE(detail.controller_generation.observed);
}

TEST(TaskCongestionObservationTest, RepeatedEventAfterRecoveryIsUnresolved) {
    TaskCongestionObservation task(1, 1);
    const auto evidence = windowEvidence();
    const auto original = evidence;
    ASSERT_TRUE(task.defer(0, 1, evidence));
    ASSERT_TRUE(task.resolve(0, 1));
    ASSERT_TRUE(task.detail().resolved);

    ASSERT_TRUE(task.defer(0, 1, evidence));
    const auto detail = task.detail();
    EXPECT_EQ(task.state(), TaskCongestionState::kCongested);
    EXPECT_FALSE(detail.resolved);
    ASSERT_TRUE(detail.affected_path.observed);
    EXPECT_EQ(detail.affected_path.value, "route-a");
    EXPECT_EQ(evidence, original);
}

TEST(TaskCongestionObservationTest, NewEvidenceClearsAbsentOptionalFields) {
    TaskCongestionObservation task(1, 1);
    auto previous = windowEvidence();
    previous.failure_scope = TaskCongestionFailureScope::kDevice;
    previous.controller_mode = TaskCongestionControllerMode::kEnforce;
    previous.window_bytes = 128;
    previous.inflight_bytes = 64;
    previous.retry_after_ns = 500;
    ASSERT_TRUE(task.avoid(0, 1, previous));
    ASSERT_TRUE(task.resolve(0, 1));

    TaskCongestionEvidence next;
    next.reason = TaskCongestionReason::kFatal;
    next.path = "x";
    const auto original = next;
    ASSERT_TRUE(task.avoid(0, 1, next));
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
    EXPECT_EQ(next, original);

    ASSERT_TRUE(task.resolve(0, 1));
    ASSERT_TRUE(task.defer(0, 1, TaskCongestionEvidence{}));
    const auto empty = task.detail();
    EXPECT_FALSE(empty.reason.observed);
    EXPECT_FALSE(empty.affected_path.observed);
    EXPECT_FALSE(empty.resolved);
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

TEST(TaskCongestionObservationTest,
     EffectivePathDeduplicatesOwnedAndReference) {
    static const std::string path = "route-a";
    auto owned = windowEvidence();
    auto referenced = owned;
    referenced.path.reset();
    referenced.interned_path = &path;
    for (bool reference_first : {false, true}) {
        TaskCongestionObservation task(2, 1);
        ASSERT_TRUE(task.defer(0, 1, reference_first ? referenced : owned));
        ASSERT_TRUE(task.defer(1, 1, windowEvidence()));
        ASSERT_TRUE(task.defer(0, 1, reference_first ? owned : referenced));
        ASSERT_TRUE(task.resolve(0, 1));
        ASSERT_TRUE(task.resolve(1, 1));
        const auto detail = task.detail();
        ASSERT_TRUE(detail.slice_id.observed);
        EXPECT_EQ(detail.slice_id.value, 1u);
        EXPECT_TRUE(detail.resolved);
    }
}

TEST(TaskCongestionObservationTest, EveryEvidenceFieldChangesTheLastEvent) {
    auto base = windowEvidence();
    base.failure_scope = TaskCongestionFailureScope::kDevice;
    base.controller_mode = TaskCongestionControllerMode::kEnforce;
    base.window_bytes = 128;
    base.inflight_bytes = 64;
    base.retry_after_ns = 500;
    std::vector<TaskCongestionEvidence> changed(9, base);
    changed[0].reason.reset();
    changed[1].failure_scope.reset();
    changed[2].path.reset();
    changed[3].observed_at_ns.reset();
    changed[4].controller_mode.reset();
    changed[5].controller_generation.reset();
    changed[6].window_bytes.reset();
    changed[7].inflight_bytes.reset();
    changed[8].retry_after_ns.reset();
    for (size_t field = 0; field < changed.size(); ++field) {
        SCOPED_TRACE(field);
        TaskCongestionObservation task(2, 1);
        ASSERT_TRUE(task.defer(0, 1, base));
        ASSERT_TRUE(task.defer(1, 1, base));
        ASSERT_TRUE(task.defer(0, 1, changed[field]));
        ASSERT_TRUE(task.resolve(0, 1));
        ASSERT_TRUE(task.resolve(1, 1));
        const auto detail = task.detail();
        ASSERT_TRUE(detail.slice_id.observed);
        EXPECT_EQ(detail.slice_id.value, 0u);
    }
}

TEST(TaskCongestionObservationTest,
     EmptyPathIsObservedAndConstReferenceIsOwned) {
    TaskCongestionObservation task(1, 1);
    ASSERT_TRUE(task.defer(0, 1, TaskCongestionEvidence{}));
    EXPECT_FALSE(task.detail().affected_path.observed);
    {
        TaskCongestionEvidence empty;
        empty.path = std::string{};
        ASSERT_TRUE(task.defer(0, 1, empty));
    }
    ASSERT_TRUE(task.detail().affected_path.observed);
    EXPECT_TRUE(task.detail().affected_path.value.empty());
    {
        TaskCongestionEvidence temporary;
        temporary.path = std::string(256, 'a');
        ASSERT_TRUE(task.defer(0, 1, temporary));
    }
    ASSERT_TRUE(task.resolve(0, 1));
    const auto detail = task.detail();
    ASSERT_TRUE(detail.affected_path.observed);
    EXPECT_EQ(detail.affected_path.value, std::string(256, 'a'));
    EXPECT_TRUE(detail.resolved);
}

TEST(TaskCongestionObservationTest, OldAttemptCannotOverrideCurrentState) {
    TaskCongestionObservation task(2, 2);
    EXPECT_EQ(task.state(), TaskCongestionState::kNormal);

    EXPECT_FALSE(task.defer(0, 1, windowEvidence()));
    EXPECT_FALSE(task.avoid(0, 1, windowEvidence()));
    EXPECT_FALSE(task.extendAttempt(1, 3));
    EXPECT_EQ(task.state(), TaskCongestionState::kNormal);

    EXPECT_TRUE(task.avoid(1, 2, windowEvidence()));
    EXPECT_EQ(task.state(), TaskCongestionState::kLongUnavailable);
    EXPECT_FALSE(task.resolve(1, 1));
    EXPECT_EQ(task.state(), TaskCongestionState::kLongUnavailable);

    EXPECT_FALSE(task.avoid(2, 2, windowEvidence()));
    EXPECT_EQ(task.state(), TaskCongestionState::kUnknown);
}

TEST(TaskCongestionObservationTest, FutureAttemptCannotOverrideCurrentState) {
    TaskCongestionObservation task(1, 1);
    EXPECT_TRUE(task.defer(0, 1, windowEvidence()));
    EXPECT_FALSE(task.extendAttempt(2, 2));
    EXPECT_FALSE(task.defer(1, 2, windowEvidence()));
    EXPECT_FALSE(task.avoid(0, 2, windowEvidence()));
    EXPECT_FALSE(task.resolve(0, 2));
    EXPECT_EQ(task.state(), TaskCongestionState::kCongested);
    const auto detail = task.detail();
    ASSERT_TRUE(detail.slice_id.observed);
    EXPECT_EQ(detail.slice_id.value, 0);
    ASSERT_TRUE(detail.attempt_id.observed);
    EXPECT_EQ(detail.attempt_id.value, 1);
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

TEST(TaskCongestionObservationTest, OwnedEvidenceExtendsOnlyCurrentAttempt) {
    TaskCongestionObservation task(1, 2);
    EXPECT_FALSE(task.defer(2, 1, windowEvidence(), 3));
    EXPECT_FALSE(task.avoid(2, 3, windowEvidence(), 3));
    EXPECT_EQ(task.state(), TaskCongestionState::kNormal);
    EXPECT_FALSE(task.detail().reason.observed);

    ASSERT_TRUE(task.defer(2, 2, windowEvidence(), 3));
    EXPECT_EQ(task.state(), TaskCongestionState::kCongested);
    ASSERT_TRUE(task.resolve(2, 2));
    ASSERT_TRUE(task.avoid(2, 2, windowEvidence(), 1));
    EXPECT_EQ(task.state(), TaskCongestionState::kLongUnavailable);
    const auto detail = task.detail();
    ASSERT_TRUE(detail.slice_id.observed);
    EXPECT_EQ(detail.slice_id.value, 2u);
    EXPECT_FALSE(detail.resolved);
    ASSERT_TRUE(task.resolve(2, 2));
    EXPECT_EQ(task.state(), TaskCongestionState::kNormal);
    EXPECT_TRUE(task.detail().resolved);
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

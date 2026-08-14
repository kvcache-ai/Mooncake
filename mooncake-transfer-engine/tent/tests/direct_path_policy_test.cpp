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

#include "tent/runtime/direct_path_policy.h"

namespace mooncake {
namespace tent {
namespace {

Request makeRequest(size_t length, IntentType intent, uint64_t deadline_ns = 0) {
    Request request{};
    request.length = length;
    request.intent_type = intent;
    request.deadline_ns = deadline_ns;
    return request;
}

TEST(DirectPathPolicyTest, ForegroundGetPriorityIsHigh) {
    EXPECT_EQ(DirectPathPolicy::priorityForIntent(IntentType::FOREGROUND_GET),
              PRIO_HIGH);
}

TEST(DirectPathPolicyTest, BackgroundIntentPrioritiesAreLow) {
    for (const auto intent : {IntentType::BACKGROUND_PREFETCH,
                             IntentType::MIGRATION,
                             IntentType::CHECKPOINT}) {
        EXPECT_EQ(DirectPathPolicy::priorityForIntent(intent), PRIO_LOW);
    }
}

TEST(DirectPathPolicyTest, WeightLoadingPriorityIsMedium) {
    EXPECT_EQ(DirectPathPolicy::priorityForIntent(IntentType::WEIGHT_LOADING),
              PRIO_MEDIUM);
}

TEST(DirectPathPolicyTest, StagingInternalPriorityIsHigh) {
    EXPECT_EQ(DirectPathPolicy::priorityForIntent(IntentType::STAGING_INTERNAL),
              PRIO_HIGH);
}

TEST(DirectPathPolicyTest, UnspecPriorityDefaultsToHigh) {
    auto request = makeRequest(4096, IntentType::INTENT_UNSPEC);
    EXPECT_EQ(request.priority, PRIO_UNSPEC);
    EXPECT_EQ(DirectPathPolicy::priorityForRequest(request), PRIO_HIGH);
}

TEST(DirectPathPolicyTest, ExplicitPriorityIsPreserved) {
    auto request = makeRequest(4096, IntentType::FOREGROUND_GET, 12345);
    request.priority = PRIO_LOW;
    EXPECT_EQ(DirectPathPolicy::priorityForRequest(request), PRIO_LOW);
}

TEST(DirectPathPolicyTest, UnspecPriorityWithDeadlineBecomesHigh) {
    auto request = makeRequest(4096, IntentType::MIGRATION, 12345);
    EXPECT_EQ(DirectPathPolicy::priorityForRequest(request), PRIO_HIGH);

    request = makeRequest(4096, IntentType::MIGRATION);
    EXPECT_EQ(DirectPathPolicy::priorityForRequest(request), PRIO_LOW);
}

TEST(DirectPathPolicyTest, AutoUsesDirectForSmallLatencyRequests) {
    auto request = makeRequest(DirectPathPolicy::kDirectPathSmallRequestMaxBytes,
                               IntentType::FOREGROUND_GET);
    EXPECT_EQ(DirectPathPolicy::decideAuto(request),
              DirectPathDecision::TryDirectPath);

    request = makeRequest(4096, IntentType::INTENT_UNSPEC, 12345);
    EXPECT_EQ(DirectPathPolicy::decideAuto(request),
              DirectPathDecision::TryDirectPath);
}

TEST(DirectPathPolicyTest, AutoKeepsLargeRequestsOnScheduledPath) {
    auto request =
        makeRequest(DirectPathPolicy::kDirectPathLargeRequestMinBytes,
                    IntentType::FOREGROUND_GET);
    EXPECT_EQ(DirectPathPolicy::decideAuto(request),
              DirectPathDecision::UseScheduledPath);
}

TEST(DirectPathPolicyTest, AutoKeepsThroughputIntentsOnScheduledPath) {
    auto request = makeRequest(4096, IntentType::BACKGROUND_PREFETCH, 12345);
    EXPECT_EQ(DirectPathPolicy::decideAuto(request),
              DirectPathDecision::UseScheduledPath);

    request = makeRequest(4096, IntentType::MIGRATION);
    EXPECT_EQ(DirectPathPolicy::decideAuto(request),
              DirectPathDecision::UseScheduledPath);
}

TEST(DirectPathPolicyTest, AutoDoesNotUseDirectWithoutLatencySignal) {
    auto request = makeRequest(4096, IntentType::INTENT_UNSPEC);
    EXPECT_EQ(DirectPathPolicy::decideAuto(request),
              DirectPathDecision::UseScheduledPath);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake

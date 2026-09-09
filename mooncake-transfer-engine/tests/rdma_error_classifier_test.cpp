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

#include "rdma_error_classifier.h"

namespace mooncake::adaptive_cc {
namespace {

void expectCompletion(ibv_wc_status status, OutcomeClass outcome,
                      FailureScope scope, bool root_failure) {
    const Classification result = classifyCompletion(status, 17);
    EXPECT_EQ(result.outcome, outcome);
    EXPECT_EQ(result.scope, scope);
    EXPECT_EQ(result.root_failure, root_failure);
    EXPECT_EQ(result.vendor_error, 17u);
}

TEST(RdmaErrorClassifierTest, ClassifiesCompletionStatus) {
    expectCompletion(IBV_WC_SUCCESS, OutcomeClass::kSuccess,
                     FailureScope::kOperation, false);
    expectCompletion(IBV_WC_RNR_RETRY_EXC_ERR, OutcomeClass::kReceiverPressure,
                     FailureScope::kQp, true);
    expectCompletion(IBV_WC_RETRY_EXC_ERR, OutcomeClass::kRouteTimeout,
                     FailureScope::kQp, true);
    expectCompletion(IBV_WC_RESP_TIMEOUT_ERR, OutcomeClass::kRouteTimeout,
                     FailureScope::kQp, true);
    expectCompletion(IBV_WC_LOC_PROT_ERR, OutcomeClass::kLocalConfiguration,
                     FailureScope::kOperation, true);
    expectCompletion(IBV_WC_LOC_QP_OP_ERR, OutcomeClass::kLocalConfiguration,
                     FailureScope::kQp, true);
    expectCompletion(IBV_WC_REM_ACCESS_ERR, OutcomeClass::kRemoteMetadata,
                     FailureScope::kOperation, true);
    expectCompletion(IBV_WC_WR_FLUSH_ERR, OutcomeClass::kDerivedFlush,
                     FailureScope::kQp, false);
    expectCompletion(IBV_WC_FATAL_ERR, OutcomeClass::kFatal, FailureScope::kQp,
                     true);
    expectCompletion(IBV_WC_GENERAL_ERR, OutcomeClass::kFatal,
                     FailureScope::kQp, true);
    expectCompletion(static_cast<ibv_wc_status>(255), OutcomeClass::kFatal,
                     FailureScope::kQp, true);
}

void expectAsync(ibv_event_type event, FailureScope scope) {
    const auto result = classifyAsyncEvent(event);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->outcome, OutcomeClass::kFatal);
    EXPECT_EQ(result->scope, scope);
    EXPECT_TRUE(result->root_failure);
    EXPECT_EQ(result->vendor_error, 0u);
}

TEST(RdmaErrorClassifierTest, PreservesAsyncFailureScope) {
    expectAsync(IBV_EVENT_QP_FATAL, FailureScope::kQp);
    expectAsync(IBV_EVENT_QP_REQ_ERR, FailureScope::kQp);
    expectAsync(IBV_EVENT_QP_ACCESS_ERR, FailureScope::kQp);
    expectAsync(IBV_EVENT_CQ_ERR, FailureScope::kCq);
    expectAsync(IBV_EVENT_PORT_ERR, FailureScope::kPort);
    expectAsync(IBV_EVENT_DEVICE_FATAL, FailureScope::kDevice);
}

TEST(RdmaErrorClassifierTest, IgnoresRecoveryAndTopologyEvents) {
    EXPECT_FALSE(classifyAsyncEvent(IBV_EVENT_PORT_ACTIVE).has_value());
    EXPECT_FALSE(classifyAsyncEvent(IBV_EVENT_GID_CHANGE).has_value());
    EXPECT_FALSE(classifyAsyncEvent(IBV_EVENT_LID_CHANGE).has_value());
    EXPECT_FALSE(classifyAsyncEvent(IBV_EVENT_PKEY_CHANGE).has_value());
}

}  // namespace
}  // namespace mooncake::adaptive_cc

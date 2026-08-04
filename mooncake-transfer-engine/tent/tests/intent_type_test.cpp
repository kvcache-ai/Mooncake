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
//
// Unit tests for IntentType enum and its integration with Request.

#include <gtest/gtest.h>

#include <vector>

#include "tent/common/types.h"

namespace mooncake {
namespace tent {
namespace {

TEST(IntentTypeTest, DefaultIsUnspec) {
    Request r{};
    EXPECT_EQ(r.intent_type, IntentType::INTENT_UNSPEC);
}

TEST(IntentTypeTest, AllValuesAssignable) {
    Request r{};
    r.intent_type = IntentType::FOREGROUND_GET;
    EXPECT_EQ(r.intent_type, IntentType::FOREGROUND_GET);
    r.intent_type = IntentType::BACKGROUND_PREFETCH;
    EXPECT_EQ(r.intent_type, IntentType::BACKGROUND_PREFETCH);
    r.intent_type = IntentType::MIGRATION;
    EXPECT_EQ(r.intent_type, IntentType::MIGRATION);
    r.intent_type = IntentType::CHECKPOINT;
    EXPECT_EQ(r.intent_type, IntentType::CHECKPOINT);
    r.intent_type = IntentType::WEIGHT_LOADING;
    EXPECT_EQ(r.intent_type, IntentType::WEIGHT_LOADING);
    r.intent_type = IntentType::STAGING_INTERNAL;
    EXPECT_EQ(r.intent_type, IntentType::STAGING_INTERNAL);
}

TEST(IntentTypeTest, IntegerValues) {
    EXPECT_EQ(static_cast<int>(IntentType::INTENT_UNSPEC), 0);
    EXPECT_EQ(static_cast<int>(IntentType::FOREGROUND_GET), 1);
    EXPECT_EQ(static_cast<int>(IntentType::BACKGROUND_PREFETCH), 2);
    EXPECT_EQ(static_cast<int>(IntentType::MIGRATION), 3);
    EXPECT_EQ(static_cast<int>(IntentType::CHECKPOINT), 4);
    EXPECT_EQ(static_cast<int>(IntentType::WEIGHT_LOADING), 5);
    EXPECT_EQ(static_cast<int>(IntentType::STAGING_INTERNAL), 6);
}

TEST(IntentTypeTest, DoesNotAffectOtherFields) {
    Request r{};
    r.opcode = Request::READ;
    r.priority = PRIO_LOW;
    r.deadline_ns = 12345;
    r.transport_hint = RDMA;
    r.intent_type = IntentType::CHECKPOINT;

    EXPECT_EQ(r.opcode, Request::READ);
    EXPECT_EQ(r.priority, PRIO_LOW);
    EXPECT_EQ(r.deadline_ns, 12345u);
    EXPECT_EQ(r.transport_hint, RDMA);
    EXPECT_EQ(r.intent_type, IntentType::CHECKPOINT);
}

TEST(IntentTypeTest, CopyPreservesIntentType) {
    Request r{};
    r.intent_type = IntentType::WEIGHT_LOADING;
    Request copy = r;
    EXPECT_EQ(copy.intent_type, IntentType::WEIGHT_LOADING);
}

TEST(IntentTypeTest, VectorOfRequests) {
    std::vector<Request> batch(4);
    batch[0].intent_type = IntentType::FOREGROUND_GET;
    batch[1].intent_type = IntentType::BACKGROUND_PREFETCH;
    batch[2].intent_type = IntentType::MIGRATION;
    batch[3].intent_type = IntentType::INTENT_UNSPEC;

    EXPECT_EQ(batch[0].intent_type, IntentType::FOREGROUND_GET);
    EXPECT_EQ(batch[1].intent_type, IntentType::BACKGROUND_PREFETCH);
    EXPECT_EQ(batch[2].intent_type, IntentType::MIGRATION);
    EXPECT_EQ(batch[3].intent_type, IntentType::INTENT_UNSPEC);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake

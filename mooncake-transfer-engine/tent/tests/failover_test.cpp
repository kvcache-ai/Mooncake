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

#include <memory>
#include <set>
#include <string>

#include "tent/common/config.h"
#include "tent/common/types.h"
#include "tent/runtime/transfer_engine_impl.h"

namespace mooncake {
namespace tent {
namespace {

// ---------------------------------------------------------------------------
// TaskInfo failover_count field tests
// ---------------------------------------------------------------------------

TEST(TaskInfoTest, DefaultFailoverCount) {
    TaskInfo task;
    EXPECT_EQ(task.failover_count, 0);
}

TEST(TaskInfoTest, FailoverCountIncrement) {
    TaskInfo task;
    ++task.failover_count;
    EXPECT_EQ(task.failover_count, 1);
    ++task.failover_count;
    ++task.failover_count;
    EXPECT_EQ(task.failover_count, 3);
}

TEST(TaskInfoTest, FailoverCountLimitCheck) {
    // Simulate the limit check pattern from resubmitTransferTask
    constexpr int kMaxAttempts = 3;
    TaskInfo task;

    // Attempts 1..3 should pass
    for (int i = 0; i < kMaxAttempts; ++i) {
        ++task.failover_count;
        EXPECT_LE(task.failover_count, kMaxAttempts)
            << "Attempt " << task.failover_count << " should be within limit";
    }

    // Attempt 4 should exceed
    ++task.failover_count;
    EXPECT_GT(task.failover_count, kMaxAttempts);
}

// ---------------------------------------------------------------------------
// TaskInfo preserves other fields after failover-related mutations
// ---------------------------------------------------------------------------

TEST(TaskInfoTest, FailoverFieldsIndependent) {
    TaskInfo task;
    task.type = RDMA;
    task.xport_priority = 0;
    task.status = TransferStatusEnum::PENDING;
    task.failover_count = 0;

    // Simulate failover: increment priority and failover_count, change type
    task.xport_priority++;
    task.failover_count++;
    task.type = TCP;
    task.status = TransferStatusEnum::PENDING;  // Reset to PENDING

    EXPECT_EQ(task.xport_priority, 1);
    EXPECT_EQ(task.failover_count, 1);
    EXPECT_EQ(task.type, TCP);
    EXPECT_EQ(task.status, TransferStatusEnum::PENDING);
}

// ---------------------------------------------------------------------------
// Config loading for max_failover_attempts
// ---------------------------------------------------------------------------

TEST(FailoverConfigTest, DefaultValue) {
    auto conf = std::make_shared<Config>();
    // When not set, should return the default
    EXPECT_EQ(conf->get("max_failover_attempts", 3), 3);
}

TEST(FailoverConfigTest, CustomValue) {
    auto conf = std::make_shared<Config>();
    conf->set("max_failover_attempts", 5);
    EXPECT_EQ(conf->get("max_failover_attempts", 3), 5);
}

TEST(FailoverConfigTest, ZeroDisablesFailover) {
    auto conf = std::make_shared<Config>();
    conf->set("max_failover_attempts", 0);
    int max_attempts = conf->get("max_failover_attempts", 3);
    EXPECT_EQ(max_attempts, 0);

    // With max=0, the very first ++failover_count > 0 check should fail
    TaskInfo task;
    ++task.failover_count;
    EXPECT_GT(task.failover_count, max_attempts);
}

// ---------------------------------------------------------------------------
// TransportType name coverage (tests the static helper indirectly via
// the enum values — the function itself is file-local in the .cpp, so we
// verify the enum values are well-defined and usable in switch)
// ---------------------------------------------------------------------------

TEST(TransportTypeTest, AllEnumValuesDistinct) {
    // Verify no accidental collisions in the enum
    std::set<int> seen;
    TransportType types[] = {RDMA,    MNNVL, SHM,          NVLINK, GDS,
                             IOURING, TCP,   AscendDirect, UNSPEC};
    for (auto t : types) {
        EXPECT_TRUE(seen.insert(static_cast<int>(t)).second)
            << "Duplicate TransportType value: " << static_cast<int>(t);
    }
}

TEST(TransportTypeTest, SupportedCount) {
    // kSupportedTransportTypes should cover all types except UNSPEC
    EXPECT_EQ(kSupportedTransportTypes, 8u);
    EXPECT_EQ(static_cast<int>(UNSPEC), 8);
}

// ---------------------------------------------------------------------------
// Failover state machine simulation (no real transport needed)
// ---------------------------------------------------------------------------

TEST(FailoverStateMachineTest, SimulateFullFailoverSequence) {
    // Simulate the sequence: submit on RDMA → fail → failover to TCP → succeed
    TaskInfo task;
    task.type = RDMA;
    task.xport_priority = 0;
    task.status = TransferStatusEnum::PENDING;
    task.failover_count = 0;

    // Step 1: RDMA reports FAILED
    task.status = TransferStatusEnum::FAILED;
    EXPECT_EQ(task.status, TransferStatusEnum::FAILED);

    // Step 2: resubmitTransferTask logic (simulated)
    constexpr int kMaxAttempts = 3;
    ++task.failover_count;
    EXPECT_LE(task.failover_count, kMaxAttempts);  // Within limit

    task.xport_priority++;
    // Simulate resolveTransport returning TCP
    task.type = TCP;
    task.status = TransferStatusEnum::PENDING;

    EXPECT_EQ(task.type, TCP);
    EXPECT_EQ(task.xport_priority, 1);
    EXPECT_EQ(task.failover_count, 1);
    EXPECT_EQ(task.status, TransferStatusEnum::PENDING);

    // Step 3: TCP succeeds
    task.status = TransferStatusEnum::COMPLETED;
    EXPECT_EQ(task.status, TransferStatusEnum::COMPLETED);
}

TEST(FailoverStateMachineTest, ExhaustAllTransports) {
    // Simulate exhausting all failover attempts
    constexpr int kMaxAttempts = 3;
    TaskInfo task;
    task.type = RDMA;
    task.xport_priority = 0;

    for (int i = 0; i < kMaxAttempts; ++i) {
        task.status = TransferStatusEnum::FAILED;
        ++task.failover_count;
        EXPECT_LE(task.failover_count, kMaxAttempts);
        task.xport_priority++;
        task.status = TransferStatusEnum::PENDING;
    }

    // Next failure should exceed the limit
    task.status = TransferStatusEnum::FAILED;
    ++task.failover_count;
    EXPECT_GT(task.failover_count, kMaxAttempts);

    // Task stays FAILED — no more failover
    EXPECT_EQ(task.status, TransferStatusEnum::FAILED);
    EXPECT_EQ(task.failover_count, kMaxAttempts + 1);
}

TEST(FailoverStateMachineTest, StagingBypassesPriorityIncrement) {
    // When task.staging is true, xport_priority should NOT increment
    // (mirroring resubmitTransferTask logic)
    TaskInfo task;
    task.type = TCP;
    task.xport_priority = 0;
    task.staging = true;

    // Simulate resubmit logic
    if (task.staging)
        task.staging = false;
    else
        task.xport_priority++;

    EXPECT_FALSE(task.staging);
    EXPECT_EQ(task.xport_priority, 0);  // Should NOT have incremented
}

}  // namespace
}  // namespace tent
}  // namespace mooncake

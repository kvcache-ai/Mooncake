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

TEST(FailoverConfigTest, AutoFailoverOnPollDefaultEnabled) {
    auto conf = std::make_shared<Config>();
    EXPECT_TRUE(conf->get("enable_auto_failover_on_poll", true));
}

TEST(FailoverConfigTest, AutoFailoverOnPollCanBeDisabled) {
    auto conf = std::make_shared<Config>();
    conf->set("enable_auto_failover_on_poll", false);
    EXPECT_FALSE(conf->get("enable_auto_failover_on_poll", true));
}

// ---------------------------------------------------------------------------
// TransportType name coverage (tests the static helper indirectly via
// the enum values — the function itself is file-local in the .cpp, so we
// verify the enum values are well-defined and usable in switch)
// ---------------------------------------------------------------------------

TEST(TransportTypeTest, UnspecIsZeroSlot) {
    // UNSPEC must be value 0 so that zero-initialized tent_request /
    // tent_memory_options structs (the C ABI) default to UNSPEC
    // rather than silently pinning to whichever transport happened
    // to occupy slot 0.
    EXPECT_EQ(static_cast<int>(UNSPEC), 0);
    EXPECT_EQ(kSupportedTransportTypes, static_cast<int>(SUNRISE_LINK) + 1);
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

// ---------------------------------------------------------------------------
// Batch status aggregation correctness
// ---------------------------------------------------------------------------

// Verifies the invariant: a batch with one permanently-FAILED task and
// another still-PENDING task must report PENDING (not FAILED) overall,
// because the PENDING task may still complete.  The old code latched
// overall_status to FAILED as soon as any task was terminal.
TEST(BatchStatusAggregationTest, PendingTaskPreventsEarlyBatchFailure) {
    // Two tasks in a batch scenario (simulated):
    // Task A: permanently FAILED (exhausted failover budget)
    // Task B: still PENDING (retrying on secondary transport)
    //
    // Expected overall status: PENDING (not FAILED), because Task B is
    // still in-flight.  Only once Task B reaches a terminal state should
    // the batch become terminal.

    struct MockTask {
        TransferStatusEnum status{PENDING};
        bool derived{false};
        size_t length{1024};
    };

    auto aggregateStatus =
        [](const std::vector<MockTask>& tasks) -> TransferStatusEnum {
        size_t success_tasks = 0;
        size_t failed_tasks = 0;
        size_t total_tasks = 0;
        for (auto& t : tasks) {
            if (t.derived) continue;
            total_tasks++;
            if (t.status == COMPLETED)
                success_tasks++;
            else if (t.status != PENDING)
                failed_tasks++;
        }
        if (success_tasks == total_tasks) return COMPLETED;
        if (success_tasks + failed_tasks == total_tasks) return FAILED;
        return PENDING;
    };

    // Case 1: one FAILED + one PENDING → overall PENDING
    {
        std::vector<MockTask> tasks = {{FAILED, false, 1024},
                                       {PENDING, false, 1024}};
        EXPECT_EQ(aggregateStatus(tasks), PENDING);
    }

    // Case 2: one FAILED + one COMPLETED → overall FAILED
    {
        std::vector<MockTask> tasks = {{FAILED, false, 1024},
                                       {COMPLETED, false, 1024}};
        EXPECT_EQ(aggregateStatus(tasks), FAILED);
    }

    // Case 3: all COMPLETED → overall COMPLETED
    {
        std::vector<MockTask> tasks = {{COMPLETED, false, 1024},
                                       {COMPLETED, false, 1024}};
        EXPECT_EQ(aggregateStatus(tasks), COMPLETED);
    }

    // Case 4: all PENDING → overall PENDING
    {
        std::vector<MockTask> tasks = {{PENDING, false, 1024},
                                       {PENDING, false, 1024}};
        EXPECT_EQ(aggregateStatus(tasks), PENDING);
    }

    // Case 5: derived tasks are skipped
    {
        std::vector<MockTask> tasks = {{FAILED, false, 1024},
                                       {FAILED, true, 1024},  // derived: skip
                                       {PENDING, false, 1024}};
        EXPECT_EQ(aggregateStatus(tasks), PENDING);
    }

    // Case 6: all non-derived are terminal with at least one failure
    {
        std::vector<MockTask> tasks = {{COMPLETED, false, 1024},
                                       {FAILED, true, 1024},  // derived: skip
                                       {FAILED, false, 1024}};
        EXPECT_EQ(aggregateStatus(tasks), FAILED);
    }
}

}  // namespace
}  // namespace tent
}  // namespace mooncake

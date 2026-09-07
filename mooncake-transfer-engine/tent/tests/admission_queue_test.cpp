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

#include "tent/runtime/admission_queue.h"

#include <algorithm>
#include <atomic>
#include <memory>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

namespace mooncake {
namespace tent {
namespace {

QueueOwnerInput makeOwner(
    size_t public_task_id, size_t length,
    QueueOwnerKind kind = QueueOwnerKind::User,
    std::vector<size_t> derived_task_ids = std::vector<size_t>()) {
    QueueOwnerInput owner;
    owner.owner_task_id = public_task_id;
    owner.derived_task_ids = std::move(derived_task_ids);
    owner.request.opcode = Request::WRITE;
    owner.request.source = nullptr;
    owner.request.target_id = 1;
    owner.request.target_offset = public_task_id * 4096;
    owner.request.length = length;
    owner.kind = kind;
    return owner;
}

QueueSubmit makeSubmit(uint64_t batch_token, size_t batch_slots_left,
                       std::vector<QueueOwnerInput> owners) {
    QueueSubmit submit;
    submit.batch_token = batch_token;
    submit.batch_slots_left = batch_slots_left;
    submit.owners = std::move(owners);
    return submit;
}

TEST(AdmissionQueueTest, AllowsEmptySubmitAsNoOp) {
    LocalTransferAdmissionQueue queue({2, 128, 0, 0});
    std::vector<QueueOwnerId> admitted_ids{99};

    auto status = queue.tryAdmit(makeSubmit(1, 0, {}), admitted_ids);

    EXPECT_EQ(status.code(), Status::Code::kOk);
    EXPECT_TRUE(admitted_ids.empty());
    EXPECT_EQ(queue.outstandingOwners(), 0u);
    EXPECT_EQ(queue.outstandingBytes(), 0u);
}

TEST(AdmissionQueueTest, RejectsSubmitWhenQueueLimitsAreInvalid) {
    LocalTransferAdmissionQueue queue({1, 128, 2, 0});
    std::vector<QueueOwnerId> admitted_ids{99};

    auto status =
        queue.tryAdmit(makeSubmit(1, 1, {makeOwner(0, 16)}), admitted_ids);

    EXPECT_EQ(status.code(), Status::Code::kInvalidArgument);
    EXPECT_TRUE(admitted_ids.empty());
    EXPECT_EQ(queue.outstandingOwners(), 0u);
    EXPECT_EQ(queue.outstandingBytes(), 0u);
}

TEST(AdmissionQueueTest, RejectsInvalidInputsWithoutPartialAdmission) {
    LocalTransferAdmissionQueue queue({4, 128, 0, 0});
    std::vector<QueueOwnerId> admitted_ids{99};

    auto status = queue.tryAdmit(
        makeSubmit(
            1, 2,
            {makeOwner(0, 16, QueueOwnerKind::User, {1}), makeOwner(1, 16)}),
        admitted_ids);

    EXPECT_EQ(status.code(), Status::Code::kInvalidArgument);
    EXPECT_TRUE(admitted_ids.empty());
    EXPECT_EQ(queue.outstandingOwners(), 0u);
    EXPECT_EQ(queue.outstandingBytes(), 0u);

    status = queue.tryAdmit(makeSubmit(1, 1, {makeOwner(2, 16)}), admitted_ids);

    ASSERT_EQ(status.code(), Status::Code::kOk);
    ASSERT_EQ(admitted_ids.size(), 1u);
    EXPECT_EQ(admitted_ids[0], 1u);
}

TEST(AdmissionQueueTest, RejectsUnsupportedOwnerKindWithoutPartialAdmission) {
    LocalTransferAdmissionQueue queue({4, 128, 0, 0});
    std::vector<QueueOwnerId> admitted_ids{99};

    auto invalid_owner = makeOwner(0, 16, static_cast<QueueOwnerKind>(99), {1});
    auto status = queue.tryAdmit(makeSubmit(1, 2, {std::move(invalid_owner)}),
                                 admitted_ids);

    EXPECT_EQ(status.code(), Status::Code::kInvalidArgument);
    EXPECT_TRUE(admitted_ids.empty());
    EXPECT_EQ(queue.outstandingOwners(), 0u);
    EXPECT_EQ(queue.outstandingBytes(), 0u);

    status = queue.tryAdmit(makeSubmit(1, 1, {makeOwner(2, 16)}), admitted_ids);

    ASSERT_EQ(status.code(), Status::Code::kOk);
    ASSERT_EQ(admitted_ids.size(), 1u);
    EXPECT_EQ(admitted_ids[0], 1u);
}

TEST(AdmissionQueueTest, RejectsCapacityExceededWithoutPartialAdmission) {
    LocalTransferAdmissionQueue queue({1, 64, 0, 0});
    std::vector<QueueOwnerId> admitted_ids;

    auto status = queue.tryAdmit(
        makeSubmit(1, 2, {makeOwner(0, 16), makeOwner(1, 16)}), admitted_ids);

    EXPECT_EQ(status.code(), Status::Code::kTooManyRequests);
    EXPECT_TRUE(admitted_ids.empty());
    EXPECT_EQ(queue.outstandingOwners(), 0u);
    EXPECT_EQ(queue.outstandingBytes(), 0u);

    status = queue.tryAdmit(makeSubmit(1, 1, {makeOwner(0, 16)}), admitted_ids);

    ASSERT_EQ(status.code(), Status::Code::kOk);
    ASSERT_EQ(admitted_ids.size(), 1u);
    EXPECT_EQ(admitted_ids[0], 1u);
}

TEST(AdmissionQueueTest, RejectsExistingPublicTaskConflictWithoutMutation) {
    LocalTransferAdmissionQueue queue({4, 128, 0, 0});
    std::vector<QueueOwnerId> admitted_ids;

    auto status =
        queue.tryAdmit(makeSubmit(1, 1, {makeOwner(0, 16)}), admitted_ids);
    ASSERT_EQ(status.code(), Status::Code::kOk);
    ASSERT_EQ(admitted_ids.size(), 1u);
    EXPECT_EQ(admitted_ids[0], 1u);

    status = queue.tryAdmit(
        makeSubmit(1, 2, {makeOwner(1, 16), makeOwner(0, 16)}), admitted_ids);

    EXPECT_EQ(status.code(), Status::Code::kInvalidEntry);
    EXPECT_TRUE(admitted_ids.empty());
    EXPECT_EQ(queue.outstandingOwners(), 1u);
    EXPECT_EQ(queue.outstandingBytes(), 16u);

    QueueOwnerId owner_id = 0;
    status = queue.resolveOwner(1, 1, owner_id);
    EXPECT_EQ(status.code(), Status::Code::kInvalidEntry);

    auto picked = queue.pickForDispatch(1, 16);
    ASSERT_EQ(picked.size(), 1u);
    status = queue.complete(picked[0], TransferStatusEnum::COMPLETED);
    ASSERT_EQ(status.code(), Status::Code::kOk);
    status = queue.retireBatch(1);
    ASSERT_EQ(status.code(), Status::Code::kOk);

    status = queue.tryAdmit(makeSubmit(2, 1, {makeOwner(0, 16)}), admitted_ids);

    ASSERT_EQ(status.code(), Status::Code::kOk);
    ASSERT_EQ(admitted_ids.size(), 1u);
    EXPECT_EQ(admitted_ids[0], 2u);
}

TEST(AdmissionQueueTest, AccountsPublicSlotsSeparatelyFromQueueOwners) {
    LocalTransferAdmissionQueue queue({2, 128, 0, 0});
    std::vector<QueueOwnerId> admitted_ids;

    auto status = queue.tryAdmit(
        makeSubmit(1, 2, {makeOwner(7, 32, QueueOwnerKind::User, {8, 9})}),
        admitted_ids);

    EXPECT_EQ(status.code(), Status::Code::kTooManyRequests);
    EXPECT_TRUE(admitted_ids.empty());
    EXPECT_EQ(queue.outstandingOwners(), 0u);

    status = queue.tryAdmit(
        makeSubmit(1, 3, {makeOwner(7, 32, QueueOwnerKind::User, {8, 9})}),
        admitted_ids);

    ASSERT_EQ(status.code(), Status::Code::kOk);
    ASSERT_EQ(admitted_ids.size(), 1u);
    EXPECT_EQ(queue.outstandingOwners(), 1u);
    EXPECT_EQ(queue.outstandingBytes(), 32u);

    QueueOwnerId resolved_owner = 0;
    status = queue.resolveOwner(1, 7, resolved_owner);
    EXPECT_EQ(status.code(), Status::Code::kOk);
    EXPECT_EQ(resolved_owner, admitted_ids[0]);
    status = queue.resolveOwner(1, 8, resolved_owner);
    EXPECT_EQ(status.code(), Status::Code::kOk);
    EXPECT_EQ(resolved_owner, admitted_ids[0]);
    status = queue.resolveOwner(1, 9, resolved_owner);
    EXPECT_EQ(status.code(), Status::Code::kOk);
    EXPECT_EQ(resolved_owner, admitted_ids[0]);
    status = queue.resolveOwner(1, 0, resolved_owner);
    EXPECT_EQ(status.code(), Status::Code::kInvalidEntry);
}

TEST(AdmissionQueueTest, PreservesStagingReserveForStagingInternalOwners) {
    LocalTransferAdmissionQueue queue({2, 100, 1, 40});
    std::vector<QueueOwnerId> admitted_ids;

    auto status =
        queue.tryAdmit(makeSubmit(1, 1, {makeOwner(0, 60)}), admitted_ids);
    ASSERT_EQ(status.code(), Status::Code::kOk);

    status = queue.tryAdmit(makeSubmit(2, 1, {makeOwner(0, 1)}), admitted_ids);
    EXPECT_EQ(status.code(), Status::Code::kTooManyRequests);
    EXPECT_TRUE(admitted_ids.empty());
    EXPECT_EQ(queue.outstandingOwners(), 1u);
    EXPECT_EQ(queue.outstandingBytes(), 60u);

    status = queue.tryAdmit(
        makeSubmit(3, 1, {makeOwner(0, 40, QueueOwnerKind::StagingInternal)}),
        admitted_ids);
    ASSERT_EQ(status.code(), Status::Code::kOk);
    ASSERT_EQ(admitted_ids.size(), 1u);
    EXPECT_EQ(admitted_ids[0], 2u);
    EXPECT_EQ(queue.outstandingOwners(), 2u);
    EXPECT_EQ(queue.outstandingBytes(), 100u);
}

TEST(AdmissionQueueTest, KeepsAdmissionOrderForDispatch) {
    LocalTransferAdmissionQueue queue({4, 128, 0, 0});
    std::vector<QueueOwnerId> admitted_ids;

    auto status = queue.tryAdmit(
        makeSubmit(1, 2,
                   {makeOwner(0, 60),
                    makeOwner(1, 10, QueueOwnerKind::StagingInternal)}),
        admitted_ids);

    ASSERT_EQ(status.code(), Status::Code::kOk);
    ASSERT_EQ(admitted_ids.size(), 2u);
    const std::vector<QueueOwnerId> expected_ids{1, 2};
    EXPECT_EQ(admitted_ids, expected_ids);

    EXPECT_TRUE(queue.pickForDispatch(2, 50).empty());

    auto picked = queue.pickForDispatch(2, 70);

    EXPECT_EQ(picked, expected_ids);
}

TEST(AdmissionQueueTest, RequiresDispatchBeforeTerminalCompletion) {
    LocalTransferAdmissionQueue queue({2, 128, 0, 0});
    std::vector<QueueOwnerId> admitted_ids;

    auto status =
        queue.tryAdmit(makeSubmit(1, 1, {makeOwner(0, 16)}), admitted_ids);
    ASSERT_EQ(status.code(), Status::Code::kOk);
    ASSERT_EQ(admitted_ids.size(), 1u);

    status = queue.complete(admitted_ids[0], TransferStatusEnum::COMPLETED);
    EXPECT_EQ(status.code(), Status::Code::kInvalidEntry);
    status = queue.complete(admitted_ids[0], TransferStatusEnum::PENDING);
    EXPECT_EQ(status.code(), Status::Code::kInvalidArgument);
    EXPECT_EQ(queue.outstandingOwners(), 1u);
    EXPECT_EQ(queue.outstandingBytes(), 16u);

    auto picked = queue.pickForDispatch(1, 16);
    ASSERT_EQ(picked.size(), 1u);

    status = queue.complete(picked[0], TransferStatusEnum::COMPLETED);
    EXPECT_EQ(status.code(), Status::Code::kOk);
    EXPECT_EQ(queue.outstandingOwners(), 0u);
    EXPECT_EQ(queue.outstandingBytes(), 0u);

    status = queue.complete(picked[0], TransferStatusEnum::COMPLETED);
    EXPECT_EQ(status.code(), Status::Code::kInvalidEntry);
}

TEST(AdmissionQueueTest, CancelsQueuedOwnerAndReleasesAccounting) {
    LocalTransferAdmissionQueue queue({2, 128, 0, 0});
    std::vector<QueueOwnerId> admitted_ids;
    ASSERT_TRUE(
        queue.tryAdmit(makeSubmit(1, 1, {makeOwner(0, 16)}), admitted_ids)
            .ok());
    ASSERT_EQ(admitted_ids.size(), 1u);

    EXPECT_TRUE(queue.cancel(admitted_ids[0]).ok());
    EXPECT_TRUE(queue.cancel(admitted_ids[0]).ok());
    EXPECT_EQ(queue.outstandingOwners(), 0u);
    EXPECT_EQ(queue.outstandingBytes(), 0u);
    EXPECT_TRUE(queue.pickForDispatch(1, 16).empty());

    TransferStatusEnum status = PENDING;
    ASSERT_TRUE(queue.getPublicStatus(1, 0, status).ok());
    EXPECT_EQ(status, CANCELED);
    EXPECT_TRUE(queue.retireBatch(1).ok());
}

TEST(AdmissionQueueTest, RejectsQueueCancelAfterDispatchStarts) {
    LocalTransferAdmissionQueue queue({2, 128, 0, 0});
    std::vector<QueueOwnerId> admitted_ids;
    ASSERT_TRUE(
        queue.tryAdmit(makeSubmit(1, 1, {makeOwner(0, 16)}), admitted_ids)
            .ok());
    auto picked = queue.pickForDispatch(1, 16);
    ASSERT_EQ(picked.size(), 1u);

    EXPECT_TRUE(queue.cancel(picked[0]).IsInvalidEntry());
    EXPECT_EQ(queue.outstandingOwners(), 1u);
    EXPECT_TRUE(queue.complete(picked[0], COMPLETED).ok());
}

TEST(AdmissionQueueTest, RetainsTerminalStatusUntilBatchRetire) {
    LocalTransferAdmissionQueue queue({2, 128, 0, 0});
    std::vector<QueueOwnerId> admitted_ids;

    auto status = queue.tryAdmit(
        makeSubmit(1, 2, {makeOwner(0, 16, QueueOwnerKind::User, {1})}),
        admitted_ids);
    ASSERT_EQ(status.code(), Status::Code::kOk);

    TransferStatusEnum public_status = TransferStatusEnum::INVALID;
    status = queue.getPublicStatus(1, 1, public_status);
    EXPECT_EQ(status.code(), Status::Code::kOk);
    EXPECT_EQ(public_status, TransferStatusEnum::PENDING);

    auto picked = queue.pickForDispatch(1, 16);
    ASSERT_EQ(picked.size(), 1u);
    status = queue.complete(picked[0], TransferStatusEnum::FAILED);
    ASSERT_EQ(status.code(), Status::Code::kOk);

    status = queue.getPublicStatus(1, 0, public_status);
    EXPECT_EQ(status.code(), Status::Code::kOk);
    EXPECT_EQ(public_status, TransferStatusEnum::FAILED);
    status = queue.getPublicStatus(1, 1, public_status);
    EXPECT_EQ(status.code(), Status::Code::kOk);
    EXPECT_EQ(public_status, TransferStatusEnum::FAILED);

    status = queue.retireBatch(1);
    ASSERT_EQ(status.code(), Status::Code::kOk);

    QueueOwnerId resolved_owner = 0;
    status = queue.resolveOwner(1, 0, resolved_owner);
    EXPECT_EQ(status.code(), Status::Code::kInvalidEntry);
    status = queue.getPublicStatus(1, 1, public_status);
    EXPECT_EQ(status.code(), Status::Code::kInvalidEntry);
}

TEST(AdmissionQueueTest, RetainsSpecificTerminalStatus) {
    LocalTransferAdmissionQueue queue({1, 128, 0, 0});
    std::vector<QueueOwnerId> admitted_ids;

    auto status =
        queue.tryAdmit(makeSubmit(1, 1, {makeOwner(0, 16)}), admitted_ids);
    ASSERT_EQ(status.code(), Status::Code::kOk);

    auto picked = queue.pickForDispatch(1, 16);
    ASSERT_EQ(picked.size(), 1u);
    status = queue.complete(picked[0], TransferStatusEnum::TIMEOUT);
    ASSERT_EQ(status.code(), Status::Code::kOk);

    TransferStatusEnum public_status = TransferStatusEnum::PENDING;
    status = queue.getPublicStatus(1, 0, public_status);
    ASSERT_EQ(status.code(), Status::Code::kOk);
    EXPECT_EQ(public_status, TransferStatusEnum::TIMEOUT);
}

TEST(AdmissionQueueTest, RejectsRetireWithNonTerminalOwners) {
    LocalTransferAdmissionQueue queue({2, 128, 0, 0});
    std::vector<QueueOwnerId> admitted_ids;

    auto status = queue.tryAdmit(
        makeSubmit(1, 2, {makeOwner(0, 16), makeOwner(1, 16)}), admitted_ids);
    ASSERT_EQ(status.code(), Status::Code::kOk);

    auto picked = queue.pickForDispatch(1, 16);
    ASSERT_EQ(picked.size(), 1u);
    status = queue.complete(picked[0], TransferStatusEnum::COMPLETED);
    ASSERT_EQ(status.code(), Status::Code::kOk);

    status = queue.retireBatch(1);
    EXPECT_EQ(status.code(), Status::Code::kInvalidEntry);

    picked = queue.pickForDispatch(1, 16);
    ASSERT_EQ(picked.size(), 1u);
    status = queue.complete(picked[0], TransferStatusEnum::COMPLETED);
    ASSERT_EQ(status.code(), Status::Code::kOk);

    status = queue.retireBatch(1);
    EXPECT_EQ(status.code(), Status::Code::kOk);
}

TEST(AdmissionQueueTest, AllowsBatchTokenReuseAfterRetire) {
    LocalTransferAdmissionQueue queue({1, 128, 0, 0});
    std::vector<QueueOwnerId> admitted_ids;

    auto status =
        queue.tryAdmit(makeSubmit(1, 1, {makeOwner(0, 16)}), admitted_ids);
    ASSERT_EQ(status.code(), Status::Code::kOk);
    ASSERT_EQ(admitted_ids.size(), 1u);
    EXPECT_EQ(admitted_ids[0], 1u);

    auto picked = queue.pickForDispatch(1, 16);
    ASSERT_EQ(picked.size(), 1u);
    status = queue.complete(picked[0], TransferStatusEnum::COMPLETED);
    ASSERT_EQ(status.code(), Status::Code::kOk);
    status = queue.retireBatch(1);
    ASSERT_EQ(status.code(), Status::Code::kOk);

    status = queue.tryAdmit(makeSubmit(1, 1, {makeOwner(0, 16)}), admitted_ids);

    ASSERT_EQ(status.code(), Status::Code::kOk);
    ASSERT_EQ(admitted_ids.size(), 1u);
    EXPECT_EQ(admitted_ids[0], 2u);

    QueueOwnerId resolved_owner = 0;
    status = queue.resolveOwner(1, 0, resolved_owner);
    EXPECT_EQ(status.code(), Status::Code::kOk);
    EXPECT_EQ(resolved_owner, 2u);
}

// --- RFC #2519 step 2: opt-in deadline-aware (EDF) dispatch ---------------

QueueOwnerInput makeOwnerWithDeadline(size_t public_task_id, size_t length,
                                      uint64_t deadline_ns) {
    QueueOwnerInput owner = makeOwner(public_task_id, length);
    owner.request.deadline_ns = deadline_ns;
    return owner;
}

QueueOwnerInput makeDegradationEligibleOwnerWithDeadline(size_t public_task_id,
                                                         size_t length,
                                                         uint64_t deadline_ns) {
    QueueOwnerInput owner =
        makeOwnerWithDeadline(public_task_id, length, deadline_ns);
    owner.degradation_eligible = true;
    return owner;
}

TEST(AdmissionQueueTest, DeadlineAwareDispatchesEarliestDeadlineFirst) {
    QueueLimits limits{4, 4096, 0, 0};
    limits.deadline_aware = true;
    LocalTransferAdmissionQueue queue(limits);
    std::vector<QueueOwnerId> admitted_ids;

    // Admitted in FIFO order 1,2,3 but with deadlines 300,100,200.
    auto status =
        queue.tryAdmit(makeSubmit(1, 3,
                                  {makeOwnerWithDeadline(0, 16, 300),
                                   makeOwnerWithDeadline(1, 16, 100),
                                   makeOwnerWithDeadline(2, 16, 200)}),
                       admitted_ids);
    ASSERT_EQ(status.code(), Status::Code::kOk);
    ASSERT_EQ(admitted_ids.size(), 3u);  // owner ids 1,2,3

    auto picked = queue.pickForDispatch(3, 4096);
    // EDF order: owner 2 (dl 100) < owner 3 (dl 200) < owner 1 (dl 300).
    const std::vector<QueueOwnerId> expected{2, 3, 1};
    EXPECT_EQ(picked, expected);
}

TEST(AdmissionQueueTest, DeadlineAwareKeepsUndeadlinedOwnersLast) {
    QueueLimits limits{4, 4096, 0, 0};
    limits.deadline_aware = true;
    LocalTransferAdmissionQueue queue(limits);
    std::vector<QueueOwnerId> admitted_ids;

    // owner 1: no deadline (0); owner 2: deadline 100; owner 3: no deadline.
    auto status = queue.tryAdmit(makeSubmit(1, 3,
                                            {makeOwnerWithDeadline(0, 16, 0),
                                             makeOwnerWithDeadline(1, 16, 100),
                                             makeOwnerWithDeadline(2, 16, 0)}),
                                 admitted_ids);
    ASSERT_EQ(status.code(), Status::Code::kOk);

    auto picked = queue.pickForDispatch(3, 4096);
    // Deadlined owner 2 first; undeadlined 1,3 keep FIFO order behind it.
    const std::vector<QueueOwnerId> expected{2, 1, 3};
    EXPECT_EQ(picked, expected);
}

TEST(AdmissionQueueTest, DeadlineUnawareKeepsStrictFifo) {
    // Default (deadline_aware == false): FIFO regardless of deadlines.
    LocalTransferAdmissionQueue queue({4, 4096, 0, 0});
    std::vector<QueueOwnerId> admitted_ids;

    auto status =
        queue.tryAdmit(makeSubmit(1, 3,
                                  {makeOwnerWithDeadline(0, 16, 300),
                                   makeOwnerWithDeadline(1, 16, 100),
                                   makeOwnerWithDeadline(2, 16, 200)}),
                       admitted_ids);
    ASSERT_EQ(status.code(), Status::Code::kOk);

    auto picked = queue.pickForDispatch(3, 4096);
    const std::vector<QueueOwnerId> expected{1, 2,
                                             3};  // FIFO, deadlines ignored
    EXPECT_EQ(picked, expected);
}

// fifo_ is kept EDF-ordered at admission time, so owners admitted across
// *separate* tryAdmit calls (out of deadline order) must still dispatch EDF —
// this exercises the ordered-insert path, not just a single sorted batch.
TEST(AdmissionQueueTest, DeadlineAwareOrdersAcrossSeparateAdmits) {
    QueueLimits limits{8, 4096, 0, 0};
    limits.deadline_aware = true;
    LocalTransferAdmissionQueue queue(limits);
    std::vector<QueueOwnerId> ids;

    // Admit one at a time, deadlines arriving out of order: 300, 100, 200, 0.
    ASSERT_EQ(
        queue
            .tryAdmit(makeSubmit(1, 1, {makeOwnerWithDeadline(0, 16, 300)}),
                      ids)
            .code(),
        Status::Code::kOk);  // owner 1
    ASSERT_EQ(
        queue
            .tryAdmit(makeSubmit(2, 1, {makeOwnerWithDeadline(0, 16, 100)}),
                      ids)
            .code(),
        Status::Code::kOk);  // owner 2
    ASSERT_EQ(
        queue
            .tryAdmit(makeSubmit(3, 1, {makeOwnerWithDeadline(0, 16, 200)}),
                      ids)
            .code(),
        Status::Code::kOk);  // owner 3
    ASSERT_EQ(
        queue.tryAdmit(makeSubmit(4, 1, {makeOwnerWithDeadline(0, 16, 0)}), ids)
            .code(),
        Status::Code::kOk);  // owner 4 (no deadline → last)

    auto picked = queue.pickForDispatch(8, 4096);
    // EDF: 100(owner2) < 200(owner3) < 300(owner1) < no-deadline(owner4).
    const std::vector<QueueOwnerId> expected{2, 3, 1, 4};
    EXPECT_EQ(picked, expected);
}

// --- RFC #2519 step 3: deadline-infeasible drop + degradation hook --------

// Helper: build a queue with deadline_aware + a θ_local, a fixed bandwidth,
// and a fixed "now" clock so MLU is deterministic.
QueueLimits step3Limits(double theta_local) {
    QueueLimits limits{4, 1 << 20, 0, 0};
    limits.deadline_aware = true;
    limits.mlu_local_threshold = theta_local;
    return limits;
}

TEST(AdmissionQueueTest, Step3DropsInfeasibleAndKeepsFeasible) {
    LocalTransferAdmissionQueue queue(step3Limits(1.5));
    // Fixed now = 1e9 ns; bandwidth = 1e9 B/s (so 16 B takes 16 ns).
    int hook_calls = 0;
    DegradationHooks hooks;
    hooks.on_local_decode_suggested = [&](const Request&) { ++hook_calls; };
    queue.setDegradationPolicy([] { return 1e9; }, hooks,
                               [] { return uint64_t{1'000'000'000}; });

    std::vector<QueueOwnerId> admitted_ids;
    // owner 1: window = 10 ns → 16 B / 1e9 = 16 ns → MLU 1.6 ≥ 1.5 → DROP.
    // owner 2: window = 1e6 ns → MLU ~1.6e-5 → feasible → dispatch.
    auto status = queue.tryAdmit(
        makeSubmit(
            1, 2,
            {makeDegradationEligibleOwnerWithDeadline(0, 16, 1'000'000'010),
             makeDegradationEligibleOwnerWithDeadline(1, 16, 2'000'000'000)}),
        admitted_ids);
    ASSERT_EQ(status.code(), Status::Code::kOk);
    ASSERT_EQ(admitted_ids.size(), 2u);

    std::vector<QueueOwnerId> dropped;
    auto picked = queue.pickForDispatch(4, 1 << 20, &dropped);

    const std::vector<QueueOwnerId> exp_pick{2};
    const std::vector<QueueOwnerId> exp_drop{1};
    EXPECT_EQ(picked, exp_pick);
    EXPECT_EQ(dropped, exp_drop);
    EXPECT_EQ(hook_calls, 1);
    // Dropped owner is charged out of the outstanding accounting.
    EXPECT_EQ(queue.outstandingOwners(), 1u);
    EXPECT_EQ(queue.outstandingBytes(), 16u);
}

// The deadline is absolute, so an owner dispatched now still has to wait
// behind everything already dispatched and not yet completed. That wait is
// an additive delay charged from the queue's own dispatch accounting, not a
// slower bandwidth.
TEST(AdmissionQueueTest, Step3DropCountsBytesAlreadyDispatched) {
    LocalTransferAdmissionQueue queue(step3Limits(1.0));
    // now = 1e9 ns; bandwidth 1e9 B/s: 1 MB takes 1 ms.
    queue.setDegradationPolicy([] { return 1e9; }, DegradationHooks{},
                               [] { return uint64_t{1'000'000'000}; });

    // owner 1: 1 MB, loose deadline; dispatched and still in flight.
    std::vector<QueueOwnerId> ids;
    ASSERT_EQ(
        queue
            .tryAdmit(makeSubmit(1, 1,
                                 {makeDegradationEligibleOwnerWithDeadline(
                                     0, 1'000'000, 5'000'000'000)}),
                      ids)
            .code(),
        Status::Code::kOk);
    ASSERT_EQ(queue.pickForDispatch(4, 1 << 20), std::vector<QueueOwnerId>{1});

    // owner 2: 16 B with a 500 us window. Alone it would take 16 ns
    // (MLU ~3e-5); behind the 1 MB it completes at ~1 ms → MLU ~2 → drop.
    ASSERT_EQ(
        queue
            .tryAdmit(makeSubmit(2, 1,
                                 {makeDegradationEligibleOwnerWithDeadline(
                                     0, 16, 1'000'500'000)}),
                      ids)
            .code(),
        Status::Code::kOk);
    std::vector<QueueOwnerId> dropped;
    EXPECT_TRUE(queue.pickForDispatch(4, 1 << 20, &dropped).empty());
    EXPECT_EQ(dropped, std::vector<QueueOwnerId>{2});

    // Once owner 1 completes nothing is ahead: the same request is feasible.
    ASSERT_EQ(queue.complete(1, TransferStatusEnum::COMPLETED).code(),
              Status::Code::kOk);
    ASSERT_EQ(
        queue
            .tryAdmit(makeSubmit(3, 1,
                                 {makeDegradationEligibleOwnerWithDeadline(
                                     0, 16, 1'000'500'000)}),
                      ids)
            .code(),
        Status::Code::kOk);
    dropped.clear();
    EXPECT_EQ(queue.pickForDispatch(4, 1 << 20, &dropped),
              std::vector<QueueOwnerId>{3});
    EXPECT_TRUE(dropped.empty());
}

// Only provider-governed bytes wait ahead of an eligible owner: an owner on
// another transport is neither counted when dispatched nor subtracted when
// it completes.
TEST(AdmissionQueueTest, Step3QueueAheadCountsOnlyProviderGovernedBytes) {
    LocalTransferAdmissionQueue queue(step3Limits(1.0));
    queue.setDegradationPolicy([] { return 1e9; }, DegradationHooks{},
                               [] { return uint64_t{1'000'000'000}; });

    // owner 1: 1 MB, not eligible (not governed by the provider); dispatched.
    std::vector<QueueOwnerId> ids;
    ASSERT_EQ(
        queue
            .tryAdmit(
                makeSubmit(
                    1, 1, {makeOwnerWithDeadline(0, 1'000'000, 5'000'000'000)}),
                ids)
            .code(),
        Status::Code::kOk);
    ASSERT_EQ(queue.pickForDispatch(4, 1 << 20), std::vector<QueueOwnerId>{1});

    // owner 2: 16 B with a 500 us window, eligible. Nothing ahead of it is
    // on the NIC, so it is feasible.
    ASSERT_EQ(
        queue
            .tryAdmit(makeSubmit(2, 1,
                                 {makeDegradationEligibleOwnerWithDeadline(
                                     0, 16, 1'000'500'000)}),
                      ids)
            .code(),
        Status::Code::kOk);
    std::vector<QueueOwnerId> dropped;
    EXPECT_EQ(queue.pickForDispatch(4, 1 << 20, &dropped),
              std::vector<QueueOwnerId>{2});
    EXPECT_TRUE(dropped.empty());

    // Completing the ungoverned owner must not disturb the count either:
    // afterwards an eligible 1 MB owner still drops a tight owner behind it.
    ASSERT_EQ(queue.complete(1, TransferStatusEnum::COMPLETED).code(),
              Status::Code::kOk);
    ASSERT_EQ(queue.complete(2, TransferStatusEnum::COMPLETED).code(),
              Status::Code::kOk);
    ASSERT_EQ(
        queue
            .tryAdmit(makeSubmit(3, 1,
                                 {makeDegradationEligibleOwnerWithDeadline(
                                     0, 1'000'000, 5'000'000'000)}),
                      ids)
            .code(),
        Status::Code::kOk);
    dropped.clear();
    ASSERT_EQ(queue.pickForDispatch(4, 1 << 20, &dropped),
              std::vector<QueueOwnerId>{3});
    EXPECT_TRUE(dropped.empty());
    ASSERT_EQ(
        queue
            .tryAdmit(makeSubmit(4, 1,
                                 {makeDegradationEligibleOwnerWithDeadline(
                                     0, 16, 1'000'500'000)}),
                      ids)
            .code(),
        Status::Code::kOk);
    dropped.clear();
    EXPECT_TRUE(queue.pickForDispatch(4, 1 << 20, &dropped).empty());
    EXPECT_EQ(dropped, std::vector<QueueOwnerId>{4});
}

// Within one pick, `bytes_ahead` for a later owner is exactly the eligible
// bytes picked before it: an ungoverned owner ahead adds nothing, and an
// eligible one is counted once (dispatching_bytes_ already grows as owners
// are picked).
TEST(AdmissionQueueTest, Step3QueueAheadWithinOnePickIsExact) {
    LocalTransferAdmissionQueue queue(step3Limits(1.0));
    // now = 1e9 ns; bandwidth 1e9 B/s: 1 MB takes 1 ms.
    queue.setDegradationPolicy([] { return 1e9; }, DegradationHooks{},
                               [] { return uint64_t{1'000'000'000}; });

    // EDF order: N (400 KB, not eligible, 560 us), A (400 KB, eligible,
    // 600 us), B (16 B, eligible, 640 us); 400 KB takes 400 us.
    //   A: nothing governed ahead -> 400 / 600 = 0.67 -> dispatch
    //      (counting N: 800 / 600 = 1.33 -> drop)
    //   B: A ahead -> ~400 / 640 = 0.63 -> dispatch
    //      (counting A twice: 800 / 640 = 1.25 -> drop)
    std::vector<QueueOwnerId> ids;
    ASSERT_EQ(
        queue
            .tryAdmit(
                makeSubmit(1, 3,
                           {makeOwnerWithDeadline(0, 400'000, 1'000'560'000),
                            makeDegradationEligibleOwnerWithDeadline(
                                1, 400'000, 1'000'600'000),
                            makeDegradationEligibleOwnerWithDeadline(
                                2, 16, 1'000'640'000)}),
                ids)
            .code(),
        Status::Code::kOk);
    std::vector<QueueOwnerId> dropped;
    const std::vector<QueueOwnerId> all{1, 2, 3};
    EXPECT_EQ(queue.pickForDispatch(4, 1 << 20, &dropped), all);
    EXPECT_TRUE(dropped.empty());
}

// dispatching_bytes_ is the drop prediction's queue-ahead term, so after
// every owner state transition it must equal the bytes of the eligible
// owners currently Dispatching: nothing more, nothing less.
TEST(AdmissionQueueTest,
     Step3DispatchingBytesEqualsEligibleBytesInDispatching) {
    LocalTransferAdmissionQueue queue(step3Limits(1.0));
    // now = 1e9 ns, bandwidth 1e9 B/s: 1 B per ns.
    queue.setDegradationPolicy([] { return 1e9; }, DegradationHooks{},
                               [] { return uint64_t{1'000'000'000}; });

    // Distinct loose deadlines: EDF order == admission order, nothing drops.
    // 1: eligible 1000 B; 2: not eligible 20000 B; 3: eligible 300 B;
    // 4: eligible 40 B, stays queued.
    std::vector<QueueOwnerId> ids;
    ASSERT_EQ(queue
                  .tryAdmit(makeSubmit(
                                1, 4,
                                {makeDegradationEligibleOwnerWithDeadline(
                                     0, 1000, 5'000'000'000),
                                 makeOwnerWithDeadline(1, 20000, 6'000'000'000),
                                 makeDegradationEligibleOwnerWithDeadline(
                                     2, 300, 7'000'000'000),
                                 makeDegradationEligibleOwnerWithDeadline(
                                     3, 40, 8'000'000'000)}),
                            ids)
                  .code(),
              Status::Code::kOk);
    EXPECT_EQ(queue.dispatchingBytes(), 0u);  // admitted is not dispatching

    // pick: eligible owners charge their length, the other charges nothing.
    ASSERT_EQ(queue.pickForDispatch(3, 1 << 20),
              (std::vector<QueueOwnerId>{1, 2, 3}));
    EXPECT_EQ(queue.dispatchingBytes(), 1300u);

    // Refused calls and a queued owner's cancel move nothing.
    EXPECT_NE(queue.cancel(1).code(), Status::Code::kOk);  // dispatching
    ASSERT_EQ(queue.cancel(4).code(), Status::Code::kOk);  // queued
    ASSERT_EQ(queue.cancel(4).code(), Status::Code::kOk);  // idempotent
    EXPECT_NE(queue.complete(4, TransferStatusEnum::COMPLETED).code(),
              Status::Code::kOk);  // not dispatching
    EXPECT_EQ(queue.dispatchingBytes(), 1300u);

    // complete: non-eligible returns nothing; eligible returns its length
    // whatever the terminal status.
    ASSERT_EQ(queue.complete(2, TransferStatusEnum::COMPLETED).code(),
              Status::Code::kOk);
    EXPECT_EQ(queue.dispatchingBytes(), 1300u);
    ASSERT_EQ(queue.complete(1, TransferStatusEnum::FAILED).code(),
              Status::Code::kOk);
    EXPECT_EQ(queue.dispatchingBytes(), 300u);

    // drop: 16 B, 10 ns window, 300 B ahead -> MLU 31.6. Queued -> Terminal
    // without passing through Dispatching.
    ASSERT_EQ(
        queue
            .tryAdmit(makeSubmit(2, 1,
                                 {makeDegradationEligibleOwnerWithDeadline(
                                     0, 16, 1'000'000'010)}),
                      ids)
            .code(),
        Status::Code::kOk);
    std::vector<QueueOwnerId> dropped;
    EXPECT_TRUE(queue.pickForDispatch(4, 1 << 20, &dropped).empty());
    EXPECT_EQ(dropped, std::vector<QueueOwnerId>{5});
    EXPECT_EQ(queue.dispatchingBytes(), 300u);

    // retireBatch: erasing terminal owners (batch 2) or being refused for a
    // dispatching one (batch 1, owner 3) moves nothing.
    ASSERT_EQ(queue.retireBatch(2).code(), Status::Code::kOk);
    EXPECT_NE(queue.retireBatch(1).code(), Status::Code::kOk);
    EXPECT_EQ(queue.dispatchingBytes(), 300u);

    ASSERT_EQ(queue.complete(3, TransferStatusEnum::COMPLETED).code(),
              Status::Code::kOk);
    EXPECT_EQ(queue.dispatchingBytes(), 0u);
    ASSERT_EQ(queue.retireBatch(1).code(), Status::Code::kOk);
    EXPECT_EQ(queue.dispatchingBytes(), 0u);
}

// --- RFC #2519 step 3: the drop / estimate feedback loop -----------------
//
// The drop decision, the traffic it lets through, and the bandwidth estimate
// learned from that traffic form a loop: dropping shapes what is sent, what
// is sent is the only thing the estimate can learn from, and the estimate
// is what the next drop is judged against. RDMA throughput is queue-depth
// bound, so a trickle admitted under a low estimate can genuinely achieve a
// low rate and confirm the estimate. These tests drive the queue against a
// model of such a link to see whether the loop returns to saturation or
// settles on a plateau below it.
//
// Model: the link saturates at `line_rate` with `saturating_owners` in
// flight and achieves a proportional fraction below that; the estimate
// follows what was achieved with the transmit smoothing (alpha weights the
// old value, as in DeviceSelector).
struct QueueDepthBoundLink {
    double line_rate;
    int saturating_owners;
    double alpha;
    double rate;

    double achieved(int concurrent) const {
        return line_rate * std::min(concurrent, saturating_owners) /
               saturating_owners;
    }
    void observe(int concurrent) {
        rate = alpha * rate + (1.0 - alpha) * achieved(concurrent);
    }
};

struct FeedbackRound {
    int admitted;
    int dropped;
    double rate_after;
};

// One round: offer `owners` equal, eligible owners with the same deadline,
// dispatch, complete whatever was admitted, retire the batch, and let the
// link learn from the concurrency that round achieved.
FeedbackRound runFeedbackRound(LocalTransferAdmissionQueue& queue,
                               QueueDepthBoundLink& link, uint64_t token,
                               int owners, size_t length,
                               uint64_t deadline_ns) {
    std::vector<QueueOwnerInput> inputs;
    for (int i = 0; i < owners; ++i)
        inputs.push_back(
            makeDegradationEligibleOwnerWithDeadline(i, length, deadline_ns));
    std::vector<QueueOwnerId> ids;
    EXPECT_EQ(queue.tryAdmit(makeSubmit(token, owners, std::move(inputs)), ids)
                  .code(),
              Status::Code::kOk);

    std::vector<QueueOwnerId> dropped;
    auto picked = queue.pickForDispatch(owners, 1 << 30, &dropped);
    for (auto id : picked)
        EXPECT_EQ(queue.complete(id, TransferStatusEnum::COMPLETED).code(),
                  Status::Code::kOk);
    EXPECT_EQ(queue.retireBatch(token).code(), Status::Code::kOk);

    link.observe(static_cast<int>(picked.size()));
    return {static_cast<int>(picked.size()), static_cast<int>(dropped.size()),
            link.rate};
}

// Shared scenario: 8 owners of 1 MB per round, a 10 ms window, theta 0.95,
// a link that needs all 8 in flight to reach 1 GB/s (1 MB per ms). At line
// rate the last owner scores MLU 0.8 and everything is admitted. The i-th
// owner is judged behind the i-1 picked before it in the same call.
constexpr double kLineRate = 1e9;
constexpr int kSaturatingOwners = 8;
constexpr size_t kOwnerBytes = 1'000'000;
constexpr uint64_t kNow = 1'000'000'000;
constexpr uint64_t kDeadline = kNow + 10'000'000;  // 10 ms window
constexpr double kThetaLocal = 0.95;

std::unique_ptr<LocalTransferAdmissionQueue> makeFeedbackQueue(
    QueueDepthBoundLink& link, int& hook_calls, size_t probe_owners = 0) {
    QueueLimits limits{16, 1 << 30, 0, 0};
    limits.deadline_aware = true;
    limits.mlu_local_threshold = kThetaLocal;
    limits.mlu_probe_owners = probe_owners;
    auto queue = std::make_unique<LocalTransferAdmissionQueue>(limits);
    DegradationHooks hooks;
    hooks.on_local_decode_suggested = [&](const Request&) { ++hook_calls; };
    queue->setDegradationPolicy([&link] { return link.rate; }, hooks,
                                [] { return kNow; });
    return queue;
}

// From an optimistic seed nothing is dropped, the link stays saturated and
// the estimate never moves: the loop does not starve itself from a clean
// start.
TEST(AdmissionQueueTest, Step3FeedbackCleanStartStaysSaturated) {
    QueueDepthBoundLink link{kLineRate, kSaturatingOwners, 0.9, kLineRate};
    int hook_calls = 0;
    auto queue = makeFeedbackQueue(link, hook_calls);
    for (uint64_t round = 1; round <= 30; ++round) {
        auto r = runFeedbackRound(*queue, link, round, kSaturatingOwners,
                                  kOwnerBytes, kDeadline);
        ASSERT_EQ(r.admitted, kSaturatingOwners) << "round " << round;
        ASSERT_EQ(r.dropped, 0) << "round " << round;
        ASSERT_NEAR(r.rate_after, kLineRate, 1e-9 * kLineRate)
            << "round " << round;
    }
    EXPECT_EQ(hook_calls, 0);
}

// An estimate depressed to what 6 of 8 owners achieve drops one owner a
// round, but the 7 that get through achieve more than the estimate says,
// so it climbs back and admission returns to 8: the shallow basin recovers.
TEST(AdmissionQueueTest, Step3FeedbackShallowDepressionRecovers) {
    QueueDepthBoundLink link{kLineRate, kSaturatingOwners, 0.9, 0.0};
    link.rate = link.achieved(6);  // 0.75 GB/s
    int hook_calls = 0;
    auto queue = makeFeedbackQueue(link, hook_calls);

    int recovered_at = -1;
    for (uint64_t round = 1; round <= 40; ++round) {
        auto r = runFeedbackRound(*queue, link, round, kSaturatingOwners,
                                  kOwnerBytes, kDeadline);
        ASSERT_GE(r.admitted, 7) << "round " << round;  // never gets worse
        if (r.admitted == kSaturatingOwners && recovered_at < 0)
            recovered_at = static_cast<int>(round);
        if (recovered_at > 0)
            ASSERT_EQ(r.admitted, kSaturatingOwners) << "round " << round;
    }
    EXPECT_GT(recovered_at, 0);
    EXPECT_LE(recovered_at, 25);
    EXPECT_NEAR(link.rate, kLineRate, 0.02 * kLineRate);
    EXPECT_EQ(hook_calls, recovered_at - 1);  // one drop per round until then
}

// An estimate depressed to what 4 of 8 owners achieve is a fixed point: the
// 5th owner scores MLU 1.0, so exactly 4 are admitted, 4 achieve exactly the
// rate the estimate already holds, and nothing ever pulls it back up. This
// pins the J21 finding under the queue-depth-bound model: with no probe
// (mlu_probe_owners = 0, the default) the loop has a self-sustaining plateau
// below saturation and nothing spends evidence on climbing out.
TEST(AdmissionQueueTest, Step3FeedbackDeepDepressionIsSelfSustaining) {
    QueueDepthBoundLink link{kLineRate, kSaturatingOwners, 0.9, 0.0};
    link.rate = link.achieved(4);  // 0.5 GB/s
    int hook_calls = 0;
    auto queue = makeFeedbackQueue(link, hook_calls);

    for (uint64_t round = 1; round <= 50; ++round) {
        auto r = runFeedbackRound(*queue, link, round, kSaturatingOwners,
                                  kOwnerBytes, kDeadline);
        ASSERT_EQ(r.admitted, 4) << "round " << round;
        ASSERT_EQ(r.dropped, 4) << "round " << round;
        ASSERT_NEAR(r.rate_after, link.achieved(4), 1e-9 * link.achieved(4))
            << "round " << round;
    }
    EXPECT_EQ(hook_calls, 4 * 50);
}

// One probe per pick from the same plateau: the 5th owner the drop would
// reject is dispatched, 5 achieve more than the estimate says, it climbs
// past the point where a 5th is admitted on its own merits, the probe moves
// to the 6th, and so on up to saturation.
TEST(AdmissionQueueTest, Step3FeedbackDeepDepressionRecoversWithOneProbe) {
    QueueDepthBoundLink link{kLineRate, kSaturatingOwners, 0.9, 0.0};
    link.rate = link.achieved(4);
    int hook_calls = 0;
    auto queue = makeFeedbackQueue(link, hook_calls, /*probe_owners=*/1);

    int recovered_at = -1;
    size_t probes_at_50 = 0;
    for (uint64_t round = 1; round <= 60; ++round) {
        auto r = runFeedbackRound(*queue, link, round, kSaturatingOwners,
                                  kOwnerBytes, kDeadline);
        ASSERT_GE(r.admitted, 4) << "round " << round;  // never below plateau
        if (r.admitted == kSaturatingOwners && recovered_at < 0)
            recovered_at = static_cast<int>(round);
        if (recovered_at > 0)
            ASSERT_EQ(r.admitted, kSaturatingOwners) << "round " << round;
        if (round == 50) probes_at_50 = queue->probeStats().dispatched;
    }
    EXPECT_GT(recovered_at, 0);
    EXPECT_LE(recovered_at, 40);
    EXPECT_NEAR(link.rate, kLineRate, 0.02 * kLineRate);
    // At most one probe a round on the way up; once the estimate carries
    // saturation on its own nothing is infeasible and probing stops.
    EXPECT_GT(queue->probeStats().dispatched, 0u);
    EXPECT_LE(queue->probeStats().dispatched, 60u);
    EXPECT_EQ(queue->probeStats().dispatched, probes_at_50);
}

// The budget buys time, not a cheaper climb. From the same plateau a larger
// budget reaches saturation in fewer rounds, but the probes spent getting
// there stay about the same: the estimate has a fixed distance to move and
// each probe moves it by about the same amount whenever it is spent. What a
// larger budget does add is exposure when the estimate is right -- that many
// more late transfers per pick -- so 1 is the recommended value.
TEST(AdmissionQueueTest,
     Step3FeedbackLargerProbeBudgetRecoversFasterAtTheSameCost) {
    struct Outcome {
        int recovered_at;
        size_t probes;
    };
    auto climb = [](size_t probe_owners) {
        QueueDepthBoundLink link{kLineRate, kSaturatingOwners, 0.9, 0.0};
        link.rate = link.achieved(4);
        int hook_calls = 0;
        auto queue = makeFeedbackQueue(link, hook_calls, probe_owners);
        Outcome out{-1, 0};
        for (uint64_t round = 1; round <= 60; ++round) {
            auto r = runFeedbackRound(*queue, link, round, kSaturatingOwners,
                                      kOwnerBytes, kDeadline);
            if (r.admitted == kSaturatingOwners && out.recovered_at < 0)
                out.recovered_at = static_cast<int>(round);
        }
        out.probes = queue->probeStats().dispatched;
        return out;
    };

    const Outcome one = climb(1), two = climb(2), four = climb(4);
    EXPECT_GT(one.recovered_at, 0);
    EXPECT_GT(two.recovered_at, 0);
    EXPECT_GT(four.recovered_at, 0);
    EXPECT_LE(two.recovered_at, one.recovered_at);
    EXPECT_LE(four.recovered_at, two.recovered_at);
    EXPECT_LT(four.recovered_at, one.recovered_at);  // strictly faster overall
    EXPECT_NEAR(static_cast<double>(two.probes),
                static_cast<double>(one.probes), 5.0);
    EXPECT_NEAR(static_cast<double>(four.probes),
                static_cast<double>(one.probes), 5.0);
}

// ---- probe mechanics ----

// With a probe budget the owner the drop would reject is dispatched instead:
// not terminal, not signalled, counted as dispatching like any other owner.
TEST(AdmissionQueueTest, Step3ProbeDispatchesTheOwnerTheDropWouldReject) {
    QueueLimits limits = step3Limits(1.5);
    limits.mlu_probe_owners = 1;
    LocalTransferAdmissionQueue queue(limits);
    int hook_calls = 0;
    DegradationHooks hooks;
    hooks.on_local_decode_suggested = [&](const Request&) { ++hook_calls; };
    queue.setDegradationPolicy([] { return 1e9; }, hooks,
                               [] { return uint64_t{1'000'000'000}; });

    // Same pair as Step3DropsInfeasibleAndKeepsFeasible: owner 1 has MLU
    // 1.6 >= 1.5 and would be dropped; owner 2 is comfortably feasible.
    std::vector<QueueOwnerId> ids;
    ASSERT_EQ(
        queue
            .tryAdmit(makeSubmit(1, 2,
                                 {makeDegradationEligibleOwnerWithDeadline(
                                      0, 16, 1'000'000'010),
                                  makeDegradationEligibleOwnerWithDeadline(
                                      1, 16, 2'000'000'000)}),
                      ids)
            .code(),
        Status::Code::kOk);

    std::vector<QueueOwnerId> dropped;
    auto picked = queue.pickForDispatch(4, 1 << 20, &dropped);
    EXPECT_EQ(picked, (std::vector<QueueOwnerId>{1, 2}));
    EXPECT_TRUE(dropped.empty());
    EXPECT_EQ(hook_calls, 0);
    EXPECT_EQ(queue.probeStats().dispatched, 1u);
    EXPECT_EQ(queue.dispatchingBytes(), 32u);  // the probe is dispatched too
    EXPECT_EQ(queue.outstandingOwners(), 2u);
}

// The budget is per pick and covers only the infeasible: two owners that
// each score MLU 1.6 on their own and a budget of one, the first is probed
// and the second dropped (it would be even with nothing ahead of it); the
// next pick starts with a fresh budget.
TEST(AdmissionQueueTest, Step3ProbeBudgetIsPerPickAndDropsTheRest) {
    QueueLimits limits = step3Limits(1.5);
    limits.mlu_probe_owners = 1;
    LocalTransferAdmissionQueue queue(limits);
    queue.setDegradationPolicy([] { return 1e9; }, DegradationHooks{},
                               [] { return uint64_t{1'000'000'000}; });

    std::vector<QueueOwnerId> ids;
    ASSERT_EQ(
        queue
            .tryAdmit(makeSubmit(1, 2,
                                 {makeDegradationEligibleOwnerWithDeadline(
                                      0, 16, 1'000'000'010),
                                  makeDegradationEligibleOwnerWithDeadline(
                                      1, 16, 1'000'000'010)}),
                      ids)
            .code(),
        Status::Code::kOk);
    std::vector<QueueOwnerId> dropped;
    EXPECT_EQ(queue.pickForDispatch(4, 1 << 20, &dropped),
              std::vector<QueueOwnerId>{1});
    EXPECT_EQ(dropped, std::vector<QueueOwnerId>{2});
    EXPECT_EQ(queue.probeStats().dispatched, 1u);

    ASSERT_EQ(queue.complete(1, TransferStatusEnum::COMPLETED).code(),
              Status::Code::kOk);
    ASSERT_EQ(
        queue
            .tryAdmit(makeSubmit(2, 1,
                                 {makeDegradationEligibleOwnerWithDeadline(
                                     0, 16, 1'000'000'010)}),
                      ids)
            .code(),
        Status::Code::kOk);
    dropped.clear();
    EXPECT_EQ(queue.pickForDispatch(4, 1 << 20, &dropped),
              std::vector<QueueOwnerId>{3});
    EXPECT_TRUE(dropped.empty());
    EXPECT_EQ(queue.probeStats().dispatched, 2u);
}

// Probing only ever replaces a drop: with the drop disabled (no threshold)
// the budget has nothing to spend on, and nothing is counted as a probe.
TEST(AdmissionQueueTest, Step3ProbeIsInertWithoutDrop) {
    QueueLimits limits = step3Limits(/*theta_local=*/0.0);
    limits.mlu_probe_owners = 1;
    LocalTransferAdmissionQueue queue(limits);
    queue.setDegradationPolicy([] { return 1e9; }, DegradationHooks{},
                               [] { return uint64_t{1'000'000'000}; });

    std::vector<QueueOwnerId> ids;
    ASSERT_EQ(
        queue
            .tryAdmit(makeSubmit(1, 2,
                                 {makeDegradationEligibleOwnerWithDeadline(
                                      0, 16, 1'000'000'010),
                                  makeDegradationEligibleOwnerWithDeadline(
                                      1, 16, 1'000'000'011)}),
                      ids)
            .code(),
        Status::Code::kOk);
    std::vector<QueueOwnerId> dropped;
    EXPECT_EQ(queue.pickForDispatch(4, 1 << 20, &dropped),
              (std::vector<QueueOwnerId>{1, 2}));
    EXPECT_TRUE(dropped.empty());
    EXPECT_EQ(queue.probeStats().dispatched, 0u);
}

// The probe's collateral when the estimate is right. A large, urgent owner
// that genuinely cannot make its deadline is the probe; its bytes join the
// queue ahead of everyone behind it, so a small, looser owner that was
// feasible on its own is now judged behind 1000 bytes it did not have to wait
// for and is dropped instead. Without the probe the large owner is dropped
// and the small one goes: the probe trades a feasible transfer for an
// infeasible one whenever the estimate was telling the truth. (Dispatching
// the probe after the feasible owners would avoid this, but a pick whose
// owner budget the feasible owners fill every time would then never have
// room for it, and an owner held back for a probe that never comes is
// neither transferred nor signalled.)
TEST(AdmissionQueueTest, Step3ProbeCanDropTheFeasibleOwnerBehindIt) {
    // 1 B per ns; A: 1000 B in a 400 ns window (MLU 2.5); B: 100 B in a
    // 600 ns window (MLU 0.17 alone, 1.83 behind A).
    auto admitBoth = [](LocalTransferAdmissionQueue& queue) {
        std::vector<QueueOwnerId> ids;
        ASSERT_EQ(
            queue
                .tryAdmit(makeSubmit(1, 2,
                                     {makeDegradationEligibleOwnerWithDeadline(
                                          0, 1000, 1'000'000'400),
                                      makeDegradationEligibleOwnerWithDeadline(
                                          1, 100, 1'000'000'600)}),
                          ids)
                .code(),
            Status::Code::kOk);
    };

    {
        LocalTransferAdmissionQueue queue(step3Limits(1.5));
        queue.setDegradationPolicy([] { return 1e9; }, DegradationHooks{},
                                   [] { return uint64_t{1'000'000'000}; });
        admitBoth(queue);
        std::vector<QueueOwnerId> dropped;
        EXPECT_EQ(queue.pickForDispatch(4, 1 << 20, &dropped),
                  std::vector<QueueOwnerId>{2});           // B goes
        EXPECT_EQ(dropped, std::vector<QueueOwnerId>{1});  // A dropped
    }
    {
        QueueLimits limits = step3Limits(1.5);
        limits.mlu_probe_owners = 1;
        LocalTransferAdmissionQueue queue(limits);
        queue.setDegradationPolicy([] { return 1e9; }, DegradationHooks{},
                                   [] { return uint64_t{1'000'000'000}; });
        admitBoth(queue);
        std::vector<QueueOwnerId> dropped;
        EXPECT_EQ(queue.pickForDispatch(4, 1 << 20, &dropped),
                  std::vector<QueueOwnerId>{1});           // A probed
        EXPECT_EQ(dropped, std::vector<QueueOwnerId>{2});  // B collateral
        EXPECT_EQ(queue.probeStats().dispatched, 1u);
        EXPECT_EQ(queue.dispatchingBytes(), 1000u);
    }
}

// Of several rejected owners the most urgent is the probe -- EDF order, the
// same order the drop meets them in -- not the smallest or the largest: its
// verdict is the soonest to arrive, and size is left out of the choice on
// purpose (a bigger probe lifts the meter more per round, and costs more
// per pick when the estimate is right). The rest are judged behind it.
TEST(AdmissionQueueTest, Step3ProbeIsTheMostUrgentRejectedOwner) {
    QueueLimits limits = step3Limits(1.5);
    limits.mlu_probe_owners = 1;
    LocalTransferAdmissionQueue queue(limits);
    queue.setDegradationPolicy([] { return 1e9; }, DegradationHooks{},
                               [] { return uint64_t{1'000'000'000}; });

    // 1 B per ns. Owner 1: 28 B in 10 ns (MLU 2.8), the most urgent and the
    // largest. Owner 2: 16 B in 10 ns (MLU 1.6 alone), same deadline,
    // admitted after 1 so behind it in EDF order, and the smallest. Owner 3:
    // 20 B in 12 ns (MLU 1.67 alone). All three are rejected; 1 is the probe.
    std::vector<QueueOwnerId> ids;
    ASSERT_EQ(
        queue
            .tryAdmit(makeSubmit(1, 3,
                                 {makeDegradationEligibleOwnerWithDeadline(
                                      0, 28, 1'000'000'010),
                                  makeDegradationEligibleOwnerWithDeadline(
                                      1, 16, 1'000'000'010),
                                  makeDegradationEligibleOwnerWithDeadline(
                                      2, 20, 1'000'000'012)}),
                      ids)
            .code(),
        Status::Code::kOk);
    std::vector<QueueOwnerId> dropped;
    EXPECT_EQ(queue.pickForDispatch(4, 1 << 20, &dropped),
              std::vector<QueueOwnerId>{1});
    EXPECT_EQ(dropped, (std::vector<QueueOwnerId>{2, 3}));
    EXPECT_EQ(queue.probeStats().dispatched, 1u);
}

// An owner far past the threshold is infeasible whatever the estimate says;
// probing it buys a late transfer and no information. With the default
// ceiling of 2 x theta, an owner at MLU 3.2 is dropped with budget to spare
// while one at 1.6 is probed, budget for both notwithstanding; with the
// ceiling removed the 3.2 owner is a candidate like any other and goes too.
TEST(AdmissionQueueTest, Step3ProbeSkipsOwnersFarPastTheThreshold) {
    auto admitBoth = [](LocalTransferAdmissionQueue& queue) {
        std::vector<QueueOwnerId> ids;
        // 1 B per ns. Owner 1: 32 B in 10 ns (MLU 3.2); owner 2: 16 B in
        // 10 ns (MLU 1.6). Same deadline, so EDF keeps admission order.
        ASSERT_EQ(
            queue
                .tryAdmit(makeSubmit(1, 2,
                                     {makeDegradationEligibleOwnerWithDeadline(
                                          0, 32, 1'000'000'010),
                                      makeDegradationEligibleOwnerWithDeadline(
                                          1, 16, 1'000'000'010)}),
                          ids)
                .code(),
            Status::Code::kOk);
    };

    {
        QueueLimits limits = step3Limits(1.5);  // ceiling 3.0
        limits.mlu_probe_owners = 1;
        LocalTransferAdmissionQueue queue(limits);
        queue.setDegradationPolicy([] { return 1e9; }, DegradationHooks{},
                                   [] { return uint64_t{1'000'000'000}; });
        admitBoth(queue);
        std::vector<QueueOwnerId> dropped;
        EXPECT_EQ(queue.pickForDispatch(4, 1 << 20, &dropped),
                  std::vector<QueueOwnerId>{2});
        EXPECT_EQ(dropped, std::vector<QueueOwnerId>{1});
        EXPECT_EQ(queue.probeStats().dispatched, 1u);
    }
    {
        QueueLimits limits = step3Limits(1.5);
        limits.mlu_probe_owners = 2;  // budget for both, ceiling drops one
        LocalTransferAdmissionQueue queue(limits);
        queue.setDegradationPolicy([] { return 1e9; }, DegradationHooks{},
                                   [] { return uint64_t{1'000'000'000}; });
        admitBoth(queue);
        std::vector<QueueOwnerId> dropped;
        EXPECT_EQ(queue.pickForDispatch(4, 1 << 20, &dropped),
                  std::vector<QueueOwnerId>{2});
        EXPECT_EQ(dropped, std::vector<QueueOwnerId>{1});
    }
    {
        QueueLimits limits = step3Limits(1.5);
        limits.mlu_probe_owners = 2;
        limits.mlu_probe_ceiling_factor = 0.0;  // no ceiling
        LocalTransferAdmissionQueue queue(limits);
        queue.setDegradationPolicy([] { return 1e9; }, DegradationHooks{},
                                   [] { return uint64_t{1'000'000'000}; });
        admitBoth(queue);
        std::vector<QueueOwnerId> dropped;
        EXPECT_EQ(queue.pickForDispatch(4, 1 << 20, &dropped),
                  (std::vector<QueueOwnerId>{1, 2}));  // EDF order
        EXPECT_TRUE(dropped.empty());
        EXPECT_EQ(queue.probeStats().dispatched, 2u);
    }
}

// complete() hands back the probe's verdict and tallies it: COMPLETED inside
// the window is a met deadline; a late completion or any other terminal
// status is a miss; an owner that was not a probe reports nothing.
TEST(AdmissionQueueTest, Step3ProbeVerdictIsJudgedAtCompletion) {
    QueueLimits limits = step3Limits(1.5);
    limits.mlu_probe_owners = 1;
    LocalTransferAdmissionQueue queue(limits);
    uint64_t now = 1'000'000'000;
    queue.setDegradationPolicy([] { return 1e9; }, DegradationHooks{},
                               [&now] { return now; });
    using Outcome = LocalTransferAdmissionQueue::ProbeOutcome;

    // Three rounds, one owner each: 16 B in a 10 ns window is MLU 1.6, the
    // probe every time. A fourth owner is feasible and never a probe.
    auto admitProbeCandidate = [&](uint64_t token) {
        std::vector<QueueOwnerId> ids;
        ASSERT_EQ(
            queue
                .tryAdmit(makeSubmit(token, 1,
                                     {makeDegradationEligibleOwnerWithDeadline(
                                         0, 16, now + 10)}),
                          ids)
                .code(),
            Status::Code::kOk);
        ASSERT_EQ(queue.pickForDispatch(4, 1 << 20).size(), 1u);
    };

    // 1: completes before its deadline -> met.
    admitProbeCandidate(1);
    Outcome out = Outcome::MissedDeadline;
    ASSERT_EQ(queue.complete(1, TransferStatusEnum::COMPLETED, &out).code(),
              Status::Code::kOk);
    EXPECT_EQ(out, Outcome::MetDeadline);

    // 2: completes, but the clock has passed its deadline -> missed.
    admitProbeCandidate(2);
    now += 10;  // exactly at the deadline is not inside the window
    ASSERT_EQ(queue.complete(2, TransferStatusEnum::COMPLETED, &out).code(),
              Status::Code::kOk);
    EXPECT_EQ(out, Outcome::MissedDeadline);

    // 3: fails inside the window -> missed all the same.
    admitProbeCandidate(3);
    ASSERT_EQ(queue.complete(3, TransferStatusEnum::FAILED, &out).code(),
              Status::Code::kOk);
    EXPECT_EQ(out, Outcome::MissedDeadline);

    // 4: a feasible owner is not a probe and reports None.
    std::vector<QueueOwnerId> ids;
    ASSERT_EQ(
        queue
            .tryAdmit(makeSubmit(4, 1,
                                 {makeDegradationEligibleOwnerWithDeadline(
                                     0, 16, now + 1'000'000)}),
                      ids)
            .code(),
        Status::Code::kOk);
    ASSERT_EQ(queue.pickForDispatch(4, 1 << 20), std::vector<QueueOwnerId>{4});
    out = Outcome::MetDeadline;
    ASSERT_EQ(queue.complete(4, TransferStatusEnum::COMPLETED, &out).code(),
              Status::Code::kOk);
    EXPECT_EQ(out, Outcome::None);

    const auto stats = queue.probeStats();
    EXPECT_EQ(stats.dispatched, 3u);
    EXPECT_EQ(stats.met_deadline, 1u);
    EXPECT_EQ(stats.missed_deadline, 2u);
}

// A probe is still an owner: with no room left in the dispatch budget it
// waits like a feasible one would, and is not dropped for lack of room.
TEST(AdmissionQueueTest, Step3ProbeRespectsTheDispatchBudget) {
    QueueLimits limits = step3Limits(1.5);
    limits.mlu_probe_owners = 1;
    LocalTransferAdmissionQueue queue(limits);
    queue.setDegradationPolicy([] { return 1e9; }, DegradationHooks{},
                               [] { return uint64_t{1'000'000'000}; });

    std::vector<QueueOwnerId> ids;
    ASSERT_EQ(
        queue
            .tryAdmit(makeSubmit(1, 1,
                                 {makeDegradationEligibleOwnerWithDeadline(
                                     0, 16, 1'000'000'010)}),
                      ids)
            .code(),
        Status::Code::kOk);
    std::vector<QueueOwnerId> dropped;
    EXPECT_TRUE(queue.pickForDispatch(4, /*max_bytes=*/8, &dropped).empty());
    EXPECT_TRUE(dropped.empty());
    EXPECT_EQ(queue.probeStats().dispatched, 0u);  // left queued, not counted
    EXPECT_EQ(queue.outstandingOwners(), 1u);
    EXPECT_EQ(queue.pickForDispatch(4, 1 << 20, &dropped),
              std::vector<QueueOwnerId>{1});
    EXPECT_EQ(queue.probeStats().dispatched, 1u);
}

// A threshold <= 0 disables the drop; a negative one must not leak through
// the predictor's "nothing to predict" sentinel and drop (or probe) owners.
TEST(AdmissionQueueTest, Step3NegativeThresholdDropsNothing) {
    QueueLimits limits = step3Limits(/*theta_local=*/-2.0);
    limits.mlu_probe_owners = 1;
    LocalTransferAdmissionQueue queue(limits);
    queue.setDegradationPolicy([] { return 1e9; }, DegradationHooks{},
                               [] { return uint64_t{1'000'000'000}; });

    std::vector<QueueOwnerId> ids;
    ASSERT_EQ(
        queue
            .tryAdmit(makeSubmit(1, 1,
                                 {makeDegradationEligibleOwnerWithDeadline(
                                     0, 16, 1'000'000'010)}),
                      ids)
            .code(),
        Status::Code::kOk);
    std::vector<QueueOwnerId> dropped;
    EXPECT_EQ(queue.pickForDispatch(4, 1 << 20, &dropped),
              std::vector<QueueOwnerId>{1});
    EXPECT_TRUE(dropped.empty());
    EXPECT_EQ(queue.probeStats().dispatched, 0u);
}

TEST(AdmissionQueueTest, Step3DropsAlreadyExpiredDeadline) {
    LocalTransferAdmissionQueue queue(step3Limits(1.5));
    queue.setDegradationPolicy([] { return 1e9; }, DegradationHooks{},
                               [] { return uint64_t{2'000'000'000}; });

    std::vector<QueueOwnerId> admitted_ids;
    // deadline 1e9 < now 2e9 → already past → dropped.
    auto status = queue.tryAdmit(
        makeSubmit(
            1, 1,
            {makeDegradationEligibleOwnerWithDeadline(0, 16, 1'000'000'000)}),
        admitted_ids);
    ASSERT_EQ(status.code(), Status::Code::kOk);

    std::vector<QueueOwnerId> dropped;
    auto picked = queue.pickForDispatch(4, 1 << 20, &dropped);
    EXPECT_TRUE(picked.empty());
    ASSERT_EQ(dropped.size(), 1u);
    EXPECT_EQ(dropped[0], 1u);
}

TEST(AdmissionQueueTest, Step3DisabledWhenThresholdZero) {
    // θ_local = 0 (default off): even a hopeless deadline is dispatched, and
    // the dropped vector stays empty — behavior is pure step-2 EDF.
    LocalTransferAdmissionQueue queue(step3Limits(0.0));
    queue.setDegradationPolicy([] { return 1e9; }, DegradationHooks{},
                               [] { return uint64_t{1'000'000'000}; });

    std::vector<QueueOwnerId> admitted_ids;
    auto status = queue.tryAdmit(
        makeSubmit(
            1, 1,
            {makeDegradationEligibleOwnerWithDeadline(0, 16, 1'000'000'001)}),
        admitted_ids);
    ASSERT_EQ(status.code(), Status::Code::kOk);

    std::vector<QueueOwnerId> dropped;
    auto picked = queue.pickForDispatch(4, 1 << 20, &dropped);
    ASSERT_EQ(picked.size(), 1u);
    EXPECT_EQ(picked[0], 1u);
    EXPECT_TRUE(dropped.empty());
}

TEST(AdmissionQueueTest, Step3NoDropWithoutBandwidthProvider) {
    // Threshold set but no bandwidth provider → cannot predict → never drops.
    LocalTransferAdmissionQueue queue(step3Limits(1.5));
    std::vector<QueueOwnerId> admitted_ids;
    auto status = queue.tryAdmit(
        makeSubmit(1, 1, {makeOwnerWithDeadline(0, 16, 1'000'000'001)}),
        admitted_ids);
    ASSERT_EQ(status.code(), Status::Code::kOk);

    std::vector<QueueOwnerId> dropped;
    auto picked = queue.pickForDispatch(4, 1 << 20, &dropped);
    ASSERT_EQ(picked.size(), 1u);
    EXPECT_TRUE(dropped.empty());
}

// A provider that cannot report bandwidth (<= 0, e.g. every RDMA NIC is
// unavailable) disables the prediction outright: nothing is dropped, not
// even an owner whose deadline has already passed, because "no estimate"
// must not be read as "infeasible".
TEST(AdmissionQueueTest, Step3NoDropWhenBandwidthIsUnknown) {
    for (const double reported : {0.0, -1.0}) {
        LocalTransferAdmissionQueue queue(step3Limits(1.5));
        int hook_calls = 0;
        DegradationHooks hooks;
        hooks.on_local_decode_suggested = [&](const Request&) { ++hook_calls; };
        queue.setDegradationPolicy([reported] { return reported; }, hooks,
                                   [] { return uint64_t{2'000'000'000}; });

        std::vector<QueueOwnerId> admitted_ids;
        // Deadline 1e9 is already behind now = 2e9.
        ASSERT_EQ(
            queue
                .tryAdmit(makeSubmit(1, 1,
                                     {makeDegradationEligibleOwnerWithDeadline(
                                         0, 16, 1'000'000'000)}),
                          admitted_ids)
                .code(),
            Status::Code::kOk);

        std::vector<QueueOwnerId> dropped;
        auto picked = queue.pickForDispatch(4, 1 << 20, &dropped);
        EXPECT_EQ(picked.size(), 1u) << "bandwidth " << reported;
        EXPECT_TRUE(dropped.empty()) << "bandwidth " << reported;
        EXPECT_EQ(hook_calls, 0) << "bandwidth " << reported;
    }
}

// Drop enabled with a healthy bandwidth: an eligible owner that carries no
// deadline has nothing to miss and is dispatched, whatever the threshold.
TEST(AdmissionQueueTest, Step3NoDropWithoutDeadline) {
    for (const double theta : {1.5, 1e-9}) {
        LocalTransferAdmissionQueue queue(step3Limits(theta));
        int hook_calls = 0;
        DegradationHooks hooks;
        hooks.on_local_decode_suggested = [&](const Request&) { ++hook_calls; };
        queue.setDegradationPolicy([] { return 1e9; }, hooks,
                                   [] { return uint64_t{1'000'000'000}; });

        std::vector<QueueOwnerId> admitted_ids;
        ASSERT_EQ(
            queue
                .tryAdmit(makeSubmit(1, 1,
                                     {makeDegradationEligibleOwnerWithDeadline(
                                         0, 1 << 20, /*deadline_ns=*/0)}),
                          admitted_ids)
                .code(),
            Status::Code::kOk);

        std::vector<QueueOwnerId> dropped;
        auto picked = queue.pickForDispatch(4, 1 << 20, &dropped);
        EXPECT_EQ(picked.size(), 1u) << "theta " << theta;
        EXPECT_TRUE(dropped.empty()) << "theta " << theta;
        EXPECT_EQ(hook_calls, 0) << "theta " << theta;
    }
}

TEST(AdmissionQueueTest, Step3DynamicBandwidthProvider) {
    LocalTransferAdmissionQueue queue(step3Limits(1.5));
    std::atomic<double> live_bw{1e9};
    int hook_calls = 0;
    DegradationHooks hooks;
    hooks.on_local_decode_suggested = [&](const Request&) { ++hook_calls; };
    queue.setDegradationPolicy([&] { return live_bw.load(); }, hooks,
                               [] { return uint64_t{1'000'000'000}; });

    std::vector<QueueOwnerId> admitted_ids;
    // At 1e9 B/s: time=16ns, window=10ns, MLU=1.6 >= 1.5 -> DROP.
    auto status = queue.tryAdmit(
        makeSubmit(
            1, 1,
            {makeDegradationEligibleOwnerWithDeadline(0, 16, 1'000'000'010)}),
        admitted_ids);
    ASSERT_EQ(status.code(), Status::Code::kOk);

    std::vector<QueueOwnerId> dropped;
    auto picked = queue.pickForDispatch(4, 1 << 20, &dropped);
    EXPECT_TRUE(picked.empty());
    ASSERT_EQ(dropped.size(), 1u);
    EXPECT_EQ(hook_calls, 1);

    // Increase bandwidth 10x -> same profile becomes feasible.
    live_bw.store(1e10);
    status = queue.tryAdmit(
        makeSubmit(
            2, 1,
            {makeDegradationEligibleOwnerWithDeadline(0, 16, 1'000'000'010)}),
        admitted_ids);
    ASSERT_EQ(status.code(), Status::Code::kOk);

    dropped.clear();
    picked = queue.pickForDispatch(4, 1 << 20, &dropped);
    // At 1e10 B/s: time=1.6ns, window=10ns, MLU=0.16 < 1.5 -> OK.
    ASSERT_EQ(picked.size(), 1u);
    EXPECT_TRUE(dropped.empty());
    EXPECT_EQ(hook_calls, 1);
}

TEST(AdmissionQueueTest, Step3SkipsNonRdmaOwner) {
    LocalTransferAdmissionQueue queue(step3Limits(1.5));
    int hook_calls = 0;
    DegradationHooks hooks;
    hooks.on_local_decode_suggested = [&](const Request&) { ++hook_calls; };
    queue.setDegradationPolicy([] { return 1e9; }, hooks,
                               [] { return uint64_t{1'000'000'000}; });

    auto owner = makeOwnerWithDeadline(0, 16, 1'000'000'010);
    owner.degradation_eligible = false;
    std::vector<QueueOwnerId> admitted_ids;
    auto status = queue.tryAdmit(makeSubmit(1, 1, {owner}), admitted_ids);
    ASSERT_EQ(status.code(), Status::Code::kOk);

    std::vector<QueueOwnerId> dropped;
    auto picked = queue.pickForDispatch(4, 1 << 20, &dropped);
    ASSERT_EQ(picked.size(), 1u);
    EXPECT_TRUE(dropped.empty());
    EXPECT_EQ(hook_calls, 0);
}

TEST(AdmissionQueueTest, Step3RequiresExplicitDegradationEligibility) {
    LocalTransferAdmissionQueue queue(step3Limits(1.5));
    int hook_calls = 0;
    DegradationHooks hooks;
    hooks.on_local_decode_suggested = [&](const Request&) { ++hook_calls; };
    queue.setDegradationPolicy([] { return 1e9; }, hooks,
                               [] { return uint64_t{1'000'000'000}; });

    std::vector<QueueOwnerId> admitted_ids;
    auto status = queue.tryAdmit(
        makeSubmit(1, 1, {makeOwnerWithDeadline(0, 16, 1'000'000'010)}),
        admitted_ids);
    ASSERT_EQ(status.code(), Status::Code::kOk);

    std::vector<QueueOwnerId> dropped;
    auto picked = queue.pickForDispatch(4, 1 << 20, &dropped);
    ASSERT_EQ(picked.size(), 1u);
    EXPECT_TRUE(dropped.empty());
    EXPECT_EQ(hook_calls, 0);
}

// --- Deadline proximity promotion (step 4) --------------------------------

QueueLimits promotionLimits(uint64_t slack_ns) {
    QueueLimits limits{8, 1 << 20, 0, 0};
    limits.deadline_aware = true;
    limits.promotion_slack_ns = slack_ns;
    return limits;
}

TEST(AdmissionQueueTest, PromotionDisabledKeepsEdfOrder) {
    QueueLimits limits{4, 4096, 0, 0};
    limits.deadline_aware = true;
    LocalTransferAdmissionQueue queue(limits);
    queue.setDegradationPolicy(nullptr, DegradationHooks{},
                               [] { return uint64_t{1000}; });

    std::vector<QueueOwnerId> ids;
    auto status =
        queue.tryAdmit(makeSubmit(1, 3,
                                  {makeOwnerWithDeadline(0, 16, 2000),
                                   makeOwnerWithDeadline(1, 16, 1500),
                                   makeOwnerWithDeadline(2, 16, 1800)}),
                       ids);
    ASSERT_EQ(status.code(), Status::Code::kOk);

    auto picked = queue.pickForDispatch(4, 4096);
    const std::vector<QueueOwnerId> expected{2, 3, 1};
    EXPECT_EQ(picked, expected);
}

TEST(AdmissionQueueTest, PromotionMovesUrgentOwnersToFront) {
    LocalTransferAdmissionQueue queue(promotionLimits(500));
    queue.setDegradationPolicy(nullptr, DegradationHooks{},
                               [] { return uint64_t{1000}; });

    std::vector<QueueOwnerId> ids;
    auto status =
        queue.tryAdmit(makeSubmit(1, 3,
                                  {makeOwnerWithDeadline(0, 16, 2000),
                                   makeOwnerWithDeadline(1, 16, 1400),
                                   makeOwnerWithDeadline(2, 16, 1300)}),
                       ids);
    ASSERT_EQ(status.code(), Status::Code::kOk);

    auto picked = queue.pickForDispatch(4, 1 << 20);
    const std::vector<QueueOwnerId> expected{3, 2, 1};
    EXPECT_EQ(picked, expected);
}

TEST(AdmissionQueueTest, PromotionReordersAcrossSeparateAdmits) {
    LocalTransferAdmissionQueue queue(promotionLimits(2000));
    queue.setDegradationPolicy(nullptr, DegradationHooks{},
                               [] { return uint64_t{5000}; });

    std::vector<QueueOwnerId> ids;
    auto s1 = queue.tryAdmit(
        makeSubmit(1, 1, {makeOwnerWithDeadline(0, 16, 10000)}), ids);
    ASSERT_EQ(s1.code(), Status::Code::kOk);
    auto s2 = queue.tryAdmit(
        makeSubmit(2, 1, {makeOwnerWithDeadline(0, 16, 6500)}), ids);
    ASSERT_EQ(s2.code(), Status::Code::kOk);
    auto s3 = queue.tryAdmit(
        makeSubmit(3, 1, {makeOwnerWithDeadline(0, 16, 6000)}), ids);
    ASSERT_EQ(s3.code(), Status::Code::kOk);

    auto picked = queue.pickForDispatch(4, 1 << 20);
    const std::vector<QueueOwnerId> expected{3, 2, 1};
    EXPECT_EQ(picked, expected);
}

TEST(AdmissionQueueTest, PromotionSkipsNoDeadlineOwners) {
    LocalTransferAdmissionQueue queue(promotionLimits(5000));
    queue.setDegradationPolicy(nullptr, DegradationHooks{},
                               [] { return uint64_t{1000}; });

    std::vector<QueueOwnerId> ids;
    auto status =
        queue.tryAdmit(makeSubmit(1, 2,
                                  {makeOwnerWithDeadline(0, 16, 0),
                                   makeOwnerWithDeadline(1, 16, 2000)}),
                       ids);
    ASSERT_EQ(status.code(), Status::Code::kOk);

    auto picked = queue.pickForDispatch(4, 1 << 20);
    const std::vector<QueueOwnerId> expected{2, 1};
    EXPECT_EQ(picked, expected);
}

TEST(AdmissionQueueTest, PromotionPreservesEdfWithinPromotedGroup) {
    LocalTransferAdmissionQueue queue(promotionLimits(2000));
    queue.setDegradationPolicy(nullptr, DegradationHooks{},
                               [] { return uint64_t{1000}; });

    std::vector<QueueOwnerId> ids;
    auto status =
        queue.tryAdmit(makeSubmit(1, 3,
                                  {makeOwnerWithDeadline(0, 16, 2500),
                                   makeOwnerWithDeadline(1, 16, 2200),
                                   makeOwnerWithDeadline(2, 16, 2800)}),
                       ids);
    ASSERT_EQ(status.code(), Status::Code::kOk);

    auto picked = queue.pickForDispatch(4, 1 << 20);
    const std::vector<QueueOwnerId> expected{2, 1, 3};
    EXPECT_EQ(picked, expected);
}

TEST(AdmissionQueueTest, PromotionCoexistsWithStep3Drop) {
    QueueLimits limits = promotionLimits(500);
    limits.mlu_local_threshold = 1.5;
    LocalTransferAdmissionQueue queue(limits);
    int hook_calls = 0;
    DegradationHooks hooks;
    hooks.on_local_decode_suggested = [&](const Request&) { ++hook_calls; };
    queue.setDegradationPolicy([] { return 1e9; }, hooks,
                               [] { return uint64_t{1000}; });

    std::vector<QueueOwnerId> ids;
    auto status = queue.tryAdmit(
        makeSubmit(1, 3,
                   {makeDegradationEligibleOwnerWithDeadline(0, 16, 1010),
                    makeDegradationEligibleOwnerWithDeadline(1, 16, 1400),
                    makeDegradationEligibleOwnerWithDeadline(2, 16, 5000)}),
        ids);
    ASSERT_EQ(status.code(), Status::Code::kOk);

    std::vector<QueueOwnerId> dropped;
    auto picked = queue.pickForDispatch(4, 1 << 20, &dropped);

    const std::vector<QueueOwnerId> exp_pick{2, 3};
    const std::vector<QueueOwnerId> exp_drop{1};
    EXPECT_EQ(picked, exp_pick);
    EXPECT_EQ(dropped, exp_drop);
    EXPECT_EQ(hook_calls, 1);
}

TEST(AdmissionQueueTest, PromotionWithAdvancingTime) {
    QueueLimits limits = promotionLimits(500);
    LocalTransferAdmissionQueue queue(limits);

    uint64_t fake_now = 1000;
    queue.setDegradationPolicy(nullptr, DegradationHooks{},
                               [&] { return fake_now; });

    std::vector<QueueOwnerId> ids;
    auto s1 = queue.tryAdmit(
        makeSubmit(1, 1, {makeOwnerWithDeadline(0, 16, 1800)}), ids);
    ASSERT_EQ(s1.code(), Status::Code::kOk);
    auto s2 = queue.tryAdmit(
        makeSubmit(2, 1, {makeOwnerWithDeadline(0, 16, 1400)}), ids);
    ASSERT_EQ(s2.code(), Status::Code::kOk);
    auto s3 = queue.tryAdmit(
        makeSubmit(3, 1, {makeOwnerWithDeadline(0, 16, 3000)}), ids);
    ASSERT_EQ(s3.code(), Status::Code::kOk);

    auto picked1 = queue.pickForDispatch(1, 1 << 20);
    ASSERT_EQ(picked1.size(), 1u);
    EXPECT_EQ(picked1[0], 2u);

    auto cstatus = queue.complete(2, TransferStatusEnum::COMPLETED);
    ASSERT_EQ(cstatus.code(), Status::Code::kOk);

    fake_now = 1500;
    auto picked2 = queue.pickForDispatch(2, 1 << 20);
    const std::vector<QueueOwnerId> expected2{1, 3};
    EXPECT_EQ(picked2, expected2);
}

TEST(AdmissionQueueTest, PromotionDisabledWithoutDeadlineAware) {
    QueueLimits limits{4, 4096, 0, 0};
    limits.deadline_aware = false;
    limits.promotion_slack_ns = 5000;
    LocalTransferAdmissionQueue queue(limits);
    queue.setDegradationPolicy(nullptr, DegradationHooks{},
                               [] { return uint64_t{1000}; });

    std::vector<QueueOwnerId> ids;
    auto status =
        queue.tryAdmit(makeSubmit(1, 3,
                                  {makeOwnerWithDeadline(0, 16, 1200),
                                   makeOwnerWithDeadline(1, 16, 5000),
                                   makeOwnerWithDeadline(2, 16, 1100)}),
                       ids);
    ASSERT_EQ(status.code(), Status::Code::kOk);

    auto picked = queue.pickForDispatch(4, 4096);
    const std::vector<QueueOwnerId> expected{1, 2, 3};
    EXPECT_EQ(picked, expected);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake

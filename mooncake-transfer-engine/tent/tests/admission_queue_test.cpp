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

#include <atomic>
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

TEST(AdmissionQueueTest, DeferredDispatchReturnsOwnerToQueue) {
    LocalTransferAdmissionQueue queue({2, 128, 0, 0});
    std::vector<QueueOwnerId> admitted_ids;
    ASSERT_TRUE(
        queue.tryAdmit(makeSubmit(1, 1, {makeOwner(0, 16)}), admitted_ids)
            .ok());

    auto picked = queue.pickForDispatch(1, 16);
    ASSERT_EQ(picked.size(), 1u);
    EXPECT_TRUE(queue.deferDispatch(picked[0]).ok());
    EXPECT_TRUE(queue.deferDispatch(picked[0]).IsInvalidEntry());

    auto retried = queue.pickForDispatch(1, 16);
    ASSERT_EQ(retried.size(), 1u);
    EXPECT_EQ(retried[0], picked[0]);
    EXPECT_TRUE(queue.complete(retried[0], TransferStatusEnum::COMPLETED).ok());
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

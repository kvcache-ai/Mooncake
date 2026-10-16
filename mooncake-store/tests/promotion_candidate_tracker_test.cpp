#include "promotion_candidate_tracker.h"

#include <chrono>
#include <limits>
#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace mooncake {
namespace {

using Clock = std::chrono::steady_clock;
using Key = PromotionCandidateTracker::Key;

const TenantId kTenant = TenantId::Default();

std::vector<Key> AllFiled(const PromotionCandidateTracker& tracker) {
    return tracker.Due(Clock::time_point::max(),
                       std::numeric_limits<size_t>::max());
}

TEST(PromotionCandidateTrackerTest, RecordFilesTheKeyAsDueNow) {
    PromotionCandidateTracker tracker;
    route::ObjectState state;
    EXPECT_TRUE(tracker.Empty());

    tracker.RecordLocked(kTenant, "k", state, /*sketch_score=*/3,
                         PromotionCandidateReason::kWatermark, ErrorCode::OK);

    ASSERT_TRUE(state.promotion_candidate);
    EXPECT_EQ(state.promotion_candidate->sketch_score, 3);
    EXPECT_FALSE(tracker.Empty());
    const auto due = tracker.Due(Clock::now(), 16);
    ASSERT_EQ(due.size(), 1u);
    EXPECT_EQ(due[0], (Key{kTenant, "k"}));
    // A candidate is not work in flight.
    EXPECT_FALSE(state.HasWorkInFlight());
}

TEST(PromotionCandidateTrackerTest, RefreshKeepsFailureHistoryAndOneEntry) {
    PromotionCandidateTracker tracker;
    route::ObjectState state;
    tracker.RecordLocked(kTenant, "k", state, 5,
                         PromotionCandidateReason::kExecutionFailed,
                         ErrorCode::OK, /*execution_failures=*/2);
    tracker.BackoffLocked(kTenant, "k", state,
                          PromotionQueueResult::kQueueCapRejected);
    ASSERT_EQ(state.promotion_candidate->retry_count, 1u);

    tracker.RecordLocked(kTenant, "k", state, 1,
                         PromotionCandidateReason::kWatermark, ErrorCode::OK);

    const PromotionCandidate& candidate = *state.promotion_candidate;
    EXPECT_EQ(candidate.retry_count, 0u);
    EXPECT_EQ(candidate.execution_failures, 2u);
    EXPECT_EQ(candidate.sketch_score, 5) << "the score only ever rises";
    EXPECT_EQ(candidate.last_reason, PromotionCandidateReason::kWatermark);
    EXPECT_EQ(AllFiled(tracker).size(), 1u);
    EXPECT_EQ(tracker.Due(Clock::now(), 16).size(), 1u)
        << "new demand makes the candidate due again";
}

TEST(PromotionCandidateTrackerTest, EraseAndConsumeUnfile) {
    PromotionCandidateTracker tracker;
    route::ObjectState a;
    route::ObjectState b;
    tracker.RecordLocked(kTenant, "a", a, 1,
                         PromotionCandidateReason::kQueueCap, ErrorCode::OK);
    tracker.RecordLocked(kTenant, "b", b, 1,
                         PromotionCandidateReason::kExecutionFailed,
                         ErrorCode::OK, /*execution_failures=*/4);

    tracker.EraseLocked(kTenant, "a", a);
    EXPECT_FALSE(a.promotion_candidate);
    EXPECT_EQ(tracker.ConsumeLocked(kTenant, "b", b), 4u);
    EXPECT_FALSE(b.promotion_candidate);
    EXPECT_EQ(tracker.ConsumeLocked(kTenant, "b", b), 0u)
        << "no candidate is a fresh chain";
    EXPECT_TRUE(tracker.Empty());
    EXPECT_TRUE(AllFiled(tracker).empty());
}

TEST(PromotionCandidateTrackerTest, BackoffRefilesInTheFuture) {
    PromotionCandidateTracker tracker;
    route::ObjectState state;
    tracker.RecordLocked(kTenant, "k", state, 1,
                         PromotionCandidateReason::kQueueCap, ErrorCode::OK);

    tracker.BackoffLocked(kTenant, "k", state,
                          PromotionQueueResult::kPushFailed);

    const PromotionCandidate& candidate = *state.promotion_candidate;
    EXPECT_EQ(candidate.retry_count, 1u);
    EXPECT_EQ(candidate.last_reason, PromotionCandidateReason::kPushFailed);
    const auto filed_at = candidate.retry_after;
    EXPECT_TRUE(
        tracker.Due(filed_at - std::chrono::nanoseconds(1), 16).empty());
    EXPECT_EQ(tracker.Due(filed_at, 16).size(), 1u);
    EXPECT_FALSE(tracker.DueLocked(kTenant, "k", state,
                                   filed_at - std::chrono::nanoseconds(1)));
    EXPECT_TRUE(tracker.DueLocked(kTenant, "k", state, filed_at));
}

TEST(PromotionCandidateTrackerTest, DueListsTheLongestOverdueFirstUpToLimit) {
    PromotionCandidateTracker tracker;
    route::ObjectState first;
    route::ObjectState second;
    route::ObjectState third;
    tracker.RecordLocked(kTenant, "first", first, 1,
                         PromotionCandidateReason::kQueueCap, ErrorCode::OK);
    tracker.RecordLocked(kTenant, "second", second, 1,
                         PromotionCandidateReason::kQueueCap, ErrorCode::OK);
    tracker.RecordLocked(kTenant, "third", third, 1,
                         PromotionCandidateReason::kQueueCap, ErrorCode::OK);
    // Backing "first" off moves it behind the others.
    tracker.BackoffLocked(kTenant, "first", first,
                          PromotionQueueResult::kQueueCapRejected);

    const auto due = AllFiled(tracker);
    ASSERT_EQ(due.size(), 3u);
    EXPECT_EQ(due.back(), (Key{kTenant, "first"}));
    EXPECT_EQ(tracker.Due(Clock::time_point::max(), 2).size(), 2u);
}

TEST(PromotionCandidateTrackerTest, StaleCandidateIsDroppedWhenDue) {
    PromotionCandidateTracker tracker;
    route::ObjectState state;
    tracker.RecordLocked(kTenant, "k", state, 1,
                         PromotionCandidateReason::kWatermark, ErrorCode::OK);

    // Past the TTL, the candidate is dropped instead of retried.
    EXPECT_FALSE(tracker.DueLocked(kTenant, "k", state,
                                   Clock::now() + std::chrono::hours(1)));
    EXPECT_FALSE(state.promotion_candidate);
    EXPECT_TRUE(tracker.Empty());
    EXPECT_TRUE(AllFiled(tracker).empty());
}

TEST(PromotionCandidateTrackerTest, RetryBudgetRunsOut) {
    PromotionCandidateTracker tracker;
    route::ObjectState state;
    tracker.RecordLocked(kTenant, "k", state, 1,
                         PromotionCandidateReason::kQueueCap, ErrorCode::OK);
    size_t retries = 0;
    while (state.promotion_candidate && retries < 1000) {
        tracker.BackoffLocked(kTenant, "k", state,
                              PromotionQueueResult::kQueueCapRejected);
        ++retries;
    }
    EXPECT_FALSE(state.promotion_candidate);
    EXPECT_LT(retries, 1000u);
    EXPECT_TRUE(tracker.Empty());
}

TEST(PromotionCandidateTrackerTest, ForgetDropsAStaleEntry) {
    PromotionCandidateTracker tracker;
    {
        // The object goes away without the tracker seeing it.
        route::ObjectState gone;
        tracker.RecordLocked(kTenant, "k", gone, 1,
                             PromotionCandidateReason::kQueueCap,
                             ErrorCode::OK);
    }
    // A new object under the key records again: one entry, one slot.
    route::ObjectState state;
    tracker.RecordLocked(kTenant, "k", state, 1,
                         PromotionCandidateReason::kQueueCap, ErrorCode::OK);
    EXPECT_EQ(AllFiled(tracker).size(), 1u);
    tracker.EraseLocked(kTenant, "k", state);
    EXPECT_TRUE(tracker.Empty());

    route::ObjectState other;
    tracker.RecordLocked(kTenant, "other", other, 1,
                         PromotionCandidateReason::kQueueCap, ErrorCode::OK);
    tracker.ForgetLocked(kTenant, "other");
    EXPECT_TRUE(tracker.Empty());
    EXPECT_TRUE(AllFiled(tracker).empty());
}

TEST(PromotionCandidateTrackerTest, KeysOfTenantsAreDistinct) {
    PromotionCandidateTracker tracker;
    const TenantId other("tenant-b");
    route::ObjectState a;
    route::ObjectState b;
    tracker.RecordLocked(kTenant, "k", a, 1,
                         PromotionCandidateReason::kQueueCap, ErrorCode::OK);
    tracker.RecordLocked(other, "k", b, 1, PromotionCandidateReason::kQueueCap,
                         ErrorCode::OK);
    EXPECT_EQ(AllFiled(tracker).size(), 2u);
    tracker.EraseLocked(other, "k", b);
    const auto due = AllFiled(tracker);
    ASSERT_EQ(due.size(), 1u);
    EXPECT_EQ(due[0].tenant_id, kTenant);
}

TEST(PromotionCandidateTrackerTest, ResetForgetsEverything) {
    PromotionCandidateTracker tracker;
    route::ObjectState state;
    tracker.RecordLocked(kTenant, "k", state, 1,
                         PromotionCandidateReason::kQueueCap, ErrorCode::OK);
    tracker.Reset();
    EXPECT_TRUE(tracker.Empty());
    EXPECT_TRUE(AllFiled(tracker).empty());
}

TEST(PromotionCandidateTrackerTest, OnlyGatesThatClearOnTheirOwnAreTransient) {
    EXPECT_TRUE(PromotionCandidateTracker::IsTransient(
        PromotionQueueResult::kWatermarkRejected));
    EXPECT_TRUE(PromotionCandidateTracker::IsTransient(
        PromotionQueueResult::kQueueCapRejected));
    EXPECT_TRUE(PromotionCandidateTracker::IsTransient(
        PromotionQueueResult::kPushFailed));
    EXPECT_FALSE(
        PromotionCandidateTracker::IsTransient(PromotionQueueResult::kQueued));
    EXPECT_FALSE(PromotionCandidateTracker::IsTransient(
        PromotionQueueResult::kNotFound));
    EXPECT_FALSE(PromotionCandidateTracker::IsTransient(
        PromotionQueueResult::kMemoryReplicaPresent));
}

}  // namespace
}  // namespace mooncake

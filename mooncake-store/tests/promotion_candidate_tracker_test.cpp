#include "promotion_candidate_tracker.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <limits>
#include <string>
#include <thread>
#include <vector>

namespace mooncake {
namespace {

using Clock = std::chrono::steady_clock;
using Key = PromotionCandidateTracker::Key;

const TenantId kTenant = TenantId::Default();

TEST(PromotionCandidateTrackerTest, RecordFilesTheKeyAsDueNow) {
    PromotionCandidateTracker tracker;
    EXPECT_TRUE(tracker.Empty());

    tracker.Record(kTenant, "k", /*sketch_score=*/3,
                   PromotionCandidateReason::kWatermark, ErrorCode::OK);

    auto candidate = tracker.Find(kTenant, "k");
    ASSERT_TRUE(candidate.has_value());
    EXPECT_EQ(candidate->sketch_score, 3);
    EXPECT_FALSE(tracker.Empty());

    const auto due = tracker.TakeDue(Clock::now(), 16);
    ASSERT_EQ(due.size(), 1u);
    EXPECT_EQ(due[0], (Key{kTenant, "k"}));
}

TEST(PromotionCandidateTrackerTest, RefreshKeepsFailureHistoryAndOneEntry) {
    PromotionCandidateTracker tracker;
    tracker.Record(kTenant, "k", 5, PromotionCandidateReason::kExecutionFailed,
                   ErrorCode::OK, /*execution_failures=*/2);
    tracker.Backoff(kTenant, "k", PromotionQueueResult::kQueueCapRejected);
    auto backed = tracker.Find(kTenant, "k");
    ASSERT_TRUE(backed.has_value());
    ASSERT_EQ(backed->retry_count, 1u);

    tracker.Record(kTenant, "k", 1, PromotionCandidateReason::kWatermark,
                   ErrorCode::OK);

    auto candidate = tracker.Find(kTenant, "k");
    ASSERT_TRUE(candidate.has_value());
    EXPECT_EQ(candidate->retry_count, 0u);
    EXPECT_EQ(candidate->execution_failures, 2u);
    EXPECT_EQ(candidate->sketch_score, 5) << "the score only ever rises";
    EXPECT_EQ(candidate->last_reason, PromotionCandidateReason::kWatermark);
    EXPECT_EQ(tracker.CountByTenant(kTenant), 1u);
    EXPECT_EQ(tracker.TakeDue(Clock::now(), 16).size(), 1u)
        << "new demand makes the candidate due again";
}

TEST(PromotionCandidateTrackerTest, EraseAndConsumeUnfile) {
    PromotionCandidateTracker tracker;
    tracker.Record(kTenant, "a", 1, PromotionCandidateReason::kQueueCap,
                   ErrorCode::OK);
    tracker.Record(kTenant, "b", 1, PromotionCandidateReason::kExecutionFailed,
                   ErrorCode::OK, /*execution_failures=*/4);

    tracker.Erase(kTenant, "a");
    EXPECT_FALSE(tracker.Find(kTenant, "a").has_value());
    EXPECT_EQ(tracker.Consume(kTenant, "b"), 4u);
    EXPECT_FALSE(tracker.Find(kTenant, "b").has_value());
    EXPECT_EQ(tracker.Consume(kTenant, "b"), 0u)
        << "no candidate is a fresh chain";
    EXPECT_TRUE(tracker.Empty());
    EXPECT_TRUE(tracker.TakeDue(Clock::now(), 16).empty());
}

TEST(PromotionCandidateTrackerTest, BackoffRefilesInTheFuture) {
    PromotionCandidateTracker tracker;
    tracker.Record(kTenant, "k", 1, PromotionCandidateReason::kQueueCap,
                   ErrorCode::OK);

    tracker.Backoff(kTenant, "k", PromotionQueueResult::kPushFailed);

    auto candidate = tracker.Find(kTenant, "k");
    ASSERT_TRUE(candidate.has_value());
    EXPECT_EQ(candidate->retry_count, 1u);
    EXPECT_EQ(candidate->last_reason, PromotionCandidateReason::kPushFailed);
    const auto filed_at = candidate->retry_after;
    EXPECT_TRUE(
        tracker.TakeDue(filed_at - std::chrono::nanoseconds(1), 16).empty());
    EXPECT_EQ(tracker.TakeDue(filed_at, 16).size(), 1u);
}

TEST(PromotionCandidateTrackerTest, DueListsTheLongestOverdueFirstUpToLimit) {
    PromotionCandidateTracker tracker;
    tracker.Record(kTenant, "first", 1, PromotionCandidateReason::kQueueCap,
                   ErrorCode::OK);
    tracker.Record(kTenant, "second", 1, PromotionCandidateReason::kQueueCap,
                   ErrorCode::OK);
    tracker.Record(kTenant, "third", 1, PromotionCandidateReason::kQueueCap,
                   ErrorCode::OK);
    // Backing "first" off moves it behind the others.
    tracker.Backoff(kTenant, "first", PromotionQueueResult::kQueueCapRejected);

    const auto future = Clock::now() + std::chrono::seconds(1);
    const auto due = tracker.TakeDue(future, 16);
    ASSERT_EQ(due.size(), 3u);
    EXPECT_EQ(due.back(), (Key{kTenant, "first"}));
    EXPECT_EQ(tracker.TakeDue(future, 2).size(), 2u);
}

TEST(PromotionCandidateTrackerTest, StaleCandidateIsDroppedWhenDue) {
    PromotionCandidateTracker tracker;
    tracker.Record(kTenant, "k", 1, PromotionCandidateReason::kWatermark,
                   ErrorCode::OK);

    // Past the TTL, TakeDue drops the candidate instead of returning it.
    EXPECT_TRUE(
        tracker.TakeDue(Clock::now() + std::chrono::hours(1), 16).empty());
    EXPECT_FALSE(tracker.Find(kTenant, "k").has_value());
    EXPECT_TRUE(tracker.Empty());
}

TEST(PromotionCandidateTrackerTest, RetryBudgetRunsOut) {
    PromotionCandidateTracker tracker;
    tracker.Record(kTenant, "k", 1, PromotionCandidateReason::kQueueCap,
                   ErrorCode::OK);
    size_t retries = 0;
    while (tracker.Find(kTenant, "k").has_value() && retries < 1000) {
        tracker.Backoff(kTenant, "k", PromotionQueueResult::kQueueCapRejected);
        ++retries;
    }
    EXPECT_FALSE(tracker.Find(kTenant, "k").has_value());
    EXPECT_LT(retries, 1000u);
    EXPECT_TRUE(tracker.Empty());
}

TEST(PromotionCandidateTrackerTest, KeysOfTenantsAreDistinct) {
    PromotionCandidateTracker tracker;
    const TenantId other("tenant-b");
    tracker.Record(kTenant, "k", 1, PromotionCandidateReason::kQueueCap,
                   ErrorCode::OK);
    tracker.Record(other, "k", 1, PromotionCandidateReason::kQueueCap,
                   ErrorCode::OK);
    EXPECT_EQ(tracker.CountByTenant(kTenant), 1u);
    EXPECT_EQ(tracker.CountByTenant(other), 1u);

    tracker.Erase(other, "k");
    EXPECT_EQ(tracker.CountByTenant(other), 0u);
    const auto due = tracker.TakeDue(Clock::now(), 16);
    ASSERT_EQ(due.size(), 1u);
    EXPECT_EQ(due[0].tenant_id, kTenant);
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

TEST(PromotionCandidateTrackerTest, ConcurrentRecordBackoffAndConsume) {
    PromotionCandidateTracker tracker;
    constexpr int kThreads = 8;
    constexpr int kKeysPerThread = 500;

    std::vector<std::thread> workers;
    workers.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        workers.emplace_back([&tracker, t]() {
            for (int i = 0; i < kKeysPerThread; ++i) {
                const std::string key =
                    "k_" + std::to_string(t) + "_" + std::to_string(i);
                tracker.Record(kTenant, key, 2,
                               PromotionCandidateReason::kWatermark,
                               ErrorCode::OK, 1);
                if (i % 3 == 0) {
                    tracker.Backoff(kTenant, key,
                                    PromotionQueueResult::kWatermarkRejected);
                }
                if (i % 2 == 0) {
                    EXPECT_EQ(tracker.Consume(kTenant, key), 1u);
                } else {
                    tracker.Erase(kTenant, key);
                }
            }
        });
    }
    for (auto& w : workers) {
        w.join();
    }
    EXPECT_TRUE(tracker.Empty());
    EXPECT_EQ(tracker.CountByTenant(kTenant), 0u);
}

TEST(PromotionCandidateTrackerTest, TakeDueBoundsStaleReapingAndRespectsLimit) {
    PromotionCandidateTracker tracker;
    constexpr int kTotal = 600;
    for (int i = 0; i < kTotal; ++i) {
        tracker.Record(kTenant, "stale_" + std::to_string(i), 1,
                       PromotionCandidateReason::kWatermark, ErrorCode::OK);
    }
    EXPECT_EQ(tracker.CountByTenant(kTenant), static_cast<size_t>(kTotal));

    // A single TakeDue call with a small limit bounds stale reaping work
    // instead of draining the entire backlog at once; subsequent calls drain
    // the remainder cleanly.
    const auto expired_now = Clock::now() + std::chrono::hours(1);
    EXPECT_TRUE(tracker.TakeDue(expired_now, 16).empty());
    const size_t after_first = tracker.CountByTenant(kTenant);
    EXPECT_GT(after_first, 0u);
    EXPECT_LT(after_first, static_cast<size_t>(kTotal));

    int rounds = 0;
    while (!tracker.Empty() && rounds < 20) {
        EXPECT_TRUE(tracker.TakeDue(expired_now, 16).empty());
        ++rounds;
    }
    EXPECT_TRUE(tracker.Empty());
    EXPECT_EQ(tracker.CountByTenant(kTenant), 0u);
}

}  // namespace
}  // namespace mooncake

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <ylt/util/tl/expected.hpp>

#include <cstddef>
#include <functional>
#include <optional>
#include <utility>
#include <vector>

#include "file_storage_notify_retry.h"
#include "types.h"

namespace mooncake {

namespace {

struct RangeCall {
    size_t begin;
    size_t end;
};

// Scripted notify endpoint: records every attempted range, marks the
// entries of each successful range as delivered (exactly once expected),
// and fails according to the configured rules. Sleep is recorded instead
// of performed, so the schedule is observable without wall-clock delays.
//
// Semantics note: a rejected range is rejected as a whole. The real
// master applies a batch prefix before hitting backpressure mid-batch;
// the exactly-once assertion below therefore proves coverage by the
// SUCCESSFUL sub-ranges, not prefix-application behavior.
class ScriptedNotify {
   public:
    ScriptedNotify(size_t total, ErrorCode transient_error,
                   size_t succeed_at_or_below,
                   std::optional<size_t> hard_fail_from = std::nullopt)
        : delivered_(total, 0),
          transient_error_(transient_error),
          succeed_at_or_below_(succeed_at_or_below),
          hard_fail_from_(hard_fail_from) {}

    tl::expected<void, ErrorCode> operator()(size_t begin, size_t end) {
        attempts_.push_back({begin, end});
        if (hard_fail_from_.has_value() && begin >= *hard_fail_from_) {
            return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
        }
        if (end - begin > succeed_at_or_below_) {
            return tl::make_unexpected(transient_error_);
        }
        for (size_t i = begin; i < end; ++i) {
            ++delivered_[i];
        }
        succeeded_.push_back({begin, end});
        return {};
    }

    void Sleep(int backoff_ms) { sleeps_.push_back(backoff_ms); }

    const std::vector<RangeCall>& attempts() const { return attempts_; }
    const std::vector<RangeCall>& succeeded() const { return succeeded_; }
    const std::vector<int>& sleeps() const { return sleeps_; }
    int DeliveredCount(size_t index) const { return delivered_[index]; }

    // Every entry delivered exactly once: completed ranges are never
    // resent, and no range is dropped after a split.
    ::testing::AssertionResult EachEntryDeliveredOnce() const {
        for (size_t i = 0; i < delivered_.size(); ++i) {
            if (delivered_[i] != 1) {
                return ::testing::AssertionFailure()
                       << "entry " << i << " delivered " << delivered_[i]
                       << " times";
            }
        }
        return ::testing::AssertionSuccess();
    }

   private:
    std::vector<int> delivered_;
    ErrorCode transient_error_;
    size_t succeed_at_or_below_;
    std::optional<size_t> hard_fail_from_;
    std::vector<RangeCall> attempts_;
    std::vector<RangeCall> succeeded_;
    std::vector<int> sleeps_;
};

}  // namespace

// Normal path: a single RPC, no backoff, no sleep.
TEST(FileStorageNotifyRetryTest, SuccessOnFirstAttempt) {
    ScriptedNotify notify(32, ErrorCode::TASK_PENDING_LIMIT_EXCEEDED,
                          /*succeed_at_or_below=*/32);
    auto result = NotifyRangesWithBackpressureRetry(
        32, std::ref(notify), [&notify](int ms) { notify.Sleep(ms); });
    ASSERT_TRUE(result);
    EXPECT_EQ(notify.attempts().size(), 1u);
    EXPECT_EQ(notify.attempts()[0].begin, 0u);
    EXPECT_EQ(notify.attempts()[0].end, 32u);
    EXPECT_TRUE(notify.EachEntryDeliveredOnce());
    EXPECT_TRUE(notify.sleeps().empty());
}

// Both retryable error codes are retried with the capped exponential
// schedule (1, 2, 4, 8, 16, 16, ... ms) before succeeding.
TEST(FileStorageNotifyRetryTest, TransientBackpressureRetriedThenSucceeds) {
    // Alternate between the two retryable codes across attempts.
    std::vector<ErrorCode> codes = {ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS,
                                    ErrorCode::TASK_PENDING_LIMIT_EXCEEDED};
    size_t calls = 0;
    size_t deliveries = 0;
    std::vector<int> sleeps;
    auto notify = [&](size_t begin,
                      size_t end) -> tl::expected<void, ErrorCode> {
        if (calls++ < 5) {
            return tl::make_unexpected(codes[calls % codes.size()]);
        }
        deliveries += end - begin;
        return {};
    };
    auto result = NotifyRangesWithBackpressureRetry(
        7, notify, [&sleeps](int ms) { sleeps.push_back(ms); });
    ASSERT_TRUE(result);
    EXPECT_EQ(calls, 6u);
    EXPECT_EQ(deliveries, 7u);
    EXPECT_EQ(sleeps, (std::vector<int>{1, 2, 4, 8, 16}));
}

// Sustained backpressure splits the batch until ranges fit; every entry
// must still be delivered exactly once. This is the regression guard for
// dropped split tails: an implementation that abandons the pending stack
// after the first sub-range success leaves entries undelivered.
TEST(FileStorageNotifyRetryTest, SustainedBackpressureSplitsWithoutGaps) {
    // Ranges larger than 3 entries never fit the (simulated) window.
    ScriptedNotify notify(16, ErrorCode::TASK_PENDING_LIMIT_EXCEEDED,
                          /*succeed_at_or_below=*/3);
    auto result = NotifyRangesWithBackpressureRetry(
        16, std::ref(notify), [&notify](int ms) { notify.Sleep(ms); });
    ASSERT_TRUE(result);
    EXPECT_TRUE(notify.EachEntryDeliveredOnce());
    // 7 oversized ranges ([0,16) and the split tree below it) exhaust
    // their attempts, 7 sleeps each before the split.
    EXPECT_EQ(notify.sleeps().size(), 7u * 7u);
    EXPECT_EQ(notify.succeeded().size(), 8u);
}

// Hard errors are never retried.
TEST(FileStorageNotifyRetryTest, NonRetryableErrorPropagatesImmediately) {
    size_t calls = 0;
    std::vector<int> sleeps;
    auto notify = [&](size_t, size_t) -> tl::expected<void, ErrorCode> {
        ++calls;
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    };
    auto result = NotifyRangesWithBackpressureRetry(
        5, notify, [&sleeps](int ms) { sleeps.push_back(ms); });
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::OBJECT_NOT_FOUND);
    EXPECT_EQ(calls, 1u);
    EXPECT_TRUE(sleeps.empty());
}

// A single undividable entry that keeps hitting backpressure gives up
// after kNotifyMasterMaxAttempts instead of looping forever, pacing the
// attempts with the full capped exponential schedule.
TEST(FileStorageNotifyRetryTest, SingleEntrySustainedBackpressureGivesUp) {
    size_t calls = 0;
    std::vector<int> sleeps;
    auto notify = [&](size_t, size_t) -> tl::expected<void, ErrorCode> {
        ++calls;
        return tl::make_unexpected(ErrorCode::TASK_PENDING_LIMIT_EXCEEDED);
    };
    auto result = NotifyRangesWithBackpressureRetry(
        1, notify, [&sleeps](int ms) { sleeps.push_back(ms); });
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::TASK_PENDING_LIMIT_EXCEEDED);
    EXPECT_EQ(calls, static_cast<size_t>(kNotifyMasterMaxAttempts));
    // The 16 ms cap must hold across the whole schedule.
    EXPECT_EQ(sleeps, (std::vector<int>{1, 2, 4, 8, 16, 16, 16}));
}

// A hard error in one split half must fail the whole batch even after
// the other half succeeded; it must not be masked by sibling success.
TEST(FileStorageNotifyRetryTest, SplitHalfHardErrorPropagates) {
    // Ranges within [8, 16) fail with a non-retryable error; the left
    // half splits down and succeeds first (stack order processes
    // [0, 8) before [8, 16)).
    ScriptedNotify notify(16, ErrorCode::TASK_PENDING_LIMIT_EXCEEDED,
                          /*succeed_at_or_below=*/3,
                          /*hard_fail_from=*/8);
    auto result = NotifyRangesWithBackpressureRetry(
        16, std::ref(notify), [&notify](int ms) { notify.Sleep(ms); });
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::OBJECT_NOT_FOUND);
    // The left half was delivered; nothing from the failing half.
    for (size_t i = 0; i < 8; ++i) {
        EXPECT_EQ(notify.DeliveredCount(i), 1);
    }
    for (size_t i = 8; i < 16; ++i) {
        EXPECT_EQ(notify.DeliveredCount(i), 0);
    }
}

// Degenerate batch: a single empty [0, 0) range is delivered (the batch
// RPC is a no-op for it) and the overall result is success.
TEST(FileStorageNotifyRetryTest, EmptyBatchDeliversSingleEmptyRange) {
    std::vector<RangeCall> calls;
    auto notify = [&](size_t begin,
                      size_t end) -> tl::expected<void, ErrorCode> {
        calls.push_back({begin, end});
        return {};
    };
    auto result = NotifyRangesWithBackpressureRetry(0, notify, [](int) {});
    ASSERT_TRUE(result);
    ASSERT_EQ(calls.size(), 1u);
    EXPECT_EQ(calls[0].begin, 0u);
    EXPECT_EQ(calls[0].end, 0u);
}

}  // namespace mooncake

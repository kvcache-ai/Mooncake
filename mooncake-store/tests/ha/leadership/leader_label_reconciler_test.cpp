#include "ha/leadership/leader_label_reconciler.h"

#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <thread>

namespace mooncake {
namespace ha {
namespace {

using namespace std::chrono_literals;

// Records every apply call and can be told to fail the first N attempts to
// simulate a flaky / unreachable K8s API server.
class FakeLabelBackend {
   public:
    LeaderLabelReconciler::ApplyFn Fn() {
        return [this](bool desired) { return Apply(desired); };
    }

    void FailNext(int count) {
        std::lock_guard<std::mutex> lock(mutex_);
        fail_remaining_ = count;
    }

    void CommitThenFailNext(int count) {
        std::lock_guard<std::mutex> lock(mutex_);
        commit_then_fail_remaining_ = count;
    }

    // Waits until the last successful apply matches `want`, or times out.
    bool WaitForApplied(bool want, std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, timeout, [&] { return applied_ == want; });
    }

    bool WaitForCommitted(bool want, std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, timeout, [&] { return committed_ == want; });
    }

    int CallCount() {
        std::lock_guard<std::mutex> lock(mutex_);
        return call_count_;
    }

    std::optional<bool> Applied() {
        std::lock_guard<std::mutex> lock(mutex_);
        return applied_;
    }

    std::optional<bool> Committed() {
        std::lock_guard<std::mutex> lock(mutex_);
        return committed_;
    }

   private:
    ErrorCode Apply(bool desired) {
        std::lock_guard<std::mutex> lock(mutex_);
        ++call_count_;
        if (commit_then_fail_remaining_ > 0) {
            --commit_then_fail_remaining_;
            committed_ = desired;
            cv_.notify_all();
            return ErrorCode::K8S_LEASE_OPERATION_ERROR;
        }
        if (fail_remaining_ > 0) {
            --fail_remaining_;
            cv_.notify_all();
            return ErrorCode::K8S_LEASE_OPERATION_ERROR;
        }
        committed_ = desired;
        applied_ = desired;
        cv_.notify_all();
        return ErrorCode::OK;
    }

    std::mutex mutex_;
    std::condition_variable cv_;
    int call_count_ = 0;
    int fail_remaining_ = 0;
    int commit_then_fail_remaining_ = 0;
    std::optional<bool> applied_;
    std::optional<bool> committed_;
};

constexpr auto kRetry = 5ms;
constexpr auto kTimeout = 2s;
constexpr auto kSlowRetry = 10s;

TEST(LeaderLabelReconcilerTest, ConvergesToLeaderAfterTransientFailures) {
    FakeLabelBackend backend;
    backend.FailNext(3);
    LeaderLabelReconciler reconciler(/*enabled=*/true, backend.Fn(), kRetry);

    reconciler.SetLeader(true);

    ASSERT_TRUE(backend.WaitForApplied(true, kTimeout));
    EXPECT_GT(backend.CallCount(), 3);
}

TEST(LeaderLabelReconcilerTest, ClearsLeaderAfterTransientFailures) {
    FakeLabelBackend backend;
    LeaderLabelReconciler reconciler(/*enabled=*/true, backend.Fn(), kRetry);

    reconciler.SetLeader(true);
    ASSERT_TRUE(backend.WaitForApplied(true, kTimeout));

    backend.FailNext(3);
    reconciler.SetLeader(false);

    ASSERT_TRUE(backend.WaitForApplied(false, kTimeout));
}

TEST(LeaderLabelReconcilerTest, LatestDesiredStateWins) {
    FakeLabelBackend backend;
    LeaderLabelReconciler reconciler(/*enabled=*/true, backend.Fn(), kRetry);

    reconciler.SetLeader(true);
    reconciler.SetLeader(false);

    ASSERT_TRUE(backend.WaitForApplied(false, kTimeout));
    EXPECT_EQ(backend.Applied(), std::make_optional(false));
}

TEST(LeaderLabelReconcilerTest, StopsCallingOnceConverged) {
    FakeLabelBackend backend;
    LeaderLabelReconciler reconciler(/*enabled=*/true, backend.Fn(), kRetry);

    reconciler.SetLeader(true);
    ASSERT_TRUE(backend.WaitForApplied(true, kTimeout));

    int settled = backend.CallCount();
    std::this_thread::sleep_for(kRetry * 20);
    EXPECT_EQ(backend.CallCount(), settled);
}

TEST(LeaderLabelReconcilerTest, DisabledNeverApplies) {
    FakeLabelBackend backend;
    LeaderLabelReconciler reconciler(/*enabled=*/false, backend.Fn(), kRetry);

    reconciler.SetLeader(true);
    std::this_thread::sleep_for(kRetry * 20);

    EXPECT_EQ(backend.CallCount(), 0);
    EXPECT_EQ(backend.Applied(), std::nullopt);
}

TEST(LeaderLabelReconcilerTest, AmbiguousSetFailureDoesNotLeaveStaleLeader) {
    FakeLabelBackend backend;
    backend.CommitThenFailNext(1);
    LeaderLabelReconciler reconciler(/*enabled=*/true, backend.Fn(),
                                     kSlowRetry);

    reconciler.SetLeader(true);
    ASSERT_TRUE(backend.WaitForCommitted(true, kTimeout));

    reconciler.SetLeader(false);

    ASSERT_TRUE(backend.WaitForCommitted(false, kTimeout));
}

TEST(LeaderLabelReconcilerTest, AmbiguousClearFailureDoesNotLeaveMissingLabel) {
    FakeLabelBackend backend;
    LeaderLabelReconciler reconciler(/*enabled=*/true, backend.Fn(),
                                     kSlowRetry);

    reconciler.SetLeader(true);
    ASSERT_TRUE(backend.WaitForCommitted(true, kTimeout));

    backend.CommitThenFailNext(1);
    reconciler.SetLeader(false);
    ASSERT_TRUE(backend.WaitForCommitted(false, kTimeout));

    reconciler.SetLeader(true);

    ASSERT_TRUE(backend.WaitForCommitted(true, kTimeout));
}

}  // namespace
}  // namespace ha
}  // namespace mooncake

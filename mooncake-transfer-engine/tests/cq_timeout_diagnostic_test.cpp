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

// Hardware-free regression tests for CQ timeout reporting. They cover stale
// poll timestamps after idle periods, real poller stalls, and transfers that
// stop making completion progress.

#include "transport/rdma_transport/cq_timeout_diagnostic.h"

#include <gtest/gtest.h>

#include <cstdint>

using mooncake::CqTimeoutDiagnostic;

namespace {

constexpr uint64_t kMs = 1000ull * 1000ull;
constexpr uint64_t kSec = 1000ull * kMs;
// monitorWorker() samples, then blocks in epoll_wait(..., 100).
constexpr uint64_t kTick = 100ull * kMs;

}  // namespace

// The CQ becomes non-empty just after a long idle period. Its old poll
// timestamp must not cause an immediate timeout report.
TEST(CqTimeoutDiagnostic, IdleGapThenNewWorkDoesNotReportImmediately) {
    CqTimeoutDiagnostic diag;
    const uint64_t now = 100 * kSec;
    const uint64_t last_poll_ts = now - 60 * kSec;
    const uint64_t processed = 100;

    const auto verdict = diag.observe(now, 2, processed, last_poll_ts);

    EXPECT_FALSE(verdict.report);
    EXPECT_EQ(verdict.poll_gap_ms, 60000u);
    EXPECT_EQ(verdict.cq_nonzero_age_ms, 0u);
    EXPECT_EQ(verdict.no_progress_age_ms, 0u);

    // A stale poll gap remains visible, but is not sufficient by itself.
    const bool pre_fix_would_report = verdict.poll_gap_ms >= 5000u;
    EXPECT_TRUE(pre_fix_would_report);
}

// Exercise the same case over monitor ticks: the worker remains idle, then a
// new batch appears before the poller has updated its timestamp.
TEST(CqTimeoutDiagnostic, IdlePollerThenFreshBatchIsNotReported) {
    CqTimeoutDiagnostic diag;
    const uint64_t last_poll_ts = 10 * kSec;  // frozen while the poller parks
    uint64_t now = last_poll_ts;
    const uint64_t processed = 1000;

    for (int i = 0; i < 600; ++i) {  // 60 s of idle monitor ticks
        now += kTick;
        ASSERT_FALSE(diag.observe(now, 0, processed, last_poll_ts).report);
    }

    now += kTick;  // the batch is posted; no completion has been processed yet
    const auto verdict = diag.observe(now, 2, processed, last_poll_ts);

    EXPECT_FALSE(verdict.report);
    EXPECT_EQ(verdict.cq_nonzero_age_ms, 0u);
    EXPECT_EQ(verdict.no_progress_age_ms, 0u);
    EXPECT_EQ(verdict.poll_gap_ms, (600 + 1) * 100u);
}

// Outstanding work can remain nonzero while completions keep arriving. This
// is progress, so the no-progress timeout must remain silent.
TEST(CqTimeoutDiagnostic, OutstandingWithSteadyProgressIsNotReported) {
    CqTimeoutDiagnostic diag;
    uint64_t now = 1 * kSec;  // 0 is the "never polled" sentinel
    uint64_t processed = 0;
    int reports = 0;
    CqTimeoutDiagnostic::Verdict last;

    for (int i = 0; i < 36000; ++i) {  // one hour of synthetic 100 ms ticks
        ++processed;  // one completion per tick: the pool is making progress
        last = diag.observe(now, 2, processed, now);
        if (last.report) ++reports;
        now += kTick;
    }

    EXPECT_EQ(reports, 0);
    // Long-lived nonzero outstanding work does not imply a lack of progress.
    EXPECT_GE(last.cq_nonzero_age_ms, 3599000u);
    EXPECT_LE(last.cq_nonzero_age_ms, 3600000u);
    EXPECT_LT(last.no_progress_age_ms, 1000u);
}

// A poller that is genuinely stuck (work in flight, no poll for a full window,
// completions frozen as a consequence) is still reported quickly - this is the
// case the diagnostic exists for.
TEST(CqTimeoutDiagnostic, StalledPollerIsReportedAfterStallWindow) {
    CqTimeoutDiagnostic diag;
    const uint64_t t0 = 100 * kSec;
    const uint64_t last_poll_ts = t0;  // last poll happened just before posting
    const uint64_t processed = 7;

    ASSERT_FALSE(diag.observe(t0, 2, processed, last_poll_ts).report);

    uint64_t first_report_ms = 0;
    int reports = 0;
    for (int i = 1; i <= 120; ++i) {  // 12 s
        const uint64_t now = t0 + i * kTick;
        const auto verdict = diag.observe(now, 2, processed, last_poll_ts);
        if (verdict.report) {
            ++reports;
            if (first_report_ms == 0) first_report_ms = (now - t0) / kMs;
        }
    }

    EXPECT_EQ(first_report_ms, 5000u);
    EXPECT_EQ(reports, 2);  // 5 s and 10 s: rate limited to one per window
}

// A single lost completion on an otherwise healthy CQ: the poller keeps
// running (poll_gap stays small), so only the no-progress window can catch it.
TEST(CqTimeoutDiagnostic, LostCompletionWithLivePollerWaitsForNoProgress) {
    CqTimeoutDiagnostic diag;
    const uint64_t t0 = 5 * kSec;
    const uint64_t processed = 42;

    ASSERT_FALSE(diag.observe(t0, 2, processed, t0).report);

    uint64_t first_report_ms = 0;
    for (int i = 1; i <= 310; ++i) {  // 31 s
        const uint64_t now = t0 + i * kTick;
        // The poller runs every tick and simply finds nothing for these WRs.
        const auto verdict = diag.observe(now, 2, processed, now);
        if (verdict.report && first_report_ms == 0)
            first_report_ms = (now - t0) / kMs;
    }

    EXPECT_EQ(first_report_ms, 30000u);
}

// Drain resets both windows: a batch posted right after a fully drained CQ is
// measured from its own post time, not from whatever happened before.
TEST(CqTimeoutDiagnostic, DrainResetsBothWindows) {
    CqTimeoutDiagnostic diag;
    const uint64_t t0 = 100 * kSec;
    const uint64_t last_poll_ts = t0;  // poller parks after this
    uint64_t processed = 9;

    EXPECT_FALSE(diag.observe(t0, 2, processed, last_poll_ts).report);
    EXPECT_TRUE(diag.observe(t0 + 5 * kSec, 2, processed, last_poll_ts).report);

    // Drained: counter back to 0.
    processed += 2;
    EXPECT_FALSE(
        diag.observe(t0 + 5100 * kMs, 0, processed, last_poll_ts).report);

    // New batch at t0 + 6 s, same stale poll timestamp. The windows restarted,
    // so the 5 s old gap must not resurrect the previous stall.
    EXPECT_FALSE(
        diag.observe(t0 + 6 * kSec, 2, processed, last_poll_ts).report);
    EXPECT_FALSE(
        diag.observe(t0 + 10900 * kMs, 2, processed, last_poll_ts).report);
    EXPECT_TRUE(
        diag.observe(t0 + 11 * kSec, 2, processed, last_poll_ts).report);
}

// A short transfer every 10 s against a poller that parks between batches.
// 200 batches, no report.
TEST(CqTimeoutDiagnostic, PeriodicTinyTransfersWithIdleGapsDoNotReport) {
    CqTimeoutDiagnostic diag;
    uint64_t now = 0;
    uint64_t processed = 1000;
    uint64_t last_poll_ts = 0;  // no poll before the first batch
    int reports = 0;

    for (int cycle = 0; cycle < 200; ++cycle) {
        for (int i = 0; i < 100; ++i) {  // 10 s idle, poller parked
            now += kTick;
            if (diag.observe(now, 0, processed, last_poll_ts).report) ++reports;
        }
        now += kTick;  // 2 slices posted, monitor samples first
        if (diag.observe(now, 2, processed, last_poll_ts).report) ++reports;
        now += kTick;  // both completions processed, poller ran
        processed += 2;
        last_poll_ts = now;
        if (diag.observe(now, 0, processed, last_poll_ts).report) ++reports;
    }

    EXPECT_EQ(reports, 0);
}

// The poller has not run yet (last_poll_ts == 0): there is no gap to measure,
// so the very first post of a process cannot be reported.
TEST(CqTimeoutDiagnostic, NeverPolledPoolHasNoPollGap) {
    CqTimeoutDiagnostic diag;
    const auto verdict = diag.observe(1 * kSec, 2, 0, 0);
    EXPECT_FALSE(verdict.report);
    EXPECT_EQ(verdict.poll_gap_ms, 0u);
}

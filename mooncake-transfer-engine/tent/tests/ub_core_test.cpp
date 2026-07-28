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

#include <atomic>
#include <cstdint>
#include <thread>
#include <vector>

#include "tent/transport/ub/quota.h"
#include "tent/transport/ub/params.h"
#include "tent/transport/ub/rail_monitor.h"
#include "tent/transport/ub/slice.h"

namespace mooncake::tent::ub {
namespace {

TEST(UbParamsTest, ParsesStrictValuesAndAllowsRetryDisable) {
    Config config;
    config.set("transports/ub/max_retries", 0);
    config.set("transports/ub/worker_count", 2);
    config.set("transports/ub/device_filter",
               std::vector<std::string>{"ub:fake0:eid0"});
    UbParams params;
    ASSERT_TRUE(UbParams::FromConfig(config, params).ok());
    EXPECT_EQ(params.max_retries, 0U);
    EXPECT_EQ(params.worker_count, 2U);
    ASSERT_EQ(params.device_filter.size(), 1U);

    config.set("transports/ub/worker_count", -1);
    EXPECT_TRUE(UbParams::FromConfig(config, params).IsInvalidArgument());
}

Request makeRequest(size_t length) {
    Request request{};
    request.opcode = Request::WRITE;
    request.length = length;
    return request;
}

UbPostPath makePath(Topology::NicID local_id = 0, int remote_id = 1,
                    uint64_t generation = 1) {
    return UbPostPath{local_id, 42, remote_id, generation};
}

TEST(UbSliceTest, RetryRejectsStaleCompletionAndNotifiesTerminalOnce) {
    int callback_count = 0;
    TransferStatus callback_status{INITIAL, 0};
    auto task = UbTask::create(
        makeRequest(128),
        [&](const TransferStatus& status) {
            ++callback_count;
            callback_status = status;
        },
        1);
    auto slice = task->addSlice(UbSliceSpec{nullptr, 0, 128, 0, 1}, 2);
    ASSERT_NE(slice, nullptr);
    ASSERT_TRUE(task->seal());
    ASSERT_TRUE(slice->markQueued(3));

    auto first_attempt = slice->beginAttempt(makePath(0, 1, 10), 4);
    ASSERT_TRUE(first_attempt.has_value());
    auto first_completion = slice->completionToken(*first_attempt);
    ASSERT_TRUE(first_completion.has_value());
    ASSERT_TRUE(first_completion->markPosted(5));
    EXPECT_EQ(first_completion->resolve(TIMEOUT, 0, true, 6),
              UbAttemptResolution::kRetryScheduled);
    EXPECT_EQ(task->transferStatus().s, PENDING);
    EXPECT_EQ(slice->snapshot().retry_count, 1U);

    ASSERT_TRUE(slice->markQueued(7));
    auto second_attempt = slice->beginAttempt(makePath(0, 2, 11), 8);
    ASSERT_TRUE(second_attempt.has_value());
    auto second_completion = slice->completionToken(*second_attempt);
    ASSERT_TRUE(second_completion.has_value());
    ASSERT_TRUE(second_completion->markPosted(9));

    // A completion from the retired endpoint generation cannot resolve the
    // replacement attempt.
    EXPECT_EQ(first_completion->resolve(COMPLETED, 128, false, 10),
              UbAttemptResolution::kIgnored);
    EXPECT_EQ(second_completion->resolve(COMPLETED, 128, false, 11),
              UbAttemptResolution::kTerminal);
    EXPECT_EQ(second_completion->resolve(COMPLETED, 128, false, 12),
              UbAttemptResolution::kIgnored);

    EXPECT_EQ(callback_count, 1);
    EXPECT_EQ(callback_status.s, COMPLETED);
    EXPECT_EQ(callback_status.transferred_bytes, 128U);
    const auto task_snapshot = task->snapshot();
    EXPECT_EQ(task_snapshot.remaining_slices, 0U);
    EXPECT_NE(task_snapshot.terminal_ns, 0U);
}

TEST(UbSliceTest, CancellationStopsQueuedWorkAndDrainsPostedWork) {
    int callback_count = 0;
    TransferStatus callback_status{INITIAL, 0};
    auto task =
        UbTask::create(makeRequest(128), [&](const TransferStatus& status) {
            ++callback_count;
            callback_status = status;
        });
    auto posted = task->addSlice(UbSliceSpec{nullptr, 0, 64, 0, 1});
    auto queued = task->addSlice(UbSliceSpec{nullptr, 64, 64, 64, 1});
    ASSERT_NE(posted, nullptr);
    ASSERT_NE(queued, nullptr);
    ASSERT_TRUE(task->seal());

    ASSERT_TRUE(posted->markQueued(1));
    auto attempt = posted->beginAttempt(makePath(), 2);
    ASSERT_TRUE(attempt.has_value());
    auto completion = posted->completionToken(*attempt);
    ASSERT_TRUE(completion.has_value());
    ASSERT_TRUE(completion->markPosted(3));
    ASSERT_TRUE(queued->markQueued(4));

    EXPECT_EQ(task->requestCancellation(5), 1U);
    EXPECT_EQ(task->transferStatus().s, PENDING);
    EXPECT_EQ(posted->snapshot().state, UbSliceState::kPosted);
    EXPECT_EQ(queued->snapshot().state, UbSliceState::kCanceled);

    // Posted work is not fabricated as immediately canceled. Its real
    // completion drains first, then the task reaches its single terminal
    // state (CANCELED because another slice never reached the device).
    EXPECT_EQ(completion->resolve(COMPLETED, 64, false, 6),
              UbAttemptResolution::kTerminal);
    EXPECT_EQ(completion->resolve(FAILED, 0, false, 7),
              UbAttemptResolution::kIgnored);
    EXPECT_EQ(callback_count, 1);
    EXPECT_EQ(callback_status.s, CANCELED);
    EXPECT_EQ(callback_status.transferred_bytes, 64U);
}

TEST(UbSliceTest, ConcurrentDuplicateCompletionsChooseOneTerminalWinner) {
    std::atomic<int> callback_count{0};
    auto task = UbTask::create(makeRequest(64), [&](const TransferStatus&) {
        callback_count.fetch_add(1, std::memory_order_relaxed);
    });
    auto slice = task->addSlice(UbSliceSpec{nullptr, 0, 64, 0, 0});
    ASSERT_NE(slice, nullptr);
    ASSERT_TRUE(task->seal());
    ASSERT_TRUE(slice->markQueued());
    auto attempt = slice->beginAttempt(makePath());
    ASSERT_TRUE(attempt.has_value());
    auto completion = slice->completionToken(*attempt);
    ASSERT_TRUE(completion.has_value());
    ASSERT_TRUE(completion->markPosted());

    std::atomic<int> terminal_winners{0};
    std::vector<std::thread> threads;
    for (int i = 0; i < 8; ++i) {
        threads.emplace_back([&] {
            if (completion->resolve(COMPLETED, 64, false) ==
                UbAttemptResolution::kTerminal) {
                terminal_winners.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    for (auto& thread : threads) thread.join();

    EXPECT_EQ(terminal_winners.load(std::memory_order_relaxed), 1);
    EXPECT_EQ(callback_count.load(std::memory_order_relaxed), 1);
    EXPECT_EQ(task->transferStatus().s, COMPLETED);
    EXPECT_EQ(task->transferStatus().transferred_bytes, 64U);
}

TEST(UbQuotaTest, EnforcesBothLevelsAndReleasesIdempotently) {
    QuotaManager quota(/*default_device_limits=*/{100, 2},
                       /*default_path_limits=*/{60, 1});
    const auto first_path = makePath(0, 1, 1);
    const auto second_path = makePath(0, 2, 1);

    auto reservation = quota.tryAcquire(first_path, 60);
    ASSERT_TRUE(reservation.has_value());
    EXPECT_FALSE(quota.tryAcquire(first_path, 1).has_value());
    // The second path has room, but the physical-device byte cap is shared.
    EXPECT_FALSE(quota.tryAcquire(second_path, 50).has_value());

    auto copied_token = *reservation;
    copied_token.path = second_path;
    copied_token.bytes = 1;
    copied_token.wrs = 99;
    EXPECT_TRUE(quota.release(copied_token));
    EXPECT_FALSE(quota.release(*reservation));

    const auto device = quota.deviceStats(0);
    const auto path = quota.pathStats(first_path);
    const auto aggregate = quota.aggregateStats();
    EXPECT_EQ(device.usage, QuotaUsage{});
    EXPECT_EQ(path.usage, QuotaUsage{});
    EXPECT_EQ(aggregate.usage, QuotaUsage{});
    EXPECT_EQ(aggregate.active_reservations, 0U);
    EXPECT_EQ(aggregate.duplicate_release_attempts, 1U);
    EXPECT_TRUE(quota.tryAcquire(first_path, 60).has_value());
}

TEST(UbRailMonitorTest, PausesOnErrorWindowAndRecoversAfterCooldown) {
    RailMonitor monitor(RailMonitorConfig{/*error_threshold=*/2,
                                          /*error_window_ns=*/100,
                                          /*cooldown_ns=*/50,
                                          /*ewma_alpha=*/0.5});
    const auto path = makePath();

    EXPECT_DOUBLE_EQ(monitor.aggregateBandwidth(1), -1.0);
    monitor.recordSuccess(path, 100, 10, 50);
    EXPECT_DOUBLE_EQ(monitor.aggregateBandwidth(50), 10'000'000'000.0);

    monitor.recordError(path, 100);
    monitor.recordTimeout(path, 110);
    EXPECT_FALSE(monitor.available(path, 159));
    EXPECT_DOUBLE_EQ(monitor.aggregateBandwidth(159), 0.0);

    EXPECT_TRUE(monitor.available(path, 160));
    const auto stats = monitor.stats(path, 160);
    EXPECT_FALSE(stats.paused);
    EXPECT_EQ(stats.errors_in_window, 0U);
    EXPECT_EQ(stats.completion_errors, 2U);
    EXPECT_EQ(stats.timeouts, 1U);
    EXPECT_EQ(stats.pauses, 1U);
    EXPECT_EQ(stats.recoveries, 1U);
    EXPECT_DOUBLE_EQ(monitor.aggregateBandwidth(160), 10'000'000'000.0);
}

TEST(UbRailMonitorTest, ErrorsOutsideWindowDoNotPauseRail) {
    RailMonitor monitor(RailMonitorConfig{/*error_threshold=*/2,
                                          /*error_window_ns=*/10,
                                          /*cooldown_ns=*/50,
                                          /*ewma_alpha=*/0.5});
    const auto path = makePath();
    monitor.recordError(path, 1);
    monitor.recordError(path, 11);
    EXPECT_TRUE(monitor.available(path, 11));
    EXPECT_EQ(monitor.stats(path, 11).errors_in_window, 1U);
}

TEST(UbRailMonitorTest, OutOfOrderErrorsUseLatestEventForCooldown) {
    RailMonitor monitor(RailMonitorConfig{/*error_threshold=*/2,
                                          /*error_window_ns=*/100,
                                          /*cooldown_ns=*/50,
                                          /*ewma_alpha=*/0.5});
    const auto path = makePath();

    monitor.recordError(path, 120);
    monitor.recordError(path, 100);

    auto stats = monitor.stats(path, 120);
    EXPECT_TRUE(stats.paused);
    EXPECT_EQ(stats.errors_in_window, 2U);
    EXPECT_EQ(stats.pause_started_ns, 120U);
    EXPECT_EQ(stats.cooldown_until_ns, 170U);
    EXPECT_FALSE(monitor.available(path, 169));
    EXPECT_TRUE(monitor.available(path, 170));

    stats = monitor.stats(path, 170);
    EXPECT_FALSE(stats.paused);
    EXPECT_EQ(stats.errors_in_window, 0U);
    EXPECT_EQ(stats.pauses, 1U);
    EXPECT_EQ(stats.recoveries, 1U);

    // Neither an older query nor an error from the recovered epoch may
    // rewind the rail or resurrect its completed pause.
    EXPECT_TRUE(monitor.available(path, 130));
    monitor.recordError(path, 110);
    stats = monitor.stats(path, 130);
    EXPECT_FALSE(stats.paused);
    EXPECT_EQ(stats.errors_in_window, 0U);
    EXPECT_EQ(stats.completion_errors, 3U);
    EXPECT_EQ(stats.pauses, 1U);
    EXPECT_EQ(stats.recoveries, 1U);
}

TEST(UbRailMonitorTest, OutOfOrderExpiredErrorDoesNotTriggerPause) {
    RailMonitor monitor(RailMonitorConfig{/*error_threshold=*/2,
                                          /*error_window_ns=*/50,
                                          /*cooldown_ns=*/25,
                                          /*ewma_alpha=*/0.5});
    const auto path = makePath();

    monitor.recordError(path, 200);
    monitor.recordError(path, 100);

    const auto stats = monitor.stats(path, 200);
    EXPECT_FALSE(stats.paused);
    EXPECT_EQ(stats.errors_in_window, 1U);
    EXPECT_EQ(stats.completion_errors, 2U);
    EXPECT_EQ(stats.last_error_ns, 200U);
    EXPECT_EQ(stats.pauses, 0U);
    EXPECT_EQ(stats.recoveries, 0U);
}

}  // namespace
}  // namespace mooncake::tent::ub

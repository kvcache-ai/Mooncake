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
#include <limits>
#include <thread>
#include <utility>
#include <vector>

#include "adaptive_congestion_control.h"

namespace mooncake::adaptive_cc {
namespace {

Config testConfig(Mode mode = Mode::kEnforce) {
    Config config;
    config.mode = mode;
    config.min_window_bytes = 64;
    config.max_window_bytes = 256;
    config.target_drain_time_ns = 1'000;
    config.high_pressure_epochs = 2;
    config.low_pressure_epochs = 2;
    config.hard_error_threshold = 2;
    config.cooldown_ns = 10'000;
    config.probe_window_bytes = 16;
    return config;
}

PathHandle pathFor(DomainState& device, DomainState& route) {
    return {&device, &route, snapshot(device).generation,
            snapshot(route).generation};
}

void recordCurrent(DomainState& state, const Signals& signals) {
    recordSignals(state, snapshot(state).generation, signals);
}

TEST(AdaptiveCongestionControlTest, OffModeDoesNotAccountOrReject) {
    DomainState device(testConfig(Mode::kOff));
    DomainState route(testConfig(Mode::kOff));
    Permit permit;

    EXPECT_EQ(tryAcquire(pathFor(device, route), 1024, permit),
              Decision::kAllow);
    EXPECT_TRUE(permit.active());
    EXPECT_EQ(snapshot(device).inflight_bytes, 0u);
    EXPECT_TRUE(
        complete(permit, OutcomeClass::kSuccess, FailureScope::kOperation));
    EXPECT_FALSE(
        complete(permit, OutcomeClass::kSuccess, FailureScope::kOperation));
}

TEST(AdaptiveCongestionControlTest, OffModeIgnoresStaleGeneration) {
    DomainState device(testConfig(Mode::kOff));
    DomainState route(testConfig(Mode::kOff));
    const PathHandle stale = pathFor(device, route);
    resetGeneration(device, stale.device_generation + 1);
    resetGeneration(route, stale.route_generation + 1);
    Permit permit;

    EXPECT_EQ(tryAcquire(stale, 1024, permit), Decision::kAllow);
    EXPECT_TRUE(
        complete(permit, OutcomeClass::kSuccess, FailureScope::kOperation));
}

TEST(AdaptiveCongestionControlTest, ObserveModeAccountsWithoutEnforcingWindow) {
    DomainState device(testConfig(Mode::kObserve));
    DomainState route(testConfig(Mode::kObserve));
    Permit permit;

    EXPECT_EQ(tryAcquire(pathFor(device, route), 1024, permit),
              Decision::kAllow);
    EXPECT_EQ(snapshot(device).inflight_bytes, 1024u);
    EXPECT_EQ(snapshot(route).inflight_bytes, 1024u);
    EXPECT_TRUE(
        complete(permit, OutcomeClass::kSuccess, FailureScope::kOperation));
    EXPECT_EQ(snapshot(device).inflight_bytes, 0u);
    EXPECT_EQ(snapshot(route).inflight_bytes, 0u);
}

TEST(AdaptiveCongestionControlTest, ObserveModeAllowsStaleGeneration) {
    Config config = testConfig(Mode::kObserve);
    DomainState device(config, 2);
    DomainState route(config, 2);
    Permit permit;

    EXPECT_EQ(tryAcquire({&device, &route, 1, 1}, 64, permit),
              Decision::kAllow);
    EXPECT_TRUE(
        complete(permit, OutcomeClass::kSuccess, FailureScope::kOperation));
    EXPECT_EQ(snapshot(device).inflight_bytes, 0u);
    EXPECT_EQ(snapshot(route).inflight_bytes, 0u);
}

TEST(AdaptiveCongestionControlTest, MovingPermitTransfersItsReservation) {
    DomainState device(testConfig());
    DomainState route(testConfig());
    Permit original;
    ASSERT_EQ(tryAcquire(pathFor(device, route), 16, original),
              Decision::kAllow);

    Permit moved(std::move(original));

    EXPECT_FALSE(original.active());
    EXPECT_TRUE(moved.active());
    EXPECT_FALSE(
        complete(original, OutcomeClass::kSuccess, FailureScope::kOperation));
    EXPECT_TRUE(
        complete(moved, OutcomeClass::kSuccess, FailureScope::kOperation));
    EXPECT_EQ(snapshot(device).inflight_bytes, 0u);
    EXPECT_EQ(snapshot(route).inflight_bytes, 0u);
}

TEST(AdaptiveCongestionControlTest, MoveAssignmentReleasesPreviousPermit) {
    DomainState device(testConfig());
    DomainState route(testConfig());
    Permit source;
    Permit destination;
    ASSERT_EQ(tryAcquire(pathFor(device, route), 16, source), Decision::kAllow);
    ASSERT_EQ(tryAcquire(pathFor(device, route), 8, destination),
              Decision::kAllow);

    destination = std::move(source);

    EXPECT_EQ(snapshot(device).inflight_bytes, 16u);
    EXPECT_EQ(snapshot(route).inflight_bytes, 16u);
    EXPECT_TRUE(complete(destination, OutcomeClass::kSuccess,
                         FailureScope::kOperation));
    EXPECT_EQ(snapshot(device).inflight_bytes, 0u);
    EXPECT_EQ(snapshot(route).inflight_bytes, 0u);
}

TEST(AdaptiveCongestionControlTest, RouteRejectionRollsBackDeviceReservation) {
    Config route_config = testConfig();
    route_config.max_window_bytes = 32;
    route_config.min_window_bytes = 32;
    DomainState device(testConfig());
    DomainState route(route_config);
    Permit occupied;
    ASSERT_EQ(tryAcquire({nullptr, &route, 0, generation(route)}, 8, occupied),
              Decision::kAllow);
    Permit rejected;

    EXPECT_EQ(tryAcquire(pathFor(device, route), 40, rejected),
              Decision::kDefer);
    EXPECT_EQ(snapshot(device).inflight_bytes, 0u);
    EXPECT_EQ(snapshot(route).inflight_bytes, 8u);
    EXPECT_EQ(snapshot(route).deferred_bytes, 40u);
    EXPECT_TRUE(complete(occupied, OutcomeClass::kDerivedFlush,
                         FailureScope::kOperation));
}

TEST(AdaptiveCongestionControlTest, OversizedSliceIsAdmittedOnlyWhenEmpty) {
    DomainState device(testConfig());
    DomainState route(testConfig());
    Permit first, second;
    ASSERT_EQ(tryAcquire(pathFor(device, route), 512, first), Decision::kAllow);
    EXPECT_EQ(snapshot(route).inflight_bytes, 512u);
    EXPECT_EQ(tryAcquire(pathFor(device, route), 1, second), Decision::kDefer);
    EXPECT_TRUE(
        complete(first, OutcomeClass::kSuccess, FailureScope::kOperation));
    EXPECT_EQ(tryAcquire(pathFor(device, route), 512, second),
              Decision::kAllow);
    EXPECT_TRUE(
        complete(second, OutcomeClass::kSuccess, FailureScope::kOperation));
    EXPECT_EQ(snapshot(device).inflight_bytes, 0u);
}

TEST(AdaptiveCongestionControlTest, ReceiverPressureStaysOnItsRoute) {
    Config config = testConfig();
    config.high_pressure_epochs = 1;
    config.low_pressure_epochs = 1;
    DomainState device(config), route(config), other(config);
    Signals low;
    low.delivery_rate_bytes_per_sec = 10'000'000;
    low.successful_completions = 1;
    recordCurrent(device, low);
    controlTick(device, 1);
    ASSERT_EQ(snapshot(device).window_bytes, 80u);
    Permit failed;
    ASSERT_EQ(tryAcquire(pathFor(device, route), 8, failed), Decision::kAllow);
    ASSERT_TRUE(
        complete(failed, OutcomeClass::kReceiverPressure, FailureScope::kQp));
    controlTick(device, 2);
    controlTick(route, 2);
    EXPECT_EQ(snapshot(device).state, PathState::kHealthy);
    EXPECT_EQ(snapshot(device).window_bytes, 80u);
    EXPECT_EQ(snapshot(route).state, PathState::kCongested);
    EXPECT_EQ(snapshot(other).state, PathState::kHealthy);
}

TEST(AdaptiveCongestionControlTest, ConcurrentOversizedSlicesAdmitOnlyOne) {
    DomainState device(testConfig());
    constexpr int kThreads = 16;
    std::atomic<bool> start{false};
    std::atomic<int> attempted{0}, admitted{0};
    std::vector<std::thread> threads;
    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([&] {
            DomainState route(testConfig());
            Permit permit;
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            if (tryAcquire(pathFor(device, route), 512, permit) ==
                Decision::kAllow) {
                admitted.fetch_add(1, std::memory_order_relaxed);
            }
            attempted.fetch_add(1, std::memory_order_release);
            // Keep the winning reservation until all contenders attempted.
            while (attempted.load(std::memory_order_acquire) != kThreads) {
                std::this_thread::yield();
            }
            complete(permit, OutcomeClass::kDerivedFlush,
                     FailureScope::kOperation);
        });
    }
    start.store(true, std::memory_order_release);
    for (auto& thread : threads) thread.join();
    EXPECT_EQ(admitted.load(), 1);
    EXPECT_EQ(snapshot(device).inflight_bytes, 0u);
}

TEST(AdaptiveCongestionControlTest,
     PressureUsesHysteresisAndAdditiveIncreaseMultiplicativeDecrease) {
    DomainState domain(testConfig());
    Signals low;
    low.backlog_bytes = 1;
    low.delivery_rate_bytes_per_sec = 10'000'000;
    low.completed_bytes = 64;
    low.successful_completions = 1;
    recordCurrent(domain, low);
    controlTick(domain, 1'000);
    recordCurrent(domain, low);
    controlTick(domain, 2'000);
    EXPECT_EQ(snapshot(domain).state, PathState::kHealthy);
    EXPECT_GT(snapshot(domain).window_bytes, 64u);
    EXPECT_LE(snapshot(domain).window_bytes, 256u);

    Signals high;
    high.backlog_bytes = 256;
    high.delivery_rate_bytes_per_sec = 1'000'000;
    high.completed_bytes = 64;
    high.successful_completions = 1;
    recordCurrent(domain, high);
    controlTick(domain, 3'000);
    EXPECT_EQ(snapshot(domain).state, PathState::kHealthy);
    recordCurrent(domain, high);
    controlTick(domain, 4'000);
    EXPECT_EQ(snapshot(domain).state, PathState::kCongested);
    EXPECT_EQ(snapshot(domain).window_bytes, 64u);
}

TEST(AdaptiveCongestionControlTest, PollerStallDoesNotBecomeRouteFailure) {
    DomainState route(testConfig());
    Signals signals;
    signals.route_timeouts = 5;
    signals.poller_stalled = true;

    recordCurrent(route, signals);
    controlTick(route, 1'000);
    EXPECT_EQ(snapshot(route).state, PathState::kHealthy);
}

TEST(AdaptiveCongestionControlTest, ConcurrentPollerStallSamplesRemainAtomic) {
    DomainState route(testConfig());
    const uint32_t generation = snapshot(route).generation;
    std::atomic<bool> done{false};
    std::thread writer([&] {
        Signals signals;
        signals.route_timeouts = 1;
        signals.poller_stalled = true;
        for (int i = 0; i < 20'000; ++i) {
            recordSignals(route, generation, signals);
        }
        done.store(true, std::memory_order_release);
    });

    uint64_t now_ns = 1;
    while (!done.load(std::memory_order_acquire)) {
        controlTick(route, now_ns++);
    }
    writer.join();
    controlTick(route, now_ns);
    EXPECT_EQ(snapshot(route).state, PathState::kHealthy);
}

TEST(AdaptiveCongestionControlTest, DeviceFatalBypassesFailureThreshold) {
    Config config = testConfig();
    config.hard_error_threshold = 100;
    DomainState device(config);
    Signals fatal;
    fatal.fatal_failures = 1;

    recordCurrent(device, fatal);
    controlTick(device, 1'000);
    EXPECT_EQ(snapshot(device).state, PathState::kQuarantined);
}

TEST(AdaptiveCongestionControlTest, ZeroCountersPreservePendingFailure) {
    DomainState route(testConfig());
    Signals failed;
    failed.fatal_failures = 1;
    recordCurrent(route, failed);
    recordCurrent(route, Signals{});
    controlTick(route, 1);
    EXPECT_EQ(snapshot(route).state, PathState::kQuarantined);
}

TEST(AdaptiveCongestionControlTest, CqFatalQuarantinesRouteOnly) {
    DomainState device(testConfig());
    DomainState route(testConfig());
    Permit permit;
    ASSERT_EQ(tryAcquire(pathFor(device, route), 8, permit), Decision::kAllow);

    ASSERT_TRUE(complete(permit, OutcomeClass::kFatal, FailureScope::kCq));
    controlTick(device, 1'000);
    controlTick(route, 1'000);
    EXPECT_EQ(snapshot(device).state, PathState::kHealthy);
    EXPECT_EQ(snapshot(route).state, PathState::kQuarantined);
}

TEST(AdaptiveCongestionControlTest, QpFatalMakesRouteSuspectFirst) {
    DomainState device(testConfig());
    DomainState route(testConfig());
    Permit permit;
    ASSERT_EQ(tryAcquire(pathFor(device, route), 8, permit), Decision::kAllow);

    ASSERT_TRUE(complete(permit, OutcomeClass::kFatal, FailureScope::kQp));
    controlTick(device, 1'000);
    controlTick(route, 1'000);
    EXPECT_EQ(snapshot(device).state, PathState::kHealthy);
    EXPECT_EQ(snapshot(route).state, PathState::kSuspect);
}

TEST(AdaptiveCongestionControlTest, QuarantineProbesThenRecovers) {
    DomainState route(testConfig());
    Signals error;
    error.hard_errors = 1;
    recordCurrent(route, error);
    controlTick(route, 1'000);
    EXPECT_EQ(snapshot(route).state, PathState::kSuspect);
    recordCurrent(route, error);
    controlTick(route, 2'000);
    EXPECT_EQ(snapshot(route).state, PathState::kQuarantined);

    DomainState device(testConfig());
    Permit blocked;
    EXPECT_EQ(tryAcquire(pathFor(device, route), 1, blocked), Decision::kAvoid);
    controlTick(route, 11'999);
    EXPECT_EQ(snapshot(route).state, PathState::kQuarantined);
    controlTick(route, 12'000);
    EXPECT_EQ(snapshot(route).state, PathState::kProbing);

    Permit oversized_probe;
    ASSERT_EQ(tryAcquire(pathFor(device, route), 17, oversized_probe),
              Decision::kAllow);
    Permit probe;
    EXPECT_EQ(tryAcquire(pathFor(device, route), 8, probe), Decision::kDefer);
    ASSERT_TRUE(complete(oversized_probe, OutcomeClass::kDerivedFlush,
                         FailureScope::kOperation));
    ASSERT_EQ(tryAcquire(pathFor(device, route), 8, probe), Decision::kAllow);
    ASSERT_TRUE(
        complete(probe, OutcomeClass::kSuccess, FailureScope::kOperation));
    controlTick(route, 13'000);
    EXPECT_EQ(snapshot(route).state, PathState::kHealthy);
    EXPECT_EQ(snapshot(route).window_bytes, 64u);
}

TEST(AdaptiveCongestionControlTest, ProbeSuccessWaitsForInflightResults) {
    Config config = testConfig();
    config.hard_error_threshold = 1;
    DomainState device(config);
    DomainState route(config);
    Signals fatal;
    fatal.fatal_failures = 1;
    recordCurrent(route, fatal);
    controlTick(route, 1'000);
    controlTick(route, 11'000);
    ASSERT_EQ(snapshot(route).state, PathState::kProbing);

    Permit first;
    Permit second;
    ASSERT_EQ(tryAcquire(pathFor(device, route), 8, first), Decision::kAllow);
    ASSERT_EQ(tryAcquire(pathFor(device, route), 8, second), Decision::kAllow);

    ASSERT_TRUE(
        complete(first, OutcomeClass::kSuccess, FailureScope::kOperation));
    controlTick(route, 12'000);
    EXPECT_EQ(snapshot(route).state, PathState::kProbing);
    Permit late;
    EXPECT_EQ(tryAcquire(pathFor(device, route), 1, late), Decision::kDefer);

    ASSERT_TRUE(
        complete(second, OutcomeClass::kRouteTimeout, FailureScope::kRoute));
    controlTick(route, 13'000);
    EXPECT_EQ(snapshot(route).state, PathState::kQuarantined);
}

TEST(AdaptiveCongestionControlTest, ProbeRecoversAfterAllSuccesses) {
    Config config = testConfig();
    config.hard_error_threshold = 1;
    DomainState device(config);
    DomainState route(config);
    Signals fatal;
    fatal.fatal_failures = 1;
    recordCurrent(route, fatal);
    controlTick(route, 1'000);
    controlTick(route, 11'000);

    Permit first;
    Permit second;
    ASSERT_EQ(tryAcquire(pathFor(device, route), 8, first), Decision::kAllow);
    ASSERT_EQ(tryAcquire(pathFor(device, route), 8, second), Decision::kAllow);
    ASSERT_TRUE(
        complete(first, OutcomeClass::kSuccess, FailureScope::kOperation));
    controlTick(route, 12'000);
    ASSERT_EQ(snapshot(route).state, PathState::kProbing);

    ASSERT_TRUE(
        complete(second, OutcomeClass::kSuccess, FailureScope::kOperation));
    controlTick(route, 13'000);
    EXPECT_EQ(snapshot(route).state, PathState::kHealthy);
}

TEST(AdaptiveCongestionControlTest, ZeroByteProbesStillWaitForEveryOutcome) {
    Config config = testConfig();
    config.hard_error_threshold = 1;
    DomainState device(config);
    DomainState route(config);
    Signals fatal;
    fatal.fatal_failures = 1;
    recordCurrent(route, fatal);
    controlTick(route, 1'000);
    controlTick(route, 11'000);

    Permit first;
    Permit second;
    ASSERT_EQ(tryAcquire(pathFor(device, route), 0, first), Decision::kAllow);
    ASSERT_EQ(tryAcquire(pathFor(device, route), 0, second), Decision::kAllow);
    ASSERT_TRUE(
        complete(first, OutcomeClass::kSuccess, FailureScope::kOperation));
    controlTick(route, 12'000);
    ASSERT_EQ(snapshot(route).state, PathState::kProbing);

    ASSERT_TRUE(
        complete(second, OutcomeClass::kRouteTimeout, FailureScope::kRoute));
    controlTick(route, 13'000);
    EXPECT_EQ(snapshot(route).state, PathState::kQuarantined);
}

TEST(AdaptiveCongestionControlTest, RouteRejectionRollsBackProbePermit) {
    Config config = testConfig();
    config.hard_error_threshold = 1;
    DomainState device(config);
    DomainState route(config);
    Signals fatal;
    fatal.fatal_failures = 1;
    recordCurrent(device, fatal);
    recordCurrent(route, fatal);
    controlTick(device, 1'000);
    controlTick(route, 1'000);
    controlTick(device, 11'000);

    Permit rejected;
    EXPECT_EQ(tryAcquire(pathFor(device, route), 8, rejected),
              Decision::kAvoid);
    EXPECT_FALSE(rejected.active());

    Permit probe;
    ASSERT_EQ(tryAcquire({&device, nullptr, generation(device), 0}, 8, probe),
              Decision::kAllow);
    ASSERT_TRUE(
        complete(probe, OutcomeClass::kSuccess, FailureScope::kOperation));
    controlTick(device, 12'000);
    EXPECT_EQ(snapshot(device).state, PathState::kHealthy);
}

TEST(AdaptiveCongestionControlTest, ProbeFailureWinsConcurrentControlTick) {
    for (uint32_t round = 0; round < 200; ++round) {
        Config config = testConfig();
        config.hard_error_threshold = 1;
        DomainState device(config);
        DomainState route(config);
        Signals fatal;
        fatal.fatal_failures = 1;
        recordCurrent(route, fatal);
        controlTick(route, 1'000);
        controlTick(route, 11'000);

        Permit first;
        Permit second;
        ASSERT_EQ(tryAcquire(pathFor(device, route), 8, first),
                  Decision::kAllow);
        ASSERT_EQ(tryAcquire(pathFor(device, route), 8, second),
                  Decision::kAllow);
        ASSERT_TRUE(
            complete(first, OutcomeClass::kSuccess, FailureScope::kOperation));
        controlTick(route, 12'000);
        ASSERT_EQ(snapshot(route).state, PathState::kProbing);

        std::atomic<bool> start{false};
        bool completed = false;
        std::thread failure([&] {
            while (!start.load(std::memory_order_acquire)) {
            }
            completed = complete(second, OutcomeClass::kRouteTimeout,
                                 FailureScope::kRoute);
        });
        std::thread tick([&] {
            while (!start.load(std::memory_order_acquire)) {
            }
            controlTick(route, 13'000);
        });
        start.store(true, std::memory_order_release);
        failure.join();
        tick.join();
        ASSERT_TRUE(completed);
        controlTick(route, 14'000);
        EXPECT_EQ(snapshot(route).state, PathState::kQuarantined)
            << "round=" << round;
    }
}

TEST(AdaptiveCongestionControlTest, ProbeAdmissionClosesAcrossAcquireRace) {
    for (uint32_t round = 0; round < 200; ++round) {
        Config config = testConfig();
        config.hard_error_threshold = 1;
        DomainState device(config);
        DomainState route(config);
        Signals fatal;
        fatal.fatal_failures = 1;
        recordCurrent(route, fatal);
        controlTick(route, 1'000);
        controlTick(route, 11'000);

        Permit first;
        ASSERT_EQ(tryAcquire(pathFor(device, route), 8, first),
                  Decision::kAllow);
        ASSERT_TRUE(
            complete(first, OutcomeClass::kSuccess, FailureScope::kOperation));

        Permit concurrent;
        Decision decision = Decision::kAvoid;
        std::atomic<bool> start{false};
        std::thread acquire([&] {
            while (!start.load(std::memory_order_acquire)) {
            }
            decision = tryAcquire(pathFor(device, route), 1, concurrent);
        });
        std::thread tick([&] {
            while (!start.load(std::memory_order_acquire)) {
            }
            controlTick(route, 12'000);
        });
        start.store(true, std::memory_order_release);
        acquire.join();
        tick.join();
        if (decision == Decision::kAllow) {
            ASSERT_TRUE(complete(concurrent, OutcomeClass::kSuccess,
                                 FailureScope::kOperation));
        }
        controlTick(route, 13'000);
        EXPECT_EQ(snapshot(route).state, PathState::kHealthy)
            << "round=" << round;
    }
}

TEST(AdaptiveCongestionControlTest, FailedProbeStartsAnotherCooldown) {
    DomainState device(testConfig());
    DomainState route(testConfig());
    Signals errors;
    errors.hard_errors = 2;
    recordCurrent(route, errors);
    controlTick(route, 1'000);
    ASSERT_EQ(snapshot(route).state, PathState::kQuarantined);
    controlTick(route, 11'000);
    ASSERT_EQ(snapshot(route).state, PathState::kProbing);

    Permit failed_probe;
    ASSERT_EQ(tryAcquire(pathFor(device, route), 8, failed_probe),
              Decision::kAllow);
    ASSERT_TRUE(complete(failed_probe, OutcomeClass::kRouteTimeout,
                         FailureScope::kRoute));
    controlTick(route, 12'000);
    EXPECT_EQ(snapshot(route).state, PathState::kQuarantined);
    controlTick(route, 21'999);
    EXPECT_EQ(snapshot(route).state, PathState::kQuarantined);
    controlTick(route, 22'000);
    EXPECT_EQ(snapshot(route).state, PathState::kProbing);
}

TEST(AdaptiveCongestionControlTest,
     OldProbeDoesNotBlockOrCompleteANewerProbeEpoch) {
    DomainState device(testConfig());
    DomainState route(testConfig());
    Signals fatal;
    fatal.fatal_failures = 1;
    recordCurrent(route, fatal);
    controlTick(route, 1'000);
    controlTick(route, 11'000);
    ASSERT_EQ(snapshot(route).state, PathState::kProbing);

    Permit old_probe;
    Permit failed_probe;
    ASSERT_EQ(tryAcquire(pathFor(device, route), 8, old_probe),
              Decision::kAllow);
    ASSERT_EQ(tryAcquire(pathFor(device, route), 8, failed_probe),
              Decision::kAllow);
    ASSERT_TRUE(complete(failed_probe, OutcomeClass::kRouteTimeout,
                         FailureScope::kRoute));
    controlTick(route, 12'000);
    ASSERT_EQ(snapshot(route).state, PathState::kQuarantined);
    controlTick(route, 22'000);
    ASSERT_EQ(snapshot(route).state, PathState::kProbing);

    Permit fresh_probe;
    ASSERT_EQ(tryAcquire(pathFor(device, route), 8, fresh_probe),
              Decision::kAllow);
    ASSERT_TRUE(complete(fresh_probe, OutcomeClass::kSuccess,
                         FailureScope::kOperation));
    controlTick(route, 23'000);
    EXPECT_EQ(snapshot(route).state, PathState::kHealthy);

    ASSERT_TRUE(
        complete(old_probe, OutcomeClass::kSuccess, FailureScope::kOperation));
    controlTick(route, 24'000);
    EXPECT_EQ(snapshot(route).state, PathState::kHealthy);
}

TEST(AdaptiveCongestionControlTest, OldProbeFailureCannotFailANewerProbeEpoch) {
    DomainState device(testConfig());
    DomainState route(testConfig());
    Signals fatal;
    fatal.fatal_failures = 1;
    recordCurrent(route, fatal);
    controlTick(route, 1'000);
    controlTick(route, 11'000);
    ASSERT_EQ(snapshot(route).state, PathState::kProbing);

    Permit old_probe;
    Permit failed_probe;
    ASSERT_EQ(tryAcquire(pathFor(device, route), 8, old_probe),
              Decision::kAllow);
    ASSERT_EQ(tryAcquire(pathFor(device, route), 8, failed_probe),
              Decision::kAllow);
    ASSERT_TRUE(complete(failed_probe, OutcomeClass::kRouteTimeout,
                         FailureScope::kRoute));
    controlTick(route, 12'000);
    controlTick(route, 22'000);
    ASSERT_EQ(snapshot(route).state, PathState::kProbing);

    ASSERT_TRUE(
        complete(old_probe, OutcomeClass::kRouteTimeout, FailureScope::kRoute));
    controlTick(route, 23'000);
    EXPECT_EQ(snapshot(route).state, PathState::kProbing);
}

TEST(AdaptiveCongestionControlTest, OldSuccessCannotCompleteAProbe) {
    Config config = testConfig();
    config.hard_error_threshold = 1;
    DomainState device(config);
    DomainState route(config);
    Permit old_permit;
    ASSERT_EQ(tryAcquire(pathFor(device, route), 8, old_permit),
              Decision::kAllow);

    Signals fatal;
    fatal.fatal_failures = 1;
    recordCurrent(route, fatal);
    controlTick(route, 1'000);
    ASSERT_EQ(snapshot(route).state, PathState::kQuarantined);
    controlTick(route, 11'000);
    ASSERT_EQ(snapshot(route).state, PathState::kProbing);

    ASSERT_TRUE(
        complete(old_permit, OutcomeClass::kSuccess, FailureScope::kOperation));
    controlTick(route, 12'000);
    EXPECT_EQ(snapshot(route).state, PathState::kProbing);

    Permit probe;
    ASSERT_EQ(tryAcquire(pathFor(device, route), 8, probe), Decision::kAllow);
    ASSERT_TRUE(
        complete(probe, OutcomeClass::kSuccess, FailureScope::kOperation));
    controlTick(route, 13'000);
    EXPECT_EQ(snapshot(route).state, PathState::kHealthy);
}

TEST(AdaptiveCongestionControlTest, SaturatedFailureCountsStillQuarantine) {
    DomainState route(testConfig());
    Signals failures;
    failures.hard_errors = std::numeric_limits<uint64_t>::max();
    failures.route_timeouts = 1;

    recordCurrent(route, failures);
    controlTick(route, 1'000);
    EXPECT_EQ(snapshot(route).state, PathState::kQuarantined);
}

TEST(AdaptiveCongestionControlTest, OldGenerationFeedbackIsIgnored) {
    DomainState device(testConfig());
    DomainState route(testConfig());
    Permit old_permit;
    ASSERT_EQ(tryAcquire(pathFor(device, route), 16, old_permit),
              Decision::kAllow);

    resetGeneration(device, 2);
    resetGeneration(route, 2);
    EXPECT_TRUE(
        complete(old_permit, OutcomeClass::kFatal, FailureScope::kRoute));
    EXPECT_EQ(snapshot(device).inflight_bytes, 0u);
    EXPECT_EQ(snapshot(route).inflight_bytes, 0u);
    controlTick(route, 1'000);
    EXPECT_EQ(snapshot(route).state, PathState::kHealthy);
    EXPECT_EQ(snapshot(route).generation, 2u);
    EXPECT_EQ(generation(route), 2u);
}

TEST(AdaptiveCongestionControlTest, LateSignalsCarryTheirGeneration) {
    DomainState route(testConfig());
    const uint32_t old_generation = snapshot(route).generation;
    resetGeneration(route, old_generation + 1);

    Signals old_error;
    old_error.hard_errors = 10;
    recordSignals(route, old_generation, old_error);
    controlTick(route, 1'000);
    EXPECT_EQ(snapshot(route).state, PathState::kHealthy);
}

TEST(AdaptiveCongestionControlTest, ResetSerializesWithPolicyCommit) {
    Config config = testConfig();
    config.hard_error_threshold = 1;
    DomainState route(config);

    for (uint32_t round = 0; round < 500; ++round) {
        const uint32_t old_generation = snapshot(route).generation;
        Signals error;
        error.hard_errors = 1;
        recordSignals(route, old_generation, error);
        std::atomic<bool> start{false};
        std::thread tick([&] {
            while (!start.load(std::memory_order_acquire)) {
            }
            controlTick(route, round + 1);
        });
        std::thread reset([&] {
            while (!start.load(std::memory_order_acquire)) {
            }
            resetGeneration(route, old_generation + 1);
        });
        start.store(true, std::memory_order_release);
        tick.join();
        reset.join();

        EXPECT_EQ(snapshot(route).generation, old_generation + 1);
        EXPECT_EQ(snapshot(route).state, PathState::kHealthy);
    }
}

TEST(AdaptiveCongestionControlTest, StalePathHandleIsRejected) {
    DomainState device(testConfig());
    DomainState route(testConfig());
    const PathHandle stale = pathFor(device, route);
    resetGeneration(route, stale.route_generation + 1);

    Permit permit;
    EXPECT_EQ(tryAcquire(stale, 8, permit), Decision::kAvoid);
    EXPECT_FALSE(permit.active());
    EXPECT_EQ(snapshot(device).inflight_bytes, 0u);
    EXPECT_EQ(snapshot(route).inflight_bytes, 0u);
}

TEST(AdaptiveCongestionControlTest, ConcurrentPermitsBalanceExactly) {
    DomainState device(testConfig());
    DomainState route(testConfig());
    std::atomic<uint64_t> allowed{0};
    std::atomic<uint64_t> deferred{0};
    std::atomic<uint64_t> completion_failures{0};
    std::vector<std::thread> threads;

    for (int thread = 0; thread < 8; ++thread) {
        threads.emplace_back([&] {
            for (int i = 0; i < 2000; ++i) {
                Permit permit;
                if (tryAcquire(pathFor(device, route), 8, permit) ==
                    Decision::kAllow) {
                    allowed.fetch_add(1, std::memory_order_relaxed);
                    if (!complete(permit, OutcomeClass::kSuccess,
                                  FailureScope::kOperation)) {
                        completion_failures.fetch_add(
                            1, std::memory_order_relaxed);
                    }
                } else {
                    deferred.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }
    for (auto& thread : threads) thread.join();

    EXPECT_GT(allowed.load(), 0u);
    EXPECT_EQ(allowed.load() + deferred.load(), 16'000u);
    EXPECT_EQ(completion_failures.load(), 0u);
    EXPECT_EQ(snapshot(device).inflight_bytes, 0u);
    EXPECT_EQ(snapshot(route).inflight_bytes, 0u);
}

}  // namespace
}  // namespace mooncake::adaptive_cc

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

namespace mooncake::adaptive_congestion_control {
namespace {

Config testConfig(Mode mode = Mode::kEnforce) {
    Config config;
    config.mode = mode;
    config.min_window_bytes = 64;
    config.max_window_bytes = 256;
    config.target_drain_time_ns = 1'000;
    config.high_pressure_epochs = 2;
    config.low_pressure_epochs = 2;
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

TEST(AdaptiveCongestionControlTest, SuccessGrowsWithHighDrainOrNoRateSample) {
    for (bool has_rate : {false, true}) {
        DomainState domain(testConfig());
        Signals signals;
        signals.backlog_bytes = 256;
        if (has_rate) signals.delivery_rate_bytes_per_sec = 1;
        signals.successful_completions = 1;
        recordCurrent(domain, signals);
        controlTick(domain, 1);
        EXPECT_EQ(snapshot(domain).window_bytes, 80u);
        EXPECT_EQ(snapshot(domain).state, PathState::kHealthy);
    }
}

TEST(AdaptiveCongestionControlTest, BacklogWithoutSuccessPreservesWindow) {
    DomainState domain(testConfig());
    Signals success;
    success.successful_completions = 1;
    recordCurrent(domain, success);
    controlTick(domain, 1);
    Signals backlog;
    backlog.backlog_bytes = std::numeric_limits<uint64_t>::max();
    backlog.delivery_rate_bytes_per_sec = 1;
    for (uint64_t epoch = 2; epoch < 6; ++epoch) {
        recordCurrent(domain, backlog);
        controlTick(domain, epoch);
        EXPECT_EQ(snapshot(domain).window_bytes, 80u);
    }
}

TEST(AdaptiveCongestionControlTest, RepeatedSuccessGrowsOncePerNonemptyEpoch) {
    for (Mode mode : {Mode::kEnforce, Mode::kObserve}) {
        DomainState device(testConfig(mode)), route(testConfig(mode));
        Signals successes;
        successes.successful_completions = std::numeric_limits<uint64_t>::max();
        for (uint64_t epoch = 1; epoch <= 4; ++epoch) {
            for (int i = 0; i < 32; ++i) {
                Permit permit;
                ASSERT_EQ(tryAcquire(pathFor(device, route), 8, permit),
                          Decision::kAllow);
                ASSERT_TRUE(complete(permit, OutcomeClass::kSuccess,
                                     FailureScope::kOperation));
                EXPECT_FALSE(complete(permit, OutcomeClass::kSuccess,
                                      FailureScope::kOperation));
                recordCurrent(device, successes);
                recordCurrent(route, successes);
            }
            controlTick(device, epoch);
            controlTick(route, epoch);
            EXPECT_EQ(snapshot(device).window_bytes, 64u + epoch * 16);
            EXPECT_EQ(snapshot(route).window_bytes, 64u + epoch * 16);
            EXPECT_EQ(snapshot(device).inflight_bytes, 0u);
            EXPECT_EQ(snapshot(route).inflight_bytes, 0u);
            controlTick(device, epoch);
            controlTick(route, epoch);
            EXPECT_EQ(snapshot(device).window_bytes, 64u + epoch * 16);
            EXPECT_EQ(snapshot(route).window_bytes, 64u + epoch * 16);
        }
    }
}

TEST(AdaptiveCongestionControlTest, PureSuccessTracksEpochAndFullGeneration) {
    DomainState device(testConfig(), 0), route(testConfig(), 0);
    ASSERT_EQ(generation(device), 1u);
    ASSERT_EQ(generation(route), 1u);
    for (uint64_t epoch = 1; epoch <= 4; ++epoch) {
        for (int i = 0; i < 32; ++i) {
            Permit permit;
            ASSERT_EQ(tryAcquire(pathFor(device, route), 8, permit),
                      Decision::kAllow);
            ASSERT_TRUE(complete(permit, OutcomeClass::kSuccess,
                                 FailureScope::kOperation));
        }
        controlTick(device, epoch);
        controlTick(route, epoch);
        EXPECT_EQ(snapshot(device).window_bytes, 64u + epoch * 16);
        EXPECT_EQ(snapshot(route).window_bytes, 64u + epoch * 16);
        controlTick(device, epoch);
        controlTick(route, epoch);
        EXPECT_EQ(snapshot(device).window_bytes, 64u + epoch * 16);
        EXPECT_EQ(snapshot(route).window_bytes, 64u + epoch * 16);
    }
    Permit stale;
    ASSERT_EQ(tryAcquire(pathFor(device, route), 8, stale), Decision::kAllow);
    for (uint32_t next : {0x80000001u, 0xffffffffu, 0u}) {
        resetGeneration(device, next);
        resetGeneration(route, next);
        if (stale.active()) {
            ASSERT_TRUE(complete(stale, OutcomeClass::kSuccess,
                                 FailureScope::kOperation));
        }
        controlTick(device, 5);
        controlTick(route, 5);
        EXPECT_EQ(snapshot(device).window_bytes, 64u);
        EXPECT_EQ(snapshot(route).window_bytes, 64u);
        Permit fresh;
        ASSERT_EQ(tryAcquire(pathFor(device, route), 8, fresh),
                  Decision::kAllow);
        ASSERT_TRUE(
            complete(fresh, OutcomeClass::kSuccess, FailureScope::kOperation));
        controlTick(device, 6);
        controlTick(route, 6);
        EXPECT_EQ(snapshot(device).window_bytes, 80u);
        EXPECT_EQ(snapshot(route).window_bytes, 80u);
        EXPECT_EQ(snapshot(device).inflight_bytes, 0u);
        EXPECT_EQ(snapshot(route).inflight_bytes, 0u);
    }
}

TEST(AdaptiveCongestionControlTest,
     RepeatedSuccessPreservesMixedSignalPriority) {
    DomainState domain(testConfig());
    Signals success;
    success.successful_completions = 100;
    for (uint64_t epoch = 1; epoch <= 8; ++epoch) {
        recordCurrent(domain, success);
        controlTick(domain, epoch);
    }
    ASSERT_EQ(snapshot(domain).window_bytes, 192u);
    for (int i = 0; i < 32; ++i) recordCurrent(domain, success);
    Signals mixed = success;
    mixed.receiver_pressure = 3;
    mixed.hard_errors = 1;
    recordCurrent(domain, mixed);
    controlTick(domain, 9);
    EXPECT_EQ(snapshot(domain).window_bytes, 96u);
    EXPECT_EQ(snapshot(domain).state, PathState::kCongested);

    mixed.receiver_pressure = 0;
    for (int i = 0; i < 32; ++i) recordCurrent(domain, mixed);
    controlTick(domain, 10);
    EXPECT_EQ(snapshot(domain).window_bytes, 96u);
    recordCurrent(domain, success);
    controlTick(domain, 11);
    EXPECT_EQ(snapshot(domain).window_bytes, 112u);
}

TEST(AdaptiveCongestionControlTest,
     ConcurrentSuccessSurvivesTickAndResetEpochs) {
    Config config = testConfig();
    config.max_window_bytes = 4096;
    DomainState domain(config);
    Signals successes;
    successes.successful_completions = 16;
    for (uint32_t round = 1; round <= 32; ++round) {
        std::vector<std::thread> writers;
        const uint32_t current = generation(domain);
        for (int writer = 0; writer < 4; ++writer) {
            writers.emplace_back([&] {
                for (int i = 0; i < 128; ++i)
                    recordSignals(domain, current, successes);
            });
        }
        for (auto& writer : writers) writer.join();
        controlTick(domain, round);
        EXPECT_EQ(snapshot(domain).window_bytes, 64u + round * 16);
    }
    const uint32_t old_generation = generation(domain);
    std::atomic<bool> start{false};
    std::thread late([&] {
        while (!start.load(std::memory_order_acquire)) {
        }
        for (int i = 0; i < 4096; ++i)
            recordSignals(domain, old_generation, successes);
    });
    start.store(true, std::memory_order_release);
    resetGeneration(domain, old_generation + 1);
    late.join();
    controlTick(domain, 33);
    EXPECT_EQ(snapshot(domain).window_bytes, 64u);
    recordCurrent(domain, successes);
    controlTick(domain, 34);
    EXPECT_EQ(snapshot(domain).window_bytes, 80u);
    controlTick(domain, 35);
    EXPECT_EQ(snapshot(domain).window_bytes, 80u);
}

TEST(AdaptiveCongestionControlTest, ReceiverPressureHalvesOnceBeforeErrorVeto) {
    DomainState domain(testConfig());
    Signals signals;
    signals.successful_completions = 1;
    for (uint64_t epoch = 1; epoch <= 8; ++epoch) {
        recordCurrent(domain, signals);
        controlTick(domain, epoch);
    }
    ASSERT_EQ(snapshot(domain).window_bytes, 192u);
    signals.receiver_pressure = 100;
    signals.fatal_failures = 1;
    recordCurrent(domain, signals);
    controlTick(domain, 9);
    EXPECT_EQ(snapshot(domain).window_bytes, 96u);
    EXPECT_EQ(snapshot(domain).state, PathState::kCongested);
    controlTick(domain, 10);
    EXPECT_EQ(snapshot(domain).window_bytes, 96u);
}

TEST(AdaptiveCongestionControlTest, RootErrorVetoUsesFailureScopeForOneEpoch) {
    for (OutcomeClass outcome :
         {OutcomeClass::kRouteTimeout, OutcomeClass::kFatal,
          OutcomeClass::kLocalConfiguration, OutcomeClass::kRemoteMetadata}) {
        for (FailureScope scope :
             {FailureScope::kOperation, FailureScope::kQp, FailureScope::kCq,
              FailureScope::kRoute, FailureScope::kPort,
              FailureScope::kDevice}) {
            DomainState device(testConfig()), route(testConfig()),
                sibling(testConfig());
            Permit failed, successful;
            ASSERT_EQ(tryAcquire(pathFor(device, route), 8, failed),
                      Decision::kAllow);
            ASSERT_EQ(tryAcquire(pathFor(device, sibling), 8, successful),
                      Decision::kAllow);
            ASSERT_TRUE(complete(successful, OutcomeClass::kSuccess,
                                 FailureScope::kOperation));
            Signals success;
            success.successful_completions = 1;
            recordCurrent(route, success);
            ASSERT_TRUE(complete(failed, outcome, scope));
            controlTick(device, 1);
            controlTick(route, 1);
            controlTick(sibling, 1);
            const bool shared = scope == FailureScope::kCq ||
                                scope == FailureScope::kPort ||
                                scope == FailureScope::kDevice;
            EXPECT_EQ(snapshot(device).window_bytes, shared ? 64u : 80u);
            EXPECT_EQ(snapshot(route).window_bytes, 64u);
            EXPECT_EQ(snapshot(sibling).window_bytes, 80u);
            recordCurrent(device, success);
            recordCurrent(route, success);
            controlTick(device, 2);
            controlTick(route, 2);
            EXPECT_EQ(snapshot(device).window_bytes, shared ? 80u : 96u);
            EXPECT_EQ(snapshot(route).window_bytes, 80u);
        }
    }
}

TEST(AdaptiveCongestionControlTest, PolicyModesPreserveOffAndObserveAdmission) {
    for (Mode mode : {Mode::kOff, Mode::kObserve}) {
        DomainState domain(testConfig(mode));
        Signals signals;
        signals.successful_completions = 1;
        recordCurrent(domain, signals);
        controlTick(domain, 1);
        EXPECT_EQ(snapshot(domain).window_bytes,
                  mode == Mode::kOff ? 64u : 80u);
        signals.hard_errors = 1;
        recordCurrent(domain, signals);
        controlTick(domain, 2);
        EXPECT_EQ(snapshot(domain).window_bytes,
                  mode == Mode::kOff ? 64u : 80u);
        signals.receiver_pressure = 1;
        recordCurrent(domain, signals);
        controlTick(domain, 3);
        EXPECT_EQ(snapshot(domain).window_bytes, 64u);
        Permit permit;
        ASSERT_EQ(
            tryAcquire({nullptr, &domain, 0, generation(domain)}, 1024, permit),
            Decision::kAllow);
        EXPECT_TRUE(complete(permit, OutcomeClass::kDerivedFlush,
                             FailureScope::kOperation));
    }
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

TEST(AdaptiveCongestionControlTest,
     FatalAndTimeoutOutcomesNeverIsolateDomains) {
    for (Mode mode : {Mode::kOff, Mode::kObserve, Mode::kEnforce}) {
        for (OutcomeClass outcome :
             {OutcomeClass::kFatal, OutcomeClass::kRouteTimeout}) {
            for (FailureScope scope :
                 {FailureScope::kOperation, FailureScope::kQp,
                  FailureScope::kCq, FailureScope::kRoute, FailureScope::kPort,
                  FailureScope::kDevice}) {
                DomainState device(testConfig(mode)), route(testConfig(mode));
                for (int i = 0; i < 8; ++i) {
                    Permit permit;
                    ASSERT_EQ(tryAcquire(pathFor(device, route), 8, permit),
                              Decision::kAllow);
                    ASSERT_TRUE(complete(permit, outcome, scope));
                    EXPECT_FALSE(complete(permit, outcome, scope));
                    controlTick(device, i + 1);
                    controlTick(route, i + 1);
                }
                EXPECT_EQ(snapshot(device).state, PathState::kHealthy);
                EXPECT_EQ(snapshot(route).state, PathState::kHealthy);
                EXPECT_EQ(snapshot(device).window_bytes, 64u);
                EXPECT_EQ(snapshot(route).window_bytes, 64u);
                EXPECT_EQ(snapshot(device).inflight_bytes, 0u);
                EXPECT_EQ(snapshot(route).inflight_bytes, 0u);
            }
        }
    }
}

TEST(AdaptiveCongestionControlTest, LegacyFailureSignalsDoNotControlAdmission) {
    DomainState route(testConfig());
    Signals failures;
    failures.hard_errors = std::numeric_limits<uint64_t>::max();
    failures.fatal_failures = std::numeric_limits<uint64_t>::max();
    failures.route_timeouts = std::numeric_limits<uint64_t>::max();
    recordCurrent(route, failures);
    controlTick(route, 1);
    EXPECT_EQ(snapshot(route).state, PathState::kHealthy);
    Permit permit;
    EXPECT_EQ(tryAcquire({nullptr, &route, 0, generation(route)}, 64, permit),
              Decision::kAllow);
    EXPECT_TRUE(complete(permit, OutcomeClass::kDerivedFlush,
                         FailureScope::kOperation));
}

TEST(AdaptiveCongestionControlTest, AdmissionEvidenceIdentifiesBlockingDomain) {
    DomainState device(testConfig()), route(testConfig());
    Permit occupied, rejected;
    ASSERT_EQ(tryAcquire(pathFor(device, route), 64, occupied),
              Decision::kAllow);
    AdmissionEvidence evidence;
    EXPECT_EQ(tryAcquire(pathFor(device, route), 8, rejected, &evidence),
              Decision::kDefer);
    EXPECT_EQ(evidence.domain, AdmissionDomain::kDevice);
    EXPECT_EQ(evidence.reason, AdmissionReason::kWindow);
    EXPECT_EQ(snapshot(device).inflight_bytes, 64u);
    EXPECT_EQ(snapshot(device).deferred_bytes, 8u);
    EXPECT_EQ(snapshot(route).deferred_bytes, 0u);
    ASSERT_TRUE(complete(occupied, OutcomeClass::kDerivedFlush,
                         FailureScope::kOperation));
    ASSERT_EQ(tryAcquire({nullptr, &route, 0, generation(route)}, 64, occupied),
              Decision::kAllow);
    EXPECT_EQ(tryAcquire(pathFor(device, route), 8, rejected, &evidence),
              Decision::kDefer);
    EXPECT_EQ(evidence.domain, AdmissionDomain::kRoute);
    EXPECT_EQ(evidence.reason, AdmissionReason::kWindow);
    EXPECT_EQ(snapshot(route).inflight_bytes, 64u);
    EXPECT_EQ(snapshot(route).deferred_bytes, 8u);
    EXPECT_EQ(snapshot(device).inflight_bytes, 0u);
    ASSERT_TRUE(complete(occupied, OutcomeClass::kDerivedFlush,
                         FailureScope::kOperation));
}

TEST(AdaptiveCongestionControlTest, EvidenceChangesOnlyOnRejection) {
    DomainState device(testConfig()), route(testConfig());
    Permit permit;
    AdmissionEvidence evidence;
    evidence.domain = AdmissionDomain::kRoute;
    evidence.reason = AdmissionReason::kStaleGeneration;
    ASSERT_EQ(tryAcquire(pathFor(device, route), 8, permit, &evidence),
              Decision::kAllow);
    EXPECT_EQ(evidence.domain, AdmissionDomain::kRoute);
    EXPECT_EQ(evidence.reason, AdmissionReason::kStaleGeneration);
    EXPECT_EQ(tryAcquire(pathFor(device, route), 8, permit, &evidence),
              Decision::kDefer);
    EXPECT_EQ(evidence.domain, AdmissionDomain::kNone);
    EXPECT_EQ(evidence.reason, AdmissionReason::kNone);
    EXPECT_EQ(snapshot(device).inflight_bytes, 8u);
    EXPECT_TRUE(
        complete(permit, OutcomeClass::kSuccess, FailureScope::kOperation));
}

TEST(AdaptiveCongestionControlTest, OffModeIgnoresPressureAndFailurePolicy) {
    DomainState domain(testConfig(Mode::kOff));
    Signals signals;
    signals.receiver_pressure = 100;
    signals.hard_errors = 100;
    signals.fatal_failures = 100;
    for (int i = 0; i < 10; ++i) {
        recordCurrent(domain, signals);
        controlTick(domain, i);
    }
    EXPECT_EQ(snapshot(domain).state, PathState::kHealthy);
    EXPECT_EQ(snapshot(domain).window_bytes, 64u);
    EXPECT_EQ(snapshot(domain).deferred_bytes, 0u);
}

TEST(AdaptiveCongestionControlTest, OldGenerationFeedbackIsIgnored) {
    Config config = testConfig();
    config.high_pressure_epochs = 1;
    DomainState device(config);
    DomainState route(config);
    Permit old_permit;
    ASSERT_EQ(tryAcquire(pathFor(device, route), 16, old_permit),
              Decision::kAllow);

    resetGeneration(device, 2);
    resetGeneration(route, 2);
    EXPECT_TRUE(complete(old_permit, OutcomeClass::kReceiverPressure,
                         FailureScope::kRoute));
    EXPECT_EQ(snapshot(device).inflight_bytes, 0u);
    EXPECT_EQ(snapshot(route).inflight_bytes, 0u);
    controlTick(route, 1'000);
    EXPECT_EQ(snapshot(route).state, PathState::kHealthy);
    EXPECT_EQ(snapshot(route).generation, 2u);
    EXPECT_EQ(generation(route), 2u);
}

TEST(AdaptiveCongestionControlTest, PermitGenerationValidityPrecedesRelease) {
    DomainState device(testConfig());
    DomainState route(testConfig());
    Permit permit;
    ASSERT_EQ(tryAcquire(pathFor(device, route), 16, permit), Decision::kAllow);
    EXPECT_TRUE(isCurrentGeneration(permit));

    resetGeneration(route, generation(route) + 1);
    EXPECT_FALSE(isCurrentGeneration(permit));
    EXPECT_TRUE(complete(permit, OutcomeClass::kFatal, FailureScope::kRoute));
    EXPECT_FALSE(isCurrentGeneration(permit));
    EXPECT_EQ(snapshot(device).inflight_bytes, 0u);
    EXPECT_EQ(snapshot(route).inflight_bytes, 0u);
}

TEST(AdaptiveCongestionControlTest, OldPermitReleasePreservesNewReservations) {
    DomainState device(testConfig()), route(testConfig());
    Permit old_permit, new_permit;
    ASSERT_EQ(tryAcquire(pathFor(device, route), 16, old_permit),
              Decision::kAllow);
    resetGeneration(device, 2);
    resetGeneration(route, 2);
    ASSERT_EQ(tryAcquire(pathFor(device, route), 32, new_permit),
              Decision::kAllow);
    EXPECT_EQ(snapshot(device).inflight_bytes, 48u);
    ASSERT_TRUE(complete(old_permit, OutcomeClass::kDerivedFlush,
                         FailureScope::kOperation));
    EXPECT_FALSE(complete(old_permit, OutcomeClass::kDerivedFlush,
                          FailureScope::kOperation));
    EXPECT_EQ(snapshot(device).inflight_bytes, 32u);
    EXPECT_EQ(snapshot(route).inflight_bytes, 32u);
    ASSERT_TRUE(
        complete(new_permit, OutcomeClass::kSuccess, FailureScope::kOperation));
    EXPECT_EQ(snapshot(device).inflight_bytes, 0u);
    EXPECT_EQ(snapshot(route).inflight_bytes, 0u);
}

TEST(AdaptiveCongestionControlTest, LateSignalsCarryTheirGeneration) {
    Config config = testConfig();
    config.high_pressure_epochs = 1;
    DomainState route(config);
    const uint32_t old_generation = snapshot(route).generation;
    resetGeneration(route, old_generation + 1);

    Signals old_error;
    old_error.receiver_pressure = 10;
    recordSignals(route, old_generation, old_error);
    controlTick(route, 1'000);
    EXPECT_EQ(snapshot(route).state, PathState::kHealthy);
}

TEST(AdaptiveCongestionControlTest, ResetSerializesWithPolicyCommit) {
    Config config = testConfig();
    config.high_pressure_epochs = 1;
    DomainState route(config);

    for (uint32_t round = 0; round < 500; ++round) {
        const uint32_t old_generation = snapshot(route).generation;
        Signals error;
        error.receiver_pressure = 1;
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
    AdmissionEvidence evidence;
    EXPECT_EQ(tryAcquire(stale, 8, permit, &evidence), Decision::kAvoid);
    EXPECT_EQ(evidence.domain, AdmissionDomain::kRoute);
    EXPECT_EQ(evidence.reason, AdmissionReason::kStaleGeneration);
    EXPECT_EQ(snapshot(route).generation, stale.route_generation + 1);
    resetGeneration(device, stale.device_generation + 1);
    EXPECT_EQ(tryAcquire(stale, 8, permit, &evidence), Decision::kAvoid);
    EXPECT_EQ(evidence.domain, AdmissionDomain::kDevice);
    EXPECT_EQ(evidence.reason, AdmissionReason::kStaleGeneration);
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

TEST(AdaptiveCongestionControlSuccessBatchTest,
     DuplicateOnlyRefundsFirstWinner) {
    std::atomic<uint32_t> adapter_generation{1};
    DomainState device(testConfig()), route(testConfig());
    Permit first, second;
    const auto path = pathFor(device, route);
    ASSERT_EQ(tryAcquire(path, 8, first), Decision::kAllow);
    ASSERT_EQ(tryAcquire(path, 12, second), Decision::kAllow);
    Permit* permits[] = {&first, &second, &first};
    uint64_t mask = UINT64_MAX;
    ASSERT_TRUE(completeSuccessBatch(permits, 3, mask, adapter_generation, 1));
    EXPECT_EQ(mask, 3u);
    EXPECT_FALSE(first.active());
    EXPECT_FALSE(second.active());
    EXPECT_EQ(snapshot(device).inflight_bytes, 0u);
    EXPECT_EQ(snapshot(route).inflight_bytes, 0u);
    EXPECT_FALSE(
        complete(first, OutcomeClass::kSuccess, FailureScope::kOperation));
    EXPECT_FALSE(
        complete(second, OutcomeClass::kSuccess, FailureScope::kOperation));
    EXPECT_FALSE(completeSuccessBatch(permits, 3, mask, adapter_generation, 1));
    EXPECT_EQ(mask, 0u);
}

TEST(AdaptiveCongestionControlSuccessBatchTest,
     PreflightPreservesScalarClaims) {
    std::atomic<uint32_t> adapter_generation{1};
    for (int scenario = 0; scenario < 7; ++scenario) {
        SCOPED_TRACE(scenario);
        DomainState device(testConfig()), route(testConfig()),
            other(testConfig());
        Permit first, second;
        auto first_path = pathFor(device, route);
        auto second_path = first_path;
        if (scenario == 0) second_path = pathFor(device, other);
        if (scenario == 1) first_path = second_path = pathFor(device, device);
        if (scenario == 2)
            first_path =
                second_path = {&device, nullptr, generation(device), 0};
        ASSERT_EQ(tryAcquire(first_path, 8, first), Decision::kAllow);
        ASSERT_EQ(tryAcquire(second_path, 12, second), Decision::kAllow);
        if (scenario == 3) resetGeneration(route, generation(route) + 1);
        const auto device_bytes = snapshot(device).inflight_bytes;
        const auto route_bytes = snapshot(route).inflight_bytes;
        const auto other_bytes = snapshot(other).inflight_bytes;
        Permit* permits[] = {&first, &second};
        if (scenario == 4) permits[1] = nullptr;
        uint64_t mask = UINT64_MAX;
        const size_t count = scenario == 5 ? 1 : scenario == 6 ? 0 : 2;
        EXPECT_FALSE(
            completeSuccessBatch(permits, count, mask, adapter_generation, 1));
        EXPECT_EQ(mask, 0u);
        EXPECT_TRUE(first.active());
        EXPECT_TRUE(second.active());
        EXPECT_EQ(snapshot(device).inflight_bytes, device_bytes);
        EXPECT_EQ(snapshot(route).inflight_bytes, route_bytes);
        EXPECT_EQ(snapshot(other).inflight_bytes, other_bytes);
        EXPECT_TRUE(
            complete(first, OutcomeClass::kSuccess, FailureScope::kOperation));
        EXPECT_TRUE(
            complete(second, OutcomeClass::kSuccess, FailureScope::kOperation));
        EXPECT_EQ(snapshot(device).inflight_bytes, 0u);
        EXPECT_EQ(snapshot(route).inflight_bytes, 0u);
        EXPECT_EQ(snapshot(other).inflight_bytes, 0u);
    }
}

TEST(AdaptiveCongestionControlSuccessBatchTest,
     OtherModesPreserveScalarClaims) {
    std::atomic<uint32_t> adapter_generation{1};
    for (Mode mode : {Mode::kOff, Mode::kObserve}) {
        DomainState device(testConfig(mode)), route(testConfig(mode));
        Permit first, second;
        const auto path = pathFor(device, route);
        ASSERT_EQ(tryAcquire(path, UINT64_MAX, first), Decision::kAllow);
        ASSERT_EQ(tryAcquire(path, 1, second), Decision::kAllow);
        const auto bytes = snapshot(device).inflight_bytes;
        Permit* permits[] = {&first, &second};
        uint64_t mask = UINT64_MAX;
        EXPECT_FALSE(
            completeSuccessBatch(permits, 2, mask, adapter_generation, 1));
        EXPECT_EQ(mask, 0u);
        EXPECT_TRUE(first.active());
        EXPECT_TRUE(second.active());
        EXPECT_EQ(snapshot(device).inflight_bytes, bytes);
        EXPECT_EQ(snapshot(route).inflight_bytes, bytes);
        EXPECT_TRUE(
            complete(first, OutcomeClass::kSuccess, FailureScope::kOperation));
        EXPECT_TRUE(
            complete(second, OutcomeClass::kSuccess, FailureScope::kOperation));
        EXPECT_EQ(snapshot(device).inflight_bytes, 0u);
        EXPECT_EQ(snapshot(route).inflight_bytes, 0u);
    }
}

TEST(AdaptiveCongestionControlSuccessBatchTest, OverflowDoesNotClaimPermit) {
    std::atomic<uint32_t> adapter_generation{1};
    DomainState device(testConfig()), route(testConfig());
    Permit permit;
    ASSERT_EQ(tryAcquire(pathFor(device, route), UINT64_MAX, permit),
              Decision::kAllow);
    Permit* permits[] = {&permit, &permit};
    uint64_t mask = UINT64_MAX;
    EXPECT_FALSE(completeSuccessBatch(permits, 2, mask, adapter_generation, 1));
    EXPECT_EQ(mask, 0u);
    EXPECT_TRUE(permit.active());
    EXPECT_EQ(snapshot(device).inflight_bytes, UINT64_MAX);
    EXPECT_EQ(snapshot(route).inflight_bytes, UINT64_MAX);
    EXPECT_TRUE(
        complete(permit, OutcomeClass::kSuccess, FailureScope::kOperation));
    EXPECT_EQ(snapshot(device).inflight_bytes, 0u);
    EXPECT_EQ(snapshot(route).inflight_bytes, 0u);
}

TEST(AdaptiveCongestionControlSuccessBatchTest,
     ChangedAdapterRefundsWithoutSuccessFeedback) {
    DomainState device(testConfig()), route(testConfig());
    Permit first, second;
    ASSERT_EQ(tryAcquire(pathFor(device, route), 8, first), Decision::kAllow);
    ASSERT_EQ(tryAcquire(pathFor(device, route), 12, second), Decision::kAllow);
    Permit* permits[] = {&first, &second};
    std::atomic<uint32_t> adapter_generation{2};
    uint64_t mask = 0;
    ASSERT_TRUE(completeSuccessBatch(permits, 2, mask, adapter_generation, 1));
    EXPECT_EQ(mask, 3u);
    EXPECT_EQ(snapshot(device).inflight_bytes, 0u);
    EXPECT_EQ(snapshot(route).inflight_bytes, 0u);
    controlTick(device, 1);
    controlTick(route, 1);
    EXPECT_EQ(snapshot(device).window_bytes, 64u);
    EXPECT_EQ(snapshot(route).window_bytes, 64u);
}

TEST(AdaptiveCongestionControlBatchTest, FullFitKeepsPerPermitCompletion) {
    DomainState device(testConfig()), route(testConfig());
    Permit first, second;
    BatchItem items[] = {{&first, 8}, {&second, 12}};
    ASSERT_TRUE(tryAcquireBatch(pathFor(device, route), items, 2));
    EXPECT_TRUE(first.active());
    EXPECT_TRUE(second.active());
    EXPECT_EQ(snapshot(device).inflight_bytes, 20u);
    EXPECT_EQ(snapshot(route).inflight_bytes, 20u);
    EXPECT_TRUE(
        complete(second, OutcomeClass::kSuccess, FailureScope::kOperation));
    EXPECT_EQ(snapshot(device).inflight_bytes, 8u);
    EXPECT_FALSE(
        complete(second, OutcomeClass::kSuccess, FailureScope::kOperation));
    EXPECT_TRUE(
        complete(first, OutcomeClass::kSuccess, FailureScope::kOperation));
    EXPECT_EQ(snapshot(device).inflight_bytes, 0u);
    EXPECT_EQ(snapshot(route).inflight_bytes, 0u);
}

TEST(AdaptiveCongestionControlBatchTest, ConflictingClaimPreservesOldPermit) {
    DomainState device(testConfig()), route(testConfig());
    Permit fresh, occupied;
    const auto path = pathFor(device, route);
    ASSERT_EQ(tryAcquire(path, 8, occupied), Decision::kAllow);
    BatchItem items[] = {{&fresh, 12}, {&occupied, 16}};
    EXPECT_FALSE(tryAcquireBatch(path, items, 2));
    EXPECT_FALSE(fresh.active());
    EXPECT_TRUE(occupied.active());
    EXPECT_TRUE(isCurrentGeneration(occupied));
    EXPECT_EQ(snapshot(device).inflight_bytes, 8u);
    EXPECT_EQ(snapshot(route).inflight_bytes, 8u);
    EXPECT_TRUE(complete(occupied, OutcomeClass::kDerivedFlush,
                         FailureScope::kOperation));
    EXPECT_FALSE(
        complete(fresh, OutcomeClass::kDerivedFlush, FailureScope::kOperation));
    BatchItem duplicate[] = {{&fresh, 8}, {&fresh, 8}};
    EXPECT_FALSE(tryAcquireBatch(path, duplicate, 2));
    EXPECT_FALSE(fresh.active());
    EXPECT_EQ(snapshot(device).inflight_bytes, 0u);
    EXPECT_EQ(snapshot(route).inflight_bytes, 0u);
}

TEST(AdaptiveCongestionControlBatchTest, RouteFailureRollsBackWithoutEvidence) {
    auto small = testConfig();
    small.min_window_bytes = 16;
    small.max_window_bytes = 16;
    DomainState device(testConfig()), route(small);
    Permit first, second;
    BatchItem items[] = {{&first, 8}, {&second, 12}};
    const auto path = pathFor(device, route);
    EXPECT_FALSE(tryAcquireBatch(path, items, 2));
    EXPECT_FALSE(first.active());
    EXPECT_FALSE(second.active());
    EXPECT_EQ(snapshot(device).inflight_bytes, 0u);
    EXPECT_EQ(snapshot(route).inflight_bytes, 0u);
    EXPECT_EQ(snapshot(device).deferred_bytes, 0u);
    EXPECT_EQ(snapshot(route).deferred_bytes, 0u);
    ASSERT_EQ(tryAcquire(path, 8, first), Decision::kAllow);
    AdmissionEvidence evidence;
    EXPECT_EQ(tryAcquire(path, 12, second, &evidence), Decision::kDefer);
    EXPECT_EQ(evidence.domain, AdmissionDomain::kRoute);
    EXPECT_EQ(evidence.reason, AdmissionReason::kWindow);
    EXPECT_EQ(snapshot(route).deferred_bytes, 12u);
    EXPECT_EQ(snapshot(device).inflight_bytes, 8u);
    EXPECT_TRUE(
        complete(first, OutcomeClass::kDerivedFlush, FailureScope::kOperation));
}

TEST(AdaptiveCongestionControlBatchTest, OversizedAndOtherModesUseScalar) {
    for (Mode mode : {Mode::kOff, Mode::kObserve, Mode::kEnforce}) {
        auto config = testConfig(mode);
        config.min_window_bytes = 16;
        config.max_window_bytes = 16;
        DomainState device(config), route(config);
        Permit first, second;
        BatchItem items[] = {{&first, 32}, {&second, 1}};
        const auto path = pathFor(device, route);
        EXPECT_FALSE(tryAcquireBatch(path, items, 2));
        EXPECT_FALSE(first.active());
        EXPECT_FALSE(second.active());
        EXPECT_EQ(snapshot(device).inflight_bytes, 0u);
        EXPECT_EQ(snapshot(route).deferred_bytes, 0u);
        ASSERT_EQ(tryAcquire(path, 32, first), Decision::kAllow);
        EXPECT_TRUE(complete(first, OutcomeClass::kDerivedFlush,
                             FailureScope::kOperation));
    }
    DomainState device(testConfig()), route(testConfig());
    Permit first, second;
    BatchItem overflow[] = {{&first, std::numeric_limits<uint64_t>::max()},
                            {&second, 1}};
    EXPECT_FALSE(tryAcquireBatch(pathFor(device, route), overflow, 2));
    EXPECT_FALSE(first.active());
    EXPECT_FALSE(second.active());
    EXPECT_EQ(snapshot(device).inflight_bytes, 0u);
}

TEST(AdaptiveCongestionControlBatchTest, StaleRouteReturnsDeviceReservation) {
    DomainState device(testConfig()), route(testConfig());
    const auto path = pathFor(device, route);
    resetGeneration(route, path.route_generation + 1);
    Permit first, second;
    BatchItem items[] = {{&first, 8}, {&second, 8}};
    EXPECT_FALSE(tryAcquireBatch(path, items, 2));
    EXPECT_FALSE(first.active());
    EXPECT_FALSE(second.active());
    EXPECT_EQ(snapshot(device).inflight_bytes, 0u);
    EXPECT_EQ(snapshot(route).inflight_bytes, 0u);
    EXPECT_EQ(snapshot(device).deferred_bytes, 0u);
    EXPECT_EQ(snapshot(route).deferred_bytes, 0u);
}

TEST(AdaptiveCongestionControlBatchTest,
     ConcurrentResetAndReservationsBalance) {
    auto config = testConfig();
    config.min_window_bytes = 256;
    DomainState device(config), route(config);
    std::atomic<bool> start{false};
    std::atomic<int> completed{0};
    std::vector<std::thread> threads;
    for (int lane = 0; lane < 6; ++lane)
        threads.emplace_back([&] {
            while (!start.load(std::memory_order_acquire)) {
            }
            for (int iteration = 0; iteration < 500; ++iteration) {
                Permit permits[6];
                BatchItem items[6];
                for (int i = 0; i < 6; ++i) items[i] = {&permits[i], 4};
                bool acquired = false;
                for (int attempt = 0; attempt < 10000 && !acquired; ++attempt)
                    acquired =
                        tryAcquireBatch(pathFor(device, route), items, 6);
                if (!acquired) continue;
                for (int i = 5; i >= 0; --i)
                    if (complete(permits[i], OutcomeClass::kDerivedFlush,
                                 FailureScope::kOperation))
                        completed.fetch_add(1, std::memory_order_relaxed);
            }
        });
    std::thread reset([&] {
        while (!start.load(std::memory_order_acquire)) {
        }
        for (uint32_t gen = 2; gen < 1000; ++gen) {
            resetGeneration(device, gen);
            resetGeneration(route, gen);
        }
    });
    start.store(true, std::memory_order_release);
    for (auto& thread : threads) thread.join();
    reset.join();
    EXPECT_GT(completed.load(), 0);
    EXPECT_EQ(snapshot(device).inflight_bytes, 0u);
    EXPECT_EQ(snapshot(route).inflight_bytes, 0u);
    EXPECT_EQ(snapshot(device).deferred_bytes, 0u);
    EXPECT_EQ(snapshot(route).deferred_bytes, 0u);
}

}  // namespace
}  // namespace mooncake::adaptive_congestion_control

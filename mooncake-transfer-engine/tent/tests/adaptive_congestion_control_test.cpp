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

#include "adaptive_congestion_control.h"
#include "task_congestion_observation.h"
#include "tent/transport/rdma/endpoint.h"
#include "tent/transport/rdma/slice.h"
#include "tent/transport/rdma/workers.h"

namespace mooncake::tent {
namespace {

adaptive_congestion_control::Config testConfig() {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    config.min_window_bytes = 1024;
    config.max_window_bytes = 1024;
    return config;
}

TEST(TentAdaptiveCongestionControlTest, AttemptReleasesOnce) {
    adaptive_congestion_control::DomainState device(testConfig(), 3);
    auto route = std::make_shared<TentRdmaCongestionControlRoute>(testConfig());
    RdmaSlice slice;
    slice.length = 512;

    adaptive_congestion_control::PathHandle path{&device, &route->domain, 3, 1};
    ASSERT_EQ(
        acquireTentCongestionControlAttempt(slice, route.get(), path, 512, 7),
        adaptive_congestion_control::Decision::kAllow);
    EXPECT_TRUE(completeTentCongestionControlAttempt(
        slice, adaptive_congestion_control::OutcomeClass::kSuccess,
        adaptive_congestion_control::FailureScope::kOperation));
    EXPECT_FALSE(completeTentCongestionControlAttempt(
        slice, adaptive_congestion_control::OutcomeClass::kSuccess,
        adaptive_congestion_control::FailureScope::kOperation));
    EXPECT_EQ(route->completed_bytes.load(std::memory_order_relaxed), 512u);
    EXPECT_EQ(adaptive_congestion_control::snapshot(device).inflight_bytes, 0);
    EXPECT_EQ(
        adaptive_congestion_control::snapshot(route->domain).inflight_bytes, 0);
}

TEST(TentAdaptiveCongestionControlTest,
     LogicalTaskTracksWindowDeferAndReadmission) {
    adaptive_congestion_control::DomainState device(testConfig(), 3);
    TentRdmaCongestionControlRoute route(testConfig());
    auto observation =
        std::make_shared<adaptive_congestion_control::TaskCongestionObservation>(2, 1);
    RdmaTask task{};
    task.congestion_observation = observation;
    task.congestion_attempt_id = 1;
    RdmaSlice first{}, second{};
    first.task = second.task = &task;
    first.slice_idx = 0;
    second.slice_idx = 1;
    first.length = 1024;
    second.length = 128;
    adaptive_congestion_control::PathHandle path{&device, &route.domain, 3, 1};

    ASSERT_EQ(acquireTentCongestionControlAttempt(first, &route, path, first.length, 7),
              adaptive_congestion_control::Decision::kAllow);
    ASSERT_EQ(acquireTentCongestionControlAttempt(second, &route, path, second.length, 7),
              adaptive_congestion_control::Decision::kDefer);
    EXPECT_EQ(observation->state(), TaskCongestionState::kCongested);
    auto detail = observation->detail();
    ASSERT_TRUE(detail.reason.observed);
    EXPECT_EQ(detail.reason.value, TaskCongestionReason::kByteWindow);
    EXPECT_EQ(detail.slice_id.value, 1);

    ASSERT_TRUE(completeTentCongestionControlAttempt(first,
                                      adaptive_congestion_control::OutcomeClass::kSuccess,
                                      adaptive_congestion_control::FailureScope::kOperation));
    ASSERT_EQ(acquireTentCongestionControlAttempt(second, &route, path, second.length, 7),
              adaptive_congestion_control::Decision::kAllow);
    EXPECT_EQ(observation->state(), TaskCongestionState::kNormal);
    EXPECT_TRUE(observation->detail().resolved);
    EXPECT_TRUE(completeTentCongestionControlAttempt(second,
                                      adaptive_congestion_control::OutcomeClass::kSuccess,
                                      adaptive_congestion_control::FailureScope::kOperation));
}

TEST(TentAdaptiveCongestionControlTest, QuarantineAvoidsCurrentSliceOnly) {
    auto config = testConfig();
    config.hard_error_threshold = 1;
    adaptive_congestion_control::DomainState device(config, 3);
    TentRdmaCongestionControlRoute route(config);
    adaptive_congestion_control::Signals failure;
    failure.fatal_failures = 1;
    adaptive_congestion_control::recordSignals(route.domain, 1, failure);
    adaptive_congestion_control::controlTick(route.domain, 1);
    ASSERT_EQ(adaptive_congestion_control::snapshot(route.domain).state,
              adaptive_congestion_control::PathState::kQuarantined);

    auto observation =
        std::make_shared<adaptive_congestion_control::TaskCongestionObservation>(2, 1);
    RdmaTask task{};
    task.congestion_observation = observation;
    task.congestion_attempt_id = 1;
    RdmaSlice slice{};
    slice.task = &task;
    slice.slice_idx = 1;
    slice.length = 128;
    adaptive_congestion_control::PathHandle path{&device, &route.domain, 3, 1};
    EXPECT_EQ(acquireTentCongestionControlAttempt(slice, &route, path, slice.length, 7),
              adaptive_congestion_control::Decision::kAvoid);
    EXPECT_EQ(observation->state(), TaskCongestionState::kLongUnavailable);
    auto detail = observation->detail();
    ASSERT_TRUE(detail.reason.observed);
    EXPECT_EQ(detail.reason.value, TaskCongestionReason::kPathQuarantined);
    EXPECT_EQ(detail.slice_id.value, 1);

    ASSERT_TRUE(observation->beginAttempt(2, 2));
    EXPECT_EQ(observation->state(), TaskCongestionState::kNormal);
    EXPECT_FALSE(observation->avoid(1, 1, {}));
    EXPECT_EQ(observation->state(), TaskCongestionState::kNormal);
}

TEST(TentAdaptiveCongestionControlTest, OldEndpointFailureIsIgnored) {
    adaptive_congestion_control::DomainState device(testConfig(), 3);
    auto route = std::make_shared<TentRdmaCongestionControlRoute>(testConfig());
    RdmaSlice slice;

    adaptive_congestion_control::PathHandle path{&device, &route->domain, 3, 1};
    ASSERT_EQ(
        acquireTentCongestionControlAttempt(slice, route.get(), path, 512, 7),
        adaptive_congestion_control::Decision::kAllow);
    route->endpoint_generation.store(8, std::memory_order_release);
    EXPECT_TRUE(completeTentCongestionControlAttempt(
        slice, adaptive_congestion_control::OutcomeClass::kFatal,
        adaptive_congestion_control::FailureScope::kQp));
    EXPECT_EQ(
        adaptive_congestion_control::snapshot(route->domain).inflight_bytes, 0);

    adaptive_congestion_control::controlTick(route->domain, 1);
    EXPECT_EQ(adaptive_congestion_control::snapshot(route->domain).state,
              adaptive_congestion_control::PathState::kHealthy);
}

TEST(TentAdaptiveCongestionControlTest,
     OldControllerGenerationDoesNotRecordTaskFeedback) {
    adaptive_congestion_control::DomainState device(testConfig(), 3);
    TentRdmaCongestionControlRoute route(testConfig());
    auto observation =
        std::make_shared<adaptive_congestion_control::TaskCongestionObservation>(1, 1);
    RdmaTask task{};
    task.congestion_observation = observation;
    task.congestion_attempt_id = 1;
    RdmaSlice slice{};
    slice.task = &task;
    slice.length = 128;
    adaptive_congestion_control::PathHandle path{&device, &route.domain, 3, 1};
    ASSERT_EQ(acquireTentCongestionControlAttempt(slice, &route, path, slice.length, 7),
              adaptive_congestion_control::Decision::kAllow);

    adaptive_congestion_control::resetGeneration(route.domain, 2);
    ASSERT_TRUE(completeTentCongestionControlAttempt(
        slice, adaptive_congestion_control::OutcomeClass::kReceiverPressure,
        adaptive_congestion_control::FailureScope::kRoute));
    EXPECT_EQ(observation->state(), TaskCongestionState::kNormal);
    EXPECT_FALSE(observation->detail().reason.observed);
}

TEST(TentAdaptiveCongestionControlTest,
     ActivePermitDeferDoesNotRecordTaskCongestion) {
    adaptive_congestion_control::DomainState device(testConfig(), 3);
    TentRdmaCongestionControlRoute route(testConfig());
    auto observation =
        std::make_shared<adaptive_congestion_control::TaskCongestionObservation>(1, 1);
    RdmaTask task{};
    task.congestion_observation = observation;
    task.congestion_attempt_id = 1;
    RdmaSlice slice{};
    slice.task = &task;
    slice.length = 128;
    adaptive_congestion_control::PathHandle path{&device, &route.domain, 3, 1};
    ASSERT_EQ(acquireTentCongestionControlAttempt(slice, &route, path, slice.length, 7),
              adaptive_congestion_control::Decision::kAllow);
    EXPECT_EQ(acquireTentCongestionControlAttempt(slice, &route, path, slice.length, 7),
              adaptive_congestion_control::Decision::kDefer);
    EXPECT_EQ(observation->state(), TaskCongestionState::kNormal);
    EXPECT_FALSE(observation->detail().reason.observed);
    EXPECT_TRUE(completeTentCongestionControlAttempt(slice,
                                      adaptive_congestion_control::OutcomeClass::kSuccess,
                                      adaptive_congestion_control::FailureScope::kOperation));
}

TEST(TentAdaptiveCongestionControlTest,
     GenerationRaceAvoidDoesNotClaimPathQuarantine) {
    adaptive_congestion_control::DomainState device(testConfig(), 3);
    TentRdmaCongestionControlRoute route(testConfig());
    auto observation =
        std::make_shared<adaptive_congestion_control::TaskCongestionObservation>(1, 1);
    RdmaTask task{};
    task.congestion_observation = observation;
    task.congestion_attempt_id = 1;
    RdmaSlice slice{};
    slice.task = &task;
    slice.length = 128;
    adaptive_congestion_control::PathHandle stale_path{&device, &route.domain, 3, 1};
    adaptive_congestion_control::resetGeneration(route.domain, 2);
    EXPECT_EQ(acquireTentCongestionControlAttempt(slice, &route, stale_path, slice.length, 7),
              adaptive_congestion_control::Decision::kAvoid);
    EXPECT_EQ(observation->state(), TaskCongestionState::kNormal);
    EXPECT_FALSE(observation->detail().reason.observed);
}

TEST(TentAdaptiveCongestionControlTest,
     CurrentEndpointFailureQuarantinesRoute) {
    auto config = testConfig();
    config.hard_error_threshold = 1;
    adaptive_congestion_control::DomainState device(config, 3);
    auto route = std::make_shared<TentRdmaCongestionControlRoute>(config);
    RdmaSlice slice;
    adaptive_congestion_control::PathHandle path{&device, &route->domain, 3, 1};

    ASSERT_EQ(
        acquireTentCongestionControlAttempt(slice, route.get(), path, 512, 7),
        adaptive_congestion_control::Decision::kAllow);
    EXPECT_TRUE(completeTentCongestionControlAttempt(
        slice, adaptive_congestion_control::OutcomeClass::kFatal,
        adaptive_congestion_control::FailureScope::kQp));
    adaptive_congestion_control::controlTick(route->domain, 1);

    EXPECT_EQ(adaptive_congestion_control::snapshot(route->domain).state,
              adaptive_congestion_control::PathState::kQuarantined);
}

TEST(TentAdaptiveCongestionControlTest, LargeSliceCanMakeProgressDuringProbe) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    config.cooldown_ns = 1;
    adaptive_congestion_control::DomainState device(config);
    TentRdmaCongestionControlRoute route(config);
    adaptive_congestion_control::Signals failure;
    failure.fatal_failures = 1;
    adaptive_congestion_control::recordSignals(route.domain, 1, failure);
    adaptive_congestion_control::controlTick(route.domain, 1);
    adaptive_congestion_control::controlTick(route.domain, 2);
    ASSERT_EQ(adaptive_congestion_control::snapshot(route.domain).state,
              adaptive_congestion_control::PathState::kProbing);
    RdmaSlice first, second;
    // A default 4 MiB write is capped at 32 slices of 128 KiB each.
    first.length = second.length = 128ULL << 10;
    adaptive_congestion_control::PathHandle path{&device, &route.domain, 1, 1};
    ASSERT_EQ(acquireTentCongestionControlAttempt(first, &route, path,
                                                  first.length, 7),
              adaptive_congestion_control::Decision::kAllow);
    EXPECT_EQ(acquireTentCongestionControlAttempt(second, &route, path,
                                                  second.length, 7),
              adaptive_congestion_control::Decision::kDefer);
    ASSERT_TRUE(completeTentCongestionControlAttempt(
        first, adaptive_congestion_control::OutcomeClass::kSuccess,
        adaptive_congestion_control::FailureScope::kOperation));
    tickTentCongestionControlRoute(route, 3, 1);
    EXPECT_EQ(adaptive_congestion_control::snapshot(route.domain).state,
              adaptive_congestion_control::PathState::kHealthy);
    EXPECT_EQ(adaptive_congestion_control::snapshot(device).inflight_bytes, 0u);
}

TEST(TentAdaptiveCongestionControlTest, EndpointUsesOneSharedRouteState) {
    TentRdmaCongestionControlRoute first(testConfig());
    TentRdmaCongestionControlRoute second(testConfig());
    RdmaEndPoint endpoint;

    EXPECT_EQ(endpoint.bindCongestionRoute(&first), &first);
    EXPECT_EQ(endpoint.bindCongestionRoute(&second), &first);
}

TEST(TentAdaptiveCongestionControlTest, RouteTelemetryGrowsAndShrinksWindow) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    config.min_window_bytes = 64;
    config.max_window_bytes = 128;
    config.target_drain_time_ns = 1'000;
    config.low_pressure_epochs = 2;
    config.high_pressure_epochs = 2;
    adaptive_congestion_control::DomainState device(config, 1);
    TentRdmaCongestionControlRoute route(config);

    for (uint64_t tick = 1; tick <= 2; ++tick) {
        RdmaSlice slice;
        slice.length = 16;
        adaptive_congestion_control::PathHandle path{
            &device, &route.domain,
            adaptive_congestion_control::generation(device),
            adaptive_congestion_control::generation(route.domain)};
        ASSERT_EQ(acquireTentCongestionControlAttempt(slice, &route, path,
                                                      slice.length, 7),
                  adaptive_congestion_control::Decision::kAllow);
        ASSERT_TRUE(completeTentCongestionControlAttempt(
            slice, adaptive_congestion_control::OutcomeClass::kSuccess,
            adaptive_congestion_control::FailureScope::kOperation));
        tickTentCongestionControlRoute(route, tick, 1'000'000'000);
    }
    EXPECT_GT(adaptive_congestion_control::snapshot(route.domain).window_bytes,
              64u);

    RdmaSlice inflight;
    inflight.length = 64;
    adaptive_congestion_control::PathHandle path{
        &device, &route.domain, adaptive_congestion_control::generation(device),
        adaptive_congestion_control::generation(route.domain)};
    ASSERT_EQ(acquireTentCongestionControlAttempt(inflight, &route, path,
                                                  inflight.length, 7),
              adaptive_congestion_control::Decision::kAllow);
    route.completed_bytes.store(1, std::memory_order_relaxed);
    tickTentCongestionControlRoute(route, 3, 1'000'000'000);
    route.completed_bytes.store(1, std::memory_order_relaxed);
    tickTentCongestionControlRoute(route, 4, 1'000'000'000);

    EXPECT_EQ(adaptive_congestion_control::snapshot(route.domain).state,
              adaptive_congestion_control::PathState::kCongested);
    EXPECT_EQ(adaptive_congestion_control::snapshot(route.domain).window_bytes,
              64u);
    EXPECT_TRUE(completeTentCongestionControlAttempt(
        inflight, adaptive_congestion_control::OutcomeClass::kDerivedFlush,
        adaptive_congestion_control::FailureScope::kOperation));
}

}  // namespace
}  // namespace mooncake::tent

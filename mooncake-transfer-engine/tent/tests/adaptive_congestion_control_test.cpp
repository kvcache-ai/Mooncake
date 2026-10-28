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
    EXPECT_EQ(adaptive_congestion_control::snapshot(device).inflight_bytes, 0);
    EXPECT_EQ(
        adaptive_congestion_control::snapshot(route->domain).inflight_bytes, 0);
}

TEST(TentAdaptiveCongestionControlTest,
     LogicalTaskTracksWindowDeferAndReadmission) {
    adaptive_congestion_control::DomainState device(testConfig(), 3);
    TentRdmaCongestionControlRoute route(testConfig());
    auto observation = std::make_shared<
        adaptive_congestion_control::TaskCongestionObservation>(2, 1);
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

    ASSERT_EQ(acquireTentCongestionControlAttempt(first, &route, path,
                                                  first.length, 7),
              adaptive_congestion_control::Decision::kAllow);
    ASSERT_EQ(acquireTentCongestionControlAttempt(second, &route, path,
                                                  second.length, 7),
              adaptive_congestion_control::Decision::kDefer);
    EXPECT_EQ(observation->state(), TaskCongestionState::kCongested);
    auto detail = observation->detail();
    ASSERT_TRUE(detail.reason.observed);
    EXPECT_EQ(detail.reason.value, TaskCongestionReason::kByteWindow);
    EXPECT_EQ(detail.slice_id.value, 1);

    ASSERT_TRUE(completeTentCongestionControlAttempt(
        first, adaptive_congestion_control::OutcomeClass::kSuccess,
        adaptive_congestion_control::FailureScope::kOperation));
    ASSERT_EQ(acquireTentCongestionControlAttempt(second, &route, path,
                                                  second.length, 7),
              adaptive_congestion_control::Decision::kAllow);
    EXPECT_EQ(observation->state(), TaskCongestionState::kNormal);
    EXPECT_TRUE(observation->detail().resolved);
    EXPECT_TRUE(completeTentCongestionControlAttempt(
        second, adaptive_congestion_control::OutcomeClass::kSuccess,
        adaptive_congestion_control::FailureScope::kOperation));
}

TEST(TentAdaptiveCongestionControlTest,
     PartiallyFilledWindowStillReportsDeferredTask) {
    adaptive_congestion_control::DomainState device(testConfig(), 3);
    TentRdmaCongestionControlRoute route(testConfig());
    auto observation = std::make_shared<
        adaptive_congestion_control::TaskCongestionObservation>(2, 1);
    RdmaTask task{};
    task.congestion_observation = observation;
    task.congestion_attempt_id = 1;
    RdmaSlice first{}, second{};
    first.task = second.task = &task;
    first.slice_idx = 0;
    second.slice_idx = 1;
    first.length = 512;
    second.length = 600;
    adaptive_congestion_control::PathHandle path{&device, &route.domain, 3, 1};

    ASSERT_EQ(acquireTentCongestionControlAttempt(first, &route, path,
                                                  first.length, 7),
              adaptive_congestion_control::Decision::kAllow);
    ASSERT_EQ(acquireTentCongestionControlAttempt(second, &route, path,
                                                  second.length, 7),
              adaptive_congestion_control::Decision::kDefer);
    EXPECT_EQ(observation->state(), TaskCongestionState::kCongested);
    auto detail = observation->detail();
    ASSERT_TRUE(detail.reason.observed);
    EXPECT_EQ(detail.reason.value, TaskCongestionReason::kByteWindow);

    ASSERT_TRUE(completeTentCongestionControlAttempt(
        first, adaptive_congestion_control::OutcomeClass::kSuccess,
        adaptive_congestion_control::FailureScope::kOperation));
    ASSERT_EQ(acquireTentCongestionControlAttempt(second, &route, path,
                                                  second.length, 7),
              adaptive_congestion_control::Decision::kAllow);
    EXPECT_EQ(observation->state(), TaskCongestionState::kNormal);
    EXPECT_TRUE(completeTentCongestionControlAttempt(
        second, adaptive_congestion_control::OutcomeClass::kSuccess,
        adaptive_congestion_control::FailureScope::kOperation));
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
    auto observation = std::make_shared<
        adaptive_congestion_control::TaskCongestionObservation>(1, 1);
    RdmaTask task{};
    task.congestion_observation = observation;
    task.congestion_attempt_id = 1;
    RdmaSlice slice{};
    slice.task = &task;
    slice.length = 128;
    adaptive_congestion_control::PathHandle path{&device, &route.domain, 3, 1};
    ASSERT_EQ(acquireTentCongestionControlAttempt(slice, &route, path,
                                                  slice.length, 7),
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
    auto observation = std::make_shared<
        adaptive_congestion_control::TaskCongestionObservation>(1, 1);
    RdmaTask task{};
    task.congestion_observation = observation;
    task.congestion_attempt_id = 1;
    RdmaSlice slice{};
    slice.task = &task;
    slice.length = 128;
    adaptive_congestion_control::PathHandle path{&device, &route.domain, 3, 1};
    ASSERT_EQ(acquireTentCongestionControlAttempt(slice, &route, path,
                                                  slice.length, 7),
              adaptive_congestion_control::Decision::kAllow);
    EXPECT_EQ(acquireTentCongestionControlAttempt(slice, &route, path,
                                                  slice.length, 7),
              adaptive_congestion_control::Decision::kDefer);
    EXPECT_EQ(observation->state(), TaskCongestionState::kNormal);
    EXPECT_FALSE(observation->detail().reason.observed);
    EXPECT_TRUE(completeTentCongestionControlAttempt(
        slice, adaptive_congestion_control::OutcomeClass::kSuccess,
        adaptive_congestion_control::FailureScope::kOperation));
}

TEST(TentAdaptiveCongestionControlTest,
     GenerationRaceAvoidDoesNotClaimPathQuarantine) {
    adaptive_congestion_control::DomainState device(testConfig(), 3);
    TentRdmaCongestionControlRoute route(testConfig());
    auto observation = std::make_shared<
        adaptive_congestion_control::TaskCongestionObservation>(1, 1);
    RdmaTask task{};
    task.congestion_observation = observation;
    task.congestion_attempt_id = 1;
    RdmaSlice slice{};
    slice.task = &task;
    slice.length = 128;
    adaptive_congestion_control::PathHandle stale_path{&device, &route.domain,
                                                       3, 1};
    adaptive_congestion_control::resetGeneration(route.domain, 2);
    EXPECT_EQ(acquireTentCongestionControlAttempt(slice, &route, stale_path,
                                                  slice.length, 7),
              adaptive_congestion_control::Decision::kAvoid);
    EXPECT_EQ(observation->state(), TaskCongestionState::kNormal);
    EXPECT_FALSE(observation->detail().reason.observed);
}

TEST(TentAdaptiveCongestionControlTest,
     CurrentEndpointFailureLeavesHealthToRailMonitor) {
    auto config = testConfig();
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
              adaptive_congestion_control::PathState::kHealthy);
}

TEST(TentAdaptiveCongestionControlTest,
     LargeSliceCanMakeProgressWithSmallWindow) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    config.min_window_bytes = config.max_window_bytes = 1024;
    adaptive_congestion_control::DomainState device(config);
    TentRdmaCongestionControlRoute route(config);
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
    tickTentCongestionControlRoute(route, 3);
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

TEST(TentAdaptiveCongestionControlTest,
     RouteGrowsOnSuccessWithoutTreatingBacklogAsPressure) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    config.min_window_bytes = 64;
    config.max_window_bytes = 128;
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
        tickTentCongestionControlRoute(route, tick);
        adaptive_congestion_control::controlTick(device, tick);
    }
    EXPECT_EQ(adaptive_congestion_control::snapshot(route.domain).window_bytes,
              96u);

    RdmaSlice inflight;
    inflight.length = 64;
    adaptive_congestion_control::PathHandle path{
        &device, &route.domain, adaptive_congestion_control::generation(device),
        adaptive_congestion_control::generation(route.domain)};
    ASSERT_EQ(acquireTentCongestionControlAttempt(inflight, &route, path,
                                                  inflight.length, 7),
              adaptive_congestion_control::Decision::kAllow);
    tickTentCongestionControlRoute(route, 3);
    EXPECT_EQ(adaptive_congestion_control::snapshot(route.domain).window_bytes,
              96u);

    RdmaSlice progress;
    progress.length = 16;
    ASSERT_EQ(acquireTentCongestionControlAttempt(progress, &route, path,
                                                  progress.length, 7),
              adaptive_congestion_control::Decision::kAllow);
    ASSERT_TRUE(completeTentCongestionControlAttempt(
        progress, adaptive_congestion_control::OutcomeClass::kSuccess,
        adaptive_congestion_control::FailureScope::kOperation));
    tickTentCongestionControlRoute(route, 4);

    EXPECT_EQ(adaptive_congestion_control::snapshot(route.domain).state,
              adaptive_congestion_control::PathState::kHealthy);
    EXPECT_EQ(adaptive_congestion_control::snapshot(route.domain).window_bytes,
              112u);
    EXPECT_TRUE(completeTentCongestionControlAttempt(
        inflight, adaptive_congestion_control::OutcomeClass::kDerivedFlush,
        adaptive_congestion_control::FailureScope::kOperation));
}

TEST(TentAdaptiveCongestionControlTest,
     ExplicitPressureBacksOffBeforeCleanSuccessResumesGrowth) {
    for (const auto pressure :
         {adaptive_congestion_control::OutcomeClass::kCongestion,
          adaptive_congestion_control::OutcomeClass::kReceiverPressure}) {
        adaptive_congestion_control::Config config;
        config.mode = adaptive_congestion_control::Mode::kEnforce;
        config.min_window_bytes = 64;
        config.max_window_bytes = 256;
        adaptive_congestion_control::DomainState device(config, 1);
        TentRdmaCongestionControlRoute route(config);
        const adaptive_congestion_control::PathHandle path{
            &device, &route.domain,
            adaptive_congestion_control::generation(device),
            adaptive_congestion_control::generation(route.domain)};
        auto finish = [&](adaptive_congestion_control::OutcomeClass outcome) {
            RdmaSlice slice;
            slice.length = 16;
            ASSERT_EQ(acquireTentCongestionControlAttempt(slice, &route, path,
                                                          slice.length, 7),
                      adaptive_congestion_control::Decision::kAllow);
            ASSERT_TRUE(completeTentCongestionControlAttempt(
                slice, outcome,
                adaptive_congestion_control::FailureScope::kOperation));
        };
        for (uint64_t tick = 1; tick <= 6; ++tick) {
            finish(adaptive_congestion_control::OutcomeClass::kSuccess);
            tickTentCongestionControlRoute(route, tick);
        }
        ASSERT_EQ(
            adaptive_congestion_control::snapshot(route.domain).window_bytes,
            160u);
        finish(adaptive_congestion_control::OutcomeClass::kSuccess);
        finish(pressure);
        finish(pressure);
        tickTentCongestionControlRoute(route, 7);
        EXPECT_EQ(
            adaptive_congestion_control::snapshot(route.domain).window_bytes,
            80u);
        EXPECT_EQ(adaptive_congestion_control::snapshot(route.domain).state,
                  adaptive_congestion_control::PathState::kCongested);
        finish(adaptive_congestion_control::OutcomeClass::kSuccess);
        tickTentCongestionControlRoute(route, 8);
        EXPECT_EQ(
            adaptive_congestion_control::snapshot(route.domain).window_bytes,
            96u);
        EXPECT_EQ(adaptive_congestion_control::snapshot(route.domain).state,
                  adaptive_congestion_control::PathState::kHealthy);
    }
}

}  // namespace
}  // namespace mooncake::tent

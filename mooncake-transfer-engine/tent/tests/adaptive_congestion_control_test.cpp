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

#include <array>
#include <atomic>
#include <thread>
#include <vector>

#include "adaptive_congestion_control.h"
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

    ASSERT_EQ(
        acquireTentCongestionControlAttempt(slice, route.get(), path, 512, 8),
        adaptive_congestion_control::Decision::kAllow);
    EXPECT_TRUE(slice.congestion_control_permit.active());
    EXPECT_EQ(adaptive_congestion_control::snapshot(device).inflight_bytes,
              512);
    EXPECT_TRUE(completeTentCongestionControlAttempt(
        slice, adaptive_congestion_control::OutcomeClass::kDerivedFlush,
        adaptive_congestion_control::FailureScope::kOperation));
    EXPECT_FALSE(completeTentCongestionControlAttempt(
        slice, adaptive_congestion_control::OutcomeClass::kDerivedFlush,
        adaptive_congestion_control::FailureScope::kOperation));
    EXPECT_FALSE(slice.congestion_control_permit.active());
    EXPECT_EQ(slice.congestion_control_route, nullptr);
    EXPECT_EQ(adaptive_congestion_control::snapshot(device).inflight_bytes, 0);
    EXPECT_EQ(
        adaptive_congestion_control::snapshot(route->domain).inflight_bytes, 0);
}

TEST(TentAdaptiveCongestionControlTest, OffObserveAndEnforceAdmission) {
    for (auto mode : {adaptive_congestion_control::Mode::kOff,
                      adaptive_congestion_control::Mode::kObserve,
                      adaptive_congestion_control::Mode::kEnforce}) {
        auto config = testConfig();
        config.mode = mode;
        adaptive_congestion_control::DomainState device(config);
        TentRdmaCongestionControlRoute route(config);
        adaptive_congestion_control::PathHandle path{&device, &route.domain, 1,
                                                     1};
        RdmaSlice first, second;
        ASSERT_EQ(
            acquireTentCongestionControlAttempt(first, &route, path, 1024, 1),
            adaptive_congestion_control::Decision::kAllow);
        EXPECT_EQ(
            acquireTentCongestionControlAttempt(second, &route, path, 1, 1),
            mode == adaptive_congestion_control::Mode::kEnforce
                ? adaptive_congestion_control::Decision::kDefer
                : adaptive_congestion_control::Decision::kAllow);
        completeTentCongestionControlAttempt(
            first, adaptive_congestion_control::OutcomeClass::kDerivedFlush,
            adaptive_congestion_control::FailureScope::kOperation);
        completeTentCongestionControlAttempt(
            second, adaptive_congestion_control::OutcomeClass::kDerivedFlush,
            adaptive_congestion_control::FailureScope::kOperation);
        EXPECT_EQ(adaptive_congestion_control::snapshot(device).inflight_bytes,
                  0u);
    }
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
    auto first = std::make_shared<TentRdmaCongestionControlRoute>(testConfig());
    auto second =
        std::make_shared<TentRdmaCongestionControlRoute>(testConfig());
    RdmaEndPoint endpoint;

    EXPECT_EQ(endpoint.bindCongestionRoute(first.get()), first.get());
    auto* bound = endpoint.bindCongestionRoute(second.get());
    EXPECT_EQ(bound, first.get());
    auto retained = bound->shared_from_this();
    std::weak_ptr<TentRdmaCongestionControlRoute> weak = first;
    first.reset();
    EXPECT_FALSE(weak.expired());
    EXPECT_EQ(retained.get(), bound);
}

TEST(TentAdaptiveCongestionControlTest, EndpointBindingPreservesNullAndWinner) {
    RdmaEndPoint endpoint;
    EXPECT_EQ(endpoint.bindCongestionRoute(nullptr), nullptr);
    auto first = std::make_shared<TentRdmaCongestionControlRoute>(testConfig());
    auto other = std::make_shared<TentRdmaCongestionControlRoute>(testConfig());
    ASSERT_EQ(endpoint.bindCongestionRoute(first.get()), first.get());
    for (int i = 0; i < 1000; ++i) {
        EXPECT_EQ(endpoint.bindCongestionRoute(other.get()), first.get());
        EXPECT_EQ(endpoint.bindCongestionRoute(nullptr), first.get());
    }
}

TEST(TentAdaptiveCongestionControlTest,
     ConcurrentEndpointBindingPublishesOneWinner) {
    constexpr size_t kThreads = 8;
    RdmaEndPoint endpoint;
    std::array<std::shared_ptr<TentRdmaCongestionControlRoute>, kThreads>
        routes;
    std::array<size_t, kThreads> payload{};
    for (auto& route : routes)
        route = std::make_shared<TentRdmaCongestionControlRoute>(testConfig());
    std::atomic<size_t> ready{0};
    std::atomic<bool> start{false};
    std::atomic<size_t> failures{0};
    std::vector<std::thread> threads;
    for (size_t i = 0; i < kThreads; ++i) {
        threads.emplace_back([&, i] {
            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire))
                std::this_thread::yield();
            payload[i] = i + 1;
            auto* winner = endpoint.bindCongestionRoute(routes[i].get());
            size_t winner_id = 0;
            while (winner_id < kThreads && routes[winner_id].get() != winner)
                ++winner_id;
            if (winner_id == kThreads || payload[winner_id] != winner_id + 1) {
                failures.fetch_add(1, std::memory_order_relaxed);
                return;
            }
            for (int repeat = 0; repeat < 1000; ++repeat) {
                if (endpoint.bindCongestionRoute(routes[i].get()) != winner ||
                    endpoint.congestionRoute() != winner)
                    failures.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    while (ready.load(std::memory_order_acquire) != kThreads)
        std::this_thread::yield();
    start.store(true, std::memory_order_release);
    for (auto& thread : threads) thread.join();
    EXPECT_EQ(failures.load(), 0u);
}

TEST(TentAdaptiveCongestionControlTest, PostedSliceRetainsRouteAndDevice) {
    auto route = std::make_shared<TentRdmaCongestionControlRoute>(testConfig());
    auto device = std::make_shared<adaptive_congestion_control::DomainState>(
        testConfig(), 1);
    route->device_owner = device;
    std::weak_ptr<TentRdmaCongestionControlRoute> weak_route = route;
    std::weak_ptr<adaptive_congestion_control::DomainState> weak_device =
        device;
    {
        RdmaSlice slice;
        slice.congestion_control_route_owner = route;
        adaptive_congestion_control::PathHandle path{device.get(),
                                                     &route->domain, 1, 1};
        ASSERT_EQ(acquireTentCongestionControlAttempt(slice, route.get(), path,
                                                      128, 7),
                  adaptive_congestion_control::Decision::kAllow);
        route.reset();
        device.reset();
        EXPECT_FALSE(weak_route.expired());
        EXPECT_FALSE(weak_device.expired());
        EXPECT_TRUE(completeTentCongestionControlAttempt(
            slice, adaptive_congestion_control::OutcomeClass::kDerivedFlush,
            adaptive_congestion_control::FailureScope::kOperation));
    }
    EXPECT_TRUE(weak_route.expired());
    EXPECT_TRUE(weak_device.expired());
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

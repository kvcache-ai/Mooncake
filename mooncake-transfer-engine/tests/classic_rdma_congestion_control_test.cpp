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

#include <vector>

#include "adaptive_congestion_control.h"
#include "transport/rdma_transport/worker_pool.h"

namespace mooncake {
namespace {

TEST(ClassicRdmaCongestionControlTest, RuntimeOffLeavesQueueUnchanged) {
    ClassicRdmaCongestionControl adapter(adaptive_congestion_control::Config{
        .mode = adaptive_congestion_control::Mode::kOff});
    Transport::Slice slice{};
    slice.length = 64;
    std::vector<Transport::Slice *> queued{&slice};

    EXPECT_EQ(adapter.gate(queued), 1u);

    EXPECT_EQ(queued, std::vector<Transport::Slice *>({&slice}));
}

TEST(ClassicRdmaCongestionControlTest, ObserveModeAdmitsBeyondByteWindow) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kObserve;
    config.min_window_bytes = 64;
    config.max_window_bytes = 64;
    ClassicRdmaCongestionControl adapter(config);
    Transport::Slice first{}, second{}, third{};
    first.length = second.length = third.length = 64;
    std::vector<Transport::Slice *> queued{&first, &second, &third};
    adapter.prepare(queued, "peer@nic");

    EXPECT_EQ(adapter.gate(queued), 3u);
    for (auto *slice : queued) {
        EXPECT_TRUE(slice->rdma_congestion_control_permit.active());
        adapter.releaseUnposted(slice);
        EXPECT_FALSE(slice->rdma_congestion_control_permit.active());
    }
}

TEST(ClassicRdmaCongestionControlTest, SuccessfulCompletionReopensByteWindow) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    config.min_window_bytes = 64;
    config.max_window_bytes = 64;
    ClassicRdmaCongestionControl adapter(config);
    Transport::Slice first{}, second{};
    first.length = second.length = 64;
    auto *endpoint = reinterpret_cast<RdmaEndPoint *>(uintptr_t{1});
    std::vector<Transport::Slice *> first_queue{&first}, next_queue{&second};
    adapter.prepare(&first, "peer@nic");
    adapter.prepare(&second, "peer@nic");
    adapter.bindEndpoint(&first, endpoint);
    adapter.bindEndpoint(&second, endpoint);
    ASSERT_EQ(adapter.gate(first_queue), 1u);
    ASSERT_EQ(adapter.gate(next_queue), 0u);

    adapter.complete(&first, IBV_WC_SUCCESS);

    EXPECT_FALSE(first.rdma_congestion_control_permit.active());
    EXPECT_EQ(adapter.gate(next_queue), 1u);
    adapter.releaseUnposted(&second);
}

TEST(ClassicRdmaCongestionControlTest,
     DeviceResetFencesOldPermitAndPreservesOutstandingAccounting) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    config.min_window_bytes = 64;
    config.max_window_bytes = 64;
    ClassicRdmaCongestionControl adapter(config);
    Transport::Slice old{}, next{}, other_route{};
    old.length = next.length = other_route.length = 64;
    adapter.prepare(&old, "peer@nic");
    adapter.prepare(&next, "peer@nic");
    adapter.prepare(&other_route, "other-peer@nic");
    std::vector<Transport::Slice *> old_queue{&old}, next_queue{&next};
    std::vector<Transport::Slice *> other_queue{&other_route};
    ASSERT_EQ(adapter.gate(old_queue), 1u);

    adapter.resetDevice();

    EXPECT_FALSE(adaptive_congestion_control::isCurrentGeneration(
        old.rdma_congestion_control_permit));
    EXPECT_EQ(adapter.gate(other_queue), 0u);
    adapter.releaseUnposted(&old);
    ASSERT_EQ(adapter.gate(next_queue), 1u);
    adapter.releaseUnposted(&old);
    EXPECT_EQ(adapter.gate(other_queue), 0u);
    adapter.releaseUnposted(&next);
    ASSERT_EQ(adapter.gate(other_queue), 1u);
    adapter.releaseUnposted(&other_route);
    EXPECT_EQ(adapter.gate(next_queue), 1u);
    adapter.releaseUnposted(&next);
}

TEST(ClassicRdmaCongestionControlTest,
     StaleDeviceSuccessDoesNotGrowCurrentWindow) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    config.min_window_bytes = 64;
    config.max_window_bytes = 128;
    ClassicRdmaCongestionControl adapter(config);
    adapter.tick(1);
    Transport::Slice stale{}, outstanding{}, probe{};
    stale.length = 1'000'000;
    outstanding.length = 64;
    probe.length = 16;
    auto *endpoint = reinterpret_cast<RdmaEndPoint *>(uintptr_t{1});
    for (auto *slice : {&stale, &outstanding, &probe}) {
        adapter.prepare(slice, "peer@nic");
        adapter.bindEndpoint(slice, endpoint);
    }
    std::vector<Transport::Slice *> queued{&stale};
    ASSERT_EQ(adapter.gate(queued), 1u);
    adapter.resetDevice();
    adapter.complete(&stale, IBV_WC_SUCCESS);
    EXPECT_FALSE(stale.rdma_congestion_control_permit.active());

    queued = {&outstanding};
    ASSERT_EQ(adapter.gate(queued), 1u);
    adapter.tick(1'000'000'001);

    queued = {&probe};
    EXPECT_EQ(adapter.gate(queued), 0u);
    adapter.releaseUnposted(&outstanding);
    adapter.releaseUnposted(&probe);
}

TEST(ClassicRdmaCongestionControlTest, CleanSuccessGrowsWindowOnNextTick) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    config.min_window_bytes = 64;
    config.max_window_bytes = 128;
    ClassicRdmaCongestionControl adapter(config);
    Transport::Slice completed{}, outstanding{}, probe{};
    completed.length = outstanding.length = 64;
    probe.length = 16;
    for (auto *slice : {&completed, &outstanding, &probe})
        adapter.prepare(slice, "peer@nic");
    std::vector<Transport::Slice *> queued{&completed};
    ASSERT_EQ(adapter.gate(queued), 1u);
    adapter.complete(&completed, IBV_WC_SUCCESS);
    adapter.tick(1);

    queued = {&outstanding};
    ASSERT_EQ(adapter.gate(queued), 1u);
    queued = {&probe};
    EXPECT_EQ(adapter.gate(queued), 1u);
    adapter.releaseUnposted(&outstanding);
    adapter.releaseUnposted(&probe);
}

TEST(ClassicRdmaCongestionControlTest, DeferredSliceStaysOnOwnerPathQueue) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    config.min_window_bytes = 64;
    config.max_window_bytes = 64;
    ClassicRdmaCongestionControl adapter(config);
    Transport::Slice first{};
    first.length = 64;
    Transport::Slice deferred{};
    deferred.length = 64;
    std::vector<Transport::Slice *> first_queue{&first};
    std::vector<Transport::Slice *> second_queue{&deferred};

    adapter.prepare(&first, "peer@nic");
    adapter.prepare(&deferred, "peer@nic");
    const size_t first_admitted = adapter.gate(first_queue);

    ASSERT_EQ(first_admitted, 1u);
    adapter.gate(second_queue);
    ASSERT_EQ(second_queue.size(), 1u);
    EXPECT_EQ(second_queue.front(), &deferred);
}

TEST(ClassicRdmaCongestionControlTest, BatchPrepareSharesOneRouteWindow) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    config.min_window_bytes = 64;
    config.max_window_bytes = 64;
    ClassicRdmaCongestionControl adapter(config);
    Transport::Slice first{};
    first.length = 64;
    Transport::Slice second{};
    second.length = 64;
    std::vector<Transport::Slice *> queued{&first, &second};

    adapter.prepare(queued, "peer@nic");

    EXPECT_EQ(adapter.gate(queued), 1u);
    EXPECT_EQ(queued.size(), 2u);
}

TEST(ClassicRdmaCongestionControlTest,
     ActiveRouteDefersPartialReplenishmentWithoutLeakingPermits) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    config.min_window_bytes = 64;
    config.max_window_bytes = 64;
    ClassicRdmaCongestionControl adapter(config);
    Transport::Slice active{};
    active.length = 48;
    std::vector<Transport::Slice *> initial{&active};
    adapter.prepare(initial, "peer@nic");
    ASSERT_EQ(adapter.gate(initial), 1u);

    Transport::Slice first{}, second{};
    first.length = second.length = 16;
    std::vector<Transport::Slice *> queued{&first, &second};
    adapter.prepare(queued, "peer@nic");
    for (int retry = 0; retry < 3; ++retry) {
        EXPECT_EQ(adapter.gate(queued), 0u);
        EXPECT_FALSE(first.rdma_congestion_control_permit.active());
        EXPECT_FALSE(second.rdma_congestion_control_permit.active());
    }

    adapter.releaseUnposted(&active);
    EXPECT_EQ(adapter.gate(queued), 2u);
    adapter.releaseUnposted(&first);
    adapter.releaseUnposted(&second);
}

TEST(ClassicRdmaCongestionControlTest, ActiveRouteAdmitsCompleteShortTail) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    config.min_window_bytes = config.max_window_bytes = 64;
    ClassicRdmaCongestionControl adapter(config);
    Transport::Slice active{}, tail{};
    active.length = 48;
    tail.length = 16;
    std::vector<Transport::Slice *> initial{&active}, queued{&tail};
    adapter.prepare(initial, "peer@nic");
    adapter.prepare(queued, "peer@nic");
    ASSERT_EQ(adapter.gate(initial), 1u);
    EXPECT_EQ(adapter.gate(queued), 1u);
    adapter.releaseUnposted(&active);
    adapter.releaseUnposted(&tail);
}

TEST(ClassicRdmaCongestionControlTest,
     ActiveRouteRetainsWholeReplenishmentGroups) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    config.min_window_bytes = 64;
    config.max_window_bytes = 128;
    ClassicRdmaCongestionControl adapter(config);
    Transport::Slice completed{};
    completed.length = 16;
    std::vector<Transport::Slice *> growth{&completed};
    adapter.prepare(growth, "peer@nic");
    for (uint64_t epoch = 1; epoch <= 4; ++epoch) {
        ASSERT_EQ(adapter.gate(growth), 1u);
        adapter.complete(&completed, IBV_WC_SUCCESS);
        adapter.tick(epoch);
    }
    Transport::Slice active{};
    active.length = 48;
    std::vector<Transport::Slice *> initial{&active};
    adapter.prepare(initial, "peer@nic");
    ASSERT_EQ(adapter.gate(initial), 1u);
    Transport::Slice slices[6]{};
    std::vector<Transport::Slice *> queued;
    for (auto &slice : slices) {
        slice.length = 16;
        queued.push_back(&slice);
    }
    adapter.prepare(queued, "peer@nic");
    EXPECT_EQ(adapter.gate(queued), 4u);
    for (size_t i = 0; i < queued.size(); ++i) {
        EXPECT_EQ(queued[i]->rdma_congestion_control_permit.active(), i < 4);
        adapter.releaseUnposted(queued[i]);
    }
    adapter.releaseUnposted(&active);
}

TEST(ClassicRdmaCongestionControlTest,
     MixedRoutesDoNotCoalescePartialAdmission) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    config.min_window_bytes = 64;
    config.max_window_bytes = 64;
    ClassicRdmaCongestionControl adapter(config);
    Transport::Slice active{}, first{}, second{};
    active.length = 32;
    first.length = 16;
    second.length = 32;
    std::vector<Transport::Slice *> initial{&active}, queued{&first, &second};
    adapter.prepare(initial, "peer@nic");
    adapter.prepare(&first, "peer@nic");
    adapter.prepare(&second, "other@nic");
    ASSERT_EQ(adapter.gate(initial), 1u);
    EXPECT_EQ(adapter.gate(queued), 1u);
    adapter.releaseUnposted(&active);
    adapter.releaseUnposted(&first);
}

TEST(ClassicRdmaCongestionControlTest,
     ReplenishmentKeepsIndivisibleSliceGroups) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    config.min_window_bytes = config.max_window_bytes = 64;
    ClassicRdmaCongestionControl adapter(config);
    Transport::Slice active{}, slices[3]{};
    active.length = 16;
    std::vector<Transport::Slice *> initial{&active}, queued;
    adapter.prepare(initial, "peer@nic");
    ASSERT_EQ(adapter.gate(initial), 1u);
    for (auto &slice : slices) {
        slice.length = 24;
        queued.push_back(&slice);
    }
    adapter.prepare(queued, "peer@nic");
    EXPECT_EQ(adapter.gate(queued), 2u);
    for (auto *slice : queued) adapter.releaseUnposted(slice);
    adapter.releaseUnposted(&active);
}

TEST(ClassicRdmaCongestionControlTest, OffAndObserveDoNotGroupReplenishment) {
    for (auto mode : {adaptive_congestion_control::Mode::kOff,
                      adaptive_congestion_control::Mode::kObserve}) {
        adaptive_congestion_control::Config config;
        config.mode = mode;
        config.min_window_bytes = config.max_window_bytes = 64;
        ClassicRdmaCongestionControl adapter(config);
        Transport::Slice active{}, first{}, second{};
        active.length = 48;
        first.length = second.length = 16;
        std::vector<Transport::Slice *> empty, initial{&active};
        std::vector<Transport::Slice *> queued{&first, &second};
        EXPECT_EQ(adapter.gate(empty), 0u);
        adapter.prepare(initial, "peer@nic");
        adapter.prepare(queued, "peer@nic");
        ASSERT_EQ(adapter.gate(initial), 1u);
        EXPECT_EQ(adapter.gate(queued), 2u);
        adapter.releaseUnposted(&active);
        adapter.releaseUnposted(&first);
        adapter.releaseUnposted(&second);
    }
}

TEST(ClassicRdmaCongestionControlTest, CompletionLetsRetryReacquire) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    config.min_window_bytes = 64;
    config.max_window_bytes = 64;
    ClassicRdmaCongestionControl adapter(config);
    Transport::Slice slice{};
    slice.length = 64;
    std::vector<Transport::Slice *> queued{&slice};
    adapter.prepare(&slice, "peer@nic");
    ASSERT_EQ(adapter.gate(queued), 1u);

    adapter.complete(&slice, IBV_WC_RETRY_EXC_ERR);
    queued = {&slice};
    EXPECT_EQ(adapter.gate(queued), 1u);
}

TEST(ClassicRdmaCongestionControlTest, LocalHandoffRebindsRouteOwner) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    const std::string path = "peer@nic";
    for (bool batch : {false, true}) {
        ClassicRdmaCongestionControl failed_adapter(config),
            healthy_adapter(config);
        Transport::Slice slice{};
        slice.length = 64;
        failed_adapter.prepare(&slice, path);
        std::vector<Transport::Slice *> queued{&slice};
        ASSERT_EQ(failed_adapter.gate(queued), 1u);
        ASSERT_TRUE(slice.rdma_congestion_control_permit.active());
        // Local failover retains the remote NIC path on the same slice.
        if (batch) {
            healthy_adapter.prepare(queued, path);
        } else {
            healthy_adapter.prepare(&slice, path);
        }
        EXPECT_FALSE(slice.rdma_congestion_control_permit.active());
        EXPECT_EQ(healthy_adapter.gate(queued), 1u);
        healthy_adapter.releaseUnposted(&slice);
    }
}

TEST(ClassicRdmaCongestionControlTest,
     CurrentEndpointFailureDoesNotCreateAnotherHealthGate) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    config.min_window_bytes = 64;
    config.max_window_bytes = 64;
    ClassicRdmaCongestionControl adapter(config);
    Transport::Slice failed{};
    failed.length = 64;
    Transport::Slice next{};
    next.length = 64;
    auto *endpoint = reinterpret_cast<RdmaEndPoint *>(uintptr_t{1});
    std::vector<Transport::Slice *> queued{&failed};

    adapter.prepare(&failed, "peer@nic");
    adapter.bindEndpoint(&failed, endpoint);
    ASSERT_EQ(adapter.gate(queued), 1u);
    adapter.complete(&failed, IBV_WC_FATAL_ERR);
    adapter.tick(1);

    adapter.prepare(&next, "peer@nic");
    queued = {&next};
    EXPECT_EQ(adapter.gate(queued), 1u);
    adapter.releaseUnposted(&next);
}

TEST(ClassicRdmaCongestionControlTest, StaleEndpointFailureIsIgnored) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    config.min_window_bytes = 64;
    config.max_window_bytes = 64;
    ClassicRdmaCongestionControl adapter(config);
    Transport::Slice stale{};
    stale.length = 64;
    Transport::Slice next{};
    next.length = 64;
    auto *old_endpoint = reinterpret_cast<RdmaEndPoint *>(uintptr_t{1});
    auto *new_endpoint = reinterpret_cast<RdmaEndPoint *>(uintptr_t{2});
    std::vector<Transport::Slice *> queued{&stale};

    adapter.prepare(&stale, "peer@nic");
    adapter.bindEndpoint(&stale, old_endpoint);
    ASSERT_EQ(adapter.gate(queued), 1u);
    adapter.bindEndpoint(&stale, new_endpoint);
    adapter.complete(&stale, IBV_WC_FATAL_ERR);
    adapter.tick(1);

    adapter.prepare(&next, "peer@nic");
    queued = {&next};
    EXPECT_EQ(adapter.gate(queued), 1u);
}

TEST(ClassicRdmaCongestionControlTest,
     ReusedEndpointAddressDoesNotReviveOldFailure) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    config.min_window_bytes = 64;
    config.max_window_bytes = 64;
    ClassicRdmaCongestionControl adapter(config);
    Transport::Slice stale{};
    stale.length = 64;
    Transport::Slice replacement{};
    replacement.length = 64;
    Transport::Slice next{};
    next.length = 64;
    auto *reused_endpoint = reinterpret_cast<RdmaEndPoint *>(uintptr_t{1});
    std::vector<Transport::Slice *> queued{&stale};

    adapter.prepare(&stale, "peer@nic");
    adapter.bindEndpoint(&stale, reused_endpoint);
    ASSERT_EQ(adapter.gate(queued), 1u);
    adapter.retireEndpoint("peer@nic", reused_endpoint);
    adapter.prepare(&replacement, "peer@nic");
    adapter.bindEndpoint(&replacement, reused_endpoint);

    adapter.complete(&stale, IBV_WC_FATAL_ERR);
    adapter.tick(1);

    adapter.prepare(&next, "peer@nic");
    queued = {&next};
    EXPECT_EQ(adapter.gate(queued), 1u);
}

}  // namespace
}  // namespace mooncake

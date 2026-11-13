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
#include <thread>
#include <vector>

#include "adaptive_congestion_control.h"
#include "multi_transport.h"
#include "task_congestion_status.h"
#include "transfer_engine.h"
#include "transfer_engine_c.h"
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
    Transport::TransferTask task{};
    task.slice_count = 1;
    first.task = &task;
    first.length = second.length = 16;
    std::vector<Transport::Slice *> queued{&first, &second};
    adapter.prepare(queued, "peer@nic");
    for (int retry = 0; retry < 3; ++retry) {
        EXPECT_EQ(adapter.gate(queued), 0u);
        EXPECT_FALSE(first.rdma_congestion_control_permit.active());
        EXPECT_FALSE(second.rdma_congestion_control_permit.active());
        EXPECT_TRUE(first.rdma_congestion_control_observed_blocked.load());
    }

    adapter.releaseUnposted(&active);
    EXPECT_EQ(adapter.gate(queued), 2u);
    EXPECT_FALSE(first.rdma_congestion_control_observed_blocked.load());
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

TEST(ClassicRdmaCongestionControlTest,
     DeferredLogicalTaskIsQueryableWithoutPollingTransport) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    config.min_window_bytes = 64;
    config.max_window_bytes = 64;
    ClassicRdmaCongestionControl adapter(config);
    TransferEngine engine(false);
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "classic-congestion-c-query"), 0);
    const auto batch_id = engine.allocateBatchID(1);
    auto &batch = Transport::toBatchDesc(batch_id);
    auto &task = batch.task_list.emplace_back();
    task.batch_id = batch_id;
    auto *deferred = new Transport::Slice{};
    deferred->length = 64;
    deferred->task = &task;
    task.slice_list.push_back(deferred);
    task.slice_count = 1;
    Transport::Slice first{};
    first.length = 64;
    std::vector<Transport::Slice *> first_queue{&first};
    std::vector<Transport::Slice *> target_queue{deferred};
    adapter.prepare(&first, "peer@nic");
    adapter.prepare(deferred, "peer@nic");
    ASSERT_EQ(adapter.gate(first_queue), 1u);
    ASSERT_EQ(adapter.gate(target_queue), 0u);
    EXPECT_TRUE(deferred->rdma_congestion_control_observed_blocked.load());

    TaskCongestionState state = TaskCongestionState::kUnknown;
    ASSERT_TRUE(engine.getTaskCongestionState(batch_id, 0, state).ok());
    EXPECT_EQ(state, TaskCongestionState::kCongested);
    TaskCongestionDetail detail;
    ASSERT_TRUE(engine.getTaskCongestionDetail(batch_id, 0, detail).ok());
    EXPECT_TRUE(detail.reason.observed);
    EXPECT_EQ(detail.reason.value, TaskCongestionReason::kByteWindow);
    // Both route and device windows are full; admission checks device first.
    ASSERT_TRUE(detail.failure_scope.observed);
    EXPECT_EQ(detail.failure_scope.value, TaskCongestionFailureScope::kDevice);

    ASSERT_TRUE(detail.observed_at_ns.observed);
    const auto episode = detail;
    for (int retry = 0; retry < 8; ++retry) {
        ASSERT_EQ(adapter.gate(target_queue), 0u);
        ASSERT_TRUE(engine.getTaskCongestionDetail(batch_id, 0, detail).ok());
        EXPECT_EQ(detail.observed_at_ns.value, episode.observed_at_ns.value);
        EXPECT_EQ(detail.window_bytes.value, episode.window_bytes.value);
        EXPECT_EQ(detail.inflight_bytes.value, episode.inflight_bytes.value);
    }

    auto c_engine = static_cast<transfer_engine_t>(&engine);
    int c_state = TASK_CONGESTION_UNKNOWN;
    ASSERT_EQ(::getTaskCongestionState(c_engine, batch_id, 0, &c_state), 0);
    EXPECT_EQ(c_state, TASK_CONGESTION_CONGESTED);
    EXPECT_NE(::getTaskCongestionState(c_engine, batch_id, 1, &c_state), 0);
    task_congestion_detail_t c_detail{};
    size_t required = 0;
    ASSERT_EQ(::getTaskCongestionDetail(c_engine, batch_id, 0, &c_detail,
                                        nullptr, 0, &required),
              0);
    ASSERT_GT(required, 1u);
    EXPECT_NE(::getTaskCongestionDetail(c_engine, batch_id, 0, nullptr, nullptr,
                                        0, &required),
              0);
    EXPECT_EQ(c_detail.state, TASK_CONGESTION_CONGESTED);
    EXPECT_NE(c_detail.observed_fields & TASK_CONGESTION_HAS_AFFECTED_PATH, 0u);
    std::vector<char> path(required, '!');
    EXPECT_NE(
        ::getTaskCongestionDetail(c_engine, batch_id, 0, &c_detail, path.data(),
                                  path.size() - 1, &required),
        0);
    for (char byte : path) EXPECT_EQ(byte, '!');
    ASSERT_EQ(::getTaskCongestionDetail(c_engine, batch_id, 0, &c_detail,
                                        path.data(), path.size(), &required),
              0);
    EXPECT_STREQ(path.data(), "peer@nic");

    adapter.releaseUnposted(&first);
    ASSERT_EQ(adapter.gate(target_queue), 1u);
    ASSERT_TRUE(engine.getTaskCongestionDetail(batch_id, 0, detail).ok());
    EXPECT_EQ(detail.state, TaskCongestionState::kNormal);
    EXPECT_TRUE(detail.resolved);
    adapter.releaseUnposted(deferred);
    ASSERT_EQ(adapter.gate(first_queue), 1u);
    ASSERT_EQ(adapter.gate(target_queue), 0u);
    ASSERT_TRUE(engine.getTaskCongestionDetail(batch_id, 0, detail).ok());
    EXPECT_EQ(detail.state, TaskCongestionState::kCongested);
    EXPECT_GT(detail.observed_at_ns.value, episode.observed_at_ns.value);
    adapter.releaseUnposted(&first);
    task.is_finished = true;
    EXPECT_TRUE(engine.freeBatchID(batch_id).ok());
}

TEST(ClassicRdmaCongestionControlTest,
     WindowEpisodeRefreshesAfterUnavailablePathOwnerAndGenerationChanges) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    config.min_window_bytes = 64;
    config.max_window_bytes = 64;
    ClassicRdmaCongestionControl first(config), second(config);
    std::string local_name = "local";
    MultiTransport transports(nullptr, local_name);
    const auto batch_id = transports.allocateBatchID(1);
    auto &task = Transport::toBatchDesc(batch_id).task_list.emplace_back();
    task.batch_id = batch_id;
    auto *slice = new Transport::Slice{};
    slice->task = &task;
    slice->length = 64;
    task.slice_list.push_back(slice);
    task.slice_count = 1;
    Transport::Slice blocker_a{}, blocker_b{};
    blocker_a.length = blocker_b.length = 64;
    first.prepare(&blocker_a, "path-a");
    second.prepare(&blocker_b, "path-b");
    std::vector<Transport::Slice *> queue_a{&blocker_a}, queue_b{&blocker_b};
    std::vector<Transport::Slice *> queued{slice};
    ASSERT_EQ(first.gate(queue_a), 1u);
    ASSERT_EQ(second.gate(queue_b), 1u);
    first.prepare(slice, "path-a");
    ASSERT_EQ(first.gate(queued), 0u);
    TaskCongestionDetail detail;
    ASSERT_TRUE(transports.getTaskCongestionDetail(batch_id, 0, detail).ok());
    auto previous = detail;

    first.observeUnavailable(slice, "path-a",
                             TaskCongestionFailureScope::kRoute, 1000);
    ASSERT_TRUE(transports.getTaskCongestionDetail(batch_id, 0, detail).ok());
    EXPECT_EQ(detail.reason.value, TaskCongestionReason::kPathQuarantined);
    EXPECT_GT(detail.observed_at_ns.value, previous.observed_at_ns.value);
    previous = detail;
    ASSERT_EQ(first.gate(queued), 0u);
    ASSERT_TRUE(transports.getTaskCongestionDetail(batch_id, 0, detail).ok());
    EXPECT_EQ(detail.state, TaskCongestionState::kCongested);
    EXPECT_EQ(detail.reason.value, TaskCongestionReason::kByteWindow);
    EXPECT_GT(detail.observed_at_ns.value, previous.observed_at_ns.value);
    previous = detail;

    first.prepare(slice, "path-b");
    ASSERT_EQ(first.gate(queued), 0u);
    ASSERT_TRUE(transports.getTaskCongestionDetail(batch_id, 0, detail).ok());
    EXPECT_EQ(detail.affected_path.value, "path-b");
    EXPECT_GT(detail.observed_at_ns.value, previous.observed_at_ns.value);
    previous = detail;
    second.prepare(slice, "path-b");
    ASSERT_EQ(second.gate(queued), 0u);
    ASSERT_TRUE(transports.getTaskCongestionDetail(batch_id, 0, detail).ok());
    EXPECT_GT(detail.observed_at_ns.value, previous.observed_at_ns.value);
    previous = detail;

    second.resetDevice();
    second.releaseUnposted(&blocker_b);
    ASSERT_EQ(second.gate(queue_b), 1u);
    ASSERT_EQ(second.gate(queued), 0u);
    ASSERT_TRUE(transports.getTaskCongestionDetail(batch_id, 0, detail).ok());
    EXPECT_EQ(detail.failure_scope.value, TaskCongestionFailureScope::kDevice);
    EXPECT_NE(detail.controller_generation.value,
              previous.controller_generation.value);
    EXPECT_GT(detail.observed_at_ns.value, previous.observed_at_ns.value);
    const auto refreshed = detail;
    ASSERT_EQ(second.gate(queued), 0u);
    ASSERT_TRUE(transports.getTaskCongestionDetail(batch_id, 0, detail).ok());
    EXPECT_EQ(detail.observed_at_ns.value, refreshed.observed_at_ns.value);
    second.releaseUnposted(&blocker_b);
    blocker_b.length = 32;
    ASSERT_EQ(second.gate(queue_b), 1u);
    slice->length = 16;
    Transport::Slice tail{}, other_route{};
    tail.length = 32;
    other_route.length = 32;
    second.prepare(&tail, "path-b");
    std::vector<Transport::Slice *> partial{slice, &tail};
    // The 16-byte admission leaves 16 bytes free, so the 32-byte tail defers.
    // Together they are below one 64-byte replenishment group and roll back.
    ASSERT_EQ(second.gate(partial), 0u);
    ASSERT_TRUE(transports.getTaskCongestionDetail(batch_id, 0, detail).ok());
    EXPECT_EQ(detail.failure_scope.value, TaskCongestionFailureScope::kRoute);
    previous = detail;
    second.prepare(&other_route, "path-c");
    std::vector<Transport::Slice *> other_queue{&other_route};
    ASSERT_EQ(second.gate(other_queue), 1u);
    ASSERT_EQ(second.gate(queued), 0u);
    ASSERT_TRUE(transports.getTaskCongestionDetail(batch_id, 0, detail).ok());
    EXPECT_EQ(detail.failure_scope.value, TaskCongestionFailureScope::kDevice);
    EXPECT_EQ(detail.controller_generation.value,
              refreshed.controller_generation.value);
    EXPECT_GT(detail.observed_at_ns.value, previous.observed_at_ns.value);
    second.releaseUnposted(&other_route);
    first.releaseUnposted(&blocker_a);
    second.releaseUnposted(&blocker_b);
    task.is_finished = true;
    EXPECT_TRUE(transports.freeBatchID(batch_id).ok());
}

TEST(ClassicRdmaCongestionControlTest,
     RecycledSliceRecordsANewTasksWindowEpisode) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    config.min_window_bytes = config.max_window_bytes = 64;
    ClassicRdmaCongestionControl adapter(config);
    std::string local_name = "local";
    MultiTransport transports(nullptr, local_name);
    const auto batch_id = transports.allocateBatchID(2);
    auto &tasks = Transport::toBatchDesc(batch_id).task_list;
    tasks.emplace_back();
    tasks.emplace_back();
    Transport::ThreadLocalSliceCache cache;
    auto *slice = cache.allocate();
    slice->task = &tasks[0];
    slice->length = 64;
    tasks[0].slice_count = 1;
    tasks[1].slice_count = 1;
    Transport::Slice blocker{};
    blocker.length = 64;
    adapter.prepare(&blocker, "peer@nic");
    adapter.prepare(slice, "peer@nic");
    std::vector<Transport::Slice *> blocking{&blocker}, queued{slice};
    ASSERT_EQ(adapter.gate(blocking), 1u);
    ASSERT_EQ(adapter.gate(queued), 0u);
    TaskCongestionDetail before, after;
    ASSERT_TRUE(transports.getTaskCongestionDetail(batch_id, 0, before).ok());
    cache.deallocate(slice);
    auto *recycled = cache.allocate();
    ASSERT_EQ(recycled, slice);
    EXPECT_EQ(recycled->rdma_congestion_control_window_episode, 0u);
    EXPECT_FALSE(recycled->rdma_congestion_control_observed_blocked.load());
    recycled->task = &tasks[1];
    adapter.prepare(recycled, "peer@nic");
    ASSERT_EQ(adapter.gate(queued), 0u);
    ASSERT_TRUE(transports.getTaskCongestionDetail(batch_id, 1, after).ok());
    EXPECT_EQ(after.state, TaskCongestionState::kCongested);
    EXPECT_GT(after.observed_at_ns.value, before.observed_at_ns.value);
    adapter.releaseUnposted(&blocker);
    cache.deallocate(recycled);
    tasks[0].is_finished = true;
    tasks[1].is_finished = true;
    EXPECT_TRUE(transports.freeBatchID(batch_id).ok());
}

TEST(ClassicRdmaCongestionControlTest,
     LatePublishedObservationCoversAlreadyPublishedSlices) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    config.min_window_bytes = 64;
    config.max_window_bytes = 64;
    ClassicRdmaCongestionControl adapter(config);
    std::string local_name = "local";
    MultiTransport transports(nullptr, local_name);
    const auto batch_id = transports.allocateBatchID(1);
    auto &task = Transport::toBatchDesc(batch_id).task_list.emplace_back();
    task.batch_id = batch_id;
    task.rdma_congestion_control_mode = {
        true, TaskCongestionControllerMode::kEnforce};
    auto *initial = new Transport::Slice{};
    initial->task = &task;
    initial->length = 64;
    task.slice_list.push_back(initial);
    task.slice_count = 1;

    // A worker can construct its lazy observation before the submitter
    // publishes another slice, then win the publication CAS after that flush.
    auto delayed = std::make_shared<
        adaptive_congestion_control::TaskCongestionObservation>(1);
    auto *deferred = new Transport::Slice{};
    deferred->task = &task;
    deferred->length = 64;
    deferred->rdma_congestion_control_slice_ordinal = 1;
    task.slice_list.push_back(deferred);
    __atomic_store_n(&task.slice_count, 2, __ATOMIC_RELEASE);
    std::atomic_store_explicit(&task.congestion_observation, delayed,
                               std::memory_order_release);

    Transport::Slice blocker{};
    blocker.length = 64;
    adapter.prepare(&blocker, "peer@nic");
    adapter.prepare(deferred, "peer@nic");
    std::vector<Transport::Slice *> blocker_queue{&blocker};
    std::vector<Transport::Slice *> deferred_queue{deferred};
    ASSERT_EQ(adapter.gate(blocker_queue), 1u);
    EXPECT_EQ(adapter.gate(deferred_queue), 0u);

    TaskCongestionState state = TaskCongestionState::kUnknown;
    EXPECT_TRUE(transports.getTaskCongestionState(batch_id, 0, state).ok());
    EXPECT_EQ(state, TaskCongestionState::kCongested);
    TaskCongestionDetail detail;
    EXPECT_TRUE(transports.getTaskCongestionDetail(batch_id, 0, detail).ok());
    EXPECT_EQ(detail.state, TaskCongestionState::kCongested);
    EXPECT_TRUE(detail.slice_id.observed);
    EXPECT_EQ(detail.slice_id.value, 1u);

    adapter.releaseUnposted(&blocker);
    EXPECT_EQ(adapter.gate(deferred_queue), 1u);
    EXPECT_TRUE(transports.getTaskCongestionState(batch_id, 0, state).ok());
    EXPECT_EQ(state, TaskCongestionState::kNormal);
    EXPECT_TRUE(transports.getTaskCongestionDetail(batch_id, 0, detail).ok());
    EXPECT_EQ(detail.state, TaskCongestionState::kNormal);
    EXPECT_TRUE(detail.resolved);
    adapter.releaseUnposted(deferred);
    task.is_finished = true;
    EXPECT_TRUE(transports.freeBatchID(batch_id).ok());
}

TEST(ClassicRdmaCongestionControlTest,
     ReceiverPressureFeedbackIsRecordedOnLogicalTask) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    ClassicRdmaCongestionControl adapter(config);
    std::string local_name = "local";
    MultiTransport transports(nullptr, local_name);
    const auto batch_id = transports.allocateBatchID(1);
    auto &batch = Transport::toBatchDesc(batch_id);
    auto &task = batch.task_list.emplace_back();
    task.batch_id = batch_id;
    auto *slice = new Transport::Slice{};
    slice->length = 64;
    slice->task = &task;
    task.slice_list.push_back(slice);
    task.slice_count = 1;
    auto *endpoint = reinterpret_cast<RdmaEndPoint *>(uintptr_t{1});
    adapter.prepare(slice, "peer@nic");
    adapter.bindEndpoint(slice, endpoint);
    std::vector<Transport::Slice *> queued{slice};
    ASSERT_EQ(adapter.gate(queued), 1u);
    EXPECT_FALSE(slice->rdma_congestion_control_observed_blocked.load());
    adapter.complete(slice, IBV_WC_RNR_RETRY_EXC_ERR);
    EXPECT_TRUE(slice->rdma_congestion_control_observed_blocked.load());

    TaskCongestionDetail detail;
    ASSERT_TRUE(transports.getTaskCongestionDetail(batch_id, 0, detail).ok());
    EXPECT_EQ(detail.state, TaskCongestionState::kCongested);
    ASSERT_TRUE(detail.reason.observed);
    EXPECT_EQ(detail.reason.value, TaskCongestionReason::kReceiverPressure);
    ASSERT_TRUE(detail.failure_scope.observed);
    EXPECT_EQ(detail.failure_scope.value, TaskCongestionFailureScope::kQp);

    task.is_finished = true;
    EXPECT_TRUE(transports.freeBatchID(batch_id).ok());
}

TEST(ClassicRdmaCongestionControlTest,
     RouteTimeoutFeedbackIsRecordedOnLogicalTask) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    ClassicRdmaCongestionControl adapter(config);
    std::string local_name = "local";
    MultiTransport transports(nullptr, local_name);
    const auto batch_id = transports.allocateBatchID(1);
    auto &task = Transport::toBatchDesc(batch_id).task_list.emplace_back();
    task.batch_id = batch_id;
    task.rdma_congestion_control_mode = {
        true, TaskCongestionControllerMode::kEnforce};
    auto *slice = new Transport::Slice{};
    slice->length = 64;
    slice->task = &task;
    task.slice_list.push_back(slice);
    task.slice_count = 1;
    auto *endpoint = reinterpret_cast<RdmaEndPoint *>(uintptr_t{1});
    adapter.prepare(slice, "peer@nic");
    adapter.bindEndpoint(slice, endpoint);
    std::vector<Transport::Slice *> queued{slice};
    ASSERT_EQ(adapter.gate(queued), 1u);
    adapter.complete(slice, IBV_WC_RETRY_EXC_ERR);

    TaskCongestionDetail detail;
    ASSERT_TRUE(transports.getTaskCongestionDetail(batch_id, 0, detail).ok());
    EXPECT_EQ(detail.state, TaskCongestionState::kCongested);
    ASSERT_TRUE(detail.reason.observed);
    EXPECT_EQ(detail.reason.value, TaskCongestionReason::kRouteTimeout);

    task.is_finished = true;
    EXPECT_TRUE(transports.freeBatchID(batch_id).ok());
}

TEST(ClassicRdmaCongestionControlTest,
     ActualRdmaModeControlsHealthyTaskClassification) {
    std::string local_name = "local";
    MultiTransport transports(nullptr, local_name);
    const auto batch_id = transports.allocateBatchID(1);
    auto &task = Transport::toBatchDesc(batch_id).task_list.emplace_back();
    task.batch_id = batch_id;
    task.rdma_congestion_control_mode = {
        true, TaskCongestionControllerMode::kEnforce};

    TaskCongestionState state = TaskCongestionState::kUnknown;
    ASSERT_TRUE(transports.getTaskCongestionState(batch_id, 0, state).ok());
    EXPECT_EQ(state, TaskCongestionState::kNormal);
    task.failed_slice_count = 1;
    ASSERT_TRUE(transports.getTaskCongestionState(batch_id, 0, state).ok());
    EXPECT_EQ(state, TaskCongestionState::kUnknown);
    task.failed_slice_count = 0;
    task.rdma_congestion_control_mode = {true,
                                         TaskCongestionControllerMode::kOff};
    ASSERT_TRUE(transports.getTaskCongestionState(batch_id, 0, state).ok());
    EXPECT_EQ(state, TaskCongestionState::kUnknown);

    task.is_finished = true;
    EXPECT_TRUE(transports.freeBatchID(batch_id).ok());
}

TEST(ClassicRdmaCongestionControlTest,
     LegacyUnavailableResolvesAfterAdmission) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    ClassicRdmaCongestionControl adapter(config);
    std::string local_name = "local";
    MultiTransport transports(nullptr, local_name);
    const auto batch_id = transports.allocateBatchID(1);
    auto &task = Transport::toBatchDesc(batch_id).task_list.emplace_back();
    task.batch_id = batch_id;
    task.rdma_congestion_control_mode = {
        true, TaskCongestionControllerMode::kEnforce};
    auto *slice = new Transport::Slice{};
    slice->task = &task;
    slice->length = 64;
    task.slice_list.push_back(slice);
    task.slice_count = 1;
    const std::string path = "peer@nic";
    adapter.prepare(slice, path);
    adapter.observeUnavailable(slice, path, TaskCongestionFailureScope::kRoute,
                               1000);
    std::vector<Transport::Slice *> queued{slice};
    EXPECT_TRUE(slice->rdma_congestion_control_observed_blocked.load());

    TaskCongestionDetail detail;
    ASSERT_TRUE(transports.getTaskCongestionDetail(batch_id, 0, detail).ok());
    EXPECT_EQ(detail.state, TaskCongestionState::kLongUnavailable);
    ASSERT_TRUE(detail.reason.observed);
    EXPECT_EQ(detail.reason.value, TaskCongestionReason::kPathQuarantined);
    ASSERT_TRUE(detail.affected_path.observed);
    EXPECT_EQ(detail.affected_path.value, path);
    EXPECT_FALSE(detail.resolved);

    adapter.tick(2);
    queued = {slice};
    ASSERT_EQ(adapter.gate(queued), 1u);
    EXPECT_FALSE(slice->rdma_congestion_control_observed_blocked.load());
    ASSERT_TRUE(transports.getTaskCongestionDetail(batch_id, 0, detail).ok());
    EXPECT_EQ(detail.state, TaskCongestionState::kNormal);
    EXPECT_TRUE(detail.resolved);
    adapter.releaseUnposted(slice);
    task.is_finished = true;
    EXPECT_TRUE(transports.freeBatchID(batch_id).ok());
}

TEST(ClassicRdmaCongestionControlTest,
     UnavailableSliceOutranksDeferredSliceInOneTask) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    config.min_window_bytes = 64;
    config.max_window_bytes = 64;
    ClassicRdmaCongestionControl adapter(config);
    std::string local_name = "local";
    MultiTransport transports(nullptr, local_name);
    const auto batch_id = transports.allocateBatchID(1);
    auto &task = Transport::toBatchDesc(batch_id).task_list.emplace_back();
    task.batch_id = batch_id;
    task.rdma_congestion_control_mode = {
        true, TaskCongestionControllerMode::kEnforce};
    auto *deferred = new Transport::Slice{};
    deferred->task = &task;
    deferred->length = 64;
    deferred->rdma_congestion_control_slice_ordinal = 0;
    task.slice_list.push_back(deferred);
    auto *unavailable = new Transport::Slice{};
    unavailable->task = &task;
    unavailable->length = 64;
    unavailable->rdma_congestion_control_slice_ordinal = 1;
    task.slice_list.push_back(unavailable);
    task.slice_count = 2;
    Transport::Slice first{};
    first.length = 64;
    adapter.prepare(&first, "peer-a@nic");
    adapter.prepare(deferred, "peer-a@nic");
    adapter.prepare(unavailable, "peer-b@nic");
    const std::string unavailable_path = "peer-b@nic";
    adapter.observeUnavailable(unavailable, unavailable_path,
                               TaskCongestionFailureScope::kRoute, 1000);
    std::vector<Transport::Slice *> first_queue{&first}, queued{deferred};
    ASSERT_EQ(adapter.gate(first_queue), 1u);
    ASSERT_EQ(adapter.gate(queued), 0u);

    TaskCongestionDetail detail;
    ASSERT_TRUE(transports.getTaskCongestionDetail(batch_id, 0, detail).ok());
    EXPECT_EQ(detail.state, TaskCongestionState::kLongUnavailable);
    ASSERT_TRUE(detail.slice_id.observed);
    EXPECT_EQ(detail.slice_id.value, 1u);
    adapter.tick(2);
    adapter.releaseUnposted(&first);
    queued = {unavailable};
    ASSERT_EQ(adapter.gate(queued), 1u);
    ASSERT_TRUE(transports.getTaskCongestionDetail(batch_id, 0, detail).ok());
    EXPECT_EQ(detail.state, TaskCongestionState::kCongested);
    EXPECT_EQ(detail.slice_id.value, 0u);
    EXPECT_FALSE(detail.resolved);
    ASSERT_TRUE(detail.reason.observed);
    EXPECT_EQ(detail.reason.value, TaskCongestionReason::kByteWindow);
    ASSERT_TRUE(detail.affected_path.observed);
    EXPECT_EQ(detail.affected_path.value, "peer-a@nic");
    adapter.releaseUnposted(unavailable);
    queued = {deferred};
    ASSERT_EQ(adapter.gate(queued), 1u);
    ASSERT_TRUE(transports.getTaskCongestionDetail(batch_id, 0, detail).ok());
    EXPECT_EQ(detail.state, TaskCongestionState::kNormal);
    EXPECT_TRUE(detail.resolved);
    EXPECT_EQ(detail.slice_id.value, 0u);
    EXPECT_EQ(detail.reason.value, TaskCongestionReason::kByteWindow);
    EXPECT_EQ(detail.affected_path.value, "peer-a@nic");
    adapter.releaseUnposted(deferred);
    task.is_finished = true;
    EXPECT_TRUE(transports.freeBatchID(batch_id).ok());
}

TEST(ClassicRdmaCongestionControlTest,
     InternedPathHistorySurvivesMetadataRefreshAndRouteChange) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    ClassicRdmaCongestionControl adapter(config);
    std::string local_name = "local";
    MultiTransport transports(nullptr, local_name);
    const auto batch_id = transports.allocateBatchID(1);
    auto &task = Transport::toBatchDesc(batch_id).task_list.emplace_back();
    task.batch_id = batch_id;
    task.slice_count = 1;
    auto *slice = new Transport::Slice{};
    slice->task = &task;
    slice->length = 64;
    task.slice_list.push_back(slice);
    std::string original_path;
    {
        TransferMetadata::SegmentDesc desc;
        desc.name = "history-peer";
        desc.devices.push_back({"history-nic", 1, "", ""});
        desc.rebuildInternedNicPaths();
        slice->interned_peer_nic_path = &desc.internedNicPath(0);
        original_path = *slice->interned_peer_nic_path;
        adapter.prepare(slice, original_path);
        adapter.observeUnavailable(slice, original_path,
                                   TaskCongestionFailureScope::kRoute, 1000);
        for (size_t i = 0; i < 256; ++i) {
            desc.name = "refreshed-peer-" + std::to_string(i);
            ++desc.metadata_version;
            desc.rebuildInternedNicPaths();
        }
        slice->interned_peer_nic_path = &desc.internedNicPath(0);
        adapter.prepare(slice, *slice->interned_peer_nic_path);
    }
    TaskCongestionDetail detail;
    ASSERT_TRUE(transports.getTaskCongestionDetail(batch_id, 0, detail).ok());
    ASSERT_TRUE(detail.affected_path.observed);
    EXPECT_EQ(detail.affected_path.value, original_path);
    EXPECT_FALSE(detail.resolved);
    {
        const std::string fallback_path(256, 'f');
        ASSERT_NE(*slice->interned_peer_nic_path, fallback_path);
        adapter.observeUnavailable(slice, fallback_path,
                                   TaskCongestionFailureScope::kDevice,
                                   std::nullopt);
    }
    ASSERT_TRUE(transports.getTaskCongestionDetail(batch_id, 0, detail).ok());
    ASSERT_TRUE(detail.affected_path.observed);
    EXPECT_EQ(detail.affected_path.value, std::string(256, 'f'));
    EXPECT_FALSE(detail.retry_after_ns.observed);
    EXPECT_EQ(detail.failure_scope.value, TaskCongestionFailureScope::kDevice);
    task.is_finished = true;
    EXPECT_TRUE(transports.freeBatchID(batch_id).ok());
}

TEST(ClassicRdmaCongestionControlTest,
     FreedBatchObservationDoesNotFollowCachedSlice) {
    struct SliceCacheAccess : Transport {
        using Transport::getSliceCache;
    };
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    ClassicRdmaCongestionControl adapter(config);
    std::string local_name = "local";
    MultiTransport transports(nullptr, local_name);
    auto &cache = SliceCacheAccess::getSliceCache();
    std::vector<Transport::Slice *> held;
    auto *slice = cache.allocate();
    while (slice->from_cache) {
        held.push_back(slice);
        slice = cache.allocate();
    }
    const auto old_batch = transports.allocateBatchID(1);
    auto &old_task = Transport::toBatchDesc(old_batch).task_list.emplace_back();
    old_task.batch_id = old_batch;
    old_task.slice_count = 1;
    old_task.slice_list.push_back(slice);
    slice->task = &old_task;
    slice->length = 64;
    adapter.prepare(slice, "old-peer@nic");
    adapter.observeUnavailable(slice, "old-peer@nic",
                               TaskCongestionFailureScope::kRoute, 1000);
    TaskCongestionDetail detail;
    ASSERT_TRUE(transports.getTaskCongestionDetail(old_batch, 0, detail).ok());
    ASSERT_EQ(detail.state, TaskCongestionState::kLongUnavailable);
    old_task.is_finished = true;
    ASSERT_TRUE(transports.freeBatchID(old_batch).ok());

    auto *recycled = cache.allocate();
    ASSERT_EQ(recycled, slice);
    const auto new_batch = transports.allocateBatchID(1);
    auto &new_task = Transport::toBatchDesc(new_batch).task_list.emplace_back();
    new_task.batch_id = new_batch;
    new_task.slice_count = 1;
    new_task.slice_list.push_back(recycled);
    new_task.rdma_congestion_control_mode = {
        true, TaskCongestionControllerMode::kEnforce};
    recycled->task = &new_task;
    adapter.prepare(recycled, "new-peer@nic");
    TaskCongestionState state;
    ASSERT_TRUE(transports.getTaskCongestionState(new_batch, 0, state).ok());
    EXPECT_EQ(state, TaskCongestionState::kNormal);
    ASSERT_TRUE(transports.getTaskCongestionDetail(new_batch, 0, detail).ok());
    EXPECT_EQ(detail.state, state);
    EXPECT_FALSE(detail.reason.observed);
    EXPECT_FALSE(detail.affected_path.observed);
    EXPECT_FALSE(detail.observed_at_ns.observed);
    EXPECT_FALSE(detail.retry_after_ns.observed);
    EXPECT_FALSE(detail.resolved);
    new_task.is_finished = true;
    EXPECT_TRUE(transports.freeBatchID(new_batch).ok());
    for (auto *held_slice : held) cache.deallocate(held_slice);
}

TEST(ClassicRdmaCongestionControlTest,
     StalePermitGenerationCannotReportReceiverPressure) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    ClassicRdmaCongestionControl adapter(config);
    std::string local_name = "local";
    MultiTransport transports(nullptr, local_name);
    const auto batch_id = transports.allocateBatchID(1);
    auto &task = Transport::toBatchDesc(batch_id).task_list.emplace_back();
    task.batch_id = batch_id;
    task.rdma_congestion_control_mode = {
        true, TaskCongestionControllerMode::kEnforce};
    auto *slice = new Transport::Slice{};
    slice->task = &task;
    slice->length = 64;
    task.slice_list.push_back(slice);
    task.slice_count = 1;
    auto *endpoint = reinterpret_cast<RdmaEndPoint *>(uintptr_t{1});
    adapter.prepare(slice, "peer@nic");
    adapter.bindEndpoint(slice, endpoint);
    std::vector<Transport::Slice *> queued{slice};
    ASSERT_EQ(adapter.gate(queued), 1u);
    adapter.resetDevice();
    adapter.complete(slice, IBV_WC_RNR_RETRY_EXC_ERR);

    TaskCongestionDetail detail;
    ASSERT_TRUE(transports.getTaskCongestionDetail(batch_id, 0, detail).ok());
    EXPECT_EQ(detail.state, TaskCongestionState::kNormal);
    EXPECT_FALSE(detail.reason.observed);
    task.is_finished = true;
    EXPECT_TRUE(transports.freeBatchID(batch_id).ok());
}

TEST(ClassicRdmaCongestionControlTest,
     LocalWorkerFailoverResolvesOldUnavailablePath) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    ClassicRdmaCongestionControl failed_adapter(config),
        healthy_adapter(config);
    std::string local_name = "local";
    MultiTransport transports(nullptr, local_name);
    const auto batch_id = transports.allocateBatchID(1);
    auto &task = Transport::toBatchDesc(batch_id).task_list.emplace_back();
    task.batch_id = batch_id;
    task.rdma_congestion_control_mode = {
        true, TaskCongestionControllerMode::kEnforce};
    auto *slice = new Transport::Slice{};
    slice->task = &task;
    slice->length = 64;
    task.slice_list.push_back(slice);
    task.slice_count = 1;
    const std::string path = "peer@nic";
    failed_adapter.prepare(slice, path);
    failed_adapter.observeUnavailable(slice, path,
                                      TaskCongestionFailureScope::kDevice);
    std::vector<Transport::Slice *> queued{slice};
    TaskCongestionState state;
    ASSERT_TRUE(transports.getTaskCongestionState(batch_id, 0, state).ok());
    ASSERT_EQ(state, TaskCongestionState::kLongUnavailable);

    healthy_adapter.prepare(slice, path);
    TaskCongestionDetail detail;
    ASSERT_TRUE(transports.getTaskCongestionDetail(batch_id, 0, detail).ok());
    EXPECT_EQ(detail.state, TaskCongestionState::kLongUnavailable);
    queued = {slice};
    EXPECT_EQ(healthy_adapter.gate(queued), 1u);
    ASSERT_TRUE(transports.getTaskCongestionDetail(batch_id, 0, detail).ok());
    EXPECT_EQ(detail.state, TaskCongestionState::kNormal);
    EXPECT_TRUE(detail.resolved);
    healthy_adapter.releaseUnposted(slice);
    task.is_finished = true;
    EXPECT_TRUE(transports.freeBatchID(batch_id).ok());
}

TEST(ClassicRdmaCongestionControlTest,
     UnavailableReplacementDoesNotClearUnavailableTask) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    ClassicRdmaCongestionControl first_adapter(config),
        replacement_adapter(config);
    std::string local_name = "local";
    MultiTransport transports(nullptr, local_name);
    const auto batch_id = transports.allocateBatchID(1);
    auto &task = Transport::toBatchDesc(batch_id).task_list.emplace_back();
    task.batch_id = batch_id;
    task.rdma_congestion_control_mode = {
        true, TaskCongestionControllerMode::kEnforce};
    auto *slice = new Transport::Slice{};
    slice->task = &task;
    slice->length = 64;
    task.slice_list.push_back(slice);
    task.slice_count = 1;
    const std::string path = "peer@nic";
    first_adapter.prepare(slice, path);
    first_adapter.observeUnavailable(slice, path,
                                     TaskCongestionFailureScope::kRoute);
    std::vector<Transport::Slice *> queued{slice};
    TaskCongestionState state;
    ASSERT_TRUE(transports.getTaskCongestionState(batch_id, 0, state).ok());
    ASSERT_EQ(state, TaskCongestionState::kLongUnavailable);

    Transport::Slice seed{};
    replacement_adapter.prepare(&seed, path);
    replacement_adapter.observeUnavailable(slice, path,
                                           TaskCongestionFailureScope::kRoute);
    replacement_adapter.prepare(slice, path);
    ASSERT_TRUE(transports.getTaskCongestionState(batch_id, 0, state).ok());
    EXPECT_EQ(state, TaskCongestionState::kLongUnavailable);

    queued = {slice};
    replacement_adapter.observeUnavailable(slice, path,
                                           TaskCongestionFailureScope::kRoute);
    ASSERT_TRUE(transports.getTaskCongestionState(batch_id, 0, state).ok());
    EXPECT_EQ(state, TaskCongestionState::kLongUnavailable);
    task.is_finished = true;
    EXPECT_TRUE(transports.freeBatchID(batch_id).ok());
}

TEST(ClassicRdmaCongestionControlTest,
     DeferredGateDoesNotReadGrowingTaskSliceVector) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    config.min_window_bytes = 64;
    config.max_window_bytes = 64;
    ClassicRdmaCongestionControl adapter(config);
    std::string local_name = "local";
    MultiTransport transports(nullptr, local_name);
    const auto batch_id = transports.allocateBatchID(1);
    auto &task = Transport::toBatchDesc(batch_id).task_list.emplace_back();
    task.batch_id = batch_id;
    auto *deferred = new Transport::Slice{};
    deferred->task = &task;
    deferred->length = 64;
    task.slice_list.push_back(deferred);
    task.slice_count = 1;
    Transport::Slice first{};
    first.length = 64;
    adapter.prepare(&first, "peer@nic");
    adapter.prepare(deferred, "peer@nic");
    std::vector<Transport::Slice *> first_queue{&first};
    ASSERT_EQ(adapter.gate(first_queue), 1u);

    std::atomic<bool> writing{false};
    std::atomic<bool> done{false};
    std::thread writer([&] {
        writing.store(true, std::memory_order_release);
        for (size_t i = 0; i < 10000; ++i) {
            auto *slice = new Transport::Slice{};
            slice->task = &task;
            slice->rdma_congestion_control_slice_ordinal =
                task.slice_list.size();
            task.slice_list.push_back(slice);
            __atomic_add_fetch(&task.slice_count, 1, __ATOMIC_RELEASE);
        }
        done.store(true, std::memory_order_release);
    });
    while (!writing.load(std::memory_order_acquire)) std::this_thread::yield();
    std::vector<Transport::Slice *> deferred_queue{deferred};
    do {
        adapter.gate(deferred_queue);
    } while (!done.load(std::memory_order_acquire));
    writer.join();
    TaskCongestionState state;
    ASSERT_TRUE(transports.getTaskCongestionState(batch_id, 0, state).ok());
    EXPECT_EQ(state, TaskCongestionState::kCongested);
    adapter.releaseUnposted(&first);
    task.is_finished = true;
    EXPECT_TRUE(transports.freeBatchID(batch_id).ok());
}

}  // namespace
}  // namespace mooncake

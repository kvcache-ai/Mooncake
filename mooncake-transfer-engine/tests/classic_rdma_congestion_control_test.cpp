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
#include "transport/rdma_transport/worker_pool.h"

namespace mooncake {
namespace {

TEST(ClassicRdmaCongestionControlTest, RuntimeOffLeavesQueueUnchanged) {
    ClassicRdmaCongestionControl adapter(adaptive_congestion_control::Config{
        .mode = adaptive_congestion_control::Mode::kOff});
    Transport::Slice slice{};
    slice.length = 64;
    std::vector<Transport::Slice *> queued{&slice};
    std::vector<Transport::Slice *> avoided;

    EXPECT_EQ(adapter.gate(queued, avoided), 1u);

    EXPECT_TRUE(avoided.empty());
    EXPECT_EQ(queued, std::vector<Transport::Slice *>({&slice}));
}

TEST(ClassicRdmaCongestionControlTest,
     DeferredLogicalTaskIsQueryableWithoutPollingTransport) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    config.min_window_bytes = 64;
    config.max_window_bytes = 64;
    ClassicRdmaCongestionControl adapter(config);
    std::string local_name = "local";
    MultiTransport transports(nullptr, local_name);
    const auto batch_id = transports.allocateBatchID(1);
    auto& batch = Transport::toBatchDesc(batch_id);
    auto& task = batch.task_list.emplace_back();
    task.batch_id = batch_id;
    auto* deferred = new Transport::Slice{};
    deferred->length = 64;
    deferred->task = &task;
    task.slice_list.push_back(deferred);
    task.slice_count = 1;
    Transport::Slice first{};
    first.length = 64;
    std::vector<Transport::Slice*> first_queue{&first};
    std::vector<Transport::Slice*> target_queue{deferred};
    std::vector<Transport::Slice*> avoided;
    adapter.prepare(&first, "peer@nic");
    adapter.prepare(deferred, "peer@nic");
    ASSERT_EQ(adapter.gate(first_queue, avoided), 1u);
    ASSERT_EQ(adapter.gate(target_queue, avoided), 0u);

    TaskCongestionState state = TaskCongestionState::kUnknown;
    ASSERT_TRUE(transports.getTaskCongestionState(batch_id, 0, state).ok());
    EXPECT_EQ(state, TaskCongestionState::kCongested);
    TaskCongestionDetail detail;
    ASSERT_TRUE(transports.getTaskCongestionDetail(batch_id, 0, detail).ok());
    EXPECT_TRUE(detail.reason.observed);
    EXPECT_EQ(detail.reason.value, TaskCongestionReason::kByteWindow);

    adapter.releaseUnposted(&first);
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
    auto& batch = Transport::toBatchDesc(batch_id);
    auto& task = batch.task_list.emplace_back();
    task.batch_id = batch_id;
    auto* slice = new Transport::Slice{};
    slice->length = 64;
    slice->task = &task;
    task.slice_list.push_back(slice);
    task.slice_count = 1;
    auto* endpoint = reinterpret_cast<RdmaEndPoint*>(uintptr_t{1});
    adapter.prepare(slice, "peer@nic");
    adapter.bindEndpoint(slice, endpoint);
    std::vector<Transport::Slice*> queued{slice}, avoided;
    ASSERT_EQ(adapter.gate(queued, avoided), 1u);
    adapter.complete(slice, IBV_WC_RNR_RETRY_EXC_ERR);

    TaskCongestionDetail detail;
    ASSERT_TRUE(transports.getTaskCongestionDetail(batch_id, 0, detail).ok());
    EXPECT_EQ(detail.state, TaskCongestionState::kCongested);
    ASSERT_TRUE(detail.reason.observed);
    EXPECT_EQ(detail.reason.value,
              TaskCongestionReason::kReceiverPressure);
    ASSERT_TRUE(detail.failure_scope.observed);
    EXPECT_EQ(detail.failure_scope.value,
              TaskCongestionFailureScope::kQp);

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
    auto& task = Transport::toBatchDesc(batch_id).task_list.emplace_back();
    task.batch_id = batch_id;
    task.rdma_congestion_control_mode = {true, TaskCongestionControllerMode::kEnforce};
    auto* slice = new Transport::Slice{};
    slice->length = 64;
    slice->task = &task;
    task.slice_list.push_back(slice);
    task.slice_count = 1;
    auto* endpoint = reinterpret_cast<RdmaEndPoint*>(uintptr_t{1});
    adapter.prepare(slice, "peer@nic");
    adapter.bindEndpoint(slice, endpoint);
    std::vector<Transport::Slice*> queued{slice}, avoided;
    ASSERT_EQ(adapter.gate(queued, avoided), 1u);
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
    auto& task = Transport::toBatchDesc(batch_id).task_list.emplace_back();
    task.batch_id = batch_id;
    task.rdma_congestion_control_mode = {true, TaskCongestionControllerMode::kEnforce};

    TaskCongestionState state = TaskCongestionState::kUnknown;
    ASSERT_TRUE(transports.getTaskCongestionState(batch_id, 0, state).ok());
    EXPECT_EQ(state, TaskCongestionState::kNormal);
    task.failed_slice_count = 1;
    ASSERT_TRUE(transports.getTaskCongestionState(batch_id, 0, state).ok());
    EXPECT_EQ(state, TaskCongestionState::kUnknown);
    task.failed_slice_count = 0;
    task.rdma_congestion_control_mode = {true, TaskCongestionControllerMode::kOff};
    ASSERT_TRUE(transports.getTaskCongestionState(batch_id, 0, state).ok());
    EXPECT_EQ(state, TaskCongestionState::kUnknown);

    task.is_finished = true;
    EXPECT_TRUE(transports.freeBatchID(batch_id).ok());
}

TEST(ClassicRdmaCongestionControlTest,
     QuarantineAvoidResolvesAfterProbeAdmission) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    config.hard_error_threshold = 1;
    config.cooldown_ns = 1;
    ClassicRdmaCongestionControl adapter(config);
    std::string local_name = "local";
    MultiTransport transports(nullptr, local_name);
    const auto batch_id = transports.allocateBatchID(1);
    auto& task = Transport::toBatchDesc(batch_id).task_list.emplace_back();
    task.batch_id = batch_id;
    task.rdma_congestion_control_mode = {true, TaskCongestionControllerMode::kEnforce};
    auto* slice = new Transport::Slice{};
    slice->task = &task;
    slice->length = 64;
    task.slice_list.push_back(slice);
    task.slice_count = 1;
    const std::string path = "peer@nic";
    adapter.prepare(slice, path);
    adapter.recordAsyncEvent(IBV_EVENT_QP_FATAL, &path);
    adapter.tick(1);
    std::vector<Transport::Slice*> queued{slice}, avoided;
    ASSERT_EQ(adapter.gate(queued, avoided), 0u);

    TaskCongestionDetail detail;
    ASSERT_TRUE(transports.getTaskCongestionDetail(batch_id, 0, detail).ok());
    EXPECT_EQ(detail.state, TaskCongestionState::kLongUnavailable);
    ASSERT_TRUE(detail.reason.observed);
    EXPECT_EQ(detail.reason.value,
              TaskCongestionReason::kPathQuarantined);
    ASSERT_TRUE(detail.affected_path.observed);
    EXPECT_EQ(detail.affected_path.value, path);
    EXPECT_FALSE(detail.resolved);

    adapter.tick(2);
    queued = {slice};
    avoided.clear();
    ASSERT_EQ(adapter.gate(queued, avoided), 1u);
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
    config.hard_error_threshold = 1;
    config.cooldown_ns = 1;
    ClassicRdmaCongestionControl adapter(config);
    std::string local_name = "local";
    MultiTransport transports(nullptr, local_name);
    const auto batch_id = transports.allocateBatchID(1);
    auto& task = Transport::toBatchDesc(batch_id).task_list.emplace_back();
    task.batch_id = batch_id;
    task.rdma_congestion_control_mode = {true, TaskCongestionControllerMode::kEnforce};
    auto* deferred = new Transport::Slice{};
    deferred->task = &task;
    deferred->length = 64;
    deferred->rdma_congestion_control_slice_ordinal = 0;
    task.slice_list.push_back(deferred);
    auto* unavailable = new Transport::Slice{};
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
    adapter.recordAsyncEvent(IBV_EVENT_QP_FATAL, &unavailable_path);
    adapter.tick(1);
    std::vector<Transport::Slice*> first_queue{&first}, queued{deferred,
                                                                unavailable};
    std::vector<Transport::Slice*> avoided;
    ASSERT_EQ(adapter.gate(first_queue, avoided), 1u);
    ASSERT_EQ(adapter.gate(queued, avoided), 0u);

    TaskCongestionDetail detail;
    ASSERT_TRUE(transports.getTaskCongestionDetail(batch_id, 0, detail).ok());
    EXPECT_EQ(detail.state, TaskCongestionState::kLongUnavailable);
    ASSERT_TRUE(detail.slice_id.observed);
    EXPECT_EQ(detail.slice_id.value, 1u);
    adapter.tick(2);
    adapter.releaseUnposted(&first);
    queued = {unavailable};
    avoided.clear();
    ASSERT_EQ(adapter.gate(queued, avoided), 1u);
    ASSERT_TRUE(transports.getTaskCongestionDetail(batch_id, 0, detail).ok());
    EXPECT_EQ(detail.state, TaskCongestionState::kCongested);
    EXPECT_EQ(detail.slice_id.value, 0u);
    adapter.releaseUnposted(unavailable);
    queued = {deferred};
    ASSERT_EQ(adapter.gate(queued, avoided), 1u);
    ASSERT_TRUE(transports.getTaskCongestionDetail(batch_id, 0, detail).ok());
    EXPECT_EQ(detail.state, TaskCongestionState::kNormal);
    adapter.releaseUnposted(deferred);
    task.is_finished = true;
    EXPECT_TRUE(transports.freeBatchID(batch_id).ok());
}

TEST(ClassicRdmaCongestionControlTest,
     StalePermitGenerationCannotReportReceiverPressure) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    ClassicRdmaCongestionControl adapter(config);
    std::string local_name = "local";
    MultiTransport transports(nullptr, local_name);
    const auto batch_id = transports.allocateBatchID(1);
    auto& task = Transport::toBatchDesc(batch_id).task_list.emplace_back();
    task.batch_id = batch_id;
    task.rdma_congestion_control_mode = {true, TaskCongestionControllerMode::kEnforce};
    auto* slice = new Transport::Slice{};
    slice->task = &task;
    slice->length = 64;
    task.slice_list.push_back(slice);
    task.slice_count = 1;
    auto* endpoint = reinterpret_cast<RdmaEndPoint*>(uintptr_t{1});
    adapter.prepare(slice, "peer@nic");
    adapter.bindEndpoint(slice, endpoint);
    std::vector<Transport::Slice*> queued{slice}, avoided;
    ASSERT_EQ(adapter.gate(queued, avoided), 1u);
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
    config.hard_error_threshold = 1;
    ClassicRdmaCongestionControl failed_adapter(config), healthy_adapter(config);
    std::string local_name = "local";
    MultiTransport transports(nullptr, local_name);
    const auto batch_id = transports.allocateBatchID(1);
    auto& task = Transport::toBatchDesc(batch_id).task_list.emplace_back();
    task.batch_id = batch_id;
    task.rdma_congestion_control_mode = {true, TaskCongestionControllerMode::kEnforce};
    auto* slice = new Transport::Slice{};
    slice->task = &task;
    slice->length = 64;
    task.slice_list.push_back(slice);
    task.slice_count = 1;
    const std::string path = "peer@nic";
    failed_adapter.prepare(slice, path);
    failed_adapter.recordAsyncEvent(IBV_EVENT_QP_FATAL, &path);
    failed_adapter.tick(1);
    std::vector<Transport::Slice*> queued{slice}, avoided;
    ASSERT_EQ(failed_adapter.gate(queued, avoided), 0u);
    TaskCongestionState state;
    ASSERT_TRUE(transports.getTaskCongestionState(batch_id, 0, state).ok());
    ASSERT_EQ(state, TaskCongestionState::kLongUnavailable);

    healthy_adapter.prepare(slice, path);
    TaskCongestionDetail detail;
    ASSERT_TRUE(transports.getTaskCongestionDetail(batch_id, 0, detail).ok());
    EXPECT_EQ(detail.state, TaskCongestionState::kLongUnavailable);
    queued = {slice};
    avoided.clear();
    EXPECT_EQ(healthy_adapter.gate(queued, avoided), 1u);
    ASSERT_TRUE(transports.getTaskCongestionDetail(batch_id, 0, detail).ok());
    EXPECT_EQ(detail.state, TaskCongestionState::kNormal);
    EXPECT_TRUE(detail.resolved);
    healthy_adapter.releaseUnposted(slice);
    task.is_finished = true;
    EXPECT_TRUE(transports.freeBatchID(batch_id).ok());
}

TEST(ClassicRdmaCongestionControlTest,
     QuarantinedReplacementDoesNotClearUnavailableTask) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    config.hard_error_threshold = 1;
    ClassicRdmaCongestionControl first_adapter(config), replacement_adapter(config);
    std::string local_name = "local";
    MultiTransport transports(nullptr, local_name);
    const auto batch_id = transports.allocateBatchID(1);
    auto& task = Transport::toBatchDesc(batch_id).task_list.emplace_back();
    task.batch_id = batch_id;
    task.rdma_congestion_control_mode = {true, TaskCongestionControllerMode::kEnforce};
    auto* slice = new Transport::Slice{};
    slice->task = &task;
    slice->length = 64;
    task.slice_list.push_back(slice);
    task.slice_count = 1;
    const std::string path = "peer@nic";
    first_adapter.prepare(slice, path);
    first_adapter.recordAsyncEvent(IBV_EVENT_QP_FATAL, &path);
    first_adapter.tick(1);
    std::vector<Transport::Slice*> queued{slice}, avoided;
    ASSERT_EQ(first_adapter.gate(queued, avoided), 0u);
    TaskCongestionState state;
    ASSERT_TRUE(transports.getTaskCongestionState(batch_id, 0, state).ok());
    ASSERT_EQ(state, TaskCongestionState::kLongUnavailable);

    Transport::Slice seed{};
    replacement_adapter.prepare(&seed, path);
    replacement_adapter.recordAsyncEvent(IBV_EVENT_QP_FATAL, &path);
    replacement_adapter.tick(1);
    replacement_adapter.prepare(slice, path);
    ASSERT_TRUE(transports.getTaskCongestionState(batch_id, 0, state).ok());
    EXPECT_EQ(state, TaskCongestionState::kLongUnavailable);

    queued = {slice};
    avoided.clear();
    EXPECT_EQ(replacement_adapter.gate(queued, avoided), 0u);
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
    auto& task = Transport::toBatchDesc(batch_id).task_list.emplace_back();
    task.batch_id = batch_id;
    auto* deferred = new Transport::Slice{};
    deferred->task = &task;
    deferred->length = 64;
    task.slice_list.push_back(deferred);
    task.slice_count = 1;
    Transport::Slice first{};
    first.length = 64;
    adapter.prepare(&first, "peer@nic");
    adapter.prepare(deferred, "peer@nic");
    std::vector<Transport::Slice*> first_queue{&first}, avoided;
    ASSERT_EQ(adapter.gate(first_queue, avoided), 1u);

    std::atomic<bool> writing{false};
    std::atomic<bool> done{false};
    std::thread writer([&] {
        writing.store(true, std::memory_order_release);
        for (size_t i = 0; i < 10000; ++i) {
            auto* slice = new Transport::Slice{};
            slice->task = &task;
            slice->rdma_congestion_control_slice_ordinal = task.slice_list.size();
            task.slice_list.push_back(slice);
            __atomic_add_fetch(&task.slice_count, 1, __ATOMIC_RELEASE);
        }
        done.store(true, std::memory_order_release);
    });
    while (!writing.load(std::memory_order_acquire)) std::this_thread::yield();
    std::vector<Transport::Slice*> deferred_queue{deferred};
    do {
        adapter.gate(deferred_queue, avoided);
    } while (!done.load(std::memory_order_acquire));
    writer.join();
    TaskCongestionState state;
    ASSERT_TRUE(transports.getTaskCongestionState(batch_id, 0, state).ok());
    EXPECT_EQ(state, TaskCongestionState::kCongested);
    adapter.releaseUnposted(&first);
    task.is_finished = true;
    EXPECT_TRUE(transports.freeBatchID(batch_id).ok());
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
    std::vector<Transport::Slice *> avoided;

    adapter.prepare(&first, "peer@nic");
    adapter.prepare(&deferred, "peer@nic");
    const size_t first_admitted = adapter.gate(first_queue, avoided);

    ASSERT_EQ(first_admitted, 1u);
    adapter.gate(second_queue, avoided);
    EXPECT_TRUE(avoided.empty());
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
    std::vector<Transport::Slice *> avoided;

    adapter.prepare(queued, "peer@nic");

    EXPECT_EQ(adapter.gate(queued, avoided), 1u);
    EXPECT_EQ(queued.size(), 2u);
    EXPECT_TRUE(avoided.empty());
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
    std::vector<Transport::Slice *> avoided;
    adapter.prepare(&slice, "peer@nic");
    ASSERT_EQ(adapter.gate(queued, avoided), 1u);

    adapter.complete(&slice, IBV_WC_RETRY_EXC_ERR);
    queued = {&slice};
    EXPECT_EQ(adapter.gate(queued, avoided), 1u);
}

TEST(ClassicRdmaCongestionControlTest, LocalHandoffRebindsRouteOwner) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    config.hard_error_threshold = 1;
    const std::string path = "peer@nic";
    for (bool batch : {false, true}) {
        ClassicRdmaCongestionControl failed_adapter(config),
            healthy_adapter(config);
        Transport::Slice slice{};
        slice.length = 64;
        failed_adapter.prepare(&slice, path);
        failed_adapter.recordAsyncEvent(IBV_EVENT_QP_FATAL, &path);
        failed_adapter.tick(1);
        std::vector<Transport::Slice *> queued{&slice}, avoided;
        // Local failover retains the remote NIC path on the same slice.
        if (batch) {
            healthy_adapter.prepare(queued, path);
        } else {
            healthy_adapter.prepare(&slice, path);
        }
        EXPECT_EQ(healthy_adapter.gate(queued, avoided), 1u);
        EXPECT_TRUE(avoided.empty());
        healthy_adapter.releaseUnposted(&slice);
    }
}

TEST(ClassicRdmaCongestionControlTest, CurrentEndpointFailureAvoidsRoute) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    config.min_window_bytes = 64;
    config.max_window_bytes = 64;
    config.hard_error_threshold = 1;
    ClassicRdmaCongestionControl adapter(config);
    Transport::Slice failed{};
    failed.length = 64;
    Transport::Slice next{};
    next.length = 64;
    auto *endpoint = reinterpret_cast<RdmaEndPoint *>(uintptr_t{1});
    std::vector<Transport::Slice *> queued{&failed};
    std::vector<Transport::Slice *> avoided;

    adapter.prepare(&failed, "peer@nic");
    adapter.bindEndpoint(&failed, endpoint);
    ASSERT_EQ(adapter.gate(queued, avoided), 1u);
    adapter.complete(&failed, IBV_WC_FATAL_ERR);
    adapter.tick(1);

    adapter.prepare(&next, "peer@nic");
    queued = {&next};
    EXPECT_EQ(adapter.gate(queued, avoided), 0u);
    ASSERT_EQ(avoided.size(), 1u);
    EXPECT_EQ(avoided.front(), &next);
}

TEST(ClassicRdmaCongestionControlTest, StaleEndpointFailureIsIgnored) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    config.min_window_bytes = 64;
    config.max_window_bytes = 64;
    config.hard_error_threshold = 1;
    ClassicRdmaCongestionControl adapter(config);
    Transport::Slice stale{};
    stale.length = 64;
    Transport::Slice next{};
    next.length = 64;
    auto *old_endpoint = reinterpret_cast<RdmaEndPoint *>(uintptr_t{1});
    auto *new_endpoint = reinterpret_cast<RdmaEndPoint *>(uintptr_t{2});
    std::vector<Transport::Slice *> queued{&stale};
    std::vector<Transport::Slice *> avoided;

    adapter.prepare(&stale, "peer@nic");
    adapter.bindEndpoint(&stale, old_endpoint);
    ASSERT_EQ(adapter.gate(queued, avoided), 1u);
    adapter.bindEndpoint(&stale, new_endpoint);
    adapter.complete(&stale, IBV_WC_FATAL_ERR);
    adapter.tick(1);

    adapter.prepare(&next, "peer@nic");
    queued = {&next};
    EXPECT_EQ(adapter.gate(queued, avoided), 1u);
    EXPECT_TRUE(avoided.empty());
}

TEST(ClassicRdmaCongestionControlTest,
     ReusedEndpointAddressDoesNotReviveOldFailure) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    config.min_window_bytes = 64;
    config.max_window_bytes = 64;
    config.hard_error_threshold = 1;
    ClassicRdmaCongestionControl adapter(config);
    Transport::Slice stale{};
    stale.length = 64;
    Transport::Slice replacement{};
    replacement.length = 64;
    Transport::Slice next{};
    next.length = 64;
    auto *reused_endpoint = reinterpret_cast<RdmaEndPoint *>(uintptr_t{1});
    std::vector<Transport::Slice *> queued{&stale};
    std::vector<Transport::Slice *> avoided;

    adapter.prepare(&stale, "peer@nic");
    adapter.bindEndpoint(&stale, reused_endpoint);
    ASSERT_EQ(adapter.gate(queued, avoided), 1u);
    adapter.retireEndpoint("peer@nic", reused_endpoint);
    adapter.prepare(&replacement, "peer@nic");
    adapter.bindEndpoint(&replacement, reused_endpoint);

    adapter.complete(&stale, IBV_WC_FATAL_ERR);
    adapter.tick(1);

    adapter.prepare(&next, "peer@nic");
    queued = {&next};
    EXPECT_EQ(adapter.gate(queued, avoided), 1u);
    EXPECT_TRUE(avoided.empty());
}

TEST(ClassicRdmaCongestionControlTest, QpEventAccumulatesOnItsRoute) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    config.hard_error_threshold = 1;
    ClassicRdmaCongestionControl adapter(config);
    Transport::Slice slice{};
    slice.length = 64;
    std::vector<Transport::Slice *> queued{&slice};
    std::vector<Transport::Slice *> avoided;
    const std::string peer_path = "peer@nic";

    adapter.prepare(&slice, peer_path);
    adapter.recordAsyncEvent(IBV_EVENT_QP_FATAL, &peer_path);
    adapter.tick(1);

    EXPECT_EQ(adapter.gate(queued, avoided), 0u);
    ASSERT_EQ(avoided.size(), 1u);
    EXPECT_EQ(avoided.front(), &slice);
}

TEST(ClassicRdmaCongestionControlTest, LargeSliceRecoversAfterQuarantine) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    config.hard_error_threshold = 1;
    config.cooldown_ns = 1;
    ClassicRdmaCongestionControl adapter(config);
    Transport::Slice slice{};
    slice.length = 128ULL << 10;
    const std::string path = "peer@nic";
    adapter.prepare(&slice, path);
    adapter.recordAsyncEvent(IBV_EVENT_QP_FATAL, &path);
    adapter.tick(1);
    adapter.tick(2);
    std::vector<Transport::Slice *> queued{&slice}, avoided;
    ASSERT_EQ(adapter.gate(queued, avoided), 1u);
    adapter.complete(&slice, IBV_WC_SUCCESS);
    adapter.tick(3);
    EXPECT_EQ(adapter.gate(queued, avoided), 1u);
    adapter.releaseUnposted(&slice);
    EXPECT_TRUE(avoided.empty());
}

TEST(ClassicRdmaCongestionControlTest, CqEventQuarantinesAllRoutes) {
    adaptive_congestion_control::Config config;
    config.mode = adaptive_congestion_control::Mode::kEnforce;
    ClassicRdmaCongestionControl adapter(config);
    Transport::Slice first{};
    first.length = 64;
    Transport::Slice second{};
    second.length = 64;
    std::vector<Transport::Slice *> first_queue{&first};
    std::vector<Transport::Slice *> second_queue{&second};
    std::vector<Transport::Slice *> avoided;

    adapter.prepare(&first, "peer-a@nic");
    adapter.prepare(&second, "peer-b@nic");
    adapter.recordAsyncEvent(IBV_EVENT_CQ_ERR);
    adapter.tick(1);

    EXPECT_EQ(adapter.gate(first_queue, avoided), 0u);
    EXPECT_EQ(adapter.gate(second_queue, avoided), 0u);
    EXPECT_EQ(avoided.size(), 2u);
}

}  // namespace
}  // namespace mooncake

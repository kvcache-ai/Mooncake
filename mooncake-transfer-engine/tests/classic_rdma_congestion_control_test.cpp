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
    std::vector<Transport::Slice *> avoided;

    EXPECT_EQ(adapter.gate(queued, avoided), 1u);

    EXPECT_TRUE(avoided.empty());
    EXPECT_EQ(queued, std::vector<Transport::Slice *>({&slice}));
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

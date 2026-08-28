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
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "tent/common/config.h"
#include "tent/runtime/control_plane.h"
#include "tent/runtime/transfer_engine_impl.h"
#include "tent/runtime/transport.h"

namespace mooncake::tent {
namespace {

class FakeNotificationTransport : public Transport {
   public:
    explicit FakeNotificationTransport(Status send_result)
        : send_result_(std::move(send_result)) {}

    bool supportNotification() const override { return true; }

    Status sendNotification(SegmentID, const Notification&) override {
        send_calls.fetch_add(1, std::memory_order_relaxed);
        return send_result_;
    }

    Status receiveNotification(
        std::vector<Notification>& notifications) override {
        receive_calls.fetch_add(1, std::memory_order_relaxed);
        notifications.clear();
        notifications.swap(queued_);
        return Status::OK();
    }

    void queue(Notification notification) {
        queued_.push_back(std::move(notification));
    }

    std::atomic<int> send_calls{0};
    std::atomic<int> receive_calls{0};

   private:
    Status send_result_;
    std::vector<Notification> queued_;
};

std::shared_ptr<Config> MakeNotificationConfig() {
    auto config = std::make_shared<Config>();
    config->set("metadata_type", "p2p");
    config->set("metadata_servers", "");
    config->set("rpc_server_hostname", "127.0.0.1");
    config->set("rpc_server_port", "0");
    config->set("log_level", "warning");
    config->set("transports/tcp/enable", false);
    config->set("transports/shm/enable", false);
    config->set("transports/rdma/enable", false);
    config->set("transports/io_uring/enable", false);
    config->set("transports/nvlink/enable", false);
    config->set("transports/mnnvl/enable", false);
    config->set("transports/gds/enable", false);
    config->set("transports/ascend_direct/enable", false);
    return config;
}

TEST(NotificationRoutingTest, ReceiveAggregatesFallbackAndEveryTransport) {
    TransferEngineImpl engine(MakeNotificationConfig());
    ASSERT_TRUE(engine.available());

    auto empty_first =
        std::make_shared<FakeNotificationTransport>(Status::OK());
    auto later = std::make_shared<FakeNotificationTransport>(Status::OK());
    later->queue({"transport", "later"});
    engine.swapTransportForTest(RDMA, empty_first);
    engine.swapTransportForTest(TCP, later);

    ASSERT_TRUE(
        ControlClient::notify(engine.getSegmentName(), {"control", "queued"})
            .ok());

    std::vector<Notification> notifications;
    ASSERT_TRUE(engine.receiveNotification(notifications).ok());
    ASSERT_EQ(notifications.size(), 2U);
    EXPECT_EQ(notifications[0].name, "control");
    EXPECT_EQ(notifications[0].msg, "queued");
    EXPECT_EQ(notifications[1].name, "transport");
    EXPECT_EQ(notifications[1].msg, "later");
    EXPECT_EQ(empty_first->receive_calls.load(), 1);
    EXPECT_EQ(later->receive_calls.load(), 1);

    ASSERT_TRUE(engine.receiveNotification(notifications).ok());
    EXPECT_TRUE(notifications.empty());
}

TEST(NotificationRoutingTest, RdmaPrePostFailureFallsBackToControlTransport) {
    TransferEngineImpl engine(MakeNotificationConfig());
    ASSERT_TRUE(engine.available());

    auto rdma = std::make_shared<FakeNotificationTransport>(
        Status::InternalError("RDMA notification was not posted"));
    auto tcp = std::make_shared<FakeNotificationTransport>(Status::OK());
    engine.swapTransportForTest(RDMA, rdma);
    engine.swapTransportForTest(TCP, tcp);

    EXPECT_TRUE(
        engine.sendNotification(LOCAL_SEGMENT_ID, {"name", "msg"}).ok());
    EXPECT_EQ(rdma->send_calls.load(), 1);
    EXPECT_EQ(tcp->send_calls.load(), 1);
}

TEST(NotificationRoutingTest, AmbiguousRpcFailureDoesNotCrossFallback) {
    TransferEngineImpl engine(MakeNotificationConfig());
    ASSERT_TRUE(engine.available());

    auto rdma = std::make_shared<FakeNotificationTransport>(
        Status::InternalError("RDMA notification was not posted"));
    auto tcp = std::make_shared<FakeNotificationTransport>(
        Status::RpcServiceError("reply was lost"));
    auto hp_tcp = std::make_shared<FakeNotificationTransport>(Status::OK());
    engine.swapTransportForTest(RDMA, rdma);
    engine.swapTransportForTest(TCP, tcp);
    engine.swapTransportForTest(HP_TCP, hp_tcp);

    const Status result =
        engine.sendNotification(LOCAL_SEGMENT_ID, {"name", "msg"});
    EXPECT_TRUE(result.IsRpcServiceError()) << result.ToString();
    EXPECT_EQ(rdma->send_calls.load(), 1);
    EXPECT_EQ(tcp->send_calls.load(), 1);
    EXPECT_EQ(hp_tcp->send_calls.load(), 0);
}

TEST(NotificationRoutingTest, RdmaOnlyFailureUsesControlServiceFallback) {
    TransferEngineImpl receiver(MakeNotificationConfig());
    TransferEngineImpl sender(MakeNotificationConfig());
    ASSERT_TRUE(receiver.available());
    ASSERT_TRUE(sender.available());

    auto rdma = std::make_shared<FakeNotificationTransport>(
        Status::InternalError("RDMA notification was not posted"));
    sender.swapTransportForTest(RDMA, rdma);

    SegmentID target = 0;
    ASSERT_TRUE(sender.openSegment(target, receiver.getSegmentName()).ok());
    ASSERT_NE(target, LOCAL_SEGMENT_ID);

    const Status sent = sender.sendNotification(target, {"fallback", "rpc"});
    ASSERT_TRUE(sent.ok()) << sent.ToString();

    std::vector<Notification> notifications;
    ASSERT_TRUE(receiver.receiveNotification(notifications).ok());
    ASSERT_EQ(notifications.size(), 1U);
    EXPECT_EQ(notifications[0].name, "fallback");
    EXPECT_EQ(notifications[0].msg, "rpc");

    ASSERT_TRUE(receiver.receiveNotification(notifications).ok());
    EXPECT_TRUE(notifications.empty());
    EXPECT_TRUE(sender.closeSegment(target).ok());
}

}  // namespace
}  // namespace mooncake::tent

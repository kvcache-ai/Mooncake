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

// A notification the RDMA channel cannot carry - no endpoint could be
// brought up, the peer has no notify QP, or the QP was disabled after a
// fault - used to be dropped with an InternalError. These tests pin the
// fallback: the engine tries the next notification transport and, with none
// left, the control-plane RPC; receiveNotification() drains every transport
// so a notification that arrived over RPC is not stranded in a queue nobody
// polls. Stand-in transports keep this free of any NIC or metadata server.

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "tent/common/config.h"
#include "tent/runtime/transfer_engine_impl.h"

namespace mooncake {
namespace tent {

namespace {

std::shared_ptr<Config> makeConfig(bool rpc_fallback = true) {
    auto config = std::make_shared<Config>();
    config->set("metadata_type", "p2p");
    config->set("rpc_server_hostname", "127.0.0.1");
    config->set("rpc_server_port", "0");
    config->set("log_level", "warning");
    // No real transport: every notification transport in these tests is a
    // stand-in swapped in after construction, so the fallback order and the
    // RPC leg are exercised on their own.
    config->set("transports/tcp/enable", false);
    config->set("transports/shm/enable", false);
    config->set("transports/rdma/enable", false);
    config->set("transports/io_uring/enable", false);
    config->set("transports/nvlink/enable", false);
    config->set("transports/mnnvl/enable", false);
    config->set("transports/gds/enable", false);
    config->set("transports/ascend_direct/enable", false);
    config->set("transports/sunrise_link/enable", false);
    config->set("transports/mpcomm/enable", false);
    config->set("transports/tpu/enable", false);
    config->set("transports/ub/enable", false);
    config->set("enable_progress_worker", false);
    config->set("enable_runtime_queue", false);
    config->set("notification/rpc_fallback", rpc_fallback);
    return config;
}

// Stands in for a notification transport: sends answer with a fixed status
// and are recorded, receives hand out whatever the test queued.
class StubNotifyTransport : public Transport {
   public:
    StubNotifyTransport(const char* name, Status send_status)
        : name_(name), send_status_(send_status) {}

    Status install(std::string&, std::shared_ptr<ControlService>,
                   std::shared_ptr<Topology>,
                   std::shared_ptr<Config>) override {
        return Status::OK();
    }

    bool supportNotification() const override { return true; }

    Status sendNotification(SegmentID, const Notification& notifi) override {
        ++send_calls;
        sent.push_back(notifi);
        return send_status_;
    }

    // Mirrors TcpTransport::receiveNotification(): the argument is cleared
    // before the queue is swapped in. An engine that hands one vector to
    // every transport in turn loses everything gathered before this one.
    Status receiveNotification(std::vector<Notification>& out) override {
        out.clear();
        if (!recv_status.ok()) return recv_status;
        out.swap(queued);
        return Status::OK();
    }

    const char* getName() const override { return name_; }

    int send_calls = 0;
    std::vector<Notification> sent;
    std::vector<Notification> queued;
    Status recv_status = Status::OK();

   private:
    const char* name_;
    Status send_status_;
};

Notification makeNotification(const std::string& msg) {
    Notification notifi;
    notifi.name = "notification-fallback-test";
    notifi.msg = msg;
    return notifi;
}

// Any non-local segment id. The stand-ins never look at it, and the tests
// that use it stop before the RPC leg would need a real peer.
constexpr SegmentID kRemote = 0x1234;

std::shared_ptr<StubNotifyTransport> installStub(TransferEngineImpl& engine,
                                                 TransportType slot,
                                                 const char* name,
                                                 Status send_status) {
    auto stub = std::make_shared<StubNotifyTransport>(name, send_status);
    std::string segment_name = engine.getSegmentName();
    EXPECT_TRUE(stub->install(segment_name, nullptr, nullptr, nullptr).ok());
    engine.swapTransportForTest(slot, stub);
    return stub;
}

}  // namespace

// The RDMA transport (slot RDMA, tried first) reports its channel
// unavailable; the next notification transport carries the notification.
// Both codes RdmaTransport::sendNotification() uses for "nothing was posted"
// must be treated the same.
TEST(NotificationFallbackTest, FallsBackToNextTransportWhenChannelUnavailable) {
    TransferEngineImpl engine(makeConfig());
    ASSERT_TRUE(engine.available());

    const Status unavailable[] = {
        Status::RdmaError("RDMA notification channel unavailable"),
        Status::DeviceNotFound("RDMA notification endpoint unavailable"),
    };
    for (const auto& code : unavailable) {
        auto rdma = installStub(engine, RDMA, "<rdma-unavailable>", code);
        auto tcp = installStub(engine, TCP, "<tcp-ok>", Status::OK());

        const Status status =
            engine.sendNotification(kRemote, makeNotification("fallback"));
        EXPECT_TRUE(status.ok()) << status.ToString();
        EXPECT_EQ(rdma->send_calls, 1) << code.ToString();
        ASSERT_EQ(tcp->send_calls, 1) << code.ToString();
        EXPECT_EQ(tcp->sent[0].msg, "fallback");
    }
}

// A transport that rejected the notification, as opposed to one that could
// not carry it, is the caller's error to see: no other path is tried.
TEST(NotificationFallbackTest, DoesNotFallBackOnInvalidArgument) {
    TransferEngineImpl engine(makeConfig());
    ASSERT_TRUE(engine.available());

    auto rdma = installStub(engine, RDMA, "<rdma-rejects>",
                            Status::InvalidArgument("payload too large"));
    auto tcp = installStub(engine, TCP, "<tcp-ok>", Status::OK());

    const Status status =
        engine.sendNotification(kRemote, makeNotification("rejected"));
    EXPECT_TRUE(status.IsInvalidArgument()) << status.ToString();
    EXPECT_EQ(rdma->send_calls, 1);
    EXPECT_EQ(tcp->send_calls, 0);
}

// The RDMA transport reports RpcServiceError when the bootstrap RPC to the
// peer failed: its control plane is not answering, so neither the next
// transport nor the engine's own RPC leg is tried - both would only wait
// out another timeout against the same peer.
TEST(NotificationFallbackTest, ControlPlaneUnreachableIsReturnedAsIs) {
    TransferEngineImpl engine(makeConfig());
    ASSERT_TRUE(engine.available());

    auto rdma = installStub(engine, RDMA, "<rdma-bootstrap-rpc-failed>",
                            Status::RpcServiceError("peer control plane "
                                                    "unreachable"));
    auto tcp = installStub(engine, TCP, "<tcp-ok>", Status::OK());

    const Status status =
        engine.sendNotification(kRemote, makeNotification("unreachable"));
    EXPECT_TRUE(status.IsRpcServiceError()) << status.ToString();
    EXPECT_EQ(rdma->send_calls, 1);
    EXPECT_EQ(tcp->send_calls, 0);
}

// The production chain with TCP loaded is RDMA then TcpTransport, whose own
// failures are RPC errors, never "channel unavailable". Such an error from
// the last transport is returned as is; the engine's own RPC leg is not a
// second attempt at the same peer.
TEST(NotificationFallbackTest, ReturnsTheLastTransportsErrorInsteadOfRpc) {
    TransferEngineImpl engine(makeConfig());
    ASSERT_TRUE(engine.available());

    auto rdma = installStub(engine, RDMA, "<rdma-unavailable>",
                            Status::RdmaError("notify QP not connected"));
    auto tcp = installStub(engine, TCP, "<tcp-rpc-fails>",
                           Status::RpcServiceError("connection refused"));

    const Status status =
        engine.sendNotification(kRemote, makeNotification("tcp-failed"));
    EXPECT_TRUE(status.IsRpcServiceError()) << status.ToString();
    EXPECT_EQ(rdma->send_calls, 1);
    EXPECT_EQ(tcp->send_calls, 1);
}

// Only when every loaded notification transport reports its channel
// unavailable does the engine make the control-plane RPC itself. kRemote was
// never opened, so reaching that leg shows up as the segment lookup failing
// - which is how this test tells it apart from an early return.
TEST(NotificationFallbackTest, AllTransportsUnavailableReachesTheRpcLeg) {
    TransferEngineImpl engine(makeConfig());
    ASSERT_TRUE(engine.available());

    auto rdma = installStub(engine, RDMA, "<rdma-unavailable>",
                            Status::RdmaError("notify QP not connected"));
    auto tcp = installStub(engine, TCP, "<tcp-unavailable>",
                           Status::DeviceNotFound("no endpoint"));

    const Status status =
        engine.sendNotification(kRemote, makeNotification("rpc-leg"));
    EXPECT_TRUE(status.IsInvalidArgument()) << status.ToString();
    EXPECT_NE(status.message().find("segment handle"), std::string::npos)
        << status.ToString();
    EXPECT_EQ(rdma->send_calls, 1);
    EXPECT_EQ(tcp->send_calls, 1);
}

// SunriseLink advertises notification support but implements neither send
// nor receive, so both come back NotImplemented. It must be skipped on the
// send chain and ignored by the receive drain rather than stop the fallback
// or turn every empty poll into an error.
TEST(NotificationFallbackTest, SkipsTransportsThatDoNotImplementNotifications) {
    TransferEngineImpl engine(makeConfig());
    ASSERT_TRUE(engine.available());

    auto rdma = installStub(engine, RDMA, "<rdma-unavailable>",
                            Status::RdmaError("notify QP not connected"));
    // Any slot between RDMA and TCP does; the real SunriseLink slot is after
    // TCP, which would hide the send-chain part of this test.
    auto stubbed = installStub(engine, SHM, "<advertised-only>",
                               Status::NotImplemented("not implemented"));
    stubbed->recv_status = Status::NotImplemented("not implemented");
    auto tcp = installStub(engine, TCP, "<tcp-ok>", Status::OK());

    const Status status =
        engine.sendNotification(kRemote, makeNotification("skip"));
    EXPECT_TRUE(status.ok()) << status.ToString();
    EXPECT_EQ(rdma->send_calls, 1);
    EXPECT_EQ(stubbed->send_calls, 1);
    ASSERT_EQ(tcp->send_calls, 1);
    EXPECT_EQ(tcp->sent[0].msg, "skip");

    tcp->queued = {makeNotification("tcp-0")};
    std::vector<Notification> received;
    ASSERT_TRUE(engine.receiveNotification(received).ok());
    ASSERT_EQ(received.size(), 1u);
    EXPECT_EQ(received[0].msg, "tcp-0");

    // An empty poll stays a successful empty poll.
    EXPECT_TRUE(engine.receiveNotification(received).ok());
    EXPECT_TRUE(received.empty());
}

// notification/rpc_fallback=false restores the first-transport-only
// behavior: the unavailable channel's error is returned untouched.
TEST(NotificationFallbackTest, FallbackCanBeDisabledByConfig) {
    TransferEngineImpl engine(makeConfig(/*rpc_fallback=*/false));
    ASSERT_TRUE(engine.available());

    auto rdma = installStub(engine, RDMA, "<rdma-unavailable>",
                            Status::RdmaError("notify QP not connected"));
    auto tcp = installStub(engine, TCP, "<tcp-ok>", Status::OK());

    const Status status =
        engine.sendNotification(kRemote, makeNotification("no-fallback"));
    EXPECT_TRUE(status.IsRdmaError()) << status.ToString();
    EXPECT_EQ(rdma->send_calls, 1);
    EXPECT_EQ(tcp->send_calls, 0);
}

// One poll hands out what every notification transport queued plus the
// in-process queue, in slot order, and each notification exactly once. The
// second stand-in clears its argument the way TcpTransport does, so this
// also pins that every transport is polled into its own vector.
TEST(NotificationFallbackTest, ReceiveMergesAllNotificationTransports) {
    TransferEngineImpl engine(makeConfig());
    ASSERT_TRUE(engine.available());

    auto rdma = installStub(engine, RDMA, "<rdma>", Status::OK());
    auto tcp = installStub(engine, TCP, "<tcp>", Status::OK());
    rdma->queued = {makeNotification("rdma-0"), makeNotification("rdma-1")};
    tcp->queued = {makeNotification("tcp-0"), makeNotification("tcp-1")};
    ASSERT_TRUE(
        engine.sendNotification(LOCAL_SEGMENT_ID, makeNotification("local"))
            .ok());

    std::vector<Notification> received;
    ASSERT_TRUE(engine.receiveNotification(received).ok());
    ASSERT_EQ(received.size(), 5u);
    EXPECT_EQ(received[0].msg, "rdma-0");
    EXPECT_EQ(received[1].msg, "rdma-1");
    EXPECT_EQ(received[2].msg, "tcp-0");
    EXPECT_EQ(received[3].msg, "tcp-1");
    EXPECT_EQ(received[4].msg, "local");

    // Drained: nothing comes back twice.
    ASSERT_TRUE(engine.receiveNotification(received).ok());
    EXPECT_TRUE(received.empty());
}

// A transport whose queue cannot be read must not stop the drain: what the
// other paths carried is still handed out, and the error only surfaces on a
// poll that has nothing to deliver.
TEST(NotificationFallbackTest, ReceiveKeepsDrainingPastAFailingTransport) {
    TransferEngineImpl engine(makeConfig());
    ASSERT_TRUE(engine.available());

    auto rdma = installStub(engine, RDMA, "<rdma-broken>", Status::OK());
    rdma->recv_status = Status::InternalError("notify CQ poll failed");
    auto tcp = installStub(engine, TCP, "<tcp>", Status::OK());
    tcp->queued = {makeNotification("tcp-0")};
    ASSERT_TRUE(
        engine.sendNotification(LOCAL_SEGMENT_ID, makeNotification("local"))
            .ok());

    std::vector<Notification> received;
    ASSERT_TRUE(engine.receiveNotification(received).ok());
    ASSERT_EQ(received.size(), 2u);
    EXPECT_EQ(received[0].msg, "tcp-0");
    EXPECT_EQ(received[1].msg, "local");

    const Status empty = engine.receiveNotification(received);
    EXPECT_TRUE(empty.IsInternalError()) << empty.ToString();
    EXPECT_TRUE(received.empty());
}

// Two engines on loopback, neither with a TCP transport. The sender's only
// notification transport reports its channel unavailable, so the engine
// itself makes the control-plane RPC; the receiver, with nothing registered
// to take RPC notifications, must still hand it out from its next poll.
TEST(NotificationFallbackTest, RpcNotifyLandsWithoutTcpTransport) {
    TransferEngineImpl receiver(makeConfig());
    ASSERT_TRUE(receiver.available());
    TransferEngineImpl sender(makeConfig());
    ASSERT_TRUE(sender.available());

    // The receiver has an RDMA transport that never delivers anything; the
    // notification must not depend on it.
    auto receiver_rdma =
        installStub(receiver, RDMA, "<receiver-rdma>", Status::OK());
    auto sender_rdma =
        installStub(sender, RDMA, "<sender-rdma-unavailable>",
                    Status::RdmaError("RDMA notification channel unavailable"));

    SegmentID remote = ~0ull;
    ASSERT_TRUE(sender.openSegment(remote, receiver.getSegmentName()).ok());
    ASSERT_NE(remote, LOCAL_SEGMENT_ID);

    const Status status =
        sender.sendNotification(remote, makeNotification("over-rpc"));
    ASSERT_TRUE(status.ok()) << status.ToString();
    EXPECT_EQ(sender_rdma->send_calls, 1);

    // The RPC is synchronous, so the notification is queued by the time
    // sendNotification() returns; the loop only guards against a scheduler
    // hiccup between the reply and the poll.
    std::vector<Notification> received;
    for (int i = 0; i < 100 && received.empty(); ++i) {
        (void)receiver.receiveNotification(received);
        if (received.empty())
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_EQ(received.size(), 1u);
    EXPECT_EQ(received[0].name, "notification-fallback-test");
    EXPECT_EQ(received[0].msg, "over-rpc");
    EXPECT_TRUE(receiver_rdma->queued.empty());

    // Delivered exactly once.
    ASSERT_TRUE(receiver.receiveNotification(received).ok());
    EXPECT_TRUE(received.empty());
}

}  // namespace tent
}  // namespace mooncake

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

// Self-targeted (intra-agent) notifications must be delivered in-process.
//
// The data plane short-circuits LOCAL_SEGMENT_ID transfers to the local
// path, but notifications used to be routed unconditionally through the
// first transport that supports them; over RDMA the local pseudo-endpoint
// has no notification QP, so the send failed and a receiver polling for the
// notification hung forever. These tests pin the in-process delivery
// contract without requiring any NIC or a metadata server.

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "tent/common/config.h"
#include "tent/runtime/transfer_engine_impl.h"

namespace mooncake {
namespace tent {

namespace {

constexpr char kSegmentName[] = "local-notification-test-segment";

std::shared_ptr<Config> makeConfig(bool enable_tcp) {
    auto config = std::make_shared<Config>();
    config->set("metadata_type", "p2p");
    config->set("local_segment_name", kSegmentName);
    config->set("rpc_server_hostname", "127.0.0.1");
    config->set("rpc_server_port", "0");
    config->set("log_level", "warning");
    config->set("transports/tcp/enable", enable_tcp);
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
    return config;
}

}  // namespace

// Opening the local segment by the engine's own advertised name (or the
// empty name) must resolve to LOCAL_SEGMENT_ID - that is the id the delivery
// below keys on. Note: in p2p mode the advertised name is the normalized
// "ip:port", not the configured local_segment_name.
TEST(LocalNotificationTest, OwnNameResolvesToLocalSegmentId) {
    TransferEngineImpl engine(makeConfig(/*enable_tcp=*/true));
    ASSERT_TRUE(engine.available());

    SegmentID handle = ~0ull;
    ASSERT_TRUE(engine.openSegment(handle, engine.getSegmentName()).ok());
    EXPECT_EQ(handle, LOCAL_SEGMENT_ID);

    handle = ~0ull;
    ASSERT_TRUE(engine.openSegment(handle, "").ok());
    EXPECT_EQ(handle, LOCAL_SEGMENT_ID);
}

// Notifications sent to LOCAL_SEGMENT_ID come back from the next
// receiveNotification() poll, in order, exactly once.
TEST(LocalNotificationTest, SelfNotificationIsDeliveredInProcess) {
    TransferEngineImpl engine(makeConfig(/*enable_tcp=*/true));
    ASSERT_TRUE(engine.available());

    const int kCount = 8;
    for (int i = 0; i < kCount; ++i) {
        Notification notifi;
        notifi.name = kSegmentName;
        notifi.msg = "payload-" + std::to_string(i);
        ASSERT_TRUE(engine.sendNotification(LOCAL_SEGMENT_ID, notifi).ok());
    }

    std::vector<Notification> received;
    ASSERT_TRUE(engine.receiveNotification(received).ok());
    ASSERT_EQ(received.size(), static_cast<size_t>(kCount));
    for (int i = 0; i < kCount; ++i) {
        EXPECT_EQ(received[i].name, kSegmentName);
        EXPECT_EQ(received[i].msg, "payload-" + std::to_string(i));
    }

    // Delivered exactly once. Poll again through the same vector the caller
    // already used: a poll reports what that poll delivered, so the previous
    // batch must not reappear just because the caller reused its buffer.
    ASSERT_TRUE(engine.receiveNotification(received).ok());
    EXPECT_TRUE(received.empty());
}

// Self-delivery must not depend on any transport advertising notification
// support: with every transport disabled the local queue still works, while
// an empty poll keeps reporting the original "not supported" error.
TEST(LocalNotificationTest, WorksWithoutAnyNotificationTransport) {
    TransferEngineImpl engine(makeConfig(/*enable_tcp=*/false));
    ASSERT_TRUE(engine.available());

    std::vector<Notification> received;
    EXPECT_FALSE(engine.receiveNotification(received).ok());

    Notification notifi;
    notifi.name = kSegmentName;
    notifi.msg = "no-transport-payload";
    ASSERT_TRUE(engine.sendNotification(LOCAL_SEGMENT_ID, notifi).ok());

    ASSERT_TRUE(engine.receiveNotification(received).ok());
    ASSERT_EQ(received.size(), 1u);
    EXPECT_EQ(received[0].msg, "no-transport-payload");

    // Drained: the same vector comes back empty, and with nothing left to
    // deliver the "not supported" error is reported again.
    EXPECT_FALSE(engine.receiveNotification(received).ok());
    EXPECT_TRUE(received.empty());
}

// Stands in for the RDMA transport on a self-addressed notification: it
// advertises notification support, so the pre-fix routing picked it, but the
// local pseudo-endpoint owns no notification QP and the send fails
// ("Notification QP not connected"). Nothing is ever received from it.
class LocalSendFailsTransport : public Transport {
   public:
    int send_attempts = 0;

    Status install(std::string&, std::shared_ptr<ControlService>,
                   std::shared_ptr<Topology>,
                   std::shared_ptr<Config>) override {
        return Status::OK();
    }

    bool supportNotification() const override { return true; }

    Status sendNotification(SegmentID, const Notification&) override {
        ++send_attempts;
        return Status::InternalError("Notification QP not connected");
    }

    Status receiveNotification(std::vector<Notification>& out) override {
        out.clear();
        return Status::OK();
    }

    const char* getName() const override { return "<local-send-fails>"; }
};

// The failure this fix addresses: an intra-agent transfer that carries a
// notification. submitTransfer() binds the notification to the batch, and the
// completion poll fires it through maybeFireSubmitHooks() ->
// sendNotification(LOCAL_SEGMENT_ID). Before the fix that call was handed to
// the first notification-capable transport, whose local pseudo-endpoint has no
// notification QP, so the notification never arrived and a receiver polling
// for it hung.
//
// The stand-in transport above is what makes this a regression test rather
// than a round trip: left to a real transport the engine's own RPC server
// answers on loopback, so a self-addressed notification can reach itself over
// the wire even without the local short-circuit. Needs no NIC, no peer
// process and no metadata server.
TEST(LocalNotificationTest, TransferBoundSelfNotificationIsDelivered) {
    TransferEngineImpl engine(makeConfig(/*enable_tcp=*/true));
    ASSERT_TRUE(engine.available());

    constexpr size_t kBufLen = 4096;
    std::vector<uint8_t> source(kBufLen, 0x5A);
    std::vector<uint8_t> target(kBufLen, 0);
    ASSERT_TRUE(engine.registerLocalMemory(source.data(), kBufLen).ok());
    ASSERT_TRUE(engine.registerLocalMemory(target.data(), kBufLen).ok());

    // Registration is done, so the data path is set up; now put the
    // can't-deliver-locally transport ahead of TCP for notification routing.
    auto probe = std::make_shared<LocalSendFailsTransport>();
    std::string seg_name = engine.getSegmentName();
    ASSERT_TRUE(probe->install(seg_name, nullptr, nullptr, nullptr).ok());
    engine.swapTransportForTest(RDMA, probe);

    Notification notifi;
    notifi.name = kSegmentName;
    notifi.msg = "transfer-bound-self";

    Request req;
    req.opcode = Request::WRITE;
    req.source = source.data();
    req.target_id = LOCAL_SEGMENT_ID;
    req.target_offset = reinterpret_cast<uint64_t>(target.data());
    req.length = kBufLen;

    BatchID batch = engine.allocateBatch(4);
    ASSERT_TRUE(engine.submitTransfer(batch, {req}, notifi).ok());

    TransferStatus overall{};
    uint64_t polls = 0;
    while (true) {
        ASSERT_TRUE(engine.getTransferStatus(batch, overall).ok());
        if (overall.s == TransferStatusEnum::COMPLETED) break;
        ASSERT_NE(overall.s, TransferStatusEnum::FAILED);
        waitBeforeNextPoll(polls++);
        ASSERT_LT(polls, 100000u) << "local transfer did not complete";
    }
    EXPECT_EQ(target[0], 0x5A);
    EXPECT_EQ(target[kBufLen - 1], 0x5A);

    std::vector<Notification> received;
    ASSERT_TRUE(engine.receiveNotification(received).ok());
    ASSERT_EQ(received.size(), 1u);
    EXPECT_EQ(received[0].msg, "transfer-bound-self");
    // Delivered in-process: the transport was never asked to carry it.
    EXPECT_EQ(probe->send_attempts, 0);

    (void)engine.freeBatch(batch);
    (void)engine.unregisterLocalMemory(source.data(), kBufLen);
    (void)engine.unregisterLocalMemory(target.data(), kBufLen);
}

}  // namespace tent
}  // namespace mooncake

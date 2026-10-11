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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <future>
#include <iterator>
#include <limits>
#include <utility>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "tent/common/config.h"
#include "tent/transfer_engine.h"
#include "tent/runtime/control_plane.h"
#include "tent/runtime/transfer_engine_impl.h"

namespace mooncake {

// The outbox of an engine: what it holds and what it dropped.
class TransferEngineImplTestPeer {
   public:
    struct Outbox {
        size_t queued;
        bool sending;
        uint64_t deadline, full, unretryable, encode, shutdown;
    };

    static Outbox outbox(tent::TransferEngineImpl& engine) {
        std::lock_guard<std::mutex> lock(engine.notify_outbox_mutex_);
        const bool sending = engine.notify_outbox_sending_.has_value();
        return {engine.notify_outbox_size_ - (sending ? 1 : 0),
                sending,
                engine.notify_outbox_dropped_deadline_.load(),
                engine.notify_outbox_dropped_full_.load(),
                engine.notify_outbox_dropped_unretryable_.load(),
                engine.notify_outbox_dropped_encode_.load(),
                engine.notify_outbox_dropped_shutdown_.load()};
    }

    static void setClock(
        tent::TransferEngineImpl& engine,
        std::function<std::chrono::steady_clock::time_point()> now) {
        std::lock_guard<std::mutex> lock(engine.notify_outbox_mutex_);
        engine.notify_outbox_clock_ = std::move(now);
    }

    static void deconstruct(tent::TransferEngineImpl& engine) {
        (void)engine.deconstruct();
    }
};

namespace tent {

namespace {

using namespace std::chrono_literals;
using Peer = ::mooncake::TransferEngineImplTestPeer;

constexpr char kSegmentName[] = "local-notification-test-segment";

std::shared_ptr<Config> makeConfig(bool enable_tcp,
                                   const char* name = kSegmentName,
                                   bool rpc_fallback = true) {
    auto config = std::make_shared<Config>();
    config->set("metadata_type", "p2p");
    config->set("local_segment_name", name);
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
    config->set("topology/priority_matrix/cpu:0",
                std::vector<std::vector<std::string>>{{}, {}});
    // The progress worker would fire submit hooks from its own thread while
    // the test reads the stand-in transport's counter, and the runtime queue
    // turns it on implicitly. Keep every poll on the calling thread.
    config->set("enable_progress_worker", false);
    config->set("enable_runtime_queue", false);
    // Off where a stand-in is the only channel: the real TCP transport would
    // carry the notification instead and the peer would never look down.
    config->set("notification/rpc_fallback", rpc_fallback);
    return config;
}

// Polls `done` until it holds or `limit` runs out.
bool waitFor(const std::function<bool()>& done,
             std::chrono::milliseconds limit = 5s) {
    const auto deadline = std::chrono::steady_clock::now() + limit;
    while (!done()) {
        if (std::chrono::steady_clock::now() > deadline) return false;
        std::this_thread::sleep_for(5ms);
    }
    return true;
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

TEST(LocalNotificationTest, CApiResultCanBeReusedAfterFree) {
    TransferEngineImpl engine(makeConfig(/*enable_tcp=*/true));
    ASSERT_TRUE(engine.available());
    auto handle = reinterpret_cast<tent_engine_t>(&engine);

    ASSERT_EQ(
        tent_send_notifs(handle, LOCAL_SEGMENT_ID, "name", "first message"), 0);
    tent_notifi_info info{};
    ASSERT_EQ(tent_recv_notifs(handle, &info), 0);
    ASSERT_EQ(info.num_records, 1);
    ASSERT_NE(info.records, nullptr);

    tent_free_notifs(&info);
    EXPECT_EQ(info.num_records, 0);
    EXPECT_EQ(info.records, nullptr);

    ASSERT_EQ(tent_recv_notifs(handle, &info), 0);
    EXPECT_EQ(info.num_records, 0);
    ASSERT_EQ(info.records, nullptr);
    tent_free_notifs(&info);
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
// It has no no-dial send of its own, so a hook's try comes here as well, on
// the polling thread; the outbox thread's retries come from another one.
class LocalSendFailsTransport : public Transport {
   public:
    std::atomic<int> send_attempts{0};
    // Those made on the thread that built this stand-in, the test's.
    std::atomic<int> poller_attempts{0};
    std::atomic<int> delivered{0};
    // The stamp of the first attempt, and whether a later one differed.
    std::atomic<uint64_t> first_seq{0};
    std::atomic<bool> seq_changed{false};
    // Lets a test stop failing, standing in for a peer that comes back.
    std::atomic<bool> fail_sends{true};
    const std::thread::id poller = std::this_thread::get_id();

    Status install(std::string&, std::shared_ptr<ControlService>,
                   std::shared_ptr<Topology>,
                   std::shared_ptr<Config>) override {
        return Status::OK();
    }

    bool supportNotification() const override { return true; }

    Status sendNotification(SegmentID, const Notification& notifi) override {
        ++send_attempts;
        if (std::this_thread::get_id() == poller) ++poller_attempts;
        uint64_t first = 0;
        if (!first_seq.compare_exchange_strong(first, notifi.seq) &&
            first != notifi.seq)
            seq_changed = true;
        if (!fail_sends) {
            ++delivered;
            return Status::OK();
        }
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
    EXPECT_EQ(probe->send_attempts.load(), 0);

    (void)engine.freeBatch(batch);
    (void)engine.unregisterLocalMemory(source.data(), kBufLen);
    (void)engine.unregisterLocalMemory(target.data(), kBufLen);
}

// A hook tries each target once; a peer that fails is retried by the outbox
// until it takes delivery. Self-targeted sends always succeed, so a peer that
// stays unreachable must not make the engine re-queue the local copy: the
// receiver would see one "transfer complete" per retry and the local queue
// would grow without bound.
TEST(LocalNotificationTest, NotifiedTargetIsNotRedeliveredWhileAPeerFails) {
    TransferEngineImpl peer(makeConfig(/*enable_tcp=*/true, "peer-segment"));
    ASSERT_TRUE(peer.available());
    TransferEngineImpl engine(makeConfig(/*enable_tcp=*/true));
    ASSERT_TRUE(engine.available());

    constexpr size_t kBufLen = 4096;
    std::vector<uint8_t> source(kBufLen, 0x5A);
    std::vector<uint8_t> local_target(kBufLen, 0);
    std::vector<uint8_t> peer_target(kBufLen, 0);
    ASSERT_TRUE(engine.registerLocalMemory(source.data(), kBufLen).ok());
    ASSERT_TRUE(engine.registerLocalMemory(local_target.data(), kBufLen).ok());
    ASSERT_TRUE(peer.registerLocalMemory(peer_target.data(), kBufLen).ok());

    SegmentID remote = ~0ull;
    ASSERT_TRUE(engine.openSegment(remote, peer.getSegmentName()).ok());
    ASSERT_NE(remote, LOCAL_SEGMENT_ID);

    // Notification routing goes to a transport that always fails, so the hook's
    // remote leg never succeeds while the data path still completes.
    auto probe = std::make_shared<LocalSendFailsTransport>();
    std::string seg_name = engine.getSegmentName();
    ASSERT_TRUE(probe->install(seg_name, nullptr, nullptr, nullptr).ok());
    engine.swapTransportForTest(RDMA, probe);

    Notification notifi;
    notifi.name = kSegmentName;
    notifi.msg = "two-targets";

    Request local_req;
    local_req.opcode = Request::WRITE;
    local_req.source = source.data();
    local_req.target_id = LOCAL_SEGMENT_ID;
    local_req.target_offset = reinterpret_cast<uint64_t>(local_target.data());
    local_req.length = kBufLen;

    Request remote_req;
    remote_req.opcode = Request::WRITE;
    remote_req.source = source.data();
    remote_req.target_id = remote;
    remote_req.target_offset = reinterpret_cast<uint64_t>(peer_target.data());
    remote_req.length = kBufLen;

    BatchID batch = engine.allocateBatch(4);
    // Order does not matter any more: every target is attempted in the pass
    // that fires the hook. SelfNotificationIsNotHeldBackByAnUnreachablePeer
    // below is the test that pins that; this one pins exactly-once delivery.
    ASSERT_TRUE(
        engine.submitTransfer(batch, {remote_req, local_req}, notifi).ok());

    TransferStatus overall{};
    uint64_t polls = 0;
    while (true) {
        ASSERT_TRUE(engine.getTransferStatus(batch, overall).ok());
        if (overall.s == TransferStatusEnum::COMPLETED) break;
        ASSERT_NE(overall.s, TransferStatusEnum::FAILED);
        waitBeforeNextPoll(polls++);
        ASSERT_LT(polls, 100000u) << "two-target transfer did not complete";
    }

    // Keep polling while the outbox retries the peer, which keeps failing.
    size_t delivered = 0;
    auto poll = [&] {
        std::vector<Notification> received;
        (void)engine.receiveNotification(received);
        delivered += received.size();
        TransferStatus again{};
        (void)engine.getTransferStatus(batch, again);
    };
    (void)waitFor([&] {
        poll();
        return probe->send_attempts >= 3;
    });

    // Let the peer recover: the outbox delivers the remote leg, and the
    // target that already took delivery is not sent a second time.
    probe->fail_sends = false;
    EXPECT_TRUE(waitFor([&] {
        poll();
        return probe->delivered > 0;
    }));
    for (int i = 0; i < 3; ++i) poll();

    // Exactly once, across the failing phase and the recovery.
    EXPECT_EQ(delivered, 1u)
        << "self-notification delivered " << delivered << " times";
    // The peer really was retried while it was down, by the outbox: the
    // polls themselves tried it once, in the pass that fired the hook.
    EXPECT_GE(probe->send_attempts.load(), 3);
    EXPECT_EQ(probe->poller_attempts.load(), 1);
    EXPECT_EQ(probe->delivered.load(), 1);
    // Every retry of the remote leg carries the stamp of its first attempt.
    EXPECT_NE(probe->first_seq.load(), 0u);
    EXPECT_FALSE(probe->seq_changed.load());

    (void)engine.freeBatch(batch);
    (void)engine.unregisterLocalMemory(source.data(), kBufLen);
    (void)engine.unregisterLocalMemory(local_target.data(), kBufLen);
    (void)peer.unregisterLocalMemory(peer_target.data(), kBufLen);
}

// Delivery to one target must not depend on where an unreachable one happens
// to sit in the iteration order. hook.targets is an unordered_set; a loop that
// stopped at the first failure would leave every target behind it unattempted,
// and because the order is stable across polls it would stay that way. A
// self-targeted notification cannot fail, so an unrelated peer being down
// would block a local receiver until that peer came back.
//
// Two unreachable peers are what makes the check order-independent: with the
// fix the pass that fires the hook attempts both of them, without it that
// pass stops after one. That count is the deterministic part. The first-poll
// delivery assertion is the property callers actually care about, but on its
// own it only fails when the iteration order happens to put a peer first.
TEST(LocalNotificationTest, SelfNotificationIsNotHeldBackByAnUnreachablePeer) {
    TransferEngineImpl peer_a(makeConfig(/*enable_tcp=*/true, "peer-a"));
    ASSERT_TRUE(peer_a.available());
    TransferEngineImpl peer_b(makeConfig(/*enable_tcp=*/true, "peer-b"));
    ASSERT_TRUE(peer_b.available());
    TransferEngineImpl engine(makeConfig(/*enable_tcp=*/true));
    ASSERT_TRUE(engine.available());

    constexpr size_t kBufLen = 4096;
    std::vector<uint8_t> source(kBufLen, 0x5A);
    std::vector<uint8_t> local_target(kBufLen, 0);
    std::vector<uint8_t> a_target(kBufLen, 0);
    std::vector<uint8_t> b_target(kBufLen, 0);
    ASSERT_TRUE(engine.registerLocalMemory(source.data(), kBufLen).ok());
    ASSERT_TRUE(engine.registerLocalMemory(local_target.data(), kBufLen).ok());
    ASSERT_TRUE(peer_a.registerLocalMemory(a_target.data(), kBufLen).ok());
    ASSERT_TRUE(peer_b.registerLocalMemory(b_target.data(), kBufLen).ok());

    SegmentID remote_a = ~0ull, remote_b = ~0ull;
    ASSERT_TRUE(engine.openSegment(remote_a, peer_a.getSegmentName()).ok());
    ASSERT_TRUE(engine.openSegment(remote_b, peer_b.getSegmentName()).ok());
    ASSERT_NE(remote_a, LOCAL_SEGMENT_ID);
    ASSERT_NE(remote_b, LOCAL_SEGMENT_ID);
    ASSERT_NE(remote_a, remote_b);

    // The data path stays real - all three writes complete, which is what
    // lets the hook fire. Only notification routing is swapped for a
    // transport that fails, standing in for peers that cannot take delivery.
    auto probe = std::make_shared<LocalSendFailsTransport>();
    std::string seg_name = engine.getSegmentName();
    ASSERT_TRUE(probe->install(seg_name, nullptr, nullptr, nullptr).ok());
    engine.swapTransportForTest(RDMA, probe);

    Notification notifi;
    notifi.name = kSegmentName;
    notifi.msg = "three-targets";

    auto make_req = [&](SegmentID target, void* dst) {
        Request req;
        req.opcode = Request::WRITE;
        req.source = source.data();
        req.target_id = target;
        req.target_offset = reinterpret_cast<uint64_t>(dst);
        req.length = kBufLen;
        return req;
    };

    BatchID batch = engine.allocateBatch(8);
    ASSERT_TRUE(
        engine
            .submitTransfer(batch,
                            {make_req(remote_a, a_target.data()),
                             make_req(remote_b, b_target.data()),
                             make_req(LOCAL_SEGMENT_ID, local_target.data())},
                            notifi)
            .ok());

    TransferStatus overall{};
    uint64_t polls = 0;
    while (true) {
        ASSERT_TRUE(engine.getTransferStatus(batch, overall).ok());
        if (overall.s == TransferStatusEnum::COMPLETED) break;
        ASSERT_NE(overall.s, TransferStatusEnum::FAILED);
        waitBeforeNextPoll(polls++);
        ASSERT_LT(polls, 100000u) << "three-target transfer did not complete";
    }

    // The poll that saw COMPLETED already ran the hook, so the local
    // notification must be waiting now - while both peers are still down.
    size_t delivered = 0;
    std::vector<Notification> first;
    ASSERT_TRUE(engine.receiveNotification(first).ok());
    delivered += first.size();
    EXPECT_EQ(first.size(), 1u)
        << "the self-targeted notification was held back by an unreachable "
           "peer";
    if (!first.empty()) EXPECT_EQ(first[0].msg, "three-targets");

    // The pass that fired the hook tried both failing peers and handed them to
    // the outbox, and later passes leave them there; stopping at the first
    // failure would try one, whatever the iteration order.
    auto pass = [&] {
        TransferStatus again{};
        (void)engine.getTransferStatus(batch, again);
        std::vector<Notification> more;
        (void)engine.receiveNotification(more);
        delivered += more.size();
    };
    constexpr int kPasses = 4;
    for (int i = 0; i < kPasses; ++i) pass();
    EXPECT_EQ(probe->poller_attempts.load(), 2)
        << "the firing pass must attempt both unreachable peers, once";

    // Let both peers recover: the outbox delivers both, and the target that
    // already took delivery is not sent a second time.
    probe->fail_sends = false;
    EXPECT_TRUE(waitFor([&] {
        pass();
        return probe->delivered >= 2;
    }));
    for (int i = 0; i < 3; ++i) pass();
    EXPECT_EQ(probe->delivered.load(), 2);
    EXPECT_EQ(probe->poller_attempts.load(), 2);
    EXPECT_EQ(delivered, 1u)
        << "self-notification delivered " << delivered << " times";

    (void)engine.freeBatch(batch);
    (void)engine.unregisterLocalMemory(source.data(), kBufLen);
    (void)engine.unregisterLocalMemory(local_target.data(), kBufLen);
    (void)peer_a.unregisterLocalMemory(a_target.data(), kBufLen);
    (void)peer_b.unregisterLocalMemory(b_target.data(), kBufLen);
}

// C callers require termination even when a field fills its truncation limit.
TEST(LocalNotificationTest, CApiTerminatesFullLengthFields) {
    TransferEngineImpl engine(makeConfig(/*enable_tcp=*/false));
    ASSERT_TRUE(engine.available());
    for (const auto [name_length, message_length] :
         {std::pair<size_t, size_t>{0, 0},
          {254, 4094},
          {255, 4095},
          {256, 4096}}) {
        SCOPED_TRACE(name_length);
        Notification notification;
        notification.name.assign(name_length, 'n');
        notification.msg.assign(message_length, 'm');
        ASSERT_TRUE(
            engine.sendNotification(LOCAL_SEGMENT_ID, notification).ok());
        tent_notifi_info result{};
        ASSERT_EQ(tent_recv_notifs(&engine, &result), 0);
        std::unique_ptr<tent_notifi_record, decltype(&std::free)> records(
            result.records, &std::free);
        ASSERT_EQ(result.num_records, 1);
        ASSERT_NE(records, nullptr);
        const auto& record = *records;
        const auto copied_name = std::min(name_length, sizeof(record.name) - 1);
        const auto copied_message =
            std::min(message_length, sizeof(record.msg) - 1);
        EXPECT_EQ(std::string(record.name, copied_name),
                  notification.name.substr(0, copied_name));
        EXPECT_EQ(std::string(record.msg, copied_message),
                  notification.msg.substr(0, copied_message));
        EXPECT_EQ(record.name[copied_name], '\0');
        EXPECT_EQ(record.msg[copied_message], '\0');
    }
}

// A msg the control plane cannot encode (it is not UTF-8) makes the send to
// the peer throw. The hook drops and counts that one notification instead of
// throwing out of every later poll, and the self copy is delivered once.
TEST(LocalNotificationTest, APeerSendThatThrowsIsDroppedOnce) {
    TransferEngineImpl peer(makeConfig(/*enable_tcp=*/true, "peer-segment"));
    ASSERT_TRUE(peer.available());
    TransferEngineImpl engine(makeConfig(/*enable_tcp=*/true));
    ASSERT_TRUE(engine.available());

    constexpr size_t kBufLen = 4096;
    std::vector<uint8_t> source(kBufLen, 0x5A);
    std::vector<uint8_t> local_target(kBufLen, 0);
    std::vector<uint8_t> peer_target(kBufLen, 0);
    ASSERT_TRUE(engine.registerLocalMemory(source.data(), kBufLen).ok());
    ASSERT_TRUE(engine.registerLocalMemory(local_target.data(), kBufLen).ok());
    ASSERT_TRUE(peer.registerLocalMemory(peer_target.data(), kBufLen).ok());
    SegmentID remote = ~0ull;
    ASSERT_TRUE(engine.openSegment(remote, peer.getSegmentName()).ok());

    Request local_req;
    local_req.opcode = Request::WRITE;
    local_req.source = source.data();
    local_req.target_id = LOCAL_SEGMENT_ID;
    local_req.target_offset = reinterpret_cast<uint64_t>(local_target.data());
    local_req.length = kBufLen;
    Request remote_req = local_req;
    remote_req.target_id = remote;
    remote_req.target_offset = reinterpret_cast<uint64_t>(peer_target.data());

    Notification notifi;
    notifi.name = kSegmentName;
    notifi.msg = "not utf-8: \xff\xfe";
    BatchID batch = engine.allocateBatch(4);
    ASSERT_TRUE(
        engine.submitTransfer(batch, {remote_req, local_req}, notifi).ok());

    // Polls on well past completion, counting what escapes the engine.
    int throws = 0;
    size_t delivered = 0;
    int polls_after = 0;
    TransferStatus overall{};
    for (uint64_t polls = 0; polls < 100000 && polls_after < 20; ++polls) {
        try {
            ASSERT_TRUE(engine.getTransferStatus(batch, overall).ok());
        } catch (const std::exception&) {
            ++throws;
        }
        ASSERT_NE(overall.s, TransferStatusEnum::FAILED);
        std::vector<Notification> received;
        (void)engine.receiveNotification(received);
        delivered += received.size();
        if (overall.s == TransferStatusEnum::COMPLETED) ++polls_after;
        waitBeforeNextPoll(polls);
    }
    ASSERT_EQ(polls_after, 20) << "two-target transfer did not complete";
    EXPECT_EQ(throws, 0);
    EXPECT_EQ(delivered, 1u)
        << "self-notification delivered " << delivered << " times";
    const auto outbox = Peer::outbox(engine);
    EXPECT_EQ(outbox.encode, 1u);
    EXPECT_EQ(outbox.queued, 0u);

    (void)engine.freeBatch(batch);
    (void)engine.unregisterLocalMemory(source.data(), kBufLen);
    (void)engine.unregisterLocalMemory(local_target.data(), kBufLen);
    (void)peer.unregisterLocalMemory(peer_target.data(), kBufLen);
}

namespace {

// A peer's control plane, as an engine runs it, serving one memory segment
// that only the stand-in in the RDMA slot below can write. Its notify callback
// does not return until release(): a peer whose control plane is silent.
class SilentPeer {
   public:
    static constexpr size_t kSlot = 4096;

    SilentPeer() : memory_(4 * kSlot, 0) {
        // A host may divert a port elsewhere: move on until one answers.
        for (int attempt = 0; attempt < 5; ++attempt) {
            service_ = std::make_unique<ControlService>("p2p", "", nullptr);
            uint16_t port = 0;
            // A silent callback holds its io thread; keep the others free.
            EXPECT_TRUE(service_->start(port, false, /*threads=*/16).ok());
            addr_ = "127.0.0.1:" + std::to_string(port);
            Status probed;
            std::thread([&] {
                probed = ControlClient::probe(addr_, 200ms);
            }).join();
            if (probed.ok()) break;
        }
        PeerHealth::instance().clear();
        BufferDesc buffer;
        buffer.addr = reinterpret_cast<uint64_t>(memory_.data());
        buffer.length = memory_.size();
        buffer.location = "cpu:0";
        buffer.transports = {RDMA};
        EXPECT_TRUE(service_->segmentManager()
                        .updateLocal([&](SegmentDesc& desc) {
                            desc = SegmentDesc{
                                addr_, SegmentType::Memory, "silent-peer",
                                addr_, MemorySegmentDesc{}, ""};
                            std::get<MemorySegmentDesc>(desc.detail)
                                .buffers.push_back(buffer);
                            return Status::OK();
                        })
                        .ok());
        service_->setNotifyCallback([this](const Notification&) {
            ++notifies_;
            released_.wait();
            return 0;
        });
    }

    ~SilentPeer() { release(); }

    void release() {
        std::call_once(once_, [this] { release_.set_value(); });
    }
    const std::string& addr() const { return addr_; }
    uint64_t slot(size_t i) const {
        return reinterpret_cast<uint64_t>(memory_.data()) + i * kSlot;
    }
    int notifies() const { return notifies_; }

   private:
    std::promise<void> release_;
    std::shared_future<void> released_{release_.get_future().share()};
    std::once_flag once_;
    std::atomic<int> notifies_{0};
    std::vector<uint8_t> memory_;
    std::string addr_;
    std::unique_ptr<ControlService> service_;
};

// The RDMA slot of the outbox tests. A write completes when polled; the `*_at`
// fields make one fail, cancel, or complete behind a poll error. The no-dial
// send needs `connected`; the full send fails while `down`, or waits (hold_at).
class NotifyStub : public Transport {
   public:
    struct Call {
        std::string msg;
        uint64_t seq;
        std::thread::id thread;
        bool no_dial;
        bool delivered;
    };

    NotifyStub() { caps.dram_to_dram = true; }

    std::atomic<bool> connected{false};
    std::atomic<bool> down{true};
    // Set while no send that reads them can run.
    Status no_dial_failure = Status::DeviceNotFound("not connected");
    Status failure = Status::RdmaError("peer down");
    std::atomic<uint64_t> failed_at{0};
    std::atomic<uint64_t> canceled_at{0};
    std::atomic<uint64_t> poll_error_at{0};
    std::atomic<uint64_t> poll_error_too_at{0};
    std::atomic<int> hold_at{0};

    void hold() { held_ = true; }
    void release() {
        std::call_once(once_, [this] { release_.set_value(); });
    }
    int dials() const { return dials_; }
    std::vector<Call> tried() {
        return select([](const Call& call) { return call.no_dial; });
    }
    std::vector<Call> sent() {
        return select([](const Call& call) { return !call.no_dial; });
    }
    std::vector<Call> delivered() {
        return select([](const Call& call) { return call.delivered; });
    }
    std::vector<std::string> events() {
        std::lock_guard<std::mutex> lock(mutex_);
        return events_;
    }

    Status install(std::string&, std::shared_ptr<ControlService>,
                   std::shared_ptr<Topology>,
                   std::shared_ptr<Config>) override {
        return Status::OK();
    }

    Status allocateSubBatch(SubBatchRef& batch, size_t) override {
        batch = new Writes();
        return Status::OK();
    }

    Status freeSubBatch(SubBatchRef& batch) override {
        delete batch;
        batch = nullptr;
        return Status::OK();
    }

    Status submitTransferTasks(SubBatchRef batch,
                               const std::vector<Request>& requests) override {
        auto& writes = static_cast<Writes*>(batch)->requests;
        writes.insert(writes.end(), requests.begin(), requests.end());
        return Status::OK();
    }

    Status getTransferStatus(SubBatchRef batch, int task_id,
                             TransferStatus& status) override {
        const Request& req = static_cast<Writes*>(batch)->requests.at(task_id);
        status = TransferStatus{TransferStatusEnum::COMPLETED, req.length};
        if (req.target_offset == poll_error_at)
            return Status::InternalError("poll failed");
        if (req.target_offset == poll_error_too_at)
            return Status::DeviceNotFound("poll failed too");
        if (req.target_offset == failed_at)
            status = TransferStatus{TransferStatusEnum::FAILED, 0};
        if (req.target_offset == canceled_at)
            status = TransferStatus{TransferStatusEnum::CANCELED, 0};
        return Status::OK();
    }

    bool supportNotification() const override { return true; }

    Status sendNotificationIfConnected(SegmentID,
                                       const Notification& notifi) override {
        const bool up = connected;
        note(notifi, /*no_dial=*/true, up);
        return up ? Status::OK() : no_dial_failure;
    }

    Status sendNotification(SegmentID, const Notification& notifi) override {
        event("send");
        if (dials_++ == hold_at && held_) released_.wait();
        const bool up = !down;
        note(notifi, /*no_dial=*/false, up);
        event("sent");
        return up ? Status::OK() : failure;
    }

    Status receiveNotification(std::vector<Notification>& out) override {
        out.clear();
        return Status::OK();
    }

    Status quiesce() override {
        event("quiesce");
        return Status::OK();
    }

    const char* getName() const override { return "<notify-stub>"; }

   private:
    struct Writes : SubBatch {
        size_t size() const override { return requests.size(); }
        std::vector<Request> requests;
    };

    void note(const Notification& notifi, bool no_dial, bool delivered) {
        std::lock_guard<std::mutex> lock(mutex_);
        calls_.push_back({notifi.msg, notifi.seq, std::this_thread::get_id(),
                          no_dial, delivered});
    }
    void event(const char* what) {
        std::lock_guard<std::mutex> lock(mutex_);
        events_.push_back(what);
    }
    std::vector<Call> select(const std::function<bool(const Call&)>& keep) {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<Call> out;
        std::copy_if(calls_.begin(), calls_.end(), std::back_inserter(out),
                     keep);
        return out;
    }

    std::atomic<bool> held_{false};
    std::promise<void> release_;
    std::shared_future<void> released_{release_.get_future().share()};
    std::once_flag once_;
    std::atomic<int> dials_{0};
    std::mutex mutex_;
    std::vector<Call> calls_;
    std::vector<std::string> events_;
};

std::vector<std::string> msgsOf(const std::vector<NotifyStub::Call>& calls) {
    std::vector<std::string> msgs;
    for (const auto& call : calls) msgs.push_back(call.msg);
    return msgs;
}

// Bound notifications to the silent peer, written and notified through the
// stand-in in the RDMA slot.
class NotifyOutboxTest : public ::testing::Test {
   protected:
    void start(std::shared_ptr<Config> config) {
        peer_ = std::make_unique<SilentPeer>();
        engine_ = std::make_unique<TransferEngineImpl>(config);
        ASSERT_TRUE(engine_->available());
        ASSERT_TRUE(
            engine_->registerLocalMemory(source_.data(), source_.size()).ok());
        std::string name = engine_->getSegmentName();
        ASSERT_TRUE(stub_->install(name, nullptr, nullptr, nullptr).ok());
        engine_->swapTransportForTest(RDMA, stub_);  // the outbox is empty
        ASSERT_TRUE(engine_->openSegment(remote_, peer_->addr()).ok());
    }

    void TearDown() override {
        stub_->release();
        if (peer_) peer_->release();
        engine_.reset();
        peer_.reset();
        PeerHealth::instance().clear();
    }

    Request write(size_t slot) {
        Request req;
        req.opcode = Request::WRITE;
        req.source = source_.data();
        req.target_id = remote_;
        req.target_offset = peer_->slot(slot);
        req.length = source_.size();
        return req;
    }

    // One write to the peer's `slot` carrying `msg`.
    BatchID submit(const std::string& msg, size_t slot = 0) {
        Notification notifi;
        notifi.name = kSegmentName;
        notifi.msg = msg;
        BatchID batch = engine_->allocateBatch(1);
        EXPECT_TRUE(engine_->submitTransfer(batch, {write(slot)}, notifi).ok());
        return batch;
    }

    // Polls `batch` here until it is done; returns how it ended.
    TransferStatusEnum poll(BatchID batch,
                            std::chrono::nanoseconds* slowest = nullptr) {
        TransferStatus status{};
        for (uint64_t polls = 0; polls < 100000; ++polls) {
            const auto begin = std::chrono::steady_clock::now();
            EXPECT_TRUE(engine_->getTransferStatus(batch, status).ok());
            const auto took = std::chrono::steady_clock::now() - begin;
            if (slowest) *slowest = std::max(*slowest, took);
            if (status.s != TransferStatusEnum::PENDING) break;
            waitBeforeNextPoll(polls);
        }
        return status.s;
    }

    BatchID transfer(const std::string& msg, TransferStatusEnum& final_status,
                     std::chrono::nanoseconds* slowest = nullptr,
                     size_t slot = 0) {
        const BatchID batch = submit(msg, slot);
        final_status = poll(batch, slowest);
        return batch;
    }

    Peer::Outbox outbox() { return Peer::outbox(*engine_); }

    bool outboxIdle() {
        const auto now = outbox();
        return now.queued == 0 && !now.sending;
    }

    std::vector<uint8_t> source_ =
        std::vector<uint8_t>(SilentPeer::kSlot, 0x5A);
    std::shared_ptr<NotifyStub> stub_ = std::make_shared<NotifyStub>();
    std::unique_ptr<SilentPeer> peer_;
    std::unique_ptr<TransferEngineImpl> engine_;
    SegmentID remote_ = 0;
};

}  // namespace

// A batch freed right after it completed still notifies a peer that was down
// then, once the peer takes delivery again, with the stamp of the hook's try.
TEST_F(NotifyOutboxTest, AFreedBatchStillNotifiesWhenThePeerReturns) {
    ASSERT_NO_FATAL_FAILURE(start(makeConfig(true, kSegmentName, false)));
    TransferStatusEnum status = TransferStatusEnum::PENDING;
    const BatchID batch = transfer("freed", status);
    ASSERT_EQ(status, TransferStatusEnum::COMPLETED);
    ASSERT_TRUE(engine_->freeBatch(batch).ok());

    EXPECT_TRUE(waitFor([&] { return stub_->dials() >= 2; }))
        << "the peer was not retried after its batch was freed";
    stub_->down = false;
    ASSERT_TRUE(waitFor([&] { return !stub_->delivered().empty(); }));
    std::this_thread::sleep_for(100ms);
    const auto tried = stub_->tried();
    const auto delivered = stub_->delivered();
    ASSERT_EQ(tried.size(), 1u);
    ASSERT_EQ(delivered.size(), 1u);
    EXPECT_NE(tried[0].seq, 0u);
    EXPECT_EQ(delivered[0].seq, tried[0].seq);
    EXPECT_EQ(outbox().queued, 0u);
}

// A hook makes its one try with the no-dial send, on the polling thread. When
// that fails, the full send is the outbox thread's, with the same stamp; when
// the channel is up, nothing reaches the outbox.
TEST_F(NotifyOutboxTest, HookUsesTheNoDialSendAndFallsBackToTheOutbox) {
    ASSERT_NO_FATAL_FAILURE(start(makeConfig(true, kSegmentName, false)));
    stub_->down = false;
    TransferStatusEnum status = TransferStatusEnum::PENDING;
    (void)transfer("queued", status);
    ASSERT_EQ(status, TransferStatusEnum::COMPLETED);
    auto tried = stub_->tried();
    ASSERT_EQ(tried.size(), 1u);
    EXPECT_EQ(tried[0].thread, std::this_thread::get_id());
    ASSERT_TRUE(waitFor([&] { return stub_->delivered().size() == 1; }));
    const auto sent = stub_->delivered()[0];
    EXPECT_FALSE(sent.no_dial);
    EXPECT_NE(sent.thread, std::this_thread::get_id());
    EXPECT_EQ(sent.seq, tried[0].seq);

    ASSERT_TRUE(waitFor([&] { return outboxIdle(); }));
    stub_->connected = true;
    (void)transfer("direct", status);
    ASSERT_EQ(status, TransferStatusEnum::COMPLETED);
    tried = stub_->tried();
    ASSERT_EQ(tried.size(), 2u);
    EXPECT_TRUE(tried[1].delivered);
    EXPECT_EQ(msgsOf(stub_->delivered()),
              (std::vector<std::string>{"queued", "direct"}));
    EXPECT_EQ(stub_->dials(), 1);
}

// The notify channel is not up and the peer's Notify does not answer: the full
// send would hold the poll that completes the transfer for a request timeout.
TEST_F(NotifyOutboxTest, HookDeliveryNeverBlocksOnTheControlPlane) {
    auto config = makeConfig(true);
    config->set("rpc/request_timeout_ms", 1000);
    ASSERT_NO_FATAL_FAILURE(start(config));
    stub_->failure = Status::DeviceNotFound("notify endpoint unavailable");
    TransferStatusEnum status = TransferStatusEnum::PENDING;
    std::chrono::nanoseconds slowest{0};
    (void)transfer("silent", status, &slowest);
    ASSERT_EQ(status, TransferStatusEnum::COMPLETED);
    EXPECT_LT(slowest.count(), std::chrono::nanoseconds(500ms).count());
    // The outbox thread took it to the peer instead.
    EXPECT_TRUE(waitFor([&] { return peer_->notifies() >= 1; }));
}

// A notification the peer has not taken by the outbox deadline is dropped,
// counted, and not tried again.
TEST_F(NotifyOutboxTest, OutboxDropsAfterDeadline) {
    ASSERT_NO_FATAL_FAILURE(start(makeConfig(true, kSegmentName, false)));
    auto skew = std::make_shared<std::atomic<int64_t>>(0);
    Peer::setClock(*engine_, [skew] {
        return std::chrono::steady_clock::now() +
               std::chrono::milliseconds(skew->load());
    });
    TransferStatusEnum status = TransferStatusEnum::PENDING;
    (void)transfer("expires", status);
    ASSERT_EQ(status, TransferStatusEnum::COMPLETED);
    ASSERT_TRUE(waitFor([&] { return stub_->dials() >= 2; }));
    EXPECT_EQ(outbox().deadline, 0u);

    skew->store(120000 + 1000);  // past the default deadline
    ASSERT_TRUE(waitFor([&] { return outbox().deadline == 1; }));
    EXPECT_EQ(outbox().queued, 0u);
    const int dials = stub_->dials();
    std::this_thread::sleep_for(300ms);
    EXPECT_EQ(stub_->dials(), dials);
    EXPECT_TRUE(stub_->delivered().empty());
}

// An entry backed off past its deadline is dropped at the deadline, not when
// its backoff ends.
TEST_F(NotifyOutboxTest, ABackedOffEntryIsDroppedAtItsDeadline) {
    auto config = makeConfig(true, kSegmentName, false);
    config->set("notification/outbox_retry_ms", 2000);
    config->set("notification/outbox_deadline_ms", 500);
    ASSERT_NO_FATAL_FAILURE(start(config));
    TransferStatusEnum status = TransferStatusEnum::PENDING;
    (void)transfer("expires", status);
    ASSERT_EQ(status, TransferStatusEnum::COMPLETED);
    // Its first send failed and backed it off for 2 s.
    ASSERT_TRUE(
        waitFor([&] { return stub_->dials() == 1 && !outbox().sending; }));
    EXPECT_TRUE(waitFor([&] { return outbox().deadline == 1; }, 1200ms));
    EXPECT_EQ(outbox().queued, 0u);
    EXPECT_EQ(stub_->dials(), 1);
}

// An outbox at outbox_max makes room by dropping its oldest entry, and the
// one that moves up waits out the backoff the dropped one was in.
TEST_F(NotifyOutboxTest, OutboxIsBoundedAndDropsTheOldest) {
    auto config = makeConfig(true, kSegmentName, false);
    config->set("notification/outbox_max", 2);
    config->set("notification/outbox_retry_ms", 1000);
    ASSERT_NO_FATAL_FAILURE(start(config));
    TransferStatusEnum status = TransferStatusEnum::PENDING;
    (void)transfer("n1", status);
    ASSERT_EQ(status, TransferStatusEnum::COMPLETED);
    // n1 failed once and is back in the outbox, backed off for a second.
    ASSERT_TRUE(
        waitFor([&] { return stub_->dials() == 1 && outbox().queued == 1; }));
    for (const char* msg : {"n2", "n3"}) {
        (void)transfer(msg, status);
        ASSERT_EQ(status, TransferStatusEnum::COMPLETED);
    }
    EXPECT_EQ(outbox().full, 1u);
    EXPECT_EQ(outbox().queued, 2u);
    std::this_thread::sleep_for(500ms);
    EXPECT_EQ(stub_->dials(), 1) << "n2 went out before n1's backoff ended";

    stub_->down = false;
    ASSERT_TRUE(waitFor([&] { return stub_->delivered().size() == 2; }));
    EXPECT_EQ(msgsOf(stub_->delivered()),
              (std::vector<std::string>{"n2", "n3"}));
}

// A peer whose control plane does not answer is dialed once per backoff, not
// once for every notification queued for it, and the first backoff is
// outbox_retry_ms.
TEST_F(NotifyOutboxTest, OutboxDialsASilentPeerOnceARound) {
    auto config = makeConfig(true);
    config->set("rpc/request_timeout_ms", 300);
    config->set("notification/outbox_retry_ms", 1000);
    ASSERT_NO_FATAL_FAILURE(start(config));
    stub_->failure = Status::DeviceNotFound("notify endpoint unavailable");
    for (size_t i = 0; i < 3; ++i) {
        TransferStatusEnum status = TransferStatusEnum::PENDING;
        (void)transfer("s" + std::to_string(i), status, nullptr, i);
        ASSERT_EQ(status, TransferStatusEnum::COMPLETED);
    }

    ASSERT_TRUE(waitFor([&] { return stub_->dials() >= 1; }));
    // The first dial times out after 300 ms and the peer is backed off for a
    // second: s1 and s2 do not go out in between, s0 goes again after it.
    std::this_thread::sleep_for(900ms);
    EXPECT_EQ(stub_->dials(), 1);
    EXPECT_TRUE(waitFor([&] { return stub_->dials() >= 2; }, 2000ms));
    EXPECT_TRUE(stub_->delivered().empty());
}

// A transfer that failed owes no notification: none is tried, queued, or
// counted as dropped, whether its batch is freed or released at teardown.
TEST_F(NotifyOutboxTest, AFailedBatchNeverEntersTheOutbox) {
    ASSERT_NO_FATAL_FAILURE(start(makeConfig(true, kSegmentName, false)));
    stub_->connected = true;
    stub_->failed_at = peer_->slot(1);
    TransferStatusEnum status = TransferStatusEnum::PENDING;
    const BatchID freed = transfer("freed", status, nullptr, 1);
    ASSERT_EQ(status, TransferStatusEnum::FAILED);
    ASSERT_TRUE(engine_->freeBatch(freed).ok());
    (void)transfer("left-to-teardown", status, nullptr, 1);
    ASSERT_EQ(status, TransferStatusEnum::FAILED);

    std::this_thread::sleep_for(100ms);
    EXPECT_EQ(outbox().queued, 0u);
    Peer::deconstruct(*engine_);
    EXPECT_EQ(outbox().shutdown, 0u);
    EXPECT_TRUE(stub_->tried().empty());
    EXPECT_EQ(stub_->dials(), 0);
}

// A transfer that completed notifies from the poll that saw it, though the
// poll then failed on another transfer of the batch, as it may until teardown.
TEST_F(NotifyOutboxTest, ACompletedTransferNotifiesWhenAnotherFailsToPoll) {
    ASSERT_NO_FATAL_FAILURE(start(makeConfig(true, kSegmentName, false)));
    stub_->connected = true;
    stub_->poll_error_at = peer_->slot(1);
    Notification notifi;
    notifi.name = kSegmentName;
    notifi.msg = "completed";
    const BatchID batch = engine_->allocateBatch(2);
    ASSERT_TRUE(engine_->submitTransfer(batch, {write(0)}, notifi).ok());
    ASSERT_TRUE(engine_->submitTransfer(batch, {write(1)}).ok());
    TransferStatus overall{};
    EXPECT_FALSE(engine_->getTransferStatus(batch, overall).ok());
    EXPECT_EQ(msgsOf(stub_->delivered()),
              std::vector<std::string>{"completed"});
    EXPECT_FALSE(engine_->getTransferStatus(batch, overall).ok());
    EXPECT_EQ(stub_->tried().size(), 1u);

    Peer::deconstruct(*engine_);
    EXPECT_EQ(outbox().shutdown, 0u);
}

// A poll that fails on some transfers still polls the ones after them and
// returns the first error: a later interval that completed notifies from that
// poll, the failing ones once a later poll sees them complete.
TEST_F(NotifyOutboxTest, ACompletedIntervalAfterAFailingPollStillNotifies) {
    ASSERT_NO_FATAL_FAILURE(start(makeConfig(true, kSegmentName, false)));
    stub_->connected = true;
    stub_->poll_error_at = peer_->slot(1);
    stub_->poll_error_too_at = peer_->slot(2);
    const BatchID batch = engine_->allocateBatch(4);
    for (size_t slot = 0; slot < 4; ++slot) {
        Notification notifi;
        notifi.name = kSegmentName;
        notifi.msg = "interval" + std::to_string(slot);
        ASSERT_TRUE(engine_->submitTransfer(batch, {write(slot)}, notifi).ok());
    }
    TransferStatus overall{};
    const Status polled = engine_->getTransferStatus(batch, overall);
    EXPECT_TRUE(polled.IsInternalError()) << polled.ToString();
    EXPECT_EQ(overall.s, TransferStatusEnum::PENDING);
    EXPECT_EQ(overall.transferred_bytes, 2 * source_.size());
    EXPECT_EQ(msgsOf(stub_->delivered()),
              (std::vector<std::string>{"interval0", "interval3"}));
    EXPECT_FALSE(engine_->getTransferStatus(batch, overall).ok());
    EXPECT_EQ(stub_->tried().size(), 2u);

    stub_->poll_error_at = 0;
    stub_->poll_error_too_at = 0;
    ASSERT_TRUE(engine_->getTransferStatus(batch, overall).ok());
    EXPECT_EQ(overall.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(msgsOf(stub_->delivered()),
              (std::vector<std::string>{"interval0", "interval3", "interval1",
                                        "interval2"}));
}

// A canceled transfer owes no notification either.
TEST_F(NotifyOutboxTest, ACanceledBatchNeverEntersTheOutbox) {
    ASSERT_NO_FATAL_FAILURE(start(makeConfig(true, kSegmentName, false)));
    stub_->connected = true;
    stub_->canceled_at = peer_->slot(1);
    TransferStatusEnum status = TransferStatusEnum::PENDING;
    const BatchID freed = transfer("freed", status, nullptr, 1);
    ASSERT_EQ(status, TransferStatusEnum::CANCELED);
    ASSERT_TRUE(engine_->freeBatch(freed).ok());
    (void)transfer("left-to-teardown", status, nullptr, 1);
    ASSERT_EQ(status, TransferStatusEnum::CANCELED);

    Peer::deconstruct(*engine_);
    EXPECT_EQ(outbox().shutdown, 0u);
    EXPECT_TRUE(stub_->tried().empty());
    EXPECT_EQ(stub_->dials(), 0);
}

// InvalidArgument is not retried: from the hook's try it never reaches the
// outbox, and from the outbox thread's send it leaves the outbox.
TEST_F(NotifyOutboxTest, AnUnretryableNotificationIsDroppedNotRetried) {
    ASSERT_NO_FATAL_FAILURE(start(makeConfig(true, kSegmentName, false)));
    stub_->no_dial_failure = Status::InvalidArgument("malformed");
    TransferStatusEnum status = TransferStatusEnum::PENDING;
    (void)transfer("from-hook", status);
    ASSERT_EQ(status, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(outbox().unretryable, 1u);
    EXPECT_EQ(outbox().queued, 0u);

    stub_->no_dial_failure = Status::DeviceNotFound("not connected");
    stub_->failure = Status::InvalidArgument("malformed");
    (void)transfer("from-outbox", status);
    ASSERT_EQ(status, TransferStatusEnum::COMPLETED);
    ASSERT_TRUE(waitFor([&] { return outbox().unretryable == 2; }));
    std::this_thread::sleep_for(300ms);
    EXPECT_EQ(stub_->dials(), 1);
    EXPECT_TRUE(outboxIdle());
}

// A notification does not overtake an older one queued for the same peer:
// once anything waits there, the hook queues behind it without trying.
TEST_F(NotifyOutboxTest, ABoundNotificationQueuesBehindAnOlderOne) {
    auto config = makeConfig(true, kSegmentName, false);
    config->set("notification/outbox_retry_ms", 1000);
    ASSERT_NO_FATAL_FAILURE(start(config));
    TransferStatusEnum status = TransferStatusEnum::PENDING;
    (void)transfer("first", status);
    ASSERT_EQ(status, TransferStatusEnum::COMPLETED);
    ASSERT_TRUE(
        waitFor([&] { return stub_->dials() == 1 && outbox().queued == 1; }));

    stub_->connected = true;
    (void)transfer("second", status);
    ASSERT_EQ(status, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(stub_->tried().size(), 1u);
    stub_->down = false;
    ASSERT_TRUE(waitFor([&] { return stub_->delivered().size() == 2; }));
    EXPECT_EQ(msgsOf(stub_->delivered()),
              (std::vector<std::string>{"first", "second"}));
}

// The same while the older one is out on the outbox thread's send.
TEST_F(NotifyOutboxTest, ABoundNotificationQueuesBehindOneInFlight) {
    ASSERT_NO_FATAL_FAILURE(start(makeConfig(true, kSegmentName, false)));
    stub_->down = false;
    stub_->hold();
    TransferStatusEnum status = TransferStatusEnum::PENDING;
    (void)transfer("first", status);
    ASSERT_EQ(status, TransferStatusEnum::COMPLETED);
    ASSERT_TRUE(waitFor([&] { return stub_->dials() == 1; }));

    stub_->connected = true;
    (void)transfer("second", status);
    ASSERT_EQ(status, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(stub_->tried().size(), 1u);
    stub_->release();
    ASSERT_TRUE(waitFor([&] { return stub_->delivered().size() == 2; }));
    EXPECT_EQ(msgsOf(stub_->delivered()),
              (std::vector<std::string>{"first", "second"}));
}

// The entry out on the outbox thread's send counts toward outbox_max, so a
// send that fails and comes back cannot leave the outbox past it.
TEST_F(NotifyOutboxTest, AnEntryInFlightCountsTowardTheBound) {
    auto config = makeConfig(true, kSegmentName, false);
    config->set("notification/outbox_max", 2);
    ASSERT_NO_FATAL_FAILURE(start(config));
    stub_->hold();
    TransferStatusEnum status = TransferStatusEnum::PENDING;
    (void)transfer("a", status);
    ASSERT_EQ(status, TransferStatusEnum::COMPLETED);
    ASSERT_TRUE(waitFor([&] { return stub_->dials() == 1; }));
    for (const char* msg : {"b", "c"}) {
        (void)transfer(msg, status);
        ASSERT_EQ(status, TransferStatusEnum::COMPLETED);
    }
    EXPECT_EQ(outbox().full, 1u);  // b, the oldest queued

    stub_->release();
    stub_->down = false;
    ASSERT_TRUE(waitFor([&] { return stub_->delivered().size() == 2; }));
    std::this_thread::sleep_for(100ms);
    EXPECT_EQ(msgsOf(stub_->delivered()), (std::vector<std::string>{"a", "c"}));
}

// Settings big enough to overflow the backoff or the deadline arithmetic are
// clamped: the entry neither expires at once nor is retried in a busy loop.
TEST_F(NotifyOutboxTest, HugeOutboxSettingsAreClamped) {
    auto config = makeConfig(true, kSegmentName, false);
    const int64_t huge = std::numeric_limits<int64_t>::max() / 1000000 + 1;
    config->set("notification/outbox_retry_ms", huge);
    config->set("notification/outbox_deadline_ms", huge);
    ASSERT_NO_FATAL_FAILURE(start(config));
    TransferStatusEnum status = TransferStatusEnum::PENDING;
    (void)transfer("clamped", status);
    ASSERT_EQ(status, TransferStatusEnum::COMPLETED);
    ASSERT_TRUE(waitFor([&] { return stub_->dials() >= 1; }));
    std::this_thread::sleep_for(500ms);
    EXPECT_EQ(stub_->dials(), 1);
    EXPECT_EQ(outbox().deadline, 0u);
    EXPECT_EQ(outbox().queued, 1u);
}

// Teardown lets the outbox's send in flight finish before the transports
// quiesce, and counts what is left as dropped at shutdown.
TEST_F(NotifyOutboxTest, OutboxStopsBeforeTransportsOnTeardown) {
    ASSERT_NO_FATAL_FAILURE(start(makeConfig(true, kSegmentName, false)));
    stub_->hold();
    TransferStatusEnum status = TransferStatusEnum::PENDING;
    (void)transfer("in-flight", status);
    ASSERT_EQ(status, TransferStatusEnum::COMPLETED);
    ASSERT_TRUE(waitFor([&] { return stub_->dials() == 1; }));
    (void)transfer("queued", status);
    ASSERT_EQ(status, TransferStatusEnum::COMPLETED);

    auto stopped =
        std::async(std::launch::async, [&] { Peer::deconstruct(*engine_); });
    EXPECT_EQ(stopped.wait_for(300ms), std::future_status::timeout);
    stub_->release();
    stopped.get();
    EXPECT_EQ(stub_->events(),
              (std::vector<std::string>{"send", "sent", "quiesce"}));
    EXPECT_EQ(stub_->dials(), 1);
    EXPECT_EQ(outbox().shutdown, 2u);
}

// Settings of 0 keep the defaults: nothing is dropped as full or expired at
// once, and the retry is no busy loop.
TEST_F(NotifyOutboxTest, ZeroOutboxSettingsKeepTheDefaults) {
    auto config = makeConfig(true, kSegmentName, false);
    config->set("notification/outbox_max", 0);
    config->set("notification/outbox_deadline_ms", 0);
    config->set("notification/outbox_retry_ms", 0);
    ASSERT_NO_FATAL_FAILURE(start(config));
    TransferStatusEnum status = TransferStatusEnum::PENDING;
    (void)transfer("z1", status);
    ASSERT_EQ(status, TransferStatusEnum::COMPLETED);
    // z1 failed once and waits in the outbox; z2 must find room there.
    ASSERT_TRUE(waitFor([&] {
        const auto now = outbox();
        return stub_->dials() >= 1 && now.queued == 1 && !now.sending;
    }));
    (void)transfer("z2", status);
    ASSERT_EQ(status, TransferStatusEnum::COMPLETED);
    std::this_thread::sleep_for(300ms);
    const auto now = outbox();
    EXPECT_EQ(now.full, 0u);
    EXPECT_EQ(now.deadline, 0u);
    EXPECT_EQ(now.queued + (now.sending ? 1u : 0u), 2u);
    EXPECT_LE(stub_->dials(), 8);
}

namespace {

// One write to `peer`'s first slot on segment `id`, carrying `msg`; polled
// here until it is done.
TransferStatusEnum notifyPeer(TransferEngineImpl& engine, SegmentID id,
                              const SilentPeer& peer, const char* msg,
                              std::vector<uint8_t>& source) {
    Notification notifi;
    notifi.name = kSegmentName;
    notifi.msg = msg;
    Request req;
    req.opcode = Request::WRITE;
    req.source = source.data();
    req.target_id = id;
    req.target_offset = peer.slot(0);
    req.length = source.size();
    BatchID batch = engine.allocateBatch(1);
    EXPECT_TRUE(engine.submitTransfer(batch, {req}, notifi).ok());
    TransferStatus status{};
    for (uint64_t polls = 0; polls < 100000; ++polls) {
        EXPECT_TRUE(engine.getTransferStatus(batch, status).ok());
        if (status.s != TransferStatusEnum::PENDING) break;
        waitBeforeNextPoll(polls);
    }
    return status.s;
}

}  // namespace

// A full outbox drops the oldest entry of any peer, whichever peer it lists
// first; both orders are tried.
TEST_F(NotifyOutboxTest, AFullOutboxDropsTheOldestOfAnyPeer) {
    auto config = makeConfig(true, kSegmentName, false);
    config->set("notification/outbox_max", 2);
    config->set("notification/outbox_retry_ms", 2000);
    ASSERT_NO_FATAL_FAILURE(start(config));
    SilentPeer second;
    SegmentID other = 0;
    ASSERT_TRUE(engine_->openSegment(other, second.addr()).ok());
    auto settled = [&](int dials, size_t queued) {
        return waitFor([&] {
            const auto now = outbox();
            return stub_->dials() == dials && now.queued == queued &&
                   !now.sending;
        });
    };
    auto send = [&](SegmentID id, const SilentPeer& peer, const char* msg) {
        return notifyPeer(*engine_, id, peer, msg, source_);
    };
    using C = TransferStatusEnum;
    // a, then b for another peer: c drops a.
    ASSERT_EQ(send(remote_, *peer_, "a"), C::COMPLETED);
    ASSERT_TRUE(settled(1, 1));
    ASSERT_EQ(send(other, second, "b"), C::COMPLETED);
    ASSERT_TRUE(settled(2, 2));
    ASSERT_EQ(send(remote_, *peer_, "c"), C::COMPLETED);
    EXPECT_EQ(outbox().full, 1u);
    stub_->down = false;
    ASSERT_TRUE(waitFor([&] { return stub_->delivered().size() == 2; }));
    ASSERT_TRUE(waitFor([&] { return outboxIdle(); }));
    // The other way round: f drops d.
    stub_->down = true;
    const int base = stub_->dials();
    ASSERT_EQ(send(other, second, "d"), C::COMPLETED);
    ASSERT_TRUE(settled(base + 1, 1));
    ASSERT_EQ(send(remote_, *peer_, "e"), C::COMPLETED);
    ASSERT_TRUE(settled(base + 2, 2));
    ASSERT_EQ(send(other, second, "f"), C::COMPLETED);
    EXPECT_EQ(outbox().full, 2u);
    stub_->down = false;
    ASSERT_TRUE(waitFor([&] { return stub_->delivered().size() == 4; }));
    auto got = msgsOf(stub_->delivered());
    std::sort(got.begin(), got.end());
    EXPECT_EQ(got, (std::vector<std::string>{"b", "c", "e", "f"}));
}

// Peers due in the same round are sent oldest first, and teardown waits for
// the send in flight only: each further silent peer would add a request
// timeout to it.
TEST_F(NotifyOutboxTest, TeardownStartsNoFurtherSend) {
    auto config = makeConfig(true, kSegmentName, false);
    config->set("notification/outbox_retry_ms", 2000);
    ASSERT_NO_FATAL_FAILURE(start(config));
    auto skew = std::make_shared<std::atomic<int64_t>>(0);
    Peer::setClock(*engine_, [skew] {
        return std::chrono::steady_clock::now() +
               std::chrono::milliseconds(skew->load());
    });
    SilentPeer second;
    SegmentID other = 0;
    ASSERT_TRUE(engine_->openSegment(other, second.addr()).ok());
    using C = TransferStatusEnum;
    ASSERT_EQ(notifyPeer(*engine_, remote_, *peer_, "p1", source_),
              C::COMPLETED);
    ASSERT_TRUE(waitFor([&] { return stub_->dials() == 1; }));
    ASSERT_EQ(notifyPeer(*engine_, other, second, "p2", source_), C::COMPLETED);
    ASSERT_TRUE(waitFor([&] {
        const auto now = outbox();
        return stub_->dials() == 2 && now.queued == 2 && !now.sending;
    }));
    // Both are backed off for two seconds: make them due together, the first
    // send of that round held.
    stub_->hold_at = 2;
    stub_->hold();
    skew->store(5000);
    ASSERT_TRUE(waitFor([&] { return stub_->dials() == 3; }));
    auto stopped =
        std::async(std::launch::async, [&] { Peer::deconstruct(*engine_); });
    EXPECT_EQ(stopped.wait_for(300ms), std::future_status::timeout);
    stub_->release();
    stopped.get();
    EXPECT_EQ(msgsOf(stub_->sent()),
              (std::vector<std::string>{"p1", "p2", "p1"}));
    EXPECT_EQ(outbox().shutdown, 2u);
}

// Of two peers due in the same round, the second is checked against the
// deadline again when its turn comes: one that expired while the first send
// was out is dropped and counted, not sent.
TEST_F(NotifyOutboxTest, AnEntryThatExpiresBehindAnotherSendIsNotSent) {
    auto config = makeConfig(true, kSegmentName, false);
    config->set("notification/outbox_retry_ms", 2000);
    ASSERT_NO_FATAL_FAILURE(start(config));
    auto skew = std::make_shared<std::atomic<int64_t>>(0);
    Peer::setClock(*engine_, [skew] {
        return std::chrono::steady_clock::now() +
               std::chrono::milliseconds(skew->load());
    });
    SilentPeer second;
    SegmentID other = 0;
    ASSERT_TRUE(engine_->openSegment(other, second.addr()).ok());
    using C = TransferStatusEnum;
    ASSERT_EQ(notifyPeer(*engine_, remote_, *peer_, "p1", source_),
              C::COMPLETED);
    ASSERT_TRUE(waitFor([&] { return stub_->dials() == 1; }));
    ASSERT_EQ(notifyPeer(*engine_, other, second, "p2", source_), C::COMPLETED);
    ASSERT_TRUE(waitFor([&] {
        const auto now = outbox();
        return stub_->dials() == 2 && now.queued == 2 && !now.sending;
    }));
    // Both due together; p1's send is held while p2 passes the deadline.
    stub_->hold_at = 2;
    stub_->hold();
    skew->store(5000);
    ASSERT_TRUE(waitFor([&] { return stub_->dials() == 3; }));
    skew->store(5000 + 120000 + 1000);
    stub_->down = false;
    stub_->release();
    ASSERT_TRUE(waitFor([&] { return outboxIdle(); }));
    std::this_thread::sleep_for(300ms);
    EXPECT_EQ(msgsOf(stub_->delivered()), (std::vector<std::string>{"p1"}));
    EXPECT_EQ(stub_->dials(), 3);
    EXPECT_EQ(outbox().deadline, 1u);
}

}  // namespace tent
}  // namespace mooncake
